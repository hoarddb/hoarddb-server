package main

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/storage"
)

// digestFormat is the version of the digest file. compare refuses a file of another one.
const digestFormat = 1

const (
	// pairKeyBytes is the [slot:2][database id:8] prefix every key of one pair starts with
	// (storage.PairPrefix).
	pairKeyBytes = 10
	// namespaceOffset is where the namespace byte of a bucket key sits (storage.Scope is 18 bytes).
	namespaceOffset = 18
	// localPairRecordBytes is the 32-byte pair record of a node without a seed record per pair:
	// [epoch:8][seq:8][cursor:8][ordinal:8] (wire.LocalAdvancer).
	localPairRecordBytes = 32
)

// replicatedNamespaces are the namespaces whose keys and values every copy of a pair holds
// identically. Why the two left out are left out, and why the rest are in:
// the copy compare note.
var replicatedNamespaces = map[byte]bool{
	storage.NSDocuments:  true,
	storage.NSVersions:   true,
	storage.NSIndex:      true,
	storage.NSQueue:      true,
	storage.NSBlobMeta:   true,
	storage.NSFieldStore: true,
	storage.NSDelivery:   true,
}

// namespaceNames names a bucket namespace in a difference report.
var namespaceNames = map[byte]string{
	storage.NSDocuments:  "documents",
	storage.NSVersions:   "versions",
	storage.NSIndex:      "index",
	storage.NSQueue:      "queue",
	storage.NSBlobMeta:   "blob-meta",
	storage.NSFieldStore: "field-store",
	storage.NSDelivery:   "delivery",
}

// skippedLabels labels the skipped keys of the digest file; a namespace neither replicated nor
// listed here is counted as "other".
var skippedLabels = map[byte]string{
	storage.NSState:  "state",
	storage.NSDedupe: "dedupe",
}

// pairDigest is one (slot, database) pair of one node: where the pair stands and what it holds.
type pairDigest struct {
	Slot     uint16 `json:"slot"`
	Database uint64 `json:"database"`
	// State is the pair record's state: ready, seeding, retired, retiring, local (the 32-byte
	// shape of a node without seed records) or no-record (keys without any pair record).
	State        string `json:"state"`
	AppliedEpoch uint64 `json:"applied_epoch"`
	AppliedSeq   uint64 `json:"applied_seq"`
	LiveKeys     uint64 `json:"live_keys"`
	// ReclaimPending counts this node's delete records for the pair whose sweep has not finished.
	// A delete replicates as a tombstone; the sweep of the document's other keys is node-local, so
	// two healthy copies differ in keys until both sweeps are done.
	ReclaimPending uint64 `json:"reclaim_pending"`
	// Digest is the hex SHA-256 over the pair's replicated keys and values in key order.
	Digest string `json:"digest"`
}

// digestFile is what faultctl digest writes for one node.
type digestFile struct {
	Format int          `json:"format"`
	Node   string       `json:"node"`
	Pairs  []pairDigest `json:"pairs"`
	// SkippedKeys counts the keys of the held pairs that the digest left out, by label, so a
	// namespace added later shows here instead of vanishing.
	SkippedKeys map[string]uint64 `json:"skipped_keys"`
}

// pairKey names one pair.
type pairKey struct {
	slot     uint16
	database uint64
}

// pairRecordView is what the digest reads of a pair record: its state and applied position.
type pairRecordView struct {
	state                    string
	appliedEpoch, appliedSeq uint64
}

var seedStateNames = map[uint8]string{
	machine.SeedSeeding:  "seeding",
	machine.SeedReady:    "ready",
	machine.SeedRetired:  "retired",
	machine.SeedRetiring: "retiring",
}

// ringFormatFile is the marker cluster/wire writes first in a ring node's directory
// (wire.ringFormatFile), the parent of the badger directory. The legacy node store has none.
const ringFormatFile = "FORMAT"

// hasRingFormatMarker reports whether storeDir sits in a ring node's directory. It is the
// signal that cannot be absent on a ring store that has had no write yet, unlike a pair record;
// a pair record is the second signal, for a store copied out of its directory.
func hasRingFormatMarker(storeDir string) bool {
	_, err := os.Stat(filepath.Join(filepath.Dir(storeDir), ringFormatFile))
	return err == nil
}

// digestStore opens the stopped node store in storeDir read-only and digests every pair it holds.
func digestStore(storeDir, node string) (digestFile, error) {
	database, err := openReadOnly(storeDir)
	if err != nil {
		return digestFile{}, err
	}
	defer func() { _ = database.Close() }() // read-only: nothing to flush

	records, err := readPairRecords(database)
	if err != nil {
		return digestFile{}, fmt.Errorf("reading the pair records of %s: %w", storeDir, err)
	}
	if len(records) == 0 && !hasRingFormatMarker(storeDir) {
		return digestFile{}, fmt.Errorf("%s is not an entry-ring node store: it has no pair record, and no %s file in %s beside it. "+
			"A store with slot keys and no pair records is the legacy node store (<data_dir>/store), whose copies are not compared by pair and which this tool does not digest. "+
			"Pass --store <data_dir>/ring/badger of an entry-ring node", storeDir, ringFormatFile, filepath.Dir(storeDir))
	}
	reclaims, err := readReclaimCounts(database)
	if err != nil {
		return digestFile{}, fmt.Errorf("reading the delete records of %s: %w", storeDir, err)
	}
	digest := digestFile{Format: digestFormat, Node: node, SkippedKeys: map[string]uint64{}}
	cursor := newReplicatedCursor(database, nil, digest.SkippedKeys)
	defer cursor.close()
	var open *pairDigest
	var openHash hash.Hash
	closePair := func() {
		if open != nil {
			open.Digest = hex.EncodeToString(openHash.Sum(nil))
			digest.Pairs = append(digest.Pairs, *open)
		}
	}
	for cursor.advance() {
		key := pairKey{binary.BigEndian.Uint16(cursor.key[0:2]), binary.BigEndian.Uint64(cursor.key[2:10])}
		if open == nil || open.Slot != key.slot || open.Database != key.database {
			closePair()
			open, openHash = newPairDigest(key, records), sha256.New()
		}
		open.LiveKeys++
		open.ReclaimPending = reclaims[key]
		hashEntry(openHash, cursor.key, cursor.value)
	}
	if cursor.err != nil {
		return digestFile{}, fmt.Errorf("digesting %s: %w", storeDir, cursor.err)
	}
	closePair()
	digest.Pairs = addRecordOnlyPairs(digest.Pairs, records, reclaims)
	return digest, nil
}

func newPairDigest(key pairKey, records map[pairKey]pairRecordView) *pairDigest {
	view, found := records[key]
	if !found {
		view = pairRecordView{state: "no-record"}
	}
	return &pairDigest{Slot: key.slot, Database: key.database, State: view.state, AppliedEpoch: view.appliedEpoch, AppliedSeq: view.appliedSeq}
}

// addRecordOnlyPairs adds the pairs that have a record and no key, with the digest of nothing,
// and returns the list ordered by (slot, database).
func addRecordOnlyPairs(pairs []pairDigest, records map[pairKey]pairRecordView, reclaims map[pairKey]uint64) []pairDigest {
	seen := make(map[pairKey]bool, len(pairs))
	for _, pair := range pairs {
		seen[pairKey{pair.Slot, pair.Database}] = true
	}
	empty := sha256.Sum256(nil)
	for key := range records {
		if !seen[key] {
			held := newPairDigest(key, records)
			held.Digest = hex.EncodeToString(empty[:])
			held.ReclaimPending = reclaims[key]
			pairs = append(pairs, *held)
		}
	}
	slices.SortFunc(pairs, func(left, right pairDigest) int {
		return cmp.Or(cmp.Compare(left.Slot, right.Slot), cmp.Compare(left.Database, right.Database))
	})
	return pairs
}

// hashEntry feeds one key and value, each length-prefixed so no two sequences hash alike.
func hashEntry(sum hash.Hash, key, value []byte) {
	var length [binary.MaxVarintLen64]byte
	sum.Write(length[:binary.PutUvarint(length[:], uint64(len(key)))]) // hash.Hash.Write never returns an error
	sum.Write(key)
	sum.Write(length[:binary.PutUvarint(length[:], uint64(len(value)))])
	sum.Write(value)
}

// replicatedCursor walks the replicated keys of the slots in key order, one key and value at
// a time: constant memory, whatever the store holds.
type replicatedCursor struct {
	snapshot storage.KVSnapshot
	iterator storage.KVIterator
	skipped  map[string]uint64
	started  bool
	// key and value are the current entry; both are overwritten by the next advance.
	key, value []byte
	err        error
}

func newReplicatedCursor(database *storage.ReadOnlyKV, prefix []byte, skipped map[string]uint64) *replicatedCursor {
	snapshot := database.NewSnapshot()
	return &replicatedCursor{
		snapshot: snapshot,
		iterator: snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)}),
		skipped:  skipped,
	}
}

// prefixEnd is the exclusive upper bound of the keys that start with prefix; nil (unbounded)
// for an empty prefix or one of only 0xFF bytes.
func prefixEnd(prefix []byte) []byte {
	for index := len(prefix) - 1; index >= 0; index-- {
		if prefix[index] != 0xFF {
			end := append([]byte(nil), prefix[:index+1]...)
			end[index]++
			return end
		}
	}
	return nil
}

func (cursor *replicatedCursor) close() {
	cursor.iterator.Close()
	cursor.snapshot.Discard()
}

// advance moves to the next replicated key and reports whether there is one; cursor.err says
// why not when a read failed.
func (cursor *replicatedCursor) advance() bool {
	if cursor.err != nil {
		return false
	}
	if cursor.started {
		cursor.iterator.Next()
	} else {
		cursor.iterator.Seek(nil)
		cursor.started = true
	}
	for ; cursor.iterator.Valid(); cursor.iterator.Next() {
		cursor.key = append(cursor.key[:0], cursor.iterator.Key()...)
		if len(cursor.key) < pairKeyBytes {
			continue
		}
		if binary.BigEndian.Uint16(cursor.key[0:2]) >= storage.SlotCount {
			return false // the node block starts here and sorts after every slot
		}
		if len(cursor.key) <= namespaceOffset || !replicatedNamespaces[cursor.key[namespaceOffset]] {
			cursor.skipped[skippedLabel(cursor.key)]++
			continue
		}
		value, err := cursor.iterator.Value()
		if err != nil {
			cursor.err = fmt.Errorf("reading the value of key %x: %w", cursor.key, err)
			return false
		}
		cursor.value = append(cursor.value[:0], value...)
		return true
	}
	cursor.err = cursor.iterator.Err()
	return false
}

func skippedLabel(key []byte) string {
	if len(key) > namespaceOffset {
		if label, found := skippedLabels[key[namespaceOffset]]; found {
			return label
		}
	}
	return "other"
}

// readPairRecords reads every pair record under the seed-state namespace of the node block.
func readPairRecords(database *storage.ReadOnlyKV) (map[pairKey]pairRecordView, error) {
	records := map[pairKey]pairRecordView{}
	prefix := storage.NodeKey(storage.NSSeedState)
	snapshot := database.NewSnapshot()
	defer snapshot.Discard()
	iterator := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	defer iterator.Close()
	for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		key := iterator.Key()
		if len(key) != len(prefix)+pairKeyBytes {
			return nil, fmt.Errorf("a key of %d bytes in the seed-state namespace, want %d: the store is damaged", len(key), len(prefix)+pairKeyBytes)
		}
		raw, err := iterator.Value()
		if err != nil {
			return nil, fmt.Errorf("reading the pair record under key %x: %w", key, err)
		}
		view, err := decodePairRecord(raw)
		if err != nil {
			return nil, fmt.Errorf("pair record under key %x: %w", key, err)
		}
		records[pairKey{binary.BigEndian.Uint16(key[len(prefix):]), binary.BigEndian.Uint64(key[len(prefix)+2:])}] = view
	}
	if err := iterator.Err(); err != nil {
		return nil, fmt.Errorf("reading the pair records: %w", err)
	}
	return records, nil
}

// reclaimKeyHeadBytes is the node-block key up to and including the 'd' kind byte of a delete
// reclaim record (wire.ReclaimRecord): [0xFFFF][0 x 8][NSReclaim]['d'], followed by the
// [slot:2][database id:8] of the pair.
const reclaimKeyHeadBytes = 12

// readReclaimCounts counts the node's delete reclaim records per pair.
func readReclaimCounts(database *storage.ReadOnlyKV) (map[pairKey]uint64, error) {
	counts := map[pairKey]uint64{}
	prefix := append(storage.NodeKey(storage.NSReclaim), 'd')
	snapshot := database.NewSnapshot()
	defer snapshot.Discard()
	iterator := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
	defer iterator.Close()
	for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		key := iterator.Key()
		if len(key) < reclaimKeyHeadBytes+pairKeyBytes {
			return nil, fmt.Errorf("a delete record key of %d bytes is too short to name a pair: the store is damaged", len(key))
		}
		counts[pairKey{binary.BigEndian.Uint16(key[reclaimKeyHeadBytes:]), binary.BigEndian.Uint64(key[reclaimKeyHeadBytes+2:])}]++
	}
	if err := iterator.Err(); err != nil {
		return nil, fmt.Errorf("reading the delete records: %w", err)
	}
	return counts, nil
}

func decodePairRecord(raw []byte) (pairRecordView, error) {
	if len(raw) == localPairRecordBytes {
		return pairRecordView{state: "local", appliedEpoch: binary.BigEndian.Uint64(raw[0:]), appliedSeq: binary.BigEndian.Uint64(raw[8:])}, nil
	}
	record, err := wire.DecodeSeedRecord(raw)
	if err != nil {
		return pairRecordView{}, err
	}
	return pairRecordView{state: seedStateNames[record.State], appliedEpoch: record.Applied.Epoch, appliedSeq: record.Applied.Seq}, nil
}

// openReadOnly opens storeDir through the storage seam's read-only open and adds the flag the
// operator should pass when the directory is no store.
func openReadOnly(storeDir string) (*storage.ReadOnlyKV, error) {
	database, err := storage.OpenReadOnly(storeDir)
	if err != nil {
		if errors.Is(err, storage.ErrStoreSealed) {
			return nil, fmt.Errorf("%w. This tool does not read encrypted stores: compare nodes that run with encryption.mode off, or check the copies another way", err)
		}
		if strings.Contains(err.Error(), "is not a node store") {
			return nil, fmt.Errorf("%w. Pass --store the node's <data_dir>/ring/badger directory", err)
		}
		return nil, err
	}
	return database, nil
}

func encodeDigest(digest digestFile) ([]byte, error) {
	encoded, err := json.MarshalIndent(digest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encoding the digest of node %q: %w", digest.Node, err)
	}
	return append(encoded, '\n'), nil
}

func writeDigestFile(path string, digest digestFile) error {
	encoded, err := encodeDigest(digest)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		return fmt.Errorf("writing the digest file %s: %w", path, err)
	}
	return nil
}

func readDigestFile(path string) (digestFile, error) {
	encoded, err := os.ReadFile(path)
	if err != nil {
		return digestFile{}, fmt.Errorf("reading the digest file %s: %w. Make one on a stopped node with 'faultctl digest --store <data_dir>/ring/badger --node NAME --out %s'", path, err, path)
	}
	var digest digestFile
	if err := json.Unmarshal(encoded, &digest); err != nil {
		return digestFile{}, fmt.Errorf("%s is not a digest file: %w. Make one with 'faultctl digest'", path, err)
	}
	if digest.Format != digestFormat {
		return digestFile{}, fmt.Errorf("%s has digest format %d and this faultctl reads format %d: make it again with this faultctl", path, digest.Format, digestFormat)
	}
	return digest, nil
}
