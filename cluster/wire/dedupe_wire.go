package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// The on-disk entry of conv §18 and the storage-format spec §2.1: a 36-byte key and a
// format-0x03 value of canonical varints. Format 0x02 (fixed-width fields, 35-byte header)
// is the build before the marker and is refused naming itself; before v1.1 nothing is
// migrated.
const (
	dedupeKeyLen      = 36
	dedupeEntryFormat = 0x03
	// dedupeMaxResult is the largest result an entry holds: the old 2-byte length's bound,
	// kept as an explicit constant now that the varint no longer limits it.
	dedupeMaxResult = math.MaxUint16
	// dedupeFingerprintLen is the one fixed-width field of the value.
	dedupeFingerprintLen = 8
)

// ErrDedupeResultTooLarge reports a result over dedupeMaxResult: a value-returning
// operation over the configured cap is refused at admission, so reaching this is a bug.
var ErrDedupeResultTooLarge = errors.New("dedupe result too large")

// DedupeEntryKey is the 36-byte key of one operation's dedupe entry:
// Scope ‖ 0x0E ‖ request id ‖ op index (conv §18). The identity is exact: the whole
// request id is in the key, so no two operations share an entry.
func DedupeEntryKey(ref storage.BucketRef, key machine.DedupeKey) []byte {
	return storage.Key(ref.Slot, ref.DatabaseID, ref.BucketID, storage.NSDedupe, key.RequestID[:], []byte{key.OpIndex})
}

// EncodeDedupeEntry returns the key and value of entry under ref:
// [format:1 = 0x03][ordinal:uvarint][epoch:uvarint][seq:uvarint][fingerprint:8 BE][resultLen:uvarint][result].
func EncodeDedupeEntry(ref storage.BucketRef, entry machine.DedupeEntry) ([]byte, []byte, error) {
	if len(entry.Result) > dedupeMaxResult {
		return nil, nil, fmt.Errorf("a result of %d bytes is over the %d-byte bound of a dedupe entry: %w", len(entry.Result), dedupeMaxResult, ErrDedupeResultTooLarge)
	}
	value := make([]byte, 0, 1+3*binary.MaxVarintLen64+dedupeFingerprintLen+binary.MaxVarintLen32+len(entry.Result))
	value = append(value, dedupeEntryFormat)
	value = binary.AppendUvarint(value, entry.Ordinal)
	value = binary.AppendUvarint(value, entry.Pos.Epoch)
	value = binary.AppendUvarint(value, entry.Pos.Seq)
	value = binary.BigEndian.AppendUint64(value, entry.Fingerprint)
	value = binary.AppendUvarint(value, uint64(len(entry.Result)))
	value = append(value, entry.Result...)
	return DedupeEntryKey(ref, entry.Key), value, nil
}

// DecodeDedupeEntry parses a stored entry and the bucket it belongs to. It checks every
// length against the bytes it was given before it copies anything, so a damaged or
// hostile value costs at most its own size, and it accepts only the canonical encoding
// (minimal varints, a result length that matches the bytes present), so every accepted
// value re-encodes byte for byte.
func DecodeDedupeEntry(key, value []byte) (storage.BucketRef, machine.DedupeEntry, error) {
	if len(key) != dedupeKeyLen || key[18] != storage.NSDedupe {
		return storage.BucketRef{}, machine.DedupeEntry{}, fmt.Errorf("dedupe key has %d bytes and namespace %#02x, want %d bytes in namespace %#02x", len(key), keyNamespace(key), dedupeKeyLen, storage.NSDedupe)
	}
	if len(value) == 0 {
		return storage.BucketRef{}, machine.DedupeEntry{}, errors.New("dedupe value is empty, want a format byte")
	}
	if value[0] != dedupeEntryFormat {
		return storage.BucketRef{}, machine.DedupeEntry{}, fmt.Errorf("dedupe value is format %#02x, this build reads format %#02x", value[0], dedupeEntryFormat)
	}
	rest := value[1:]
	var entry machine.DedupeEntry
	var err error
	if entry.Ordinal, rest, err = dedupeUvarint(rest, "ordinal"); err != nil {
		return storage.BucketRef{}, machine.DedupeEntry{}, err
	}
	if entry.Pos.Epoch, rest, err = dedupeUvarint(rest, "epoch"); err != nil {
		return storage.BucketRef{}, machine.DedupeEntry{}, err
	}
	if entry.Pos.Seq, rest, err = dedupeUvarint(rest, "seq"); err != nil {
		return storage.BucketRef{}, machine.DedupeEntry{}, err
	}
	if len(rest) < dedupeFingerprintLen {
		return storage.BucketRef{}, machine.DedupeEntry{}, fmt.Errorf("dedupe value ends %d bytes into the 8-byte fingerprint", len(rest))
	}
	entry.Fingerprint = binary.BigEndian.Uint64(rest[:dedupeFingerprintLen])
	rest = rest[dedupeFingerprintLen:]
	resultLen, rest, err := dedupeUvarint(rest, "result length")
	if err != nil {
		return storage.BucketRef{}, machine.DedupeEntry{}, err
	}
	if resultLen > dedupeMaxResult || uint64(len(rest)) != resultLen {
		return storage.BucketRef{}, machine.DedupeEntry{}, fmt.Errorf("dedupe value says a %d-byte result and carries %d bytes after its header", resultLen, len(rest))
	}
	entry.Result = append([]byte(nil), rest...)
	ref := storage.BucketRef{
		Slot:       binary.BigEndian.Uint16(key[0:2]),
		DatabaseID: binary.BigEndian.Uint64(key[2:10]),
		BucketID:   binary.BigEndian.Uint64(key[10:18]),
	}
	copy(entry.Key.RequestID[:], key[19:35])
	entry.Key.OpIndex = key[35]
	return ref, entry, nil
}

// dedupeUvarint reads one canonical uvarint from the front of buf and returns the rest. A
// truncated, overflowing or non-minimal encoding (a trailing continuation byte with no
// payload, 0x80 0x00) is refused, so the decoder accepts exactly what the encoder writes.
func dedupeUvarint(buf []byte, field string) (uint64, []byte, error) {
	v, n := binary.Uvarint(buf)
	switch {
	case n == 0:
		return 0, nil, fmt.Errorf("dedupe value ends inside its %s varint", field)
	case n < 0:
		return 0, nil, fmt.Errorf("dedupe value's %s varint overflows 64 bits", field)
	case n > 1 && buf[n-1] == 0:
		return 0, nil, fmt.Errorf("dedupe value's %s varint is not minimal", field)
	}
	return v, buf[n:], nil
}

func keyNamespace(key []byte) byte {
	if len(key) <= 18 {
		return 0
	}
	return key[18]
}

// RecordDedupe feeds a newly applied record to window and writes the entry it returns
// into batch, the batch that carries the write itself. It is the one place an entry is
// born, for the owner's apply and for a copy's. It returns the window as it stands once
// this record counts, and the caller keeps that only after the batch commits: the window
// is taken by value, so a batch that fails can never leave a live window inflated. The
// caller sends it only for a record the sequence rules have not already applied, or a
// replay would count a record twice and a live entry would be swept early. A refused
// write returns the window unchanged.
//
// A result over cfg.MaxResult is refused here, after the position was appended, which
// leaves the pair needing a replay that refuses it again. The oversize check therefore
// belongs to the applier of pop, take and claim, in its Precondition, before a position
// is taken (obligation of those units; nothing refuses it at admission today).
func RecordDedupe(batch storage.KVBatch, ref storage.BucketRef, window machine.Window, write machine.DedupeWrite, cfg machine.WindowConfig) (machine.Window, error) {
	// The entry's own bound, checked before the window moves: the configured cap is far
	// below it, so this is the backstop for a cap set above what an entry can hold.
	if len(write.Result) > dedupeMaxResult {
		return window, fmt.Errorf("a result of %d bytes is over the %d-byte bound of a dedupe entry, so a retry could not be answered from it: %w", len(write.Result), dedupeMaxResult, ErrDedupeResultTooLarge)
	}
	decision := window.Apply(write, cfg)
	if decision.Refuse {
		return window, fmt.Errorf("a result of %d bytes is over the %d-byte cap, so a retry could not be answered from it: %w", len(write.Result), cfg.MaxResult, ErrDedupeResultTooLarge)
	}
	key, value, err := EncodeDedupeEntry(ref, *decision.Write)
	if err != nil {
		return window, fmt.Errorf("encoding the dedupe entry of position %s: %w", write.Pos, err)
	}
	if err := batch.Set(key, value); err != nil {
		return window, fmt.Errorf("writing the dedupe entry of position %s: %w", write.Pos, err)
	}
	return window, nil
}

// CutOrdinal sets the pair's ordinal from its reloaded pair record or a seed cut
// (machine.OrdinalCut), in either direction. A reload calls it before the pair's first
// lookup; the owner path never calls it, because the ordinal moves with each apply.
func CutOrdinal(pair *cluster.Pair, ordinal uint64) {
	window := machine.Window{Ordinal: pair.Ordinal}
	window.Apply(machine.OrdinalCut{Ordinal: ordinal}, machine.WindowConfig{})
	pair.Ordinal = window.Ordinal
}

// KVDedupeReader reads dedupe entries from the shared store. It holds nothing but the
// store, so it costs nothing per bucket, and a seeded or caught-up copy answers from the
// keys it was given with no side channel.
type KVDedupeReader struct {
	kv storage.KV
}

// NewKVDedupeReader returns a reader over kv.
func NewKVDedupeReader(kv storage.KV) *KVDedupeReader {
	return &KVDedupeReader{kv: kv}
}

// Lookup point-reads exactly the entry's 36-byte key.
func (r *KVDedupeReader) Lookup(slot uint16, db, bucketID uint64, key machine.DedupeKey) (*machine.DedupeEntry, error) {
	ref := storage.BucketRef{Slot: slot, DatabaseID: db, BucketID: bucketID}
	storedKey := DedupeEntryKey(ref, key)
	snapshot := r.kv.NewSnapshot()
	defer snapshot.Discard()
	value, err := snapshot.Get(storedKey)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading the dedupe entry %x: %w", storedKey, err)
	}
	_, entry, err := DecodeDedupeEntry(storedKey, value)
	if err != nil {
		return nil, fmt.Errorf("decoding the dedupe entry %x: %w", storedKey, err)
	}
	return &entry, nil
}

// sweepPair names a pair for the sweeper's cursor.
type sweepPair struct {
	slot uint16
	db   uint64
}

// DedupeSweeper deletes the dedupe entries that have left the record window. It is the
// bounded background pass of conv §18: no goroutine of its own (the caller schedules
// Pass against the reclaim budget), and its only state is one cursor of at most 36
// bytes per pair it is sweeping, never one per bucket.
type DedupeSweeper struct {
	kv    storage.KV
	pairs *cluster.PairSet
	cfg   machine.WindowConfig
	// advice ends the report of an entry that does not decode: the node's ringDirAdvice, set by
	// the node; empty adds none.
	advice string

	mu      sync.Mutex
	cursors map[sweepPair][]byte
}

// NewDedupeSweeper returns a sweeper over kv that reads each pair's ordinal from pairs.
func NewDedupeSweeper(kv storage.KV, pairs *cluster.PairSet, cfg machine.WindowConfig) *DedupeSweeper {
	return &DedupeSweeper{kv: kv, pairs: pairs, cfg: cfg, cursors: map[sweepPair][]byte{}}
}

// sweepCandidate is one entry the scan saw, with the bytes it saw, so the reap can tell
// that a retry rewrote the key since.
type sweepCandidate struct {
	key   []byte
	value []byte
	entry machine.DedupeEntry
}

// sweepScan is what one bounded walk of the pair's range found.
type sweepScan struct {
	candidates []sweepCandidate
	// next is where the following pass resumes; done says the walk reached the end.
	next    []byte
	done    bool
	damaged error
	// visited counts the keys and seeks the walk spent, the cost the budget is charged.
	visited int
}

// Pass runs one bounded step for the pair: it visits at most budget keys of the pair's
// range, hopping over every namespace but 0x0E, and deletes the dead entries among
// those it saw, at most budget of them. The next Pass resumes where this one stopped
// and starts over after the end, so a pair is swept in full over repeated passes. It
// returns the keys it visited and the entries it deleted, the cost its caller charges to
// the reclaim budget. A pair this node does not hold is scanned and nothing is deleted.
func (s *DedupeSweeper) Pass(slot uint16, db uint64, budget int) (visited, deleted int, err error) {
	if budget <= 0 {
		return 0, 0, nil
	}
	pair := sweepPair{slot: slot, db: db}
	s.mu.Lock()
	from := s.cursors[pair]
	s.mu.Unlock()

	scanned, err := s.scan(slot, db, from, budget)
	if err != nil {
		return 0, 0, fmt.Errorf("sweeping the dedupe entries of slot %d database %d: %w", slot, db, err)
	}
	s.mu.Lock()
	if scanned.done {
		delete(s.cursors, pair)
	} else {
		s.cursors[pair] = scanned.next
	}
	s.mu.Unlock()

	deleted, reapErr := s.reap(slot, db, scanned.candidates, budget)
	if reapErr != nil {
		reapErr = fmt.Errorf("sweeping the dedupe entries of slot %d database %d: %w", slot, db, reapErr)
	}
	return scanned.visited, deleted, errors.Join(scanned.damaged, reapErr)
}

// Forget drops the pair's resume cursor: PairSet.Drop calls it, so a pair dropped mid-walk
// leaves nothing in the map for the life of the node.
func (s *DedupeSweeper) Forget(slot uint16, db uint64) {
	s.mu.Lock()
	delete(s.cursors, sweepPair{slot: slot, db: db})
	s.mu.Unlock()
}

// scan walks the pair's range from from. Everything that is not a dedupe key is jumped
// over with one seek per bucket, and each seek or key counts as one of the budget's
// visits, so a pass is bounded however the range is shaped. An entry that will not
// decode is reported and skipped: the pass must keep moving past it.
func (s *DedupeSweeper) scan(slot uint16, db uint64, from []byte, budget int) (sweepScan, error) {
	snapshot := s.kv.NewSnapshot()
	defer snapshot.Discard()
	prefix := storage.PairPrefix(slot, db)
	iter := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: keyRangeEnd(prefix)})
	defer iter.Close()
	iter.Seek(from)

	var out sweepScan
	var damaged []error
	exhausted := false
	visits := 0
	for ; visits < budget && iter.Valid() && !exhausted; visits++ {
		key := iter.Key()
		switch {
		case len(key) < 19:
			iter.Next()
		case key[18] < storage.NSDedupe:
			iter.Seek(append(append([]byte(nil), key[:18]...), storage.NSDedupe))
		case key[18] > storage.NSDedupe:
			end := keyRangeEnd(key[:18])
			if end == nil {
				exhausted = true
				break
			}
			iter.Seek(end)
		default:
			candidate, err := s.candidate(slot, db, iter)
			if err != nil {
				damaged = append(damaged, err)
			} else {
				out.candidates = append(out.candidates, candidate)
			}
			iter.Next()
		}
	}
	out.visited = visits
	if iter.Valid() && !exhausted {
		out.next = append([]byte(nil), iter.Key()...)
	} else {
		if err := iter.Err(); err != nil {
			return sweepScan{}, fmt.Errorf("walking the pair's range: %w", err)
		}
		out.done = true
	}
	out.damaged = errors.Join(damaged...)
	return out, nil
}

// candidate reads the entry under the iterator. One that does not decode is reported with
// the storage-format spec's R4 text and left in place: an entry of an older format or a
// damaged one is never deleted by a sweep, and never read as "no entry" by a lookup.
func (s *DedupeSweeper) candidate(slot uint16, db uint64, iter storage.KVIterator) (sweepCandidate, error) {
	key := append([]byte(nil), iter.Key()...)
	value, err := iter.Value()
	if err != nil {
		return sweepCandidate{}, fmt.Errorf("reading the dedupe entry %x: %w", key, err)
	}
	_, entry, err := DecodeDedupeEntry(key, value)
	if err != nil {
		return sweepCandidate{}, damagedDedupeEntry(key, slot, db, err, s.advice)
	}
	return sweepCandidate{key: key, value: value, entry: entry}, nil
}

// reap deletes the candidates that are dead now. It runs under the pair's lock, the one
// every apply holds while it commits, so no retry can rewrite an entry between the check
// and the delete; a key whose bytes changed since the scan was rewritten by such a retry
// and is left alone. The ordinal is read under the lock too: it only grows, so an entry
// live at the scan is live here or dead for good. A pair this node no longer holds is
// skipped: WithHeld never creates one, so a sweep of a pair Drop just forgot cannot
// resurrect it (the entries it left are the next holder's or the next drop's).
func (s *DedupeSweeper) reap(slot uint16, db uint64, candidates []sweepCandidate, budget int) (int, error) {
	var failure error
	deleted := 0
	s.pairs.WithHeld(slot, db, func(pair *cluster.Pair) {
		deleted, failure = s.reapLocked(pair.Ordinal, candidates, budget)
	})
	return deleted, failure
}

func (s *DedupeSweeper) reapLocked(ordinal uint64, candidates []sweepCandidate, budget int) (int, error) {
	seen := make([]machine.DedupeEntry, len(candidates))
	for i, c := range candidates {
		seen[i] = c.entry
	}
	window := machine.Window{Ordinal: ordinal}
	decision := window.Apply(machine.SweepBatch{Candidates: seen, Budget: budget}, s.cfg)
	if len(decision.Delete) == 0 {
		return 0, nil
	}
	doomed := make(map[machine.DedupeKey]struct{}, len(decision.Delete))
	for _, key := range decision.Delete {
		doomed[key] = struct{}{}
	}

	snapshot := s.kv.NewSnapshot()
	defer snapshot.Discard()
	batch := s.kv.NewBatch()
	defer batch.Discard()
	deleted := 0
	for _, c := range candidates {
		if _, ok := doomed[c.entry.Key]; !ok {
			continue
		}
		current, err := snapshot.Get(c.key)
		if errors.Is(err, storage.ErrKeyNotFound) || (err == nil && !bytes.Equal(current, c.value)) {
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("rechecking the dedupe entry %x: %w", c.key, err)
		}
		if err := batch.Delete(c.key); err != nil {
			return 0, fmt.Errorf("deleting the dedupe entry %x: %w", c.key, err)
		}
		deleted++
	}
	if deleted == 0 {
		return 0, nil
	}
	if err := batch.Commit(false); err != nil {
		return 0, fmt.Errorf("committing the deletion of %d dedupe entries: %w", deleted, err)
	}
	return deleted, nil
}

// keyRangeEnd is the smallest key above every key that begins with prefix, or nil when
// there is none.
func keyRangeEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

// damagedDedupeEntry is the storage-format spec's R4 report of a dedupe entry that does not decode.
// It names the damage; advice, the node's ringDirAdvice, says what to do about a damaged store,
// since a reseed is not safe advice for a cluster member (store-vouched-cut review R-1). An empty
// advice adds nothing.
func damagedDedupeEntry(key []byte, slot uint16, db uint64, err error, advice string) error {
	if advice != "" {
		advice = ". " + advice
	}
	return fmt.Errorf("the dedupe entry %x of slot %d database %d does not decode (%w) and is left in place; "+
		"a retry of its operation applies again once its window passes. Run 'status' to see this node; "+
		"a store that reports many of these is damaged%s", key, slot, db, err, advice)
}
