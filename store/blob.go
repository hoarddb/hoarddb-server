package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ids"
	"github.com/hoarddb/hoarddb-server/sealed"
	"github.com/hoarddb/hoarddb-server/storage"
)

// BlobStore is a blob bucket on the node's shared store (shared-storage spec
// §3.5). Two maps live in the bucket's range: [s][id][0x01][user key] → blob
// id, and [s][id][0x06][blob id] → the blob's metadata, with content below
// InlineThreshold inline in it. Larger content is a file in one node-wide tree,
// <root>/<h[0:2]>/<h[2:4]>/<slot>-<bucket id>-<blob id>, so a blob bucket costs
// no directory, no descriptor and no goroutine.
//
// Every mutation goes through Apply under the bucket mutex the caller holds,
// and the order around the file is the correctness: a put makes its file whole
// and durable under its final name before the metadata commits, and a delete
// commits and syncs the metadata delete before it unlinks. A crash therefore
// leaves an orphan file — the orphan pass's (Step 17) — and never metadata
// naming a missing or torn one. owed is the bucket's failed-apply carry,
// which the caller owns: it outlives the engine (Carry).
type BlobStore struct {
	owed       *OwedApplies
	kv         storage.KV
	slot       uint16
	databaseID uint64
	id         uint64
	root       string
	// keys seals the payload files; nil on a plaintext node, whose payloads
	// are written and read as they are.
	keys sealed.BlobKeySource
}

// InlineThreshold is the size from which content is a file rather than part
// of its metadata value (spec §11 Q5, decided: 64 KiB).
const InlineThreshold = 64 << 10

// temporarySuffix marks a payload being written: the final name plus this, so
// the orphan pass can recognise a temporary of a name it can parse (SA-25).
const temporarySuffix = ".tmp"

// ErrPayloadLeft reports a blob whose metadata is gone but whose payload file
// could not be unlinked. The mutation itself succeeded; the file is an orphan
// the orphan pass removes, so callers report it and do not fail the request.
var ErrPayloadLeft = errors.New("a blob payload file was left behind for the orphan pass")

// ErrBlobChecksumMismatch reports a blob whose content was read whole but does
// not hash to the SHA-256 its put recorded: the metadata and the payload
// disagree (the payload may belong to another put). It is the third of the
// three read failures of at-rest encryption spec §4.2.1 h, and the only one the
// store decides: a missing key epoch is sealed.ErrBlobKeyMissing and a payload
// that fails authentication is sealed.ErrBlobCorrupt. Its text never carries
// either hash: a hash of the content in an error is a hash of the content in a
// log.
var ErrBlobChecksumMismatch = errors.New("the blob's content does not match the checksum its put recorded")

// blobBeforeRenameHook, when set, runs after a payload's temporary is written
// and synced and before it is renamed into place; an error stops the put
// there, as a crash would. Nil in production.
var blobBeforeRenameHook atomic.Pointer[func(tempPath string) error]

// blobBetweenDeleteStepsHook, when set, runs between a delete's two steps —
// the metadata commit and the unlink — with the payload's path; an error stops
// the delete there, as a crash would. Nil in production.
var blobBetweenDeleteStepsHook atomic.Pointer[func(payloadPath string) error]

// blobMeta is the value at [s][id][0x06][blob id]. Exactly one of Inline and
// Path holds the content: Path, relative to the blob root, when the content
// is a file. Inline is stored as "content" because the BSON library reads a
// field named "inline" as its inline-struct flag.
type blobMeta struct {
	Filename    string                 `bson:"filename"`
	ContentType string                 `bson:"content_type"`
	Size        int64                  `bson:"size"`
	SHA256      string                 `bson:"sha256"`
	CreatedAt   time.Time              `bson:"created_at"`
	Extra       map[string]interface{} `bson:"extra,omitempty"`
	Inline      []byte                 `bson:"content,omitempty"`
	Path        string                 `bson:"path,omitempty"`
}

// blobEnumeratedEntry is the document a dump emits for one blob: the fields
// the per-bucket store's dump carried, under the names it used, and never the
// content.
type blobEnumeratedEntry struct {
	ID          string                 `bson:"_id_"`
	Filename    string                 `bson:"filename"`
	ContentType string                 `bson:"content_type"`
	Size        int64                  `bson:"size"`
	Checksum    string                 `bson:"checksum"`
	CreatedAt   time.Time              `bson:"created_at"`
	Extra       map[string]interface{} `bson:"extra,omitempty"`
}

// StoredBlob is one blob as Get returns it.
type StoredBlob struct {
	BlobID   string
	Content  []byte
	Metadata map[string]interface{}
}

// BlobPayloads is where a node keeps its blob payload files and how it seals
// them.
type BlobPayloads struct {
	// Root is the node-wide payload tree.
	Root string
	// Keys is the node's blob key source: a sealed node passes one and every
	// payload is sealed with it; a plaintext node passes nil.
	Keys sealed.BlobKeySource
}

// NewBlobStore returns the blob store of bucket id in slot, which must be the
// bucket's recorded slot, carrying its failed applies in owed, with payload
// files where payloads says.
func NewBlobStore(kv storage.KV, ref storage.BucketRef, owed *OwedApplies, payloads BlobPayloads) *BlobStore {
	return &BlobStore{owed: owed, kv: kv, slot: ref.Slot, databaseID: ref.DatabaseID, id: ref.BucketID, root: payloads.Root, keys: payloads.Keys}
}

// Carry is the bucket's failed-apply carry the store was built with.
func (bs *BlobStore) Carry() *OwedApplies { return bs.owed }

// payloadSite is where one bucket's payload files live and under whose keys:
// what a blob bucket and a blobStore field of that bucket share.
type payloadSite struct {
	root     string
	slot     uint16
	bucketID uint64
	keys     sealed.BlobKeySource
}

func (bs *BlobStore) site() payloadSite {
	return payloadSite{root: bs.root, slot: bs.slot, bucketID: bs.id, keys: bs.keys}
}

// PayloadName is a payload file name, parsed.
type PayloadName struct {
	Slot     uint16
	BucketID uint64
	BlobID   string
	// Temporary is a payload a put had not yet renamed into place.
	Temporary bool
}

// PayloadFileName is the name of a blob's payload file,
// <slot:3 or 4 hex>-<bucket id:16 hex>-<blob id> (spec §3.5, §11 Q20). The slot
// leads because the orphan pass builds the blob's metadata key from the name
// alone; it is the bucket's recorded slot, never a recomputed one.
func PayloadFileName(slot uint16, bucketID uint64, blobID string) string {
	return fmt.Sprintf("%03x-%016x-%s", slot, bucketID, blobID)
}

// ParsePayloadFileName is PayloadFileName's inverse, and reports a temporary
// (the final name plus .tmp, SA-25) as such. Anything else — a foreign file, a
// name in another case, an id that is not a canonical UUID — does not parse:
// the orphan pass skips such a name and never unlinks it. The slot is 3 or 4
// hex digits: 4 once the slot passes 4095.
func ParsePayloadFileName(name string) (PayloadName, bool) {
	var parsed PayloadName
	if trimmed, ok := strings.CutSuffix(name, temporarySuffix); ok {
		name, parsed.Temporary = trimmed, true
	}
	// The slot is 3 hex digits below 4096 and 4 above it: a 13-bit slot
	// reaches 0x1FFF, and %03x does not truncate.
	slotEnd := strings.IndexByte(name, '-')
	if slotEnd != 3 && slotEnd != 4 {
		return PayloadName{}, false
	}
	idEnd := slotEnd + 1 + 16
	if len(name) <= idEnd+1 || name[idEnd] != '-' {
		return PayloadName{}, false
	}
	slot, ok := parseLowerHex(name[:slotEnd], 16)
	if !ok || slot >= storage.SlotCount {
		return PayloadName{}, false
	}
	bucketID, ok := parseLowerHex(name[slotEnd+1:idEnd], 64)
	if !ok {
		return PayloadName{}, false
	}
	blobID := name[idEnd+1:]
	if canonical, err := uuid.Parse(blobID); err != nil || canonical.String() != blobID {
		return PayloadName{}, false
	}
	parsed.Slot, parsed.BucketID, parsed.BlobID = uint16(slot), bucketID, blobID
	return parsed, true
}

// PayloadPathOf returns the payload file a blob's metadata value names,
// relative to the blob root, or "" when the content is inline. It is how the
// reclaimer finds a dropped blob bucket's files (spec §5.2.3 step 4) without
// opening the bucket's store.
func PayloadPathOf(metadataValue []byte) (string, error) {
	var meta blobMeta
	if err := bson.Unmarshal(metadataValue, &meta); err != nil {
		return "", fmt.Errorf("decoding a blob's metadata: %w", err)
	}
	return meta.Path, nil
}

// PayloadSizeOf returns the size a blob's metadata value records, which fixes
// the length of its sealed payload file. A migration target checks each
// payload it receives against it before sealing (spec §4.2.1 c).
func PayloadSizeOf(metadataValue []byte) (int64, error) {
	var meta blobMeta
	if err := bson.Unmarshal(metadataValue, &meta); err != nil {
		return 0, fmt.Errorf("decoding a blob's metadata: %w", err)
	}
	return meta.Size, nil
}

// RepointPayload returns a blob's metadata value with its payload path set to
// relPath. A migration target rewrites each file-backed blob's metadata this
// way, because the source's path names the source's slot and id and the
// target writes the file under its own (spec §5.3).
func RepointPayload(metadataValue []byte, relPath string) ([]byte, error) {
	var meta blobMeta
	if err := bson.Unmarshal(metadataValue, &meta); err != nil {
		return nil, fmt.Errorf("decoding a blob's metadata: %w", err)
	}
	meta.Path = relPath
	encoded, err := bson.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("encoding a blob's metadata: %w", err)
	}
	return encoded, nil
}

// parseLowerHex parses digits that are lowercase hex only; strconv alone
// would also take uppercase, which PayloadFileName never writes.
func parseLowerHex(digits string, bits int) (uint64, bool) {
	for _, digit := range digits {
		if (digit < '0' || digit > '9') && (digit < 'a' || digit > 'f') {
			return 0, false
		}
	}
	n, err := strconv.ParseUint(digits, 16, bits)
	return n, err == nil
}

// PayloadRelPath is where a payload lives under the blob root, named with the
// bucket's recorded slot and id. The two levels are the xxHash64 of the blob
// id, never the id's own prefix: a UUIDv7's first hex digits change once every
// ~49.7 days, which would put every blob of seven weeks in one leaf (spec
// §3.5, N-14).
func PayloadRelPath(slot uint16, bucketID uint64, blobID string) string {
	fanOut := fmt.Sprintf("%016x", xxhash.Sum64String(blobID))
	return filepath.Join(fanOut[0:2], fanOut[2:4], PayloadFileName(slot, bucketID, blobID))
}

func (bs *BlobStore) mapKey(userKey string) []byte {
	return storage.Key(bs.slot, bs.databaseID, bs.id, storage.NSDocuments, []byte(userKey))
}

func (bs *BlobStore) metaKey(blobID string) []byte {
	return storage.Key(bs.slot, bs.databaseID, bs.id, storage.NSBlobMeta, []byte(blobID))
}

// metaRange bounds [s][id][0x06], half-open.
func (bs *BlobStore) metaRange() storage.IterOptions {
	return storage.IterOptions{
		Lower: storage.Key(bs.slot, bs.databaseID, bs.id, storage.NSBlobMeta),
		Upper: storage.Key(bs.slot, bs.databaseID, bs.id, storage.NSBlobMeta+1),
	}
}

func (bs *BlobStore) apply(info ApplyInfo, op func(storage.KVSnapshot, *BucketState, storage.KVBatch) error) (bool, error) {
	return Apply(ApplyRequest{KV: bs.kv, Slot: bs.slot, DatabaseID: bs.databaseID, ID: bs.id, Keyed: true, Info: info, Owed: bs.owed, Op: op})
}

// Put stores content under userKey with its metadata and returns the new blob
// id. A key already present is an overwrite: its old blob's metadata goes in
// the same batch, and its old file after the commit. A replayed entry the gate
// skips writes nothing, not even its payload file.
//
// The payload file is written inside the apply, so a failure to write it is
// the store failing like a failed commit (prepareBlob marks it): carried,
// recorded and retried by replay. Written before the apply, its failure went
// unrecorded, and the owner never held a blob its replicas did.
func (bs *BlobStore) Put(info ApplyInfo, userKey string, content []byte, metadata map[string]interface{}) (string, error) {
	var (
		blobID       string
		meta         blobMeta
		replacedPath string
	)
	applied, err := bs.apply(info, func(snap storage.KVSnapshot, st *BucketState, b storage.KVBatch) error {
		var encoded []byte
		var err error
		if blobID, meta, encoded, err = prepareBlob(bs.site(), content, metadata); err != nil {
			return err
		}
		replaced, err := bs.readMapped(snap, userKey)
		if err != nil {
			return err
		}
		if replaced == nil {
			st.Entries++
		} else {
			st.LogicalBytes -= int64(len(userKey)) + replaced.meta.Size
			replacedPath = replaced.meta.Path
			if err := b.Delete(bs.metaKey(replaced.blobID)); err != nil {
				return fmt.Errorf("deleting the metadata of replaced blob %s: %w", replaced.blobID, err)
			}
		}
		st.LogicalBytes += int64(len(userKey)) + meta.Size
		if err := b.Set(bs.mapKey(userKey), []byte(blobID)); err != nil {
			return fmt.Errorf("mapping key %s to blob %s: %w", userKey, blobID, err)
		}
		if err := b.Set(bs.metaKey(blobID), encoded); err != nil {
			return fmt.Errorf("writing the metadata of blob %s: %w", blobID, err)
		}
		return nil
	})
	if err != nil {
		// A store failure may have committed after all (spec §4.4 rule 5), and
		// then the file is named: leave it for the orphan pass. Anything else
		// committed nothing, so no metadata names it.
		if !errors.Is(err, ErrApplyFailed) && meta.Path != "" {
			err = errors.Join(err, bs.removePayload(meta.Path))
		}
		return "", fmt.Errorf("putting blob under key %s: %w", userKey, err)
	}
	if !applied {
		return "", nil
	}
	if replacedPath != "" {
		return blobID, bs.unlinkAfterCommit(replacedPath)
	}
	return blobID, nil
}

// prepareBlob mints a blob's id and builds its metadata: content below
// InlineThreshold inline, anything larger written — durable, under its final
// name in the bucket (slot, bucketID) — before any batch can name it. A blob
// bucket's put and a blobStore field's put share it, so both keep spec §3.5's
// order. Both call it inside their apply's operation, and a failure to mint
// the id or to write the payload is marked a store failure: it says nothing
// about the write, so the apply carries it and replay retries it, where a
// refusal would be the entry's answer for good.
func prepareBlob(site payloadSite, content []byte, metadata map[string]interface{}) (string, blobMeta, []byte, error) {
	blobID, err := ids.NewDocumentID()
	if err != nil {
		return "", blobMeta{}, nil, asStoreFailure(fmt.Errorf("minting a blob id: %w", err))
	}
	checksum := sha256.Sum256(content)
	meta := blobMeta{
		Filename:    metadataString(metadata, "filename"),
		ContentType: metadataString(metadata, "content_type"),
		Size:        int64(len(content)),
		SHA256:      hex.EncodeToString(checksum[:]),
		CreatedAt:   time.Now().UTC(),
		Extra:       metadata,
	}
	if len(content) < InlineThreshold {
		meta.Inline = content
	} else {
		relPath, err := WritePayload(PayloadWrite{
			Root: site.root, Slot: site.slot, BucketID: site.bucketID, BlobID: blobID, Keys: site.keys, Content: content,
		})
		if err != nil {
			return "", blobMeta{}, nil, asStoreFailure(fmt.Errorf("writing blob %s: %w", blobID, err))
		}
		meta.Path = relPath
	}
	encoded, err := bson.Marshal(meta)
	if err != nil {
		return "", blobMeta{}, nil, fmt.Errorf("encoding the metadata of blob %s: %w", blobID, err)
	}
	return blobID, meta, encoded, nil
}

// PayloadWrite is one payload to make durable under its final name.
type PayloadWrite struct {
	Root     string
	Slot     uint16
	BucketID uint64
	BlobID   string
	// Keys seals the file; nil writes Content as it is, a plaintext node's.
	Keys    sealed.BlobKeySource
	Content []byte
}

// WritePayload makes a payload durable and returns its path under the blob
// root, spec §3.5's order: a temporary in the leaf, synced, renamed into
// place, and the leaf directory synced, so the rename is on disk before any
// metadata that names it can be. A put and a migration target (which writes
// every payload before the batch holding its metadata) both write through it.
func WritePayload(req PayloadWrite) (string, error) {
	relPath := PayloadRelPath(req.Slot, req.BucketID, req.BlobID)
	finalPath := filepath.Join(req.Root, relPath)
	leaf := filepath.Dir(finalPath)
	if err := os.MkdirAll(leaf, 0o700); err != nil {
		return "", fmt.Errorf("creating the payload directory %s: %w", leaf, err)
	}
	tempPath := finalPath + temporarySuffix
	if err := writeSynced(tempPath, payloadFiller(req)); err != nil {
		return "", errors.Join(err, removeIfPresent(tempPath))
	}
	if hook := blobBeforeRenameHook.Load(); hook != nil {
		if err := (*hook)(tempPath); err != nil {
			return "", errors.Join(err, removeIfPresent(tempPath))
		}
	}
	if err := os.Rename(tempPath, finalPath); err != nil {
		return "", errors.Join(fmt.Errorf("renaming %s into place: %w", tempPath, err), removeIfPresent(tempPath))
	}
	return relPath, syncDir(leaf)
}

// payloadFiller is what writes a payload's bytes: the sealed file of the
// codec on a node that has a key source, the content as it is on one that has
// not. The file key's identity is the final name, recomputed here from the
// slot, bucket and blob id: the .tmp it is written under is renamed away.
func payloadFiller(req PayloadWrite) func(io.Writer) error {
	if req.Keys == nil {
		return func(file io.Writer) error {
			_, err := file.Write(req.Content)
			return err
		}
	}
	return func(file io.Writer) error {
		return sealed.SealBlobFile(file, sealed.SealBlobRequest{
			Keys:        req.Keys,
			PayloadName: PayloadFileName(req.Slot, req.BucketID, req.BlobID),
			Content:     req.Content,
		})
	}
}

// PayloadRead is one payload file to read back.
type PayloadRead struct {
	Root string
	// Path is the metadata's path under Root. It locates the file and is never
	// a key derivation input (spec §4.2.1 f): the name is recomputed from Slot,
	// BucketID and BlobID.
	Path     string
	Slot     uint16
	BucketID uint64
	BlobID   string
	// Size is the metadata's size, which fixes a sealed file's length and chunk
	// count.
	Size int64
	// Keys opens the file; nil reads it as it is, a plaintext node's. A sealed
	// node never falls back to reading a file as plaintext (spec §4.2.1 e).
	Keys sealed.BlobKeySource
}

// ReadPayload returns a payload's content. On a sealed node it is all or
// nothing: the plaintext of a chunk that opened is never returned when a later
// chunk fails. A failure is sealed.ErrBlobKeyMissing, sealed.ErrBlobCorrupt or
// an I/O error, which the caller tells apart with errors.Is. It does not
// compare the content with the metadata's checksum; that is the caller's last
// step.
func ReadPayload(req PayloadRead) ([]byte, error) {
	path := filepath.Join(req.Root, req.Path)
	if req.Keys == nil {
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading the payload of blob %s: %w", req.BlobID, err)
		}
		return content, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening the payload of blob %s: %w", req.BlobID, err)
	}
	content, err := openSealedPayload(file, req)
	if closeErr := file.Close(); closeErr != nil {
		err = errors.Join(err, fmt.Errorf("closing %s: %w", path, closeErr))
	}
	if err != nil {
		return nil, err
	}
	return content, nil
}

func openSealedPayload(file *os.File, req PayloadRead) ([]byte, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("measuring the payload of blob %s: %w", req.BlobID, err)
	}
	content, err := sealed.OpenBlobFile(file, sealed.OpenBlobRequest{
		Keys:        req.Keys,
		PayloadName: PayloadFileName(req.Slot, req.BucketID, req.BlobID),
		Size:        req.Size,
		FileLen:     info.Size(),
	})
	if err != nil {
		return nil, fmt.Errorf("blob %s: %w", req.BlobID, err)
	}
	return content, nil
}

// writeSynced creates path at 0600, fills it and syncs it.
func writeSynced(path string, fill func(io.Writer) error) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("creating %s: %w", path, err)
	}
	if err := fill(file); err != nil {
		return errors.Join(fmt.Errorf("writing %s: %w", path, err), file.Close())
	}
	if err := file.Sync(); err != nil {
		return errors.Join(fmt.Errorf("syncing %s: %w", path, err), file.Close())
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", path, err)
	}
	return nil
}

func syncDir(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("opening %s to sync it: %w", dir, err)
	}
	if err := handle.Sync(); err != nil {
		return errors.Join(fmt.Errorf("syncing %s: %w", dir, err), handle.Close())
	}
	if err := handle.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", dir, err)
	}
	return nil
}

func removeIfPresent(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("removing %s: %w", path, err)
	}
	return nil
}

// removePayload unlinks a payload no metadata names.
func (bs *BlobStore) removePayload(relPath string) error {
	return RemovePayload(bs.root, relPath)
}

// RemovePayload unlinks the payload at relPath under root, which no metadata
// names: a put that committed nothing. A failure is ErrPayloadLeft.
func RemovePayload(root, relPath string) error {
	if err := removeIfPresent(filepath.Join(root, relPath)); err != nil {
		return fmt.Errorf("%w: %w", ErrPayloadLeft, err)
	}
	return nil
}

// unlinkAfterCommit is a delete's second step.
func (bs *BlobStore) unlinkAfterCommit(relPath string) error {
	return UnlinkPayloadAfterCommit(bs.kv, bs.root, relPath)
}

// UnlinkPayloadAfterCommit is a delete's second step, for a blob bucket and a
// blobStore field alike. The metadata delete has committed, but unsynced,
// like every apply; it is synced first, because the unlink reaches the disk
// through the filesystem's journal on its own, and a power loss that kept the
// unlink and lost the delete would bring back metadata naming a missing file.
func UnlinkPayloadAfterCommit(kv storage.KV, root, relPath string) error {
	if err := kv.Sync(); err != nil {
		return fmt.Errorf("%w: syncing the metadata delete before unlinking %s: %w", ErrPayloadLeft, relPath, err)
	}
	if hook := blobBetweenDeleteStepsHook.Load(); hook != nil {
		if err := (*hook)(filepath.Join(root, relPath)); err != nil {
			return err
		}
	}
	return RemovePayload(root, relPath)
}

// mappedBlob is the blob a user key names.
type mappedBlob struct {
	blobID string
	meta   blobMeta
}

// readMapped returns the blob userKey names in snap, or nil.
func (bs *BlobStore) readMapped(snap storage.KVSnapshot, userKey string) (*mappedBlob, error) {
	blobID, err := snap.Get(bs.mapKey(userKey))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading the blob id of key %s: %w", userKey, err)
	}
	encoded, err := snap.Get(bs.metaKey(string(blobID)))
	if err != nil {
		return nil, fmt.Errorf("reading the metadata of blob %s (key %s): %w", blobID, userKey, err)
	}
	var meta blobMeta
	if err := bson.Unmarshal(encoded, &meta); err != nil {
		return nil, fmt.Errorf("decoding the metadata of blob %s (key %s): %w", blobID, userKey, err)
	}
	return &mappedBlob{blobID: string(blobID), meta: meta}, nil
}

// Delete removes the blob under userKey and reports whether there was one:
// the map and metadata keys in one batch, then — synced — the file.
func (bs *BlobStore) Delete(info ApplyInfo, userKey string) (bool, error) {
	var deleted *mappedBlob
	applied, err := bs.apply(info, func(snap storage.KVSnapshot, st *BucketState, b storage.KVBatch) error {
		mapped, err := bs.readMapped(snap, userKey)
		if err != nil || mapped == nil {
			return err
		}
		deleted = mapped
		st.Entries--
		st.LogicalBytes -= int64(len(userKey)) + mapped.meta.Size
		if err := b.Delete(bs.mapKey(userKey)); err != nil {
			return fmt.Errorf("unmapping key %s: %w", userKey, err)
		}
		if err := b.Delete(bs.metaKey(mapped.blobID)); err != nil {
			return fmt.Errorf("deleting the metadata of blob %s: %w", mapped.blobID, err)
		}
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("deleting the blob under key %s: %w", userKey, err)
	}
	if !applied || deleted == nil {
		return false, nil
	}
	if deleted.meta.Path != "" {
		return true, bs.unlinkAfterCommit(deleted.meta.Path)
	}
	return true, nil
}

// Get returns the blob under userKey, its content checked against the
// checksum its put recorded, or nil when the key names none. The caller holds
// the bucket's read lock, so no overwrite can unlink the file between the
// metadata read and the file read.
func (bs *BlobStore) Get(userKey string) (*StoredBlob, error) {
	snap := bs.kv.NewSnapshot()
	defer snap.Discard()
	mapped, err := bs.readMapped(snap, userKey)
	if err != nil || mapped == nil {
		return nil, err
	}
	stored, err := readBlobContent(bs.site(), mapped.blobID, mapped.meta)
	if err != nil {
		return nil, fmt.Errorf("key %s: %w", userKey, err)
	}
	return stored, nil
}

// readBlobContent reads a blob's content — inline, or its payload file under
// root — and checks it against the checksum its put recorded. A mismatch is
// ErrBlobChecksumMismatch, and says so without printing either hash.
func readBlobContent(site payloadSite, blobID string, meta blobMeta) (*StoredBlob, error) {
	content := meta.Inline
	if meta.Path != "" {
		var err error
		content, err = ReadPayload(PayloadRead{
			Root: site.root, Path: meta.Path, Slot: site.slot, BucketID: site.bucketID,
			BlobID: blobID, Size: meta.Size, Keys: site.keys,
		})
		if err != nil {
			return nil, err
		}
	}
	checksum := sha256.Sum256(content)
	if got := hex.EncodeToString(checksum[:]); got != meta.SHA256 {
		return nil, fmt.Errorf("blob %s was read whole (%d bytes), but its content does not match what its put recorded: "+
			"the metadata and the payload disagree. Nothing was returned. Re-put the blob, or restore its metadata and its "+
			"payload from the same backup: %w", blobID, len(content), ErrBlobChecksumMismatch)
	}
	if content == nil {
		content = []byte{}
	}
	return &StoredBlob{BlobID: blobID, Content: content, Metadata: meta.Extra}, nil
}

// Len is the bucket's blob count from its state document, exact after a
// restart without a scan.
func (bs *BlobStore) Len() (int64, error) {
	snap := bs.kv.NewSnapshot()
	defer snap.Discard()
	st, err := LoadBucketState(snap, bs.slot, bs.databaseID, bs.id)
	if err != nil {
		return 0, fmt.Errorf("reading the blob count: %w", err)
	}
	return st.Entries, nil
}

// DocCount counts the blobs by walking their metadata, as a dump's manifest
// requires (store/enumerate.go).
func (bs *BlobStore) DocCount() (int64, error) {
	var n int64
	err := bs.IterateDocuments(func(_, _ []byte) error {
		n++
		return nil
	})
	return n, err
}

// IterateDocuments walks every blob's metadata in one snapshot, in blob id
// order, and invokes fn with the blob id and a document of its metadata. It
// never reads a payload, so a dump of a blob bucket touches no payload path.
func (bs *BlobStore) IterateDocuments(fn func(key, value []byte) error) error {
	snap := bs.kv.NewSnapshot()
	defer snap.Discard()
	bounds := bs.metaRange()
	it := snap.NewIterator(bounds)
	defer it.Close()
	for it.Seek(nil); it.Valid(); it.Next() {
		blobID := string(it.Key()[len(bounds.Lower):])
		encoded, err := it.Value()
		if err != nil {
			return fmt.Errorf("reading the metadata of blob %s: %w", blobID, err)
		}
		doc, err := blobDumpDocument(blobID, encoded)
		if err != nil {
			return err
		}
		if err := fn([]byte(blobID), doc); err != nil {
			return fmt.Errorf("iterating blob %s: %w", blobID, err)
		}
	}
	if err := it.Err(); err != nil {
		return fmt.Errorf("walking the blob metadata: %w", err)
	}
	return nil
}

// blobDumpDocument is the document a dump emits for one blob: its metadata
// under the names the per-bucket store's dump used, never its content.
func blobDumpDocument(blobID string, encoded []byte) ([]byte, error) {
	var meta blobMeta
	if err := bson.Unmarshal(encoded, &meta); err != nil {
		return nil, fmt.Errorf("decoding the metadata of blob %s: %w", blobID, err)
	}
	doc, err := bson.Marshal(blobEnumeratedEntry{
		ID:          blobID,
		Filename:    meta.Filename,
		ContentType: meta.ContentType,
		Size:        meta.Size,
		Checksum:    meta.SHA256,
		CreatedAt:   meta.CreatedAt,
		Extra:       meta.Extra,
	})
	if err != nil {
		return nil, fmt.Errorf("encoding the metadata of blob %s: %w", blobID, err)
	}
	return doc, nil
}

// metadataString returns metadata[field] when it is a string.
func metadataString(metadata map[string]interface{}, field string) string {
	value, _ := metadata[field].(string)
	return value
}
