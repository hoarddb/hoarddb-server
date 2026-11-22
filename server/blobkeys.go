package server

import (
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/sealed"
	"github.com/hoarddb/hoarddb-server/storage"
)

// The blob epoch registry, keys/blobs.wrapped, and the node record that says
// whether this node has ever had one (at-rest encryption spec §4.2.1 g).
//
// The WAL's registry (walkeys.go) treats a missing file as a first start and
// mints. That is wrong here: a blob registry that went missing beside existing
// payloads would be replaced by one whose epoch 1 is another key, after which
// every old blob fails its first chunk exactly as a damaged one does. So the
// registry is minted only when a record in the node store says none was ever
// created, and that record is written only once the registry is durable.

const (
	// nodeKeysDirName is the directory of the node's wrapped keys under the
	// data directory.
	nodeKeysDirName = "keys"
	// blobEpochRegistryFileName is the registry's name under <data>/keys. It is
	// not under the blob root: the orphan pass walks that tree by file name.
	blobEpochRegistryFileName = "blobs.wrapped"
	// blobEpochRegistryIdentity is the registry's sealing identity; sealed.Seal
	// appends "/" + gen. The WAL's is "wal", so one presented as the other does
	// not open.
	blobEpochRegistryIdentity = "blobs"
	// blobRegistryFormatVersion is the format version of both the registry and
	// its node record.
	blobRegistryFormatVersion = 1
)

// blobRegistryRename and blobRegistryDirFsyncer are the two steps that make a
// minted registry durable, as seams: a test fails the first to stand in for a
// crash before the rename, and watches the second to see that the node record
// is written only after it.
var (
	blobRegistryRename     = os.Rename
	blobRegistryDirFsyncer = syncDirectory
)

// blobEpochRegistry is the plaintext inside the registry's envelope: the shape
// of walEpochRegistry. It holds no per-bucket and no per-blob state.
type blobEpochRegistry struct {
	FormatVersion int                      `bson:"format_version"`
	Current       uint64                   `bson:"current"` // the epoch new payloads are sealed under
	Epochs        []blobEpochRegistryEntry `bson:"epochs"`
}

// blobEpochRegistryEntry is one epoch of a blobEpochRegistry.
type blobEpochRegistryEntry struct {
	ID        uint64 `bson:"id"`         // >= 1; the payload file header's epoch field
	Kid       string `bson:"kid"`        // sealed.Kid(Key), checked on load
	Key       []byte `bson:"key"`        // exactly 32 bytes
	CreatedAt int64  `bson:"created_at"` // unix seconds
}

// blobRegistryRecord is the node record's value. It deliberately holds no key
// and no kid: it says only that a registry was created, and when.
type blobRegistryRecord struct {
	FormatVersion int32     `bson:"format_version"`
	CreatedAt     time.Time `bson:"created_at"`
}

// blobRegistryRecordKey is the node record's key, [0xFFFF][0][0x0B].
func blobRegistryRecordKey() []byte {
	return storage.NodeKey(storage.NSBlobRegistry)
}

// blobKeySource implements sealed.BlobKeySource over an opened registry.
type blobKeySource struct {
	currentEpoch uint64
	epochKeys    map[uint64][]byte
}

// Format prints a key source as its epoch count and current epoch, whatever
// the verb. Without it %v, %s and a logger's default formatting print the
// struct, and so every epoch key it holds. fmt.Formatter has no error to
// return, and state is fmt's own buffer.
func (source *blobKeySource) Format(state fmt.State, _ rune) {
	_, _ = fmt.Fprintf(state, "blob key source (current epoch %d, %d held)", source.currentEpoch, len(source.epochKeys))
}

// CurrentEpoch is the epoch id new payload files are sealed under.
func (source *blobKeySource) CurrentEpoch() uint64 {
	return source.currentEpoch
}

// FileKey is HKDF(epoch key, 0x06, payloadName). An epoch the registry does
// not hold wraps sealed.ErrBlobKeyMissing, which the codec reports as a key
// problem and not as a damaged payload.
func (source *blobKeySource) FileKey(epoch uint64, payloadName string) ([]byte, error) {
	epochKey, held := source.epochKeys[epoch]
	if !held {
		return nil, fmt.Errorf("blob key epoch %d is not in keys/%s: %w", epoch, blobEpochRegistryFileName, sealed.ErrBlobKeyMissing)
	}
	fileKey, err := sealed.FileKey(epochKey, sealed.PurposeBlobChunk, payloadName)
	if err != nil {
		return nil, fmt.Errorf("deriving the file key of payload %s under blob key epoch %d: %w", payloadName, epoch, err)
	}
	return fileKey, nil
}

// newBlobKeySource indexes a registry by epoch id, checking each entry as it
// goes: a key of the wrong length or one whose kid does not match is a
// registry that must not be served from.
func newBlobKeySource(registry *blobEpochRegistry) (*blobKeySource, error) {
	if registry.FormatVersion != blobRegistryFormatVersion {
		return nil, fmt.Errorf("unsupported format version %d", registry.FormatVersion)
	}
	epochKeys := make(map[uint64][]byte, len(registry.Epochs))
	for _, entry := range registry.Epochs {
		if len(entry.Key) != 32 {
			return nil, fmt.Errorf("epoch %d has a key of %d bytes, want 32", entry.ID, len(entry.Key))
		}
		if computedKid := sealed.Kid(entry.Key); entry.Kid != computedKid {
			return nil, fmt.Errorf("epoch %d's key does not match its recorded kid %q", entry.ID, entry.Kid)
		}
		if _, duplicate := epochKeys[entry.ID]; duplicate {
			return nil, fmt.Errorf("epoch %d is listed twice", entry.ID)
		}
		epochKeys[entry.ID] = entry.Key
	}
	if _, held := epochKeys[registry.Current]; !held || registry.Current == 0 {
		return nil, fmt.Errorf("the current epoch %d names no epoch", registry.Current)
	}
	return &blobKeySource{currentEpoch: registry.Current, epochKeys: epochKeys}, nil
}

// openBlobEpochRegistry unseals a registry under the node master.
func openBlobEpochRegistry(nodeMasterKey, wrapped []byte) (*blobEpochRegistry, error) {
	plaintext, _, err := sealed.Open(sealed.PurposeEpochRegistry, nodeMasterKey, func(gen uint64) string {
		return fmt.Sprintf("%s/%d", blobEpochRegistryIdentity, gen)
	}, wrapped)
	if err != nil {
		return nil, err
	}
	registry := &blobEpochRegistry{}
	if err := bson.Unmarshal(plaintext, registry); err != nil {
		return nil, fmt.Errorf("decoding it: %w", err)
	}
	return registry, nil
}

// readBlobRegistryRecord reads the node record. Only storage.ErrKeyNotFound is
// "absent": any other read failure, and a record that does not decode, is an
// error, because treating either as absent is what would mint over a lost
// registry.
func readBlobRegistryRecord(kv storage.KV) (blobRegistryRecord, bool, error) {
	var record blobRegistryRecord
	snap := kv.NewSnapshot()
	defer snap.Discard()
	encoded, err := snap.Get(blobRegistryRecordKey())
	if errors.Is(err, storage.ErrKeyNotFound) {
		return record, false, nil
	}
	if err != nil {
		return record, false, fmt.Errorf("reading the node store's blob key registry record: %w", err)
	}
	if err := bson.Unmarshal(encoded, &record); err != nil {
		return record, false, fmt.Errorf("decoding the node store's blob key registry record: %w", err)
	}
	if record.FormatVersion != blobRegistryFormatVersion {
		return record, false, fmt.Errorf("the node store's blob key registry record has format version %d, and this server reads %d",
			record.FormatVersion, blobRegistryFormatVersion)
	}
	return record, true, nil
}

// writeBlobRegistryRecord records, durably, that the registry exists. The
// batch is synced: the record must be on disk before the key source is handed
// out, so that a registry lost later is never mistaken for one never made.
func writeBlobRegistryRecord(kv storage.KV, createdAt time.Time) error {
	encoded, err := bson.Marshal(blobRegistryRecord{FormatVersion: blobRegistryFormatVersion, CreatedAt: createdAt})
	if err != nil {
		return fmt.Errorf("encoding the node store's blob key registry record: %w", err)
	}
	batch := kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(blobRegistryRecordKey(), encoded); err != nil {
		return fmt.Errorf("writing the node store's blob key registry record: %w", err)
	}
	if err := batch.Commit(true); err != nil {
		return fmt.Errorf("committing the node store's blob key registry record: %w", err)
	}
	return nil
}

// blobRegistryMissingError is the refusal for a node whose store records a
// blob key registry that is no longer in the data directory, worded as spec
// §4.2.1 g gives it.
type blobRegistryMissingError struct {
	dataDir   string
	createdAt time.Time
}

func (e *blobRegistryMissingError) Error() string {
	return fmt.Sprintf("refusing to start: keys/%[1]s is missing from data directory %[2]q, but this node's store records that a "+
		"blob key registry was created here on %[3]s. Every sealed blob payload on this node can only be opened with the epoch "+
		"keys in that file. A new registry would hold different keys and would make every existing blob unreadable, so none was "+
		"created and nothing was written. Restore keys/%[1]s from a backup of this data directory: the epoch keys exist nowhere "+
		"else, and the node master escrow (-export-key) does not cover them. Without a backup the blob payload files cannot be "+
		"opened; to recover everything else, start a new data directory and restore a dump into it (a dump carries blob "+
		"metadata, not content).",
		blobEpochRegistryFileName, e.dataDir, e.createdAt.UTC().Format(time.RFC3339))
}

// blobRegistryUnusableError is the refusal for a registry file that is present
// and cannot be served from. A present file is never minted over.
type blobRegistryUnusableError struct {
	dataDir       string
	nodeMasterKid string
	cause         error
}

func (e *blobRegistryUnusableError) Error() string {
	return fmt.Sprintf("refusing to start: keys/%[1]s in data directory %[2]q cannot be used under this node's master key (kid %[3]s): "+
		"%[4]v. It may belong to another node, be a copy of the WAL's registry (wal/%[5]s), or be damaged. It was left as it is and "+
		"no new registry was created: a new one would hold different keys and would make every existing blob unreadable. Restore "+
		"keys/%[1]s from a backup of this data directory taken together with keys/node-master.key.wrapped.",
		blobEpochRegistryFileName, e.dataDir, e.nodeMasterKid, e.cause, WALEpochRegistryFileName)
}

func (e *blobRegistryUnusableError) Unwrap() error { return e.cause }

// LoadOrCreateBlobKeySource returns the node's blob key source, or nil on a
// plaintext node (nm is nil), where it touches nothing. It is called once at
// start, after the node store is open and its bucket id allocator seeded, and
// before any blob put can run.
//
// The registry file present: it is opened under the node master — a file that
// does not open is refused by name and never replaced — and the node record is
// written if a crash left it unwritten. The file absent: the node record
// decides. Recorded, the start is refused, because the registry was lost and a
// new one could open no existing payload. Not recorded, this node has never
// had a registry (a fresh node, or one sealed before blobs were), and one is
// minted: made durable first, recorded second, returned third.
//
// The blob root is never read. cfg carries storage.blob_dir, and this function
// must not consult it: an unmounted volume looks exactly like a node with no
// blobs, which is the case the node record exists to tell apart.
func LoadOrCreateBlobKeySource(cfg *config.Config, nm *NodeMaster, kv storage.KV, logger *slog.Logger) (sealed.BlobKeySource, error) {
	if nm == nil {
		return nil, nil
	}
	dataDir := cfg.Storage.DataDir
	registryPath := filepath.Join(dataDir, nodeKeysDirName, blobEpochRegistryFileName)
	wrapped, readErr := os.ReadFile(registryPath)
	if readErr == nil {
		return loadBlobKeySource(dataDir, nm, kv, wrapped, logger)
	}
	if !errors.Is(readErr, os.ErrNotExist) {
		return nil, fmt.Errorf("reading the blob key registry %s: %w", registryPath, readErr)
	}
	record, recorded, err := readBlobRegistryRecord(kv)
	if err != nil {
		return nil, fmt.Errorf("%s is absent, and whether this node ever had it could not be determined, so none was created: %w", registryPath, err)
	}
	if recorded {
		return nil, &blobRegistryMissingError{dataDir: dataDir, createdAt: record.CreatedAt}
	}
	return mintBlobKeySource(registryPath, nm, kv, logger)
}

// loadBlobKeySource serves from a registry file that is present, and makes
// sure the node record says so.
func loadBlobKeySource(dataDir string, nm *NodeMaster, kv storage.KV, wrapped []byte, logger *slog.Logger) (sealed.BlobKeySource, error) {
	registry, err := openBlobEpochRegistry(nm.Bytes(), wrapped)
	if err != nil {
		return nil, &blobRegistryUnusableError{dataDir: dataDir, nodeMasterKid: nm.Kid(), cause: err}
	}
	source, err := newBlobKeySource(registry)
	if err != nil {
		return nil, &blobRegistryUnusableError{dataDir: dataDir, nodeMasterKid: nm.Kid(), cause: err}
	}
	_, recorded, err := readBlobRegistryRecord(kv)
	if err != nil {
		return nil, err
	}
	if !recorded {
		// A crash between the registry's rename and the record's commit, or a
		// failed commit: the registry is the one this node minted, so record it.
		createdAt := time.Unix(registry.Epochs[0].CreatedAt, 0).UTC()
		for _, entry := range registry.Epochs {
			if entryCreatedAt := time.Unix(entry.CreatedAt, 0).UTC(); entryCreatedAt.Before(createdAt) {
				createdAt = entryCreatedAt
			}
		}
		if err := writeBlobRegistryRecord(kv, createdAt); err != nil {
			return nil, err
		}
		logger.Info("recorded the existing blob key registry in the node store",
			"file", filepath.Join(nodeKeysDirName, blobEpochRegistryFileName), "created_at", createdAt)
	}
	return source, nil
}

// mintBlobKeySource creates the registry with one epoch. The order is the
// crash safety: the wrapped registry is written to a temporary, synced,
// renamed into place and its directory synced (createNodeStoreKey's order);
// only then is the node record committed, synced; only then is the source
// returned. A crash before the rename leaves nothing and the next start mints
// again; one after it leaves a registry the next start loads and records. No
// payload can be sealed under a registry whose existence is not recorded.
func mintBlobKeySource(registryPath string, nm *NodeMaster, kv storage.KV, logger *slog.Logger) (sealed.BlobKeySource, error) {
	epochKey := make([]byte, 32)
	if _, err := rand.Read(epochKey); err != nil {
		return nil, fmt.Errorf("generating the blob epoch key: %w", err)
	}
	createdAt := time.Now().UTC().Truncate(time.Second)
	registry := &blobEpochRegistry{
		FormatVersion: blobRegistryFormatVersion,
		Current:       1,
		Epochs:        []blobEpochRegistryEntry{{ID: 1, Kid: sealed.Kid(epochKey), Key: epochKey, CreatedAt: createdAt.Unix()}},
	}
	encoded, err := bson.Marshal(registry)
	if err != nil {
		return nil, fmt.Errorf("encoding the blob key registry: %w", err)
	}
	wrapped, err := sealed.Seal(sealed.PurposeEpochRegistry, nm.Bytes(), blobEpochRegistryIdentity, 1, encoded)
	if err != nil {
		return nil, fmt.Errorf("sealing the blob key registry: %w", err)
	}

	keysDir := filepath.Dir(registryPath)
	if err := os.MkdirAll(keysDir, 0o700); err != nil {
		return nil, fmt.Errorf("creating %s: %w", keysDir, err)
	}
	tempPath := registryPath + ".tmp"
	if err := writeFileSynced(tempPath, wrapped); err != nil {
		return nil, errors.Join(err, removeRegistryTemporary(tempPath))
	}
	if err := blobRegistryRename(tempPath, registryPath); err != nil {
		return nil, errors.Join(fmt.Errorf("renaming %s into place: %w", tempPath, err), removeRegistryTemporary(tempPath))
	}
	if err := blobRegistryDirFsyncer(keysDir); err != nil {
		return nil, fmt.Errorf("syncing %s after writing the blob key registry: %w", keysDir, err)
	}
	if err := writeBlobRegistryRecord(kv, createdAt); err != nil {
		return nil, err
	}
	logger.Info("created the blob key registry", "file", filepath.Join(nodeKeysDirName, blobEpochRegistryFileName), "epoch", registry.Current)
	return &blobKeySource{currentEpoch: registry.Current, epochKeys: map[uint64][]byte{registry.Current: epochKey}}, nil
}

// removeRegistryTemporary removes a registry temporary that was not renamed
// into place. It holds a wrapped key nothing will ever read.
func removeRegistryTemporary(tempPath string) error {
	if err := os.Remove(tempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("removing %s: %w", tempPath, err)
	}
	return nil
}
