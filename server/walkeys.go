package server

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/sealed"
	"github.com/hoarddb/hoarddb-server/storage"
)

const WALEpochRegistryFileName = "keys.wrapped"

// legacyWALRegistryIdentity is the sealing identity of the legacy log's epoch registry;
// the ring log's registry uses ringWALKeyIdentity, so neither file opens as the other.
const legacyWALRegistryIdentity = "wal"

// walDirFsyncer fsyncs a directory, which makes a rename into it durable.
var walDirFsyncer = func(dirPath string) error {
	dirFile, err := os.Open(dirPath)
	if err != nil {
		return err
	}
	defer func() { _ = dirFile.Close() }()
	return dirFile.Sync()
}

// walEpochRegistry is the plaintext structure inside a 0x08 envelope for WAL epoch keys.
type walEpochRegistry struct {
	FormatVersion int                     `bson:"format_version"` // always 1
	Current       uint64                  `bson:"current"`        // the epoch new files are sealed under
	Epochs        []walEpochRegistryEntry `bson:"epochs"`
}

// walEpochRegistryEntry is one epoch within a walEpochRegistry.
type walEpochRegistryEntry struct {
	ID        uint64 `bson:"id"`         // >= 1; indexes the sealed-file header's epoch field
	Kid       string `bson:"kid"`        // hex(sha256(Key))[:8] — for integrity checking
	Key       []byte `bson:"key"`        // exactly 32 bytes
	CreatedAt int64  `bson:"created_at"` // unix seconds
}

// walKeySource implements storage.WALKeySource over an epoch registry file.
type walKeySource struct {
	registry *walEpochRegistry
	// idToEntry maps epoch ID to walEpochRegistryEntry for fast lookup
	idToEntry map[uint64]*walEpochRegistryEntry
}

// CurrentEpoch returns the epoch id that new WAL segments are sealed under.
func (w *walKeySource) CurrentEpoch() uint64 {
	return w.registry.Current
}

// FileKey returns the 32-byte key for the given segment index under the given epoch.
func (w *walKeySource) FileKey(epoch uint64, segIdx int) ([]byte, error) {
	entry, exists := w.idToEntry[epoch]
	if !exists {
		return nil, fmt.Errorf("WAL epoch %d not found in registry", epoch)
	}

	// Derive the file key using the epoch key as parent.
	// The identity is "wal/{segmentIndex}" where segmentIndex is decimal, unpadded.
	identity := "wal/" + strconv.Itoa(segIdx)
	key, err := sealed.FileKey(entry.Key, sealed.PurposeWALEntry, identity)
	if err != nil {
		return nil, fmt.Errorf("deriving WAL file key for segment %d: %w", segIdx, err)
	}

	return key, nil
}

// sealWALEpochRegistry marshals and seals an epoch registry under the node master, with the
// sealing identity of the log it belongs to.
func sealWALEpochRegistry(nodeMasterKey []byte, registry *walEpochRegistry, identity string) ([]byte, error) {
	// Marshal to BSON
	registryBytes, err := bson.Marshal(registry)
	if err != nil {
		return nil, fmt.Errorf("marshaling WAL epoch registry: %w", err)
	}

	// Seal under the node master with purpose 0x08.
	// sealed.Seal adds the gen automatically, so identity carries no generation.
	sealed, err := sealed.Seal(sealed.PurposeEpochRegistry, nodeMasterKey, identity, 1, registryBytes)
	if err != nil {
		return nil, fmt.Errorf("sealing WAL epoch registry: %w", err)
	}

	return sealed, nil
}

// openWALEpochRegistry unseals and unmarshals an epoch registry sealed under identity,
// building lookup maps. It verifies that each stored Kid matches the computed Kid of the
// stored Key.
func openWALEpochRegistry(nodeMasterKey []byte, blob []byte, identity string) (*walEpochRegistry, map[uint64]*walEpochRegistryEntry, error) {
	// Unseal. The identity callback receives the gen from the envelope header
	// and returns the full identity string. sealed.Open will use FileKey to derive
	// the decryption key from parent, purpose, and identity+gen.
	plaintext, _, err := sealed.Open(sealed.PurposeEpochRegistry, nodeMasterKey, func(gen uint64) string {
		return fmt.Sprintf("%s/%d", identity, gen)
	}, blob)
	if err != nil {
		return nil, nil, fmt.Errorf("opening WAL epoch registry: %w", err)
	}

	// Unmarshal BSON
	registry := &walEpochRegistry{}
	if err := bson.Unmarshal(plaintext, registry); err != nil {
		return nil, nil, fmt.Errorf("unmarshaling WAL epoch registry: %w", err)
	}

	// Verify registry format
	if registry.FormatVersion != 1 {
		return nil, nil, fmt.Errorf("WAL epoch registry: unsupported format version %d", registry.FormatVersion)
	}

	// Build lookup maps and verify kids
	idToEntry := make(map[uint64]*walEpochRegistryEntry)

	for i := range registry.Epochs {
		entry := &registry.Epochs[i]

		// Verify the key is exactly 32 bytes
		if len(entry.Key) != 32 {
			return nil, nil, fmt.Errorf("WAL epoch registry: epoch %d has key of %d bytes, want 32", entry.ID, len(entry.Key))
		}

		// Verify the stored Kid matches the computed Kid
		computedKid := sealed.Kid(entry.Key)
		if entry.Kid != computedKid {
			return nil, nil, fmt.Errorf("WAL epoch registry: epoch %d kid mismatch (stored %q, computed %q)", entry.ID, entry.Kid, computedKid)
		}

		// Add to maps
		idToEntry[entry.ID] = entry
	}

	// Verify that Current names an entry
	if registry.Current == 0 || idToEntry[registry.Current] == nil {
		return nil, nil, fmt.Errorf("WAL epoch registry: Current %d does not name an epoch", registry.Current)
	}

	return registry, idToEntry, nil
}

// LoadOrCreateWALKeySource loads or creates a WAL key source from the epoch registry.
// If the wrapped registry exists, it is unsealed and loaded. If it does not exist,
// a new epoch registry is created with a single epoch.
//
// This function is called once at startup (NewService), before the WAL is opened.
func LoadOrCreateWALKeySource(dataDir string, nm *NodeMaster) (storage.WALKeySource, error) {
	if nm == nil {
		return nil, nil
	}

	walDir := filepath.Join(dataDir, "wal")
	registryPath := filepath.Join(walDir, WALEpochRegistryFileName)

	// Check if the wrapped registry file exists
	registryData, err := os.ReadFile(registryPath)
	if err == nil {
		// Registry exists: unseal and load it
		registry, idToEntry, err := openWALEpochRegistry(nm.Bytes(), registryData, legacyWALRegistryIdentity)
		if err != nil {
			return nil, fmt.Errorf("loading WAL epoch registry: %w", err)
		}

		return &walKeySource{
			registry:  registry,
			idToEntry: idToEntry,
		}, nil
	}

	if !os.IsNotExist(err) {
		// Some other error reading the registry file
		return nil, fmt.Errorf("reading WAL epoch registry: %w", err)
	}

	// The segments' keys live only in this file, so an absent registry beside
	// sealed segments is a loss to report, not a first start.
	sealedSegments, err := findSealedWALSegments(walDir, walDirName)
	if err != nil {
		return nil, fmt.Errorf("deciding whether the missing WAL epoch registry is a first start: %w", err)
	}
	if len(sealedSegments) > 0 {
		return nil, missingWALRegistryError(dataDir, sealedSegments)
	}

	registry, wrappedRegistry, err := newSealedWALEpochRegistry(nm.Bytes(), legacyWALRegistryIdentity)
	if err != nil {
		return nil, err
	}

	// Ensure the WAL directory exists
	if err := os.MkdirAll(walDir, 0o700); err != nil {
		return nil, fmt.Errorf("creating WAL directory: %w", err)
	}

	// Write to temp file, fsync, rename (matching the N6 ordering pattern)
	tempPath := registryPath + ".tmp"
	if err := os.WriteFile(tempPath, wrappedRegistry, 0o600); err != nil {
		return nil, fmt.Errorf("writing wrapped WAL epoch registry: %w", err)
	}

	// Fsync the temp file
	f, err := os.Open(tempPath)
	if err != nil {
		_ = os.Remove(tempPath)
		return nil, fmt.Errorf("opening temp WAL epoch registry: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tempPath)
		return nil, fmt.Errorf("syncing temp WAL epoch registry: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tempPath)
		return nil, fmt.Errorf("closing temp WAL epoch registry: %w", err)
	}

	// Rename onto the final path
	if err := os.Rename(tempPath, registryPath); err != nil {
		_ = os.Remove(tempPath)
		return nil, fmt.Errorf("renaming WAL epoch registry: %w", err)
	}

	// Fsync the WAL directory (via seam for testability)
	if err := walDirFsyncer(walDir); err != nil {
		return nil, fmt.Errorf("syncing WAL directory: %w", err)
	}

	return newWALKeySource(registry), nil
}

// newSealedWALEpochRegistry makes a registry of one new epoch and its wrapped form, sealed
// under the node master with identity. The registry is never rewritten: a node's log keeps
// its first epoch for its life.
func newSealedWALEpochRegistry(nodeMasterKey []byte, identity string) (*walEpochRegistry, []byte, error) {
	epochKey := make([]byte, 32)
	if _, err := rand.Read(epochKey); err != nil {
		return nil, nil, fmt.Errorf("generating WAL epoch key: %w", err)
	}
	registry := &walEpochRegistry{
		FormatVersion: 1,
		Current:       1,
		Epochs: []walEpochRegistryEntry{
			{
				ID:        1,
				Kid:       sealed.Kid(epochKey),
				Key:       epochKey,
				CreatedAt: time.Now().Unix(),
			},
		},
	}
	wrappedRegistry, err := sealWALEpochRegistry(nodeMasterKey, registry, identity)
	if err != nil {
		return nil, nil, fmt.Errorf("sealing WAL epoch registry: %w", err)
	}
	return registry, wrappedRegistry, nil
}

// newWALKeySource is the key source over a registry, with its epochs indexed by id.
func newWALKeySource(registry *walEpochRegistry) *walKeySource {
	idToEntry := make(map[uint64]*walEpochRegistryEntry, len(registry.Epochs))
	for i := range registry.Epochs {
		entry := &registry.Epochs[i]
		idToEntry[entry.ID] = entry
	}
	return &walKeySource{registry: registry, idToEntry: idToEntry}
}
