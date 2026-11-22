package server

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/sealed"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// The node store: one ordered key-value store per node, holding the catalog,
// the bucket id allocator and (as each engine moves into it) every bucket's
// keys. Shared-storage spec §5.1, §5.2.2 and §3.1.1; the directory and the
// data key are the build doc's SA-3, answered by Hermes as recommended.

const (
	// nodeStoreDirName is the store's directory under the data directory.
	nodeStoreDirName = "store"
	// blobRootDirName is the node-wide tree of blob payload files under the
	// data directory (spec §3.5) when storage.blob_dir is unset: the one
	// thing the node writes outside its store.
	blobRootDirName = "blobs"
	// nodeStoreKeyFileName is the store's wrapped data key, under <data>/keys.
	nodeStoreKeyFileName = "store.wrapped"
	// nodeStoreKeyIdentity is the sealing identity of the store's data key,
	// where the per-bucket layout sealed each bucket key under its database +
	// "/" + bucket.
	nodeStoreKeyIdentity = "store"
)

// slotOverrideHook, when set and returning true, supplies the slot a create
// records in place of storage.SlotOf. Tests use it to give a bucket a slot its
// name does not hash to. Nil in production.
var slotOverrideHook atomic.Pointer[func(database, bucket string) (uint16, bool)]

// allocatorCommitHook, when set, runs inside the allocator's critical section
// immediately before the create batch commits, with the id being committed.
// Tests use it to delay commits. Nil in production.
var allocatorCommitHook atomic.Pointer[func(id uint64)]

// bucketIDAllocator hands out bucket ids. The mutex is held from taking the
// next id until the batch that records it has committed, so the persisted
// high-water mark is written in id order and never goes backwards (spec
// §5.2.2, T-27(d)). The one lock taken while it is held is Cluster.migrateMu,
// which a database drop and a materialise's catalog commit take under it to
// order the drop's pull cancel against the commit; migrateMu guards
// only map bookkeeping and takes nothing, so the order cannot cycle.
type bucketIDAllocator struct {
	mu sync.Mutex
	// last is the highest id handed out, seeded from the store's allocator
	// record when the store opens.
	last uint64
}

// bucketIDsExhaustedError is the ceiling's refusal, worded as spec §3.1 gives it.
type bucketIDsExhaustedError struct {
	bucket string
	last   uint64
}

func (e *bucketIDsExhaustedError) Error() string {
	return fmt.Sprintf("Bucket %q cannot be created: this node has allocated every bucket id it can "+
		"(the last was %d). Bucket ids are never reused, so this node's catalog is full; restore its data "+
		"into a fresh data directory with 'hoarddb dump' and 'restore'.", e.bucket, e.last)
}

func (e *bucketIDsExhaustedError) Unwrap() error { return storage.ErrBucketIDsExhausted }

// catalogKey is a bucket's catalog key, [0xFFFF][0][0x01][database "/" bucket].
// A database's own marker is the same key with an empty bucket name, which no
// bucket can have.
func catalogKey(database, bucket string) []byte {
	return storage.NodeKey(storage.NSCatalog, []byte(database+"/"+bucket))
}

// catalogPrefix bounds every catalog key.
func catalogPrefix() (lower, upper []byte) {
	return storage.NodeKey(storage.NSCatalog),
		storage.NodeKey(storage.NSCatalog + 1)
}

// allocatorKey is the allocator's high-water mark, [0xFFFF][0][0x09].
func allocatorKey() []byte {
	return storage.NodeKey(storage.NSAllocator)
}

// reclaimKey is a dropped bucket's reclaim record, [0xFFFF][0][0x08][id : 8].
// It is a node record, so it lies in no slot's range.
func reclaimKey(id uint64) []byte {
	return storage.NodeKey(storage.NSReclaim, encodeBucketID(id))
}

// reclaimRecord is the value of a reclaim record (spec §5.2.2). It carries the
// bucket's recorded slot because the reclaimer walks [slot][id] … [slot][id+1]
// and must not recompute the slot from the name (build doc §1 item 6). The
// cursor and the two counters are the reclaimer's (Step 15); a drop writes
// them empty.
type reclaimRecord struct {
	ID       int64      `bson:"id"`
	Slot     int32      `bson:"slot"`
	Type     BucketType `bson:"type"`
	Database string     `bson:"database"`
	Name     string     `bson:"name"`
	// RangeDatabase is the database whose key range this record reclaims when
	// Database is empty: a failed materialise's record leaves Database and Name
	// empty on purpose (it is not a drop) but its keys sit under the database
	// being materialised.
	RangeDatabase string `bson:"range_database,omitempty"`
	DroppedSeq    int64  `bson:"dropped_seq"`
	// DroppedAt is when the drop committed. The reclaimer's oldest-pending
	// gauge and its day-old warning read it; the spec's record lists no time,
	// and without one the age is unknowable without O(pending) memory.
	DroppedAt     time.Time `bson:"dropped_at"`
	Cursor        []byte    `bson:"cursor"`
	KeysDeleted   int64     `bson:"keys_deleted"`
	FilesUnlinked int64     `bson:"files_unlinked"`
}

// rangeDatabaseID is the database id the record's key range sits under.
func (r reclaimRecord) rangeDatabaseID() uint64 {
	if r.Database != "" {
		return storage.LegacyDatabaseID(r.Database)
	}
	return storage.LegacyDatabaseID(r.RangeDatabase)
}

// databaseDropKey is a dropped database's record,
// [0xFFFF][0][0x08][DatabaseDropID][database]. DatabaseDropID is above every
// id the allocator can hand out, so these records sort after every bucket
// reclaim record and after the orphan cursor.
func databaseDropKey(database string) []byte {
	return storage.NodeKey(storage.NSReclaim, encodeBucketID(storage.DatabaseDropID), []byte(database))
}

// databaseDropRange bounds every database-drop record, half-open.
func databaseDropRange() (lower, upper []byte) {
	return reclaimKey(storage.DatabaseDropID), storage.NodeKey(storage.NSReclaim + 1)
}

// databaseDropRecord is the value of a database-drop record (spec §5.2.2):
// one record for the whole database, which the reclaimer expands into bucket
// reclaim records in the background. MaxID is the allocator's high-water mark
// at the drop: every bucket of the dropped database has an id at or below it,
// and every bucket created afterwards — in a re-created database of the same
// name too — has one above it, which is what tells the two apart without a WAL
// sequence. Cursor is the expander's position in the database's catalog
// entries.
type databaseDropRecord struct {
	Database   string    `bson:"database"`
	DroppedSeq int64     `bson:"dropped_seq"`
	MaxID      int64     `bson:"max_id"`
	Cursor     []byte    `bson:"cursor"`
	DroppedAt  time.Time `bson:"dropped_at"`
	// Generations are the incarnations the drop removed: a catalog entry of
	// the database is the drop's when its generation is one of them. A second
	// drop of the name before the expander finishes merges its generations in.
	// Absent on a record a drop logged before generations existed left, which
	// MaxID alone decides.
	Generations []int64 `bson:"generations,omitempty"`
}

// covers reports whether a catalog entry of the database is this drop's to
// reclaim: an entry of a generation it names, or — for a record a drop logged
// before generations existed left — an entry at or below its max_id.
func (r databaseDropRecord) covers(entry catalogEntry) bool {
	if r.MaxID > 0 && entry.ID <= r.MaxID {
		return true
	}
	return slices.Contains(r.Generations, entryGeneration(entry))
}

// readDatabaseDrops reads every pending database-drop record — O(pending
// database drops), normally none. Catalog load reads them before it scans the
// catalog (spec §5.2.2, N-04).
func (s *Server) readDatabaseDrops() (map[string]databaseDropRecord, error) {
	lower, upper := databaseDropRange()
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: lower, Upper: upper})
	defer it.Close()
	drops := make(map[string]databaseDropRecord)
	for it.Seek(nil); it.Valid(); it.Next() {
		encoded, err := it.Value()
		if err != nil {
			return nil, fmt.Errorf("reading the database-drop record at %x: %w", it.Key(), err)
		}
		var record databaseDropRecord
		if err := bson.Unmarshal(encoded, &record); err != nil {
			return nil, fmt.Errorf("decoding the database-drop record at %x: %w", it.Key(), err)
		}
		drops[record.Database] = record
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("walking the database-drop records: %w", err)
	}
	return drops, nil
}

// pendingDatabaseDrop reads database's drop record, if its expansion is
// still pending.
func (s *Server) pendingDatabaseDrop(database string) (databaseDropRecord, bool, error) {
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	encoded, err := snap.Get(databaseDropKey(database))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return databaseDropRecord{}, false, nil
	}
	if err != nil {
		return databaseDropRecord{}, false, fmt.Errorf("reading the drop record of database %q: %w", database, err)
	}
	var record databaseDropRecord
	if err := bson.Unmarshal(encoded, &record); err != nil {
		return databaseDropRecord{}, false, fmt.Errorf("decoding the drop record of database %q: %w", database, err)
	}
	return record, true, nil
}

// commitDatabaseDrop is a database drop's one synced batch: the drop record,
// whose max_id is the allocator's high-water mark, and the database's marker
// deleted. The allocator's mutex is held from reading the mark to the commit,
// as a create holds it, so no bucket of the database can be created with an
// id at or below the mark after the record says the database is gone. A
// database dropped again before its first drop was expanded keeps the first
// drop's time, which is what the oldest-pending age measures. A drop logged
// before generations names every generation, so it deletes every awaiting and
// needs-a-copy record of the database in its batch (spec §3.3).
func (s *Server) commitDatabaseDrop(database string, droppedSeq uint64) error {
	earlier, pending, err := s.pendingDatabaseDrop(database)
	if err != nil {
		return err
	}
	s.bucketIDs.mu.Lock()
	defer s.bucketIDs.mu.Unlock()
	record := databaseDropRecord{
		Database:   database,
		DroppedSeq: int64(droppedSeq),
		MaxID:      int64(s.bucketIDs.last),
		DroppedAt:  time.Now().UTC(),
	}
	if pending {
		record.DroppedAt = earlier.DroppedAt
		record.Generations = earlier.Generations
	}
	encoded, err := bson.Marshal(record)
	if err != nil {
		return fmt.Errorf("encoding the drop record of database %q: %w", database, err)
	}
	batch := s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(databaseDropKey(database), encoded); err != nil {
		return fmt.Errorf("writing the drop record of database %q: %w", database, err)
	}
	if err := batch.Delete(catalogKey(database, "")); err != nil {
		return fmt.Errorf("deleting the catalog entry of database %q: %w", database, err)
	}
	scan := s.kv.NewSnapshot()
	defer scan.Discard()
	if err := deleteNodeRecords(scan, batch, storage.AwaitingRecordKind, databaseRecordScope(database), nil); err != nil {
		return fmt.Errorf("deleting the awaiting records of database %q: %w", database, err)
	}
	if err := deleteNodeRecords(scan, batch, storage.NeedsCopyRecordKind, databaseRecordScope(database), nil); err != nil {
		return fmt.Errorf("deleting the needs-a-copy records of database %q: %w", database, err)
	}
	if err := batch.Commit(true); err != nil {
		return fmt.Errorf("committing the drop of database %q: %w", database, err)
	}
	// Still under the allocator's mutex: a materialise re-checks its
	// pull under the same mutex before its catalog commit, so it either
	// committed before this drop, under max_id, or sees the cancel.
	if s.cluster != nil {
		s.cluster.dropDatabaseClearsPulls(database, nil)
		s.cluster.forgetAwaitingDatabase(database, nil)
		s.cluster.forgetPendingCopiesOf(database, nil)
	}
	return nil
}

func encodeBucketID(id uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, id)
}

// openNodeStore opens the node's store at <data dir>/store, sealed under the
// node's data key when encryption.mode is on or require, at the memtable size
// resolveMemtable decides. The memtable choice is written only after the start
// has passed every refusal that needs no store. A legacy-layout directory is
// refused earlier still, by NewService (RefuseLegacyLayout).
func openNodeStore(cfg *config.Config, nm *NodeMaster) (storage.KV, memtableResolution, error) {
	dataDir := cfg.Storage.DataDir
	dataKey, err := nodeStoreDataKey(cfg, nm)
	if err != nil {
		return nil, memtableResolution{}, err
	}
	memtable, err := resolveMemtable(memtableInputsFor(cfg))
	if err != nil {
		return nil, memtableResolution{}, err
	}
	tuning := storage.BadgerTuning{
		BlockCacheMB:  cfg.Storage.BadgerBlockCacheMB,
		IndexCacheMB:  cfg.Storage.BadgerIndexCacheMB,
		NumCompactors: cfg.Storage.BadgerNumCompactors,
		TableSync:     cfg.Storage.BadgerTableSync,
		// GAP-38: a joined node will take the cluster's agreed value here.
		MemTableMB:                memtable.appliedMB,
		MemTableRequestedBy:       memtable.explicitBy,
		MemTableRequestedByChoice: memtable.explicitBy == "",
	}
	kv, err := storage.OpenBadgerKV(filepath.Join(dataDir, nodeStoreDirName), tuning, dataKey)
	if err != nil {
		return nil, memtableResolution{}, err
	}
	return kv, memtable, nil
}

// blobRoot is the node's blob payload tree: storage.blob_dir when set, and
// <data dir>/blobs otherwise. It is the one place the path is built — every
// write, read, migration and reclaim goes through it — so the setting cannot
// be honoured by some paths and not others. A blob store creates the leaf a
// payload needs when it writes one.
func (s *Server) blobRoot() string {
	if s.config.Storage.BlobDir != "" {
		return filepath.Clean(s.config.Storage.BlobDir)
	}
	return filepath.Join(s.config.Storage.DataDir, blobRootDirName)
}

// RefuseLegacyLayout is spec §7's trigger, both shapes. NewService runs it
// first, before it creates any key, token or store in the data directory.
func RefuseLegacyLayout(cfg *config.Config) error {
	if err := refuseLegacyCatalog(cfg.Storage.DataDir); err != nil {
		return err
	}
	return refuseLegacyBucketDirs(cfg)
}

// refuseLegacyCatalog refuses a data directory that holds catalog.json: it was
// written by the per-bucket layout, which this server does not convert in
// place (spec §7).
func refuseLegacyCatalog(dataDir string) error {
	raw, err := os.ReadFile(filepath.Join(dataDir, legacyCatalogFileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading %s to check the storage layout: %w", filepath.Join(dataDir, legacyCatalogFileName), err)
	}
	// Only version 1 was ever written; a file that does not parse is still the
	// old layout's file, and is named as that version.
	legacy := struct {
		Version int `json:"version"`
	}{Version: 1}
	if json.Unmarshal(raw, &legacy) != nil {
		legacy.Version = 1
	}
	return &legacyLayoutError{dataDir: dataDir, version: legacy.Version}
}

// legacyBucketMarkers are the entries a per-bucket layout's bucket directory
// held: its Badger instance, its segment files, its blob files, its index
// instances and its wrapped key.
var legacyBucketMarkers = []string{"badger", "segments", "blobs", "indexes", "key.wrapped"}

// refuseLegacyBucketDirs is the second shape of spec §7's trigger: a data
// directory with no catalog.json that still holds <database>/<bucket>/
// directories of the per-bucket layout. Without it the server would start
// beside them and log a warning whose remedy no longer re-attaches anything.
// It looks two levels down and no further, and skips the directories the node
// owns, so on a shared-layout directory it reads a handful of entries.
func refuseLegacyBucketDirs(cfg *config.Config) error {
	dataDir := cfg.Storage.DataDir
	skipped := map[string]bool{blobRootDirName: true}
	for _, name := range nodeOwnedDirNames {
		skipped[name] = true
	}
	// A configured blob directory can sit inside the data directory under any
	// name; it is the node's, not a database's.
	if cfg.Storage.BlobDir != "" && filepath.Dir(filepath.Clean(cfg.Storage.BlobDir)) == filepath.Clean(dataDir) {
		skipped[filepath.Base(filepath.Clean(cfg.Storage.BlobDir))] = true
	}

	databases, err := os.ReadDir(dataDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading %s to check the storage layout: %w", dataDir, err)
	}
	for _, database := range databases {
		if !database.IsDir() || skipped[database.Name()] {
			continue
		}
		buckets, err := os.ReadDir(filepath.Join(dataDir, database.Name()))
		if err != nil {
			return fmt.Errorf("reading %s to check the storage layout: %w", filepath.Join(dataDir, database.Name()), err)
		}
		for _, bucket := range buckets {
			if !bucket.IsDir() {
				continue
			}
			for _, marker := range legacyBucketMarkers {
				_, err := os.Lstat(filepath.Join(dataDir, database.Name(), bucket.Name(), marker))
				if err == nil {
					// Only version 1 was ever written.
					return &legacyLayoutError{dataDir: dataDir, version: 1}
				}
				if !errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("checking %s for the per-bucket storage layout: %w", filepath.Join(dataDir, database.Name(), bucket.Name()), err)
				}
			}
		}
	}
	return nil
}

// legacyLayoutError is the refusal for a data directory in the per-bucket
// layout (spec §7's trigger): a sentence an operator reads, capitalised and
// punctuated as one. It names no other build: none was released, so the fix
// is an empty data directory.
type legacyLayoutError struct {
	dataDir string
	version int
}

func (e *legacyLayoutError) Error() string {
	return fmt.Sprintf("Data directory %s uses the per-bucket storage layout (catalog version %d), "+
		"and this release does not read that layout: it reads the shared layout (version %d). "+
		"Nothing in the directory was changed. "+
		"Start hoarddb-server with an empty data directory: set storage.data_dir (HOARDDB_DATA_DIR) to a new path, "+
		"or move this directory aside and keep it if you want it.",
		e.dataDir, e.version, catalogFormatVersion)
}

// refuseSealedStoreWithEncryptionOff is nodeStoreDataKey's first refusal on
// its own, with no side effect: a node whose store is sealed, started with
// encryption off. NewService asks it before anything reads a sealed file,
// because users.json is sealed under the same node master, and reading it
// first answered this start with "node master is nil" — which names neither
// the store nor the setting to change.
func refuseSealedStoreWithEncryptionOff(cfg *config.Config) error {
	mode := cfg.Encryption.Mode
	if mode == "on" || mode == "require" {
		return nil
	}
	keyPath := filepath.Join(cfg.Storage.DataDir, "keys", nodeStoreKeyFileName)
	if _, err := os.Stat(keyPath); err != nil {
		return nil
	}
	return storeModeMismatch(filepath.Join(cfg.Storage.DataDir, nodeStoreDirName), "encrypted", mode, "on", cfg.Cluster.Enabled)
}

// nodeStoreDataKey returns the store's data key, or nil for a plaintext store.
// The store's sealing is fixed when it is created: a start whose mode
// disagrees with it is refused rather than served.
func nodeStoreDataKey(cfg *config.Config, nm *NodeMaster) ([]byte, error) {
	dataDir := cfg.Storage.DataDir
	storeDir := filepath.Join(dataDir, nodeStoreDirName)
	keyPath := filepath.Join(dataDir, "keys", nodeStoreKeyFileName)
	wrapped, readErr := os.ReadFile(keyPath)
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		return nil, fmt.Errorf("reading the node store's key %s: %w", keyPath, readErr)
	}
	sealedStore := readErr == nil
	_, manifestErr := os.Stat(filepath.Join(storeDir, "MANIFEST"))
	existingStore := manifestErr == nil

	mode := cfg.Encryption.Mode
	wantSealed := mode == "on" || mode == "require"
	switch {
	case !wantSealed && sealedStore:
		return nil, storeModeMismatch(storeDir, "encrypted", mode, "on", cfg.Cluster.Enabled)
	case !wantSealed:
		return nil, nil
	case nm == nil:
		return nil, fmt.Errorf("encryption.mode is %q but no node master key is loaded; set encryption.keystore so the node store at %s can be sealed", mode, storeDir)
	case sealedStore:
		dataKey, err := openWrappedStoreKey(nm.Bytes(), wrapped, nodeStoreKeyIdentity)
		if err != nil {
			return nil, fmt.Errorf("unwrapping the node store's key %s under node master kid %s: %w — start with the keystore that sealed it", keyPath, nm.Kid(), err)
		}
		return dataKey, nil
	// A store dir with a MANIFEST but no key file is not necessarily
	// "not encrypted" — it may be sealed with its key file lost. storeIsSealed
	// asks the store itself (its KEYREGISTRY rejects an empty key only if a
	// real key sealed it), so this case fires only for a store that truly was
	// sealed, never for one that never had a key.
	case existingStore && storeIsSealed(storeDir):
		return nil, keyFileMissingError(keyPath, storeDir)
	case existingStore:
		return nil, storeModeMismatch(storeDir, "not encrypted", mode, "off", cfg.Cluster.Enabled)
	}
	return createNodeStoreKey(keyPath, nm)
}

// openWrappedStoreKey unwraps a wrapped store key under a node master, sealed
// under identity: nodeStoreKeyIdentity for keys/store.wrapped, ringStoreKeyIdentity
// for ring/keys/store.wrapped. The start path and -import-key's check pass the
// same constant, so they cannot disagree about which master opens it.
func openWrappedStoreKey(nodeMaster, wrapped []byte, identity string) ([]byte, error) {
	dataKey, _, err := sealed.Open(sealed.PurposeStoreKey, nodeMaster, func(gen uint64) string {
		return fmt.Sprintf("%s/%d", identity, gen)
	}, wrapped)
	if err != nil {
		return nil, err
	}
	if len(dataKey) != 32 {
		return nil, fmt.Errorf("the wrapped store key unwraps to %d bytes, want 32", len(dataKey))
	}
	return dataKey, nil
}

// storeIsSealed reports whether storeDir's Badger KEYREGISTRY proves the
// store was sealed under a real key, independent of whether the node's own
// wrapped key file (keys/store.wrapped) still exists. Badger writes the
// registry's sanity text in the clear when no key was used and encrypted
// under the real key otherwise (badger/v4 key_registry.go:WriteKeyRegistry),
// so opening it read-only with an empty key succeeds for a never-sealed store
// and fails with ErrEncryptionKeyMismatch for a sealed one. Read-only: it
// creates nothing when the registry is absent.
func storeIsSealed(storeDir string) bool {
	_, err := badger.OpenKeyRegistry(badger.KeyRegistryOptions{
		Dir:      storeDir,
		ReadOnly: true,
	})
	return errors.Is(err, badger.ErrEncryptionKeyMismatch)
}

// keyFileMissingError is the refusal for a node store the KEYREGISTRY
// proves was sealed, but whose wrapped key file is gone: the truthful
// diagnosis, distinct from storeModeMismatch's "not encrypted", which would
// send the operator toward encryption.mode off — a setting that cannot open
// a sealed KEYREGISTRY.
func keyFileMissingError(keyPath, storeDir string) error {
	return fmt.Errorf("the node store at %s is sealed, but its key file %s is missing. "+
		"encryption.mode off cannot open a sealed store. Restore keys/store.wrapped from backup or escrow, "+
		"then start this node again", storeDir, keyPath)
}

// storeModeMismatch is the refusal for a store whose sealing disagrees with
// encryption.mode. The server is in ring mode on every start of this build, and
// this refusal fires only when the ring directory holds no evidence of its mode
// (loadRingAtRest answers first otherwise), so its advice is the ring's mode
// switch: move the earlier store and its key aside with the ring directory (ring
// encryption at rest spec §5.5). A cluster member is told to move only the earlier
// store and its key, and to leave the ring directory in place. It goes with the earlier engine.
func storeModeMismatch(storeDir, sealing, mode, storeMode string, member bool) error {
	dataDir := filepath.Dir(storeDir)
	if member {
		// The ring holds no evidence of its mode, so the earlier engine's store is the whole
		// mismatch: moving it and its key aside clears it, and the ring recovery would not (an empty
		// ring directory leaves this store where it is). ring/ is left alone: it may hold Raft state.
		return fmt.Errorf("the node store at %s is %s, but encryption.mode is %q. A node's encryption is fixed when its "+
			"store is created and is not switched in place. Start this node with encryption.mode %s. This node is "+
			"configured as a member of a cluster (cluster.enabled): leave %s in place. To keep encryption.mode %q, stop "+
			"this node and move %s and %s aside (keep them: the ring does not read them), then "+
			"start it again",
			storeDir, sealing, mode, storeMode, filepath.Join(dataDir, ringDirName), mode,
			storeDir, filepath.Join(dataDir, "keys", nodeStoreKeyFileName))
	}
	return fmt.Errorf("the node store at %s is %s, but encryption.mode is %q. A node's encryption is fixed when its "+
		"store is created and is not switched in place. Start this node with encryption.mode %s, or "+
		"move %s and %s aside together with %s, then start again",
		storeDir, sealing, mode, storeMode,
		storeDir, filepath.Join(dataDir, "keys", nodeStoreKeyFileName), filepath.Join(dataDir, ringDirName))
}

// syncDirectory fsyncs a directory, which makes a rename into it durable.
func syncDirectory(dirPath string) error {
	dirFile, err := os.Open(dirPath)
	if err != nil {
		return fmt.Errorf("opening %s to sync it: %w", dirPath, err)
	}
	if err := dirFile.Sync(); err != nil {
		return errors.Join(fmt.Errorf("syncing %s: %w", dirPath, err), dirFile.Close())
	}
	if err := dirFile.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", dirPath, err)
	}
	return nil
}

// createNodeStoreKey generates the store's data key and makes its wrapped form
// durable before the store is opened with it: write a temporary file, fsync
// it, rename it into place, fsync the directory. A store sealed under a key
// whose wrap did not survive a crash could never be opened again.
func createNodeStoreKey(keyPath string, nm *NodeMaster) ([]byte, error) {
	dataKey := make([]byte, 32)
	if _, err := rand.Read(dataKey); err != nil {
		return nil, fmt.Errorf("generating the node store's data key: %w", err)
	}
	wrapped, err := sealed.Seal(sealed.PurposeStoreKey, nm.Bytes(), nodeStoreKeyIdentity, 1, dataKey)
	if err != nil {
		return nil, fmt.Errorf("sealing the node store's data key: %w", err)
	}
	keyDir := filepath.Dir(keyPath)
	if err := os.MkdirAll(keyDir, 0o700); err != nil {
		return nil, fmt.Errorf("creating %s: %w", keyDir, err)
	}
	tempPath := keyPath + ".tmp"
	if err := writeFileSynced(tempPath, wrapped); err != nil {
		return nil, err
	}
	if err := os.Rename(tempPath, keyPath); err != nil {
		return nil, fmt.Errorf("renaming %s into place: %w", tempPath, err)
	}
	if err := syncDirectory(keyDir); err != nil {
		return nil, fmt.Errorf("syncing %s after writing the node store's key: %w", keyDir, err)
	}
	return dataKey, nil
}

func writeFileSynced(path string, contents []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("creating %s: %w", path, err)
	}
	if _, err := file.Write(contents); err != nil {
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

// seedBucketIDAllocator reads the allocator's record and nothing else (spec
// §5.2.2). Present and 8 bytes: the counter starts there. Absent in a store
// that holds no key at all: a new store, which is given its record now so
// that from here on a store with any key always has one. Anything else is a
// refusal to start — never a scan of surviving ids, which is exactly the
// reuse the record exists to prevent.
func seedBucketIDAllocator(kv storage.KV, dataDir string) (uint64, error) {
	snap := kv.NewSnapshot()
	recorded, getErr := snap.Get(allocatorKey())
	holdsAKey := false
	var walkErr error
	if getErr != nil {
		it := snap.NewIterator(storage.IterOptions{})
		it.Seek(nil)
		holdsAKey = it.Valid()
		walkErr = it.Err()
		it.Close()
	}
	snap.Discard()
	if walkErr != nil {
		// A store that could not show its first key is not known to be new,
		// and writing a fresh allocator over one that is not would reuse ids.
		return 0, fmt.Errorf("checking whether the node store is new: %w", walkErr)
	}

	switch {
	case getErr == nil && len(recorded) == 8:
		return binary.BigEndian.Uint64(recorded), nil
	case getErr != nil && !errors.Is(getErr, storage.ErrKeyNotFound):
		return 0, fmt.Errorf("reading the bucket id allocator's record: %w", getErr)
	case getErr != nil && !holdsAKey:
		batch := kv.NewBatch()
		defer batch.Discard()
		if err := batch.Set(allocatorKey(), encodeBucketID(0)); err != nil {
			return 0, fmt.Errorf("recording a new store's bucket id allocator: %w", err)
		}
		if err := batch.Commit(true); err != nil {
			return 0, fmt.Errorf("recording a new store's bucket id allocator: %w", err)
		}
		return 0, nil
	}
	return 0, &allocatorRecordError{dataDir: dataDir}
}

// allocatorRecordError is spec §5.2.2's refusal to start, worded as the spec
// gives it.
type allocatorRecordError struct {
	dataDir string
}

func (e *allocatorRecordError) Error() string {
	return fmt.Sprintf("The bucket id allocator's record is missing or damaged in %s, but the store holds buckets. "+
		"Starting would risk giving a new bucket the id of a dropped one and serving its leftover data. "+
		"Restore %s from a backup — see the backup-restore page of the user docs.", e.dataDir, e.dataDir)
}

// slotForCreate is the slot a new bucket records. It is one of storage.SlotOf's
// two non-test callers; the other is the attach assertion.
func slotForCreate(database, bucket string) uint16 {
	if hook := slotOverrideHook.Load(); hook != nil {
		if slot, ok := (*hook)(database, bucket); ok {
			return slot
		}
	}
	return storage.SlotOf(storage.PlacementKey(database, bucket, nil))
}

// commitBucketCreate allocates the bucket's id and commits its create batch —
// the catalog entry, the bucket's state document (applied_seq at the create
// floor, and the lineage the create starts: its term, its position, and
// opened on its origin), the allocator's new high-water mark, and the
// database's marker if it has none — holding the allocator from the id to the
// commit. When the
// name's catalog key still holds an entry of a dropped database the expander
// has not reached, converted is that entry's reclaim record: it goes in the
// same batch, so the new entry never overwrites the old range's only pointer
// (spec §5.2.2, Q21).
func (s *Server) commitBucketCreate(entry catalogEntry, state store.BucketState, converted *reclaimRecord) (uint64, error) {
	s.bucketIDs.mu.Lock()
	defer s.bucketIDs.mu.Unlock()
	// G-6 (a) under the mutex a drop's tombstones are written under: a drop
	// of the generation either committed before this, and the create is
	// refused, or commits after it and drops the bucket with the rest.
	if dropped, err := s.generationDropped(entry.database, entryGeneration(entry)); err != nil || dropped {
		if err == nil {
			err = errGenerationDropped
		}
		return 0, err
	}
	id, err := storage.NextBucketID(s.bucketIDs.last)
	if err != nil {
		return 0, &bucketIDsExhaustedError{bucket: entry.bucket, last: s.bucketIDs.last}
	}
	batch := s.kv.NewBatch()
	defer batch.Discard()
	if converted != nil {
		encoded, err := bson.Marshal(converted)
		if err != nil {
			return 0, fmt.Errorf("encoding the reclaim record of bucket %d: %w", converted.ID, err)
		}
		if err := batch.Set(reclaimKey(uint64(converted.ID)), encoded); err != nil {
			return 0, fmt.Errorf("writing the reclaim record of bucket %d: %w", converted.ID, err)
		}
	}
	if err := s.writeCreateRecords(batch, entry, id, state); err != nil {
		return 0, err
	}
	if err := batch.Set(allocatorKey(), encodeBucketID(id)); err != nil {
		return 0, fmt.Errorf("advancing the bucket id allocator to %d: %w", id, err)
	}
	if hook := allocatorCommitHook.Load(); hook != nil {
		(*hook)(id)
	}
	if err := batch.Commit(false); err != nil {
		return 0, fmt.Errorf("committing the create of bucket %q (id %d): %w", entry.bucket, id, err)
	}
	s.bucketIDs.last = id
	return id, nil
}

// writeCreateRecords adds a bucket's create records to batch: its catalog
// entry under id, its state document, and its database's marker naming the
// bucket's generation — a new marker stamped from the bucket's own floor,
// state.AppliedSeq, since a bucket that brings its database into being starts
// that incarnation here. The caller holds the allocator's mutex, under which
// every marker change is made.
func (s *Server) writeCreateRecords(batch storage.KVBatch, entry catalogEntry, id uint64, state store.BucketState) error {
	entry.ID = int64(id)
	encoded, err := encodeCatalogEntry(entry)
	if err != nil {
		return err
	}
	if err := batch.Set(catalogKey(entry.database, entry.bucket), encoded); err != nil {
		return fmt.Errorf("writing the catalog entry of bucket %q: %w", entry.bucket, err)
	}
	if err := state.Put(batch, uint16(entry.Slot), storage.LegacyDatabaseID(entry.database), id); err != nil {
		return fmt.Errorf("writing the state of bucket %q: %w", entry.bucket, err)
	}
	return s.addGenerationToMarker(batch, entry.database, entryGeneration(entry), state.AppliedSeq)
}

// materializeKey is the record of an id reserved for a migrated range whose
// catalog entry has not committed, [0xFFFF][0][0x0A][id : 8].
func materializeKey(id uint64) []byte {
	return storage.NodeKey(storage.NSMaterialize, encodeBucketID(id))
}

// materializeRecordsRange bounds every materialise record, half-open.
func materializeRecordsRange() (lower, upper []byte) {
	return storage.NodeKey(storage.NSMaterialize),
		storage.NodeKey(storage.NSMaterialize + 1)
}

// materializeRecord is the value of a materialise record. The
// reservation writes it in the same batch as the allocator's mark, before any
// key or payload file of the id exists, and the catalog batch deletes it in the
// same batch as it writes the catalog entry — so a record exists exactly while
// the id holds data no catalog entry names. It carries what a reclaim record
// needs: the slot, because the range is walked by the recorded slot and never
// a recomputed one (build doc §1 item 6).
type materializeRecord struct {
	ID         int64      `bson:"id"`
	Slot       int32      `bson:"slot"`
	Type       BucketType `bson:"type"`
	Database   string     `bson:"database"`
	Name       string     `bson:"name"`
	ReservedAt time.Time  `bson:"reserved_at"`
}

// materializeTracker is the ids whose materialise is running in this process.
// Its mutex is held across a reservation's commit and the id's registration,
// and across the reclaimer's check, re-read and conversion of a record, so the
// reclaimer can never see a record whose materialise has not registered yet.
// A restart empties it, which is right: no materialise survives one. Zero
// value is ready to use.
type materializeTracker struct {
	mu       sync.Mutex
	inFlight map[uint64]struct{}
}

// reserveBucketID allocates an id and persists the allocator's mark on its
// own, for a bucket whose data is written before its catalog entry (a
// migrated range, which can exceed one batch). The mark and the id's
// materialise record are committed before any key of the id exists, so no
// restart can hand the id out again, and a materialise that never reaches its
// catalog batch leaves a record the reclaimer finds. The id is registered as in
// flight before the tracker's mutex is released; the caller ends it with
// endMaterialize once it has returned from every write to the id.
func (s *Server) reserveBucketID(record materializeRecord) (uint64, error) {
	s.materializations.mu.Lock()
	defer s.materializations.mu.Unlock()
	s.bucketIDs.mu.Lock()
	defer s.bucketIDs.mu.Unlock()
	id, err := storage.NextBucketID(s.bucketIDs.last)
	if err != nil {
		return 0, &bucketIDsExhaustedError{bucket: record.Name, last: s.bucketIDs.last}
	}
	record.ID = int64(id)
	encoded, err := bson.Marshal(record)
	if err != nil {
		return 0, fmt.Errorf("encoding the materialise record of bucket id %d: %w", id, err)
	}
	batch := s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(allocatorKey(), encodeBucketID(id)); err != nil {
		return 0, fmt.Errorf("advancing the bucket id allocator to %d: %w", id, err)
	}
	if err := batch.Set(materializeKey(id), encoded); err != nil {
		return 0, fmt.Errorf("writing the materialise record of bucket id %d: %w", id, err)
	}
	if err := batch.Commit(false); err != nil {
		return 0, fmt.Errorf("reserving bucket id %d for %q: %w", id, record.Name, err)
	}
	s.bucketIDs.last = id
	if s.materializations.inFlight == nil {
		s.materializations.inFlight = make(map[uint64]struct{})
	}
	s.materializations.inFlight[id] = struct{}{}
	return id, nil
}

// endMaterialize retires an id reserveBucketID registered. From here on a
// materialise record left under the id is the reclaimer's.
func (s *Server) endMaterialize(id uint64) {
	s.materializations.mu.Lock()
	defer s.materializations.mu.Unlock()
	delete(s.materializations.inFlight, id)
}

// reclaimFailedMaterialize converts the materialise record of id into a
// reclaim record, in one batch, when no materialise of the id is running — the
// residue of one that stopped before its catalog batch, in this process or
// before a restart. It reports whether it converted one.
//
// The record is read again under the tracker's mutex, after the in-flight
// check: a materialise that committed its catalog batch deleted its record in
// that batch, before it ended, so a record still present once the id is not in
// flight is one no catalog batch ever committed — its id is catalogued nowhere.
func (s *Server) reclaimFailedMaterialize(id uint64) (bool, error) {
	s.materializations.mu.Lock()
	defer s.materializations.mu.Unlock()
	if _, running := s.materializations.inFlight[id]; running {
		return false, nil
	}
	record, present, err := s.readMaterializeRecord(id)
	if err != nil || !present {
		return false, err
	}
	// Database and Name are left empty: no catalog entry ever named
	// this id, so this record is not a drop, but recentlyDroppedBucket cannot
	// tell the two apart except by Database+Name. A failed materialise's record
	// must never match that lookup — a write to the pulled bucket's name must
	// keep reading as "rebalancing has not moved the data here yet" (the data
	// is still on the previous holder), never "was dropped — create it again",
	// which would strand it there. The fields survive on the materialise
	// record itself (record.Database, record.Name), which the reclaimer's own
	// log line reads directly, so nothing here loses provenance for logs.
	encoded, err := bson.Marshal(reclaimRecord{
		ID: record.ID, Slot: record.Slot, Type: record.Type,
		DroppedAt: record.ReservedAt, RangeDatabase: record.Database,
	})
	if err != nil {
		return false, fmt.Errorf("encoding the reclaim record of failed materialise %d: %w", id, err)
	}
	batch := s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Delete(materializeKey(id)); err != nil {
		return false, fmt.Errorf("deleting the materialise record of bucket id %d: %w", id, err)
	}
	if err := batch.Set(reclaimKey(id), encoded); err != nil {
		return false, fmt.Errorf("writing the reclaim record of failed materialise %d: %w", id, err)
	}
	if err := batch.Commit(false); err != nil {
		return false, fmt.Errorf("committing the reclaim of failed materialise %d: %w", id, err)
	}
	return true, nil
}

func (s *Server) readMaterializeRecord(id uint64) (materializeRecord, bool, error) {
	var record materializeRecord
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	encoded, err := snap.Get(materializeKey(id))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return record, false, nil
	}
	if err != nil {
		return record, false, fmt.Errorf("reading the materialise record of bucket id %d: %w", id, err)
	}
	if err := bson.Unmarshal(encoded, &record); err != nil {
		return record, false, fmt.Errorf("decoding the materialise record of bucket id %d: %w", id, err)
	}
	return record, true, nil
}

// databaseMarker is the value of a database's own catalog key. A database is
// recorded on its own so an empty one survives a restart, as it did when the
// catalog was a file.
type databaseMarker struct {
	CreatedAt time.Time `bson:"created_at"`
	// CreatedSeq is this node's log position when this generation of the
	// database was created (generationSeq). It is the database's counterpart
	// of a bucket's applied_seq floor: a logged drop at or below it was
	// appended before this generation existed and cannot be its drop, which
	// is what replay's OpDeleteDB gate compares. A drop deletes
	// the marker and a re-create writes a new one, so it is always the live
	// generation's. 0 on a marker written before the field existed.
	CreatedSeq int64 `bson:"created_seq"`
	// Generations are the database's live incarnations on this node, L(D),
	// in the order they were added. Absent on a marker written before
	// generations existed, which reads as the legacy generation alone.
	Generations []int64 `bson:"generations,omitempty"`
}

// generationSeq is the log position a new database generation is stamped
// with: the apply sequence of the logged entry creating it, or — for a create
// that carries none, a direct call or a materialised copy's database — the
// log's tail, at or above every entry already logged and so every drop that
// could precede this generation, the same floor a materialised bucket takes
// (build doc SA-18). A node with no log replays nothing and records 0.
func (s *Server) generationSeq(applySeq uint64) uint64 {
	if applySeq != 0 || s.wal == nil {
		return applySeq
	}
	return s.wal.LastSequence()
}

func (s *Server) databaseMarkerMissing(database string) (bool, error) {
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	_, err := snap.Get(catalogKey(database, ""))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("reading the catalog entry of database %q: %w", database, err)
	}
	return false, nil
}

// addGenerationToMarker adds to batch database's marker with generation among
// its live generations, unless it is there already. A database with no marker
// gets one stamped with the log position of applySeq (generationSeq); a
// marker that gains a generation keeps the position it was created at, which
// the replay gate of a drop logged before generations existed reads. The
// caller holds the allocator's mutex.
func (s *Server) addGenerationToMarker(batch storage.KVBatch, database string, generation int64, applySeq uint64) error {
	snap := s.kv.NewSnapshot()
	live, marker, present, err := markerGenerations(snap, database)
	snap.Discard()
	if err != nil {
		return err
	}
	if present && slices.Contains(live, generation) {
		return nil
	}
	if !present {
		marker = databaseMarker{CreatedAt: time.Now().UTC(), CreatedSeq: int64(s.generationSeq(applySeq))}
	}
	marker.Generations = append(live, generation)
	return s.putDatabaseMarker(batch, database, marker)
}

// putDatabaseMarker adds database's marker to batch.
func (s *Server) putDatabaseMarker(batch storage.KVBatch, database string, marker databaseMarker) error {
	encoded, err := bson.Marshal(marker)
	if err != nil {
		return fmt.Errorf("encoding the catalog entry of database %q: %w", database, err)
	}
	if err := batch.Set(catalogKey(database, ""), encoded); err != nil {
		return fmt.Errorf("writing the catalog entry of database %q: %w", database, err)
	}
	return nil
}

// databaseCatalogRange bounds a database's catalog keys: its marker, then each
// of its buckets' entries.
func databaseCatalogRange(database string) (lower, upper []byte) {
	lower = catalogKey(database, "")
	upper = append([]byte(nil), lower...)
	upper[len(upper)-1]++ // the byte after "/" bounds every "database/…" name
	return lower, upper
}
