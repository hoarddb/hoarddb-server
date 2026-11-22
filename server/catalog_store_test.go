package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Step 4 of the shared-storage build: the catalog, the allocator and the ids
// live in the node store. The keys below are built with storage.Key from the
// spec's layout (§3.1, §3.1.1), not with the server's own helpers, so a test
// that reads them checks the layout rather than agreeing with itself.

// sharedTestNodeStores gives every hand-built server over one data directory
// the same in-memory node store, the way a restarted server finds the store
// its predecessor wrote.
var sharedTestNodeStores sync.Map

func sharedTestNodeStore(dataDir string) storage.KV {
	kv, _ := sharedTestNodeStores.LoadOrStore(dataDir, kvfault.New())
	return kv.(storage.KV)
}

func testCatalogKey(database, bucket string) []byte {
	return storage.NodeKey(storage.NSCatalog, []byte(database+"/"+bucket))
}

func testAllocatorKey() []byte {
	return storage.NodeKey(storage.NSAllocator)
}

// newServiceAt starts a service over dataDir and returns NewService's error
// instead of failing the test, for the starts that must be refused.
func newServiceAt(t *testing.T, dataDir string) (*Service, error) {
	t.Helper()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("loading default config: %v", err)
	}
	cfg.Storage.DataDir = dataDir
	cfg.Server.ReplicationFactor = 1
	qs, err := NewService(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = qs.Stop() })
	return qs, nil
}

// withStoppedNodeStore opens the node store of a stopped server directly, for
// the edits a test makes behind the server's back.
func withStoppedNodeStore(t *testing.T, dataDir string, edit func(kv storage.KV)) {
	t.Helper()
	kv, err := storage.OpenBadgerKV(filepath.Join(dataDir, "store"), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening the stopped node's store: %v", err)
	}
	edit(kv)
	if err := kv.Close(); err != nil {
		t.Fatalf("closing the stopped node's store: %v", err)
	}
}

func setStoreKey(t *testing.T, kv storage.KV, key, value []byte) {
	t.Helper()
	batch := kv.NewBatch()
	if err := batch.Set(key, value); err != nil {
		t.Fatalf("setting %x: %v", key, err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing %x: %v", key, err)
	}
}

func deleteStoreKey(t *testing.T, kv storage.KV, key []byte) {
	t.Helper()
	batch := kv.NewBatch()
	if err := batch.Delete(key); err != nil {
		t.Fatalf("deleting %x: %v", key, err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing the delete of %x: %v", key, err)
	}
}

func readStoreKey(t *testing.T, kv storage.KV, key []byte) ([]byte, bool) {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	value, err := snap.Get(key)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return nil, false
	}
	if err != nil {
		t.Fatalf("reading %x: %v", key, err)
	}
	return value, true
}

// bucketIDOf is the id the server holds for a bucket, or 0 when it serves none.
func bucketIDOf(t *testing.T, s *Server, database, bucket string) uint64 {
	t.Helper()
	b := lookupBucket(s, database, bucket)
	if b == nil {
		return 0
	}
	b.rec.mu.RLock()
	defer b.rec.mu.RUnlock()
	return b.id.Load()
}

func mustCreateBucket(t *testing.T, s *Server, database, bucket string) uint64 {
	t.Helper()
	resp, err := s.CreateBucket(CreateBucketRequest{Database: database, Name: bucket, Type: BucketTypeHash})
	if err != nil || !resp.Success {
		t.Fatalf("CreateBucket(%s/%s) = %+v, %v", database, bucket, resp, err)
	}
	id := bucketIDOf(t, s, database, bucket)
	if id == 0 {
		t.Fatalf("bucket %s/%s was created but holds no id", database, bucket)
	}
	return id
}

func mustDropBucket(t *testing.T, s *Server, database, bucket string) {
	t.Helper()
	resp, err := s.DeleteBucket(DeleteBucketRequest{Database: database, Name: bucket})
	if err != nil || !resp.Success {
		t.Fatalf("DeleteBucket(%s/%s) = %+v, %v", database, bucket, resp, err)
	}
}

// TestBucketIDsAreNeverReusedAcrossRestart is spec T-18.
func TestBucketIDsAreNeverReusedAcrossRestart(t *testing.T) {
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	droppedID := mustCreateBucket(t, first.core, "app", "users")
	mustDropBucket(t, first.core, "app", "users")

	second := restartCatalog(t, first, dataDir)
	recreatedID := mustCreateBucket(t, second.core, "app", "users")
	if recreatedID <= droppedID {
		t.Fatalf("a bucket created after a restart got id %d, not above the dropped bucket's id %d: ids must never be reused", recreatedID, droppedID)
	}
}

// TestDropAndRecreateGetsANewID is spec T-04, first pass: the drop is still
// the directory removal plus the catalog key's delete. Step 14 re-runs it.
func TestDropAndRecreateGetsANewID(t *testing.T) {
	s := startCatalogServerAt(t, t.TempDir()).core
	oldID := mustCreateBucket(t, s, "app", "users")
	if _, err := s.Insert(InsertRequest{Database: "app", Bucket: "users", Key: "old-key", Data: map[string]interface{}{"n": 1}}); err != nil {
		t.Fatalf("Insert into the old bucket: %v", err)
	}
	mustDropBucket(t, s, "app", "users")

	newID := mustCreateBucket(t, s, "app", "users")
	if newID == oldID {
		t.Fatalf("the re-created bucket has the dropped bucket's id %d", oldID)
	}
	found, err := s.Find(FindRequest{Database: "app", Bucket: "users", Key: "old-key"})
	if err != nil {
		t.Fatalf("Find in the new bucket: %v", err)
	}
	if found.Found {
		t.Fatal("the re-created bucket returned a key written to the dropped one")
	}
}

// TestAllocatorRecordMissingRefusesStart is spec T-28(b).
func TestAllocatorRecordMissingRefusesStart(t *testing.T) {
	const refusal = "The bucket id allocator's record is missing or damaged in"
	for _, damage := range []struct {
		name  string
		apply func(t *testing.T, kv storage.KV)
	}{
		{"deleted", func(t *testing.T, kv storage.KV) { deleteStoreKey(t, kv, testAllocatorKey()) }},
		{"three bytes", func(t *testing.T, kv storage.KV) { setStoreKey(t, kv, testAllocatorKey(), []byte{0, 0, 5}) }},
	} {
		t.Run(damage.name, func(t *testing.T) {
			dataDir := t.TempDir()
			first := startCatalogServerAt(t, dataDir)
			mustCreateBucket(t, first.core, "app", "users")
			first.stop(t)
			withStoppedNodeStore(t, dataDir, func(kv storage.KV) { damage.apply(t, kv) })

			_, err := newServiceAt(t, dataDir)
			if err == nil {
				t.Fatal("a store holding buckets started with its allocator record " + damage.name)
			}
			for _, want := range []string{refusal, dataDir, "the backup-restore page of the user docs"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal must contain %q, got: %v", want, err)
				}
			}
		})
	}

	t.Run("an empty store starts at id 1", func(t *testing.T) {
		s := startCatalogServerAt(t, t.TempDir()).core
		if id := mustCreateBucket(t, s, "app", "users"); id != 1 {
			t.Fatalf("the first bucket of a new store got id %d, want 1", id)
		}
	})
}

// TestAllocatorCeilingRefusesCreate is spec T-28(d).
func TestAllocatorCeilingRefusesCreate(t *testing.T) {
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	mustCreateBucket(t, first.core, "app", "users")
	first.stop(t)
	ceiling := binary.BigEndian.AppendUint64(nil, storage.MaxBucketID)
	withStoppedNodeStore(t, dataDir, func(kv storage.KV) { setStoreKey(t, kv, testAllocatorKey(), ceiling) })

	second := startCatalogServerAt(t, dataDir)
	if err := second.qs.LoadCatalog(); err != nil {
		t.Fatalf("LoadCatalog: %v", err)
	}
	resp, err := second.core.CreateBucket(CreateBucketRequest{Database: "app", Name: "sessions", Type: BucketTypeHash})
	if err != nil {
		t.Fatalf("CreateBucket at the ceiling returned a transport error: %v", err)
	}
	if resp.Success {
		t.Fatal("a create past the id ceiling succeeded")
	}
	want := `Bucket "sessions" cannot be created: this node has allocated every bucket id it can (the last was 9223372036854775807). ` +
		`Bucket ids are never reused, so this node's catalog is full; restore its data into a fresh data directory with 'hoarddb dump' and 'restore'.`
	if resp.Message != want {
		t.Errorf("the ceiling's refusal is\n  %s\nwant spec §3.1's\n  %s", resp.Message, want)
	}
	second.stop(t)
	withStoppedNodeStore(t, dataDir, func(kv storage.KV) {
		recorded, _ := readStoreKey(t, kv, testAllocatorKey())
		if !bytes.Equal(recorded, ceiling) {
			t.Errorf("the refused create changed the allocator's record to %x, want it left at %x", recorded, ceiling)
		}
	})
}

// TestLegacyCatalogJSONRefusesStart is spec T-17, shape 1.
func TestLegacyCatalogJSONRefusesStart(t *testing.T) {
	dataDir := t.TempDir()
	legacy := []byte(`{"version": 1, "databases": {"app": {"buckets": {"users": {"type": "hash"}}}}}`)
	if err := os.WriteFile(filepath.Join(dataDir, "catalog.json"), legacy, 0o644); err != nil {
		t.Fatalf("writing the legacy catalog: %v", err)
	}
	before := topLevelNames(t, dataDir)
	_, err := newServiceAt(t, dataDir)
	if err == nil {
		t.Fatal("a data directory holding the per-bucket layout's catalog.json started")
	}
	assertStartCreatedNothing(t, dataDir, before)
	if want := wantLegacyLayoutRefusal(dataDir); !strings.Contains(err.Error(), want) {
		t.Errorf("the refusal is\n  %v\nwant\n  %s", err, want)
	}
	if _, statErr := os.Stat(filepath.Join(dataDir, "store")); !os.IsNotExist(statErr) {
		t.Errorf("the refused start created the node store beside the legacy layout (stat: %v)", statErr)
	}
}

// TestBucketNotFoundIsASentinel: a find on a missing bucket is ErrBucketNotFound
// in Go and transport.ErrNotFound on the wire.
func TestBucketNotFoundIsASentinel(t *testing.T) {
	s := newTestServer(t)
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	_, err := s.Find(FindRequest{Database: "app", Bucket: "absent", Key: "k"})
	if !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("Find on a missing bucket returned %v, want an error that is ErrBucketNotFound", err)
	}
	if !strings.Contains(err.Error(), "show buckets in app") {
		t.Errorf("the error lost its recovery hint: %v", err)
	}

	payload, marshalErr := bson.Marshal(FindRequest{Database: "app", Bucket: "absent", Key: "k"})
	if marshalErr != nil {
		t.Fatalf("marshal find: %v", marshalErr)
	}
	frame, err := s.dispatch(t.Context(), &transport.Frame{Opcode: transport.OpGet, Payload: payload})
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	var errResp ErrorResponse
	if frame.Opcode != transport.OpError {
		t.Fatalf("a find on a missing bucket answered %s, want an error frame", frame.Opcode)
	}
	if err := bson.Unmarshal(frame.Payload, &errResp); err != nil {
		t.Fatalf("decode error frame: %v", err)
	}
	if transport.ErrorCode(errResp.Code) != transport.ErrNotFound {
		t.Errorf("wire code = %v, want %v", transport.ErrorCode(errResp.Code), transport.ErrNotFound)
	}
}

// TestNodeRecordsLieOutsideEverySlot is spec T-31: the catalog, allocator and
// reclaim-record clauses. The reclaim record is written by a drop (build doc
// Step 14), and its key is built here from the layout, not from reclaimKey.
//
// The reclaim record's presence is never asserted: DeleteBucket's drop path
// (server/manage.go:handToReclaimer) wakes the reclaimer the instant the
// record is written, and that function's own doc comment says an empty
// bucket's one reclaim lap can finish before the next reader checks — so the
// record can legitimately be gone by the time this test reads it.
// Its placement claim ("lies outside every slot") is instead checked from the
// key bytes storage.Key built, which does not depend on whether the record
// still exists in the store.
func TestNodeRecordsLieOutsideEverySlot(t *testing.T) {
	s := startCatalogServerAt(t, t.TempDir()).core
	mustCreateBucket(t, s, "app", "users")
	droppedID := mustCreateBucket(t, s, "app", "dropped")
	if resp, err := s.DeleteBucket(DeleteBucketRequest{Database: "app", Name: "dropped"}); err != nil || !resp.Success {
		t.Fatalf("drop: %+v, %v", resp, err)
	}
	reclaimRecordKey := storage.NodeKey(storage.NSReclaim, binary.BigEndian.AppendUint64(nil, droppedID))

	// catalogKey and allocatorKey are not reclaim-transient: DeleteBucket
	// never removes either (it stops the bucket from resolving; it does not
	// touch the allocator record), so both must still be found where the
	// layout puts them. reclaimRecordKey joins them only for the "does not
	// lie inside any slot" walk below, which is a property of the key bytes
	// and never requires the record to still be present.
	catalogKey := testCatalogKey("app", "users")
	allocatorKey := testAllocatorKey()
	requiredNodeRecords := [][]byte{catalogKey, allocatorKey}
	nodeRecords := [][]byte{catalogKey, allocatorKey, reclaimRecordKey}
	nodePrefix := append([]byte{0xFF, 0xFF}, make([]byte, 8)...)
	for _, key := range requiredNodeRecords {
		if _, found := readStoreKey(t, s.kv, key); !found {
			t.Fatalf("node record %x is not where the layout puts it", key)
		}
		if !bytes.HasPrefix(key, nodePrefix) {
			t.Fatalf("node record %x does not begin 0xFFFF then eight zero bytes", key)
		}
	}
	if !bytes.HasPrefix(reclaimRecordKey, nodePrefix) {
		t.Fatalf("reclaim record %x does not begin 0xFFFF then eight zero bytes", reclaimRecordKey)
	}

	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	for slot := 0; slot < storage.SlotCount; slot++ {
		it := snap.NewIterator(storage.IterOptions{
			Lower: storage.PairPrefix(uint16(slot), 0),
			Upper: storage.PairPrefix(uint16(slot+1), 0),
		})
		for it.Seek(nil); it.Valid(); it.Next() {
			for _, record := range nodeRecords {
				if bytes.Equal(it.Key(), record) {
					t.Fatalf("node record %x lies inside slot %d's range", record, slot)
				}
			}
		}
		it.Close()
	}

	it := snap.NewIterator(storage.IterOptions{Lower: storage.PairPrefix(0, 0), Upper: storage.PairPrefix(1, 0)})
	defer it.Close()
	for it.Seek(nil); it.Valid(); it.Next() {
		if bytes.HasPrefix(it.Key(), nodePrefix) {
			t.Fatalf("a walk of slot 0 returned node record %x", it.Key())
		}
	}
}

// everyStoredServerKey returns every key of a stopped node's store, Badger's
// internal keys and every version included. It is storage/keys_test.go's
// everyStoredKey: that one lives in package storage's internal test, which
// this package cannot import.
func everyStoredServerKey(t *testing.T, dataDir string) [][]byte {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions(filepath.Join(dataDir, "store")).WithLogger(nil))
	if err != nil {
		t.Fatalf("opening the stopped node's store: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("closing the stopped node's store: %v", err)
		}
	}()
	var keys [][]byte
	err = db.View(func(txn *badger.Txn) error {
		iterator := txn.NewIterator(badger.IteratorOptions{AllVersions: true, InternalAccess: true})
		defer iterator.Close()
		for iterator.Rewind(); iterator.Valid(); iterator.Next() {
			keys = append(keys, iterator.Item().KeyCopy(nil))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking every stored key: %v", err)
	}
	return keys
}

// TestNoStoreKeyBeginsWithBadgerPrefix is spec T-32, first pass. Re-run
// unchanged at Step 13, when all six engines write keys.
func TestNoStoreKeyBeginsWithBadgerPrefix(t *testing.T) {
	for slot := 0; slot < storage.SlotCount; slot++ {
		if first := storage.Key(uint16(slot), 1, 1, storage.NSDocuments)[0]; first > 0x1F {
			t.Fatalf("a key in slot %d begins with 0x%02X, outside 0x00–0x1F, below Badger's 0x21", slot, first)
		}
	}

	dataDir := t.TempDir()
	cs := startCatalogServerAt(t, dataDir)
	for _, name := range []string{"users", "events", "sessions"} {
		mustCreateBucket(t, cs.core, "app", name)
	}
	cs.stop(t)

	written := 0
	for _, key := range everyStoredServerKey(t, dataDir) {
		if bytes.HasPrefix(key, []byte("!badger!")) {
			continue
		}
		written++
		if key[0] <= 0x0F || bytes.HasPrefix(key, []byte{0xFF, 0xFF}) {
			continue
		}
		t.Errorf("the store holds key %x, whose first byte 0x%02X is neither a slot's nor the node records'", key, key[0])
	}
	if written == 0 {
		t.Fatal("the walk found no key hoardDB wrote, so it checked nothing")
	}
}

// TestRecordedSlotMatchesSlotOf is spec T-33: an entry whose slot is not the
// one its name hashes to refuses the start; the same entry corrected attaches.
func TestRecordedSlotMatchesSlotOf(t *testing.T) {
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	mustCreateBucket(t, first.core, "app", "users")
	if _, err := first.core.Insert(InsertRequest{Database: "app", Bucket: "users", Key: "u1", Data: map[string]interface{}{"n": 1}}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	first.stop(t)

	rightSlot := storage.SlotOf(storage.PlacementKey("app", "users", nil))
	wrongSlot := (rightSlot + 1) % storage.SlotCount
	rewriteSlot := func(slot uint16) {
		withStoppedNodeStore(t, dataDir, func(kv storage.KV) {
			encoded, found := readStoreKey(t, kv, testCatalogKey("app", "users"))
			if !found {
				t.Fatal("the bucket's catalog entry is not where the layout puts it")
			}
			var entry bson.M
			if err := bson.Unmarshal(encoded, &entry); err != nil {
				t.Fatalf("decoding the catalog entry: %v", err)
			}
			entry["slot"] = int32(slot)
			rewritten, err := bson.Marshal(entry)
			if err != nil {
				t.Fatalf("encoding the catalog entry: %v", err)
			}
			setStoreKey(t, kv, testCatalogKey("app", "users"), rewritten)
		})
	}

	rewriteSlot(wrongSlot)
	refused := startCatalogServerAt(t, dataDir)
	err := refused.qs.LoadCatalog()
	if err == nil {
		t.Fatalf("a catalog entry recording slot %d for a name that hashes to %d attached", wrongSlot, rightSlot)
	}
	for _, want := range []string{`Bucket "users" in database "app" cannot be opened`, "records storage slot " + strconv.Itoa(int(wrongSlot)),
		"hashes to slot " + strconv.Itoa(int(rightSlot)), "'hoarddb dump' and 'restore'"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal must contain %q, got: %v", want, err)
		}
	}
	refused.stop(t)

	rewriteSlot(rightSlot)
	served := restartCatalog(t, startCatalogServerAt(t, dataDir), dataDir)
	found, err := served.core.Find(FindRequest{Database: "app", Bucket: "users", Key: "u1"})
	if err != nil || !found.Found {
		t.Fatalf("the corrected entry does not serve its document: %+v, %v", found, err)
	}
}

// TestNoBucketRenameVerbExists is spec T-33's second clause (G-01): the slot
// comes from (database, bucket), so no verb may rename a bucket. It pins an
// absence and passes by design; its proof is the break that adds one.
func TestNoBucketRenameVerbExists(t *testing.T) {
	for op := 0; op <= 255; op++ {
		if name := transport.Opcode(op).String(); strings.Contains(strings.ToUpper(name), "RENAME") {
			t.Errorf("opcode 0x%02X is %s: renaming a bucket is dump and restore (spec §3.1.1)", op, name)
		}
	}
	serverType := reflect.TypeOf(&Server{})
	for i := 0; i < serverType.NumMethod(); i++ {
		if name := serverType.Method(i).Name; strings.Contains(name, "Rename") {
			t.Errorf("Server has method %s: renaming a bucket is dump and restore (spec §3.1.1)", name)
		}
	}
}

// TestEmptyDatabaseSurvivesRestart pins the database's own catalog key: with
// the catalog in the store, a database with no bucket would otherwise vanish
// on a restart, which the catalog file never let happen.
func TestEmptyDatabaseSurvivesRestart(t *testing.T) {
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	if resp, err := first.core.CreateDatabase("empty"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	second := restartCatalog(t, first, dataDir)
	databases, err := second.core.ListDatabases(ListDatabasesRequest{})
	if err != nil {
		t.Fatalf("ListDatabases: %v", err)
	}
	if len(databases.Databases) != 1 || databases.Databases[0].Name != "empty" {
		t.Fatalf("databases after restart = %+v, want the empty database back", databases.Databases)
	}
}
