package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
)

// catalogServer is a core server plus the wrapper needed to shut it down, so a
// test can stop one server and start another over the same files.
type catalogServer struct {
	qs      *Service
	core    *Server
	stopped bool
}

func (cs *catalogServer) stop(t *testing.T) {
	t.Helper()

	if cs.stopped {
		return
	}
	if err := cs.qs.Stop(); err != nil {
		t.Fatalf("stopping server: %v", err)
	}
	cs.stopped = true
}

// startCatalogServerAt builds a core server over a caller-chosen data
// directory. newTestServer always allocates a fresh temp dir, which makes it
// impossible to open the same data twice the way a restart does.
func startCatalogServerAt(t *testing.T, dataDir string) *catalogServer {
	t.Helper()

	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("loading default config: %v", err)
	}
	cfg.Storage.DataDir = dataDir
	cfg.Server.ReplicationFactor = 1

	qs, err := NewService(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("creating server: %v", err)
	}

	cs := &catalogServer{qs: qs, core: qs.core}
	t.Cleanup(func() {
		if cs.stopped {
			return
		}
		cs.stopped = true
		if err := qs.Stop(); err != nil {
			t.Logf("stopping server: %v", err)
		}
	})
	return cs
}

// restartCatalog shuts a server down and opens the same directory again, the
// way a real restart does. BadgerDB takes an exclusive lock on its directory, so
// the previous server has to be closed before the next one can open it.
func restartCatalog(t *testing.T, prev *catalogServer, dataDir string) *catalogServer {
	t.Helper()

	prev.stop(t)
	next := startCatalogServerAt(t, dataDir)
	if err := next.qs.LoadCatalog(); err != nil {
		t.Fatalf("LoadCatalog: %v", err)
	}
	return next
}

// TestCatalogRestoresDatabasesAndBucketsAfterRestart covers the case where a
// restart reported "no databases yet" and every document was unreachable, even
// though the bytes were still on disk.
func TestCatalogRestoresDatabasesAndBucketsAfterRestart(t *testing.T) {
	dataDir := t.TempDir()

	first := startCatalogServerAt(t, dataDir)
	s := first.core

	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	for _, b := range []struct {
		name string
		typ  BucketType
	}{
		{"users", BucketTypeHash},
		{"events", BucketTypeBTree},
		{"queue", BucketTypeFIFO},
	} {
		resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: b.name, Type: b.typ})
		if err != nil || !resp.Success {
			t.Fatalf("CreateBucket(%q) = %+v, %v", b.name, resp, err)
		}
	}
	if _, err := s.Insert(InsertRequest{
		Database: "app",
		Bucket:   "users",
		Key:      "u1",
		Data:     map[string]interface{}{"Name": "ada"},
	}); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// The catalog is keys in the node store; the per-bucket layout's file is
	// never written, and its presence would refuse the next start.
	if _, err := os.Stat(filepath.Join(dataDir, "store", "MANIFEST")); err != nil {
		t.Fatalf("the node store holding the catalog was not written to the data directory: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "catalog.json")); !os.IsNotExist(err) {
		t.Fatalf("catalog.json exists in the data directory (stat error %v); the catalog belongs in the node store", err)
	}

	second := restartCatalog(t, first, dataDir)
	s2 := second.core

	databases, err := s2.ListDatabases(ListDatabasesRequest{})
	if err != nil {
		t.Fatalf("ListDatabases after restart: %v", err)
	}
	if len(databases.Databases) != 1 || databases.Databases[0].Name != "app" {
		t.Fatalf("databases after restart = %+v, want one named app", databases.Databases)
	}

	buckets, err := s2.ListBuckets(ListBucketsRequest{Database: "app"})
	if err != nil {
		t.Fatalf("ListBuckets after restart: %v", err)
	}
	if len(buckets.Buckets) != 3 {
		t.Fatalf("buckets after restart = %+v, want 3", buckets.Buckets)
	}
	// The type cannot be inferred from the directory (hash, btree and heap all
	// use a BadgerDB directory), so it has to come back from the catalog.
	wantTypes := map[string]BucketType{
		"users":  BucketTypeHash,
		"events": BucketTypeBTree,
		"queue":  BucketTypeFIFO,
	}
	for _, b := range buckets.Buckets {
		if wantTypes[b.Name] != b.Type {
			t.Errorf("bucket %q came back as %q, want %q", b.Name, b.Type, wantTypes[b.Name])
		}
	}

	found, err := s2.Find(FindRequest{Database: "app", Bucket: "users", Key: "u1"})
	if err != nil {
		t.Fatalf("Find after restart: %v", err)
	}
	if found.Found == false {
		t.Fatal("document written before the restart was not found after it")
	}
	var doc map[string]interface{}
	if err := bson.Unmarshal(found.Data, &doc); err != nil {
		t.Fatalf("decode found document: %v", err)
	}
	if doc["Name"] != "ada" {
		t.Errorf("Name after restart = %v, want ada", doc["Name"])
	}

	// A hash bucket rebuilt from storage must report its real entry count; its
	// counter lives in memory and starts at zero on every open.
	count, err := s2.Count(CountRequest{Database: "app", Bucket: "users"})
	if err != nil {
		t.Fatalf("Count after restart: %v", err)
	}
	if count.Count != 1 {
		t.Errorf("Count after restart = %d, want 1", count.Count)
	}
}

// TestBatchInsertedDocumentsAreCounted covers a bucket filled by batch insert
// reporting zero entries: the hash store's counter was maintained by Put but not
// by PutBatch, so the documents were on disk and invisible to Count.
func TestBatchInsertedDocumentsAreCounted(t *testing.T) {
	s := startCatalogServerAt(t, t.TempDir()).core

	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	if resp, err := s.CreateBucket(CreateBucketRequest{
		Database: "app",
		Name:     "users",
		Type:     BucketTypeHash,
	}); err != nil || !resp.Success {
		t.Fatalf("CreateBucket = %+v, %v", resp, err)
	}

	const n = 25
	docs := make([]map[string]interface{}, 0, n)
	for i := 0; i < n; i++ {
		docs = append(docs, map[string]interface{}{
			"_id_":  fmt.Sprintf("u%02d", i),
			"Email": fmt.Sprintf("u%02d@example.com", i),
		})
	}
	resp, err := s.BatchInsert(BatchInsertRequest{Database: "app", Bucket: "users", Data: docs})
	if err != nil || !resp.Success {
		t.Fatalf("BatchInsert = %+v, %v", resp, err)
	}

	count, err := s.Count(CountRequest{Database: "app", Bucket: "users"})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count.Count != n {
		t.Errorf("Count after batch inserting %d documents = %d, want %d", n, count.Count, n)
	}

	// The documents must also be individually readable, not just counted.
	found, err := s.Find(FindRequest{Database: "app", Bucket: "users", Key: "u07"})
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if !found.Found {
		t.Error("u07 was counted but could not be read back")
	}
}

// TestCatalogForgetsDroppedDatabases keeps a dropped database from coming back
// on a restart. The catalog is the only record of a database, so the drop must
// remove it from there; there is no directory to prune a stale entry by.
func TestCatalogForgetsDroppedDatabases(t *testing.T) {
	dataDir := t.TempDir()

	first := startCatalogServerAt(t, dataDir)
	if resp, err := first.core.CreateDatabase("gone"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	if resp, err := first.core.CreateBucket(CreateBucketRequest{Database: "gone", Name: "b", Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("CreateBucket = %+v, %v", resp, err)
	}
	if resp, err := first.core.DeleteDatabase(DeleteDatabaseRequest{Name: "gone"}); err != nil || !resp.Success {
		t.Fatalf("DeleteDatabase = %+v, %v", resp, err)
	}

	second := restartCatalog(t, first, dataDir)

	databases, err := second.core.ListDatabases(ListDatabasesRequest{})
	if err != nil {
		t.Fatalf("ListDatabases: %v", err)
	}
	if len(databases.Databases) != 0 {
		t.Errorf("databases = %+v, want none: a dropped database must not come back", databases.Databases)
	}
}

// TestDeclaredIndexesApplyAndSurviveRestart covers the two ways a declared
// index used to disappear: the server only understood the object form of
// "indices" while the CLI teaches the ["Email"] form, and the declaration was
// never written to the catalog, so a restart silently dropped the bucket's
// indexes while its documents stayed.
func TestDeclaredIndexesApplyAndSurviveRestart(t *testing.T) {
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	s := first.core

	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	created, err := s.CreateBucket(CreateBucketRequest{
		Database: "app",
		Name:     "users",
		Type:     BucketTypeHash,
		Options:  map[string]interface{}{"indices": bson.A{"Email"}},
	})
	if err != nil || !created.Success {
		t.Fatalf("CreateBucket with indices = %+v, %v", created, err)
	}

	stats, err := s.BucketStats(BucketStatsRequest{Database: "app", Bucket: "users"})
	if err != nil {
		t.Fatalf("BucketStats: %v", err)
	}
	if len(stats.Indexes) != 1 || !strings.Contains(stats.Indexes[0], "Email") {
		t.Fatalf("indexes after create = %v, want one on Email", stats.Indexes)
	}

	// A declaration that names no field must fail loudly, not create a bucket
	// that claims an index it does not have.
	bad, err := s.CreateBucket(CreateBucketRequest{
		Database: "app",
		Name:     "broken",
		Type:     BucketTypeHash,
		Options:  map[string]interface{}{"indices": bson.A{"   "}},
	})
	if err != nil {
		t.Fatalf("CreateBucket with an empty index declaration: %v", err)
	}
	if bad.Success {
		t.Fatalf("an index declaration naming no field was accepted: %+v", bad)
	}

	second := restartCatalog(t, first, dataDir)
	stats2, err := second.core.BucketStats(BucketStatsRequest{Database: "app", Bucket: "users"})
	if err != nil {
		t.Fatalf("BucketStats after restart: %v", err)
	}
	if len(stats2.Indexes) != 1 || !strings.Contains(stats2.Indexes[0], "Email") {
		t.Fatalf("indexes after restart = %v, want the Email index back", stats2.Indexes)
	}
}

// legacyCatalogEntry is catalogEntry as it was encoded before it had a
// ReplicationFactor: the same keys, in the same order, with the same tags. It
// is the byte-for-byte reference for "an entry that declares no factor is
// today's entry". It must not grow a field — that is the point of it.
type legacyCatalogEntry struct {
	ID          int64          `bson:"id"`
	Slot        int32          `bson:"slot"`
	Type        BucketType     `bson:"type"`
	Indices     []catalogIndex `bson:"indices,omitempty"`
	CreatedAt   time.Time      `bson:"created_at"`
	NextIndexID *int32         `bson:"next_index_id,omitempty"`
	Stores      []catalogStore `bson:"stores,omitempty"`
	NextStoreID int32          `bson:"next_store_id,omitempty"`
}

func factorTestEntry(replicationFactor int32) catalogEntry {
	return catalogEntry{
		database: "app", bucket: "events",
		ID: 7, Slot: 12, Type: BucketTypeHash,
		CreatedAt:         time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC),
		ReplicationFactor: replicationFactor,
	}
}

// TestCatalogEntryWithoutAFactorLoadsAsInheritedAndReencodesToTheSameBytes is
// T-28's first half. An entry written before the field, and the entry of a
// bucket that declares nothing, must be the same bytes: there is no rewrite
// pass at upgrade, so a bucket that never declared a factor must keep the
// placement it has. Absent means inherit.
func TestCatalogEntryWithoutAFactorLoadsAsInheritedAndReencodesToTheSameBytes(t *testing.T) {
	legacy, err := bson.Marshal(legacyCatalogEntry{
		ID: 7, Slot: 12, Type: BucketTypeHash,
		CreatedAt: time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("encoding the legacy entry: %v", err)
	}

	var loaded catalogEntry
	if err := bson.Unmarshal(legacy, &loaded); err != nil {
		t.Fatalf("decoding an entry written before the factor existed: %v", err)
	}
	if loaded.ReplicationFactor != 0 {
		t.Fatalf("an entry without a factor loaded as %d, want 0 (inherit)", loaded.ReplicationFactor)
	}

	reencoded, err := encodeCatalogEntry(factorTestEntry(0))
	if err != nil {
		t.Fatalf("encodeCatalogEntry: %v", err)
	}
	if !bytes.Equal(reencoded, legacy) {
		t.Fatalf("an entry that declares no factor encodes differently from today's:\n now %v\n was %v", bson.Raw(reencoded), bson.Raw(legacy))
	}
}

// TestCatalogEntryRoundTripsAnExplicitFactor is T-28's second half, and pins
// the two numbers spec §17.3 quotes: the key is replication_factor, and it
// costs 24 bytes (type byte, 19 name bytes and a NUL, 4 value bytes).
func TestCatalogEntryRoundTripsAnExplicitFactor(t *testing.T) {
	without, err := encodeCatalogEntry(factorTestEntry(0))
	if err != nil {
		t.Fatalf("encodeCatalogEntry: %v", err)
	}
	with, err := encodeCatalogEntry(factorTestEntry(3))
	if err != nil {
		t.Fatalf("encodeCatalogEntry: %v", err)
	}

	factor, ok := bson.Raw(with).Lookup("replication_factor").Int32OK()
	if !ok || factor != 3 {
		t.Fatalf("the entry carries replication_factor %d (present %v), want int32 3; the document is %v", factor, ok, bson.Raw(with))
	}
	if extra := len(with) - len(without); extra != 24 {
		t.Fatalf("an explicit factor costs %d catalog bytes, want 24", extra)
	}

	var loaded catalogEntry
	if err := bson.Unmarshal(with, &loaded); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if loaded.ReplicationFactor != 3 {
		t.Fatalf("the factor round-tripped as %d, want 3", loaded.ReplicationFactor)
	}
}

// TestCatalogEncoderKeepsARecordedNextIndexID: the one encoder never
// recomputes a recorded next_index_id downward. A catalog written by a build
// that dropped an index records a next id above the highest current one, and
// handing that id out again would key a new index's entries onto the dropped
// one's (BUG-0128).
func TestCatalogEncoderKeepsARecordedNextIndexID(t *testing.T) {
	recorded := int32(5)
	entry := factorTestEntry(0)
	entry.Indices = []catalogIndex{{ID: 0, Name: "a"}, {ID: 1, Name: "b"}}
	entry.NextIndexID = &recorded

	encoded, err := encodeCatalogEntry(entry)
	if err != nil {
		t.Fatalf("encodeCatalogEntry: %v", err)
	}
	next, ok := bson.Raw(encoded).Lookup("next_index_id").Int32OK()
	if !ok || next != 5 {
		t.Fatalf("the encoder wrote next_index_id %d (present %v), want the recorded 5: %v", next, ok, bson.Raw(encoded))
	}

	var loaded catalogEntry
	if err := bson.Unmarshal(encoded, &loaded); err != nil {
		t.Fatalf("decoding the encoded entry: %v", err)
	}
	if loaded.NextIndexID == nil || *loaded.NextIndexID != 5 {
		t.Fatalf("the entry re-decoded with next_index_id %v, want 5", loaded.NextIndexID)
	}
}

// TestCatalogEncoderRoundTripsAnUnreadableIndex: an index element this binary
// cannot read is held and written back byte for byte by the one encoder, so a
// rollback after an upgrade does not lose an index the newer binary declared
// (BUG-0128, spec §4 F-11).
func TestCatalogEncoderRoundTripsAnUnreadableIndex(t *testing.T) {
	element, err := bson.Marshal(bson.D{{Key: "id", Value: int32(0)}, {Key: "name", Value: "by_future"}, {Key: "future_key", Value: int32(1)}})
	if err != nil {
		t.Fatalf("encoding the unreadable index element: %v", err)
	}
	entry := factorTestEntry(0)
	entry.Indices = []catalogIndex{{recorded: element}}

	encoded, err := encodeCatalogEntry(entry)
	if err != nil {
		t.Fatalf("encodeCatalogEntry: %v", err)
	}
	var loaded catalogEntry
	if err := bson.Unmarshal(encoded, &loaded); err != nil {
		t.Fatalf("decoding the encoded entry: %v", err)
	}
	if len(loaded.Indices) != 1 || loaded.Indices[0].recorded == nil {
		t.Fatalf("the unreadable index did not round-trip as recorded: %+v", loaded.Indices)
	}
	if !bytes.Equal(loaded.Indices[0].recorded, element) {
		t.Fatalf("the unreadable index changed:\n written %v\n now     %v", bson.Raw(element), bson.Raw(loaded.Indices[0].recorded))
	}
}

// TestEveryCatalogEntryWriterKeepsTheFactor is T-28's third half: the factor
// survives each thing that writes a catalog entry. Two of them build the entry
// from a literal (create here, materialise in step 1.7) and would drop the
// field without an error, because a dropped factor is a legal "inherit". The
// other two read the entry, change one field and write it back.
func TestEveryCatalogEntryWriterKeepsTheFactor(t *testing.T) {
	writers := []struct {
		name  string
		write func(t *testing.T, s *Server)
	}{
		{"create", func(t *testing.T, s *Server) {}},
		{"index rebuild through persistIndexSpecs", func(t *testing.T, s *Server) {
			bucket := servedBucket(t, s, "app", "events")
			bucket.rec.mu.Lock()
			defer bucket.rec.mu.Unlock()
			if err := s.persistIndexSpecs(bucket, "app", "events", bucket.declaredIndexes()); err != nil {
				t.Fatalf("persistIndexSpecs: %v", err)
			}
		}},
		{"store-field declare", func(t *testing.T, s *Server) {
			resp, err := s.DeclareStoreFieldsCtx(context.Background(), declareReq("app", "events", StoreDeclaration{Name: "Notes", Engine: "hashStore"}))
			if err != nil || !resp.Success {
				t.Fatalf("DeclareStoreFieldsCtx = %+v, %v", resp, err)
			}
		}},
	}
	for _, writer := range writers {
		t.Run(writer.name, func(t *testing.T) {
			s := newTestServer(t)
			if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
				t.Fatalf("CreateDatabase = %+v, %v", resp, err)
			}
			created, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "events", Type: BucketTypeHash, ReplicationFactor: 3})
			if err != nil || !created.Success {
				t.Fatalf("CreateBucket = %+v, %v", created, err)
			}
			writer.write(t, s)

			entry, catalogued, err := s.readCatalogEntry("app", "events")
			if err != nil || !catalogued {
				t.Fatalf("readCatalogEntry = %+v, %v, %v", entry, catalogued, err)
			}
			if entry.ReplicationFactor != 3 {
				t.Fatalf("after %s the catalog entry carries factor %d, want 3", writer.name, entry.ReplicationFactor)
			}
		})
	}
}

// TestCreateBucketRefusesANegativeReplicationFactor: a negative count of
// copies is malformed, not policy, so it is refused at every depth — unlike
// "more copies than members", which a replay must still accept.
func TestCreateBucketRefusesANegativeReplicationFactor(t *testing.T) {
	s := newTestServer(t)
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "events", Type: BucketTypeHash, ReplicationFactor: -1})
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if resp.Success {
		t.Fatalf("a replication factor of -1 was accepted: %+v", resp)
	}
	if !strings.Contains(resp.Message, "replication_factor: 3") {
		t.Fatalf("the refusal does not name the working form: %q", resp.Message)
	}
	if _, catalogued, _ := s.readCatalogEntry("app", "events"); catalogued {
		t.Fatal("a refused create left a catalog entry")
	}
}

// TestAttachedBucketTakesItsFactorFromTheCatalogEntry: a restart re-attaches
// the bucket from its entry, and the bucket's own factor must come from there.
func TestAttachedBucketTakesItsFactorFromTheCatalogEntry(t *testing.T) {
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	if resp, err := first.core.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	for name, factor := range map[string]int{"declared": 3, "inherits": 0} {
		if resp, err := first.core.CreateBucket(CreateBucketRequest{Database: "app", Name: name, Type: BucketTypeHash, ReplicationFactor: factor}); err != nil || !resp.Success {
			t.Fatalf("CreateBucket(%s) = %+v, %v", name, resp, err)
		}
	}
	if got := servedBucket(t, first.core, "app", "declared").rec.replicationFactor.Load(); got != 3 {
		t.Fatalf("a bucket created at 3 holds %d", got)
	}

	second := restartCatalog(t, first, dataDir)
	if got := servedBucket(t, second.core, "app", "declared").rec.replicationFactor.Load(); got != 3 {
		t.Fatalf("after a restart the bucket declared at 3 holds %d", got)
	}
	if got := servedBucket(t, second.core, "app", "inherits").rec.replicationFactor.Load(); got != 0 {
		t.Fatalf("after a restart the bucket that declared nothing holds %d, want 0", got)
	}
}

// TestCreateOfAnExistingBucketThatNamesAnotherFactorSaysWhatItKept is review
// F-6. A create of a name that exists stays a success — a replay and a
// redelivery take this path — but a create that names a factor different from
// the bucket's must not read as if it had applied it (rule 10: never change or
// ignore a user's setting silently).
func TestCreateOfAnExistingBucketThatNamesAnotherFactorSaysWhatItKept(t *testing.T) {
	s := newTestServer(t)
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "events", Type: BucketTypeHash, ReplicationFactor: 3}); err != nil || !resp.Success {
		t.Fatalf("CreateBucket = %+v, %v", resp, err)
	}

	again, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "events", Type: BucketTypeHash, ReplicationFactor: 2})
	if err != nil || !again.Success {
		t.Fatalf("a repeated create must stay a success: %+v, %v", again, err)
	}
	for _, want := range []string{"already exists", "replication_factor 3", "asked for 2", "not changed",
		"alter bucket events { replication_factor: 2 }"} {
		if !strings.Contains(again.Message, want) {
			t.Fatalf("the message does not say %q: %s", want, again.Message)
		}
	}
	if strings.Contains(again.Message, "dump the bucket and restore") {
		t.Fatalf("the message still advises a dump and restore, which the alter replaced: %s", again.Message)
	}
	if entry, _, err := s.readCatalogEntry("app", "events"); err != nil || entry.ReplicationFactor != 3 {
		t.Fatalf("the repeated create changed the stored factor to %d (%v)", entry.ReplicationFactor, err)
	}

	// The same factor, or none, adds nothing to the ordinary message.
	for _, factor := range []int{0, 3} {
		same, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "events", Type: BucketTypeHash, ReplicationFactor: factor})
		if err != nil || !same.Success || strings.Contains(same.Message, "asked for") {
			t.Fatalf("a repeated create at factor %d = %+v, %v; want the ordinary message", factor, same, err)
		}
	}
}

// TestMaterialiseWritesTheFactorIntoTheCatalogEntry is the materialise row of
// T-28. materializeRange builds the pulled bucket's entry from a literal, the
// second of the two sites that would drop the factor without an error.
func TestMaterialiseWritesTheFactorIntoTheCatalogEntry(t *testing.T) {
	s := newTestServer(t)
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	pulled := migratedBucket{database: "app", name: "events", storeType: BucketTypeHash, replicationFactor: 3}
	if err := s.materializeRange(pulled, nil, 0); err != nil {
		t.Fatalf("materializeRange: %v", err)
	}
	entry, catalogued, err := s.readCatalogEntry("app", "events")
	if err != nil || !catalogued || entry.ReplicationFactor != 3 {
		t.Fatalf("the materialised bucket's entry is %+v (catalogued %v, %v), want factor 3", entry, catalogued, err)
	}
	if got := servedBucket(t, s, "app", "events").rec.replicationFactor.Load(); got != 3 {
		t.Fatalf("the materialised bucket holds factor %d, want 3", got)
	}
}
