package server

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// The upgrade rebuild (GAP-14 build doc Step 4): an index declared before this
// build is rebuilt from the documents at attach under this build's rules, and
// search refuses it while it is building.

// seedPreBuildIndex leaves a data directory holding bucket app/users whose
// catalog records a pre-build index on Email ({field: "Email"}) and whose
// documents were indexed under the pre-build rule: count documents, every
// third without an Email. It returns the stopped server.
func seedPreBuildIndex(t *testing.T, dataDir string, count int) *catalogServer {
	t.Helper()
	cs := startCatalogServerAt(t, dataDir)
	s := cs.core
	if _, err := s.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "users", Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("CreateBucket = %+v, %v", resp, err)
	}
	users := servedBucket(t, s, "app", "users")
	preBuild := indexedView(t, s, users, indexOptions([]catalogIndex{{Field: "Email"}}))
	for i := 0; i < count; i++ {
		doc := map[string]interface{}{"n": i}
		if i%3 != 0 {
			doc["Email"] = fmt.Sprintf("u%d@example.com", i)
		}
		if err := preBuild.hashStore.Put(store.ApplyInfo{}, fmt.Sprintf("u%04d", i), doc, 0); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	writeCatalogIndices(t, s, "app", "users", []bson.M{{"field": "Email", "type": "equality"}})
	cs.stop(t)
	return cs
}

// wantRebuiltEmailIndex checks the Email index of app/users holds exactly one
// entry per document — a null entry for each document without an Email — and
// that the catalog records it as this build's.
func wantRebuiltEmailIndex(t *testing.T, s *Server, count int) {
	t.Helper()
	users := servedBucket(t, s, "app", "users")
	entries := indexEntries(t, s, users, 0)
	if len(entries) != count {
		t.Errorf("the rebuilt index holds %d entries for %d documents", len(entries), count)
	}
	nullPrefix := append(indexEntryPrefix(users, 0), 0x10)
	nulls := 0
	seen := make(map[string]bool, len(entries))
	for _, entry := range entries {
		if bytes.HasPrefix(entry, nullPrefix) {
			nulls++
		}
		seen[string(entry)] = true
	}
	if want := (count + 2) / 3; nulls != want {
		t.Errorf("the rebuilt index holds %d null entries, want %d (one per document without Email)", nulls, want)
	}
	stored := storedCatalogIndices(t, s, "app", "users")
	if len(stored) != 1 || stored[0]["version"] != int32(2) || stored[0]["state"] != nil {
		t.Errorf("after the rebuild the catalog records %v, want one ready version-2 index", stored)
	}
}

// TestPreBuildIndexIsRebuiltWithNullEntries (spec §10's
// TestV1IndexDirectoryIsRebuiltNotTranslated, recast: there are no v1
// directories, only descriptors that predate the null rule): a bucket whose
// catalog holds a version-0 index comes up with a null entry for each
// document lacking the field, and version 2.
func TestPreBuildIndexIsRebuiltWithNullEntries(t *testing.T) {
	dataDir := t.TempDir()
	first := seedPreBuildIndex(t, dataDir, 9)
	second := restartCatalog(t, first, dataDir)
	wantRebuiltEmailIndex(t, second.core, 9)
	// The null entries serve search({Email: null}), which answers what find
	// answers for it — find matches an explicit null, and a missing field
	// matches nothing — because search re-applies the filter (Step 12).
	byNull := map[string]interface{}{"Email": nil}
	keys, used := searchKeys(t, second.core, "users", byNull)
	if want := scanKeys(t, second.core, "users", byNull); used != "Email_1" || strings.Join(keys, ",") != strings.Join(want, ",") {
		t.Fatalf("search({Email: null}) after the rebuild, served by %q, = %v; find = %v", used, keys, want)
	}
}

// TestRebuildClearsTheIndexRangeFirst (spec §10's TestRebuildDropsStaleV1Entries,
// recast): an entry for a value no document carries is gone after the
// rebuild. Clearing first is what makes a restart mid-rebuild idempotent.
func TestRebuildClearsTheIndexRangeFirst(t *testing.T) {
	dataDir := t.TempDir()
	first := seedPreBuildIndex(t, dataDir, 6)
	// Open the store without attaching anything, plant the entry, and stop.
	planting := startCatalogServerAt(t, dataDir)
	var users *inMemoryBucket
	for _, entry := range mustReadCatalog(t, planting.core) {
		if entry.bucket == "users" {
			users = &inMemoryBucket{slot: uint16(entry.Slot), databaseID: storage.LegacyDatabaseID(entry.database)}
			users.id.Store(uint64(entry.ID))
		}
	}
	stale, err := store.EncodeComponent("nobody@example.com")
	if err != nil {
		t.Fatalf("EncodeComponent: %v", err)
	}
	staleKey := append(append(indexEntryPrefix(users, 0), stale...), "u9999"...)
	batch := planting.core.kv.NewBatch()
	if err := batch.Set(staleKey, []byte{}); err != nil {
		t.Fatalf("planting a stale entry: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing the stale entry: %v", err)
	}
	batch.Discard()
	_ = first

	second := restartCatalog(t, planting, dataDir)
	for _, entry := range indexEntries(t, second.core, servedBucket(t, second.core, "app", "users"), 0) {
		if bytes.HasSuffix(entry, []byte("u9999")) {
			t.Fatalf("the stale entry %x survived the rebuild", entry)
		}
	}
	wantRebuiltEmailIndex(t, second.core, 6)
}

// setRebuildHook installs a rebuild hook for the test and returns it.
func setRebuildHook(t *testing.T, hook func(spec store.IndexSpec, batch int) error) {
	t.Helper()
	indexRebuildHook.Store(&hook)
	t.Cleanup(func() { indexRebuildHook.Store(nil) })
}

// smallRebuildBatches makes a rebuild commit every n entries, so a test can
// stop one between batches.
func smallRebuildBatches(t *testing.T, n int) {
	t.Helper()
	previous := rebuildBatchEntries
	rebuildBatchEntries = n
	t.Cleanup(func() { rebuildBatchEntries = previous })
}

// TestSearchRefusedWhileBuilding (spec §10): while a rebuild is held after its
// first batch, search on the index is refused naming the state and the way
// out; find and count answer, because they use no index.
func TestSearchRefusedWhileBuilding(t *testing.T) {
	dataDir := t.TempDir()
	first := seedPreBuildIndex(t, dataDir, 30)
	smallRebuildBatches(t, 5)
	held, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	setRebuildHook(t, func(store.IndexSpec, int) error {
		once.Do(func() {
			close(held)
			<-release
		})
		return nil
	})
	first.stop(t)
	reopened := startCatalogServerAt(t, dataDir)
	loaded := make(chan error, 1)
	go func() { loaded <- reopened.qs.LoadCatalog() }()
	select {
	case <-held:
	case <-time.After(10 * time.Second):
		t.Fatal("the rebuild never reached its first batch")
	}

	s := reopened.core
	hits, err := s.Search(SearchRequest{Database: "app", Bucket: "users", Filter: map[string]interface{}{"Email": "u1@example.com"}})
	switch {
	case err != nil:
		t.Errorf("search while building returned an error instead of a refusal: %v", err)
	case len(hits.Results) != 0 || !strings.Contains(hits.Message, "building") || !strings.Contains(hits.Message, "find"):
		t.Errorf("search while building answered %+v, want a refusal naming the building state and find", hits)
	}
	if found, err := s.Find(FindRequest{Database: "app", Bucket: "users", Key: "u0001"}); err != nil || !found.Found {
		t.Errorf("find while building = %+v, %v; want the document", found, err)
	}
	if counted, err := s.CountCtx(t.Context(), CountRequest{Database: "app", Bucket: "users"}); err != nil || counted.Count != 30 {
		t.Errorf("count while building = %+v, %v; want 30", counted, err)
	}
	close(release)
	if err := <-loaded; err != nil {
		t.Fatalf("LoadCatalog: %v", err)
	}
	hits, err = s.Search(SearchRequest{Database: "app", Bucket: "users", Filter: map[string]interface{}{"Email": "u1@example.com"}})
	if err != nil || hits.Total != 1 {
		t.Fatalf("search after the rebuild = %+v, %v; want one hit", hits, err)
	}
}

// TestCrashMidRebuildRestartsCleanly (spec §10): a rebuild that fails after
// its second batch leaves the index building and version 0 on disk; the next
// start rebuilds it whole — complete, version 2, no duplicate and no stale
// entry.
func TestCrashMidRebuildRestartsCleanly(t *testing.T) {
	dataDir := t.TempDir()
	first := seedPreBuildIndex(t, dataDir, 30)
	smallRebuildBatches(t, 5)
	setRebuildHook(t, func(_ store.IndexSpec, batch int) error {
		if batch == 2 {
			return errors.New("injected crash after the second batch")
		}
		return nil
	})
	crashed := restartCatalog(t, first, dataDir)
	stored := storedCatalogIndices(t, crashed.core, "app", "users")
	if len(stored) != 1 || stored[0]["state"] != "building" || stored[0]["version"] != nil {
		t.Fatalf("after a failed rebuild the catalog records %v, want the index building at version 0", stored)
	}
	if entries := indexEntries(t, crashed.core, servedBucket(t, crashed.core, "app", "users"), 0); len(entries) >= 30 {
		t.Fatalf("precondition: the failed rebuild wrote %d entries; the injection did not stop it part-way", len(entries))
	}
	indexRebuildHook.Store(nil)
	second := restartCatalog(t, crashed, dataDir)
	wantRebuiltEmailIndex(t, second.core, 30)
}

// TestRebuildLeavesTheStateDocumentAlone (a pin): index entries are not
// counted in the bucket's state, so the rebuild writes no state document and
// no WAL entry — every replica runs the same local rebuild.
func TestRebuildLeavesTheStateDocumentAlone(t *testing.T) {
	dataDir := t.TempDir()
	first := seedPreBuildIndex(t, dataDir, 9)
	reopened := startCatalogServerAt(t, dataDir)
	stateBytes := func(s *Server) []byte {
		t.Helper()
		var slot uint16
		var databaseID, id uint64
		for _, entry := range mustReadCatalog(t, s) {
			if entry.bucket == "users" {
				slot, databaseID, id = uint16(entry.Slot), storage.LegacyDatabaseID(entry.database), uint64(entry.ID)
			}
		}
		snap := s.kv.NewSnapshot()
		defer snap.Discard()
		encoded, err := snap.Get(storage.Key(slot, databaseID, id, storage.NSState))
		if err != nil {
			t.Fatalf("reading the state document: %v", err)
		}
		return append([]byte(nil), encoded...)
	}
	before := stateBytes(reopened.core)
	_ = first
	second := restartCatalog(t, reopened, dataDir)
	wantRebuiltEmailIndex(t, second.core, 9)
	if after := stateBytes(second.core); !bytes.Equal(before, after) {
		t.Fatalf("the rebuild changed the state document:\n  before %v\n  after  %v", bson.Raw(before), bson.Raw(after))
	}
}

func mustReadCatalog(t *testing.T, s *Server) []catalogEntry {
	t.Helper()
	_, entries, err := s.readCatalog(nil)
	if err != nil {
		t.Fatalf("readCatalog: %v", err)
	}
	return entries
}

// seedConflictedUnique leaves a data directory holding app/users with two
// documents sharing one Email, "dup-a" and "dup-b", a third with its own, and a
// catalog record declaring a version-0 unique index on Email plus a
// version-2 index on Name. unique could not be declared before this build, so
// the version-0 unique descriptor is test-only: it is how a duplicate that
// predates enforcement reaches a rebuild.
func seedConflictedUnique(t *testing.T, dataDir string) *catalogServer {
	t.Helper()
	cs := startCatalogServerAt(t, dataDir)
	s := cs.core
	if _, err := s.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "users", Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("CreateBucket = %+v, %v", resp, err)
	}
	users := servedBucket(t, s, "app", "users")
	plain := indexedView(t, s, users, map[string]interface{}{"indices": []interface{}{"Email", "Name"}})
	for key, doc := range map[string]map[string]interface{}{
		"dup-a":  {"Email": "same@example.com", "Name": "a"},
		"dup-b":  {"Email": "same@example.com", "Name": "b"},
		"single": {"Email": "own@example.com", "Name": "c"},
	} {
		doc["_id_"] = key
		if err := plain.hashStore.Put(store.ApplyInfo{}, key, doc, 0); err != nil {
			t.Fatalf("Put(%s): %v", key, err)
		}
	}
	writeCatalogIndices(t, s, "app", "users", []bson.M{
		{"id": int32(0), "name": "Email_1", "fields": bson.A{"Email"}, "unique": true},
		{"id": int32(1), "name": "Name_1", "fields": bson.A{"Name"}, "version": int32(2)},
	})
	cs.stop(t)
	return cs
}

// TestRebuildRefusesToMarkReadyOnDuplicate (spec §10; guard 5): a unique
// index rebuilt over documents that already hold one value twice is marked
// conflicted — persisted, so a restart does not forget it — and search refuses
// it naming both documents; find and count answer, and nothing is deleted or
// chosen for the operator.
func TestRebuildRefusesToMarkReadyOnDuplicate(t *testing.T) {
	dataDir := t.TempDir()
	first := seedConflictedUnique(t, dataDir)
	for _, stage := range []string{"after the rebuild", "after a second restart"} {
		next := restartCatalog(t, first, dataDir)
		first = next
		s := next.core
		stored := storedCatalogIndices(t, s, "app", "users")
		if len(stored) != 2 || stored[0]["state"] != "conflicted" {
			t.Fatalf("%s the catalog records %v, want the unique index conflicted", stage, stored)
		}
		hits, err := s.Search(SearchRequest{Database: "app", Bucket: "users", Filter: map[string]interface{}{"Email": "own@example.com"}})
		if err != nil {
			t.Fatalf("%s: search: %v", stage, err)
		}
		if len(hits.Results) != 0 || !strings.Contains(hits.Message, "conflicted") ||
			!strings.Contains(hits.Message, `"dup-a"`) || !strings.Contains(hits.Message, `"dup-b"`) {
			t.Fatalf("%s search answered %+v, want a refusal naming the conflicted state and both documents", stage, hits)
		}
		scanned, err := s.Scan(ScanRequest{Database: "app", Bucket: "users", Filter: map[string]interface{}{"Email": "same@example.com"}})
		if err != nil || scanned.Total != 2 {
			t.Fatalf("%s find = %+v, %v; want both holders", stage, scanned, err)
		}
		if counted, err := s.CountCtx(t.Context(), CountRequest{Database: "app", Bucket: "users"}); err != nil || counted.Count != 3 {
			t.Fatalf("%s count = %+v, %v; want all 3 documents", stage, counted, err)
		}
	}
}

// TestConflictedIndexDoesNotTakeTheBucketOffline (spec §10): another index of
// the bucket still answers search.
func TestConflictedIndexDoesNotTakeTheBucketOffline(t *testing.T) {
	dataDir := t.TempDir()
	first := seedConflictedUnique(t, dataDir)
	s := restartCatalog(t, first, dataDir).core
	hits, err := s.Search(SearchRequest{Database: "app", Bucket: "users", Filter: map[string]interface{}{"Name": "c"}})
	if err != nil || hits.Total != 1 {
		t.Fatalf("search on the bucket's other index = %+v, %v; want its one hit", hits, err)
	}
}

// TestConflictedIndexClearsAfterTheDataIsFixed: delete one holder and restart
// — the conflicted index is rebuilt at attach and comes back ready.
func TestConflictedIndexClearsAfterTheDataIsFixed(t *testing.T) {
	dataDir := t.TempDir()
	first := seedConflictedUnique(t, dataDir)
	conflicted := restartCatalog(t, first, dataDir)
	if resp, err := conflicted.core.Delete(DeleteRequest{Database: "app", Bucket: "users", Key: "dup-b"}); err != nil || !resp.Success {
		t.Fatalf("deleting one holder = %+v, %v", resp, err)
	}
	fixed := restartCatalog(t, conflicted, dataDir)
	stored := storedCatalogIndices(t, fixed.core, "app", "users")
	if len(stored) != 2 || stored[0]["state"] != nil || stored[0]["version"] != int32(2) {
		t.Fatalf("after the fix and a restart the catalog records %v, want the unique index ready at version 2", stored)
	}
	hits, err := fixed.core.Search(SearchRequest{Database: "app", Bucket: "users", Filter: map[string]interface{}{"Email": "same@example.com"}})
	if err != nil || hits.Total != 1 {
		t.Fatalf("search after the fix = %+v, %v; want the remaining holder", hits, err)
	}
}
