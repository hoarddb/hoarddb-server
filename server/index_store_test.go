package server

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// Step 9 of the shared-storage build: indexes live in the node's store, in the
// document's batch (spec §3.6; T-11, T-24).

// batchRecordingKV is a node store that records the keys of every batch it
// is asked to commit, and can fail the next commit as the store failing.
type batchRecordingKV struct {
	storage.KV
	mu       sync.Mutex
	failNext bool
	lastKeys [][]byte
}

func (k *batchRecordingKV) NewBatch() storage.KVBatch {
	return &recordingBatch{KVBatch: k.KV.NewBatch(), kv: k}
}

type recordingBatch struct {
	storage.KVBatch
	kv   *batchRecordingKV
	keys [][]byte
}

func (b *recordingBatch) Set(key, value []byte) error {
	b.keys = append(b.keys, append([]byte(nil), key...))
	return b.KVBatch.Set(key, value)
}

func (b *recordingBatch) Delete(key []byte) error {
	b.keys = append(b.keys, append([]byte(nil), key...))
	return b.KVBatch.Delete(key)
}

func (b *recordingBatch) Commit(sync bool) error {
	b.kv.mu.Lock()
	fail := b.kv.failNext
	b.kv.failNext = false
	b.kv.lastKeys = b.keys
	b.kv.mu.Unlock()
	if fail {
		b.Discard()
		return errors.New("injected commit failure")
	}
	return b.KVBatch.Commit(sync)
}

// lastBatchHolds reports which of the namespaces of bucket (slot, databaseID, id) the last
// batch wrote into.
func (k *batchRecordingKV) lastBatchHolds(slot uint16, databaseID, id uint64, namespaces ...byte) map[byte]bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	held := make(map[byte]bool)
	for _, ns := range namespaces {
		prefix := storage.Key(slot, databaseID, id, ns)
		for _, key := range k.lastKeys {
			if bytes.HasPrefix(key, prefix) {
				held[ns] = true
			}
		}
	}
	return held
}

// serverOnStore is a standalone server over the given node store.
func serverOnStore(t *testing.T, kv storage.KV) *Server {
	t.Helper()
	return &Server{
		config:      &config.Config{Storage: config.StorageConfig{DataDir: t.TempDir()}, Encryption: config.EncryptionConfig{Mode: "off", Keystore: "off"}},
		logger:      discardLogger(),
		dataStore:   &serverData{dbs: make(map[string]*inMemoryDB)},
		kv:          kv,
		authReqSeen: make(map[string]time.Time),
		knownNodes:  make(map[string]*NodeInfo),
	}
}

func createIndexedHashBucket(t *testing.T, s *Server, database, bucket, field string) {
	t.Helper()
	resp, err := s.createBucketLocked(context.Background(), CreateBucketRequest{
		Database: database, Name: bucket, Type: BucketTypeHash,
		Options: map[string]interface{}{"indices": []string{field}},
	})
	if err != nil || !resp.Success {
		t.Fatalf("create %s/%s: %+v, %v", database, bucket, resp, err)
	}
}

// TestIndexedInsertIsOneTransaction is T-11: a document, its read-repair
// version and its index entries are one batch. A commit that fails leaves none
// of the three readable; one that succeeds leaves all three.
func TestIndexedInsertIsOneTransaction(t *testing.T) {
	const database, bucket = "app", "users"
	kv := &batchRecordingKV{KV: kvfault.New()}
	s := serverOnStore(t, kv)
	createIndexedHashBucket(t, s, database, bucket, "Email")
	b := lookupBucket(s, database, bucket)
	versioned := withWriteVersion(context.Background(), 7)
	insert := InsertRequest{Database: database, Bucket: bucket, Key: "u1", Data: map[string]interface{}{"Email": "dalek@example.com"}}

	kv.mu.Lock()
	kv.failNext = true
	kv.mu.Unlock()
	if resp, err := s.InsertCtx(versioned, insert); err == nil && resp.Success {
		t.Fatal("the insert succeeded although its commit failed")
	}
	held := kv.lastBatchHolds(b.slot, b.databaseID, b.id.Load(), storage.NSDocuments, storage.NSVersions, storage.NSIndex)
	if !held[storage.NSDocuments] || !held[storage.NSVersions] || !held[storage.NSIndex] {
		t.Fatalf("the failed batch held documents=%v versions=%v index=%v: the three must be one transaction",
			held[storage.NSDocuments], held[storage.NSVersions], held[storage.NSIndex])
	}
	assertIndexedReads(t, s, database, bucket, false)

	if resp, err := s.InsertCtx(versioned, insert); err != nil || !resp.Success {
		t.Fatalf("insert: %+v, %v", resp, err)
	}
	assertIndexedReads(t, s, database, bucket, true)
}

// assertIndexedReads checks the document u1, its version and its index entry
// are all readable, or none is.
func assertIndexedReads(t *testing.T, s *Server, database, bucket string, want bool) {
	t.Helper()
	found, err := s.Find(FindRequest{Database: database, Bucket: bucket, Key: "u1"})
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	version, err := lookupBucket(s, database, bucket).hashStore.GetVersion("u1")
	if err != nil {
		t.Fatalf("version: %v", err)
	}
	search, err := s.Search(SearchRequest{Database: database, Bucket: bucket, Filter: map[string]interface{}{"Email": "dalek@example.com"}})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	indexed := search.Total == 1
	if found.Found != want || (version == 7) != want || indexed != want {
		t.Fatalf("document readable %v, version %d, index entry readable %v: want all readable = %v",
			found.Found, version, indexed, want)
	}
}

// TestSameFieldIndexesDoNotCollide is T-24: two buckets indexing the same
// field name share one store, and a search in one never returns the other's
// documents — before and after a restart. Both buckets hold a document under
// the same key, so an index entry that crossed buckets would resolve to a real
// document of the searched bucket rather than to nothing.
func TestSameFieldIndexesDoNotCollide(t *testing.T) {
	const database = "app"
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	for _, bucket := range []string{"left", "right"} {
		createDB(t, first.core, database)
		resp, err := first.core.CreateBucket(CreateBucketRequest{
			Database: database, Name: bucket, Type: BucketTypeHash,
			Options: map[string]interface{}{"indices": []string{"email"}},
		})
		if err != nil || !resp.Success {
			t.Fatalf("create %s: %+v, %v", bucket, resp, err)
		}
		if resp, err := first.core.Insert(InsertRequest{
			Database: database, Bucket: bucket, Key: "doc",
			Data: map[string]interface{}{"email": bucket + "@example.com"},
		}); err != nil || !resp.Success {
			t.Fatalf("insert into %s: %+v, %v", bucket, resp, err)
		}
	}
	assertSearchStaysInItsBucket(t, first.core, database)
	second := restartCatalog(t, first, dataDir)
	assertSearchStaysInItsBucket(t, second.core, database)
}

func assertSearchStaysInItsBucket(t *testing.T, s *Server, database string) {
	t.Helper()
	for bucket, other := range map[string]string{"left": "right", "right": "left"} {
		own, err := s.Search(SearchRequest{Database: database, Bucket: bucket, Filter: map[string]interface{}{"email": bucket + "@example.com"}})
		if err != nil {
			t.Fatalf("search %s: %v", bucket, err)
		}
		if own.Total != 1 {
			t.Fatalf("a search of %s for its own document's value returned %d documents, want 1", bucket, own.Total)
		}
		crossed, err := s.Search(SearchRequest{Database: database, Bucket: bucket, Filter: map[string]interface{}{"email": other + "@example.com"}})
		if err != nil {
			t.Fatalf("search %s: %v", bucket, err)
		}
		if crossed.Total != 0 {
			t.Fatalf("a search of %s for a value only %s holds returned %d documents (%v): the index crossed buckets",
				bucket, other, crossed.Total, crossed.Results)
		}
	}
}

// createDB creates database if it does not exist yet.
func createDB(t *testing.T, s *Server, database string) {
	t.Helper()
	if s.serverDataAccessor().getDB(database) != nil {
		return
	}
	if resp, err := s.CreateDatabase(database); err != nil || !resp.Success {
		t.Fatalf("create database %s: %+v, %v", database, resp, err)
	}
}
