package server

import (
	"bytes"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// nameOfBytes returns a name of exactly total bytes that starts with prefix.
func nameOfBytes(prefix string, total int) string {
	return prefix + strings.Repeat("n", total-len(prefix))
}

// TestCreateBucketRefusesANamePairOverTheLimit proves a database or bucket
// name over 255 bytes is refused before anything is written, with a message
// that names the limit and the length without echoing the whole name (catalog
// GC build CG-2; owner ruling 41.3).
func TestCreateBucketRefusesANamePairOverTheLimit(t *testing.T) {
	for _, tt := range []struct {
		name         string
		database     string
		bucket       string
		wantContains []string
	}{
		{"bucket one byte over", "app", nameOfBytes("post-", 256), []string{"255", "256 bytes", "bucket name"}},
		{"bucket 300 bytes", "app", nameOfBytes("post-", 300), []string{"255", "300 bytes", "bucket name"}},
		{"bucket 64,000 bytes", "app", nameOfBytes("post-", 64000), []string{"255", "64,000 bytes", "bucket name"}},
		{"database one byte over", nameOfBytes("db-", 256), "users", []string{"255", "256 bytes", "database name"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestServer(t)
			resp, err := s.CreateBucket(CreateBucketRequest{Database: tt.database, Name: tt.bucket, Type: BucketTypeHash})
			if err != nil {
				t.Fatalf("CreateBucket: %v", err)
			}
			if resp.Success {
				t.Fatal("a create over the name limit was accepted")
			}
			if len(resp.Message) >= 1024 {
				t.Errorf("the refusal is %d bytes; it must stay under 1 KiB however long the names are", len(resp.Message))
			}
			for _, want := range tt.wantContains {
				if !strings.Contains(resp.Message, want) {
					t.Errorf("the refusal does not contain %q: %s", want, resp.Message)
				}
			}
			if _, catalogued, err := s.readCatalogEntry(tt.database, tt.bucket); err != nil || catalogued {
				t.Errorf("a refused create left a catalog entry (%v, %v)", catalogued, err)
			}
		})
	}
}

// TestCreateDatabaseRefusesANameOverTheLimit proves the database create
// itself refuses a name over the limit and accepts one at it.
func TestCreateDatabaseRefusesANameOverTheLimit(t *testing.T) {
	s := newTestServer(t)
	refused, err := s.CreateDatabase(nameOfBytes("db-", maxNameBytes+1))
	if err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if refused.Success || !strings.Contains(refused.Message, "255") || !strings.Contains(refused.Message, "256 bytes") {
		t.Errorf("a database name of 256 bytes was not refused with the limit named: %+v", refused)
	}
	accepted, err := s.CreateDatabase(nameOfBytes("db-", maxNameBytes))
	if err != nil || !accepted.Success {
		t.Errorf("a database name of 255 bytes was refused: %+v, %v", accepted, err)
	}
}

// TestOverLongNameLeavesNoGhost proves a name too long for a store key is
// refused before the commit: the commit used to fail in the store with a raw
// key dump and leave an empty struct behind.
func TestOverLongNameLeavesNoGhost(t *testing.T) {
	s := newTestServer(t)
	createDatabaseIn(t, s, "app")
	attachedBefore := s.bucketsAttached.Load()
	longBucket := nameOfBytes("post-", 65100)

	resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: longBucket, Type: BucketTypeHash})
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if resp.Success || !strings.Contains(resp.Message, "255") {
		t.Fatalf("the create was not refused with the limit named: success=%v, message of %d bytes starting %.120q", resp.Success, len(resp.Message), resp.Message)
	}
	if ghost := s.serverDataAccessor().getDB("app").getBucket(longBucket); ghost != nil {
		t.Error("a refused create left a bucket struct behind")
	}
	listing, err := s.ListBuckets(ListBucketsRequest{Database: "app"})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	if len(listing.Buckets) != 0 {
		t.Errorf("ListBuckets = %+v, want none", listing.Buckets)
	}
	if attached := s.bucketsAttached.Load(); attached != attachedBefore {
		t.Errorf("bucketsAttached = %d, want %d", attached, attachedBefore)
	}
}

// TestCreateBucketAcceptsAPairAtTheLimit is the guard for the other side: two
// names of exactly 255 bytes are created, served and still there after a
// restart. Break it by making the check refuse at the limit.
func TestCreateBucketAcceptsAPairAtTheLimit(t *testing.T) {
	dataDir := t.TempDir()
	server := startCatalogServerAt(t, dataDir)
	database, bucket := nameOfBytes("db-", maxNameBytes), nameOfBytes("bk-", maxNameBytes)

	// Whole: a spread bucket's name stops at 250 bytes (TestSpreadBucketNameCappedAt250Bytes).
	resp, err := server.core.CreateBucket(CreateBucketRequest{Database: database, Name: bucket, Type: BucketTypeHash,
		Options: map[string]interface{}{"placement": placementWhole}})
	if err != nil || !resp.Success {
		t.Fatalf("a create at the limit: %+v, %v", resp, err)
	}
	if got := viewOf(t, server.core, database, bucket); !got.catalogued || !got.open || !got.listed {
		t.Fatalf("a create at the limit is not served: %+v", got)
	}
	assertSameAfterRestart(t, server, dataDir, database, bucket)
}

// TestEveryNameBearingKeyFitsAtTheNameLimit builds every key that carries a
// database or bucket name at the longest allowed pair and asserts the store
// would take it. The store refuses a key over 65,000 bytes
// (third_party/badger/txn.go:Txn.modify); the replication keys are longer
// than the catalog key, so they set the bound.
func TestEveryNameBearingKeyFitsAtTheNameLimit(t *testing.T) {
	const storeKeyLimit = 65000
	database, bucket := nameOfBytes("db-", maxNameBytes), nameOfBytes("bk-", maxNameBytes)
	source := strings.Repeat("s", 512)

	keys := map[string][]byte{
		"catalogKey":       catalogKey(database, bucket),
		"placementMarkKey": placementMarkKey(placementOf(database, bucket)),
		"seededKey":        seededKey(source, database, bucket),
		"legacySeededKey":  legacySeededKey(source, bucket),
		"tombstoneKey":     tombstoneKey(database, 1),
		"databaseDropKey":  databaseDropKey(database),
	}
	for name, key := range keys {
		if len(key) > storeKeyLimit {
			t.Errorf("%s is %d bytes at the name limit; the store refuses over %d", name, len(key), storeKeyLimit)
		}
	}
}

// TestBadgerKeyLimitIsWhatTheNameLimitAssumes guards the 65,000-byte figure
// the name limit's headroom is derived from: against the real node store, a
// key of exactly that size commits and one byte more does not. It fails if the
// patched Badger in third_party/badger changes the limit.
func TestBadgerKeyLimitIsWhatTheNameLimitAssumes(t *testing.T) {
	s := newTestServer(t)
	commit := func(keyBytes int) error {
		key := storage.NodeKey(storage.NSReplication, bytes.Repeat([]byte{'z'}, keyBytes-11))
		batch := s.kv.NewBatch()
		defer batch.Discard()
		if err := batch.Set(key, []byte("v")); err != nil {
			return err
		}
		return batch.Commit(false)
	}
	if err := commit(65000); err != nil {
		t.Fatalf("a key of 65,000 bytes was refused: %v", err)
	}
	if err := commit(65001); err == nil {
		t.Fatal("a key of 65,001 bytes was accepted; the name limit's derivation is out of date")
	}
	cleanup := s.kv.NewBatch()
	if err := cleanup.Delete(storage.NodeKey(storage.NSReplication, bytes.Repeat([]byte{'z'}, 65000-11))); err != nil {
		t.Fatalf("deleting the probe key: %v", err)
	}
	if err := cleanup.Commit(false); err != nil {
		t.Fatalf("committing the probe key's delete: %v", err)
	}
}

// TestClusteredCreateOverTheNameLimitIsNeitherLoggedNorShipped proves the
// refusal is the owner's, before the append: a create the owner logged and
// shipped would be applied as a failure on every member (build doc D-3).
func TestClusteredCreateOverTheNameLimitIsNeitherLoggedNorShipped(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForFeatureConvergence(t, tc)
	long := bucketOwnedBy(t, tc, 0, strings.Repeat("n", 297))
	if len(long) != 300 {
		t.Fatalf("the fixture name is %d bytes, want 300", len(long))
	}
	loggedBefore := tc.nodes[0].server.core.wal.LastSequence()

	refusal := createRefusal(t, tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase, Name: long, Type: BucketTypeHash,
	}))
	if refusal == "" {
		t.Fatal("a create over the name limit was accepted")
	}
	for _, want := range []string{"255", "300 bytes"} {
		if !strings.Contains(refusal, want) {
			t.Errorf("the refusal does not contain %q: %s", want, refusal)
		}
	}
	if logged := tc.nodes[0].server.core.wal.LastSequence(); logged != loggedBefore {
		t.Errorf("the owner's log moved from %d to %d for a refused create", loggedBefore, logged)
	}
	requireNoBucketAnywhere(t, tc, testDatabase, long)
	for index, node := range tc.nodes {
		if got := walCreates(t, node.server.core.wal, long); got != 0 {
			t.Errorf("node %d logged %d creates of a refused bucket", index+1, got)
		}
	}
}

// TestClusteredCreateDatabaseOverTheNameLimitIsNotLogged: a
// database create is judged before the append like a bucket create, not logged
// and shipped to be refused at apply on every member.
func TestClusteredCreateDatabaseOverTheNameLimitIsNotLogged(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	long := nameOfBytes("db-", 300)
	loggedBefore := tc.nodes[0].server.core.wal.LastSequence()

	reply := tc.frame(0, transport.OpCreateDB, CreateDatabaseRequest{Name: long})
	var refused CreateDatabaseResponse
	if err := bson.Unmarshal(reply.Payload, &refused); err != nil {
		t.Fatalf("decoding the create database answer (opcode %s): %v", reply.Opcode, err)
	}
	if refused.Success {
		t.Fatal("a database create over the name limit was accepted")
	}
	for _, want := range []string{"255", "300 bytes"} {
		if !strings.Contains(refused.Message, want) {
			t.Errorf("the refusal does not contain %q: %s", want, refused.Message)
		}
	}
	if logged := tc.nodes[0].server.core.wal.LastSequence(); logged != loggedBefore {
		t.Errorf("the node's log moved from %d to %d for a refused database create", loggedBefore, logged)
	}
}

// TestClusteredCreateDatabaseUnsafeNameIsNotLogged is BUG-0379: the pre-log
// check asks only the name length, so a reserved, path-bearing or NUL-bearing
// database name is appended and shipped to be refused at apply on every
// member. The full databaseNameRefusal runs before the append.
func TestClusteredCreateDatabaseUnsafeNameIsNotLogged(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	for _, tt := range []struct{ label, name string }{
		{"reserved", "_system"},
		{"slash", "a/b"},
		{"dotdot", ".."},
		{"nul", "nul\x00byte"},
		{"backslash", `x\y`},
	} {
		t.Run(tt.label, func(t *testing.T) {
			loggedBefore := tc.nodes[0].server.core.wal.LastSequence()
			refusal := createRefusalOf(t, tc.frame(0, transport.OpCreateDB, CreateDatabaseRequest{Name: tt.name}))
			if refusal == "" {
				t.Fatalf("a create database named %q was accepted", tt.name)
			}
			if logged := tc.nodes[0].server.core.wal.LastSequence(); logged != loggedBefore {
				t.Errorf("the node's log moved from %d to %d for a refused create database named %q", loggedBefore, logged, tt.name)
			}
		})
	}
}

// TestPulledBucketIsNotRefusedByTheNameLimit is the guard for build doc §6
// item 4: a bucket the owner holds is never refused by a replica pulling it,
// whatever its name length (an older owner could have created one). Break it
// by adding the check to claimPulledBucket.
func TestPulledBucketIsNotRefusedByTheNameLimit(t *testing.T) {
	s := newTestServer(t)
	name := nameOfBytes("old-", 64500)
	_, live, err := s.serverDataAccessor().claimPulledBucket("app", name, BucketTypeHash, 1, func() bool { return false })
	if err != nil || !live {
		t.Fatalf("a pulled bucket with a %d-byte name was refused: live %v, %v", len(name), live, err)
	}
}
