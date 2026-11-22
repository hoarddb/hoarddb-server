package server

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TTL on a cluster (GAP-14 build doc Step 19, spec §6.2.2–§6.2.3, §6.5).

// ttlCluster starts count nodes at rf with testDatabase/sessions declaring a
// one-hour TTL on CreatedAt, seeds expired documents and one live one at
// w=all, and returns the cluster and the owner's index. The bucket is whole,
// so its owner sweeps every document; ttlClusterPlaced takes the placement.
func ttlCluster(t *testing.T, count, rf, expired int, now time.Time) (*testCluster, int) {
	t.Helper()
	return ttlClusterPlaced(t, count, rf, expired, now, placementWhole)
}

// ttlClusterPlaced is ttlCluster with sessions created at placement. A spread
// bucket's documents are in its parts, each an ordinary bucket that carries
// the TTL index and is swept by its own owner.
func ttlClusterPlaced(t *testing.T, count, rf, expired int, now time.Time, placement string) (*testCluster, int) {
	t.Helper()
	tc := newTestCluster(t, count, rf)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "sessions", Type: BucketTypeHash,
		Options: map[string]interface{}{"placement": placement, "indices": []interface{}{ttlDeclaration("CreatedAt", 3600)}}})
	put := func(key string, at time.Time) {
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: "sessions", Key: key,
			Data: map[string]interface{}{"CreatedAt": primitive.NewDateTimeFromTime(at)}, WriteConcern: "all"})
	}
	for i := 0; i < expired; i++ {
		put(fmt.Sprintf("old-%02d", i), now.Add(-2*time.Hour))
	}
	put("live", now)
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, "sessions"))
	if err != nil {
		t.Fatalf("resolving the owner: %v", err)
	}
	return tc, nodeIndexByAddr(t, tc.addrs, owner.ID)
}

// sessionsOn is the keys node index's own copies of sessions hold, sorted:
// the bucket's, or for a spread bucket its parts' (the template holds only
// the router's born set, which is no session).
func sessionsOn(t *testing.T, tc *testCluster, index int) []string {
	t.Helper()
	s := tc.nodes[index].server.core
	buckets := []string{"sessions"}
	if s.spreadTemplateHeld(testDatabase, "sessions") {
		buckets = nil
		for p := 0; p < defaultSpreadParts; p++ {
			if s.bucketStoreOpen(testDatabase, partName("sessions", p)) {
				buckets = append(buckets, partName("sessions", p))
			}
		}
	}
	var keys []string
	for _, bucket := range buckets {
		scanned, err := s.Scan(ScanRequest{Database: testDatabase, Bucket: bucket, Limit: 1000})
		if err != nil {
			t.Fatalf("find on node %d: %v", index+1, err)
		}
		keys = append(keys, documentKeysOf(t, scanned.Documents)...)
	}
	sort.Strings(keys)
	return keys
}

// waitForSessions waits until every running node holds exactly want.
func waitForSessions(t *testing.T, tc *testCluster, nodes []int, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for _, index := range nodes {
		for strings.Join(sessionsOn(t, tc, index), ",") != want {
			if time.Now().After(deadline) {
				t.Fatalf("node %d holds %v, want [%s]", index+1, sessionsOn(t, tc, index), want)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

// deletesInWAL counts the OpDelete entries of sessions in a node's WAL after
// sequence from.
func deletesInWAL(t *testing.T, tc *testCluster, index int, from uint64) []bson.Raw {
	t.Helper()
	entries, err := tc.nodes[index].server.core.wal.ReadFrom(from + 1)
	if err != nil {
		t.Fatalf("reading node %d's WAL: %v", index+1, err)
	}
	var deletes []bson.Raw
	for _, entry := range entries {
		if transport.Opcode(entry.Op) == transport.OpDelete && entry.Bucket == "sessions" {
			deletes = append(deletes, bson.Raw(entry.Data))
		}
	}
	return deletes
}

// TestTTLDeleteIsWALRecordedAndReplicated (spec §10, VER-14): an expiry is a
// WAL entry on the owner, applied by each replica.
func TestTTLDeleteIsWALRecordedAndReplicated(t *testing.T) {
	now := time.Now()
	tc, owner := ttlCluster(t, 3, 3, 2, now)
	defer tc.stop()
	base := tc.nodes[owner].server.core.wal.LastSequence()
	tc.nodes[owner].server.core.sweepTTL(context.Background(), now)
	if deletes := deletesInWAL(t, tc, owner, base); len(deletes) != 2 {
		t.Fatalf("the owner's WAL holds %d expiries, want 2", len(deletes))
	}
	waitForSessions(t, tc, []int{0, 1, 2}, "live")
}

// TestReplicaDoesNotSweep (spec §10; guard 2): every node runs its sweeper,
// and the WAL records one delete per expiry — a replica that swept would have
// its delete forwarded to the owner and appended again, RF times per expiry.
func TestReplicaDoesNotSweep(t *testing.T) {
	now := time.Now()
	tc, owner := ttlCluster(t, 3, 3, 2, now)
	defer tc.stop()
	base := tc.nodes[owner].server.core.wal.LastSequence()
	for _, node := range tc.nodes {
		node.server.core.sweepTTL(context.Background(), now)
	}
	if deletes := deletesInWAL(t, tc, owner, base); len(deletes) != 2 {
		t.Fatalf("after every node swept, the owner's WAL holds %d expiries for 2 expired documents, want 2", len(deletes))
	}
}

// TestSweepUsesTheLowestWriteConcern (spec §6.2.2): the delete carries
// write_concern "1", so an expiry never waits for a replica.
func TestSweepUsesTheLowestWriteConcern(t *testing.T) {
	now := time.Now()
	tc, owner := ttlCluster(t, 3, 3, 1, now)
	defer tc.stop()
	base := tc.nodes[owner].server.core.wal.LastSequence()
	tc.nodes[owner].server.core.sweepTTL(context.Background(), now)
	deletes := deletesInWAL(t, tc, owner, base)
	if len(deletes) != 1 {
		t.Fatalf("the owner's WAL holds %d expiries, want 1", len(deletes))
	}
	if concern, _ := deletes[0].Lookup("write_concern").StringValueOK(); concern != "1" {
		t.Fatalf("the expiry's payload asks for write concern %q, want \"1\": %v", concern, deletes[0])
	}
	if _, err := deletes[0].LookupErr("expire_if"); err != nil {
		t.Fatalf("the expiry's payload carries no expiry condition: %v", deletes[0])
	}
}

// TestSweeperMakesProgressWithAnUnreachableReplica (spec §10): with a replica
// down, the owner deletes and the pass ends, rather than waiting out the write
// concern timeout once per document.
func TestSweeperMakesProgressWithAnUnreachableReplica(t *testing.T) {
	now := time.Now()
	tc, owner := ttlCluster(t, 3, 3, 3, now)
	defer tc.stop()
	down := (owner + 1) % len(tc.nodes)
	_ = tc.nodes[down].client.Close()
	tc.nodes[down].client = nil
	if err := tc.nodes[down].server.Stop(); err != nil {
		t.Fatalf("stopping node %d: %v", down+1, err)
	}
	tc.nodes[down].server = nil
	started := time.Now()
	pass := tc.nodes[owner].server.core.sweepTTL(context.Background(), now)
	if elapsed := time.Since(started); elapsed > DefaultWriteConcernTimeout {
		t.Fatalf("with one replica down the pass took %v (%+v): it waited on the replica", elapsed, pass)
	}
	if got := strings.Join(sessionsOn(t, tc, owner), ","); got != "live" {
		t.Fatalf("the owner holds %v after the pass (%+v), want [live]", got, pass)
	}
}

// TestSweepPausesWhileABucketIsRebuilding (spec §10): a TTL index that is not
// ready — being rebuilt — issues no expiry; its entries are not the truth
// yet. The first pass after it is ready drains the backlog.
func TestSweepPausesWhileABucketIsRebuilding(t *testing.T) {
	s := newTestServer(t)
	now := time.Now()
	seedSessions(t, s, now, 3)
	sessions := servedBucket(t, s, "app", "sessions")
	ready := sessions.declaredIndexes()
	building := cloneSpecs(ready)
	building[0].State = store.IndexBuilding
	sessions.decl.Load().indexSpecs.store(building)
	s.sweepTTL(context.Background(), now)
	if got := countOf(t, s, "sessions"); got != 4 {
		t.Fatalf("a pass over a building TTL index left %d of 4 documents: it expired from half an index", got)
	}
	sessions.decl.Load().indexSpecs.store(ready)
	s.sweepTTL(context.Background(), now)
	if got := countOf(t, s, "sessions"); got != 1 {
		t.Fatalf("the first pass after the index was ready left %d documents, want the live one", got)
	}
}

// TestExpiredDocumentRemovesItsIndexEntries (spec §10): an expiry is an
// ordinary delete, so no entry of any index on the bucket — single,
// compound, unique, null-valued — names the document afterwards.
func TestExpiredDocumentRemovesItsIndexEntries(t *testing.T) {
	s := newTestServer(t)
	now := time.Now()
	createUniqueBucket(t, s, "app", "sessions",
		ttlDeclaration("CreatedAt", 3600),
		"Name",
		map[string]interface{}{"fields": []interface{}{"Name", "Region"}},
		map[string]interface{}{"field": "Token", "unique": true},
	)
	mustInsert(t, s, "app", "sessions", "gone", map[string]interface{}{
		"CreatedAt": primitive.NewDateTimeFromTime(now.Add(-2 * time.Hour)), "Name": "ada", "Token": "t-1",
	})
	s.sweepTTL(context.Background(), now)
	sessions := servedBucket(t, s, "app", "sessions")
	for id := uint16(0); id < 4; id++ {
		for _, entry := range indexEntries(t, s, sessions, id) {
			if strings.HasSuffix(string(entry), "gone") {
				t.Errorf("index %d still names the expired document: %x", id, entry)
			}
		}
	}
	if documentOf(t, s, "app", "sessions", "gone") != nil {
		t.Fatal("precondition: the document did not expire")
	}
}
