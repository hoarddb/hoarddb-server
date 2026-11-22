package server

import (
	"fmt"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Tests of a bucket's own replication factor: where it is placed, that the
// node learns the factor before it decides what it holds, and that a read
// never trusts a placement the node has no evidence for. Ported from the
// branch's 17dd938; every test below carries the one-line re-aim in its doc
// comment.

// registeredFactor reads the factor the cluster has registered for a bucket,
// and whether it has one. It looks in the registry itself and not through
// effectiveRF, so that "inherits the node default" and "registered at the
// node default" are different answers.
func registeredFactor(c *Cluster, database, bucket string) (int32, bool) {
	c.viewMu.RLock()
	defer c.viewMu.RUnlock()
	factor, registered := c.bucketFactors[placementOf(database, bucket)]
	return factor, registered
}

// nodesHoldingLocally counts the nodes that can answer for a key out of their
// own storage, with no network hop. Server.Find goes straight to the local
// engine, so a node that answers has the bytes; a node that does not is
// either not placed or not yet seeded.
func nodesHoldingLocally(t *testing.T, tc *testCluster, database, bucket, key string) int {
	t.Helper()

	held := 0
	for _, node := range tc.nodes {
		resp, err := node.server.core.Find(FindRequest{Database: database, Bucket: bucket, Key: key})
		if err == nil && resp != nil && resp.Found {
			held++
		}
	}
	return held
}

// waitForLocalCopies polls until want nodes hold the key in their own storage.
// Replication is asynchronous past the write concern's acknowledgement budget,
// so the count is a convergence property rather than an instant one.
func waitForLocalCopies(t *testing.T, tc *testCluster, database, bucket, key string, want int) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for {
		got := nodesHoldingLocally(t, tc, database, bucket, key)
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s.%s key %q is on %d nodes' own storage, want %d", database, bucket, key, got, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// waitForPlacement polls until every node places the bucket on want nodes.
// Bucket DDL is replicated, not broadcast: it commits on the owner and reaches
// the rest of the replica set through the owner's sender queue, so a node's
// view of a just-created bucket's factor converges rather than arriving with
// the response. Only meaningful when every node holds the bucket (a factor at
// or above the node default); a node outside the set never learns the factor.
func waitForPlacement(t *testing.T, tc *testCluster, database, bucket string, want int) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for {
		disagreeing, got := -1, 0
		for index, node := range tc.nodes {
			placed := mustReplicaIDs(t, node.server.core.cluster, database, bucket)
			if len(placed) != want {
				disagreeing, got = index, len(placed)
				break
			}
		}
		if disagreeing < 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %d places %s.%s on %d nodes, want %d", disagreeing, database, bucket, got, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func createBucketAt(tc *testCluster, index int, database, bucket string, factor int) {
	tc.ok(index, transport.OpCreateBucket, CreateBucketRequest{
		Database: database, Name: bucket, Type: BucketTypeHash, ReplicationFactor: factor,
	})
}

func putDocument(tc *testCluster, index int, database, bucket, key string) {
	putDocumentAt(tc, index, database, bucket, key, "")
}

func putDocumentAt(tc *testCluster, index int, database, bucket, key, concern string) {
	tc.ok(index, transport.OpPut, InsertRequest{
		Database: database, Bucket: bucket, Key: key, Data: map[string]interface{}{"n": 1}, WriteConcern: concern,
	})
}

// TestRF1AndRF3BucketsLandOnDifferentNodeCounts is T-03: two buckets in one
// database, one declaring three copies and one declaring nothing, are placed on
// different numbers of nodes and their documents exist on exactly those nodes.
//
// This is the whole feature in one assertion. Before it, the node-wide factor
// decided placement for every bucket on the node and a per-bucket declaration
// was dropped on the floor.
//
// Ported from 17dd938, re-aimed: placement is asked by (database, bucket) —
// mustReplicaIDs — not by bare name, and "own storage" is the bucket's own
// engine through Server.Find, not a directory.
func TestRF1AndRF3BucketsLandOnDifferentNodeCounts(t *testing.T) {
	// The node default is 1, so an un-stamped bucket gets one copy and the
	// three-copy bucket can only have got there from its own declaration.
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, 0, testDatabase, "wide", 3)
	createBucketAt(tc, 0, testDatabase, "narrow", 0)

	// Every node must agree where "wide" lives, or a write placed by one is
	// read from somewhere the other never wrote. The create commits on the
	// owner, which is what it reports; the rest converge.
	waitForPlacement(t, tc, testDatabase, "wide", 3)
	if placed := mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, "narrow"); len(placed) != 1 {
		t.Fatalf("narrow is placed on %v, want one node", placed)
	}

	// Every node must name the same owner: a per-bucket factor that moved
	// ownership would put the apply fence and the DDL placement on different
	// nodes, which is what the prefix property (T-01) rules out.
	owner := mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, "wide")[0]
	for index, node := range tc.nodes {
		if got := mustReplicaIDs(t, node.server.core.cluster, testDatabase, "wide")[0]; got != owner {
			t.Fatalf("node %d names %q the owner of wide; node 0 names %q", index, got, owner)
		}
	}

	putDocument(tc, 0, testDatabase, "wide", "k")
	putDocument(tc, 0, testDatabase, "narrow", "k")
	waitForLocalCopies(t, tc, testDatabase, "wide", "k", 3)

	// The narrow bucket's document must be on exactly one node's storage and
	// stay there: a bucket that declared one copy getting three is the same
	// defect as a bucket that declared three getting one.
	if got := nodesHoldingLocally(t, tc, testDatabase, "narrow", "k"); got != 1 {
		t.Fatalf("a default-factor bucket's document is on %d nodes' own storage, want 1", got)
	}
	time.Sleep(500 * time.Millisecond)
	if got := nodesHoldingLocally(t, tc, testDatabase, "narrow", "k"); got != 1 {
		t.Fatalf("a default-factor bucket's document spread to %d nodes, want 1", got)
	}
}

// TestRF1BucketIsNeverSweptAsUnderReplicated is T-11. migrateIfMissing acts
// only for members of the bucket's replica set, so a bucket at one copy is
// complete at one copy and no other node ever pulls it. This is the behaviour
// most likely to be broken later by a well-meaning "the cluster looks
// under-replicated" optimisation, which is why it has a test of its own.
//
// Ported from 17dd938, re-aimed: the bucket is asked of by (database, bucket).
// The node default stays 1, as on the branch. A bucket declared BELOW a higher
// node default is a different question — a node that never held it computes the
// set at its own default and would pull — and belongs to step 1.7 (T-37).
func TestRF1BucketIsNeverSweptAsUnderReplicated(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, 0, testDatabase, "solo", 0)
	putDocument(tc, 0, testDatabase, "solo", "k")
	waitForLocalCopies(t, tc, testDatabase, "solo", "k", 1)

	placed := mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, "solo")
	if len(placed) != 1 {
		t.Fatalf("solo is placed on %v, want one node", placed)
	}

	// Ask every node that is not in the placed set to pull the bucket. Each
	// must do nothing, because its own placement does not put it in the set.
	for _, node := range tc.nodes {
		c := node.server.core.cluster
		if c.self.ID == placed[0] {
			continue
		}
		if err := c.migrateIfMissing(c.ctx, testDatabase, "solo"); err != nil {
			t.Fatalf("a non-member asked to sweep a bucket it is not placed on reported it unsettled: %v", err)
		}
	}

	if got := nodesHoldingLocally(t, tc, testDatabase, "solo", "k"); got != 1 {
		t.Fatalf("after asking every non-member to sweep, the one-copy bucket is on %d nodes, want 1", got)
	}
}

// TestUserStoreReservedKeyKeepsTheNodeDefault is T-16. The user store places
// itself with a reserved key, which is not a bucket: it has no catalog entry
// and keeps the node default however many buckets declare a factor.
//
// Ported from 17dd938, re-aimed: only the first half remains. A reserved key
// cannot equal a bucket's placement key (server/placement.go:reservedPlacement),
// so the branch's second half — a bucket named like the reserved key — is
// unreachable rather than guarded.
func TestUserStoreReservedKeyKeepsTheNodeDefault(t *testing.T) {
	tc := newTestCluster(t, 3, 2)
	defer tc.stop()

	c := tc.nodes[0].server.core.cluster
	reserved := reservedPlacement(authReservedKey)
	if got := c.effectiveRF(reserved); got != c.rf {
		t.Fatalf("the reserved key %q resolves to %d, want the node default %d", authReservedKey, got, c.rf)
	}
	before, err := c.Replicas(reserved)
	if err != nil || len(before) != c.rf {
		t.Fatalf("the reserved key is placed on %v (%v), want the node default %d", before, err, c.rf)
	}

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, 0, testDatabase, "ordinary", 3)
	waitForPlacement(t, tc, testDatabase, "ordinary", 3)

	if got := c.effectiveRF(reserved); got != c.rf {
		t.Fatalf("after an unrelated bucket declared a factor, the reserved key resolves to %d, want %d", got, c.rf)
	}
	after, err := c.Replicas(reserved)
	if err != nil || fmt.Sprint(after) != fmt.Sprint(before) {
		t.Fatalf("the user store moved from %v to %v (%v)", before, after, err)
	}
}

// TestSameNamedBucketsInTwoDatabasesKeepSeparateFactors is T-26: app.events at
// 3 and analytics.events at 2, on nodes whose default is 1, keep separate
// factors and separate replica sets, and dropping one leaves the other's
// registry entry alone. The registry is keyed by the placement key, so the
// second create cannot overwrite the first.
func TestSameNamedBucketsInTwoDatabasesKeepSeparateFactors(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	for _, database := range []string{"app", "analytics"} {
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: database})
	}
	createBucketAt(tc, 0, "app", "events", 3)
	createBucketAt(tc, 0, "analytics", "events", 2)

	waitForPlacement(t, tc, "app", "events", 3)
	waitForRegistrations(t, tc, "analytics", "events", 2)
	for _, node := range tc.nodes {
		c := node.server.core.cluster
		if factor, registered := registeredFactor(c, "analytics", "events"); registered {
			if factor != 2 {
				t.Fatalf("analytics.events is registered at %d, want 2", factor)
			}
			if got := len(mustReplicaIDs(t, c, "analytics", "events")); got != 2 {
				t.Fatalf("analytics.events is placed on %d nodes, want 2", got)
			}
		}
		if factor, registered := registeredFactor(c, "app", "events"); !registered || factor != 3 {
			t.Fatalf("app.events is registered as %d (registered %v), want 3", factor, registered)
		}
	}

	// Dropping app.events removes its entry and no other.
	tc.ok(0, transport.OpDeleteBucket, DeleteBucketRequest{Database: "app", Name: "events"})
	waitForRegistrations(t, tc, "app", "events", 0)
	waitForRegistrations(t, tc, "analytics", "events", 2)
}

// waitForRegistrations polls until exactly want nodes register a factor for the
// bucket. A node registers a bucket it serves, and DDL reaches the rest of a
// bucket's replica set asynchronously, so the count converges.
func waitForRegistrations(t *testing.T, tc *testCluster, database, bucket string, want int) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for {
		registered := 0
		for _, node := range tc.nodes {
			if _, has := registeredFactor(node.server.core.cluster, database, bucket); has {
				registered++
			}
		}
		if registered == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d nodes register %s.%s, want %d", registered, database, bucket, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestDroppingABucketReachesEveryCopyOfAFactorAboveTheNodeDefault: dropping a
// bucket forgets its factor as the drop applies, and the owner ships the drop
// to the replica set afterwards. Read after the forget, that set is the node
// default's — one node — so the two replicas of a bucket declared at 3 are
// never told, keep the bucket, and serve it again the next time the name is
// created. The set a drop ships to is the set the bucket had.
func TestDroppingABucketReachesEveryCopyOfAFactorAboveTheNodeDefault(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, 0, testDatabase, "wide", 3)
	waitForServing(t, tc, testDatabase, "wide", 3)

	tc.ok(0, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: "wide"})

	waitForServing(t, tc, testDatabase, "wide", 0)
}

// stopNode stops one node and returns a function that starts it again on the
// same data directory and addresses.
func stopNode(t *testing.T, tc *testCluster, index int) (start func() *testNode) {
	t.Helper()

	dataDir := tc.nodes[index].cfg.Storage.DataDir
	if tc.nodes[index].client != nil {
		_ = tc.nodes[index].client.Close()
		tc.nodes[index].client = nil
	}
	if err := tc.nodes[index].server.Stop(); err != nil {
		t.Fatalf("stop node %d: %v", index+1, err)
	}
	tc.nodes[index].server = nil
	return func() *testNode {
		restarted := tc.startNode(index, filepath.Dir(dataDir), tc.token)
		tc.nodes[index] = restarted
		return restarted
	}
}

// TestADropReachesAReplicaThatWasDownWhenItCommitted is F-1's other half. The
// live path holds an undelivered drop in memory, so an owner restart loses it,
// and catch-up decides per entry from the owner's current placement: after the
// drop the bucket has none, no peer is relevant, and the replica that was down
// is never told. It keeps the bucket, keeps serving reads from it, and the
// next create of the name meets the old documents.
func TestADropReachesAReplicaThatWasDownWhenItCommitted(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, 0, testDatabase, "wide", 3)
	waitForPlacement(t, tc, testDatabase, "wide", 3)
	waitForServing(t, tc, testDatabase, "wide", 3)

	ids := mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, "wide")
	ownerIndex := nodeIndexByAddr(t, tc.addrs, ids[0])
	replicaIndex := nodeIndexByAddr(t, tc.addrs, ids[2])
	startReplica := stopNode(t, tc, replicaIndex)

	tc.ok(ownerIndex, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: "wide"})
	// The undelivered drop lives in the owner's memory until this restart.
	tc.restartNode(ownerIndex)
	revived := startReplica()

	deadline := time.Now().Add(20 * time.Second)
	for revived.server.core.bucketStoreOpen(testDatabase, "wide") {
		if time.Now().After(deadline) {
			t.Fatalf("the replica that was down when the bucket was dropped still serves it: catch-up never delivered the drop")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestRetentionKeepsADropOwedToAReplicaThatIsDown: retention's bound is the
// lowest position a relevant peer may still need, and relevance follows the
// buckets the owner has now. A drop removes the only bucket that made the
// replica relevant, so without the owed drop in the bound the very next
// retention pass prunes the entry the replica has not received.
func TestRetentionKeepsADropOwedToAReplicaThatIsDown(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, 0, testDatabase, "wide", 3)
	waitForPlacement(t, tc, testDatabase, "wide", 3)
	waitForServing(t, tc, testDatabase, "wide", 3)

	ids := mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, "wide")
	ownerIndex := nodeIndexByAddr(t, tc.addrs, ids[0])
	stopNode(t, tc, nodeIndexByAddr(t, tc.addrs, ids[2]))

	tc.ok(ownerIndex, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: "wide"})

	owner := tc.nodes[ownerIndex].server.core
	last := owner.wal.LastSequence()
	if bound := owner.cluster.replicaAckBound(); bound >= last {
		t.Fatalf("retention may prune the owner's log through %d, the last entry (the drop), though a replica has not received it", bound)
	}
}

// waitForServing polls until exactly want nodes serve the bucket from an open
// store.
func waitForServing(t *testing.T, tc *testCluster, database, bucket string, want int) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for {
		serving := 0
		for _, node := range tc.nodes {
			if node.server.core.bucketStoreOpen(database, bucket) {
				serving++
			}
		}
		if serving == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d nodes serve %s.%s, want %d", serving, database, bucket, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestDroppingADatabaseForgetsTheFactorsOfItsBuckets: a registry entry for a
// bucket that no longer exists would place a re-created bucket of the same
// name at the old factor, silently.
func TestDroppingADatabaseForgetsTheFactorsOfItsBuckets(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	createBucketAt(tc, 0, "app", "events", 3)
	waitForPlacement(t, tc, "app", "events", 3)

	tc.ok(0, transport.OpDeleteDB, DeleteDatabaseRequest{Name: "app"})
	waitForRegistrations(t, tc, "app", "events", 0)
}

// restartNodeAfterEditingItsWAL stops one node, lets edit append to its
// write-ahead log while nothing has it open, and starts it again on the same
// data directory. It is how a test builds "the create is in the log and the
// catalog never got it" without racing a crash.
func restartNodeAfterEditingItsWAL(t *testing.T, tc *testCluster, index int, edit func(wal *storage.WAL)) *testNode {
	t.Helper()

	dataDir := tc.nodes[index].cfg.Storage.DataDir
	if tc.nodes[index].client != nil {
		_ = tc.nodes[index].client.Close()
		tc.nodes[index].client = nil
	}
	if err := tc.nodes[index].server.Stop(); err != nil {
		t.Fatalf("stop node %d: %v", index+1, err)
	}
	tc.nodes[index].server = nil

	wal, err := storage.OpenWAL(filepath.Join(dataDir, "wal"), nil)
	if err != nil {
		t.Fatalf("opening node %d's WAL: %v", index+1, err)
	}
	edit(wal)
	if err := wal.Close(); err != nil {
		t.Fatalf("closing node %d's WAL: %v", index+1, err)
	}

	restarted := tc.startNode(index, filepath.Dir(dataDir), tc.token)
	tc.nodes[index] = restarted
	return restarted
}

// appendCreateBucketToWAL puts a create in the log exactly as a replicated
// create is logged, without committing it to the catalog.
func appendCreateBucketToWAL(t *testing.T, wal *storage.WAL, database, bucket string, factor int) {
	t.Helper()

	for _, step := range []struct {
		op      transport.Opcode
		bucket  string
		payload any
	}{
		{transport.OpCreateDB, "", CreateDatabaseRequest{Name: database}},
		{transport.OpCreateBucket, bucket, CreateBucketRequest{Database: database, Name: bucket, Type: BucketTypeHash, ReplicationFactor: factor}},
	} {
		payload, err := bson.Marshal(step.payload)
		if err != nil {
			t.Fatalf("encoding the %s entry: %v", step.op, err)
		}
		if _, err := wal.Append(step.bucket, uint8(step.op), nil, payload); err != nil {
			t.Fatalf("appending the %s entry to the WAL: %v", step.op, err)
		}
	}
}

// TestFactorRegistryIsFilledOnEveryPathABucketBecomesServed is T-29. A bucket
// becomes served on a node by a client create (through the replicated write
// path), by the load of a catalogued entry at start, by the replay of a create
// that is in the log and not in the catalog, and by the materialise of a pulled
// range (its row is added in step 1.7). The registry must hold the factor after
// each: populating it in one place a process reaches only sometimes is how a
// running node and a restarted one come to place the same bucket differently.
func TestFactorRegistryIsFilledOnEveryPathABucketBecomesServed(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	requireRegistered := func(t *testing.T, node *testNode, database, bucket string, want int32) {
		t.Helper()
		if factor, registered := registeredFactor(node.server.core.cluster, database, bucket); !registered || factor != want {
			t.Fatalf("%s.%s is registered as %d (registered %v) on %s, want %d", database, bucket, factor, registered, node.addr, want)
		}
	}

	t.Run("client create", func(t *testing.T) {
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		createBucketAt(tc, 0, testDatabase, "wide", 3)
		waitForPlacement(t, tc, testDatabase, "wide", 3)
		for _, node := range tc.nodes {
			requireRegistered(t, node, testDatabase, "wide", 3)
		}
	})

	t.Run("load from the catalog after a restart", func(t *testing.T) {
		restarted := tc.restartNode(1)
		requireRegistered(t, restarted, testDatabase, "wide", 3)
	})

	t.Run("replay of a create the catalog never got", func(t *testing.T) {
		restarted := restartNodeAfterEditingItsWAL(t, tc, 2, func(wal *storage.WAL) {
			appendCreateBucketToWAL(t, wal, "replayed", "wide", 3)
		})
		requireRegistered(t, restarted, "replayed", "wide", 3)
	})
}

// TestRestartDoesNotReleaseACopyPlacedByAnExplicitFactor is T-30, and the most
// expensive test here to skip. A replica of a bucket whose factor exceeds the
// node default is, until the registry knows the factor, "misplaced": start-time
// relocation hands the bucket to the set it computes at the node default and,
// once that set confirms, deletes the copy. The registry must therefore be
// complete before Cluster.Start, which is why it is filled inside LoadCatalog's
// own loop.
//
// The assertion is the copy itself, read from the restarted node's own store —
// not a timeout and not a log line.
func TestRestartDoesNotReleaseACopyPlacedByAnExplicitFactor(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, 0, testDatabase, "wide", 3)
	waitForPlacement(t, tc, testDatabase, "wide", 3)
	putDocument(tc, 0, testDatabase, "wide", "k")
	waitForLocalCopies(t, tc, testDatabase, "wide", "k", 3)

	// A replica that is not the owner: the node default of 1 places the bucket
	// on its owner alone, so this node is the one relocation would call
	// misplaced.
	ids := mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, "wide")
	restartIndex := nodeIndexByAddr(t, tc.addrs, ids[2])
	restarted := tc.restartNode(restartIndex)

	holdsTheCopy := func() bool {
		resp, err := restarted.server.core.Find(FindRequest{Database: testDatabase, Bucket: "wide", Key: "k"})
		return err == nil && resp != nil && resp.Found
	}
	// One relocation pass is a ring lookup per held bucket and, for a bucket it
	// calls misplaced, one round of questions to the other members: well under
	// this on loopback.
	settled := time.Now().Add(3 * time.Second)
	for time.Now().Before(settled) {
		if !holdsTheCopy() {
			t.Fatalf("the restarted replica %s released its copy of a bucket placed at 3 on a node whose default is 1: "+
				"start-time relocation treated it as misplaced because the factor registry was not complete before Cluster.Start",
				restarted.addr)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if misplaced := restarted.server.core.cluster.misplacedBuckets(); len(misplaced) != 0 {
		t.Fatalf("the restarted replica still calls %v misplaced", misplaced)
	}
}

// TestReadOfABucketBelowTheNodeDefaultIsForwardedByANodeThatDoesNotHoldIt: a
// node that never held a bucket computes its replica set at the node default,
// which for a bucket declared below it includes the node itself. "I am in the
// set" is not "I hold the copy", so a read is served locally only when the
// store is open, and otherwise goes to the owner.
func TestReadOfABucketBelowTheNodeDefaultIsForwardedByANodeThatDoesNotHoldIt(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, 0, testDatabase, "solo", 1)
	putDocument(tc, 0, testDatabase, "solo", "k")

	holders := 0
	for _, node := range tc.nodes {
		if node.server.core.bucketStoreOpen(testDatabase, "solo") {
			holders++
		}
	}
	if holders != 1 {
		t.Fatalf("a bucket declared at 1 is open on %d nodes, want 1", holders)
	}

	for index, node := range tc.nodes {
		frame := tc.frame(index, transport.OpGet, FindRequest{Database: testDatabase, Bucket: "solo", Key: "k"})
		if frame.Opcode == transport.OpError {
			t.Fatalf("a read through node %d (holds the bucket: %v) failed: %s",
				index+1, node.server.core.bucketStoreOpen(testDatabase, "solo"), string(frame.Payload))
		}
		var found FindResponse
		if err := bson.Unmarshal(frame.Payload, &found); err != nil || !found.Found {
			t.Fatalf("a read through node %d did not find the document (%v): %+v", index+1, err, found)
		}
	}
}

// TestRecordingAFactorCostsTheSameWithAHundredThousandRecorded is T-43. The
// registry is mutated in place under the ring's lock, so recording one factor
// must not depend on how many are recorded: a copy of the whole map per DDL
// would make a scripted create of 10^6 buckets quadratic. It also reports
// what the registry costs per entry (spec §17.3), measured and not counted.
func TestRecordingAFactorCostsTheSameWithAHundredThousandRecorded(t *testing.T) {
	const recorded = 100_000
	newRegistry := func() *Cluster { return &Cluster{bucketFactors: make(map[placementKey]int32)} }

	// The registry is filled first, before anything else allocates, and read
	// with the collector's help on both sides: what stays live is the map and
	// the key strings it retains. Names are 12 and 41 characters, a short
	// bucket name and a uuid-sized one.
	//
	// The first pass is discarded: a process that has run other tests carries
	// unswept garbage and a warm allocator, and the first delta read 24 bytes
	// where the settled figure is 58 (measured, three runs).
	perEntry := map[string]float64{}
	for pass := 0; pass < 2; pass++ {
		for _, name := range []struct {
			label string
			of    func(i int) string
		}{
			{"12-char names", func(i int) string { return fmt.Sprintf("bucket-%06d", i) }},
			{"41-char names", func(i int) string { return fmt.Sprintf("user-%036d", i) }},
		} {
			loaded := newRegistry()
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			for i := 0; i < recorded; i++ {
				loaded.recordBucketFactor(placementOf("app", name.of(i)), 3)
			}
			runtime.GC()
			runtime.ReadMemStats(&after)
			runtime.KeepAlive(loaded)
			perEntry[name.label] = float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)) / recorded
		}
	}

	probes := make([]placementKey, 200)
	for i := range probes {
		probes[i] = placementOf("probe", fmt.Sprintf("bucket-%03d", i))
	}
	cost := func(c *Cluster) float64 {
		next := 0
		return testing.AllocsPerRun(100, func() {
			c.recordBucketFactor(probes[next], 3)
			next++
		})
	}
	empty := cost(newRegistry())
	full := newRegistry()
	for i := 0; i < recorded; i++ {
		full.recordBucketFactor(placementOf("app", fmt.Sprintf("bucket-%06d", i)), 3)
	}
	loadedCost := cost(full)

	t.Logf("registry at %d entries: %.1f bytes per entry with 12-character bucket names, %.1f with 41-character ones (live heap, measured); "+
		"allocations to record one: %.0f on an empty registry, %.0f on a full one",
		recorded, perEntry["12-char names"], perEntry["41-char names"], empty, loadedCost)
	if loadedCost > empty {
		t.Fatalf("recording a factor allocates %.0f times with %d recorded and %.0f times with none", loadedCost, recorded, empty)
	}
}

// TestACreateAboveTheNodeDefaultIsShippedAfterTheOwnersLogWasPruned is F-2. A
// peer the owner never shipped a bucket-scoped entry to has an acknowledgement
// frozen at the last database-level broadcast, and retention prunes every
// rotated segment while no peer is relevant. A create of a bucket declared
// above the node default is the first bucket-scoped entry to reach such a peer,
// and peer.ship refuses a relevant peer whose position is behind the horizon:
// it retried that one batch for ever, and nothing the owner shipped to the
// peer afterwards was delivered either. A fresh cluster, with an unpruned log,
// cannot show it.
func TestACreateAboveTheNodeDefaultIsShippedAfterTheOwnersLogWasPruned(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, 0, testDatabase, "bulk", 0)
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, "bulk"))
	if err != nil {
		t.Fatalf("resolve owner: %v", err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	ownerCluster := tc.nodes[ownerIndex].server.core.cluster
	ownerWAL := tc.nodes[ownerIndex].server.core.wal

	// Give each peer a real, non-zero acknowledgement, standing in for the
	// database-level broadcasts it would have acked (retention_test.go does the
	// same), then push the log past a segment and prune it to its tail.
	for _, node := range ownerCluster.Members() {
		if node.ID == ownerCluster.self.ID {
			continue
		}
		seq, err := ownerCluster.appendWAL(ReplEntry{Op: int32(transport.OpPing)}, storage.WALOrigin{Kind: storage.WALOriginOwn, Epoch: ownerCluster.Epoch()})
		if err != nil {
			t.Fatalf("append WAL: %v", err)
		}
		if err := ownerCluster.peerFor(node).ship([]ReplEntry{{Seq: seq, Op: int32(transport.OpPing)}}); err != nil {
			t.Fatalf("test setup: could not ship the initial entry to %s: %v", node.ID, err)
		}
	}
	frozen := ownerCluster.peerFor(ownerCluster.Members()[0]).ackedThrough(ownerCluster.self.ID)
	blob := strings.Repeat("x", 20<<20)
	for i := 0; i < 6; i++ {
		putDocumentWith(tc, ownerIndex, testDatabase, "bulk", fmt.Sprintf("big%02d", i), map[string]interface{}{"blob": blob})
	}
	if err := ownerWAL.Truncate(ownerWAL.LastSequence()); err != nil {
		t.Fatalf("forced truncation: %v", err)
	}
	if first, err := ownerWAL.FirstSequence(); err != nil || first <= frozen {
		t.Fatalf("test setup: the horizon %d (%v) did not pass the peers' frozen position %d", first, err, frozen)
	}

	// A bucket the same owner owns, declared at 3.
	wide := ""
	for i := 0; wide == ""; i++ {
		name := fmt.Sprintf("wide%03d", i)
		if placed, err := ownerCluster.Owner(placementOf(testDatabase, name)); err == nil && placed.ID == owner.ID {
			wide = name
		}
	}
	createBucketAt(tc, ownerIndex, testDatabase, wide, 3)

	waitForServing(t, tc, testDatabase, wide, 3)
	putDocument(tc, ownerIndex, testDatabase, wide, "k")
	waitForLocalCopies(t, tc, testDatabase, wide, "k", 3)
}

func putDocumentWith(tc *testCluster, index int, database, bucket, key string, data map[string]interface{}) {
	tc.ok(index, transport.OpPut, InsertRequest{Database: database, Bucket: bucket, Key: key, Data: data})
}

// upsertDocumentWith is putDocumentWith for a key the bucket already holds:
// insert refuses an existing id, so a write that means to replace sends an
// upsert.
func upsertDocumentWith(tc *testCluster, index int, database, bucket, key string, data map[string]interface{}) {
	tc.ok(index, transport.OpUpsert, InsertRequest{Database: database, Bucket: bucket, Key: key, Data: data})
}

// TestExplicitFactorSurvivesARestartAndPlacementMatches is T-02. The factor
// lives in the bucket's catalog entry, so a restart brings it back with the
// bucket, and the placement the restarted node computes is the one it computed
// before; `show buckets` reports the effective factor and whether the bucket
// declared it, from this node's own state and with no request to another node.
func TestExplicitFactorSurvivesARestartAndPlacementMatches(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, 0, testDatabase, "wide", 3)
	inheriting := bucketOwnedBy(t, tc, 1, "plain")
	createBucketAt(tc, 0, testDatabase, inheriting, 0)
	waitForPlacement(t, tc, testDatabase, "wide", 3)
	waitForServing(t, tc, testDatabase, "wide", 3)
	before := mustReplicaIDs(t, tc.nodes[1].server.core.cluster, testDatabase, "wide")

	restarted := tc.restartNode(1).server.core
	entry, catalogued, err := restarted.readCatalogEntry(testDatabase, "wide")
	if err != nil || !catalogued || entry.ReplicationFactor != 3 {
		t.Fatalf("after a restart the catalog entry is %+v (catalogued %v, %v), want factor 3", entry, catalogued, err)
	}
	if after := mustReplicaIDs(t, restarted.cluster, testDatabase, "wide"); fmt.Sprint(after) != fmt.Sprint(before) || len(after) != 3 {
		t.Fatalf("after a restart the bucket is placed on %v, before it was %v; want the same three nodes", after, before)
	}

	listing, err := restarted.ListBuckets(ListBucketsRequest{Database: testDatabase})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	seen := map[string]BucketInfo{}
	for _, info := range listing.Buckets {
		seen[info.Name] = info
	}
	if wide := seen["wide"]; wide.ReplicationFactor != 3 || wide.Inherited {
		t.Errorf("the listing reports wide as factor %d inherited %v, want 3 declared", wide.ReplicationFactor, wide.Inherited)
	}
	if plain, listed := seen[inheriting]; listed && (plain.ReplicationFactor != 1 || !plain.Inherited) {
		t.Errorf("the listing reports %s as factor %d inherited %v, want 1 inherited", inheriting, plain.ReplicationFactor, plain.Inherited)
	}
}

// spawnedByServerCode reports whether a stack's goroutine was started by a `go`
// statement in the server package, as opposed to by the testing package that
// runs the caller.
func spawnedByServerCode(stack string) bool {
	for _, line := range strings.Split(stack, "\n") {
		if strings.HasPrefix(line, "created by ") && strings.Contains(line, "github.com/hoarddb/hoarddb-server/server.") {
			return true
		}
	}
	return false
}

// TestTheFactorRegistryIsFilledOnTheLoadingGoroutine is verifier D-7. T-30
// passes if the fill is asynchronous and usually wins the race against
// Cluster.Start, and fails only if the fill is slow; a fill that can lose the
// race is the defect, whether or not this run lost it. So the assertion is
// structural: every registration made while a node restarts happens on a
// goroutine that no `go` statement created, which is the goroutine that is
// loading the catalog (or replaying, or materialising).
func TestTheFactorRegistryIsFilledOnTheLoadingGoroutine(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, 0, testDatabase, "wide", 3)
	waitForServing(t, tc, testDatabase, "wide", 3)

	var mu sync.Mutex
	var stacks []string
	record := func() {
		mu.Lock()
		defer mu.Unlock()
		stacks = append(stacks, string(debug.Stack()))
	}
	factorRegisteredHook.Store(&record)
	defer factorRegisteredHook.Store(nil)

	tc.restartNode(1)
	factorRegisteredHook.Store(nil)

	mu.Lock()
	defer mu.Unlock()
	if len(stacks) == 0 {
		t.Fatal("no registration was observed while the node restarted: the hook is not on the registration path")
	}
	for _, stack := range stacks {
		if spawnedByServerCode(stack) {
			t.Fatalf("a factor was registered on a goroutine some other code started; the registry is not complete when the load returns:\n%s", stack)
		}
		if !strings.Contains(stack, "LoadCatalog") {
			t.Fatalf("a registration during the restart did not come from the catalog load:\n%s", stack)
		}
	}
}
