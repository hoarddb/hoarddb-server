package server

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// requestBody encodes a request body for a push built by hand.
func requestBody(t *testing.T, request any) []byte {
	t.Helper()
	encoded, err := bson.Marshal(request)
	if err != nil {
		t.Fatalf("encode %T: %v", request, err)
	}
	return encoded
}

// sourcePosition is receiver's position on source's stream: a push built by
// hand must be above it to be applied.
func sourcePosition(receiver *testNode, source string) uint64 {
	c := receiver.server.core.cluster
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	return c.appliedFrom[source]
}

// TestADropOnANodeThatDoesNotHoldTheDatabaseStillWritesItsTombstones is
// review F-8: a drop that reaches a node holding nothing of its database
// records the dropped generation all the same, so a create of that generation
// arriving after it is ignored.
//
// Guard: G-4's tombstone write placed beside the database's own drop record,
// after Server.DeleteDatabaseCtx's "no database" answer, where it writes
// nothing on exactly the node that needs it.
func TestADropOnANodeThatDoesNotHoldTheDatabaseStillWritesItsTombstones(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	receiver := tc.nodes[1]
	const source, database, generation = "test-source", "tenant", int64(4242)
	base := sourcePosition(receiver, source)

	pushToReceiver(t, receiver, source, []ReplEntry{{Seq: base + 1, Op: int32(transport.OpDeleteDB),
		Payload: requestBody(t, DeleteDatabaseRequest{Name: database, Generations: []int64{generation}})}})
	dropped, err := receiver.server.core.generationDropped(database, generation)
	if err != nil || !dropped {
		t.Fatalf("a drop of generation %d on a node without %q left no record of it (%v)", generation, database, err)
	}
	pushToReceiver(t, receiver, source, []ReplEntry{{Seq: base + 2, Op: int32(transport.OpCreateDB),
		Payload: requestBody(t, CreateDatabaseRequest{Name: database, Generation: generation})}})
	if listsDatabase(t, receiver, database) {
		t.Fatalf("a create of a generation the node had seen dropped brought %q back", database)
	}
}

// TestANodesOwnReplayAppliesADropThatNamesGenerations is review F-8's silent
// failure 2: a drop of one generation is logged and its commit fails, another
// generation of the database is created meanwhile, and the node restarts. Its
// replay of its own log applies the drop by generation: the dropped
// generation's bucket is gone and the other's stays.
//
// Guard: a replayed drop that names generations applied as a drop logged
// before generations existed — gated by the marker's log position, or applied
// by max_id, which takes every bucket the database holds then.
func TestANodesOwnReplayAppliesADropThatNamesGenerations(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	const database = "tenant"
	node := tc.nodes[0]
	first := bucketOwnedIn(t, tc, 0, database, "first")
	// Owned by the other node, so this node applies its create as a replica.
	second := bucketOwnedIn(t, tc, 1, database, "second")
	// The other node, taught the drop by it, would log it again and ship it
	// back (G-12), and this node would drop the bucket before it restarts.
	generationSeamsHook.Store(&generationSeams{noRelog: true})
	defer generationSeamsHook.Store(nil)

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: database})
	waitListedBy(t, tc, database, 1)
	createBucketAt(tc, 0, database, first, 0)
	waitForServing(t, tc, database, first, 2)
	live, err := node.server.core.liveGenerations(database)
	if err != nil || len(live) != 1 {
		t.Fatalf("setup: live generations %v (%v)", live, err)
	}

	var failed atomic.Bool
	hook := func(name string) error {
		if name == database && failed.CompareAndSwap(false, true) {
			return errors.New("injected: the drop's commit failed")
		}
		return nil
	}
	generationDropCommitHook.Store(&hook)
	defer generationDropCommitHook.Store(nil)
	tc.frame(0, transport.OpDeleteDB, DeleteDatabaseRequest{Name: database})
	if !failed.Load() || !node.server.core.bucketStoreOpen(database, first) {
		t.Fatal("setup: the drop's commit did not fail on the node that logged it")
	}

	// Another incarnation of the database, created through another node.
	const source, later = "test-source", int64(777)
	base := sourcePosition(node, source)
	pushToReceiver(t, node, source, []ReplEntry{
		{Seq: base + 1, Op: int32(transport.OpCreateDB), Payload: requestBody(t, CreateDatabaseRequest{Name: database, Generation: later})},
		{Seq: base + 2, Op: int32(transport.OpCreateBucket), Bucket: second,
			Payload: requestBody(t, CreateBucketRequest{Database: database, Name: second, Type: BucketTypeHash, DatabaseGeneration: later})},
	})
	pushToReceiver(t, node, source, []ReplEntry{{Seq: base + 3, Op: int32(transport.OpPut), Bucket: second,
		Payload: requestBody(t, InsertRequest{Database: database, Bucket: second, Key: "kept", Data: map[string]interface{}{"n": 1}})}})
	if !holdsKeyIn(node, database, second, "kept") {
		t.Fatal("setup: the later incarnation's bucket was not created and written")
	}
	secondID := heldBucketID(node, database, second)

	restarted := tc.restartNode(0)
	if restarted.server.core.bucketStoreOpen(database, first) {
		t.Errorf("after the replay %s, created in the dropped generation %d, is still served", first, live[0])
	}
	// Compared by id: a replay that dropped the copy would rebuild it from
	// the log, and a log that has been pruned rebuilds nothing.
	if id := heldBucketID(restarted, database, second); id != secondID || !holdsKeyIn(restarted, database, second, "kept") {
		t.Errorf("after the replay %s, created in generation %d which nothing dropped, is bucket id %d (it was %d)", second, later, id, secondID)
	}
	remaining, err := restarted.server.core.liveGenerations(database)
	if err != nil || !slices.Equal(remaining, []int64{later}) {
		t.Errorf("after the replay the live generations are %v (%v); want [%d]", remaining, err, later)
	}
}

// TestTwoDropsBeforeTheReclaimerRunsMergeTheirGenerations is review F-8's
// silent failure 3: a database is dropped and the expander holds a page of the
// drop's record, converted and not yet written back; the database is then
// re-created with a bucket, dropped again and re-created with another. Once
// released, the expander removes both dropped generations' buckets and keeps
// the third's, and a restart serves exactly that.
//
// Guards: the second drop merging its generations into the record, keyed by
// name, rather than overwriting it; and the expander's write-back not
// overwriting a record a merge changed after it read it
// (Server.advanceDropRecord). Either leaves the second generation's bucket
// catalogued for ever.
func TestTwoDropsBeforeTheReclaimerRunsMergeTheirGenerations(t *testing.T) {
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	s := first.core
	const database = "tenant"
	reached := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	hold := func(name string) {
		if name == database {
			once.Do(func() {
				close(reached)
				<-release
			})
		}
	}
	expandWriteBackHook.Store(&hold)
	defer expandWriteBackHook.Store(nil)
	var released sync.Once
	defer released.Do(func() { close(release) })

	for round, bucket := range []string{"one", "two", "three"} {
		if resp, err := s.CreateDatabase(database); err != nil || !resp.Success {
			t.Fatalf("round %d: CreateDatabase = %+v, %v", round, resp, err)
		}
		if resp, err := s.CreateBucket(CreateBucketRequest{Database: database, Name: bucket, Type: BucketTypeHash}); err != nil || !resp.Success {
			t.Fatalf("round %d: CreateBucket = %+v, %v", round, resp, err)
		}
		if bucket == "three" {
			break
		}
		if resp, err := s.DeleteDatabase(DeleteDatabaseRequest{Name: database}); err != nil || !resp.Success {
			t.Fatalf("round %d: DeleteDatabase = %+v, %v", round, resp, err)
		}
		if round == 0 {
			s.wakeReclaimer()
			select {
			case <-reached:
			case <-time.After(15 * time.Second):
				t.Fatal("setup: the expander never reached its write-back of the first drop's record")
			}
		}
	}
	record, pending, err := s.pendingDatabaseDrop(database)
	if err != nil || !pending || len(record.Generations) != 2 {
		t.Fatalf("the pending drop record names %v (pending %v, %v); want both dropped generations", record.Generations, pending, err)
	}

	released.Do(func() { close(release) })
	s.wakeReclaimer()
	deadline := time.Now().Add(15 * time.Second)
	for catalogEntriesOf(t, s.kv, database) != 1 {
		if time.Now().After(deadline) {
			record, pending, err := s.pendingDatabaseDrop(database)
			t.Fatalf("the database still has %d catalog entries after the expander ran; want the third generation's alone (drop record %+v pending %v %v)",
				catalogEntriesOf(t, s.kv, database), record, pending, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	second := restartCatalog(t, first, dataDir)
	buckets, err := second.core.ListBuckets(ListBucketsRequest{Database: database})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	var names []string
	for _, bucket := range buckets.Buckets {
		names = append(names, bucket.Name)
	}
	if !slices.Equal(names, []string{"three"}) {
		t.Fatalf("after a restart %q serves %v; want [three]", database, names)
	}
}

// TestARecreateThroughAnotherNodeCarriesTheDropToALaggingOwner is rev 3's
// A-1 (a): a drop reaches one member only and its coordinator is lost; the
// client re-creates the database through that member. The create carries the
// drop, so the member that never received it drops the old incarnation's
// bucket when the create arrives, and buckets created afterwards hold their
// writes on every node. The re-logged drop (G-12) is switched off, so the
// create's carried drops are the only way the owner can learn it; the test is
// run again with it on.
//
// Guard: a create that carries no dropped generations (with G-12 off): the
// owner keeps serving the dropped incarnation's bucket.
func TestARecreateThroughAnotherNodeCarriesTheDropToALaggingOwner(t *testing.T) {
	for _, relog := range []bool{false, true} {
		t.Run(fmt.Sprintf("relog=%v", relog), func(t *testing.T) {
			if !relog {
				generationSeamsHook.Store(&generationSeams{noRelog: true})
				defer generationSeamsHook.Store(nil)
			}
			recreateCarriesTheDrop(t)
		})
	}
}

func recreateCarriesTheDrop(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	coordinator, owner, member := 0, 1, 2
	old := bucketOwnedIn(t, tc, owner, testDatabase, "old")

	tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, 0, 1, 2)
	createBucketAt(tc, owner, testDatabase, old, 0)
	waitForServing(t, tc, testDatabase, old, 3)

	hold := holdPushes(peerOf(t, tc, coordinator, owner))
	defer hold.Release()
	tc.ok(coordinator, transport.OpDeleteDB, DeleteDatabaseRequest{Name: testDatabase})
	pollUntil(t, "the one member the drop reached applied it", func() bool { return !listsDatabase(t, tc.nodes[member], testDatabase) })
	_ = stopNode(t, tc, coordinator)

	tc.ok(member, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	create := loggedDatabaseDDL(t, tc, member, transport.OpCreateDB, testDatabase)
	waitAcked(t, peerOf(t, tc, member, owner), create)
	if tc.nodes[owner].server.core.bucketStoreOpen(testDatabase, old) {
		t.Fatalf("the owner still serves %s, created in the incarnation the re-create says was dropped", old)
	}

	var buckets []string
	for i := 0; len(buckets) < 10; i++ {
		name := fmt.Sprintf("new%03d", i)
		ownerOf, err := clusterOf(tc, owner).Owner(placementOf(testDatabase, name))
		if err != nil {
			t.Fatalf("owner of %s: %v", name, err)
		}
		index := nodeIndexByAddr(t, tc.addrs, ownerOf.ID)
		if index == coordinator {
			continue
		}
		createBucketAtOne(tc, index, testDatabase, name)
		putIn(tc, index, testDatabase, name, "written", "one")
		buckets = append(buckets, name)
	}
	waitAckedEverywhere(t, tc, owner)
	waitAckedEverywhere(t, tc, member)
	for _, name := range buckets {
		for _, index := range []int{owner, member} {
			if !holdsKeyIn(tc.nodes[index], testDatabase, name, "written") {
				t.Errorf("node %d does not hold the write to %s", index+1, name)
			}
		}
	}
}
