package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Rule F5, the awaiting rule (bucket-replication-rev8-spec §6.2, §7.3; G3
// build doc step 1): an owner addresses nothing of a bucket but the alter
// itself to a member it holds an awaiting record for, live, in a replay, on a
// settled owner and on a ceded one, and a ceded owner's follower does not wait
// for such a member. Each test reads the member's own log, after a barrier
// shows its stream has carried everything logged before it: a member that was
// routed an entry of a bucket it does not hold refuses it, and the owner's
// stream to it stalls, so the barrier never reaches it.

// holdPullsOf holds every pull of bucket that starts on a node of this
// process at pullStartedHook, closes held at the first, and lets them all go
// when release is called.
func holdPullsOf(t *testing.T, bucket string) (held <-chan struct{}, release func()) {
	t.Helper()
	return holdBucketAt(t, &pullStartedHook, bucket)
}

// holdBucketAt holds every call of hook for bucket, on any node of this
// process, closes held at the first, and lets them all go when release is
// called.
func holdBucketAt(t *testing.T, at *atomic.Pointer[func(database, bucket string)], bucket string) (held <-chan struct{}, release func()) {
	t.Helper()
	reached, released := make(chan struct{}), make(chan struct{})
	var first, done sync.Once
	hook := func(_, name string) {
		if name != bucket {
			return
		}
		first.Do(func() { close(reached) })
		<-released
	}
	at.Store(&hook)
	release = func() { done.Do(func() { close(released) }) }
	t.Cleanup(func() {
		release()
		at.Store(nil)
	})
	return reached, release
}

// requireWriteReachesEveryCopy writes one document to bucket through node
// index with write concern all, and fails unless it is confirmed by want
// copies: every member's stream to the owner is in sync.
func requireWriteReachesEveryCopy(t *testing.T, tc *testCluster, index int, bucket, key string, want int32) {
	t.Helper()
	frame := tc.frame(index, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: key,
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	if frame.Opcode == transport.OpError {
		t.Fatalf("a write of %q with concern all to bucket %q was refused: %s", key, bucket, bson.Raw(frame.Payload))
	}
	confirmed, _, hasConfirmed, _ := copiesConfirmedOf(frame.Payload)
	if !hasConfirmed || confirmed != want {
		t.Fatalf("a write of %q with concern all to bucket %q was confirmed by %d copies, want %d: %s", key, bucket, confirmed, want, bson.Raw(frame.Payload))
	}
}

// TestAReplayAfterAnIncreaseShipsNoEarlierEntryToAnAwaitingMember is T-152.
// The member the increase newly places is down while the bucket is written and
// while the alter is logged, so when it comes back the owner replays its log to
// it from before those writes, reading them at the factor the alter set. The
// member's pull is held as it starts (driven by migrate), so the awaiting
// record still stands while the replay runs. The member's log holds the alter
// and none of the bucket's other entries, and a write with concern all to
// another bucket the member holds is confirmed by every copy.
func TestAReplayAfterAnIncreaseShipsNoEarlierEntryToAnAwaitingMember(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	other := bucketOwnedBy(t, tc, 0, "accounts")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	createBucketAt(tc, 0, testDatabase, other, 3)
	waitForServing(t, tc, testDatabase, other, 3)
	member := nodeIndexByAddr(t, tc.addrs, mustReplicaIDsAt(t, clusterOf(tc, 0), testDatabase, bucket, 3)[1])

	base := tc.stopNodeKeepingData(member)
	for i := 0; i < 5; i++ {
		putDocument(tc, 0, testDatabase, bucket, fmt.Sprintf("before-%d", i))
	}
	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}))
	for i := 0; i < 3; i++ {
		putDocument(tc, 0, testDatabase, bucket, fmt.Sprintf("after-%d", i))
	}
	held, release := holdPullsOf(t, bucket)
	tc.nodes[member] = tc.startNode(member, base, tc.token)
	pulled := make(chan error, 1)
	go func(c *Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		pulled <- c.ensureMigrated(ctx, testDatabase, bucket)
	}(clusterOf(tc, member))
	awaitHeld(t, held, "the new member's pull")

	waitForBarrier(t, tc, 0, "barrier", member)
	if alters := walOpCount(t, tc.nodes[member].server.core.wal, transport.OpAlterBucket, bucket); alters != 1 {
		t.Errorf("node %d, awaiting its copy, logged %d alters of bucket %q, want 1", member+1, alters, bucket)
	}
	if puts := walOpCount(t, tc.nodes[member].server.core.wal, transport.OpPut, bucket); puts != 0 {
		t.Errorf("node %d, awaiting its copy, was replayed %d of the bucket's writes, want none", member+1, puts)
	}
	requireWriteReachesEveryCopy(t, tc, 0, other, "after-the-replay", 3)

	release()
	select {
	case err := <-pulled:
		if err != nil {
			t.Fatalf("node %d's migrate of bucket %q failed: %v", member+1, bucket, err)
		}
	case <-time.After(60 * time.Second):
		t.Fatalf("node %d's migrate of bucket %q never returned", member+1, bucket)
	}
}

// TestARestartedOwnerStillRoutesNothingToAnAwaitingMember is T-56: the
// awaiting map lives in memory, and an owner that restarts after an increase
// rebuilds it from its records, so its first write after the restart is not
// routed to the members still awaiting their copies.
func TestARestartedOwnerStillRoutesNothingToAnAwaitingMember(t *testing.T) {
	// Auto-rebalance off: the copy stays pending, which the pending-copy
	// worker would otherwise make at once.
	tc := newTestClusterWith(t, 3, 1, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}))

	tc.restartNode(0)
	putDocument(tc, 0, testDatabase, bucket, "after-the-restart")
	requireNothingOfTheBucketReachedTheAwaitingMembers(t, tc, bucket)
}

// requireNothingOfTheBucketReachedTheAwaitingMembers waits until both other
// nodes of a three-node cluster whose owner is node 1 have logged a later
// database create, and fails if either was sent a write of bucket or if the
// owner's stream to either is stalled.
func requireNothingOfTheBucketReachedTheAwaitingMembers(t *testing.T, tc *testCluster, bucket string) {
	t.Helper()
	waitForBarrier(t, tc, 0, "barrier", 1, 2)
	for member := 1; member <= 2; member++ {
		if puts := walOpCount(t, tc.nodes[member].server.core.wal, transport.OpPut, bucket); puts != 0 {
			t.Errorf("node %d, awaiting its copy, was sent %d writes of bucket %q", member+1, puts, bucket)
		}
		if !peerOf(t, tc, 0, member).inSync() {
			t.Errorf("the owner's stream to node %d, awaiting its copy, is stalled", member+1)
		}
	}
}

// pendingOn names count buckets owned by node 1, and the node their walk at
// three places third. A ring walks the members after the owner in one fixed
// order, so it is the same node for each: created at 2 and raised to 3, each
// is pending on that member alone.
func pendingOn(t *testing.T, tc *testCluster, count int, prefix string) (names []string, member int) {
	t.Helper()
	c := clusterOf(tc, 0)
	member = -1
	for attempt := 0; attempt < 4000 && len(names) < count; attempt++ {
		name := fmt.Sprintf("%s%04d", prefix, attempt)
		walk := mustReplicaIDsAt(t, c, testDatabase, name, 3)
		if walk[0] != c.self.ID {
			continue
		}
		third := nodeIndexByAddr(t, tc.addrs, walk[2])
		if member >= 0 && third != member {
			t.Fatalf("bucket %q of node 1 is placed third on node %d, and an earlier one on node %d", name, third+1, member+1)
		}
		member = third
		names = append(names, name)
	}
	if len(names) < count {
		t.Fatalf("found %d of %d bucket names owned by node 1", len(names), count)
	}
	return names, member
}

// TestPendingCopiesStallNothingAndOnePullCostsOneCopy is T-39, in two halves:
// a pull driven by migrate, the manual form, with auto-rebalance off; and one
// driven by the member's pending-copy worker with it on (OQ-G3-5;
// bucket_rf_worker_test.go).
func TestPendingCopiesStallNothingAndOnePullCostsOneCopy(t *testing.T) {
	t.Run("migrate", pendingCopiesStallNothingThroughMigrate)
	t.Run("the worker", pendingCopiesStallNothingThroughTheWorker)
}

// pendingCopiesStallNothingThroughMigrate is T-39's migrate half. Two buckets
// are pending on one member, and nothing pulls them: auto-rebalance is off,
// on every node because which member they await is known only once the ring
// is. A third bucket, which every node holds, keeps every copy across a
// hundred rounds of writes to all three. Then each pending bucket is pulled in
// turn, held after its snapshot and before it lands: a write of the bucket in
// that window is routed to the member, which refuses it, so the third bucket
// loses the member from its in-sync set while the pull runs (spec §7.3, the
// documented limit), and regains it once the copy has landed.
func pendingCopiesStallNothingThroughMigrate(t *testing.T) {
	tc := newTestClusterWith(t, 3, 1, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	pending, member := pendingOn(t, tc, 2, "pending")
	third := bucketOwnedBy(t, tc, 0, "third")
	createBucketAt(tc, 0, testDatabase, third, 3)
	waitForServing(t, tc, testDatabase, third, 3)
	for _, bucket := range pending {
		createBucketAt(tc, 0, testDatabase, bucket, 2)
		waitForServing(t, tc, testDatabase, bucket, 2)
		answer := alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}))
		if len(answer.Pending) != 1 || answer.Pending[0] != tc.addrs[member] {
			t.Fatalf("the increase of %q names %v as pending, want only node %d", bucket, answer.Pending, member+1)
		}
	}

	for round := 0; round < 100; round++ {
		for _, bucket := range pending {
			putDocument(tc, 0, testDatabase, bucket, fmt.Sprintf("w%03d", round))
		}
		requireWriteReachesEveryCopy(t, tc, 0, third, fmt.Sprintf("w%03d", round), 3)
	}
	waitForBarrier(t, tc, 0, "barrier", member)
	for _, bucket := range pending {
		if puts := walOpCount(t, tc.nodes[member].server.core.wal, transport.OpPut, bucket); puts != 0 {
			t.Errorf("node %d, awaiting its copy of %q, was sent %d of its writes", member+1, bucket, puts)
		}
	}

	for _, bucket := range pending {
		held, release := holdBucketAt(t, &beforeMaterializeHook, bucket)
		pulled := make(chan error, 1)
		go func(c *Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			pulled <- c.ensureMigrated(ctx, testDatabase, bucket)
		}(clusterOf(tc, member))
		awaitHeld(t, held, fmt.Sprintf("node %d's pull of %q", member+1, bucket))
		putDocument(tc, 0, testDatabase, bucket, "during-the-pull")
		probe := 0
		confirmsOn := func(want int32) func() bool {
			return func() bool {
				probe++
				return writeConfirms(t, tc, 0, third, fmt.Sprintf("probe-%s-%03d", bucket, probe)) == want
			}
		}
		if !holdsWithin(10*time.Second, confirmsOn(2)) {
			t.Fatalf("node %d never left the in-sync set of %q while its pull of %q ran: the snapshot did not clear its awaiting record",
				member+1, third, bucket)
		}
		release()
		select {
		case err := <-pulled:
			if err != nil {
				t.Fatalf("node %d's migrate of %q failed: %v", member+1, bucket, err)
			}
		case <-time.After(60 * time.Second):
			t.Fatalf("node %d's migrate of %q never returned", member+1, bucket)
		}
		if !holdsWithin(20*time.Second, confirmsOn(3)) {
			t.Fatalf("node %d never rejoined the in-sync set of %q once its copy of %q landed", member+1, third, bucket)
		}
	}
}

// writeConfirms writes one document to bucket through node index with write
// concern all and returns how many copies confirmed it, 0 when it was refused.
func writeConfirms(t *testing.T, tc *testCluster, index int, bucket, key string) int32 {
	t.Helper()
	frame := tc.frame(index, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: key,
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	if frame.Opcode == transport.OpError {
		return 0
	}
	confirmed, _, _, _ := copiesConfirmedOf(frame.Payload)
	return confirmed
}

// cededPlacement is a bucket of testDatabase on a four-node cluster that a
// fifth node, joining, takes over: owner holds it first before the join, and
// after the join owner, the member that held it second or third (keeper) and
// the member every walk from owner puts fourth (awaiting) are in its set at 4.
// Created at 3 and raised to 4 before the join, it is pending on awaiting,
// which the old owner's replay and its follower's wait must both leave out.
type cededPlacement struct {
	bucket                  string
	owner, keeper, awaiting int
}

// findCededWithOneAwaiting finds a cededPlacement. A ring walks the members
// after an owner in one fixed order, and the joiner's place in it is fixed by
// its identity, so the old owner is searched for as well as the name. It
// reports false when no name qualifies: whether one does depends on where the
// joiner's drawn identity falls in the ring, so the caller draws again.
func findCededWithOneAwaiting(t *testing.T, tc *testCluster, joiner string) (cededPlacement, bool) {
	t.Helper()
	before := ringWith(t, 3, tc.addrs...)
	after := ringWith(t, 3, append(append([]string(nil), tc.addrs...), joiner)...)
	for attempt := 0; attempt < 8000; attempt++ {
		name := fmt.Sprintf("ceded%04d", attempt)
		key := placementOf(testDatabase, name).encoded
		oldSet, err := before.Replicas(key, 4)
		if err != nil {
			t.Fatalf("placing %s before the join: %v", name, err)
		}
		newSet, err := after.Replicas(key, 4)
		if err != nil {
			t.Fatalf("placing %s after the join: %v", name, err)
		}
		if newSet[0].ID != joiner || !nodeListed(newSet, oldSet[0].ID) || !nodeListed(newSet, oldSet[3].ID) {
			continue
		}
		var kept []int
		for _, holder := range oldSet[1:3] {
			if nodeListed(newSet, holder.ID) {
				kept = append(kept, nodeIndexByAddr(t, tc.addrs, holder.ID))
			}
		}
		if len(kept) == 1 {
			return cededPlacement{bucket: name, owner: nodeIndexByAddr(t, tc.addrs, oldSet[0].ID), keeper: kept[0],
				awaiting: nodeIndexByAddr(t, tc.addrs, oldSet[3].ID)}, true
		}
	}
	return cededPlacement{}, false
}

// TestTermDeliveredWaitsForExactlyTheMembersTheCededReplayShipsTo is T-153.
// A bucket at 3 on four nodes is raised to 4, so the fourth node of its walk
// awaits a copy; then a fifth node joins and takes the bucket over, and its old
// owner is ceded. The old owner's follower may stop waiting for its own term's
// delivery once every member its ceded replay ships to has acknowledged it:
// the other holder that stays in the set, and not the awaiting member, which
// is shipped nothing. The old owner's follow round is held so it stays in its
// term, every pull of the bucket is held so the awaiting member, now in the
// joiner's set, does not copy it in, and the old owner's stream to the other
// holder is frozen across the last write, then released.
func TestTermDeliveredWaitsForExactlyTheMembersTheCededReplayShipsTo(t *testing.T) {
	tc := newTestCluster(t, 4, 1)
	defer tc.stop()
	// The joiner's ports fix its place in the ring; some draws leave no name
	// that fits, so draw again rather than fail the setup (BUG-0487 family).
	var (
		ports  *testNodePorts
		placed cededPlacement
		found  bool
	)
	for draw := 0; draw < 20 && !found; draw++ {
		ports = reserveNodePorts(t)
		placed, found = findCededWithOneAwaiting(t, tc, ports.ringNode().ID)
	}
	if !found {
		t.Fatalf("setup: no port draw in 20 gave a joiner that owns a ceded bucket with its old owner and awaiting member staying in its set")
	}
	bucket := placed.bucket
	tc.ok(placed.owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	createBucketAt(tc, placed.owner, testDatabase, bucket, 3)
	waitForServing(t, tc, testDatabase, bucket, 3)
	// Held before the alter: the awaiting member's pending-copy worker pulls
	// as soon as the alter reaches it.
	_, releasePulls := holdPullsOf(t, bucket)
	defer releasePulls()
	answer := alterAccepted(t, alterFrame(t, tc, placed.owner, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 4}))
	if len(answer.Pending) != 1 || answer.Pending[0] != tc.addrs[placed.awaiting] {
		t.Fatalf("the increase names %v as pending, want only node %d", answer.Pending, placed.awaiting+1)
	}
	for i := 0; i < 3; i++ {
		putDocument(tc, placed.owner, testDatabase, bucket, fmt.Sprintf("after-%d", i))
	}

	owner := clusterOf(tc, placed.owner)
	key := placementOf(testDatabase, bucket)
	followHeld := make(chan struct{})
	hold := func(nodeID, _, name string) {
		if nodeID == owner.self.ID && name == bucket {
			<-followHeld
		}
	}
	takeOverBeforeReadHook.Store(&hold)
	defer takeOverBeforeReadHook.Store(nil)
	defer close(followHeld)
	unfreeze := freezePeer(owner.peerFor(mustNode(t, owner, tc.addrs[placed.keeper])))
	defer unfreeze()
	putDocumentAt(tc, placed.owner, testDatabase, bucket, "frozen", "one")

	joinCtx, cancelJoin := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelJoin()
	if err := owner.proposeJoin(joinCtx, ports.ringNode()); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	joiner := startJoinTarget(t, tc, ports, tc.token)
	defer func() { _ = joiner.client.Close() }()
	pollUntil(t, "the joiner adopts the join", func() bool { return joiner.server.core.cluster.Epoch() >= owner.Epoch() })
	if owner.actsAsOwner(key) {
		t.Fatalf("node %d still acts as the owner of %q after the join", placed.owner+1, bucket)
	}
	own, err := owner.answerCopy(ReplBucketRef{Database: testDatabase, Bucket: bucket})
	if err != nil || !own.Holds || !own.Opened {
		t.Fatalf("node %d's copy of %q after the join: %+v, %v", placed.owner+1, bucket, own, err)
	}

	if holdsWithin(time.Second, func() bool { return owner.termDelivered(key, own) }) {
		t.Fatalf("node %d's term counts as delivered while its stream to node %d, which holds the bucket, is frozen", placed.owner+1, placed.keeper+1)
	}
	unfreeze()
	if !holdsWithin(20*time.Second, func() bool { return owner.termDelivered(key, own) }) {
		t.Fatalf("node %d's term never counts as delivered once node %d acknowledged it: it waits on node %d, which awaits its copy "+
			"and is shipped nothing (acknowledged through %d, the copy's position %d)", placed.owner+1, placed.keeper+1, placed.awaiting+1,
			owner.peerFor(mustNode(t, owner, tc.addrs[placed.awaiting])).ackedThrough(owner.self.ID), own.Through)
	}
}
