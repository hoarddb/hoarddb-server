package server

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The decrease (bucket-replication-rev8-spec §8; the G4 to G7 build doc step
// 4). Before the owner logs a decrease it asks every other member of the old
// set what its copy is, and logs it only when every survivor holds, in the
// owner's term, everything each demoted copy holds (R11). The demoted copies
// are then released by the sweep's own path, which waits on the same
// coverage, and a copy that waits never holds the node's settle state or the
// join freeze.

// decreaseSet is a bucket created at 3, owned by node 1, holding one document
// on every copy, and the node indexes of its walk on the cluster's ring, owner
// first: at a decrease to 2, walk[1] survives and walk[2] is demoted.
type decreaseSet struct {
	tc     *testCluster
	bucket string
	walk   []int
}

// newDecreaseSet starts count nodes at default 3 with configure and creates
// the bucket.
func newDecreaseSet(t *testing.T, count int, configure func(index int, cfg *config.Config)) decreaseSet {
	t.Helper()
	return newDecreaseSetNamed(t, count, configure, func(tc *testCluster) string { return bucketOwnedBy(t, tc, 0, "lowered") })
}

// newDecreaseSetNamed is newDecreaseSet with the bucket's name chosen by name.
func newDecreaseSetNamed(t *testing.T, count int, configure func(index int, cfg *config.Config), name func(tc *testCluster) string) decreaseSet {
	t.Helper()
	tc := newTestClusterWith(t, count, 3, configure)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := name(tc)
	createBucketAt(tc, 0, testDatabase, bucket, 3)
	waitForServing(t, tc, testDatabase, bucket, 3)
	putDocumentAt(tc, 0, testDatabase, bucket, "before", "all")
	walk := make([]int, 0, count)
	for _, id := range mustReplicaIDsAt(t, clusterOf(tc, 0), testDatabase, bucket, count) {
		walk = append(walk, nodeIndexByAddr(t, tc.addrs, id))
	}
	return decreaseSet{tc: tc, bucket: bucket, walk: walk}
}

// alterTo sends an alter of the bucket to factor through node index.
func (ds decreaseSet) alterTo(t *testing.T, index int, factor int32) *transport.Frame {
	t.Helper()
	return alterFrame(t, ds.tc, index, AlterBucketRequest{Database: testDatabase, Name: ds.bucket, ReplicationFactor: factor})
}

// shortenSweepRearm lets a rearm run every interval for the rest of the test.
func shortenSweepRearm(t *testing.T, interval time.Duration) {
	t.Helper()
	restore := sweepRearmInterval.get()
	sweepRearmInterval.set(interval)
	t.Cleanup(func() { sweepRearmInterval.set(restore) })
}

// lowerPastAFrozenSurvivor lowers the bucket from 3 to 2 so that the survivor
// is short of what the demoted copy holds when the alter is logged: R11 asks
// while every copy is current, the alter is held before its fence, the
// owner's stream to the survivor is frozen, a write reaches the owner and the
// demoted copy alone, and the alter is let go. It returns the survivor's
// unfreeze. The spec's own case: writes go on between the question and the
// append (§8.2), so the release waits for the survivor.
func lowerPastAFrozenSurvivor(t *testing.T, ds decreaseSet) (unfreeze func()) {
	t.Helper()
	owner := clusterOf(ds.tc, 0)
	held, release := holdAt(t, &beforeOwnerFenceHook, owner.self.ID, transport.OpAlterBucket, ds.bucket)
	answered := sendInBackground(t, owner, transport.OpAlterBucket, AlterBucketRequest{Database: testDatabase, Name: ds.bucket, ReplicationFactor: 2})
	awaitHeld(t, held, "the decrease before its fence")
	unfreeze = freezePeer(owner.peerFor(mustNode(t, owner, ds.tc.addrs[ds.walk[1]])))
	putDocument(ds.tc, 0, testDatabase, ds.bucket, "between")
	if !holdsKeyIn(ds.tc.nodes[ds.walk[2]], testDatabase, ds.bucket, "between") {
		t.Fatalf("setup: the demoted node %d does not hold the write made between the question and the append", ds.walk[2]+1)
	}
	close(release)
	alterAccepted(t, awaitAnswer(t, answered, "the decrease"))
	if holdsKeyIn(ds.tc.nodes[ds.walk[1]], testDatabase, ds.bucket, "between") {
		t.Fatalf("setup: the frozen survivor node %d holds the write made between the question and the append", ds.walk[1]+1)
	}
	return unfreeze
}

// lowerToTwoEverywhere lowers the bucket to 2 and waits until the demoted
// node has applied the decrease: the owner answers once it has applied it.
func lowerToTwoEverywhere(t *testing.T, ds decreaseSet) {
	t.Helper()
	lowerUntilAccepted(t, ds, 0, 2)
	demoted := ds.tc.nodes[ds.walk[2]].server.core
	pollUntil(t, fmt.Sprintf("the demoted node %d applies the decrease", ds.walk[2]+1), func() bool {
		return heldFactorOf(t, demoted, ds.bucket).catalog == 2
	})
}

// residueLists reports whether c's recorded residue names the bucket.
func residueLists(c *Cluster, bucket string) bool {
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	return slices.Contains(c.residue.list, heldBucket{database: testDatabase, bucket: bucket})
}

// joinFreezePaused reports whether c's last join-freeze report pauses schema
// changes.
func joinFreezePaused(c *Cluster) bool {
	report := c.joinFreeze.Load()
	return report != nil && report.Paused
}

// requireDemotedCopyWaits holds for long enough for several rearms and fails
// if the demoted node released its copy, moved its settle state, recorded the
// copy in its residue or paused schema changes.
func requireDemotedCopyWaits(t *testing.T, ds decreaseSet, settled uint64, when string) {
	t.Helper()
	demoted := ds.tc.nodes[ds.walk[2]].server.core
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if !demoted.bucketStoreOpen(testDatabase, ds.bucket) {
			t.Fatalf("%s: the demoted node %d released its copy while the survivor node %d does not hold everything it holds",
				when, ds.walk[2]+1, ds.walk[1]+1)
		}
		if got := demoted.cluster.settled.Load(); got != settled {
			t.Fatalf("%s: the demoted node's settled epoch moved from %d to %d while its copy waits", when, settled, got)
		}
		if residueLists(demoted.cluster, ds.bucket) {
			t.Fatalf("%s: the demoted node records its waiting copy of %q in the sweep's residue", when, ds.bucket)
		}
		if joinFreezePaused(demoted.cluster) {
			t.Fatalf("%s: schema changes are paused while a demoted copy waits", when)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// releaseWithin bounds the wait for a demoted copy's release: the rearm that
// attempts it runs at a healthy gossip round, every 10*peerRetryInterval per
// peer, and the first attempt can find the survivor not caught up yet.
const releaseWithin = 3 * 10 * peerRetryInterval

// requireReleased waits until the demoted node has released its copy.
func requireReleased(t *testing.T, ds decreaseSet) {
	t.Helper()
	demoted := ds.tc.nodes[ds.walk[2]].server.core
	if !holdsWithin(releaseWithin, func() bool { return !demoted.bucketStoreOpen(testDatabase, ds.bucket) }) {
		// Why, from one more attempt by hand on the release's own path.
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		why := demoted.cluster.migrateIfMissing(ctx, testDatabase, ds.bucket)
		t.Fatalf("the demoted node %d never released its copy of %q once every survivor covers it (an attempt now: %v)",
			ds.walk[2]+1, ds.bucket, why)
	}
}

// requireReleasedWithEveryWrite waits until the demoted node has released its
// copy and checks every survivor holds both writes.
func requireReleasedWithEveryWrite(t *testing.T, ds decreaseSet) {
	t.Helper()
	requireReleased(t, ds)
	for _, survivor := range ds.walk[:2] {
		for _, key := range []string{"before", "between"} {
			if !holdsKeyIn(ds.tc.nodes[survivor], testDatabase, ds.bucket, key) {
				t.Errorf("survivor node %d does not hold %q after the demoted copy was released", survivor+1, key)
			}
		}
	}
}

// removeNodeFrom removes the member at index through node proposer, with the
// call the CLI's remove-node ends in, and brings every other running node to
// the new epoch the way gossip would. It returns the new epoch.
func removeNodeFrom(t *testing.T, tc *testCluster, proposer, removed int) uint64 {
	t.Helper()
	proposing := clusterOf(tc, proposer)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := proposing.proposeRemoval(ctx, tc.addrs[removed]); err != nil {
		t.Fatalf("remove node %d: %v", removed+1, err)
	}
	for index, node := range tc.nodes {
		if index == proposer || index == removed || node.server == nil {
			continue
		}
		if err := clusterOf(tc, index).reconcileMembership(tc.addrs[proposer], proposing.memberNodes(), proposing.memberIDs(), proposing.Epoch()); err != nil {
			t.Fatalf("bring node %d to the new epoch: %v", index+1, err)
		}
	}
	return proposing.Epoch()
}

// TestADecreaseReleasesADemotedCopyOnlyAfterEverySurvivorCoversIt is T-08
// (spec §8.3, R9.1-14.9): with the survivor short of what the demoted copy
// holds, the copy stays, and while it waits the demoted node's settle state,
// its residue and the join freeze do not move; once the survivor catches up,
// the copy is released through the sweep with no membership change. Across a
// membership change committed while it waits, the change's full walk visits
// the copy and the node still settles at the new epoch with the copy out of
// its residue.
func TestADecreaseReleasesADemotedCopyOnlyAfterEverySurvivorCoversIt(t *testing.T) {
	t.Run("the alter alone", func(t *testing.T) {
		shortenSweepRearm(t, 200*time.Millisecond)
		ds := newDecreaseSet(t, 3, nil)
		defer ds.tc.stop()
		settled := clusterOf(ds.tc, ds.walk[2]).settled.Load()

		unfreeze := lowerPastAFrozenSurvivor(t, ds)
		defer unfreeze()
		requireDemotedCopyWaits(t, ds, settled, "the survivor frozen")

		unfreeze()
		requireReleasedWithEveryWrite(t, ds)
	})

	t.Run("across a membership change", func(t *testing.T) {
		shortenSweepRearm(t, 200*time.Millisecond)
		ds := newDecreaseSetNamed(t, 4, nil, keptSetWithoutTheFourth(t))
		defer ds.tc.stop()
		demoted := clusterOf(ds.tc, ds.walk[2])
		settled := demoted.settled.Load()

		unfreeze := lowerPastAFrozenSurvivor(t, ds)
		defer unfreeze()
		requireDemotedCopyWaits(t, ds, settled, "the survivor frozen")

		// The fourth node of the walk is outside the bucket's set at both
		// factors, and the bucket is named so that removing it changes the
		// membership and not the walk: a ring that placed the demoted node
		// again would rightly take its copy out of the demoted set.
		epoch := removeNodeFrom(t, ds.tc, ds.walk[0], ds.walk[3])
		want := []string{ds.tc.addrs[ds.walk[0]], ds.tc.addrs[ds.walk[1]]}
		if set := mustReplicaIDsAt(t, demoted, testDatabase, ds.bucket, 2); !slices.Equal(set, want) {
			t.Fatalf("setup: after the removal the demoted node places the bucket on %v, want %v", set, want)
		}
		pollUntil(t, fmt.Sprintf("the demoted node %d settles at epoch %d while its copy waits", ds.walk[2]+1, epoch), func() bool {
			return demoted.settled.Load() >= epoch
		})
		if residueLists(demoted, ds.bucket) {
			t.Errorf("the membership change's sweep recorded the waiting copy of %q in the demoted node's residue", ds.bucket)
		}
		if !ds.tc.nodes[ds.walk[2]].server.core.bucketStoreOpen(testDatabase, ds.bucket) {
			t.Fatalf("the membership change's sweep released the demoted copy while the survivor node %d is frozen", ds.walk[1]+1)
		}
		requireDemotedCopyWaits(t, ds, demoted.settled.Load(), "after the membership change")

		unfreeze()
		requireReleasedWithEveryWrite(t, ds)
	})
}

// lowerUntilAnswered sends the decrease to factor through node index until it
// is accepted or refused by R11, retrying every other UNAVAILABLE refusal (a
// start-up hold, R6, R7), and returns the last frame.
func lowerUntilAnswered(t *testing.T, ds decreaseSet, index int, factor int32) *transport.Frame {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		frame := ds.alterTo(t, index, factor)
		if frame.Opcode != transport.OpError || strings.Contains(refusalOf(t, frame).Message, "cannot be lowered from") {
			return frame
		}
		if time.Now().After(deadline) {
			t.Fatalf("the decrease of %q through node %d was never answered for its coverage: %s", ds.bucket, index+1, refusalOf(t, frame).Message)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// lowerUntilAccepted retries the decrease through node index until it is
// accepted.
func lowerUntilAccepted(t *testing.T, ds decreaseSet, index int, factor int32) AlterBucketResponse {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		frame := ds.alterTo(t, index, factor)
		if frame.Opcode != transport.OpError {
			return alterAccepted(t, frame)
		}
		if time.Now().After(deadline) {
			t.Fatalf("the decrease of %q through node %d was never accepted: %s", ds.bucket, index+1, refusalOf(t, frame).Message)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// requireCoverageRefusal fails unless frame is R11's refusal of the decrease
// from 3 to factor naming survivor and the demoted members, in its wire class,
// and no node logged an alter.
func requireCoverageRefusal(t *testing.T, ds decreaseSet, frame *transport.Frame, factor int32, survivor, owner string, demoted ...string) string {
	t.Helper()
	if frame.Opcode != transport.OpError {
		var answer AlterBucketResponse
		if err := bson.Unmarshal(frame.Payload, &answer); err == nil {
			t.Fatalf("the decrease was answered success %v without R11's refusal: %q", answer.Success, answer.Message)
		}
		t.Fatalf("the decrease was answered without R11's refusal: %s", frame.Payload)
	}
	refused := requireUnavailableNotApplied(t, frame,
		fmt.Sprintf("bucket %q cannot be lowered from replication_factor 3 to %d: %s has not shown that it holds everything %s",
			ds.bucket, factor, survivor, joinNames(demoted)),
		fmt.Sprintf("Run 'show buckets' on %s and retry when the row reads 3/3.", owner))
	requireNoAlterLogged(t, ds.tc, ds.bucket)
	return refused.Message
}

// TestADecreaseIsRefusedUntilTheSurvivorsAreShownToCoverIt is T-58 (spec §8.2,
// rev 3 F-12): after the owner restarts, a decrease with a survivor stopped is
// refused UNAVAILABLE and not_applied, naming the survivor and remove-node, and
// is logged nowhere; once the survivor is back it is accepted and the demoted
// copy is released.
func TestADecreaseIsRefusedUntilTheSurvivorsAreShownToCoverIt(t *testing.T) {
	shortenSweepRearm(t, 200*time.Millisecond)
	ds := newDecreaseSet(t, 3, nil)
	defer ds.tc.stop()
	ds.tc.restartNode(0)
	waitTakenOver(t, ds.tc.nodes[0].server.core, testDatabase, ds.bucket)
	survivor, demoted := ds.tc.addrs[ds.walk[1]], ds.tc.addrs[ds.walk[2]]
	startSurvivor := stopNode(t, ds.tc, ds.walk[1])

	message := requireCoverageRefusal(t, ds, lowerUntilAnswered(t, ds, 0, 2), 2, survivor, ds.tc.addrs[0], demoted)
	if remedy := fmt.Sprintf("If %s is not coming back, run 'remove-node %s' first.", survivor, survivor); !strings.Contains(message, remedy) {
		t.Errorf("the refusal does not end with %q: %q", remedy, message)
	}

	startSurvivor()
	lowerUntilAccepted(t, ds, 0, 2)
	requireReleased(t, ds)
}

// keptSetWithoutTheFourth names a bucket node 1 owns whose walk on the ring
// without its fourth holder keeps the first three in the same order.
func keptSetWithoutTheFourth(t *testing.T) func(tc *testCluster) string {
	return func(tc *testCluster) string {
		t.Helper()
		before := ringWith(t, 4, tc.addrs...)
		for attempt := 0; attempt < 4000; attempt++ {
			name := fmt.Sprintf("kept%04d", attempt)
			key := placementOf(testDatabase, name).encoded
			walk, err := before.Replicas(key, 4)
			if err != nil {
				t.Fatalf("placing %s: %v", name, err)
			}
			if walk[0].ID != tc.addrs[0] {
				continue
			}
			var rest []string
			for _, addr := range tc.addrs {
				if addr != walk[3].ID {
					rest = append(rest, addr)
				}
			}
			kept, err := ringWith(t, 3, rest...).Replicas(key, 3)
			if err != nil {
				t.Fatalf("placing %s without its fourth holder: %v", name, err)
			}
			if slices.Equal(nodeIDs(kept), nodeIDs(walk[:3])) {
				return name
			}
		}
		t.Fatal("no bucket name of node 1's keeps its set once its fourth holder is removed")
		return ""
	}
}

// keptOrderWithoutNodeOne names a bucket node 1 owns whose walk on the ring
// without node 1 is the rest of its walk in the same order: once node 1 is
// removed, its second holder owns the bucket and its third is a survivor.
func keptOrderWithoutNodeOne(t *testing.T) func(tc *testCluster) string {
	return func(tc *testCluster) string {
		t.Helper()
		before := ringWith(t, 4, tc.addrs...)
		after := ringWith(t, 3, tc.addrs[1:]...)
		for attempt := 0; attempt < 4000; attempt++ {
			name := fmt.Sprintf("departed%04d", attempt)
			key := placementOf(testDatabase, name).encoded
			walk, err := before.Replicas(key, 4)
			if err != nil {
				t.Fatalf("placing %s: %v", name, err)
			}
			kept, err := after.Replicas(key, 3)
			if err != nil {
				t.Fatalf("placing %s without node 1: %v", name, err)
			}
			if walk[0].ID == tc.addrs[0] && slices.Equal(nodeIDs(kept), nodeIDs(walk[1:])) {
				return name
			}
		}
		t.Fatal("no bucket name of node 1's keeps the order of its other holders once node 1 is removed")
		return ""
	}
}

// TestASurvivorBehindADepartedOwnerRefusesTheDecrease is T-178 (rev 4 F-8): a
// survivor left in the stopped and removed owner's term, its take-over paused, has not
// followed the new owner's term, and the decrease is refused naming it; the
// question compares lineages, which survive an owner change, never a stream's
// acknowledgements. Once it follows, the decrease is accepted.
func TestASurvivorBehindADepartedOwnerRefusesTheDecrease(t *testing.T) {
	ds := newDecreaseSetNamed(t, 4, nil, keptOrderWithoutNodeOne(t))
	defer ds.tc.stop()
	newOwner, behind, placed := ds.walk[1], ds.walk[2], ds.walk[3]
	behindID := ds.tc.addrs[behind]
	paused := func(nodeID string) bool { return nodeID == behindID }
	takeOverPausedHook.Store(&paused)
	t.Cleanup(func() { takeOverPausedHook.Store(nil) })

	// The owner departs: stopped, then removed, as an operator removes a node
	// that is not coming back.
	stopNode(t, ds.tc, ds.walk[0])
	removeNodeFrom(t, ds.tc, newOwner, ds.walk[0])
	owner := ds.tc.nodes[newOwner].server.core
	want := []string{ds.tc.addrs[newOwner], behindID, ds.tc.addrs[placed]}
	if walk := mustReplicaIDsAt(t, owner.cluster, testDatabase, ds.bucket, 3); !slices.Equal(walk, want) {
		t.Fatalf("setup: without the departed owner the bucket's set is %v, want %v", walk, want)
	}
	waitTakenOver(t, owner, testDatabase, ds.bucket)
	ref := ReplBucketRef{Database: testDatabase, Bucket: ds.bucket}
	ownerCopy, err := owner.cluster.answerCopy(ref)
	if err != nil {
		t.Fatalf("reading the new owner's copy: %v", err)
	}
	behindCopy, err := ds.tc.nodes[behind].server.core.cluster.answerCopy(ref)
	if err != nil {
		t.Fatalf("reading the paused survivor's copy: %v", err)
	}
	if !behindCopy.Holds || behindCopy.Term == ownerCopy.Term {
		t.Fatalf("setup: the paused survivor's copy is %+v, the new owner's %+v; want it held in the departed owner's term", behindCopy, ownerCopy)
	}

	requireCoverageRefusal(t, ds, lowerUntilAnswered(t, ds, newOwner, 2), 2, behindID, ds.tc.addrs[newOwner], ds.tc.addrs[placed])

	takeOverPausedHook.Store(nil)
	waitFollowing(t, ds.tc.nodes[behind].server.core, owner, testDatabase, ds.bucket)
	lowerUntilAccepted(t, ds, newOwner, 2)
}

// TestADecreaseToOneChecksTheOwnersCopy is T-179 (spec §8.2): the owner's own
// copy is a survivor, read under its fence. An owner whose copy owes an apply
// its store failed is not shown to hold what the demoted copies hold, so a
// decrease to 1 is refused naming the owner itself.
func TestADecreaseToOneChecksTheOwnersCopy(t *testing.T) {
	ds := newDecreaseSet(t, 3, nil)
	defer ds.tc.stop()
	owner := ds.tc.nodes[0].server.core
	failNextApplyOn(t, owner.kv, anySeq)
	ds.tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: ds.bucket, Key: "failed", Data: map[string]interface{}{"n": 1}})
	store.ApplyFaultHook.Store(nil)
	putDocument(ds.tc, 0, testDatabase, ds.bucket, "after")
	ownerCopy, err := owner.cluster.answerCopy(ReplBucketRef{Database: testDatabase, Bucket: ds.bucket})
	if err != nil || !ownerCopy.Owes {
		t.Fatalf("setup: the owner's copy is %+v (%v), want it owing the apply its store failed", ownerCopy, err)
	}

	requireCoverageRefusal(t, ds, ds.alterTo(t, 0, 1), 1, ds.tc.addrs[0], ds.tc.addrs[0], ds.tc.addrs[ds.walk[1]], ds.tc.addrs[ds.walk[2]])
}

// TestADemotedCopyServesNoRead is T-180 (spec §8.4): with auto-rebalance off
// the demoted copy stays, unserved; a read through the demoted node is not
// answered from its own copy, frozen at the alter, but from the bucket's set.
func TestADemotedCopyServesNoRead(t *testing.T) {
	ds := newDecreaseSet(t, 3, autoRebalanceOff)
	defer ds.tc.stop()
	putDocumentWith(ds.tc, 0, testDatabase, ds.bucket, "k", map[string]interface{}{"v": "before"})
	lowerToTwoEverywhere(t, ds)
	upsertDocumentWith(ds.tc, 0, testDatabase, ds.bucket, "k", map[string]interface{}{"v": "after"})
	demoted := ds.tc.nodes[ds.walk[2]]
	if !demoted.server.core.bucketStoreOpen(testDatabase, ds.bucket) {
		t.Fatalf("setup: with auto-rebalance off the demoted node %d released its copy", ds.walk[2]+1)
	}

	frame := ds.tc.frame(ds.walk[2], transport.OpGet, FindRequest{Database: testDatabase, Bucket: ds.bucket, Key: "k"})
	if frame.Opcode == transport.OpError {
		t.Fatalf("the read through the demoted node failed: %s", frame.Payload)
	}
	var found FindResponse
	if err := bson.Unmarshal(frame.Payload, &found); err != nil {
		t.Fatalf("decoding the read: %v", err)
	}
	if got := found.Data.Lookup("v").StringValue(); !found.Found || got != "after" {
		t.Errorf("a read through the demoted node %d answered found %v, v %q; want the set's %q, not its own copy frozen at the alter",
			ds.walk[2]+1, found.Found, got, "after")
	}
}

// TestMigrateOnADemotedNodeIsRefused is T-15, a guard: Cluster.ensureMigrated's
// held branch refuses a migrate on a copy outside its set, naming the set
// (refuseMigrateFromOutsideTheSet), so an operator cannot keep a demoted copy
// by migrating it.
func TestMigrateOnADemotedNodeIsRefused(t *testing.T) {
	ds := newDecreaseSet(t, 3, autoRebalanceOff)
	defer ds.tc.stop()
	lowerToTwoEverywhere(t, ds)
	demoted := ds.tc.nodes[ds.walk[2]].server.core
	if !demoted.bucketStoreOpen(testDatabase, ds.bucket) {
		t.Fatalf("setup: with auto-rebalance off the demoted node %d released its copy", ds.walk[2]+1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := demoted.cluster.ensureMigrated(ctx, testDatabase, ds.bucket)
	if err == nil {
		t.Fatal("migrate on the node a decrease demoted was told it worked")
	}
	if !strings.Contains(err.Error(), "replica set") || !strings.Contains(err.Error(), ds.tc.addrs[ds.walk[1]]) {
		t.Errorf("the refusal does not name the set: %v", err)
	}
}

// TestALaterAlterSupersedesAPendingIncrease is T-185a (R9-14.13, owner R9-Q1):
// 3 to 5 onto a full fourth member, whose pull is refused for its disk, so the
// owner's awaiting record stands; 5 to 4, which still places it, is refused
// R8; 5 to 3 passes R11 (the fourth holds nothing, the fifth is covered) and
// supersedes, so the owner's awaiting record and the fourth's needs-a-copy
// record and queue entry are gone.
func TestALaterAlterSupersedesAPendingIncrease(t *testing.T) {
	tc := newTestClusterWith(t, 5, 1, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = true })
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "superseded")
	createBucketAt(tc, 0, testDatabase, bucket, 3)
	waitForServing(t, tc, testDatabase, bucket, 3)
	owner := tc.nodes[0].server.core
	key := placementOf(testDatabase, bucket)
	walk := mustReplicaIDsAt(t, owner.cluster, testDatabase, bucket, 5)
	full, fifth := nodeIndexByAddr(t, tc.addrs, walk[3]), nodeIndexByAddr(t, tc.addrs, walk[4])
	fullCore := tc.nodes[full].server.core
	setCapacityProbes(t, fullCore, func(p *capacityProbes) { p.diskStatfs = fixedDisk(diskFixture(92)) })

	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 5}))
	waitForPendingCopyLanded(t, tc, fifth, bucket)
	pollUntil(t, fmt.Sprintf("node %d queues its pending copy of %q while its disk refuses the pull", full+1, bucket), func() bool {
		_, pending := fullCore.cluster.pendingCopyOf(key)
		return pending
	})
	if pending := sortedAwaiting(owner.cluster, key); !slices.Equal(pending, []string{walk[3]}) {
		t.Fatalf("setup: the owner awaits %v, want the full member %s alone", pending, walk[3])
	}

	requireUnavailableNotApplied(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 4}),
		fmt.Sprintf("is still copying to %s", walk[3]))

	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}))
	if pending := awaitingInMap(owner.cluster, key); len(pending) != 0 {
		t.Errorf("after 5 to 3 the owner's map still awaits %v", pending)
	}
	if onDisk := awaitingOnDisk(t, owner, key); len(onDisk) != 0 {
		t.Errorf("after 5 to 3 the owner's store still holds awaiting records for %v", onDisk)
	}
	pollUntil(t, fmt.Sprintf("node %d deletes its needs-a-copy record of %q", full+1, bucket), func() bool {
		_, recorded := needsCopyOnDisk(t, fullCore, bucket)
		return !recorded
	})
	requireNotQueued(t, fullCore, bucket, "after the alter that no longer places it")
}
