package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestAnOwnerRegainingItsOutsideCopyKeepsWhatOnlyItHolds: an owner regains a
// bucket whose copy it kept outside the set, and that copy is the best one any
// remaining node holds. It opens it where it lies — the outside mark from the
// change that took the bucket away says nothing about the copy's content —
// and never deletes it to copy from itself. Three nodes at factor 2. O owns b and
// alone holds five writes at one (its stream to R held). A join makes the
// newcomer J the owner and puts O outside (its hand-off held); J copies from O
// and opens. R's worker is paused, so R stays behind. J is stopped and
// removed: O owns b again, and its own outside-marked copy is the best that
// any remaining node holds. O must serve the five. Guard: copy the best copy
// in when it is this node's own and marked outside — it is deleted, the pull
// from itself fails, and the next round takes R's poorer copy.
func TestAnOwnerRegainingItsOutsideCopyKeepsWhatOnlyItHolds(t *testing.T) {
	tc := newTestCluster(t, 3, 2)
	defer tc.stop()
	shortenSweeps(t, 0)
	before := ringWith(t, 2, tc.addrs...)
	var owner, replica int
	// regained is the test's placement for joiner: a name it owns once it
	// joins, whose owner leaves the set and whose replica stays in it.
	regained := func(joiner string) func(name string) bool {
		joined := ringWith(t, 2, append(append([]string(nil), tc.addrs...), joiner)...)
		return func(name string) bool {
			key := placementOf(testDatabase, name).encoded
			old, _ := before.Replicas(key, 2)
			next, _ := joined.Replicas(key, 2)
			if next[0].ID != joiner || nodeListed(next, old[0].ID) || !nodeListed(next, old[1].ID) {
				return false
			}
			owner, replica = nodeIndexByAddr(t, tc.addrs, old[0].ID), nodeIndexByAddr(t, tc.addrs, old[1].ID)
			return true
		}
	}
	ports := reserveJoinerPlaced(t, func(joiner string) bool {
		_, found := findBucketName("regain", regained(joiner))
		return found
	})
	newcomer := ports.ringNode()
	bucket := bucketNamed(t, "regain", regained(newcomer.ID))
	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 2)
	releaseR := holdStreamPushesOnly(peerOf(t, tc, owner, replica))
	defer releaseR()
	for i := 0; i < 5; i++ {
		tc.ok(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("k%d", i),
			Data: map[string]interface{}{"n": i}, WriteConcern: "one"})
	}
	ownerID, replicaID := tc.addrs[owner], tc.addrs[replica]
	keep := func(fromID, _, _, name string) error {
		if fromID == ownerID && name == bucket {
			return errors.New("test: the old owner's hand-off is held")
		}
		return nil
	}
	askNodeToMigrateHook.Store(&keep)
	defer askNodeToMigrateHook.Store(nil)
	paused := func(nodeID string) bool { return nodeID == replicaID }
	takeOverPausedHook.Store(&paused)
	defer takeOverPausedHook.Store(nil)
	joinFreezeOffHook.Store(true)
	defer joinFreezeOffHook.Store(false)
	joiner := joinNewcomer(t, tc, owner, ports)
	if !holdsWithin(takeOverWait, func() bool {
		joiner.cluster().wakeTakeOver(testDatabase, bucket)
		core := joiner.node.server.core
		_, opened, err := core.bucketLineage(testDatabase, bucket)
		return core.bucketStoreOpen(testDatabase, bucket) && err == nil && opened && holdsKeyLocally(joiner.node, bucket, "k4")
	}) {
		t.Fatalf("setup: the newcomer never took %s over", bucket)
	}
	ownerCore := tc.nodes[owner].server.core
	key := placementOf(testDatabase, bucket)
	if !ownerCore.bucketStoreOpen(testDatabase, bucket) || ownerCore.cluster.placementMarksOf(key).Outside == 0 {
		t.Fatalf("setup: the old owner's copy is not kept outside the set: open=%v marks=%+v",
			ownerCore.bucketStoreOpen(testDatabase, bucket), ownerCore.cluster.placementMarksOf(key))
	}
	if holdsKeyLocally(tc.nodes[replica], bucket, "k0") {
		t.Fatalf("setup: the replica holds the writes")
	}
	joiner.stop()
	removeAndForget(t, tc, owner, newcomer.ID)
	replicas, err := ownerCore.cluster.Replicas(key)
	if err != nil || replicas[0].ID != ownerID {
		t.Fatalf("setup: node %d does not own %s again: %v %v", owner+1, bucket, replicas, err)
	}
	t.Logf("old owner after removal: marks=%+v open=%v k4=%v", ownerCore.cluster.placementMarksOf(key),
		ownerCore.bucketStoreOpen(testDatabase, bucket), holdsKeyLocally(tc.nodes[owner], bucket, "k4"))
	var lost bool
	released := make(chan string, 4)
	afterRelease := func(nodeID, _, name string) {
		if name == bucket {
			released <- nodeID
		}
	}
	takeOverAfterReleaseHook.Store(&afterRelease)
	defer takeOverAfterReleaseHook.Store(nil)
	settled := holdsWithin(takeOverWait, func() bool {
		ownerCore.cluster.wakeTakeOver(testDatabase, bucket)
		open := ownerCore.bucketStoreOpen(testDatabase, bucket)
		if !open {
			lost = true
		}
		_, opened, err := ownerCore.bucketLineage(testDatabase, bucket)
		return open && err == nil && opened && ownerCore.cluster.placementMarksOf(key) == (placementMarks{})
	})
	missing := 0
	for i := 0; i < 5; i++ {
		if !holdsKeyLocally(tc.nodes[owner], bucket, fmt.Sprintf("k%d", i)) {
			missing++
		}
	}
	close(released)
	for id := range released {
		t.Logf("copyForTakeOver released its own copy on %s (owner is %s)", id, ownerID)
	}
	t.Logf("settled=%v copy-deleted-at-some-point=%v missing=%d status=%+v", settled, lost, missing, ownerCore.cluster.takeOverStatusOf(key))
	if missing > 0 {
		t.Fatalf("node %d, owner again, serves %s without %d of the five writes only it held", owner+1, bucket, missing)
	}
}

// TestAnOwnerWhoseOwnBestCopyOwesKeepsItAndRefuses is the owing variant of the
// test above (A-10.3): the most complete copy is this node's own and owes an
// apply. A copy that owes is never opened as it stands — the open would clear
// the debt — and, being the best there is, it is never deleted either: the
// take-over keeps it, does not complete, and the refusal names the sequences
// it owes. Guard: open it like any other own best copy — the debt is gone.
func TestAnOwnerWhoseOwnBestCopyOwesKeepsItAndRefuses(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	pair := ringWith(t, 2, tc.addrs...)
	bucket := bucketNamed(t, "owing", func(name string) bool { return ownerOn(pair, name) == tc.addrs[0] })
	setUpAnOwingOwner(t, tc, bucket, 0)
	c := clusterOf(tc, 0)
	key := placementOf(testDatabase, bucket)
	own, err := c.answerCopy(ReplBucketRef{Database: testDatabase, Bucket: bucket})
	if err != nil || !own.Owes {
		t.Fatalf("setup: the owner's copy does not owe (%+v, %v)", own, err)
	}
	mine := heldCopy{node: c.self, answer: own, self: true, current: true}
	bestOwnCopy := &takingOver{roundItem: roundItem{held: heldBucket{database: testDatabase, bucket: bucket}, key: key, owner: c.self},
		own: mine, plan: takeOverPlan{best: mine}, decided: true, everyAnswered: true}
	outcome, err := c.completeTakeOver(context.Background(), bestOwnCopy)
	if err == nil || outcome != copyPending {
		t.Errorf("the take-over over an own best copy that owes answered (%v, %v); want it pending with a reason", outcome, err)
	}
	after, err := c.answerCopy(ReplBucketRef{Database: testDatabase, Bucket: bucket})
	if err != nil || !after.Holds || !after.Owes {
		t.Fatalf("the owner's own best copy after the take-over: %+v (%v); want it kept, still owing", after, err)
	}
	refusal := c.handoffInProgress(testDatabase, bucket).Error()
	if !strings.Contains(refusal, "owes the applies of sequences") {
		t.Errorf("the refusal does not name what the copy owes: %s", refusal)
	}
}

// TestTheOwingRefusalSaysWhatARestartCanAndCannotReplay holds the refusal and
// the WARN of an own best copy that owes an apply to rule 3, under the owner's
// ruling that a restart is the remedy: both name the node and the exact
// restart, and neither promises a replay the restart cannot make. A queue or
// heap write never commutes with later writes (it has no key), so a restart
// never replays it beneath the mark: the messages say so and give the way out,
// a dump of the bucket from a replica that holds the write, taken before the
// restart, restored over the copy after it. A keyed write is replayed unless a
// later write to its key overtook it, and the messages say both. Guard: the
// promise "Restart … to replay them" for every bucket.
func TestTheOwingRefusalSaysWhatARestartCanAndCannotReplay(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	pair := ringWith(t, 2, tc.addrs...)
	c := clusterOf(tc, 0)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for _, kind := range []BucketType{BucketTypeFIFO, BucketTypeHeap, BucketTypeHash} {
		t.Run(string(kind), func(t *testing.T) {
			bucket := bucketNamed(t, "owing"+string(kind), func(name string) bool { return ownerOn(pair, name) == tc.addrs[0] })
			tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: kind})
			waitForServing(t, tc, testDatabase, bucket, 2)
			// One write that fails in this node's store and applies on the
			// replica: the copy owes it.
			// The next write's batch records the debt in the state document.
			write := func(n int) {
				switch kind {
				case BucketTypeFIFO:
					tc.frame(0, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: bucket, Data: map[string]interface{}{"n": n}, WriteConcern: "all"})
				case BucketTypeHeap:
					tc.frame(0, transport.OpHeapPush, PushRequest{Database: testDatabase, Bucket: bucket, Data: map[string]interface{}{"n": n}, Priority: int64(n), WriteConcern: "all"})
				default:
					tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("k%d", n), Data: map[string]interface{}{"n": n}, WriteConcern: "all"})
				}
			}
			failed := failNextApplyOn(t, tc.nodes[0].server.core.kv, anySeq)
			write(1)
			store.ApplyFaultHook.Store(nil)
			if failed.Load() == 0 {
				t.Fatal("setup: the owner's store failed no apply")
			}
			write(2)
			if own, err := c.answerCopy(ReplBucketRef{Database: testDatabase, Bucket: bucket}); err != nil || !own.Owes {
				t.Fatalf("setup: the owner's copy does not owe the failed apply (%+v, %v)", own, err)
			}
			var logged bytes.Buffer
			saved := c.logger
			c.logger = slog.New(slog.NewTextHandler(&logged, nil))
			defer func() { c.logger = saved }()
			key := placementOf(testDatabase, bucket)
			if err := c.keepOwingBestCopy(heldBucket{database: testDatabase, bucket: bucket}, key); err == nil {
				t.Fatal("keeping an owing copy answered no reason")
			}
			refusal := c.handoffInProgress(testDatabase, bucket).Error()
			warning := logged.String()
			never := kind != BucketTypeHash
			for what, text := range map[string]string{"the refusal": refusal, "the warning": warning} {
				for _, want := range []string{c.self.ID, "restart hoarddb-server on " + c.self.ID, "hoarddb dump", "hoarddb restore", "--drop"} {
					if !strings.Contains(text, want) {
						t.Errorf("%s does not say %q: %s", what, want, text)
					}
				}
				if never && !strings.Contains(text, "never replays") {
					t.Errorf("%s for a %s bucket does not say that a restart never replays the write: %s", what, kind, text)
				}
				if never && strings.Contains(text, "to replay them") {
					t.Errorf("%s for a %s bucket promises a replay a restart cannot make: %s", what, kind, text)
				}
				if !never && !strings.Contains(text, "a later write to the same key") {
					t.Errorf("%s for a keyed bucket does not say when the replay cannot apply the write: %s", what, text)
				}
			}
		})
	}
}

// TestATakeOverNeverPullsFromThisNode: a take-over's copy is never pulled
// from the node taking the bucket over. pullFromHolder refuses such a source
// by name, and copyForTakeOver refuses it before it releases anything, so the
// node's copy stays. completeTakeOver opens an own best copy instead and
// never gets here; this is the guard behind that. Guard: no self-source check
// — the copy is deleted and the pull from this node fails.
func TestATakeOverNeverPullsFromThisNode(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	pair := ringWith(t, 2, tc.addrs...)
	bucket := bucketNamed(t, "selfsource", func(name string) bool { return ownerOn(pair, name) == tc.addrs[0] })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 2)
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "kept", Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	c := clusterOf(tc, 0)
	own, err := c.answerCopy(ReplBucketRef{Database: testDatabase, Bucket: bucket})
	if err != nil {
		t.Fatalf("reading the owner's copy: %v", err)
	}
	held := heldBucket{database: testDatabase, bucket: bucket}
	mine := heldCopy{node: c.self, answer: own, self: true, current: true}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.pullFromHolder(ctx, held, mine, nil); !errors.Is(err, errPullFromThisNode) {
		t.Errorf("a pull whose source is this node answered %v; want the refusal of a pull from this node", err)
	}
	if err := c.copyForTakeOver(ctx, held, placementOf(testDatabase, bucket), own, takeOverPlan{best: mine}); !errors.Is(err, errPullFromThisNode) {
		t.Errorf("a take-over's copy from this node answered %v; want the refusal of a pull from this node", err)
	}
	if !holdsKeyLocally(tc.nodes[0], bucket, "kept") {
		t.Fatal("the node's own copy is gone: a take-over released it to copy it in from itself")
	}
}
