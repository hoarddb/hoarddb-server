package server

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Dispatch 10a, step 10a.7 (R8-D): a create or a
// drop at an owner that holds no copy asks the members before the fence, and
// re-checks only local facts inside it.

// requireRetryRefusal fails unless frame is a refusal with retry
// (UNAVAILABLE) of a request that changed nothing (not_applied).
func requireRetryRefusal(t *testing.T, frame *transport.Frame, what string) ErrorResponse {
	t.Helper()
	if frame.Opcode != transport.OpError {
		t.Fatalf("%s was answered %s; want a refusal with retry", what, frame.Opcode)
	}
	refused := refusalOf(t, frame)
	if refused.Code != int(transport.ErrUnavailable) || refused.WriteOutcome != outcomeNotApplied {
		t.Errorf("%s: code %d, outcome %q; want UNAVAILABLE, not_applied: %s", what, refused.Code, refused.WriteOutcome, refused.Message)
	}
	return refused
}

// TestADropAtAnOwnerWithoutItsCopyIsRefusedAndNothingIsLogged is
// lifted from the review probe zz_review_drop_pull_test.go (commit 32895375):
// a joiner owns a bucket it has not pulled, and a `drop bucket` reaches it.
// The drop is refused UNAVAILABLE, not_applied; no node's log grows; the
// members keep the bucket, and the joiner's pull then lands it. Guard: no drop
// question — the drop is logged, shipped, and undone by the joiner's pull.
func TestADropAtAnOwnerWithoutItsCopyIsRefusedAndNothingIsLogged(t *testing.T) {
	switchJoinFreezeOff(t)
	s := setUpJoinerReadingUnpulledBuckets(t)
	bucket := s.hashBucket
	before, joinerBefore := s.tc.walSequences(), s.joiner.s.wal.LastSequence()

	frame := s.tc.frameOn(s.joinerNode, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: bucket})
	requireRetryRefusal(t, frame, "a drop at an owner that holds no copy")
	time.Sleep(500 * time.Millisecond)
	if after := s.tc.walSequences(); !slices.Equal(after, before) {
		t.Errorf("the members' logs grew from %v to %v: the refused drop was logged", before, after)
	}
	if after := s.joiner.s.wal.LastSequence(); after != joinerBefore {
		t.Errorf("the joiner's log grew from %d to %d: the refused drop was logged", joinerBefore, after)
	}
	for i, node := range s.tc.nodes {
		if !holdsKeyLocally(node, bucket, "held") {
			t.Errorf("member %d no longer holds %s after a refused drop", i+1, bucket)
		}
	}
	pullNow(t, s.joiner, testDatabase, bucket)
	if !holdsKeyLocally(s.joinerNode, bucket, "held") {
		t.Errorf("the joiner's pulled copy of %s lacks its document", bucket)
	}
}

// TestANewBucketIsCreatedWhileAMemberIsDownAfterAnUnrelatedChange (review
// M-5): five members at factor 2 that have all settled a membership change;
// one member is stopped, then another is removed. The settled floor is the
// epoch before the removal. A create of a name whose set the removal did not
// change succeeds; a create of a name whose set it changed is refused with
// retry, naming the unheard member. Guard: refuse whenever a member is unheard
// (rev 8's rule) — the first create is refused too.
//
// Five, not the build doc's four: a removal needs a majority of the members to
// accept it, and of four, with one stopped and one removed, two are left. The
// cluster starts with six nodes and removes the sixth first, so the floor the
// creates are judged on is the settle of a change, not only the founders'
// first epoch (owner ruling 52; TestAMemberDownDoesNotRefuseNewNamesOnA-
// ClusterThatNeverChanged covers that one).
func TestANewBucketIsCreatedWhileAMemberIsDownAfterAnUnrelatedChange(t *testing.T) {
	switchJoinFreezeOff(t)
	tc := newTestCluster(t, 6, 2)
	defer tc.stop()
	const via, removed, down, first = 0, 2, 3, 5
	downID := tc.addrs[down]
	viaCluster := clusterOf(tc, via)
	removeNode := func(index int, others []int) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := viaCluster.proposeRemoval(ctx, tc.addrs[index]); err != nil {
			t.Fatalf("setup: removing node %d: %v", index+1, err)
		}
		for _, other := range others {
			if err := clusterOf(tc, other).reconcileMembership(viaCluster.self.ID, viaCluster.memberNodes(), viaCluster.memberIDs(), viaCluster.Epoch()); err != nil {
				t.Fatalf("setup: bringing node %d to the removal of node %d: %v", other+1, index+1, err)
			}
		}
	}
	removeNode(first, []int{1, 2, 3, 4, first})
	settledAt := viaCluster.Epoch()
	heard := func() bool {
		c := viaCluster
		if c.settled.Load() < settledAt {
			return false
		}
		c.settleMu.Lock()
		defer c.settleMu.Unlock()
		for _, index := range []int{1, 2, 3, 4} {
			if c.settledOf[tc.addrs[index]] < settledAt {
				return false
			}
		}
		return true
	}
	if !eventually(heard) {
		t.Fatalf("setup: node 1 never heard every member settle at epoch %d", settledAt)
	}
	tc.ok(via, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, 0, 1, 2, 3, 4)

	full := ringWith(t, 2, tc.addrs[0], tc.addrs[1], tc.addrs[2], tc.addrs[3], tc.addrs[4])
	reduced := ringWith(t, 2, tc.addrs[0], tc.addrs[1], tc.addrs[3], tc.addrs[4])
	stays := func(name string) bool {
		now := replicaIDs(t, reduced, 2, name)
		return slices.Equal(replicaIDs(t, full, 2, name), now) && now[0] != downID
	}
	unchanged := bucketNamed(t, "kept", stays)
	moved := bucketNamed(t, "moved", func(name string) bool {
		now := replicaIDs(t, reduced, 2, name)
		return !slices.Equal(replicaIDs(t, full, 2, name), now) && now[0] != downID
	})

	tc.stopNodeKeepingData(down)
	removeNode(removed, []int{1, 4, removed})
	if floor := viaCluster.settledFloorNow(); floor != settledAt {
		t.Fatalf("setup: the settled floor is %d, want %d (the epoch the stopped member settled at)", floor, settledAt)
	}

	owner := func(name string) int { return nodeIndexByAddr(t, tc.addrs, ownerOn(reduced, name)) }
	tc.ok(owner(unchanged), transport.OpCreateBucket, bson.M{"database": testDatabase, "name": unchanged, "type": string(BucketTypeHash), "write_concern": "one"})
	refused := requireRetryRefusal(t, tc.frame(owner(moved), transport.OpCreateBucket,
		bson.M{"database": testDatabase, "name": moved, "type": string(BucketTypeHash), "write_concern": "one"}),
		"a create of a name whose set changed while a member is unheard")
	if !strings.Contains(refused.Message, downID) {
		t.Errorf("the refusal does not name the unheard member %s: %s", downID, refused.Message)
	}
}

// TestACreateQuestionDoesNotHoldUpOtherWrites (review M-4): the members'
// answers to a create's catalog question are held until the test releases
// them; meanwhile writes at `all` to another bucket of the same owner complete,
// each while the create is still waiting for its answers. Guard: ask inside
// applyAsOwner after shipOrder.open — the create's open ship ticket then holds
// up the routing of every later write until the question ends, so no write
// completes before the create does.
//
// The rule is asserted by order, not by a duration (BUG-0487): an earlier
// version required each write to finish in 500 ms, which a loaded gate host
// missed (1.22 s) with nothing holding the write. The create gives up on its
// question after catalogQuestionWait, so the writes here have that long.
func TestACreateQuestionDoesNotHoldUpOtherWrites(t *testing.T) {
	const writesDuringTheQuestion = 3
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	full := ringWith(t, 3, tc.addrs...)
	ownedHere := func(name string) bool { return ownerOn(full, name) == tc.addrs[0] }
	busy := bucketNamed(t, "busy", ownedHere)
	fresh := bucketNamed(t, "fresh", func(name string) bool { return ownedHere(name) && name != busy })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: busy, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, busy, 3)

	asked, release := make(chan struct{}), make(chan struct{})
	var askedOnce, releaseOnce sync.Once
	hold := func(string) {
		askedOnce.Do(func() { close(asked) })
		<-release
	}
	catalogAnswerHook.Store(&hold)
	defer catalogAnswerHook.Store(nil)
	// Registered after tc.stop, so it runs first: a held answer never keeps
	// the cluster from stopping.
	defer releaseOnce.Do(func() { close(release) })
	creator := tc.dial(0)
	defer func() { _ = creator.Close() }()
	payload, err := bson.Marshal(CreateBucketRequest{Database: testDatabase, Name: fresh, Type: BucketTypeHash})
	if err != nil {
		t.Fatal(err)
	}
	created := make(chan struct{})
	go func() {
		defer close(created)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_, _ = creator.SendRequest(ctx, transport.OpCreateBucket, payload)
	}()
	select {
	case <-asked:
	case <-created:
		t.Fatal("setup: the create finished without asking a member about the bucket")
	}
	for i := 0; i < writesDuringTheQuestion; i++ {
		tc.ok(0, transport.OpUpsert, InsertRequest{Database: testDatabase, Bucket: busy, Key: "k", Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
		select {
		case <-created:
			t.Fatalf("write %d at all to another bucket completed only once the create had finished, though the members' answers are still held: the question holds up routing", i+1)
		default:
		}
	}
	releaseOnce.Do(func() { close(release) })
	<-created
}

// TestACreateWhoseSurveyIsOverTakenByAPullIsRefused (review M-4): a pull of
// the name registers between the create's question and the bucket's fence.
// The create is refused with retry before anything is logged. Guard: drop the
// re-check inside the fence.
func TestACreateWhoseSurveyIsOverTakenByAPullIsRefused(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	full := ringWith(t, 3, tc.addrs...)
	bucket := bucketNamed(t, "overtaken", func(name string) bool { return ownerOn(full, name) == tc.addrs[0] })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, 0, 1, 2)
	c := clusterOf(tc, 0)
	key := migrateKey(testDatabase, bucket)
	registerPull := func(_ transport.Opcode, _, name string) {
		if name != bucket {
			return
		}
		c.migrateMu.Lock()
		c.migrateActive[key] = make(chan struct{})
		c.migrateMu.Unlock()
	}
	afterJoinFenceHook.Store(&registerPull)
	defer afterJoinFenceHook.Store(nil)
	defer func() {
		c.migrateMu.Lock()
		delete(c.migrateActive, key)
		c.migrateMu.Unlock()
	}()
	before := c.s.wal.LastSequence()
	requireRetryRefusal(t, tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash}),
		"a create whose question a pull overtook")
	if after := c.s.wal.LastSequence(); after != before {
		t.Errorf("the owner's log grew from %d to %d: the create was logged", before, after)
	}
}

// TestADropRoutedAndNotAppliedHereIsUnknown (R8-D (d), spec §19.4 P4): the
// owner's copy is dropped between the drop's fence and its local apply, so the
// drop applies nowhere here — but it was routed to every member, which may
// apply it. It answers unknown, never not_applied. Guard: keep the
// earlier shortcut for drops.
func TestADropRoutedAndNotAppliedHereIsUnknown(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	full := ringWith(t, 3, tc.addrs...)
	bucket := bucketNamed(t, "vanish", func(name string) bool { return ownerOn(full, name) == tc.addrs[0] })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 3)
	owner := tc.nodes[0].server.core
	var once sync.Once
	vanish := func(_ string, entry ReplEntry) {
		if transport.Opcode(entry.Op) != transport.OpDeleteBucket || entry.Bucket != bucket {
			return
		}
		once.Do(func() { _, _ = owner.DeleteBucket(DeleteBucketRequest{Database: testDatabase, Name: bucket}) })
	}
	beforeLocalApplyHook.Store(&vanish)
	defer beforeLocalApplyHook.Store(nil)
	frame := tc.frame(0, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: bucket})
	if frame.Opcode != transport.OpError {
		t.Fatalf("the drop was answered %s; its local apply found nothing", frame.Opcode)
	}
	if outcome := refusalOf(t, frame).WriteOutcome; outcome != outcomeUnknown {
		t.Errorf("a drop routed to every member and not applied here answers %q; want %q", outcome, outcomeUnknown)
	}
}
