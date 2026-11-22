package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Step 10b.6 (rev 7 step 10.6; spec §20.8.6): every other copy follows its
// owner. A copy that holds what the owner's term was opened from takes the
// term without a copy; a copy above that base, owing, diverged or back from
// outside its set is copied afresh from the owner; a copy of another
// incarnation is never copied over.

// divergenceSettleWait bounds the worker's re-seed of a diverged replica: one
// round of its back-off, a question to the owner and one pull.
const divergenceSettleWait = 30 * time.Second

// divergenceNamedWait bounds the automatic re-seed of a diverged replica:
// well under takeOverRetryCeiling, so a re-seed that waited for the worker's
// back-off timer instead of the divergence's own wake fails.
const divergenceNamedWait = 10 * time.Second

// TestADivergedReplicaOfTheOwnersIncarnationIsCopiedAfresh is 10a.8's second
// instance without `migrate` (R8-E, made automatic): a replica whose
// declaration this binary cannot read refuses a write its owner applied and
// records the divergence. Within a bounded wait the take-over worker re-seeds
// it from the owner — the copy is of the owner's incarnation — the record
// settles, the replica holds the write, and a later write at all is confirmed
// by every copy without the divergence counter moving again. Guard: leave the
// record — the replica keeps refusing, and the record never settles.
func TestADivergedReplicaOfTheOwnersIncarnationIsCopiedAfresh(t *testing.T) {
	s := setUpDivergedDeclaration(t)
	defer s.tc.stop()
	s.tc.restartNode(s.replica)
	s.tc.waitReady()
	counted := divergencesCounted(t)
	// The worker runs from before the divergence, and nothing names the
	// bucket to it but the divergence's record.
	s.resumeReseeds()
	began := time.Now()
	s.tc.frame(s.owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: s.bucket, Key: "k",
		Data: map[string]interface{}{"Age": 1}, WriteConcern: "one"})

	replica := clusterOf(s.tc, s.replica)
	if !eventually(func() bool { return divergencesCounted(t) > counted }) {
		t.Fatal("setup: the replica recorded no divergence for the write its owner applied")
	}
	counted = divergencesCounted(t)
	if !holdsWithin(divergenceNamedWait, func() bool { _, diverged := replica.divergenceFrom(s.tc.addrs[s.owner]); return !diverged }) {
		record, _ := replica.divergenceFrom(s.tc.addrs[s.owner])
		t.Fatalf("the replica's divergence on %s did not settle within %s without an operator: %+v", s.bucket, divergenceNamedWait, record)
	}
	t.Logf("the divergence settled %s after the write", time.Since(began).Round(time.Millisecond))
	if !holdsWithin(divergenceSettleWait, func() bool { return holdsKeyLocally(s.tc.nodes[s.replica], s.bucket, "k") }) {
		t.Error("the re-seeded replica lacks the write its owner applied")
	}
	if fields := rawCatalogEntry(t, replica.s, testDatabase, s.bucket).Lookup("fields"); fields.Value != nil &&
		strings.Contains(fields.String(), "polygon") {
		t.Error("the re-seeded copy still carries the unreadable declaration")
	}
	frame := s.tc.frame(s.owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: s.bucket, Key: "after",
		Data: map[string]interface{}{"Age": 2}, WriteConcern: "all"})
	if frame.Opcode == transport.OpError {
		t.Errorf("a write at all after the re-seed: %s", frame.Payload)
	}
	if got := divergencesCounted(t) - counted; got != 0 {
		t.Errorf("hoarddb_replica_divergences_total rose by %v after the re-seed; the copy follows its owner again", got)
	}
}

// TestAnEntryLoggedBeforeAnAcceptIsNotReplayedAfterIt (rev 7 steps 10.2 and
// 10.6; trap 8): a replica logs an entry of its copy's term and the apply
// fails, as a store failure leaves it — logged, not applied. The copy then
// accepts its owner's next term, whose base is the copy as it stands, without
// the entry. The power goes and the log is replayed: the entry belongs to the
// copy this one no longer is, and it is not applied onto the new term. Guard:
// leave applied_seq as it was in acceptTerm — the replay applies it.
func TestAnEntryLoggedBeforeAnAcceptIsNotReplayedAfterIt(t *testing.T) {
	s, kv, c, dataDir := newReplicaOnFaultStore(t)
	failed := failNextApplyOn(t, kv, anySeq)
	payload, err := bson.Marshal(InsertRequest{Database: "app", Bucket: "held", Key: "old-term", Data: map[string]interface{}{"v": 1}})
	if err != nil {
		t.Fatalf("encoding the put: %v", err)
	}
	if _, err := asReplica(c, ReplEntry{Seq: 40, Op: int32(transport.OpPut), Bucket: "held", Key: []byte("old-term"), Payload: payload}); err == nil {
		t.Fatal("setup: the replica's apply of the old term's entry succeeded; the store failure did not fire")
	}
	if failed.Load() == 0 || s.wal.LastSequence() == 0 {
		t.Fatalf("setup: the entry is not logged and unapplied (failed sequence %d, log at %d)", failed.Load(), s.wal.LastSequence())
	}
	if found, _ := s.Find(FindRequest{Database: "app", Bucket: "held", Key: "old-term"}); found != nil && found.Found {
		t.Fatal("setup: the failed apply stored the entry")
	}

	held := heldBucket{database: "app", bucket: "held"}
	ref := ReplBucketRef{Database: held.database, Bucket: held.bucket}
	asked, err := c.answerCopy(ref)
	if err != nil {
		t.Fatalf("reading the replica's copy: %v", err)
	}
	baseTerm, baseThrough := copyLineage(asked.Term, asked.Through, asked.BaseTerm, asked.BaseThrough)
	ownersTerm := ReplCopy{Holds: true, Opened: true, Incarnation: asked.Incarnation, Term: asked.Term + 3,
		BaseTerm: baseTerm, BaseThrough: baseThrough}
	if err := c.acceptTerm(held, placementOf(held.database, held.bucket), asked, ownersTerm, ring.Node{ID: "test-owner"}); err != nil {
		t.Fatalf("accepting the owner's term: %v", err)
	}

	restarted := killedAfterPowerLoss(t, dataDir, kv, s)
	if err := restarted.s.replayWAL(context.Background()); err != nil {
		t.Fatalf("replaying the log: %v", err)
	}
	if found, err := restarted.s.Find(FindRequest{Database: "app", Bucket: "held", Key: "old-term"}); err == nil && found.Found {
		t.Error("the replay applied an entry of the old term, logged before the accept, onto the owner's new term")
	}
	if term, _, err := restarted.s.bucketLineage("app", "held"); err != nil || term != ownersTerm.Term {
		t.Errorf("after the replay the copy follows term %d (%v), want the accepted %d", term, err, ownersTerm.Term)
	}
}

// emptyTermChain is a bucket at factor 3 on five nodes whose owner O, once
// removed, is succeeded by A, and A, once removed, by M; R holds the bucket on
// all three rings and is neither A nor M.
type emptyTermChain struct {
	bucket               string
	first, second, third int
	replica              int
}

// pickEmptyTermChain chooses the bucket and its four nodes on the rings of
// tc's members, of the members without O, and of the members without O and A.
func pickEmptyTermChain(t *testing.T, tc *testCluster) emptyTermChain {
	t.Helper()
	without := func(gone ...string) []string {
		var rest []string
		for _, addr := range tc.addrs {
			if !slices.Contains(gone, addr) {
				rest = append(rest, addr)
			}
		}
		return rest
	}
	var chain emptyTermChain
	bucketNamed(t, "emptyterm", func(name string) bool {
		key := placementOf(testDatabase, name).encoded
		set0, _ := ringWith(t, 3, tc.addrs...).Replicas(key, 3)
		set1, _ := ringWith(t, 3, without(set0[0].ID)...).Replicas(key, 3)
		set2, _ := ringWith(t, 3, without(set0[0].ID, set1[0].ID)...).Replicas(key, 3)
		if !nodeListed(set0, set1[0].ID) {
			return false
		}
		for _, member := range set0 {
			if member.ID != set0[0].ID && member.ID != set1[0].ID && member.ID != set2[0].ID &&
				nodeListed(set1, member.ID) && nodeListed(set2, member.ID) {
				chain = emptyTermChain{bucket: name, first: nodeIndexByAddr(t, tc.addrs, set0[0].ID),
					second: nodeIndexByAddr(t, tc.addrs, set1[0].ID), third: nodeIndexByAddr(t, tc.addrs, set2[0].ID),
					replica: nodeIndexByAddr(t, tc.addrs, member.ID)}
				return true
			}
		}
		return false
	})
	return chain
}

// TestAReplicaAtTheBaseOfAnEmptyTermAcceptsItWithoutACopy is the designer's
// case of A-10.2 (spec §20.8.1, a term with no entry is transparent): the
// owner O is removed while it runs and A takes the bucket over, opening a term
// in which nothing is written; the replica R accepts it and stands at
// (A's term, 0). A is removed in turn and M takes the bucket over from the
// same content. R's copy is compared by the base A's term was opened from,
// which is M's base too, so it accepts M's term and nothing is copied. Guard:
// compare the bare (term, through) — R's (A's term, 0) is not M's base, and
// R is copied whole.
func TestAReplicaAtTheBaseOfAnEmptyTermAcceptsItWithoutACopy(t *testing.T) {
	tc := newTestCluster(t, 5, 3)
	defer tc.stop()
	chain := pickEmptyTermChain(t, tc)
	tc.ok(chain.first, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(chain.first, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: chain.bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, chain.bucket, 3)
	for i := 0; i < 3; i++ {
		tc.ok(chain.first, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: chain.bucket, Key: fmt.Sprintf("w%d", i),
			Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
	}
	replica := tc.nodes[chain.replica].server.core

	removeAndForget(t, tc, chain.replica, tc.addrs[chain.first])
	waitTakenOver(t, tc.nodes[chain.second].server.core, testDatabase, chain.bucket)
	waitFollowing(t, replica, tc.nodes[chain.second].server.core, testDatabase, chain.bucket)
	accepted := lineageOf(t, replica, testDatabase, chain.bucket)
	if accepted.Through != 0 || (accepted.BaseTerm == 0 && accepted.BaseThrough == 0) {
		t.Fatalf("setup: the replica follows A's term at (%d, %d) with base (%d, %d); want position 0 over a base",
			accepted.Term, accepted.Through, accepted.BaseTerm, accepted.BaseThrough)
	}
	copiesBefore := replica.cluster.takeOverCopies.Load()

	removeAndForget(t, tc, chain.replica, tc.addrs[chain.second])
	third := tc.nodes[chain.third].server.core
	waitTakenOver(t, third, testDatabase, chain.bucket)
	waitFollowing(t, replica, third, testDatabase, chain.bucket)
	if copies := replica.cluster.takeOverCopies.Load() - copiesBefore; copies != 0 {
		t.Errorf("the replica, node %d, was copied whole %d times to follow M; it held M's base and should have accepted its term",
			chain.replica+1, copies)
	}
	for i := 0; i < 3; i++ {
		if key := fmt.Sprintf("w%d", i); !holdsKeyIn(tc.nodes[chain.replica], testDatabase, chain.bucket, key) {
			t.Errorf("the replica, node %d, lacks %s after it followed M", chain.replica+1, key)
		}
	}
}

// TestAReplicaAheadOfTheNewOwnerIsCopiedNotMerged is rev 7 step 10.6's test
// with A-10.10's construction (F-8 item 1). Four nodes at factor 3: the owner
// O and one replica R alone hold five writes at `one` — the other member's
// stream is frozen — and both are removed while stopped, the take-over workers
// held until both removals are in so nothing takes R's copy. The new owner
// opens without the five, from the best copy the members hold. R is joined
// again on its data directory: its copy is above the owner's base, so it is
// copied afresh from the owner — it ends up equal to the owner's — and its
// WARN names the five entries it gives up. Guard: merge — accept the owner's
// term over a copy above its base, and R keeps the five beside the owner's
// entries.
func TestAReplicaAheadOfTheNewOwnerIsCopiedNotMerged(t *testing.T) {
	var capturing atomic.Int32
	capturing.Store(-1)
	logged := &syncBuffer{}
	tc := newTestClusterTuned(t, 4, 3, nil, func(index int, c *Cluster) {
		if int32(index) == capturing.Load() {
			c.logger = slog.New(slog.NewTextHandler(logged, &slog.HandlerOptions{Level: slog.LevelWarn}))
		}
	})
	defer tc.stop()
	full := ringWith(t, 3, tc.addrs...)
	var owner, ahead, behind int
	bucket := bucketNamed(t, "ahead", func(name string) bool {
		set, err := full.Replicas(placementOf(testDatabase, name).encoded, 3)
		if err != nil {
			return false
		}
		owner, ahead, behind = nodeIndexByAddr(t, tc.addrs, set[0].ID), nodeIndexByAddr(t, tc.addrs, set[1].ID), nodeIndexByAddr(t, tc.addrs, set[2].ID)
		// R, joined again, is a member of the set and not its owner: the
		// ring then is every node but O.
		var withoutOwner []string
		for _, addr := range tc.addrs {
			if addr != set[0].ID {
				withoutOwner = append(withoutOwner, addr)
			}
		}
		if ownerOn(ringWith(t, 3, withoutOwner...), name) == tc.addrs[ahead] {
			ahead, behind = behind, ahead
		}
		return true
	})
	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 3)
	tc.ok(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "seed",
		Data: map[string]interface{}{"n": -1}, WriteConcern: "all"})

	unfreeze := freezePeer(peerOf(t, tc, owner, behind))
	defer unfreeze()
	for i := 0; i < 5; i++ {
		tc.ok(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("w%d", i),
			Data: map[string]interface{}{"n": i}, WriteConcern: "one"})
	}
	pollUntil(t, "the replica that is not frozen holds the five writes", func() bool {
		return holdsKeyIn(tc.nodes[ahead], testDatabase, bucket, "w4")
	})
	if holdsKeyIn(tc.nodes[behind], testDatabase, bucket, "w0") {
		t.Fatal("setup: the frozen member received the writes")
	}

	everyWorker := func(string) bool { return true }
	takeOverPausedHook.Store(&everyWorker)
	t.Cleanup(func() { takeOverPausedHook.Store(nil) })
	tc.stopNodeKeepingData(owner)
	removeAndForget(t, tc, behind, tc.addrs[owner])
	aheadBase := tc.stopNodeKeepingData(ahead)
	removeAndForget(t, tc, behind, tc.addrs[ahead])
	takeOverPausedHook.Store(nil)

	var remaining []string
	for index, addr := range tc.addrs {
		if index != owner && index != ahead {
			remaining = append(remaining, addr)
		}
	}
	newOwner := nodeIndexByAddr(t, tc.addrs, ownerOn(ringWith(t, 3, remaining...), bucket))
	newOwnerCore := tc.nodes[newOwner].server.core
	newOwnerCore.cluster.wakeTakeOver(testDatabase, bucket)
	waitTakenOver(t, newOwnerCore, testDatabase, bucket)
	if holdsKeyIn(tc.nodes[newOwner], testDatabase, bucket, "w0") {
		t.Fatal("setup: the new owner took the bucket over with the five writes; no copy above its base is left")
	}
	tc.ok(newOwner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "after",
		Data: map[string]interface{}{"n": 9}, WriteConcern: "all"})

	via := clusterOf(tc, behind)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := via.proposeJoin(ctx, ring.Node{ID: tc.addrs[ahead], Addr: tc.addrs[ahead]}); err != nil {
		t.Fatalf("joining the replica that was ahead again: %v", err)
	}
	tc.bringMembershipTo(t, via)
	capturing.Store(int32(ahead))
	tc.nodes[ahead] = tc.startNode(ahead, aheadBase, tc.token)
	tc.bringMembershipTo(t, via)
	rejoined := tc.nodes[ahead].server.core
	waitFollowing(t, rejoined, newOwnerCore, testDatabase, bucket)

	for _, key := range []string{"seed", "after"} {
		if !holdsKeyIn(tc.nodes[ahead], testDatabase, bucket, key) {
			t.Errorf("the rejoined replica lacks %s, which its owner holds", key)
		}
	}
	for i := 0; i < 5; i++ {
		if key := fmt.Sprintf("w%d", i); holdsKeyIn(tc.nodes[ahead], testDatabase, bucket, key) {
			t.Errorf("the rejoined replica holds %s, which its owner took the bucket over without: the owner's entries were merged over its copy", key)
		}
	}
	if mine, owners := localCountOf(t, rejoined, bucket), localCountOf(t, newOwnerCore, bucket); mine != owners {
		t.Errorf("the rejoined replica counts %d documents, its owner %d", mine, owners)
	}
	if !strings.Contains(logged.String(), "holds 5 entries its owner") {
		t.Errorf("the rejoined replica did not warn that it gives up the five entries; its warnings:\n%s", logged.String())
	}
}

// TestACopyThatLeftTheSetAndReturnedIsNeverAppliedToAgain is rev 7 step
// 10.2's test (spec §20.8.3, R-7's outside row), green with the follower that
// copies such a copy afresh. Three nodes at factor 2: a join puts the joiner
// in a FIFO bucket's set in place of D, whose copy is kept (its hand-off is
// held) and marked outside; five pushes are not shipped to it. The joiner is
// removed, which returns D to the set before its copy was released, and five
// more pushes follow. With D's worker held, D applies none of them: a queue
// that lacks the middle five must not take the last five on top. Released,
// the worker copies D afresh from the owner, and D holds the ten, in the
// owner's order. A create and a drop of another bucket between the two sets of
// pushes carry D's position in the owner's stream past the first five, as any
// entry addressed to every member does. Guard: no outside test in the
// receiver's rule — D applies the last five over a copy without the middle
// five. (Rev 7's second guard, acknowledge a skip before the checkpoint, is
// about an entry of an older term, which this construction never sends, and
// cannot fail it.)
func TestACopyThatLeftTheSetAndReturnedIsNeverAppliedToAgain(t *testing.T) {
	tc := newTestCluster(t, 3, 2)
	defer tc.stop()
	shortenSweeps(t, 0)
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	before := ringWith(t, 2, tc.addrs...)
	joined := ringWith(t, 2, append(append([]string(nil), tc.addrs...), newcomer.ID)...)
	var owner, dropped int
	bucket := bucketNamed(t, "returned", func(name string) bool {
		key := placementOf(testDatabase, name).encoded
		old, _ := before.Replicas(key, 2)
		next, _ := joined.Replicas(key, 2)
		if old[0].ID != next[0].ID || next[1].ID != newcomer.ID {
			return false
		}
		owner, dropped = nodeIndexByAddr(t, tc.addrs, old[0].ID), nodeIndexByAddr(t, tc.addrs, old[1].ID)
		return true
	})
	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeFIFO})
	waitForServing(t, tc, testDatabase, bucket, 2)

	droppedID := tc.addrs[dropped]
	keep := func(fromID, _, _, name string) error {
		if fromID == droppedID && name == bucket {
			return errors.New("test: the dropped holder's hand-off is held")
		}
		return nil
	}
	askNodeToMigrateHook.Store(&keep)
	defer askNodeToMigrateHook.Store(nil)
	joinFreezeOffHook.Store(true)
	defer joinFreezeOffHook.Store(false)
	joiner := joinNewcomer(t, tc, owner, ports)
	pollUntil(t, "the joiner holds the bucket", func() bool { return joiner.node.server.core.bucketStoreOpen(testDatabase, bucket) })
	droppedCore := tc.nodes[dropped].server.core
	if droppedCore.cluster.placementMarksOf(placementOf(testDatabase, bucket)).Outside == 0 {
		t.Fatalf("setup: node %d's copy is not marked outside the set after the join", dropped+1)
	}
	push := func(n int32, concern string) {
		frame := tc.frame(owner, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: bucket,
			Data: map[string]interface{}{"n": n}, WriteConcern: concern})
		if frame.Opcode == transport.OpError {
			t.Fatalf("push %d at %s: %s", n, concern, frame.Payload)
		}
	}
	for n := int32(0); n < 5; n++ {
		push(n, "majority")
	}
	// A drop is addressed to every member (R8-C), D included: its delivery
	// moves D's position in the owner's stream past the five pushes, so once D
	// is back in the set the stream resumes after them, and only the outside
	// test keeps D from applying the next five on a copy without them.
	passing := bucketNamed(t, "passing", func(name string) bool { return ownerOn(joined, name) == tc.addrs[owner] && name != bucket })
	tc.ok(owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: passing, Type: BucketTypeHash})
	tc.ok(owner, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: passing})
	time.Sleep(2 * time.Second)

	held := func(nodeID string) bool { return nodeID == droppedID }
	takeOverPausedHook.Store(&held)
	t.Cleanup(func() { takeOverPausedHook.Store(nil) })
	joiner.stop()
	removeAndForget(t, tc, owner, newcomer.ID)
	if !droppedCore.cluster.Hosts(placementOf(testDatabase, bucket)) {
		t.Fatalf("setup: the removal did not return node %d to the bucket's set", dropped+1)
	}
	for n := int32(5); n < 10; n++ {
		push(n, "one")
	}
	history := []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	if got := queueElementsOn(t, tc.nodes[owner].server.core, bucket); !slices.Equal(got, history) {
		t.Fatalf("setup: the owner's queue holds %v, want %v", got, history)
	}
	// The stream retries what D refuses; D must stay a prefix of the history
	// for as long as its copy has not been copied afresh.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got := queueElementsOn(t, droppedCore, bucket); !slices.Equal(got, history[:len(got)]) {
			t.Fatalf("node %d, back in the set with a copy from before it left, holds %v: it applied entries on top of a copy that lacks the ones sent while it was outside",
				dropped+1, got)
		}
		time.Sleep(100 * time.Millisecond)
	}

	takeOverPausedHook.Store(nil)
	droppedCore.cluster.wakeTakeOver(testDatabase, bucket)
	var got []int32
	if !holdsWithin(takeOverWait, func() bool {
		got = queueElementsOn(t, droppedCore, bucket)
		return slices.Equal(got, history)
	}) {
		t.Errorf("node %d's queue holds %v once its worker ran; want the owner's %v", dropped+1, got, history)
	}
}
