package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Step 11.4b (dispatch 11): the owner's answer has authority over absence
// only once it has taken the bucket over, and a consistent get is refused
// while it has not; a joiner that holds no copy yet does not answer, and the
// other copies decide.

// takeOverHold holds one node's take-over of one bucket before its open until
// released.
type takeOverHold struct {
	reached     chan struct{}
	release     func()
	reachedOnce sync.Once
}

func holdTakeOverOf(t *testing.T, node, bucket string) *takeOverHold {
	t.Helper()
	gate := make(chan struct{})
	var releaseOnce sync.Once
	h := &takeOverHold{reached: make(chan struct{}), release: func() { releaseOnce.Do(func() { close(gate) }) }}
	hold := func(nodeID, _, held string) {
		if nodeID != node || held != bucket {
			return
		}
		h.reachedOnce.Do(func() { close(h.reached) })
		<-gate
	}
	takeOverBeforeOpenHook.Store(&hold)
	t.Cleanup(func() { takeOverBeforeOpenHook.Store(nil) })
	t.Cleanup(h.release)
	return h
}

func (h *takeOverHold) waitReached(t *testing.T, what string) {
	t.Helper()
	select {
	case <-h.reached:
	case <-time.After(takeOverWait):
		t.Fatalf("setup: %s never reached the open of its take-over", what)
	}
}

// joinLayout is three founders and a joiner J that becomes a bucket's owner:
// A owns it on the founders' ring and stays in J's set beside S; X leaves it.
type joinLayout struct {
	tc        *testCluster
	ports     *testNodePorts
	joinerID  string
	bucket    string
	a, s, x   int
	joinEpoch uint64
}

func newJoinLayout(t *testing.T) joinLayout {
	t.Helper()
	tc := newTestCluster(t, 3, 3)
	ports := reserveNodePorts(t)
	joinerID := ports.ringNode().ID
	founders := ringWith(t, 3, tc.addrs...)
	joined := ringWith(t, 3, append(append([]string(nil), tc.addrs...), joinerID)...)
	l := joinLayout{tc: tc, ports: ports, joinerID: joinerID}
	l.bucket = bucketNamed(t, "ceded", func(name string) bool {
		key := placementOf(testDatabase, name).encoded
		before, _ := founders.Replicas(key, 3)
		after, _ := joined.Replicas(key, 3)
		if after[0].ID != joinerID || !nodeListed(after, before[0].ID) {
			return false
		}
		l.a = nodeIndexByAddr(t, tc.addrs, before[0].ID)
		for index, addr := range tc.addrs {
			switch {
			case index == l.a:
			case nodeListed(after, addr):
				l.s = index
			default:
				l.x = index
			}
		}
		return true
	})
	tc.ok(l.a, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(l.a, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: l.bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, l.bucket, 3)
	requireEveryCopy(t, tc.frame(l.a, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: l.bucket, Key: "k",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"}), "the put of k")
	l.joinEpoch = clusterOf(tc, l.a).Epoch() + 1
	return l
}

// proposeJoin commits J's join through the founder at via.
func (l joinLayout) proposeJoin(t *testing.T, via int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	outcome, err := clusterOf(l.tc, via).proposeJoinOutcome(ctx, l.ports.ringNode())
	if err != nil || outcome.kind != changeCommitted {
		t.Fatalf("joining %s: outcome %q, %v; want committed", l.joinerID, outcome.kind, err)
	}
}

// holdBehind refuses node's adoption of the join, so its ring still names
// the ceded owner.
func holdBehind(t *testing.T, node string, epoch uint64) {
	t.Helper()
	refuse := func(nodeID string, adopted uint64) error {
		if nodeID == node && adopted >= epoch {
			return errors.New("a test holds this node on the earlier epoch")
		}
		return nil
	}
	adoptMembershipHook.Store(&refuse)
	t.Cleanup(func() { adoptMembershipHook.Store(nil) })
}

// TestConsistentGetIsRefusedWhileItsOwnerTakesOver: the owner's answer has
// authority over absence only once it has taken the bucket over (D3).
//
// (a) A removal: the bucket's owner is stopped and removed, and the new owner,
// a replica that holds a copy, is held before it opens it. A consistent get
// from the other replica is refused UNAVAILABLE with the hand-off text and
// repairs nothing; released, the read answers. Guard: no refusal while the
// owner is not settled.
//
// (b) A coordinator held one epoch behind reads the ceded owner of a join,
// which still holds its opened copy, marked moved by its placement pass. The
// read is refused: by the epoch check, and without it by the owner's
// authority. Guard: no epoch check, and the owner's authority read from its
// opened flag alone.
//
// (c) As (b), with the ceded owner held after it adopted the join and before
// its placement pass ran.
//
// (d) A join whose joiner holds no copy yet: it does not answer, and with
// every other holder answering alike the read answers; with one of them
// unreachable it is refused, naming the joiner. Guard: refuse whenever the
// owner does not answer.
func TestConsistentGetIsRefusedWhileItsOwnerTakesOver(t *testing.T) {
	t.Run("a removal", func(t *testing.T) {
		tc := newTestCluster(t, 4, 3)
		defer tc.stop()
		full := ringWith(t, 3, tc.addrs...)
		// A name whose next owner, once its owner is removed, is one of its
		// replicas: a new owner that already holds a copy.
		bucket := bucketNamed(t, "removal", func(name string) bool {
			key := placementOf(testDatabase, name).encoded
			set, _ := full.Replicas(key, 3)
			var rest []string
			for _, addr := range tc.addrs {
				if addr != set[0].ID {
					rest = append(rest, addr)
				}
			}
			next, _ := ringWith(t, 3, rest...).Replicas(key, 3)
			return nodeListed(set, next[0].ID)
		})
		set, _ := full.Replicas(placementOf(testDatabase, bucket).encoded, 3)
		owner := nodeIndexByAddr(t, tc.addrs, set[0].ID)
		tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		tc.ok(owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, bucket, 3)
		requireEveryCopy(t, tc.frame(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k",
			Data: map[string]interface{}{"n": 1}, WriteConcern: "all"}), "the put of k")
		var remaining []string
		for _, addr := range tc.addrs {
			if addr != set[0].ID {
				remaining = append(remaining, addr)
			}
		}
		newSet, _ := ringWith(t, 3, remaining...).Replicas(placementOf(testDatabase, bucket).encoded, 3)
		newOwner := nodeIndexByAddr(t, tc.addrs, newSet[0].ID)
		if !nodeListed(set, newSet[0].ID) {
			t.Fatalf("setup: the new owner %s held no copy", newSet[0].ID)
		}
		coordinator := -1
		for _, member := range set[1:] {
			if index := nodeIndexByAddr(t, tc.addrs, member.ID); index != newOwner {
				coordinator = index
			}
		}
		hold := holdTakeOverOf(t, newSet[0].ID, bucket)
		stopNode(t, tc, owner)
		removeCommitted(t, tc, coordinator, owner)
		hold.waitReached(t, "the new owner")
		running := []*testNode{tc.nodes[newOwner], tc.nodes[coordinator]}
		logs := logTails(running...)
		message := requireConsistentGetRefused(t, consistentGetFrame(t, tc.nodes[coordinator], testDatabase, bucket, "k"), transport.ErrUnavailable,
			newSet[0].ID, "has not taken it over")
		t.Logf("(a) refused: %s", message)
		if after := logTails(running...); fmt.Sprint(after) != fmt.Sprint(logs) {
			t.Errorf("(a) the refused read logged a repair: the logs went from %v to %v", logs, after)
		}
		hold.release()
		waitTakenOver(t, tc.nodes[newOwner].server.core, testDatabase, bucket)
		pollUntil(t, "the read answers once the new owner has taken the bucket over", func() bool {
			frame := consistentGetFrame(t, tc.nodes[coordinator], testDatabase, bucket, "k")
			return frame.Opcode != transport.OpError
		})
	})

	t.Run("coordinator behind", func(t *testing.T) {
		l := newJoinLayout(t)
		defer l.tc.stop()
		holdBehind(t, l.tc.addrs[l.s], l.joinEpoch)
		l.proposeJoin(t, l.a)
		ceded := clusterOf(l.tc, l.a)
		key := placementOf(testDatabase, l.bucket)
		pollUntil(t, "the ceded owner's placement pass marks its copy moved", func() bool {
			return ceded.placementPassFinished() && ceded.placementMarksOf(key).Moved != 0
		})
		if _, opened, err := ceded.s.bucketLineage(testDatabase, l.bucket); err != nil || !opened {
			t.Fatalf("setup: the ceded owner's copy is not opened (%v)", err)
		}
		if behind := clusterOf(l.tc, l.s).Epoch(); behind >= l.joinEpoch {
			t.Fatalf("setup: the coordinator adopted epoch %d", behind)
		}
		message := requireConsistentGetRefused(t, consistentGetFrame(t, l.tc.nodes[l.s], testDatabase, l.bucket, "k"), transport.ErrUnavailable)
		t.Logf("(b) refused: %s", message)
	})

	t.Run("condition 3", func(t *testing.T) {
		l := newJoinLayout(t)
		defer l.tc.stop()
		holdBehind(t, l.tc.addrs[l.s], l.joinEpoch)
		gate := make(chan struct{})
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(gate) }) }
		defer release()
		ceded := l.tc.addrs[l.a]
		hold := func(nodeID string, epoch uint64) {
			if nodeID == ceded && epoch == l.joinEpoch {
				<-gate
			}
		}
		viewAdoptedHook.Store(&hold)
		defer viewAdoptedHook.Store(nil)
		proposed := make(chan struct{})
		go func() {
			defer close(proposed)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, _ = clusterOf(l.tc, l.x).proposeJoinOutcome(ctx, l.ports.ringNode())
		}()
		cededCluster := clusterOf(l.tc, l.a)
		pollUntil(t, "the ceded owner adopts the join's view", func() bool { return cededCluster.Epoch() == l.joinEpoch })
		if cededCluster.placementPassFinished() {
			t.Fatal("setup: the ceded owner's placement pass finished while it was held before it")
		}
		message := requireConsistentGetRefused(t, consistentGetFrame(t, l.tc.nodes[l.s], testDatabase, l.bucket, "k"), transport.ErrUnavailable)
		t.Logf("(c) refused: %s", message)
		release()
		<-proposed
	})

	t.Run("join", func(t *testing.T) {
		l := newJoinLayout(t)
		defer l.tc.stop()
		hold := holdTakeOverOf(t, l.joinerID, l.bucket)
		l.proposeJoin(t, l.a)
		joiner := startJoinTarget(t, l.tc, l.ports, l.tc.token)
		defer func() { _ = joiner.client.Close() }()
		bringNodesTo(t, append(append([]*testNode(nil), l.tc.nodes...), joiner), clusterOf(l.tc, l.a))
		hold.waitReached(t, "the joiner")
		if joiner.server.core.bucketStoreOpen(testDatabase, l.bucket) {
			t.Fatal("setup: the joiner holds a copy before its take-over opens")
		}
		resp := answeredConsistentGet(t, consistentGetFrame(t, l.tc.nodes[l.s], testDatabase, l.bucket, "k"))
		if !resp.Found {
			t.Errorf("(d) with the joiner holding nothing and the other copies agreeing, the read answered found=%v (%s)", resp.Found, resp.Message)
		}
		restore := failVersionedGets(t, clusterOf(l.tc, l.s), l.tc.addrs[l.a])
		message := requireConsistentGetRefused(t, consistentGetFrame(t, l.tc.nodes[l.s], testDatabase, l.bucket, "k"), transport.ErrUnavailable, l.joinerID)
		t.Logf("(d) refused: %s", message)
		restore()
		// The joiner's worker is let go and its take-over finished before any
		// node stops: a worker still held when its node stops resumes on a
		// closed store.
		hold.release()
		waitTakenOver(t, joiner.server.core, testDatabase, l.bucket)
	})
}
