package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Step 11.5 (dispatch 11): the four hand-off collectors of spec §20.14, on
// each cluster's own registry, unlabelled, read from per-cluster atomics.

// handoffCollectors are the four series step 11.5 exports.
var handoffCollectors = []string{
	"hoarddb_handoffs_in_progress",
	"hoarddb_handoff_copies_total",
	"hoarddb_handoffs_without_previous_owner_total",
	"hoarddb_handoff_entries_reconciled_total",
}

// createHashBuckets creates one hash bucket per name through node index.
func createHashBuckets(t *testing.T, tc *testCluster, index int, names []string) {
	t.Helper()
	for _, name := range names {
		tc.ok(index, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
	}
}

// movedBucketNames returns count names of the form moved%03d, at least one of
// which the joiner owns once members (joiner last) are the ring. A joiner that
// owns none is only seeded as a replica, and hoarddb_handoff_copies_total
// counts take-over and fresh copies, not that seeding, so it would serve 0
// whatever the join did: the port draw decides it (BUG-0487).
func movedBucketNames(t *testing.T, members []string, count int) []string {
	t.Helper()
	joinerID := members[len(members)-1]
	joined := ringWith(t, 3, members...)
	names := make([]string, 0, count)
	for i := 0; i < count; i++ {
		names = append(names, fmt.Sprintf("moved%03d", i))
	}
	for _, name := range names {
		if ownerOn(joined, name) == joinerID {
			return names
		}
	}
	names[count-1] = bucketNamed(t, "moved", func(name string) bool { return ownerOn(joined, name) == joinerID })
	return names
}

// joinedCluster is three founders at factor 3 holding buckets, and a joiner
// whose join has committed and every member has adopted.
type joinedCluster struct {
	tc      *testCluster
	joiner  *testNode
	buckets []string
}

func joinOverBuckets(t *testing.T, count int, beforeJoin func(joinerID string)) joinedCluster {
	t.Helper()
	tc := newTestCluster(t, 3, 3)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	// The joiner's address is drawn first so the bucket names can be chosen
	// against the ring the join will make.
	ports := reserveNodePorts(t)
	buckets := movedBucketNames(t, append(append([]string(nil), tc.addrs...), ports.ringNode().ID), count)
	createHashBuckets(t, tc, 0, buckets)
	for _, bucket := range buckets {
		waitForServing(t, tc, testDatabase, bucket, 3)
	}
	if beforeJoin != nil {
		beforeJoin(ports.ringNode().ID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	outcome, err := clusterOf(tc, 0).proposeJoinOutcome(ctx, ports.ringNode())
	if err != nil || outcome.kind != changeCommitted {
		t.Fatalf("joining: outcome %q, %v; want committed", outcome.kind, err)
	}
	joiner := startJoinTarget(t, tc, ports, tc.token)
	t.Cleanup(func() { _ = joiner.client.Close() })
	bringNodesTo(t, append(append([]*testNode(nil), tc.nodes...), joiner), clusterOf(tc, 0))
	return joinedCluster{tc: tc, joiner: joiner, buckets: buckets}
}

// TestNoHandoffCollectorIsLabelledByBucketOrDatabase: after a join, every
// node's own registry serves the four hand-off series with no label, and
// hoarddb_handoff_copies_total is each node's own: the joiner, which copied
// the buckets it gained, reads above a founder, which gained none. Guards: a
// bucket label (a series is labelled); the four registered as process
// globals (the second node's construction panics with a duplicate
// registration).
func TestNoHandoffCollectorIsLabelledByBucketOrDatabase(t *testing.T) {
	j := joinOverBuckets(t, 12, nil)
	defer j.tc.stop()
	members := map[string]*Server{j.joiner.addr: j.joiner.server.core}
	for index, node := range j.tc.nodes {
		members[j.tc.addrs[index]] = node.server.core
	}
	var unsettled string
	if !holdsWithin(takeOverWait, func() bool {
		unsettled = handOffUnsettled(members, testDatabase, j.buckets)
		return unsettled == ""
	}) {
		t.Fatalf("the join did not settle: %s", unsettled)
	}
	for id, s := range members {
		served := servedMetrics(t, s)
		for _, name := range handoffCollectors {
			series := served[name]
			if len(series) != 1 {
				t.Errorf("%s serves %d series of %s, want one", id, len(series), name)
				continue
			}
			if labels := series[0].GetLabel(); len(labels) != 0 {
				t.Errorf("%s's %s carries labels %v", id, name, labels)
			}
		}
	}
	joinerCopies := replicationGauge(t, j.joiner.server.core, "hoarddb_handoff_copies_total")
	founderCopies := replicationGauge(t, j.tc.nodes[0].server.core, "hoarddb_handoff_copies_total")
	if joinerCopies == 0 || joinerCopies == founderCopies {
		t.Errorf("the joiner made %v copies and a founder %v: each node's count is not its own", joinerCopies, founderCopies)
	}
}

// TestHandoffsInProgressReturnsToZero: a join over 200 buckets, with the
// joiner's take-overs held. While held, a founder that held a moved bucket
// reads above 0 once its placement pass has marked it, and the joiner, which
// holds nothing yet, reads 0. Released, once every member's placement pass has
// finished, every node reads 0 within thirty seconds. Guard: read a count that
// is never decremented (a node never reads 0).
func TestHandoffsInProgressReturnsToZero(t *testing.T) {
	gate := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	defer release()
	j := joinOverBuckets(t, 200, func(joinerID string) {
		hold := func(nodeID, _, _ string) {
			if nodeID == joinerID {
				<-gate
			}
		}
		takeOverBeforeOpenHook.Store(&hold)
		t.Cleanup(func() { takeOverBeforeOpenHook.Store(nil) })
	})
	defer j.tc.stop()
	waitForGauge(t, j.tc.nodes[0].server.core, "hoarddb_handoffs_in_progress", takeOverWait,
		"a founder that held moved buckets, while the joiner's take-overs are held", func(v float64) bool { return v > 0 })
	if held := replicationGauge(t, j.joiner.server.core, "hoarddb_handoffs_in_progress"); held != 0 {
		t.Errorf("the joiner, which holds nothing yet, reads %v buckets in progress", held)
	}
	release()
	everyone := append(append([]*testNode(nil), j.tc.nodes...), j.joiner)
	pollUntil(t, "every member's placement pass finishes", func() bool {
		for _, node := range everyone {
			if !node.server.core.cluster.placementPassFinished() {
				return false
			}
		}
		return true
	})
	for _, node := range everyone {
		waitForGauge(t, node.server.core, "hoarddb_handoffs_in_progress", 30*time.Second,
			node.addr+" after the hand-off", func(v float64) bool { return v == 0 })
	}
}

// questionFailingConn fails the first rounds of the take-over's questions
// sent on it, and counts them.
type questionFailingConn struct {
	peerConn
	rounds, failed *atomic.Int64
}

func (q *questionFailingConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	if op == transport.OpReplPush {
		var push ReplPushRequest
		if err := bson.Unmarshal(payload, &push); err == nil && push.Handoff != nil && len(push.Handoff.Ask) > 0 &&
			q.failed.Load() < q.rounds.Load() {
			q.failed.Add(1)
			return nil, errors.New("a test holds this member's answer to the take-over's question")
		}
	}
	return q.peerConn.SendRequest(ctx, lane, op, payload)
}

// TestTheFallbackIsCountedOncePerTakeOver: the owner of a set of buckets is
// stopped and removed, so the new owner takes them over by the fallback; one
// other member's answers to its question are held for two rounds, so the
// fallback is decided three times. hoarddb_handoffs_without_previous_owner_total
// on the new owner rises by exactly the buckets it took. Guard: count where
// the fallback is decided (it rises three times as much).
func TestTheFallbackIsCountedOncePerTakeOver(t *testing.T) {
	tc := newTestCluster(t, 4, 3)
	defer tc.stop()
	full := ringWith(t, 3, tc.addrs...)
	removedID := ownerOn(full, testBucket)
	removed := nodeIndexByAddr(t, tc.addrs, removedID)
	var remaining []string
	for _, addr := range tc.addrs {
		if addr != removedID {
			remaining = append(remaining, addr)
		}
	}
	reduced := ringWith(t, 3, remaining...)
	newOwnerID := ownerOn(reduced, testBucket)
	newOwner := nodeIndexByAddr(t, tc.addrs, newOwnerID)
	var held int
	for index, addr := range tc.addrs {
		if addr != removedID && addr != newOwnerID {
			held = index
		}
	}
	tc.ok(newOwner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	var buckets []string
	for i := 0; len(buckets) < 3 && i < 2000; i++ {
		name := fmt.Sprintf("fallback%03d", i)
		if ownerOn(full, name) == removedID && ownerOn(reduced, name) == newOwnerID {
			tc.ok(newOwner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
			buckets = append(buckets, name)
		}
	}
	for _, bucket := range buckets {
		waitForServing(t, tc, testDatabase, bucket, 3)
	}
	var rounds, failed atomic.Int64
	rounds.Store(2)
	owner := clusterOf(tc, newOwner)
	wrapPeerConn(owner.peerFor(mustNode(t, owner, tc.addrs[held])), func(inner peerConn) peerConn {
		return &questionFailingConn{peerConn: inner, rounds: &rounds, failed: &failed}
	})
	before := replicationGauge(t, tc.nodes[newOwner].server.core, "hoarddb_handoffs_without_previous_owner_total")
	stopNode(t, tc, removed)
	removeCommitted(t, tc, newOwner, removed)
	for _, bucket := range buckets {
		waitTakenOver(t, tc.nodes[newOwner].server.core, testDatabase, bucket)
	}
	if failed.Load() != 2 {
		t.Errorf("the member's answers were held for %d rounds, want 2", failed.Load())
	}
	if counted := replicationGauge(t, tc.nodes[newOwner].server.core, "hoarddb_handoffs_without_previous_owner_total") - before; counted != float64(len(buckets)) {
		t.Errorf("the new owner counted %v take-overs without the previous owner, want %d (buckets %s)", counted, len(buckets), strings.Join(buckets, ", "))
	}
}
