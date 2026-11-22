package server

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Replication dispatch 10b (build doc §7): the new owner of a bucket takes it
// over before it serves. These are the tests of step 10b.1 — rev 7's step 10.1
// with A-10.10's corrections — and each is committed with the step that turns
// it green.

// takeOverWait bounds how long a test waits for a take-over to finish: the
// fallback asks a removed node with a three-second deadline, and a whole copy
// is one pull.
const takeOverWait = 60 * time.Second

// holdsWithin polls cond for up to limit and reports whether it held.
func holdsWithin(limit time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(limit)
	for !cond() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
	return true
}

// waitTakenOver waits until s, the bucket's owner, has taken it over: it
// serves an opened copy with no mark (step 10b.5).
func waitTakenOver(t *testing.T, s *Server, database, bucket string) {
	t.Helper()
	key := placementOf(database, bucket)
	if !holdsWithin(takeOverWait, func() bool {
		if !s.bucketStoreOpen(database, bucket) {
			return false
		}
		_, opened, err := s.bucketLineage(database, bucket)
		return err == nil && opened && s.cluster.placementMarksOf(key).Moved == 0
	}) {
		t.Fatalf("%s never took %s/%s over", s.cluster.self.ID, database, bucket)
	}
}

// waitFollowing waits until follower's copy of the bucket is in owner's term
// with no mark (step 10b.6).
func waitFollowing(t *testing.T, follower, owner *Server, database, bucket string) {
	t.Helper()
	key := placementOf(database, bucket)
	if !holdsWithin(takeOverWait, func() bool {
		ownerTerm, _, ownerErr := owner.bucketLineage(database, bucket)
		term, _, err := follower.bucketLineage(database, bucket)
		marks := follower.cluster.placementMarksOf(key)
		return ownerErr == nil && err == nil && follower.bucketStoreOpen(database, bucket) && term == ownerTerm &&
			marks.Moved == 0 && marks.Outside == 0
	}) {
		t.Fatalf("%s never came to follow %s's term of %s/%s", follower.cluster.self.ID, owner.cluster.self.ID, database, bucket)
	}
}

// ownerLoss is spec §20.17's probe 1 (rev 7 step 10.1): four nodes at factor 3,
// buckets owned by node 1 that a lagging replica holds, five writes at
// majority acknowledged while that replica was stopped, and node 1 stopped
// before the replica returns, then removed. Nothing re-ships the five writes
// to the lagging replica, so the only complete copies are on node 1 and on the
// other replica.
type ownerLoss struct {
	tc      *testCluster
	owner   int
	lagging int
	// toNothing are buckets whose new owner held no copy before the removal,
	// and whose first holder in the member order is the lagging replica.
	toNothing []string
	// toLagging are buckets whose new owner is the lagging replica.
	toLagging []string
	// newOwner is each bucket's owner after the removal, by node index.
	newOwner map[string]int
	// writes are the keys acknowledged at majority while the lagging replica
	// was stopped.
	writes []string
}

// pickOwnerLoss chooses the owner and the lagging replica by the layout: the
// first pair whose buckets give perKind of each kind the tests assert over.
func pickOwnerLoss(t *testing.T, tc *testCluster, perKind int) *ownerLoss {
	t.Helper()
	for owner := range tc.nodes {
		for lagging := range tc.nodes {
			if lagging == owner {
				continue
			}
			if loss, ok := ownerLossBuckets(t, tc, owner, lagging, perKind); ok {
				return loss
			}
		}
	}
	t.Fatalf("setup: no owner and lagging replica of the layout give %d buckets of each kind; raise the bucket count", perKind)
	return nil
}

// ownerLossBuckets chooses bucket names on the rings before and after node
// owner's removal: perKind of each kind the tests assert over, or false.
func ownerLossBuckets(t *testing.T, tc *testCluster, owner, lagging, perKind int) (*ownerLoss, bool) {
	t.Helper()
	before := ringWith(t, 3, tc.addrs...)
	var rest []string
	for index, addr := range tc.addrs {
		if index != owner {
			rest = append(rest, addr)
		}
	}
	after := ringWith(t, 3, rest...)
	loss := &ownerLoss{tc: tc, owner: owner, lagging: lagging, newOwner: make(map[string]int)}
	for attempt := 0; attempt < 4000 && (len(loss.toNothing) < perKind || len(loss.toLagging) < perKind); attempt++ {
		name := fmt.Sprintf("moved%04d", attempt)
		key := placementOf(testDatabase, name).encoded
		set, err := before.Replicas(key, 3)
		if err != nil || set[0].ID != tc.addrs[owner] || !nodeListed(set, tc.addrs[lagging]) {
			continue
		}
		next, err := after.Replicas(key, 3)
		if err != nil {
			t.Fatalf("setup: placing %s after the removal: %v", name, err)
		}
		newOwner := nodeIndexByAddr(t, tc.addrs, next[0].ID)
		switch {
		case newOwner == lagging && len(loss.toLagging) < perKind:
			loss.toLagging = append(loss.toLagging, name)
		case !nodeListed(set, next[0].ID) && len(loss.toNothing) < perKind:
			loss.toNothing = append(loss.toNothing, name)
		default:
			continue
		}
		loss.newOwner[name] = newOwner
	}
	return loss, len(loss.toNothing) == perKind && len(loss.toLagging) == perKind
}

// buckets is every bucket of the construction.
func (l *ownerLoss) buckets() []string {
	return append(append([]string(nil), l.toNothing...), l.toLagging...)
}

// setUpAnOwnerLossWithALaggingReplica builds the construction up to the
// removal of the owner, and returns before the removal so a test can watch
// from before it.
func setUpAnOwnerLossWithALaggingReplica(t *testing.T) *ownerLoss {
	t.Helper()
	tc := newTestCluster(t, 4, 3)
	loss := pickOwnerLoss(t, tc, 4)
	owner, lagging := loss.owner, loss.lagging
	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for _, bucket := range loss.buckets() {
		tc.ok(owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, bucket, 3)
		tc.ok(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "seed",
			Data: map[string]interface{}{"n": -1}, WriteConcern: "all"})
	}

	laggingBase := tc.stopNodeKeepingData(lagging)
	for i := 0; i < 5; i++ {
		loss.writes = append(loss.writes, fmt.Sprintf("w%d", i))
	}
	for _, bucket := range loss.buckets() {
		for i, key := range loss.writes {
			tc.ok(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: key,
				Data: map[string]interface{}{"n": i}, WriteConcern: "majority"})
		}
	}
	tc.stopNodeKeepingData(owner)
	tc.nodes[lagging] = tc.startNode(lagging, laggingBase, tc.token)
	pollUntil(t, "the lagging replica is back and has confirmed its membership", func() bool {
		return !clusterOf(tc, lagging).bootstrapped.Load()
	})
	for _, bucket := range loss.buckets() {
		if holdsKeyIn(tc.nodes[lagging], testDatabase, bucket, loss.writes[0]) {
			t.Fatalf("setup: the lagging replica holds %s of %s before the removal; nothing should have re-shipped it", loss.writes[0], bucket)
		}
	}
	return loss
}

// removeTheStoppedOwner removes the stopped owner through a remaining node and
// brings every remaining node to the new epoch, as gossip would.
func (l *ownerLoss) removeTheStoppedOwner(t *testing.T) {
	t.Helper()
	var remaining []int
	for index := range l.tc.nodes {
		if index != l.owner {
			remaining = append(remaining, index)
		}
	}
	proposer := clusterOf(l.tc, remaining[len(remaining)-1])
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := proposer.proposeRemoval(ctx, l.tc.addrs[l.owner]); err != nil {
		t.Fatalf("removing the stopped owner, node %d: %v", l.owner+1, err)
	}
	for _, index := range remaining {
		c := clusterOf(l.tc, index)
		if c == proposer {
			continue
		}
		if err := c.reconcileMembership(proposer.self.ID, proposer.memberNodes(), proposer.memberIDs(), proposer.Epoch()); err != nil {
			t.Fatalf("bringing node %d to the removal: %v", index+1, err)
		}
	}
}

// missingWrites is the acknowledged writes of bucket that node index's own
// store does not hold.
func (l *ownerLoss) missingWrites(index int, bucket string) []string {
	var missing []string
	for _, key := range l.writes {
		if !holdsKeyIn(l.tc.nodes[index], testDatabase, bucket, key) {
			missing = append(missing, key)
		}
	}
	return missing
}

// firstHolderInMemberOrder is the first node in c's member order, other than
// c's own, that held a copy of bucket before the removal.
func (l *ownerLoss) firstHolderInMemberOrder(t *testing.T, c *Cluster, bucket string) int {
	t.Helper()
	before := ringWith(t, 3, l.tc.addrs...)
	set, err := before.Replicas(placementOf(testDatabase, bucket).encoded, 3)
	if err != nil {
		t.Fatalf("placing %s: %v", bucket, err)
	}
	for _, member := range c.Members() {
		if member.ID != c.self.ID && nodeListed(set, member.ID) {
			return nodeIndexByAddr(t, l.tc.addrs, member.ID)
		}
	}
	t.Fatalf("no member of %s's view held %s before the removal", c.self.ID, bucket)
	return -1
}

// TestOwnerMoveSeedsFromAHolderWithEveryAcknowledgedWrite is the owner-move
// seed test, the first half of spec §20.17's probe 1: for every bucket whose new owner
// held nothing, and whose first holder in member order is the replica that
// was behind, the new owner's own copy holds the five writes acknowledged at
// majority. Guard: take the first holder in member order.
func TestOwnerMoveSeedsFromAHolderWithEveryAcknowledgedWrite(t *testing.T) {
	loss := setUpAnOwnerLossWithALaggingReplica(t)
	defer loss.tc.stop()
	loss.removeTheStoppedOwner(t)

	asserted := 0
	for _, bucket := range loss.toNothing {
		newOwner := loss.newOwner[bucket]
		if loss.firstHolderInMemberOrder(t, clusterOf(loss.tc, newOwner), bucket) != loss.lagging {
			continue
		}
		asserted++
		node := loss.tc.nodes[newOwner]
		if !holdsWithin(takeOverWait, func() bool { return node.server.core.bucketStoreOpen(testDatabase, bucket) }) {
			t.Errorf("node %d, the new owner of %s, never came to hold a copy", newOwner+1, bucket)
			continue
		}
		if missing := loss.missingWrites(newOwner, bucket); len(missing) > 0 {
			t.Errorf("node %d, the new owner of %s, which held nothing, serves a copy without %d of the 5 writes acknowledged at majority (%v): "+
				"it was seeded from the replica that was behind", newOwner+1, bucket, len(missing), missing)
		}
	}
	if asserted == 0 {
		t.Fatal("setup: no bucket moved to a node that held nothing with the lagging replica first in its member order; raise the bucket count")
	}
}

// TestALaggingReplicaThatBecomesOwnerIsCompleteBeforeItServes is the second
// half of probe 1: for every bucket whose new owner is the replica that was
// behind, a client writes to the bucket through that node in a loop from
// before the removal, and every write it is answered a success for is
// answered by a node whose own copy holds the five. Guard: open without
// waiting — the lagging copy serves at once.
func TestALaggingReplicaThatBecomesOwnerIsCompleteBeforeItServes(t *testing.T) {
	loss := setUpAnOwnerLossWithALaggingReplica(t)
	defer loss.tc.stop()

	type firstSuccess struct {
		bucket  string
		missing []string
	}
	answered := make(chan firstSuccess, len(loss.toLagging))
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		pending := slices.Clone(loss.toLagging)
		for attempt := 0; len(pending) > 0; attempt++ {
			select {
			case <-stop:
				return
			default:
			}
			var left []string
			for _, bucket := range pending {
				frame := loss.tc.frame(loss.lagging, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket,
					Key: fmt.Sprintf("loop%d", attempt), Data: map[string]interface{}{"n": attempt}})
				if frame.Opcode == transport.OpError {
					left = append(left, bucket)
					continue
				}
				answered <- firstSuccess{bucket: bucket, missing: loss.missingWrites(loss.lagging, bucket)}
			}
			pending = left
			time.Sleep(20 * time.Millisecond)
		}
	}()
	loss.removeTheStoppedOwner(t)

	successes := 0
	timeout := time.After(takeOverWait)
	for successes < len(loss.toLagging) {
		select {
		case first := <-answered:
			successes++
			if len(first.missing) > 0 {
				t.Errorf("node %d, the replica that was behind and the new owner of %s, answered a write a success while its own copy "+
					"lacked %d of the 5 writes acknowledged at majority (%v)", loss.lagging+1, first.bucket, len(first.missing), first.missing)
			}
		case <-timeout:
			t.Errorf("only %d of the %d buckets moved to the lagging replica accepted a write within %s", successes, len(loss.toLagging), takeOverWait)
			successes = len(loss.toLagging)
		}
	}
	close(stop)
	<-done
}

// TestOwnerLossWithAPrunedLogLosesNothingARemainingNodeHolds is the first test
// with no log left to complete from (rev 7 A-10.10, F-8 item 4): after the
// writes, every node's log is pruned past them, the lagging replica's claim
// on the logs removed by restarting it past a handshake with its stream from
// the owner held, so that no remaining node's log holds an entry of the five.
// The new owner still holds all five. Guard: complete the new owner from a log
// instead of a copy — there is none to complete from.
func TestOwnerLossWithAPrunedLogLosesNothingARemainingNodeHolds(t *testing.T) {
	tc := newTestCluster(t, 4, 3)
	defer tc.stop()
	loss := pickOwnerLoss(t, tc, 4)
	owner, lagging := loss.owner, loss.lagging
	for index := range tc.nodes {
		tc.nodes[index].server.core.wal.SetMaxSegmentBytes(4 << 10)
	}
	tc.ok(owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for _, bucket := range loss.buckets() {
		tc.ok(owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, bucket, 3)
	}
	// The lagging replica keeps every entry of the owner's stream from
	// reaching it while it is up, so it can hand its claims on the logs back
	// without being caught up.
	release := holdStreamPushesOnly(peerOf(t, tc, owner, lagging))
	defer release()
	for i := 0; i < 5; i++ {
		loss.writes = append(loss.writes, fmt.Sprintf("w%d", i))
	}
	for _, bucket := range loss.buckets() {
		for i, key := range loss.writes {
			tc.ok(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: key,
				Data: map[string]interface{}{"n": i, "pad": string(make([]byte, 512))}, WriteConcern: "majority"})
		}
	}
	// Padding writes rotate the segments the five sit in on every node. A node
	// logs only the buckets whose set includes it, so the padding goes to
	// buckets whose sets together cover every node: one padding bucket, at
	// factor 3 on four nodes, leaves a node out on some port layouts, and that
	// node's last segment, which holds the five, is never sealed.
	var padding []string
	covered := make(map[string]bool)
	members := ringWith(t, 3, tc.addrs...)
	for i := 0; len(covered) < len(tc.addrs); i++ {
		name := fmt.Sprintf("padding%03d", i)
		set, err := members.Replicas(placementOf(testDatabase, name).encoded, 3)
		if err != nil {
			t.Fatalf("placing %s: %v", name, err)
		}
		adds := false
		for _, member := range set {
			adds = adds || !covered[member.ID]
		}
		if !adds {
			continue
		}
		for _, member := range set {
			covered[member.ID] = true
		}
		padding = append(padding, name)
		tc.ok(owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
	}
	padded := 0
	pad := func() {
		for end := padded + 40; padded < end; padded++ {
			tc.ok(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: padding[padded%len(padding)], Key: fmt.Sprintf("p%d", padded),
				Data: map[string]interface{}{"pad": string(make([]byte, 512))}, WriteConcern: "majority"})
		}
	}
	pad()
	for _, bucket := range loss.buckets() {
		if holdsKeyIn(tc.nodes[lagging], testDatabase, bucket, loss.writes[0]) {
			t.Fatalf("setup: the lagging replica holds %s of %s; its stream from the owner was not held", loss.writes[0], bucket)
		}
	}
	// Pruned while the owner still answers advance notices: a node's log is
	// held by every member's cursor in its stream, the owner's included. A
	// cursor not yet past a sealed segment holds it: the condition is then
	// built again with more padding, and the logs pruned again.
	pruned := func(index int) bool {
		return holdsWithin(10*time.Second, func() bool {
			if err := clusterOf(tc, index).pruneWAL(); err != nil {
				t.Logf("pruning node %d's log: %v", index+1, err)
			}
			return !logHoldsAnyOf(t, tc, index, loss)
		})
	}
	for attempt := 1; ; attempt++ {
		holding := -1
		for index := range tc.nodes {
			if index != owner && !pruned(index) {
				holding = index
				break
			}
		}
		if holding < 0 {
			break
		}
		if attempt == 5 {
			t.Fatalf("setup: node %d's log still holds an entry of the five writes after pruning and %d padding writes; the condition is not built",
				holding+1, padded)
		}
		pad()
	}
	tc.stopNodeKeepingData(owner)
	loss.removeTheStoppedOwner(t)
	for _, bucket := range loss.toNothing {
		newOwner := loss.newOwner[bucket]
		node := tc.nodes[newOwner]
		if !holdsWithin(takeOverWait, func() bool { return node.server.core.bucketStoreOpen(testDatabase, bucket) }) {
			t.Errorf("node %d, the new owner of %s, never came to hold a copy", newOwner+1, bucket)
			continue
		}
		if missing := loss.missingWrites(newOwner, bucket); len(missing) > 0 {
			t.Errorf("node %d, the new owner of %s, serves a copy without %d of the 5 writes acknowledged at majority (%v), "+
				"although a remaining node still holds them", newOwner+1, bucket, len(missing), missing)
		}
	}
}

// logHoldsAnyOf reports whether node index's own log holds an entry of one of
// the construction's writes.
func logHoldsAnyOf(t *testing.T, tc *testCluster, index int, loss *ownerLoss) bool {
	t.Helper()
	entries, err := tc.nodes[index].server.core.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading node %d's log: %v", index+1, err)
	}
	buckets := loss.buckets()
	for _, entry := range entries {
		if transport.Opcode(entry.Op) == transport.OpPut && slices.Contains(buckets, entry.Bucket) {
			return true
		}
	}
	return false
}

// queueHistory is a queue bucket's acknowledged history: the elements of every
// push answered a success, less those every pop answered a success took, in
// the order the queue serves them.
type queueHistory struct {
	bucket string
	kind   BucketType
	pushed []int32
	popped []int32
}

// remaining is the history's elements in the order a drain returns them: a
// FIFO's in push order, a heap's highest priority first.
func (h queueHistory) remaining() []int32 {
	var left []int32
	for _, n := range h.pushed {
		if !slices.Contains(h.popped, n) {
			left = append(left, n)
		}
	}
	if h.kind == BucketTypeHeap {
		slices.SortFunc(left, func(a, b int32) int { return int(b - a) })
	}
	return left
}

// TestAQueueKeepsItsOrderAndCountAcrossAnOwnerChange: a FIFO and a heap
// bucket; forty pushes and ten pops at majority with one replica's stream
// held; a join that moves the buckets to the joiner, then the removal of the
// joiner while it is stopped. Each remaining holder's own copy, drained,
// equals the acknowledged history — not merely the other holders' (rev 7
// A-10.10, F-8 item 5). Guard: rev 7's.
func TestAQueueKeepsItsOrderAndCountAcrossAnOwnerChange(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	shortenSweeps(t, 0)
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	before := ringWith(t, 3, tc.addrs...)
	after := ringWith(t, 3, append(append([]string(nil), tc.addrs...), newcomer.ID)...)
	var owner string
	movedToTheJoiner := func(name string) bool {
		key := placementOf(testDatabase, name).encoded
		joined, _ := after.Replicas(key, 3)
		was, _ := before.Owner(key)
		if joined[0].ID != newcomer.ID || (owner != "" && was.ID != owner) {
			return false
		}
		owner = was.ID
		return true
	}
	fifo := bucketNamed(t, "fifo", movedToTheJoiner)
	heap := bucketNamed(t, "heap", movedToTheJoiner)
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner)
	delayed := (ownerIndex + 1) % len(tc.nodes)

	tc.ok(ownerIndex, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	histories := []*queueHistory{{bucket: fifo, kind: BucketTypeFIFO}, {bucket: heap, kind: BucketTypeHeap}}
	for _, history := range histories {
		tc.ok(ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: history.bucket, Type: history.kind})
		waitForServing(t, tc, testDatabase, history.bucket, 3)
	}
	release := holdStreamPushesOnly(peerOf(t, tc, ownerIndex, delayed))
	for _, history := range histories {
		push, pop := transport.OpFIFOPush, transport.OpFIFOPop
		if history.kind == BucketTypeHeap {
			push, pop = transport.OpHeapPush, transport.OpHeapPop
		}
		for n := int32(0); n < 40; n++ {
			frame := tc.frame(ownerIndex, push, PushRequest{Database: testDatabase, Bucket: history.bucket,
				Data: map[string]interface{}{"n": n}, Priority: int64(n), WriteConcern: "majority"})
			if frame.Opcode != transport.OpError {
				history.pushed = append(history.pushed, n)
			}
		}
		for i := 0; i < 10; i++ {
			frame := tc.frame(ownerIndex, pop, bson.M{"database": testDatabase, "bucket": history.bucket, "write_concern": "majority"})
			if frame.Opcode == transport.OpError {
				continue
			}
			var popped PopResponse
			if err := bson.Unmarshal(frame.Payload, &popped); err != nil {
				t.Fatalf("decoding a pop of %s: %v", history.bucket, err)
			}
			var element struct {
				N int32 `bson:"n"`
			}
			if popped.Found && bson.Unmarshal(popped.Items[0], &element) == nil {
				history.popped = append(history.popped, element.N)
			}
		}
	}

	joiner := joinNewcomer(t, tc, ownerIndex, ports)
	pollUntil(t, "the joiner serves both queues it now owns", func() bool {
		for _, history := range histories {
			if !joiner.node.server.core.bucketStoreOpen(testDatabase, history.bucket) {
				return false
			}
		}
		return true
	})
	// The held push outlives its ship timeout, so the old owner's sender gives
	// it up and meets the entries again from its log after the join: the
	// delayed replica is behind at the adoption, and only a ceded owner that
	// finishes delivering brings it level.
	time.Sleep(clusterOf(tc, ownerIndex).replShipTimeout() + time.Second)
	for _, history := range histories {
		if held := queueElementsOn(t, tc.nodes[delayed].server.core, history.bucket); len(held) >= len(history.remaining()) {
			t.Fatalf("setup: the delayed replica, node %d, holds %d elements of %s at the join; it was not behind", delayed+1, len(held), history.bucket)
		}
	}
	release()
	joiner.stop()
	proposer := clusterOf(tc, ownerIndex)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := proposer.proposeRemoval(ctx, newcomer.ID); err != nil {
		t.Fatalf("removing the stopped joiner: %v", err)
	}
	for index := range tc.nodes {
		if index != ownerIndex {
			if err := clusterOf(tc, index).reconcileMembership(proposer.self.ID, proposer.memberNodes(), proposer.memberIDs(), proposer.Epoch()); err != nil {
				t.Fatalf("bringing node %d to the removal: %v", index+1, err)
			}
		}
	}
	// The holders settle: each follows the owner, or is copied from it.
	for _, history := range histories {
		want := history.remaining()
		for index := range tc.nodes {
			var got []int32
			converged := holdsWithin(takeOverWait, func() bool {
				got = queueElementsOn(t, tc.nodes[index].server.core, history.bucket)
				return slices.Equal(got, want)
			})
			if !converged {
				t.Errorf("node %d's own copy of %s %s holds %v; the acknowledged history is %v", index+1, history.kind, history.bucket, got, want)
			}
		}
	}
}

// queueElementsOn reads a queue bucket's elements from one node's own store,
// in the order it serves them, without changing it: a snapshot of the bucket's
// range (Cluster.snapshotBucket) is materialised in a scratch server and
// drained there.
func queueElementsOn(t *testing.T, s *Server, bucket string) []int32 {
	t.Helper()
	if !s.bucketStoreOpen(testDatabase, bucket) {
		return nil
	}
	snapshot, err := s.cluster.snapshotBucket(testDatabase, bucket, "")
	if err != nil {
		return nil
	}
	scratch := newReplayTestServer(t, filepath.Join(t.TempDir(), "scratch"), quietLogger())
	defer closeReplayTestServer(t, scratch)
	var stream []byte
	for _, chunk := range snapshot.chunks {
		stream = append(stream, chunk...)
	}
	pulled := migratedBucket{database: testDatabase, name: bucket, storeType: snapshot.storeType, generation: snapshot.generation}
	if err := scratch.materializeRange(pulled, stream, 0); err != nil {
		t.Fatalf("materialising node %s's copy of %s for reading: %v", s.cluster.self.ID, bucket, err)
	}
	return drainQueue(t, scratch, bucket)
}

// TestTakeOverRoundStopsAtCancelledContext cancels the round's context during
// the first of three owned buckets and checks that the round reads no later
// bucket's copy, and leaves each of them pending so a restart finds it still
// unsettled.
func TestTakeOverRoundStopsAtCancelledContext(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	defer tc.stop()
	cluster := clusterOf(tc, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var owned []roundItem
	for _, name := range []string{"first", "second", "third"} {
		owned = append(owned, roundItem{held: heldBucket{database: testDatabase, bucket: name}, key: placementOf(testDatabase, name)})
	}
	var reads []string
	hook := func(nodeID, database, bucket string) {
		reads = append(reads, bucket)
		cancel()
	}
	takeOverBeforeReadHook.Store(&hook)
	t.Cleanup(func() { takeOverBeforeReadHook.Store(nil) })

	outcomes := make(map[heldBucket]copyOutcome)
	cluster.takeOverOwned(ctx, owned, outcomes, nil)

	if len(reads) != 1 || reads[0] != "first" {
		t.Errorf("the round read the copies of %v after its context was cancelled; want only [first]", reads)
	}
	for _, later := range owned[1:] {
		if outcome, noted := outcomes[later.held]; !noted || outcome != copyPending {
			t.Errorf("bucket %q left by a cancelled round has outcome %v (noted %v); want pending", later.held.bucket, outcome, noted)
		}
	}
}

// TestATakeOverThatCopiesOverItsOwnCopySaysWhatItGivesUp (BUG-0475): a
// take-over that replaces this node's own copy with the best copy releases
// entries the own copy held past the best copy's base, and that WARN is the
// only trace an operator gets of them. Two members at factor 2 hold a bucket;
// the owner runs copyForTakeOver over its own copy with the replica's copy as
// the best. The pull is real; the two lineages are the answers handed in,
// which is all the concession is judged from, set to the case each subtest
// names. Guard: copyForTakeOver without its noteConcession call; nothing is
// logged in any case.
func TestATakeOverThatCopiesOverItsOwnCopySaysWhatItGivesUp(t *testing.T) {
	cases := []struct {
		name string
		// best is the best copy's lineage, from the own copy's (term, through).
		best     func(ownTerm, ownThrough uint64) (term, through, baseTerm, baseThrough uint64)
		warnSays string
	}{
		{name: "the best copy's term was opened from the own copy's term below its position",
			best: func(ownTerm, ownThrough uint64) (uint64, uint64, uint64, uint64) {
				return ownTerm + 1, 1, ownTerm, ownThrough - 2
			},
			warnSays: "held 2 entries its owner took the bucket over without"},
		{name: "the best copy's term was opened from a term below the own copy's",
			best: func(ownTerm, ownThrough uint64) (uint64, uint64, uint64, uint64) {
				return ownTerm + 1, 1, ownTerm - 1, 3
			},
			warnSays: fmt.Sprintf("at least %d entries", 5)},
		{name: "the best copy's term was opened from the own copy's position",
			best: func(ownTerm, ownThrough uint64) (uint64, uint64, uint64, uint64) {
				return ownTerm + 1, 1, ownTerm, ownThrough
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			warnings := &syncBuffer{}
			cluster := newTestClusterTuned(t, 2, 2, nil, func(index int, c *Cluster) {
				if index == 0 {
					c.logger = slog.New(slog.NewTextHandler(warnings, &slog.HandlerOptions{Level: slog.LevelWarn}))
				}
			})
			defer cluster.stop()
			pair := ringWith(t, 2, cluster.addrs...)
			bucket := bucketNamed(t, "concede", func(name string) bool { return ownerOn(pair, name) == cluster.addrs[0] })
			cluster.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
			cluster.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
			waitForServing(t, cluster, testDatabase, bucket, 2)
			for i := range 5 {
				cluster.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("k%d", i),
					Data: map[string]interface{}{"n": i}, WriteConcern: "all"})
			}
			pollUntil(t, "the replica holds every key", func() bool { return holdsKeyLocally(cluster.nodes[1], bucket, "k4") })
			owner, replica := clusterOf(cluster, 0), clusterOf(cluster, 1)
			ref := ReplBucketRef{Database: testDatabase, Bucket: bucket}
			own, err := owner.answerCopy(ref)
			if err != nil {
				t.Fatalf("reading the owner's copy: %v", err)
			}
			theirs, err := replica.answerCopy(ref)
			if err != nil {
				t.Fatalf("reading the replica's copy: %v", err)
			}
			// The own copy is placed at (term 4, through 5) so every case's
			// base exists; the best copy's lineage follows from it.
			own.Term, own.Through, own.BaseTerm, own.BaseThrough = 4, 5, 0, 0
			theirs.Term, theirs.Through, theirs.BaseTerm, theirs.BaseThrough = tc.best(own.Term, own.Through)
			best := heldCopy{node: replica.self, answer: theirs, current: true}
			ctx, cancel := context.WithTimeout(context.Background(), takeOverWait)
			defer cancel()
			if err := owner.copyForTakeOver(ctx, heldBucket{database: testDatabase, bucket: bucket}, placementOf(testDatabase, bucket), own,
				takeOverPlan{best: best}); err != nil {
				t.Fatalf("copyForTakeOver: %v", err)
			}
			written := warnings.String()
			if tc.warnSays == "" {
				if strings.Contains(written, "released a copy") {
					t.Errorf("a take-over that gives up nothing logged a concession: %s", written)
				}
				return
			}
			if !strings.Contains(written, "released a copy") || !strings.Contains(written, tc.warnSays) {
				t.Errorf("a take-over that copied over its own copy (term %d, through %d) with the best copy (term %d, base %d/%d) "+
					"did not log the concession saying %q; logged: %q", own.Term, own.Through, theirs.Term, theirs.BaseTerm, theirs.BaseThrough,
					tc.warnSays, written)
			}
		})
	}
}
