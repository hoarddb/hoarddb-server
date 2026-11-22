package server

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Step 10b.3 (spec §20.8.4): the check inside the fence. An owner that has not
// taken a bucket over refuses its writes and its drops, and a create of it
// waits for the take-over and is answered from the copy it lands.

// heldTakeOver is a bucket whose owner moves to a joiner that holds nothing,
// with the joiner's take-over held before it copies the bucket in.
type heldTakeOver struct {
	tc       *testCluster
	joiner   *restartableJoiner
	bucket   string
	oldOwner int
	release  func()
}

// nodes is every node of the construction, the joiner last.
func (h *heldTakeOver) nodes() []*testNode {
	return append(append([]*testNode(nil), h.tc.nodes...), h.joiner.node)
}

// logs is each node's last log sequence.
func (h *heldTakeOver) logs() []uint64 {
	var sequences []uint64
	for _, node := range h.nodes() {
		sequences = append(sequences, node.server.core.wal.LastSequence())
	}
	return sequences
}

// frameThrough sends a request through node index of nodes().
func (h *heldTakeOver) frameThrough(index int, op transport.Opcode, req any) *transport.Frame {
	if index == len(h.tc.nodes) {
		return h.tc.frameOn(h.joiner.node, op, req)
	}
	return h.tc.frame(index, op, req)
}

// setUpAHeldTakeOver starts three nodes at factor 2, creates a bucket the
// join of a fourth moves to the joiner, writes "held" to it, and joins the
// fourth with its take-over of the bucket held before it copies it in.
// also narrows the bucket choice further: the bucket must also satisfy every
// also(addrs, newcomerID, name), given the cluster's addresses before the join.
func setUpAHeldTakeOver(t *testing.T, also ...func(addrs []string, newcomerID, name string) bool) *heldTakeOver {
	t.Helper()
	tc := newTestCluster(t, 3, 2)
	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	after := ringWith(t, 2, append(append([]string(nil), tc.addrs...), newcomer.ID)...)
	before := ringWith(t, 2, tc.addrs...)
	bucket := bucketNamed(t, "moving", func(name string) bool {
		if ownerOn(after, name) != newcomer.ID {
			return false
		}
		for _, ok := range also {
			if !ok(tc.addrs, newcomer.ID, name) {
				return false
			}
		}
		return true
	})
	h := &heldTakeOver{tc: tc, bucket: bucket, oldOwner: nodeIndexByAddr(t, tc.addrs, ownerOn(before, bucket))}
	tc.ok(h.oldOwner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(h.oldOwner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 2)
	tc.ok(h.oldOwner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "held",
		Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})

	reached := make(chan struct{})
	gate := make(chan struct{})
	var reachedOnce, releaseOnce sync.Once
	h.release = func() { releaseOnce.Do(func() { close(gate) }) }
	hold := func(nodeID, database, name string) {
		if nodeID != newcomer.ID || name != bucket {
			return
		}
		reachedOnce.Do(func() { close(reached) })
		<-gate
	}
	takeOverBeforeOpenHook.Store(&hold)
	t.Cleanup(func() { takeOverBeforeOpenHook.Store(nil) })
	t.Cleanup(h.release)
	h.joiner = joinNewcomer(t, tc, h.oldOwner, ports)
	// Cleanups run last in, first out: registered after the joiner's, the
	// release runs before the joiner's Stop, which waits on the take-over
	// worker parked in hold. Without it a test that fails before its own
	// release hangs until the binary's timeout instead of reporting.
	t.Cleanup(h.release)
	select {
	case <-reached:
	case <-time.After(takeOverWait):
		t.Fatalf("setup: the joiner never reached the open of its take-over of %s", bucket)
	}
	return h
}

// TestAWriteIsRefusedNotStoredWhileOwnershipIsMoving (rev 7 step 10.3): a
// hook holds the new owner's take-over before its open; writes through every
// node are refused UNAVAILABLE with write_outcome not_applied, and no node's
// log grows; released, the next write succeeds. And the race the check exists
// for: a writer held by afterJoinFenceHook on the old owner while a join is
// adopted is refused once released, and its log has not grown. Guard: make the
// check in Cluster.write, before the fence — the held writer appends.
func TestAWriteIsRefusedNotStoredWhileOwnershipIsMoving(t *testing.T) {
	t.Run("through every node while the take-over is held", func(t *testing.T) {
		h := setUpAHeldTakeOver(t)
		defer h.tc.stop()
		before := h.logs()
		for index := range h.nodes() {
			frame := h.frameThrough(index, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: h.bucket,
				Key: fmt.Sprintf("refused%d", index), Data: map[string]interface{}{"n": index}})
			requireRetryRefusal(t, frame, fmt.Sprintf("a write through node %d while the take-over is held", index+1))
		}
		if after := h.logs(); fmt.Sprint(after) != fmt.Sprint(before) {
			t.Errorf("the nodes' logs went from %v to %v for writes refused while ownership moved", before, after)
		}
		h.release()
		if !holdsWithin(takeOverWait, func() bool {
			return h.frameThrough(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: h.bucket, Key: "after",
				Data: map[string]interface{}{"n": 0}}).Opcode != transport.OpError
		}) {
			t.Fatal("no write succeeded after the take-over was released")
		}
		if !holdsKeyIn(h.joiner.node, testDatabase, h.bucket, "held") {
			t.Error("the new owner serves a copy without the write made before the join")
		}
	})

	t.Run("a writer held across the adoption on the old owner", func(t *testing.T) {
		tc := newTestCluster(t, 3, 2)
		defer tc.stop()
		ports := reserveNodePorts(t)
		newcomer := ports.ringNode()
		after := ringWith(t, 2, append(append([]string(nil), tc.addrs...), newcomer.ID)...)
		before := ringWith(t, 2, tc.addrs...)
		bucket := bucketNamed(t, "raced", func(name string) bool { return ownerOn(after, name) == newcomer.ID })
		oldOwner := nodeIndexByAddr(t, tc.addrs, ownerOn(before, bucket))
		tc.ok(oldOwner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		tc.ok(oldOwner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
		waitForServing(t, tc, testDatabase, bucket, 2)

		held := make(chan struct{})
		gate := make(chan struct{})
		var heldOnce sync.Once
		hold := func(op transport.Opcode, database, name string) {
			if op != transport.OpPut || name != bucket {
				return
			}
			heldOnce.Do(func() { close(held) })
			<-gate
		}
		afterJoinFenceHook.Store(&hold)
		defer afterJoinFenceHook.Store(nil)
		answered := make(chan *transport.Frame, 1)
		go func() {
			answered <- tc.frame(oldOwner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "raced",
				Data: map[string]interface{}{"n": 1}})
		}()
		select {
		case <-held:
		case <-time.After(10 * time.Second):
			t.Fatal("setup: the write never reached the hook after the join fence")
		}
		logged := tc.nodes[oldOwner].server.core.wal.LastSequence()
		epoch := clusterOf(tc, oldOwner).Epoch()
		joiner := joinNewcomer(t, tc, (oldOwner+1)%len(tc.nodes), ports)
		defer joiner.stop()
		pollUntil(t, "the old owner adopts the join", func() bool { return clusterOf(tc, oldOwner).Epoch() > epoch })
		close(gate)
		var frame *transport.Frame
		select {
		case frame = <-answered:
		case <-time.After(30 * time.Second):
			t.Fatal("the held write was never answered")
		}
		if frame.Opcode != transport.OpError {
			t.Errorf("a write held across the adoption was answered %s; the old owner no longer owns the bucket", frame.Opcode)
		} else if refused := refusalOf(t, frame); refused.WriteOutcome != outcomeNotApplied {
			t.Errorf("a write held across the adoption was refused %q, want not_applied: %s", refused.WriteOutcome, refused.Message)
		}
		if now := tc.nodes[oldOwner].server.core.wal.LastSequence(); now != logged {
			t.Errorf("the old owner's log went from %d to %d for a write it no longer owned", logged, now)
		}
	})
}

// TestACreateAtAnOwnerMidTakeOverAnswersFromTheTakenCopy: the take-over held
// before it copies the bucket in; a client's create of the name through a
// member does not return while held; released, it answers that the bucket
// exists, no node logged a create, and "held" reads back at the owner.
// The join freeze is switched off: the test is about the take-over's own
// line, which a schema change meets only after the freeze. Guard: exempt the
// create — it is answered before the take-over lands.
func TestACreateAtAnOwnerMidTakeOverAnswersFromTheTakenCopy(t *testing.T) {
	switchJoinFreezeOff(t)
	h := setUpAHeldTakeOver(t)
	defer h.tc.stop()
	creates := h.createsLogged(t)
	answered := make(chan *transport.Frame, 1)
	through := (h.oldOwner + 1) % len(h.tc.nodes)
	go func() {
		answered <- h.frameThrough(through, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: h.bucket, Type: BucketTypeHash})
	}()
	select {
	case frame := <-answered:
		t.Fatalf("the create returned while the take-over was held: %s %s", frame.Opcode, frame.Payload)
	case <-time.After(2 * time.Second):
	}
	h.release()
	var frame *transport.Frame
	select {
	case frame = <-answered:
	case <-time.After(takeOverWait):
		t.Fatal("the create was never answered after the take-over was released")
	}
	if frame.Opcode == transport.OpError {
		t.Fatalf("the create was refused after the take-over landed: %s", frame.Payload)
	}
	var created CreateBucketResponse
	if err := bson.Unmarshal(frame.Payload, &created); err != nil {
		t.Fatalf("decoding the create's answer: %v", err)
	}
	if !created.Success || !strings.Contains(created.Message, "already exists") {
		t.Errorf("the create answered %+v; want the bucket that already exists", created)
	}
	if after := h.createsLogged(t); fmt.Sprint(after) != fmt.Sprint(creates) {
		t.Errorf("creates of %s logged went from %v to %v; the create must not be logged", h.bucket, creates, after)
	}
	if !foundThrough(h.tc, through, testDatabase, h.bucket, "held") {
		t.Error("\"held\", written before the join, does not read back through the owner")
	}
}

// createsLogged is how many creates of the bucket each node's log holds.
func (h *heldTakeOver) createsLogged(t *testing.T) []int {
	t.Helper()
	var counts []int
	for index, node := range h.nodes() {
		entries, err := node.server.core.wal.ReadFrom(1)
		if err != nil {
			t.Fatalf("reading node %d's log: %v", index+1, err)
		}
		count := 0
		for _, entry := range entries {
			if transport.Opcode(entry.Op) == transport.OpCreateBucket && entry.Bucket == h.bucket {
				count++
			}
		}
		counts = append(counts, count)
	}
	return counts
}

// TestADropAtAnOwnerMidTakeOverIsRefused: the new owner holds a copy it has
// not taken over — its predecessor was removed while running, and its
// take-over is held before it opens — and a drop of the bucket is refused
// UNAVAILABLE, not_applied, with no node's log grown (spec Q39). The join
// freeze is switched off, as for the create. Guard: exempt the drop.
func TestADropAtAnOwnerMidTakeOverIsRefused(t *testing.T) {
	switchJoinFreezeOff(t)
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	before := ringWith(t, 3, tc.addrs...)
	bucket := bucketNamed(t, "dropped", func(name string) bool { return ownerOn(before, name) == tc.addrs[0] })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 3)
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "held", Data: map[string]interface{}{"n": 1}, WriteConcern: "all"})
	newOwnerID := ownerOn(ringWith(t, 3, tc.addrs[1], tc.addrs[2]), bucket)
	newOwner := nodeIndexByAddr(t, tc.addrs, newOwnerID)

	reached := make(chan struct{})
	gate := make(chan struct{})
	var reachedOnce, releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	defer release()
	hold := func(nodeID, database, name string) {
		if nodeID != newOwnerID || name != bucket {
			return
		}
		reachedOnce.Do(func() { close(reached) })
		<-gate
	}
	takeOverBeforeOpenHook.Store(&hold)
	defer takeOverBeforeOpenHook.Store(nil)
	removeAndTell(t, tc, newOwner, 0)
	other := 3 - newOwner
	via := clusterOf(tc, newOwner)
	if err := clusterOf(tc, other).reconcileMembership(via.self.ID, via.memberNodes(), via.memberIDs(), via.Epoch()); err != nil {
		t.Fatalf("bringing node %d to the removal: %v", other+1, err)
	}
	select {
	case <-reached:
	case <-time.After(takeOverWait):
		t.Fatal("setup: the new owner never reached the open of its take-over")
	}
	logs := []uint64{tc.nodes[newOwner].server.core.wal.LastSequence(), tc.nodes[other].server.core.wal.LastSequence()}
	for _, index := range []int{newOwner, other} {
		frame := tc.frame(index, transport.OpDeleteBucket, bson.M{"database": testDatabase, "name": bucket})
		requireRetryRefusal(t, frame, fmt.Sprintf("a drop through node %d while the new owner's take-over is held", index+1))
	}
	if now := []uint64{tc.nodes[newOwner].server.core.wal.LastSequence(), tc.nodes[other].server.core.wal.LastSequence()}; fmt.Sprint(now) != fmt.Sprint(logs) {
		t.Errorf("the members' logs went from %v to %v for a drop refused mid take-over", logs, now)
	}
	release()
	if !holdsWithin(takeOverWait, func() bool { return foundThrough(tc, newOwner, testDatabase, bucket, "held") }) {
		t.Error("the bucket did not serve \"held\" once the take-over was released")
	}
}
