package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// streamPush sends entries to core as one push of source's stream, under the
// incarnation it names, and returns the answer.
func streamPush(t *testing.T, core *Server, source string, incarnation []byte, entries []ReplEntry) ReplPushResponse {
	t.Helper()
	push, err := bson.Marshal(ReplPushRequest{
		Token: core.cluster.localToken(), FromNode: source, Entries: entries, Epoch: core.cluster.Epoch(),
		Stream: &ReplStream{Incarnation: incarnation},
	})
	if err != nil {
		t.Fatalf("marshal a push of %s's stream: %v", source, err)
	}
	respFrame, err := handleReplPush(context.Background(), transport.NewFrame(transport.OpReplPush, push), core)
	if err != nil {
		t.Fatalf("push of %s's stream: %v", source, err)
	}
	var resp ReplPushResponse
	if err := bson.Unmarshal(respFrame.Payload, &resp); err != nil {
		t.Fatalf("decode the answer to a push of %s's stream: %v", source, err)
	}
	return resp
}

// openedStream waits until receiver has recorded an incarnation for source —
// source's stream to it has opened — and returns the Stream a push of that
// stream carries. A test that sends a stream push by hand sends it as one
// that has opened, which is the only kind a receiver applies.
func openedStream(t *testing.T, receiver *Cluster, source string) *ReplStream {
	t.Helper()
	var incarnation []byte
	pollUntil(t, fmt.Sprintf("%s's stream to %s has opened", source, receiver.self.ID), func() bool {
		incarnation = receiver.recordedPosition(source).Incarnation
		return len(incarnation) > 0
	})
	return &ReplStream{Incarnation: incarnation}
}

// streamOf is the Stream a push from source to receiver carries once source's
// stream has opened. For a member, it waits for the member's own sender to
// open it. A source the test names and no node runs has no sender, so the
// test opens its stream by hand, as a sender with one incarnation would.
func streamOf(t *testing.T, receiver *Cluster, source string) *ReplStream {
	t.Helper()
	if _, member := receiver.memberByID(source); member {
		return openedStream(t, receiver, source)
	}
	if recorded := receiver.recordedPosition(source).Incarnation; len(recorded) > 0 {
		return &ReplStream{Incarnation: recorded}
	}
	named := sha256.Sum256([]byte(source))
	incarnation := named[:16]
	resp := receiver.answerHandshake(source, &ReplStream{Incarnation: incarnation, Open: true,
		Rebase: &ReplRebase{ValidThrough: math.MaxInt64, Log: incarnation}})
	if !resp.Success {
		t.Fatalf("opening %s's stream to %s by hand: %s", source, receiver.self.ID, resp.Error)
	}
	return &ReplStream{Incarnation: incarnation}
}

// heldStreamConn holds a stream's handshake — a push that carries no entries —
// until openRelease is closed, and every push that carries entries until
// entryRelease is closed. A nil channel holds nothing. Everything else passes.
type heldStreamConn struct {
	peerConn
	openRelease  <-chan struct{}
	entryRelease <-chan struct{}
}

func (h *heldStreamConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	if op == transport.OpReplPush {
		var push ReplPushRequest
		if err := bson.Unmarshal(payload, &push); err != nil {
			return nil, fmt.Errorf("decoding a held push: %w", err)
		}
		gate := h.entryRelease
		if len(push.Entries) == 0 {
			gate = h.openRelease
		}
		if gate != nil {
			select {
			case <-gate:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return h.peerConn.SendRequest(ctx, lane, op, payload)
}

// holdStreamEntries holds p's pushes that carry entries until the returned
// function is called, and lets its handshake through; the function is safe to
// call more than once.
func holdStreamEntries(p *peer) (release func()) {
	gate := make(chan struct{})
	wrapPeerConn(p, func(inner peerConn) peerConn { return &heldStreamConn{peerConn: inner, entryRelease: gate} })
	var once sync.Once
	return func() { once.Do(func() { close(gate) }) }
}

// currentIncarnation is the incarnation node index's log is in: the one every
// push of its stream names once the stream has opened.
func currentIncarnation(tc *testCluster, index int) []byte {
	id := tc.nodes[index].server.core.wal.Incarnation()
	return id[:]
}

// heldPull is a pull of the test bucket onto target, held between recording
// its snapshot position and materialising the snapshot.
type heldPull struct {
	release func()
	done    chan error
}

// wait returns the pull's result and whether it ended within limit. The pull's
// own 60 s context cannot cut it short once it is inside Badger: a read
// transaction opened on a store that has been closed waits on a watermark no
// goroutine serves any more, so an unbounded wait on done is a wait for the
// shard's 30-minute timeout.
func (pull *heldPull) wait(limit time.Duration) (ended bool, err error) {
	select {
	case err := <-pull.done:
		return true, err
	case <-time.After(limit):
		return false, nil
	}
}

func holdAPull(t *testing.T, tc *testCluster, targetIndex int, ownerID string) *heldPull {
	t.Helper()
	held := make(chan struct{})
	release := make(chan struct{})
	var once atomic.Bool
	hook := func(database, bucket string) {
		if once.CompareAndSwap(false, true) {
			close(held)
			<-release
		}
	}
	beforeMaterializeHook.Store(&hook)
	t.Cleanup(func() { beforeMaterializeHook.Store(nil) })
	target := tc.nodes[targetIndex].server.core
	pull := &heldPull{done: make(chan error, 1)}
	var released atomic.Bool
	pull.release = func() {
		if released.CompareAndSwap(false, true) {
			close(release)
		}
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		pull.done <- target.cluster.pullBucket(ctx, testDatabase, testBucket, mustNode(t, target.cluster, ownerID))
	}()
	select {
	case <-held:
	case err := <-pull.done:
		t.Fatalf("the pull ended before it reached its materialise: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("the pull never reached the point between recording its position and materialising")
	}
	return pull
}

// TestReplPushIsNotAcknowledgedBeforeTheSnapshotIsMaterialised covers
// T-84, R-4: a pull records its snapshot position before it materialises
// the copy, so a replay cannot run history over it. On the starting
// tree a push at or below that position was skipped and acknowledged while
// the bucket's store was still closed: a write counted on a copy that did not
// exist, and that never would if the pull then failed. The skip under a
// snapshot position now needs the store open; until then the push is refused,
// naming the bucket, and nothing at or past it is acknowledged.
func TestReplPushIsNotAcknowledgedBeforeTheSnapshotIsMaterialised(t *testing.T) {
	setup := func(t *testing.T) (*testCluster, int, int, string) {
		tc := newTestCluster(t, 3, 1)
		t.Cleanup(tc.stop)
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
		for _, key := range []string{"k1", "k2", "k3"} {
			tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: key, Data: map[string]interface{}{"n": 1}})
		}
		ownerIndex := ownerIndexOf(t, tc, testBucket)
		return tc, ownerIndex, (ownerIndex + 1) % 3, tc.addrs[ownerIndex]
	}
	refusedAtTheFloor := func(t *testing.T, tc *testCluster, ownerIndex, targetIndex int, ownerID, when string) uint64 {
		t.Helper()
		target := tc.nodes[targetIndex].server.core
		floor := target.cluster.seededThroughFor(ownerID, testDatabase, testBucket)
		if floor == 0 {
			t.Fatalf("setup: no snapshot position of %s/%s from the owner on node %d %s", testDatabase, testBucket, targetIndex+1, when)
		}
		if target.bucketStoreOpen(testDatabase, testBucket) {
			t.Fatalf("setup: node %d's store for %s/%s is open %s", targetIndex+1, testDatabase, testBucket, when)
		}
		entry := ReplEntry{Seq: floor, Op: int32(transport.OpPut), Bucket: testBucket, Payload: putPayload(t, testDatabase, testBucket, "at-the-floor")}
		resp := streamPush(t, target, ownerID, currentIncarnation(tc, ownerIndex), []ReplEntry{entry})
		if resp.Success || resp.AppliedThrough >= floor {
			t.Fatalf("%s, a push at the snapshot position %d was answered success=%t applied_through=%d while the bucket's store is closed: "+
				"a write counted on a copy that does not exist", when, floor, resp.Success, resp.AppliedThrough)
		}
		if resp.Refusal != "awaiting_copy" || !strings.Contains(resp.Error, testBucket) {
			t.Fatalf("%s, the push was refused with refusal %q and error %q; want awaiting_copy, naming bucket %q", when, resp.Refusal, resp.Error, testBucket)
		}
		return floor
	}

	t.Run("while the pull is held", func(t *testing.T) {
		tc, ownerIndex, targetIndex, ownerID := setup(t)
		pull := holdAPull(t, tc, targetIndex, ownerID)
		floor := refusedAtTheFloor(t, tc, ownerIndex, targetIndex, ownerID, "while the pull is held")
		pull.release()
		ended, err := pull.wait(30 * time.Second)
		if !ended {
			t.Fatal("the released pull did not finish within 30s")
		}
		if err != nil {
			t.Fatalf("the pull, released: %v", err)
		}
		entry := ReplEntry{Seq: floor, Op: int32(transport.OpPut), Bucket: testBucket, Payload: putPayload(t, testDatabase, testBucket, "at-the-floor")}
		resp := streamPush(t, tc.nodes[targetIndex].server.core, ownerID, currentIncarnation(tc, ownerIndex), []ReplEntry{entry})
		if !resp.Success || resp.AppliedThrough != floor {
			t.Fatalf("after the pull landed, the push at the snapshot position %d was answered success=%t applied_through=%d (%s %q); want it skipped and acknowledged",
				floor, resp.Success, resp.AppliedThrough, resp.Refusal, resp.Error)
		}
	})

	t.Run("after a restart while the pull was held", func(t *testing.T) {
		tc, ownerIndex, targetIndex, ownerID := setup(t)
		pull := holdAPull(t, tc, targetIndex, ownerID)
		// The node dies between recording the position and materialising: it
		// comes back with the position in its store and no bucket.
		tc.restartNode(targetIndex)
		pull.release()
		// Only the restarted node's answer is asserted. The held pull belongs to
		// the node that was stopped: it resumes on that node's closed store, where
		// it can block for good (a CI shard hung 23 minutes here), so its end is
		// waited for briefly and not required.
		if ended, _ := pull.wait(10 * time.Second); !ended {
			t.Logf("the stopped node's released pull is still blocked on its closed store after 10s; not waiting for it")
		}
		refusedAtTheFloor(t, tc, ownerIndex, targetIndex, ownerID, "after a restart with the position recorded and no copy")
	})
}

// TestAPushUnderAnotherIncarnationIsRefusedNotSkipped is T-88: a position is a
// fact about one life of a log, so a push that names an incarnation the
// receiver has not recorded for its sender is refused — applying nothing and
// acknowledging nothing — and the sender opens the stream again. On the
// starting tree an entry at or below the remembered position was skipped and
// acknowledged whatever log it came from, and one above it
// was applied under a position learned from another log.
func TestAPushUnderAnotherIncarnationIsRefusedNotSkipped(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	ownerIndex := ownerIndexOf(t, tc, testBucket)
	for i := 0; i < 10; i++ {
		if frame := tc.writeConcerned(ownerIndex, "settled"+string(rune('a'+i)), "all"); frame.Opcode == transport.OpError {
			t.Fatalf("write %d at all: %s", i, frame.Payload)
		}
	}
	replicaIndex := (ownerIndex + 1) % 3
	replica := tc.nodes[replicaIndex].server.core
	ownerID := tc.addrs[ownerIndex]
	position := appliedSnapshot(replica.cluster)[ownerID]
	if position < 10 {
		t.Fatalf("setup: the replica's position for the owner is %d after ten writes at all", position)
	}

	another := bytes.Repeat([]byte{0xab}, 16)
	for _, tc2 := range []struct {
		name string
		seq  uint64
		key  string
	}{
		{"at the remembered position", position, "under-another-at"},
		{"above the remembered position", position + 1000, "under-another-above"},
	} {
		entry := ReplEntry{Seq: tc2.seq, Op: int32(transport.OpPut), Bucket: testBucket, Payload: putPayload(t, testDatabase, testBucket, tc2.key)}
		resp := streamPush(t, replica, ownerID, another, []ReplEntry{entry})
		if resp.Success || resp.AppliedThrough != 0 || resp.Refusal != "reopen" {
			t.Fatalf("%s: a push under an incarnation the replica never recorded was answered success=%t applied_through=%d refusal=%q; "+
				"want refused with reopen and nothing acknowledged", tc2.name, resp.Success, resp.AppliedThrough, resp.Refusal)
		}
		if holdsKey(t, replica, testDatabase, testBucket, tc2.key) {
			t.Fatalf("%s: the replica applied an entry of a log it holds no position in", tc2.name)
		}
	}
	if got := appliedSnapshot(replica.cluster)[ownerID]; got != position {
		t.Fatalf("the refused pushes moved the replica's position for the owner from %d to %d", position, got)
	}
}

// TestAPullIsServedOnlyOverAnOpenStream is A-4.2. A snapshot position is a
// position in the source's log, so it must be recorded under the incarnation
// the puller holds for that source. A source serves a pull only to a peer its
// stream has completed a handshake with in this process; otherwise a position
// taken after a restart would be recorded under the incarnation from before
// it, the next rebase would lower it to that incarnation's limit, and the
// entries under it would be applied over the snapshot — a FIFO element twice.
func TestAPullIsServedOnlyOverAnOpenStream(t *testing.T) {
	never := make(chan struct{})
	opened := make(chan struct{})
	entriesReleased := make(chan struct{})
	var holdOwnerStream atomic.Bool
	var joinerID atomic.Value
	holdStreamTo := func(c *Cluster, id string, openRelease <-chan struct{}, entryRelease <-chan struct{}) {
		node, ok := c.memberByID(id)
		if !ok {
			t.Errorf("%s does not list %s as a member", c.self.ID, id)
			return
		}
		wrapPeerConn(c.peerFor(node), func(inner peerConn) peerConn {
			return &heldStreamConn{peerConn: inner, openRelease: openRelease, entryRelease: entryRelease}
		})
	}
	tc := newTestClusterTuned(t, 3, 3, func(index int, cfg *config.Config) {
		cfg.Server.AutoRebalance = false
	}, func(index int, c *Cluster) {
		if id, _ := joinerID.Load().(string); holdOwnerStream.Load() {
			// The restarted owner's handshake to the joiner waits for opened,
			// and its entries for entriesReleased.
			holdStreamTo(c, id, opened, entriesReleased)
		}
	})
	defer tc.stop()
	defer func() {
		for _, gate := range []chan struct{}{opened, entriesReleased} {
			select {
			case <-gate:
			default:
				close(gate)
			}
		}
	}()
	ports := reserveNodePorts(t)
	sc := findSeededReplicaScenario(t, tc, ports.ringNode(), "open")
	ownerID := tc.addrs[sc.ownerIndex]
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(sc.ownerIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: sc.bucket, Type: BucketTypeFIFO})
	push := func(element string, concern string) {
		t.Helper()
		frame := tc.frame(sc.ownerIndex, transport.OpFIFOPush, PushRequest{
			Database: testDatabase, Bucket: sc.bucket, Data: map[string]interface{}{"e": element}, WriteConcern: concern,
		})
		if frame.Opcode == transport.OpError {
			t.Fatalf("push %s: %s", element, frame.Payload)
		}
	}
	push("before-the-join", "all")

	// Before the owner restarts, its stream to the joiner opens and delivers
	// nothing: the joiner holds a record of the owner from that process, and
	// no copy of the queue except the one the test pulls.
	joiner := joinLiveReplica(t, tc, ports, func(*Service) {
		holdStreamTo(tc.nodes[sc.ownerIndex].server.core.cluster, ports.ringNode().ID, nil, never)
	})
	joinerID.Store(joiner.addr)
	jc := joiner.server.core.cluster
	pollUntil(t, "the owner's stream to the joiner answered its handshake", func() bool {
		oc := tc.nodes[sc.ownerIndex].server.core.cluster
		return oc.peerFor(mustNode(t, oc, joiner.addr)).logReads.Load() > 0
	})

	holdOwnerStream.Store(true)
	tc.restartNode(sc.ownerIndex)
	holdOwnerStream.Store(false)
	push("after-the-restart", "one")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	refused := jc.pullBucket(ctx, testDatabase, sc.bucket, mustNode(t, jc, ownerID))
	if refused == nil || !strings.Contains(refused.Error(), "has not opened") {
		t.Fatalf("the restarted owner served a pull to a node its stream has not opened to in this process (pull: %v)", refused)
	}
	if joiner.server.core.bucketStoreOpen(testDatabase, sc.bucket) {
		t.Fatal("the refused pull left a copy on the joiner")
	}

	close(opened)
	if err := jc.pullBucket(ctx, testDatabase, sc.bucket, mustNode(t, jc, ownerID)); err != nil {
		t.Fatalf("the pull once the owner's stream to the joiner is open: %v", err)
	}
	close(entriesReleased)
	oc := tc.nodes[sc.ownerIndex].server.core.cluster
	if err := catchUpPeer(ctx, oc.peerFor(mustNode(t, oc, joiner.addr))); err != nil {
		t.Fatalf("the owner's stream to the joiner after the pull: %v", err)
	}
	owner := tc.nodes[sc.ownerIndex].server.core
	want, err := owner.Count(CountRequest{Database: testDatabase, Bucket: sc.bucket})
	if err != nil {
		t.Fatalf("count on the owner: %v", err)
	}
	got, err := joiner.server.core.Count(CountRequest{Database: testDatabase, Bucket: sc.bucket})
	if err != nil {
		t.Fatalf("count on the joiner: %v", err)
	}
	if got.Count != want.Count {
		t.Fatalf("the joiner holds %d elements of the queue and the owner %d: the write logged between the restart and the pull "+
			"was applied over the snapshot that already held it", got.Count, want.Count)
	}
}

// TestAHeldPullWaitIsBounded pins the harness half of the shard hang: a pull
// that never answers makes wait return, not block.
func TestAHeldPullWaitIsBounded(t *testing.T) {
	stuck := &heldPull{done: make(chan error, 1)}
	started := time.Now()
	ended, _ := stuck.wait(50 * time.Millisecond)
	if ended {
		t.Fatal("wait reported a pull that never answered as ended")
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("wait on a pull that never answers took %s; want about its 50ms limit", elapsed)
	}
	stuck.done <- nil
	if ended, err := stuck.wait(time.Second); !ended || err != nil {
		t.Fatalf("wait on an answered pull returned ended=%t err=%v; want ended without error", ended, err)
	}
}
