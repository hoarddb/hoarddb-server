package server

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// T-106 (spec §19.9, §19.4 P3): copies_confirmed N means at least N nodes hold
// the write. The test runs the conditions under which a count could run ahead
// of the copies — a replica stopped through more writes than its queue, the
// replica returning under load, an owner restart with a backlog, the owner
// removed — with eight writers and forced retention passes throughout, and
// stops after each to count the copies in every node's own store.

// queueElementIDs is the non-destructive local read T-106 needs: how many
// times each element id appears in a FIFO bucket of node's own store.
func queueElementIDs(node *testNode, bucket string) (map[string]int, error) {
	held, snap, err := node.server.core.resolveForRead(testDatabase, bucket)
	if err != nil {
		return nil, fmt.Errorf("resolving %s on %s: %w", bucket, node.addr, err)
	}
	defer snap.Discard()
	ids := make(map[string]int)
	walkErr := held.queueStore.Walk(func(entry []byte) error {
		id, ok := bson.Raw(entry).Lookup("_id_").StringValueOK()
		if !ok {
			return fmt.Errorf("an element of %s on %s has no _id_", bucket, node.addr)
		}
		ids[id]++
		return nil
	})
	if walkErr != nil {
		return nil, fmt.Errorf("walking %s on %s: %w", bucket, node.addr, walkErr)
	}
	return ids, nil
}

// shipGate holds every sender before its next ship while closed, and counts
// the ships under way, through beforeShipHook.
type shipGate struct {
	mu       sync.Mutex
	changed  *sync.Cond
	closed   bool
	underWay int
}

func newShipGate() *shipGate {
	gate := &shipGate{}
	gate.changed = sync.NewCond(&gate.mu)
	return gate
}

func (g *shipGate) before(string) func() {
	g.mu.Lock()
	for g.closed {
		g.changed.Wait()
	}
	g.underWay++
	g.mu.Unlock()
	return func() {
		g.mu.Lock()
		g.underWay--
		g.changed.Broadcast()
		g.mu.Unlock()
	}
}

// close holds every sender before its next ship and returns once the ships
// already under way have returned.
func (g *shipGate) close() {
	g.mu.Lock()
	g.closed = true
	for g.underWay > 0 {
		g.changed.Wait()
	}
	g.mu.Unlock()
}

func (g *shipGate) open() {
	g.mu.Lock()
	g.closed = false
	g.changed.Broadcast()
	g.mu.Unlock()
}

// acknowledgedWrite is a write a client was told succeeded, and how many
// copies it was told hold it.
type acknowledgedWrite struct {
	bucket, id string
	queue      bool
	confirmed  int32
}

// proofRun is T-106's cluster, writers and bookkeeping.
type proofRun struct {
	t                *testing.T
	tc               *testCluster
	queue, keyed     string
	nodesMu          sync.Mutex // guards tc.nodes against the retention loop and the checkpoint reads
	pause            sync.RWMutex
	ackMu            sync.Mutex
	acks             []acknowledgedWrite
	checked          int
	threes           atomic.Int64
	writes           atomic.Int64
	gate             *shipGate
	stop             chan struct{}
	done             sync.WaitGroup
	writeFailuresLog atomic.Int64
}

// writer writes through node coordinator until the run stops: every write its
// own id, alternating the two buckets, at the writer's concern.
func (r *proofRun) writer(w, coordinator int) {
	defer r.done.Done()
	client := r.tc.dial(coordinator)
	defer func() { _ = client.Close() }()
	concern := []string{"one", "majority", "all"}[w%3]
	for i := 0; ; i++ {
		select {
		case <-r.stop:
			return
		default:
		}
		id := fmt.Sprintf("w%d-%06d", w, i)
		queue := i%2 == 0
		var op transport.Opcode
		var request any
		bucket := r.keyed
		if queue {
			bucket = r.queue
			op, request = transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: r.queue,
				Data: map[string]interface{}{"_id_": id}, WriteConcern: concern}
		} else {
			op, request = transport.OpPut, InsertRequest{Database: testDatabase, Bucket: r.keyed, Key: id,
				Data: map[string]interface{}{"n": i}, WriteConcern: concern}
		}
		payload, err := bson.Marshal(request)
		if err != nil {
			r.t.Errorf("encoding %s: %v", id, err)
			return
		}
		r.pause.RLock()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		frame, err := client.SendRequest(ctx, op, payload)
		cancel()
		if err == nil && frame.Opcode != transport.OpError {
			confirmed, _, _, _ := copiesConfirmedOf(frame.Payload)
			r.ackMu.Lock()
			r.acks = append(r.acks, acknowledgedWrite{bucket: bucket, id: id, queue: queue, confirmed: confirmed})
			r.ackMu.Unlock()
			if confirmed == 3 {
				r.threes.Add(1)
			}
		} else {
			r.writeFailuresLog.Add(1)
		}
		r.writes.Add(1)
		r.pause.RUnlock()
	}
}

// retentionPasses runs a retention pass on every running node every 50 ms.
func (r *proofRun) retentionPasses() {
	defer r.done.Done()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-ticker.C:
		}
		r.nodesMu.Lock()
		for _, node := range r.tc.nodes {
			if node.server != nil {
				_ = node.server.core.cluster.pruneWAL()
			}
		}
		r.nodesMu.Unlock()
	}
}

// checkpoint pauses the writers, holds every sender before its next ship and
// waits for the ships under way, then reads every running node's own store:
// each write acknowledged since the last checkpoint is held by at least as
// many nodes as it was told, and no node holds a queue element twice. Then,
// with the senders released and before the writers resume, transition runs.
func (r *proofRun) checkpoint(phase string, transition func()) {
	r.t.Helper()
	r.pause.Lock()
	defer r.pause.Unlock()
	r.gate.close()
	r.nodesMu.Lock()
	r.verify(phase)
	r.nodesMu.Unlock()
	r.gate.open()
	if transition != nil {
		r.nodesMu.Lock()
		transition()
		r.nodesMu.Unlock()
	}
}

func (r *proofRun) verify(phase string) {
	r.t.Helper()
	queues := make([]map[string]int, 0, len(r.tc.nodes))
	var running []*testNode
	for _, node := range r.tc.nodes {
		if node.server == nil {
			continue
		}
		ids, err := queueElementIDs(node, r.queue)
		if err != nil {
			r.t.Fatalf("%s: %v", phase, err)
		}
		queues = append(queues, ids)
		running = append(running, node)
		for id, n := range ids {
			if n > 1 {
				r.t.Errorf("%s: %s holds queue element %s %d times", phase, node.addr, id, n)
			}
		}
	}
	r.ackMu.Lock()
	acks := r.acks[r.checked:]
	r.checked = len(r.acks)
	r.ackMu.Unlock()
	short := 0
	for _, ack := range acks {
		if ack.confirmed < 1 {
			r.t.Fatalf("%s: %s in %s was acknowledged without copies_confirmed", phase, ack.id, ack.bucket)
		}
		holders := 0
		for i, node := range running {
			if ack.queue && queues[i][ack.id] > 0 || !ack.queue && holdsKeyLocallyIn(node, ack.bucket, ack.id) {
				holders++
			}
		}
		if int32(holders) < ack.confirmed {
			if short < 10 {
				r.t.Errorf("%s: %s in %s was answered copies_confirmed %d and %d running nodes hold it", phase, ack.id, ack.bucket, ack.confirmed, holders)
			}
			short++
		}
	}
	counts := make(map[int32]int)
	for _, ack := range acks {
		counts[ack.confirmed]++
	}
	levels := make([]int, 0, len(counts))
	for level := range counts {
		levels = append(levels, int(level))
	}
	sort.Ints(levels)
	summary := ""
	for _, level := range levels {
		summary += fmt.Sprintf(" %d×%d", counts[int32(level)], level)
	}
	r.t.Logf("%s: %d writes acknowledged since the last checkpoint (copies_confirmed:%s), %d short; %d running nodes",
		phase, len(acks), summary, short, len(running))
	if short > 0 {
		r.t.Errorf("%s: %d acknowledged writes are held by fewer nodes than their copies_confirmed", phase, short)
	}
}

// waitWrites waits until n more writes have been answered.
func (r *proofRun) waitWrites(n int64, within time.Duration) {
	r.t.Helper()
	target := r.writes.Load() + n
	deadline := time.Now().Add(within)
	for r.writes.Load() < target {
		if time.Now().After(deadline) {
			r.t.Fatalf("only %d of %d writes were answered in %v", r.writes.Load()-(target-n), n, within)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// proofSegments rotates a node's log every few dozen entries, so a retention
// pass has whole segments to remove without a disk flush per entry. The shared
// smallSegments uses 256 bytes, which rotates on nearly every entry: on a real
// disk that rotation's fsync, not the replication rule, set this test's pace
// and the returned replica could not reach copies_confirmed 3 within its bound
// (BUG-0567). Eight kilobytes still seals a segment during every phase, so a
// retention pass has something to remove, while one flush covers a run of
// writes. A restart opens the log at its default size again, so the test calls
// this again after one.
func proofSegments(node *testNode) {
	node.server.core.wal.SetMaxSegmentBytes(8 << 10)
}

// TestCopiesConfirmedNeverExceedsTheCopiesThatHoldTheWrite is T-106, the proof
// of P3 as a test (review F-12). Three nodes at factor 3; a FIFO bucket and a
// hash bucket with one owner; eight writers at one, majority and all, writing
// through the replica that is never stopped; a retention pass on every node
// every 50 ms over 8 KiB log segments. A checkpoint follows each phase:
// steady; a replica stopped through more writes than its queue holds; the
// replica returned with the writers still running, and a response of 3 within
// thirty seconds; an owner restart with a backlog; the owner removed. Guards,
// each broken by hand: count from the in-sync set instead of from cursors;
// require an idle instant to re-enter streaming; ship by present ownership
// instead of by origin; acknowledge under an unmaterialised snapshot; advance
// to the log's tail instead of the ticket floor.
//
// What it measures: for every write a client was told succeeded, how many
// running nodes held it at the next checkpoint, against the copies_confirmed
// it was told. It assumes a log flush costs a real disk something: the segment
// size is chosen so rotation and retention stay exercised (a segment seals in
// every phase) while their flush cost does not dominate the 30 s bound for a
// returned replica (4.5 s measured on a real disk with these segments, against
// a red that never reached the bound in 64–107 s with the 256-byte ones). On a
// disk whose flush cost pushes that bound,
// the test fails on the bound rather than passing: it does not skip or lower
// what it asserts about the rule.
func TestCopiesConfirmedNeverExceedsTheCopiesThatHoldTheWrite(t *testing.T) {
	tc := newTestClusterWith(t, 3, 3, withoutRetentionLoop)
	defer tc.stop()
	ownerIndex := 0
	queue := bucketOwnedBy(t, tc, ownerIndex, "proofq")
	keyed := bucketOwnedBy(t, tc, ownerIndex, "proofh")
	stoppedIndex, coordinatorIndex := 1, 2
	tc.ok(coordinatorIndex, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(coordinatorIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: queue, Type: BucketTypeFIFO})
	tc.ok(coordinatorIndex, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: keyed, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, queue, 3)
	waitForServing(t, tc, testDatabase, keyed, 3)
	for _, node := range tc.nodes {
		proofSegments(node)
	}

	r := &proofRun{t: t, tc: tc, queue: queue, keyed: keyed, gate: newShipGate(), stop: make(chan struct{})}
	hook := r.gate.before
	beforeShipHook.Store(&hook)
	defer beforeShipHook.Store(nil)
	const writers = 8
	for w := 0; w < writers; w++ {
		r.done.Add(1)
		go r.writer(w, coordinatorIndex)
	}
	r.done.Add(1)
	go r.retentionPasses()
	stopped := false
	defer func() {
		if !stopped {
			close(r.stop)
			r.done.Wait()
		}
	}()

	// Steady.
	r.waitWrites(600, 60*time.Second)
	var startStopped func() *testNode
	r.checkpoint("steady", func() {
		startStopped = stopNode(t, tc, stoppedIndex)
	})

	// A replica stopped through more writes than its queue holds (1,024).
	r.waitWrites(1500, 120*time.Second)
	r.checkpoint("a replica stopped", nil)

	// The replica returns with the writers running. A write is answered 3 only
	// once the returned replica has caught up: until then every `all` write it
	// would hold is answered with the copies that do acknowledge it, because the
	// stream cannot reach the write's sequence before the concern times out.
	// Under load the stream drains the writes it missed more slowly than the
	// writers add them, so it can stay behind for as long as they run. Hold the
	// writers while it catches up — the pause a checkpoint takes — then let them
	// run: a response of 3 must follow. What is on trial is the replica's return,
	// not the wall clock.
	r.nodesMu.Lock()
	proofSegments(startStopped())
	r.nodesMu.Unlock()
	ownerCluster := tc.nodes[ownerIndex].server.core.cluster
	returnedPeer := ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[stoppedIndex]))
	func() {
		r.pause.Lock()
		defer r.pause.Unlock()
		// The writers are held, so the log's tail is the last write that must
		// reach the returned replica before it can hold a new one. Wait for the
		// replica's cursor to reach it: caught-up-at-the-tail, not merely a
		// moment between ships.
		tail := tc.nodes[ownerIndex].server.core.wal.LastSequence()
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := waitForStream(ctx, returnedPeer, func() bool {
			return returnedPeer.ackedThrough(ownerCluster.self.ID) >= tail
		}); err != nil {
			t.Fatalf("the returned replica did not catch up to the owner's log %d: %v", tail, err)
		}
	}()
	threes := r.threes.Load()
	returnedAt := time.Now()
	deadline := returnedAt.Add(30 * time.Second)
	for r.threes.Load() == threes {
		if time.Now().After(deadline) {
			t.Fatalf("no write was answered copies_confirmed 3 within 30 s of the stopped replica's catch-up, with the writers running")
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("the returned replica reached copies_confirmed 3 %v after it returned", time.Since(returnedAt))
	r.waitWrites(400, 60*time.Second)
	r.checkpoint("the replica returned", func() {
		// The owner's log gets a backlog: its stream to the returned replica is
		// frozen through some writes, and the owner restarts with them unshipped.
		ownerCluster := tc.nodes[ownerIndex].server.core.cluster
		freezePeer(ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[stoppedIndex])))
	})
	r.waitWrites(300, 60*time.Second)
	r.nodesMu.Lock()
	proofSegments(tc.restartNode(ownerIndex))
	r.nodesMu.Unlock()
	r.waitWrites(400, 120*time.Second)
	r.checkpoint("an owner restart with a backlog", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := tc.nodes[coordinatorIndex].server.core.cluster.proposeRemoval(ctx, tc.addrs[ownerIndex]); err != nil {
			t.Fatalf("removing the owner %s: %v", tc.addrs[ownerIndex], err)
		}
	})

	// The owner removed: the writers carry on through the coordinator.
	r.waitWrites(600, 120*time.Second)
	r.checkpoint("the owner removed", nil)

	stopped = true
	close(r.stop)
	r.done.Wait()
	t.Logf("%d writes, %d acknowledged, %d refused or failed", r.writes.Load(), len(r.acks), r.writeFailuresLog.Load())
}
