package server

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Spec §20.8.6, as the owner ruled on 2026-10-09: a copy behind its owner's
// base waits for the old owner's stream while that stream keeps bringing it
// forward. It is copied afresh after one write-concern timeout with no
// progress, or at ten write-concern timeouts from its first wait, whichever
// comes first.

// pushPace delays every replication push on the connections it wraps, and
// after deliver pushes (0: no limit) holds every later one until its context
// ends: a stream that drains slowly, and one that stops. Its state is shared
// by every connection the peer dials while it is in place.
type pushPace struct {
	delay   time.Duration
	deliver int64
	// held reads how many entries the replica behind the stream holds (-1:
	// none). When set, the pace records when a push raised it: the event the
	// follower's wait clock starts from (spec §20.8.6), which a frame that
	// carries none of the bucket's entries does not move.
	held func() int64

	delivered atomic.Int64
	mu        sync.Mutex
	first     time.Time
	last      time.Time
	advanced  time.Time
}

func (p *pushPace) wrap(inner peerConn) peerConn { return &pacedPushConn{peerConn: inner, pace: p} }

// span is the time from the first push delivered to the last.
func (p *pushPace) span() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.last.Sub(p.first)
}

// lastAdvanced is when a push last raised the replica's count; zero if none
// did, or if the pace has no held reader.
func (p *pushPace) lastAdvanced() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.advanced
}

// reserve claims one of the deliver slots, or reports that none is left. The
// claim is atomic: two pushes in flight at once cannot both take the last slot.
func (p *pushPace) reserve() bool {
	if p.delivered.Add(1) > p.deliver && p.deliver > 0 {
		p.delivered.Add(-1)
		return false
	}
	return true
}

type pacedPushConn struct {
	peerConn
	pace *pushPace
}

func (c *pacedPushConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	if op != transport.OpReplPush {
		return c.peerConn.SendRequest(ctx, lane, op, payload)
	}
	if !c.pace.reserve() {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	select {
	case <-time.After(c.pace.delay):
	case <-ctx.Done():
		c.pace.delivered.Add(-1)
		return nil, ctx.Err()
	}
	var before int64
	if c.pace.held != nil {
		before = c.pace.held()
	}
	frame, err := c.peerConn.SendRequest(ctx, lane, op, payload)
	if err != nil {
		c.pace.delivered.Add(-1)
		return frame, err
	}
	now := time.Now()
	c.pace.mu.Lock()
	if c.pace.first.IsZero() {
		c.pace.first = now
	}
	c.pace.last = now
	if c.pace.held != nil && c.pace.held() > before {
		c.pace.advanced = now
	}
	c.pace.mu.Unlock()
	return frame, nil
}

// cededBacklog is a FIFO bucket a join moved from oldOwner to the joiner,
// with the replica staying in its set left behind by backlog entries of the
// old owner's term, whose stream to it now runs at a test's pace.
type cededBacklog struct {
	fifo              string
	oldOwner, staying int
	joiner            *testNode
	copiesBefore      []int64
	// adopted is when the joiner adopted the join, just before the stream to
	// the replica was released.
	adopted time.Time
}

// cedeBehindABacklog builds a cededBacklog: the old owner's stream to the
// replica is frozen while backlog pushes are written at concern one, a fourth
// node joins and becomes the bucket's owner, and the stream is released
// through pace.
func cedeBehindABacklog(t *testing.T, tc *testCluster, backlog int, pace *pushPace) *cededBacklog {
	t.Helper()
	ports := reserveNodePorts(t)
	fifo, _, oldOwner, staying := bucketsCededTogether(t, tc, ports.ringNode().ID)
	tc.ok(oldOwner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(oldOwner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: fifo, Type: BucketTypeFIFO})
	waitForServing(t, tc, testDatabase, fifo, 3)
	owner := clusterOf(tc, oldOwner)
	stream := owner.peerFor(mustNode(t, owner, tc.addrs[staying]))
	unfreeze := freezePeer(stream)
	t.Cleanup(unfreeze)
	pushBacklog(t, tc, oldOwner, fifo, backlog)
	copiesBefore := make([]int64, len(tc.nodes))
	for index := range tc.nodes {
		copiesBefore[index] = clusterOf(tc, index).takeOverCopies.Load()
	}
	joinCtx, cancelJoin := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelJoin()
	if err := owner.proposeJoin(joinCtx, ports.ringNode()); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	joiner := startJoinTarget(t, tc, ports, tc.token)
	t.Cleanup(func() { _ = joiner.client.Close() })
	pollUntil(t, "the joiner adopts the join", func() bool { return joiner.server.core.cluster.Epoch() >= owner.Epoch() })
	adopted := time.Now()
	replica := tc.nodes[staying].server.core
	pace.held = func() int64 {
		if !replica.bucketStoreOpen(testDatabase, fifo) {
			return -1
		}
		counted, err := replica.Count(CountRequest{Database: testDatabase, Bucket: fifo})
		if err != nil {
			return -1
		}
		return counted.Count
	}
	wrapPeerConn(stream, pace.wrap)
	unfreeze()
	return &cededBacklog{fifo: fifo, oldOwner: oldOwner, staying: staying, joiner: joiner, copiesBefore: copiesBefore, adopted: adopted}
}

// pushBacklog writes count pushes to bucket through node index at concern
// one, from eight clients.
func pushBacklog(t *testing.T, tc *testCluster, index int, bucket string, count int) {
	t.Helper()
	const clients = 8
	var failed atomic.Int64
	var wg sync.WaitGroup
	for writer := 0; writer < clients; writer++ {
		client := tc.dial(index)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { _ = client.Close() }()
			for n := writer; n < count; n += clients {
				request := PushRequest{Database: testDatabase, Bucket: bucket, Data: map[string]interface{}{"n": n}, WriteConcern: "one"}
				if !sendAndReadSuccess(client, transport.OpFIFOPush, request) {
					failed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if failed.Load() != 0 {
		t.Fatalf("setup: %d of %d pushes to %s at concern one were not answered a success", failed.Load(), count, bucket)
	}
}

// copiesSince is how many whole copies node index made since the backlog was
// written.
func (b *cededBacklog) copiesSince(tc *testCluster, index int) int64 {
	return clusterOf(tc, index).takeOverCopies.Load() - b.copiesBefore[index]
}

// TestAReplicaWaitsForAStreamThatKeepsDelivering: the old owner's stream
// brings the replica it left behind through a backlog of 640 entries, ten
// pushes 800 ms apart — longer than the write-concern timeout, and moving
// all the while. The replica waits it out and follows the new owner without
// being copied whole, and so does every other node. Guard: count the wait
// from its first round — the replica is copied afresh mid-drain.
func TestAReplicaWaitsForAStreamThatKeepsDelivering(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	const backlog = 640
	pace := &pushPace{delay: 800 * time.Millisecond}
	b := cedeBehindABacklog(t, tc, backlog, pace)
	replica := tc.nodes[b.staying].server.core
	if !holdsWithin(60*time.Second, func() bool {
		return followsTerm(replica, b.joiner.server.core, b.fifo) && localCountOf(t, replica, b.fifo) == backlog
	}) {
		t.Fatalf("a minute after the release, node %d does not follow the new owner's term with all %d elements of %s (it holds %d)",
			b.staying+1, backlog, b.fifo, localCountOf(t, replica, b.fifo))
	}
	copied := false
	for index := range tc.nodes {
		if copies := b.copiesSince(tc, index); copies != 0 {
			copied = true
			t.Errorf("node %d made %d whole copies while the old owner's stream was delivering (%d pushes over %v)",
				index+1, copies, pace.delivered.Load(), pace.span())
		}
	}
	if wc := clusterOf(tc, b.staying).wcTimeout; !copied && pace.span() <= wc {
		t.Fatalf("setup: the stream drained in %v, within the write-concern timeout %v; the test shows nothing", pace.span(), wc)
	}
}

// TestAReplicaIsCopiedAfreshOneTimeoutAfterItsStreamStops: the stream
// delivers eight of the ten pushes the backlog needs, 800 ms apart — past the
// write-concern timeout — and then delivers nothing. The replica is copied
// afresh from the new owner about one write-concern timeout after the last
// push, not before, and ends with every element. Guard: let the wait end only
// at the cap — the replica is still behind well past one timeout.
func TestAReplicaIsCopiedAfreshOneTimeoutAfterItsStreamStops(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	const backlog = 640
	pace := &pushPace{delay: 800 * time.Millisecond, deliver: 8}
	b := cedeBehindABacklog(t, tc, backlog, pace)
	wc := clusterOf(tc, b.staying).wcTimeout
	if !holdsWithin(60*time.Second, func() bool { return b.copiesSince(tc, b.staying) >= 1 }) {
		t.Fatalf("a minute after the release, node %d, whose stream stopped after %d pushes, was not copied afresh", b.staying+1, pace.delivered.Load())
	}
	handedOver := time.Now()
	if pace.delivered.Load() < pace.deliver {
		t.Fatalf("setup: node %d was copied afresh after %d of %d pushes; the stream had not stopped", b.staying+1, pace.delivered.Load(), pace.deliver)
	}
	// The follower's clock starts when its position last rose, not at the last
	// frame answered: some pushes carry none of this bucket's entries.
	stalled := pace.lastAdvanced()
	if stalled.IsZero() {
		t.Fatalf("setup: no push of the stream raised node %d's count of %s; there is no last advance to measure from", b.staying+1, b.fifo)
	}
	if waited := handedOver.Sub(stalled); waited < wc-250*time.Millisecond || waited > wc+5*time.Second {
		t.Errorf("node %d was copied afresh %v after its stream's last push; want about the write-concern timeout, %v", b.staying+1, waited, wc)
	}
	replica := tc.nodes[b.staying].server.core
	if !holdsWithin(30*time.Second, func() bool {
		return followsTerm(replica, b.joiner.server.core, b.fifo) && localCountOf(t, replica, b.fifo) == backlog
	}) {
		t.Errorf("node %d does not follow the new owner's term with all %d elements of %s after its copy afresh (it holds %d)",
			b.staying+1, backlog, b.fifo, localCountOf(t, replica, b.fifo))
	}
}

// TestAReplicaIsCopiedAfreshAtTheCapWhileItsStreamTrickles: with a
// two-second write-concern timeout the cap is twenty seconds. The stream
// delivers a 3,600-entry backlog in 57 pushes 600 ms apart, about 34 s:
// always moving, never done in time. The replica is copied afresh at about
// the cap from its first wait, while the stream is still delivering. Guard:
// drop the cap — the replica waits out the whole drain.
func TestAReplicaIsCopiedAfreshAtTheCapWhileItsStreamTrickles(t *testing.T) {
	const wc = 2 * time.Second
	// The owner's ruling: ten write-concern timeouts from the first wait.
	const waitCap = 10 * wc
	tc := newTestClusterTuned(t, 3, 3, nil, func(_ int, c *Cluster) { c.wcTimeout = wc })
	defer tc.stop()
	const backlog = 3600
	pushes := int64((backlog + maxReplBatch - 1) / maxReplBatch)
	pace := &pushPace{delay: 600 * time.Millisecond}
	b := cedeBehindABacklog(t, tc, backlog, pace)
	limit := waitCap + 8*time.Second
	if !holdsWithin(limit, func() bool { return b.copiesSince(tc, b.staying) >= 1 }) {
		t.Fatalf("%v after the release, node %d, whose stream still trickles (%d of %d pushes), was not copied afresh; the cap is %v",
			limit, b.staying+1, pace.delivered.Load(), pushes, waitCap)
	}
	waited := time.Since(b.adopted)
	if delivered := pace.delivered.Load(); delivered >= pushes {
		t.Fatalf("setup: the stream delivered all %d pushes before node %d was copied afresh; it was not trickling at the cap", delivered, b.staying+1)
	}
	if waited < waitCap-time.Second {
		t.Errorf("node %d was copied afresh %v after the join while its stream delivered a push every %v; want about the cap, %v",
			b.staying+1, waited, pace.delay, waitCap)
	}
	t.Logf("copied afresh %v after the join, %d of %d pushes delivered", waited, pace.delivered.Load(), pushes)
}
