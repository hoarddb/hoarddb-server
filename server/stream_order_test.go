package server

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The order and completeness of what an owner delivers to its replicas
// (replication stream build doc, dispatch 3). A frozen peer, a stopped peer and
// a delayed peer are built for real: a peerConn wrapper or a stopped node, never
// a state set by hand.

// ringOf builds the placement ring members would have, so a test can choose a
// bucket by where it will be placed after a membership change it has not made
// yet.
func ringOf(t *testing.T, members []ring.Node, rf int) *ring.Ring {
	t.Helper()
	placement := ring.New(rf)
	for _, member := range members {
		if err := placement.AddNode(member); err != nil {
			t.Fatalf("building a ring of %v: %v", members, err)
		}
	}
	return placement
}

// TestWriteConcernOneIsNotStalledByAFencedPeer is the second criterion
// for fenced peers (T-79). A joiner with auto-rebalance off is in a bucket's replica set and
// holds no copy, so it refuses every push of that bucket. The owner's queue for
// it fills; on the starting tree every write past the queue's depth then waited
// the whole write-concern timeout for room, at `one`, which needs no copy but
// the owner's.
//
// What it guards is peer.route's state check: the stream to a peer that
// refuses is never streaming, so a write routes nothing to it. A blocking send
// on a streaming queue does not reach it here — the stream is not streaming —
// and is guarded by TestAFullQueueNeverBlocksAWriter (T-77) and
// TestAStallEmptiesTheQueueAndTheLogDeliversEachEntryOnce.
func TestWriteConcernOneIsNotStalledByAFencedPeer(t *testing.T) {
	tc := newTestClusterWith(t, 3, 3, autoRebalanceOff)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})

	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	before := tc.nodes[0].server.core.cluster.Members()
	after := ringOf(t, append(append([]ring.Node{}, before...), newcomer), 3)

	// A bucket whose owner does not change with the join and whose replica set
	// gains the joiner.
	bucket, ownerID := "", ""
	for attempt := 0; attempt < 500 && bucket == ""; attempt++ {
		name := fmt.Sprintf("fenced%03d", attempt)
		key := placementOf(testDatabase, name)
		was, err := tc.nodes[0].server.core.cluster.Owner(key)
		if err != nil {
			t.Fatal(err)
		}
		set, err := after.Replicas(key.encoded, 3)
		if err != nil {
			t.Fatal(err)
		}
		if set[0].ID != was.ID {
			continue
		}
		for _, member := range set {
			if member.ID == newcomer.ID {
				bucket, ownerID = name, was.ID
			}
		}
	}
	if bucket == "" {
		t.Fatal("test setup: no bucket name keeps its owner and gains the joiner")
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, ownerID)
	createBucketAt(tc, ownerIndex, testDatabase, bucket, 0)
	waitForServing(t, tc, testDatabase, bucket, 3)
	// A replay that starts below the bucket's create would give the joiner an
	// empty copy of it, and a member that holds a copy is not fenced.
	pruneLogPast(t, tc, ownerIndex, bucket)

	// The owner proposes the join, so it has adopted the new ring when the
	// proposal returns.
	ownerCluster := tc.nodes[ownerIndex].server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := ownerCluster.proposeJoin(ctx, newcomer); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	joiner := startJoinTarget(t, tc, ports, tc.token, func(c *config.Config) { c.Server.AutoRebalance = false })
	if err := joiner.server.core.cluster.reconcileMembership(ownerID, ownerCluster.memberNodes(), ownerCluster.memberIDs(), ownerCluster.Epoch()); err != nil {
		t.Fatalf("joining node resync: %v", err)
	}
	if ids := mustReplicaIDs(t, ownerCluster, testDatabase, bucket); !containsID(ids, newcomer.ID) {
		t.Fatalf("test setup: the owner places %s on %v, which does not include the joiner %s", bucket, ids, newcomer.ID)
	}
	if joiner.server.core.bucketStoreOpen(testDatabase, bucket) {
		t.Fatal("test setup: the joiner holds the bucket; auto-rebalance pulled it")
	}

	const writes = 1100
	// The build doc says 100 ms. A write that waits for the queue waits the
	// write-concern timeout, 5 s; under the full suite with -race one write in
	// 1,100 has taken 115 ms with no queue involved, so the bound is set well
	// clear of load and well below the wait it detects.
	const bound = time.Second
	slowest := time.Duration(0)
	for i := 0; i < writes; i++ {
		started := time.Now()
		frame := tc.frame(ownerIndex, transport.OpPut, InsertRequest{
			Database: testDatabase, Bucket: bucket, Key: fmt.Sprintf("k%04d", i),
			Data: map[string]interface{}{"n": i}, WriteConcern: "one",
		})
		took := time.Since(started)
		if frame.Opcode == transport.OpError {
			t.Fatalf("write %d at one with a fenced replica was answered with an error: %s", i, frame.Payload)
		}
		if took > bound {
			t.Fatalf("write %d at one took %v with a fenced replica in the set; a write at one waits for no peer (the slowest before it took %v)", i, took, slowest)
		}
		slowest = max(slowest, took)
	}
	t.Logf("%d writes at one with a fenced replica; the slowest took %v", writes, slowest)
}

// pruneLogPast writes to bucket at `all`, with the owner's segments shrunk,
// until a retention pass has removed every entry the owner had logged before
// the call. It returns the first sequence left.
func pruneLogPast(t *testing.T, tc *testCluster, ownerIndex int, bucket string) uint64 {
	t.Helper()
	owner := tc.nodes[ownerIndex].server.core
	past := owner.wal.LastSequence()
	owner.wal.SetMaxSegmentBytes(1024)
	defer owner.wal.SetMaxSegmentBytes(64 << 20)
	deadline := time.Now().Add(20 * time.Second)
	for i := 0; ; i++ {
		putDocumentAt(tc, ownerIndex, testDatabase, bucket, fmt.Sprintf("prune%03d", i), "all")
		if err := owner.cluster.pruneWAL(); err != nil {
			t.Fatalf("pruning the owner's log: %v", err)
		}
		first, err := owner.wal.FirstSequence()
		if err != nil {
			t.Fatalf("the owner's first sequence: %v", err)
		}
		if first > past {
			return first
		}
		if time.Now().After(deadline) {
			t.Fatalf("test setup: the owner's log still starts at %d after 20 s of writes at all; it was to be pruned past %d", first, past)
		}
	}
}

func containsID(ids []string, id string) bool {
	for _, candidate := range ids {
		if candidate == id {
			return true
		}
	}
	return false
}

// streamStatus reports a stream's state and the reason it last left
// streaming.
func (p *peer) streamStatus() (streamState, string) {
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	return p.state, p.stallReason
}

// caughtUp reports whether the stream is streaming, has shipped the log up to
// the boundary it entered streaming at, and the peer has acknowledged
// everything queued for it since.
func (p *peer) caughtUp() bool {
	acked := p.ackedThrough(p.c.self.ID)
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	return p.state == streamStreaming && p.boundaryShipped && acked >= p.queuedThrough
}

// waitForStream wakes p's sender and waits until done holds of its stream, or
// ctx ends. A stalled sender is woken again at every look, as the old direct
// catch-up call tried the peer at once.
func waitForStream(ctx context.Context, p *peer, done func() bool) error {
	for {
		signal := p.c.streamChangedSignal()
		if done() {
			return nil
		}
		if state, _ := p.streamStatus(); state != streamStreaming {
			p.wakeSender()
		}
		select {
		case <-signal:
		case <-time.After(20 * time.Millisecond):
		case <-ctx.Done():
			state, reason := p.streamStatus()
			return fmt.Errorf("the stream to %s is %s (%q): %w", p.node.ID, state, reason, ctx.Err())
		}
	}
}

// catchUpPeer is what a test that called the old catch-up directly waits for
// now: the sender has replayed the log to p and p has acknowledged it.
func catchUpPeer(ctx context.Context, p *peer) error {
	return waitForStream(ctx, p, p.caughtUp)
}

// waitStalledOn waits until p's stream is stalled for a reason that says want.
func waitStalledOn(ctx context.Context, p *peer, want string) error {
	return waitForStream(ctx, p, func() bool {
		state, reason := p.streamStatus()
		return state == streamStalled && strings.Contains(reason, want)
	})
}

// horizonRefusal is what a stall below the log's horizon says.
const horizonRefusal = "needs this node's log from sequence"

// wrapPeerConn makes every connection p uses go through wrap, the live one and
// any it dials later.
func wrapPeerConn(p *peer, wrap func(peerConn) peerConn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.wrapConn = wrap
	if p.client != nil {
		p.client = wrap(p.client)
	}
}

// heldPushConn holds every replication push sent on it until release is
// closed or the push's context ends — a frozen peer. Everything else passes.
type heldPushConn struct {
	peerConn
	release <-chan struct{}
}

func (h *heldPushConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	if op == transport.OpReplPush {
		select {
		case <-h.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return h.peerConn.SendRequest(ctx, lane, op, payload)
}

// freezePeer freezes p's replication pushes until the returned function is
// called; the function is safe to call more than once.
func freezePeer(p *peer) (unfreeze func()) {
	release := make(chan struct{})
	wrapPeerConn(p, func(inner peerConn) peerConn { return &heldPushConn{peerConn: inner, release: release} })
	var once sync.Once
	return func() { once.Do(func() { close(release) }) }
}

// replayToPeer makes p's sender replay the log to it, as a direct call of the
// old catch-up did, and waits until p has acknowledged the replay.
func replayToPeer(ctx context.Context, p *peer) error {
	p.stall("a test asked for a replay")
	return catchUpPeer(ctx, p)
}

// listsDatabase reports whether a node's own catalog has the database.
func listsDatabase(t *testing.T, node *testNode, name string) bool {
	t.Helper()
	listed, err := node.server.core.ListDatabases(ListDatabasesRequest{})
	if err != nil {
		t.Fatalf("list databases on %s: %v", node.addr, err)
	}
	for _, database := range listed.Databases {
		if database.Name == name {
			return true
		}
	}
	return false
}

// slowestWriteAtOne measures what a write at `one` costs on this disk, as the
// slowest of samples writes through the owner's client path. A write at `one`
// waits for the owner's own log flush, which is the cost a test that bounds
// per-write time has to scale to.
func slowestWriteAtOne(t *testing.T, tc *testCluster, ownerIndex, samples int) time.Duration {
	t.Helper()
	slowest := time.Duration(0)
	for i := 0; i < samples; i++ {
		started := time.Now()
		frame := tc.frame(ownerIndex, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket,
			Key: fmt.Sprintf("cost%02d", i), Data: map[string]interface{}{"n": i}, WriteConcern: "one"})
		took := time.Since(started)
		if frame.Opcode == transport.OpError {
			t.Fatalf("the write measuring this disk's cost at one was answered with an error after %v: %s", took, frame.Payload)
		}
		slowest = max(slowest, took)
	}
	return slowest
}

// frozenReplicaOfABucket starts three nodes at factor 3, creates the test
// bucket, and freezes its owner's replication pushes to one replica. It
// returns the owner's index, the replica's index and the unfreeze.
func frozenReplicaOfABucket(t *testing.T, tc *testCluster) (int, int, func()) {
	t.Helper()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, testBucket, 3)
	ownerIndex := ownerIndexOf(t, tc, testBucket)
	replicaIndex := (ownerIndex + 1) % len(tc.nodes)
	ownerCluster := tc.nodes[ownerIndex].server.core.cluster
	unfreeze := freezePeer(ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[replicaIndex])))
	return ownerIndex, replicaIndex, unfreeze
}

// TestAFullQueueNeverBlocksAWriter is T-77's frozen case: a replica's
// connection is frozen, so its queue fills and stays full. 2,000 writes at
// `one` are answered with no error and none of them waits for room on the
// queue, and once the replica answers again every one of them is on it. On the
// starting tree every write past the queue waited the write-concern timeout for
// room and was then answered with an error.
//
// What it measures: the time each write took against the time one write costs
// on the disk it runs on, measured first in the same run. A write at `one`
// waits for the owner's own log flush, so a fixed total for 2,000 writes
// measures the disk's flush rate and says nothing about the queue (BUG-0567).
// The bound is twenty times the measured write cost, with a two-second floor
// for a loaded host (a write with no queue involved has taken 810 ms on a real
// disk while this bug was being fixed), and it never rises above half the
// write-concern timeout so that a write which waits for room on the queue
// (5 s) is always red. The
// assumption is that a write which does not wait for the queue costs about what
// the measured write costs; a disk slow enough to exceed the half-timeout cap on
// its own flushes fails here on the disk rather than passing silently and
// claiming the rule was exercised.
func TestAFullQueueNeverBlocksAWriter(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	ownerIndex, replicaIndex, unfreeze := frozenReplicaOfABucket(t, tc)
	defer unfreeze()

	const writes = 2000
	perWrite := slowestWriteAtOne(t, tc, ownerIndex, 5)
	ceiling := min(max(2*time.Second, 20*perWrite), DefaultWriteConcernTimeout/2)
	keys := make([]string, 0, writes)
	slowest := time.Duration(0)
	for i := 0; i < writes; i++ {
		key := fmt.Sprintf("q%04d", i)
		keys = append(keys, key)
		started := time.Now()
		frame := tc.frame(ownerIndex, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: key,
			Data: map[string]interface{}{"n": i}, WriteConcern: "one"})
		took := time.Since(started)
		if frame.Opcode == transport.OpError {
			t.Fatalf("write %d at one, with a frozen replica, was answered with an error after %v: %s", i, took, frame.Payload)
		}
		if took > ceiling {
			t.Fatalf("write %d at one took %v with a frozen replica, over the %v bound (one write measured %v on this disk); a write waits for no peer's queue", i, took, ceiling, perWrite)
		}
		slowest = max(slowest, took)
	}
	t.Logf("%d writes at one with a frozen replica; the slowest took %v (one write measured %v, bound %v)", writes, slowest, perWrite, ceiling)

	unfreeze()
	if missing := missingOnReplica(tc.nodes[ownerIndex], tc.nodes[replicaIndex], testBucket, keys, 60*time.Second); len(missing) > 0 {
		t.Fatalf("%d of %d writes are missing on the replica 60 s after it answered again (first %q)", len(missing), writes, missing[0])
	}
}

// TestDDLLeftOutOfAQueueIsReplayed is A-3.1: a database created while a peer's
// queue is full reaches that peer when it answers again, from the log, with no
// restart of anything. On the starting tree the broadcast logged "could not
// queue broadcast entry" and nothing ever sent it: catch-up replayed bucket
// entries only.
func TestDDLLeftOutOfAQueueIsReplayed(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	ownerIndex, replicaIndex, unfreeze := frozenReplicaOfABucket(t, tc)
	defer unfreeze()

	for i := 0; i < 1100; i++ {
		tc.frame(ownerIndex, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: fmt.Sprintf("fill%04d", i),
			Data: map[string]interface{}{"n": i}, WriteConcern: "one"})
	}
	const created = "after-a-full-queue"
	tc.ok(ownerIndex, transport.OpCreateDB, CreateDatabaseRequest{Name: created})
	if listsDatabase(t, tc.nodes[replicaIndex], created) {
		t.Fatal("test setup: the frozen replica already lists the database")
	}

	unfreeze()
	deadline := time.Now().Add(30 * time.Second)
	for !listsDatabase(t, tc.nodes[replicaIndex], created) {
		if time.Now().After(deadline) {
			t.Fatalf("the replica does not list database %q 30 s after it answered again: a create left out of its full queue was never sent", created)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestAStalledSenderReadsNoLogUntilThePeerAnswers is T-80 (A-3.3): a sender
// whose peer is down probes it and reads nothing of the log, however long the
// log grows. It has no red run: on the starting tree a stopped peer fails
// gossip and catch-up never ran for it either. It is proven by its guard.
func TestAStalledSenderReadsNoLogUntilThePeerAnswers(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, testBucket, 3)
	ownerIndex := ownerIndexOf(t, tc, testBucket)
	downIndex := (ownerIndex + 1) % len(tc.nodes)
	ownerCluster := tc.nodes[ownerIndex].server.core.cluster
	down := ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[downIndex]))
	stopNode(t, tc, downIndex)

	// The stream notices the stop at its first failed ship or probe.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tc.frame(ownerIndex, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "first", Data: map[string]interface{}{"n": 0}, WriteConcern: "one"})
	if err := waitForStream(ctx, down, func() bool { state, _ := down.streamStatus(); return state == streamStalled }); err != nil {
		t.Fatalf("the stream to the stopped peer never stalled: %v", err)
	}
	before := down.logReads.Load()

	until := time.Now().Add(10 * time.Second)
	for i := 0; time.Now().Before(until); i++ {
		tc.frame(ownerIndex, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: fmt.Sprintf("grow%05d", i),
			Data: map[string]interface{}{"n": i}, WriteConcern: "one"})
		// The peer's own gossip wakes its sender too; a wake is not an answer.
		down.wakeSender()
		time.Sleep(5 * time.Millisecond)
	}
	if reads := down.logReads.Load() - before; reads != 0 {
		t.Fatalf("the sender read the log %d times in 10 s for a peer that never answered its probe", reads)
	}
}

// TestAReplayPassesAnEntryOfABucketDroppedSince is A-3.2, spec §20.2's added
// row of deliverTo: a bucket is created, the owner's log is pruned past the
// create, the bucket takes three writes and is dropped, and a fourth node
// joins. The owner's replay to the joiner reads the three writes; a write to a
// bucket the owner no longer holds is addressed to nobody, and the drop is what
// the joiner is owed. Without the row the joiner refuses the first of them for
// ever and the stream never streams.
func TestAReplayPassesAnEntryOfABucketDroppedSince(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})

	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	members := tc.nodes[0].server.core.cluster.Members()
	after := ringOf(t, append(append([]ring.Node{}, members...), newcomer), 3)
	// Two buckets with one owner before and after the join, both placed on the
	// joiner.
	candidates := map[string][]string{}
	for attempt := 0; attempt < 1000; attempt++ {
		name := fmt.Sprintf("dropped%03d", attempt)
		key := placementOf(testDatabase, name)
		was, err := tc.nodes[0].server.core.cluster.Owner(key)
		if err != nil {
			t.Fatal(err)
		}
		set, err := after.Replicas(key.encoded, 3)
		if err != nil {
			t.Fatal(err)
		}
		if set[0].ID == was.ID && containsID(nodeIDs(set), newcomer.ID) {
			candidates[was.ID] = append(candidates[was.ID], name)
		}
	}
	ownerID := ""
	for id, names := range candidates {
		if len(names) >= 2 {
			ownerID = id
		}
	}
	if ownerID == "" {
		t.Fatal("test setup: no member owns two buckets before and after the join that are placed on the joiner")
	}
	kept, dropped := candidates[ownerID][0], candidates[ownerID][1]
	ownerIndex := nodeIndexByAddr(t, tc.addrs, ownerID)
	ownerCluster := tc.nodes[ownerIndex].server.core.cluster
	createBucketAt(tc, ownerIndex, testDatabase, kept, 0)
	createBucketAt(tc, ownerIndex, testDatabase, dropped, 0)
	waitForServing(t, tc, testDatabase, kept, 3)
	waitForServing(t, tc, testDatabase, dropped, 3)

	first := pruneLogPast(t, tc, ownerIndex, dropped)
	for i := 0; i < 3; i++ {
		putDocumentAt(tc, ownerIndex, testDatabase, dropped, fmt.Sprintf("before-the-drop-%d", i), "all")
	}
	tc.ok(ownerIndex, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: dropped})
	waitForServing(t, tc, testDatabase, dropped, 0)
	t.Logf("the owner's log starts at %d, after both creates", first)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := ownerCluster.proposeJoin(ctx, newcomer); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	joiner := startJoinTarget(t, tc, ports, tc.token)
	if err := joiner.server.core.cluster.reconcileMembership(ownerCluster.self.ID, ownerCluster.memberNodes(), ownerCluster.memberIDs(), ownerCluster.Epoch()); err != nil {
		t.Fatalf("joining node resync: %v", err)
	}
	pollUntil(t, "the joiner holds the kept bucket", func() bool { return joiner.server.core.bucketStoreOpen(testDatabase, kept) })

	toJoiner := ownerCluster.peerFor(newcomer)
	deadline := time.Now().Add(10 * time.Second)
	for !toJoiner.caughtUp() {
		if time.Now().After(deadline) {
			state, reason := toJoiner.streamStatus()
			t.Fatalf("the owner's stream to the joiner is %s 10 s after the joiner pulled its bucket: %s", state, reason)
		}
		time.Sleep(20 * time.Millisecond)
	}
	putDocumentAt(tc, ownerIndex, testDatabase, kept, "after-the-join", "one")
	pollUntil(t, "the joiner holds a write to the bucket it pulled", func() bool { return holdsKeyLocallyIn(joiner, kept, "after-the-join") })
}

// holdsKeyLocallyIn reports whether a node's own store holds key in bucket.
func holdsKeyLocallyIn(node *testNode, bucket, key string) bool {
	resp, err := node.server.core.Find(FindRequest{Database: testDatabase, Bucket: bucket, Key: key})
	return err == nil && resp != nil && resp.Found
}

// delayedConn sleeps before every request it sends: a slow link.
type delayedConn struct {
	peerConn
	delay time.Duration
}

func (d *delayedConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	time.Sleep(d.delay)
	return d.peerConn.SendRequest(ctx, lane, op, payload)
}

// TestAReturnedReplicaIsStreamingAgainWhileWritersKeepWriting is T-78 (review
// F-9): sixteen writers never stop, the owner's link to a replica is slow, and
// the replica is stopped for two seconds and started again. Its stream is
// streaming again within a bound stated as a multiple of this host's own
// stalled-stream return, without the writers pausing, because entering
// streaming records a boundary instead of waiting for an instant in which
// nothing is routed — an instant sixteen writers never give. At the end every
// key is on the replica.
//
// Each writer waits writerPace between its writes. Unpaced, sixteen writers on
// a tmpfs log append about 55,000 entries a second, faster than the slow link
// carries them: the stream stalled on a full queue before the replica was ever
// stopped, and no stream can catch up with a log that grows faster than it
// ships. Paced, they still leave no instant in which nothing is routed.
//
// A stream that falls behind stalls on its bounded queue and comes back from
// the log; that is not a fault, and on a host slower than the one the pace was
// chosen on it can be the state at any given instant. The setup therefore
// waits for the stream to be streaming again instead of looking once
// (BUG-0299).
func TestAReturnedReplicaIsStreamingAgainWhileWritersKeepWriting(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, testBucket, 3)
	ownerIndex := ownerIndexOf(t, tc, testBucket)
	replicaIndex := (ownerIndex + 1) % len(tc.nodes)
	ownerCluster := tc.nodes[ownerIndex].server.core.cluster
	toReplica := ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[replicaIndex]))
	wrapPeerConn(toReplica, func(inner peerConn) peerConn { return &delayedConn{peerConn: inner, delay: 500 * time.Microsecond} })

	const writers = 16
	const writerPace = 2 * time.Millisecond
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var keysMu sync.Mutex
	var keys []string
	var lastWrite [writers]atomic.Int64
	for w := 0; w < writers; w++ {
		client := tc.dial(ownerIndex)
		defer func() { _ = client.Close() }()
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("w%02d-%06d", w, i)
				payload, err := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: testBucket, Key: key,
					Data: map[string]interface{}{"n": i}, WriteConcern: "one"})
				if err != nil {
					t.Errorf("marshal %s: %v", key, err)
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				frame, err := client.SendRequest(ctx, transport.OpPut, payload)
				cancel()
				if err != nil || frame.Opcode == transport.OpError {
					t.Errorf("write %s: %v", key, err)
					return
				}
				lastWrite[w].Store(time.Now().UnixNano())
				keysMu.Lock()
				keys = append(keys, key)
				keysMu.Unlock()
				time.Sleep(writerPace)
			}
		}(w)
	}
	stopWriters := func() {
		select {
		case <-stop:
		default:
			close(stop)
		}
		wg.Wait()
	}
	defer stopWriters()

	// The load is one the slow link carries: the stream is streaming under it
	// before the replica stops. A stream that falls behind stalls on its
	// bounded queue and comes back from the log, so wait for streaming instead
	// of sampling the state once, and time the wait: it is how long a stalled
	// stream takes to return on this host, and the deadline after the restart
	// is a multiple of it rather than a bare ten seconds tuned on a faster one
	// (BUG-0299).
	toReplica.stall("the test measures how long this host takes to return a stalled stream to streaming")
	measureFrom := time.Now()
	setupCtx, setupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	setupErr := waitForStream(setupCtx, toReplica, func() bool {
		state, _ := toReplica.streamStatus()
		return state == streamStreaming
	})
	setupCancel()
	if setupErr != nil {
		t.Fatalf("test setup: the stream did not return to streaming under %d writers and a slow link: %v; this host cannot carry the load", writers, setupErr)
	}
	stalledReturn := time.Since(measureFrom)
	// The restart's return contains a fresh dial and handshake, which can cost
	// two ship timeouts before the replay starts, so the floor is ten times the
	// two-to-three seconds a host with the CPUs the pace was chosen on takes.
	streamingDeadline := max(30*time.Second, 10*stalledReturn)
	start := stopNode(t, tc, replicaIndex)
	time.Sleep(2 * time.Second)
	start()
	restarted := time.Now()

	// Streaming again, with every writer still writing.
	for {
		if state, _ := toReplica.streamStatus(); state == streamStreaming {
			break
		}
		if elapsed := time.Since(restarted); elapsed > streamingDeadline {
			state, reason := toReplica.streamStatus()
			t.Fatalf("the stream to the returned replica is %s (%s) %v after the restart, with %d writers writing; a stalled stream took %v to return under the same load, so the bound was %v",
				state, reason, elapsed, writers, stalledReturn, streamingDeadline)
		}
		time.Sleep(10 * time.Millisecond)
	}
	streamingAfter := time.Since(restarted)
	for w := range lastWrite {
		if idle := time.Since(time.Unix(0, lastWrite[w].Load())); idle > time.Second {
			t.Fatalf("writer %d had not written for %v when the stream became streaming: the writers paused", w, idle)
		}
	}
	stopWriters()
	t.Logf("streaming again %v after the restart; %d writes in all", streamingAfter, len(keys))

	keysMu.Lock()
	written := append([]string(nil), keys...)
	keysMu.Unlock()
	if missing := missingOnReplica(tc.nodes[ownerIndex], tc.nodes[replicaIndex], testBucket, written, 60*time.Second); len(missing) > 0 {
		t.Fatalf("%d of %d keys are missing on the replica 60 s after the writers stopped (first %q)", len(missing), len(written), missing[0])
	}
}

// adoptionScenario names an owner, a replica, a member that is proposed and
// never started, and two buckets: one the owner keeps after the join, still
// placed on the replica, and one the join moves to the newcomer. The replica
// holds both before the join (three nodes, factor 3).
type adoptionScenario struct {
	owner, replica int
	newcomer       ring.Node
	kept, moved    string
}

func findAdoptionScenario(t *testing.T, tc *testCluster) adoptionScenario {
	t.Helper()
	newcomerAddr := fabricatedMemberAddr(t)
	newcomer := ring.Node{ID: newcomerAddr, Addr: newcomerAddr}
	members := tc.nodes[0].server.core.cluster.Members()
	before := ringOf(t, members, 3)
	after := ringOf(t, append(append([]ring.Node{}, members...), newcomer), 3)
	for owner := range tc.nodes {
		for replica := range tc.nodes {
			if replica == owner {
				continue
			}
			sc := adoptionScenario{owner: owner, replica: replica, newcomer: newcomer}
			for attempt := 0; attempt < 3000 && (sc.kept == "" || sc.moved == ""); attempt++ {
				name := fmt.Sprintf("adopt%04d", attempt)
				key := placementOf(testDatabase, name)
				was, err := before.Replicas(key.encoded, 3)
				if err != nil {
					t.Fatal(err)
				}
				now, err := after.Replicas(key.encoded, 3)
				if err != nil {
					t.Fatal(err)
				}
				if was[0].ID != tc.addrs[owner] {
					continue
				}
				switch {
				case sc.kept == "" && now[0].ID == tc.addrs[owner] && containsID(nodeIDs(now), tc.addrs[replica]):
					sc.kept = name
				case sc.moved == "" && now[0].ID == newcomer.ID:
					sc.moved = name
				}
			}
			if sc.kept != "" && sc.moved != "" {
				return sc
			}
		}
	}
	t.Fatal("test setup: no member owns a bucket the join keeps on a replica and one the join moves to the newcomer")
	return adoptionScenario{}
}

// TestAnAdoptionReDerivesEveryStreamFromTheLog is S-8: entries queued for a
// peer when a membership change is adopted are not shipped from the queue;
// the stream starts again from the log under the new ring. An entry of a
// bucket the owner still owns arrives once; an entry of a bucket the change
// moved to another owner arrives too, from the log, because a ceded owner
// finishes delivering its term (re-aimed by replication dispatch 10b, step
// 10b.4: it was stranded before).
//
// The change is a join of a member that is proposed and never started, which
// takes buckets from every owner. A removal on a bounded-load ring moved none
// of a surviving owner's buckets on four of ten port draws, and its new owner
// is a running node, which would re-originate the old owner's last pushes as
// they land on it (build doc §1, trap 1; dispatch 4's to close).
func TestAnAdoptionReDerivesEveryStreamFromTheLog(t *testing.T) {
	tc := newTestClusterTuned(t, 3, 3, autoRebalanceOff, func(_ int, c *Cluster) {
		// A frozen ship must outlast the adoption; it gives up at half this.
		c.wcTimeout = time.Minute
	})
	defer tc.stop()
	sc := findAdoptionScenario(t, tc)
	tc.ok(sc.owner, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(sc.owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: sc.kept, Type: BucketTypeFIFO})
	tc.ok(sc.owner, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: sc.moved, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, sc.kept, 3)
	waitForServing(t, tc, testDatabase, sc.moved, 3)

	ownerCluster := tc.nodes[sc.owner].server.core.cluster
	toReplica := ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[sc.replica]))
	unfreeze := freezePeer(toReplica)
	defer unfreeze()

	push := func(i int) {
		tc.ok(sc.owner, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: sc.kept,
			Data: map[string]interface{}{"n": i}, WriteConcern: "one"})
	}
	// The first push is taken by the sender into a ship the frozen peer holds;
	// everything after it waits on the queue.
	push(0)
	time.Sleep(200 * time.Millisecond)
	const pushes, puts = 20, 20
	for i := 1; i < pushes; i++ {
		push(i)
	}
	movedKeys := make([]string, 0, puts)
	for i := 0; i < puts; i++ {
		key := fmt.Sprintf("queued-%02d", i)
		movedKeys = append(movedKeys, key)
		tc.ok(sc.owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: sc.moved, Key: key,
			Data: map[string]interface{}{"n": i}, WriteConcern: "one"})
	}
	if queued := len(toReplica.sendCh); queued == 0 {
		t.Fatal("test setup: nothing is queued for the frozen replica")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := ownerCluster.proposeJoin(ctx, sc.newcomer); err != nil {
		t.Fatalf("join %s: %v", sc.newcomer.ID, err)
	}
	if owner, err := ownerCluster.Owner(placementOf(testDatabase, sc.moved)); err != nil || owner.ID != sc.newcomer.ID {
		t.Fatalf("test setup: %s is owned by %v (%v) after the join, not by the newcomer", sc.moved, owner, err)
	}
	unfreeze()

	replicaCore := tc.nodes[sc.replica].server.core
	pollUntil(t, "the replica holds every push to the bucket its owner kept", func() bool {
		count, err := replicaCore.Count(CountRequest{Database: testDatabase, Bucket: sc.kept})
		return err == nil && count.Count >= pushes
	})
	time.Sleep(2 * time.Second)
	if count, err := replicaCore.Count(CountRequest{Database: testDatabase, Bucket: sc.kept}); err != nil || count.Count != pushes {
		t.Fatalf("the replica's queue holds %d elements (%v), want each of the %d pushes once", count.Count, err, pushes)
	}
	// The bucket the join moved: node 1 ceded it, and finishes delivering its
	// term from its log to the members of the bucket's set now that were in it
	// when the entries were logged (spec §20.8.3, step 10b.4; re-aimed from
	// §19.3.12's stranding, which dispatch 10b replaces). The newcomer never
	// starts, so nothing takes its term. A replica the join left out of the
	// set is not sent them.
	movedSet, err := ownerCluster.Replicas(placementOf(testDatabase, sc.moved))
	if err != nil {
		t.Fatalf("placing %s after the join: %v", sc.moved, err)
	}
	if nodeListed(movedSet, tc.addrs[sc.replica]) {
		pollUntil(t, "the replica holds every put node 1 finishes delivering", func() bool {
			for _, key := range movedKeys {
				if !holdsKeyLocallyIn(tc.nodes[sc.replica], sc.moved, key) {
					return false
				}
			}
			return true
		})
		return
	}
	for _, key := range movedKeys {
		if holdsKeyLocallyIn(tc.nodes[sc.replica], sc.moved, key) {
			t.Fatalf("the replica holds %s, written to %s while node %d owned it and queued before the join left the replica out of the bucket's set: node %d shipped it outside the set",
				key, sc.moved, sc.owner+1, sc.owner+1)
		}
	}
}
