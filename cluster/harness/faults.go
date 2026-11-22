package harness

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster/peer"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Faults is the harness's fault layer. Every connection a node dials, on every channel
// including Raft, passes through it between TCP and TLS, so a fault changes what the real
// transport sees and the transport code under test is the production code.
//
// The five faults:
//   - a partition between node groups (Partition, Heal, Pause): connections across the cut
//     are severed and new dials are refused, in both directions;
//   - a one-way cut (BlockOneWay): the dials of one named node to another are refused and its
//     live connections to it severed, while the other node's dials to it go on;
//   - a channel fault between two nodes: DelayChannel adds latency to every write on it,
//     DropChannel cuts it now and refuses its next dials;
//   - a process fault: Kill and Restart on Cluster stop and reopen a node on the same
//     directory and address;
//   - a disk fault: FailDisk makes one node's log refuse appends like a full disk, and
//     SlowDisk makes every append on one node take at least a set time (a slow disk).
//
// All of them are deterministic: they act at the moment of the call, not on a timer.
type Faults struct {
	mu sync.Mutex
	// blocked holds unordered node pairs that cannot talk.
	blocked map[[2]string]bool
	// oneWay holds ordered (from, to) pairs whose dials are refused; the reverse direction
	// is untouched.
	oneWay map[[2]string]bool
	// delay is the latency added to writes per (from, to, channel).
	delay map[faultPath]time.Duration
	// refuse is how many more dials of a path are refused.
	refuse map[faultPath]int
	// conns are the live dialed connections, so a fault can cut them.
	conns map[*faultConn]struct{}
	// stalled holds nodes that accept connections and read what is sent but never answer
	// (a process stopped with SIGSTOP while its sockets stay open).
	stalled map[string]bool
	// addrs maps an internode address to its node id.
	addrs map[string]string
	// dropReplies holds the paths whose established connections lose what they read: the
	// request is delivered and handled, the answer never arrives.
	dropReplies map[faultPath]bool
}

type faultPath struct {
	from, to string
	ch       transport.Channel
}

func newFaults() *Faults {
	return &Faults{
		blocked: map[[2]string]bool{}, oneWay: map[[2]string]bool{}, delay: map[faultPath]time.Duration{}, refuse: map[faultPath]int{},
		conns: map[*faultConn]struct{}{}, addrs: map[string]string{}, stalled: map[string]bool{}, dropReplies: map[faultPath]bool{},
	}
}

func pairOf(a, b string) [2]string {
	if a > b {
		a, b = b, a
	}
	return [2]string{a, b}
}

func (f *Faults) register(id, addr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addrs[addr] = id
}

// block cuts a and b from each other.
func (f *Faults) block(a, b string) {
	f.mu.Lock()
	f.blocked[pairOf(a, b)] = true
	f.mu.Unlock()
	f.sever(func(c *faultConn) bool { return pairOf(c.from, c.to) == pairOf(a, b) })
}

// BlockOneWay cuts the direction from to to while on is true: every dial from from to to, on
// every channel including Raft, is refused, and the live connections from dialed to to are
// severed (their replies die with them). Connections that to dialed to from are untouched, so
// to keeps reaching from and from keeps receiving what to sends on them. Turning it off lifts
// only this pair and direction; Heal lifts every cut.
//
// Pick the fault by what the row needs. "The owner's pushes never get acked" is
// DropReplies(owner, copy, data, true), not this: the acks travel on the copy's own
// connection, which a one-way cut leaves alone. "The owner's heartbeats never reach the
// leader" and "this node can send nothing it dials" are BlockOneWay(owner, leader, true).
func (f *Faults) BlockOneWay(from, to string, on bool) {
	f.mu.Lock()
	if on {
		f.oneWay[[2]string{from, to}] = true
	} else {
		delete(f.oneWay, [2]string{from, to})
	}
	f.mu.Unlock()
	if on {
		f.sever(func(c *faultConn) bool { return c.from == from && c.to == to })
	}
}

// sever closes every live connection the filter picks.
func (f *Faults) sever(pick func(*faultConn) bool) {
	f.mu.Lock()
	var cut []*faultConn
	for c := range f.conns {
		if pick(c) {
			cut = append(cut, c)
		}
	}
	f.mu.Unlock()
	for _, c := range cut {
		_ = c.Close() // best-effort: the fault is the point
	}
}

// Stall makes node id stop answering without closing anything: connections to it stay
// open, writes to it succeed, and every read from it blocks until Unstall.
func (f *Faults) Stall(id string, stalled bool) {
	f.mu.Lock()
	if stalled {
		f.stalled[id] = true
	} else {
		delete(f.stalled, id)
	}
	f.mu.Unlock()
}

func (f *Faults) isStalled(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stalled[id]
}

// Heal removes every partition and every one-way cut. Connections reopen on the next call; a peer in backoff
// retries within the backoff bound.
func (f *Faults) Heal() {
	f.mu.Lock()
	clear(f.blocked)
	clear(f.oneWay)
	f.mu.Unlock()
}

// DelayChannel adds d to every write from one node to another on a channel, now and for
// connections dialed later. A zero d removes it.
func (f *Faults) DelayChannel(from, to string, ch transport.Channel, d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if d <= 0 {
		delete(f.delay, faultPath{from, to, ch})
		return
	}
	f.delay[faultPath{from, to, ch}] = d
}

// DropChannel cuts the live connection from one node to another on a channel and refuses
// its next dials, so the channel is down for exactly that many reconnect attempts.
func (f *Faults) DropChannel(from, to string, ch transport.Channel, dials int) {
	f.mu.Lock()
	f.refuse[faultPath{from, to, ch}] = dials
	f.mu.Unlock()
	f.sever(func(c *faultConn) bool { return c.path == faultPath{from, to, ch} })
}

// DropReplies makes every established connection from one node to another on a channel fail
// its reads while on is true: a request still reaches the far node and is handled, and the
// caller never sees the answer. New connections complete their handshake, so each retry is
// delivered too.
func (f *Faults) DropReplies(from, to string, ch transport.Channel, on bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if on {
		f.dropReplies[faultPath{from, to, ch}] = true
		return
	}
	delete(f.dropReplies, faultPath{from, to, ch})
}

// dialer is the peer.Dialer of node from: refuse, or connect, wrap and pin.
func (f *Faults) dialer(from string, trust *peer.Trust) peer.Dialer {
	return func(ctx context.Context, _ string, ch transport.Channel, addr string) (net.Conn, error) {
		// A stopped process dials nobody: its own elections and requests never leave it.
		for f.isStalled(from) {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("harness: %s is stalled and cannot dial: %w", from, ctx.Err())
			case <-time.After(5 * time.Millisecond): // the stall is the fault being injected
			}
		}
		f.mu.Lock()
		to := f.addrs[addr]
		path := faultPath{from, to, ch}
		cut := f.blocked[pairOf(from, to)]
		cutOneWay := f.oneWay[[2]string{from, to}]
		refused := f.refuse[path] > 0
		if refused {
			f.refuse[path]--
		}
		f.mu.Unlock()
		if cut {
			return nil, fmt.Errorf("harness: %s cannot reach %s (partitioned)", from, to)
		}
		if cutOneWay {
			return nil, fmt.Errorf("harness: %s cannot dial %s (one-way cut)", from, to)
		}
		if refused {
			return nil, fmt.Errorf("harness: the %s channel from %s to %s is dropped", ch, from, to)
		}
		raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("harness: dialing %s: %w", to, err)
		}
		wrapped := &faultConn{Conn: raw, faults: f, from: from, to: to, path: path}
		f.mu.Lock()
		f.conns[wrapped] = struct{}{}
		f.mu.Unlock()
		conf, err := trust.ClientConfig(addr)
		if err != nil {
			return nil, errors.Join(err, wrapped.Close())
		}
		secured := tls.Client(wrapped, conf)
		if err := secured.HandshakeContext(ctx); err != nil {
			return nil, errors.Join(fmt.Errorf("harness: TLS to %s: %w", to, err), wrapped.Close())
		}
		wrapped.ready.Store(true)
		return secured, nil
	}
}

// faultConn is a dialed connection the fault layer can delay and cut.
type faultConn struct {
	net.Conn
	faults   *Faults
	from, to string
	path     faultPath
	once     sync.Once
	once2    sync.Once
	// readDeadline is the caller's read deadline in Unix nanoseconds, 0 for none.
	readDeadline atomic.Int64
	done         chan struct{}
	// ready is set once the TLS handshake is done; DropReplies spares the handshake.
	ready atomic.Bool
}

// Write delays the write when the path has a delay, and fails it once the pair is cut.
func (c *faultConn) Write(p []byte) (int, error) {
	c.faults.mu.Lock()
	delay := c.faults.delay[c.path]
	cut := c.faults.blocked[pairOf(c.from, c.to)]
	cutOneWay := c.faults.oneWay[[2]string{c.from, c.to}]
	c.faults.mu.Unlock()
	if cut {
		return 0, fmt.Errorf("harness: %s cannot reach %s (partitioned)", c.from, c.to)
	}
	if cutOneWay {
		return 0, fmt.Errorf("harness: %s cannot write to %s (one-way cut)", c.from, c.to)
	}
	for c.faults.isStalled(c.from) {
		select {
		case <-c.closed():
			return 0, net.ErrClosed
		case <-time.After(5 * time.Millisecond): // a stopped process sends nothing
		}
	}
	if delay > 0 {
		time.Sleep(delay) // the injected latency is the fault; nothing waits on it for logic
	}
	return c.Conn.Write(p)
}

// Read blocks while the far node is stalled, and returns once the connection is closed.
func (c *faultConn) Read(p []byte) (int, error) {
	if c.ready.Load() {
		c.faults.mu.Lock()
		dropped := c.faults.dropReplies[c.path]
		c.faults.mu.Unlock()
		if dropped {
			_ = c.Close() // best-effort: the lost answer is the fault
			return 0, net.ErrClosed
		}
	}
	for c.faults.isStalled(c.to) {
		if deadline := time.Unix(0, c.readDeadline.Load()); c.readDeadline.Load() != 0 && time.Now().After(deadline) {
			return 0, os.ErrDeadlineExceeded // a stalled peer still honours the caller's deadline
		}
		select {
		case <-c.closed():
			return 0, net.ErrClosed
		case <-time.After(5 * time.Millisecond): // the stall is the fault being injected
		}
	}
	return c.Conn.Read(p)
}

// SetReadDeadline records the deadline for the stall loop and passes it on.
func (c *faultConn) SetReadDeadline(t time.Time) error {
	if t.IsZero() {
		c.readDeadline.Store(0)
	} else {
		c.readDeadline.Store(t.UnixNano())
	}
	return c.Conn.SetReadDeadline(t)
}

// SetDeadline records the read half and passes it on.
func (c *faultConn) SetDeadline(t time.Time) error {
	if t.IsZero() {
		c.readDeadline.Store(0)
	} else {
		c.readDeadline.Store(t.UnixNano())
	}
	return c.Conn.SetDeadline(t)
}

func (c *faultConn) closed() chan struct{} {
	c.once2.Do(func() { c.done = make(chan struct{}) })
	return c.done
}

// Close closes once and forgets the connection.
func (c *faultConn) Close() error {
	var err error
	c.once.Do(func() {
		c.faults.mu.Lock()
		delete(c.faults.conns, c)
		c.faults.mu.Unlock()
		close(c.closed())
		err = c.Conn.Close()
	})
	return err
}

// ---- the disk fault ----------------------------------------------------------------

// diskFault makes a node's log refuse appends while set.
type diskFault struct {
	failing atomic.Bool
	// slowNanos is added to every append: a slow disk, so a test can see what waits on it.
	slowNanos atomic.Int64
}

// errDiskFull is what a failing log appends return.
var errDiskFull = errors.New("harness: injected disk fault: the log refuses appends (no space left on device)")

type faultyLog struct {
	wire.Log
	fault *diskFault
}

// AppendFrom fails while the fault is set, and takes at least the slow-disk delay.
func (l faultyLog) AppendFrom(bucket string, op byte, key, data []byte, origin storage.WALOrigin) (uint64, error) {
	if l.fault.failing.Load() {
		return 0, errDiskFull
	}
	if d := l.fault.slowNanos.Load(); d > 0 {
		time.Sleep(time.Duration(d)) // the injected latency is the fault; nothing waits on it for logic
	}
	return l.Log.AppendFrom(bucket, op, key, data, origin)
}

// AppendUnsyncedFrom is AppendFrom's unsynced path with the same failure. It adds no delay: a write that
// splits its append from its flush pays the slow-disk delay once, in SyncThrough, which runs outside the
// pair lock like the fsync it stands for.
func (l faultyLog) AppendUnsyncedFrom(bucket string, op byte, key, data []byte, origin storage.WALOrigin) (uint64, error) {
	if l.fault.failing.Load() {
		return 0, errDiskFull
	}
	return l.Log.AppendUnsyncedFrom(bucket, op, key, data, origin)
}

// SyncThrough waits the slow-disk delay and never fails: the fault refuses writes, not the flush of what was written.
func (l faultyLog) SyncThrough(seq uint64) error {
	if d := l.fault.slowNanos.Load(); d > 0 {
		time.Sleep(time.Duration(d)) // the injected latency is the fault; nothing waits on it for logic
	}
	return l.Log.SyncThrough(seq)
}

// ackFault fails the acknowledgements a node's copy side sends while set, and counts every attempt.
type ackFault struct {
	failing  atomic.Bool
	attempts atomic.Int64
	// calls and entries count what the copy side sent on the ack path: one call carries one ack
	// through Ack and several through AckBatch, so calls below entries shows the batch in use.
	calls, entries atomic.Int64
	// writeCalls and writeEntries count the owner side's WRITE and WRITE_BATCH calls and the
	// records they carried.
	writeCalls, writeEntries atomic.Int64
	// catchUps counts the CATCHUP_REQs the node sent, by the node asked (string to *atomic.Int64).
	catchUps sync.Map
}

// catchUpsTo is the counter of the catch-up requests sent to one node.
func (f *ackFault) catchUpsTo(source string) *atomic.Int64 {
	counter, _ := f.catchUps.LoadOrStore(source, new(atomic.Int64))
	return counter.(*atomic.Int64)
}

// errAckFault is what a failing acknowledgement returns.
var errAckFault = errors.New("harness: injected fault: the acknowledgement was not sent")

// ackFaultLink is the replicator's data channel with the acknowledgement fault in front of Ack.
type ackFaultLink struct {
	wire.Link
	fault *ackFault
}

// Ack records the attempt and fails while the fault is set.
func (l ackFaultLink) Ack(ctx context.Context, peer string, ack wire.WriteAck) error {
	l.fault.attempts.Add(1)
	l.fault.calls.Add(1)
	l.fault.entries.Add(1)
	if l.fault.failing.Load() {
		return errAckFault
	}
	return l.Link.Ack(ctx, peer, ack)
}

// AckBatch records the attempt of every ack in the batch, counts one call, and fails while the fault is set. The wrapper must
// forward it: the embedded Link interface hides the inner link's AckBatcher, so without this method the node would keep the
// per-record path and the harness would prove nothing about batching.
func (l ackFaultLink) AckBatch(ctx context.Context, peer string, acks []wire.WriteAck) error {
	l.fault.attempts.Add(int64(len(acks)))
	l.fault.calls.Add(1)
	l.fault.entries.Add(int64(len(acks)))
	if l.fault.failing.Load() {
		return errAckFault
	}
	batcher, ok := l.Link.(wire.AckBatcher)
	if !ok {
		return fmt.Errorf("harness: the replication link %T cannot send acknowledgements in a batch", l.Link)
	}
	return batcher.AckBatch(ctx, peer, acks)
}

// CatchUp counts the request by the node asked and forwards it.
func (l ackFaultLink) CatchUp(ctx context.Context, source string, req wire.CatchUpRequest) (wire.CatchUpChunk, error) {
	l.fault.catchUpsTo(source).Add(1)
	return l.Link.CatchUp(ctx, source, req)
}

// WriteBatch counts the call and the records in it and forwards it. The wrapper must forward it for the same reason as AckBatch:
// the embedded Link interface hides the inner link's WriteBatcher, so without this method the owner would keep shipping one
// record per call.
func (l ackFaultLink) WriteBatch(ctx context.Context, peer string, frames []wire.WriteFrame) error {
	l.fault.writeCalls.Add(1)
	l.fault.writeEntries.Add(int64(len(frames)))
	batcher, ok := l.Link.(wire.WriteBatcher)
	if !ok {
		return fmt.Errorf("harness: the replication link %T cannot send writes in a batch", l.Link)
	}
	return batcher.WriteBatch(ctx, peer, frames)
}
