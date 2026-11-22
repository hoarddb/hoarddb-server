// Package peer is a node's connection to the other nodes: one internode listener that
// carries the data, control, seed and raft channels, and one authenticated connection per
// peer per channel for the calls the node makes. It knows nothing of the messages it
// carries; cluster/wire gives them meaning.
//
// Cost per peer: no goroutine of its own (a call runs on its caller's goroutine) and a
// bounded number of outbound descriptors: one per channel in use, and for the data channel
// one for replication, one for declaration reads and up to ForwardConns for forwards (a lane
// for each, so a long catch-up never queues behind a client write, and forwarded writes reach their owner
// concurrently so its group commit can share a flush). The forward connections are opened
// only while that many calls are in flight at once. The peer's server pays one goroutine per
// accepted connection. Nothing here is per database, slot or bucket.
package peer

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Lane names which connection of a channel a call uses. Calls on LaneMain are serial; the
// connections of LaneForward form a pool (see ForwardConns).
type Lane int

// The lanes. The data channel has both; the others use LaneMain.
const (
	// LaneMain carries the channel's ordinary calls (replication on the data channel).
	LaneMain Lane = iota
	// LaneForward carries forwarded client writes and reads on the data channel.
	LaneForward
	// LaneDecl carries the declaration reads of a node whose cache is cold. It is a lane of
	// its own so that read, which is small and answered without a gate, never waits for
	// the forwarded writes that keep every LaneForward connection busy (BUG-0770).
	LaneDecl
	laneCount
)

// ForwardConns is the most connections the forward lane opens to one peer, and so the most
// forwarded calls in flight to one peer at once (BUG-0769). A connection carries one request
// at a time on both ends (ChannelClient.Call holds its turn from request to reply; the
// channel server answers one frame per connection before it reads the next), so concurrency
// to the owner is connections. Connections are opened on demand, when every open one is busy,
// so a caller that waits for each reply uses one, as before; they stay open until the peer
// is forgotten or the network closes (an unused one is redialled after ConnMaxIdle). This is a per-peer constant: it does not grow with
// databases, buckets or slots. It stays under the receiving side's per-peer in-flight share
// (a quarter of 256 requests) and, over a few peers, well under its 256 connection cap.
// Sizing limit: a peer holds up to ForwardConns + 2 (replication, declaration) = 10 data
// connections to every other node, against the receiver's cluster.peer_max_conns (256), so
// past about 25 peers all sending under full load an extra dial is closed at accept, that
// call fails as unknown and the peer's forward lane backs off. A cluster that large needs a
// cap-aware pool (open item of BUG-0769) or a higher peer_max_conns.
//
// Ordering: the pool gives no order between two calls in flight together, and none was
// promised before: the client-facing server already runs pipelined frames of one connection
// concurrently, so they raced for the lane's single turn in no defined order. A caller that
// waits for a reply before its next call still reaches the owner in order. A call is never
// moved to another connection: when its connection breaks, that call fails (the caller
// learns the write is unknown) and is not retried here.
const ForwardConns = 8

// Dialer opens the raw connection to a peer. The default dials TCP and, when the network
// has a TLS config, runs the pinned handshake. The harness replaces it to inject faults.
type Dialer func(ctx context.Context, peer string, ch transport.Channel, addr string) (net.Conn, error)

// Config opens a Network.
type Config struct {
	// Self is this node's id.
	Self string
	// Listener is the internode listener, already wrapped in TLS when the node uses it.
	// The Network owns it and closes it.
	Listener net.Listener
	// Hello is this node's handshake for a new connection; the channel is set per dial.
	Hello func() transport.Handshake
	// Addr resolves a member id to its internode address.
	Addr func(id string) (string, bool)
	// Admit may refuse a peer after its handshake (cluster id, stale incarnation).
	Admit func(transport.Handshake) error
	// Data, Control and Seed answer the requests of their channels. A nil handler leaves
	// the channel unserved: connections for it are dropped after a bounded wait.
	Data, Control, Seed transport.ChannelHandler
	// Identity answers IDENTITY_REQUEST on the identity channel, one request per connection.
	// IdentityAdmit admits its callers: the pinned key alone, so a node that is not yet a
	// member can be asked who it is. A nil Identity leaves the channel unserved.
	Identity      transport.ChannelHandler
	IdentityAdmit func(transport.Handshake) error
	// SeedLimits bound the seed channel's server (connection cap, idle deadline); zero
	// fields take the transport's defaults.
	SeedLimits transport.ChannelLimits
	// MuxLimits bound the shared port before a connection is routed; PeerLimits bound the
	// data and control channel servers. Zero fields take the transport's defaults.
	MuxLimits  transport.PeerMuxLimits
	PeerLimits transport.ChannelLimits
	// TLS is the client TLS config for a dial to addr (pinned fingerprints); nil dials
	// plain TCP, which only a single-process test or an untrusted-network-free host does.
	TLS func(id, addr string) (*tls.Config, error)
	// IDAt names the member or configured server whose address is addr, for a dial that carries
	// no id (the Raft stream layer dials by address). Nil, or a miss, refuses the dial.
	IDAt func(addr string) (id string, found bool, err error)
	// Dial replaces the whole dial (after Self-side bookkeeping); nil uses TCP plus TLS.
	Dial Dialer
	// Backoff bounds the retry delay of a peer that stopped answering. Zero values take
	// 50 ms and 2 s.
	BackoffBase, BackoffMax time.Duration
	// MaxMessageBytes bounds the body of a message this node sends. Zero takes 16 MiB.
	MaxMessageBytes int
	// ConnMaxIdle is how long an outbound connection may sit unused before the next call
	// redials it instead of writing into a socket the peer has probably closed. Zero takes
	// 60 s. Set it below the peer's idle limit (cluster.peer_idle_seconds, 120 s).
	ConnMaxIdle time.Duration
	// Now reads the clock the backoff uses; nil takes time.Now.
	Now func() time.Time
	// Warn receives the one-line hint of a peer that closes its connection right after the
	// handshake (it may run another build), at most once a minute per peer; nil drops it.
	Warn func(message string)
}

// ErrBackoff is the refusal for a call to a peer that failed recently and is not yet due
// for another try.
var ErrBackoff = errors.New("peer is in backoff")

// ErrTooLarge is the refusal for a message body over the configured bound.
var ErrTooLarge = errors.New("message body too large")

type clientKey struct {
	peer string
	ch   transport.Channel
	lane Lane
}

// pooledClient is one connection of a lane and when its last call finished, so a connection
// the peer has probably closed for idleness is redialled before it is written into.
type pooledClient struct {
	client *transport.ChannelClient
	last   time.Time
}

// defaultConnMaxIdle is Config.ConnMaxIdle's default: half the peer's default idle limit.
const defaultConnMaxIdle = 60 * time.Second

// clientEntry is the connection or pool of one (peer, channel, lane). A lane other than
// LaneForward has one connection; the forward lane has up to ForwardConns, opened on demand.
// Backoff is the lane's, not a connection's: a peer that stopped answering stops them all.
type clientEntry struct {
	addr string
	// idle holds the connections no call is using; a call takes one and puts it back.
	idle chan *pooledClient
	// size is the most connections; made, all and retired are guarded by Network.mu.
	size    int
	made    int
	all     []*transport.ChannelClient
	retired bool
	// done is closed when the entry is retired, to wake calls waiting for a connection.
	done chan struct{}
	// failures and retryAt are the backoff state of the lane.
	failures int
	retryAt  time.Time
}

// Network is a node's internode transport.
type Network struct {
	cfg     Config
	mux     *transport.PeerMux
	servers []*transport.ChannelServer

	mu      sync.Mutex
	clients map[clientKey]*clientEntry
	closed  bool
	// closedWarned is when each peer last got the closed-handshake hint; guarded by mu.
	closedWarned map[string]time.Time

	closeOnce sync.Once
	closeErr  error
}

// New starts serving cfg.Listener. Close it to end every connection and goroutine.
func New(cfg Config) (*Network, error) {
	if cfg.Self == "" || cfg.Listener == nil || cfg.Hello == nil || cfg.Addr == nil {
		return nil, errors.New("opening the peer network needs Self, Listener, Hello and Addr: the node's id, its internode listener, its handshake and how to find a member's address")
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = 50 * time.Millisecond
	}
	if cfg.BackoffMax <= 0 {
		cfg.BackoffMax = 2 * time.Second
	}
	if cfg.MaxMessageBytes <= 0 {
		cfg.MaxMessageBytes = 16 << 20
	}
	if cfg.ConnMaxIdle <= 0 {
		cfg.ConnMaxIdle = defaultConnMaxIdle
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	n := &Network{cfg: cfg, mux: transport.NewPeerMuxWithLimits(cfg.Listener, cfg.MuxLimits), clients: map[clientKey]*clientEntry{}}
	for ch, handler := range map[transport.Channel]transport.ChannelHandler{
		transport.ChannelData: cfg.Data, transport.ChannelControl: cfg.Control, transport.ChannelSeed: cfg.Seed,
	} {
		if handler == nil {
			continue
		}
		limits := cfg.PeerLimits
		if ch == transport.ChannelSeed {
			limits = cfg.SeedLimits
		}
		if ch == transport.ChannelControl {
			// A lease request keeps a follower serving; slow probes must not crowd it out.
			limits.Priority = func(kind byte) bool { return kind == transport.KindLeaseReq }
		}
		n.servers = append(n.servers, transport.ServeChannelWithLimits(n.mux.Listener(ch), ch, cfg.Admit, handler, limits))
	}
	if cfg.Identity != nil {
		limits := cfg.PeerLimits
		limits.OneShot = true
		n.servers = append(n.servers, transport.ServeChannelWithLimits(n.mux.Listener(transport.ChannelIdentity), transport.ChannelIdentity, cfg.IdentityAdmit, cfg.Identity, limits))
	}
	return n, nil
}

// Addr is the address peers dial.
func (n *Network) Addr() net.Addr { return n.mux.Addr() }

// RaftListener is the listener the Raft stream layer accepts on: the connections whose
// handshake names the raft channel.
func (n *Network) RaftListener() net.Listener { return n.mux.Listener(transport.ChannelRaft) }

// DialRaft opens a connection for the Raft stream layer the way the other channels do:
// through the same TLS and the same fault-injectable dialer.
func (n *Network) DialRaft(addr string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return n.dial(ctx, "", transport.ChannelRaft, addr)
}

func (n *Network) dial(ctx context.Context, peer string, ch transport.Channel, addr string) (net.Conn, error) {
	if peer == "" {
		// A dial that names no peer is held to the key of whoever holds the address; an address
		// that resolves to no one would otherwise be dialled under "any pinned key" (G1).
		id, ok := "", false
		if n.cfg.IDAt != nil {
			var err error
			id, ok, err = n.cfg.IDAt(addr)
			if err != nil {
				return nil, fmt.Errorf("refused to dial %s for the %s channel: could not read which node holds that address (%w). This is not a verdict on the address: the dial is tried again on the next heartbeat", addr, ch, err)
			}
		}
		if !ok {
			return nil, fmt.Errorf("refused to dial %s for the %s channel: no member and no configured server has that address, so no key can be required of it. A join registers the address first", addr, ch)
		}
		peer = id
	}
	if n.cfg.Dial != nil {
		return n.cfg.Dial(ctx, peer, ch, addr)
	}
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dialing %s channel of %s at %s: %w", ch, peer, addr, err)
	}
	if n.cfg.TLS == nil {
		return raw, nil
	}
	conf, err := n.cfg.TLS(peer, addr)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("building the TLS config for %s: %w", addr, err), raw.Close())
	}
	conn := tls.Client(raw, conf)
	if err := conn.HandshakeContext(ctx); err != nil {
		return nil, errors.Join(fmt.Errorf("TLS handshake with %s at %s (is its fingerprint in the pinned list?): %w", peer, addr, err), raw.Close())
	}
	return conn, nil
}

// Ask sends msg on channel ch to the node peer at addr over a connection of its own and returns
// the reply. It is for a node that is not a member yet, so it has no cached client and no
// address in the committed state: the caller names the id whose key the node must present, and
// the connection is closed when the answer arrives.
func (n *Network) Ask(ctx context.Context, peer, addr string, ch transport.Channel, msg transport.Message) (*transport.Message, error) {
	// The identity dial declares no state range, whatever this build's is: the acceptor is
	// exempt from the range check so the asker can judge the reply itself, and a joiner on a
	// build from before the range would close a handshake carrying one without a word.
	hello := n.cfg.Hello
	if ch == transport.ChannelIdentity {
		hello = func() transport.Handshake {
			h := n.cfg.Hello()
			h.Min, h.Max, h.Writes = 0, 0, 0
			return h
		}
	}
	client := transport.NewChannelClient(addr, ch, hello)
	client.SetDialer(func(ctx context.Context, addr string) (net.Conn, error) { return n.dial(ctx, peer, ch, addr) })
	reply, err := client.Call(ctx, msg)
	n.noteClosedHandshake(ctx, peer, err)
	if closeErr := client.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		return nil, fmt.Errorf("asking %s at %s on the %s channel: %w", peer, addr, ch, err)
	}
	return reply, nil
}

// DialControl opens a control-channel connection to peer at addr the way every channel
// dials, for the control package, which keeps its own clients.
func (n *Network) DialControl(ctx context.Context, peer, addr string) (net.Conn, error) {
	return n.dial(ctx, peer, transport.ChannelControl, addr)
}

// Call sends msg to peer on channel ch and returns its reply. A failed call starts a
// backoff for that connection: later calls fail at once with ErrBackoff until it ends, so
// a dead peer costs no dial per frame.
func (n *Network) Call(ctx context.Context, peer string, ch transport.Channel, lane Lane, msg transport.Message) (*transport.Message, error) {
	if len(msg.Body) > n.cfg.MaxMessageBytes {
		return nil, fmt.Errorf("sending %d bytes to %s: %w (limit %d)", len(msg.Body), peer, ErrTooLarge, n.cfg.MaxMessageBytes)
	}
	entry, err := n.entry(peer, ch, lane)
	if err != nil {
		return nil, err
	}
	if wait, failures := n.backoffLeft(entry); wait > 0 {
		return nil, fmt.Errorf("calling %s on the %s channel: %w for another %s after %d failures", peer, ch, ErrBackoff, wait.Round(time.Millisecond), failures)
	}
	pooled, err := n.take(ctx, peer, ch, entry)
	if err != nil {
		return nil, err
	}
	reply, err := pooled.client.Call(ctx, msg)
	n.noteClosedHandshake(ctx, peer, err)
	// Back to the pool even after a failure: a client that lost its connection redials on its
	// next call, and this call is not repeated on any connection.
	pooled.last = time.Now()
	entry.idle <- pooled
	var refusal *transport.WireError
	switch {
	case err == nil || errors.As(err, &refusal):
		// An answer, even a refusal, proves the peer is alive.
		n.settle(entry, true)
	case ctx.Err() != nil:
		// The caller gave up; that says nothing about the peer.
	default:
		n.settle(entry, false)
	}
	if err != nil {
		return nil, fmt.Errorf("calling %s on the %s channel: %w", peer, ch, err)
	}
	return reply, nil
}

// take returns a connection of entry no call is using: an idle one, else a new one while the
// lane is under its bound, else the first to be returned. It waits only for the caller's
// context or the entry's retirement. A connection unused for longer than ConnMaxIdle is
// dropped first, so its next call dials instead of writing into a socket the peer has
// closed (the peer drops a silent connection after cluster.peer_idle_seconds); a write into
// such a socket fails and could only be reported as unknown, and is never retried here.
func (n *Network) take(ctx context.Context, peer string, ch transport.Channel, entry *clientEntry) (*pooledClient, error) {
	select {
	case pooled := <-entry.idle:
		return n.fresh(pooled), nil
	default:
	}
	n.mu.Lock()
	if entry.retired {
		n.mu.Unlock()
		return nil, fmt.Errorf("the %s channel client for %s is closed", ch, entry.addr)
	}
	if entry.made < entry.size {
		client := transport.NewChannelClient(entry.addr, ch, n.cfg.Hello)
		client.SetDialer(func(ctx context.Context, addr string) (net.Conn, error) { return n.dial(ctx, peer, ch, addr) })
		entry.made++
		entry.all = append(entry.all, client)
		n.mu.Unlock()
		return &pooledClient{client: client}, nil
	}
	n.mu.Unlock()
	select {
	case pooled := <-entry.idle:
		return n.fresh(pooled), nil
	case <-entry.done:
		return nil, fmt.Errorf("the %s channel client for %s is closed", ch, entry.addr)
	case <-ctx.Done():
		return nil, fmt.Errorf("waiting to call the %s channel of %s: %w", ch, entry.addr, context.Cause(ctx))
	}
}

// fresh drops the connection of a pooled client that has been unused past ConnMaxIdle.
func (n *Network) fresh(pooled *pooledClient) *pooledClient {
	if !pooled.last.IsZero() && time.Since(pooled.last) > n.cfg.ConnMaxIdle {
		// Best-effort: a failed close of a connection we are abandoning changes nothing; the
		// next call dials a new one either way.
		_ = pooled.client.DropConn()
	}
	return pooled
}

// retire ends an entry: later calls are refused, callers waiting for a connection wake, and
// the connections it opened are returned for the caller to close outside the lock. The
// caller holds n.mu.
func (e *clientEntry) retire() []*transport.ChannelClient {
	if e.retired {
		return nil
	}
	e.retired = true
	close(e.done)
	return e.all
}

func (n *Network) backoffLeft(entry *clientEntry) (time.Duration, int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return entry.retryAt.Sub(n.cfg.Now()), entry.failures
}

func (n *Network) settle(entry *clientEntry, ok bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if ok {
		entry.failures, entry.retryAt = 0, time.Time{}
		return
	}
	entry.failures++
	delay := n.cfg.BackoffBase << min(entry.failures-1, 16)
	if delay > n.cfg.BackoffMax || delay <= 0 {
		delay = n.cfg.BackoffMax
	}
	entry.retryAt = n.cfg.Now().Add(delay)
}

func (n *Network) entry(peer string, ch transport.Channel, lane Lane) (*clientEntry, error) {
	addr, ok := n.cfg.Addr(peer)
	if !ok {
		return nil, fmt.Errorf("member %q has no internode address; run 'nodes' to list the members", peer)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, fmt.Errorf("the peer network of %s is closed", n.cfg.Self)
	}
	key := clientKey{peer, ch, lane}
	if entry, ok := n.clients[key]; ok && entry.addr == addr {
		return entry, nil
	} else if ok {
		// Best-effort: the old connections are for an address the member left. Closing waits
		// for each client's turn, which its in-flight call gives up when the close interrupts it.
		for _, client := range entry.retire() {
			_ = client.Close()
		}
	}
	size := 1
	if lane == LaneForward {
		size = ForwardConns
	}
	entry := &clientEntry{addr: addr, size: size, idle: make(chan *pooledClient, size), done: make(chan struct{})}
	n.clients[key] = entry
	return entry, nil
}

// Forget closes every connection to peer, for a member that left or was replaced.
func (n *Network) Forget(peer string) error {
	n.mu.Lock()
	var gone []*transport.ChannelClient
	for key, entry := range n.clients {
		if key.peer == peer {
			gone = append(gone, entry.retire()...)
			delete(n.clients, key)
		}
	}
	n.mu.Unlock()
	var errs []error
	for _, client := range gone {
		errs = append(errs, client.Close())
	}
	return errors.Join(errs...)
}

// Close ends the listener, the servers and every outbound connection, and waits for
// their goroutines.
func (n *Network) Close() error {
	n.closeOnce.Do(func() {
		n.mu.Lock()
		n.closed = true
		var gone []*transport.ChannelClient
		for key, entry := range n.clients {
			gone = append(gone, entry.retire()...)
			delete(n.clients, key)
		}
		n.mu.Unlock()
		var errs []error
		for _, client := range gone {
			errs = append(errs, client.Close())
		}
		for _, server := range n.servers {
			errs = append(errs, server.Close())
		}
		errs = append(errs, n.mux.Close())
		n.closeErr = errors.Join(errs...)
	})
	return n.closeErr
}

// closedHandshakeEvery is the shortest gap between two hints about one peer.
const closedHandshakeEvery = time.Minute

// noteClosedHandshake logs, rate-limited per peer, a connection the peer closed right after this
// node wrote its handshake. An older build refuses a handshake that carries a state range and,
// on every channel but Raft, closes without a word, so the dialer is the only side that can say
// what is probably wrong (mixed-build spec 9.5).
func (n *Network) noteClosedHandshake(ctx context.Context, peer string, err error) {
	if err == nil || n.cfg.Warn == nil || ctx.Err() != nil {
		return
	}
	if !errors.Is(err, transport.ErrClosedAfterHandshake) {
		return
	}
	now := time.Now()
	if n.cfg.Now != nil {
		now = n.cfg.Now()
	}
	n.mu.Lock()
	if n.closedWarned == nil {
		n.closedWarned = map[string]time.Time{}
	}
	last, seen := n.closedWarned[peer]
	if seen && now.Sub(last) < closedHandshakeEvery {
		n.mu.Unlock()
		return
	}
	n.closedWarned[peer] = now
	n.mu.Unlock()
	n.cfg.Warn(fmt.Sprintf("peer %s closed the connection right after the handshake: it may run another build. Run hoarddb-server -v on it and on this node, and start both on the same build, or upgrade it to the cluster's version.", peer))
}
