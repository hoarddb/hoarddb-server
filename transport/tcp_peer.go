// Package transport — internode TCP peer client.
//
// The cluster's peer connections (GOSSIP, REPL_*, MIGRATE_*,
// CLUSTER_PROPOSE, VERSIONED_GET, MEMBERSHIP_CHANGE) use a pooled TCP client
// (the internode-tcp-build record; see the quic-removal-decision record for why
// this is now the only internode transport).
//
// Each socket is multiplexed (query-pipelining spec §3.6, QP-5): many
// requests are in flight on it at once, each response is matched to its
// request by the 16-byte request ID, and a caller's deadline ends only its own
// wait, never the socket. The sockets are split into two lanes, so client
// traffic forwarded between nodes cannot delay replication: socket 0 is the
// control lane (replication shipping, catch-up, gossip, liveness, membership,
// auth-config pushes) and sockets 1 to n−1 are the request lane (forwarded
// client reads and writes, consistent reads, migration). The caller names the
// lane, because the opcode cannot: a forwarded client frame, a catch-up batch
// and a replication ship are all REPL_PUSH.
//
// The pool keeps itself whole. A socket that fails fails the
// requests pending on it, and a replacement is dialed in the background into
// the same slot, so a failed control socket's replacement is the control lane
// again. If a replacement cannot be dialed, the whole pool goes out of service
// and every waiting and later SendRequest fails at once with
// ErrPeerClientFailed, which is the cluster's cue (peer.dial checks Failed)
// to open a fresh pool. No caller has to remember to drop the pool.
package transport

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// ErrPeerClientFailed is returned by TCPPeerClient.SendRequest once the pool
// is out of service: a broken socket could not be replaced, or the pool was
// closed. The pool serves no further requests; the caller opens a new one.
var ErrPeerClientFailed = errors.New("internode connection pool is out of service; the next dial to this peer opens a fresh pool")

// ErrNoRequestLane is returned at once by a request-lane SendRequest on a
// pool of one socket, which carries only the control lane (spec §3.6, OQ-14).
// No frame is written. Borrowing the control socket instead would put client
// traffic in front of replication, which is what the lanes exist to prevent.
var ErrNoRequestLane = errors.New("the internode pool has no request lane")

// errPeerClientClosed is the out-of-service cause recorded by Close.
var errPeerClientClosed = errors.New("the pool was closed")

// peerSocketDialTimeout bounds dialing, handshaking and authenticating one
// pooled socket, initial or replacement. A lane whose only socket is being
// replaced makes its senders wait at most this long before the pool either
// refills the slot or goes out of service.
const peerSocketDialTimeout = 10 * time.Second

// peerAuthTimeout bounds one pooled socket's AUTH exchange once it is
// connected. The internode listener closes a connection that has not
// authenticated within the same bound (peerPreAuthTimeout), so the two are
// one constant.
const peerAuthTimeout = 15 * time.Second

// peerWriteTimeout bounds writing one frame on a pooled socket (build doc
// SA-6). Deadlines are per request, never per socket, but a frame write still
// needs one: a peer that stops reading would otherwise hold the socket's write
// turn forever. It equals the listener's default frame timeout. Missing it
// fails the socket and its pending requests; a peer that has not read for
// 30 s is not answering them either.
const peerWriteTimeout = 30 * time.Second

// InternodePoolSize is the per-peer socket pool size for internode TCP
// connections: one control socket and three request sockets (spec §3.6,
// OC-3). The descriptor cost is unchanged from the lockstep pool
// (each socket costs one descriptor per peer).
const InternodePoolSize = 4

// PeerControlSocketFrames is the most frames the client keeps in flight on
// the control socket. Replication, gossip and probes are short and answered
// promptly, so 8 is enough there, and a larger figure would only let a
// stalled peer pile up more unanswered control frames.
const PeerControlSocketFrames = 8

// PeerRequestSocketFrames is the most frames the client keeps in flight on
// each request socket (spec §3.6, BUG-0723). A forwarded write holds its
// slot for the owner's whole cycle (log, fsync, apply, quorum), so the
// lane's capacity is the owner's concurrency for writes forwarded by this
// node: at 8 per socket a holder had at most 24 writes in flight per owner.
// Raising it adds no socket, descriptor or goroutine per peer; the listener's
// byte window (32 MiB per connection) is unchanged.
const PeerRequestSocketFrames = 32

// PeerPipelineFramesFloor is the least the internode listener reads ahead on
// a connection (ServeTCP's PeerMode floor). The listener cannot tell a
// control socket from a request socket, so the floor is the larger of the
// two lanes' per-socket bounds, and the client can never have more frames
// outstanding on any socket than the listener will read.
const PeerPipelineFramesFloor = max(PeerControlSocketFrames, PeerRequestSocketFrames)

// socketFrames is the most frames the client keeps in flight on one socket
// of the lane.
func (l Lane) socketFrames() int {
	if l == LaneControl {
		return PeerControlSocketFrames
	}
	return PeerRequestSocketFrames
}

// Lane is which of a pool's sockets a request may use (spec §3.6).
type Lane uint8

const (
	// LaneControl is socket 0: replication shipping, catch-up, gossip,
	// liveness probes, membership votes and auth-config pushes.
	LaneControl Lane = iota
	// LaneRequest is sockets 1 to n−1: forwarded client reads and writes,
	// consistent reads and migration.
	LaneRequest
)

// String names the lane as the internode in-flight gauge labels it.
func (l Lane) String() string {
	if l == LaneControl {
		return "control"
	}
	return "request"
}

// InternodePeerAddr derives a member's internode TCP address from its ring
// address (port+1): a node listening on 0.0.0.0:4433 is reachable for
// internode traffic on port 4434. The derivation is applied to the
// *member's* Addr, never the local listen address — the local config has
// one port, the ring has many. It is also used at Start() to derive a
// node's own internode listen address from its own Server.Listen when no
// explicit override is given; port 0 ("pick any free port", used by tests)
// is passed through unchanged rather than offset, since "ephemeral port
// plus one" names nothing — a real deployment always configures a
// concrete port, so this case only matters for ephemeral/test binding.
func InternodePeerAddr(memberAddr string) (string, error) {
	host, port, err := net.SplitHostPort(memberAddr)
	if err != nil {
		return "", fmt.Errorf("internode addr from %q: %w", memberAddr, err)
	}
	var portNum int
	if _, err := fmt.Sscanf(port, "%d", &portNum); err != nil {
		return "", fmt.Errorf("internode addr from %q: port %q is not a number: %w", memberAddr, port, err)
	}
	if portNum == 0 {
		return net.JoinHostPort(host, "0"), nil
	}
	return net.JoinHostPort(host, fmt.Sprintf("%d", portNum+1)), nil
}

// TCPPeerClient is a pool of authenticated, multiplexed TLS sockets to one
// peer, split into a control lane and a request lane.
//
// Invariants, while the pool is in service: slot i holds socket i or is nil
// while that socket's replacement is being dialed; every open socket is in
// its slot and in open; every socket has one reader goroutine. Once the pool
// leaves service, a socket with nothing pending closes at once and one with
// requests pending closes when its last response arrives, so a failed pool
// that nobody Closes (peer.installClient drops it deliberately) still ends
// every goroutine and descriptor it holds.
//
// Lock order: poolMu, then a socket's mu. Nothing takes poolMu while holding
// a socket's mu. changeMu is a leaf.
type TCPPeerClient struct {
	// outOfService is closed when the pool stops serving, which wakes every
	// SendRequest waiting for a slot.
	outOfService chan struct{}
	// peerFingerprint is the canonical fingerprint of the certificate the peer
	// presented on the first socket. It is the value a caller records as the
	// peer's identity, learned from the verified connection rather than from a
	// self-asserted request field.
	peerFingerprint string
	target          peerDialTarget

	// redialCtx is cancelled when the pool leaves service, so a replacement
	// still dialing stops at once; redials lets Close wait for those dials.
	redialCtx     context.Context
	cancelRedials context.CancelFunc
	redials       sync.WaitGroup
	// readers counts the sockets' reader goroutines, so Close can wait for
	// them. Added to under poolMu while in service, like redials.
	readers sync.WaitGroup

	// observer, when set, is told of every slot taken (+1) and released (−1).
	observer atomic.Pointer[func(lane Lane, delta int)]

	poolMu sync.Mutex
	// failure is why the pool left service; nil while it serves.
	failure error
	// slots is indexed by socket: 0 is the control lane, 1..n−1 the request
	// lane. Its length is the pool size and never changes.
	slots []*muxSocket
	// generation counts the sockets installed over the pool's life, the first
	// ones included, so it is never zero and it changes whenever a socket is
	// re-established (Generation).
	generation uint64
	// open holds every socket not yet closed and accounted for, in a slot or
	// retiring, so Close can close one whatever state it is in.
	open map[*muxSocket]struct{}

	// changed is closed and replaced whenever a sender waiting for a slot
	// could now find one: a response released a slot, a socket was installed
	// or failed, or the pool left service.
	changeMu sync.Mutex
	changed  chan struct{}
}

// peerDialTarget is what opening one pooled socket needs: where the peer is,
// how to verify it, and the cluster token to authenticate with. The pool
// keeps it so a replacement socket reaches the same peer the same way.
type peerDialTarget struct {
	addr   string
	tlsCfg *tls.Config
	token  string
}

// poolSocket is one authenticated connection and the buffered reader over
// it. They are one unit: the reader holds bytes read ahead from exactly this
// connection, so reading through any other reader would lose them or cross
// two sockets' responses (the reason for TestTCPPeerPoolPairsSocketWithReader).
type poolSocket struct {
	conn *tls.Conn
	br   *bufio.Reader
}

// muxSocket is one multiplexed socket of the pool. Only its reader goroutine
// reads from br. Writers take writeTurn, a channel of capacity 1 rather than a
// mutex so a waiting writer can give up when its context ends, and a frame is
// therefore never started by a caller who has gone.
type muxSocket struct {
	poolSocket
	index     int
	writeTurn chan struct{}

	mu sync.Mutex
	// pending is every request written, or about to be, whose response has
	// not been read. Its size is the socket's in-flight count, bounded by
	// its lane's socketFrames: a slot is taken by adding a request and
	// released by removing it.
	pending map[[RequestIDLen]byte]*pendingRequest
	// failed is set once, when the socket fails or is closed; a failed socket
	// takes no new request.
	failed bool
	// retiring is set when the pool leaves service: the socket takes no new
	// request and closes once pending is empty.
	retiring bool
}

func (ms *muxSocket) lane() Lane {
	if ms.index == 0 {
		return LaneControl
	}
	return LaneRequest
}

// pendingRequest is one request awaiting its response. outcome has capacity 1
// and receives exactly one value, from whoever removes the request from its
// socket's pending map, so delivering never blocks: a caller that gave up is
// simply not reading.
type pendingRequest struct {
	outcome chan requestOutcome
}

type requestOutcome struct {
	response *Frame
	err      error
}

// NewTCPPeerClient opens a pool of NodeUser-authenticated TLS sockets to
// addr. Each socket writes an OpAuth frame with the cluster token before
// entering the pool — the same first-frame contract a QUIC session honours
// per connection. A pool of one socket carries only the control lane.
func NewTCPPeerClient(ctx context.Context, addr string, tlsCfg *tls.Config, token string, size int) (*TCPPeerClient, error) {
	if size < 1 {
		size = 1
	}
	redialCtx, cancelRedials := context.WithCancel(context.Background())
	client := &TCPPeerClient{
		outOfService:  make(chan struct{}),
		target:        peerDialTarget{addr: addr, tlsCfg: tlsCfg, token: token},
		redialCtx:     redialCtx,
		cancelRedials: cancelRedials,
		slots:         make([]*muxSocket, size),
		open:          make(map[*muxSocket]struct{}, size),
		changed:       make(chan struct{}),
	}
	for i := 0; i < size; i++ {
		ps, err := dialPoolSocket(ctx, client.target)
		if err != nil {
			_ = client.Close() // best-effort: the dial error is the one worth returning
			return nil, err
		}
		if i == 0 {
			client.peerFingerprint = peerCertFingerprint(ps.conn.ConnectionState())
		}
		client.poolMu.Lock()
		client.installLocked(i, ps)
		client.poolMu.Unlock()
	}
	return client, nil
}

// dialPoolSocket opens, handshakes and authenticates one pooled socket. ctx
// bounds all three: the TLS dialer observes it, and closing the connection
// when it ends interrupts an authentication exchange the peer never answers.
func dialPoolSocket(ctx context.Context, target peerDialTarget) (*poolSocket, error) {
	dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: peerSocketDialTimeout}, Config: target.tlsCfg}
	netConn, err := dialer.DialContext(ctx, "tcp", target.addr)
	if err != nil {
		return nil, fmt.Errorf("dial peer %s: %w", target.addr, err)
	}
	// tls.Dialer.DialContext documents that the Conn it returns is always a
	// *tls.Conn.
	conn := netConn.(*tls.Conn)
	stopInterrupt := context.AfterFunc(ctx, func() { _ = conn.Close() })

	ps := &poolSocket{conn: conn, br: bufio.NewReader(conn)}
	_ = conn.SetDeadline(time.Now().Add(peerAuthTimeout))
	authErr := authPoolSocket(ps, target.token)
	// A false stop means ctx ended and the interrupt has closed, or is
	// closing, the socket — even if authentication had already succeeded —
	// and the context's reason says more than the read error it caused.
	if !stopInterrupt() {
		_ = conn.Close() // best-effort: already closed or closing
		return nil, fmt.Errorf("authenticate peer %s: %w", target.addr, context.Cause(ctx))
	}
	if authErr != nil {
		_ = conn.Close() // best-effort: the socket is discarded, the auth error is returned
		return nil, fmt.Errorf("authenticate peer %s: %w", target.addr, authErr)
	}
	_ = conn.SetDeadline(time.Time{})
	return ps, nil
}

// PeerFingerprint returns the canonical fingerprint of the certificate the
// peer presented, or "" when no certificate was available. A caller records
// this observed value for the peer, so the recorded identity comes from the
// verified connection rather than from the request that described it.
func (c *TCPPeerClient) PeerFingerprint() string { return c.peerFingerprint }

// peerCertFingerprint derives the canonical fingerprint from a completed TLS
// handshake's peer certificates. A non-ed25519 or absent certificate yields
// the empty string; the pinning callback has already refused those cases, so
// this is only a read of what was accepted.
func peerCertFingerprint(state tls.ConnectionState) string {
	if len(state.PeerCertificates) == 0 {
		return ""
	}
	pub, ok := state.PeerCertificates[0].PublicKey.(ed25519.PublicKey)
	if !ok {
		return ""
	}
	return Fingerprint(pub)
}

// authPoolSocket writes OpAuth with the NodeUser credentials and requires
// OpAuthOK before the socket is usable. The server's auth handler remains
// the authority on the credential; this only prepares the frame. __node__
// is server.NodeUser's value — mirroring it here would import a cycle, so
// it is stated with a comment instead (and the two values are asserted
// equal by a test).
//
// It reads through the pooled socket's own reader rather than a fresh one: a
// reader discarded here would take any bytes the peer sent after OpAuthOK with
// it, and this socket's later requests would then block on a response that had
// already been buffered and thrown away.
func authPoolSocket(ps *poolSocket, token string) error {
	payload, err := bson.Marshal(map[string]string{
		"username": "__node__",
		"password": token,
	})
	if err != nil {
		return fmt.Errorf("marshal auth: %w", err)
	}
	if _, err := ps.conn.Write(NewFrame(OpAuth, payload).Encode()); err != nil {
		return fmt.Errorf("write auth: %w", err)
	}
	resp, err := DecodeFrame(ps.br)
	if err != nil {
		return fmt.Errorf("read auth response: %w", err)
	}
	if resp.Opcode != OpAuthOK {
		return fmt.Errorf("auth rejected with %s", resp.Opcode)
	}
	return nil
}

// SetInFlightObserver installs fn, which is called with +1 each time a
// request takes a slot on a socket of lane and −1 each time a slot is
// released: when its response is read, or the socket fails, or a request
// withdraws before its frame was written. It must be cheap and must not call
// back into the pool.
func (c *TCPPeerClient) SetInFlightObserver(fn func(lane Lane, delta int)) {
	c.observer.Store(&fn)
}

func (c *TCPPeerClient) observe(lane Lane, delta int) {
	if fn := c.observer.Load(); fn != nil {
		(*fn)(lane, delta)
	}
}

// SendRequest writes one frame on a socket of lane and waits for its
// response.
//
// It takes a slot on the socket of that lane with the fewest requests in
// flight, waiting while every one is full or being redialed; the wait ends
// with the context's error if ctx ends first, or with ErrPeerClientFailed if
// the pool leaves service. A control-lane send never borrows a request socket
// and a request-lane send never borrows the control socket. On a pool of one
// socket a request-lane send fails at once with ErrNoRequestLane.
//
// ctx bounds only this request's waits, never the socket: when it ends,
// SendRequest returns, but a frame already written keeps its slot until the
// peer's response to it is read (spec §3.6), because the peer will still
// answer it and the client must never have more frames outstanding than the
// peer reads ahead. A failed socket fails every request pending on it and is
// replaced by the pool, so the caller owes the pool nothing.
func (c *TCPPeerClient) SendRequest(ctx context.Context, lane Lane, op Opcode, payload []byte) (*Frame, error) {
	return c.send(ctx, lane, op, payload, 0)
}

// ErrPeerSocketsChanged is SendRequestIfUnchanged's refusal: a socket of the
// pool was re-established after the generation the caller named.
var ErrPeerSocketsChanged = errors.New("a socket to the peer was re-established since the caller last confirmed what is at the other end")

// Generation returns a number that changes every time the pool installs a
// socket, so a caller can tell whether every socket it may be given is one
// that existed when it last asked the peer something. The pool replaces a
// broken socket by dialing the same address again, and what answers there may
// be a different process than before: a caller that learnt something about the
// peer over the old sockets reads Generation first and sends with
// SendRequestIfUnchanged.
func (c *TCPPeerClient) Generation() uint64 {
	c.poolMu.Lock()
	defer c.poolMu.Unlock()
	return c.generation
}

// SendRequestIfUnchanged is SendRequest, refused with ErrPeerSocketsChanged
// unless the pool's generation is still generation when the request takes its
// slot. The socket it is written to was therefore installed no later than the
// moment the caller read that generation.
func (c *TCPPeerClient) SendRequestIfUnchanged(ctx context.Context, lane Lane, op Opcode, payload []byte, generation uint64) (*Frame, error) {
	return c.send(ctx, lane, op, payload, generation)
}

// send is SendRequest; a non-zero generation makes it conditional
// (SendRequestIfUnchanged).
func (c *TCPPeerClient) send(ctx context.Context, lane Lane, op Opcode, payload []byte, generation uint64) (*Frame, error) {
	if lane == LaneRequest && len(c.slots) == 1 {
		return nil, fmt.Errorf("%w: pool of %d socket(s) to %s has only the control lane", ErrNoRequestLane, len(c.slots), c.target.addr)
	}
	frame := NewFrame(op, payload)
	request := &pendingRequest{outcome: make(chan requestOutcome, 1)}
	ms, err := c.acquire(ctx, lane, frame.RequestID, request, generation)
	if err != nil {
		return nil, err
	}
	if err := c.writeFrame(ctx, ms, frame, request); err != nil {
		return nil, err
	}
	select {
	case outcome := <-request.outcome:
		return outcome.response, outcome.err
	case <-ctx.Done():
		return nil, fmt.Errorf("waiting for the %s response from peer %s: %w", op, c.target.addr, ctx.Err())
	}
}

// acquire waits for a slot on a live socket of lane and registers request on
// it under id. An already-ended ctx is refused before the first look, which a
// select would otherwise pick at random against a free slot.
func (c *TCPPeerClient) acquire(ctx context.Context, lane Lane, id [RequestIDLen]byte, request *pendingRequest, generation uint64) (*muxSocket, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("waiting for a %s-lane slot to peer %s: %w", lane, c.target.addr, err)
		}
		// Taken before looking, so a change that happens after the look
		// has already closed this channel and the wait below returns at once.
		changed := c.changeSignal()
		ms, err := c.reserve(lane, id, request, generation)
		if err != nil || ms != nil {
			return ms, err
		}
		select {
		case <-changed:
		case <-c.outOfService:
			return nil, c.outOfServiceError()
		case <-ctx.Done():
			return nil, fmt.Errorf("waiting for a %s-lane slot to peer %s: %w", lane, c.target.addr, ctx.Err())
		}
	}
}

// reserve registers request on the live socket of lane with the fewest in
// flight, ties to the lowest index, or returns nil when every socket of the
// lane is full, failed or being redialed.
//
// A non-zero generation that is no longer the pool's refuses the request:
// installLocked raises the generation under the same poolMu, so every socket
// this can choose was installed at or before the generation the caller named.
func (c *TCPPeerClient) reserve(lane Lane, id [RequestIDLen]byte, request *pendingRequest, generation uint64) (*muxSocket, error) {
	c.poolMu.Lock()
	defer c.poolMu.Unlock()
	if c.failure != nil {
		return nil, c.outOfServiceErrorLocked()
	}
	if generation != 0 && generation != c.generation {
		return nil, fmt.Errorf("%w (peer %s)", ErrPeerSocketsChanged, c.target.addr)
	}
	var chosen *muxSocket
	chosenLoad := lane.socketFrames()
	for _, ms := range c.laneSlots(lane) {
		if ms == nil {
			continue
		}
		ms.mu.Lock()
		load, live := len(ms.pending), !ms.failed
		ms.mu.Unlock()
		if live && load < chosenLoad {
			chosen, chosenLoad = ms, load
		}
	}
	if chosen == nil {
		return nil, nil
	}
	chosen.mu.Lock()
	defer chosen.mu.Unlock()
	// Only a reader failing the socket can have changed it since the look
	// above (a new request needs poolMu, which this holds), and the failure
	// signals a change, so the caller simply looks again.
	if chosen.failed {
		return nil, nil
	}
	if _, duplicate := chosen.pending[id]; duplicate {
		return nil, fmt.Errorf("request ID %x is already pending on a socket to peer %s; refusing a second request with it", id, c.target.addr)
	}
	chosen.pending[id] = request
	// Observed under the socket's mu, so a failure that releases this slot
	// is always observed after it was taken.
	c.observe(chosen.lane(), 1)
	return chosen, nil
}

func (c *TCPPeerClient) laneSlots(lane Lane) []*muxSocket {
	if lane == LaneControl {
		return c.slots[:1]
	}
	return c.slots[1:]
}

// writeFrame takes the socket's write turn and writes frame. Waiting for the
// turn observes ctx, so a caller who has gone never starts a frame; a frame
// that has started is finished, or the socket fails.
func (c *TCPPeerClient) writeFrame(ctx context.Context, ms *muxSocket, frame *Frame, request *pendingRequest) error {
	select {
	case ms.writeTurn <- struct{}{}:
	case outcome := <-request.outcome:
		// The socket failed while this request waited for the turn.
		return outcome.err
	case <-ctx.Done():
		c.withdraw(ms, frame.RequestID)
		return fmt.Errorf("waiting to write %s to peer %s: %w", frame.Opcode, c.target.addr, ctx.Err())
	}
	defer func() { <-ms.writeTurn }()
	_ = ms.conn.SetWriteDeadline(time.Now().Add(peerWriteTimeout))
	if _, err := ms.conn.Write(frame.Encode()); err != nil {
		writeErr := fmt.Errorf("write %s to peer %s: %w", frame.Opcode, c.target.addr, err)
		c.failSocket(ms, writeErr)
		return writeErr
	}
	return nil
}

// withdraw releases the slot of a request whose frame was never written, so
// no response will come for it.
func (c *TCPPeerClient) withdraw(ms *muxSocket, id [RequestIDLen]byte) {
	ms.mu.Lock()
	_, stillPending := ms.pending[id]
	delete(ms.pending, id)
	drained := ms.retiring && len(ms.pending) == 0
	ms.mu.Unlock()
	if !stillPending {
		return // the socket failed and released it already
	}
	c.observe(ms.lane(), -1)
	c.signal()
	if drained {
		_ = ms.conn.Close() // its reader sees the close, accounts for the socket and exits
	}
}

// readResponses is a socket's reader goroutine. It delivers each response to
// the request with its ID, releasing that request's slot. It exits when the
// socket fails or closes: a read error, an answer to no pending request, or
// the last response of a retiring socket.
func (c *TCPPeerClient) readResponses(ms *muxSocket) {
	defer c.readers.Done()
	for {
		response, err := DecodeFrame(ms.br)
		if err != nil {
			c.failSocket(ms, fmt.Errorf("read response from peer %s: %w", c.target.addr, err))
			return
		}
		ms.mu.Lock()
		request, known := ms.pending[response.RequestID]
		delete(ms.pending, response.RequestID)
		drained := ms.retiring && len(ms.pending) == 0
		ms.mu.Unlock()
		if !known {
			c.failSocket(ms, fmt.Errorf("peer %s sent a %s response to request %x, which is not pending on this socket: "+
				"the socket's framing can no longer be trusted, so it is closed and replaced", c.target.addr, response.Opcode, response.RequestID))
			return
		}
		request.outcome <- requestOutcome{response: response}
		c.observe(ms.lane(), -1)
		c.signal()
		if drained {
			_ = ms.conn.Close() // the next read fails and accounts for the socket
		}
	}
}

// failSocket fails a socket once: it takes the socket out of use, closes it,
// fails every request pending on it with cause, and hands its slot to
// replaceSocket.
func (c *TCPPeerClient) failSocket(ms *muxSocket, cause error) {
	failedRequests, first := ms.markFailed()
	if !first {
		return
	}
	_ = ms.conn.Close() // best-effort: the socket already failed; cause says why
	c.failRequests(ms, failedRequests, cause)
}

// markFailed marks the socket failed and takes what was pending on it. It
// reports false if the socket had failed already, in which case whoever failed
// it first owns the rest of the failure.
func (ms *muxSocket) markFailed() (map[[RequestIDLen]byte]*pendingRequest, bool) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.failed {
		return nil, false
	}
	ms.failed = true
	failedRequests := ms.pending
	ms.pending = map[[RequestIDLen]byte]*pendingRequest{}
	return failedRequests, true
}

// failRequests fails the requests taken from a failed socket and hands its
// slot to replaceSocket. It runs without the socket's mu, because
// replaceSocket takes poolMu (lock order).
func (c *TCPPeerClient) failRequests(ms *muxSocket, failedRequests map[[RequestIDLen]byte]*pendingRequest, cause error) {
	for _, request := range failedRequests {
		request.outcome <- requestOutcome{err: cause}
		c.observe(ms.lane(), -1)
	}
	c.signal()
	c.replaceSocket(ms)
}

// replaceSocket forgets a failed socket and, while the pool is in service,
// starts dialing its replacement into the same slot. The requests on the
// pool's other sockets are untouched, which is why the pool heals one socket
// rather than being dropped whole: dropping it would fail their in-flight
// requests as collateral damage.
func (c *TCPPeerClient) replaceSocket(broken *muxSocket) {
	c.poolMu.Lock()
	defer c.poolMu.Unlock()
	delete(c.open, broken)
	c.slots[broken.index] = nil
	if c.failure != nil {
		return
	}
	// Add happens under poolMu while the pool is in service, and Close marks
	// it out of service under poolMu before it Waits, so no Add races the Wait.
	c.redials.Add(1)
	go c.redialSocket(broken.index)
}

// redialSocket dials the replacement for slot index. A failed dial takes the
// pool out of service rather than leaving a lane a socket short, so waiters
// get an error instead of waiting on a socket nothing will install.
func (c *TCPPeerClient) redialSocket(index int) {
	defer c.redials.Done()
	dialCtx, cancel := context.WithTimeout(c.redialCtx, peerSocketDialTimeout)
	defer cancel()
	ps, err := dialPoolSocket(dialCtx, c.target)

	c.poolMu.Lock()
	defer c.poolMu.Unlock()
	if err != nil {
		c.leaveServiceLocked(fmt.Errorf("replacing a failed socket: %w", err))
		return
	}
	if c.failure != nil {
		_ = ps.conn.Close() // best-effort: the pool left service while this dialed
		return
	}
	c.installLocked(index, ps)
}

// installLocked puts a freshly dialed socket in slot index and starts its
// reader. The caller holds poolMu and the pool is in service.
func (c *TCPPeerClient) installLocked(index int, ps *poolSocket) {
	ms := &muxSocket{
		poolSocket: *ps,
		index:      index,
		writeTurn:  make(chan struct{}, 1),
	}
	ms.pending = make(map[[RequestIDLen]byte]*pendingRequest, ms.lane().socketFrames())
	c.slots[index] = ms
	c.generation++
	c.open[ms] = struct{}{}
	c.readers.Add(1)
	go c.readResponses(ms)
	c.signal()
}

// leaveServiceLocked takes the pool out of service with cause, once: it wakes
// every waiter, stops replacements still dialing, closes the sockets with
// nothing pending, and marks the rest retiring, to close when their last
// response arrives. The caller holds poolMu.
func (c *TCPPeerClient) leaveServiceLocked(cause error) {
	if c.failure != nil {
		return
	}
	c.failure = cause
	close(c.outOfService)
	c.cancelRedials()
	for ms := range c.open {
		ms.mu.Lock()
		ms.retiring = true
		idle := len(ms.pending) == 0
		ms.mu.Unlock()
		if idle {
			_ = ms.conn.Close() // its reader sees the close, accounts for the socket and exits
		}
	}
	c.signal()
}

// changeSignal returns the channel the next change closes.
func (c *TCPPeerClient) changeSignal() <-chan struct{} {
	c.changeMu.Lock()
	defer c.changeMu.Unlock()
	return c.changed
}

// signal wakes every sender waiting for a slot, to look again.
func (c *TCPPeerClient) signal() {
	c.changeMu.Lock()
	close(c.changed)
	c.changed = make(chan struct{})
	c.changeMu.Unlock()
}

func (c *TCPPeerClient) outOfServiceError() error {
	c.poolMu.Lock()
	defer c.poolMu.Unlock()
	return c.outOfServiceErrorLocked()
}

func (c *TCPPeerClient) outOfServiceErrorLocked() error {
	return fmt.Errorf("%w (peer %s): %w", ErrPeerClientFailed, c.target.addr, c.failure)
}

// Failed reports whether the pool is out of service: a failed socket could
// not be replaced, or the pool was closed. A failed pool answers every
// SendRequest with ErrPeerClientFailed, so its owner redials instead of
// reusing it.
func (c *TCPPeerClient) Failed() bool {
	select {
	case <-c.outOfService:
		return true
	default:
		return false
	}
}

// Close takes the pool out of service, waits for any replacement dial to
// stop, closes every open socket — retiring ones with requests still pending
// included, whose requests then fail with ErrPeerClientFailed — and waits for
// every reader goroutine to exit. A socket the pool closed itself is not
// reported as a Close error.
func (c *TCPPeerClient) Close() error {
	c.poolMu.Lock()
	c.leaveServiceLocked(errPeerClientClosed)
	openSockets := make([]*muxSocket, 0, len(c.open))
	for ms := range c.open {
		openSockets = append(openSockets, ms)
	}
	closedErr := c.outOfServiceErrorLocked()
	c.poolMu.Unlock()
	c.redials.Wait()

	// Each socket is marked failed before it is closed, so its reader, woken
	// by the close, finds the failure already owned and its pending requests
	// fail with ErrPeerClientFailed rather than with the read error the close
	// causes.
	var firstErr error
	for _, ms := range openSockets {
		failedRequests, first := ms.markFailed()
		if err := ms.conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) && firstErr == nil {
			firstErr = err
		}
		if first {
			c.failRequests(ms, failedRequests, closedErr)
		}
	}
	c.readers.Wait()
	return firstErr
}

// The handshake (conventions section 6.2) is the 60-byte HDPX header and the
// sender's node id. Every field is big-endian:
//
//	[0:4]   "HDPX"        [4] major     [5] channel     [6] min level   [7] writes level
//	[8:24]  cluster_id    [24:32] epoch [32:40] state_seq
//	[40:48] sent_at_ns    [48:50] nodeIDLen
//	[50:58] incarnation   [58] max level  [59] reserved (0)    [60:] node id
//
// Bytes 6, 7 and 58 are the sender's state-level range (Handshake.Min, Writes and Max; the
// mixed-build spec section 9.1). They were the flags and reserved bytes, which no build ever
// set, so a build from before the range refuses a non-zero one. Level 0 in all three means a
// sender that declares no range. Byte 59 is the one byte left, zero.
const (
	// HandshakeBytes is the fixed part of the handshake; the node id follows.
	HandshakeBytes = 60
	handshakeMagic = "HDPX"
	// maxHandshakeNodeID bounds the node id so a hostile peer cannot make a
	// listener read an arbitrary length before it has authenticated.
	maxHandshakeNodeID = 255
)

// The five channels of conventions section 6, numbered in the order of its
// table. The spec names the channels and never numbers them; these values are
// this build's and ride in byte 5.
const (
	HandshakeChannelData    byte = 1
	HandshakeChannelControl byte = 2
	HandshakeChannelSeed    byte = 3
	HandshakeChannelRaft    byte = 4
	// HandshakeChannelIdentity carries the one question a leader asks a node it is about
	// to add: who are you (membership node-side plan section 1).
	HandshakeChannelIdentity byte = 5
)

// ErrStaleIncarnation is the refusal at the handshake: the peer is a stopped
// process speaking for an identity that has since been replaced.
var ErrStaleIncarnation = errors.New("stale incarnation")

// Handshake is the decoded header of a node-to-node connection.
type Handshake struct {
	Channel     byte
	ClusterID   [16]byte
	Epoch       uint64
	StateSeq    uint64
	SentAtNs    int64
	Incarnation uint64
	NodeID      string
	// Min, Max and Writes are the sender's state-level range: the lowest level it acts at, the
	// highest it can apply, and the level it emits now. All zero declares no range (an identity
	// dial, or a build from before the check). transport cannot import cluster, so the callers
	// that know the build's range set them.
	Min, Max, Writes uint8
	// PeerFingerprint is the SHA256 fingerprint of the ed25519 certificate the peer
	// authenticated its TLS connection with, filled in by the server side after the
	// handshake is read; it is never on the wire. Empty when the connection is not TLS.
	PeerFingerprint string
}

func handshakeRefusal(format string, args ...any) error {
	return fmt.Errorf("handshake refused: "+format, args...)
}

// validStateRange is nil for a range that is all zeros (none declared) or 1 <= min <= writes <= max.
// Encode and DecodeHandshake both apply it: a malformed range admitted on either side would let a
// lie about the range pass the overlap check.
func validStateRange(low, writes, high uint8) error {
	if low|high|writes == 0 || (low >= 1 && low <= writes && writes <= high) {
		return nil
	}
	return handshakeRefusal("the state range min %d, max %d, writes %d is malformed: it is all zeros or 1 <= min <= writes <= max", low, high, writes)
}

// Encode writes the header and node id. It refuses an unknown channel, a node id that is empty or
// longer than 255 bytes, and a malformed state range.
func (h Handshake) Encode() ([]byte, error) {
	if h.Channel < HandshakeChannelData || h.Channel > HandshakeChannelIdentity {
		return nil, handshakeRefusal("channel %d is not one of 1 data, 2 control, 3 seed, 4 raft, 5 identity", h.Channel)
	}
	if len(h.NodeID) == 0 || len(h.NodeID) > maxHandshakeNodeID {
		return nil, handshakeRefusal("node id is %d bytes: it must be 1 to %d", len(h.NodeID), maxHandshakeNodeID)
	}
	if err := validStateRange(h.Min, h.Writes, h.Max); err != nil {
		return nil, err
	}
	raw := make([]byte, HandshakeBytes, HandshakeBytes+len(h.NodeID))
	copy(raw, handshakeMagic)
	raw[4], raw[5] = ProtocolMajor, h.Channel
	raw[6], raw[7], raw[58] = h.Min, h.Writes, h.Max
	copy(raw[8:24], h.ClusterID[:])
	binary.BigEndian.PutUint64(raw[24:], h.Epoch)
	binary.BigEndian.PutUint64(raw[32:], h.StateSeq)
	binary.BigEndian.PutUint64(raw[40:], uint64(h.SentAtNs))
	binary.BigEndian.PutUint16(raw[48:], uint16(len(h.NodeID)))
	binary.BigEndian.PutUint64(raw[50:], h.Incarnation)
	return append(raw, h.NodeID...), nil
}

// DecodeHandshake reads exactly the bytes Encode writes and refuses anything
// else, including a set reserved byte (a future field must not be silently
// ignored). It never panics, and what it accepts encodes back to the same
// bytes.
func DecodeHandshake(raw []byte) (Handshake, error) {
	if len(raw) < HandshakeBytes {
		return Handshake{}, handshakeRefusal("%d bytes is shorter than the %d-byte header", len(raw), HandshakeBytes)
	}
	if string(raw[:4]) != handshakeMagic {
		return Handshake{}, handshakeRefusal("it does not start with %q, so the peer is not a hoardDB node", handshakeMagic)
	}
	if raw[4] != ProtocolMajor {
		return Handshake{}, handshakeRefusal("peer speaks major version %d, this node speaks %d", raw[4], ProtocolMajor)
	}
	if raw[59] != 0 {
		return Handshake{}, handshakeRefusal("the reserved byte is set: this build defines none")
	}
	if err := validStateRange(raw[6], raw[7], raw[58]); err != nil {
		return Handshake{}, err
	}
	idLen := int(binary.BigEndian.Uint16(raw[48:]))
	if len(raw) != HandshakeBytes+idLen {
		return Handshake{}, handshakeRefusal("header announces a %d-byte node id but %d bytes follow it", idLen, len(raw)-HandshakeBytes)
	}
	h := Handshake{
		Channel:     raw[5],
		Epoch:       binary.BigEndian.Uint64(raw[24:]),
		StateSeq:    binary.BigEndian.Uint64(raw[32:]),
		SentAtNs:    int64(binary.BigEndian.Uint64(raw[40:])),
		Incarnation: binary.BigEndian.Uint64(raw[50:]),
		NodeID:      string(raw[HandshakeBytes:]),
		Min:         raw[6], Writes: raw[7], Max: raw[58],
	}
	copy(h.ClusterID[:], raw[8:24])
	if _, err := h.Encode(); err != nil {
		return Handshake{}, err
	}
	return h, nil
}

// ReadHandshake reads one handshake from r: the fixed header, then the node
// id it announces.
func ReadHandshake(r io.Reader) (Handshake, error) {
	raw := make([]byte, HandshakeBytes, HandshakeBytes+maxHandshakeNodeID)
	if _, err := io.ReadFull(r, raw); err != nil {
		return Handshake{}, fmt.Errorf("read handshake header: %w", err)
	}
	idLen := int(binary.BigEndian.Uint16(raw[48:]))
	if idLen > maxHandshakeNodeID {
		return Handshake{}, handshakeRefusal("header announces a %d-byte node id; the limit is %d", idLen, maxHandshakeNodeID)
	}
	raw = raw[:HandshakeBytes+idLen]
	if _, err := io.ReadFull(r, raw[HandshakeBytes:]); err != nil {
		return Handshake{}, fmt.Errorf("read handshake node id: %w", err)
	}
	return DecodeHandshake(raw)
}

// CheckIncarnation refuses a peer whose incarnation is older than the one the
// cluster committed for its id: it is a stopped process speaking for a
// replaced identity. An equal or newer one is accepted, because a replacement
// can connect before this node has applied the commit that names it.
func (h Handshake) CheckIncarnation(committed uint64) error {
	if h.Incarnation < committed {
		return fmt.Errorf("%w: refused %s connection from %s: its incarnation %d is older than the committed %d, so it is a stopped process speaking for a replaced identity",
			ErrStaleIncarnation, channelWord(h.Channel), h.NodeID, h.Incarnation, committed)
	}
	return nil
}

func channelWord(channel byte) string {
	switch channel {
	case HandshakeChannelData:
		return "data"
	case HandshakeChannelControl:
		return "control"
	case HandshakeChannelSeed:
		return "seed"
	case HandshakeChannelRaft:
		return "raft"
	case HandshakeChannelIdentity:
		return "identity"
	}
	return "unknown"
}
