// Package transport — TCP driver listener.
//
// This file implements the server side of hoardDB's TCP+TLS transport
// (the tcp-protocol-spec record): a listener for language drivers over
// plain stdlib sockets. It reuses the exact frame codec (DecodeFrame/
// WriteFrame) and the single shared auth gate (ServeFrame) so the driver
// and internode listeners can never drift on the security model.
package transport

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// TCPConfig configures the TCP driver listener. It is the transport-level
// view of config.TCPConfig; the server package maps one to the other so
// the transport layer does not depend on the config package.
type TCPConfig struct {
	// Listen is the TCP+TLS listen address.
	Listen string
	// IdleTimeout is how long a connection may sit without sending the
	// first byte of a frame before it is closed. Jittered ±10% per
	// connection so an idle pool does not return as one handshake storm.
	IdleTimeout time.Duration
	// FrameTimeout bounds one frame's header+body once it has started, so a
	// client drip-feeding one byte at a time cannot hold a goroutine and TLS
	// buffers open indefinitely.
	FrameTimeout time.Duration
	// MaxConnsPerIP caps concurrent connections per source address. Zero
	// disables the per-IP cap.
	MaxConnsPerIP int
	// AuthMaxFails is the consecutive-failure count after which an IP is
	// delayed before auth work, and the count at which it is refused
	// outright for 60s. Zero disables auth attempt limiting.
	AuthMaxFails int
	// PipelineFrames bounds the read-ahead window in frames; 1 is strict
	// lockstep, the CLI-compatible mode.
	PipelineFrames int
	// PipelineBytes bounds the read-ahead window in in-flight payload bytes.
	PipelineBytes int
	// PeerMode turns this listener into the internode port: connections
	// draw the reserved peer budget at accept (never the client budget, so
	// a driver flood cannot starve replication through this port) and
	// node-to-node opcodes are served instead of refused by name. The
	// driver listener keeps PeerMode false and keeps refusing them.
	PeerMode bool
}

// SessionDeps carries the cross-cutting dependencies a serving connection
// needs beyond its own transport. The server package fills these from the
// shared service state; tests fill only what they exercise.
type SessionDeps struct {
	// Logger for connection lifecycle and gate events. Defaults to
	// slog.Default().
	Logger *slog.Logger
	// Context is cancelled at shutdown; connection loops and dispatches
	// observe it to exit promptly.
	Context context.Context
	// Audit records an auth refusal of the per-IP ledger (spec 5.5) with the
	// source IP and the result text for the audit entry, which says why it was
	// refused: counted failures, or logins still being checked. Optional; nil
	// skips the audit hook.
	Audit func(ip string, result string)
}

// TCPListener owns the TCP driver listener and its connections. The
// server package's Service keeps one so Stop can close it and wait for
// connections to drain before the rest of shutdown proceeds.
type TCPListener struct {
	ln       net.Listener
	limiter  *ConnLimiter
	cfg      TCPConfig
	handler  RequestHandler
	deps     *SessionDeps
	logger   *slog.Logger
	closeErr error

	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	wg        sync.WaitGroup // one slot per accepted connection goroutine

	connMu sync.Mutex
	conns  map[net.Conn]struct{} // live connections, so Close can unblock them

	// sessions maps an authenticated username to that user's live connections
	// on this listener, so a drop or disable can close them (spec 8.8).
	sessions *SessionRegistry

	// afterAuthOK is a test seam, nil in production: it runs on the dispatch
	// goroutine of a successful AUTH after the reply is written and before
	// the session is registered, which is the window a drop or disable can
	// land in (SetAfterAuthOKTestHook).
	afterAuthOK atomic.Pointer[func()]

	refuseMu   sync.Mutex
	lastRefuse time.Time // rate-limits refusal log lines
	refused    uint64    // total refusals, for visibility in one log line

	ipMu    sync.Mutex
	ipConns map[string]int // per-IP live connection count, counted at accept
	authLim *authAttemptLimiter

	// refusals holds one token per refusal in progress (a TLS handshake and
	// one error frame), so refusing cannot itself grow without bound.
	refusals chan struct{}

	// dispatch runs every connection's frame handlers on reused goroutines, so a handler
	// does not regrow its stack on each request.
	dispatch *dispatchPool

	// unauthenticated is set on the internode listener only (PeerMode): the
	// per-address share of the peer budget held by connections that have not
	// authenticated.
	unauthenticated *unauthenticatedPeers
}

// handshakeTimeout bounds the TLS handshake for accepted and refused
// connections alike, so a client that connects and never handshakes cannot
// hold accept-loop resources indefinitely.
const handshakeTimeout = 10 * time.Second

// maxPendingRefusals bounds the refusals in progress on one listener. Each is a
// goroutine and a server-side TLS handshake; past the bound a refused
// connection is closed without either, and its client sees the connection
// close instead of the OVERLOADED message.
const maxPendingRefusals = 64

// keepAlivePeriod is the TCP keepalive backstop (spec 5.7: keepalive is a
// backstop, not the liveness mechanism — heartbeats are).
const keepAlivePeriod = 60 * time.Second

// ListenTCP starts the TCP driver listener: it accepts connections, applies
// the shared connection limiter, verifies the negotiated ALPN, and serves
// each connection through the shared ServeFrame gate. The accept loop runs
// in a goroutine; Close stops it and drains the connections.
//
// Returns a listener handle rather than just an error because the caller
// must be able to stop it: close the listener first (stop accepting), then
// wait for connections to drain.
func ListenTCP(addr string, tlsCfg *tls.Config, limiter *ConnLimiter, cfg TCPConfig,
	handler RequestHandler, deps *SessionDeps) (*TCPListener, error) {

	ln, err := tls.Listen("tcp", addr, tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("listen tcp on %s: %w", addr, err)
	}
	return ServeTCP(ln, limiter, cfg, handler, deps), nil
}

// ServeTCP serves an already-listening TLS listener. It lets a caller that
// must own the bind — a test harness reserving a port and handing the
// listener over, instead of returning a released port string — keep the port
// bound from reservation until ServeTCP takes over, so no other connection
// can claim it in the gap. The listener is closed by Close,
// exactly as one from ListenTCP.
//
// ln must already be TLS-wrapped (ListenTCP wraps it; a caller holding a plain
// listener wraps it with tls.NewListener) so the handshake policy is never
// accidentally skipped.
func ServeTCP(ln net.Listener, limiter *ConnLimiter, cfg TCPConfig, handler RequestHandler,
	deps *SessionDeps) *TCPListener {

	if deps == nil {
		deps = &SessionDeps{}
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.Context == nil {
		deps.Context = context.Background()
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 5 * time.Minute
	}
	if cfg.FrameTimeout <= 0 {
		cfg.FrameTimeout = 30 * time.Second
	}
	if cfg.PipelineFrames < 1 {
		cfg.PipelineFrames = 1
	}
	if cfg.PipelineBytes < 1 {
		cfg.PipelineBytes = 1 << 20
	}
	// Internode profile: one migration chunk alone is 8 MiB
	// (snapshotChunkSize), so the peer port starts at a window that keeps a
	// chunk plus concurrent gossip/repl frames in flight, where the driver
	// port's 1 MiB byte bound would clamp to one-at-a-time. Callers may
	// raise it with explicit values, never below the frames a peer client
	// keeps in flight on its fullest socket (a request socket). The byte
	// bound does not follow the frame floor: worst-case read-ahead bytes per
	// connection stay 32 MiB plus one frame however many frames are admitted.
	if cfg.PeerMode && cfg.PipelineFrames < PeerPipelineFramesFloor {
		cfg.PipelineFrames = PeerPipelineFramesFloor
	}
	if cfg.PeerMode && cfg.PipelineBytes < 32<<20 {
		cfg.PipelineBytes = 32 << 20
	}

	ctx, cancel := context.WithCancel(deps.Context)
	l := &TCPListener{
		ln:       ln,
		limiter:  limiter,
		cfg:      cfg,
		handler:  handler,
		deps:     deps,
		logger:   deps.Logger,
		ctx:      ctx,
		cancel:   cancel,
		conns:    make(map[net.Conn]struct{}),
		ipConns:  make(map[string]int),
		sessions: NewSessionRegistry(),
		refusals: make(chan struct{}, maxPendingRefusals),
	}
	l.dispatch = newDispatchPool(ctx, &l.wg)
	if cfg.AuthMaxFails > 0 {
		l.authLim = newAuthAttemptLimiter(cfg.AuthMaxFails)
	}
	if cfg.PeerMode {
		l.unauthenticated = newUnauthenticatedPeers(peerOrClientCap(limiter, true))
	}

	l.wg.Add(1)
	go l.acceptLoop()

	return l
}

// Addr returns the listener's bound address.
func (l *TCPListener) Addr() net.Addr {
	return l.ln.Addr()
}

// CloseSessionsFor closes every live session for username on this listener
// and returns how many it closed. The server installs it as the callback a
// drop or disable uses (spec 8.8).
func (l *TCPListener) CloseSessionsFor(username string) int {
	if l.sessions == nil {
		return 0
	}
	return l.sessions.CloseSessionsFor(username)
}

// SetAfterAuthOKTestHook installs a function that runs on every successful
// AUTH between the write of its reply and the registration of its session;
// nil removes it. It exists for tests, here and in the server package, that
// must place an account change inside that window, and nothing in the
// product calls it.
func (l *TCPListener) SetAfterAuthOKTestHook(hook func()) {
	if hook == nil {
		l.afterAuthOK.Store(nil)
		return
	}
	l.afterAuthOK.Store(&hook)
}

// SetBeforeRegistrationTestHook installs a function that runs inside every
// registration of a session, before the session is recorded; nil removes it.
// It is the second half of the window SetAfterAuthOKTestHook opens: an
// account change placed here completes after anything the listener checked
// ahead of the registration, so only a check made after the registration can
// see it. It exists for tests, and nothing in the product calls it.
func (l *TCPListener) SetBeforeRegistrationTestHook(hook func()) {
	if hook == nil {
		l.sessions.beforeAdd.Store(nil)
		return
	}
	l.sessions.beforeAdd.Store(&hook)
}

// refuseMessage is the OpError payload for a connection refused at the
// global cap (spec 5.6): the cap, the current count, and retry guidance, so
// a developer does not have to guess why the connection died.
func (l *TCPListener) refuseMessage(capValue, current int, envName string) string {
	return fmt.Sprintf("server connection limit reached (%d, %s); retry shortly or raise the cap (currently %d connections)",
		capValue, envName, current)
}

// noteRefusal records a refused connection and logs it rate-limited: one
// line per interval, not one line per rejection, so overload is visible
// without the log becoming the outage (spec 5.6).
func (l *TCPListener) noteRefusal(reason string) {
	l.refuseMu.Lock()
	l.refused++
	now := time.Now()
	rateLimited := now.Sub(l.lastRefuse) < 10*time.Second
	if !rateLimited {
		l.lastRefuse = now
	}
	refused := l.refused
	l.refuseMu.Unlock()
	if !rateLimited {
		l.logger.Warn("connection refused", "reason", reason, "total_refused", refused)
	}
}

// acceptLoop accepts connections and hands each to serveConn.
func (l *TCPListener) acceptLoop() {
	defer l.wg.Done()
	var acceptBackoff time.Duration
	for {
		raw, err := l.ln.Accept()
		if err != nil {
			if l.ctx.Err() != nil {
				return // shutting down
			}
			// Transient accept errors (e.g. EMFILE) use exponential backoff capped at ~1s.
			// Rate-limit the log line: one error per 10s, not one per retry.
			if acceptBackoff == 0 {
				acceptBackoff = 50 * time.Millisecond
			} else {
				acceptBackoff *= 2
				if acceptBackoff > time.Second {
					acceptBackoff = time.Second
				}
			}
			l.noteRefusal(fmt.Sprintf("accept failed: %v", err))
			time.Sleep(acceptBackoff)
			continue
		}
		acceptBackoff = 0 // reset backoff on successful accept

		// The per-address cap is counted here, at accept, before the TLS
		// handshake: counted after it, connections that never handshake
		// would hold budget slots for the whole handshake timeout without
		// meeting the cap.
		host := remoteHost(raw.RemoteAddr())
		releaseAddress, admitted := l.admitAddress(host)
		if !admitted {
			l.noteRefusal("per-IP cap reached")
			l.refuseInBackground(raw, ErrOverloaded,
				fmt.Sprintf("connection limit for %s reached (%d, HOARDDB_TCP_MAX_CONNS_PER_IP); retry shortly", host, l.cfg.MaxConnsPerIP))
			continue
		}

		// Acquire a budget slot before doing anything else. On the driver
		// port that is a client slot, refused with a named OpError when the
		// budget is full (an opaque close is indistinguishable from a TLS
		// misconfiguration). On the internode port (PeerMode) the slot is
		// the reserved peer budget: peers draw peer capacity only, so they
		// are never refused because the client budget is full — that
		// reservation is the whole point of the split.
		acquire := l.limiter.AcquireClient
		budgetName := "HOARDDB_MAX_CONNS"
		refuseReason := "client budget full"
		if l.cfg.PeerMode {
			acquire = l.limiter.AcquirePeer
			budgetName = "HOARDDB_MAX_PEER_CONNS"
			refuseReason = "peer budget full"
		}
		release, ok := acquire()
		if !ok {
			releaseAddress()
			clients, peers := l.limiter.Counts()
			current := clients
			if l.cfg.PeerMode {
				current = peers
			}
			l.noteRefusal(refuseReason)
			l.refuseInBackground(raw, ErrOverloaded, l.refuseMessage(peerOrClientCap(l.limiter, l.cfg.PeerMode), current, budgetName))
			continue
		}

		// On the internode port, one address's unauthenticated connections
		// may hold only a share of the peer budget, so a party that never
		// authenticates cannot take the slots a member's dial needs
		//. Checked after the budget, so a full budget is still
		// refused by its own name, and the slot is handed back at once.
		releaseUnauthenticated := func() {}
		if l.cfg.PeerMode {
			admitted, ok := l.unauthenticated.admit(host)
			if !ok {
				release()
				releaseAddress()
				l.noteRefusal("unauthenticated share of the peer budget reached")
				l.refuseInBackground(raw, ErrOverloaded, l.unauthenticated.refusal(host, peerOrClientCap(l.limiter, true)))
				continue
			}
			releaseUnauthenticated = admitted
		}

		l.wg.Add(1)
		go func() {
			defer l.wg.Done()
			defer releaseAddress()
			defer release()
			defer releaseUnauthenticated()
			l.serveConn(raw, releaseUnauthenticated)
		}()
	}
}

// admitAddress counts one more connection from host against MaxConnsPerIP and
// returns the func that gives it back. It reports false, counting nothing,
// when host is at the cap. Check and increment share one critical section, so
// a burst from one address cannot all read the same count and all pass.
func (l *TCPListener) admitAddress(host string) (release func(), ok bool) {
	capPerIP := l.cfg.MaxConnsPerIP
	if capPerIP <= 0 {
		return func() {}, true
	}
	l.ipMu.Lock()
	defer l.ipMu.Unlock()
	if l.ipConns[host] >= capPerIP {
		return nil, false
	}
	l.ipConns[host]++
	var once sync.Once
	return func() {
		once.Do(func() {
			l.ipMu.Lock()
			defer l.ipMu.Unlock()
			l.ipConns[host]--
			if l.ipConns[host] == 0 {
				delete(l.ipConns, host)
			}
		})
	}, true
}

// refuseInBackground refuses raw with one OpError frame on its own goroutine,
// within maxPendingRefusals. Past the bound the socket is closed at once,
// without a handshake. A refusal goroutine is tracked by l.wg like an accepted
// connection, so Close's "stop accepting, then drain everything" ordering
// covers a refusal still mid-handshake.
func (l *TCPListener) refuseInBackground(raw net.Conn, code ErrorCode, message string) {
	select {
	case l.refusals <- struct{}{}:
	default:
		_ = raw.Close()
		return
	}
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		defer func() { <-l.refusals }()
		l.refuse(raw, code, message)
	}()
}

// peerOrClientCap returns the capacity of the budget a PeerMode (or
// driver) acceptor drew from, for the refusal message.
func peerOrClientCap(l *ConnLimiter, peerMode bool) int {
	if peerMode {
		return cap(l.peerSlots)
	}
	return cap(l.clientSlots)
}

// refuse completes the TLS handshake, writes one OpError frame naming the
// reason, and closes. It runs in its own goroutine bounded by
// handshakeTimeout so a client that never handshakes cannot stall the
// accept loop.
func (l *TCPListener) refuse(raw net.Conn, code ErrorCode, message string) {
	tc, ok := raw.(*tls.Conn)
	if !ok {
		_ = raw.Close()
		return
	}
	_ = tc.SetDeadline(time.Now().Add(handshakeTimeout))
	if err := tc.HandshakeContext(l.ctx); err != nil {
		_ = raw.Close()
		return
	}
	var zeroID [RequestIDLen]byte
	if err := WriteErrorResponse(tc, code, message, zeroID); err != nil {
		l.logger.Debug("write refusal", "addr", raw.RemoteAddr().String(), "error", err)
	}
	_ = tc.Close()
}

// serveConn runs one accepted connection: handshake, ALPN gate, per-IP cap,
// then the read-dispatch-write loop under the read-ahead window. On the
// internode port, releaseUnauthenticated gives back the connection's place in
// its address's unauthenticated share once it authenticates.
func (l *TCPListener) serveConn(raw net.Conn, releaseUnauthenticated func()) {
	tc, ok := raw.(*tls.Conn)
	if !ok {
		_ = raw.Close()
		return
	}

	// Registry entry so Close can unblock this connection during shutdown.
	l.connMu.Lock()
	l.conns[tc] = struct{}{}
	l.connMu.Unlock()
	defer func() {
		l.connMu.Lock()
		delete(l.conns, tc)
		l.connMu.Unlock()
	}()
	// Every exit path (idle, frame timeout, bad frame, shutdown) tears the
	// socket down; otherwise a returned loop leaves the client waiting on
	// a connection the server has already forgotten.
	defer func() { _ = tc.Close() }()

	addr := tc.RemoteAddr().String()
	// The idle timeout covers the gap *between* frames (spec 5.7). The jitter
	// is drawn once, for the connection's life, so a pool that goes idle
	// together does not come back together as one handshake spike (spec
	// review R4).
	idleTimeout := jittered(l.cfg.IdleTimeout)
	win := newWindow(l.cfg.PipelineFrames, l.cfg.PipelineBytes, tc, idleTimeout)
	defer win.close()

	// A per-connection context, not l.ctx directly: the shutdown watcher
	// below must stop when THIS connection ends (client disconnect, idle
	// timeout, bad frame), not only when the listener itself shuts down —
	// otherwise every connection that ever closed normally leaves that
	// goroutine parked on l.ctx.Done() for the life of the process.
	connCtx, connCancel := context.WithCancel(l.ctx)
	defer connCancel()

	hsCtx, hsCancel := context.WithTimeout(l.ctx, handshakeTimeout)
	err := tc.HandshakeContext(hsCtx)
	hsCancel()
	if err != nil {
		l.logger.Debug("handshake failed", "addr", addr, "error", err)
		_ = tc.Close()
		return
	}
	addr = tc.RemoteAddr().String()

	// ALPN version gate (spec 3.1 rule 1): the negotiated identifier fixes
	// the protocol version for the whole connection, and one we do not serve
	// is refused by name before a single frame is read. Go's crypto/tls
	// only fails the handshake when the client offered ALPN with zero
	// overlap, so an ALPN-less client would otherwise slide through here.
	if err := VerifyNegotiatedALPN(tc.ConnectionState().NegotiatedProtocol); err != nil {
		l.logger.Warn("refusing connection: ALPN not served", "addr", addr, "error", err)
		var we *WireError
		if errors.As(err, &we) {
			l.refuse(tc, we.Code, we.Message)
		} else {
			l.logger.Error("failed to extract WireError from ALPN verification", "addr", addr, "error", err)
			_ = tc.Close()
		}
		return
	}

	host, _, splitErr := net.SplitHostPort(addr)
	if splitErr != nil {
		host = addr
	}

	// TCP keepalive as a backstop for dead peers; heartbeats (OpPing) are
	// the liveness mechanism proper.
	if tcpConn, isTCP := tc.NetConn().(*net.TCPConn); isTCP {
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(keepAlivePeriod)
	}

	sess := &Session{
		Authenticated: false,
		RemoteAddr:    addr,
		Peer:          l.cfg.PeerMode,
	}
	// A user session is bound to its connection so a drop or disable can
	// close it. Only the driver listener registers: peer sessions are
	// node-to-node and authenticate as the node identity, not as a user
	// (spec 8.8).
	if !l.cfg.PeerMode {
		defer l.sessions.Remove(sess)
	}
	// On the internode port a connection that has not authenticated by the
	// deadline is closed and its peer slot freed. The timer is absolute:
	// PING is answered before AUTH and re-arms the idle deadline, so the idle
	// deadline alone let an unauthenticated connection live forever
	//. Only a node can authenticate on this port
	// (server/dispatch.go:handleAuth), so authenticated here means a member.
	if l.cfg.PeerMode {
		preAuthTimeout := peerPreAuthTimeout(l.cfg.IdleTimeout)
		preAuthDeadline := time.AfterFunc(preAuthTimeout, func() {
			if !sess.IsAuthenticated() {
				l.logger.Info("closing internode connection: not authenticated within the pre-AUTH deadline",
					"addr", addr, "deadline", preAuthTimeout)
				connCancel()
			}
		})
		defer preAuthDeadline.Stop()
	}
	// The session travels in the connection context so the server's AUTH
	// handler can set identity without transport importing server (spec 3.7).
	serveCtx := WithSession(connCtx, sess)

	br := bufio.NewReader(tc)
	var writeMu sync.Mutex
	writes := &mutexWriter{w: tc, mu: &writeMu, frameTimeout: l.cfg.FrameTimeout}
	var dispatches sync.WaitGroup
	var fatalOnce sync.Once
	// failed is closed by fatal once drainBy is set.
	failed := make(chan struct{})
	var drainBy time.Time
	fatal := func(requestID [RequestIDLen]byte, handlerErr error) {
		// TCP has no RST_STREAM: a fatal handler error is answered with one
		// OpError frame echoing the failing request, and the connection is
		// closed (build doc step 4). Not at once: the other requests in flight
		// may have applied already, a pop's removal among them, and their
		// answers are the only copy of what they did (BUG-0563). The
		// connection stops reading and the reader, which is not a dispatch and
		// so cannot be waiting for itself, closes it once they have answered.
		fatalOnce.Do(func() {
			_ = WriteErrorResponse(writes, ErrInternal, handlerErr.Error(), requestID)
			drainBy = time.Now().Add(l.cfg.FrameTimeout)
			close(failed)
			win.stopAdmitting()
		})
	}
	// The connection's dispatches must be drained before the connection is
	// torn down, or Stop would leak goroutines mid-response. After a handler
	// error the wait is bounded by the frame timeout: past it the connection
	// is closed and its work cancelled, so a request that never finishes on
	// its own cannot keep a failed connection open.
	defer func() {
		select {
		case <-failed:
			if !drainedBy(&dispatches, drainBy) {
				l.logger.Info("closing connection: requests still in flight at the bound after a handler error",
					"addr", addr, "bound", l.cfg.FrameTimeout)
				_ = tc.NetConn().Close()
				connCancel()
			}
		default:
		}
		dispatches.Wait()
	}()

	// On shutdown (listener-wide, via l.ctx, or this connection ending, via
	// connCancel above) the peek below then errors out and the loop returns
	// promptly instead of waiting out the idle deadline. The window also
	// has to be released, because the loop may be blocked inside admit.
	go func() {
		<-connCtx.Done()
		win.close()
		_ = tc.Close()
	}()

	// A read that fails because the client went away cancels the
	// connection's work before the deferred drain waits for it (spec §3.4):
	// the drain is then quick, because the walks stop. A read that fails
	// because the server is closing the connection — shutdown, or a fatal
	// handler error — keeps the drain first.
	readEnded := func(err error) {
		switch {
		case l.ctx.Err() != nil:
			l.logger.Debug("connection closed on shutdown", "addr", addr)
		case win.isClosed():
			l.logger.Debug("connection closed by the server", "addr", addr, "error", err)
		default:
			l.logger.Debug("client gone; cancelling its requests", "addr", addr, "error", err)
			connCancel()
		}
	}

	for {
		// Waiting for a new frame's first byte is a gap between frames: the
		// jittered idle deadline applies, but only while nothing is in
		// flight (awaitNextFrame). The moment a byte arrives a frame is in
		// progress and the per-frame deadline takes over, so a drip-fed
		// frame is caught within FrameTimeout while a healthy client that
		// simply waits between requests is not. The read comes before the
		// window is reserved, so the reader sees a client leave even when
		// the window is full.
		_ = tc.SetReadDeadline(time.Time{})
		win.awaitNextFrame()
		_, err := br.Peek(1)
		win.frameStarted()
		if err != nil {
			if isTimeout(err) && l.ctx.Err() == nil && !win.isClosed() {
				l.logger.Info("connection idle timeout", "addr", addr, "idle_timeout", l.cfg.IdleTimeout)
				return
			}
			readEnded(err)
			return
		}

		// Read-ahead window (spec 6): if either bound is reached, the next
		// frame is not read until a response completes, so the client's
		// writes block on TCP backpressure. There is nothing to detect and
		// nothing to punish for a client that read ahead further.
		if !l.reserveReadSlot(tc, br, win, readEnded) {
			return
		}

		// Frame started: bound its header+body by the frame deadline. Until
		// AUTH has succeeded only AUTH and PING are served, so a frame is
		// held to their size: a connection that has not logged in cannot make
		// the server read, or hold, more than that.
		win.armFrameDeadline(l.cfg.FrameTimeout)
		frameLimit := uint32(MaxFrameLength)
		if !sess.IsAuthenticated() {
			frameLimit = PreAuthFrameLength
		}
		frame, err := DecodeFrameWithin(br, frameLimit)
		if err != nil {
			var oversize *FrameTooLargeError
			if errors.As(err, &oversize) && l.answerOversizeRequest(br, writes, sess, oversize, addr) {
				// Answered here, on the reader: the slot reserved for it is
				// given back, or the window shrinks by one for good and the
				// idle deadline, armed only with nothing in flight, never is.
				win.complete(0)
				continue
			}
			l.logger.Info("closing connection: bad frame", "addr", addr, "error", err)
			return
		}
		win.track(len(frame.Payload))
		// A handler failed while this frame was being read: the connection
		// admits nothing new, and nothing of this frame has run.
		if win.isClosed() {
			return
		}

		// Node-to-node opcodes are refused by name on the driver port (spec
		// 7): a driver has no business sending them, and refusal keeps a
		// driver bug debuggable instead of silent. The internode port exists
		// for exactly those opcodes, so the refusal is skipped there.
		if !l.cfg.PeerMode && isNodeToNodeOpcode(frame.Opcode) {
			l.logger.Warn("node-to-node opcode on TCP connection", "addr", addr, "opcode", frame.Opcode)
			_ = WriteErrorResponse(writes, ErrInvalidArgument,
				fmt.Sprintf("opcode %s is node-to-node traffic and is not served over TCP", frame.Opcode),
				frame.RequestID)
			return
		}

		// Auth attempt limiting (spec 5.5): refuse or delay before any auth
		// work is done.
		if frame.Opcode == OpAuth && l.authLim != nil {
			// The wait for logins in flight ends with the connection; the
			// reader is the one waiting, so nothing else is read meanwhile.
			admission, waitErr := l.authLim.beforeAuth(connCtx, host)
			if waitErr != nil {
				l.logger.Debug("connection closed while its login waited for others in flight", "addr", addr, "error", waitErr)
				return
			}
			if admission.refused {
				message := ledgerRefusalMessage(admission.failures)
				auditResult := fmt.Sprintf("auth rate limit: %d consecutive failures", admission.failures)
				if admission.checking > 0 {
					// Nothing failed: the reply and the audit entry say what
					// held the login back instead.
					l.logger.Warn("auth refused (logins in flight)", "addr", addr, "ip", host,
						"checking", admission.checking, "waiting", admission.waiting)
					message = inFlightRefusalMessage(admission, l.cfg.AuthMaxFails)
					auditResult = fmt.Sprintf("login refused: %d being checked, %d waiting; no password was checked",
						admission.checking, admission.waiting)
				} else {
					l.logger.Warn("auth refused (rate limit)", "addr", addr, "ip", host, "failures", admission.failures)
				}
				if l.deps.Audit != nil {
					l.deps.Audit(host, auditResult)
				}
				// A rate-limit refusal carries RATE_LIMITED instead of a nil
				// payload, so a driver backs off instead of treating it as a
				// credential failure and retrying the password (spec 3.3c).
				payload, _ := bson.Marshal(struct {
					Reason  uint8  `bson:"reason"`
					Message string `bson:"message,omitempty"`
				}{
					Reason:  uint8(AuthFailRateLimited),
					Message: message,
				})
				_ = WriteFrame(writes, NewFrameWithID(OpAuthFail, frame.RequestID, payload))
				return
			}
			if delay := admission.delay; delay > 0 {
				l.logger.Info("delaying auth attempt (rate limit)", "addr", addr, "ip", host, "failures", admission.failures, "delay", delay)
				select {
				case <-time.After(delay):
				case <-l.ctx.Done():
					// The attempt was admitted and will never be dispatched.
					l.recordAuthOutcome(host, addr, authLedgerUntouched)
					return
				}
			}
		}

		dispatches.Add(1)
		l.dispatch.submit(func() {
			defer dispatches.Done()
			defer win.complete(len(frame.Payload))
			// An AUTH's ledger reservation is settled by serveAuthFrame; a
			// fault before it does so gives the reservation back here.
			settled := false
			// The request boundary: a fault in the handler fails this request,
			// not the process. It runs before the two defers above,
			// so the window slot and the dispatch count are released after the
			// fatal answer is written.
			defer func() {
				recovered := recover()
				if recovered == nil {
					return
				}
				if frame.Opcode == OpAuth && !settled {
					l.recordAuthOutcome(host, addr, authLedgerUntouched)
				}
				fatal(frame.RequestID, l.handlerFault(recovered, addr, frame.Opcode))
			}()
			// The write mutex lives inside the writer, so concurrent
			// dispatches run their handlers in parallel and only the frame
			// writes serialize on the wire (build doc step 4: serialize
			// WriteFrame, never the dispatch itself).
			var serveErr error
			if frame.Opcode == OpAuth {
				// The ledger is updated inside serveAuthFrame, before the
				// reply is written, so a client cannot see an outcome the
				// limiter has not counted. The attempt was reserved when it
				// was admitted; serveAuthFrame settles it unless the handler
				// fails first, so the reservation is released here in that
				// case and never twice.
				serveErr = serveAuthFrame(serveCtx, authFrameRequest{
					Session: sess,
					Frame:   frame,
					Writer:  writes,
					Handler: l.handler,
					Log:     l.logger,
					Record: func(effect authLedgerEffect) {
						settled = true
						l.recordAuthOutcome(host, addr, effect)
					},
				})
				if !settled {
					l.recordAuthOutcome(host, addr, authLedgerUntouched)
				}
			} else {
				serveErr = ServeFrame(serveCtx, sess, frame, writes, l.handler, l.logger)
			}
			// A response the connection could not take means the client is
			// gone, or not reading: its work is cancelled and the connection
			// closed, so nothing more is read and run for it. fatal is not
			// called, because it would write on the writer that just failed.
			// The socket under TLS is closed first: tls.Conn.Close sends a
			// close_notify under a 5 s write deadline, which on a writer
			// nobody reads would keep the socket open that long.
			// A revoked login was answered by ServeFrame; the connection is
			// closed at once, and the work in flight on it with it.
			if errors.Is(serveErr, ErrLoginRevoked) {
				_ = tc.NetConn().Close()
				connCancel()
				win.close()
				return
			}
			if errors.Is(serveErr, ErrResponseWrite) {
				l.logger.Info("closing connection: a response could not be written", "addr", addr, "opcode", frame.Opcode, "error", serveErr)
				_ = tc.NetConn().Close()
				connCancel()
				win.close()
				return
			}
			if serveErr != nil {
				l.logger.Error("handler error", "addr", addr, "opcode", frame.Opcode, "error", serveErr)
				fatal(frame.RequestID, serveErr)
				return
			}

			// A successful AUTH binds this connection to its username so a
			// later drop or disable can close it (spec 8.8).
			if frame.Opcode == OpAuth && sess.IsAuthenticated() && !l.cfg.PeerMode {
				if hook := l.afterAuthOK.Load(); hook != nil {
					(*hook)()
				}
				if identity, ok := sess.Identity(); ok {
					l.sessions.Add(identity.Username, sess, tc)
					// The account may have been dropped, disabled or given
					// new credentials since its login was verified, and the
					// close that change made ran before this registration
					// and found nothing. Every such change revokes the
					// standing before it closes the registered sessions, so
					// one of the two always sees the other: either that
					// close finds this session, or this check, made after
					// the registration, finds the standing revoked.
					if sess.loginRevoked() {
						l.logger.Info("closing connection: its login was revoked as it completed", "addr", addr, "username", identity.Username)
						l.sessions.Remove(sess)
						_ = tc.NetConn().Close()
						connCancel()
						win.close()
						return
					}
				}
			}
			// A member's connection no longer counts against its address's
			// unauthenticated share, so a host running several members
			// keeps every pooled socket it has authenticated.
			if frame.Opcode == OpAuth && sess.IsAuthenticated() && l.cfg.PeerMode {
				releaseUnauthenticated()
			}
		})
	}
}

// reserveReadSlot reserves the window's slot for the frame whose first byte
// the reader has seen. While the window is full the reader probes: it waits
// for the client's next bytes without consuming them (Peek past what is
// buffered), so a client that leaves is seen at once, and a completing
// response interrupts the wait (window.complete). It reports false when the
// connection is ending, after readEnded has been told why.
//
// The one residual: once the buffer is full nothing more can be read without
// consuming it, so the reader waits for a slot blind (window.admit), and a
// client that pipelined more than the buffer past a full window and then
// left is noticed when a slot frees.
func (l *TCPListener) reserveReadSlot(tc *tls.Conn, br *bufio.Reader, win *window, readEnded func(error)) bool {
	for {
		// Cleared before the window is read, for the reason awaitNextFrame
		// gives: a completing response's interrupt is set under the window's
		// lock, and this clear must not land after it.
		_ = tc.SetReadDeadline(time.Time{})
		reserved, full := win.reserveOrProbe()
		if reserved {
			return true
		}
		if !full {
			return false // the window was closed: shutdown, or a fatal error
		}
		_, err := br.Peek(br.Buffered() + 1)
		win.probeEnded()
		switch {
		case err == nil:
			// The client sent more; it is buffered, and the window is
			// checked again.
		case errors.Is(err, bufio.ErrBufferFull):
			return win.admit()
		case isTimeout(err) && l.ctx.Err() == nil && !win.isClosed():
			// A response completed and interrupted the probe. A read timeout
			// on a TLS connection is not sticky: a partial record stays
			// buffered and the next read resumes it.
		default:
			readEnded(err)
			return false
		}
	}
}

// recordAuthOutcome settles the attempt the limiter reserved when it admitted
// this AUTH, exactly once: a password login clears the count, a failure turns
// the reservation into a counted failure (spec 5.5), and a refusal that
// checked no credential only releases it, because counting it would lock out
// an IP that sent only correct passwords; serveAuthFrame classifies the frame.
func (l *TCPListener) recordAuthOutcome(host, addr string, effect authLedgerEffect) {
	if l.authLim == nil {
		return
	}
	failures := l.authLim.settle(host, effect)
	switch effect {
	case authLedgerClear:
	case authLedgerUntouched:
		l.logger.Debug("auth frame neither logged in nor failed a credential check; not counted", "addr", addr, "ip", host)
	default:
		l.logger.Debug("auth failed over TCP", "addr", addr, "ip", host, "failures", failures)
	}
	l.authLim.applied.Add(1)
}

// maxDrainedFrameLength bounds the length word of an oversize request that the
// server reads past. Beyond it the server answers and closes. The
// tradeoff: draining lets an authenticated client make the server read and
// discard up to this many bytes per request, all inside the frame deadline
// that is already running, so a slow sender is cut off like any other.
const maxDrainedFrameLength = 2 * MaxFrameLength

// answerOversizeRequest answers a request whose length word is over the frame
// cap with an error that carries its request ID and names the cap, instead of
// closing the connection unexplained. It reports whether the connection can
// go on: an authenticated session whose oversize body is within
// maxDrainedFrameLength has the body discarded and keeps its connection, so
// the requests pipelined behind it are not lost. A session that has not
// authenticated is answered and closed, so only an identified client can make
// the server read a body it will not use.
func (l *TCPListener) answerOversizeRequest(br *bufio.Reader, writes io.Writer, sess *Session, oversize *FrameTooLargeError, addr string) bool {
	l.logger.Info("request over the frame limit", "addr", addr, "opcode", oversize.Opcode, "bytes", oversize.BodyLength, "limit", oversize.Limit)
	if err := WriteErrorResponse(writes, ErrInvalidArgument, oversize.Error(), oversize.RequestID); err != nil {
		l.logger.Debug("write oversize-request refusal", "addr", addr, "error", err)
		return false
	}
	if !sess.IsAuthenticated() || oversize.BodyLength > maxDrainedFrameLength {
		return false
	}
	if _, err := io.CopyN(io.Discard, br, oversize.UnreadBodyBytes()); err != nil {
		l.logger.Debug("discarding oversize request body", "addr", addr, "error", err)
		return false
	}
	return true
}

// mutexWriter serializes response writes on one connection. ServeFrame is
// called without holding any write lock, so concurrent dispatches run their
// handlers in parallel and only the frame writes themselves wait — building
// block for the pipelining window (build doc step 4). The write deadline is
// refreshed immediately before each write so a slow handler does not eat
// into the next response's budget.
type mutexWriter struct {
	mu           *sync.Mutex
	w            net.Conn
	frameTimeout time.Duration
}

func (m *mutexWriter) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_ = m.w.SetWriteDeadline(time.Now().Add(m.frameTimeout))
	return m.w.Write(p)
}

// isNodeToNodeOpcode reports whether an opcode is reserved for internode
// traffic (spec 7). The TCP listener refuses these by name: a driver has no
// business sending them, and refusal keeps a driver bug debuggable.
func isNodeToNodeOpcode(op Opcode) bool {
	switch op {
	case OpGossip, OpReplPush, OpReplAck, OpMigrateStart, OpMigrateData,
		OpMigrateDone, OpClusterPropose, OpClusterProbe, OpClusterProbeReply, OpVersionedGet, OpMembershipChange,
		OpAuthConfigPush, OpAuthConfigAck,
		OpStreamOpen, OpStreamData, OpStreamEnd, OpStreamAck:
		return true
	default:
		return false
	}
}

// window is the read-ahead pipelining bound (spec 6). admit blocks the
// reader before the next frame is read when either bound is reached; that
// is what throttles a client that pipelines more than the window — the
// server stops reading, TCP backpressure does the rest, and no error is
// ever sent.
//
// The window also holds the reader's state, so that a response completing
// can reach a reader that is waiting on the socket (query-pipelining spec
// §3.4). Every read deadline it sets on conn is set under mu: a deadline set
// outside it could land after, and undo, the one a completing response set.
type window struct {
	mu        sync.Mutex
	cond      *sync.Cond
	inFlight  int // frames read whose responses are not yet written
	inBytes   int // payload bytes of those frames
	maxFrames int
	maxBytes  int
	closed    bool

	conn        net.Conn
	idleTimeout time.Duration
	// probing is set while the reader, with the window full, waits for the
	// client's next bytes: a completing response interrupts that read.
	probing bool
	// idleArmPending is set while the reader waits for the next frame with no
	// deadline because responses were in flight: the response that leaves
	// none in flight arms the idle deadline.
	idleArmPending bool
}

func newWindow(maxFrames, maxBytes int, conn net.Conn, idleTimeout time.Duration) *window {
	w := &window{maxFrames: maxFrames, maxBytes: maxBytes, conn: conn, idleTimeout: idleTimeout}
	w.cond = sync.NewCond(&w.mu)
	return w
}

// awaitNextFrame prepares the reader's wait for the next frame's first byte.
// The idle deadline runs only while nothing is in flight, so a query longer
// than the idle timeout is answered; otherwise the last response arms it.
// The caller has cleared the read deadline before this, outside mu: cleared
// after, it could erase a deadline a completing response had armed in
// between, and the connection would never idle out.
func (w *window) awaitNextFrame() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		_ = w.conn.SetReadDeadline(time.Now())
		return
	}
	if w.inFlight == 0 {
		_ = w.conn.SetReadDeadline(time.Now().Add(w.idleTimeout))
		return
	}
	w.idleArmPending = true
}

// frameStarted ends the wait awaitNextFrame prepared.
func (w *window) frameStarted() {
	w.mu.Lock()
	w.idleArmPending = false
	w.mu.Unlock()
}

// reserveOrProbe reserves a read slot if the window has room. When it is
// full it marks the reader probing and reports full, and the reader then
// reads the socket without consuming anything until a slot frees. As in
// awaitNextFrame, the caller has cleared the read deadline before this.
func (w *window) reserveOrProbe() (reserved, full bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return false, false
	}
	if w.inFlight >= w.maxFrames || w.inBytes >= w.maxBytes {
		w.probing = true
		return false, true
	}
	w.inFlight++
	return true, false
}

// probeEnded clears the probing mark.
func (w *window) probeEnded() {
	w.mu.Lock()
	w.probing = false
	w.mu.Unlock()
}

// isClosed reports whether the window was closed: the connection is being
// torn down by the server, not left by its client.
func (w *window) isClosed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed
}

// admit reserves one read slot just before the next frame is read, blocking
// when the count or byte bound is reached. It never reports an error to the
// client — it simply does not read. Returns false only when the window was
// closed while waiting (listener shutdown or the fatal path).
func (w *window) admit() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.inFlight >= w.maxFrames || w.inBytes >= w.maxBytes {
		if w.closed {
			return false
		}
		w.cond.Wait()
	}
	w.inFlight++
	return true
}

// track adds a freshly-read frame's payload to the byte account. Called
// after DecodeFrame; the byte bound therefore sees the frame only after it
// has been read, so inBytes can overshoot maxBytes by at most one frame —
// the same slack MaxFrameLength already has — and the count bound above
// still holds exactly.
func (w *window) track(payloadBytes int) {
	w.mu.Lock()
	w.inBytes += payloadBytes
	w.mu.Unlock()
}

// complete releases one read slot and subtracts the frame's payload once
// its response has been written, waking any reader blocked in admit. A
// reader probing a full window is interrupted so it re-checks the window,
// and a reader waiting with no deadline gets its idle deadline when nothing
// is left in flight.
func (w *window) complete(payloadBytes int) {
	w.mu.Lock()
	w.inFlight--
	w.inBytes -= payloadBytes
	if w.inBytes < 0 {
		w.inBytes = 0
	}
	if w.probing {
		_ = w.conn.SetReadDeadline(time.Now())
	}
	if w.inFlight == 0 && w.idleArmPending && !w.closed {
		_ = w.conn.SetReadDeadline(time.Now().Add(w.idleTimeout))
		w.idleArmPending = false
	}
	w.mu.Unlock()
	w.cond.Broadcast()
}

// close wakes every admit waiter so they observe the closed flag and exit
// instead of blocking forever.
func (w *window) close() {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.cond.Broadcast()
}

// stopAdmitting closes the window and interrupts the reader's read, so the
// reader returns without reading another frame while the socket stays open
// for the responses still in flight. The deadline is set under mu, and every
// deadline the reader sets afterwards checks closed under mu
// (awaitNextFrame, armFrameDeadline), so none can undo it.
func (w *window) stopAdmitting() {
	w.mu.Lock()
	w.closed = true
	_ = w.conn.SetReadDeadline(time.Now())
	w.mu.Unlock()
	w.cond.Broadcast()
}

// armFrameDeadline bounds the frame the reader has started by timeout, or
// ends its read at once when the window was closed.
func (w *window) armFrameDeadline(timeout time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		_ = w.conn.SetReadDeadline(time.Now())
		return
	}
	_ = w.conn.SetReadDeadline(time.Now().Add(timeout))
}

// drainedBy waits until every dispatch counted by dispatches has ended or
// deadline has passed, and reports whether they all ended. Its waiter
// goroutine exits when they end; the caller waits for that unconditionally
// once it has cancelled them, so the waiter does not outlive the connection.
func drainedBy(dispatches *sync.WaitGroup, deadline time.Time) bool {
	drained := make(chan struct{})
	go func() {
		dispatches.Wait()
		close(drained)
	}()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-drained:
		return true
	case <-timer.C:
		return false
	}
}

// jittered returns d jittered ±10%, so idle pools do not all return in one
// handshake-and-auth spike (spec 5.7, review R4).
func jittered(d time.Duration) time.Duration {
	factor := 0.9 + 0.2*rand.Float64()
	return time.Duration(float64(d) * factor)
}

func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// Close stops the listener: no new accepts, live connections unblocked and
// closed, and every connection goroutine drained. Callers must close the
// TCP listener before tearing down transport state.
func (l *TCPListener) Close() error {
	l.closeOnce.Do(func() {
		l.cancel()
		if err := l.ln.Close(); err != nil {
			l.closeErr = fmt.Errorf("close TCP listener: %w", err)
		}
		l.connMu.Lock()
		for c := range l.conns {
			_ = c.Close()
		}
		l.connMu.Unlock()
	})
	l.wg.Wait()
	return l.closeErr
}

// --- per-IP auth attempt limiting (spec 5.5) ---

// Auth-failure rate-limit constants. After authDelayStart consecutive
// failures an IP's next attempt is delayed (exponentially up to
// authDelayCap); after authMaxFails (from config) it is refused outright for
// authRefuseWindow. Counters decay after authDecayInterval without a
// failure, and a success clears the IP entirely.
const (
	authDelayStart    = 5
	authDelayBase     = time.Second
	authDelayCap      = 30 * time.Second
	authRefuseWindow  = 60 * time.Second
	authDecayInterval = 15 * time.Minute
	// authInFlightWait bounds how long a login that finds every place under
	// the limit taken by logins still being checked waits for one to settle.
	// It matches the server's Argon2 derivation timeout (NewAuthManager), the
	// longest an admitted password login waits for its own derivation.
	authInFlightWait = 5 * time.Second
	// authWaitingPerPlace is how many logins per place under the limit may
	// wait at once; past it a login is refused at once, so an address cannot
	// queue without bound.
	authWaitingPerPlace = 4
)

// authAttemptLimiter tracks per-IP consecutive AUTH failures. One instance
// is shared by every connection of the TCP listener, so the counter is per
// IP across connections, not per connection.
type authAttemptLimiter struct {
	mu       sync.Mutex
	failures map[string]*ipFailureRecord
	maxFails int
	// recordDelay is nanoseconds settle waits before it counts a failure. Only
	// tests set it, to model a loaded host where the ledger update is slow: the
	// reply is held back with it, so a client cannot observe an outcome the
	// ledger has not counted. Zero costs one atomic load.
	recordDelay atomic.Int64
	// clearDelay is the same for a password success: nanoseconds settle waits
	// before it clears. Zero costs one atomic load.
	clearDelay atomic.Int64
	// holdDelay is nanoseconds settle waits before it settles an outcome of any
	// kind, so a test can keep every admitted attempt in flight while the rest
	// of a burst is judged. Zero costs one atomic load.
	holdDelay atomic.Int64
	// applied counts AUTH outcomes the ledger has finished with, whether the
	// outcome cleared, counted or touched nothing. Some leave no trace in the
	// ledger (a clear of an address with no record), so a test that must know
	// the outcome landed reads this instead of the counts.
	applied atomic.Int64
	// inFlightWait is nanoseconds a login waits for logins in flight to settle
	// (authInFlightWait); tests shorten it.
	inFlightWait atomic.Int64
}

type ipFailureRecord struct {
	// count is the number of consecutive failures (reset by success, decayed
	// by time).
	count int
	// inflight is the number of attempts beforeAuth admitted that settle has
	// not yet settled. They count against the limit so a burst of frames read
	// together cannot all be judged against the same count. A record with
	// attempts in flight is never swept: settle needs it.
	inflight int
	// waiting is the number of logins in beforeAuth waiting for an attempt in
	// flight to settle. A record with waiters is never swept either: each
	// waiter gives its place back on this record.
	waiting int
	// settled is closed, and cleared, by the next settle; waiters select on
	// it. Made by the first waiter, so an address nobody waits on has none.
	settled chan struct{}
	// lastFailure is when the most recent failure was recorded; entries are
	// swept after authDecayInterval.
	lastFailure time.Time
	// refusingUntil is non-zero during an outright-refusal window.
	refusingUntil time.Time
}

func newAuthAttemptLimiter(maxFails int) *authAttemptLimiter {
	limiter := &authAttemptLimiter{
		failures: make(map[string]*ipFailureRecord),
		maxFails: maxFails,
	}
	limiter.inFlightWait.Store(int64(authInFlightWait))
	return limiter
}

// authAdmission is what beforeAuth decided for one AUTH attempt.
type authAdmission struct {
	// delay is how long an admitted attempt waits before it is dispatched.
	delay time.Duration
	// refused means the attempt is answered RATE_LIMITED and not dispatched.
	refused bool
	// failures is the consecutive failures counted for the address.
	failures int
	// checking, non-zero only on a refusal, means the attempt was refused
	// because logins still being checked held every place under the limit,
	// not because of failures: it is how many were in flight, and waiting how
	// many others were waiting for them.
	checking int
	waiting  int
}

// beforeAuth decides what to do with an AUTH attempt from ip before any
// credential work: refuse outright (during a refusal window or past the
// threshold), delay (escalating), or admit. An attempt that finds every place
// under the limit taken by attempts still in flight waits, up to
// inFlightWait, for one to settle and is judged again; it is refused only when
// the wait runs out or too many already wait. An admitted attempt is reserved,
// and the caller must settle it exactly once. The error is ctx's, when the
// connection ends during the wait; nothing is reserved then.
func (a *authAttemptLimiter) beforeAuth(ctx context.Context, ip string) (authAdmission, error) {
	waitUntil := time.Now().Add(time.Duration(a.inFlightWait.Load()))
	for {
		a.mu.Lock()
		rec := a.failures[ip]
		if rec == nil {
			rec = &ipFailureRecord{}
			a.failures[ip] = rec
		}
		admission, full := a.judge(rec, time.Now())
		if !full {
			a.mu.Unlock()
			return admission, nil
		}
		remaining := time.Until(waitUntil)
		if remaining <= 0 || rec.waiting >= a.maxFails*authWaitingPerPlace {
			refusal := authAdmission{refused: true, failures: rec.count, checking: rec.inflight, waiting: rec.waiting}
			a.mu.Unlock()
			return refusal, nil
		}
		if rec.settled == nil {
			rec.settled = make(chan struct{})
		}
		settled := rec.settled
		rec.waiting++
		a.mu.Unlock()

		waitErr := awaitSettle(ctx, settled, remaining)

		a.mu.Lock()
		rec.waiting--
		if rec.inflight == 0 && rec.count == 0 && rec.waiting == 0 {
			delete(a.failures, ip)
		}
		a.mu.Unlock()
		if waitErr != nil {
			return authAdmission{}, fmt.Errorf("waiting for logins in flight from %s: %w", ip, waitErr)
		}
	}
}

// judge applies the ledger to one attempt under a.mu. full reports that only
// attempts in flight stand in the way: the count is under the limit but every
// place left is reserved. The returned admission is meaningful when full is
// false.
func (a *authAttemptLimiter) judge(rec *ipFailureRecord, now time.Time) (admission authAdmission, full bool) {
	// Decay: no failures for authDecayInterval means the ledger forgets the
	// IP, so a shared NAT is not held hostage by someone else's typos.
	if rec.count > 0 && now.Sub(rec.lastFailure) > authDecayInterval {
		rec.count = 0
		rec.refusingUntil = time.Time{}
	}
	if now.Before(rec.refusingUntil) {
		return authAdmission{refused: true, failures: rec.count}, false
	}
	// Refusal window has expired; reset the count so the IP gets a fresh start.
	if !rec.refusingUntil.IsZero() {
		rec.count = 0
		rec.refusingUntil = time.Time{}
	}
	if rec.count >= a.maxFails {
		rec.refusingUntil = now.Add(authRefuseWindow)
		return authAdmission{refused: true, failures: rec.count}, false
	}
	// Attempts in flight may still fail, so they take a place under the limit:
	// a burst of guesses cannot all be judged against the same low count.
	if rec.count+rec.inflight >= a.maxFails {
		return authAdmission{}, true
	}
	admission.failures = rec.count
	if rec.count >= authDelayStart {
		admission.delay = authDelayBase << (rec.count - authDelayStart) // 1s, 2s, 4s, …
		if admission.delay > authDelayCap {
			admission.delay = authDelayCap
		}
	}
	rec.inflight++
	return admission, false
}

// awaitSettle blocks until settled is closed, bound passes or ctx ends. It
// returns ctx's error whichever woke it, so a connection that ended is never
// judged again and admitted; otherwise the caller judges the attempt again.
func awaitSettle(ctx context.Context, settled <-chan struct{}, bound time.Duration) error {
	timer := time.NewTimer(bound)
	defer timer.Stop()
	select {
	case <-settled:
	case <-timer.C:
	case <-ctx.Done():
	}
	return ctx.Err()
}

// inFlightRefusalMessage is the RATE_LIMITED text for an AUTH refused because
// logins from its address still being checked held every place under the
// limit for as long as it could wait. Nothing failed, so it says so.
func inFlightRefusalMessage(admission authAdmission, maxFails int) string {
	return fmt.Sprintf("too many logins from this address are being checked at once (%d being checked, %d waiting); "+
		"this login's password was not checked and it counts toward no limit. HOARDDB_TCP_AUTH_MAX_FAILS (%d) is how many "+
		"logins from one address are checked at once, and up to %d more wait %ds for them. The server is closing this "+
		"connection; reconnect and retry in a moment, or raise HOARDDB_TCP_AUTH_MAX_FAILS",
		admission.checking, admission.waiting, maxFails, maxFails*authWaitingPerPlace, int(authInFlightWait.Seconds()))
}

// ledgerRefusalMessage is the RATE_LIMITED text for an AUTH the per-IP ledger
// refuses. It names what was counted and what was not, so a client whose
// passwords are right does not go hunting for a wrong one.
func ledgerRefusalMessage(failures int) string {
	return fmt.Sprintf("too many failed logins from this address (%d with no password login between them); every login from it "+
		"is refused for up to %ds, before any password is checked. The server is closing this connection; "+
		"wait out the window, then reconnect and retry with the correct password. "+
		"Wrong passwords, unknown users and rejected tokens count toward this limit; a busy server's "+
		"\"password was not checked\" refusal does not", failures, int(authRefuseWindow.Seconds()))
}

// settle ends the reservation beforeAuth made for one admitted attempt from
// ip, and returns the failures counted for ip afterwards. A failure turns the
// reservation into a counted failure, a password login clears the count, and
// anything else only releases the reservation.
func (a *authAttemptLimiter) settle(ip string, effect authLedgerEffect) int {
	time.Sleep(time.Duration(a.holdDelay.Load()))
	switch effect {
	case authLedgerClear:
		time.Sleep(time.Duration(a.clearDelay.Load()))
	case authLedgerCount:
		time.Sleep(time.Duration(a.recordDelay.Load()))
	}
	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	rec := a.failures[ip]
	rec.inflight--
	if rec.settled != nil {
		close(rec.settled)
		rec.settled = nil
	}
	switch effect {
	case authLedgerClear:
		rec.count = 0
		rec.refusingUntil = time.Time{}
	case authLedgerCount:
		// Sweep expired entries on write: a map this small is cheaper to sweep
		// than to background-goroutine. The record being counted is skipped: a
		// first failure has no lastFailure yet and would look expired.
		for k, other := range a.failures {
			if other != rec && other.inflight == 0 && other.waiting == 0 && now.Sub(other.lastFailure) > authDecayInterval {
				delete(a.failures, k)
			}
		}
		rec.count++
		rec.lastFailure = now
		rec.refusingUntil = time.Time{}
	}
	failures := rec.count
	if rec.inflight == 0 && rec.count == 0 && rec.waiting == 0 {
		delete(a.failures, ip)
	}
	return failures
}
