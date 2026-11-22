package transport

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// authEchoHandler authenticates every AUTH (answering OpAuthOK so the gate
// marks the session) and echoes any other frame's opcode with its payload. It
// sets an identity because the gate refuses an AUTH_OK without one.
func authEchoHandler(ctx context.Context, frame *Frame) (*Frame, error) {
	if frame.Opcode == OpAuth {
		if sess, ok := SessionFromContext(ctx); ok {
			sess.SetIdentity(Identity{Username: "test"})
		}
		return NewFrameWithID(OpAuthOK, frame.RequestID, nil), nil
	}
	return NewFrameWithID(frame.Opcode, frame.RequestID, frame.Payload), nil
}

func testTCPConfig(overrides TCPConfig) TCPConfig {
	cfg := TCPConfig{
		Listen:         "127.0.0.1:0",
		IdleTimeout:    5 * time.Second,
		FrameTimeout:   2 * time.Second,
		MaxConnsPerIP:  0,
		AuthMaxFails:   0,
		PipelineFrames: 4,
		PipelineBytes:  1 << 20,
	}
	if overrides.Listen != "" {
		cfg.Listen = overrides.Listen
	}
	if overrides.IdleTimeout != 0 {
		cfg.IdleTimeout = overrides.IdleTimeout
	}
	if overrides.FrameTimeout != 0 {
		cfg.FrameTimeout = overrides.FrameTimeout
	}
	if overrides.MaxConnsPerIP != 0 {
		cfg.MaxConnsPerIP = overrides.MaxConnsPerIP
	}
	if overrides.AuthMaxFails != 0 {
		cfg.AuthMaxFails = overrides.AuthMaxFails
	}
	if overrides.PipelineFrames != 0 {
		cfg.PipelineFrames = overrides.PipelineFrames
	}
	if overrides.PipelineBytes != 0 {
		cfg.PipelineBytes = overrides.PipelineBytes
	}
	cfg.PeerMode = overrides.PeerMode
	return cfg
}

func startTestTCPListener(t *testing.T, cfg TCPConfig, handler RequestHandler, deps *SessionDeps) *TCPListener {
	t.Helper()
	return startTestTCPListenerWithLimiter(t, cfg, handler, deps, NewConnLimiter(64, 64))
}

func startTestTCPListenerWithLimiter(t *testing.T, cfg TCPConfig, handler RequestHandler, deps *SessionDeps, limiter *ConnLimiter) *TCPListener {
	t.Helper()
	if deps == nil {
		deps = &SessionDeps{Logger: discardLogger()}
	}
	keyDir := t.TempDir()
	tlsCfg, _, err := MakeServerTLSConfig(keyDir)
	if err != nil {
		t.Fatalf("server TLS config: %v", err)
	}
	ln, err := ListenTCP(cfg.Listen, tlsCfg, limiter, cfg, handler, deps)
	if err != nil {
		t.Fatalf("ListenTCP: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return ln
}

// dialTestTCP opens a TCP client with the pinned-key client config. By
// default it offers the served hoarddb ALPN (a real driver does); pass
// nextProtos explicitly for the ALPN-gate cases.
func dialTestTCP(t *testing.T, addr string, nextProtos ...string) *tls.Conn {
	t.Helper()
	tlsCfg, err := MakeClientTLSConfig("", "", TrustPolicy{Insecure: &InsecureTrust{Reason: "transport test client"}}, addr)
	if err != nil {
		t.Fatalf("client TLS config: %v", err)
	}
	if len(nextProtos) == 0 {
		nextProtos = []string{ALPNFor(ProtocolMajor)}
	}
	tlsCfg.NextProtos = nextProtos
	conn, err := tls.Dial("tcp", addr, tlsCfg)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// dialTestTCPNoALPN dials without offering any ALPN identifier — legal TLS,
// and exactly the case the negotiated-ALPN gate exists to refuse.
func dialTestTCPNoALPN(t *testing.T, addr string) *tls.Conn {
	t.Helper()
	tlsCfg, err := MakeClientTLSConfig("", "", TrustPolicy{Insecure: &InsecureTrust{Reason: "transport test client"}}, addr)
	if err != nil {
		t.Fatalf("client TLS config: %v", err)
	}
	tlsCfg.NextProtos = nil
	conn, err := tls.Dial("tcp", addr, tlsCfg)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func mustDecode(t *testing.T, r io.Reader) *Frame {
	t.Helper()
	f, err := DecodeFrame(r)
	if err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	return f
}

func wireErrorOf(t *testing.T, frame *Frame) (code ErrorCode, message string) {
	t.Helper()
	var doc struct {
		Code    int    `bson:"code"`
		Message string `bson:"message"`
	}
	if err := bson.Unmarshal(frame.Payload, &doc); err != nil {
		t.Fatalf("unmarshal OpError payload: %v", err)
	}
	return ErrorCode(doc.Code), doc.Message
}

// TestTCPFrameRoundTrip drives a full AUTH + request/response cycle over a
// real listener. The auth frame is required first: the same gate QUIC
// enforces must hold over TCP.
func TestTCPFrameRoundTrip(t *testing.T) {
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{}), authEchoHandler, nil)
	conn := dialTestTCP(t, ln.Addr().String())
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReader(conn)

	if _, err := conn.Write(NewFrame(OpAuth, []byte("admin")).Encode()); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	if resp := mustDecode(t, br); resp.Opcode != OpAuthOK {
		t.Fatalf("auth response opcode = %s, want AUTH_OK", resp.Opcode)
	}

	get := NewFrame(OpGet, []byte("hello"))
	if _, err := conn.Write(get.Encode()); err != nil {
		t.Fatalf("write get: %v", err)
	}
	resp := mustDecode(t, br)
	if resp.Opcode != OpGet {
		t.Fatalf("response opcode = %s, want GET echo", resp.Opcode)
	}
	if resp.RequestID != get.RequestID {
		t.Fatalf("response request ID %x does not echo request %x", resp.RequestID, get.RequestID)
	}
	if !bytes.Equal(resp.Payload, []byte("hello")) {
		t.Fatalf("payload = %q, want %q", resp.Payload, "hello")
	}
}

// TestTCPUnauthenticatedRefused verifies a non-AUTH frame before AUTH gets
// the NOT_AUTHENTICATED named refusal and the connection is closed.
func TestTCPUnauthenticatedRefused(t *testing.T) {
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{}), authEchoHandler, nil)
	conn := dialTestTCP(t, ln.Addr().String())
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReader(conn)

	if _, err := conn.Write(NewFrame(OpGet, nil).Encode()); err != nil {
		t.Fatalf("write: %v", err)
	}
	resp := mustDecode(t, br)
	if resp.Opcode != OpError {
		t.Fatalf("response opcode = %s, want OpError", resp.Opcode)
	}
	if code, _ := wireErrorOf(t, resp); code != ErrNotAuthenticated {
		t.Fatalf("error code = %s, want NOT_AUTHENTICATED", code)
	}
}

// TestTCPOversizedLengthRefused verifies the length cap inherited from the
// shared codec: a header claiming more than MaxFrameLength closes the
// connection before any body allocation.
func TestTCPOversizedLengthRefused(t *testing.T) {
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{}), authEchoHandler, nil)
	conn := dialTestTCP(t, ln.Addr().String())
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	hdr := make([]byte, 5)
	hdr[0] = ProtocolVersion
	binary.LittleEndian.PutUint32(hdr[1:], MaxFrameLength+1)
	if _, err := conn.Write(hdr); err != nil {
		t.Fatalf("write oversized header: %v", err)
	}
	var one [1]byte
	if _, err := conn.Read(one[:]); err == nil {
		t.Fatal("connection stayed open after an oversized length; want close")
	}
}

// TestTCPFrameDeadline verifies a client that starts a frame and stalls
// mid-body is closed within the frame timeout, not left holding a goroutine.
func TestTCPFrameDeadline(t *testing.T) {
	cfg := testTCPConfig(TCPConfig{FrameTimeout: 500 * time.Millisecond})
	ln := startTestTCPListener(t, cfg, authEchoHandler, nil)
	conn := dialTestTCP(t, ln.Addr().String())

	// Header claims a body that is never sent.
	hdr := make([]byte, 5)
	hdr[0] = ProtocolVersion
	binary.LittleEndian.PutUint32(hdr[1:], 1+RequestIDLen+100)
	if _, err := conn.Write(hdr); err != nil {
		t.Fatalf("write header: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	start := time.Now()
	var one [1]byte
	if _, err := conn.Read(one[:]); err == nil {
		t.Fatal("connection stayed open after stalling mid-frame; want close")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("connection closed after %v; want close within the frame timeout (~500ms)", elapsed)
	}
}

// TestTCPIdleDeadlineResetsOnTraffic verifies the idle timeout measures the
// gap between frames, not the connection's age (spec 5.7). A driver whose
// session outlives the idle timeout while it is continuously busy — the 10M
// leg of the benchmark is one — must not be cut off mid-request; only a
// connection that genuinely goes quiet is closed.
func TestTCPIdleDeadlineResetsOnTraffic(t *testing.T) {
	cfg := testTCPConfig(TCPConfig{IdleTimeout: 500 * time.Millisecond, FrameTimeout: 2 * time.Second})
	ln := startTestTCPListener(t, cfg, authEchoHandler, nil)
	conn := dialTestTCP(t, ln.Addr().String())
	br := bufio.NewReader(conn)

	if _, err := conn.Write(NewFrame(OpAuth, []byte("admin")).Encode()); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	if resp := mustDecode(t, br); resp.Opcode != OpAuthOK {
		t.Fatalf("auth response opcode = %s, want AUTH_OK", resp.Opcode)
	}

	// Keep the connection busy for three idle timeouts, one request every
	// 100ms. A deadline fixed at connection start cuts this client off at
	// roughly 500ms, which is the bug this test exists for.
	start := time.Now()
	stop := start.Add(1500 * time.Millisecond)
	requests := 0
	for time.Now().Before(stop) {
		if _, err := conn.Write(NewFrame(OpGet, []byte("busy")).Encode()); err != nil {
			t.Fatalf("write request %d after %v of traffic: %v", requests, time.Since(start), err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		if resp := mustDecode(t, br); resp.Opcode != OpGet {
			t.Fatalf("response opcode = %s, want GET echo", resp.Opcode)
		}
		requests++
		time.Sleep(100 * time.Millisecond)
	}
	if requests < 10 {
		t.Fatalf("only %d requests completed in 1.5s; the loop is not exercising the timeout", requests)
	}

	// Now go quiet: the timeout must still close a connection with no traffic.
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	idleStart := time.Now()
	var one [1]byte
	if _, err := conn.Read(one[:]); err == nil {
		t.Fatal("idle connection stayed open past the idle timeout")
	}
	if elapsed := time.Since(idleStart); elapsed > 3*time.Second {
		t.Fatalf("idle connection closed after %v; want close within the idle timeout (~500ms)", elapsed)
	}
}

// TestTCPWindowThrottles is the spec-section-6 proof: a client that writes
// 100 frames without reading is throttled to the window — the handler never
// runs more than PipelineFrames times concurrently — and every response
// still arrives exactly once with a request ID the client sent.
func TestTCPWindowThrottles(t *testing.T) {
	const frames = 100
	cfg := testTCPConfig(TCPConfig{PipelineFrames: 2})

	release := make(chan struct{})
	var cur, maxInHandler atomic.Int64
	blockingHandler := func(ctx context.Context, frame *Frame) (*Frame, error) {
		if frame.Opcode == OpAuth {
			if sess, ok := SessionFromContext(ctx); ok {
				sess.SetIdentity(Identity{Username: "test"})
			}
			return NewFrameWithID(OpAuthOK, frame.RequestID, nil), nil
		}
		n := cur.Add(1)
		for {
			m := maxInHandler.Load()
			if n <= m || maxInHandler.CompareAndSwap(m, n) {
				break
			}
		}
		<-release
		cur.Add(-1)
		return NewFrameWithID(frame.Opcode, frame.RequestID, frame.Payload), nil
	}

	ln := startTestTCPListener(t, cfg, blockingHandler, nil)
	conn := dialTestTCP(t, ln.Addr().String())
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	br := bufio.NewReader(conn)

	if _, err := conn.Write(NewFrame(OpAuth, nil).Encode()); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	if resp := mustDecode(t, br); resp.Opcode != OpAuthOK {
		t.Fatalf("auth response opcode = %s, want AUTH_OK", resp.Opcode)
	}

	sent := make(map[[RequestIDLen]byte]bool, frames)
	for i := 0; i < frames; i++ {
		f := NewFrame(OpGet, []byte("x"))
		sent[f.RequestID] = true
		if _, err := conn.Write(f.Encode()); err != nil {
			t.Fatalf("write frame %d: %v", i, err)
		}
	}

	// Give the server time to read as far as the window allows, then prove
	// the throttling held: no matter how many frames the client flung, the
	// server never ran more than the window's worth of handlers at once.
	time.Sleep(100 * time.Millisecond)
	if got := maxInHandler.Load(); got > int64(cfg.PipelineFrames) {
		t.Fatalf("handler concurrency peaked at %d, want ≤ %d (read-ahead window)", got, cfg.PipelineFrames)
	}

	close(release)
	seen := make(map[[RequestIDLen]byte]int, frames)
	for i := 0; i < frames; i++ {
		resp := mustDecode(t, br)
		if resp.Opcode != OpGet {
			t.Fatalf("response %d opcode = %s, want GET", i, resp.Opcode)
		}
		if !sent[resp.RequestID] {
			t.Fatalf("response %d carries unknown request ID %x", i, resp.RequestID)
		}
		seen[resp.RequestID]++
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("request %x answered %d times; want exactly once", id, n)
		}
	}
}

// TestTCPWindowSequentialW1 proves PipelineFrames=1 is strict lockstep:
// a client that fires 20 frames before reading gets responses back in
// send order, because the server never has more than one frame in flight.
func TestTCPWindowSequentialW1(t *testing.T) {
	const n = 20
	cfg := testTCPConfig(TCPConfig{PipelineFrames: 1})
	ln := startTestTCPListener(t, cfg, authEchoHandler, nil)
	conn := dialTestTCP(t, ln.Addr().String())
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	br := bufio.NewReader(conn)

	if _, err := conn.Write(NewFrame(OpAuth, nil).Encode()); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	if resp := mustDecode(t, br); resp.Opcode != OpAuthOK {
		t.Fatalf("auth response opcode = %s, want AUTH_OK", resp.Opcode)
	}

	for i := 0; i < n; i++ {
		f := NewFrame(OpGet, []byte{byte(i)})
		if _, err := conn.Write(f.Encode()); err != nil {
			t.Fatalf("write frame %d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		resp := mustDecode(t, br)
		if resp.Opcode != OpGet {
			t.Fatalf("response %d opcode = %s, want GET", i, resp.Opcode)
		}
		if len(resp.Payload) != 1 || resp.Payload[0] != byte(i) {
			t.Fatalf("response %d payload = %v, want %d — responses must stay in send order at W=1", i, resp.Payload, i)
		}
	}
}

// TestTCPNodeToNodeRefused verifies a node-to-node opcode over TCP is
// refused by name (spec 7): a driver bug must be debuggable, not silent.
func TestTCPNodeToNodeRefused(t *testing.T) {
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{}), authEchoHandler, nil)
	conn := dialTestTCP(t, ln.Addr().String())
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReader(conn)

	if _, err := conn.Write(NewFrame(OpGossip, []byte("view")).Encode()); err != nil {
		t.Fatalf("write gossip: %v", err)
	}
	resp := mustDecode(t, br)
	if resp.Opcode != OpError {
		t.Fatalf("response opcode = %s, want OpError", resp.Opcode)
	}
	code, message := wireErrorOf(t, resp)
	if code != ErrInvalidArgument {
		t.Fatalf("error code = %s, want INVALID_ARGUMENT", code)
	}
	if !strings.Contains(message, "GOSSIP") {
		t.Fatalf("refusal %q does not name the refused opcode", message)
	}
}

// TestTCPALPNRefusedWithSupportedList verifies the negotiated-ALPN gate: a
// client that offers no ALPN completes the TLS handshake but is refused
// before any frame, with the supported version named (spec 3.1 rule 1).
func TestTCPALPNRefusedWithSupportedList(t *testing.T) {
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{}), authEchoHandler, nil)
	conn := dialTestTCPNoALPN(t, ln.Addr().String())
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReader(conn)

	resp := mustDecode(t, br)
	if resp.Opcode != OpError {
		t.Fatalf("response opcode = %s, want OpError", resp.Opcode)
	}
	code, message := wireErrorOf(t, resp)
	if code != ErrUnsupportedVersion {
		t.Fatalf("error code = %s, want UNSUPPORTED_VERSION", code)
	}
	if !strings.Contains(message, ALPNv1) {
		t.Fatalf("refusal %q does not name the supported version", message)
	}
}

// TestTCPAuthRateLimitDelaysAfterThreshold verifies spec 5.5 end to end:
// after 5 consecutive failed AUTHs from one IP, the sixth attempt is
// delayed (1s at count 5) before any auth work happens.
func TestTCPAuthRateLimitDelaysAfterThreshold(t *testing.T) {
	cfg := testTCPConfig(TCPConfig{AuthMaxFails: 20})
	failingAuth := func(ctx context.Context, frame *Frame) (*Frame, error) {
		return NewFrameWithID(OpAuthFail, frame.RequestID, nil), nil
	}
	ln := startTestTCPListener(t, cfg, failingAuth, nil)
	ln.authLim.recordDelay.Store(int64(ledgerRecordLag))

	finishedAuths := int64(0)
	doFailedAuth := func() {
		conn := dialTestTCP(t, ln.Addr().String())
		_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
		br := bufio.NewReader(conn)
		if _, err := conn.Write(NewFrame(OpAuth, nil).Encode()); err != nil {
			t.Fatalf("write auth: %v", err)
		}
		if resp := mustDecode(t, br); resp.Opcode != OpAuthFail {
			t.Fatalf("response opcode = %s, want AUTH_FAIL", resp.Opcode)
		}
		finishedAuths++
		awaitLedgerApplied(t, ln, finishedAuths)
		_ = conn.Close()
	}

	for i := 0; i < 5; i++ {
		doFailedAuth()
	}
	start := time.Now()
	doFailedAuth()
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Fatalf("6th auth attempt was not delayed: %v; want ≥ 1s (count reached the delay threshold)", elapsed)
	}
}

// TestAuthAttemptLimiterRefusalAndDecay unit-tests the lockout behaviour at
// the ledger level so the 20-failure outright refusal and the 15-minute
// decay never need a 31-second integration test to prove.
func TestAuthAttemptLimiterRefusalAndDecay(t *testing.T) {
	a := newAuthAttemptLimiter(20)
	ctx := context.Background()
	judged := func(ip string) authAdmission {
		t.Helper()
		admission, err := a.beforeAuth(ctx, ip)
		if err != nil {
			t.Fatalf("beforeAuth(%s): %v", ip, err)
		}
		return admission
	}

	for i := 0; i < 20; i++ {
		if judged("192.0.2.1").refused {
			t.Fatalf("attempt %d was refused before the limit", i+1)
		}
		a.settle("192.0.2.1", authLedgerCount)
	}
	if got := judged("192.0.2.1"); !got.refused || got.delay != 0 || got.failures != 20 || got.checking != 0 {
		t.Fatalf("at 20 failures: want outright refusal (delay=0 refused=true failures=20 checking=0), got %+v", got)
	}
	// An outstanding auth inside the now-open 60s refusal window is refused
	// without needing to pass the threshold again.
	if !judged("192.0.2.1").refused {
		t.Fatal("attempt inside the refusal window must be refused")
	}

	// A password login that was admitted before the window opened still
	// settles inside it.
	a.failures["192.0.2.1"].inflight++
	a.settle("192.0.2.1", authLedgerClear)
	if got := judged("192.0.2.1"); got.refused || got.failures != 0 {
		t.Fatalf("success should clear the ledger; got %+v", got)
	}

	// Decay: an entry whose last failure is older than the interval is
	// forgotten, so a shared NAT is not locked out by stale typos.
	a.failures["192.0.2.2"] = &ipFailureRecord{count: 30, lastFailure: time.Now().Add(-20 * time.Minute)}
	if got := judged("192.0.2.2"); got.refused || got.failures != 0 {
		t.Fatalf("stale entry should decay; got %+v", got)
	}
}

// TestTCPPerIPCap verifies HOARDDB_TCP_MAX_CONNS_PER_IP: a second connection
// from the same source is refused with a named OpError while the cap holds.
func TestTCPPerIPCap(t *testing.T) {
	cfg := testTCPConfig(TCPConfig{MaxConnsPerIP: 1})
	ln := startTestTCPListener(t, cfg, authEchoHandler, nil)

	first := dialTestTCP(t, ln.Addr().String())
	defer func() { _ = first.Close() }()
	_ = first.SetDeadline(time.Now().Add(10 * time.Second))

	// A finished client handshake does not mean the server has counted this
	// connection: tls.Dial returns once the client side is done, while the
	// server's serveConn may not have reached the per-IP check yet. If the
	// second dial were served first, the second would be admitted, the first
	// refused, and the second would sit idle until the server's idle timeout
	// closed it (the "read frame header: EOF"). An AUTH round
	// trip is answered only after the server has admitted the connection, so
	// it proves the first one holds the single slot.
	if _, err := first.Write(NewFrame(OpAuth, []byte("any")).Encode()); err != nil {
		t.Fatalf("write auth on the first connection: %v", err)
	}
	if admitted := mustDecode(t, bufio.NewReader(first)); admitted.Opcode != OpAuthOK {
		t.Fatalf("first connection response opcode = %s, want AUTH_OK (it must hold the slot before the second dials)", admitted.Opcode)
	}

	second := dialTestTCP(t, ln.Addr().String())
	_ = second.SetDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReader(second)
	resp := mustDecode(t, br)
	if resp.Opcode != OpError {
		t.Fatalf("response opcode = %s, want OpError (per-IP cap)", resp.Opcode)
	}
	code, message := wireErrorOf(t, resp)
	if code != ErrOverloaded {
		t.Fatalf("error code = %s, want OVERLOADED", code)
	}
	if !strings.Contains(message, "HOARDDB_TCP_MAX_CONNS_PER_IP") {
		t.Fatalf("refusal %q does not name the per-IP cap", message)
	}
	_ = second.Close()
}

// TestTCPPeerModeServesNodeToNode is the mirror of the driver-port
// refusal: on the internode port (PeerMode) a node-to-node opcode is
// served, not refused by name.
func TestTCPPeerModeServesNodeToNode(t *testing.T) {
	cfg := testTCPConfig(TCPConfig{PeerMode: true})
	ln := startTestTCPListener(t, cfg, authEchoHandler, nil)
	conn := dialTestTCP(t, ln.Addr().String())
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReader(conn)

	// The internode port still requires AUTH before anything else — same
	// gate, same first-frame contract as every transport.
	if _, err := conn.Write(NewFrame(OpAuth, []byte("any")).Encode()); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	if resp := mustDecode(t, br); resp.Opcode != OpAuthOK {
		t.Fatalf("auth response opcode = %s, want AUTH_OK", resp.Opcode)
	}

	if _, err := conn.Write(NewFrame(OpReplPush, []byte("entry")).Encode()); err != nil {
		t.Fatalf("write repl push: %v", err)
	}
	resp := mustDecode(t, br)
	if resp.Opcode == OpError {
		t.Fatalf("peer-mode listener refused REPL_PUSH: %s", resp.Payload)
	}
	if resp.Opcode != OpReplPush {
		t.Fatalf("response opcode = %s, want the REPL_PUSH echo", resp.Opcode)
	}
}

// TestTCPPeerModeDrawsPeerBudget verifies the internode port's accept
// budget is the reserved peer budget: with the client budget full (a
// driver flood), a peer connects fine, a second peer is refused only by
// the peer cap, and the client budget stays untouched.
func TestTCPPeerModeDrawsPeerBudget(t *testing.T) {
	lim := NewConnLimiter(1, 1)
	cfg := testTCPConfig(TCPConfig{PeerMode: true})
	ln := startTestTCPListenerWithLimiter(t, cfg, authEchoHandler, nil, lim)

	// The flood: the single client slot is held.
	clientRelease, ok := lim.AcquireClient()
	if !ok {
		t.Fatal("seed client acquire failed")
	}
	defer clientRelease()

	// First peer is admitted — peer capacity, not client capacity.
	peer1 := dialTestTCP(t, ln.Addr().String())
	_ = peer1.SetDeadline(time.Now().Add(10 * time.Second))

	// Second peer hits the peer cap and is refused with the peer knob named.
	peer2 := dialTestTCP(t, ln.Addr().String())
	_ = peer2.SetDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReader(peer2)
	resp := mustDecode(t, br)
	if resp.Opcode != OpError {
		t.Fatalf("second peer response = %s, want OpError (peer cap)", resp.Opcode)
	}
	code, message := wireErrorOf(t, resp)
	if code != ErrOverloaded {
		t.Fatalf("error code = %s, want OVERLOADED", code)
	}
	if !strings.Contains(message, "HOARDDB_MAX_PEER_CONNS") {
		t.Fatalf("refusal %q does not name the peer cap", message)
	}
	_ = peer2.Close()

	// The client budget is intact: the flood's slot is still held, and a
	// fresh client acquire must still fail.
	if _, ok := lim.AcquireClient(); ok {
		t.Fatal("peer-mode listener drew client capacity: client slot freed")
	}
	_ = peer1.Close()
}

// TestInternodePeerAddrDerivation verifies the udp→tcp port+1 convention
// both the listener (from the QUIC listen address) and the dialer (from
// the ring member address) rely on.
func TestInternodePeerAddrDerivation(t *testing.T) {
	got, err := InternodePeerAddr("127.0.0.1:4433")
	if err != nil {
		t.Fatalf("InternodePeerAddr: %v", err)
	}
	if got != "127.0.0.1:4434" {
		t.Fatalf("InternodePeerAddr(127.0.0.1:4433) = %q, want 127.0.0.1:4434", got)
	}
	got, err = InternodePeerAddr("[::1]:4433")
	if err != nil {
		t.Fatalf("InternodePeerAddr v6: %v", err)
	}
	if got != "[::1]:4434" {
		t.Fatalf("InternodePeerAddr([::1]:4433) = %q, want [::1]:4434", got)
	}
	if _, err := InternodePeerAddr("not-an-address"); err == nil {
		t.Fatal("InternodePeerAddr accepted a garbage address")
	}
}

// scriptedAuthHandler answers the n-th AUTH it receives with script[n]: a
// reason code to refuse with, or zero to authenticate. Past the end of the
// script it authenticates. calls counts every AUTH that reached it, which is
// how a test tells a handler refusal from the per-IP ledger's own refusal.
type scriptedAuthHandler struct {
	script []AuthFailReason
	calls  atomic.Int64
	// markUnchecked marks every scripted refusal as made before any
	// credential was checked, the way the real handler marks a refusal for an
	// unavailable auth config (BUG-0237).
	markUnchecked bool
}

func (h *scriptedAuthHandler) handle(ctx context.Context, frame *Frame) (*Frame, error) {
	if frame.Opcode != OpAuth {
		return NewFrameWithID(frame.Opcode, frame.RequestID, frame.Payload), nil
	}
	n := int(h.calls.Add(1)) - 1
	if n < len(h.script) {
		payload, err := bson.Marshal(struct {
			Reason  uint8  `bson:"reason"`
			Message string `bson:"message"`
		}{Reason: uint8(h.script[n]), Message: "scripted refusal"})
		if err != nil {
			return nil, err
		}
		refusal := NewFrameWithID(OpAuthFail, frame.RequestID, payload)
		if h.markUnchecked {
			refusal.MarkCredentialUnchecked()
		}
		return refusal, nil
	}
	return authEchoHandler(ctx, frame)
}

// sendAuths sends count AUTH frames one after another, each on a new
// connection from 127.0.0.1, and returns every response opcode with its
// AUTH_FAIL message. A connection the ledger closes is fine: the next AUTH
// dials again.
func sendAuths(t *testing.T, ln *TCPListener, count int) (opcodes []Opcode, messages []string) {
	t.Helper()
	finished := int64(0)
	for i := 0; i < count; i++ {
		conn := dialTestTCP(t, ln.Addr().String())
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write(NewFrame(OpAuth, nil).Encode()); err != nil {
			t.Fatalf("AUTH %d: write: %v", i+1, err)
		}
		resp := mustDecode(t, bufio.NewReader(conn))
		var fail struct {
			Message string `bson:"message"`
		}
		if resp.Opcode == OpAuthFail && len(resp.Payload) > 0 {
			_ = bson.Unmarshal(resp.Payload, &fail)
		}
		opcodes = append(opcodes, resp.Opcode)
		messages = append(messages, fail.Message)
		// A refusal by the ledger itself reached no outcome to wait for; every
		// other reply is followed by an update of the ledger, which must land
		// before the next AUTH is judged.
		if !strings.Contains(fail.Message, "too many failed logins") {
			finished++
			awaitLedgerApplied(t, ln, finished)
		}
		_ = conn.Close()
	}
	return opcodes, messages
}

// repeatReason returns n copies of reason.
func repeatReason(reason AuthFailReason, n int) []AuthFailReason {
	reasons := make([]AuthFailReason, n)
	for i := range reasons {
		reasons[i] = reason
	}
	return reasons
}

// ledgerTestMaxFails sits below authDelayStart so these tests exercise the
// outright refusal without paying the escalating delay before it.
const ledgerTestMaxFails = 4

// TestBusyAuthRefusalsDoNotLockOutTheIP: RATE_LIMITED from the
// handler means the password was not checked, so 25 of them in a row are not
// failed logins and the IP's next AUTH still reaches the handler and succeeds.
func TestBusyAuthRefusalsDoNotLockOutTheIP(t *testing.T) {
	handler := &scriptedAuthHandler{script: repeatReason(AuthFailRateLimited, 25)}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{AuthMaxFails: ledgerTestMaxFails}), handler.handle, nil)

	opcodes, messages := sendAuths(t, ln, 26)
	if got := handler.calls.Load(); got != 26 {
		t.Fatalf("handler saw %d of 26 AUTHs; the per-IP ledger refused the rest (last message %q)",
			got, messages[len(messages)-1])
	}
	if last := opcodes[25]; last != OpAuthOK {
		t.Fatalf("AUTH after 25 busy refusals = %s %q, want AUTH_OK", last, messages[25])
	}
}

// TestConfigUnavailableAuthRefusalsDoNotLockOutTheIP: a refusal the handler
// makes before checking any credential (the auth config is unavailable) is
// reported as INTERNAL, which also covers real server errors, so the reason
// code cannot say "not checked". The handler's explicit marker does; 25 such
// refusals are not failed logins and the IP's next AUTH still gets through.
func TestConfigUnavailableAuthRefusalsDoNotLockOutTheIP(t *testing.T) {
	handler := &scriptedAuthHandler{script: repeatReason(AuthFailInternal, 25), markUnchecked: true}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{AuthMaxFails: ledgerTestMaxFails}), handler.handle, nil)

	opcodes, messages := sendAuths(t, ln, 26)
	if got := handler.calls.Load(); got != 26 {
		t.Fatalf("handler saw %d of 26 AUTHs; the per-IP ledger refused the rest (last message %q)",
			got, messages[len(messages)-1])
	}
	if last := opcodes[25]; last != OpAuthOK {
		t.Fatalf("AUTH after 25 config-unavailable refusals = %s %q, want AUTH_OK", last, messages[25])
	}
}

// TestUnmarkedInternalAuthRefusalStillCounts is the other half: an INTERNAL
// refusal the handler did not mark (a server error after a credential was
// checked) is still a failed login, so the marker cannot be inferred from the
// reason code.
func TestUnmarkedInternalAuthRefusalStillCounts(t *testing.T) {
	handler := &scriptedAuthHandler{script: repeatReason(AuthFailInternal, 10)}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{AuthMaxFails: ledgerTestMaxFails}), handler.handle, nil)
	ln.authLim.recordDelay.Store(int64(ledgerRecordLag))

	_, messages := sendAuths(t, ln, ledgerTestMaxFails+1)
	if got := handler.calls.Load(); got != ledgerTestMaxFails {
		t.Fatalf("handler saw %d AUTHs, want %d: unmarked INTERNAL refusals must count toward the lockout",
			got, ledgerTestMaxFails)
	}
	if !strings.Contains(messages[ledgerTestMaxFails], "failed logins") {
		t.Fatalf("ledger refusal %q does not say it was failed logins", messages[ledgerTestMaxFails])
	}
}

// TestWrongPasswordLockoutUnchanged guards the brute-force
// ledger: credential failures still count, and after AuthMaxFails of them
// the IP's next AUTH is refused before it reaches the handler.
func TestWrongPasswordLockoutUnchanged(t *testing.T) {
	handler := &scriptedAuthHandler{script: repeatReason(AuthFailCredentials, 10)}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{AuthMaxFails: ledgerTestMaxFails}), handler.handle, nil)
	ln.authLim.recordDelay.Store(int64(ledgerRecordLag))

	opcodes, messages := sendAuths(t, ln, ledgerTestMaxFails+1)
	if got := handler.calls.Load(); got != ledgerTestMaxFails {
		t.Fatalf("handler saw %d AUTHs, want %d: the AUTH after %d wrong passwords must be refused by the ledger",
			got, ledgerTestMaxFails, ledgerTestMaxFails)
	}
	if last := opcodes[ledgerTestMaxFails]; last != OpAuthFail {
		t.Fatalf("AUTH after %d wrong passwords = %s, want AUTH_FAIL from the ledger", ledgerTestMaxFails, last)
	}
	if !strings.Contains(messages[ledgerTestMaxFails], "failed logins") {
		t.Fatalf("ledger refusal %q does not say it was failed logins", messages[ledgerTestMaxFails])
	}
}

// TestBusyAuthRefusalsGiveNoExtraGuesses guards against using
// busy refusals to launder a guessing run: they neither count nor reset the
// count, so wrong passwords interleaved with any number of them still reach
// the lockout after exactly AuthMaxFails credential failures.
func TestBusyAuthRefusalsGiveNoExtraGuesses(t *testing.T) {
	var script []AuthFailReason
	for i := 0; i < ledgerTestMaxFails; i++ {
		script = append(script, AuthFailCredentials)
		script = append(script, repeatReason(AuthFailRateLimited, 5)...)
	}
	script = append(script, repeatReason(AuthFailCredentials, 10)...)
	handler := &scriptedAuthHandler{script: script}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{AuthMaxFails: ledgerTestMaxFails}), handler.handle, nil)
	ln.authLim.recordDelay.Store(int64(ledgerRecordLag))

	// Each round is one wrong password then five busy refusals. The ledger
	// locks the IP on the AuthMaxFails-th wrong password, so that round's busy
	// refusals never reach the handler: (max-1) full rounds, then one wrong
	// password, then the ledger's refusal.
	reached := (ledgerTestMaxFails-1)*6 + 1
	opcodes, _ := sendAuths(t, ln, reached+1)
	if got := handler.calls.Load(); got != int64(reached) {
		t.Fatalf("handler saw %d AUTHs, want %d: busy refusals must neither count nor reset the wrong-password count",
			got, reached)
	}
	if last := opcodes[reached]; last != OpAuthFail {
		t.Fatalf("AUTH after %d wrong passwords = %s, want AUTH_FAIL from the ledger", ledgerTestMaxFails, last)
	}
}
