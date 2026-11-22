package transport

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// outcomeAuthHandler answers an AUTH whose payload is "good" with a password
// AUTH_OK, "token" with a token AUTH_OK, "badtoken" with a token failure, and
// any other AUTH with a credential failure, counting every AUTH it sees.
type outcomeAuthHandler struct {
	calls atomic.Int64
}

func (h *outcomeAuthHandler) handle(ctx context.Context, frame *Frame) (*Frame, error) {
	if frame.Opcode != OpAuth {
		return NewFrameWithID(frame.Opcode, frame.RequestID, frame.Payload), nil
	}
	h.calls.Add(1)
	failReason := AuthFailCredentials
	switch string(frame.Payload) {
	case "good":
		return authEchoHandler(ctx, frame)
	case "token":
		if sess, ok := SessionFromContext(ctx); ok {
			sess.SetIdentity(Identity{Username: "test", Resumed: true})
		}
		return NewFrameWithID(OpAuthOK, frame.RequestID, nil), nil
	case "badtoken":
		failReason = AuthFailTokenInvalid
	}
	payload, err := bson.Marshal(struct {
		Reason uint8 `bson:"reason"`
	}{Reason: uint8(failReason)})
	if err != nil {
		return nil, err
	}
	return NewFrameWithID(OpAuthFail, frame.RequestID, payload), nil
}

// authOn sends one AUTH frame carrying payload on conn and returns the reply.
func authOn(t *testing.T, conn *tls.Conn, reader *bufio.Reader, payload string) *Frame {
	t.Helper()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write(NewFrame(OpAuth, []byte(payload)).Encode()); err != nil {
		t.Fatalf("AUTH write: %v", err)
	}
	return mustDecode(t, reader)
}

// failureCount is the ledger's consecutive-failure count for ip, zero when it
// holds no record. Tests poll it instead of sleeping.
func (a *authAttemptLimiter) failureCount(ip string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	if rec := a.failures[ip]; rec != nil {
		return rec.count
	}
	return 0
}

// awaitLedgerCount blocks until the ledger counts want failures for the
// address conn dialled from. The ledger is updated before the AUTH reply is
// written, so this wait is belt and braces for a test that reads the count;
// TestTCPAuthLimitHoldsForBackToBackRetries is the one that proves the order.
func awaitLedgerCount(t *testing.T, ln *TCPListener, conn *tls.Conn, want int) {
	t.Helper()
	ip, _, err := net.SplitHostPort(conn.LocalAddr().String())
	if err != nil {
		t.Fatalf("splitting the client address %q: %v", conn.LocalAddr(), err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for ln.authLim.failureCount(ip) != want {
		if time.Now().After(deadline) {
			t.Fatalf("the ledger counted %d failures for %s after 5s; want %d", ln.authLim.failureCount(ip), ip, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// awaitLedgerApplied blocks until the ledger has finished with want AUTH
// outcomes in all, on any connection. The ledger is updated before the reply is
// written, so a test that has read the reply already holds the outcome; the
// wait stays for the tests that read the counter itself.
// Unlike awaitLedgerCount it also covers an outcome that leaves the count where
// it was.
func awaitLedgerApplied(t *testing.T, ln *TCPListener, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for ln.authLim.applied.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("the ledger had finished with %d AUTH outcomes after 5s; want %d", ln.authLim.applied.Load(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// ledgerRecordLag is how long the injected-delay variants hold back each
// ledger update. It is far above the 50 ms the test used to sleep.
const ledgerRecordLag = 300 * time.Millisecond

// TestFailedLoginLedgerHoldsAcrossConnections pins the per-IP ledger's
// lockout count when one address has several connections (spec 5.5). The
// "lagging" variants delay every ledger update, as a loaded host does; the
// reply waits for the update, so the next connection is judged against it.
func TestFailedLoginLedgerHoldsAcrossConnections(t *testing.T) {
	for _, between := range []string{"bad", "good", "token"} {
		for _, lag := range []time.Duration{0, ledgerRecordLag} {
			name := "between=" + between
			if lag > 0 {
				name += "/lagging"
			}
			t.Run(name, func(t *testing.T) {
				handler := &outcomeAuthHandler{}
				ln := startTestTCPListener(t, testTCPConfig(TCPConfig{AuthMaxFails: ledgerTestMaxFails}), handler.handle, nil)
				ln.authLim.recordDelay.Store(int64(lag))
				// The first good login's clear is slow here; its reply waits
				// for it, so the first failure cannot be wiped by it.
				ln.authLim.clearDelay.Store(int64(2 * lag))
				addr := ln.Addr().String()

				held := dialTestTCP(t, addr)
				heldReader := bufio.NewReader(held)
				if resp := authOn(t, held, heldReader, "good"); resp.Opcode != OpAuthOK {
					t.Fatalf("first AUTH on the held connection = %s, want AUTH_OK", resp.Opcode)
				}
				awaitLedgerApplied(t, ln, 1)

				counted := 0
				failOnNewConnection := func(waitForCount bool) Opcode {
					conn := dialTestTCP(t, addr)
					defer func() { _ = conn.Close() }()
					opcode := authOn(t, conn, bufio.NewReader(conn), "bad").Opcode
					if waitForCount {
						counted++
						awaitLedgerCount(t, ln, conn, counted)
					}
					return opcode
				}
				for i := 0; i < ledgerTestMaxFails-1; i++ {
					if got := failOnNewConnection(true); got != OpAuthFail {
						t.Fatalf("failure %d = %s, want AUTH_FAIL", i+1, got)
					}
				}
				// The held connection is already authenticated, so whatever it
				// sends here counts for nothing and clears nothing.
				authOn(t, held, heldReader, between)

				if got := failOnNewConnection(true); got != OpAuthFail {
					t.Fatalf("failure %d = %s, want AUTH_FAIL", ledgerTestMaxFails, got)
				}
				before := handler.calls.Load()
				failOnNewConnection(false)
				if handler.calls.Load() != before {
					t.Fatalf("the AUTH after %d failures reached the handler; the ledger should have refused it",
						ledgerTestMaxFails)
				}
			})
		}
	}
}

// TestOnlyAPasswordLoginClearsTheAddressCount pins which login on a new
// connection resets the per-IP failure count (spec 5.5): a password login
// does; a token login and a failed token login do not.
func TestOnlyAPasswordLoginClearsTheAddressCount(t *testing.T) {
	cases := []struct {
		login      string
		wantOpcode Opcode
		// failuresLeft is how many more credential failures the address may
		// make after the login before the next AUTH is refused.
		failuresLeft int
	}{
		{login: "good", wantOpcode: OpAuthOK, failuresLeft: ledgerTestMaxFails},
		{login: "token", wantOpcode: OpAuthOK, failuresLeft: 1},
		{login: "badtoken", wantOpcode: OpAuthFail, failuresLeft: 0},
	}
	for _, tc := range cases {
		t.Run("login="+tc.login, func(t *testing.T) {
			handler := &outcomeAuthHandler{}
			ln := startTestTCPListener(t, testTCPConfig(TCPConfig{AuthMaxFails: ledgerTestMaxFails}), handler.handle, nil)
			addr := ln.Addr().String()

			// counted is the failure count the ledger must reach after each
			// AUTH, or -1 for the last one, which the ledger refuses outright.
			counted := 0
			authOnNewConnection := func(payload string, countAfter func(int) int) Opcode {
				conn := dialTestTCP(t, addr)
				defer func() { _ = conn.Close() }()
				opcode := authOn(t, conn, bufio.NewReader(conn), payload).Opcode
				if countAfter != nil {
					counted = countAfter(counted)
					awaitLedgerCount(t, ln, conn, counted)
				}
				return opcode
			}
			plusOne := func(n int) int { return n + 1 }
			clearsTheCount := func(int) int { return 0 }
			unchanged := func(n int) int { return n }
			loginEffect := map[string]func(int) int{"good": clearsTheCount, "token": unchanged, "badtoken": plusOne}[tc.login]

			for i := 0; i < ledgerTestMaxFails-1; i++ {
				if got := authOnNewConnection("bad", plusOne); got != OpAuthFail {
					t.Fatalf("failure %d = %s, want AUTH_FAIL", i+1, got)
				}
			}
			if got := authOnNewConnection(tc.login, loginEffect); got != tc.wantOpcode {
				t.Fatalf("%s login = %s, want %s", tc.login, got, tc.wantOpcode)
			}
			for i := 0; i < tc.failuresLeft; i++ {
				if got := authOnNewConnection("bad", plusOne); got != OpAuthFail {
					t.Fatalf("failure %d after the %s login = %s, want AUTH_FAIL", i+1, tc.login, got)
				}
			}
			before := handler.calls.Load()
			authOnNewConnection("bad", nil)
			if handler.calls.Load() != before {
				t.Fatalf("after the %s login and %d more failures the next AUTH reached the handler; "+
					"the ledger should have refused it", tc.login, tc.failuresLeft)
			}
		})
	}
}

// backToBackAuths sends count AUTH frames, each the instant the previous reply
// is read, and does not wait for the ledger. A client that guesses passwords
// behaves this way. With sameConnection false each frame uses a new
// connection, as the driver-vector generator does. It returns every reply's
// opcode and AUTH_FAIL message.
func backToBackAuths(t *testing.T, ln *TCPListener, count int, sameConnection bool) (opcodes []Opcode, messages []string) {
	t.Helper()
	var conn *tls.Conn
	var reader *bufio.Reader
	for i := 0; i < count; i++ {
		if conn == nil || !sameConnection {
			conn = dialTestTCP(t, ln.Addr().String())
			reader = bufio.NewReader(conn)
			t.Cleanup(func() { _ = conn.Close() })
		}
		reply := authOn(t, conn, reader, "guess")
		var fail struct {
			Message string `bson:"message"`
		}
		if reply.Opcode == OpAuthFail && len(reply.Payload) > 0 {
			if err := bson.Unmarshal(reply.Payload, &fail); err != nil {
				t.Fatalf("AUTH %d: decoding the AUTH_FAIL payload: %v", i+1, err)
			}
		}
		opcodes = append(opcodes, reply.Opcode)
		messages = append(messages, fail.Message)
	}
	return opcodes, messages
}

// TestTCPAuthLimitHoldsForBackToBackRetries pins that the limiter refuses at
// exactly AuthMaxFails failures for a client that sends each AUTH the instant
// the previous reply arrives. The failure must be counted before its reply is
// written; counted after, the next AUTH is judged against the old count. The
// injected record delay stands in for a loaded host, which is why this test
// does not wait on awaitLedgerApplied.
func TestTCPAuthLimitHoldsForBackToBackRetries(t *testing.T) {
	reasons := map[string]AuthFailReason{
		"wrong password or unknown user": AuthFailCredentials,
		"rejected token":                 AuthFailTokenInvalid,
		"malformed request":              AuthFailProtocolMismatch,
	}
	for name, reason := range reasons {
		for _, sameConnection := range []bool{false, true} {
			subtest := name
			if sameConnection {
				subtest += "/one connection"
			}
			t.Run(subtest, func(t *testing.T) {
				t.Parallel()
				handler := &scriptedAuthHandler{script: repeatReason(reason, 10)}
				ln := startTestTCPListener(t, testTCPConfig(TCPConfig{AuthMaxFails: ledgerTestMaxFails}), handler.handle, nil)
				ln.authLim.recordDelay.Store(int64(100 * time.Millisecond))

				opcodes, messages := backToBackAuths(t, ln, ledgerTestMaxFails+1, sameConnection)
				if got := handler.calls.Load(); got != ledgerTestMaxFails {
					t.Fatalf("the handler saw %d AUTHs, want %d: the attempt after %d failures was judged before the last failure was counted",
						got, ledgerTestMaxFails, ledgerTestMaxFails)
				}
				if last := opcodes[ledgerTestMaxFails]; last != OpAuthFail {
					t.Fatalf("AUTH after %d failures = %s, want AUTH_FAIL from the ledger", ledgerTestMaxFails, last)
				}
				if !strings.Contains(messages[ledgerTestMaxFails], "failed logins") {
					t.Fatalf("the last reply %q is not the ledger's refusal", messages[ledgerTestMaxFails])
				}
			})
		}
	}
}

// parallelAuthAttempts is how many wrong-password AUTHs the parallel tests
// send against ledgerTestMaxFails, enough that an unbounded burst is plain.
const parallelAuthAttempts = 16

// authOutcomes is what a burst of AUTH frames got back.
type authOutcomes struct {
	handlerRefusals int // replies the handler produced
	rateLimited     int // AUTH_FAIL replies the ledger produced (RATE_LIMITED)
}

// countAuthReplies reads up to maxReplies replies from reader, stopping early
// when the connection ends, and sorts them by who refused: RATE_LIMITED comes
// from the ledger, since the scripted handler refuses with another reason.
// A connection the ledger refuses on is closed after its refusal, which is how
// a pipelined burst ends.
func countAuthReplies(t *testing.T, conn *tls.Conn, reader *bufio.Reader, maxReplies int) authOutcomes {
	t.Helper()
	var outcomes authOutcomes
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	for i := 0; i < maxReplies; i++ {
		reply, err := DecodeFrame(reader)
		if err != nil {
			return outcomes
		}
		if authFailReasonOf(reply) == AuthFailRateLimited {
			outcomes.rateLimited++
		} else {
			outcomes.handlerRefusals++
		}
	}
	return outcomes
}

// parallelAuths sends attempts AUTH frames that are in flight together and
// returns what came back. With sameConnection false each frame has a
// connection of its own and all are written before any reply is read; with it
// true all frames are written to one connection in a single write.
func parallelAuths(t *testing.T, ln *TCPListener, attempts int, sameConnection bool) authOutcomes {
	t.Helper()
	dialAuthConn := func() (*tls.Conn, *bufio.Reader) {
		conn := dialTestTCP(t, ln.Addr().String())
		t.Cleanup(func() { _ = conn.Close() })
		return conn, bufio.NewReader(conn)
	}
	var total authOutcomes
	if sameConnection {
		conn, reader := dialAuthConn()
		var burst []byte
		for i := 0; i < attempts; i++ {
			burst = append(burst, NewFrame(OpAuth, []byte("guess")).Encode()...)
		}
		if _, err := conn.Write(burst); err != nil {
			t.Fatalf("writing the pipelined AUTH burst: %v", err)
		}
		return countAuthReplies(t, conn, reader, attempts)
	}
	conns := make([]*tls.Conn, attempts)
	readers := make([]*bufio.Reader, attempts)
	for i := range conns {
		conns[i], readers[i] = dialAuthConn()
	}
	for i, conn := range conns {
		if _, err := conn.Write(NewFrame(OpAuth, []byte("guess")).Encode()); err != nil {
			t.Fatalf("AUTH %d: write: %v", i+1, err)
		}
	}
	for i, conn := range conns {
		outcomes := countAuthReplies(t, conn, readers[i], 1)
		total.handlerRefusals += outcomes.handlerRefusals
		total.rateLimited += outcomes.rateLimited
	}
	return total
}

// TestTCPAuthLimitHoldsForParallelAttempts pins that AUTH frames in flight
// together, from several connections of one address or pipelined on one
// connection, cannot get more than AuthMaxFails attempts to the handler. The
// limiter has to count an attempt from the moment it admits it, not when the
// handler finishes; admitted against the finished count, every frame of the
// burst sees the same low number. The record delay holds each handler's
// outcome back so the whole burst is in flight before any is counted.
func TestTCPAuthLimitHoldsForParallelAttempts(t *testing.T) {
	for _, sameConnection := range []bool{false, true} {
		name := "several connections"
		if sameConnection {
			name = "pipelined on one connection"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			handler := &scriptedAuthHandler{script: repeatReason(AuthFailCredentials, parallelAuthAttempts)}
			cfg := testTCPConfig(TCPConfig{AuthMaxFails: ledgerTestMaxFails, PipelineFrames: parallelAuthAttempts})
			ln := startTestTCPListener(t, cfg, handler.handle, nil)
			ln.authLim.recordDelay.Store(int64(300 * time.Millisecond))

			outcomes := parallelAuths(t, ln, parallelAuthAttempts, sameConnection)
			if got := handler.calls.Load(); got > ledgerTestMaxFails {
				t.Fatalf("%d of %d parallel AUTHs reached the handler, want at most %d (AuthMaxFails)",
					got, parallelAuthAttempts, ledgerTestMaxFails)
			}
			if outcomes.rateLimited == 0 {
				t.Fatalf("no parallel AUTH was refused with RATE_LIMITED: %+v", outcomes)
			}
			if outcomes.handlerRefusals > ledgerTestMaxFails {
				t.Fatalf("%d AUTHs were answered by the handler, want at most %d: %+v",
					outcomes.handlerRefusals, ledgerTestMaxFails, outcomes)
			}
		})
	}
}

// uncountedAuthHandler answers every AUTH other than "good" with an outcome the
// ledger does not count, as chosen by outcome, and "good" with a password
// AUTH_OK.
type uncountedAuthHandler struct {
	outcome string
}

func (h *uncountedAuthHandler) handle(ctx context.Context, frame *Frame) (*Frame, error) {
	if frame.Opcode != OpAuth {
		return NewFrameWithID(frame.Opcode, frame.RequestID, frame.Payload), nil
	}
	if string(frame.Payload) == "good" {
		return authEchoHandler(ctx, frame)
	}
	switch h.outcome {
	case "handler error":
		return nil, errors.New("test handler failure")
	case "token login":
		if sess, ok := SessionFromContext(ctx); ok {
			sess.SetIdentity(Identity{Username: "test", Resumed: true})
		}
		return NewFrameWithID(OpAuthOK, frame.RequestID, nil), nil
	}
	reason := AuthFailRateLimited
	if h.outcome == "already authed" {
		reason = AuthFailAlreadyAuthed
	}
	payload, err := bson.Marshal(struct {
		Reason uint8 `bson:"reason"`
	}{Reason: uint8(reason)})
	if err != nil {
		return nil, err
	}
	return NewFrameWithID(OpAuthFail, frame.RequestID, payload), nil
}

// TestTCPAuthReservationsAreReleasedWhenNothingIsCounted pins that an attempt
// admitted and then not counted gives its place back. A burst of AUTHs in
// flight together fills the limit with reservations; once they are settled, a
// correct password from the same address must reach the handler and log in. A
// reservation that leaked would lock the address out for good.
func TestTCPAuthReservationsAreReleasedWhenNothingIsCounted(t *testing.T) {
	for _, outcome := range []string{"busy refusal", "already authed", "token login", "handler error"} {
		t.Run(outcome, func(t *testing.T) {
			t.Parallel()
			handler := &uncountedAuthHandler{outcome: outcome}
			ln := startTestTCPListener(t, testTCPConfig(TCPConfig{AuthMaxFails: ledgerTestMaxFails}), handler.handle, nil)
			ln.authLim.holdDelay.Store(int64(200 * time.Millisecond))

			parallelAuths(t, ln, parallelAuthAttempts, false)

			conn := dialTestTCP(t, ln.Addr().String())
			t.Cleanup(func() { _ = conn.Close() })
			reply := authOn(t, conn, bufio.NewReader(conn), "good")
			if reply.Opcode != OpAuthOK {
				t.Fatalf("a correct password after %d %s attempts in flight = %s, want AUTH_OK: a reservation was not released",
					parallelAuthAttempts, outcome, reply.Opcode)
			}
		})
	}
}

// reservedAttempts is the number of admitted AUTH attempts from ip that the
// limiter has not settled.
func (a *authAttemptLimiter) reservedAttempts(ip string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	if rec := a.failures[ip]; rec != nil {
		return rec.inflight
	}
	return 0
}

// TestTCPAuthReservationIsReleasedWhenShutdownInterruptsTheDelay pins that an
// attempt admitted and then abandoned while it waits out the escalating delay
// gives its place back. Nothing else would: the attempt is never dispatched, so
// no handler outcome settles it.
func TestTCPAuthReservationIsReleasedWhenShutdownInterruptsTheDelay(t *testing.T) {
	handler := &scriptedAuthHandler{}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{AuthMaxFails: 10}), handler.handle, nil)
	const clientIP = "127.0.0.1"
	// authDelayStart failures put the next attempt in the delay, one second long.
	ln.authLim.failures[clientIP] = &ipFailureRecord{count: authDelayStart, lastFailure: time.Now()}

	conn := dialTestTCP(t, ln.Addr().String())
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.Write(NewFrame(OpAuth, []byte("guess")).Encode()); err != nil {
		t.Fatalf("AUTH write: %v", err)
	}
	awaitReserved(t, ln, clientIP, 1)

	if err := ln.Close(); err != nil {
		t.Fatalf("closing the listener: %v", err)
	}
	awaitReserved(t, ln, clientIP, 0)
	if got := handler.calls.Load(); got != 0 {
		t.Fatalf("the handler saw %d AUTHs, want 0: the attempt was abandoned in its delay", got)
	}
	if got := ln.authLim.failureCount(clientIP); got != authDelayStart {
		t.Fatalf("the ledger counts %d failures, want %d: releasing must not count the attempt", got, authDelayStart)
	}
}

// awaitReserved blocks until the limiter holds want unsettled attempts for ip.
func awaitReserved(t *testing.T, ln *TCPListener, ip string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for ln.authLim.reservedAttempts(ip) != want {
		if time.Now().After(deadline) {
			t.Fatalf("the limiter holds %d unsettled attempts for %s after 5s; want %d", ln.authLim.reservedAttempts(ip), ip, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestLedgerRefusalMessageSaysConnectionClosed pins that the ledger's
// RATE_LIMITED refusal tells the client the server is closing this connection
// and that it must reconnect after the window. The reason code is shared with
// the busy-server refusal, which keeps the connection open, so the text is the
// only thing that says which one a client received.
func TestLedgerRefusalMessageSaysConnectionClosed(t *testing.T) {
	msg := ledgerRefusalMessage(ledgerTestMaxFails)
	for _, want := range []string{"closing this connection", "reconnect"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("the ledger refusal %q does not contain %q; a client reading it would retry on the closed socket", msg, want)
		}
	}
}

// slowAuthHandler answers like outcomeAuthHandler after holding each AUTH for
// hold, the way an Argon2 derivation keeps a password login in flight. "token"
// is a token login, which the ledger neither counts nor clears.
type slowAuthHandler struct {
	hold         time.Duration
	wrongChecked atomic.Int64
	outcomes     outcomeAuthHandler
}

func (h *slowAuthHandler) handle(ctx context.Context, frame *Frame) (*Frame, error) {
	if frame.Opcode == OpAuth {
		if string(frame.Payload) == "guess" {
			h.wrongChecked.Add(1)
		}
		time.Sleep(h.hold)
	}
	return h.outcomes.handle(ctx, frame)
}

// burstLogins opens one connection per payload, writes every AUTH before any
// reply is read, and returns the replies in payload order.
func burstLogins(t *testing.T, ln *TCPListener, payloads []string) []*Frame {
	t.Helper()
	conns := make([]*tls.Conn, len(payloads))
	for i := range conns {
		conns[i] = dialTestTCP(t, ln.Addr().String())
		conn := conns[i]
		t.Cleanup(func() { _ = conn.Close() })
	}
	for i, conn := range conns {
		if _, err := conn.Write(NewFrame(OpAuth, []byte(payloads[i])).Encode()); err != nil {
			t.Fatalf("AUTH %d: write: %v", i+1, err)
		}
	}
	replies := make([]*Frame, len(conns))
	for i, conn := range conns {
		_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
		reply, err := DecodeFrame(bufio.NewReader(conn))
		if err != nil {
			t.Fatalf("AUTH %d of the burst: reading the reply: %v", i+1, err)
		}
		replies[i] = reply
	}
	return replies
}

// authFailMessageOf is the message an AUTH_FAIL reply carries, empty for any
// other reply.
func authFailMessageOf(t *testing.T, reply *Frame) string {
	t.Helper()
	if reply.Opcode != OpAuthFail || len(reply.Payload) == 0 {
		return ""
	}
	var fail struct {
		Message string `bson:"message"`
	}
	if err := bson.Unmarshal(reply.Payload, &fail); err != nil {
		t.Fatalf("decoding the AUTH_FAIL payload: %v", err)
	}
	return fail.Message
}

func repeatPayload(payload string, n int) []string {
	payloads := make([]string, n)
	for i := range payloads {
		payloads[i] = payload
	}
	return payloads
}

// The defaults a fresh server runs with, config.DefaultConfig's TCP.AuthMaxFails
// and TCP.MaxConnsPerIP; transport cannot import config, which imports it.
const (
	defaultServerAuthMaxFails  = 20
	defaultServerMaxConnsPerIP = 64
)

// TestTCPAuthBurstOfCorrectPasswordsIsAdmitted pins BUG-0697: a burst of
// correct-password logins from one address, larger than AuthMaxFails, is
// admitted on default settings. Logins still being checked hold a place under
// the limit, but one that finds the places taken only by them waits for them
// to settle instead of being refused as if they had failed.
func TestTCPAuthBurstOfCorrectPasswordsIsAdmitted(t *testing.T) {
	handler := &slowAuthHandler{hold: 100 * time.Millisecond}
	cfg := testTCPConfig(TCPConfig{AuthMaxFails: defaultServerAuthMaxFails, MaxConnsPerIP: defaultServerMaxConnsPerIP})
	ln := startTestTCPListener(t, cfg, handler.handle, nil)

	replies := burstLogins(t, ln, repeatPayload("good", defaultServerMaxConnsPerIP))
	refused := 0
	firstRefusal := ""
	for _, reply := range replies {
		if reply.Opcode == OpAuthOK {
			continue
		}
		refused++
		if firstRefusal == "" {
			firstRefusal = fmt.Sprintf("%s (reason %d): %q", reply.Opcode, authFailReasonOf(reply), authFailMessageOf(t, reply))
		}
	}
	if refused > 0 {
		t.Fatalf("%d of %d concurrent correct-password logins were refused with AuthMaxFails=%d and no failed login; first refusal: %s",
			refused, len(replies), defaultServerAuthMaxFails, firstRefusal)
	}
}

// TestLedgerRefusalMessageForInFlightOnlyDoesNotSayFailed pins that a login
// refused because too many from its address are already being checked says
// so, names the setting that raises the bound, and does not claim failed
// logins: none has failed. With AuthMaxFails=1 one login is checked and at most
// four wait, so a burst of seven correct passwords has some refused at once.
func TestLedgerRefusalMessageForInFlightOnlyDoesNotSayFailed(t *testing.T) {
	handler := &slowAuthHandler{hold: 300 * time.Millisecond}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{AuthMaxFails: 1}), handler.handle, nil)

	replies := burstLogins(t, ln, repeatPayload("good", 7))
	refused := 0
	for i, reply := range replies {
		if reply.Opcode == OpAuthOK {
			continue
		}
		refused++
		if reason := authFailReasonOf(reply); reason != AuthFailRateLimited {
			t.Fatalf("login %d = %s with reason %d, want AUTH_OK or RATE_LIMITED", i+1, reply.Opcode, reason)
		}
		message := authFailMessageOf(t, reply)
		if strings.Contains(message, "failed login") {
			t.Fatalf("login %d was refused with %q: it claims failed logins, but every password in the burst was correct", i+1, message)
		}
		for _, want := range []string{"being checked", "HOARDDB_TCP_AUTH_MAX_FAILS", "closing this connection"} {
			if !strings.Contains(message, want) {
				t.Fatalf("login %d was refused with %q, which does not contain %q", i+1, message, want)
			}
		}
	}
	if refused == 0 {
		t.Fatalf("all %d logins were admitted with AuthMaxFails=1: the queue of waiting logins is not bounded", len(replies))
	}
}

// TestTCPAuthLimitHoldsForWrongPasswordsInABurst pins that wrong passwords
// mixed into a burst of logins still reach the handler at most AuthMaxFails
// times. The logins that are not guesses are token logins, which neither count
// nor clear, so nothing in the burst may reopen the budget: whatever waits
// behind the guesses must be judged against what they counted.
func TestTCPAuthLimitHoldsForWrongPasswordsInABurst(t *testing.T) {
	handler := &slowAuthHandler{hold: 50 * time.Millisecond}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{AuthMaxFails: ledgerTestMaxFails}), handler.handle, nil)

	var payloads []string
	for i := 0; i < parallelAuthAttempts; i++ {
		payloads = append(payloads, "guess", "token")
	}
	replies := burstLogins(t, ln, payloads)
	if got := handler.wrongChecked.Load(); got > ledgerTestMaxFails {
		t.Fatalf("%d wrong passwords of a mixed burst reached the handler, want at most %d (AuthMaxFails)", got, ledgerTestMaxFails)
	}
	rateLimited := 0
	for _, reply := range replies {
		if authFailReasonOf(reply) == AuthFailRateLimited {
			rateLimited++
		}
	}
	if rateLimited == 0 {
		t.Fatalf("no login of a burst of %d with %d wrong passwords was refused with RATE_LIMITED", len(replies), parallelAuthAttempts)
	}
}

// waitingLogins is the number of logins from ip waiting for others in flight.
func (a *authAttemptLimiter) waitingLogins(ip string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	if rec := a.failures[ip]; rec != nil {
		return rec.waiting
	}
	return 0
}

// awaitWaiting blocks until the limiter holds want waiting logins for ip.
func awaitWaiting(t *testing.T, ln *TCPListener, ip string, want int, bound time.Duration) {
	t.Helper()
	deadline := time.Now().Add(bound)
	for ln.authLim.waitingLogins(ip) != want {
		if time.Now().After(deadline) {
			t.Fatalf("the limiter holds %d waiting logins for %s after %v; want %d", ln.authLim.waitingLogins(ip), ip, bound, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestTCPAuthWaitForLoginsInFlightIsBounded pins that a login waiting for
// others in flight is refused, with the in-flight text, once its wait runs out,
// and that neither the wait nor the refusal leaves anything in the ledger.
func TestTCPAuthWaitForLoginsInFlightIsBounded(t *testing.T) {
	handler := &slowAuthHandler{hold: 2 * time.Second}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{AuthMaxFails: 1}), handler.handle, nil)
	ln.authLim.inFlightWait.Store(int64(100 * time.Millisecond))
	const clientIP = "127.0.0.1"

	checked := dialTestTCP(t, ln.Addr().String())
	t.Cleanup(func() { _ = checked.Close() })
	if _, err := checked.Write(NewFrame(OpAuth, []byte("good")).Encode()); err != nil {
		t.Fatalf("AUTH write: %v", err)
	}
	awaitReserved(t, ln, clientIP, 1)

	waiter := dialTestTCP(t, ln.Addr().String())
	t.Cleanup(func() { _ = waiter.Close() })
	started := time.Now()
	reply := authOn(t, waiter, bufio.NewReader(waiter), "good")
	waited := time.Since(started)
	if reply.Opcode != OpAuthFail || authFailReasonOf(reply) != AuthFailRateLimited {
		t.Fatalf("a login behind one held for 2s with a 100ms wait = %s (reason %d), want AUTH_FAIL RATE_LIMITED",
			reply.Opcode, authFailReasonOf(reply))
	}
	if waited >= time.Second {
		t.Fatalf("the refusal came after %v; the wait is bounded at 100ms", waited)
	}
	if message := authFailMessageOf(t, reply); !strings.Contains(message, "1 being checked, 0 waiting") {
		t.Fatalf("the refusal %q does not say one login was being checked and none waited", message)
	}
	if got := ln.authLim.waitingLogins(clientIP); got != 0 {
		t.Fatalf("%d logins still wait after the refusal, want 0", got)
	}
	if got := ln.authLim.failureCount(clientIP); got != 0 {
		t.Fatalf("the ledger counts %d failures after an in-flight refusal, want 0: nothing failed", got)
	}
}

// TestTCPAuthWaitEndsWithTheListener pins that a login waiting for others in
// flight stops waiting when the listener closes, at once rather than when the
// login ahead of it settles, and is never admitted afterwards.
func TestTCPAuthWaitEndsWithTheListener(t *testing.T) {
	handler := &slowAuthHandler{hold: 3 * time.Second}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{AuthMaxFails: 1}), handler.handle, nil)
	const clientIP = "127.0.0.1"

	for i := 0; i < 2; i++ {
		conn := dialTestTCP(t, ln.Addr().String())
		t.Cleanup(func() { _ = conn.Close() })
		if _, err := conn.Write(NewFrame(OpAuth, []byte("good")).Encode()); err != nil {
			t.Fatalf("AUTH %d write: %v", i+1, err)
		}
		if i == 0 {
			awaitReserved(t, ln, clientIP, 1)
		}
	}
	awaitWaiting(t, ln, clientIP, 1, 5*time.Second)

	closed := make(chan error, 1)
	go func() { closed <- ln.Close() }()
	awaitWaiting(t, ln, clientIP, 0, time.Second)
	if err := <-closed; err != nil {
		t.Fatalf("closing the listener: %v", err)
	}
	if got := handler.outcomes.calls.Load(); got != 1 {
		t.Fatalf("the handler saw %d AUTHs, want 1: the waiting login was admitted after its connection ended", got)
	}
	awaitReserved(t, ln, clientIP, 0)
}

// TestTCPAuthInFlightRefusalIsAudited pins that a login refused because too
// many from its address are being checked is recorded by the audit hook, as
// every other refusal of the ledger is: a burst that saturates the address is
// worth an audit line even though nothing failed. The line says what held the
// login back and does not claim failures.
func TestTCPAuthInFlightRefusalIsAudited(t *testing.T) {
	audit := &auditRecorder{}
	deps := &SessionDeps{Logger: discardLogger(), Audit: audit.record}
	handler := &slowAuthHandler{hold: 300 * time.Millisecond}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{AuthMaxFails: 1}), handler.handle, deps)

	refused := 0
	for _, reply := range burstLogins(t, ln, repeatPayload("good", 7)) {
		if reply.Opcode != OpAuthOK {
			refused++
		}
	}
	if refused == 0 {
		t.Fatal("no login of the burst was refused; the test needs an in-flight refusal")
	}
	results := audit.snapshot()
	if len(results) != refused {
		t.Fatalf("%d of %d in-flight refusals were audited, want all: %q", len(results), refused, results)
	}
	for _, result := range results {
		if strings.Contains(result, "consecutive failures") {
			t.Fatalf("the in-flight refusal was audited as %q: it claims failures, but every password was correct", result)
		}
		for _, want := range []string{"login refused: 1 being checked", "waiting; no password was checked"} {
			if !strings.Contains(result, want) {
				t.Fatalf("the in-flight refusal was audited as %q, which does not contain %q", result, want)
			}
		}
	}
}

// auditRecorder collects the result texts the audit hook is given.
type auditRecorder struct {
	mu      sync.Mutex
	results []string
}

func (r *auditRecorder) record(_ string, result string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results = append(r.results, result)
}

func (r *auditRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.results...)
}

// TestTCPAuthCountedRefusalAuditIsUnchanged pins the audit text of a refusal
// for counted failures, byte for byte as it was before the hook carried text.
func TestTCPAuthCountedRefusalAuditIsUnchanged(t *testing.T) {
	audit := &auditRecorder{}
	deps := &SessionDeps{Logger: discardLogger(), Audit: audit.record}
	handler := &outcomeAuthHandler{}
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{AuthMaxFails: 1}), handler.handle, deps)

	conn := dialTestTCP(t, ln.Addr().String())
	t.Cleanup(func() { _ = conn.Close() })
	if reply := authOn(t, conn, bufio.NewReader(conn), "guess"); reply.Opcode != OpAuthFail {
		t.Fatalf("a wrong password = %s, want AUTH_FAIL", reply.Opcode)
	}
	awaitLedgerApplied(t, ln, 1)
	refusedConn := dialTestTCP(t, ln.Addr().String())
	t.Cleanup(func() { _ = refusedConn.Close() })
	if reply := authOn(t, refusedConn, bufio.NewReader(refusedConn), "good"); authFailReasonOf(reply) != AuthFailRateLimited {
		t.Fatalf("a login after AuthMaxFails failures = %s (reason %d), want RATE_LIMITED", reply.Opcode, authFailReasonOf(reply))
	}
	const want = "auth rate limit: 1 consecutive failures"
	if results := audit.snapshot(); len(results) != 1 || results[0] != want {
		t.Fatalf("the counted-failure refusal was audited as %q, want exactly [%q]", results, want)
	}
}
