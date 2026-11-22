package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// oversizedResponses counts handler responses the frame-length guard in
// ServeFrame replaced with RESULT_TOO_LARGE. Every one is a server bug: the
// query accounting that should have refused the result missed a retention.
var oversizedResponses atomic.Uint64

// OversizedResponses reports how many handler responses this process has
// replaced with RESULT_TOO_LARGE because their frame would have exceeded
// MaxFrameLength. Any non-zero value is a server bug.
func OversizedResponses() uint64 {
	return oversizedResponses.Load()
}

// ErrResponseWrite is a response the connection could not take: its write
// failed or missed its deadline. After one, a TLS connection fails every later
// write too, so the client is treated as gone (query-pipelining spec §3.4):
// the caller cancels the connection's work and closes it, and writes nothing
// more to it.
var ErrResponseWrite = errors.New("writing the response failed; the connection cannot be answered on")

// responseWriteFailed wraps a failed response write in ErrResponseWrite.
func responseWriteFailed(what string, op Opcode, err error) error {
	return fmt.Errorf("%w: %s %s: %w", ErrResponseWrite, what, op, err)
}

// ErrLoginRevoked ends a connection whose login the server revoked: its
// account was dropped, disabled or given new credentials after it logged in.
// ServeFrame has already answered the frame with NOT_AUTHENTICATED; the
// caller closes the connection without writing anything more.
var ErrLoginRevoked = errors.New("the login behind this connection was revoked")

// LoginStanding is shared by the sessions one account opened under one set of
// credentials. The server revokes it when that account is dropped, disabled
// or given new credentials, on every node that applies the change, and the
// frame gate ends those sessions at their next frame. A role change does not
// revoke it: roles reach an open connection at its next login.
type LoginStanding struct {
	revoked atomic.Bool
}

// Revoke marks the standing revoked; every session holding it is ended.
func (s *LoginStanding) Revoke() {
	s.revoked.Store(true)
}

// Revoked reports whether the standing was revoked. A nil standing, which the
// node identity has, is never revoked.
func (s *LoginStanding) Revoked() bool {
	return s != nil && s.revoked.Load()
}

// RequestHandler processes a decoded frame and returns a response.
type RequestHandler func(ctx context.Context, frame *Frame) (*Frame, error)

// Session is the per-connection state the frame gate needs, with no
// transport type in it — the TCP listener is the only transport left, but
// this stays transport-agnostic on purpose: before this extraction there
// was a QUIC gate and a second copy on the TCP path, and two gates drift
// apart (the failure mode that already produced a dead duplicate dispatcher
// in this repo).
type Session struct {
	// Authenticated is true after a successful AUTH (Option B token auth).
	Authenticated bool
	// RemoteAddr is the peer address for logging and per-IP limiting. Fixed
	// at construction.
	RemoteAddr string
	// Peer is true when the connection arrived on the internode listener
	// (PeerMode); alone it means only that, not that the session is a cluster
	// member. It is one of the two conditions in
	// server/authz.go:clusterMemberSession, which also requires the NodeUser
	// identity: server/dispatch.go:handleAuth accepts the __node__ credential
	// only here and refuses user credentials here. Carried on Session, it
	// costs handleAuth no sixth parameter.
	Peer bool

	mu sync.Mutex // protects Authenticated and the AUTH claim
	// identity is the authenticated user, set by the server's AUTH handler
	// through the context (transport cannot import server). It is published as
	// a whole immutable snapshot: SetIdentity stores a new value and never
	// mutates one a reader may hold, so a read needs no mutex and no copy
	// (spec 3.7, BUG-0098).
	identity atomic.Pointer[Identity]
	// passwordAuthClaimed records that this connection's one password AUTH
	// has been used. It is claimed before the derivation runs so concurrent
	// pipelined AUTH frames cannot each pay for one (spec 3.8).
	passwordAuthClaimed bool
	// peerNode is the member node id this internode connection speaks for:
	// the sender named by its first request that names one (BindPeerNode).
	// Guarded by mu. One string per connection, nothing per bucket.
	peerNode      string
	peerNodeBound bool
}

// BindPeerNode ties the connection to the member node id its requests name
// as their sender. The first call binds claimed; every later call reports
// whether claimed is the bound id, and returns the bound id. The cluster
// token every member holds says only that the caller is a member, so without
// this one connection could speak for several members in turn, moving what
// the receiver records for each of them.
func (s *Session) BindPeerNode(claimed string) (bound string, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.peerNodeBound {
		s.peerNode, s.peerNodeBound = claimed, true
	}
	return s.peerNode, s.peerNode == claimed
}

// ScopedGrant is one {role, db} pair carried on a session identity. The wire
// claims stay flat for driver compatibility (auth-token-spec.md 3.7), but the
// authorization gate needs the pairing the record actually holds, so it is
// reconstructed at AUTH and frozen on the session for that connection.
type ScopedGrant struct {
	Role string
	DB   string
}

// Identity is who a connection is. It is written once by a password AUTH and
// replaced by a token refresh, and read for audit attribution, per-frame
// logging, and the authorization gate. The value a session publishes is
// immutable: SetIdentity copies its argument and installs a whole new value, so
// a reader may hold the slices without copying them and must not modify them.
type Identity struct {
	Username string
	// Roles and Databases are the flat claim lists, kept for the wire and for
	// audit. Authorization reads Grants, not these: matching the two lists
	// independently lets a role in one database satisfy another.
	Roles     []string
	Databases []string
	// Grants is the typed, database-scoped grant set frozen at AUTH. An empty
	// set fails closed for every non-ActionNone rule.
	Grants         []ScopedGrant
	TokenExpiresAt time.Time
	// Resumed is true when the session authenticated by token, false by
	// password. It is recorded in the audit log rather than left as a
	// decorative field.
	Resumed bool
	// Standing is the account's login standing, taken at AUTH together with
	// the credentials checked. One atomic load per frame tells the gate the
	// account is still as it was; it costs nothing per bucket.
	Standing *LoginStanding
	// Incarnation is the account's created_seq in a cluster's user table (the
	// commit that created it), so a user command names the account the
	// session logged in as and not a later one of the same name (ring user
	// store spec 4.2 B17). Zero on a node without a cluster.
	Incarnation int64
}

// BeginPasswordAuth claims this connection's single password AUTH. It returns
// false when the connection already authenticated or already used its password
// claim, including when a concurrent pipelined AUTH frame raced it. Token
// AUTH does not go through this claim: a refresh is allowed on an
// authenticated connection.
//
// Pipelined password guessing has two bounds, and neither may be weakened on
// the strength of the other. This claim is the per-connection bound: one
// checked password per connection (BUG-0236, pinned by
// TestPipelinedPasswordAuthChecksOnePasswordPerConnection). The per-IP
// ledger's in-flight reservation is the per-address bound across connections
// (pinned by TestTCPAuthLimitHoldsForParallelAttempts).
func (s *Session) BeginPasswordAuth() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Authenticated || s.passwordAuthClaimed {
		return false
	}
	s.passwordAuthClaimed = true
	return true
}

// AbandonPasswordAuth gives back the claim taken by BeginPasswordAuth when the
// AUTH that took it was refused before its password was checked, such as a
// busy derivation gate. The claim bounds checked passwords per
// connection; no password was checked, so keeping it would cost the client
// its only attempt for nothing. It must never be called once a password has
// been checked: that would give the connection a second guess.
func (s *Session) AbandonPasswordAuth() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.passwordAuthClaimed = false
}

// SetIdentity records the authenticated user. The slices are copied so the
// caller cannot mutate the session's identity afterwards, and the whole
// identity is published in one atomic store, so a concurrent read sees either
// all of the old identity or all of the new one (spec 3.7, BUG-0098).
func (s *Session) SetIdentity(id Identity) {
	s.identity.Store(&Identity{
		Username:       id.Username,
		Roles:          append([]string(nil), id.Roles...),
		Databases:      append([]string(nil), id.Databases...),
		Grants:         append([]ScopedGrant(nil), id.Grants...),
		TokenExpiresAt: id.TokenExpiresAt,
		Resumed:        id.Resumed,
		Standing:       id.Standing,
		Incarnation:    id.Incarnation,
	})
}

// Identity returns the session identity. Its Roles, Databases and Grants are
// the published snapshot's own slices, which must not be modified: SetIdentity
// replaces the snapshot and never writes one a reader may hold, so a reader
// may keep them across a token refresh without a race (spec 3.7). That
// immutability is what makes a copy-free read safe, and a copy of three short
// slices on every authorized frame was the per-call cost BUG-0098 removed;
// PipelineFrames dispatches admitted frames concurrently, so the publication
// has to stay atomic.
func (s *Session) Identity() (Identity, bool) {
	snapshot := s.identity.Load()
	if snapshot == nil {
		return Identity{}, false
	}
	return *snapshot, true
}

// sessionContextKey is a private context key so no other package can collide
// with it.
type sessionContextKey struct{}

// WithSession attaches the connection's Session to its context. The server's
// AUTH handler reads it to set identity without transport importing server.
func WithSession(ctx context.Context, sess *Session) context.Context {
	return context.WithValue(ctx, sessionContextKey{}, sess)
}

// SessionFromContext returns the Session attached by WithSession.
func SessionFromContext(ctx context.Context) (*Session, bool) {
	sess, ok := ctx.Value(sessionContextKey{}).(*Session)
	return sess, ok
}

// servedBeforeLogin reports whether a connection that has not logged in may
// send op. AUTH and PING are the only requests a client makes before login;
// AUTH_OK, AUTH_FAIL and PONG are what the server sends back, so a client that
// sends one is refused like any other request.
func servedBeforeLogin(op Opcode) bool {
	return op == OpAuth || op == OpPing
}

// MarkAuthenticated records a successful AUTH. Authenticated is written by
// the AUTH path and read by the gate for every other frame, so the write
// and the reads must not race.
func (s *Session) MarkAuthenticated() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Authenticated = true
}

// loginRevoked reports whether the login this session holds was revoked
// since it authenticated.
func (s *Session) loginRevoked() bool {
	snapshot := s.identity.Load()
	return snapshot != nil && snapshot.Standing.Revoked()
}

// IsAuthenticated reports whether AUTH has succeeded on this session.
func (s *Session) IsAuthenticated() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Authenticated
}

// ServeFrame applies the per-connection auth gate to one decoded frame and
// writes its response. It returns the handler's error, if any; a fatal
// handler error is answered by the caller (TCP: an OpError frame, and the
// connection closed once the requests already in flight on it have answered
// or the frame timeout has passed). A handler that holds an answer, a
// refusal included, returns it with a nil error: the frame returned with an
// error is not written. A write that fails returns ErrResponseWrite,
// after the response's written hook has run.
func ServeFrame(ctx context.Context, sess *Session, frame *Frame, w io.Writer,
	handler RequestHandler, log *slog.Logger) error {

	// Auth gate: block non-AUTH frames until authenticated. Option B
	// (token-based) requires this; Option A (client certs) would already be
	// authenticated at the TLS layer.
	if !sess.IsAuthenticated() && !servedBeforeLogin(frame.Opcode) {
		log.Warn("unauthenticated stream rejected",
			"addr", sess.RemoteAddr,
			"opcode", frame.Opcode,
		)
		if err := WriteErrorResponse(w, ErrNotAuthenticated,
			"connection not authenticated; send AUTH first",
			frame.RequestID); err != nil {
			return responseWriteFailed("refusing an unauthenticated", frame.Opcode, err)
		}
		return nil
	}

	if sess.loginRevoked() {
		log.Info("ending a connection whose login was revoked", "addr", sess.RemoteAddr, "opcode", frame.Opcode)
		if err := WriteErrorResponse(w, ErrNotAuthenticated,
			"this connection's login is no longer valid: the account was dropped, disabled or given a new password; connect and log in again",
			frame.RequestID); err != nil {
			return responseWriteFailed("refusing a revoked login's", frame.Opcode, err)
		}
		return ErrLoginRevoked
	}

	// Handle PING directly without going through the handler.
	if frame.Opcode == OpPing {
		pong := NewFrameWithID(OpPong, frame.RequestID, nil)
		if err := WriteFrame(w, pong); err != nil {
			return responseWriteFailed("answering", frame.Opcode, err)
		}
		return nil
	}

	if frame.Opcode == OpAuth {
		return serveAuthFrame(ctx, authFrameRequest{Session: sess, Frame: frame, Writer: w, Handler: handler, Log: log})
	}

	// Dispatch to the handler. A handler that returns an error has released
	// whatever its response would have held, so nothing is written and no
	// written hook fires.
	resp, err := handler(ctx, frame)
	if err != nil {
		log.Error("handler error",
			"addr", sess.RemoteAddr,
			"opcode", frame.Opcode,
			"error", err,
		)
		return err
	}
	// The response's written hook fires exactly once, after the last write
	// below has returned, whether it succeeded or failed (spec §3.1).
	if hook := resp.TakeWrittenHook(); hook != nil {
		defer hook()
	}

	// The frame-length guard (query-pipelining spec §3.3). The peer's
	// DecodeFrame refuses a body over MaxFrameLength and cannot find the next
	// frame after it, so writing one would lose the connection and every
	// request in flight on it. The body is measured as DecodeFrame measures
	// it: opcode, request ID and payload, not the header.
	if bodyLen := 1 + RequestIDLen + len(resp.Payload); bodyLen > MaxFrameLength {
		oversizedResponses.Add(1)
		message := fmt.Sprintf("the %s response is %d bytes, over the %d MiB frame limit, so it was not sent. "+
			"This is a server bug: the result should have been refused before it grew this large. "+
			"Report it with the request that caused it; a smaller request (a lower limit, or a narrower filter) "+
			"avoids it meanwhile", frame.Opcode, bodyLen, MaxFrameLength>>20)
		log.Error("response over the frame limit replaced by RESULT_TOO_LARGE; this is a server bug",
			"addr", sess.RemoteAddr,
			"opcode", frame.Opcode,
			"bytes", bodyLen,
			"limit", MaxFrameLength,
		)
		if err := WriteErrorResponse(w, ErrResultTooLarge, message, frame.RequestID); err != nil {
			return responseWriteFailed("refusing the oversized response to", frame.Opcode, err)
		}
		return nil
	}

	if err := WriteFrame(w, resp); err != nil {
		return responseWriteFailed("answering", frame.Opcode, err)
	}
	return nil
}

// authLedgerEffect is what one AUTH frame does to the per-IP failed-login
// ledger (spec 5.5).
type authLedgerEffect uint8

const (
	// authLedgerCount records a failed login. It is the zero value so an
	// outcome nobody classified fails closed.
	authLedgerCount authLedgerEffect = iota
	// authLedgerClear is a frame that logged the connection in by password.
	authLedgerClear
	// authLedgerUntouched is a frame that neither logged anyone in nor
	// checked a credential that failed.
	authLedgerUntouched
)

// authFrameRequest is one AUTH frame and everything serveAuthFrame needs to
// answer it.
type authFrameRequest struct {
	Session *Session
	Frame   *Frame
	Writer  io.Writer
	Handler RequestHandler
	Log     *slog.Logger
	// Record, when set, receives the frame's effect on the per-IP ledger. It
	// runs before the reply is written, so a client that sends its next AUTH
	// the instant it reads the reply is judged against a ledger that already
	// holds this outcome.
	Record func(authLedgerEffect)
}

// serveAuthFrame runs one AUTH frame through the handler, marks the session
// authenticated on AUTH_OK, reports the frame's effect on the per-IP ledger,
// and then writes the response.
//
// Only a password login clears the ledger (spec 5.5); a token login leaves it
// as it was. A refusal that checked no credential
// (RATE_LIMITED: spec §4 item 3e; ALREADY_AUTHED: the connection's
// one password claim was already taken; or any refusal the handler marked
// with MarkCredentialUnchecked) is not counted.
func serveAuthFrame(ctx context.Context, req authFrameRequest) error {
	sess, frame, log := req.Session, req.Frame, req.Log
	record := func(effect authLedgerEffect) {
		if req.Record != nil {
			req.Record(effect)
		}
	}

	wasAuthenticated := sess.IsAuthenticated()
	resp, err := req.Handler(ctx, frame)
	if err != nil {
		log.Error("auth handler error", "addr", sess.RemoteAddr, "error", err)
		return err
	}
	effect := authLedgerCount
	if resp.Opcode == OpAuthOK {
		if _, ok := sess.Identity(); !ok {
			// An authenticated-but-anonymous session is worse than a failed
			// auth: audit would record nobody. Refuse loudly and do not mark
			// the session authenticated (spec 3.7).
			log.Error("AUTH_OK without a session identity; refusing to authenticate",
				"addr", sess.RemoteAddr)
			if wasAuthenticated {
				record(authLedgerUntouched)
			} else {
				record(authLedgerCount)
			}
			if err := WriteErrorResponse(req.Writer, ErrInternal,
				"server bug: authenticated session has no identity",
				frame.RequestID); err != nil {
				return responseWriteFailed("refusing an identity-less AUTH_OK to", frame.Opcode, err)
			}
			return nil
		}
		sess.MarkAuthenticated()
		method := "password"
		effect = authLedgerClear
		if id, ok := sess.Identity(); ok && id.Resumed {
			method = "token"
			effect = authLedgerUntouched
		}
		log.Info("client authenticated", "addr", sess.RemoteAddr, "auth_method", method)
	}
	switch reason := authFailReasonOf(resp); {
	case wasAuthenticated:
		effect = authLedgerUntouched
	case reason == AuthFailRateLimited || reason == AuthFailAlreadyAuthed:
		effect = authLedgerUntouched
	case resp.CredentialUnchecked():
		// INTERNAL covers server errors after a check as well, so the reason
		// code cannot say "not checked"; the handler's marker does (BUG-0237).
		effect = authLedgerUntouched
	}
	record(effect)
	if err := WriteFrame(req.Writer, resp); err != nil {
		return responseWriteFailed("answering", frame.Opcode, err)
	}
	return nil
}

// authFailReasonOf returns the reason code of an AUTH_FAIL response, or the
// zero value (never a valid reason) for any other response or an AUTH_FAIL
// whose payload carries none. Unknown means "count it": the ledger fails
// closed.
func authFailReasonOf(resp *Frame) AuthFailReason {
	if resp.Opcode != OpAuthFail || len(resp.Payload) == 0 {
		return 0
	}
	var fail struct {
		Reason uint8 `bson:"reason"`
	}
	if err := bson.Unmarshal(resp.Payload, &fail); err != nil {
		return 0
	}
	return AuthFailReason(fail.Reason)
}
