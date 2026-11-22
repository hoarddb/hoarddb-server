package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// gateTestHandler always succeeds: AUTH answers OpAuthOK (marking the
// session via ServeFrame), everything else echoes the request opcode with
// the payload. The PING branch must never be reached — the gate answers
// PING inline — so the handler records a call instead.
type gateTestHandler struct {
	calls int
}

func (h *gateTestHandler) handle(ctx context.Context, frame *Frame) (*Frame, error) {
	h.calls++
	if frame.Opcode == OpAuth {
		// The real server sets identity from the record it verified; the
		// gate enforces that an AUTH_OK without one does not authenticate
		// (spec 3.7).
		if sess, ok := SessionFromContext(ctx); ok {
			sess.SetIdentity(Identity{Username: "test"})
		}
		return NewFrameWithID(OpAuthOK, frame.RequestID, nil), nil
	}
	return NewFrameWithID(frame.Opcode, frame.RequestID, frame.Payload), nil
}

func serveViaGate(t *testing.T, sess *Session, frame *Frame, h *gateTestHandler) (*Frame, error) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var buf bytes.Buffer
	err := ServeFrame(WithSession(context.Background(), sess), sess, frame, &buf, h.handle, logger)
	if err != nil {
		return nil, err
	}
	return DecodeFrame(bytes.NewReader(buf.Bytes()))
}

// wireErrorCode extracts the ErrorCode from an OpError payload (BSON
// {code: uint8, message: string} as written by WriteErrorResponse).
func wireErrorCode(t *testing.T, frame *Frame) ErrorCode {
	t.Helper()
	var doc struct {
		Code uint8 `bson:"code"`
	}
	if err := bson.Unmarshal(frame.Payload, &doc); err != nil {
		t.Fatalf("unmarshal OpError payload: %v", err)
	}
	return ErrorCode(doc.Code)
}

// TestServeFrameAuthGateOrder pins the gate order the extraction must
// preserve: an unauthenticated non-AUTH frame is refused with
// ErrNotAuthenticated and never reaches the handler.
func TestServeFrameAuthGateOrder(t *testing.T) {
	h := &gateTestHandler{}
	frame := NewFrame(OpGet, []byte("key"))
	resp, err := serveViaGate(t, &Session{}, frame, h)
	if err != nil {
		t.Fatalf("ServeFrame: %v", err)
	}
	if resp.Opcode != OpError {
		t.Fatalf("opcode = %s, want OpError", resp.Opcode)
	}
	if code := wireErrorCode(t, resp); code != ErrNotAuthenticated {
		t.Fatalf("error code = %s, want NOT_AUTHENTICATED", code)
	}
	if h.calls != 0 {
		t.Fatalf("handler called %d times for a gated frame; want 0", h.calls)
	}
}

// TestHealthCheckIsRefusedBeforeAuth: HEALTH_CHECK answers an authenticated
// session only. The health reply names the node's CPUs, memory and load, which
// are not shown to a caller that has not logged in.
func TestHealthCheckIsRefusedBeforeAuth(t *testing.T) {
	for _, op := range []Opcode{OpHealthCheck, OpBucketInfo} {
		h := &gateTestHandler{}
		resp, err := serveViaGate(t, &Session{}, NewFrame(op, nil), h)
		if err != nil {
			t.Fatalf("ServeFrame(%s): %v", op, err)
		}
		if resp.Opcode != OpError || wireErrorCode(t, resp) != ErrNotAuthenticated || h.calls != 0 {
			t.Errorf("%s before AUTH answered %s (handler calls %d), want NOT_AUTHENTICATED without reaching the handler", op, resp.Opcode, h.calls)
		}
	}
}

// TestServeFrameAuthMarksSession verifies a successful AUTH marks the
// session and subsequent frames pass the gate.
func TestServeFrameAuthMarksSession(t *testing.T) {
	h := &gateTestHandler{}
	sess := &Session{}

	resp, err := serveViaGate(t, sess, NewFrame(OpAuth, []byte("\x10username\x00admin\x00")), h)
	if err != nil {
		t.Fatalf("ServeFrame(AUTH): %v", err)
	}
	if resp.Opcode != OpAuthOK {
		t.Fatalf("opcode = %s, want AUTH_OK", resp.Opcode)
	}
	if !sess.IsAuthenticated() {
		t.Fatal("session not marked authenticated after AUTH_OK")
	}
	if id, ok := sess.Identity(); !ok || id.Username != "test" {
		t.Fatalf("identity = %+v ok=%v, want test", id, ok)
	}

	resp, err = serveViaGate(t, sess, NewFrame(OpGet, []byte("key")), h)
	if err != nil {
		t.Fatalf("ServeFrame(GET after auth): %v", err)
	}
	if resp.Opcode != OpGet {
		t.Fatalf("opcode = %s, want GET echo", resp.Opcode)
	}
}

// TestServeFrameAuthOKWithoutIdentityRefused pins the invariant: an AUTH_OK
// with no identity must not authenticate the session and must answer with an
// INTERNAL OpError (spec 3.7, acceptance 9.5).
func TestServeFrameAuthOKWithoutIdentityRefused(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sess := &Session{}
	anonymous := func(ctx context.Context, frame *Frame) (*Frame, error) {
		return NewFrameWithID(OpAuthOK, frame.RequestID, nil), nil
	}
	var buf bytes.Buffer
	if err := ServeFrame(WithSession(context.Background(), sess), sess,
		NewFrame(OpAuth, nil), &buf, anonymous, logger); err != nil {
		t.Fatalf("ServeFrame: %v", err)
	}
	resp, err := DecodeFrame(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("decode refusal: %v", err)
	}
	if resp.Opcode != OpError {
		t.Fatalf("opcode = %s, want OpError", resp.Opcode)
	}
	if code := wireErrorCode(t, resp); code != ErrInternal {
		t.Fatalf("error code = %s, want INTERNAL", code)
	}
	if sess.IsAuthenticated() {
		t.Fatal("anonymous AUTH_OK marked the session authenticated")
	}
}

// TestServeFramePingInline verifies PING is answered by the gate itself,
// even before AUTH, and never reaches the handler.
func TestServeFramePingInline(t *testing.T) {
	h := &gateTestHandler{}
	resp, err := serveViaGate(t, &Session{}, NewFrame(OpPing, nil), h)
	if err != nil {
		t.Fatalf("ServeFrame(PING): %v", err)
	}
	if resp.Opcode != OpPong {
		t.Fatalf("opcode = %s, want PONG", resp.Opcode)
	}
	if h.calls != 0 {
		t.Fatalf("PING reached the handler (%d calls); gate must answer inline", h.calls)
	}
}

// TestServeFrameHandlerErrorReturned verifies a handler error is returned
// to the caller (which terminates the transport) rather than swallowed.
func TestServeFrameHandlerErrorReturned(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sess := &Session{Authenticated: true}
	failing := func(ctx context.Context, frame *Frame) (*Frame, error) {
		return nil, io.ErrUnexpectedEOF
	}
	var buf bytes.Buffer
	err := ServeFrame(WithSession(context.Background(), sess), sess, NewFrame(OpGet, nil), &buf, failing, logger)
	if err == nil {
		t.Fatal("ServeFrame returned nil error for a failing handler")
	}
}

// TestSessionIdentityIsAnImmutableSnapshot verifies the guarantee Identity
// offers instead of copying (spec 3.7, acceptance 9.6; BUG-0098): SetIdentity
// copies its argument, publishes the whole identity atomically, and never
// mutates one already published, so a value a reader holds across a refresh
// does not change and costs no copy to obtain. A caller must not modify the
// returned slices; the copy on read is what BUG-0098 removed, and
// TestSessionIdentityReadDoesNotAllocate is the other half of this pair.
func TestSessionIdentityIsAnImmutableSnapshot(t *testing.T) {
	sess := &Session{}
	roles := []string{"admin"}
	dbs := []string{"*"}
	sess.SetIdentity(Identity{Username: "alice", Roles: roles, Databases: dbs})

	roles[0] = "mutated"
	dbs[0] = "mutated"
	first, ok := sess.Identity()
	if !ok {
		t.Fatal("Identity returned ok=false")
	}
	if first.Roles[0] != "admin" || first.Databases[0] != "*" {
		t.Fatalf("SetIdentity aliases the caller's slices: %v / %v", first.Roles, first.Databases)
	}

	sess.SetIdentity(Identity{Username: "alice", Roles: []string{"reader"}, Databases: []string{"app"}})
	if first.Roles[0] != "admin" || first.Databases[0] != "*" {
		t.Fatalf("a refresh rewrote the identity a reader already held: %v / %v", first.Roles, first.Databases)
	}
	second, _ := sess.Identity()
	if second.Roles[0] != "reader" || second.Databases[0] != "app" {
		t.Fatalf("a refresh was not visible to the next read: %v / %v", second.Roles, second.Databases)
	}
}

// TestSessionIdentityReadDoesNotAllocate is BUG-0098's failing test on the
// transport side: the session publishes its identity once and never mutates a
// published value, so reading it for the authorization gate costs no copies.
// Before the fix every read allocated the three copied slices (Roles,
// Databases, Grants) on every authorized frame.
func TestSessionIdentityReadDoesNotAllocate(t *testing.T) {
	sess := &Session{}
	sess.SetIdentity(Identity{
		Username:  "alice",
		Roles:     []string{"root"},
		Databases: []string{"*"},
		Grants:    []ScopedGrant{{Role: "root", DB: "*"}},
	})
	allocs := testing.AllocsPerRun(1000, func() {
		if _, ok := sess.Identity(); !ok {
			t.Fatal("Identity returned ok=false for an authenticated session")
		}
	})
	if allocs != 0 {
		t.Errorf("Identity allocated %.0f objects per read, want 0: a published identity is immutable, so a read must not copy its slices", allocs)
	}
}

// TestSessionIdentityReadIsRaceFreeUnderConcurrentRefresh is the -race half of
// spec 3.7 acceptance 9.6: PipelineFrames dispatches admitted frames
// concurrently, so a token refresh can replace the identity while other
// goroutines read it for the authorization gate and audit attribution. Every
// read must see one whole identity — never a username from one refresh beside
// roles from another.
func TestSessionIdentityReadIsRaceFreeUnderConcurrentRefresh(t *testing.T) {
	generation := func(n int) Identity {
		name := "user-" + strconv.Itoa(n)
		return Identity{
			Username:  name,
			Roles:     []string{"role-" + name},
			Databases: []string{"db-" + name},
			Grants:    []ScopedGrant{{Role: "role-" + name, DB: "db-" + name}},
		}
	}

	sess := &Session{}
	sess.SetIdentity(generation(0))

	const refreshes = 200
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(done)
		for n := 1; n <= refreshes; n++ {
			sess.SetIdentity(generation(n))
		}
	}()

	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				identity, ok := sess.Identity()
				if !ok {
					t.Error("Identity returned ok=false during a refresh")
					return
				}
				name := identity.Username
				if len(identity.Roles) != 1 || len(identity.Databases) != 1 || len(identity.Grants) != 1 {
					t.Errorf("an identity read during a refresh has %d roles, %d databases and %d grants; want one of each",
						len(identity.Roles), len(identity.Databases), len(identity.Grants))
					return
				}
				if identity.Roles[0] != "role-"+name || identity.Databases[0] != "db-"+name ||
					identity.Grants[0] != (ScopedGrant{Role: "role-" + name, DB: "db-" + name}) {
					t.Errorf("a torn identity: username %q, role %q, database %q, grant %+v",
						name, identity.Roles[0], identity.Databases[0], identity.Grants[0])
					return
				}
			}
		}()
	}
	wg.Wait()
}

// TestServeFrameEndsARevokedLogin: once the standing a session took at login
// is revoked, its next frame of any kind is answered NOT_AUTHENTICATED without
// reaching the handler, and ServeFrame returns ErrLoginRevoked so the caller
// closes the connection. Until then the frame is served as usual.
func TestServeFrameEndsARevokedLogin(t *testing.T) {
	for _, op := range []Opcode{OpGet, OpPing, OpAuth} {
		t.Run(op.String(), func(t *testing.T) {
			standing := &LoginStanding{}
			sess := &Session{Authenticated: true}
			sess.SetIdentity(Identity{Username: "alice", Standing: standing})
			h := &gateTestHandler{}
			if _, err := serveViaGate(t, sess, NewFrame(OpGet, nil), h); err != nil {
				t.Fatalf("a frame before the revocation: %v", err)
			}

			standing.Revoke()
			callsBefore := h.calls
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			var buf bytes.Buffer
			err := ServeFrame(WithSession(context.Background(), sess), sess, NewFrame(op, nil), &buf, h.handle, logger)
			if !errors.Is(err, ErrLoginRevoked) {
				t.Fatalf("ServeFrame after the revocation = %v, want ErrLoginRevoked", err)
			}
			if h.calls != callsBefore {
				t.Fatal("a revoked login's frame reached the handler")
			}
			answer, decodeErr := DecodeFrame(bytes.NewReader(buf.Bytes()))
			if decodeErr != nil {
				t.Fatalf("decode the refusal: %v", decodeErr)
			}
			if answer.Opcode != OpError || wireErrorCode(t, answer) != ErrNotAuthenticated {
				t.Fatalf("answer %s, want an OpError with NOT_AUTHENTICATED", answer.Opcode)
			}
		})
	}
}
