package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// authCall sends an AUTH frame and returns the raw response plus the decoded
// AuthResponse when the server answered AUTH_OK.
func (c *tcpServerClient) authCall(req *AuthRequest) (*transport.Frame, AuthResponse, AuthFailResponse) {
	c.t.Helper()
	var resp AuthResponse
	frame := c.call(transport.OpAuth, req, &resp)

	var fail AuthFailResponse
	if frame.Opcode == transport.OpAuthFail && len(frame.Payload) > 0 {
		if err := bson.Unmarshal(frame.Payload, &fail); err != nil {
			c.t.Fatalf("decode AUTH_FAIL payload: %v", err)
		}
	}
	return frame, resp, fail
}

func hasCapability(caps []string, want string) bool {
	for _, cap := range caps {
		if cap == want {
			return true
		}
	}
	return false
}

// TestAuthTokenResumeIntegration drives the real TCP server: password AUTH
// mints a token, a new connection resumes with it, and the response carries
// both expiry numbers and the auth_token capability (spec 9.2).
func TestAuthTokenResumeIntegration(t *testing.T) {
	_, addr := newTCPEnabledServer(t, nil)

	c1 := dialTCPServer(t, addr)
	frame, resp, _ := c1.authCall(&AuthRequest{Username: "admin", Password: "test123"})
	if frame.Opcode != transport.OpAuthOK {
		t.Fatalf("password AUTH opcode = %s, want AUTH_OK", frame.Opcode)
	}
	if resp.Token == "" {
		t.Fatal("AUTH_OK carried no token")
	}
	if resp.TokenExpiresAt <= time.Now().Unix() {
		t.Fatalf("token_expires_at = %d, want a future time", resp.TokenExpiresAt)
	}
	if resp.TokenDeadline < resp.TokenExpiresAt {
		t.Fatalf("token_deadline %d < token_expires_at %d", resp.TokenDeadline, resp.TokenExpiresAt)
	}
	if !hasCapability(resp.Capabilities, "auth_token") {
		t.Fatalf("capabilities %v do not advertise auth_token", resp.Capabilities)
	}

	// A new connection presenting only the token resumes.
	c2 := dialTCPServer(t, addr)
	frame2, resp2, _ := c2.authCall(&AuthRequest{Token: resp.Token})
	if frame2.Opcode != transport.OpAuthOK {
		t.Fatalf("token resume opcode = %s, want AUTH_OK", frame2.Opcode)
	}
	if resp2.Token == "" {
		t.Fatal("resume returned no token")
	}
}

// TestAuthTokenTamperedFallsBackToPassword verifies a tampered token is
// refused with TOKEN_INVALID and the same socket can then authenticate with
// a password (spec 9.2/9.3).
func TestAuthTokenTamperedFallsBackToPassword(t *testing.T) {
	_, addr := newTCPEnabledServer(t, nil)

	c1 := dialTCPServer(t, addr)
	_, resp, _ := c1.authCall(&AuthRequest{Username: "admin", Password: "test123"})

	claimsB64, sigB64, _ := strings.Cut(resp.Token, ".")
	tampered := flipFirstChar(claimsB64) + "." + sigB64

	c2 := dialTCPServer(t, addr)
	frame, _, fail := c2.authCall(&AuthRequest{Token: tampered})
	if frame.Opcode != transport.OpAuthFail {
		t.Fatalf("tampered token opcode = %s, want AUTH_FAIL", frame.Opcode)
	}
	if fail.Reason != uint8(transport.AuthFailTokenInvalid) {
		t.Fatalf("tampered token reason = %d, want TOKEN_INVALID", fail.Reason)
	}

	// Password fallback on the same socket, once.
	frame, _, _ = c2.authCall(&AuthRequest{Username: "admin", Password: "test123"})
	if frame.Opcode != transport.OpAuthOK {
		t.Fatalf("password fallback opcode = %s, want AUTH_OK", frame.Opcode)
	}
}

// TestAuthTokenSemantics pins the precedence rule: a valid token plus a wrong
// password authenticates (the password is ignored), and an invalid token plus
// a correct password fails with TOKEN_* (there is no in-frame fallback).
func TestAuthTokenSemantics(t *testing.T) {
	_, addr := newTCPEnabledServer(t, nil)

	c1 := dialTCPServer(t, addr)
	_, resp, _ := c1.authCall(&AuthRequest{Username: "admin", Password: "test123"})

	// Valid token + wrong password: authenticates.
	c2 := dialTCPServer(t, addr)
	frame, _, fail := c2.authCall(&AuthRequest{
		Token: resp.Token, Username: "admin", Password: "definitely-wrong",
	})
	if frame.Opcode != transport.OpAuthOK {
		t.Fatalf("valid token + wrong password opcode = %s (reason %d), want AUTH_OK (password must be ignored)", frame.Opcode, fail.Reason)
	}

	// Invalid token + correct password: fails, no fallback.
	c3 := dialTCPServer(t, addr)
	frame, _, fail = c3.authCall(&AuthRequest{
		Token: "garbage.claims", Username: "admin", Password: "test123",
	})
	if frame.Opcode != transport.OpAuthFail || transport.AuthFailReason(fail.Reason) != transport.AuthFailTokenInvalid {
		t.Fatalf("invalid token + correct password = %s reason %d, want AUTH_FAIL TOKEN_INVALID", frame.Opcode, fail.Reason)
	}
}

// TestAuthTokenExpiredFallsBack verifies an expired token is TOKEN_EXPIRED and
// the pool can fall back to a password (spec 9.3).
func TestAuthTokenExpiredFallsBack(t *testing.T) {
	qs, addr := newTCPEnabledServer(t, nil)

	claims, err := qs.Auth().ClaimsFor("admin")
	if err != nil {
		t.Fatalf("ClaimsFor: %v", err)
	}
	claims.ExpiresAt = time.Now().Add(-time.Hour).Unix()
	claims.Deadline = time.Now().Add(time.Hour).Unix()
	expired, err := qs.Auth().GenerateToken(claims)
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}

	c := dialTCPServer(t, addr)
	frame, _, fail := c.authCall(&AuthRequest{Token: expired})
	if frame.Opcode != transport.OpAuthFail || transport.AuthFailReason(fail.Reason) != transport.AuthFailTokenExpired {
		t.Fatalf("expired token = %s reason %d, want AUTH_FAIL TOKEN_EXPIRED", frame.Opcode, fail.Reason)
	}
	frame, _, _ = c.authCall(&AuthRequest{Username: "admin", Password: "test123"})
	if frame.Opcode != transport.OpAuthOK {
		t.Fatalf("password fallback after expiry = %s, want AUTH_OK", frame.Opcode)
	}
}

// TestAuthTokenSubjectMismatch verifies a token whose subject disagrees with
// the frame's username is refused as TOKEN_INVALID (spec 9.3).
func TestAuthTokenSubjectMismatch(t *testing.T) {
	_, addr := newTCPEnabledServer(t, nil)
	c1 := dialTCPServer(t, addr)
	_, resp, _ := c1.authCall(&AuthRequest{Username: "admin", Password: "test123"})

	c2 := dialTCPServer(t, addr)
	frame, _, fail := c2.authCall(&AuthRequest{Token: resp.Token, Username: "eve"})
	if frame.Opcode != transport.OpAuthFail || transport.AuthFailReason(fail.Reason) != transport.AuthFailTokenInvalid {
		t.Fatalf("subject mismatch = %s reason %d, want AUTH_FAIL TOKEN_INVALID", frame.Opcode, fail.Reason)
	}
}

// TestAuthTokenForeignKeyRefused verifies a token from a different
// deployment (different signing key) is refused.
func TestAuthTokenForeignKeyRefused(t *testing.T) {
	_, addrA := newTCPEnabledServer(t, nil)
	_, addrB := newTCPEnabledServer(t, nil)

	cA := dialTCPServer(t, addrA)
	_, resp, _ := cA.authCall(&AuthRequest{Username: "admin", Password: "test123"})

	cB := dialTCPServer(t, addrB)
	frame, _, fail := cB.authCall(&AuthRequest{Token: resp.Token})
	if frame.Opcode != transport.OpAuthFail || transport.AuthFailReason(fail.Reason) != transport.AuthFailTokenInvalid {
		t.Fatalf("foreign-key token = %s reason %d, want AUTH_FAIL TOKEN_INVALID", frame.Opcode, fail.Reason)
	}
}

// TestHandleAuthSetsSessionIdentity verifies the server records identity and
// the Resumed flag through the context session (spec 3.7), for both paths.
func TestHandleAuthSetsSessionIdentity(t *testing.T) {
	am := newTestAuthManager(t, discardLogger())
	seedUser(t, am, "admin")
	s := &Server{logger: discardLogger()}
	logger := discardLogger()

	// Password path.
	payload, _ := bson.Marshal(&AuthRequest{Username: "admin", Password: "correct-horse-battery"})
	sess := &transport.Session{}
	ctx := transport.WithSession(context.Background(), sess)
	resp, err := handleAuth(ctx, transport.NewFrame(transport.OpAuth, payload), am, s, logger)
	if err != nil {
		t.Fatalf("handleAuth: %v", err)
	}
	if resp.Opcode != transport.OpAuthOK {
		t.Fatalf("password path opcode = %s, want AUTH_OK", resp.Opcode)
	}
	id, ok := sess.Identity()
	if !ok || id.Username != "admin" {
		t.Fatalf("password identity = %+v ok=%v, want admin", id, ok)
	}
	if id.Resumed {
		t.Fatal("password AUTH must set Resumed=false")
	}
	var authResp AuthResponse
	_ = bson.Unmarshal(resp.Payload, &authResp)

	// Token path.
	tokenPayload, _ := bson.Marshal(&AuthRequest{Token: authResp.Token})
	sess2 := &transport.Session{}
	ctx2 := transport.WithSession(context.Background(), sess2)
	resp2, err := handleAuth(ctx2, transport.NewFrame(transport.OpAuth, tokenPayload), am, s, logger)
	if err != nil {
		t.Fatalf("handleAuth token: %v", err)
	}
	if resp2.Opcode != transport.OpAuthOK {
		t.Fatalf("token path opcode = %s, want AUTH_OK", resp2.Opcode)
	}
	id2, ok := sess2.Identity()
	if !ok || id2.Username != "admin" {
		t.Fatalf("token identity = %+v ok=%v, want admin", id2, ok)
	}
	if !id2.Resumed {
		t.Fatal("token AUTH must set Resumed=true")
	}
}

// TestLoginWithParallelismFourCredentialOnOneCPU is the end of the
// restart path: a root user seeded at GOMAXPROCS=4 must load and log in over
// AUTH on a node that sees one CPU. Argon2 runs its lanes as goroutines, so a
// parallelism-4 hash derives the same bytes on fewer Ps.
func TestLoginWithParallelismFourCredentialOnOneCPU(t *testing.T) {
	dir, _ := setupAuthDir(t)
	pinGOMAXPROCS(t, 4)
	if err := newPersistentManager(t, dir).BootstrapUser("admin", "test123", true); err != nil {
		t.Fatalf("seed at GOMAXPROCS=4: %v", err)
	}

	pinGOMAXPROCS(t, 1)
	am := newPersistentManager(t, dir)
	if _, _, err := am.LoadUsers(UsersFilePath(dir), nil, nil); err != nil {
		t.Fatalf("LoadUsers at GOMAXPROCS=1: %v", err)
	}
	payload, err := bson.Marshal(&AuthRequest{Username: "admin", Password: "test123"})
	if err != nil {
		t.Fatalf("marshal AUTH: %v", err)
	}
	ctx := transport.WithSession(context.Background(), &transport.Session{})
	resp, err := handleAuth(ctx, transport.NewFrame(transport.OpAuth, payload), am, &Server{logger: discardLogger()}, discardLogger())
	if err != nil {
		t.Fatalf("handleAuth: %v", err)
	}
	if resp.Opcode != transport.OpAuthOK {
		t.Fatalf("password AUTH at GOMAXPROCS=1 = %s, want AUTH_OK", resp.Opcode)
	}
}

// TestAuthOKReportsTheConfiguredPasswordMinimum (BUG-0493): the server never
// sees a new user's plaintext, so it tells every authenticated client its
// configured minimum on AUTH_OK, after a password login and after a token
// resume, as the int32 field password_min_length.
func TestAuthOKReportsTheConfiguredPasswordMinimum(t *testing.T) {
	_, addr := newTCPEnabledServer(t, func(cfg *config.Config) { cfg.Auth.PasswordMinLength = 16 })

	c1 := dialTCPServer(t, addr)
	frame, resp, _ := c1.authCall(&AuthRequest{Username: "admin", Password: "test123"})
	if frame.Opcode != transport.OpAuthOK {
		t.Fatalf("password AUTH opcode = %s, want AUTH_OK", frame.Opcode)
	}
	requirePasswordMinimum(t, "password AUTH_OK", frame.Payload, 16)

	c2 := dialTCPServer(t, addr)
	resumed, _, _ := c2.authCall(&AuthRequest{Token: resp.Token})
	if resumed.Opcode != transport.OpAuthOK {
		t.Fatalf("token resume opcode = %s, want AUTH_OK", resumed.Opcode)
	}
	requirePasswordMinimum(t, "token-resume AUTH_OK", resumed.Payload, 16)
}

// requirePasswordMinimum fails unless payload carries password_min_length as
// an int32 equal to want.
func requirePasswordMinimum(t *testing.T, label string, payload []byte, want int32) {
	t.Helper()
	element, err := bson.Raw(payload).LookupErr("password_min_length")
	if err != nil {
		t.Fatalf("%s carries no password_min_length: %v", label, err)
	}
	got, ok := element.Int32OK()
	if !ok {
		t.Fatalf("%s password_min_length is BSON %s, want int32", label, element.Type)
	}
	if got != want {
		t.Fatalf("%s password_min_length = %d, want %d", label, got, want)
	}
}
