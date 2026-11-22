package server

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// legacyAuthRequest is the pre-token AuthRequest shape. It exists so the
// wire-compatibility test proves a new frame still decodes into an old peer
// instead of asserting it by inspection (spec 9.7 / Appendix B).
type legacyAuthRequest struct {
	Username        string   `json:"username"`
	Password        string   `json:"password"`
	ProtocolVersion uint32   `json:"protocol_version" bson:"protocol_version"`
	Capabilities    []string `json:"capabilities" bson:"capabilities"`
	Client          string   `json:"client" bson:"client"`
}

func bsonDocument(t *testing.T, v interface{}) bson.M {
	t.Helper()
	raw, err := bson.Marshal(v)
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}
	var doc bson.M
	if err := bson.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("bson.Unmarshal into map: %v", err)
	}
	return doc
}

func TestAuthRequestWireFieldNames(t *testing.T) {
	doc := bsonDocument(t, &AuthRequest{Username: "alice", Password: "secret", Token: "t"})
	for _, key := range []string{"username", "password", "token"} {
		if _, ok := doc[key]; !ok {
			t.Fatalf("AuthRequest wire document %v is missing %q", doc, key)
		}
	}

	// A password-only request carries no token key at all (omitempty): it is
	// byte-identical to today's frames.
	doc = bsonDocument(t, &AuthRequest{Username: "alice", Password: "secret"})
	if _, ok := doc["token"]; ok {
		t.Fatalf("password-only AuthRequest carried a token key: %v", doc)
	}
}

func TestAuthResponseWireFieldNames(t *testing.T) {
	doc := bsonDocument(t, &AuthResponse{
		Success:        true,
		Token:          "t",
		TokenExpiresAt: 123,
		TokenDeadline:  456,
		Capabilities:   []string{"auth_token"},
	})
	for _, key := range []string{"token", "token_expires_at", "token_deadline", "capabilities"} {
		if _, ok := doc[key]; !ok {
			t.Fatalf("AuthResponse wire document %v is missing %q", doc, key)
		}
	}
}

func TestAuthFailResponseWireFieldNames(t *testing.T) {
	doc := bsonDocument(t, &AuthFailResponse{Reason: uint8(3), Message: "nope"})
	for _, key := range []string{"reason", "message"} {
		if _, ok := doc[key]; !ok {
			t.Fatalf("AuthFailResponse wire document %v is missing %q", doc, key)
		}
	}
}

func TestAuthWireBackwardCompatibility(t *testing.T) {
	// New request (with token) decoded by the old shape: username and
	// password intact, no decode error.
	raw, err := bson.Marshal(&AuthRequest{Username: "alice", Password: "secret", Token: "tok"})
	if err != nil {
		t.Fatalf("marshal new request: %v", err)
	}
	var legacy legacyAuthRequest
	if err := bson.Unmarshal(raw, &legacy); err != nil {
		t.Fatalf("old shape rejected a new request: %v", err)
	}
	if legacy.Username != "alice" || legacy.Password != "secret" {
		t.Fatalf("old shape decoded %+v, want alice/secret", legacy)
	}

	// Token-only request decoded by the old shape: valid decode with empty
	// credentials, so the old server answers a clean AUTH_FAIL rather than a
	// protocol/decode error.
	raw, err = bson.Marshal(&AuthRequest{Token: "tok"})
	if err != nil {
		t.Fatalf("marshal token-only request: %v", err)
	}
	legacy = legacyAuthRequest{}
	if err := bson.Unmarshal(raw, &legacy); err != nil {
		t.Fatalf("old shape failed to decode a token-only request: %v", err)
	}
	if legacy.Username != "" || legacy.Password != "" {
		t.Fatalf("token-only request decoded credentials %+v, want empty", legacy)
	}

	// Old request decoded by the new shape: token empty.
	raw, err = bson.Marshal(&legacyAuthRequest{Username: "alice", Password: "secret"})
	if err != nil {
		t.Fatalf("marshal old request: %v", err)
	}
	var current AuthRequest
	if err := bson.Unmarshal(raw, &current); err != nil {
		t.Fatalf("new shape rejected an old request: %v", err)
	}
	if current.Token != "" {
		t.Fatalf("old request decoded a non-empty token %q", current.Token)
	}
}

// busyAuthManager returns a manager with one derivation slot and a short wait,
// so a test can make the server "busy" by holding that slot itself.
func busyAuthManager(t *testing.T) *AuthManager {
	t.Helper()
	keyring, err := BuildKeyring(t.TempDir(), "", "", time.Hour, discardLogger())
	if err != nil {
		t.Fatalf("BuildKeyring: %v", err)
	}
	am := NewAuthManager(AuthTokenOptions{
		TTL: time.Hour, MaxLifetime: time.Hour, Skew: time.Minute,
		Keyring: keyring, MaxDerivations: 1, DerivationTimeout: 30 * time.Millisecond,
	}, discardLogger())
	seedUser(t, am, "alice")
	return am
}

// passwordAuth sends one password AUTH on sess and returns the opcode and the
// decoded failure (zero on AUTH_OK).
func passwordAuth(t *testing.T, am *AuthManager, sess *transport.Session, password string) (transport.Opcode, AuthFailResponse) {
	t.Helper()
	payload, err := bson.Marshal(&AuthRequest{Username: "alice", Password: password})
	if err != nil {
		t.Fatalf("marshal AUTH: %v", err)
	}
	ctx := transport.WithSession(context.Background(), sess)
	resp, err := handleAuth(ctx, transport.NewFrame(transport.OpAuth, payload), am, &Server{logger: discardLogger()}, discardLogger())
	if err != nil {
		t.Fatalf("handleAuth: %v", err)
	}
	var fail AuthFailResponse
	if resp.Opcode == transport.OpAuthFail {
		if err := bson.Unmarshal(resp.Payload, &fail); err != nil {
			t.Fatalf("decode AUTH_FAIL: %v", err)
		}
	}
	return resp.Opcode, fail
}

// busyRefusal holds the only derivation slot while one password AUTH runs on
// sess, asserts it was refused RATE_LIMITED, and frees the slot again.
func busyRefusal(t *testing.T, am *AuthManager, sess *transport.Session, password string) AuthFailResponse {
	t.Helper()
	if !am.acquireDerivation() {
		t.Fatal("could not take the derivation slot to make the server busy")
	}
	opcode, fail := passwordAuth(t, am, sess, password)
	am.releaseDerivation()
	if opcode != transport.OpAuthFail || transport.AuthFailReason(fail.Reason) != transport.AuthFailRateLimited {
		t.Fatalf("AUTH while every slot is held = %s reason %s, want AUTH_FAIL RATE_LIMITED",
			opcode, transport.AuthFailReason(fail.Reason))
	}
	return fail
}

// TestRateLimitedPasswordAuthCanRetryOnSameConnection: a busy
// refusal checks no password, so it must not use up the connection's one
// password attempt. A retry on the same connection once a slot is free
// succeeds instead of being told the connection is already authenticated.
func TestRateLimitedPasswordAuthCanRetryOnSameConnection(t *testing.T) {
	am := busyAuthManager(t)
	sess := &transport.Session{}

	busyRefusal(t, am, sess, "correct-horse-battery")

	opcode, fail := passwordAuth(t, am, sess, "correct-horse-battery")
	if opcode != transport.OpAuthOK {
		t.Fatalf("same-connection retry after RATE_LIMITED = %s reason %s %q, want AUTH_OK",
			opcode, transport.AuthFailReason(fail.Reason), fail.Message)
	}
}

// TestBusyRefusalGivesNoExtraPasswordGuess is the guard on the
// once-per-connection rule: giving the claim back after a busy refusal must
// not give a connection more than one checked password. After the retry's
// password is checked and wrong, a further attempt on that connection is still
// ALREADY_AUTHED, and exactly one derivation ran.
func TestBusyRefusalGivesNoExtraPasswordGuess(t *testing.T) {
	am := busyAuthManager(t)
	var derivations atomic.Int64
	verifyUserHook = func() { derivations.Add(1) }
	defer func() { verifyUserHook = nil }()
	sess := &transport.Session{}

	for i := 0; i < 3; i++ {
		busyRefusal(t, am, sess, "wrong-guess")
	}
	if got := derivations.Load(); got != 0 {
		t.Fatalf("busy refusals ran %d derivations, want 0: a refusal must not check the password", got)
	}

	opcode, fail := passwordAuth(t, am, sess, "wrong-guess")
	if opcode != transport.OpAuthFail || transport.AuthFailReason(fail.Reason) != transport.AuthFailCredentials {
		t.Fatalf("first checked password = %s reason %s, want AUTH_FAIL CREDENTIALS",
			opcode, transport.AuthFailReason(fail.Reason))
	}
	opcode, fail = passwordAuth(t, am, sess, "correct-horse-battery")
	if opcode != transport.OpAuthFail || transport.AuthFailReason(fail.Reason) != transport.AuthFailAlreadyAuthed {
		t.Fatalf("second checked password on the same connection = %s reason %s, want AUTH_FAIL ALREADY_AUTHED",
			opcode, transport.AuthFailReason(fail.Reason))
	}
	if got := derivations.Load(); got != 1 {
		t.Fatalf("derivations = %d, want exactly 1 checked password per connection", got)
	}
}

// TestBusyAuthRefusalMessageExplainsTheFix holds the busy refusal to root
// AGENTS.md rule 3: it says the password was not checked, what the server
// waited for and how long, and names every remedy.
func TestBusyAuthRefusalMessageExplainsTheFix(t *testing.T) {
	am := busyAuthManager(t)
	fail := busyRefusal(t, am, &transport.Session{}, "correct-horse-battery")

	for _, want := range []string{
		"password was not checked",
		"30ms",
		"1 password-hashing slot",
		"retry",
		"this connection",
		"pooled connections",
		"token",
		"HOARDDB_AUTH_MAX_DERIVATIONS",
	} {
		if !strings.Contains(fail.Message, want) {
			t.Errorf("busy refusal %q does not contain %q", fail.Message, want)
		}
	}
}

// TestPipelinedPasswordAuthChecksOnePasswordPerConnection pins the claim that
// bounds pipelined guessing (BUG-0236). The per-IP ledger admits every
// pipelined AUTH frame before any failure is recorded, so it cannot bound
// them; the per-connection password claim is the only thing that does. Many
// wrong-password frames sent back to back on one connection, more than
// AuthMaxFails, must check at most one password and answer the rest
// ALREADY_AUTHED.
func TestPipelinedPasswordAuthChecksOnePasswordPerConnection(t *testing.T) {
	const pipelinedFrames = 32 // well above the default AuthMaxFails of 5
	am := busyAuthManager(t)
	var derivations atomic.Int64
	verifyUserHook = func() { derivations.Add(1) }
	defer func() { verifyUserHook = nil }()
	sess := &transport.Session{}

	reasons := make(chan transport.AuthFailReason, pipelinedFrames)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < pipelinedFrames; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			opcode, fail := passwordAuth(t, am, sess, "wrong-guess")
			if opcode != transport.OpAuthFail {
				t.Errorf("wrong password answered %s, want AUTH_FAIL", opcode)
			}
			reasons <- transport.AuthFailReason(fail.Reason)
		}()
	}
	close(start)
	wg.Wait()
	close(reasons)

	reasonCounts := map[transport.AuthFailReason]int{}
	for reason := range reasons {
		reasonCounts[reason]++
	}
	if got := derivations.Load(); got > 1 {
		t.Errorf("%d pipelined wrong passwords were checked on one connection, want at most 1", got)
	}
	if got := reasonCounts[transport.AuthFailCredentials]; got != 1 {
		t.Errorf("%d frames answered CREDENTIALS, want exactly 1 (reasons: %v)", got, reasonCounts)
	}
	if got := reasonCounts[transport.AuthFailAlreadyAuthed]; got != pipelinedFrames-1 {
		t.Errorf("%d frames answered ALREADY_AUTHED, want %d (reasons: %v)", got, pipelinedFrames-1, reasonCounts)
	}
}

// TestConfigUnavailableAuthRefusalIsMarkedUnchecked: a node with no auth
// config refuses before it checks any credential, and the response says so
// for the per-IP ledger (BUG-0237). A wrong password on a node that can check
// it is not marked: that one is a failed login.
func TestConfigUnavailableAuthRefusalIsMarkedUnchecked(t *testing.T) {
	am := busyAuthManager(t)
	srv := &Server{logger: discardLogger()}
	send := func(request AuthRequest) *transport.Frame {
		t.Helper()
		payload, err := bson.Marshal(&request)
		if err != nil {
			t.Fatalf("marshal AUTH: %v", err)
		}
		ctx := transport.WithSession(context.Background(), &transport.Session{})
		resp, err := handleAuth(ctx, transport.NewFrame(transport.OpAuth, payload), am, srv, discardLogger())
		if err != nil {
			t.Fatalf("handleAuth: %v", err)
		}
		return resp
	}

	if resp := send(AuthRequest{Username: "alice", Password: "wrong-guess"}); resp.CredentialUnchecked() {
		t.Fatal("a wrong password on a node with auth config was marked unchecked; it is a failed login")
	}

	am.SetConfigBlocked(true)
	for name, request := range map[string]AuthRequest{
		"password": {Username: "alice", Password: "correct-horse-battery"},
		"token":    {Username: "alice", Token: "any-token"},
	} {
		resp := send(request)
		if resp.Opcode != transport.OpAuthFail {
			t.Fatalf("%s AUTH with no auth config = %s, want AUTH_FAIL", name, resp.Opcode)
		}
		if !resp.CredentialUnchecked() {
			t.Errorf("%s AUTH refused for an unavailable auth config was not marked unchecked; the ledger would count it", name)
		}
	}
}
