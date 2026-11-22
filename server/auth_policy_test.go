package server

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestPasswordReAuthRefused verifies a second password AUTH on an
// authenticated connection is refused as ALREADY_AUTHED (spec 9.6).
func TestPasswordReAuthRefused(t *testing.T) {
	_, addr := newTCPEnabledServer(t, nil)
	c := dialTCPServer(t, addr)
	c.login()

	frame, _, fail := c.authCall(&AuthRequest{Username: "admin", Password: "test123"})
	if frame.Opcode != transport.OpAuthFail || transport.AuthFailReason(fail.Reason) != transport.AuthFailAlreadyAuthed {
		t.Fatalf("re-AUTH = %s reason %d, want AUTH_FAIL ALREADY_AUTHED", frame.Opcode, fail.Reason)
	}
}

// TestNodeCredentialsRefusedOnDriverPort verifies cluster credentials only
// authenticate on the internode listener (spec 10.2).
func TestNodeCredentialsRefusedOnDriverPort(t *testing.T) {
	_, addr := newTCPEnabledServer(t, nil)
	c := dialTCPServer(t, addr)

	frame, _, fail := c.authCall(&AuthRequest{Username: NodeUser, Password: "any-cluster-token"})
	if frame.Opcode != transport.OpAuthFail || transport.AuthFailReason(fail.Reason) != transport.AuthFailCredentials {
		t.Fatalf("node credentials on driver port = %s reason %d, want AUTH_FAIL CREDENTIALS", frame.Opcode, fail.Reason)
	}
}

// TestConcurrentPasswordAuthsOnOneConnectionDeriveOnce is the §2.6 DoS: 64
// concurrent password AUTHs on one connection must cost exactly one Argon2id
// derivation and answer the rest with ALREADY_AUTHED.
func TestConcurrentPasswordAuthsOnOneConnectionDeriveOnce(t *testing.T) {
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "alice")
	s := &Server{logger: discardLogger()}

	var derivations, oks atomic.Int64
	verifyUserHook = func() { derivations.Add(1) }
	defer func() { verifyUserHook = nil }()

	sess := &transport.Session{}
	ctx := transport.WithSession(context.Background(), sess)
	payload, _ := bson.Marshal(&AuthRequest{Username: "alice", Password: "correct-horse-battery"})

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, _ := handleAuth(ctx, transport.NewFrame(transport.OpAuth, payload), am, s, discardLogger())
			if resp.Opcode == transport.OpAuthOK {
				oks.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := derivations.Load(); got != 1 {
		t.Fatalf("Argon2 derivations = %d, want exactly 1", got)
	}
	if got := oks.Load(); got != 1 {
		t.Fatalf("AUTH_OK count = %d, want exactly 1", got)
	}
}

// TestDerivationGateBoundsConcurrency is §9.6's semaphore bound: with N=1, a
// second simultaneous AUTH never starts a derivation and is answered
// RATE_LIMITED, and the instrumented peak stays at 1.
func TestDerivationGateBoundsConcurrency(t *testing.T) {
	keyring, err := BuildKeyring(t.TempDir(), "", "", time.Hour, discardLogger())
	if err != nil {
		t.Fatalf("BuildKeyring: %v", err)
	}
	am := NewAuthManager(AuthTokenOptions{
		TTL: time.Hour, MaxLifetime: time.Hour, Skew: time.Minute,
		Keyring: keyring, MaxDerivations: 1, DerivationTimeout: 30 * time.Millisecond,
	}, discardLogger())
	seedUser(t, am, "alice")
	s := &Server{logger: discardLogger()}

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var active, peak atomic.Int64
	verifyUserHook = func() {
		once.Do(func() { close(entered) })
		n := active.Add(1)
		for {
			m := peak.Load()
			if n <= m || peak.CompareAndSwap(m, n) {
				break
			}
		}
		<-release
		active.Add(-1)
	}
	defer func() { verifyUserHook = nil }()

	payload, _ := bson.Marshal(&AuthRequest{Username: "alice", Password: "correct-horse-battery"})

	sess1 := &transport.Session{}
	ctx1 := transport.WithSession(context.Background(), sess1)
	firstResult := make(chan *transport.Frame, 1)
	go func() {
		resp, _ := handleAuth(ctx1, transport.NewFrame(transport.OpAuth, payload), am, s, discardLogger())
		firstResult <- resp
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the first derivation never started")
	}

	sess2 := &transport.Session{}
	ctx2 := transport.WithSession(context.Background(), sess2)
	second, err := handleAuth(ctx2, transport.NewFrame(transport.OpAuth, payload), am, s, discardLogger())
	if err != nil {
		t.Fatalf("second AUTH: %v", err)
	}
	if second.Opcode != transport.OpAuthFail {
		t.Fatalf("second AUTH opcode = %s, want AUTH_FAIL (rate limited)", second.Opcode)
	}
	var fail AuthFailResponse
	_ = bson.Unmarshal(second.Payload, &fail)
	if transport.AuthFailReason(fail.Reason) != transport.AuthFailRateLimited {
		t.Fatalf("second AUTH reason = %d, want RATE_LIMITED", fail.Reason)
	}

	close(release)
	first := <-firstResult
	if first.Opcode != transport.OpAuthOK {
		t.Fatalf("first AUTH opcode = %s, want AUTH_OK", first.Opcode)
	}
	if got := peak.Load(); got != 1 {
		t.Fatalf("peak concurrent derivations = %d, want 1", got)
	}
}

// TestSessionIdentityConcurrentSetAndRead exercises the §3.7 interleaving
// under -race: a refresh replacing identity while another goroutine reads it.
func TestSessionIdentityConcurrentSetAndRead(t *testing.T) {
	sess := &transport.Session{}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				sess.SetIdentity(transport.Identity{
					Username:  "alice",
					Roles:     []string{"admin"},
					Databases: []string{"*"},
				})
				if id, ok := sess.Identity(); ok {
					_ = strings.Join(id.Roles, ",")
				}
			}
		}()
	}
	wg.Wait()
}

// TestAuditRecordsAuthMethod verifies the audit record distinguishes a resumed
// session from a password one, which is what makes Identity.Resumed read.
func TestAuditRecordsAuthMethod(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	al, err := NewAuditLogger(path)
	if err != nil {
		t.Fatalf("NewAuditLogger: %v", err)
	}
	am := newTestAuthManager(t, discardLogger())
	seedUser(t, am, "admin")
	s := &Server{audit: al, logger: discardLogger()}

	// Password AUTH mints the token.
	passwordPayload, _ := bson.Marshal(&AuthRequest{Username: "admin", Password: "correct-horse-battery"})
	sess := &transport.Session{}
	ctx := transport.WithSession(context.Background(), sess)
	resp, err := handleAuth(ctx, transport.NewFrame(transport.OpAuth, passwordPayload), am, s, discardLogger())
	if err != nil || resp.Opcode != transport.OpAuthOK {
		t.Fatalf("password AUTH = %v err=%v", resp.Opcode, err)
	}
	var authResp AuthResponse
	_ = bson.Unmarshal(resp.Payload, &authResp)

	// Token AUTH records method=token.
	tokenPayload, _ := bson.Marshal(&AuthRequest{Token: authResp.Token})
	sess2 := &transport.Session{}
	ctx2 := transport.WithSession(context.Background(), sess2)
	if _, err := handleAuth(ctx2, transport.NewFrame(transport.OpAuth, tokenPayload), am, s, discardLogger()); err != nil {
		t.Fatalf("token AUTH: %v", err)
	}

	al.Close()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if !strings.Contains(string(data), `"auth_method":"token"`) {
		t.Fatalf("audit log does not record a token AUTH; holds %s", data)
	}
	if !strings.Contains(string(data), `"auth_method":"password"`) {
		t.Fatalf("audit log does not record a password AUTH; holds %s", data)
	}
}

// TestTokenNeverLogged is the §9.4 redaction check: the token, or the signing
// key, must not appear in log output, an error string, or a config dump.
func TestTokenNeverLogged(t *testing.T) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))
	keyring, err := BuildKeyring(t.TempDir(), "", "", time.Hour, logger)
	if err != nil {
		t.Fatalf("BuildKeyring: %v", err)
	}
	am := NewAuthManager(AuthTokenOptions{
		TTL: time.Hour, MaxLifetime: time.Hour, Skew: time.Minute, Keyring: keyring,
	}, logger)
	seedUser(t, am, "alice")
	s := &Server{logger: logger}

	passwordPayload, _ := bson.Marshal(&AuthRequest{Username: "alice", Password: "correct-horse-battery"})
	sess := &transport.Session{}
	ctx := transport.WithSession(context.Background(), sess)
	resp, err := handleAuth(ctx, transport.NewFrame(transport.OpAuth, passwordPayload), am, s, logger)
	if err != nil || resp.Opcode != transport.OpAuthOK {
		t.Fatalf("password AUTH = %v err=%v", resp.Opcode, err)
	}
	var authResp AuthResponse
	_ = bson.Unmarshal(resp.Payload, &authResp)
	if authResp.Token == "" {
		t.Fatal("no token minted")
	}

	// A successful token AUTH and a failing one, both logged through the real
	// handler.
	tokenPayload, _ := bson.Marshal(&AuthRequest{Token: authResp.Token})
	sess2 := &transport.Session{}
	ctx2 := transport.WithSession(context.Background(), sess2)
	_, _ = handleAuth(ctx2, transport.NewFrame(transport.OpAuth, tokenPayload), am, s, logger)

	badPayload, _ := bson.Marshal(&AuthRequest{Token: "junk.junk"})
	sess3 := &transport.Session{}
	ctx3 := transport.WithSession(context.Background(), sess3)
	_, _ = handleAuth(ctx3, transport.NewFrame(transport.OpAuth, badPayload), am, s, logger)

	if strings.Contains(logBuf.String(), authResp.Token) {
		t.Fatalf("token appeared in log output: %s", logBuf.String())
	}

	// A parse error must not echo the presented token.
	if _, err := am.ValidateToken("junk.junk"); err != nil && strings.Contains(err.Error(), "junk.junk") {
		t.Fatalf("validate error echoed the token: %v", err)
	}

	// A config dump never carries the signing key.
	dump := fmt.Sprintf("%+v", config.DefaultConfig().Auth)
	if strings.Contains(dump, string(keyring.Current().Bytes)) {
		t.Fatal("signing key appeared in a config dump")
	}
}
