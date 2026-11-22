package server

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newTestAuthManager builds an AuthManager backed by a real temp-file
// keyring, with the same shape production uses.
func newTestAuthManager(t *testing.T, logger *slog.Logger) *AuthManager {
	t.Helper()
	keyring, err := BuildKeyring(t.TempDir(), "", "", time.Hour, logger)
	if err != nil {
		t.Fatalf("BuildKeyring: %v", err)
	}
	return NewAuthManager(AuthTokenOptions{
		TTL:         time.Hour,
		MaxLifetime: 12 * time.Hour,
		Skew:        60 * time.Second,
		Keyring:     keyring,
	}, logger)
}

func auditFrame(t *testing.T, username, password string) *transport.Frame {
	t.Helper()
	payload, err := bson.Marshal(AuthRequest{Username: username, Password: password})
	if err != nil {
		t.Fatalf("marshal auth request: %v", err)
	}
	return transport.NewFrame(transport.OpAuth, payload)
}

// The audit logger was constructed, exposed as Server.Audit() and closed at
// shutdown, but Log had no callers anywhere in the tree — so an enabled audit
// log stayed empty forever. This drives the real auth handler and asserts the
// entry lands on disk.
func TestFailedAuthIsAudited(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	al, err := NewAuditLogger(path)
	if err != nil {
		t.Fatalf("NewAuditLogger: %v", err)
	}
	logger := discardLogger()
	am := newTestAuthManager(t, logger)
	if err := am.SeedRootUser("root", "correct-horse-battery"); err != nil {
		t.Fatalf("seed root user: %v", err)
	}
	s := &Server{audit: al, logger: logger}

	resp, err := handleAuth(context.Background(), auditFrame(t, "root", "wrong"), am, s, logger)
	if err != nil {
		t.Fatalf("handleAuth: %v", err)
	}
	if resp.Opcode != transport.OpAuthFail {
		t.Fatalf("opcode = %v, want OpAuthFail", resp.Opcode)
	}
	al.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if !strings.Contains(string(data), `"event":"auth.failure"`) {
		t.Fatalf("failed auth was not audited; log holds %q", string(data))
	}

	// Audit entries name users and targets, so the file must not be world-readable.
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat audit log: %v", err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Errorf("audit log permissions = %o, want 600", perm)
	}
}

// A successful login is audited too, otherwise the log shows only failures.
func TestSuccessfulAuthIsAudited(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	al, err := NewAuditLogger(path)
	if err != nil {
		t.Fatalf("NewAuditLogger: %v", err)
	}
	logger := discardLogger()
	am := newTestAuthManager(t, logger)
	if err := am.SeedRootUser("root", "correct-horse-battery"); err != nil {
		t.Fatalf("seed root user: %v", err)
	}
	s := &Server{audit: al, logger: logger}

	if _, err := handleAuth(context.Background(), auditFrame(t, "root", "correct-horse-battery"), am, s, logger); err != nil {
		t.Fatalf("handleAuth: %v", err)
	}
	al.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if !strings.Contains(string(data), `"event":"auth.login"`) {
		t.Fatalf("successful auth was not audited; log holds %q", string(data))
	}
}

// Audit logging is opt-in (config.Audit.Enabled), so the request path runs with
// a nil logger whenever it is off. That must be a no-op, not a panic.
func TestAuditDisabledDoesNotPanic(t *testing.T) {
	var al *AuditLogger
	al.Log(AuditEntry{Event: EventAuthFailure, User: "nobody"})
	al.Close()

	logger := discardLogger()
	am := newTestAuthManager(t, logger)
	if err := am.SeedRootUser("root", "pw"); err != nil {
		t.Fatalf("seed root user: %v", err)
	}
	s := &Server{logger: logger} // audit disabled: s.audit stays nil

	resp, err := handleAuth(context.Background(), auditFrame(t, "root", "wrong"), am, s, logger)
	if err != nil {
		t.Fatalf("handleAuth with audit disabled: %v", err)
	}
	if resp.Opcode != transport.OpAuthFail {
		t.Fatalf("opcode = %v, want OpAuthFail", resp.Opcode)
	}
}

// TestLedgerAuditHookRecordsTheTransportResult pins that the hook the TCP
// listeners call for a refusal of the per-IP ledger writes the transport's
// result text unchanged under auth.failure, so a refusal for logins in flight
// is not recorded as consecutive failures.
func TestLedgerAuditHookRecordsTheTransportResult(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	al, err := NewAuditLogger(path)
	if err != nil {
		t.Fatalf("NewAuditLogger: %v", err)
	}
	qs := &Service{core: &Server{audit: al, logger: discardLogger()}}
	const inFlight = "login refused: 20 being checked, 3 waiting; no password was checked"
	const counted = "auth rate limit: 20 consecutive failures"
	hook := qs.auditHook()
	hook("192.0.2.7", inFlight)
	hook("192.0.2.7", counted)
	al.Close()

	logged, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(logged)), "\n")
	if len(lines) != 2 {
		t.Fatalf("the audit log holds %d lines, want 2: %q", len(lines), logged)
	}
	for i, want := range []string{inFlight, counted} {
		for _, part := range []string{`"event":"auth.failure"`, `"result":"` + want + `"`} {
			if !strings.Contains(lines[i], part) {
				t.Fatalf("audit line %d %q does not contain %q", i+1, lines[i], part)
			}
		}
	}
}
