package server

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestLoginAttemptLogsAreBounded is the regression test: what a login attempt writes
// to the server log and the audit log does not grow with the identifiers the
// client sent. Each attempt below carries about 29 KiB of them.
func TestLoginAttemptLogsAreBounded(t *testing.T) {
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "alice")
	var serverLog bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&serverLog, nil))
	auditPath := filepath.Join(t.TempDir(), "audit.log")
	audit, err := NewAuditLogger(auditPath)
	if err != nil {
		t.Fatalf("NewAuditLogger: %v", err)
	}
	s := &Server{logger: logger, audit: audit}

	capabilities := make([]string, 200)
	for i := range capabilities {
		capabilities[i] = strings.Repeat("c", 64)
	}
	long := AuthRequest{
		Username:     strings.Repeat("u", 8<<10),
		Password:     "wrong-password-123",
		Client:       strings.Repeat("d", 8<<10),
		Capabilities: capabilities,
	}
	mismatched := long
	mismatched.ProtocolVersion = uint32(transport.ProtocolMajor) + 1

	attempts := []struct {
		name    string
		request AuthRequest
		peer    bool
	}{
		{"wrong password on the driver port", long, false},
		{"protocol mismatch", mismatched, false},
		{"user credentials on the internode port", long, true},
	}
	for _, attempt := range attempts {
		payload, err := bson.Marshal(attempt.request)
		if err != nil {
			t.Fatalf("%s: marshal: %v", attempt.name, err)
		}
		if len(payload) > transport.PreAuthFrameLength {
			t.Fatalf("%s: payload is %d bytes, over the before-login limit the test means to stay under", attempt.name, len(payload))
		}
		sess := &transport.Session{RemoteAddr: "198.51.100.7:4000", Peer: attempt.peer}
		ctx := transport.WithSession(context.Background(), sess)
		answer, err := handleAuth(ctx, transport.NewFrame(transport.OpAuth, payload), am, s, logger)
		if err != nil || answer.Opcode != transport.OpAuthFail {
			t.Fatalf("%s: answered %v, %v; want AUTH_FAIL", attempt.name, answer, err)
		}
	}
	audit.Close()
	auditBytes, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}

	const perAttempt = 2 << 10
	if got, bound := serverLog.Len(), perAttempt*len(attempts); got > bound {
		t.Errorf("server log grew by %d bytes for %d login attempts, want at most %d", got, len(attempts), bound)
	}
	if got, bound := len(auditBytes), perAttempt*len(attempts); got > bound {
		t.Errorf("audit log grew by %d bytes for %d login attempts, want at most %d", got, len(attempts), bound)
	}
}
