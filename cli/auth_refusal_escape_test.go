package cli

import (
	"crypto/tls"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// startAuthRefusingServer answers every AUTH with an AUTH_FAIL that carries
// message, so a test sees what the CLI does with text only a server writes.
func startAuthRefusingServer(t *testing.T, reason transport.AuthFailReason, message string) string {
	t.Helper()
	tlsConfig, _, err := transport.MakeServerTLSConfig(t.TempDir())
	if err != nil {
		t.Fatalf("TLS config: %v", err)
	}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", tlsConfig)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer func() { _ = conn.Close() }()
				frame, err := transport.DecodeFrame(conn)
				if err != nil {
					return
				}
				payload, _ := bson.Marshal(server.AuthFailResponse{Reason: uint8(reason), Message: message})
				_ = transport.WriteFrame(conn, transport.NewFrameWithID(transport.OpAuthFail, frame.RequestID, payload))
			}(conn)
		}
	}()
	return listener.Addr().String()
}

// TestAuthRefusalTextIsEscapedOnEveryPrintPath covers the paths that write an
// error straight to standard error, past the output funnel: the session's
// connect failure, dump and restore's, and ingest's.
func TestAuthRefusalTextIsEscapedOnEveryPrintPath(t *testing.T) {
	reasons := map[string]transport.AuthFailReason{
		"credentials":  transport.AuthFailCredentials,
		"rate limited": transport.AuthFailRateLimited,
	}
	dir := t.TempDir()
	input := filepath.Join(dir, "in.jsonl")
	if err := os.WriteFile(input, []byte("{\"_id_\":\"a\"}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Restore verifies the file before it dials, so it needs a real dump to reach the refusal.
	dumpFile := writeVerifyTestDump(t, dumpVerifyTestRecords(t, "hashStore"), dumpVerifyTestCounts)
	for reasonName, reason := range reasons {
		address := startAuthRefusingServer(t, reason, hostileText)
		verbs := map[string][]string{
			"session": {"-address", address, "-insecure", "-user", "u", "-password", "p"},
			"dump":    {"dump", "--address", address, "--insecure", "--user", "u", "--password", "p", "--out", filepath.Join(dir, "d")},
			"restore": {"restore", "--address", address, "--insecure", "--user", "u", "--password", "p", dumpFile},
			"ingest":  {"ingest", input, "--into", "app.users", "--address", address, "--insecure", "--user", "u", "--password", "p"},
		}
		for verbName, args := range verbs {
			t.Run(reasonName+"/"+verbName, func(t *testing.T) {
				stderr := runCapturingProcessStderr(t, args...)
				// The session and every verb dial the ring, whose client words the refusal its own way.
				reached := strings.Contains(stderr, "authentication failed") || strings.Contains(stderr, "rejected the login")
				if !reached {
					t.Fatalf("this invocation did not reach the server's refusal:\n%s", stderr)
				}
				if character, found := drivesTerminal(stderr); found {
					t.Errorf("stderr holds %U, a character a terminal acts on:\n%q", character, stderr)
				}
			})
		}
	}
}

// runCapturingProcessStderr runs the CLI and returns everything written to its
// Invocation's stderr and to the process's own: dump and restore write to the
// latter.
func runCapturingProcessStderr(t *testing.T, args ...string) string {
	t.Helper()
	capture, err := os.CreateTemp(t.TempDir(), "stderr")
	if err != nil {
		t.Fatalf("creating the capture file: %v", err)
	}
	previous := os.Stderr
	os.Stderr = capture
	_, _, invocationStderr := runMainCapturing(t, args...)
	os.Stderr = previous
	if err := capture.Close(); err != nil {
		t.Fatalf("closing the capture file: %v", err)
	}
	processStderr, err := os.ReadFile(capture.Name())
	if err != nil {
		t.Fatalf("reading the capture file: %v", err)
	}
	return invocationStderr + string(processStderr)
}
