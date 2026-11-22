package cli

import (
	"bufio"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// connectSpecFor is a spec that reaches addr with the given credentials, with
// the process environment out of the way: a developer's HOARDDB_* variables
// and a data directory in the working directory must not decide the outcome.
func connectSpecFor(t *testing.T, address, user, password string) ConnectSpec {
	t.Helper()
	for _, name := range []string{
		"HOARDDB_ROOT_USER", "HOARDDB_ROOT_PASSWORD", "HOARDDB_AUTH_TOKEN",
		"HOARDDB_TCP_LISTEN", "HOARDDB_DATA_DIR",
	} {
		t.Setenv(name, "")
	}
	configPath := filepath.Join(t.TempDir(), "hoarddb.yaml")
	body := "storage:\n  data_dir: " + t.TempDir() + "\n"
	if err := os.WriteFile(configPath, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return ConnectSpec{
		Address: address, ExplicitAddress: true, Config: configPath, Insecure: true,
		User: user, Password: password, NoPrompt: true, Chatter: io.Discard,
		Getenv: func(string) string { return "" },
	}
}

// overloadedAuthListener answers the first frame of every connection (the AUTH
// request) with an OVERLOADED error and closes, the way a server at its
// connection limit does.
func overloadedAuthListener(t *testing.T) string {
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
			go func() {
				defer func() { _ = conn.Close() }()
				frame, err := transport.DecodeFrame(bufio.NewReader(conn))
				if err != nil {
					return
				}
				_ = transport.WriteErrorResponse(conn, transport.ErrOverloaded, "server at its connection limit", frame.RequestID)
			}()
		}
	}()
	return listener.Addr().String()
}

// TestOverloadedDuringAuthIsConnect is T5's second half: an OVERLOADED error
// frame in answer to AUTH is a connect failure (exit 6, retry later), not an
// authentication failure (exit 7, fix the credentials).
func TestOverloadedDuringAuthIsConnect(t *testing.T) {
	address := overloadedAuthListener(t)
	_, err := connectSession(connectSpecFor(t, address, "admin", "whatever"))
	if err == nil {
		t.Fatal("connectSession succeeded against a server that refused AUTH")
	}
	var connectErr *ConnectError
	if !errors.As(err, &connectErr) || connectErr.Phase != "auth-handshake" {
		t.Fatalf("error = %#v, want a *ConnectError in phase auth-handshake", err)
	}
	if info := classify(err); info.Exit != ExitConnect || info.Code != "OVERLOADED" {
		t.Errorf("classified as exit %d code %q, want exit %d code OVERLOADED", info.Exit, info.Code, ExitConnect)
	}
}

// TestConnectFailuresAreTypedByWhatFailed is T5 and T6: a dead listener is a
// dial failure (6), a rejected password passes through as the *AuthError the
// handler produced (7) and is not wrapped as a connect failure.
func TestConnectFailuresAreTypedByWhatFailed(t *testing.T) {
	session := newGoldenSession(t)

	dead := closedTCPAddr(t)
	_, err := connectSession(connectSpecFor(t, dead, "admin", goldenPassword))
	var connectErr *ConnectError
	if !errors.As(err, &connectErr) || connectErr.Phase != "dial" {
		t.Fatalf("dead listener error = %#v, want a *ConnectError in phase dial", err)
	}
	if info := classify(err); info.Exit != ExitConnect {
		t.Errorf("dead listener exited %d, want %d", info.Exit, ExitConnect)
	}

	_, err = connectSession(connectSpecFor(t, session.addr, "admin", "wrong-password-golden"))
	var authErr *AuthError
	if !errors.As(err, &authErr) {
		t.Fatalf("wrong password error = %#v, want an *AuthError", err)
	}
	if errors.As(err, &connectErr) {
		t.Errorf("an authentication failure was wrapped as a connect failure: %v", err)
	}
	if info := classify(err); info.Exit != ExitAuth {
		t.Errorf("wrong password exited %d, want %d", info.Exit, ExitAuth)
	}
}

// TestConnectErrorKeepsTheREPLText: the REPL printed "connect to ADDR: ..." and
// "build TLS config for ADDR: ..." before ConnectError existed on this path;
// the "hql-spec" user-docs page shows those lines, so they stay.
func TestConnectErrorKeepsTheREPLText(t *testing.T) {
	cause := errors.New("boom")
	tests := []struct {
		name string
		err  *ConnectError
		want string
	}{
		{"dial", &ConnectError{Phase: "dial", Address: "db:7433", Err: cause}, "connect to db:7433: boom"},
		{"tls", &ConnectError{Phase: "tls", Address: "db:7433", Err: cause}, "build TLS config for db:7433: boom"},
		{"pin", &ConnectError{Phase: "pin", Address: "db:7433", Err: cause}, "could not connect (pin): boom"},
		{"no address", &ConnectError{Phase: "dial", Err: cause}, "could not connect (dial): boom"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.err.Error(); got != tc.want {
				t.Errorf("Error() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestNoCredentialsIsUsage is T6's second half: with no user, password or
// token anywhere the answer is a usage error and nothing was dialled.
func TestNoCredentialsIsUsage(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	var accepted atomic.Int32
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			_ = conn.Close()
		}
	}()

	_, err = connectSession(connectSpecFor(t, listener.Addr().String(), "", ""))
	var usage *UsageError
	if !errors.As(err, &usage) {
		t.Fatalf("error = %#v, want a *UsageError", err)
	}
	if info := classify(err); info.Exit != ExitUsage {
		t.Errorf("exited %d, want %d", info.Exit, ExitUsage)
	}
	if got := accepted.Load(); got != 0 {
		t.Errorf("the listener accepted %d connections; a session with no credentials must not dial", got)
	}
}

// TestNoPromptRefusesTrustOnAnInteractiveSession: the verbs stop asking
// "Trust this server?" (stop-and-ask S4). /dev/null is a character device, so
// it stands in for a terminal without a pty.
func TestNoPromptRefusesTrustOnAnInteractiveSession(t *testing.T) {
	terminal, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = terminal.Close() })

	var asked strings.Builder
	prompting := NewCLI(WithInput(terminal), WithTerminal(&asked))
	prompting.promptTrust("db:7433", "SHA256:abc")
	if !strings.Contains(asked.String(), "Trust this server?") {
		t.Fatalf("a terminal session no longer prompts, so this test proves nothing:\n%s", asked.String())
	}

	var refused strings.Builder
	quiet := NewCLI(WithInput(terminal), WithTerminal(&refused), WithNoPrompt(true))
	if quiet.promptTrust("db:7433", "SHA256:abc") {
		t.Error("NoPrompt trusted an unknown server")
	}
	if strings.Contains(refused.String(), "Trust this server?") {
		t.Errorf("NoPrompt still asked:\n%s", refused.String())
	}
	if !strings.Contains(refused.String(), "Refusing to trust it non-interactively") {
		t.Errorf("NoPrompt did not say why it refused:\n%s", refused.String())
	}
}
