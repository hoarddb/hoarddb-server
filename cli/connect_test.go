package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestFailedConnectKeepsTheCurrentSession: Reconnect closed the
// working connection before it dialled, so a mistyped address left the session
// pointing at the old server with no connection.
func TestFailedConnectKeepsTheCurrentSession(t *testing.T) {
	session := newGoldenSession(t)
	err := session.cli.handle("connect 127.0.0.1:1")
	if err == nil {
		t.Fatal("connect to a dead address succeeded")
	}
	if !strings.Contains(err.Error(), session.addr) {
		t.Errorf("error %q does not say the session is still connected to %s", err, session.addr)
	}
	session.out.Reset()
	if err := session.cli.handle("show databases"); err != nil {
		t.Fatalf("the session was lost by a failed connect: %v", err)
	}
}

// TestSuccessfulConnectClosesTheOldConnection keeps the reconnect path from
// leaking a socket: the old connection is closed once the new one is in place.
func TestSuccessfulConnectClosesTheOldConnection(t *testing.T) {
	session := newGoldenSession(t)
	oldConn := session.cli.conn
	if err := session.cli.handle("connect " + session.addr); err != nil {
		t.Fatalf("connect to the same server: %v", err)
	}
	if session.cli.conn == oldConn {
		t.Fatal("connect kept the old connection")
	}
	if _, err := oldConn.SendRequest(context.Background(), transport.OpUserList, nil); err == nil {
		t.Error("the old connection is still usable after a successful connect")
	}
	if err := session.cli.handle("show databases"); err != nil {
		t.Fatalf("show databases on the new connection: %v", err)
	}
}

// TestBuildTLSConfigInsecureSkipsVerification pins WithInsecureTLS's
// contract: it is the named, loud escape hatch (the driver-spec record
// §3.2) — no VerifyPeerCertificate callback is installed, and the operator
// is warned by name, every time, not just once. The name in the warning is the
// flag that caused it, not the address: the address is on the "Connected to"
// line the session prints on the next line, and repeating it here is what made
// the notice print twice (2026-10-11).
func TestBuildTLSConfigInsecureSkipsVerification(t *testing.T) {
	var out bytes.Buffer
	c := NewCLI(WithAddress("127.0.0.1:0"), WithTerminal(&out), WithInsecureTLS(true))

	tlsCfg, err := c.buildTLSConfig()
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if tlsCfg.VerifyPeerCertificate != nil {
		t.Fatal("WithInsecureTLS(true) still installed a verification callback")
	}
	if !strings.Contains(out.String(), "WARNING") || !strings.Contains(out.String(), "--insecure") {
		t.Fatalf("insecure mode did not warn by name: %q", out.String())
	}
}

// TestBuildTLSConfigDefaultInstallsVerification pins the opposite default:
// without WithInsecureTLS, the CLI must install real verification — this is
// the actual bug being fixed (MakeClientTLSConfig used to be called with a
// nil knownServers store unconditionally, so this callback was always nil).
func TestBuildTLSConfigDefaultInstallsVerification(t *testing.T) {
	var out bytes.Buffer
	c := NewCLI(
		WithAddress("127.0.0.1:0"),
		WithTerminal(&out),
		WithKnownServersPath(cliKnownServersPath(t)),
	)

	tlsCfg, err := c.buildTLSConfig()
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if tlsCfg.VerifyPeerCertificate == nil {
		t.Fatal("default (non-insecure) TLS config installed no verification callback — the client would authenticate nothing")
	}
	if strings.Contains(out.String(), "WARNING") {
		t.Fatalf("default (secure) path printed the insecure warning: %q", out.String())
	}
}

// TestPromptTrustRefusesNonInteractively pins promptTrust's safe failure
// mode: a script or pipe has no operator to confirm a new server's
// fingerprint, so it must refuse rather than silently trusting (or hanging
// waiting for input that will never come).
func TestPromptTrustRefusesNonInteractively(t *testing.T) {
	var out bytes.Buffer
	// A strings.Reader is not an *os.File, so CLI.interactive() reports
	// false regardless of what bytes it holds — the same as a real pipe.
	c := NewCLI(WithTerminal(&out), WithInput(strings.NewReader("y\n")))

	if c.promptTrust("example.com:7433", "SHA256:deadbeef") {
		t.Fatal("promptTrust trusted an unknown server non-interactively")
	}
	if !strings.Contains(out.String(), "Refusing to trust it non-interactively") {
		t.Fatalf("refusal did not explain the escape hatch: %q", out.String())
	}
	if !strings.Contains(out.String(), "SHA256:deadbeef") {
		t.Fatalf("prompt did not show the fingerprint to verify: %q", out.String())
	}
}
