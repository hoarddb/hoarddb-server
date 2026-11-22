package cli

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"flag"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ringclient"
	"github.com/hoarddb/hoarddb-server/transport"
)

// verbDialFlags parses a verb's connection flags the way a verb runner does,
// and keeps the flag set so the dial can tell an explicit -address from the
// configuration file's.
func verbDialFlags(t *testing.T, args ...string) (*flag.FlagSet, *connectionFlags) {
	t.Helper()
	fs := flag.NewFlagSet("hoarddb ingest", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	connection := registerConnectionFlags(fs)
	if err := fs.Parse(args); err != nil {
		t.Fatalf("parsing the connection flags: %v", err)
	}
	return fs, connection
}

// TestVerbDialRefusesWithoutFingerprintOrInsecure: a verb refuses a ring it
// cannot trust before it dials, with the session's own text and exit code, so
// the two entry points cannot drift apart.
func TestVerbDialRefusesWithoutFingerprintOrInsecure(t *testing.T) {
	fs, connection := verbDialFlags(t, "-address", "127.0.0.1:1", "-user", "root", "-password=pw")
	var stderr bytes.Buffer
	client, redial, exit := dialRingForVerb(context.Background(), &stderr, fs, connection)
	if client != nil || redial != nil {
		t.Fatalf("a dial that trusts nothing returned a client (%v) and a redial (%v)", client, redial != nil)
	}
	if exit != ExitConnect {
		t.Fatalf("the verb refused with exit %d, want ExitConnect (%d): %q", exit, ExitConnect, stderr.String())
	}
	if !strings.Contains(stderr.String(), "-fingerprint SHA256:<hex>") || !strings.Contains(stderr.String(), "-insecure") {
		t.Fatalf("the refusal names neither fix: %q", stderr.String())
	}

	// The session's own text, driven through RunMain with no server needed:
	// the trust rule refuses before any dial.
	var sessionErr bytes.Buffer
	status := RunMain(Invocation{
		Args:   []string{"-address", "127.0.0.1:1", "-user", "root", "-password=pw"},
		Stdin:  strings.NewReader(""),
		Stdout: io.Discard, Stderr: &sessionErr,
		Getenv: func(string) string { return "" },
	})
	if status != ExitConnect {
		t.Fatalf("the session refused with exit %d, want ExitConnect (%d): %q", status, ExitConnect, sessionErr.String())
	}
	if stderr.String() != sessionErr.String() {
		t.Fatalf("the verb says %q, the session says %q", stderr.String(), sessionErr.String())
	}
}

// TestVerbDialUsesCredentialsFromTheFlags: the verb dial honours -address,
// -user, -password and -insecure from its own flag set, and prints the
// insecure warning on the stream it was given.
func TestVerbDialUsesCredentialsFromTheFlags(t *testing.T) {
	addr := u52LiveServer(t)
	fs, connection := verbDialFlags(t, "-address", addr, "-user", "root", "-password=live-test-password", "-insecure")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var stderr bytes.Buffer
	client, redial, exit := dialRingForVerb(ctx, &stderr, fs, connection)
	if client == nil {
		t.Fatalf("the verb dial failed with exit %d: %q", exit, stderr.String())
	}
	if exit != ExitOK {
		t.Fatalf("a working dial returned exit %d, want ExitOK (%d)", exit, ExitOK)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("closing the client: %v", err)
		}
	})
	if redial == nil {
		t.Fatal("the verb dial returned no redial")
	}
	if _, err := client.ShowDatabases(ctx, ringclient.PageRequest{Limit: 10}); err != nil {
		t.Fatalf("the client the verb dial returned does not answer: %v", err)
	}
	if !strings.Contains(stderr.String(), "TLS certificate verification is disabled") {
		t.Fatalf("the insecure dial printed no warning: %q", stderr.String())
	}
}

// TestRedialReturnsAWorkingClientAfterTheFirstIsClosed: a connection that died
// mid-run is replaced by a new one with the same options (build plan 3.4 step
// 6), and the replacement answers.
func TestRedialReturnsAWorkingClientAfterTheFirstIsClosed(t *testing.T) {
	addr := u52LiveServer(t)
	fs, connection := verbDialFlags(t, "-address", addr, "-user", "root", "-password=live-test-password", "-insecure")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var stderr bytes.Buffer
	first, redial, exit := dialRingForVerb(ctx, &stderr, fs, connection)
	if first == nil {
		t.Fatalf("the verb dial failed with exit %d: %q", exit, stderr.String())
	}
	if err := first.Close(); err != nil {
		t.Fatalf("closing the first client: %v", err)
	}
	second, err := redial(ctx)
	if err != nil {
		t.Fatalf("redial after the first client closed: %v", err)
	}
	t.Cleanup(func() {
		if err := second.Close(); err != nil {
			t.Errorf("closing the redialed client: %v", err)
		}
	})
	if second == first {
		t.Fatal("redial returned the closed client")
	}
	if _, err := second.ShowDatabases(ctx, ringclient.PageRequest{Limit: 10}); err != nil {
		t.Fatalf("the redialed client does not answer: %v", err)
	}
}

// liveServerFingerprint reads the fingerprint a ring server presents, the value
// the server prints when it starts, so a test can pin the right key and a wrong one.
func liveServerFingerprint(t *testing.T, addr string) string {
	t.Helper()
	conn, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true}) //nolint:gosec // reading the presented key to pin it, in a test
	if err != nil {
		t.Fatalf("reading the server's key: %v", err)
	}
	defer func() { _ = conn.Close() }()
	pub, ok := conn.ConnectionState().PeerCertificates[0].PublicKey.(ed25519.PublicKey)
	if !ok {
		t.Fatal("the server presented no ed25519 key")
	}
	return transport.Fingerprint(pub)
}

// TestVerbDialHonoursFingerprint: a verb dial pins the server by -fingerprint. A
// wrong pin is ExitConnect and no client; the right pin connects, with no
// insecure warning; a pin given twice is refused as usage, as in the session.
func TestVerbDialHonoursFingerprint(t *testing.T) {
	addr := u52LiveServer(t)
	right := liveServerFingerprint(t, addr)
	wrong := "SHA256:" + strings.Repeat("00", 32)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	base := []string{"-address", addr, "-user", "root", "-password=live-test-password"}

	var wrongErr bytes.Buffer
	fs, connection := verbDialFlags(t, append(base, "-fingerprint", wrong)...)
	client, redial, exit := dialRingForVerb(ctx, &wrongErr, fs, connection)
	if client != nil || redial != nil {
		t.Fatal("a wrong pin returned a client")
	}
	if exit != ExitConnect {
		t.Fatalf("a wrong pin exited %d, want ExitConnect (%d): %q", exit, ExitConnect, wrongErr.String())
	}

	var rightErr bytes.Buffer
	fs, connection = verbDialFlags(t, append(base, "-fingerprint", right)...)
	client, redial, exit = dialRingForVerb(ctx, &rightErr, fs, connection)
	if client == nil || redial == nil || exit != ExitOK {
		t.Fatalf("the right pin did not connect: exit %d: %q", exit, rightErr.String())
	}
	opened := client
	t.Cleanup(func() {
		if err := opened.Close(); err != nil {
			t.Errorf("closing the client: %v", err)
		}
	})
	if strings.Contains(rightErr.String(), "TLS certificate verification is disabled") {
		t.Fatalf("a pinned dial printed the insecure warning: %q", rightErr.String())
	}

	var twiceErr bytes.Buffer
	fs, connection = verbDialFlags(t, append(base, "-fingerprint", right, "-fingerprint", wrong)...)
	client, _, exit = dialRingForVerb(ctx, &twiceErr, fs, connection)
	if client != nil || exit != ExitUsage {
		t.Fatalf("a pin given twice: exit %d, want ExitUsage (%d)", exit, ExitUsage)
	}
	if !strings.Contains(twiceErr.String(), ringFingerprintTwiceText) {
		t.Fatalf("the twice refusal says %q", twiceErr.String())
	}
}
