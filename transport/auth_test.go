package transport

import (
	"crypto/ed25519"
	"crypto/tls"
	"encoding/hex"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

// tlsServerFixture starts a bare TLS listener with a freshly generated
// ed25519 self-signed certificate and accepts (and immediately closes) one
// connection per call to accept — enough to drive a client-side handshake
// without any hoardDB framing.
type tlsServerFixture struct {
	ln net.Listener
}

func newTLSServerFixture(t *testing.T) *tlsServerFixture {
	t.Helper()
	dir := t.TempDir()
	paths, _, err := GenerateEd25519Keys(dir)
	if err != nil {
		t.Fatalf("generate server keys: %v", err)
	}
	tlsCfg, _, err := LoadServerTLSConfigFromPaths(paths)
	if err != nil {
		t.Fatalf("load server TLS config: %v", err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", tlsCfg)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return &tlsServerFixture{ln: ln}
}

// accept accepts and completes the handshake on one connection, then closes
// it, so the client-side dial that triggered it can observe the handshake
// outcome (success or the client's own VerifyPeerCertificate rejection).
func (f *tlsServerFixture) accept(t *testing.T) {
	t.Helper()
	conn, err := f.ln.Accept()
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	tc, ok := conn.(*tls.Conn)
	if !ok {
		return
	}
	_ = tc.SetDeadline(time.Now().Add(5 * time.Second))
	_ = tc.Handshake()
}

// TestClientVerifyPeerCertFuncTrustsOnFirstSightThenPinsIt pins the actual
// TOFU contract cli.go's buildTLSConfig now relies on: an unknown server is
// only trusted if promptFunc says so, and once trusted, a later connection
// to the same host with the *same* key succeeds without prompting again.
func TestClientVerifyPeerCertFuncTrustsOnFirstSightThenPinsIt(t *testing.T) {
	srv := newTLSServerFixture(t)
	addr := srv.ln.Addr().String()

	known, err := NewKnownServers(knownServersTestPath(t))
	if err != nil {
		t.Fatalf("new known servers: %v", err)
	}

	prompted := 0
	promptFunc := func(host, fingerprint string) bool {
		prompted++
		return true // accept
	}

	go srv.accept(t)
	clientCfg := &tls.Config{
		InsecureSkipVerify:    true, //nolint:gosec // test dial mirrors MakeClientTLSConfig's real pattern
		VerifyPeerCertificate: ClientVerifyPeerCertFunc(known, addr, promptFunc),
	}
	conn, err := tls.Dial("tcp", addr, clientCfg)
	if err != nil {
		t.Fatalf("first dial (unknown server, prompt accepts): %v", err)
	}
	_ = conn.Close()
	if prompted != 1 {
		t.Fatalf("prompted %d times on first connection, want exactly 1", prompted)
	}
	if ok, _ := known.IsKnownFingerprint(addr); !ok {
		t.Fatal("accepting the prompt did not persist the server's key to the known-servers store")
	}

	// Second connection to the same host, same key: must succeed without
	// prompting again — the whole point of pinning it the first time.
	go srv.accept(t)
	promptFunc2Called := false
	clientCfg2 := &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // test dial mirrors MakeClientTLSConfig's real pattern
		VerifyPeerCertificate: ClientVerifyPeerCertFunc(known, addr, func(string, string) bool {
			promptFunc2Called = true
			return true
		}),
	}
	conn2, err := tls.Dial("tcp", addr, clientCfg2)
	if err != nil {
		t.Fatalf("second dial (known server, same key): %v", err)
	}
	_ = conn2.Close()
	if promptFunc2Called {
		t.Fatal("a known, matching server key re-triggered the trust prompt")
	}
}

// TestClientVerifyPeerCertFuncRejectsUnknownServerWhenPromptDeclines pins the
// refusal path cli.go's promptTrust uses for a non-interactive session: when
// promptFunc returns false, the connection must fail — never silently
// proceed. This is the exact bug being fixed: MakeClientTLSConfig used to be
// called with a nil knownServers store, which skipped this check entirely.
func TestClientVerifyPeerCertFuncRejectsUnknownServerWhenPromptDeclines(t *testing.T) {
	srv := newTLSServerFixture(t)
	addr := srv.ln.Addr().String()

	known, err := NewKnownServers(knownServersTestPath(t))
	if err != nil {
		t.Fatalf("new known servers: %v", err)
	}

	go srv.accept(t)
	clientCfg := &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // test dial mirrors MakeClientTLSConfig's real pattern
		VerifyPeerCertificate: ClientVerifyPeerCertFunc(known, addr, func(string, string) bool {
			return false // decline
		}),
	}
	if _, err := tls.Dial("tcp", addr, clientCfg); err == nil {
		t.Fatal("dial succeeded against an unknown server after the trust prompt declined")
	}
	if ok, _ := known.IsKnownFingerprint(addr); ok {
		t.Fatal("a declined prompt must not persist the server's key")
	}
}

// TestClientVerifyPeerCertFuncHardFailsOnKeyMismatch pins the other named
// requirement (the driver-spec record §3.2): "mismatch = hard failure."
// A server whose key no longer matches what was previously pinned for that
// host must never be silently re-trusted, regardless of promptFunc — a
// changed key is exactly the case TOFU exists to catch (a MITM presenting a
// different key than the one the client trusted before).
func TestClientVerifyPeerCertFuncHardFailsOnKeyMismatch(t *testing.T) {
	srv1 := newTLSServerFixture(t)
	addr := srv1.ln.Addr().String()

	known, err := NewKnownServers(knownServersTestPath(t))
	if err != nil {
		t.Fatalf("new known servers: %v", err)
	}

	go srv1.accept(t)
	firstCfg := &tls.Config{
		InsecureSkipVerify:    true, //nolint:gosec // test dial mirrors MakeClientTLSConfig's real pattern
		VerifyPeerCertificate: ClientVerifyPeerCertFunc(known, addr, func(string, string) bool { return true }),
	}
	conn, err := tls.Dial("tcp", addr, firstCfg)
	if err != nil {
		t.Fatalf("pin the first server's key: %v", err)
	}
	_ = conn.Close()

	// A second server, listening on a *different* address but verified
	// against the *first* server's known-servers entry (host string reused
	// deliberately) — standing in for a MITM presenting a different key for
	// the same host the client already trusts.
	srv2 := newTLSServerFixture(t)
	go func() {
		for {
			c, err := srv2.ln.Accept()
			if err != nil {
				return
			}
			tc, ok := c.(*tls.Conn)
			if ok {
				_ = tc.SetDeadline(time.Now().Add(5 * time.Second))
				_ = tc.Handshake()
			}
			_ = c.Close()
		}
	}()

	promptCalledAgain := false
	secondCfg := &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // test dial mirrors MakeClientTLSConfig's real pattern
		VerifyPeerCertificate: ClientVerifyPeerCertFunc(known, addr, func(string, string) bool {
			// Even an operator who would say "yes" must not be asked: a key
			// change is refused outright, not re-prompted.
			promptCalledAgain = true
			return true
		}),
	}
	if _, err := tls.Dial("tcp", srv2.ln.Addr().String(), secondCfg); err == nil {
		t.Fatal("dial succeeded against a server presenting a different key for an already-known host")
	}
	if promptCalledAgain {
		t.Fatal("a key mismatch re-invoked the trust prompt instead of failing outright")
	}
}

// TestKnownServersRefusesTwoKeysForOneHost pins that a file holding two
// different keys for one host must not load, because keeping the last line
// lets an appended line override the key the person verified.
func TestKnownServersRefusesTwoKeysForOneHost(t *testing.T) {
	firstKey := strings.Repeat("11", ed25519.PublicKeySize)
	secondKey := strings.Repeat("22", ed25519.PublicKeySize)
	hostLine := func(key string) string { return "n1:7433 " + key + "\n" }

	tests := []struct {
		name      string
		content   string
		wantError bool
	}{
		{"two different keys", hostLine(firstKey) + hostLine(secondKey), true},
		{"the same key twice", hostLine(firstKey) + hostLine(firstKey), false},
		{"two hosts one key each", hostLine(firstKey) + "n2:7433 " + secondKey + "\n", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := knownServersTestPath(t)
			if err := os.WriteFile(path, []byte(tc.content), 0o600); err != nil {
				t.Fatalf("write known_servers: %v", err)
			}
			_, err := NewKnownServers(path)
			if !tc.wantError {
				if err != nil {
					t.Fatalf("NewKnownServers: unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("NewKnownServers accepted two different keys for one host")
			}
			for _, want := range []string{path, "n1:7433", Fingerprint(mustKey(t, firstKey)), Fingerprint(mustKey(t, secondKey)), "delete the line"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not mention %q", err, want)
				}
			}
		})
	}
}

func mustKey(t *testing.T, hexKey string) ed25519.PublicKey {
	t.Helper()
	keyBytes, err := hex.DecodeString(hexKey)
	if err != nil {
		t.Fatalf("decode key: %v", err)
	}
	return ed25519.PublicKey(keyBytes)
}
