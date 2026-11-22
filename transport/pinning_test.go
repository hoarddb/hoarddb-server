package transport

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"log/slog"
	"strings"
	"testing"
)

// TestParseFingerprint: the canonical form is accepted, case-insensitively,
// and everything else is refused with the expected shape named.
func TestParseFingerprint(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	canonical := Fingerprint(pub)

	got, err := ParseFingerprint(canonical)
	if err != nil {
		t.Fatalf("ParseFingerprint(%q): %v", canonical, err)
	}
	if got != canonical {
		t.Fatalf("ParseFingerprint(%q) = %q, want %q", canonical, got, canonical)
	}

	upper := "SHA256:" + strings.ToUpper(strings.TrimPrefix(canonical, "SHA256:"))
	got, err = ParseFingerprint(upper)
	if err != nil {
		t.Fatalf("ParseFingerprint(%q): %v", upper, err)
	}
	if got != canonical {
		t.Fatalf("ParseFingerprint(%q) = %q, want canonical %q", upper, got, canonical)
	}

	bad := []string{
		strings.TrimPrefix(canonical, "SHA256:"),         // bare hex, no prefix
		"SHA256:abcd",                                    // wrong length
		"SHA256:" + strings.Repeat("zz", 32),             // not hex
		"hoarddb-" + "sha256:" + strings.Repeat("a", 64), // retired prefix
		"",
	}
	for _, in := range bad {
		if _, err := ParseFingerprint(in); err == nil {
			t.Fatalf("ParseFingerprint(%q) accepted a malformed fingerprint", in)
		}
	}
}

// TestPeerLearnsAndPins: a fingerprint verified once is learned, a later dial
// succeeds from the learned store alone, and a changed certificate for a known
// host is a hard failure — never a silent re-trust.
func TestPeerLearnsAndPins(t *testing.T) {
	first := generatedCertDER(t)
	second := generatedCertDER(t)
	host := "127.0.0.1:4434"

	learned, err := NewKnownServers(knownServersTestPath(t))
	if err != nil {
		t.Fatalf("NewKnownServers: %v", err)
	}

	// The operator's list verifies the first dial and learns the key.
	expected, err := NewFingerprintSet([]string{Fingerprint(first.pub)}, learned)
	if err != nil {
		t.Fatalf("NewFingerprintSet: %v", err)
	}
	if err := expected.Verify(host)([][]byte{first.der}, nil); err != nil {
		t.Fatalf("first dial with the configured fingerprint: %v", err)
	}

	// The same host is now trusted from the learned store alone.
	fromLearned, err := NewFingerprintSet(nil, learned)
	if err != nil {
		t.Fatalf("NewFingerprintSet(learned): %v", err)
	}
	if err := fromLearned.Verify(host)([][]byte{first.der}, nil); err != nil {
		t.Fatalf("second dial from the learned store: %v", err)
	}

	// A different certificate for the same host is refused.
	err = fromLearned.Verify(host)([][]byte{second.der}, nil)
	if err == nil {
		t.Fatal("a changed certificate for a known host was accepted")
	}
	if !strings.Contains(err.Error(), "has changed") {
		t.Fatalf("changed-certificate error does not explain the mismatch: %v", err)
	}
}

// TestInsecureTrustLogsReason: the one path that skips verification is named,
// requires a reason, and logs it; an empty policy is refused outright.
func TestInsecureTrustLogsReason(t *testing.T) {
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	_, err := MakeClientTLSConfig("", "", TrustPolicy{Insecure: &InsecureTrust{Reason: "benchmark harness"}}, "127.0.0.1:4433")
	if err != nil {
		t.Fatalf("insecure policy with a reason: %v", err)
	}
	if !strings.Contains(logged.String(), "benchmark harness") {
		t.Fatalf("insecure trust was not logged with its reason; log = %q", logged.String())
	}

	_, err = MakeClientTLSConfig("", "", TrustPolicy{Insecure: &InsecureTrust{}}, "127.0.0.1:4433")
	if err == nil {
		t.Fatal("insecure trust with no reason was accepted")
	}
	_, err = MakeClientTLSConfig("", "", TrustPolicy{}, "127.0.0.1:4433")
	if err == nil {
		t.Fatal("an empty trust policy was accepted; it must be a programming error")
	}
}

type generatedCert struct {
	der []byte
	pub ed25519.PublicKey
}

func generatedCertDER(t *testing.T) generatedCert {
	t.Helper()
	paths, _, err := GenerateEd25519Keys(t.TempDir())
	if err != nil {
		t.Fatalf("GenerateEd25519Keys: %v", err)
	}
	cert, err := tls.LoadX509KeyPair(paths.Cert, paths.Key)
	if err != nil {
		t.Fatalf("load key pair: %v", err)
	}
	pub, err := ParseEd25519PublicKey(cert)
	if err != nil {
		t.Fatalf("parse public key: %v", err)
	}
	return generatedCert{der: cert.Certificate[0], pub: pub}
}

// TestChangedKeyMessageNamesKnownServersFile: both refusals for a changed
// server key send the developer to the file hoardDB really has, by path, and
// never to SSH's known_hosts (BUG-0261).
func TestChangedKeyMessageNamesKnownServersFile(t *testing.T) {
	first := generatedCertDER(t)
	second := generatedCertDER(t)
	host := "127.0.0.1:4434"
	storePath := knownServersTestPath(t)

	learned, err := NewKnownServers(storePath)
	if err != nil {
		t.Fatalf("NewKnownServers: %v", err)
	}
	if err := learned.Trust(host, first.pub); err != nil {
		t.Fatalf("Trust: %v", err)
	}
	pinned, err := NewFingerprintSet(nil, learned)
	if err != nil {
		t.Fatalf("NewFingerprintSet: %v", err)
	}

	tests := []struct {
		name   string
		verify func([][]byte, [][]*x509.Certificate) error
	}{
		{"client trust-on-first-use", ClientVerifyPeerCertFunc(learned, host, nil)},
		{"internode pinning", pinned.Verify(host)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			refusal := tc.verify([][]byte{second.der}, nil)
			if refusal == nil {
				t.Fatal("a changed certificate for a known host was accepted")
			}
			message := refusal.Error()
			if strings.Contains(message, "known_hosts") {
				t.Errorf("message names SSH's known_hosts, which hoardDB does not have: %v", message)
			}
			if !strings.Contains(message, storePath) {
				t.Errorf("message does not carry the known_servers path %q: %v", storePath, message)
			}
		})
	}
}
