package cli

import (
	"bytes"
	"crypto/ed25519"
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// cliKnownServersPath is a known_servers path in a fresh 0700 directory.
// t.TempDir's own directories are 0777 less the umask (0775 under the
// common 0002), which the trust store's safety rule refuses (BUG-0532).
func cliKnownServersPath(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "hoarddb")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("create %s: %v", dir, err)
	}
	return filepath.Join(dir, "known_servers")
}

// serverCertificate is a server certificate's DER and its ed25519 key.
func serverCertificate(t *testing.T) ([]byte, ed25519.PublicKey) {
	t.Helper()
	paths, _, err := transport.GenerateEd25519Keys(t.TempDir())
	if err != nil {
		t.Fatalf("generate server keys: %v", err)
	}
	certificate, err := tls.LoadX509KeyPair(paths.Cert, paths.Key)
	if err != nil {
		t.Fatalf("load key pair: %v", err)
	}
	key, err := transport.ParseEd25519PublicKey(certificate)
	if err != nil {
		t.Fatalf("parse key: %v", err)
	}
	return certificate.Certificate[0], key
}

// TestCLIFindsServerKeyUnderAnotherSpelling: the CLI dialling
// "LocalHost.:7433" finds the key recorded as localhost:7433 and connects
// without asking.
func TestCLIFindsServerKeyUnderAnotherSpelling(t *testing.T) {
	der, key := serverCertificate(t)
	path := cliKnownServersPath(t)
	if err := os.WriteFile(path, fmt.Appendf(nil, "localhost:7433 %x\n", []byte(key)), 0o600); err != nil {
		t.Fatal(err)
	}
	var chatter bytes.Buffer
	c := NewCLI(WithAddress("LocalHost.:7433"), WithKnownServersPath(path), WithNoPrompt(true), WithTerminal(&chatter), WithChatter(&chatter))
	tlsConfig, err := c.buildTLSConfig()
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if err := tlsConfig.VerifyPeerCertificate([][]byte{der}, nil); err != nil {
		t.Fatalf("the key recorded for localhost:7433 was not found for LocalHost.:7433: %v", err)
	}
	if strings.Contains(chatter.String(), "authenticity") {
		t.Fatalf("a recorded server was offered as unknown: %s", chatter.String())
	}
}

// TestCLIRefusesChangedKeyUnderAnotherSpelling: a different key
// under another spelling of a recorded host is refused naming both
// fingerprints, never asked about.
func TestCLIRefusesChangedKeyUnderAnotherSpelling(t *testing.T) {
	der, presented := serverCertificate(t)
	_, recorded := serverCertificate(t)
	path := cliKnownServersPath(t)
	if err := os.WriteFile(path, fmt.Appendf(nil, "db.internal:7433 %x\n", []byte(recorded)), 0o600); err != nil {
		t.Fatal(err)
	}
	var chatter bytes.Buffer
	c := NewCLI(WithAddress("DB.Internal:7433"), WithKnownServersPath(path), WithTerminal(&chatter), WithChatter(&chatter), WithInput(strings.NewReader("y\n")))
	tlsConfig, err := c.buildTLSConfig()
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	refusal := tlsConfig.VerifyPeerCertificate([][]byte{der}, nil)
	if refusal == nil {
		t.Fatal("a changed key under another spelling was accepted")
	}
	for _, want := range []string{transport.Fingerprint(recorded), transport.Fingerprint(presented), "has changed", path} {
		if !strings.Contains(refusal.Error(), want) {
			t.Errorf("refusal %q does not name %q", refusal, want)
		}
	}
	if strings.Contains(chatter.String(), "authenticity") {
		t.Fatalf("a changed key was offered as a first sight: %s", chatter.String())
	}
}

// TestBuildTLSConfigRefusesUnsafeKnownServers (BUG-0532): a known_servers
// directory or file other users can write stops the CLI before it dials,
// with the chmod that fixes it.
func TestBuildTLSConfigRefusesUnsafeKnownServers(t *testing.T) {
	path := cliKnownServersPath(t)
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o666); err != nil {
		t.Fatal(err)
	}
	c := NewCLI(WithAddress("127.0.0.1:7433"), WithKnownServersPath(path), WithTerminal(&bytes.Buffer{}))
	_, err := c.buildTLSConfig()
	if err == nil {
		t.Fatal("a world-writable known_servers was accepted")
	}
	if !strings.Contains(err.Error(), "chmod 600 "+path) {
		t.Fatalf("error %q does not give chmod 600 %s", err, path)
	}
}
