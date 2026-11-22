package server

import (
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Internode TLS pinning tests. The acceptance test is
// TestInternodeRejectsUnpinnedCertificate: it must fail on the pre-fix code
// (where every internode dial passed a nil known-servers store and therefore
// accepted any certificate) and pass after the fix.

const pinnedTestToken = "pinned-test-cluster-token"

// startPinnedTestNode boots one real server with an internode listener and
// audit enabled, and returns the service, the audit log path, and a
// stop-once closure (Stop closes the audit logger, so a double Stop would
// panic; callers must be able to stop before reading the log).
func startPinnedTestNode(t *testing.T, nodeID string) (*Service, string, func()) {
	t.Helper()
	dir := t.TempDir()
	if err := SaveClusterToken(dir, ClusterToken{
		Token:     pinnedTestToken,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("save cluster token: %v", err)
	}
	auditPath := filepath.Join(dir, "audit.jsonl")
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dir
	cfg.Server.Listen = "127.0.0.1:0"
	cfg.Server.NodeID = nodeID
	// ReplicationFactor 2 enables the cluster machinery so NodeUser auth is
	// meaningful, without configuring any seed (no pinning configured here).
	cfg.Server.ReplicationFactor = 2
	cfg.TCP.Listen = "127.0.0.1:0"
	cfg.TCP.InternodeListen = "127.0.0.1:0"
	cfg.Metrics.Enabled = false
	cfg.Audit.Enabled = true
	cfg.Audit.Output = auditPath
	cfg.Logging.Output = "stderr"

	srv, err := NewService(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if srv.internodeTCPLn == nil {
		t.Fatal("internode listener not started")
	}
	var once sync.Once
	stop := func() { once.Do(func() { _ = srv.Stop() }) }
	t.Cleanup(stop)
	return srv, auditPath, stop
}

// newTestInternodeTLSConfig builds an internode client config through the
// production pinned builder, so a test that needs a verified internode dial
// exercises the same trust policy the server does.
func newTestInternodeTLSConfig(t *testing.T, host string, expected []string) *tls.Config {
	t.Helper()
	fs, err := transport.NewFingerprintSet(expected, nil)
	if err != nil {
		t.Fatalf("NewFingerprintSet: %v", err)
	}
	cfg, err := fs.ClientConfig(host)
	if err != nil {
		t.Fatalf("ClientConfig: %v", err)
	}
	return cfg
}

// TestInternodeRejectsUnpinnedCertificate is the acceptance test for internode pinning.
// An internode dial pinned to a fingerprint the server does not
// present must abort at the TLS handshake, before any AUTH frame (and thus
// the cluster token) is written.
func TestInternodeRejectsUnpinnedCertificate(t *testing.T) {
	srv, auditPath, stop := startPinnedTestNode(t, "pinned-b")
	internodeAddr := srv.internodeTCPLn.Addr().String()

	// A fingerprint that is not this node's, and a fresh store, so nothing
	// can rescue the dial: the expected set cannot match and no peer has been
	// learned yet.
	wrong := "SHA256:" + strings.Repeat("00", 32)
	fs, err := transport.NewFingerprintSet([]string{wrong}, nil)
	if err != nil {
		t.Fatalf("NewFingerprintSet: %v", err)
	}
	tlsCfg, err := fs.ClientConfig(internodeAddr)
	if err != nil {
		t.Fatalf("ClientConfig: %v", err)
	}

	_, err = transport.NewTCPPeerClient(context.Background(), internodeAddr, tlsCfg, pinnedTestToken, 1)
	if err == nil {
		t.Fatal("dial completed against a certificate that does not match the pinned fingerprint")
	}

	// The cluster token must not have been transmitted: a refused handshake
	// writes no AUTH frame, and the audit log is the evidence.
	stop()
	data, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if strings.Contains(string(data), `"event":"auth.`) {
		t.Fatalf("cluster token reached the server after a refused handshake; audit log records an auth event:\n%s", data)
	}
}

// TestPinnedDialAcceptsExpectedFingerprint is the positive control: a fix
// that locks out legitimate joins would pass the rejection test and break
// the product.
func TestPinnedDialAcceptsExpectedFingerprint(t *testing.T) {
	srv, _, stop := startPinnedTestNode(t, "pinned-ok")
	defer stop()
	internodeAddr := srv.internodeTCPLn.Addr().String()

	fs, err := transport.NewFingerprintSet([]string{srv.core.fingerprint}, nil)
	if err != nil {
		t.Fatalf("NewFingerprintSet: %v", err)
	}
	tlsCfg, err := fs.ClientConfig(internodeAddr)
	if err != nil {
		t.Fatalf("ClientConfig: %v", err)
	}

	peer, err := transport.NewTCPPeerClient(context.Background(), internodeAddr, tlsCfg, pinnedTestToken, 1)
	if err != nil {
		t.Fatalf("pinned dial to the expected fingerprint failed: %v", err)
	}
	t.Cleanup(func() { _ = peer.Close() })
}

// TestJoinRequiresSeedFingerprints proves the fail-closed configuration
// check: seeds without fingerprints refuses to load, and the error names the
// variable and tells the operator where to read the value.
func TestJoinRequiresSeedFingerprints(t *testing.T) {
	t.Setenv("HOARDDB_SEED_NODES", "127.0.0.1:4433")
	t.Setenv("HOARDDB_SEED_FINGERPRINTS", "")
	_, err := config.LoadAllowingOldReplicatedMode("")
	if err == nil {
		t.Fatal("config with seed_nodes and no seed_fingerprints was accepted; must fail closed")
	}
	for _, want := range []string{"HOARDDB_SEED_FINGERPRINTS", "fingerprint"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("startup error does not name %q:\n%v", want, err)
		}
	}
}

// TestFingerprintFormatUnified: the value computed from a PEM certificate and
// the value computed from the parsed public key are the same canonical
// string, and ParseFingerprint accepts it. A mismatch here fails every join
// in a confusing way.
func TestFingerprintFormatUnified(t *testing.T) {
	dir := t.TempDir()
	paths, _, err := transport.GenerateEd25519Keys(dir)
	if err != nil {
		t.Fatalf("GenerateEd25519Keys: %v", err)
	}
	certPEM, err := os.ReadFile(paths.Cert)
	if err != nil {
		t.Fatalf("read certificate: %v", err)
	}
	got, err := FingerprintFromCert(certPEM)
	if err != nil {
		t.Fatalf("FingerprintFromCert: %v", err)
	}
	cert, err := tls.LoadX509KeyPair(paths.Cert, paths.Key)
	if err != nil {
		t.Fatalf("load key pair: %v", err)
	}
	pub, err := transport.ParseEd25519PublicKey(cert)
	if err != nil {
		t.Fatalf("parse public key: %v", err)
	}
	if want := transport.Fingerprint(pub); got != want {
		t.Fatalf("FingerprintFromCert = %q, transport.Fingerprint = %q", got, want)
	}
	if _, err := transport.ParseFingerprint(got); err != nil {
		t.Fatalf("ParseFingerprint rejects the canonical value %q: %v", got, err)
	}
}

// TestSingleNodeUnaffected: no seeds means no pinning, no new failure mode,
// and no cluster state (the common case).
func TestSingleNodeUnaffected(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Server.Listen = "127.0.0.1:0"
	cfg.TCP.Listen = "127.0.0.1:0"
	cfg.TCP.InternodeListen = "127.0.0.1:0"
	cfg.Metrics.Enabled = false
	cfg.Audit.Enabled = false
	cfg.Logging.Output = "stderr"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("single-node config rejected: %v", err)
	}
	srv, err := NewService(cfg, discardLogger())
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if srv.core.cluster != nil {
		t.Fatal("a single node with no seeds started cluster state")
	}
}
