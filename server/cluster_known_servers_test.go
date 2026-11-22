package server

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
)

// knownServersClusterAt is the smallest NewCluster call over dir: one node,
// no seeds, a cluster token saved first so the call reaches known_servers.
func knownServersClusterAt(t *testing.T, dir string) (*Cluster, error) {
	t.Helper()
	token, err := GenerateClusterToken()
	if err != nil {
		t.Fatalf("generate cluster token: %v", err)
	}
	if err := SaveClusterToken(dir, ClusterToken{Token: token, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("save cluster token: %v", err)
	}
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dir
	cfg.Server.Listen = "127.0.0.1:4433"
	cfg.Server.NodeID = "n1"
	cfg.Server.ReplicationFactor = 1
	return NewCluster(newTestServer(t), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// TestClusterKnownServersRefusesUnsafeClusterDir (BUG-0532): a cluster
// directory other users can write, as a volume prepared by another tool can
// be, stops the node with the chmod that fixes it instead of being trusted.
func TestClusterKnownServersRefusesUnsafeClusterDir(t *testing.T) {
	s := newTestServer(t)
	clusterDir := filepath.Join(s.config.Storage.DataDir, "cluster")
	if err := os.Mkdir(clusterDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(clusterDir, 0o777); err != nil {
		t.Fatal(err)
	}
	_, err := knownServersClusterAt(t, s.config.Storage.DataDir)
	if err == nil {
		t.Fatal("NewCluster accepted a world-writable cluster directory")
	}
	if !strings.Contains(err.Error(), "chmod 700 "+clusterDir) {
		t.Fatalf("error %q does not give chmod 700 %s", err, clusterDir)
	}
}

// TestClusterKnownServersRefusesForeignOwner (BUG-0532): a cluster
// directory owned by another user (a container volume from another uid) is
// refused with the chown. Needs root to give a directory another owner; the
// decision is covered without root by transport's TestTrustPathRefusalTable.
func TestClusterKnownServersRefusesForeignOwner(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root to give a directory another owner; transport's TestTrustPathRefusalTable covers the decision without a real uid")
	}
	s := newTestServer(t)
	clusterDir := filepath.Join(s.config.Storage.DataDir, "cluster")
	if err := os.Mkdir(clusterDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(clusterDir, 54321, 54321); err != nil {
		t.Fatal(err)
	}
	_, err := knownServersClusterAt(t, s.config.Storage.DataDir)
	if err == nil || !strings.Contains(err.Error(), "chown") {
		t.Fatalf("err = %v, want a refusal giving the chown", err)
	}
}
