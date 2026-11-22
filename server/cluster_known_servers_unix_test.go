//go:build unix

package server

import (
	"crypto/ed25519"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// TestClusterKnownServersCreatesOwnerOnlyFiles (BUG-0532): the server's own
// learned-peer store under <data dir>/cluster is created 0700/0600 even under
// umask 0, so it passes the checks it is held to on the next start.
func TestClusterKnownServersCreatesOwnerOnlyFiles(t *testing.T) {
	previous := syscall.Umask(0)
	t.Cleanup(func() { syscall.Umask(previous) })
	dir := newTestServer(t).config.Storage.DataDir
	cluster, err := knownServersClusterAt(t, dir)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	peerKey := ed25519.PublicKey(make([]byte, ed25519.PublicKeySize))
	if err := cluster.knownServers.Trust("127.0.0.1:4434", peerKey); err != nil {
		t.Fatalf("learn a peer: %v", err)
	}
	clusterDir := filepath.Join(dir, "cluster")
	for path, want := range map[string]os.FileMode{clusterDir: 0o700, filepath.Join(clusterDir, "known_servers"): 0o600} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != want {
			t.Errorf("%s is %04o, want %04o", path, got, want)
		}
	}
	if _, err := knownServersClusterAt(t, dir); err != nil {
		t.Fatalf("a restart on the files the node created itself was refused: %v", err)
	}
}
