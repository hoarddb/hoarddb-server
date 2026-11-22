package server

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/clusterpepper"
)

// TestAClusterNodeReportsItsPasswordKeyAndKeepsItInItsDataDirectory (spec 5.2, 5.4): a
// cluster-mode node reports password_key none_yet in its health before any user exists, and the
// key it creates for the first administrator is auth.cluster.pepper in its data directory, mode
// 0600 at encryption.mode off; a zero-config node reports no password_key. Break: leave
// ClusterOptions.Pepper unset in ringNodeSettings; red.
func TestAClusterNodeReportsItsPasswordKeyAndKeepsItInItsDataDirectory(t *testing.T) {
	qs := ringClusterService(t, ringClusterPorts(t), nil, nil)
	ringClusterStart(t, qs)
	health, err := qs.core.HealthCheck()
	if err != nil {
		t.Fatal(err)
	}
	if health.PasswordKey != wire.PasswordKeyNoneYet {
		t.Fatalf("health password_key = %q, want %q", health.PasswordKey, wire.PasswordKeyNoneYet)
	}
	pepper, err := qs.ringNode.BootstrapPepper()
	if err != nil {
		t.Fatalf("BootstrapPepper: %v", err)
	}
	path := filepath.Join(qs.config.Storage.DataDir, clusterpepper.FileName)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("the key is not in the data directory: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("%s is mode %v, want 0600", path, info.Mode().Perm())
	}
	onDisk, err := newClusterPepperFile(qs.core.credentials, qs.config.Storage.DataDir).Load()
	if err != nil || !onDisk.Equal(pepper) {
		t.Fatalf("the file holds another key: %v", err)
	}

	zero := newCapacityService(t, nil, nil)
	if err := zero.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = zero.Stop() })
	zeroHealth, err := zero.core.HealthCheck()
	if err != nil {
		t.Fatal(err)
	}
	if zeroHealth.PasswordKey != "" {
		t.Fatalf("a zero-config node reports password_key %q", zeroHealth.PasswordKey)
	}
}
