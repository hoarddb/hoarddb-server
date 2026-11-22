package server

import (
	"os"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
)

// TestSealedNodeStartedOffNamesTheMode: A node sealed at
// mode=require, with a bootstrapped users.json, restarted with encryption off
// must be refused with the node store's own diagnosis — the store is
// encrypted, and encryption.mode is the setting to fix. NewService read the
// sealed users.json before it opened the store, so the refusal the operator
// saw was users.json's "node master is nil", which names neither.
func TestSealedNodeStartedOffNamesTheMode(t *testing.T) {
	dataDir := t.TempDir()
	keyPath := makeKeyFile(t, t.TempDir(), 0x40)
	srv, err := NewService(sealedServiceConfig(dataDir, keyPath, "require"), quietLogger())
	if err != nil {
		t.Fatalf("NewService at mode=require: %v", err)
	}
	if err := srv.SeedRootUser("admin", "sealed-started-off-password"); err != nil {
		t.Fatalf("SeedRootUser: %v", err)
	}
	if err := srv.Stop(); err != nil {
		t.Fatalf("stopping the sealed node: %v", err)
	}
	users, err := os.ReadFile(UsersFilePath(dataDir))
	if err != nil || !isEncryptedUsersFile(users) {
		t.Fatalf("test setup: users.json is not sealed (%v); the case under test is a sealed users file", err)
	}

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dataDir
	cfg.Metrics.Enabled = false
	cfg.Audit.Enabled = false
	restarted, err := NewService(cfg, quietLogger())
	if err == nil {
		_ = restarted.Stop()
		t.Fatal("a sealed node started with encryption off")
	}
	for _, want := range []string{"is encrypted", "encryption.mode"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q: %v", want, err)
		}
	}
}
