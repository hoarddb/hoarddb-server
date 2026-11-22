package server

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
)

// TestStandaloneDataDirRestartsWithSeeds is BUG-0320. A data directory first
// started standalone holds users whose users.json and password peppers hang
// off its own signing key, auth.token.key. Started again with seed nodes, the
// node signs with the key derived from the cluster token, so those users
// cannot be opened (or, with the keystore on, cannot log in). The owner's
// ruling (2026-10-18) is to refuse that start before anything is written,
// with an error that names the transition and the steps that do work, and to
// leave the directory as it was: started standalone again, it still runs.
func TestStandaloneDataDirRestartsWithSeeds(t *testing.T) {
	tests := []struct {
		name     string
		keystore bool
	}{
		{name: "users.json sealed under auth.token.key (keystore off)"},
		{name: "users.json sealed under the node master (keystore on)", keystore: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.Storage.DataDir = t.TempDir()
			cfg.Server.NodeID = "n1"
			cfg.Server.Listen = "127.0.0.1:24433"
			cfg.TCP.Listen = "127.0.0.1:0"
			cfg.Metrics.Enabled = false
			cfg.Audit.Enabled = false
			if tt.keystore {
				cfg.Encryption.Keystore = "keyfile"
				cfg.Encryption.KeyFile = makeKeyFile(t, t.TempDir(), 0x40)
				cfg.Encryption.Mode = "on"
			}
			if _, _, err := InitClusterToken(cfg.Storage.DataDir); err != nil {
				t.Fatalf("InitClusterToken: %v", err)
			}

			const rootPassword = "standalone-root-password-0320"
			startStandaloneWithRoot(t, cfg, rootPassword)
			usersBefore, err := os.ReadFile(UsersFilePath(cfg.Storage.DataDir))
			if err != nil {
				t.Fatalf("read users.json after the standalone run: %v", err)
			}

			clusteredCfg := *cfg
			clusteredCfg.HashRing.SeedNodes = []string{"n1@127.0.0.1:24433", "n2@127.0.0.1:24435"}
			clusteredCfg.HashRing.SeedFingerprints = []string{"SHA256:" + strings.Repeat("ab", 32)}
			clustered, err := NewService(&clusteredCfg, discardLogger())
			if err == nil {
				ok, verifyErr := clustered.Auth().VerifyUser("root", rootPassword)
				if stopErr := clustered.Stop(); stopErr != nil {
					t.Errorf("clustered Stop: %v", stopErr)
				}
				t.Fatalf("a standalone data directory started with seed nodes; want a refusal naming the transition "+
					"(root verifies: ok=%v err=%v)", ok, verifyErr)
			}
			for _, want := range []string{
				"standalone", "auth.token.key", "HOARDDB_SEED_NODES",
				"user list", "user describe", "hoarddb dump", "HOARDDB_DATA_DIR", "hoarddb restore", "create user",
			} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal does not name %q: %v", want, err)
				}
			}

			usersAfter, err := os.ReadFile(UsersFilePath(cfg.Storage.DataDir))
			if err != nil {
				t.Fatalf("read users.json after the refused start: %v", err)
			}
			if !bytes.Equal(usersBefore, usersAfter) {
				t.Error("the refused start rewrote users.json")
			}

			// Step 1 of the refusal: started standalone again, the node runs
			// and its users still log in.
			standalone, err := NewService(cfg, discardLogger())
			if err != nil {
				t.Fatalf("standalone restart after the refusal: %v", err)
			}
			defer func() {
				if err := standalone.Stop(); err != nil {
					t.Errorf("standalone Stop: %v", err)
				}
			}()
			if ok, err := standalone.Auth().VerifyUser("root", rootPassword); err != nil || !ok {
				t.Fatalf("root after the standalone restart: ok=%v err=%v", ok, err)
			}
		})
	}
}

// startStandaloneWithRoot runs a node with no seeds, seeds its root user and
// stops it, leaving users.json written under the standalone key.
func startStandaloneWithRoot(t *testing.T, cfg *config.Config, rootPassword string) {
	t.Helper()
	standalone, err := NewService(cfg, discardLogger())
	if err != nil {
		t.Fatalf("standalone NewService: %v", err)
	}
	if err := standalone.BootstrapUsers("root", rootPassword, true); err != nil {
		t.Fatalf("BootstrapUsers: %v", err)
	}
	if err := standalone.Stop(); err != nil {
		t.Fatalf("standalone Stop: %v", err)
	}
}
