package server

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

func firstStartLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// firstStartDamagedKeys lays down what a power cut in the first seconds leaves.
func firstStartDamagedKeys(t *testing.T, dataDir string) (keyPath, certPath string) {
	t.Helper()
	keyDir := filepath.Join(dataDir, "keys")
	if err := os.MkdirAll(keyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	keyPath, certPath = filepath.Join(keyDir, "server.key"), filepath.Join(keyDir, "server.crt")
	for _, path := range []string{keyPath, certPath} {
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return keyPath, certPath
}

func firstStartTouch(t *testing.T, path string, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestStartRegeneratesKeysAfterUnfinishedFirstStart: empty key and certificate
// beside only the files a first start writes before the TLS identity is a
// first start that never completed, so a new identity is made.
func TestStartRegeneratesKeysAfterUnfinishedFirstStart(t *testing.T) {
	dataDir := t.TempDir()
	firstStartDamagedKeys(t, dataDir)
	firstStartTouch(t, filepath.Join(dataDir, "cluster.token"), "{}")
	firstStartTouch(t, filepath.Join(dataDir, "auth.token.key"), "x")
	firstStartTouch(t, filepath.Join(dataDir, "users.json"), "{}")

	if err := recoverUnfinishedFirstStart(dataDir, firstStartLogger()); err != nil {
		t.Fatalf("recoverUnfinishedFirstStart: %v", err)
	}
	if _, fingerprint, err := (CredentialKeys{}).loadNodeTLS(dataDir); err != nil || fingerprint == "" {
		t.Fatalf("loadNodeTLS after recovery = %q, %v; want a new identity", fingerprint, err)
	}
	for _, name := range []string{"cluster.token", "auth.token.key", "users.json"} {
		if _, err := os.Stat(filepath.Join(dataDir, name)); err != nil {
			t.Errorf("recovery touched %s: %v", name, err)
		}
	}
}

// TestStartRefusesDamagedKeysBesideExistingState: anything else in the data
// directory, or one intact half of the pair, means an identity may be pinned,
// so nothing is replaced and the error names the files and the fix.
func TestStartRefusesDamagedKeysBesideExistingState(t *testing.T) {
	cases := map[string]func(t *testing.T, dataDir string, keyPath, certPath string){
		"a store directory exists": func(t *testing.T, dataDir, _, _ string) {
			firstStartTouch(t, filepath.Join(dataDir, "wal", "segment"), "x")
		},
		"a membership record exists": func(t *testing.T, dataDir, _, _ string) {
			firstStartTouch(t, filepath.Join(dataDir, "membership.json"), "{}")
		},
		"the certificate is intact": func(t *testing.T, _, keyPath, certPath string) {
			identity, err := transport.NewEd25519Identity()
			if err != nil {
				t.Fatal(err)
			}
			firstStartTouch(t, certPath, string(identity.CertPEM))
		},
	}
	for name, arrange := range cases {
		t.Run(name, func(t *testing.T) {
			dataDir := t.TempDir()
			keyPath, certPath := firstStartDamagedKeys(t, dataDir)
			arrange(t, dataDir, keyPath, certPath)
			before, _ := os.ReadFile(keyPath)

			err := recoverUnfinishedFirstStart(dataDir, firstStartLogger())
			if err == nil {
				t.Fatal("want a refusal, got nil")
			}
			for _, want := range []string{keyPath, "server.crt", "remove", "restart"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not mention %q", err, want)
				}
			}
			after, readErr := os.ReadFile(keyPath)
			if readErr != nil || string(before) != string(after) {
				t.Errorf("the key file was changed: %v", readErr)
			}
		})
	}
}

// TestNodeMasterWritesUseTheDurableHelper: creating the node master goes
// through the shared durable write.
func TestNodeMasterWritesUseTheDurableHelper(t *testing.T) {
	var written []string
	original := writeNodeMasterFile
	writeNodeMasterFile = func(path string, contents []byte) error {
		written = append(written, filepath.Base(path))
		return original(path, contents)
	}
	t.Cleanup(func() { writeNodeMasterFile = original })

	dataDir := t.TempDir()
	cfg := firstStartKeyfileConfig(t, dataDir)
	keystore, err := OpenKeystore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreateNodeMaster(dataDir, keystore); err != nil {
		t.Fatal(err)
	}
	if len(written) != 1 || written[0] != NodeMasterFileName {
		t.Fatalf("create wrote through the helper %v, want [%s]", written, NodeMasterFileName)
	}

	exportPath := filepath.Join(t.TempDir(), "master.export")
	if err := ExportNodeMaster(cfg, exportPath); err != nil {
		t.Fatal(err)
	}
	written = nil
	if err := ImportNodeMaster(cfg, exportPath, true); err != nil {
		t.Fatal(err)
	}
	if len(written) != 1 || written[0] != NodeMasterFileName {
		t.Fatalf("import wrote through the helper %v, want [%s]", written, NodeMasterFileName)
	}
}

func firstStartKeyfileConfig(t *testing.T, dataDir string) *config.Config {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dataDir
	cfg.Encryption.Keystore = "keyfile"
	cfg.Encryption.Mode = "on"
	cfg.Encryption.KeyFile = makeKeyFile(t, t.TempDir(), 0)
	return cfg
}

// TestNewServiceStartsAfterAPowerCutInTheFirstSeconds is the whole start: the
// empty key pair a power cut leaves no longer stops the node.
func TestNewServiceStartsAfterAPowerCutInTheFirstSeconds(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Metrics.Enabled, cfg.Audit.Enabled = false, false
	firstStartDamagedKeys(t, cfg.Storage.DataDir)

	service, err := NewService(cfg, firstStartLogger())
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := service.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
