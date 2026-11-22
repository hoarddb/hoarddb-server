package server

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
)

const (
	resetOldPassword = "correct-horse-battery"
	resetNewPassword = "a-new-longer-password"
)

// bootstrapStoppedNode starts a node under edit's encryption settings, creates
// its root user, stops it, and returns the configuration an offline command
// would run with.
func bootstrapStoppedNode(t *testing.T, edit func(*config.Config)) *config.Config {
	t.Helper()
	dataDir := t.TempDir()
	node := newCapacityServiceAt(t, dataDir, edit)
	if err := node.BootstrapUsersFrom("admin", resetOldPassword, true, ""); err != nil {
		t.Fatalf("bootstrap the root user: %v", err)
	}
	if err := node.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	return sealedTestConfigAt(t, dataDir, edit)
}

// assertNodeAcceptsPassword restarts the node and logs admin in.
func assertNodeAcceptsPassword(t *testing.T, cfg *config.Config, password string) {
	t.Helper()
	restarted := newCapacityServiceAt(t, cfg.Storage.DataDir, func(restartCfg *config.Config) {
		restartCfg.Encryption = cfg.Encryption
	})
	stopService(t, restarted)
	if ok, err := restarted.core.auth.VerifyUser("admin", password); err != nil || !ok {
		t.Fatalf("admin does not log in with the reset password after a restart: ok=%v err=%v", ok, err)
	}
}

// assertNoUsersFileInTheClear reads every users file and temporary the reset
// could have left, so a plaintext write that was later replaced still fails.
func assertNoUsersFileInTheClear(t *testing.T, dataDir string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dataDir, "*users*"))
	if err != nil {
		t.Fatalf("listing users files: %v", err)
	}
	hidden, err := filepath.Glob(filepath.Join(dataDir, ".users-*"))
	if err != nil {
		t.Fatalf("listing users temporaries: %v", err)
	}
	for _, path := range append(matches, hidden...) {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		if bytes.Contains(contents, []byte("argon2id")) || bytes.Contains(contents, []byte(`"username"`)) {
			t.Errorf("%s holds the user records in the clear on an encrypted node", path)
		}
	}
}

// TestResetPasswordOnAnEncryptedNode: -reset-password on a node with encryption
// at rest opens the sealed users.json, resets the password, keeps the file
// sealed under the node master, and the node logs the user in after a restart.
func TestResetPasswordOnAnEncryptedNode(t *testing.T) {
	for _, mode := range []string{"on", "require"} {
		t.Run(mode, func(t *testing.T) {
			cfg := bootstrapStoppedNode(t, sealEdit(t, mode))
			usersPath := UsersFilePath(cfg.Storage.DataDir)
			assertSealedCredential(t, usersPath, 0x01)
			before, err := os.ReadFile(usersPath)
			if err != nil {
				t.Fatalf("reading users.json: %v", err)
			}

			term, version, err := ResetPassword(cfg, "admin", resetNewPassword, true, discardLogger())
			if err != nil {
				t.Fatalf("ResetPassword on an encrypted node: %v", err)
			}
			if term == 0 || version == 0 {
				t.Fatalf("ResetPassword answered term %d, version %d", term, version)
			}

			assertSealedCredential(t, usersPath, 0x01, []byte(resetNewPassword), []byte("argon2id"))
			assertNoUsersFileInTheClear(t, cfg.Storage.DataDir)
			after, err := os.ReadFile(usersPath)
			if err != nil {
				t.Fatalf("reading users.json after the reset: %v", err)
			}
			if bytes.Equal(before, after) {
				t.Fatal("users.json is unchanged after the reset")
			}
			assertNodeAcceptsPassword(t, cfg, resetNewPassword)
		})
	}
}

// TestResetPasswordKeepsTheLoginGenerationBumpOnAnEncryptedNode: the reset
// still moves the user's login generation when the file is sealed.
func TestResetPasswordKeepsTheLoginGenerationBumpOnAnEncryptedNode(t *testing.T) {
	cfg := bootstrapStoppedNode(t, sealEdit(t, "require"))
	node := newCapacityServiceAt(t, cfg.Storage.DataDir, func(restartCfg *config.Config) { restartCfg.Encryption = cfg.Encryption })
	before := node.core.auth.SnapshotConfig()
	if err := node.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if _, _, err := ResetPassword(cfg, "admin", resetNewPassword, true, discardLogger()); err != nil {
		t.Fatalf("ResetPassword: %v", err)
	}
	restarted := newCapacityServiceAt(t, cfg.Storage.DataDir, func(restartCfg *config.Config) { restartCfg.Encryption = cfg.Encryption })
	stopService(t, restarted)
	after := restarted.core.auth.SnapshotConfig()
	if after.Users[0].LoginGeneration != before.Users[0].LoginGeneration+1 {
		t.Fatalf("login generation = %d, want %d", after.Users[0].LoginGeneration, before.Users[0].LoginGeneration+1)
	}
}

// TestResetPasswordWithEncryptionOffStillWorks: a node without encryption keeps
// its users file under the signing key and the reset does not seal it.
func TestResetPasswordWithEncryptionOffStillWorks(t *testing.T) {
	cfg := bootstrapStoppedNode(t, func(*config.Config) {})
	if _, _, err := ResetPassword(cfg, "admin", resetNewPassword, true, discardLogger()); err != nil {
		t.Fatalf("ResetPassword with encryption off: %v", err)
	}
	contents, err := os.ReadFile(UsersFilePath(cfg.Storage.DataDir))
	if err != nil {
		t.Fatalf("reading users.json: %v", err)
	}
	if len(contents) > 5 && contents[4] == 0x02 && isEncryptedUsersFile(contents) {
		t.Fatal("a node with encryption off had its users.json sealed under a node master")
	}
	assertNodeAcceptsPassword(t, cfg, resetNewPassword)
}

// TestResetPasswordWithoutTheKeystoreNamesTheSettings: a sealed users.json and
// no keystore in the configuration is refused with the settings to set, not
// with a claim that the keystore is off.
func TestResetPasswordWithoutTheKeystoreNamesTheSettings(t *testing.T) {
	cfg := bootstrapStoppedNode(t, sealEdit(t, "require"))
	cfg.Encryption = config.DefaultConfig().Encryption
	_, _, err := ResetPassword(cfg, "admin", resetNewPassword, true, discardLogger())
	if err == nil {
		t.Fatal("ResetPassword opened a sealed users.json with no keystore configured")
	}
	for _, want := range []string{"HOARDDB_ENCRYPTION_KEYSTORE", "HOARDDB_ENCRYPTION_KEY_FILE", "HOARDDB_ENCRYPTION_MODE"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not name %s: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "keystore is off") {
		t.Errorf("the refusal still says the keystore is off: %v", err)
	}
}

// TestSealedUsersFileWithNoNodeMasterNamesTheSettings: the refusal of a sealed
// users.json read with no node master names the settings that are missing, not
// "encryption.keystore is off", which is wrong when the keystore is configured.
func TestSealedUsersFileWithNoNodeMasterNamesTheSettings(t *testing.T) {
	_, _, err := openUsersWithNodeMaster([]byte("sealed"), nil)
	if err == nil {
		t.Fatal("a sealed users.json opened with no node master")
	}
	if !strings.Contains(err.Error(), "HOARDDB_ENCRYPTION_KEYSTORE") || strings.Contains(err.Error(), "keystore is off") {
		t.Fatalf("the refusal is %q, want it to name the HOARDDB_ENCRYPTION_* settings", err)
	}
}

// TestResetPasswordGivenTheKeystoreAndNoModeKeepsUsersSealed: an operator who
// runs the offline reset with the keystore settings but not the mode must not
// get users.json rewritten under the signing key. The node master that guards
// sealed material is never written back in the clear (usersSealingMode).
func TestResetPasswordGivenTheKeystoreAndNoModeKeepsUsersSealed(t *testing.T) {
	cfg := bootstrapStoppedNode(t, sealEdit(t, "require"))
	startMode := cfg.Encryption.Mode
	cfg.Encryption.Mode = config.DefaultConfig().Encryption.Mode
	if modeSealsStore(cfg.Encryption.Mode) {
		t.Fatalf("the default mode %q seals the store, so this test would not run without a mode", cfg.Encryption.Mode)
	}
	if _, _, err := ResetPassword(cfg, "admin", resetNewPassword, true, discardLogger()); err != nil {
		t.Fatalf("ResetPassword with the keystore and no mode: %v", err)
	}
	assertSealedCredential(t, UsersFilePath(cfg.Storage.DataDir), 0x01, []byte(resetNewPassword), []byte("argon2id"))
	cfg.Encryption.Mode = startMode
	assertNodeAcceptsPassword(t, cfg, resetNewPassword)
}
