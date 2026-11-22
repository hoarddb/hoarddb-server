package server

import (
	"bytes"
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
)

func setupAuthDir(t *testing.T) (string, *config.Config) {
	t.Helper()
	dir := t.TempDir()
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dir
	cfg.Server.NodeID = "node-a"
	cfg.TCP.Listen = "127.0.0.1:0"
	return dir, cfg
}

func newPersistentManager(t *testing.T, dir string) *AuthManager {
	t.Helper()
	keyring, err := BuildKeyring(dir, "", "", time.Hour, discardLogger())
	if err != nil {
		t.Fatalf("BuildKeyring: %v", err)
	}
	return NewAuthManager(AuthTokenOptions{
		TTL: time.Hour, MaxLifetime: 12 * time.Hour, Skew: time.Minute,
		Keyring: keyring, UsersPath: UsersFilePath(dir), Master: "node-a",
	}, discardLogger())
}

func TestBootstrapSeedsEmptyStore(t *testing.T) {
	dir, _ := setupAuthDir(t)
	am := newPersistentManager(t, dir)

	if err := am.BootstrapUser("root", "correct-horse-battery", true); err != nil {
		t.Fatalf("BootstrapUser: %v", err)
	}
	if _, err := os.Stat(UsersFilePath(dir)); err != nil {
		t.Fatalf("bootstrap did not write users.json: %v", err)
	}
	if ok, err := am.VerifyUser("root", "correct-horse-battery"); err != nil || !ok {
		t.Fatalf("seeded root does not authenticate: %v, %v", ok, err)
	}
}

func TestBootstrapNeverOverwritesStoredRecord(t *testing.T) {
	dir, _ := setupAuthDir(t)
	first := newPersistentManager(t, dir)
	if err := first.BootstrapUser("root", "correct-horse-battery", true); err != nil {
		t.Fatalf("first bootstrap: %v", err)
	}

	// Restart: the file is loaded, and a stale env password must not replace
	// the stored credential (spec 7.1 rule 2).
	second := newPersistentManager(t, dir)
	if _, _, err := second.LoadUsers(UsersFilePath(dir), nil, nil); err != nil {
		t.Fatalf("LoadUsers: %v", err)
	}
	if err := second.BootstrapUser("root", "a-stale-environment-password", true); err != nil {
		t.Fatalf("BootstrapUser with a stale credential: %v", err)
	}
	if ok, err := second.VerifyUser("root", "correct-horse-battery"); err != nil || !ok {
		t.Fatalf("stored root credential was overwritten: %v, %v", ok, err)
	}

	// And a stale env *username* must not add an administrator.
	if err := second.BootstrapUser("intruder", "correct-horse-battery", true); err != nil {
		t.Fatalf("BootstrapUser with a foreign user: %v", err)
	}
	second.mu.RLock()
	_, added := second.users["intruder"]
	second.mu.RUnlock()
	if added {
		t.Fatal("a stale environment variable added an administrator")
	}
}

func TestBootstrapRootDisabledWritesNothing(t *testing.T) {
	dir, _ := setupAuthDir(t)
	am := newPersistentManager(t, dir)
	if err := am.BootstrapUser("root", "correct-horse-battery", false); err != nil {
		t.Fatalf("BootstrapUser: %v", err)
	}
	if _, err := os.Stat(UsersFilePath(dir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("auth_bootstrap_root=false still wrote users.json: %v", err)
	}
}

func TestResetPasswordBumpsTheTerm(t *testing.T) {
	dir, cfg := setupAuthDir(t)
	am := newPersistentManager(t, dir)
	if err := am.BootstrapUser("root", "correct-horse-battery", true); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	before, err := LoadUsersConfig(UsersFilePath(dir), am.keyring, nil)
	if err != nil {
		t.Fatalf("LoadUsersConfig before: %v", err)
	}

	term, version, err := ResetPassword(cfg, "root", "a-new-longer-password", true, discardLogger())
	if err != nil {
		t.Fatalf("ResetPassword: %v", err)
	}
	if term != before.Term+1 {
		t.Fatalf("term = %d, want %d (a reset must bump the term, not only the version)", term, before.Term+1)
	}
	if version != before.Version+1 {
		t.Fatalf("version = %d, want %d", version, before.Version+1)
	}

	after, err := LoadUsersConfig(UsersFilePath(dir), am.keyring, nil)
	if err != nil {
		t.Fatalf("LoadUsersConfig after: %v", err)
	}
	if after.Users[0].Epoch != before.Users[0].Epoch+1 {
		t.Fatalf("epoch = %d, want %d", after.Users[0].Epoch, before.Users[0].Epoch+1)
	}

	// The new password must verify; the old one must not.
	reloaded := newPersistentManager(t, dir)
	if _, _, err := reloaded.LoadUsers(UsersFilePath(dir), nil, nil); err != nil {
		t.Fatalf("reload after reset: %v", err)
	}
	if ok, err := reloaded.VerifyUser("root", "a-new-longer-password"); err != nil || !ok {
		t.Fatalf("new password does not authenticate: %v, %v", ok, err)
	}
	if ok, _ := reloaded.VerifyUser("root", "correct-horse-battery"); ok {
		t.Fatal("old password still authenticates after a reset")
	}
}

func TestResetPasswordRefusesRunningServer(t *testing.T) {
	_, cfg := setupAuthDir(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve listener: %v", err)
	}
	defer func() { _ = ln.Close() }()
	cfg.TCP.Listen = ln.Addr().String()

	if err := CheckServerStopped(cfg); err == nil {
		t.Fatal("CheckServerStopped = nil while the driver port is bound, want refusal")
	}
}

func TestResetPasswordRefusesNonMasterWithoutForce(t *testing.T) {
	dir, cfg := setupAuthDir(t)
	am := newPersistentManager(t, dir)
	if err := am.BootstrapUser("root", "correct-horse-battery", true); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	// Rewrite the header to claim a different master.
	loaded, err := LoadUsersConfig(UsersFilePath(dir), am.keyring, nil)
	if err != nil {
		t.Fatalf("LoadUsersConfig: %v", err)
	}
	loaded.Master = "node-b"
	if err := loaded.Save(UsersFilePath(dir), usersSealing{Keyring: am.keyring, NodeMaster: nil, Mode: "off"}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if _, _, err := ResetPassword(cfg, "root", "a-new-longer-password", false, discardLogger()); err == nil {
		t.Fatal("ResetPassword on a non-master without --force = nil, want refusal")
	}
	if _, _, err := ResetPassword(cfg, "root", "a-new-longer-password", true, discardLogger()); err != nil {
		t.Fatalf("ResetPassword with --force: %v", err)
	}
}

func TestResetPasswordOnForeignKeyNamesTheKid(t *testing.T) {
	dir, cfg := setupAuthDir(t)
	am := newPersistentManager(t, dir)
	if err := am.BootstrapUser("root", "correct-horse-battery", true); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	usersPath := UsersFilePath(dir)

	// Re-seal the store under key material this node does not hold: that is
	// what a lost or replaced auth.token.key looks like to the loader, and it
	// is the failure the operator has to be able to tell apart from a missing
	// file.
	stored, err := os.ReadFile(usersPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	plaintext, err := decryptConfig(stored, am.keyring)
	if err != nil {
		t.Fatalf("decryptConfig: %v", err)
	}
	foreignKey := bytes.Repeat([]byte{0x5a}, authKeyLen)
	sealed, err := encryptConfig(plaintext, foreignKey, "deadbeef")
	if err != nil {
		t.Fatalf("encryptConfig: %v", err)
	}
	if err := os.WriteFile(usersPath, sealed, 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, _, err = ResetPassword(cfg, "root", "a-new-longer-password", true, discardLogger())
	if err == nil {
		t.Fatal("ResetPassword on a store sealed under another key = nil, want a refusal")
	}
	if !strings.Contains(err.Error(), "deadbeef") {
		t.Fatalf("error %q does not name the kid the store is sealed under", err)
	}
	if strings.Contains(err.Error(), "start the server once to create it") {
		t.Fatalf("error %q blames a missing users.json when the file is present but under another key", err)
	}
}

func TestResetPasswordMissingFileStillNamesTheFix(t *testing.T) {
	_, cfg := setupAuthDir(t)

	_, _, err := ResetPassword(cfg, "root", "a-new-longer-password", true, discardLogger())
	if err == nil {
		t.Fatal("ResetPassword with no users.json = nil, want a refusal")
	}
	if !strings.Contains(err.Error(), "start the server once to create it") {
		t.Fatalf("error %q does not name the way to create the store", err)
	}
}

func TestResetPasswordEnforcesMinimumLength(t *testing.T) {
	dir, cfg := setupAuthDir(t)
	am := newPersistentManager(t, dir)
	if err := am.BootstrapUser("root", "correct-horse-battery", true); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	_, _, err := ResetPassword(cfg, "root", "short", true, discardLogger())
	if err == nil {
		t.Fatal("ResetPassword accepted a below-minimum password")
	}
	if !strings.Contains(err.Error(), "minimum") {
		t.Fatalf("error %q does not name the limit", err)
	}
}

// TestResetPasswordCountsCharactersNotBytes: the minimum is in characters, as
// the CLI counts it. Eleven two-byte characters are 22 bytes, so a byte count
// would accept them; the refusal must report 11 characters, not 22 bytes.
func TestResetPasswordCountsCharactersNotBytes(t *testing.T) {
	dir, cfg := setupAuthDir(t)
	cfg.Auth.PasswordMinLength = 12
	am := newPersistentManager(t, dir)
	if err := am.BootstrapUser("root", "correct-horse-battery", true); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	_, _, err := ResetPassword(cfg, "root", strings.Repeat("é", 11), true, discardLogger())
	if err == nil {
		t.Fatal("ResetPassword accepted 11 two-byte characters against a minimum of 12 characters")
	}
	if !strings.Contains(err.Error(), "11 characters") {
		t.Fatalf("error %q does not report the character count 11", err)
	}

	if _, _, err := ResetPassword(cfg, "root", strings.Repeat("é", 12), true, discardLogger()); err != nil {
		t.Fatalf("ResetPassword refused 12 two-byte characters against a minimum of 12 characters: %v", err)
	}
}

// TestResetPasswordRetiresBootstrapRootFile: a reset-password for
// the configured root user must not leave the old bootstrap secret sitting on
// disk at 0600 once users.json is the authoritative credential.
func TestResetPasswordRetiresBootstrapRootFile(t *testing.T) {
	dir, cfg := setupAuthDir(t)
	cfg.Server.RootUser = "root"
	am := newPersistentManager(t, dir)
	if err := am.BootstrapUser("root", "correct-horse-battery", true); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	// Seed a leftover bootstrap file, the way a first start of a version
	// before the hash-only change left one behind, at 0600, if no start has
	// converted it yet.
	bootstrapPath := filepath.Join(dir, rootPasswordFile)
	if err := os.WriteFile(bootstrapPath, []byte("stale-bootstrap-secret\n"), 0600); err != nil {
		t.Fatalf("seed stale root.password: %v", err)
	}

	if _, _, err := ResetPassword(cfg, "root", "a-new-longer-password", true, discardLogger()); err != nil {
		t.Fatalf("ResetPassword: %v", err)
	}

	if _, err := os.Stat(bootstrapPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("root.password still present after reset-password for the root user: %v", err)
	}
}

// TestResetPasswordWithNoBootstrapFileIsUnaffected is the acceptance
// criteria's second arm: the first-start-with-env path never created
// root.password, and a reset must not error trying to remove a file that was
// never there.
func TestResetPasswordWithNoBootstrapFileIsUnaffected(t *testing.T) {
	dir, cfg := setupAuthDir(t)
	cfg.Server.RootUser = "root"
	am := newPersistentManager(t, dir)
	if err := am.BootstrapUser("root", "correct-horse-battery", true); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	bootstrapPath := filepath.Join(dir, rootPasswordFile)
	if _, err := os.Stat(bootstrapPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("test setup: root.password unexpectedly exists: %v", err)
	}

	if _, _, err := ResetPassword(cfg, "root", "a-new-longer-password", true, discardLogger()); err != nil {
		t.Fatalf("ResetPassword with no bootstrap file present: %v", err)
	}
	if _, err := os.Stat(bootstrapPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("root.password unexpectedly created by a reset-password: %v", err)
	}
}

// TestResetPasswordReportsBootstrapFileRemovalFailure covers a
// remaining gap: a reset that cannot retire the superseded
// bootstrap file must not swallow that failure behind a log line -- the
// caller needs to know the stale secret is still on disk. root.password is
// seeded as a non-empty directory so os.Remove fails with "directory not
// empty" without touching dataDir's own permissions, which the users.json
// write earlier in the same call still needs.
func TestResetPasswordReportsBootstrapFileRemovalFailure(t *testing.T) {
	dir, cfg := setupAuthDir(t)
	cfg.Server.RootUser = "root"
	am := newPersistentManager(t, dir)
	if err := am.BootstrapUser("root", "correct-horse-battery", true); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	bootstrapPath := filepath.Join(dir, rootPasswordFile)
	if err := os.Mkdir(bootstrapPath, 0700); err != nil {
		t.Fatalf("seed unremovable root.password: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bootstrapPath, "occupied"), []byte("x"), 0600); err != nil {
		t.Fatalf("seed unremovable root.password: %v", err)
	}

	_, _, err := ResetPassword(cfg, "root", "a-new-longer-password", true, discardLogger())
	if err == nil {
		t.Fatal("ResetPassword must report a bootstrap-file removal failure, not swallow it behind a log line")
	}
	if !strings.Contains(err.Error(), rootPasswordFile) {
		t.Fatalf("error %q does not name the bootstrap file that could not be removed", err)
	}
}

// TestResetPasswordOfNonRootUserLeavesBootstrapFileAlone proves the
// retirement is scoped to the configured root user: resetting an unrelated
// user's password must not touch a bootstrap file that belongs to a
// different credential entirely.
func TestResetPasswordOfNonRootUserLeavesBootstrapFileAlone(t *testing.T) {
	dir, cfg := setupAuthDir(t)
	cfg.Server.RootUser = "root"
	am := newPersistentManager(t, dir)
	if err := am.BootstrapUser("root", "correct-horse-battery", true); err != nil {
		t.Fatalf("bootstrap root: %v", err)
	}
	if err := am.SeedRootUser("alice", "a-second-user-password"); err != nil {
		t.Fatalf("seed alice: %v", err)
	}

	bootstrapPath := filepath.Join(dir, rootPasswordFile)
	if err := os.WriteFile(bootstrapPath, []byte("stale-bootstrap-secret\n"), 0600); err != nil {
		t.Fatalf("seed stale root.password: %v", err)
	}

	if _, _, err := ResetPassword(cfg, "alice", "a-new-longer-password", true, discardLogger()); err != nil {
		t.Fatalf("ResetPassword for alice: %v", err)
	}

	if _, err := os.Stat(bootstrapPath); err != nil {
		t.Fatalf("root.password removed by a reset of an unrelated user: %v", err)
	}
}

func TestUsersJSONStays0600AfterMutation(t *testing.T) {
	dir, _ := setupAuthDir(t)
	am := newPersistentManager(t, dir)
	if err := am.BootstrapUser("root", "correct-horse-battery", true); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if err := am.InvalidateUserTokens("root"); err != nil {
		t.Fatalf("InvalidateUserTokens: %v", err)
	}
	info, err := os.Stat(filepath.Join(dir, UsersFileName))
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("users.json mode = %o after a mutation, want 0600", info.Mode().Perm())
	}
}

// pinGOMAXPROCS sets the scheduler's P count for one test and restores it.
// The credential clamp once read runtime.GOMAXPROCS(0), so a test that wants
// to stand on a 1-CPU host only has to lower it. A caller must not
// call t.Parallel: the setting is process-wide.
func pinGOMAXPROCS(t *testing.T, procs int) {
	t.Helper()
	previous := runtime.GOMAXPROCS(procs)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
}

// TestBootstrapSeedsRootUserWithOneCPU is the zero-config start: the
// default config seeds a parallelism-4 root user, and on a host where Go sees
// one CPU the server's own validator refused it, so the server never started.
// The seed must succeed, keep the portable default of 4, and save a file that
// validates.
func TestBootstrapSeedsRootUserWithOneCPU(t *testing.T) {
	pinGOMAXPROCS(t, 1)
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Server.Listen = "127.0.0.1:0"
	cfg.TCP.Listen = "127.0.0.1:0"

	qs, err := NewService(cfg, discardLogger())
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	t.Cleanup(func() {
		if stopErr := qs.Stop(); stopErr != nil {
			t.Logf("stopping server: %v", stopErr)
		}
	})
	if err := qs.BootstrapUsers("admin", "one-cpu-root-pass", true); err != nil {
		t.Fatalf("BootstrapUsers at GOMAXPROCS=1: %v", err)
	}

	saved, err := LoadUsersConfig(UsersFilePath(cfg.Storage.DataDir), qs.Auth().keyring, nil)
	if err != nil {
		t.Fatalf("LoadUsersConfig: %v", err)
	}
	if err := saved.Validate(); err != nil {
		t.Fatalf("the seeded users file does not validate at GOMAXPROCS=1: %v", err)
	}
	if got := saved.Users[0].Credentials.Params.Parallelism; got != defaultArgon2Parallelism {
		t.Fatalf("seeded parallelism = %d, want the portable default %d (never the host's CPU count)",
			got, defaultArgon2Parallelism)
	}
	if ok, err := qs.Auth().VerifyUser("admin", "one-cpu-root-pass"); err != nil || !ok {
		t.Fatalf("the seeded root user does not authenticate at GOMAXPROCS=1: %v, %v", ok, err)
	}
}

// BUG-0504: a root password seeded from HOARDDB_ROOT_PASSWORD on a first
// start is measured against password_min_length like every other password the
// server sees. The refusal names the length, the minimum and the setting, and
// never the password, and it writes no users.json.
func TestBootstrapRefusesShortSeedPassword(t *testing.T) {
	dir, _ := setupAuthDir(t)
	keyring, err := BuildKeyring(dir, "", "", time.Hour, discardLogger())
	if err != nil {
		t.Fatalf("BuildKeyring: %v", err)
	}
	am := NewAuthManager(AuthTokenOptions{
		TTL: time.Hour, MaxLifetime: 12 * time.Hour, Skew: time.Minute,
		Keyring: keyring, UsersPath: UsersFilePath(dir), Master: "node-a",
		PasswordMinLength: 12,
	}, discardLogger())

	const short = "ab"
	err = am.BootstrapUser("admin", short, true)
	if err == nil {
		t.Fatal("BootstrapUser seeded a 2-character root password; want a refusal")
	}
	for _, want := range []string{"2 characters", "12-character minimum", "HOARDDB_PASSWORD_MIN_LENGTH"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not contain %q", err, want)
		}
	}
	if strings.Contains(err.Error(), short) {
		t.Errorf("refusal %q echoes the password", err)
	}
	if _, statErr := os.Stat(UsersFilePath(dir)); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("refused bootstrap still wrote users.json: %v", statErr)
	}
}

// BUG-0684: the refusal must not offer a password the server never makes up.
// chooseRootPassword uses the setting, a legacy root.password, or a terminal
// prompt, and refuses otherwise, so the remedies are a longer value and a
// first start from a terminal that can type one.
func TestShortSeedRefusalNamesTheRealFix(t *testing.T) {
	dir, _ := setupAuthDir(t)
	keyring, err := BuildKeyring(dir, "", "", time.Hour, discardLogger())
	if err != nil {
		t.Fatalf("BuildKeyring: %v", err)
	}
	am := NewAuthManager(AuthTokenOptions{
		TTL: time.Hour, MaxLifetime: 12 * time.Hour, Skew: time.Minute,
		Keyring: keyring, UsersPath: UsersFilePath(dir), Master: "node-a",
		PasswordMinLength: 12,
	}, discardLogger())

	err = am.BootstrapUserFrom("admin", "ab", true, "HOARDDB_ROOT_PASSWORD")
	if err == nil {
		t.Fatal("BootstrapUserFrom seeded a 2-character root password; want a refusal")
	}
	refusal := err.Error()
	if strings.Contains(refusal, "generates") {
		t.Errorf("refusal %q offers a root password the server generates; no code path does", refusal)
	}
	for _, want := range []string{
		"choose a longer one",
		"unset HOARDDB_ROOT_PASSWORD and start hoarddb-server from a terminal to type one",
	} {
		if !strings.Contains(refusal, want) {
			t.Errorf("refusal %q does not contain %q", refusal, want)
		}
	}
}
