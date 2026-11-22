package config

import (
	"os"
	"path/filepath"
	"testing"
)

// TestRootPasswordFileOverridesEnv is spec 7.1 rule 4: a file path is
// readable only by whoever can read the file, so the _FILE form wins when
// both are set.
func TestRootPasswordFileOverridesEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rootpw")
	if err := os.WriteFile(path, []byte("from-file-password\n"), 0600); err != nil {
		t.Fatalf("write secret: %v", err)
	}
	t.Setenv("HOARDDB_ROOT_PASSWORD", "from-env-password")
	t.Setenv("HOARDDB_ROOT_PASSWORD_FILE", path)

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Server.RootPassword != "from-file-password" {
		t.Fatalf("RootPassword = %q, want the file value", cfg.Server.RootPassword)
	}
}

func TestRootPasswordFileTrimsTrailingNewline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rootpw")
	if err := os.WriteFile(path, []byte("secret\n\n"), 0600); err != nil {
		t.Fatalf("write secret: %v", err)
	}
	t.Setenv("HOARDDB_ROOT_PASSWORD_FILE", path)

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Server.RootPassword != "secret" {
		t.Fatalf("RootPassword = %q, want the trimmed file value", cfg.Server.RootPassword)
	}
}

func TestRootPasswordFileMissingIsError(t *testing.T) {
	t.Setenv("HOARDDB_ROOT_PASSWORD_FILE", filepath.Join(t.TempDir(), "does-not-exist"))
	if _, err := Load(""); err == nil {
		t.Fatal("a missing HOARDDB_ROOT_PASSWORD_FILE must fail, not silently use nothing")
	}
}

func TestRootPasswordFileEmptyIsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rootpw")
	if err := os.WriteFile(path, []byte("\n"), 0600); err != nil {
		t.Fatalf("write secret: %v", err)
	}
	t.Setenv("HOARDDB_ROOT_PASSWORD_FILE", path)
	if _, err := Load(""); err == nil {
		t.Fatal("an empty HOARDDB_ROOT_PASSWORD_FILE must fail")
	}
}
