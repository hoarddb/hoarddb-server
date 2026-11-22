package main

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// BUG-0504 follow-up: a refused password from HOARDDB_ROOT_PASSWORD_FILE must
// name that setting, not HOARDDB_ROOT_PASSWORD, because the operator set the
// file and the refusal has to point at the value they can change.
func TestShortRootPasswordFileRefusalNamesTheFileSetting(t *testing.T) {
	secret := filepath.Join(t.TempDir(), "root-password")
	if err := os.WriteFile(secret, []byte("abc\n"), 0600); err != nil {
		t.Fatalf("write secret file: %v", err)
	}
	t.Setenv("HOARDDB_ROOT_PASSWORD_FILE", secret)
	cfg := loadStartConfig(t)

	_, err := prepare(cfg, discardSlog(), io.Discard, noTerminal())
	if err == nil {
		t.Fatal("prepare seeded a 3-character root password from HOARDDB_ROOT_PASSWORD_FILE; want a refusal")
	}
	if !strings.Contains(err.Error(), "HOARDDB_ROOT_PASSWORD_FILE") {
		t.Fatalf("refusal %q does not name HOARDDB_ROOT_PASSWORD_FILE", err)
	}
}

func discardSlog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
