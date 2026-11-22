package main

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
)

// TestUnattendedFirstStartNeedsARootPassword: a first start with nobody at a
// terminal and no root password set refuses and writes nothing. A password it
// made up would have to be printed to an output a log keeps, so it is not made
// up; the refusal names the two ways forward.
func TestUnattendedFirstStartNeedsARootPassword(t *testing.T) {
	cfg := loadStartConfig(t)
	var banners, logs concurrentBuffer
	srv, err := prepare(cfg, slog.New(slog.NewJSONHandler(&logs, nil)), &banners, noTerminal())
	if err == nil {
		_ = srv.Stop()
		t.Fatalf("an unattended first start with no root password started; banner:\n%s", banners.String())
	}
	for _, want := range []string{"HOARDDB_ROOT_PASSWORD=", "terminal"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal %q does not name %q", err, want)
		}
	}
	if _, statErr := os.Stat(server.UsersFilePath(cfg.Storage.DataDir)); !os.IsNotExist(statErr) {
		t.Errorf("the refused start wrote users.json (stat: %v)", statErr)
	}
	if strings.Contains(banners.String(), "HOARDDB_ROOT_PASSWORD='") {
		t.Errorf("the refused start printed a root password:\n%s", banners.String())
	}
}

// TestAFirstStartPrintsNoClusterTokenAndWritesNone: this release has no cluster
// token (members trust each other by pinned keys), so a first start says nothing
// about one and writes no cluster.token, on a terminal or off one, with encryption
// at rest or without. The root user is still created from the password it was given.
func TestAFirstStartPrintsNoClusterTokenAndWritesNone(t *testing.T) {
	for _, tc := range []struct {
		name        string
		operatorTTY bool
		sealed      bool
	}{{"terminal", true, false}, {"not a terminal", false, false}, {"terminal, encryption on", true, true}} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := loadStartConfig(t)
			cfg.Server.RootPassword = "an-operator-chosen-password"
			if tc.sealed {
				sealWithKeyFile(t, cfg)
				cfg.Encryption.Mode = "require"
			}
			var banners, logs concurrentBuffer
			srv, err := prepare(cfg, slog.New(slog.NewJSONHandler(&logs, nil)), &banners, &scriptedTerminal{operatorTTY: tc.operatorTTY})
			if err != nil {
				t.Fatalf("prepare: %v", err)
			}
			defer func() { _ = srv.Stop() }()
			for name, output := range map[string]string{"notice": banners.String(), "log": logs.String()} {
				if lowered := strings.ToLower(output); strings.Contains(lowered, "cluster token") || strings.Contains(lowered, "cluster.token") ||
					strings.Contains(output, "HOARDDB_CLUSTER_TOKEN") {
					t.Errorf("the first start's %s speaks of a cluster token, and this release has none:\n%s", name, output)
				}
			}
			if _, statErr := os.Stat(filepath.Join(cfg.Storage.DataDir, "cluster.token")); !os.IsNotExist(statErr) {
				t.Errorf("the first start wrote cluster.token (stat: %v), and nothing reads it", statErr)
			}
			if _, statErr := os.Stat(server.UsersFilePath(cfg.Storage.DataDir)); statErr != nil {
				t.Errorf("the first start did not create the root user: %v", statErr)
			}
		})
	}
}

// TestTheClusterTokenVariableIsIgnoredWithOneWarning: an operator who still sets
// HOARDDB_CLUSTER_TOKEN (or its _FILE form) is told once that it does nothing; no token is
// saved, and the value is not logged. A _FILE that does not exist no longer stops the start.
func TestTheClusterTokenVariableIsIgnoredWithOneWarning(t *testing.T) {
	const token = "7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d7d"
	for _, tc := range []struct{ name, variable, value string }{
		{"the value", "HOARDDB_CLUSTER_TOKEN", token},
		{"the file form, naming no file", "HOARDDB_CLUSTER_TOKEN_FILE", filepath.Join(t.TempDir(), "absent")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := loadStartConfig(t)
			cfg.Server.RootPassword = "an-operator-chosen-password"
			t.Setenv(tc.variable, tc.value)
			var banners, logs concurrentBuffer
			srv, err := prepare(cfg, slog.New(slog.NewJSONHandler(&logs, nil)), &banners, noTerminal())
			if err != nil {
				t.Fatalf("prepare: %v", err)
			}
			defer func() { _ = srv.Stop() }()
			if warnings := strings.Count(logs.String(), tc.variable+" is not used by this release"); warnings != 1 {
				t.Errorf("the log warns %d times that %s is not used, want once:\n%s", warnings, tc.variable, logs.String())
			}
			if strings.Contains(logs.String(), token) || strings.Contains(banners.String(), token) {
				t.Errorf("the start printed the value of %s", tc.variable)
			}
			if _, statErr := os.Stat(filepath.Join(cfg.Storage.DataDir, "cluster.token")); !os.IsNotExist(statErr) {
				t.Errorf("the start wrote cluster.token from %s (stat: %v)", tc.variable, statErr)
			}
		})
	}
}

// TestAnExistingClusterTokenFileIsLeftAsItIs: a data directory an earlier build wrote may hold
// cluster.token. A start neither reads it, changes it, removes it nor speaks of it.
func TestAnExistingClusterTokenFileIsLeftAsItIs(t *testing.T) {
	cfg := loadStartConfig(t)
	cfg.Server.RootPassword = "an-operator-chosen-password"
	if err := os.MkdirAll(cfg.Storage.DataDir, 0o700); err != nil {
		t.Fatalf("creating the data directory: %v", err)
	}
	tokenPath := filepath.Join(cfg.Storage.DataDir, "cluster.token")
	earlier := []byte("an earlier build's file, not even a token\n")
	if err := os.WriteFile(tokenPath, earlier, 0o600); err != nil {
		t.Fatalf("writing %s: %v", tokenPath, err)
	}
	var banners, logs concurrentBuffer
	srv, err := prepare(cfg, slog.New(slog.NewJSONHandler(&logs, nil)), &banners, noTerminal())
	if err != nil {
		t.Fatalf("prepare beside an existing cluster.token: %v", err)
	}
	defer func() { _ = srv.Stop() }()
	if after, readErr := os.ReadFile(tokenPath); readErr != nil || string(after) != string(earlier) {
		t.Errorf("the start changed %s: %q (err %v)", tokenPath, after, readErr)
	}
	if strings.Contains(strings.ToLower(banners.String()+logs.String()), "cluster token") {
		t.Errorf("the start speaks of a cluster token:\n%s\n%s", banners.String(), logs.String())
	}
}

// TestPromptedFirstStartNamesTheCLIBinary: the note a prompted first start
// prints names the command that connects, so it must name the CLI binary as it
// is built (hoarddb), which a case-sensitive filesystem holds to.
func TestPromptedFirstStartNamesTheCLIBinary(t *testing.T) {
	cfg := loadStartConfig(t)
	var banners, logs concurrentBuffer
	terminal := &scriptedTerminal{interactive: true, answers: []string{startRootPassword, startRootPassword}}
	srv, err := prepare(cfg, slog.New(slog.NewJSONHandler(&logs, nil)), &banners, terminal)
	if err != nil {
		t.Fatalf("prepare (prompted first start): %v", err)
	}
	defer func() { _ = srv.Stop() }()
	if want := "Connect with: hoarddb -insecure -user admin -password"; !strings.Contains(banners.String(), want) {
		t.Fatalf("the prompted first start does not print %q:\n%s", want, banners.String())
	}
}
