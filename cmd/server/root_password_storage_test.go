package main

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/server"
)

// scriptedTerminal answers the first-start prompt from a fixed list and
// records every prompt it was shown, so a test drives the interactive path
// without a real terminal.
type scriptedTerminal struct {
	interactive bool
	// operatorTTY says whether the operator's output (stderr) is a terminal,
	// the one place a first start may show a secret.
	operatorTTY bool
	answers     []string
	prompts     []string
}

func (terminal *scriptedTerminal) Interactive() bool { return terminal.interactive }

func (terminal *scriptedTerminal) OperatorOutIsTerminal() bool { return terminal.operatorTTY }

func (terminal *scriptedTerminal) ReadPassword(prompt string) (string, error) {
	terminal.prompts = append(terminal.prompts, prompt)
	if len(terminal.answers) == 0 {
		return "", errors.New("scripted terminal: no answer left")
	}
	answer := terminal.answers[0]
	terminal.answers = terminal.answers[1:]
	return answer, nil
}

// noTerminal is a first start with nobody at the keyboard: a service manager,
// a container, or a test.
func noTerminal() firstStartTerminal { return &scriptedTerminal{} }

// reloadConfig reads the configuration again for a restart in the same
// working directory, as a second process would.
func reloadConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("config.Load (restart): %v", err)
	}
	return cfg
}

// requireRootAuthenticates starts the server again on cfg's data directory and
// checks the root user's password against what users.json holds.
func requireRootAuthenticates(t *testing.T, cfg *config.Config, password string) {
	t.Helper()
	srv, err := prepare(cfg, discardSlog(), io.Discard, noTerminal())
	if err != nil {
		t.Fatalf("prepare (restart): %v", err)
	}
	defer func() {
		if err := srv.Stop(); err != nil {
			t.Errorf("stop restarted server: %v", err)
		}
	}()
	ok, err := srv.Auth().VerifyUser(cfg.Server.RootUser, password)
	if err != nil || !ok {
		t.Fatalf("root %q does not authenticate with its password after a restart: ok=%v err=%v", cfg.Server.RootUser, ok, err)
	}
}

// filesHolding walks the whole of dir and returns every regular file whose
// bytes contain secret.
func filesHolding(t *testing.T, dir, secret string) []string {
	t.Helper()
	var holding []string
	walkErr := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(contents, []byte(secret)) {
			holding = append(holding, path)
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walk %s: %v", dir, walkErr)
	}
	return holding
}

// a data directory written by an older version holds root.password.
// The upgrade start authenticates with that password and deletes the file,
// whether the earlier first start finished (users.json exists) or was
// interrupted before it seeded users.json.
func TestUpgradeConvertsTheRootPasswordFile(t *testing.T) {
	const legacyPassword = "legacy-root-password-0459"

	t.Run("users.json exists", func(t *testing.T) {
		t.Setenv("HOARDDB_ROOT_PASSWORD", legacyPassword)
		cfg := loadStartConfigKeepingPassword(t)
		srv, err := prepare(cfg, discardSlog(), io.Discard, noTerminal())
		if err != nil {
			t.Fatalf("prepare (seed users.json): %v", err)
		}
		if err := srv.Stop(); err != nil {
			t.Fatalf("stop seeding server: %v", err)
		}
		legacyFile := filepath.Join(cfg.Storage.DataDir, "root.password")
		if err := os.WriteFile(legacyFile, []byte(legacyPassword+"\n"), 0o600); err != nil {
			t.Fatalf("write the older version's root.password: %v", err)
		}

		t.Setenv("HOARDDB_ROOT_PASSWORD", "")
		requireRootAuthenticates(t, reloadConfig(t), legacyPassword)
		if _, err := os.Stat(legacyFile); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("root.password is still in the data directory after the upgrade start (stat: %v)", err)
		}
	})

	t.Run("users.json missing", func(t *testing.T) {
		cfg := loadStartConfig(t)
		if err := os.MkdirAll(cfg.Storage.DataDir, 0o700); err != nil {
			t.Fatalf("create data directory: %v", err)
		}
		legacyFile := filepath.Join(cfg.Storage.DataDir, "root.password")
		if err := os.WriteFile(legacyFile, []byte(legacyPassword+"\n"), 0o600); err != nil {
			t.Fatalf("write the older version's root.password: %v", err)
		}

		var banner bytes.Buffer
		srv, err := prepare(cfg, discardSlog(), &banner, noTerminal())
		if err != nil {
			t.Fatalf("prepare (upgrade, no users.json): %v", err)
		}
		if err := srv.Stop(); err != nil {
			t.Fatalf("stop upgraded server: %v", err)
		}
		if strings.Contains(banner.String(), legacyPassword) {
			t.Errorf("the upgrade start printed the existing root password again:\n%s", banner.String())
		}
		if _, err := os.Stat(legacyFile); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("root.password is still in the data directory after the upgrade start (stat: %v)", err)
		}
		requireRootAuthenticates(t, reloadConfig(t), legacyPassword)
	})
}

// with require_root_password on, a first start with no supplied
// password refuses, names HOARDDB_ROOT_PASSWORD, and writes nothing. A
// terminal does not satisfy it: the setting is for unattended nodes.
func TestRequireRootPasswordRefusesWithoutOne(t *testing.T) {
	for _, terminal := range []*scriptedTerminal{
		{},
		{interactive: true, answers: []string{"typed-root-password-1", "typed-root-password-1"}},
	} {
		name := "no terminal"
		if terminal.interactive {
			name = "terminal"
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv("HOARDDB_REQUIRE_ROOT_PASSWORD", "true")
			cfg := loadStartConfig(t)

			srv, err := prepare(cfg, discardSlog(), io.Discard, terminal)
			if err == nil {
				_ = srv.Stop()
				t.Fatal("prepare started with require_root_password on and no password supplied; want a refusal")
			}
			if !strings.Contains(err.Error(), "HOARDDB_ROOT_PASSWORD") {
				t.Errorf("refusal does not name HOARDDB_ROOT_PASSWORD: %v", err)
			}
			if !strings.Contains(err.Error(), "docker run -e HOARDDB_ROOT_PASSWORD=") {
				t.Errorf("refusal does not name the container form: %v", err)
			}
			if len(terminal.prompts) > 0 {
				t.Errorf("require_root_password asked on the terminal (%q); it must refuse instead", terminal.prompts)
			}
			for _, name := range []string{server.UsersFileName, "cluster.token", "root.password"} {
				if _, statErr := os.Stat(filepath.Join(cfg.Storage.DataDir, name)); !errors.Is(statErr, fs.ErrNotExist) {
					t.Errorf("the refused start wrote %s (stat: %v)", name, statErr)
				}
			}
		})
	}

	t.Run("password supplied", func(t *testing.T) {
		t.Setenv("HOARDDB_REQUIRE_ROOT_PASSWORD", "true")
		t.Setenv("HOARDDB_ROOT_PASSWORD", "supplied-root-password")
		cfg := loadStartConfigKeepingPassword(t)
		srv, err := prepare(cfg, discardSlog(), io.Discard, noTerminal())
		if err != nil {
			t.Fatalf("prepare with require_root_password and a supplied password: %v", err)
		}
		if err := srv.Stop(); err != nil {
			t.Fatalf("stop server: %v", err)
		}
	})
}

// an interactive first start with no configured password asks for
// one twice, keeps only its hash, and refuses (writing nothing) when the two
// entries differ.
func TestInteractiveFirstStartPromptsTwice(t *testing.T) {
	t.Run("matching entries", func(t *testing.T) {
		const chosen = "chosen-at-the-terminal-0459"
		cfg := loadStartConfig(t)
		terminal := &scriptedTerminal{interactive: true, answers: []string{chosen, chosen}}

		var banner bytes.Buffer
		srv, err := prepare(cfg, discardSlog(), &banner, terminal)
		if err != nil {
			t.Fatalf("prepare (interactive first start): %v", err)
		}
		if err := srv.Stop(); err != nil {
			t.Fatalf("stop server: %v", err)
		}
		if len(terminal.prompts) != 2 {
			t.Fatalf("the interactive first start prompted %d times (%q), want 2", len(terminal.prompts), terminal.prompts)
		}
		if strings.Contains(banner.String(), chosen) {
			t.Errorf("the typed password was echoed to the operator output:\n%s", banner.String())
		}
		if holding := filesHolding(t, cfg.Storage.DataDir, chosen); len(holding) > 0 {
			t.Errorf("the typed root password is stored in plain text: %v", holding)
		}
		requireRootAuthenticates(t, reloadConfig(t), chosen)
	})

	t.Run("mismatched entries", func(t *testing.T) {
		cfg := loadStartConfig(t)
		terminal := &scriptedTerminal{interactive: true, answers: []string{"first-entry-0459-a", "second-entry-0459-b"}}

		srv, err := prepare(cfg, discardSlog(), io.Discard, terminal)
		if err == nil {
			_ = srv.Stop()
			t.Fatalf("prepare accepted two different entries (prompts %q); want a refusal", terminal.prompts)
		}
		if !strings.Contains(err.Error(), "did not match") {
			t.Errorf("mismatch refusal does not say the entries did not match: %v", err)
		}
		if len(terminal.prompts) != 2 {
			t.Errorf("prompted %d times, want 2", len(terminal.prompts))
		}
		if _, statErr := os.Stat(server.UsersFilePath(cfg.Storage.DataDir)); !errors.Is(statErr, fs.ErrNotExist) {
			t.Errorf("a refused prompt still wrote users.json (stat: %v)", statErr)
		}
	})
}

// loadStartConfigKeepingPassword is loadStartConfig for a test that set
// HOARDDB_ROOT_PASSWORD itself: it keeps that value instead of clearing it.
func loadStartConfigKeepingPassword(t *testing.T) *config.Config {
	t.Helper()
	password := os.Getenv("HOARDDB_ROOT_PASSWORD")
	cfg := loadStartConfig(t)
	t.Setenv("HOARDDB_ROOT_PASSWORD", password)
	cfg.Server.RootPassword = password
	return cfg
}
