package main

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/termio"
)

// firstStartTerminal is where an interactive first start asks for the root
// password. main passes the process's own terminal; tests pass a scripted one,
// so the prompt is exercised without a real TTY.
type firstStartTerminal interface {
	// Interactive reports whether someone can answer a prompt.
	Interactive() bool
	// ReadPassword shows prompt and reads one entry without echoing it.
	ReadPassword(prompt string) (string, error)
	// OperatorOutIsTerminal reports whether the operator's output (stderr)
	// is a terminal, the only place a first start shows a secret.
	OperatorOutIsTerminal() bool
}

// stdinTerminal is the server's standard input. Service managers and
// containers started without -t connect stdin to /dev/null or a pipe, so
// they are never interactive.
type stdinTerminal struct{}

func (stdinTerminal) Interactive() bool { return termio.StdinIsTerminal() }

func (stdinTerminal) ReadPassword(prompt string) (string, error) {
	return termio.ReadPassword(prompt)
}

func (stdinTerminal) OperatorOutIsTerminal() bool { return termio.StderrIsTerminal() }

// rootSource is where this start's root password came from, which decides
// what prepare tells the operator about it.
type rootSource int

const (
	// rootUnchosen: users.json already holds the root user, or seeding is
	// off, so this start has no password to choose.
	rootUnchosen rootSource = iota
	// rootFromSetting: HOARDDB_ROOT_PASSWORD or HOARDDB_ROOT_PASSWORD_FILE.
	rootFromSetting
	// rootFromLegacyFile: the root.password an older version left behind.
	rootFromLegacyFile
	// rootPrompted: typed twice at the terminal.
	rootPrompted
)

// rootChoice is the root password a start seeds users.json with, and where it
// came from.
type rootChoice struct {
	password string
	source   rootSource
}

// chooseRootPassword picks the root password for this start before anything
// is written: a supplied setting, then an older
// version's root.password, then a refusal when require_root_password is on,
// then a prompt on a terminal, and otherwise a refusal: a password made up
// with nobody at a terminal would have to be printed to an output that a log
// keeps. Only a start with
// no users.json chooses at all; once the root user exists, its hash is the
// credential and nothing here is asked again.
func chooseRootPassword(cfg *config.Config, rootUser string, terminal firstStartTerminal) (rootChoice, error) {
	if cfg.Server.RootPassword != "" {
		return rootChoice{password: cfg.Server.RootPassword, source: rootFromSetting}, nil
	}
	dataDir := cfg.Storage.DataDir
	if cfg.Cluster.Enabled {
		// A cluster member keeps no users.json: only the data node that starts a new cluster asks
		// (ring user store spec 6.5); a joiner, a witness and a restart never do.
		allowed, err := server.ClusterRootPromptAllowed(cfg)
		if err != nil || !allowed {
			return rootChoice{source: rootUnchosen}, err
		}
	}
	if _, err := os.Stat(server.UsersFilePath(dataDir)); err == nil {
		return rootChoice{source: rootUnchosen}, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return rootChoice{}, fmt.Errorf("check for the user store in %s: %w", dataDir, err)
	}
	if !cfg.Auth.AuthBootstrapRoot {
		return rootChoice{source: rootUnchosen}, nil
	}
	legacy, found, err := server.ReadLegacyRootPassword(dataDir)
	if err != nil {
		return rootChoice{}, err
	}
	if found {
		return rootChoice{password: legacy, source: rootFromLegacyFile}, nil
	}
	if cfg.Auth.RequireRootPassword {
		return rootChoice{}, fmt.Errorf("no root password was supplied, and require_root_password is on (HOARDDB_REQUIRE_ROOT_PASSWORD), so this first start will not generate one. "+
			"Set HOARDDB_ROOT_PASSWORD (or HOARDDB_ROOT_PASSWORD_FILE) and start again. On this machine: HOARDDB_ROOT_PASSWORD='<choose one>' hoarddb-server. "+
			"In a container: docker run -e HOARDDB_ROOT_PASSWORD='<choose one>' -p 7433:7433 hoarddb. Nothing was written to %s", dataDir)
	}
	if !terminal.Interactive() {
		return rootChoice{}, fmt.Errorf("no root password was supplied, and there is no terminal to ask for one, so this first start will not create the root user %q. "+
			"Set it and start again: HOARDDB_ROOT_PASSWORD='<choose one>' hoarddb-server (or HOARDDB_ROOT_PASSWORD_FILE), "+
			"or start hoarddb-server once from a terminal and type one. Nothing was written to %s", rootUser, dataDir)
	}
	password, err := promptRootPassword(terminal, rootUser, dataDir, cfg.Auth.PasswordMinLength)
	if err != nil {
		return rootChoice{}, err
	}
	return rootChoice{password: password, source: rootPrompted}, nil
}

// promptRootPassword asks twice and returns the password only when both
// entries match and it meets password_min_length. Every refusal says nothing
// was written and how to retry or skip the prompt.
func promptRootPassword(terminal firstStartTerminal, rootUser, dataDir string, minLength int) (string, error) {
	first, err := terminal.ReadPassword(fmt.Sprintf("No root password is set. Choose one for the root user %q: ", rootUser))
	if err != nil {
		return "", fmt.Errorf("read the root password: %w", err)
	}
	second, err := terminal.ReadPassword("Repeat it: ")
	if err != nil {
		return "", fmt.Errorf("read the root password again: %w", err)
	}
	retry := fmt.Sprintf("nothing was written to %s. Start hoarddb-server again to retry, or set HOARDDB_ROOT_PASSWORD to skip the prompt", dataDir)
	switch length := utf8.RuneCountInString(first); {
	case first != second:
		return "", fmt.Errorf("the two root passwords did not match; %s", retry)
	case first == "":
		return "", fmt.Errorf("the root password is empty; %s", retry)
	case minLength > 0 && length < minLength:
		return "", fmt.Errorf("the root password is %d characters, below the %d-character minimum (password_min_length, HOARDDB_PASSWORD_MIN_LENGTH); %s",
			length, minLength, retry)
	}
	return first, nil
}

// convertRootPasswordFile deletes the root.password an older version left in
// dataDir once users.json holds the root credential, and says so once. A
// failed delete does not stop the start: the credential is no more exposed
// than before the upgrade, so it warns with the fix and the next start tries
// again.
func convertRootPasswordFile(dataDir, usersPath string, logger *slog.Logger) {
	legacyPath := server.RootPasswordFilePath(dataDir)
	if _, err := os.Lstat(legacyPath); errors.Is(err, fs.ErrNotExist) {
		return
	}
	if _, err := os.Stat(usersPath); err != nil {
		// Nothing holds the root credential yet (auth_bootstrap_root is
		// off), so the file is still the only copy of it.
		return
	}
	if err := server.RetireRootPasswordFile(dataDir); err != nil {
		logger.Warn("could not delete the plain-text root password file; users.json already holds its hash",
			"path", legacyPath, "error", err, "fix", "remove it by hand: rm "+legacyPath)
		return
	}
	logger.Info("converted the first-run root password file: only its hash is kept, in users.json, and the file is deleted",
		"deleted", legacyPath, "users_file", usersPath)
}
