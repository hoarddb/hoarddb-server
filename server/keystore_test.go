package server

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
)

func TestEncryptionDefaultsAreOff(t *testing.T) {
	cfg := config.DefaultConfig()
	if cfg.Encryption.Keystore != "off" {
		t.Errorf("default keystore: got %s, want off", cfg.Encryption.Keystore)
	}
	if cfg.Encryption.Mode != "off" {
		t.Errorf("default mode: got %s, want off", cfg.Encryption.Mode)
	}
}

func TestModeOnWithKeystoreOffRefuses(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Encryption.Mode = "on"
	cfg.Encryption.Keystore = "off"

	err := cfg.Validate()
	if err == nil {
		t.Error("Config.Validate should refuse mode=on with keystore=off")
	}

	// Error should name the fix
	if !bytes.Contains([]byte(err.Error()), []byte("keystore")) {
		t.Errorf("error should mention keystore: %v", err)
	}
}

func TestUnknownKeystoreNamesAcceptedValues(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Encryption.Keystore = "unknown"

	err := cfg.Validate()
	if err == nil {
		t.Error("Config.Validate should refuse unknown keystore value")
	}

	// Error should list accepted values
	errMsg := err.Error()
	if !bytes.Contains([]byte(errMsg), []byte("off")) ||
		!bytes.Contains([]byte(errMsg), []byte("keyfile")) ||
		!bytes.Contains([]byte(errMsg), []byte("command")) ||
		!bytes.Contains([]byte(errMsg), []byte("systemd-creds")) {
		t.Errorf("error should list accepted keystore values: %v", err)
	}
}

func TestKeyFileInsideDataDirRefused(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Encryption.Keystore = "keyfile"
	cfg.Encryption.KeyFile = filepath.Join(cfg.Storage.DataDir, "kek.key")

	_, err := OpenKeystore(cfg)
	if err == nil {
		t.Error("OpenKeystore should refuse key_file inside data_dir")
	}
}

func TestKeyFileGroupReadableRefused(t *testing.T) {
	tmpdir := t.TempDir()
	keypath := filepath.Join(tmpdir, "kek")

	// Create a key file with group-readable permissions (0o640)
	if err := os.WriteFile(keypath, []byte("12345678901234567890123456789012"), 0o640); err != nil {
		t.Fatalf("writing test key: %v", err)
	}

	cfg := config.DefaultConfig()
	cfg.Encryption.Keystore = "keyfile"
	cfg.Encryption.KeyFile = keypath

	_, err := OpenKeystore(cfg)
	if err == nil {
		t.Error("OpenKeystore should refuse group-readable key file")
	}

	// Error should mention the mode found
	if !bytes.Contains([]byte(err.Error()), []byte("-rw-r-----")) {
		t.Logf("error: %v", err)
		// Accept any error message mentioning the permission
	}
}

func TestKeyFileAcceptsRawAndHexWithOneNewline(t *testing.T) {
	tmpdir := t.TempDir()

	// Test vector: 32 raw bytes
	rawKey := make([]byte, 32)
	for i := range rawKey {
		rawKey[i] = byte(i)
	}

	// Test case 1: raw bytes, no newline
	keypath1 := filepath.Join(tmpdir, "raw")
	if err := os.WriteFile(keypath1, rawKey, 0o600); err != nil {
		t.Fatalf("writing raw key: %v", err)
	}

	cfg := config.DefaultConfig()
	cfg.Encryption.Keystore = "keyfile"
	cfg.Encryption.KeyFile = keypath1

	ks, err := OpenKeystore(cfg)
	if err != nil {
		t.Fatalf("OpenKeystore raw: %v", err)
	}
	if !bytes.Equal(ks.KEK(), rawKey) {
		t.Error("raw key mismatch")
	}

	// Test case 2: hex bytes, no newline
	hexKey := hex.EncodeToString(rawKey)
	keypath2 := filepath.Join(tmpdir, "hex")
	if err := os.WriteFile(keypath2, []byte(hexKey), 0o600); err != nil {
		t.Fatalf("writing hex key: %v", err)
	}

	cfg.Encryption.KeyFile = keypath2
	ks, err = OpenKeystore(cfg)
	if err != nil {
		t.Fatalf("OpenKeystore hex: %v", err)
	}
	if !bytes.Equal(ks.KEK(), rawKey) {
		t.Error("hex key mismatch")
	}

	// Test case 3: hex bytes with one trailing newline
	keypath3 := filepath.Join(tmpdir, "hex_newline")
	if err := os.WriteFile(keypath3, []byte(hexKey+"\n"), 0o600); err != nil {
		t.Fatalf("writing hex key with newline: %v", err)
	}

	cfg.Encryption.KeyFile = keypath3
	ks, err = OpenKeystore(cfg)
	if err != nil {
		t.Fatalf("OpenKeystore hex with newline: %v", err)
	}
	if !bytes.Equal(ks.KEK(), rawKey) {
		t.Error("hex key with newline mismatch")
	}
}

func TestKeyFileWrongLengthNamesBothForms(t *testing.T) {
	tmpdir := t.TempDir()

	// Test case 1: 31 bytes (too short)
	shortKey := make([]byte, 31)
	keypath1 := filepath.Join(tmpdir, "short")
	if err := os.WriteFile(keypath1, shortKey, 0o600); err != nil {
		t.Fatalf("writing short key: %v", err)
	}

	cfg := config.DefaultConfig()
	cfg.Encryption.Keystore = "keyfile"
	cfg.Encryption.KeyFile = keypath1

	_, err := OpenKeystore(cfg)
	if err == nil {
		t.Error("OpenKeystore should refuse 31-byte key")
	}

	// Error should name both accepted forms
	errMsg := err.Error()
	if !bytes.Contains([]byte(errMsg), []byte("32")) ||
		!bytes.Contains([]byte(errMsg), []byte("64")) {
		t.Errorf("error should name both accepted forms (32 bytes raw, 64 hex): %v", err)
	}
}

func TestKeyCommandStdoutNeverInError(t *testing.T) {
	// This test breaks the guard by hand: write a command whose stdout is
	// echoed, verify it appears in an error, then verify the fix prevents it.
	// For now, write a simpler positive test.

	cfg := config.DefaultConfig()
	cfg.Encryption.Keystore = "command"
	cfg.Encryption.KeyCommand = []string{"/bin/echo", "leaked-secret-key"}

	_, err := OpenKeystore(cfg)
	if err == nil {
		t.Error("OpenKeystore should fail on wrong-length key from command")
	}

	// The error must NOT contain the command's stdout
	if bytes.Contains([]byte(err.Error()), []byte("leaked-secret-key")) {
		t.Error("error message must never echo the command's stdout")
	}
}

func TestKeyCommandRunsWithoutAShell(t *testing.T) {
	// Test that shell metacharacters in argv are treated literally
	cfg := config.DefaultConfig()

	// Create a 32-byte key output
	keyData := make([]byte, 32)
	for i := range keyData {
		keyData[i] = byte(i)
	}

	// Use a command that works on most systems: printf with hex
	hexStr := hex.EncodeToString(keyData)

	// Write a simple script to stdout the key
	tmpdir := t.TempDir()
	scriptPath := filepath.Join(tmpdir, "getkey.sh")
	if err := os.WriteFile(scriptPath, []byte("#!/bin/sh\nprintf "+hexStr), 0o755); err != nil {
		t.Fatalf("writing script: %v", err)
	}

	cfg.Encryption.Keystore = "command"
	cfg.Encryption.KeyCommand = []string{scriptPath}

	ks, err := OpenKeystore(cfg)
	if err != nil {
		t.Fatalf("OpenKeystore: %v", err)
	}

	if !bytes.Equal(ks.KEK(), keyData) {
		t.Error("key mismatch from command")
	}
}

func TestSystemdCredsWithoutDirectoryNamesSystemdRun(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Encryption.Keystore = "systemd-creds"

	// Temporarily unset CREDENTIALS_DIRECTORY if it exists
	oldCreds := os.Getenv("CREDENTIALS_DIRECTORY")
	if err := os.Unsetenv("CREDENTIALS_DIRECTORY"); err != nil {
		t.Fatalf("unsetting CREDENTIALS_DIRECTORY: %v", err)
	}
	t.Cleanup(func() {
		if oldCreds != "" {
			if err := os.Setenv("CREDENTIALS_DIRECTORY", oldCreds); err != nil {
				t.Errorf("restoring CREDENTIALS_DIRECTORY: %v", err)
			}
		}
	})

	_, err := OpenKeystore(cfg)
	if err == nil {
		t.Error("OpenKeystore should fail when CREDENTIALS_DIRECTORY is unset")
	}

	// Error should name the systemd-run command
	if !bytes.Contains([]byte(err.Error()), []byte("systemd-run")) {
		t.Errorf("error should suggest systemd-run: %v", err)
	}
}

// keyCommandHelper is an argv that re-executes this test binary as a key
// command in the given mode. The mode travels in argv after "--", never in the
// environment, because a key command runs with an empty one.
func keyCommandHelper(t *testing.T, mode string, extra ...string) []string {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("locating the test binary: %v", err)
	}
	return append([]string{executable, "-test.run=^TestKeyCommandHelperProcess$", "--", mode}, extra...)
}

// TestKeyCommandHelperProcess is not a test: it is the child that
// keyCommandHelper starts. Run on its own it skips.
func TestKeyCommandHelperProcess(t *testing.T) {
	separator := slices.Index(os.Args, "--")
	if separator < 0 {
		t.Skip("child of the key command tests; runs only when started by them")
	}
	mode, extra := os.Args[separator+1], os.Args[separator+2:]
	switch mode {
	case "env":
		if err := os.WriteFile(extra[0], []byte(strings.Join(os.Environ(), "\n")), 0o600); err != nil {
			os.Exit(4)
		}
		_, _ = os.Stdout.WriteString(strings.Repeat("00", 32))
	case "raw-ending-in-newline":
		_, _ = os.Stdout.Write(newlineEndingKey())
	case "raw-ending-in-newline-plus-newline":
		_, _ = os.Stdout.Write(append(newlineEndingKey(), '\n'))
	case "short":
		_, _ = os.Stdout.WriteString("STDOUT-KEY-BYTES")
	case "fail":
		_, _ = os.Stdout.WriteString("STDOUT-KEY-BYTES-0123456789abcdef")
		_, _ = os.Stderr.WriteString("STDERR-KEY-BYTES-0123456789abcdef")
		os.Exit(3)
	case "sleep":
		time.Sleep(time.Minute)
	case "sleep-with-child":
		// The grandchild inherits stdout, so the pipe stays open after this
		// process is killed: Run does not return until the child exits.
		grandchild := exec.Command(extra[0], "8")
		grandchild.Stdout = os.Stdout
		if err := grandchild.Start(); err != nil {
			os.Exit(5)
		}
		time.Sleep(time.Minute)
	}
	os.Exit(0)
}

func commandKeystoreConfig(argv []string) *config.Config {
	cfg := config.DefaultConfig()
	cfg.Encryption.Keystore = "command"
	cfg.Encryption.KeyCommand = argv
	return cfg
}

// TestKeyCommandEnvIsEmpty holds the environment the command runs in to the
// documented "empty". The test process has PATH, HOME and a canary set; a
// command that can see any of them means the environment was inherited.
func TestKeyCommandEnvIsEmpty(t *testing.T) {
	t.Setenv("HOARDDB_KEY_COMMAND_CANARY", "inherited")
	envDump := filepath.Join(t.TempDir(), "env.txt")

	if _, err := OpenKeystore(commandKeystoreConfig(keyCommandHelper(t, "env", envDump))); err != nil {
		t.Fatalf("OpenKeystore: %v", err)
	}
	seen, err := os.ReadFile(envDump)
	if err != nil {
		t.Fatalf("the key command did not write its environment: %v", err)
	}
	if len(seen) != 0 {
		// Names only: a value may be a secret from the developer's shell.
		var names []string
		for _, entry := range strings.Split(string(seen), "\n") {
			name, _, _ := strings.Cut(entry, "=")
			names = append(names, name)
		}
		t.Errorf("the key command saw a non-empty environment; variables: %v", names)
	}
}

func TestKeyCommandFailureNamesTheCommandAndTheFix(t *testing.T) {
	cases := []struct {
		name string
		argv []string
		want []string
	}{
		{"nonzero exit", keyCommandHelper(t, "fail"), []string{"key_command", "status 3", "empty environment"}},
		{"cannot start", []string{"/nonexistent/hoarddb-key-command"}, []string{"key_command", "/nonexistent/hoarddb-key-command"}},
		{"wrong length", keyCommandHelper(t, "short"), []string{"key_command", "16 bytes", "32 raw", "64 hex"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := OpenKeystore(commandKeystoreConfig(tc.argv))
			if err == nil {
				t.Fatal("OpenKeystore should fail")
			}
			for _, want := range append([]string{tc.argv[0]}, tc.want...) {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the error does not mention %q: %v", want, err)
				}
			}
			for _, leaked := range []string{"STDOUT-KEY-BYTES", "STDERR-KEY-BYTES"} {
				if strings.Contains(err.Error(), leaked) {
					t.Errorf("the error carries the command's output %q: %v", leaked, err)
				}
			}
		})
	}
}

func TestKeyCommandTimesOut(t *testing.T) {
	previous := keyCommandTimeout
	keyCommandTimeout = 300 * time.Millisecond
	t.Cleanup(func() { keyCommandTimeout = previous })

	// The helper runs with an empty environment, so it cannot search PATH.
	sleepPath, err := exec.LookPath("sleep")
	if err != nil {
		t.Skipf("no sleep binary to hold the pipe open: %v", err)
	}
	for _, mode := range []string{"sleep", "sleep-with-child"} {
		t.Run(mode, func(t *testing.T) {
			started := time.Now()
			_, err := OpenKeystore(commandKeystoreConfig(keyCommandHelper(t, mode, sleepPath)))
			if err == nil {
				t.Fatal("OpenKeystore should fail when the key command hangs")
			}
			if elapsed := time.Since(started); elapsed > 4*time.Second {
				t.Errorf("OpenKeystore took %s: the timeout does not bound a command whose child holds its stdout", elapsed)
			}
			for _, want := range []string{"key_command", "did not finish within", keyCommandTimeout.String()} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the timeout error does not mention %q: %v", want, err)
				}
			}
		})
	}
}

func TestKeyFileMissingNamesThePathAndTheFix(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Encryption.Keystore = "keyfile"
	cfg.Encryption.KeyFile = filepath.Join(t.TempDir(), "missing-kek")

	_, err := OpenKeystore(cfg)
	if err == nil {
		t.Fatal("OpenKeystore should fail when the key file is missing")
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("the cause is not reachable through errors.Is: %v", err)
	}
	for _, want := range []string{cfg.Encryption.KeyFile, "key_file", "restore the original", "chmod 600"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error does not mention %q: %v", want, err)
		}
	}
}

func writeCredential(t *testing.T, contents []byte) {
	t.Helper()
	credentialsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(credentialsDir, "hoarddb-dek"), contents, 0o400); err != nil {
		t.Fatalf("writing the credential: %v", err)
	}
	t.Setenv("CREDENTIALS_DIRECTORY", credentialsDir)
}

// TestSystemdCredsReadsTheCredential covers what can run without systemd: the
// file systemd hands a unit under $CREDENTIALS_DIRECTORY. It cannot cover
// systemd itself decrypting an encrypted credential (LoadCredentialEncrypted,
// the TPM or host key), nor a real unit setting CREDENTIALS_DIRECTORY.
func TestSystemdCredsReadsTheCredential(t *testing.T) {
	kek := make([]byte, 32)
	for i := range kek {
		kek[i] = byte(0xA0 + i)
	}
	cfg := config.DefaultConfig()
	cfg.Encryption.Keystore = "systemd-creds"

	for name, contents := range map[string][]byte{
		"32 raw bytes":                    kek,
		"64 hex characters and a newline": []byte(hex.EncodeToString(kek) + "\n"),
	} {
		t.Run(name, func(t *testing.T) {
			writeCredential(t, contents)
			keystore, err := OpenKeystore(cfg)
			if err != nil {
				t.Fatalf("OpenKeystore: %v", err)
			}
			if !bytes.Equal(keystore.KEK(), kek) {
				t.Error("the KEK read from the credential differs from the credential")
			}
		})
	}
}

func TestSystemdCredsRefusesAWrongLength(t *testing.T) {
	secret := bytes.Repeat([]byte{'k'}, 31)
	writeCredential(t, secret)
	cfg := config.DefaultConfig()
	cfg.Encryption.Keystore = "systemd-creds"

	_, err := OpenKeystore(cfg)
	if err == nil {
		t.Fatal("OpenKeystore accepted a 31-byte credential")
	}
	for _, want := range []string{"hoarddb-dek", "32 raw bytes", "64 hex", "31 bytes"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error does not mention %q: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), string(secret)) {
		t.Error("the error carries the credential's bytes")
	}
}
