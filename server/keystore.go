package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/sealed"
)

// Keystore holds a KEK (Key Encryption Key) and its metadata.
type Keystore struct {
	kek  []byte
	kid  string
	mode string
	// source is encryption.keystore: keyfile, command or systemd-creds.
	source string
}

// KEK returns the Key Encryption Key (32 bytes).
func (ks *Keystore) KEK() []byte {
	return ks.kek
}

// Kid returns the key ID: first 8 hex chars of sha256(KEK).
func (ks *Keystore) Kid() string {
	return ks.kid
}

// Source returns encryption.keystore, the keystore the KEK came from: keyfile,
// command or systemd-creds.
func (ks *Keystore) Source() string {
	return ks.source
}

// Mode returns the encryption mode: "off", "on", "require".
func (ks *Keystore) Mode() string {
	return ks.mode
}

// OpenKeystore resolves the keystore and returns the Keystore or an error.
// When keystore=off, returns (nil, nil).
func OpenKeystore(cfg *config.Config) (*Keystore, error) {
	if cfg.Encryption.Keystore == "off" {
		return nil, nil
	}

	var kek []byte
	var err error

	switch cfg.Encryption.Keystore {
	case "keyfile":
		kek, err = openKeystoreKeyfile(cfg.Encryption.KeyFile, cfg.Storage.DataDir)
	case "command":
		kek, err = openKeystoreCommand(cfg.Encryption.KeyCommand)
	case "systemd-creds":
		kek, err = openKeystoreSystemdCreds()
	default:
		return nil, fmt.Errorf("unknown keystore type: %s", cfg.Encryption.Keystore)
	}

	if err != nil {
		return nil, err
	}

	if len(kek) != 32 {
		return nil, fmt.Errorf("keystore returned %d bytes, want 32", len(kek))
	}

	return &Keystore{
		kek:    kek,
		kid:    sealed.Kid(kek),
		mode:   cfg.Encryption.Mode,
		source: cfg.Encryption.Keystore,
	}, nil
}

// openKeystoreKeyfile reads and parses a key file.
func openKeystoreKeyfile(keyPath, dataDir string) ([]byte, error) {
	// Refuse a path inside the data directory
	abs, err := filepath.Abs(keyPath)
	if err != nil {
		return nil, fmt.Errorf("resolving key file path: %w", err)
	}

	dataAbs, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("resolving data directory: %w", err)
	}

	if strings.HasPrefix(abs, dataAbs+string(filepath.Separator)) || abs == dataAbs {
		return nil, fmt.Errorf("key_file must not be inside data_dir (%s is inside %s)", abs, dataAbs)
	}

	// Check file permissions
	info, err := os.Stat(abs)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("key_file %s does not exist: %w. If this node has run with encryption before, restore the original key file to that path: "+
			"a newly generated key cannot open data sealed under the old one. For a new node, create one with "+
			"'head -c 32 /dev/urandom > %s && chmod 600 %s' and keep a copy outside the data directory; "+
			"the path comes from encryption.key_file (HOARDDB_ENCRYPTION_KEY_FILE)", abs, err, abs, abs)
	}
	if err != nil {
		return nil, fmt.Errorf("opening key_file %s: %w", abs, err)
	}

	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return nil, fmt.Errorf("key_file is group or world-readable (mode %04o); must be 0600", perm)
	}

	// Read and parse the key
	data, err := os.ReadFile(abs)
	if err != nil {
		return nil, fmt.Errorf("reading key file: %w", err)
	}

	if kek, ok := parseKeyMaterial(data); ok {
		return kek, nil
	}
	return nil, fmt.Errorf("key file must contain exactly 32 raw bytes or 64 hex characters (one trailing newline is allowed); got %d bytes", len(data))
}

// parseKeyMaterial accepts 32 raw bytes or 64 hex characters, each optionally
// followed by one newline. The length is judged before any newline is removed,
// because a raw key may itself end in 0x0a: trimming first would read such a
// key as 31 bytes. The four accepted lengths (32, 33, 64, 65) do not overlap,
// so no input is ambiguous.
func parseKeyMaterial(material []byte) ([]byte, bool) {
	switch {
	case len(material) == 32:
		return material, true
	case len(material) == 33 && material[32] == '\n':
		return material[:32], true
	}
	hexText := material
	if len(hexText) == 65 && hexText[64] == '\n' {
		hexText = hexText[:64]
	}
	if len(hexText) != 64 {
		return nil, false
	}
	decoded, err := hex.DecodeString(string(hexText))
	if err != nil {
		return nil, false
	}
	return decoded, true
}

// keyCommandTimeout bounds a key command. A variable only so a test can shorten
// it; nothing else assigns it.
var keyCommandTimeout = 10 * time.Second

// keyCommandWaitDelay bounds how long Run waits for stdout to close after the
// command has exited or been killed. Without it, a child the command started
// (a wrapper script's `sleep`, an agent) keeps the pipe open and the 10 s
// timeout waits on that child instead.
const keyCommandWaitDelay = time.Second

// openKeystoreCommand runs a command and reads its stdout.
func openKeystoreCommand(argv []string) ([]byte, error) {
	if len(argv) == 0 {
		return nil, fmt.Errorf("key_command is empty: with encryption.keystore=command, set encryption.key_command to the argv list of a program that prints the key (32 raw bytes or 64 hex characters) on stdout")
	}

	ctx, cancel := context.WithTimeout(context.Background(), keyCommandTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Env = []string{} // Run with empty environment
	cmd.WaitDelay = keyCommandWaitDelay

	// stderr is left unset (discarded) and stdout is never put in an error:
	// either may carry the key.
	var stdout bytes.Buffer
	cmd.Stdout = &stdout

	if err := cmd.Run(); err != nil {
		return nil, keyCommandFailure(argv, ctx.Err(), err)
	}

	output := stdout.Bytes()
	if kek, ok := parseKeyMaterial(output); ok {
		return kek, nil
	}
	return nil, fmt.Errorf("key_command %q printed %d bytes on stdout; it must print exactly 32 raw bytes or 64 hex characters (one trailing newline is allowed)", argv[0], len(output))
}

// keyCommandFailure words a failed key command. It names the executable and
// how many arguments it had, not the arguments: they may carry a credential.
// The fix half is the same for every cause because the empty environment is the
// usual one.
func keyCommandFailure(argv []string, contextErr, runErr error) error {
	command := fmt.Sprintf("%q (with %d argument(s))", argv[0], len(argv)-1)
	const fix = "The command runs without a shell and with an empty environment (no PATH, HOME, VAULT_ADDR or other variables), so use absolute paths and put " +
		"anything it needs on its own command line or configuration. Try it by hand with 'env -i <your key_command>': it must print the key on stdout and exit 0"

	var exitErr *exec.ExitError
	switch {
	case errors.Is(contextErr, context.DeadlineExceeded):
		return fmt.Errorf("key_command %s did not finish within %s and was stopped. %s: %w", command, keyCommandTimeout, fix, runErr)
	case errors.Is(runErr, exec.ErrWaitDelay):
		return fmt.Errorf("key_command %s exited, but a process it started is still holding its output open, so the output may be incomplete. "+
			"Make the command exit without leaving children behind. %s: %w", command, fix, runErr)
	case errors.As(runErr, &exitErr) && exitErr.ExitCode() >= 0:
		return fmt.Errorf("key_command %s exited with status %d. %s: %w", command, exitErr.ExitCode(), fix, runErr)
	case errors.As(runErr, &exitErr):
		return fmt.Errorf("key_command %s was terminated (%s). %s: %w", command, exitErr.ProcessState, fix, runErr)
	default:
		return fmt.Errorf("key_command %s could not be started. Check that %q exists, is executable and is an absolute path. %s: %w", command, argv[0], fix, runErr)
	}
}

// openKeystoreSystemdCreds reads the hoarddb-dek credential from systemd.
func openKeystoreSystemdCreds() ([]byte, error) {
	credsDir := os.Getenv("CREDENTIALS_DIRECTORY")
	if credsDir == "" {
		return nil, fmt.Errorf(`CREDENTIALS_DIRECTORY is not set; systemd-creds requires the LoadCredential unit directive
	worked example (to use with -export-key, -import-key, -reset-password):
	  systemd-run --wait --pipe -p LoadCredential=hoarddb-dek:/etc/credstore.encrypted/hoarddb-dek hoarddb-server -export-key /tmp/nm.key`)
	}

	credPath := filepath.Join(credsDir, "hoarddb-dek")
	data, err := os.ReadFile(credPath)
	if err != nil {
		return nil, fmt.Errorf("reading systemd credential hoarddb-dek: %w", err)
	}

	if kek, ok := parseKeyMaterial(data); ok {
		return kek, nil
	}
	return nil, fmt.Errorf("hoarddb-dek credential must contain exactly 32 raw bytes or 64 hex characters (one trailing newline is allowed); got %d bytes", len(data))
}
