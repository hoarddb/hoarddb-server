package server

import (
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
)

// newlineEndingKey is a valid 32-byte raw key whose last byte is 0x0a, the
// byte the readers allow as one trailing newline. One raw key in 256 drawn
// from /dev/urandom ends this way.
func newlineEndingKey() []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(0x40 + i)
	}
	key[31] = '\n'
	return key
}

// TestRawKeyEndingInNewlineOpens: every keystore that reads key
// bytes accepts a raw key ending in 0x0a, with and without one more trailing
// newline, and the KEK is exactly those 32 bytes. Hex keys keep their
// optional newline.
func TestRawKeyEndingInNewlineOpens(t *testing.T) {
	key := newlineEndingKey()
	hexKey := []byte(hex.EncodeToString(key))
	contents := map[string][]byte{
		"raw":              key,
		"raw plus newline": append(append([]byte(nil), key...), '\n'),
		"hex":              hexKey,
		"hex plus newline": append(append([]byte(nil), hexKey...), '\n'),
	}

	for name, content := range contents {
		t.Run("keyfile/"+name, func(t *testing.T) {
			keyPath := filepath.Join(t.TempDir(), "kek")
			if err := os.WriteFile(keyPath, content, 0o600); err != nil {
				t.Fatalf("writing key file: %v", err)
			}
			cfg := config.DefaultConfig()
			cfg.Encryption.Keystore = "keyfile"
			cfg.Encryption.KeyFile = keyPath
			requireKEK(t, cfg, key)
		})
		t.Run("systemd-creds/"+name, func(t *testing.T) {
			credsDir := t.TempDir()
			if err := os.WriteFile(filepath.Join(credsDir, "hoarddb-dek"), content, 0o600); err != nil {
				t.Fatalf("writing credential: %v", err)
			}
			t.Setenv("CREDENTIALS_DIRECTORY", credsDir)
			cfg := config.DefaultConfig()
			cfg.Encryption.Keystore = "systemd-creds"
			requireKEK(t, cfg, key)
		})
	}
	for _, mode := range []string{"raw-ending-in-newline", "raw-ending-in-newline-plus-newline"} {
		t.Run("command/"+mode, func(t *testing.T) {
			requireKEK(t, commandKeystoreConfig(keyCommandHelper(t, mode)), key)
		})
	}
}

// TestKeyReadersRefuseAKeyWithTwoTrailingNewlines keeps "one trailing newline
// is allowed" exact: a 32-byte key followed by two newlines is 34 bytes and is
// refused, not trimmed until it fits.
func TestKeyReadersRefuseAKeyWithTwoTrailingNewlines(t *testing.T) {
	content := append(append([]byte(nil), newlineEndingKey()...), '\n', '\n')
	keyPath := filepath.Join(t.TempDir(), "kek")
	if err := os.WriteFile(keyPath, content, 0o600); err != nil {
		t.Fatalf("writing key file: %v", err)
	}
	cfg := config.DefaultConfig()
	cfg.Encryption.Keystore = "keyfile"
	cfg.Encryption.KeyFile = keyPath
	if _, err := OpenKeystore(cfg); err == nil {
		t.Fatal("a 34-byte key file was accepted")
	}
}

func requireKEK(t *testing.T, cfg *config.Config, want []byte) {
	t.Helper()
	ks, err := OpenKeystore(cfg)
	if err != nil {
		t.Fatalf("OpenKeystore: %v", err)
	}
	if !bytes.Equal(ks.KEK(), want) {
		t.Fatalf("KEK = %x, want %x", ks.KEK(), want)
	}
}
