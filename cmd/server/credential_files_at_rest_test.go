package main

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

// sealedEnvelopePrefix is the start of every whole-file sealed envelope: the
// magic 00 'H' 'D' 'B' and version 0x02 (at-rest-encryption spec §2.3).
var sealedEnvelopePrefix = []byte{0x00, 'H', 'D', 'B', 0x02}

// assertFileSealedWithout fails unless path starts with a sealed envelope of
// purpose and holds none of the given byte strings.
func assertFileSealedWithout(t *testing.T, path string, purpose byte, absent ...[]byte) {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	if !bytes.HasPrefix(contents, append(append([]byte(nil), sealedEnvelopePrefix...), purpose)) {
		t.Errorf("%s is not sealed on an encrypted node (first bytes %q)", path, firstBytesOf(contents, 8))
	}
	for _, secret := range absent {
		if bytes.Contains(contents, secret) {
			t.Errorf("%s holds %q in the clear on an encrypted node", path, secret)
		}
	}
}

func firstBytesOf(contents []byte, count int) []byte {
	if len(contents) < count {
		return contents
	}
	return contents[:count]
}

// TestFirstEncryptedStartWritesNoCredentialInTheClear: the first start of an
// encrypted node writes its signing key and TLS key sealed under the node
// master, from the start: neither is ever written in the clear.
func TestFirstEncryptedStartWritesNoCredentialInTheClear(t *testing.T) {
	cfg := loadStartConfigWithRoot(t)
	sealWithKeyFile(t, cfg)
	cfg.Encryption.Mode = "require"
	var banners, logs concurrentBuffer
	srv, err := prepare(cfg, slog.New(slog.NewJSONHandler(&logs, nil)), &banners, &scriptedTerminal{operatorTTY: true})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if err := srv.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	dataDir := cfg.Storage.DataDir
	assertFileSealedWithout(t, filepath.Join(dataDir, "auth.token.key"), 0x09)
	assertFileSealedWithout(t, filepath.Join(dataDir, "keys", "server.key"), 0x0B, []byte("PRIVATE KEY"))
}

// TestAFirstStartCreatesAMissingDataDirectoryOwnerOnly: a node started on a data
// directory that does not exist yet, parents included, creates it owner-only.
func TestAFirstStartCreatesAMissingDataDirectoryOwnerOnly(t *testing.T) {
	cfg := loadStartConfigWithRoot(t)
	cfg.Storage.DataDir = filepath.Join(t.TempDir(), "not", "yet")
	var banners, logs concurrentBuffer
	srv, err := prepare(cfg, slog.New(slog.NewJSONHandler(&logs, nil)), &banners, noTerminal())
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if err := srv.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	info, err := os.Stat(cfg.Storage.DataDir)
	if err != nil {
		t.Fatalf("stat the data directory: %v", err)
	}
	if mode := info.Mode().Perm(); mode != 0o700 {
		t.Errorf("the data directory was created with mode %o, want 700", mode)
	}
}
