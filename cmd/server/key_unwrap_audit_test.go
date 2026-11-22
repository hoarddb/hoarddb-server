package main

import (
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/server"
)

// writeTestKEKBytes writes a 32-byte KEK at 0600 and returns its bytes, so a
// test can later look for them in what was written.
func writeTestKEKBytes(t *testing.T, path string, seed byte) []byte {
	t.Helper()
	kek := make([]byte, 32)
	for i := range kek {
		kek[i] = seed + byte(i)
	}
	if err := os.WriteFile(path, kek, 0o600); err != nil {
		t.Fatalf("writing KEK %s: %v", path, err)
	}
	return kek
}

// sealedNodeStartedWithTheWrongKey first starts a sealed node with a good KEK
// and then points cfg at a different key, so the next prepare fails to unwrap
// the node master. It returns the good and wrong KEK bytes.
func sealedNodeStartedWithTheWrongKey(t *testing.T, cfg *config.Config) (goodKEK, wrongKEK []byte) {
	t.Helper()
	goodPath := filepath.Join(t.TempDir(), "good-kek")
	goodKEK = writeTestKEKBytes(t, goodPath, 0x40)
	cfg.Encryption = config.EncryptionConfig{Keystore: "keyfile", KeyFile: goodPath, Mode: "on"}

	first, err := prepare(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), io.Discard, noTerminal())
	if err != nil {
		t.Fatalf("first start with the right key: %v", err)
	}
	if err := first.Stop(); err != nil {
		t.Fatalf("stopping the first service: %v", err)
	}

	wrongPath := filepath.Join(t.TempDir(), "wrong-kek")
	wrongKEK = writeTestKEKBytes(t, wrongPath, 0x80)
	cfg.Encryption.KeyFile = wrongPath
	return goodKEK, wrongKEK
}

// TestPrepareAuditsAFailedKeyUnwrap is BUG-0315: a sealed node started with the
// wrong key refuses in prepare, before the service and its audit log exist, so
// prepare must append the one key.unwrap result=failed entry itself. Before the
// fix the audit file is never created and this test finds no entry.
func TestPrepareAuditsAFailedKeyUnwrap(t *testing.T) {
	cfg := loadStartConfigWithRoot(t)
	goodKEK, wrongKEK := sealedNodeStartedWithTheWrongKey(t, cfg)
	auditPath := filepath.Join(t.TempDir(), "audit.jsonl")
	cfg.Audit.Enabled = true
	cfg.Audit.Output = auditPath

	if _, err := prepare(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), io.Discard, noTerminal()); err == nil {
		t.Fatal("prepare with the wrong key returned no error")
	}

	raw, readErr := os.ReadFile(auditPath)
	if readErr != nil && !os.IsNotExist(readErr) {
		t.Fatalf("reading the audit log: %v", readErr)
	}

	var failed *server.AuditEntry
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.Contains(line, `"`+server.EventKeyUnwrap+`"`) {
			continue
		}
		var entry server.AuditEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("parsing audit line %q: %v", line, err)
		}
		if entry.Result == "failed" {
			failed = &entry
			break
		}
	}
	if failed == nil {
		t.Fatalf("no key.unwrap result=failed entry in the audit log:\n%s", raw)
	}
	if failed.Target == "" {
		t.Error("the failed key.unwrap entry names no key source")
	}
	for _, material := range []string{hex.EncodeToString(goodKEK), hex.EncodeToString(wrongKEK)} {
		if strings.Contains(string(raw), material) {
			t.Errorf("the audit entry carries key material (%s):\n%s", material, raw)
		}
	}
}

// TestPrepareStillFailsWhenTheAuditLogIsUnwritable is BUG-0315's second
// acceptance criterion: the audit entry is best-effort. A node whose audit path
// cannot be opened must still refuse with the original key error and warn,
// never change the exit path.
func TestPrepareStillFailsWhenTheAuditLogIsUnwritable(t *testing.T) {
	cfg := loadStartConfigWithRoot(t)
	sealedNodeStartedWithTheWrongKey(t, cfg)
	cfg.Audit.Enabled = true
	// The parent directory does not exist, so NewAuditLogger cannot create the file.
	cfg.Audit.Output = filepath.Join(t.TempDir(), "missing", "audit.jsonl")

	var logs concurrentBuffer
	_, err := prepare(cfg, slog.New(slog.NewTextHandler(&logs, nil)), io.Discard, noTerminal())
	if err == nil || !strings.Contains(err.Error(), "node master") {
		t.Fatalf("prepare with an unwritable audit log = %v, want the original key error", err)
	}
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "audit") {
		t.Errorf("an unwritable audit log was not warned about:\n%s", logs.String())
	}
}
