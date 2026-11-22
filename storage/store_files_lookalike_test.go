package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeCutManifestLookalike builds the state that looks like a first-flush
// crash but is not: a store flushed once (3000 documents in 000001.sst), then
// written to again, copied while running, with the copy's manifest cut to its
// header. Moving the table aside there loses the 3000 documents.
func writeCutManifestLookalike(t *testing.T, encryptionKey []byte) (dir, tableName string) {
	t.Helper()
	storeDir := writeClosedStore(t, encryptionKey)
	tableName = filepath.Base(tableFiles(t, storeDir)[0])
	kv, err := OpenBadgerKV(storeDir, BadgerTuning{MemTableMB: 32}, encryptionKey)
	if err != nil {
		t.Fatalf("reopening the store: %v", err)
	}
	batch := kv.NewBatch()
	for index := corruptDocumentCount; index < corruptDocumentCount+10; index++ {
		if err := batch.Set(corruptKey(index), corruptValue(index)); err != nil {
			t.Fatalf("setting document %d: %v", index, err)
		}
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing: %v", err)
	}
	dir = filepath.Join(t.TempDir(), "copy")
	copyStoreFiles(t, storeDir, dir)
	if err := kv.Close(); err != nil {
		t.Fatalf("closing the store: %v", err)
	}
	if err := os.Truncate(filepath.Join(dir, "MANIFEST"), 8); err != nil {
		t.Fatalf("cutting the manifest: %v", err)
	}
	return dir, tableName
}

// TestACutManifestLookalikeIsNeverToldToMoveTheTableAside defends
// the first-flush case against its dangerous twin: the same files as a first-flush crash
// (header-only manifest, one table, a memtable log), but the table holds data
// older than anything in the log. Moving it aside loses that data, so the
// advice must be restore or reseed. Encrypted stores cannot be told apart
// (the table index and the log are sealed), so they get the same advice.
func TestACutManifestLookalikeIsNeverToldToMoveTheTableAside(t *testing.T) {
	for _, mode := range corruptionModes {
		t.Run(mode.name, func(t *testing.T) {
			dir, tableName := writeCutManifestLookalike(t, mode.encryptionKey)
			outcome := reopenInChild(t, dir, mode.encryptionKey)
			assertNoMoveAsideAdvice(t, outcome, dir, tableName)
		})
	}
}

// TestAnEncryptedFirstFlushCrashGetsRestoreAdvice records that a real crash on
// an encrypted store is refused with the general advice: the guard cannot read
// the sealed index and log to confirm it, so it does not offer the move.
func TestAnEncryptedFirstFlushCrashGetsRestoreAdvice(t *testing.T) {
	encryptionKey := corruptionModes[1].encryptionKey
	dir, tableName := writeFirstFlushCrashStore(t, encryptionKey)
	outcome := reopenInChild(t, dir, encryptionKey)
	assertNoMoveAsideAdvice(t, outcome, dir, tableName)
}

func assertNoMoveAsideAdvice(t *testing.T, outcome corruptOutcome, dir, tableName string) {
	t.Helper()
	if outcome.panicked || outcome.openError == "" {
		t.Fatalf("the store must be refused, got found=%d missing=%d\n%s", outcome.found, outcome.missing, outcome.helperOutput)
	}
	// What the repair prints and leaves behind; a refusal shows none of it.
	for _, repaired := range []string{firstFlushQuarantineSuffix, "first flush", "moved aside"} {
		if strings.Contains(outcome.openError, repaired) || strings.Contains(outcome.helperOutput, repaired) {
			t.Errorf("the refused open reports a first-flush repair (%q): %s", repaired, outcome.helperOutput)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, tableName+firstFlushQuarantineSuffix)); err == nil {
		t.Errorf("the refused open quarantined %s", tableName)
	}
	if !strings.Contains(outcome.openError, "restore this node from a backup") {
		t.Errorf("the refusal does not give the restore-or-reseed advice: %s", outcome.openError)
	}
	if _, err := os.Stat(filepath.Join(dir, tableName)); err != nil {
		t.Errorf("the refused open did not keep the table: %v", err)
	}
}
