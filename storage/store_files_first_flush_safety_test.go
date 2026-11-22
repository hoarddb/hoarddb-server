package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// leakedFirstLogStore builds the state the equality test of the first-flush check used to refuse:
// a first memtable log that survived (its removal failed after the flush), and one table that was
// written later and holds newer versions than that log. Badger's flush order (C1,
// DB.flushToNamedTable) makes this state unreachable: a log that begins at version 1 is on disk
// only if no table ever reached a level.
func leakedFirstLogStore(t *testing.T) (dir, tableName string) {
	t.Helper()
	dir, tableName = writeFirstFlushCrashStore(t, nil)
	newerDir := filepath.Join(t.TempDir(), "newer")
	kv, err := OpenBadgerKV(newerDir, BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	for _, version := range []string{"first", "second"} {
		batch := kv.NewBatch()
		for index := 0; index < corruptDocumentCount; index++ {
			if err := batch.Set(corruptKey(index), append(corruptValue(index), version...)); err != nil {
				t.Fatalf("setting document %d: %v", index, err)
			}
		}
		if err := batch.Commit(true); err != nil {
			t.Fatalf("committing: %v", err)
		}
	}
	if err := kv.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}
	newerTable := tableFiles(t, newerDir)
	if len(newerTable) != 1 {
		t.Fatalf("want one table in the newer store, got %v", newerTable)
	}
	tableBytes, err := os.ReadFile(newerTable[0])
	if err != nil {
		t.Fatalf("reading the newer table: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, tableName), tableBytes, 0o600); err != nil {
		t.Fatalf("writing the newer table: %v", err)
	}
	return dir, tableName
}

// TestALaterTableBesideALeakedFirstLogIsAcceptedBecauseC1MakesItUnreachable pins what BUG-0859's
// relaxation does with the state it can no longer tell from a cut first flush: the leftover table's
// highest version is at or above the oldest log's, so the table is set aside, kept, and the log is
// replayed. Before the relaxation (equality) this state was refused. The relaxation is safe only
// because C1's order makes the state unreachable; if that order is ever undone, this test is the
// place that says what the guard then loses.
func TestALaterTableBesideALeakedFirstLogIsAcceptedBecauseC1MakesItUnreachable(t *testing.T) {
	dir, tableName := leakedFirstLogStore(t)
	outcome := reopenInChild(t, dir, nil)
	if outcome.panicked || outcome.openError != "" {
		t.Fatalf("the store must open with the leftover set aside, got %q\n%s", outcome.openError, outcome.helperOutput)
	}
	if _, err := os.Stat(filepath.Join(dir, tableName+firstFlushQuarantineSuffix)); err != nil {
		t.Errorf("the leftover table was not kept beside its old name: %v", err)
	}
}

// TestAQuarantineNameAlreadyTakenGetsANumber defends the never-overwrite rule
// for an earlier crash's leftover.
func TestAQuarantineNameAlreadyTakenGetsANumber(t *testing.T) {
	dir, tableName := writeFirstFlushCrashStore(t, nil)
	earlier := []byte("an earlier crash's leftover")
	if err := os.WriteFile(filepath.Join(dir, tableName+".first-flush-crash"), earlier, 0o600); err != nil {
		t.Fatalf("writing the earlier leftover: %v", err)
	}
	tableBytes, err := os.ReadFile(filepath.Join(dir, tableName))
	if err != nil {
		t.Fatalf("reading the table: %v", err)
	}
	outcome := reopenInChild(t, dir, nil)
	if outcome.openError != "" || outcome.found != corruptDocumentCount {
		t.Fatalf("open error %q, found %d\n%s", outcome.openError, outcome.found, outcome.helperOutput)
	}
	keptEarlier, err := os.ReadFile(filepath.Join(dir, tableName+".first-flush-crash"))
	if err != nil || !bytes.Equal(keptEarlier, earlier) {
		t.Errorf("the earlier leftover was overwritten or lost: %v", err)
	}
	keptNew, err := os.ReadFile(filepath.Join(dir, tableName+".first-flush-crash.2"))
	if err != nil || !bytes.Equal(keptNew, tableBytes) {
		t.Errorf("the new leftover is not kept as .2: %v", err)
	}
}

// TestAFailedDirectorySyncAfterTheMoveSaysJustStartAgain defends the
// wording of the manual repair: the rename succeeded, so the instruction must
// not tell the operator to move a file that is already gone.
func TestAFailedDirectorySyncAfterTheMoveSaysJustStartAgain(t *testing.T) {
	dir, tableName := writeFirstFlushCrashStore(t, nil)
	original := syncDirectory
	syncDirectory = func(string) error { return os.ErrInvalid }
	t.Cleanup(func() { syncDirectory = original })

	_, err := OpenBadgerKV(dir, BadgerTuning{MemTableMB: 32}, nil)
	if err == nil {
		t.Fatal("the open succeeded although the directory sync failed")
	}
	for _, want := range []string{"If " + tableName + " is still", "otherwise just start again"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error does not say %q: %v", want, err)
		}
	}
}

// TestARefusedMemtableShrinkLeavesTheFirstFlushLeftoverInPlace defends the
// order of the guards: a refusal must never follow a change to the store.
func TestARefusedMemtableShrinkLeavesTheFirstFlushLeftoverInPlace(t *testing.T) {
	dir, tableName := writeFirstFlushCrashStore(t, nil)
	_, err := OpenBadgerKV(dir, BadgerTuning{MemTableMB: 16}, nil)
	if err == nil || !strings.Contains(err.Error(), "Memtable size mismatch") {
		t.Fatalf("want the memtable-size refusal, got %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, tableName)); statErr != nil {
		t.Errorf("the refused open moved the table: %v", statErr)
	}
}
