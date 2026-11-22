package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// firstFlushLeftovers are the tables a kill during the first flush can leave:
// the complete table (killed after the table was written, before the manifest
// record) and every half-written one, cut at several offsets. The footer is
// the last 12 bytes or more of the file, so "just short" cuts land inside it.
var firstFlushLeftovers = append([]struct {
	name   string
	damage func(tableBytes []byte) []byte
}{
	{name: "complete table", damage: func(tableBytes []byte) []byte { return tableBytes }},
	{name: "empty file", damage: func(tableBytes []byte) []byte { return nil }},
	{name: "cut to 300 bytes", damage: func(tableBytes []byte) []byte { return tableBytes[:300] }},
	{name: "cut to the last byte before the footer checksum", damage: func(tableBytes []byte) []byte {
		return tableBytes[:len(tableBytes)-9]
	}},
	{name: "cut one byte short of the end", damage: func(tableBytes []byte) []byte { return tableBytes[:len(tableBytes)-1] }},
}, damagedLeftoverTables...)

// TestAFirstFlushCrashLeftoverIsMovedAsideAndTheStoreOpens defends BUG-0741:
// a manifest that names no table, one table that is complete or half written,
// and the memtable log that holds every acknowledged write is a crash the
// server repairs itself. The store opens, every document is replayed from the
// log, the leftover is kept beside its old name, and the log says so.
func TestAFirstFlushCrashLeftoverIsMovedAsideAndTheStoreOpens(t *testing.T) {
	for _, leftover := range firstFlushLeftovers {
		t.Run(leftover.name, func(t *testing.T) {
			dir, tableName := writeFirstFlushCrashStore(t, nil)
			tablePath := filepath.Join(dir, tableName)
			tableBytes, err := os.ReadFile(tablePath)
			if err != nil {
				t.Fatalf("reading %s: %v", tablePath, err)
			}
			leftoverBytes := leftover.damage(bytes.Clone(tableBytes))
			if err := os.WriteFile(tablePath, leftoverBytes, 0o600); err != nil {
				t.Fatalf("writing %s: %v", tablePath, err)
			}

			outcome := reopenInChild(t, dir, nil)
			if outcome.panicked {
				t.Fatalf("opening the first-flush crash state panicked:\n%s", outcome.helperOutput)
			}
			if outcome.openError != "" {
				t.Fatalf("the first-flush crash state was refused instead of repaired: %s", outcome.openError)
			}
			if outcome.found != corruptDocumentCount || outcome.missing != 0 {
				t.Fatalf("found %d of %d documents, %d missing", outcome.found, corruptDocumentCount, outcome.missing)
			}
			keptBytes, err := os.ReadFile(tablePath + firstFlushQuarantineSuffix)
			if err != nil {
				t.Fatalf("the leftover table was not kept beside its old name: %v", err)
			}
			if !bytes.Equal(keptBytes, leftoverBytes) {
				t.Errorf("the kept leftover differs from the one the crash left (%d bytes, want %d)", len(keptBytes), len(leftoverBytes))
			}
			for _, want := range []string{tableName, tableName + firstFlushQuarantineSuffix, "first flush", "every write still in the memtable log"} {
				if !strings.Contains(outcome.helperOutput, want) {
					t.Errorf("the log does not say %q:\n%s", want, outcome.helperOutput)
				}
			}
		})
	}
}

// TestAStoreThatLooksLikeAFirstFlushCrashButIsNotStaysRefused defends the
// limit of the repair. The table may be moved aside only when nothing was ever
// flushed before it, because then the log holds every write the store has
// seen. Each state here fails that proof in one way and must keep today's
// refusal, with the table left where it is.
func TestAStoreThatLooksLikeAFirstFlushCrashButIsNotStaysRefused(t *testing.T) {
	cases := []struct {
		name  string
		build func(t *testing.T) (dir, tableName string)
	}{
		{name: "a cut manifest over older data and a half-written table", build: func(t *testing.T) (string, string) {
			// The log starts at version 2: version 1 is in the table, so the
			// table holds data the log does not.
			dir, tableName := writeCutManifestLookalike(t, nil)
			cutTableForTest(t, filepath.Join(dir, tableName), 300)
			return dir, tableName
		}},
		{name: "a cut manifest over older data and a corrupt table index", build: func(t *testing.T) (string, string) {
			dir, tableName := writeCutManifestLookalike(t, nil)
			flipTableByteForTest(t, filepath.Join(dir, tableName), 40)
			return dir, tableName
		}},
		{name: "a half-written table that is not the store's first table", build: func(t *testing.T) (string, string) {
			dir, tableName := writeFirstFlushCrashStore(t, nil)
			renamed := "000002.sst"
			if err := os.Rename(filepath.Join(dir, tableName), filepath.Join(dir, renamed)); err != nil {
				t.Fatalf("renaming the table: %v", err)
			}
			cutTableForTest(t, filepath.Join(dir, renamed), 300)
			return dir, renamed
		}},
		{name: "a half-written table and no memtable log", build: func(t *testing.T) (string, string) {
			dir, tableName := writeFirstFlushCrashStore(t, nil)
			cutTableForTest(t, filepath.Join(dir, tableName), 300)
			memLogs, err := filepath.Glob(filepath.Join(dir, "*.mem"))
			if err != nil {
				t.Fatalf("listing the memtable logs: %v", err)
			}
			for _, memLog := range memLogs {
				if err := os.Remove(memLog); err != nil {
					t.Fatalf("removing %s: %v", memLog, err)
				}
			}
			return dir, tableName
		}},
		{name: "a complete table and no memtable log", build: func(t *testing.T) (string, string) {
			dir, tableName := writeFirstFlushCrashStore(t, nil)
			memLogs, err := filepath.Glob(filepath.Join(dir, "*.mem"))
			if err != nil {
				t.Fatalf("listing the memtable logs: %v", err)
			}
			for _, memLog := range memLogs {
				if err := os.Remove(memLog); err != nil {
					t.Fatalf("removing %s: %v", memLog, err)
				}
			}
			return dir, tableName
		}},
		{name: "two table files", build: func(t *testing.T) (string, string) {
			dir, tableName := writeFirstFlushCrashStore(t, nil)
			cutTableForTest(t, filepath.Join(dir, tableName), 300)
			if err := os.WriteFile(filepath.Join(dir, "000002.sst"), []byte("not a table"), 0o600); err != nil {
				t.Fatalf("writing the second table: %v", err)
			}
			return dir, tableName
		}},
	}
	for _, refusedCase := range cases {
		t.Run(refusedCase.name, func(t *testing.T) {
			dir, tableName := refusedCase.build(t)
			outcome := reopenInChild(t, dir, nil)
			if outcome.panicked || outcome.openError == "" {
				t.Fatalf("the store must be refused, got found=%d missing=%d\n%s", outcome.found, outcome.missing, outcome.helperOutput)
			}
			if !strings.Contains(outcome.openError, "restore this node from a backup") {
				t.Errorf("the refusal does not give the restore-or-reseed advice: %s", outcome.openError)
			}
			if _, err := os.Stat(filepath.Join(dir, tableName)); err != nil {
				t.Errorf("the refused open moved or removed the table: %v", err)
			}
			if _, err := os.Stat(filepath.Join(dir, tableName+firstFlushQuarantineSuffix)); err == nil {
				t.Errorf("the refused open quarantined %s", tableName)
			}
		})
	}
}

func cutTableForTest(t *testing.T, tablePath string, size int64) {
	t.Helper()
	if err := os.Truncate(tablePath, size); err != nil {
		t.Fatalf("cutting %s: %v", tablePath, err)
	}
}

func flipTableByteForTest(t *testing.T, tablePath string, bytesFromEnd int) {
	t.Helper()
	tableBytes, err := os.ReadFile(tablePath)
	if err != nil {
		t.Fatalf("reading %s: %v", tablePath, err)
	}
	tableBytes[len(tableBytes)-bytesFromEnd] ^= 0xFF
	if err := os.WriteFile(tablePath, tableBytes, 0o600); err != nil {
		t.Fatalf("writing %s: %v", tablePath, err)
	}
}
