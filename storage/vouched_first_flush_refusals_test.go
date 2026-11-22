package storage

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The vouched open relaxes the first-flush check from "the leftover table's highest version equals
// the oldest log's" to "is at or above it" (BUG-0859). That is safe only for a store whose first
// log is still on disk because no table ever reached a level (C1), so every state below, which has
// a table the MANIFEST names no longer or an older table, must stay refused, naming the damage and
// no advice: the ring node adds the advice, which depends on whether it is a cluster member.

// refusedByTheVouchedOpen opens dir through the cut and returns the refusal, failing when the
// store opens.
func refusedByTheVouchedOpen(t *testing.T, dir string) string {
	t.Helper()
	kv, err := openVouchedTestStore(dir, BadgerTuning{MemTableMB: 32})
	if err == nil {
		_ = kv.Close() // a test's close; the failure below is the result
		t.Fatal("the store opened, want it refused: the state is not a first-flush crash")
	}
	var damaged *StoreDamagedError
	if !errors.As(err, &damaged) {
		t.Fatalf("the refusal is %T (%v), want a StoreDamagedError", err, err)
	}
	return err.Error()
}

// assertRefusalWithoutAdvice checks that a refusal on the ring node's store names the store and
// the damage and gives no advice (requireNoRingAdvice), and that the leftover table was kept where
// it was.
func assertRefusalWithoutAdvice(t *testing.T, refusal, dir, tableName string) {
	t.Helper()
	for _, want := range []string{"the node store at " + dir + " is damaged: ", "so the store was not opened and nothing in it was changed"} {
		if !strings.Contains(refusal, want) {
			t.Errorf("the refusal lacks %q: %s", want, refusal)
		}
	}
	if !strings.HasSuffix(refusal, "nothing in it was changed") {
		t.Errorf("the refusal goes on past the damage, with advice storage cannot give: %s", refusal)
	}
	requireNoRingAdvice(t, refusal)
	if _, err := os.Stat(filepath.Join(dir, tableName)); err != nil {
		t.Errorf("the refused open did not keep %s: %v", tableName, err)
	}
	if _, err := os.Stat(filepath.Join(dir, tableName+firstFlushQuarantineSuffix)); err == nil {
		t.Errorf("the refused open moved %s aside", tableName)
	}
}

// writeVouchedFirstFlushImage builds the state of the committed positive test: the image of a
// vouched store as of its first sync (marker, a MANIFEST naming no table, a log with version 1),
// plus the table a close wrote holding that commit and one commit more. It returns the image and
// the table's name.
func writeVouchedFirstFlushImage(t *testing.T) (dir, tableName string) {
	t.Helper()
	tuning := BadgerTuning{MemTableMB: 32}
	running := filepath.Join(t.TempDir(), "running")
	kv, err := openVouchedTestStore(running, tuning)
	noErr(t, err, "opening the store")
	first := kv.NewBatch()
	noErr(t, first.Set([]byte("vouched"), []byte("kept")), "setting the vouched key")
	noErr(t, first.Commit(true), "committing the vouched key, synced")
	dir = filepath.Join(t.TempDir(), "image")
	copyStoreFiles(t, running, dir)
	second := kv.NewBatch()
	noErr(t, second.Set([]byte("unsynced"), []byte("above the marker")), "setting the unsynced key")
	noErr(t, second.Commit(false), "committing the unsynced key")
	noErr(t, kv.Close(), "closing the running store")
	tables := tableFiles(t, running)
	if len(tables) != 1 {
		t.Fatalf("SCENARIO NOT REACHED: the close left %d tables, want one", len(tables))
	}
	tableBytes, err := os.ReadFile(tables[0])
	noErr(t, err, "reading the flushed table")
	tableName = filepath.Base(tables[0])
	noErr(t, os.WriteFile(filepath.Join(dir, tableName), tableBytes, 0o600), "writing the leftover table")
	return dir, tableName
}

// TestAVouchedFirstFlushLeftoverBelowTheLogStaysRefused: the leftover table's highest version is
// below the oldest log's, so it is not that log's flush. Accepting any version (the mutation this
// kills) would move it aside as if it were.
func TestAVouchedFirstFlushLeftoverBelowTheLogStaysRefused(t *testing.T) {
	tuning := BadgerTuning{MemTableMB: 32}
	oneCommit := filepath.Join(t.TempDir(), "one")
	shortStore, err := openVouchedTestStore(oneCommit, tuning)
	noErr(t, err, "opening the store whose table is the short one")
	batch := shortStore.NewBatch()
	noErr(t, batch.Set([]byte("a"), []byte("1")), "setting a")
	noErr(t, batch.Commit(true), "committing a")
	noErr(t, shortStore.Close(), "closing it, which flushes a table holding version 1")
	shortTable := tableFiles(t, oneCommit)[0]

	twoCommits := filepath.Join(t.TempDir(), "two")
	longStore, err := openVouchedTestStore(twoCommits, tuning)
	noErr(t, err, "opening the store whose log is the long one")
	for _, key := range []string{"a", "b"} {
		batch := longStore.NewBatch()
		noErr(t, batch.Set([]byte(key), []byte("1")), "setting "+key)
		noErr(t, batch.Commit(true), "committing "+key)
	}
	image := filepath.Join(t.TempDir(), "image")
	copyStoreFiles(t, twoCommits, image)
	noErr(t, longStore.Close(), "closing the long store")
	if tables, _ := filepath.Glob(filepath.Join(image, "*.sst")); len(tables) != 0 {
		t.Fatalf("SCENARIO NOT REACHED: the image already holds tables: %v", tables)
	}
	tableBytes, err := os.ReadFile(shortTable)
	noErr(t, err, "reading the short table")
	tableName := filepath.Base(shortTable)
	noErr(t, os.WriteFile(filepath.Join(image, tableName), tableBytes, 0o600), "writing the leftover table")

	assertRefusalWithoutAdvice(t, refusedByTheVouchedOpen(t, image), image, tableName)
}

// TestAVouchedStoreWithADamagedLeftoverThatIsNotItsFirstTableStaysRefused: a half-written table can
// be set aside only when it is 000001.sst, the table nothing was flushed before. Any other id has
// an older table behind it. Confirming every incomplete table (the mutation this kills) would
// move one of those aside.
func TestAVouchedStoreWithADamagedLeftoverThatIsNotItsFirstTableStaysRefused(t *testing.T) {
	dir, tableName := writeVouchedFirstFlushImage(t)
	const notFirst = "000002.sst"
	noErr(t, os.Rename(filepath.Join(dir, tableName), filepath.Join(dir, notFirst)), "renaming the leftover")
	noErr(t, os.Truncate(filepath.Join(dir, notFirst), 300), "cutting the leftover")
	assertRefusalWithoutAdvice(t, refusedByTheVouchedOpen(t, dir), dir, notFirst)
}

// TestAVouchedStoreCutAfterEarlierFlushesStaysRefused: the MANIFEST was cut to its header after a
// flush completed, so C1 durably removed log 1 and the log begins above version 1: the table holds
// data the log does not. Dropping the "logs begin at version 1" condition (the mutation the
// half-written row kills) would set that table aside and lose it.
func TestAVouchedStoreCutAfterEarlierFlushesStaysRefused(t *testing.T) {
	tuning := BadgerTuning{MemTableMB: 32}
	for _, table := range []struct {
		name   string
		damage func(t *testing.T, tablePath string)
	}{
		{name: "complete table", damage: func(*testing.T, string) {}},
		{name: "half-written table", damage: func(t *testing.T, tablePath string) { noErr(t, os.Truncate(tablePath, 300), "cutting the table") }},
	} {
		t.Run(table.name, func(t *testing.T) {
			running := filepath.Join(t.TempDir(), "running")
			kv, err := openVouchedTestStore(running, tuning)
			noErr(t, err, "opening the store")
			early := kv.NewBatch()
			noErr(t, early.Set([]byte("early"), []byte("in the table")), "setting the early key")
			noErr(t, early.Commit(true), "committing the early key")
			noErr(t, kv.Close(), "closing, which completes the first flush")

			kv, err = openVouchedTestStore(running, tuning)
			noErr(t, err, "reopening the store")
			later := kv.NewBatch()
			noErr(t, later.Set([]byte("later"), []byte("in the log")), "setting the later key")
			noErr(t, later.Commit(true), "committing the later key")
			dir := filepath.Join(t.TempDir(), "image")
			copyStoreFiles(t, running, dir)
			noErr(t, kv.Close(), "closing the running store")

			noErr(t, os.Truncate(filepath.Join(dir, "MANIFEST"), 8), "cutting the MANIFEST to its header")
			tables := tableFiles(t, dir)
			if len(tables) != 1 {
				t.Fatalf("SCENARIO NOT REACHED: the image holds %d tables, want one", len(tables))
			}
			table.damage(t, tables[0])
			assertRefusalWithoutAdvice(t, refusedByTheVouchedOpen(t, dir), dir, filepath.Base(tables[0]))
		})
	}
}
