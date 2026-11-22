package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestAVouchedStoreCutDuringItsFirstFlushReopens is the deterministic form of a failure T10
// (TestStoreReopensAsAPrefix) hits about once in 150 rounds on a host with slow fsyncs: a power
// cut during the very first flush, after the table was written and before its MANIFEST record,
// with the tail of the memtable log not yet written back. The marker vouches the first commit,
// the log holds it, and the table (which the MANIFEST does not name) holds one commit more.
//
// FINDING (BUG-0827, vouched cut rerun of 2026-11-18): the open is refused. guardStoreFiles
// confirms a first-flush crash only when the leftover table's highest version equals the oldest
// log's (store_files_crash.go: confirmFirstFlushCrash, its recorded "shortcut: a log cut short
// by the same kill ... the state is refused, the safe direction"). That guard predates the cut
// and refuses this state with or without a marker; nothing is lost or changed, but the node does
// not start, and the vouched cut's contract is that such a store reopens holding exactly the
// commits through max(V, T). Committed failing; the fix is a design decision for the owner.
func TestAVouchedStoreCutDuringItsFirstFlushReopens(t *testing.T) {
	image, _, _ := buildFirstFlushCutImage(t)
	tuning := firstFlushCutTuning
	reopened, err := openVouchedTestStore(image, tuning)
	noErr(t, err, "a vouched store cut during its first flush, its log's tail not written back, did not reopen")
	defer func() { _ = reopened.Close() }() // a test's close; the assertions below are the result

	snapshot := reopened.NewSnapshot()
	defer snapshot.Discard()
	kept, err := snapshot.Get([]byte("vouched"))
	noErr(t, err, "the vouched commit is gone after the reopen")
	equal(t, "kept", string(kept), "the vouched commit's value")
	if _, err := snapshot.Get([]byte("unsynced")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("the commit above the marker, which only the table the MANIFEST does not name holds, is visible after the reopen (read error: %v)", err)
	}
}

// firstFlushCutTuning keeps the first flush's memtable large enough that the image is taken before it.
var firstFlushCutTuning = BadgerTuning{MemTableMB: 32}

// buildFirstFlushCutImage builds the store directory a power cut leaves during the very first
// flush, after the table was written and before its MANIFEST record, with the tail of the memtable
// log not yet written back: the marker vouches the first commit, the log holds it, and the table
// (which the MANIFEST does not name) holds one commit more. It opens the store through
// openVouchedTestStore, so withVouchedTestKey builds the sealed form of the same image. It returns
// the image directory, the leftover table's path in it and that table's bytes.
func buildFirstFlushCutImage(t *testing.T) (image, leftover string, tableBytes []byte) {
	t.Helper()
	tuning := firstFlushCutTuning
	running := filepath.Join(t.TempDir(), "running")
	kv, err := openVouchedTestStore(running, tuning)
	noErr(t, err, "opening the store")

	vouched := kv.NewBatch()
	noErr(t, vouched.Set([]byte("vouched"), []byte("kept")), "setting the vouched key")
	noErr(t, vouched.Commit(true), "committing the vouched key, synced")

	// The image as of that sync: the marker, a MANIFEST that names no table, and the memtable log
	// holding the vouched commit and nothing later.
	image = filepath.Join(t.TempDir(), "image")
	copyStoreFiles(t, running, image)
	if tables, _ := filepath.Glob(filepath.Join(image, "*.sst")); len(tables) != 0 {
		t.Fatalf("SCENARIO NOT REACHED: the store flushed before the image was taken: %v", tables)
	}
	if _, err := os.Stat(filepath.Join(image, vouchedFileName)); err != nil {
		t.Fatalf("SCENARIO NOT REACHED: the image has no sync marker: %v", err)
	}

	// One more commit, not synced: its log pages never reach the disk before the power cut.
	unsynced := kv.NewBatch()
	noErr(t, unsynced.Set([]byte("unsynced"), []byte("above the marker")), "setting the unsynced key")
	noErr(t, unsynced.Commit(false), "committing the unsynced key")

	// Close flushes the memtable: the table the first flush wrote, holding both commits. In the
	// image it is the file a flush leaves when the power goes before its MANIFEST record.
	noErr(t, kv.Close(), "closing the running store")
	tables := tableFiles(t, running)
	if len(tables) != 1 {
		t.Fatalf("SCENARIO NOT REACHED: the close left %d tables, want the one of the first flush", len(tables))
	}
	tableBytes, err = os.ReadFile(tables[0])
	noErr(t, err, "reading the flushed table")
	leftover = filepath.Join(image, filepath.Base(tables[0]))
	noErr(t, os.WriteFile(leftover, tableBytes, 0o600), "writing the leftover table")
	return image, leftover, tableBytes
}
