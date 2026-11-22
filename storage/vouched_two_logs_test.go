package storage

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

// TestACutThatReachesThreeLogsKeepsOnlyTheVouchedCommit builds on purpose the case T10
// (TestStoreReopensAsAPrefix) reaches in 0 to 5 rounds of 200 and so cannot gate on (review R1 of
// C1): the kill leaves several memtable logs whose commits are all above the marker, so the cut
// shortens the first and removes the later ones. The flusher is held in front of its first
// flush, so the rotated memtables wait in db.imm and their logs stay on disk, the only copy of
// their commits; the image is the directory as of the kill. The reopened store holds the vouched
// commit and nothing from the logs above it, and the report says the cut reached two logs or
// more (the counters T10 reads).
func TestACutThatReachesThreeLogsKeepsOnlyTheVouchedCommit(t *testing.T) {
	hold := &flushHold{}
	defer badger.SetFlushTestHook(hold.hook)()
	running := filepath.Join(t.TempDir(), "running")
	kv, err := openVouchedTestStore(running, prefixTuning)
	noErr(t, err, "opening the store")
	defer func() {
		hold.letGo()
		_ = kv.Close() // a failed test's close
	}()

	vouched := kv.NewBatch()
	noErr(t, vouched.Set([]byte("vouched"), []byte("kept")), "setting the vouched key")
	noErr(t, vouched.Commit(true), "committing the vouched key, synced")

	hold.holdAt(running)
	logCount := func() int {
		logs, err := filepath.Glob(filepath.Join(running, "*.mem"))
		noErr(t, err)
		return len(logs)
	}
	fill := prefixValue{seed: 11, size: 32 << 10}
	deadline := time.Now().Add(60 * time.Second)
	for fillCommit := 0; logCount() < 3; fillCommit++ {
		isTrue(t, time.Now().Before(deadline), "SCENARIO NOT REACHED: %d memtable logs after 60 s of fills, want 3", logCount())
		batch := kv.NewBatch()
		for i := range 24 {
			noErr(t, batch.Set([]byte(fmt.Sprintf("above/%d/%02d", fillCommit, i)), fill.bytes()), "setting a fill key")
		}
		noErr(t, batch.Commit(false), "committing fills")
	}
	if tables, _ := filepath.Glob(filepath.Join(running, "*.sst")); len(tables) != 0 {
		t.Fatalf("SCENARIO NOT REACHED: a table was flushed in spite of the hold: %v", tables)
	}
	image, err := capturePrefixDir(running)
	noErr(t, err, "copying the store as of the kill")
	hold.letGo()
	noErr(t, kv.Close(), "closing the running store")

	imageDir := filepath.Join(t.TempDir(), "image")
	writePrefixImage(t, imageDir, image)
	reopened, err := openVouchedTestStore(imageDir, prefixTuning)
	noErr(t, err, "the image did not open")
	defer func() { _ = reopened.Close() }() // a test's close; the assertions below are the result

	report := reopened.VouchedOpenReport()
	isTrue(t, report.LogsTruncated+report.LogsRemoved >= 2,
		"the cut reached %d logs (truncated %d, removed %d), want two or more", report.LogsTruncated+report.LogsRemoved,
		report.LogsTruncated, report.LogsRemoved)
	snapshot := reopened.NewSnapshot()
	defer snapshot.Discard()
	kept, err := snapshot.Get([]byte("vouched"))
	noErr(t, err, "the vouched commit is gone after the cut")
	equal(t, "kept", string(kept), "the vouched value")
	for _, probe := range []string{"above/0/00", "above/0/23"} {
		if _, err := snapshot.Get([]byte(probe)); err == nil {
			t.Fatalf("%s, committed above the marker, is visible after the cut", probe)
		}
	}
}
