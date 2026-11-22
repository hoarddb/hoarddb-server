package storage

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

// A store whose marker vouches nothing (version 0) holds no commit a reported sync covered: every
// commit is above the marker, the cut discards what the logs hold of them, and the node replays
// them from its own log from the marker's floor. A table the MANIFEST does not name is then set
// aside whatever it holds, which loses nothing, where the first-flush rule alone (the logs begin at
// version 1) cannot confirm it: a power cut during a first flush with nothing synced can leave the
// logs empty or gone, since nothing fsynced their bytes or their directory entries (review R-3,
// probe P3).

// driveFirstFlushUnsynced runs a vouched store through its first flush with unsynced commits only,
// copying the directory at each step of the flush (crashPoints), and returns the first flush's
// images.
func driveFirstFlushUnsynced(t *testing.T) []crashPointImage {
	t.Helper()
	t.Setenv("HOARDDB_EXPERIMENT_BADGER_NUM_LEVEL_ZERO_TABLES", "30")
	running := filepath.Join(t.TempDir(), "running")
	points := &crashPoints{dir: filepath.Clean(running), lastSync: map[string]prefixFile{}}
	restore := badger.SetFlushStepHook(points.hook)
	defer restore()
	kv, err := openVouchedTestStore(running, prefixTuning)
	noErr(t, err, "opening the store")
	// The directory as of the first start's marker: the last durable state before the flush.
	initial, err := capturePrefixDir(running)
	noErr(t, err)
	points.mu.Lock()
	points.lastSync = initial
	points.mu.Unlock()
	fill := prefixValue{seed: 9, size: 32 << 10}
	deadline := time.Now().Add(90 * time.Second)
	for fillCommit := 0; ; fillCommit++ {
		started, _, failure := points.state()
		noErr(t, failure)
		if started >= 1 {
			break
		}
		isTrue(t, time.Now().Before(deadline), "SCENARIO NOT REACHED: the first flush did not start in 90 s")
		batch := kv.NewBatch()
		for i := range 24 {
			noErr(t, batch.Set([]byte(fmt.Sprintf("fill/%d/%02d", fillCommit, i)), fill.bytes()), "setting a fill key")
		}
		noErr(t, batch.Commit(false), "committing fills, unsynced")
	}
	for {
		_, completed, failure := points.state()
		noErr(t, failure)
		if completed >= 1 {
			break
		}
		isTrue(t, time.Now().Before(deadline), "SCENARIO NOT REACHED: the first flush did not finish in 90 s")
		time.Sleep(5 * time.Millisecond)
	}
	noErr(t, kv.Close(), "closing the running store")
	points.mu.Lock()
	defer points.mu.Unlock()
	var images []crashPointImage
	for _, image := range points.images {
		if image.flush == 1 {
			images = append(images, image)
		}
	}
	return images
}

// TestAFirstFlushWithNothingVouchedOpensAfterACrash: [GUARD a vouched store whose marker vouches
// nothing opens at every step of its first flush, kill and power cut, setting aside any table its
// MANIFEST does not name; red when the power-cut image at "table synced" is refused as damage].
// The other row: the same image under a marker that vouches a commit stays refused, since there
// the logs alone must cover it and they do not.
func TestAFirstFlushWithNothingVouchedOpensAfterACrash(t *testing.T) {
	images := driveFirstFlushUnsynced(t)
	reached := map[string]bool{}
	for _, image := range images {
		name := fmt.Sprintf("%s/%s", image.step, image.kind)
		reached[name] = true
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "image")
			writePrefixImage(t, dir, image.files)
			kv, err := openVouchedTestStore(dir, prefixTuning)
			noErr(t, err, "the store as of this step of a first flush with nothing vouched did not open")
			noErr(t, kv.Close())
		})
	}
	powerCut := fmt.Sprintf("%s/power cut", badger.FlushTableSynced)
	isTrue(t, reached[powerCut], "SCENARIO NOT REACHED: no image at %q", powerCut)

	t.Run("the same image under a marker that vouches a commit stays refused", func(t *testing.T) {
		var image crashPointImage
		for _, candidate := range images {
			if candidate.step == badger.FlushTableSynced && candidate.kind == "power cut" {
				image = candidate
			}
		}
		dir := filepath.Join(t.TempDir(), "image")
		writePrefixImage(t, dir, image.files)
		noErr(t, writeVouchedMarker(dir, vouchedMarker{version: 1, floor: 1}))
		tables := tableFiles(t, dir)
		isTrue(t, len(tables) == 1, "SCENARIO NOT REACHED: the image holds %d tables, want the first flush's one", len(tables))
		assertRefusalWithoutAdvice(t, refusedByTheVouchedOpen(t, dir), dir, filepath.Base(tables[0]))
	})
}
