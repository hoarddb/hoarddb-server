package storage

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

// crashPointImage is the store directory at one step of a flush, as a crash there leaves it.
type crashPointImage struct {
	flush  int // 1 for the first flush of the store
	step   badger.FlushStep
	kind   string // "kill" or "power cut"
	files  map[string]prefixFile
	synced int // how many of the synced commits had completed
}

// crashPoints records the store at every step of every flush (badger.SetFlushStepHook). The hook
// runs on the flusher's goroutine and the flusher waits for it, so each copy is the directory at
// that instant: after the table's sync, after the MANIFEST names it, after the log's unlink,
// after the directory's fsync, after the table is in level 0.
type crashPoints struct {
	dir string

	mu         sync.Mutex
	images     []crashPointImage
	flushes    int
	completed  int
	synced     int
	lastSync   map[string]prefixFile // the directory right after the last completed sync
	atTableOut map[string]prefixFile // the directory at this flush's FlushTableSynced
	failure    error
}

func (c *crashPoints) hook(step badger.FlushStep, db *badger.DB) {
	if filepath.Clean(db.Opts().Dir) != c.dir {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	files, err := capturePrefixDir(c.dir)
	if err != nil && c.failure == nil {
		c.failure = fmt.Errorf("copying the store at the step %q: %w", step, err)
		return
	}
	switch step {
	case badger.FlushTableSynced:
		c.flushes++
		c.atTableOut = files
	case badger.FlushTableInLevelZero:
		c.completed++
	}
	c.images = append(c.images, crashPointImage{flush: c.flushes, step: step, kind: "kill", files: files, synced: c.synced})
	// A power cut: the logs hold what the last sync made durable, and an unlink the directory's
	// fsync has not yet covered is not there. A log created after the last sync keeps its bytes.
	cut := map[string]prefixFile{}
	for name, file := range files {
		cut[name] = file
		if synced, ok := c.lastSync[name]; ok && strings.HasSuffix(name, ".mem") {
			cut[name] = synced
		}
	}
	if step == badger.FlushLogUnlinked {
		for name, file := range c.atTableOut {
			if _, present := files[name]; !present && strings.HasSuffix(name, ".mem") {
				if synced, ok := c.lastSync[name]; ok {
					file = synced
				}
				cut[name] = file
			}
		}
	}
	c.images = append(c.images, crashPointImage{flush: c.flushes, step: step, kind: "power cut", files: cut, synced: c.synced})
}

func (c *crashPoints) state() (flushesStarted, flushesCompleted int, failure error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flushes, c.completed, c.failure
}

// TestNoStepOfAFlushLeavesAFirstLogBesideALaterTable guards the premise of BUG-0859's relaxation
// (storage/store_files_crash.go: confirmFirstFlushCrash accepts a leftover table at or above the
// oldest log's highest version): the state "a first log survived beside a later table with newer
// versions" must be unreachable through any order of a flush's steps. It runs three flushes of a
// vouched store, copies the directory at every step of each (kill and power-cut images), and opens
// every copy. Each must open and hold every commit a completed sync covered, and a leftover table
// may be set aside only in the first flush, before the MANIFEST names its table: there the log is
// the table's own memtable, and the commits only the table holds were never synced.
//
// Red when a flush unlinks its log before the MANIFEST names its table (the log is gone, the
// table is unnamed: the open refuses, or would set a table aside whose commits only it holds).
//
// What it does not guard: the order of the table's entry into level 0 after the log's retirement.
// It disables compactions and copies files, so an in-memory insert order is invisible to it (review
// R-7: with the insert moved before the retirement it passes). That order is guarded by C1's tests,
// TestAFlushedLogIsGoneBeforeItsTableIsVisible and TestTheRemovalIsDurableBeforeTheTableIsInALevel
// (third_party/badger/flushed_log_retire_test.go), both red under that mutation.
func TestNoStepOfAFlushLeavesAFirstLogBesideALaterTable(t *testing.T) {
	runFlushCrashPoints(t, false)
}

// TestNoStepOfAnEncryptedFlushLeavesAFirstLogBesideALaterTableOrRepairsIt: [GUARD the same three
// flushes on a store sealed under the test key, every step's kill and power-cut image opened with
// the key: the images of the first flush at the step "table synced" (the only ones the plaintext
// guard repairs) are refused with the first-flush refusal and nothing is set aside, and every
// other image opens holding every synced commit; red when the key is not passed through the
// vouched open: the store is then plaintext, those images are repaired and set-aside images
// appear]. Ring encryption at rest spec §7 and §9.1 step 5.
func TestNoStepOfAnEncryptedFlushLeavesAFirstLogBesideALaterTableOrRepairsIt(t *testing.T) {
	withVouchedTestKey(t)
	runFlushCrashPoints(t, true)
}

// runFlushCrashPoints drives three flushes of a vouched store, opened through openVouchedTestStore,
// copies the directory at every step of each, and opens every copy. encrypted says the store is
// sealed (withVouchedTestKey): its first-flush images are refused, not repaired.
func runFlushCrashPoints(t *testing.T, encrypted bool) {
	// No compaction while the images are copied: a compaction deletes and writes tables, and the
	// store's own setting of two level-0 tables starts one from the second flush on.
	t.Setenv("HOARDDB_EXPERIMENT_BADGER_NUM_LEVEL_ZERO_TABLES", "30")
	running := filepath.Join(t.TempDir(), "running")
	points := &crashPoints{dir: filepath.Clean(running), lastSync: map[string]prefixFile{}}
	restore := badger.SetFlushStepHook(points.hook)
	kv, err := openVouchedTestStore(running, prefixTuning)
	noErr(t, err, "opening the store")
	closed := false
	defer func() {
		restore()
		if !closed {
			_ = kv.Close() // a failed test's close
		}
	}()

	syncedValue := func(index int) []byte { return []byte(fmt.Sprintf("synced value %d", index)) }
	commitSynced := func() {
		t.Helper()
		batch := kv.NewBatch()
		noErr(t, batch.Set([]byte(fmt.Sprintf("synced/%d", points.synced)), syncedValue(points.synced)), "setting a synced key")
		noErr(t, batch.Commit(true), "committing synced")
		after, err := capturePrefixDir(running)
		noErr(t, err, "copying the store after a sync")
		points.mu.Lock()
		points.synced++
		points.lastSync = after
		points.mu.Unlock()
	}
	commitSynced()

	const flushesWanted = 3
	fill := prefixValue{seed: 7, size: 32 << 10}
	deadline := time.Now().Add(90 * time.Second)
	for flush := 1; flush <= flushesWanted; flush++ {
		// Unsynced fills until the memtable rotates and the flusher takes it.
		for fillCommit := 0; ; fillCommit++ {
			started, _, failure := points.state()
			noErr(t, failure)
			if started >= flush {
				break
			}
			isTrue(t, time.Now().Before(deadline), "SCENARIO NOT REACHED: flush %d did not start in 90 s", flush)
			batch := kv.NewBatch()
			for i := range 24 {
				noErr(t, batch.Set([]byte(fmt.Sprintf("fill/%d/%02d", fillCommit, i)), fill.bytes()), "setting a fill key")
			}
			noErr(t, batch.Commit(false), "committing fills")
		}
		for {
			_, completed, failure := points.state()
			noErr(t, failure)
			if completed >= flush {
				break
			}
			isTrue(t, time.Now().Before(deadline), "SCENARIO NOT REACHED: flush %d did not finish in 90 s", flush)
			time.Sleep(5 * time.Millisecond)
		}
		commitSynced()
	}
	noErr(t, kv.Close(), "closing the running store")
	closed = true
	restore()

	// Closing flushed the active memtable once more; only the flushes the test drove are judged.
	var images []crashPointImage
	points.mu.Lock()
	for _, image := range points.images {
		if image.flush <= flushesWanted {
			images = append(images, image)
		}
	}
	points.mu.Unlock()
	steps := map[badger.FlushStep]int{}
	for _, image := range images {
		steps[image.step]++
	}
	for _, step := range []badger.FlushStep{badger.FlushTableSynced, badger.FlushTableNamed, badger.FlushLogUnlinked,
		badger.FlushDirectorySynced, badger.FlushTableInLevelZero} {
		isTrue(t, steps[step] >= 2*flushesWanted, "SCENARIO NOT REACHED: %d images at the step %q, want kill and power cut of each of %d flushes",
			steps[step], step, flushesWanted)
	}

	if encrypted {
		requireSealedStore(t, running)
	}
	setAsideImages, refusedImages := 0, 0
	for _, image := range images {
		name := fmt.Sprintf("flush %d/%s/%s", image.flush, image.step, image.kind)
		t.Run(name, func(t *testing.T) {
			imageDir := filepath.Join(t.TempDir(), "image")
			writePrefixImage(t, imageDir, image.files)
			reopened, err := openVouchedTestStore(imageDir, prefixTuning)
			if encrypted && image.flush == 1 && image.step == badger.FlushTableSynced {
				// The first flush's table, unnamed, beside its own log: the state the plaintext
				// guard confirms and repairs. Sealed, confirmFirstFlushCrash cannot read it.
				if err == nil {
					_ = reopened.Close()
					t.Fatal("the sealed store as of the first flush's table sync opened: the guard repaired a table it cannot read")
				}
				isTrue(t, isFirstFlushRefusal(err), "the sealed first-flush image was refused, but not with the first-flush refusal: %v", err)
				setAside, globErr := filepath.Glob(filepath.Join(imageDir, "*"+firstFlushQuarantineSuffix+"*"))
				noErr(t, globErr)
				isTrue(t, len(setAside) == 0, "the refused open set files aside: %v", setAside)
				refusedImages++
				return
			}
			noErr(t, err, "the store as of this step of the flush did not open")
			defer func() { _ = reopened.Close() }() // a test's close; the assertions are the result
			snapshot := reopened.NewSnapshot()
			defer snapshot.Discard()
			for index := range image.synced {
				got, err := snapshot.Get([]byte(fmt.Sprintf("synced/%d", index)))
				noErr(t, err, "the commit synced/%d, which a completed sync covered, is gone after the reopen", index)
				equal(t, string(syncedValue(index)), string(got), "the value of synced/%d", index)
			}
			setAside, err := filepath.Glob(filepath.Join(imageDir, "*"+firstFlushQuarantineSuffix+"*"))
			noErr(t, err)
			if len(setAside) > 0 {
				setAsideImages++
				isTrue(t, image.flush == 1 && image.step == badger.FlushTableSynced,
					"the open set a table aside (%v) in an image of flush %d at the step %q: only the first flush, "+
						"before the MANIFEST names its table, leaves a first log beside an unnamed table",
					setAside, image.flush, image.step)
			}
		})
	}
	if encrypted {
		// Sealed, nothing is ever set aside, and the state the plaintext guard repairs must have
		// been built and refused, or the loop above proves nothing about it.
		isTrue(t, setAsideImages == 0, "%d images of the sealed store had a table set aside", setAsideImages)
		isTrue(t, refusedImages >= 2, "SCENARIO NOT REACHED: %d first-flush images were refused, want the first "+
			"flush's kill and power-cut images", refusedImages)
		return
	}
	// The one state the guard may accept must have been built, or the loop above proves nothing
	// about the relaxation: the first flush's table, unnamed, beside its log (whole and cut).
	isTrue(t, setAsideImages >= 2, "SCENARIO NOT REACHED: %d images had a leftover table set aside, want the first "+
		"flush's kill and power-cut images", setAsideImages)
}
