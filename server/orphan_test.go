package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ids"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/store"
)

// Build doc Step 17: the orphan pass, the reclaimer's second work source.
// T-07's first clause, T-19 base, (a) and (b), and T-35. Every test runs on a
// server with no reclaim goroutine and drives the pass tick by tick.

const (
	orphanTestDatabase = "app"
	// liveFixtureBucket is the bucket of the live-file fixture. Its real slot
	// is non-zero, which every test asserts rather than forces: T-19 restarts,
	// and at restart the attach assertion refuses a forced slot (Part A).
	liveFixtureBucket = "keep"
)

// liveFixture is a live blob of 64 KiB or more in a bucket whose recorded slot
// is its real, non-zero SlotOf, with its payload file's mtime set past the
// grace age: an orphan pass that reads its metadata with any slot but the
// parsed one misses, and unlinks it.
type liveFixture struct {
	slot       uint16
	databaseID uint64
	id         uint64
	content    []byte
	path       string
	leaf       string
}

func newLiveFixture(t *testing.T, s *Server) liveFixture {
	t.Helper()
	slot := storage.SlotOf(storage.PlacementKey(orphanTestDatabase, liveFixtureBucket, nil))
	if slot == 0 {
		t.Fatalf("precondition: SlotOf(%q, %q) is 0; the fixture needs a bucket in a non-zero slot", orphanTestDatabase, liveFixtureBucket)
	}
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: orphanTestDatabase, Name: liveFixtureBucket, Type: BucketTypeBlob}); err != nil || !resp.Success {
		t.Fatalf("create %s: %+v, %v", liveFixtureBucket, resp, err)
	}
	bucket := lookupBucket(s, orphanTestDatabase, liveFixtureBucket)
	if bucket.slot != slot {
		t.Fatalf("precondition: bucket %s recorded slot %d, its SlotOf is %d", liveFixtureBucket, bucket.slot, slot)
	}
	content := make([]byte, store.InlineThreshold+123)
	for i := range content {
		content[i] = byte(i * 7)
	}
	if resp, err := s.BlobWrite(BlobWriteRequest{Database: orphanTestDatabase, Bucket: liveFixtureBucket, Key: "live", Data: content}); err != nil || !resp.Success {
		t.Fatalf("blob write %s/live: %+v, %v", liveFixtureBucket, resp, err)
	}
	files := payloadFilesOf(t, s.blobRoot(), bucket.id.Load())
	if len(files) != 1 {
		t.Fatalf("precondition: the fixture has %d payload files, want 1", len(files))
	}
	ageFile(t, files[0])
	return liveFixture{slot: slot, databaseID: bucket.databaseID, id: bucket.id.Load(), content: content, path: files[0], leaf: leafOf(t, s, files[0])}
}

// check fails the test unless the fixture's file is still on disk and its blob
// is served byte-identical.
func (f liveFixture) check(t *testing.T, s *Server) {
	t.Helper()
	if _, err := os.Stat(f.path); err != nil {
		t.Fatalf("the live blob's payload file in slot %d is gone after the orphan pass: %v", f.slot, err)
	}
	read, err := s.BlobRead(BlobReadRequest{Database: orphanTestDatabase, Bucket: liveFixtureBucket, Key: "live"})
	if err != nil || !read.Found || !bytes.Equal(read.Data, f.content) {
		t.Fatalf("the live blob in slot %d is not served byte-identical after the orphan pass: %s", f.slot, describeBlobRead(read, err))
	}
}

// describeBlobRead says what a blob read returned, for a failure message.
func describeBlobRead(read *BlobReadResponse, err error) string {
	if err != nil {
		return "error: " + err.Error()
	}
	return fmt.Sprintf("found %v, %d bytes", read.Found, len(read.Data))
}

// ageFile sets a file's mtime past the orphan pass's grace age.
func ageFile(t *testing.T, path string) {
	t.Helper()
	old := time.Now().Add(-2 * orphanGraceAge)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatalf("setting the mtime of %s back: %v", path, err)
	}
}

// leafOf is the leaf of the blob tree a payload path lies in, as the orphan
// cursor records it.
func leafOf(t *testing.T, s *Server, path string) string {
	t.Helper()
	rel, err := filepath.Rel(s.blobRoot(), filepath.Dir(path))
	if err != nil {
		t.Fatalf("locating %s under the blob root: %v", path, err)
	}
	return filepath.ToSlash(rel)
}

// plantFile writes a file named name into leaf, aged past the grace age when
// aged is set, and returns its path.
func plantFile(t *testing.T, s *Server, leaf, name string, aged bool) string {
	t.Helper()
	path := filepath.Join(s.blobRoot(), filepath.FromSlash(leaf), name)
	if err := os.WriteFile(path, []byte("planted"), 0o644); err != nil {
		t.Fatalf("planting %s: %v", path, err)
	}
	if aged {
		ageFile(t, path)
	}
	return path
}

// orphanName is a payload name for bucket id in slot whose blob has no
// metadata: an orphan of that bucket.
func orphanName(t *testing.T, slot uint16, id uint64) string {
	t.Helper()
	blobID, err := ids.NewDocumentID()
	if err != nil {
		t.Fatalf("minting a blob id: %v", err)
	}
	return store.PayloadFileName(slot, id, blobID)
}

// setOrphanCursor points the orphan pass at the start of leaf, after last.
func setOrphanCursor(t *testing.T, kv storage.KV, leaf, last string) {
	t.Helper()
	encoded, err := bson.Marshal(orphanCursor{Leaf: leaf, LastName: last})
	if err != nil {
		t.Fatalf("encoding the orphan cursor: %v", err)
	}
	batch := kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(reclaimKey(storage.OrphanCursorID), encoded); err != nil {
		t.Fatalf("setting the orphan cursor: %v", err)
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("committing the orphan cursor: %v", err)
	}
}

func readOrphanCursorOrFail(t *testing.T, kv storage.KV) orphanCursor {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	encoded, err := snap.Get(reclaimKey(storage.OrphanCursorID))
	if err != nil {
		t.Fatalf("reading the orphan cursor: %v", err)
	}
	var cursor orphanCursor
	if err := bson.Unmarshal(encoded, &cursor); err != nil {
		t.Fatalf("decoding the orphan cursor: %v", err)
	}
	return cursor
}

// orphanWatch records what the orphan pass reads and examines.
type orphanWatch struct {
	mu       sync.Mutex
	readDirs []string
	examined []string
}

// watchOrphanPass watches s's own orphan pass only: the hooks are
// still process-global (server/reclaim.go's orphanReadDirHook and
// orphanExamineHook fire for every Server's pass in the test binary), so the
// watch filters on s's blob root instead of recording everything it sees.
// The root, not s itself, is the identity that must survive a restart:
// TestOrphanPassIsBudgetedAndResumable reassigns s to a new *Server over the
// same data directory mid-test and expects the watch to keep observing it.
func watchOrphanPass(t *testing.T, s *Server) *orphanWatch {
	t.Helper()
	root := s.blobRoot()
	watch := &orphanWatch{}
	readDir := func(observedRoot, leaf string) {
		if observedRoot != root {
			return
		}
		watch.mu.Lock()
		defer watch.mu.Unlock()
		watch.readDirs = append(watch.readDirs, leaf)
	}
	examine := func(observedRoot, _, name string) {
		if observedRoot != root {
			return
		}
		watch.mu.Lock()
		defer watch.mu.Unlock()
		watch.examined = append(watch.examined, name)
	}
	orphanReadDirHook.Store(&readDir)
	orphanExamineHook.Store(&examine)
	t.Cleanup(func() {
		orphanReadDirHook.Store(nil)
		orphanExamineHook.Store(nil)
	})
	return watch
}

// take returns what was recorded since the last take, and forgets it.
func (w *orphanWatch) take() (readDirs, examined []string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	readDirs, examined = w.readDirs, w.examined
	w.readDirs, w.examined = nil, nil
	return readDirs, examined
}

func orphanTickOrFail(t *testing.T, s *Server) {
	t.Helper()
	if err := newReclaimer(s).orphanTick(context.Background()); err != nil {
		t.Fatalf("orphan tick: %v", err)
	}
}

func fileExists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat %s: %v", path, err)
	}
	return err == nil
}

func restartKVFaultServer(t *testing.T, dataDir string, kv storage.KV) *Server {
	t.Helper()
	restarted := newKVFaultServer(dataDir, kv)
	if err := restarted.LoadCatalog(); err != nil {
		t.Fatalf("LoadCatalog after the restart: %v", err)
	}
	return restarted
}

// TestOrphanPassIsBudgetedAndResumable is T-19 base and (a): a tick examines
// orphanTickFiles names and no more, reads one leaf directory, and a restart
// mid-leaf resumes after the recorded name — re-examining nothing at or before
// it and skipping nothing after. A lap runs one tick in orphanLapRatio.
func TestOrphanPassIsBudgetedAndResumable(t *testing.T) {
	useReclaimClock(t, newFakeReclaimClock())
	dataDir := t.TempDir()
	kv := kvfault.New()
	s := newKVFaultServer(dataDir, kv)
	fixture := newLiveFixture(t, s)

	var orphans []string
	for i := 0; i < 2*orphanTickFiles+5; i++ {
		orphans = append(orphans, plantFile(t, s, fixture.leaf, orphanName(t, fixture.slot, fixture.id), true))
	}
	young := plantFile(t, s, fixture.leaf, orphanName(t, fixture.slot, fixture.id), false)
	foreign := plantFile(t, s, fixture.leaf, "notes.txt", true)
	entries, err := os.ReadDir(filepath.Join(s.blobRoot(), filepath.FromSlash(fixture.leaf)))
	if err != nil {
		t.Fatalf("reading the leaf: %v", err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)

	watch := watchOrphanPass(t, s)
	setOrphanCursor(t, kv, fixture.leaf, "")
	orphanTickOrFail(t, s)
	readDirs, examined := watch.take()
	if len(readDirs) != 1 || readDirs[0] != fixture.leaf {
		t.Fatalf("one tick read the directories %v, want exactly the leaf %s", readDirs, fixture.leaf)
	}
	if len(examined) != orphanTickFiles {
		t.Fatalf("one tick examined %d names, want the budget of %d", len(examined), orphanTickFiles)
	}
	cursor := readOrphanCursorOrFail(t, kv)
	if cursor.Leaf != fixture.leaf || cursor.LastName != names[orphanTickFiles-1] {
		t.Fatalf("after one tick the cursor is (%s, %s), want (%s, %s)", cursor.Leaf, cursor.LastName, fixture.leaf, names[orphanTickFiles-1])
	}

	// The restart: a new server over the same store and directory resumes
	// from the persisted (leaf, name), not from any state of the first.
	s = restartKVFaultServer(t, dataDir, kv)
	orphanTickOrFail(t, s)
	readDirs, examined = watch.take()
	if len(readDirs) != 1 {
		t.Fatalf("the resumed tick read %d directories, want 1", len(readDirs))
	}
	if want := names[orphanTickFiles : 2*orphanTickFiles]; !equalStrings(examined, want) {
		t.Fatalf("the resumed tick examined %d names from %q; want exactly the %d after the cursor, from %q", len(examined), firstOf(examined), len(want), want[0])
	}

	orphanTickOrFail(t, s)
	_, examined = watch.take()
	if want := names[2*orphanTickFiles:]; !equalStrings(examined, want) {
		t.Fatalf("the last tick of the leaf examined %d names, want the %d left", len(examined), len(want))
	}
	cursor = readOrphanCursorOrFail(t, kv)
	if cursor.Leaf == fixture.leaf || cursor.LastName != "" {
		t.Fatalf("after the leaf's last name the cursor is (%s, %q), want the next leaf from the empty name", cursor.Leaf, cursor.LastName)
	}

	for _, orphan := range orphans {
		if fileExists(t, orphan) {
			t.Fatalf("the orphan %s, past the grace age, survived the pass over its leaf", orphan)
		}
	}
	if !fileExists(t, young) {
		t.Fatal("an orphan younger than the grace age was unlinked")
	}
	if !fileExists(t, foreign) {
		t.Fatal("a file whose name does not parse was unlinked")
	}
	fixture.check(t, s)

	// One tick in orphanLapRatio laps, charged to the same loop.
	r := newReclaimer(s)
	const laps = 2 * orphanLapRatio
	for lap := 0; lap < laps; lap++ {
		if _, err := r.lap(context.Background()); err != nil {
			t.Fatalf("lap %d: %v", lap, err)
		}
	}
	if readDirs, _ = watch.take(); len(readDirs) != laps/orphanLapRatio {
		t.Fatalf("%d laps ran %d orphan ticks, want %d", laps, len(readDirs), laps/orphanLapRatio)
	}
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func firstOf(names []string) string {
	if len(names) == 0 {
		return ""
	}
	return names[0]
}

// TestOrphanPassSparesALivePut is T-19(b): a put whose metadata commit is
// held back across a tick keeps its file — younger than the grace age — and
// is served once the commit lands; an orphan past the grace age in the same
// leaf is unlinked by that tick.
func TestOrphanPassSparesALivePut(t *testing.T) {
	useReclaimClock(t, newFakeReclaimClock())
	dataDir := t.TempDir()
	kv := kvfault.New()
	s := newKVFaultServer(dataDir, kv)
	fixture := newLiveFixture(t, s)

	atCommit, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	hold := func(_ storage.KV, _ uint16, id uint64, _ uint64) error {
		if id == fixture.id {
			once.Do(func() { close(atCommit) })
			<-release
		}
		return nil
	}
	store.ApplyFaultHook.Store(&hold)
	t.Cleanup(func() { store.ApplyFaultHook.Store(nil) })

	content := bytes.Repeat([]byte("incoming"), store.InlineThreshold/8+1)
	putDone := make(chan error, 1)
	go func() {
		resp, err := s.BlobWrite(BlobWriteRequest{Database: orphanTestDatabase, Bucket: liveFixtureBucket, Key: "incoming", Data: content})
		if err == nil && !resp.Success {
			err = errors.New(resp.Message)
		}
		putDone <- err
	}()
	waitFor(t, atCommit, "the put to reach its metadata commit")

	var putPath string
	for _, path := range payloadFilesOf(t, s.blobRoot(), fixture.id) {
		if path != fixture.path {
			putPath = path
		}
	}
	if putPath == "" {
		close(release)
		t.Fatal("the held put's payload file is not on disk before its commit")
	}
	putLeaf := leafOf(t, s, putPath)
	orphan := plantFile(t, s, putLeaf, orphanName(t, fixture.slot, fixture.id), true)

	setOrphanCursor(t, kv, putLeaf, "")
	orphanTickOrFail(t, s)
	setOrphanCursor(t, kv, fixture.leaf, "")
	orphanTickOrFail(t, s)

	close(release)
	if err := <-putDone; err != nil {
		t.Fatalf("the held put failed once released: %v", err)
	}
	store.ApplyFaultHook.Store(nil)
	read, err := s.BlobRead(BlobReadRequest{Database: orphanTestDatabase, Bucket: liveFixtureBucket, Key: "incoming"})
	if err != nil || !read.Found || !bytes.Equal(read.Data, content) {
		t.Fatalf("the put held across an orphan tick is not served after its commit: %s", describeBlobRead(read, err))
	}
	if fileExists(t, orphan) {
		t.Fatal("an orphan past the grace age survived the tick over its leaf")
	}
	fixture.check(t, s)
}

// TestCrashBeforeMetadataLeavesAnOrphan is T-07's first clause, with SA-25's
// temporary: a put that stops between its rename and its metadata commit
// leaves a file no metadata names; after a restart the key is not found, and
// the pass removes the file once it is past the grace age and not before. A
// temporary a crash before the rename left is collected past the grace age
// too, with no metadata read, and spared while younger.
func TestCrashBeforeMetadataLeavesAnOrphan(t *testing.T) {
	useReclaimClock(t, newFakeReclaimClock())
	dataDir := t.TempDir()
	kv := kvfault.New()
	s := newKVFaultServer(dataDir, kv)
	fixture := newLiveFixture(t, s)

	errCrash := errors.New("simulated crash between the rename and the metadata commit")
	crash := func(_ storage.KV, _ uint16, id uint64, _ uint64) error {
		if id == fixture.id {
			return errCrash
		}
		return nil
	}
	store.ApplyFaultHook.Store(&crash)
	t.Cleanup(func() { store.ApplyFaultHook.Store(nil) })
	content := bytes.Repeat([]byte{'c'}, store.InlineThreshold+1)
	if resp, err := s.BlobWrite(BlobWriteRequest{Database: orphanTestDatabase, Bucket: liveFixtureBucket, Key: "crashed", Data: content}); err == nil && resp.Success {
		t.Fatal("the put succeeded through a failed commit")
	}
	store.ApplyFaultHook.Store(nil)

	s = restartKVFaultServer(t, dataDir, kv)
	if read, err := s.BlobRead(BlobReadRequest{Database: orphanTestDatabase, Bucket: liveFixtureBucket, Key: "crashed"}); err != nil || read.Found {
		t.Fatalf("after the restart the crashed put should be not-found: %s", describeBlobRead(read, err))
	}
	var orphan string
	for _, path := range payloadFilesOf(t, s.blobRoot(), fixture.id) {
		if path != fixture.path {
			orphan = path
		}
	}
	if orphan == "" {
		t.Fatal("the crashed put left no payload file: there is no orphan to collect")
	}
	leaf := leafOf(t, s, orphan)
	oldTemporary := plantFile(t, s, leaf, orphanName(t, fixture.slot, fixture.id)+".tmp", true)
	youngTemporary := plantFile(t, s, leaf, orphanName(t, fixture.slot, fixture.id)+".tmp", false)

	setOrphanCursor(t, kv, leaf, "")
	orphanTickOrFail(t, s)
	if !fileExists(t, orphan) {
		t.Fatal("the orphan was unlinked while younger than the grace age")
	}
	if fileExists(t, oldTemporary) {
		t.Fatal("a temporary past the grace age survived the tick over its leaf")
	}
	if !fileExists(t, youngTemporary) {
		t.Fatal("a temporary younger than the grace age was unlinked")
	}

	ageFile(t, orphan)
	setOrphanCursor(t, kv, leaf, "")
	orphanTickOrFail(t, s)
	if fileExists(t, orphan) {
		t.Fatal("the crashed put's orphan, past the grace age, survived the tick over its leaf")
	}
	setOrphanCursor(t, kv, fixture.leaf, "")
	orphanTickOrFail(t, s)
	fixture.check(t, s)
}

// TestOrphanWatchIgnoresAnotherServersPass: the orphan pass's
// watch records only the fixture Server's own tick, so a second Server
// running its own orphan pass in the same test binary cannot move the
// fixture's counts. Deterministic by construction — the foreign tick runs
// synchronously inside the watched window, not from a leaked goroutine under
// load.
func TestOrphanWatchIgnoresAnotherServersPass(t *testing.T) {
	useReclaimClock(t, newFakeReclaimClock())

	dataDir := t.TempDir()
	kv := kvfault.New()
	s := newKVFaultServer(dataDir, kv)
	fixture := newLiveFixture(t, s)

	otherDataDir := t.TempDir()
	otherKV := kvfault.New()
	other := newKVFaultServer(otherDataDir, otherKV)
	otherFixture := newLiveFixture(t, other)

	watch := watchOrphanPass(t, s)
	setOrphanCursor(t, kv, fixture.leaf, "")
	setOrphanCursor(t, otherKV, otherFixture.leaf, "")

	// other's own orphan tick, mid-window: every orphan pass in
	// the process writes into the same watch, so this tick's directory and
	// file land in the fixture's counts even though nothing here watches
	// other.
	orphanTickOrFail(t, other)
	orphanTickOrFail(t, s)

	readDirs, examined := watch.take()
	if len(readDirs) != 1 || readDirs[0] != fixture.leaf {
		t.Fatalf("the fixture's watch read the directories %v, want exactly its own leaf %s — another Server's orphan tick was recorded", readDirs, fixture.leaf)
	}
	if len(examined) != 1 {
		t.Fatalf("the fixture's watch examined %v, want exactly 1 name — another Server's orphan tick was recorded", examined)
	}
	fixture.check(t, s)
	otherFixture.check(t, other)
}

// TestLiveBlobInNonZeroSlotSurvivesOrphanPass is T-35: the orphan read builds
// the metadata key from the slot parsed out of the file name. A live blob in a
// bucket whose real slot is non-zero, its file past the grace age, survives a
// tick and is served byte-identical; a file in the same leaf whose name does
// not parse, also past the grace age, is left in place.
func TestLiveBlobInNonZeroSlotSurvivesOrphanPass(t *testing.T) {
	useReclaimClock(t, newFakeReclaimClock())
	dataDir := t.TempDir()
	kv := kvfault.New()
	s := newKVFaultServer(dataDir, kv)
	fixture := newLiveFixture(t, s)
	foreign := plantFile(t, s, fixture.leaf, "README", true)

	setOrphanCursor(t, kv, fixture.leaf, "")
	orphanTickOrFail(t, s)
	fixture.check(t, s)
	if !fileExists(t, foreign) {
		t.Fatal("a file whose name does not parse, past the grace age, was unlinked by the orphan pass")
	}
}
