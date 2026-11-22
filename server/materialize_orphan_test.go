package server

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/store"
)

// Regression: a materialise that fails between reserving its id and committing
// its catalog batch leaves keys and payload files under an id no catalog entry
// names. These tests drive materializeRange directly on the fault store, with
// no reclaim goroutine, and run the reclaimer lap by lap.

// errMaterializeCrash is what the catalog hook returns to stop a materialise
// where a crash would.
var errMaterializeCrash = errors.New("simulated crash before the migrated bucket's catalog batch")

// migrationSource is a blob bucket with an inline blob and a file-backed one,
// and the migration stream of its range.
type migrationSource struct {
	stream  []byte
	content []byte
}

func newMigrationSource(t *testing.T, s *Server, name string) migrationSource {
	t.Helper()
	createBlobBucket(t, s, name)
	content := bytes.Repeat([]byte{0x5A}, store.InlineThreshold+321)
	if resp, err := s.BlobWrite(BlobWriteRequest{Database: "app", Bucket: name, Key: "big", Data: content}); err != nil || !resp.Success {
		t.Fatalf("blob write %s/big: %+v, %v", name, resp, err)
	}
	if resp, err := s.BlobWrite(BlobWriteRequest{Database: "app", Bucket: name, Key: "small", Data: []byte("inline")}); err != nil || !resp.Success {
		t.Fatalf("blob write %s/small: %+v, %v", name, resp, err)
	}
	source := lookupBucket(s, "app", name)
	var stream bytes.Buffer
	if err := s.streamBucketRange(&stream, s.kv, source.slot, source.databaseID, source.id.Load()); err != nil {
		t.Fatalf("streaming %s: %v", name, err)
	}
	return migrationSource{stream: stream.Bytes(), content: content}
}

func createBlobBucket(t *testing.T, s *Server, name string) {
	t.Helper()
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: name, Type: BucketTypeBlob}); err != nil || !resp.Success {
		t.Fatalf("create %s: %+v, %v", name, resp, err)
	}
}

// failNextMaterializeCatalog makes the next materialise stop before its catalog
// batch commits, and returns the id it had reserved.
func failNextMaterializeCatalog(t *testing.T) (reservedID func() uint64) {
	t.Helper()
	var captured uint64
	hook := func(id uint64) error {
		captured = id
		return errMaterializeCrash
	}
	materializeCatalogHook.Store(&hook)
	t.Cleanup(func() { materializeCatalogHook.Store(nil) })
	return func() uint64 {
		materializeCatalogHook.Store(nil)
		return captured
	}
}

// liveNeighbour is a catalogued blob bucket whose state the orphaned-range
// pass must leave exactly as it found it.
type liveNeighbour struct {
	name       string
	slot       uint16
	databaseID uint64
	id         uint64
	keys       int
	files      []string
	content    []byte
}

func snapshotNeighbour(t *testing.T, s *Server, name string, content []byte) liveNeighbour {
	t.Helper()
	bucket := lookupBucket(s, "app", name)
	neighbour := liveNeighbour{name: name, slot: bucket.slot, databaseID: bucket.databaseID, id: bucket.id.Load(), content: content}
	neighbour.keys = liveKeysIn(t, s.kv, bucket.slot, bucket.databaseID, bucket.id.Load())
	neighbour.files = payloadFilesOf(t, s.blobRoot(), bucket.id.Load())
	if neighbour.keys == 0 || len(neighbour.files) == 0 {
		t.Fatalf("precondition: live bucket %s (id %d) holds %d keys and %d payload files; it needs both to prove anything",
			name, bucket.id.Load(), neighbour.keys, len(neighbour.files))
	}
	return neighbour
}

func (n liveNeighbour) check(t *testing.T, s *Server) {
	t.Helper()
	if keys := liveKeysIn(t, s.kv, n.slot, n.databaseID, n.id); keys != n.keys {
		t.Fatalf("the orphaned-range pass reached catalogued bucket %s (id %d): its range went from %d to %d live keys", n.name, n.id, n.keys, keys)
	}
	for _, path := range n.files {
		if !fileExists(t, path) {
			t.Fatalf("the orphaned-range pass unlinked %s, a payload file of catalogued bucket %s (id %d)", path, n.name, n.id)
		}
	}
	read, err := s.BlobRead(BlobReadRequest{Database: "app", Bucket: n.name, Key: "big"})
	if err != nil || !read.Found || !bytes.Equal(read.Data, n.content) {
		t.Fatalf("catalogued bucket %s (id %d) no longer serves its blob byte-identical: %s", n.name, n.id, describeBlobRead(read, err))
	}
}

// orphanTickOverFiles ages every file and runs one orphan tick over each of
// their leaves, so the blob-tree pass has its chance at them too.
func orphanTickOverFiles(t *testing.T, s *Server, paths []string) {
	t.Helper()
	for _, path := range paths {
		if !fileExists(t, path) {
			continue
		}
		ageFile(t, path)
		setOrphanCursor(t, s.kv, leafOf(t, s, path), "")
		orphanTickOrFail(t, s)
	}
}

// TestFailedMaterializeRangeIsReclaimed is the acceptance test for the orphan reclaim. A
// materialise stops before its catalog batch, leaving keys and payload files
// under its reserved id; the reclaimer must empty that range and unlink those
// files. Its safety arm: two catalogued buckets in the same slot, with the ids
// directly below and directly above the orphaned one — one created, one itself
// materialised — keep every key and every file.
func TestFailedMaterializeRangeIsReclaimed(t *testing.T) {
	useReclaimClock(t, newFakeReclaimClock())
	kv := kvfault.New()
	s := newKVFaultServer(t.TempDir(), kv)
	source := newMigrationSource(t, s, "source")

	forced := (storage.SlotOf(storage.PlacementKey("app", "failed", nil)) + 1) % storage.SlotCount
	forceSlot(t, forced, "below", "failed", "above")

	createBlobBucket(t, s, "below")
	belowContent := bytes.Repeat([]byte{0xB0}, store.InlineThreshold+17)
	if resp, err := s.BlobWrite(BlobWriteRequest{Database: "app", Bucket: "below", Key: "big", Data: belowContent}); err != nil || !resp.Success {
		t.Fatalf("blob write below/big: %+v, %v", resp, err)
	}

	reservedID := failNextMaterializeCatalog(t)
	err := s.materializeRange(migratedBucket{database: "app", name: "failed", storeType: BucketTypeBlob}, source.stream, 0)
	if !errors.Is(err, errMaterializeCrash) {
		t.Fatalf("the materialise of %q returned %v, want the simulated crash", "failed", err)
	}
	orphanedID := reservedID()

	if err := s.materializeRange(migratedBucket{database: "app", name: "above", storeType: BucketTypeBlob}, source.stream, 0); err != nil {
		t.Fatalf("materialising %q: %v", "above", err)
	}
	below, above := snapshotNeighbour(t, s, "below", belowContent), snapshotNeighbour(t, s, "above", source.content)
	if below.id != orphanedID-1 || above.id != orphanedID+1 || below.slot != forced || above.slot != forced {
		t.Fatalf("precondition: the live buckets are {slot %d, id %d} and {slot %d, id %d}; they must sit in slot %d at ids %d and %d, next to the orphaned id",
			below.slot, below.id, above.slot, above.id, forced, orphanedID-1, orphanedID+1)
	}

	residueKeys := liveKeysIn(t, kv, forced, below.databaseID, orphanedID)
	residueFiles := payloadFilesOf(t, s.blobRoot(), orphanedID)
	if residueKeys == 0 || len(residueFiles) == 0 {
		t.Fatalf("precondition: the failed materialise left %d keys and %d payload files under id %d; the test needs both",
			residueKeys, len(residueFiles), orphanedID)
	}
	if _, catalogued, err := s.readCatalogEntry("app", "failed"); err != nil || catalogued {
		t.Fatalf("precondition: the failed materialise is catalogued (%v, %v); its range would not be an orphan", catalogued, err)
	}

	lapUntilDrained(t, s)
	orphanTickOverFiles(t, s, append(append(append([]string(nil), residueFiles...), below.files...), above.files...))

	if keys := liveKeysIn(t, kv, forced, below.databaseID, orphanedID); keys != 0 {
		t.Errorf("%d of the failed materialise's %d keys under {slot %d, id %d} survive the reclaimer; no catalog entry names the id and nothing reclaims it",
			keys, residueKeys, forced, orphanedID)
	}
	for _, path := range residueFiles {
		if _, err := os.Stat(path); err == nil {
			t.Errorf("payload file %s of the failed materialise (id %d) survives the reclaimer and the orphan pass", path, orphanedID)
		}
	}
	below.check(t, s)
	above.check(t, s)
}

// TestMaterializeInFlightIsNotAnOrphan: while a materialise is between its
// reservation and its catalog batch, laps of the reclaimer leave its range and
// its files alone, and once it commits the bucket serves every blob — a range
// mid-materialise is not an orphan, and a committed one never becomes one.
func TestMaterializeInFlightIsNotAnOrphan(t *testing.T) {
	useReclaimClock(t, newFakeReclaimClock())
	kv := kvfault.New()
	s := newKVFaultServer(t.TempDir(), kv)
	source := newMigrationSource(t, s, "source")

	reached := make(chan uint64, 1)
	release := make(chan struct{})
	hook := func(id uint64) error {
		reached <- id
		<-release
		return nil
	}
	materializeCatalogHook.Store(&hook)
	t.Cleanup(func() { materializeCatalogHook.Store(nil) })

	materialized := make(chan error, 1)
	go func() {
		materialized <- s.materializeRange(migratedBucket{database: "app", name: "moving", storeType: BucketTypeBlob}, source.stream, 0)
	}()
	var movingID uint64
	select {
	case movingID = <-reached:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("the materialise never reached its catalog batch")
	}
	slot := slotForCreate("app", "moving")
	databaseID := storage.LegacyDatabaseID("app")
	keysBefore := liveKeysIn(t, kv, slot, databaseID, movingID)
	filesBefore := payloadFilesOf(t, s.blobRoot(), movingID)
	if keysBefore == 0 || len(filesBefore) == 0 {
		close(release)
		t.Fatalf("precondition: the in-flight materialise holds %d keys and %d payload files; the test needs both", keysBefore, len(filesBefore))
	}

	r := newReclaimer(s)
	for lap := 0; lap < 3; lap++ {
		if _, err := r.lap(context.Background()); err != nil {
			close(release)
			t.Fatalf("lap %d: %v", lap, err)
		}
	}
	keysDuring := liveKeysIn(t, kv, slot, databaseID, movingID)
	filesDuring := payloadFilesOf(t, s.blobRoot(), movingID)
	close(release)
	if err := <-materialized; err != nil {
		t.Fatalf("materialising %q: %v", "moving", err)
	}
	if keysDuring != keysBefore || len(filesDuring) != len(filesBefore) {
		t.Fatalf("laps run while the materialise was in flight changed its range from %d keys and %d files to %d and %d",
			keysBefore, len(filesBefore), keysDuring, len(filesDuring))
	}

	// The catalog batch adds the state document, so the committed range is
	// measured again rather than compared with the in-flight count.
	keysCommitted := liveKeysIn(t, kv, slot, databaseID, movingID)
	lapUntilDrained(t, s)
	if keys := liveKeysIn(t, kv, slot, databaseID, movingID); keys != keysCommitted {
		t.Fatalf("after the materialise committed, the reclaimer changed its range from %d to %d live keys", keysCommitted, keys)
	}
	read, err := s.BlobRead(BlobReadRequest{Database: "app", Bucket: "moving", Key: "big"})
	if err != nil || !read.Found || !bytes.Equal(read.Data, source.content) {
		t.Fatalf("the materialised bucket does not serve its blob byte-identical after the laps: %s", describeBlobRead(read, err))
	}
}
