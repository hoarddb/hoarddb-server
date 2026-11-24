package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// ringFifoElementLimit is the most bytes the ring takes as one fifo element, measured on a
// real server: an element of 1048563 bytes is accepted and one of 1048564 is refused with "at
// most 1048576 bytes are taken" (the 1 MiB limit of cluster/wire/ringkeyed.go, maxVerbArgBytes,
// which is not exported, less the push envelope). A hashStore or heapStore element has a
// bigger envelope, so its limit is lower. The test fails if the boundary moves.
const ringFifoElementLimit = 1048563

// bsonDocumentOverhead is what a one-member document {Text: <string>} adds to the string's
// bytes: 4 length, 1 type, "Text" and its nul, 4 string length, the string's nul and the
// document's nul.
const bsonDocumentOverhead = 16

// TestRestoreOfAMaximumSizeElement pins both outcomes at the real boundary. The plan expected
// elements of 24 to 30 MiB; the ring takes at most ringFifoElementLimit, so a dump the ring
// wrote never holds a bigger one, and a hand-made dump that does is refused at that element.
// At the limit the element comes back byte for byte, also through a second dump and a restore
// under another name, and the restore exits 0; one byte over it the restore stops with exit 2,
// its output names the entity, the field and the server's text, and it sends nothing after
// the refused frame.
func TestRestoreOfAMaximumSizeElement(t *testing.T) {
	t.Run("an element of the limit round-trips", func(t *testing.T) {
		textBytes := ringFifoElementLimit - bsonDocumentOverhead
		element := largeRestoreElement(t, textBytes)
		if len(element) != ringFifoElementLimit {
			t.Fatalf("the test element is %d bytes, want %d", len(element), ringFifoElementLimit)
		}
		path := writeLargeRestoreDump(t, 1, 1, 1, textBytes)
		addr, target := newRingTestHost(t)
		if code, _, stderr := restoreOnHost(t, addr, path); code != ExitOK {
			t.Fatalf("restore exited %d:\n%s", code, stderr)
		}
		// Dump what was restored and restore that under another name, on the same ring.
		second := dumpToFile(t, addr)
		if code, _, stderr := restoreOnHost(t, addr, second, "--db", "app", "--as", "app_copy"); code != ExitOK {
			t.Fatalf("restore under another name exited %d:\n%s", code, stderr)
		}
		for _, database := range []string{"app", "app_copy"} {
			page, err := target.Bucket(database, "User").StoreList(restoreTestContext(t), "u00000", "Items", 10, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Elements) != 1 || !bytes.Equal(page.Elements[0], element) {
				t.Fatalf("the element of %s did not come back byte-equal: %d elements", database, len(page.Elements))
			}
		}
	})

	t.Run("an element one byte over the limit stops the restore at that element", func(t *testing.T) {
		// Twelve elements one byte over the limit do not fit one frame (the cap is
		// restoreFrameByteCap), so the second store frame is the one a restore that went on
		// past the refusal would send.
		path := writeLargeRestoreDump(t, 1, 1, 17, ringFifoElementLimit+1-bsonDocumentOverhead)
		addr, target := newRingTestHost(t)
		harness := newRestoreHarness(t, addr, path, ringRestoreFlags{})
		counting := &countingRestoreTransport{restoreTransport: harness.sender.transport}
		harness.sender.transport = counting
		err := harness.applier.apply()
		if err == nil {
			t.Fatal("a restore of elements over the ring's limit exited without an error")
		}
		if exit := restoreExit(harness.run.ctx, err); exit != ExitRefused {
			t.Errorf("exit %d, want %d (refused)", exit, ExitRefused)
		}
		text := err.Error()
		for _, want := range []string{"app.User", `"u00000"`, `"Items"`, "1048576 bytes"} {
			if !strings.Contains(text, want) {
				t.Errorf("the refusal does not contain %q:\n%s", want, text)
			}
		}
		storeFrames := 0
		for _, frame := range counting.frames {
			if len(frame) > 0 && frame[0] != restoreInsert {
				storeFrames++
			}
		}
		if storeFrames != 1 {
			t.Errorf("%d store frames were sent, want exactly the one that was refused (frames %v)", storeFrames, counting.frames)
		}
		if got := storeCount(t, target, "app", "u00000", "Items"); got != 0 {
			t.Errorf("the fifo holds %d elements after a refused frame", got)
		}
	})
}

// TestRestoreMemoryIsBoundedByOneFrame is a measurement (section 2.1): a dump much bigger than
// a frame is restored through a transport that answers at once, and the test reports the most
// heap the restore held above where it began. It asserts that the peak stays under a few frames
// and nowhere near the size of the file, which is what holding a bucket would cost.
func TestRestoreMemoryIsBoundedByOneFrame(t *testing.T) {
	const entities, elementsPerEntity, elementBytes = 80, 4, 600 << 10
	path := writeLargeRestoreDump(t, entities, 1, elementsPerEntity, elementBytes)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	addr, _ := newRingTestHost(t)
	harness := newRestoreHarness(t, addr, path, ringRestoreFlags{})
	script := &scriptedRestoreTransport{answer: func(_ int, ops []restoreOp) []ringDocAnswer { return make([]ringDocAnswer, len(ops)) }}
	harness.sender.transport = &frameDroppingTransport{scripted: script}

	// A tight collector, so a frame that was let go is not counted as held.
	previous := debug.SetGCPercent(10)
	t.Cleanup(func() { debug.SetGCPercent(previous) })
	held, applyErr := heapHeldAbove(harness.applier.apply)
	if applyErr != nil {
		t.Fatalf("apply: %v", applyErr)
	}
	t.Logf("measurement: dump file %d MiB, frame cap %d MiB, restore held at most %d MiB above its start", info.Size()>>20, restoreFrameByteCap>>20, held>>20)
	if bound := int64(4 * restoreFrameByteCap); held > bound {
		t.Errorf("the restore held %d MiB, over %d MiB (four frames); the file is %d MiB", held>>20, bound>>20, info.Size()>>20)
	}
}

// heapSampleInterval is how often heapHeldAbove reads the heap while run is going.
const heapSampleInterval = 2 * time.Millisecond

// heapHeldAbove runs run and returns the most reachable heap above where it began, sampled every
// heapSampleInterval, and run's error.
func heapHeldAbove(run func() error) (int64, error) {
	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	var (
		mutex sync.Mutex
		peak  uint64
		done  = make(chan struct{})
		wait  sync.WaitGroup
	)
	wait.Add(1)
	go func() {
		defer wait.Done()
		for {
			select {
			case <-done:
				return
			case <-time.After(heapSampleInterval):
				// HeapAlloc counts garbage not yet swept, so collect first: the peak is then
				// what is reachable, which is what the restore holds.
				runtime.GC()
				var now runtime.MemStats
				runtime.ReadMemStats(&now)
				mutex.Lock()
				peak = max(peak, now.HeapAlloc)
				mutex.Unlock()
			}
		}
	}()
	runErr := run()
	close(done)
	wait.Wait()
	return int64(peak) - int64(base.HeapAlloc), runErr
}

// TestHeapHeldAboveDoesNotCountGarbageTheCollectorHasNotSwept (BUG-0916): run lets go of every
// buffer it allocates, with the collector off so none is swept while it runs. The restore memory
// test fails about one run in four because HeapAlloc counts that garbage, so the sampler must
// collect before it reads.
func TestHeapHeldAboveDoesNotCountGarbageTheCollectorHasNotSwept(t *testing.T) {
	const buffers, bufferBytes = 16, 8 << 20
	previous := debug.SetGCPercent(-1)
	t.Cleanup(func() { debug.SetGCPercent(previous) })
	var sink []byte
	held, err := heapHeldAbove(func() error {
		for range buffers {
			sink = make([]byte, bufferBytes)
			sink[0] = 1
			// Paced, because a collection that runs while the loop allocates counts the
			// buffers allocated meanwhile as live; a few are expected, all of them are not.
			time.Sleep(2 * heapSampleInterval)
		}
		sink = nil
		time.Sleep(10 * heapSampleInterval)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if bound := int64(buffers / 2 * bufferBytes); held > bound {
		t.Fatalf("the sampler counted %d MiB held, over %d MiB, with one 8 MiB buffer live at a time", held>>20, bound>>20)
	}
}

// TestRestoreMemoryBoundBitesWhenTheRestoreHoldsTheBucket (BUG-0916): the same restore through
// the scripted transport, which keeps every frame, holds about the whole dump, and the bound the
// real test asserts is what must catch it.
func TestRestoreMemoryBoundBitesWhenTheRestoreHoldsTheBucket(t *testing.T) {
	const entities, elementsPerEntity, elementBytes = 80, 4, 600 << 10
	path := writeLargeRestoreDump(t, entities, 1, elementsPerEntity, elementBytes)
	addr, _ := newRingTestHost(t)
	harness := newRestoreHarness(t, addr, path, ringRestoreFlags{})
	keeping := &scriptedRestoreTransport{answer: func(_ int, ops []restoreOp) []ringDocAnswer { return make([]ringDocAnswer, len(ops)) }}
	harness.sender.transport = keeping
	previous := debug.SetGCPercent(10)
	t.Cleanup(func() { debug.SetGCPercent(previous) })
	held, err := heapHeldAbove(harness.applier.apply)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if bound := int64(4 * restoreFrameByteCap); held <= bound {
		t.Fatalf("a restore that kept every frame held %d MiB, not over the %d MiB bound the memory test asserts", held>>20, bound>>20)
	}
}

// frameDroppingTransport answers every frame as applied and keeps nothing of it, unlike the
// scripted transport, which records the frames and so would hold the whole file.
type frameDroppingTransport struct{ scripted *scriptedRestoreTransport }

func (f *frameDroppingTransport) NextRequestID() [16]byte { return f.scripted.NextRequestID() }
func (f *frameDroppingTransport) close() error            { return nil }
func (f *frameDroppingTransport) Send(_ context.Context, ops []restoreOp, _ [16]byte) ([]ringDocAnswer, error) {
	return make([]ringDocAnswer, len(ops)), nil
}

// writeManyBucketDump writes a dump of one database with buckets buckets of one entity each.
func writeManyBucketDump(t *testing.T, buckets int) string {
	t.Helper()
	dir := t.TempDir()
	writer, err := openDumpWriter(dir, "many.hoarddump")
	if err != nil {
		t.Fatal(err)
	}
	scalars, err := bson.Marshal(bson.D{{Key: "Name", Value: "n"}})
	if err != nil {
		t.Fatal(err)
	}
	records := []any{dumpTestHeader(), &dumpDB{T: dumpKindDB, Name: "app", RF: 1, Storage: "disk"}}
	for index := 0; index < buckets; index++ {
		name := fmt.Sprintf("B%05d", index)
		records = append(records,
			&dumpBucket{T: dumpKindBucket, DB: "app", Name: name, Fields: []dumpField{{Name: "Name", Type: "string"}}, Aggregates: []dumpAggregate{}, Unsupported: []string{}},
			&dumpDoc{T: dumpKindDoc, DB: "app", B: name, ID: "e1", F: scalars})
	}
	for _, record := range records {
		if err := writer.WriteRecord(record); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(dumpCounters{Databases: 1, Buckets: int64(buckets), Entities: int64(buckets)}); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "many.hoarddump")
}

// TestRestoreOneThousandBuckets is a measurement (section 2.1), size-gated: it restores a
// dump of 1,000 buckets of one entity each into a fresh ring, asserts that every bucket and
// entity is there, and reports the milliseconds per bucket of the whole restore and of the one
// read each bucket costs before its create (DescribeBucket, the check that the name is free).
// The figures are those of the host the test ran on and are published only with that host's
// name.
func TestRestoreOneThousandBuckets(t *testing.T) {
	if os.Getenv("RINGCLI_LARGE_TESTS") != "1" {
		t.Skip("set RINGCLI_LARGE_TESTS=1")
	}
	const buckets = 1000
	path := writeManyBucketDump(t, buckets)
	addr, target := newRingTestHost(t)

	started := time.Now()
	code, _, stderr := restoreOnHost(t, addr, path)
	elapsed := time.Since(started)
	if code != ExitOK {
		t.Fatalf("restore exited %d:\n%s", code, stderr)
	}
	ctx := restoreTestContext(t)
	count := 0
	var cursor []byte
	for {
		page, err := target.ShowBuckets(ctx, "app", ringclient.PageRequest{Limit: 500, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		count += len(page.Buckets)
		if !page.More {
			break
		}
		cursor = page.Cursor
	}
	if count != buckets {
		t.Fatalf("the target holds %d buckets, want %d", count, buckets)
	}

	readStarted := time.Now()
	for index := 0; index < buckets; index++ {
		if _, err := target.DescribeBucket(ctx, "app", fmt.Sprintf("B%05d", index)); err != nil {
			t.Fatal(err)
		}
	}
	reads := time.Since(readStarted)
	t.Logf("measurement (%d CPUs, %s/%s): %d buckets restored in %s, %.2f ms per bucket; one DescribeBucket read costs %.3f ms of that",
		runtime.NumCPU(), runtime.GOOS, runtime.GOARCH, buckets, elapsed.Round(time.Millisecond),
		float64(elapsed.Microseconds())/1000/buckets, float64(reads.Microseconds())/1000/buckets)
}

// TestRestoreRetainedBytesPerLinkedBucket is a measurement, size-gated: it reports what pass 2
// and the applier keep in memory for each bucket that declares a link, for a bucket of 2 fields
// and one of 8 (the shape of the rule 11 example), and asserts only that the cost stays under
// 4 KiB a bucket. The page quotes these figures; they are heap bytes on the host that ran the
// test, and grow with the number of fields.
func TestRestoreRetainedBytesPerLinkedBucket(t *testing.T) {
	if os.Getenv("RINGCLI_LARGE_TESTS") != "1" {
		t.Skip("set RINGCLI_LARGE_TESTS=1")
	}
	const buckets = 20000
	for _, fieldCount := range []int{2, 8} {
		dir := t.TempDir()
		writer, err := openDumpWriter(dir, "links.hoarddump")
		if err != nil {
			t.Fatal(err)
		}
		records := []any{dumpTestHeader(), &dumpDB{T: dumpKindDB, Name: "app", RF: 1, Storage: "disk"},
			&dumpBucket{T: dumpKindBucket, DB: "app", Name: "Hub", Fields: []dumpField{{Name: "Name", Type: "string"}}, Aggregates: []dumpAggregate{}, Unsupported: []string{}}}
		for index := 0; index < buckets; index++ {
			fields := []dumpField{{Name: "Hub", Type: "string", Of: "Hub"}}
			for extra := 1; extra < fieldCount; extra++ {
				fields = append(fields, dumpField{Name: fmt.Sprintf("Field%d", extra), Type: "string"})
			}
			records = append(records, &dumpBucket{T: dumpKindBucket, DB: "app", Name: fmt.Sprintf("U%06d", index), Fields: fields, Aggregates: []dumpAggregate{}, Unsupported: []string{}})
		}
		for _, record := range records {
			if err := writer.WriteRecord(record); err != nil {
				t.Fatal(err)
			}
		}
		if err := writer.Close(dumpCounters{Databases: 1, Buckets: buckets + 1}); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "links.hoarddump")

		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		plan, err := planRestoreTargets(context.Background(), &ringRestoreFlags{file: path}, &stubRestoreTarget{}, path)
		if err != nil {
			t.Fatal(err)
		}
		applier := &restoreApplier{created: map[linkBucketKey]bool{}}
		for _, record := range plan.linked("app") {
			applier.created[linkBucketKey{db: record.DB, name: record.Name}] = true
			applier.progress.createdAhead("app", record.Name)
		}
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		perBucket := (int64(after.HeapAlloc) - int64(before.HeapAlloc)) / buckets
		t.Logf("measurement: %d fields per bucket: %d bytes retained per linked bucket (%d buckets)", fieldCount, perBucket, buckets)
		if perBucket > 4096 {
			t.Errorf("%d bytes retained per linked bucket of %d fields, over 4096", perBucket, fieldCount)
		}
		runtime.KeepAlive(plan)
		runtime.KeepAlive(applier)
	}
}
