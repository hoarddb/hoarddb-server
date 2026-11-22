package cli

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// dumpParallelLargeElements is how many elements each large entity of the size-gated memory
// tests holds: 100,000 elements of 1 KiB each are about 100 MiB, which is at least eight
// times the formula's bound for --parallel 2, so a dump that holds a whole entity fails
// the measurement (section 3.3).
const dumpParallelLargeElements = 100_000

// dumpHeapInUse is the process's live heap, the figure the peak of section 3.3 samples.
// runtime.MemStats has no peak field, so the sampler below records the maximum itself.
func dumpHeapInUse() uint64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapInuse + stats.StackInuse
}

// dumpHeapPeak samples the live heap while a dump runs and keeps the maximum over the
// baseline, which is the figure taken after a forced collection just before the dump
// starts (section 3.3, "How to measure a peak"). It sets debug.SetGCPercent(20) for the
// run so garbage is not counted as peak, and restores the previous percentage when it
// stops. The dump process's own live heap before the run is the host's share: the baseline
// the test logs, so a reader can tell how much of the peak is the run's and how much is the
// test process around it.
type dumpHeapPeak struct {
	baseline          uint64
	peak              atomic.Uint64
	stop              chan struct{}
	done              chan struct{}
	stopped           atomic.Bool
	previousGCPercent int
}

// startDumpHeapPeak starts sampling. The caller stops it with finish, and the test's
// cleanup stops it too, so a failure inside the run leaves no sampler goroutine behind.
func startDumpHeapPeak(t *testing.T) *dumpHeapPeak {
	t.Helper()
	sampler := &dumpHeapPeak{
		stop:              make(chan struct{}),
		done:              make(chan struct{}),
		previousGCPercent: debug.SetGCPercent(20),
	}
	runtime.GC()
	sampler.baseline = dumpHeapInUse()
	go func() {
		defer close(sampler.done)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-sampler.stop:
				return
			case <-ticker.C:
				used := dumpHeapInUse()
				for {
					peak := sampler.peak.Load()
					if used <= peak || sampler.peak.CompareAndSwap(peak, used) {
						break
					}
				}
			}
		}
	}()
	t.Cleanup(sampler.stopSampling)
	return sampler
}

// finish stops the sampling and returns the measured peak over the baseline, in bytes.
func (p *dumpHeapPeak) finish(t *testing.T) int64 {
	t.Helper()
	p.stopSampling()
	peak := p.peak.Load()
	if peak <= p.baseline {
		return 0
	}
	return int64(peak - p.baseline)
}

// stopSampling stops the sampler once and restores the GC percent it changed.
func (p *dumpHeapPeak) stopSampling() {
	if p.stopped.CompareAndSwap(false, true) {
		close(p.stop)
		<-p.done
		debug.SetGCPercent(p.previousGCPercent)
	}
}

// dumpWaitFor polls condition until it holds or the deadline passes. Every wait in this
// file is a poll with a deadline and never a fixed sleep, so a test fails with a reason
// instead of passing or failing by timing.
func dumpWaitFor(t *testing.T, what string, deadline time.Duration, condition func() bool) bool {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if condition() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return condition()
}

// dumpParallelStoresStub scripts two buckets of several List pages each, every writable
// store type, and a queue of one entity that spans three StoreList pages and three elems
// chunks, so the serial and the parallel dump have something order-sensitive to disagree
// about.
func dumpParallelStoresStub(t *testing.T) *dumpStub {
	t.Helper()
	declaration := func(name string) ringclient.BucketInfo {
		return ringclient.BucketInfo{Database: "app", Name: name, State: "active", Fields: []ringclient.FieldInfo{
			{Name: dumpBucketIDField, Type: "string"},
			{Name: "Name", Type: "string"},
			{Name: "Queue", Type: dumpFifoStoreWord},
			{Name: "Stack", Type: dumpLifoStoreWord},
			{Name: "Tags", Type: dumpHashStoreWord},
			{Name: "Recipes", Type: dumpBTreeStoreWord},
			{Name: "Books", Type: dumpHeapStoreWord},
			{Name: "Data", Type: dumpBlobStoreWord},
		}}
	}
	stub := &dumpStub{
		databasePages: dumpStubDatabasePages([]ringclient.DatabaseSummary{{Name: "app", RF: 1}}, 1),
		bucketPages: map[string][]ringclient.BucketPage{
			"app": dumpStubBucketPages([]ringclient.BucketSummary{
				{Name: "Alpha", State: "active"},
				{Name: "Beta", State: "active"},
			}, 1),
		},
		descriptions: map[string]ringclient.DatabaseDescription{"app": {Name: "app", RF: 1, Storage: "disk"}},
		declarations: map[string]ringclient.BucketInfo{
			"app/Alpha": declaration("Alpha"),
			"app/Beta":  declaration("Beta"),
		},
		documentPages: map[string][]ringclient.EntryPage{},
		elementPages:  map[string][]ringclient.ElementPage{},
	}
	for _, scripted := range []struct {
		bucket   string
		entities int
		pageSize int
	}{
		{bucket: "Alpha", entities: 6, pageSize: 2},
		{bucket: "Beta", entities: 5, pageSize: 3},
	} {
		entries := make([]ringclient.Entry, scripted.entities)
		for index := range entries {
			id := fmt.Sprintf("%s-%d", strings.ToLower(scripted.bucket), index)
			entries[index] = ringclient.Entry{ID: id, Body: dumpTestBody(t, id, fmt.Sprintf("%s entity %d", scripted.bucket, index))}
			key := "app/" + scripted.bucket + "/" + id + "/"
			// One queue spans three pages of 800 elements and three chunks of 1,000; the
			// rest are short, so the page walk is exercised at both sizes.
			queueCount := 3
			if scripted.bucket == "Alpha" && index == 0 {
				queueCount = 2500
			}
			queue := make([]bson.Raw, queueCount)
			for element := range queue {
				queue[element] = dumpTestElement(t, fmt.Sprintf("m%d-%d", index, element))
			}
			stub.elementPages[key+"Queue"] = dumpStubElementPages(ringclient.ElementPage{Elements: queue}, 800)
			stub.elementPages[key+"Stack"] = dumpStubElementPages(ringclient.ElementPage{Elements: []bson.Raw{
				dumpTestElement(t, "s1"), dumpTestElement(t, "s2"), dumpTestElement(t, "s3"),
			}}, 2)

			keys := []string{"k1", "k2", "k3"}
			tags := []bson.Raw{dumpTestElement(t, "t1"), dumpTestElement(t, "t2"), dumpTestElement(t, "t3")}
			stub.elementPages[key+"Tags"] = dumpStubElementPages(ringclient.ElementPage{Elements: tags, Keys: keys}, 2)
			recipes := []bson.Raw{dumpTestElement(t, "r1"), dumpTestElement(t, "r2")}
			stub.elementPages[key+"Recipes"] = dumpStubElementPages(ringclient.ElementPage{
				Elements: recipes, Keys: []string{"r1", "r2"},
			}, 2)
			books := []bson.Raw{dumpTestElement(t, "b1"), dumpTestElement(t, "b2"), dumpTestElement(t, "b3")}
			stub.elementPages[key+"Books"] = dumpStubElementPages(ringclient.ElementPage{
				Elements: books, Keys: []string{"e1", "e2", "e3"}, Priorities: []int64{30, 20, 10},
			}, 2)
		}
		stub.documentPages["app/"+scripted.bucket] = dumpStubDocumentPages(entries, scripted.pageSize)
	}
	return stub
}

// TestParallelDumpIsByteIdenticalToSerial: the entities of one List page are read
// concurrently and written in page order, so a --parallel 8 dump of a quiet ring is the
// same file as a --parallel 1 dump of it (section 3.3). The header's created time differs,
// and the footer's checksum with it, because the checksum covers the header (section 3.1);
// every record between them must be equal, the summary included.
func TestParallelDumpIsByteIdenticalToSerial(t *testing.T) {
	stub := dumpParallelStoresStub(t)
	outDir := t.TempDir()
	serialPath := filepath.Join(outDir, "serial.hoarddump")
	parallelPath := filepath.Join(outDir, "parallel.hoarddump")
	var spiller *dumpSpiller
	dumpSpillerCreated = func(created *dumpSpiller) { spiller = created }
	t.Cleanup(func() { dumpSpillerCreated = nil })
	// Every record is over the spill limit, so a path that buffers at all creates a spill
	// file and the serial run's promise (no buffer at all) is observable. The parallel run
	// then also exercises the spill and copy-back path.
	previousSpillBytes := dumpSpillBytes
	dumpSpillBytes = 1
	t.Cleanup(func() { dumpSpillBytes = previousSpillBytes })

	for _, run := range []struct {
		parallel string
		path     string
	}{
		{parallel: "1", path: serialPath},
		{parallel: "8", path: parallelPath},
	} {
		code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--parallel", run.parallel, "--out", run.path)
		if code != ExitOK {
			t.Fatalf("dumping with --parallel %s: exit %d, want %d\nstdout: %s\nstderr: %s",
				run.parallel, code, ExitOK, stdout, stderr)
		}
		if stdout != "" {
			t.Errorf("the --parallel %s run wrote to stdout: %q (the report goes to stderr)", run.parallel, stdout)
		}
		if spiller == nil {
			t.Fatalf("the --parallel %s run created no spiller", run.parallel)
		}
		// --parallel 1 reads each entity straight into the file: no buffer, so no spill
		// file, even with a limit every record passes.
		if run.parallel == "1" && spiller.peak.Load() != 0 {
			t.Errorf("a --parallel 1 dump created %d spill files; it buffers no entity", spiller.peak.Load())
		}
	}
	if files := dumpSpillFiles(t, outDir); len(files) != 0 {
		t.Errorf("the dumps left the spill files %v in %s", files, outDir)
	}

	// The data must really exercise what the two paths could differ on: one queue of
	// several pages and several chunks, and several pages of several entities.
	if got, want := stub.calls.storeListCallsFor("app/Alpha/alpha-0/Queue"), 4*2; got != want {
		t.Fatalf("StoreList was asked %d times for the 2,500-element queue over two dumps, want %d pages: the fixture no longer spans pages", got, want)
	}

	serial := readDumpRecords(t, serialPath)
	parallel := readDumpRecords(t, parallelPath)
	if len(serial) < 3 {
		t.Fatalf("the serial dump holds %d records, want at least a header, a summary and a footer", len(serial))
	}
	if len(parallel) != len(serial) {
		t.Fatalf("the parallel dump holds %d records and the serial one %d, want the same", len(parallel), len(serial))
	}
	serialHeader, isHeader := serial[0].(*dumpHeader)
	if !isHeader {
		t.Fatalf("the first record of a dump is a %T, want the header", serial[0])
	}
	parallelHeader, isHeader := parallel[0].(*dumpHeader)
	if !isHeader {
		t.Fatalf("the first record of the parallel dump is a %T, want the header", parallel[0])
	}
	if parallelHeader.Tool != serialHeader.Tool || parallelHeader.Scope != serialHeader.Scope ||
		parallelHeader.Consistency != serialHeader.Consistency || parallelHeader.V != serialHeader.V {
		t.Errorf("the headers differ beyond their created time: serial %+v, parallel %+v", serialHeader, parallelHeader)
	}
	// Everything but the header (its created time) and the footer (whose checksum covers
	// the header) must be equal record for record.
	if !reflect.DeepEqual(serial[1:len(serial)-1], parallel[1:len(parallel)-1]) {
		for index := 1; index < len(serial)-1; index++ {
			if !reflect.DeepEqual(serial[index], parallel[index]) {
				t.Errorf("record %d differs:\nserial:   %+v\nparallel: %+v", index+1, serial[index], parallel[index])
				break
			}
		}
	}
	serialFooter, serialIsFooter := serial[len(serial)-1].(*dumpFooter)
	parallelFooter, parallelIsFooter := parallel[len(parallel)-1].(*dumpFooter)
	if !serialIsFooter || !parallelIsFooter {
		t.Fatalf("the last records of the dumps are %T and %T, want the footer", serial[len(serial)-1], parallel[len(parallel)-1])
	}
	if serialFooter.Bytes != parallelFooter.Bytes {
		t.Errorf("the serial file covers %d bytes and the parallel file %d, want the same", serialFooter.Bytes, parallelFooter.Bytes)
	}

	serialVerification, err := verifyDump(serialPath)
	if err != nil {
		t.Fatalf("the serial dump does not pass the check restore --verify runs: %v", err)
	}
	parallelVerification, err := verifyDump(parallelPath)
	if err != nil {
		t.Fatalf("the parallel dump does not pass the check restore --verify runs: %v", err)
	}
	// The checksum covers the header, whose created time differs between the two runs, so it
	// is the one member that cannot be equal; the counts are what the two files must share.
	serialVerification.SHA256, parallelVerification.SHA256 = [32]byte{}, [32]byte{}
	if !reflect.DeepEqual(serialVerification, parallelVerification) {
		t.Errorf("the two files read as %+v and %+v, want the same summary", serialVerification, parallelVerification)
	}
	if parallelVerification.Buckets != 2 || parallelVerification.Entities != 11 {
		t.Fatalf("the dump holds %d buckets and %d entities, want 2 and 11: the fixture is not what this test measures",
			parallelVerification.Buckets, parallelVerification.Entities)
	}
	// Eleven entities: one queue of 2,500 elements and ten of 3, plus 3+3+2+3 elements of
	// the four keyed and sequence stores each, and no element of the blobStore field.
	if want := int64(2_500 + 10*3 + 11*(3+3+2+3)); parallelVerification.Elements != want {
		t.Errorf("the dump holds %d elements, want %d", parallelVerification.Elements, want)
	}
}

// dumpParallelFifoStub scripts one bucket whose single List page holds entityCount
// entities, each with a fifoStore field served in pagesPerEntity pages, so a test can hold
// every worker of a page at a known moment.
func dumpParallelFifoStub(t *testing.T, entityCount, pagesPerEntity int) *dumpStub {
	t.Helper()
	entries := make([]ringclient.Entry, entityCount)
	stub := &dumpStub{
		databasePages: dumpStubDatabasePages([]ringclient.DatabaseSummary{{Name: "app", RF: 1}}, 1),
		bucketPages: map[string][]ringclient.BucketPage{
			"app": dumpStubBucketPages([]ringclient.BucketSummary{{Name: "Jobs", State: "active"}}, 1),
		},
		descriptions: map[string]ringclient.DatabaseDescription{"app": {Name: "app", RF: 1, Storage: "disk"}},
		declarations: map[string]ringclient.BucketInfo{
			"app/Jobs": {Database: "app", Name: "Jobs", State: "active", Fields: []ringclient.FieldInfo{
				{Name: dumpBucketIDField, Type: "string"},
				{Name: "Name", Type: "string"},
				{Name: "Queue", Type: dumpFifoStoreWord},
			}},
		},
		documentPages: map[string][]ringclient.EntryPage{},
		elementPages:  map[string][]ringclient.ElementPage{},
	}
	for index := range entries {
		id := fmt.Sprintf("job-%d", index)
		entries[index] = ringclient.Entry{ID: id, Body: dumpTestBody(t, id, fmt.Sprintf("job %d", index))}
		elements := make([]bson.Raw, pagesPerEntity)
		for page := range elements {
			elements[page] = dumpTestElement(t, fmt.Sprintf("m-%s-%d", id, page))
		}
		stub.elementPages["app/Jobs/"+id+"/Queue"] = dumpStubElementPages(ringclient.ElementPage{Elements: elements}, 1)
	}
	stub.documentPages["app/Jobs"] = []ringclient.EntryPage{{Entries: entries}}
	return stub
}

// TestParallelWorkersExitOnCancel: a cancelled run stops every worker and leaves nothing
// behind. The stub's store reads block until the context is cancelled, so the cancel lands
// while the workers are busy, and the test then proves that no goroutine outlived the page
// (section 3.3). It is the race detector's test: run it with -race.
func TestParallelWorkersExitOnCancel(t *testing.T) {
	baseline := runtime.NumGoroutine()
	stub := dumpParallelFifoStub(t, 6, 2)
	// Every buffer spills at once, so the cancel has real spill files to clean up and the
	// test fails a build that leaves them behind on the error path.
	previousSpillBytes := dumpSpillBytes
	dumpSpillBytes = 1
	t.Cleanup(func() { dumpSpillBytes = previousSpillBytes })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var started atomic.Int64
	stub.beforeStoreList = func(_, _, _, _ string) {
		started.Add(1)
		// The dump's own reads wait here until the test cancels, so the cancel lands in a
		// page whose workers are all busy.
		<-ctx.Done()
	}
	outDir := t.TempDir()
	outPath := filepath.Join(outDir, "cancelled.hoarddump")
	var stdout, stderr bytes.Buffer
	finished := make(chan int, 1)
	go func() {
		finished <- runRingDumpWithSource(ctx, []string{"--db", "app", "--parallel", "8", "--out", outPath},
			ringDumpEnv{stdout: &stdout, stderr: &stderr}, stub, "root")
	}()
	if !dumpWaitFor(t, "the workers to reach their store reads", 10*time.Second, func() bool { return started.Load() >= 4 }) {
		t.Fatalf("only %d store reads started before the deadline; the dump is not reading a page concurrently", started.Load())
	}
	if files := dumpSpillFiles(t, outDir); len(files) == 0 {
		t.Fatalf("no buffer spilled while the workers are blocked, so this test cannot see a spill file left behind")
	}
	cancel()

	var code int
	select {
	case code = <-finished:
	case <-time.After(30 * time.Second):
		t.Fatalf("the dump did not return after its context was cancelled:\nstderr: %s", stderr.String())
	}
	if code != ExitInterrupted {
		t.Fatalf("cancelling a parallel dump: exit %d, want %d\nstdout: %s\nstderr: %s",
			code, ExitInterrupted, stdout.String(), stderr.String())
	}
	if want := "dump: interrupted. No dump file was written. Run the same command again."; !strings.Contains(stderr.String(), want) {
		t.Errorf("the interrupted dump does not say %q:\nstderr: %s", want, stderr.String())
	}
	entries, err := os.ReadDir(outDir)
	if err != nil {
		t.Fatalf("listing %s: %v", outDir, err)
	}
	if len(entries) != 0 {
		names := make([]string, len(entries))
		for index, entry := range entries {
			names[index] = entry.Name()
		}
		t.Errorf("an interrupted parallel dump left %v in %s; the .partial file and every spill file are removed", names, outDir)
	}
	if !dumpWaitFor(t, "every dump goroutine to exit", 10*time.Second, func() bool { return runtime.NumGoroutine() <= baseline }) {
		t.Errorf("the dump left %d goroutines running, up from %d before it: a worker outlived the page",
			runtime.NumGoroutine(), baseline)
	}
}

// TestSlowEntityDoesNotGrowMemoryWhileOthersWait: a worker starts a new entity only when
// fewer than N buffers wait, so a slow entity holds one of the two buffers --parallel 2
// allows and no more than two entities are ever read at once (section 3.3). The page is
// still written in page order once the slow entity finishes.
func TestSlowEntityDoesNotGrowMemoryWhileOthersWait(t *testing.T) {
	stub := dumpParallelFifoStub(t, 6, 1)
	entries := stub.documentPages["app/Jobs"][0].Entries
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	var guard sync.Mutex
	started := map[string]bool{}
	maxStarted := 0
	startedCount := func() int {
		guard.Lock()
		defer guard.Unlock()
		return len(started)
	}
	highestStarted := func() int {
		guard.Lock()
		defer guard.Unlock()
		return maxStarted
	}
	stub.beforeStoreList = func(_, _, id, _ string) {
		guard.Lock()
		started[id] = true
		if len(started) > maxStarted {
			maxStarted = len(started)
		}
		guard.Unlock()
		if id == "job-0" {
			// Entity 0 holds its buffer until the test has watched who else started.
			<-release
		}
	}
	outPath := filepath.Join(t.TempDir(), "slow.hoarddump")
	finished := make(chan int, 1)
	go func() {
		code, _, _ := runStubDump(t, stub, "--db", "app", "--parallel", "2", "--out", outPath)
		finished <- code
	}()
	if !dumpWaitFor(t, "two entities to start", 10*time.Second, func() bool { return startedCount() >= 2 }) {
		t.Fatalf("only %d entities started while entity 0 is blocked; want the two --parallel 2 allows", startedCount())
	}
	// While entity 0 is blocked, the other slot may finish and wait, but a third entity
	// must not start: a buffer is held for each of the two.
	end := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(end) {
		if got := highestStarted(); got > 2 {
			t.Fatalf("%d entities were read at once with --parallel 2: a worker started an entity while two buffers waited", got)
		}
		time.Sleep(5 * time.Millisecond)
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case code := <-finished:
		if code != ExitOK {
			t.Fatalf("dumping a page with one slow entity: exit %d, want %d", code, ExitOK)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("the dump did not finish after the slow entity was released")
	}
	if got := startedCount(); got != len(entries) {
		t.Errorf("%d entities were read, want %d", got, len(entries))
	}
	docs := dumpDocsOf(readDumpRecords(t, outPath), "app", "Jobs")
	if len(docs) != len(entries) {
		t.Fatalf("the dump holds %d doc records, want %d", len(docs), len(entries))
	}
	for index, doc := range docs {
		if want := entries[index].ID; doc.ID != want {
			t.Errorf("doc record %d is %q, want %q: the page is written in page order whatever the workers do", index+1, doc.ID, want)
		}
	}
}

// dumpParallelLargeStub scripts one List page of four entities: two short ones and two of
// 100,000 one-KiB elements each. The elements are generated by the stub's lazy page source,
// so the stub does not hold the 200 MiB it serves; that is what makes the measurement of
// the dump's own memory possible (section 3.3).
func dumpParallelLargeStub(t *testing.T) (*dumpStub, int) {
	t.Helper()
	element := dumpParallelElement(t, 1<<10)
	ids := []string{"small-0", "large-0", "small-1", "large-1"}
	entries := make([]ringclient.Entry, len(ids))
	for index, id := range ids {
		entries[index] = ringclient.Entry{ID: id, Body: dumpTestBody(t, id, id)}
	}
	stub := &dumpStub{
		databasePages: dumpStubDatabasePages([]ringclient.DatabaseSummary{{Name: "app", RF: 1}}, 1),
		bucketPages: map[string][]ringclient.BucketPage{
			"app": dumpStubBucketPages([]ringclient.BucketSummary{{Name: "Events", State: "active"}}, 1),
		},
		descriptions: map[string]ringclient.DatabaseDescription{"app": {Name: "app", RF: 1, Storage: "disk"}},
		declarations: map[string]ringclient.BucketInfo{
			"app/Events": {Database: "app", Name: "Events", State: "active", Fields: []ringclient.FieldInfo{
				{Name: dumpBucketIDField, Type: "string"},
				{Name: "Name", Type: "string"},
				{Name: "Blob", Type: dumpFifoStoreWord},
			}},
		},
		documentPages: map[string][]ringclient.EntryPage{"app/Events": {{Entries: entries}}},
	}
	stub.elementPageSource = func(key string, after []byte) (ringclient.ElementPage, error) {
		if strings.HasSuffix(key, "/small-0/Blob") || strings.HasSuffix(key, "/small-1/Blob") {
			return ringclient.ElementPage{Elements: []bson.Raw{element, element, element}}, nil
		}
		if !strings.HasSuffix(key, "/large-0/Blob") && !strings.HasSuffix(key, "/large-1/Blob") {
			return ringclient.ElementPage{}, fmt.Errorf("the lazy element source was asked for %s", key)
		}
		offset := 0
		if len(after) > 0 {
			if len(after) != 8 {
				return ringclient.ElementPage{}, fmt.Errorf("the lazy element source was asked for %s with a cursor of %d bytes", key, len(after))
			}
			offset = int(binary.BigEndian.Uint64(after))
		}
		count := min(dumpStorePageSize, dumpParallelLargeElements-offset)
		elements := make([]bson.Raw, count)
		for index := range elements {
			elements[index] = element
		}
		page := ringclient.ElementPage{Elements: elements, More: offset+count < dumpParallelLargeElements}
		if page.More {
			page.Cursor = binary.BigEndian.AppendUint64(nil, uint64(offset+count))
		}
		return page, nil
	}
	return stub, len(element)
}

// dumpParallelElement builds one store element whose payload is payloadBytes long, so the
// memory tests can compute what an element costs from the element itself.
func dumpParallelElement(t *testing.T, payloadBytes int) bson.Raw {
	t.Helper()
	body, err := bson.Marshal(bson.D{{Key: "Payload", Value: strings.Repeat("x", payloadBytes)}})
	if err != nil {
		t.Fatalf("building a %d-byte element: %v", payloadBytes, err)
	}
	return bson.Raw(body)
}

// TestLargeEntitiesUnderParallelTwoStayInsideTheFormula: with --parallel 2 and two entities
// of 100,000 one-KiB elements, the sampled peak stays inside the formula computed from the
// elements written, the spiller never holds more than two spill files, and none is left
// behind (section 3.3; size-gated with RINGCLI_LARGE_TESTS=1).
func TestLargeEntitiesUnderParallelTwoStayInsideTheFormula(t *testing.T) {
	if os.Getenv("RINGCLI_LARGE_TESTS") != "1" {
		t.Skip("set RINGCLI_LARGE_TESTS=1 to run the large-entity memory test")
	}
	stub, elementBytes := dumpParallelLargeStub(t)
	outDir := t.TempDir()
	outPath := filepath.Join(outDir, "large.hoarddump")
	spillLimit := int64(dumpSpillBytes)
	var spiller *dumpSpiller
	dumpSpillerCreated = func(created *dumpSpiller) { spiller = created }
	t.Cleanup(func() { dumpSpillerCreated = nil })

	peak := startDumpHeapPeak(t)
	code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--parallel", "2", "--out", outPath)
	measured := peak.finish(t)
	if code != ExitOK {
		t.Fatalf("dumping two 100,000-element entities with --parallel 2: exit %d, want %d\nstdout: %s\nstderr: %s",
			code, ExitOK, stdout, stderr)
	}
	parallel := int64(2)
	chunkBytes := int64(dumpChunkElements * elementBytes)
	pageBytes := int64(dumpStorePageSize * elementBytes)
	bound := parallel*(spillLimit+chunkBytes+2*pageBytes+int64(elementBytes)) + 16<<20
	entityBytes := int64(dumpParallelLargeElements * elementBytes)
	t.Logf("measured peak %d bytes, bound %d bytes, heap in use before the dump %d bytes (the test process's own share)",
		measured, bound, peak.baseline)
	if bound >= entityBytes/2 {
		t.Fatalf("the bound is %d bytes and one entity is %d: at least eight times smaller is what makes this test fail a whole-entity buffer",
			bound, entityBytes)
	}
	if measured >= bound {
		t.Errorf("the dump's peak was %d bytes and the formula allows %d: --parallel 2 is holding more than two buffers", measured, bound)
	}
	if spiller == nil {
		t.Fatalf("the run created no spiller, so no entity of it could spill")
	}
	if got := spiller.peak.Load(); got > parallel {
		t.Errorf("the run held %d spill files at once, want at most %d (one per parallel buffer)", got, parallel)
	}
	if live := spiller.live.Load(); live != 0 {
		t.Errorf("the run left %d spill files live, want 0", live)
	}
	if files := dumpSpillFiles(t, outDir); len(files) != 0 {
		t.Errorf("the dump left the spill files %v in %s", files, outDir)
	}
	verification, err := verifyDump(outPath)
	if err != nil {
		t.Fatalf("the dump does not pass the check restore --verify runs: %v", err)
	}
	if verification.Entities != 4 || verification.Elements != 200_006 {
		t.Errorf("the dump holds %d entities and %d elements, want 4 and 200,006", verification.Entities, verification.Elements)
	}
}

// TestTheMemoryMeasurementCatchesAWholeEntityBuffer proves that the measurement above can
// fail: the same run with dumpSpillBytes raised so that a buffer never spills holds whole
// entities in memory, and the sampled peak must then be over the formula's bound (section
// 3.3, R2-05). The bound is computed from the shipped spill limit, not the raised one, or
// raising the limit would raise the bound with it and the test would prove nothing.
func TestTheMemoryMeasurementCatchesAWholeEntityBuffer(t *testing.T) {
	if os.Getenv("RINGCLI_LARGE_TESTS") != "1" {
		t.Skip("set RINGCLI_LARGE_TESTS=1 to run the large-entity memory test")
	}
	stub, elementBytes := dumpParallelLargeStub(t)
	spillLimit := int64(dumpSpillBytes)
	previous := dumpSpillBytes
	dumpSpillBytes = 1 << 40
	t.Cleanup(func() { dumpSpillBytes = previous })

	peak := startDumpHeapPeak(t)
	code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--parallel", "2", "--out", filepath.Join(t.TempDir(), "whole.hoarddump"))
	measured := peak.finish(t)
	if code != ExitOK {
		t.Fatalf("dumping two entities with the spill limit raised: exit %d, want %d\nstdout: %s\nstderr: %s",
			code, ExitOK, stdout, stderr)
	}
	parallel := int64(2)
	chunkBytes := int64(dumpChunkElements * elementBytes)
	pageBytes := int64(dumpStorePageSize * elementBytes)
	bound := parallel*(spillLimit+chunkBytes+2*pageBytes+int64(elementBytes)) + 16<<20
	t.Logf("measured peak %d bytes with whole-entity buffers, bound %d bytes, heap in use before the dump %d bytes",
		measured, bound, peak.baseline)
	if measured <= bound {
		t.Errorf("holding two whole entities peaked at %d bytes, under the formula's %d: the measurement cannot fail, so the bound test above proves nothing",
			measured, bound)
	}
}

// TestOneMaximumElementEntityStaysInsideTheFormula: one entity whose fifoStore field holds
// one 20 MiB element, dumped with --parallel 1, stays inside the formula's "one maximum
// element" and chunk-cap terms (section 3.3). It cannot tell a whole-entity design apart,
// because one element is the entity; it checks the first-element rule and the formula.
func TestOneMaximumElementEntityStaysInsideTheFormula(t *testing.T) {
	const payloadBytes = 20 << 20
	element := dumpParallelElement(t, payloadBytes)
	stub := &dumpStub{
		databasePages: dumpStubDatabasePages([]ringclient.DatabaseSummary{{Name: "app", RF: 1}}, 1),
		bucketPages: map[string][]ringclient.BucketPage{
			"app": dumpStubBucketPages([]ringclient.BucketSummary{{Name: "Book", State: "active"}}, 1),
		},
		descriptions: map[string]ringclient.DatabaseDescription{"app": {Name: "app", RF: 1, Storage: "disk"}},
		declarations: map[string]ringclient.BucketInfo{
			"app/Book": {Database: "app", Name: "Book", State: "active", Fields: []ringclient.FieldInfo{
				{Name: dumpBucketIDField, Type: "string"},
				{Name: "Blob", Type: dumpFifoStoreWord},
			}},
		},
		documentPages: map[string][]ringclient.EntryPage{
			"app/Book": {{Entries: []ringclient.Entry{{ID: "b1", Body: dumpTestBody(t, "b1", "big")}}}},
		},
		elementPages: map[string][]ringclient.ElementPage{
			"app/Book/b1/Blob": {{Elements: []bson.Raw{element}}},
		},
	}
	outPath := filepath.Join(t.TempDir(), "maximum.hoarddump")

	peak := startDumpHeapPeak(t)
	code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--parallel", "1", "--out", outPath)
	measured := peak.finish(t)
	if code != ExitOK {
		t.Fatalf("dumping one 20 MiB element: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
	// dumpSpillBytes + the chunk cap + the page (which holds the one element) twice for the
	// reply and its decoded copy + the element + the stated margin.
	bound := int64(dumpSpillBytes+dumpChunkBytes+2*len(element)+len(element)) + 16<<20
	t.Logf("measured peak %d bytes, bound %d bytes, heap in use before the dump %d bytes (the test process's own share, the 20 MiB element included)",
		measured, bound, peak.baseline)
	if measured >= bound {
		t.Errorf("one 20 MiB element peaked at %d bytes and the formula allows %d", measured, bound)
	}
	chunks := dumpElemsOf(readDumpRecords(t, outPath), "app", "Book", "b1", "Blob")
	if len(chunks) != 1 || len(chunks[0].Els) != 1 {
		t.Fatalf("the dump holds %d elems records for Blob, want one record with the one element (the first element always goes in)", len(chunks))
	}
	if len(chunks[0].Els[0]) != len(element) {
		t.Errorf("the dumped element is %d bytes and the element was %d", len(chunks[0].Els[0]), len(element))
	}
}

// TestParallelFlagRefusesOutOfRangeValues: --parallel is 1 to 64, and a value outside it is
// a usage refusal naming the range and the fix, before anything is dialled or read
// (section 3.3, AGENTS.md rule 3). The help names the flag too.
func TestParallelFlagRefusesOutOfRangeValues(t *testing.T) {
	stub := &dumpStub{}
	for _, value := range []string{"0", "65", "-1"} {
		t.Run("parallel "+value, func(t *testing.T) {
			outPath := filepath.Join(t.TempDir(), "never.hoarddump")
			code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--parallel", value, "--out", outPath)
			if code != ExitUsage {
				t.Fatalf("--parallel %s: exit %d, want %d\nstdout: %s\nstderr: %s", value, code, ExitUsage, stdout, stderr)
			}
			want := "--parallel must be a number from 1 to 64, and got " + value + "."
			if !strings.Contains(stderr, want) {
				t.Errorf("--parallel %s does not say %q:\nstderr: %s", value, want, stderr)
			}
			if !strings.Contains(stderr, "Example: hoarddb dump --parallel 8.") {
				t.Errorf("--parallel %s does not name the fix:\nstderr: %s", value, stderr)
			}
			if !strings.Contains(stderr, "Run 'hoarddb dump --help' for the flags and examples.") {
				t.Errorf("--parallel %s does not name the help:\nstderr: %s", value, stderr)
			}
			if stdout != "" {
				t.Errorf("--parallel %s wrote to stdout: %q", value, stdout)
			}
			requireNoDumpFile(t, outPath)
			if calls := stub.callCounts(); calls.ShowDatabases != 0 {
				t.Errorf("--parallel %s asked the ring for %d database pages before refusing: a flag problem ends before the dial",
					value, calls.ShowDatabases)
			}
		})
	}

	code, stdout, stderr := runRingDumpForTest(t, "--help")
	if code != ExitOK {
		t.Fatalf("dump --help: exit %d, want %d\nstderr: %s", code, ExitOK, stderr)
	}
	for _, want := range []string{"--parallel", "how many entities of a bucket are read at once"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("dump --help does not say %q:\nstdout: %s", want, stdout)
		}
	}
}

// TestAtMostParallelBuffersAreLiveWhileTheCopierIsBusy holds the copier just before it copies
// the first entity, while the workers are free to run ahead, and checks that no more than
// --parallel buffers exist: the slot of an entity is released only after its copy. Every
// buffer spills at once (dumpSpillBytes is 1), so the spiller's peak is the number of
// buffers that were live at the same time. A build that released the slot before the copy
// would let the dispatcher start one more entity while the first is still being copied, and
// the peak would be --parallel + 1.
func TestAtMostParallelBuffersAreLiveWhileTheCopierIsBusy(t *testing.T) {
	const parallel = 2
	stub := dumpParallelFifoStub(t, 8, 1)
	previousSpillBytes := dumpSpillBytes
	dumpSpillBytes = 1
	t.Cleanup(func() { dumpSpillBytes = previousSpillBytes })
	var spiller *dumpSpiller
	dumpSpillerCreated = func(created *dumpSpiller) { spiller = created }
	t.Cleanup(func() { dumpSpillerCreated = nil })
	var held atomic.Bool
	dumpBeforeEntityCopy = func(index int) {
		if index != 0 || !held.CompareAndSwap(false, true) {
			return
		}
		// Give the dispatcher every chance to start more entities. The wait ends as soon as
		// one too many buffers exist; a correct build never gets there and waits it out.
		dumpWaitFor(t, "a third buffer", 500*time.Millisecond, func() bool { return spiller.peak.Load() > parallel })
	}
	t.Cleanup(func() { dumpBeforeEntityCopy = nil })
	outPath := filepath.Join(t.TempDir(), "bounded.hoarddump")

	code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--parallel", strconv.Itoa(parallel), "--out", outPath)
	if code != ExitOK {
		t.Fatalf("exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
	if peak := spiller.peak.Load(); peak > parallel {
		t.Errorf("%d buffers were live at once with --parallel %d: a slot was released before its entity was copied", peak, parallel)
	}
	if peak := spiller.peak.Load(); peak < parallel {
		t.Errorf("only %d buffers were ever live with --parallel %d: the test did not exercise the bound", peak, parallel)
	}
}

// TestAnErrorInTheMiddleOfAPageDoesNotDeadlock: an entity fails while later entities of the
// same page are waiting for a slot. The copier stops at the failure and must cancel the
// page, or the dispatcher waits for a slot nobody will release. The run is bounded by a
// watchdog, so a deadlock is a failure with a reason and not a hung test.
func TestAnErrorInTheMiddleOfAPageDoesNotDeadlock(t *testing.T) {
	stub := dumpParallelFifoStub(t, 8, 1)
	stub.failures = map[string]error{"StoreList app/Jobs/job-1/Queue": errors.New("the disk under the owner is gone")}
	outDir := t.TempDir()
	outPath := filepath.Join(outDir, "failed.hoarddump")
	finished := make(chan int, 1)
	go func() {
		code, _, _ := runStubDump(t, stub, "--db", "app", "--parallel", "2", "--out", outPath)
		finished <- code
	}()
	select {
	case code := <-finished:
		if code == ExitOK {
			t.Errorf("a dump whose entity could not be read exited %d", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the dump did not return within 10 seconds after an entity failed in the middle of a page: the page was not cancelled")
	}
	entries, err := os.ReadDir(outDir)
	if err != nil {
		t.Fatalf("reading %s: %v", outDir, err)
	}
	if len(entries) != 0 {
		t.Errorf("a failed dump left %d files in its directory", len(entries))
	}
}
