package server

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The bucket lock is an RWMutex: mutations take it exclusively, read-only paths
// take it shared. That distinction is not visible from a single request's
// result — a serialised read returns the same document as a concurrent one —
// but it is the difference between a node serving ~84K reads/s and the same
// node serving as many as its cores allow. So it is asserted directly here.

// TestReadPathsTakeTheReadLock holds the read lock and requires the documented
// read paths to still complete. A read path that took the write lock cannot
// make progress while this lock is held, so it fails on the timeout instead.
func TestReadPathsTakeTheReadLock(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)

	if _, err := s.Insert(InsertRequest{
		Database: "app",
		Bucket:   "users",
		Key:      "u1",
		Data:     map[string]interface{}{"n": 1},
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	db := s.serverDataAccessor().getDB("app")
	if db == nil {
		t.Fatal("database app not found")
	}
	bucket := db.getBucket("users")
	if bucket == nil {
		t.Fatal("bucket users not found")
	}

	bucket.mu.RLock()
	defer bucket.mu.RUnlock()

	readPaths := []struct {
		name string
		call func() error
	}{
		{"Find", func() error {
			resp, err := s.Find(FindRequest{Database: "app", Bucket: "users", Key: "u1"})
			if err != nil {
				return err
			}
			if !resp.Found {
				return fmt.Errorf("document u1 not found")
			}
			return nil
		}},
		{"Count", func() error {
			resp, err := s.Count(CountRequest{Database: "app", Bucket: "users"})
			if err != nil {
				return err
			}
			if resp.Count != 1 {
				return fmt.Errorf("count = %d, want 1", resp.Count)
			}
			return nil
		}},
		// A peek on a hash bucket is refused (BUG-0469: one wrong-type
		// refusal for every verb); the refusal must still not wait for the
		// write lock, which is what this table guards.
		{"Peek", func() error {
			_, err := s.Peek(PeekRequest{Database: "app", Bucket: "users"})
			if errors.Is(err, ErrWrongBucketType) {
				return nil
			}
			if err == nil {
				return fmt.Errorf("a peek on a hash bucket was answered, want the wrong-type refusal")
			}
			return err
		}},
	}

	for _, path := range readPaths {
		done := make(chan error, 1)
		go func(call func() error) { done <- call() }(path.call)

		select {
		case err := <-done:
			if err != nil {
				t.Errorf("%s failed while a shared read lock was held: %v", path.name, err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("%s blocked while a shared read lock was held: it is taking the write lock, "+
				"which serialises every read of a bucket inside one process", path.name)
		}
	}
}

// TestConcurrentReadsSeeEveryDocument runs many point reads of one bucket at
// once. Under an exclusive lock this still passes, but slowly; its job is to
// keep the shared read path exercised (it is the test that -race watches) and
// to catch a read path that starts returning empty results under concurrency.
func TestConcurrentReadsSeeEveryDocument(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)

	const (
		docs    = 200
		workers = 16
	)
	for i := 0; i < docs; i++ {
		key := fmt.Sprintf("u%03d", i)
		if _, err := s.Insert(InsertRequest{
			Database: "app",
			Bucket:   "users",
			Key:      key,
			Data:     map[string]interface{}{"key": key},
		}); err != nil {
			t.Fatalf("insert %s: %v", key, err)
		}
	}

	var (
		wg     sync.WaitGroup
		hits   int64
		misses int64
		errs   atomic.Int64
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < docs; i++ {
				key := fmt.Sprintf("u%03d", (i+worker)%docs)
				resp, err := s.Find(FindRequest{Database: "app", Bucket: "users", Key: key})
				if err != nil {
					errs.Add(1)
					return
				}
				if resp.Found {
					atomic.AddInt64(&hits, 1)
					continue
				}
				atomic.AddInt64(&misses, 1)
			}
		}(w)
	}
	wg.Wait()

	if n := errs.Load(); n != 0 {
		t.Errorf("%d concurrent reads returned an error", n)
	}
	if misses != 0 {
		t.Errorf("%d of %d concurrent reads missed a document that is stored", misses, workers*docs)
	}
	if want := int64(workers * docs); hits != want {
		t.Errorf("hits = %d, want %d", hits, want)
	}
}

// lockFreeReadDeadline bounds how long a read path may take while the bucket's
// lock is held by a writer. A read that queues on the lock never finishes
// inside it, so a generous bound costs nothing on a pass.
const lockFreeReadDeadline = 5 * time.Second

// readPath is one read entry point, run against a bucket whose lock a writer
// holds.
type readPath struct {
	name string
	call func() error
}

// runReadPathsUnderLock starts every path at once and reports each that has
// not finished by the shared deadline. It returns only when every path has
// returned, after unlock runs, so no read outlives the test's server.
func runReadPathsUnderLock(t *testing.T, paths []readPath, unlock func()) {
	t.Helper()
	finished := make([]chan error, len(paths))
	for i, path := range paths {
		finished[i] = make(chan error, 1)
		go func(call func() error, done chan<- error) { done <- call() }(path.call, finished[i])
	}
	deadline := time.After(lockFreeReadDeadline)
	blocked := make([]bool, len(paths))
	for i, path := range paths {
		select {
		case err := <-finished[i]:
			if err != nil {
				t.Errorf("%s failed while a writer held the bucket lock: %v", path.name, err)
			}
		case <-deadline:
			// Every later path has had the same five seconds.
			deadline = closedTimer()
			blocked[i] = true
			t.Errorf("%s did not finish within %v while a writer held the bucket lock: "+
				"it queues on inMemoryBucket.mu, so one writer stalls every read of the bucket (S-2)",
				path.name, lockFreeReadDeadline)
		}
	}
	unlock()
	for i := range paths {
		if blocked[i] {
			<-finished[i]
		}
	}
}

// closedTimer is a channel that is always ready, for the paths checked after
// the shared deadline has passed.
func closedTimer() <-chan time.Time {
	ready := make(chan time.Time, 1)
	ready <- time.Now()
	return ready
}

// TestReadPathsTakeNoBucketLock is T-1 (query-pipelining spec §3.5, R-2):
// with the bucket's lock held exclusively, as a writer holds it for the whole
// of its apply, every read that resolves through resolveForRead still
// completes. HEALTH_CHECK is not in the list: R-2 excludes it.
func TestReadPathsTakeNoBucketLock(t *testing.T) {
	s := newTestServer(t)
	createIndexedTestBucket(t, s, "app", "users", []interface{}{"Email"})
	createTestBucket(t, s, "app", "events", BucketTypeBTree)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("u%02d", i)
		insertScanDoc(t, s, "app", "users", key, map[string]interface{}{
			"Email": key + "@example.com", "group": "all", "n": int32(i),
		})
		insertScanDoc(t, s, "app", "events", fmt.Sprintf("e%02d", i), map[string]interface{}{"n": int32(i)})
	}

	hashPaths := []readPath{
		{"Find", func() error {
			resp, err := s.Find(FindRequest{Database: "app", Bucket: "users", Key: "u01"})
			if err == nil && !resp.Found {
				err = fmt.Errorf("document u01 not found")
			}
			return err
		}},
		{"Scan", func() error {
			resp, err := s.Scan(ScanRequest{Database: "app", Bucket: "users"})
			if err == nil && len(resp.Documents) != 10 {
				err = fmt.Errorf("scan returned %d documents, want 10 (%s)", len(resp.Documents), resp.Message)
			}
			return err
		}},
		{"Scan with a filter", func() error {
			resp, err := s.Scan(ScanRequest{Database: "app", Bucket: "users", Filter: map[string]interface{}{"group": "all"}})
			if err == nil && len(resp.Documents) != 10 {
				err = fmt.Errorf("filtered scan returned %d documents, want 10 (%s)", len(resp.Documents), resp.Message)
			}
			return err
		}},
		{"Count", func() error {
			resp, err := s.Count(CountRequest{Database: "app", Bucket: "users"})
			if err == nil && resp.Count != 10 {
				err = fmt.Errorf("count = %d, want 10", resp.Count)
			}
			return err
		}},
		{"Search", func() error {
			resp, err := s.Search(SearchRequest{Database: "app", Bucket: "users", Filter: map[string]interface{}{"Email": "u03@example.com"}})
			if err == nil && len(resp.Results) != 1 {
				err = fmt.Errorf("search returned %d results, want 1", len(resp.Results))
			}
			return err
		}},
		{"Distinct", func() error {
			resp, err := s.Distinct(DistinctRequest{Database: "app", Bucket: "users", Field: "group"})
			if err == nil && len(resp.Values) != 1 {
				err = fmt.Errorf("distinct returned %v, want one value", resp.Values)
			}
			return err
		}},
		{"BucketStats", func() error {
			resp, err := s.BucketStats(BucketStatsRequest{Database: "app", Bucket: "users"})
			if err == nil && resp.Entries != 10 {
				err = fmt.Errorf("bucket stats entries = %d, want 10", resp.Entries)
			}
			return err
		}},
	}
	btreePaths := []readPath{
		{"RangeQuery", func() error {
			resp, err := s.RangeQuery(BTreeRangeRequest{Database: "app", Bucket: "events", Low: "e00", High: "e09"})
			if err == nil && len(resp.Entries) != 10 {
				err = fmt.Errorf("range returned %d entries, want 10", len(resp.Entries))
			}
			return err
		}},
	}

	for _, held := range []struct {
		bucket string
		paths  []readPath
	}{{"users", hashPaths}, {"events", btreePaths}} {
		served, err := s.resolveBucket("app", held.bucket)
		if err != nil {
			t.Fatalf("resolving app.%s: %v", held.bucket, err)
		}
		served.rec.mu.Lock()
		runReadPathsUnderLock(t, held.paths, served.rec.mu.Unlock)
	}
}

// TestReadNotBlockedByMultiUpdate is T-2: a multi-document update holds the
// bucket's lock across its whole walk and its batch. A scan of the same bucket
// started while the update is inside its walk completes, and it reads the
// bucket as it was before the update.
func TestReadNotBlockedByMultiUpdate(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)
	const docCount = 20
	for i := 0; i < docCount; i++ {
		insertScanDoc(t, s, "app", "users", fmt.Sprintf("u%02d", i), map[string]interface{}{"group": "all", "state": "original"})
	}

	insideWalk := make(chan struct{})
	releaseWalk := make(chan struct{})
	hold := func(database, bucket string) {
		if database != "app" || bucket != "users" {
			return
		}
		close(insideWalk)
		<-releaseWalk
	}
	multiUpdateWalkHook.Store(&hold)
	t.Cleanup(func() { multiUpdateWalkHook.Store(nil) })

	updateDone := make(chan error, 1)
	go func() {
		resp, err := s.Update(UpdateRequest{
			Database: "app", Bucket: "users", Multi: true,
			Filter: map[string]interface{}{"group": "all"},
			Set:    map[string]interface{}{"state": "updated"},
		})
		if err == nil && !resp.Success {
			err = fmt.Errorf("multi-update refused: %s", resp.Message)
		}
		updateDone <- err
	}()
	select {
	case <-insideWalk:
	case <-time.After(lockFreeReadDeadline):
		close(releaseWalk)
		t.Fatal("the multi-update never reached its walk")
	}

	scanDone := make(chan *ScanResponse, 1)
	go func() {
		resp, err := s.Scan(ScanRequest{Database: "app", Bucket: "users"})
		if err != nil {
			t.Errorf("scan during the multi-update: %v", err)
		}
		scanDone <- resp
	}()
	var during *ScanResponse
	select {
	case during = <-scanDone:
	case <-time.After(lockFreeReadDeadline):
		t.Errorf("a scan did not finish within %v while a multi-update held the bucket inside its walk: "+
			"reads queue on inMemoryBucket.mu behind the whole update (S-2)", lockFreeReadDeadline)
	}
	close(releaseWalk)
	if err := <-updateDone; err != nil {
		t.Fatalf("multi-update: %v", err)
	}
	if during == nil {
		<-scanDone
		return
	}
	if len(during.Documents) != docCount {
		t.Fatalf("scan during the update returned %d documents, want %d (%s)", len(during.Documents), docCount, during.Message)
	}
	for _, doc := range during.Documents {
		if state := doc.Lookup("state").StringValue(); state != "original" {
			t.Errorf("scan during the update saw %s with state %q: a read must see the snapshot it opened, "+
				"from before the update's batch", doc.Lookup("_id_").StringValue(), state)
		}
	}

	after, err := s.Scan(ScanRequest{Database: "app", Bucket: "users"})
	if err != nil {
		t.Fatalf("scan after the update: %v", err)
	}
	for _, doc := range after.Documents {
		if state := doc.Lookup("state").StringValue(); state != "updated" {
			t.Errorf("after the update, %s has state %q, want updated", doc.Lookup("_id_").StringValue(), state)
		}
	}
}

// TestReadViewPublishRace is T-3, the -race guard for I-1: a bucket's type,
// slot and index declaration are written before its id is published, and a
// reader that sees the id reads them with no lock. Readers resolve each name
// throughout its create, so they see the struct unserved, then served. Run
// under -race; a reader that can see the id before the fields it guards is a
// data race, and a served view with no type is the same fault seen without
// the detector.
func TestReadViewPublishRace(t *testing.T) {
	s := newTestServer(t)
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase(app) = %+v, %v", resp, err)
	}
	const (
		bucketCount = 60
		readers     = 4
	)
	for i := 0; i < bucketCount; i++ {
		name := fmt.Sprintf("race%03d", i)
		btype := BucketTypeHash
		if i%2 == 1 {
			btype = BucketTypeBTree
		}
		stop := make(chan struct{})
		var wg sync.WaitGroup
		var served atomic.Int64
		for r := 0; r < readers; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					view, snap, err := s.resolveForRead("app", name)
					if err != nil {
						continue
					}
					gotType := view.bucketType
					snap.Discard()
					served.Add(1)
					if gotType != btype {
						t.Errorf("a served view of %s has type %q, want %q: the id was visible before the type", name, gotType, btype)
						return
					}
				}
			}()
		}
		resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: name, Type: btype})
		if err != nil || !resp.Success {
			close(stop)
			wg.Wait()
			t.Fatalf("CreateBucket(%s) = %+v, %v", name, resp, err)
		}
		// Let the readers see the bucket served before moving on.
		servedBy := time.Now().Add(lockFreeReadDeadline)
		for served.Load() < readers && time.Now().Before(servedBy) {
			runtime.Gosched()
		}
		if served.Load() == 0 {
			t.Errorf("no reader saw %s served within %v of its create", name, lockFreeReadDeadline)
		}
		close(stop)
		wg.Wait()
	}
}
