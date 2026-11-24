package server

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
)

// committedLIFOOrder is the bucket's entries in commit order — oldest first —
// read straight from the storage keys. A queue entry's key ends in the
// position Apply gave it while holding the bucket's write lock, so ascending
// key order is the order the pushes committed in, and it does not pass through
// readPage, which is the reader this test judges.
func committedLIFOOrder(t *testing.T, s *Server, database, bucket string) []string {
	t.Helper()
	b := lookupBucket(s, database, bucket)
	if b == nil {
		t.Fatalf("bucket %s/%s is not served", database, bucket)
	}
	b.rec.mu.RLock()
	slot, databaseID, id := b.slot, b.databaseID, b.id.Load()
	b.rec.mu.RUnlock()

	lower := storage.Key(slot, databaseID, id, storage.NSQueue)
	upper := append([]byte(nil), lower...)
	upper[len(upper)-1]++

	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: lower, Upper: upper})
	defer it.Close()
	var committed []string
	for it.Seek(lower); it.Valid(); it.Next() {
		entry, err := it.Value()
		if err != nil {
			t.Fatalf("reading the entry at key %x: %v", it.Key(), err)
		}
		var fields struct {
			ID string `bson:"_id_"`
		}
		if err := bson.Unmarshal(entry, &fields); err != nil {
			t.Fatalf("decoding the entry at key %x: %v", it.Key(), err)
		}
		committed = append(committed, fields.ID)
	}
	return committed
}

// scanIDs returns the _id_ of every document of one scan page, newest first.
func scanIDs(t *testing.T, s *Server, request ScanRequest) ([]string, int64) {
	t.Helper()
	page, err := s.Scan(request)
	if err != nil {
		t.Errorf("Scan(skip %d, limit %d): %v", request.Skip, request.Limit, err)
		return nil, 0
	}
	pageIDs := make([]string, 0, len(page.Documents))
	for _, document := range decodedDocuments(t, page.Documents) {
		pageIDs = append(pageIDs, document["_id_"].(string))
	}
	return pageIDs, page.Total
}

// TestLIFOConcurrentPushesReadBackInCommitOrder settles whether a scan of a
// LIFO bucket can read its entries out of order when many clients push at
// once, the way a social-feed seed burst does. Pushers race on one bucket
// while scanners read pages; afterwards every page, full and skipped, must be
// the reverse of the order the storage keys record, with nothing missing or
// repeated, and every page read during the pushes must be a consistent prefix:
// a run of the newest entries of some moment, whose Total names that moment.
func TestLIFOConcurrentPushesReadBackInCommitOrder(t *testing.T) {
	const (
		pushers        = 128
		pushesEach     = 3
		scanners       = 4
		scanPageLength = 20
	)
	s := newTestServer(t)
	createTestBucket(t, s, "app", "timeline", BucketTypeLIFO)

	type observedPage struct {
		ids   []string
		total int64
	}
	var (
		pushedIDs sync.Map
		observed  [scanners][]observedPage
		pushing   sync.WaitGroup
		scanning  sync.WaitGroup
		finished  atomic.Bool
		start     = make(chan struct{})
	)
	for writer := 0; writer < pushers; writer++ {
		pushing.Add(1)
		go func(writer int) {
			defer pushing.Done()
			<-start
			for n := 0; n < pushesEach; n++ {
				pushed, err := s.Push(PushRequest{Database: "app", Bucket: "timeline", Data: map[string]interface{}{"writer": writer, "n": n}})
				if err != nil || !pushed.Success {
					t.Errorf("writer %d push %d: %+v, %v", writer, n, pushed, err)
					return
				}
				pushedIDs.Store(pushed.ID, struct{}{})
			}
		}(writer)
	}
	for scanner := 0; scanner < scanners; scanner++ {
		scanning.Add(1)
		go func(scanner int) {
			defer scanning.Done()
			<-start
			for !finished.Load() {
				ids, total := scanIDs(t, s, ScanRequest{Database: "app", Bucket: "timeline", Limit: scanPageLength})
				observed[scanner] = append(observed[scanner], observedPage{ids: ids, total: total})
			}
		}(scanner)
	}
	close(start)
	pushing.Wait()
	finished.Store(true)
	scanning.Wait()

	committed := committedLIFOOrder(t, s, "app", "timeline")
	if len(committed) != pushers*pushesEach {
		t.Fatalf("storage holds %d entries after %d pushes", len(committed), pushers*pushesEach)
	}
	committedPosition := make(map[string]int, len(committed))
	for position, id := range committed {
		if _, repeated := committedPosition[id]; repeated {
			t.Fatalf("entry %q is stored twice", id)
		}
		if _, acknowledged := pushedIDs.Load(id); !acknowledged {
			t.Fatalf("entry %q is stored but no push acknowledged it", id)
		}
		committedPosition[id] = position
	}
	newestFirst := make([]string, len(committed))
	for position, id := range committed {
		newestFirst[len(committed)-1-position] = id
	}

	assertSlice := func(label string, got []string, skip, limit int) {
		t.Helper()
		want := newestFirst[min(skip, len(newestFirst)):min(skip+limit, len(newestFirst))]
		if len(got) != len(want) {
			t.Fatalf("%s: %d entries, want %d", label, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("%s: entry %d is %q (commit position %d), want %q (commit position %d)",
					label, i, got[i], committedPosition[got[i]], want[i], committedPosition[want[i]])
			}
		}
	}
	fullPage, total := scanIDs(t, s, ScanRequest{Database: "app", Bucket: "timeline", Limit: len(committed)})
	assertSlice("full page", fullPage, 0, len(committed))
	if total != int64(len(committed)) {
		t.Fatalf("Total = %d, want %d", total, len(committed))
	}
	for skip := 0; skip < len(committed); skip += scanPageLength {
		pageIDs, _ := scanIDs(t, s, ScanRequest{Database: "app", Bucket: "timeline", Skip: skip, Limit: scanPageLength})
		assertSlice(fmt.Sprintf("page at skip %d", skip), pageIDs, skip, scanPageLength)
	}

	for scanner := range observed {
		for _, page := range observed[scanner] {
			if len(page.ids) == 0 {
				if page.total != 0 {
					t.Fatalf("an empty page reports Total %d", page.total)
				}
				continue
			}
			newest := committedPosition[page.ids[0]]
			if page.total != int64(newest+1) {
				t.Fatalf("a page read during the pushes starts at commit position %d but reports Total %d: the page and its count are from different moments", newest, page.total)
			}
			if want := min(scanPageLength, newest+1); len(page.ids) != want {
				t.Fatalf("a page read during the pushes holds %d entries, want %d", len(page.ids), want)
			}
			for i, id := range page.ids {
				if want := committed[newest-i]; id != want {
					t.Fatalf("a page read during the pushes is not a consistent prefix: entry %d is %q (commit position %d), want %q (commit position %d)",
						i, id, committedPosition[id], want, newest-i)
				}
			}
		}
	}
}
