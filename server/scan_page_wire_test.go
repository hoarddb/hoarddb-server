package server

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// fullWalkScanPage is the unfiltered scan as it answered before the page read: walk
// every stored document in natural order, keep the page, count the rest for
// Total. It is the reference the page read is held to, byte for byte on the
// wire, so the fix cannot change what a client receives.
func fullWalkScanPage(t *testing.T, s *Server, req ScanRequest) *ScanResponse {
	t.Helper()
	bucket, snap, err := s.resolveForRead(req.Database, req.Bucket)
	if err != nil {
		t.Fatalf("resolving %s.%s: %v", req.Database, req.Bucket, err)
	}
	defer snap.Discard()
	limit := req.Limit
	if limit <= 0 {
		limit = defaultScanLimit
	}
	skip := max(req.Skip, 0)
	pageEnd := int64(skip) + int64(limit)

	var documents []bson.Raw
	var total int64
	walkErr := walkBucketDocuments(bucket, req.Database, req.Bucket, func(rawEntry []byte) error {
		if total >= int64(skip) && total < pageEnd {
			documents = append(documents, bson.Raw(rawEntry))
		}
		total++
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walking %s.%s: %v", req.Database, req.Bucket, walkErr)
	}
	return &ScanResponse{
		Found: true, Database: req.Database, Bucket: req.Bucket,
		Documents: documents, Total: total, HasMore: pageEnd < total,
		IndexUsed: indexUsedFullScan, BucketID: int64(bucket.id.Load()),
	}
}

// writeScanWireBucket fills a bucket of btype with entries documents and then
// removes some of them without compaction, so the range holds delete markers
// and, for hash and btree, superseded versions: none of them may be returned
// or counted.
func writeScanWireBucket(t *testing.T, s *Server, bucket string, btype BucketType, entries int) {
	t.Helper()
	createTestBucket(t, s, "app", bucket, btype)
	if entries == 0 {
		return
	}
	for i := range entries {
		doc := map[string]interface{}{"_id_": fmt.Sprintf("e-%03d", i), "n": int64(i)}
		if btype == BucketTypeHeap {
			// Priorities repeat, so the heap's order has ties to keep stable.
			pushed, err := s.Push(PushRequest{Database: "app", Bucket: bucket, Priority: int64(i % 7), Data: doc})
			if err != nil || !pushed.Success {
				t.Fatalf("push %d into %s: %+v, %v", i, bucket, pushed, err)
			}
			continue
		}
		insertScanDoc(t, s, "app", bucket, fmt.Sprintf("e-%03d", i), map[string]interface{}{"n": int64(i)})
	}

	switch btype {
	case BucketTypeHash, BucketTypeBTree:
		for i := 0; i < entries; i += 5 {
			key := fmt.Sprintf("e-%03d", i)
			deleted, err := s.Delete(DeleteRequest{Database: "app", Bucket: bucket, Key: key})
			if err != nil || !deleted.Success {
				t.Fatalf("delete %s from %s: %+v, %v", key, bucket, deleted, err)
			}
		}
		for i := 1; i < entries; i += 3 {
			insertScanDoc(t, s, "app", bucket, fmt.Sprintf("e-%03d", i), map[string]interface{}{"n": int64(i), "rewritten": true})
		}
	default:
		for range min(7, entries) {
			popped, err := s.Pop(PopRequest{Database: "app", Bucket: bucket})
			if err != nil || !popped.Found {
				t.Fatalf("pop from %s: %+v, %v", bucket, popped, err)
			}
		}
		if btype == BucketTypeLIFO || btype == BucketTypeFIFO {
			// A push after pops leaves a hole in a lifo's positions.
			insertScanDoc(t, s, "app", bucket, "late", map[string]interface{}{"n": int64(-1)})
		}
	}
}

// TestUnfilteredScanWireOutputMatchesTheFullWalk holds every unfiltered page —
// each store type, empty, paged across, at the end, past the end — to the
// full walk's response, compared as the BSON a client receives.
func TestUnfilteredScanWireOutputMatchesTheFullWalk(t *testing.T) {
	s := newTestServer(t)
	types := []BucketType{BucketTypeHash, BucketTypeBTree, BucketTypeFIFO, BucketTypeLIFO, BucketTypeHeap}
	pages := []struct{ skip, limit int }{
		{0, 0}, {0, 20}, {20, 20}, {40, 20}, {44, 1}, {45, 20}, {0, 1000}, {1000, 20}, {-3, 5},
	}
	for _, btype := range types {
		for _, entries := range []int{0, 1, 57} {
			bucket := fmt.Sprintf("wire-%s-%d", btype, entries)
			writeScanWireBucket(t, s, bucket, btype, entries)

			// The pages that end exactly at the bucket's last entry, where
			// HasMore turns false, depend on what the writes left.
			live := int(fullWalkScanPage(t, s, ScanRequest{Database: "app", Bucket: bucket, Limit: 1}).Total)
			bucketPages := append(pages[:len(pages):len(pages)],
				struct{ skip, limit int }{max(live-1, 0), 1},
				struct{ skip, limit int }{max(live-20, 0), 20},
				struct{ skip, limit int }{0, max(live, 1)},
				struct{ skip, limit int }{live, 1})
			pagedTotal := 0
			for _, page := range bucketPages {
				req := ScanRequest{Database: "app", Bucket: bucket, Skip: page.skip, Limit: page.limit}
				got, err := s.Scan(req)
				if err != nil {
					t.Fatalf("%s: Scan(skip %d, limit %d): %v", bucket, page.skip, page.limit, err)
				}
				want := fullWalkScanPage(t, s, req)
				gotWire, err := bson.Marshal(got)
				if err != nil {
					t.Fatalf("%s: encoding the scan response: %v", bucket, err)
				}
				wantWire, err := bson.Marshal(want)
				if err != nil {
					t.Fatalf("%s: encoding the reference response: %v", bucket, err)
				}
				if !bytes.Equal(gotWire, wantWire) {
					t.Errorf("%s skip %d limit %d: the wire response differs from the full walk's\n got: %v\nwant: %v",
						bucket, page.skip, page.limit, bson.Raw(gotWire), bson.Raw(wantWire))
				}
				if page.limit == 20 && page.skip >= 0 {
					pagedTotal += len(got.Documents)
				}
			}
			if entries == 57 && pagedTotal == 0 {
				t.Errorf("%s: paging returned no documents at all", bucket)
			}
		}
	}
}

// TestUnfilteredScanIsOneSnapshotUnderConcurrentWrites races scans against
// pushes and pops. Total and the page come from one snapshot, so a page wide
// enough to hold the whole queue always holds exactly Total entries, in the
// queue's order; Total read from a second snapshot would disagree with the
// page under this race. Run it with -race.
func TestUnfilteredScanIsOneSnapshotUnderConcurrentWrites(t *testing.T) {
	s := newTestServer(t)
	for _, btype := range []BucketType{BucketTypeLIFO, BucketTypeFIFO} {
		t.Run(string(btype), func(t *testing.T) {
			bucket := "race-" + string(btype)
			createTestBucket(t, s, "app", bucket, btype)

			var stop atomic.Bool
			var pushes atomic.Int64
			var writers sync.WaitGroup
			writeErrs := make(chan error, 1)
			writers.Add(1)
			go func() {
				defer writers.Done()
				// The writer holds the queue near 200 entries, so a scan of
				// all of it stays well inside the result budget.
				held := 0
				for sequence := int64(0); !stop.Load(); sequence++ {
					pushed, err := s.Push(PushRequest{Database: "app", Bucket: bucket, Data: map[string]interface{}{"seq": sequence}})
					if err == nil && !pushed.Success {
						err = fmt.Errorf("push %d refused: %+v", sequence, pushed)
					}
					held++
					pushes.Add(1)
					if err == nil && (held > 200 || sequence%3 == 2) {
						_, err = s.Pop(PopRequest{Database: "app", Bucket: bucket})
						held--
					}
					if err != nil {
						writeErrs <- err
						return
					}
				}
			}()
			defer func() {
				stop.Store(true)
				writers.Wait()
				select {
				case err := <-writeErrs:
					t.Fatalf("writer: %v", err)
				default:
				}
			}()

			// Scan until the writer has pushed enough to have moved both ends of
			// the queue many times under the scans.
			for scan := 0; scan < 40 || pushes.Load() < 1000; scan++ {
				resp, err := s.Scan(ScanRequest{Database: "app", Bucket: bucket, Limit: 10_000})
				if err != nil {
					t.Fatalf("scan %d: %v", scan, err)
				}
				if int64(len(resp.Documents)) != resp.Total || resp.HasMore {
					t.Fatalf("scan %d: %d documents with Total %d and HasMore %v: the page and Total are from different snapshots",
						scan, len(resp.Documents), resp.Total, resp.HasMore)
				}
				var previous int64 = -1
				for position, doc := range decodedDocuments(t, resp.Documents) {
					sequence, _ := doc["seq"].(int64)
					ordered := previous < 0 || (btype == BucketTypeLIFO && sequence < previous) || (btype == BucketTypeFIFO && sequence > previous)
					if !ordered {
						t.Fatalf("scan %d: document %d has seq %d after %d, out of %s order", scan, position, sequence, previous, btype)
					}
					previous = sequence
				}
				short, err := s.Scan(ScanRequest{Database: "app", Bucket: bucket, Limit: 5})
				if err != nil {
					t.Fatalf("short scan %d: %v", scan, err)
				}
				if short.HasMore != (short.Total > 5) || int64(len(short.Documents)) != min(short.Total, 5) {
					t.Fatalf("short scan %d: %d documents, Total %d, HasMore %v: inconsistent page", scan, len(short.Documents), short.Total, short.HasMore)
				}
			}
		})
	}
}
