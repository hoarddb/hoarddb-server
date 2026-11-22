package server

import (
	"context"
	"testing"
	"time"
)

// listedRow is the row of bucket in the listing of database app.
func listedRow(t *testing.T, s *Server, bucket string) BucketInfo {
	t.Helper()
	listed, err := s.ListBuckets(ListBucketsRequest{Database: "app"})
	if err != nil {
		t.Fatalf("list buckets: %v", err)
	}
	for _, row := range listed.Buckets {
		if row.Name == bucket {
			return row
		}
	}
	t.Fatalf("list buckets has no row for %s", bucket)
	return BucketInfo{}
}

// TestLenAndPeekReportPendingAndDead (claim-ack-spec 8.1): peek, an unfiltered
// count, BUCKET_INFO and the bucket's row in the listing all say how many
// entries are claimed and how many are dead, and the number len() answers stays
// the entries available to take.
func TestLenAndPeekReportPendingAndDead(t *testing.T) {
	s, _ := leaseServer(t)
	for _, bucketType := range leaseBucketTypes {
		bucket := string(bucketType)
		claimOf(t, s, bucket, 2, 0)
		restoredDead(t, s, bucket, bucketType, 1, "")

		peeked, err := s.Peek(PeekRequest{Database: "app", Bucket: bucket, Limit: 10})
		if err != nil || len(peeked.Items) != 3 || peeked.Pending != 2 || peeked.Dead != 1 || peeked.Expired {
			t.Errorf("%s: peek = %+v, %v; want 3 items, pending 2, dead 1", bucket, peeked, err)
		}
		counted, err := s.CountCtx(context.Background(), CountRequest{Database: "app", Bucket: bucket})
		if err != nil || counted.Count != 3 || counted.Pending != 2 || counted.Dead != 1 || counted.Expired {
			t.Errorf("%s: count = %+v, %v; want 3, pending 2, dead 1", bucket, counted, err)
		}
		info, err := s.BucketInfo(BucketInfoRequest{Database: "app", Bucket: bucket})
		if err != nil || info.Bucket.Entries != 3 || info.Bucket.Pending != 2 || info.Bucket.Dead != 1 {
			t.Errorf("%s: bucket info = %+v, %v; want entries 3, pending 2, dead 1", bucket, info, err)
		}
		if row := listedRow(t, s, bucket); row.Entries != 3 || row.Pending != 2 || row.Dead != 1 {
			t.Errorf("%s: the listing row = %+v; want entries 3, pending 2, dead 1", bucket, row)
		}
	}
	filtered, err := s.CountCtx(context.Background(), CountRequest{Database: "app", Bucket: "fifo", Filter: map[string]interface{}{"n": int32(5)}})
	if err != nil || filtered.Pending != 0 || filtered.Dead != 0 {
		t.Errorf("a filtered count = %+v, %v; want no claimed or dead members", filtered, err)
	}
}

// TestPopResponseCarriesPendingAndDead: a pop of a bucket whose entries are all
// claimed answers empty and says how many are claimed; a pop that takes entries
// says the counts after it.
func TestPopResponseCarriesPendingAndDead(t *testing.T) {
	s, _ := leaseServer(t)
	for _, bucketType := range leaseBucketTypes {
		bucket := string(bucketType)
		claimOf(t, s, bucket, 5, 0)
		restoredDead(t, s, bucket, bucketType, 2, "")
		popped, err := s.Pop(PopRequest{Database: "app", Bucket: bucket})
		if err != nil || popped.Found || popped.Pending != 5 || popped.Dead != 2 {
			t.Errorf("%s: pop of an all-claimed bucket = %+v, %v; want empty with pending 5, dead 2", bucket, popped, err)
		}
	}
	other := "mixed"
	createTestBucket(t, s, "app", other, BucketTypeFIFO)
	pushPeekEntries(t, s, other, 3, "")
	claimOf(t, s, other, 1, 0)
	popped, err := s.Pop(PopRequest{Database: "app", Bucket: other, Limit: 1})
	if err != nil || !popped.Found || popped.Pending != 1 {
		t.Errorf("pop beside a claim = %+v, %v; want the entry and pending 1", popped, err)
	}
}

// TestPeekAfterExpiryShowsWhatPopTakes (claim-ack-spec 8.2): once the soonest
// lease has ended by the serving node's clock, peek and count say expired, and
// the next pop returns the expired entries first.
func TestPeekAfterExpiryShowsWhatPopTakes(t *testing.T) {
	s, clock := leaseServer(t)
	for _, engine := range peekEngines {
		bucket := string(engine.bucketType)
		held := claimOf(t, s, bucket, 2, 1000)
		before, err := s.Peek(PeekRequest{Database: "app", Bucket: bucket, Limit: 10})
		if err != nil || before.Expired {
			t.Fatalf("%s: peek during the lease = %+v, %v; want not expired", bucket, before, err)
		}
		clock.advance(5 * time.Second)
		after, err := s.Peek(PeekRequest{Database: "app", Bucket: bucket, Limit: 10})
		if err != nil || !after.Expired || after.Pending != 2 {
			t.Fatalf("%s: peek after the deadline = %+v, %v; want expired with pending 2", bucket, after, err)
		}
		if counted, err := s.CountCtx(context.Background(), CountRequest{Database: "app", Bucket: bucket}); err != nil || !counted.Expired {
			t.Errorf("%s: count after the deadline = %+v, %v; want expired", bucket, counted, err)
		}
		popped, err := s.Pop(PopRequest{Database: "app", Bucket: bucket, Limit: 5})
		if err != nil || len(popped.Items) != 5 {
			t.Fatalf("%s: the pop after expiry = %+v, %v; want all five", bucket, popped, err)
		}
		if got := peekedNumbers(t, popped.Items); len(got) != 5 || got[0] != engine.popOrder[0] {
			t.Errorf("%s: the pop took %v, want the returned entries back in pop order %v", bucket, got, engine.popOrder)
		}
		_ = held
	}
}
