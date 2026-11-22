package server

import (
	"testing"
)

// The btree and blob entry counts are cached atomics so Count() is O(1). These
// tests pin that the cache stays exact across upserts and deletes, because a
// count that drifts is worse than a slow count.
func TestBTreeCountExactAcrossUpsertAndDelete(t *testing.T) {
	s := newTestServer(t)
	defer func() { _ = s.Stop() }()
	createTypedTestBucket(t, s, "app", "t", BucketTypeBTree)

	for _, k := range []string{"a", "b", "c"} {
		if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "t", Key: k, Data: map[string]interface{}{"v": k}}); err != nil || !resp.Success {
			t.Fatalf("Insert(%s) = %+v, %v", k, resp, err)
		}
	}
	wantCount(t, s, "app", "t", 3)

	// Upserting an existing key must not grow the count.
	if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "t", Key: "a", Data: map[string]interface{}{"v": "a2"}}); err != nil || !resp.Success {
		t.Fatalf("upsert = %+v, %v", resp, err)
	}
	wantCount(t, s, "app", "t", 3)

	// Deleting one key decrements; deleting a missing key leaves it alone.
	if resp, err := s.Delete(DeleteRequest{Database: "app", Bucket: "t", Key: "b"}); err != nil || !resp.Success {
		t.Fatalf("Delete(b) = %+v, %v", resp, err)
	}
	wantCount(t, s, "app", "t", 2)
	if resp, err := s.Delete(DeleteRequest{Database: "app", Bucket: "t", Key: "missing"}); err != nil || resp.Success {
		t.Fatalf("Delete(missing) = %+v, %v, want the not-found answer", resp, err)
	}
	wantCount(t, s, "app", "t", 2)
}

func TestBlobCountExact(t *testing.T) {
	s := newTestServer(t)
	defer func() { _ = s.Stop() }()
	createTypedTestBucket(t, s, "app", "blobs", BucketTypeBlob)

	insert := func(key string) {
		t.Helper()
		if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "blobs", Key: key, Data: map[string]interface{}{"v": key}}); err != nil || !resp.Success {
			t.Fatalf("Insert(%s) = %+v, %v", key, resp, err)
		}
	}
	insert("k1")
	insert("k2")
	wantCount(t, s, "app", "blobs", 2)

	if resp, err := s.BlobWrite(BlobWriteRequest{Database: "app", Bucket: "blobs", Key: "k3", Data: []byte("hi")}); err != nil || !resp.Success {
		t.Fatalf("BlobWrite = %+v, %v", resp, err)
	}
	wantCount(t, s, "app", "blobs", 3)

	// Overwriting an existing key via insert must not grow the count.
	insert("k1")
	wantCount(t, s, "app", "blobs", 3)

	if resp, err := s.Delete(DeleteRequest{Database: "app", Bucket: "blobs", Key: "k2"}); err != nil || !resp.Success {
		t.Fatalf("Delete(k2) = %+v, %v", resp, err)
	}
	wantCount(t, s, "app", "blobs", 2)
}

// The hash store's count is the same cached-atomic design as btree's, and it
// had the opposite bug: every Put incremented, so an upsert grew the count and
// a crash restart (which replays the write-ahead log through Put) reported more
// entries than it held. Measured on a real node: 864 documents counted as 1727.
func TestHashCountExactAcrossUpsertAndDelete(t *testing.T) {
	s := newTestServer(t)
	defer func() { _ = s.Stop() }()
	createTypedTestBucket(t, s, "app", "t", BucketTypeHash)

	insert := func(k string) {
		t.Helper()
		if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "t", Key: k, Data: map[string]interface{}{"v": k}}); err != nil || !resp.Success {
			t.Fatalf("Insert(%s) = %+v, %v", k, resp, err)
		}
	}
	for _, k := range []string{"a", "b", "c"} {
		insert(k)
	}
	wantCount(t, s, "app", "t", 3)

	// The upsert case. This is the one that inflated a replayed node's count.
	insert("a")
	insert("a")
	wantCount(t, s, "app", "t", 3)

	// Deleting a key that is not there must not push the count negative.
	if resp, err := s.Delete(DeleteRequest{Database: "app", Bucket: "t", Key: "missing"}); err != nil || resp.Success {
		t.Fatalf("Delete(missing) = %+v, %v, want the not-found answer", resp, err)
	}
	wantCount(t, s, "app", "t", 3)

	if resp, err := s.Delete(DeleteRequest{Database: "app", Bucket: "t", Key: "b"}); err != nil || !resp.Success {
		t.Fatalf("Delete(b) = %+v, %v", resp, err)
	}
	wantCount(t, s, "app", "t", 2)

	// The same key twice in one batch is one entry, and a batch that overwrites
	// what is already there adds nothing.
	if resp, err := s.BatchInsert(BatchInsertRequest{
		Database: "app",
		Bucket:   "t",
		Data: []map[string]interface{}{
			{"_id_": "a", "v": "a"},
			{"_id_": "d", "v": "d"},
			{"_id_": "d", "v": "d"},
		},
	}); err != nil || !resp.Success {
		t.Fatalf("BatchInsert = %+v, %v", resp, err)
	}
	wantCount(t, s, "app", "t", 3)
}

func wantCount(t *testing.T, s *Server, db, bucket string, want int64) {
	t.Helper()
	c, err := s.Count(CountRequest{Database: db, Bucket: bucket})
	if err != nil {
		t.Fatalf("Count(%s.%s): %v", db, bucket, err)
	}
	if c.Count != want {
		t.Fatalf("Count(%s.%s) = %d, want %d", db, bucket, c.Count, want)
	}
}
