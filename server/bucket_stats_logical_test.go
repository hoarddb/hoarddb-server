package server

import (
	"bytes"
	"context"
	"testing"
)

// TestBucketStatsReportsLogicalSizeAndNoPaths is T-13 at the server: after a
// known number of key and value bytes are written, BucketStats reports exactly
// that logical size (spec §5.4, Q22). The disk-block count and the path the
// response used to carry are gone from the type: the node has one store.
func TestBucketStatsReportsLogicalSizeAndNoPaths(t *testing.T) {
	const (
		database, bucket = "app", "files"
		key              = "report"
		payloadBytes     = 1000
	)
	s := newTestServer(t)
	createTestBucket(t, s, database, bucket, BucketTypeBlob)
	writeBlobThroughDispatch(t, s, context.Background(), database, bucket, key, bytes.Repeat([]byte{'x'}, payloadBytes))

	stats, err := s.BucketStats(BucketStatsRequest{Database: database, Bucket: bucket})
	if err != nil {
		t.Fatalf("BucketStats: %v", err)
	}
	if want := int64(len(key) + payloadBytes); stats.LogicalBytes != want {
		t.Errorf("LogicalBytes = %d, want %d: the key and the payload, before compression and compaction", stats.LogicalBytes, want)
	}
}
