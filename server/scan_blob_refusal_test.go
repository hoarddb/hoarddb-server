package server

import "testing"

// TestScanOfBlobIsARefusalNotAnEmptyHit is BUG-0516 row 8, then BUG-0469: find
// on a blob bucket answered found:true with no documents and its refusal in
// message, which a driver that tests found read as an empty result. It is now
// an ERROR frame like every wrong-type refusal, so there is no found to misread.
func TestScanOfBlobIsARefusalNotAnEmptyHit(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "files", BucketTypeBlob)
	seedMatrixBucket(t, s, "files", BucketTypeBlob)

	_, err := s.Scan(ScanRequest{Database: "app", Bucket: "files"})
	requireWrongTypeRefusal(t, err, "does not scan blob buckets")
}
