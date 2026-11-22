package server

import (
	"context"
	"regexp"
	"strings"
	"testing"
)

// namedCommand matches a db.<bucket>.<verb>( shape inside an error message.
var namedCommand = regexp.MustCompile(`db\.([A-Za-z0-9_]+)\.([a-z]+)\(`)

// TestBlobReadMissHintIsValidForBlobBucket pins the advice, not just the text:
// whatever command a blob-read miss names must be one the same server accepts
// on a blob bucket, and the miss must keep the shape clients classify as an
// absence (cli.foundFalse: `blob "` prefix and " not found in " infix).
func TestBlobReadMissHintIsValidForBlobBucket(t *testing.T) {
	const database, bucket = "app", "files"
	s := newTestServer(t)
	createTestBucket(t, s, database, bucket, BucketTypeBlob)

	miss, err := s.BlobRead(BlobReadRequest{Database: database, Bucket: bucket, Key: "nothing.txt"})
	if err != nil {
		t.Fatalf("BlobRead of a missing key: %v", err)
	}
	if miss.Found {
		t.Fatalf("BlobRead of a missing key reported Found: %+v", miss)
	}
	if !strings.HasPrefix(miss.Message, `blob "nothing.txt"`) || !strings.Contains(miss.Message, " not found in app.files") {
		t.Fatalf("miss lost the shape clients classify on: %q", miss.Message)
	}

	for _, command := range namedCommand.FindAllStringSubmatch(miss.Message, -1) {
		namedBucket, verb := command[1], command[2]
		if namedBucket != bucket {
			t.Errorf("miss names bucket %q, asked for %q: %q", namedBucket, bucket, miss.Message)
		}
		switch verb {
		case "count":
			countResp, countErr := s.CountCtx(context.Background(), CountRequest{Database: database, Bucket: bucket})
			if countErr != nil || countResp.Message != "" {
				t.Errorf("miss names db.%s.count() but the server refuses it: %+v, %v", bucket, countResp, countErr)
			}
		case "find":
			scanResp, scanErr := s.ScanCtx(context.Background(), ScanRequest{Database: database, Bucket: bucket})
			if scanErr == nil && scanResp.Message != "" {
				t.Errorf("miss names db.%s.find() but the server refuses it: %q (miss: %q)", bucket, scanResp.Message, miss.Message)
			}
		default:
			t.Errorf("miss names db.%s.%s(), a verb this test does not know how to run: %q", bucket, verb, miss.Message)
		}
	}
}
