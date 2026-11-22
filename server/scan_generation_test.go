package server

import (
	"context"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// scanOverTheWire sends req as an OpScan frame through dispatch and returns
// the decoded page, or the error frame's code and message when the server
// refused it. T-25 is about what a client sees, so it goes through the frame
// and not through Server.ScanCtx.
func scanOverTheWire(t *testing.T, s *Server, req ScanRequest) (page ScanResponse, refused *ErrorResponse) {
	t.Helper()
	payload, err := bson.Marshal(req)
	if err != nil {
		t.Fatalf("marshal scan request: %v", err)
	}
	frame, err := s.dispatch(context.Background(), &transport.Frame{Opcode: transport.OpScan, Payload: payload})
	if err != nil {
		t.Fatalf("dispatch scan: %v", err)
	}
	if frame.Opcode == transport.OpError {
		var errResp ErrorResponse
		if err := bson.Unmarshal(frame.Payload, &errResp); err != nil {
			t.Fatalf("decode error frame: %v", err)
		}
		return page, &errResp
	}
	if err := bson.Unmarshal(frame.Payload, &page); err != nil {
		t.Fatalf("decode scan response: %v", err)
	}
	return page, nil
}

func dropCommentsBucket(t *testing.T, s *Server) {
	t.Helper()
	dropped, err := s.DeleteBucket(DeleteBucketRequest{Database: "app", Name: "comments"})
	if err != nil || !dropped.Success {
		t.Fatalf("drop app.comments = %+v, %v", dropped, err)
	}
}

// recreateComments replaces app.comments with a different bucket of the same
// name holding a different set of keys.
func recreateComments(t *testing.T, s *Server) {
	t.Helper()
	dropCommentsBucket(t, s)
	createTestBucket(t, s, "app", "comments", BucketTypeHash)
	for _, key := range []string{"new-0", "new-1", "new-2", "new-3"} {
		insertScanDoc(t, s, "app", "comments", key, nil)
	}
}

const wantGenerationMessage = `Bucket "comments" in database "app" was dropped and re-created since the previous page ` +
	`(it is a different bucket now), so this page would not continue your scan. ` +
	`Start again from the first page: find({}, {limit: 100}).`

func seedCommentsBucket(t *testing.T) *Server {
	t.Helper()
	s := newTestServer(t)
	createTestBucket(t, s, "app", "comments", BucketTypeHash)
	for _, key := range []string{"old-0", "old-1", "old-2", "old-3"} {
		insertScanDoc(t, s, "app", "comments", key, nil)
	}
	return s
}

// TestPageAcrossDropAndRecreateFails is T-25(a): page two, carrying page
// one's bucket id, is refused with ErrBucketNotFound's wire code and the
// spec's sentence when the bucket was dropped and re-created between them.
func TestPageAcrossDropAndRecreateFails(t *testing.T) {
	s := seedCommentsBucket(t)
	first, refused := scanOverTheWire(t, s, ScanRequest{Database: "app", Bucket: "comments", Limit: 2})
	if refused != nil {
		t.Fatalf("page one refused: %+v", refused)
	}
	if first.BucketID == 0 {
		t.Fatal("page one carried no bucket_id: every scan response must name the bucket that served it")
	}

	secondRequest := ScanRequest{Database: "app", Bucket: "comments", Skip: 2, Limit: 2, ExpectBucketID: first.BucketID}
	if same, refused := scanOverTheWire(t, s, secondRequest); refused != nil || len(same.Documents) != 2 {
		t.Fatalf("page two of the same bucket = %+v, refused %+v; want the next two documents", same, refused)
	}

	recreateComments(t, s)
	_, refused = scanOverTheWire(t, s, secondRequest)
	if refused == nil {
		t.Fatal("page two of a re-created bucket was served as a continuation of page one")
	}
	if transport.ErrorCode(refused.Code) != transport.ErrNotFound {
		t.Errorf("wire code = %v, want %v", transport.ErrorCode(refused.Code), transport.ErrNotFound)
	}
	if refused.Message != wantGenerationMessage {
		t.Errorf("message =\n%q\nwant\n%q", refused.Message, wantGenerationMessage)
	}
}

// TestPageAfterDropWithoutRecreateFails is the other half of "or the name no
// longer resolves": the expectation turns a missing bucket into the same
// refusal rather than an empty Found:false page.
func TestPageAfterDropWithoutRecreateFails(t *testing.T) {
	s := seedCommentsBucket(t)
	first, refused := scanOverTheWire(t, s, ScanRequest{Database: "app", Bucket: "comments", Limit: 2})
	if refused != nil {
		t.Fatalf("page one refused: %+v", refused)
	}
	dropCommentsBucket(t, s)
	_, refused = scanOverTheWire(t, s, ScanRequest{Database: "app", Bucket: "comments", Skip: 2, Limit: 2, ExpectBucketID: first.BucketID})
	if refused == nil || transport.ErrorCode(refused.Code) != transport.ErrNotFound {
		t.Fatalf("page two of a dropped bucket = refused %+v, want a not-found error frame", refused)
	}
}

// TestScanWithoutExpectationIsUnchanged is T-25(c), the compatibility half:
// a client that never heard of expect_bucket_id is served the new bucket's
// rows, exactly as before, and the response now names the new bucket.
func TestScanWithoutExpectationIsUnchanged(t *testing.T) {
	s := seedCommentsBucket(t)
	first, refused := scanOverTheWire(t, s, ScanRequest{Database: "app", Bucket: "comments", Limit: 2})
	if refused != nil {
		t.Fatalf("page one refused: %+v", refused)
	}

	recreateComments(t, s)
	second, refused := scanOverTheWire(t, s, ScanRequest{Database: "app", Bucket: "comments", Skip: 2, Limit: 2})
	if refused != nil {
		t.Fatalf("a scan without expect_bucket_id was refused: %+v", refused)
	}
	if !second.Found || len(second.Documents) != 2 {
		t.Fatalf("page two = found %v with %d documents, want the new bucket's last two", second.Found, len(second.Documents))
	}
	if second.BucketID == 0 || second.BucketID == first.BucketID {
		t.Errorf("bucket_id = %d after a re-create, want a non-zero id different from %d", second.BucketID, first.BucketID)
	}
}
