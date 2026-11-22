package server

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0554: a BLOB_WRITE with no data is the client's mistake, so it is
// answered INVALID_ARGUMENT with the unchanged message, not INTERNAL; the
// neighbouring refusals keep their codes.
func TestEmptyBlobWriteIsInvalidArgument(t *testing.T) {
	handler := NewHandler(newTestServer(t))
	handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "files", Type: BucketTypeBlob})
	handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "docs", Type: BucketTypeHash})

	tests := []struct {
		name     string
		request  BlobWriteRequest
		wantCode transport.ErrorCode
		wantText string
	}{
		{"empty data on a blob bucket", BlobWriteRequest{Database: "app", Bucket: "files", Key: "k", Data: []byte{}}, transport.ErrInvalidArgument,
			`no data to store — use db.files.put({_id_: "key", data: "..."})`},
		{"missing bucket stays not found", BlobWriteRequest{Database: "app", Bucket: "nope", Key: "k", Data: []byte{}}, transport.ErrNotFound, ""},
		{"non-blob bucket stays invalid argument", BlobWriteRequest{Database: "app", Bucket: "docs", Key: "k", Data: []byte{1}}, transport.ErrInvalidArgument, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			answer := readScanAnswer(t, sendOrderFrame(t, rootSession(), handler, transport.OpBlobWrite, mustMarshalBSON(t, tc.request)))
			if !answer.refused {
				t.Fatalf("answered %s, want an error frame", answer.opcode)
			}
			if answer.code != int(tc.wantCode) {
				t.Errorf("answered code %d (%q), want %d", answer.code, answer.message, tc.wantCode)
			}
			if tc.wantText != "" && answer.message != tc.wantText {
				t.Errorf("message %q, want %q", answer.message, tc.wantText)
			}
		})
	}
}
