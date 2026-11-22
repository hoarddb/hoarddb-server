package server

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0467 G11: a batch over the store's atomic bound is an ERROR frame with
// its own code, BATCH_TOO_LARGE, so a driver halves the batch on the code and
// not on a phrase of the message. These tests fail before the fix: the answer
// was BATCH_PUT_OK with success false and no code.

// overBoundBatchFrame is a BATCH_PUT of count documents of bodyBytes each.
func overBoundBatchFrame(t *testing.T, bucket string, count, bodyBytes int) *transport.Frame {
	t.Helper()
	body := strings.Repeat("x", bodyBytes)
	documents := make([]map[string]interface{}, count)
	for i := range documents {
		documents[i] = map[string]interface{}{"_id_": fmt.Sprintf("k%06d", i), "body": body}
	}
	return transport.NewFrame(transport.OpBatchPut, mustMarshalBSON(t, BatchInsertRequest{Database: "app", Bucket: bucket, Data: documents}))
}

// requireBatchTooLarge checks the one shape every over-bound batch has.
func requireBatchTooLarge(t *testing.T, refusal ErrorResponse, what string, wantInMessage ...string) {
	t.Helper()
	requireOutcome(t, refusal, what, outcomeNotApplied, transport.ErrBatchTooLarge)
	for _, want := range append(wantInMessage, "none of it was written", "Send fewer", "limitations page") {
		if !strings.Contains(refusal.Message, want) {
			t.Errorf("%s: message %q does not contain %q", what, refusal.Message, want)
		}
	}
}

func TestOverBoundBatchByBytesIsBatchTooLarge(t *testing.T) {
	for _, engine := range []BucketType{BucketTypeHash, BucketTypeBTree, BucketTypeFIFO, BucketTypeLIFO, BucketTypeHeap} {
		t.Run(string(engine), func(t *testing.T) {
			s := newTestServerFloorMemtable(t)
			createTestBucket(t, s, "app", "big", engine)
			handler := NewHandler(s)

			refusal := refusalFrom(t, handler, overBoundBatchFrame(t, "big", 60, 100*1024), "a 60 x 100 KiB batch")
			// The bound it names, and the size it was: documents and encoded bytes.
			requireBatchTooLarge(t, refusal, "a 60 x 100 KiB batch",
				groupThousands(int(s.kv.MaxBatchBytes())), "60 documents", "MiB encoded")
		})
	}
}

func TestOverBoundBatchByEntryCountIsBatchTooLarge(t *testing.T) {
	s := newTestServerFloorMemtable(t)
	createTestBucket(t, s, "app", "tiny", BucketTypeHash)
	handler := NewHandler(s)

	refusal := refusalFrom(t, handler, overBoundBatchFrame(t, "tiny", 60000, 0), "60,000 small documents")
	requireBatchTooLarge(t, refusal, "60,000 small documents", "entry-count", "60,000 documents")
}
