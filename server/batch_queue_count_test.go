package server

import (
	"context"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// TestBatchPutOnQueueReportsWhatItStored is BUG-0516 row 4: a BATCH_PUT of N
// documents to a fifo or lifo pushed them and answered count 0 and no ids, so a
// client that retried on "nothing stored" pushed every document twice.
func TestBatchPutOnQueueReportsWhatItStored(t *testing.T) {
	for _, bucketType := range []BucketType{BucketTypeFIFO, BucketTypeLIFO} {
		t.Run(string(bucketType), func(t *testing.T) {
			s := newTestServer(t)
			createTestBucket(t, s, "app", "q", bucketType)

			resp, err := s.BatchInsertCtx(context.Background(), BatchInsertRequest{Database: "app", Bucket: "q",
				Data: []map[string]interface{}{
					{"_id_": "chosen", "n": int32(1)},
					{"n": int32(2)},
					{"n": int32(3)},
				}})
			if err != nil || !resp.Success {
				t.Fatalf("batch put to a %s: %+v, %v", bucketType, resp, err)
			}

			peeked, err := s.Peek(PeekRequest{Database: "app", Bucket: "q", Limit: 10})
			if err != nil {
				t.Fatalf("peeking the %s: %v", bucketType, err)
			}
			if resp.Count != int32(len(peeked.Items)) || resp.Count != 3 {
				t.Errorf("the batch answered count %d; the %s holds %d documents, and 3 were sent", resp.Count, bucketType, len(peeked.Items))
			}
			if len(resp.IDs) != 3 {
				t.Fatalf("the batch answered %d ids, want 3", len(resp.IDs))
			}
			if resp.IDs[0] != "chosen" {
				t.Errorf("the first id is %q, want the caller's _id_ %q", resp.IDs[0], "chosen")
			}
			stored := make(map[string]bool, len(peeked.Items))
			for _, item := range peeked.Items {
				var element struct {
					ID string `bson:"_id_"`
				}
				if err := bson.Unmarshal(item, &element); err != nil {
					t.Fatalf("decoding a stored element: %v", err)
				}
				stored[element.ID] = true
			}
			for _, id := range resp.IDs {
				if !stored[id] {
					t.Errorf("the batch reported id %q, which is not the _id_ of any stored element %v", id, stored)
				}
			}
		})
	}
}
