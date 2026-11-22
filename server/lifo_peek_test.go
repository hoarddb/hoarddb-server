package server

import (
	"fmt"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// TestLIFOPeekAfterPushes pins the contract a queue bucket has: after N pushes,
// a peek returns the most recently pushed document, and never reports empty.
// Written after a store-type benchmark found a LIFO bucket reporting itself
// empty while the equivalent FIFO bucket did not.
func TestLIFOPeekAfterPushes(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "q", BucketTypeLIFO)

	// Kept small on purpose: the store-level peek order, deletes and all, is
	// store/queue_test.go:TestQueuePeekDoesNotConsume's.
	const pushes = 200
	for i := 1; i <= pushes; i++ {
		resp, err := s.Push(PushRequest{
			Database: "app",
			Bucket:   "q",
			Data:     map[string]interface{}{"n": i},
		})
		if err != nil || !resp.Success {
			t.Fatalf("push %d: %+v %v", i, resp, err)
		}

		peek, err := s.Peek(PeekRequest{Database: "app", Bucket: "q"})
		if err != nil {
			t.Fatalf("peek after %d pushes: %v", i, err)
		}
		if !peek.Found {
			t.Fatalf("peek after %d pushes reports empty: %s", i, peek.Message)
		}
		var doc map[string]interface{}
		if err := bson.Unmarshal(peek.Items[0], &doc); err != nil {
			t.Fatalf("decode peeked document: %v", err)
		}
		// BSON decodes small integers as int32, so compare as text rather than
		// asserting a Go integer type the wire format does not promise.
		if got := fmt.Sprint(doc["n"]); got != fmt.Sprint(i) {
			t.Fatalf("peek after %d pushes returned n=%s (LIFO peek must return the newest entry)", i, got)
		}
	}
}
