package server

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// pushHeapElements pushes one element per priority, each tagged with its
// priority so a pop can say which one it took.
func pushHeapElements(t *testing.T, s *Server, database, bucket string, priorities []int64, firstSeq uint64) {
	t.Helper()
	for i, priority := range priorities {
		ctx := context.Background()
		if firstSeq != 0 {
			ctx = withApplySeq(ctx, firstSeq+uint64(i))
		}
		resp, err := s.PushCtx(ctx, PushRequest{Database: database, Bucket: bucket, Priority: priority,
			Data: map[string]interface{}{"priority": priority}})
		if err != nil || !resp.Success {
			t.Fatalf("push priority %d: %+v, %v", priority, resp, err)
		}
	}
}

// popHeapThroughDispatch pops through the opcode handler, the path a live
// client and WAL replay both take, and returns the popped element's priority
// and whether anything was popped.
func popHeapThroughDispatch(t *testing.T, s *Server, ctx context.Context, database, bucket string) (int64, bool) {
	t.Helper()
	payload, err := bson.Marshal(PopRequest{Database: database, Bucket: bucket})
	if err != nil {
		t.Fatalf("encoding pop: %v", err)
	}
	frame, err := s.dispatch(ctx, &transport.Frame{Opcode: transport.OpHeapPop, Payload: payload})
	if err != nil {
		t.Fatalf("dispatching pop: %v", err)
	}
	// An empty heap's PopResponse{Found: false} does not marshal (its nil
	// bson.Raw), and handleHeapPop discards the error, so "nothing popped"
	// arrives as a zero-length payload. That is a defect of its own, reported
	// with this step; here it only means nothing was popped.
	if len(frame.Payload) == 0 {
		return 0, false
	}
	var resp PopResponse
	if err := bson.Unmarshal(frame.Payload, &resp); err != nil {
		t.Fatalf("decoding pop response: %v", err)
	}
	if !resp.Found {
		return 0, false
	}
	var element struct {
		Priority int64 `bson:"priority"`
	}
	if err := bson.Unmarshal(resp.Items[0], &element); err != nil {
		t.Fatalf("decoding popped element: %v", err)
	}
	return element.Priority, true
}

// TestHeapBucketLivesOnTheNodeStore is build doc Step 11 at the server: a heap
// bucket opens no Badger instance of its own, and after a restart — with no
// in-memory heap to rebuild — its pops still come highest priority first,
// negatives included, straight off the node store's key order (spec §3.3
// rev 5.2.2).
func TestHeapBucketLivesOnTheNodeStore(t *testing.T) {
	const database, bucket = "app", "jobs"
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	createTestBucket(t, first.core, database, bucket, BucketTypeHeap)
	pushHeapElements(t, first.core, database, bucket, []int64{-1, 3, -5, 0}, 0)

	ownDir := filepath.Join(dataDir, database, bucket, "badger")
	if _, err := os.Stat(ownDir); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a heap bucket has its own Badger directory %s (stat: %v); it must live on the node's store", ownDir, err)
	}

	second := restartCatalog(t, first, dataDir)
	var popped []int64
	for {
		priority, found := popHeapThroughDispatch(t, second.core, context.Background(), database, bucket)
		if !found {
			break
		}
		popped = append(popped, priority)
	}
	if want := []int64{3, 0, -1, -5}; !reflect.DeepEqual(popped, want) {
		t.Fatalf("after a restart the heap popped %v, want highest first %v", popped, want)
	}
}

// TestHeapReplayedPopIsGated is spec §4.4 rule 2 for a heap: a replayed pop at
// or below the bucket's applied_seq removes nothing. Before the heap moved
// onto the node store a replayed pop took whatever was first now — a second
// element the original pop never touched.
func TestHeapReplayedPopIsGated(t *testing.T) {
	const database, bucket = "app", "gated-heap"
	s := newTestServer(t)
	createTestBucket(t, s, database, bucket, BucketTypeHeap)
	pushHeapElements(t, s, database, bucket, []int64{1, 2}, 1)

	if priority, found := popHeapThroughDispatch(t, s, withApplySeq(context.Background(), 3), database, bucket); !found || priority != 2 {
		t.Fatalf("live pop at sequence 3 took priority %d (found %v), want 2", priority, found)
	}
	for _, seq := range []uint64{2, 3} {
		if priority, found := popHeapThroughDispatch(t, s, withReplay(withApplySeq(context.Background(), seq)), database, bucket); found {
			t.Fatalf("replaying the entry at sequence %d, at or below applied_seq 3, popped priority %d", seq, priority)
		}
	}

	counted, err := s.Count(CountRequest{Database: database, Bucket: bucket})
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if counted.Count != 1 {
		t.Fatalf("count after gated replays = %d, want 1: a replayed pop removed an element", counted.Count)
	}
	if st := bucketStateOf(t, s, database, bucket); st.AppliedSeq != 3 || st.Entries != 1 {
		t.Fatalf("state after gated replays: applied_seq %d, entries %d; want 3 and 1", st.AppliedSeq, st.Entries)
	}
}
