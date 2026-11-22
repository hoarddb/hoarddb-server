package server

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// dumpBucketRaw drives OpDumpStart/Chunk/Done for one bucket and returns the
// manifest plus the exact concatenated bytes streamed — the same bytes a
// real dump would write to <bucket>.bson — so restore tests restore from a
// genuine dump rather than a hand-built payload (dump-restore-spec.md
// §12.1's round trip means dumping first).
func dumpBucketRaw(t *testing.T, s *Server, ctx context.Context, database, bucket string, wantDocs int64) (*DumpBucketManifest, []byte) {
	t.Helper()
	startFrame, msg := dumpDispatch(t, s, ctx, transport.OpDumpStart, DumpStartRequest{Database: database, Bucket: bucket})
	if msg != "" {
		t.Fatalf("dump start %s/%s: %s", database, bucket, msg)
	}
	var start DumpStartResponse
	if err := bson.Unmarshal(startFrame.Payload, &start); err != nil {
		t.Fatalf("decode dump start response: %v", err)
	}
	if start.Manifest == nil {
		t.Fatalf("dump start %s/%s returned no manifest", database, bucket)
	}

	var all []byte
	for {
		chunkFrame, cmsg := dumpDispatch(t, s, ctx, transport.OpDumpChunk, DumpChunkRequest{Database: database, Bucket: bucket})
		if cmsg != "" {
			t.Fatalf("dump chunk %s/%s: %s", database, bucket, cmsg)
		}
		var chunk DumpChunkResponse
		if err := bson.Unmarshal(chunkFrame.Payload, &chunk); err != nil {
			t.Fatalf("decode dump chunk response: %v", err)
		}
		all = append(all, chunk.Data...)
		if chunk.EOF {
			break
		}
	}

	doneFrame, dmsg := dumpDispatch(t, s, ctx, transport.OpDumpDone, DumpDoneRequest{Database: database, Bucket: bucket})
	if dmsg != "" {
		t.Fatalf("dump done %s/%s: %s", database, bucket, dmsg)
	}
	var done DumpDoneResponse
	if err := bson.Unmarshal(doneFrame.Payload, &done); err != nil {
		t.Fatalf("decode dump done response: %v", err)
	}
	if done.Documents != wantDocs {
		t.Fatalf("dump done for %s/%s reported %d documents, want %d", database, bucket, done.Documents, wantDocs)
	}
	return start.Manifest, all
}

// restoreBucketRaw drives OpRestoreStart/Chunk/Done for one bucket from
// already-dumped bytes, exactly as the CLI does (one Start, the whole
// stream as chunk data, one Done naming the expected count), and returns
// the responses so a caller can assert on a refusal as well as a success —
// mirroring dumpDispatch's own "return the message, don't just fatal"
// convention so refusal-path tests can use this helper too.
func restoreBucketRaw(t *testing.T, s *Server, ctx context.Context, req RestoreStartRequest, raw []byte, documents int64) (*RestoreStartResponse, *RestoreDoneResponse, string) {
	t.Helper()
	startFrame, msg := dumpDispatch(t, s, ctx, transport.OpRestoreStart, req)
	if msg != "" {
		return nil, nil, msg
	}
	var start RestoreStartResponse
	if err := bson.Unmarshal(startFrame.Payload, &start); err != nil {
		t.Fatalf("decode restore start response: %v", err)
	}

	if len(raw) > 0 {
		chunkFrame, cmsg := dumpDispatch(t, s, ctx, transport.OpRestoreChunk, RestoreChunkRequest{
			Database: req.Database, Bucket: req.Bucket, SessionID: start.SessionID, Data: raw,
		})
		_ = chunkFrame
		if cmsg != "" {
			return &start, nil, cmsg
		}
	}

	doneFrame, dmsg := dumpDispatch(t, s, ctx, transport.OpRestoreDone, RestoreDoneRequest{
		Database: req.Database, Bucket: req.Bucket, SessionID: start.SessionID, Documents: documents,
	})
	if dmsg != "" {
		return &start, nil, dmsg
	}
	var done RestoreDoneResponse
	if err := bson.Unmarshal(doneFrame.Payload, &done); err != nil {
		t.Fatalf("decode restore done response: %v", err)
	}
	return &start, &done, ""
}

// restoreRequestFromManifest builds a RestoreStartRequest from a real
// dump's manifest, the way the CLI reads it back out of metadata.json.
func restoreRequestFromManifest(m *DumpBucketManifest, bytes int64, drop bool) RestoreStartRequest {
	return RestoreStartRequest{
		Database:      m.Database,
		Bucket:        m.Bucket,
		FormatVersion: m.FormatVersion,
		StoreType:     m.StoreType,
		Indexes:       m.Indexes,
		Bytes:         bytes,
		Drop:          drop,
	}
}

// popTag pops one element from a FIFO/LIFO/heap bucket and returns its
// "tag" field.
func popTag(t *testing.T, s *Server, database, bucket string) string {
	t.Helper()
	resp, err := s.Pop(PopRequest{Database: database, Bucket: bucket})
	if err != nil {
		t.Fatalf("pop %s/%s: %v", database, bucket, err)
	}
	if !resp.Found {
		t.Fatalf("pop %s/%s: found=false (%s)", database, bucket, resp.Message)
	}
	var m bson.M
	if err := bson.Unmarshal(resp.Items[0], &m); err != nil {
		t.Fatalf("decode popped doc from %s/%s: %v", database, bucket, err)
	}
	tag, _ := m["tag"].(string)
	return tag
}

// TestRestoreRoundTripEachStoreType is dump-restore-spec.md §12.1: populate,
// dump, restore into a fresh bucket, compare value by value — never a
// count, which would pass on a dump that wrote the same key N times.
func TestRestoreRoundTripEachStoreType(t *testing.T) {
	s := newTestServer(t)
	ctx := rootContext()
	const n = 5

	t.Run("hash", func(t *testing.T) {
		createTestBucket(t, s, "app", "hashb", BucketTypeHash)
		want := map[string]string{}
		for i := 0; i < n; i++ {
			key := fmt.Sprintf("k%d", i)
			tag := fmt.Sprintf("hash-%d", i)
			if _, err := s.Insert(InsertRequest{Database: "app", Bucket: "hashb", Key: key, Data: map[string]interface{}{"tag": tag}}); err != nil {
				t.Fatalf("insert: %v", err)
			}
			want[key] = tag
		}
		manifest, raw := dumpBucketRaw(t, s, ctx, "app", "hashb", n)
		manifest.Bucket = "hashb_restored"
		req := restoreRequestFromManifest(manifest, int64(len(raw)), false)
		_, done, msg := restoreBucketRaw(t, s, ctx, req, raw, n)
		if msg != "" {
			t.Fatalf("restore refused: %s", msg)
		}
		if done.Applied != n {
			t.Fatalf("restore applied %d, want %d", done.Applied, n)
		}
		for key, tag := range want {
			resp, err := s.Find(FindRequest{Database: "app", Bucket: "hashb_restored", Key: key})
			if err != nil || !resp.Found {
				t.Fatalf("restored hash missing key %q: found=%v err=%v", key, resp.Found, err)
			}
			var m bson.M
			if err := bson.Unmarshal(resp.Data, &m); err != nil {
				t.Fatalf("decode restored doc %q: %v", key, err)
			}
			if m["tag"] != tag {
				t.Fatalf("restored hash key %q tag = %v, want %q (value-by-value check)", key, m["tag"], tag)
			}
		}
	})

	t.Run("btree", func(t *testing.T) {
		createTestBucket(t, s, "app", "btreeb", BucketTypeBTree)
		want := map[string]string{}
		for i := 0; i < n; i++ {
			key := fmt.Sprintf("k%d", i)
			tag := fmt.Sprintf("btree-%d", i)
			if _, err := s.Insert(InsertRequest{Database: "app", Bucket: "btreeb", Key: key, Data: map[string]interface{}{"tag": tag}}); err != nil {
				t.Fatalf("insert: %v", err)
			}
			want[key] = tag
		}
		manifest, raw := dumpBucketRaw(t, s, ctx, "app", "btreeb", n)
		manifest.Bucket = "btreeb_restored"
		req := restoreRequestFromManifest(manifest, int64(len(raw)), false)
		_, done, msg := restoreBucketRaw(t, s, ctx, req, raw, n)
		if msg != "" {
			t.Fatalf("restore refused: %s", msg)
		}
		if done.Applied != n {
			t.Fatalf("restore applied %d, want %d", done.Applied, n)
		}
		for key, tag := range want {
			resp, err := s.Find(FindRequest{Database: "app", Bucket: "btreeb_restored", Key: key})
			if err != nil || !resp.Found {
				t.Fatalf("restored btree missing key %q", key)
			}
			var m bson.M
			if err := bson.Unmarshal(resp.Data, &m); err != nil {
				t.Fatalf("decode restored doc %q: %v", key, err)
			}
			if m["tag"] != tag {
				t.Fatalf("restored btree key %q tag = %v, want %q", key, m["tag"], tag)
			}
		}
	})

	t.Run("fifo_preserves_push_order", func(t *testing.T) {
		createTestBucket(t, s, "app", "fifob", BucketTypeFIFO)
		var want []string
		for i := 0; i < n; i++ {
			tag := fmt.Sprintf("fifo-%d", i)
			if _, err := s.Push(PushRequest{Database: "app", Bucket: "fifob", Data: map[string]interface{}{"tag": tag}}); err != nil {
				t.Fatalf("push: %v", err)
			}
			want = append(want, tag)
		}
		manifest, raw := dumpBucketRaw(t, s, ctx, "app", "fifob", n)
		manifest.Bucket = "fifob_restored"
		req := restoreRequestFromManifest(manifest, int64(len(raw)), false)
		_, done, msg := restoreBucketRaw(t, s, ctx, req, raw, n)
		if msg != "" {
			t.Fatalf("restore refused: %s", msg)
		}
		if done.Applied != n {
			t.Fatalf("restore applied %d, want %d", done.Applied, n)
		}
		for i := 0; i < n; i++ {
			if got := popTag(t, s, "app", "fifob_restored"); got != want[i] {
				t.Fatalf("restored FIFO pop %d = %q, want %q (value-by-value, in order)", i, got, want[i])
			}
		}
	})

	t.Run("lifo_reproduces_pop_order", func(t *testing.T) {
		createTestBucket(t, s, "app", "lifob", BucketTypeLIFO)
		var pushed []string
		for i := 0; i < n; i++ {
			tag := fmt.Sprintf("lifo-%d", i)
			if _, err := s.Push(PushRequest{Database: "app", Bucket: "lifob", Data: map[string]interface{}{"tag": tag}}); err != nil {
				t.Fatalf("push: %v", err)
			}
			pushed = append(pushed, tag)
		}
		// Original pop order is newest-first.
		want := make([]string, n)
		for i, tag := range pushed {
			want[n-1-i] = tag
		}
		manifest, raw := dumpBucketRaw(t, s, ctx, "app", "lifob", n)
		manifest.Bucket = "lifob_restored"
		req := restoreRequestFromManifest(manifest, int64(len(raw)), false)
		_, done, msg := restoreBucketRaw(t, s, ctx, req, raw, n)
		if msg != "" {
			t.Fatalf("restore refused: %s", msg)
		}
		if done.Applied != n {
			t.Fatalf("restore applied %d, want %d", done.Applied, n)
		}
		// This is the case that catches a naive "push in file order"
		// restore: the .bson file lists elements newest-first
		// (store/queue.go's QueueStore.IterateDocuments), so pushing it back verbatim
		// would reverse the stack instead of reproducing it.
		for i := 0; i < n; i++ {
			if got := popTag(t, s, "app", "lifob_restored"); got != want[i] {
				t.Fatalf("restored LIFO pop %d = %q, want %q (must reproduce original pop order, not file order)", i, got, want[i])
			}
		}
	})

	t.Run("heap_preserves_priority_order", func(t *testing.T) {
		createTestBucket(t, s, "app", "heapb", BucketTypeHeap)
		var want []string
		for i := 0; i < n; i++ {
			tag := fmt.Sprintf("heap-%d", i)
			if _, err := s.Push(PushRequest{Database: "app", Bucket: "heapb", Priority: int64(i), Data: map[string]interface{}{"tag": tag}}); err != nil {
				t.Fatalf("push: %v", err)
			}
			want = append(want, tag)
		}
		// Highest priority (n-1, pushed last) pops first.
		wantPopOrder := make([]string, n)
		for i := 0; i < n; i++ {
			wantPopOrder[i] = want[n-1-i]
		}
		manifest, raw := dumpBucketRaw(t, s, ctx, "app", "heapb", n)
		manifest.Bucket = "heapb_restored"
		req := restoreRequestFromManifest(manifest, int64(len(raw)), false)
		_, done, msg := restoreBucketRaw(t, s, ctx, req, raw, n)
		if msg != "" {
			t.Fatalf("restore refused: %s", msg)
		}
		if done.Applied != n {
			t.Fatalf("restore applied %d, want %d", done.Applied, n)
		}
		for i := 0; i < n; i++ {
			if got := popTag(t, s, "app", "heapb_restored"); got != wantPopOrder[i] {
				t.Fatalf("restored heap pop %d = %q, want %q — priority was not reattached correctly", i, got, wantPopOrder[i])
			}
		}
	})

	t.Run("blob_payload_never_restored_but_counted", func(t *testing.T) {
		createTestBucket(t, s, "app", "blobb", BucketTypeBlob)
		payload := []byte("this payload must never be restored")
		if _, err := s.BlobWrite(BlobWriteRequest{Database: "app", Bucket: "blobb", Key: "b1",
			Data: payload, Metadata: map[string]interface{}{"tag": "blob-0"}}); err != nil {
			t.Fatalf("blob write: %v", err)
		}
		manifest, raw := dumpBucketRaw(t, s, ctx, "app", "blobb", 1)
		manifest.Bucket = "blobb_restored"
		req := restoreRequestFromManifest(manifest, int64(len(raw)), false)
		_, done, msg := restoreBucketRaw(t, s, ctx, req, raw, 1)
		if msg != "" {
			t.Fatalf("restore refused: %s", msg)
		}
		if done.Applied != 0 || done.Skipped != 1 {
			t.Fatalf("blob restore = applied %d, skipped %d, want applied 0, skipped 1", done.Applied, done.Skipped)
		}
		// Nothing was written for the metadata document: the restored blob
		// bucket exists (created) but holds no key→blob mapping.
		countResp, err := s.Count(CountRequest{Database: "app", Bucket: "blobb_restored"})
		if err != nil {
			t.Fatalf("count restored blob bucket: %v", err)
		}
		if countResp.Count != 0 {
			t.Fatalf("restored blob bucket holds %d entries, want 0 (payloads are never restored)", countResp.Count)
		}
	})
}

// TestRestoreMergeIdempotency is dump-restore-spec.md §12.4: restoring the
// same dump twice converges for keyed engines (no duplicates), and for a
// queue produces the elements twice, in order — the specified semantic,
// not a bug (§19 C-2/C-4).
func TestRestoreMergeIdempotency(t *testing.T) {
	s := newTestServer(t)
	ctx := rootContext()

	t.Run("hash_converges", func(t *testing.T) {
		createTestBucket(t, s, "app", "hashm", BucketTypeHash)
		if _, err := s.Insert(InsertRequest{Database: "app", Bucket: "hashm", Key: "a", Data: map[string]interface{}{"tag": "v1"}}); err != nil {
			t.Fatalf("insert: %v", err)
		}
		manifest, raw := dumpBucketRaw(t, s, ctx, "app", "hashm", 1)
		req := restoreRequestFromManifest(manifest, int64(len(raw)), false)

		for i := 0; i < 2; i++ {
			_, done, msg := restoreBucketRaw(t, s, ctx, req, raw, 1)
			if msg != "" {
				t.Fatalf("restore #%d refused: %s", i, msg)
			}
			if done.Applied != 1 {
				t.Fatalf("restore #%d applied %d, want 1", i, done.Applied)
			}
		}
		countResp, err := s.Count(CountRequest{Database: "app", Bucket: "hashm"})
		if err != nil {
			t.Fatalf("count: %v", err)
		}
		if countResp.Count != 1 {
			t.Fatalf("hash bucket holds %d entries after restoring the same dump twice, want 1 (no duplicates)", countResp.Count)
		}
	})

	t.Run("queue_duplicates_in_order", func(t *testing.T) {
		createTestBucket(t, s, "app", "fifom", BucketTypeFIFO)
		if _, err := s.Push(PushRequest{Database: "app", Bucket: "fifom", Data: map[string]interface{}{"tag": "only"}}); err != nil {
			t.Fatalf("push: %v", err)
		}
		manifest, raw := dumpBucketRaw(t, s, ctx, "app", "fifom", 1)
		req := restoreRequestFromManifest(manifest, int64(len(raw)), false)

		for i := 0; i < 2; i++ {
			_, done, msg := restoreBucketRaw(t, s, ctx, req, raw, 1)
			if msg != "" {
				t.Fatalf("restore #%d refused: %s", i, msg)
			}
			if done.Applied != 1 {
				t.Fatalf("restore #%d applied %d, want 1", i, done.Applied)
			}
		}
		// The live push plus two restores: three elements total, all "only".
		for i := 0; i < 3; i++ {
			if got := popTag(t, s, "app", "fifom"); got != "only" {
				t.Fatalf("pop %d = %q, want %q", i, got, "only")
			}
		}
		if resp, err := s.Pop(PopRequest{Database: "app", Bucket: "fifom"}); err != nil {
			t.Fatalf("4th pop: unexpected error: %v", err)
		} else if resp.Found {
			t.Fatalf("a 4th pop succeeded (items=%s); queue restore should duplicate exactly twice, not more", resp.Items)
		}
	})

	// A heap restores through a push too, but it converges: the restored
	// element keeps the _id_ it was dumped with, and a push under an id
	// already present at the same priority overwrites it (store/heap.go's
	// entryIDFor and putElements). Undocumented until BUG-0495.
	t.Run("heap_converges", func(t *testing.T) {
		createTestBucket(t, s, "app", "heaps", BucketTypeHeap)
		for _, pushed := range []struct {
			tag      string
			priority int64
		}{{"high", 10}, {"low", 1}} {
			if _, err := s.Push(PushRequest{Database: "app", Bucket: "heaps", Data: map[string]interface{}{"tag": pushed.tag}, Priority: pushed.priority}); err != nil {
				t.Fatalf("push %s: %v", pushed.tag, err)
			}
		}
		manifest, raw := dumpBucketRaw(t, s, ctx, "app", "heaps", 2)
		req := restoreRequestFromManifest(manifest, int64(len(raw)), false)

		for i := 0; i < 2; i++ {
			_, done, msg := restoreBucketRaw(t, s, ctx, req, raw, 2)
			if msg != "" {
				t.Fatalf("restore #%d refused: %s", i, msg)
			}
			if done.Applied != 2 {
				t.Fatalf("restore #%d applied %d, want 2", i, done.Applied)
			}
		}
		countResp, err := s.Count(CountRequest{Database: "app", Bucket: "heaps"})
		if err != nil {
			t.Fatalf("count: %v", err)
		}
		if countResp.Count != 2 {
			t.Fatalf("heap holds %d entries after restoring the same dump twice, want 2 (each element overwritten by its id)", countResp.Count)
		}
	})

	// The one heap case that does not converge is the caveat the backup page
	// states: an element whose priority moved between the dump and the restore.
	// A heap entry key is the id and the priority (store/heap.go's makeKey), so
	// the restored copy lands beside the moved element rather than on top of
	// it. The page's "--drop or check the count" advice for that case is only
	// honest while this holds.
	t.Run("heap_priority_change_duplicates", func(t *testing.T) {
		createTestBucket(t, s, "app", "heappr", BucketTypeHeap)
		element := map[string]interface{}{"_id_": "task-1", "tag": "moved"}
		if _, err := s.Push(PushRequest{Database: "app", Bucket: "heappr", Data: element, Priority: 10}); err != nil {
			t.Fatalf("push: %v", err)
		}
		manifest, raw := dumpBucketRaw(t, s, ctx, "app", "heappr", 1)
		req := restoreRequestFromManifest(manifest, int64(len(raw)), false)

		moved := map[string]interface{}{"_id_": "task-1", "tag": "moved"}
		if _, err := s.Push(PushRequest{Database: "app", Bucket: "heappr", Data: moved, Priority: 1}); err != nil {
			t.Fatalf("re-push the same id at a new priority: %v", err)
		}

		_, done, msg := restoreBucketRaw(t, s, ctx, req, raw, 1)
		if msg != "" {
			t.Fatalf("restore refused: %s", msg)
		}
		if done.Applied != 1 {
			t.Fatalf("restore applied %d, want 1", done.Applied)
		}
		countResp, err := s.Count(CountRequest{Database: "app", Bucket: "heappr"})
		if err != nil {
			t.Fatalf("count: %v", err)
		}
		if countResp.Count != 2 {
			t.Fatalf("heap holds %d entries after one element moved priority between the dump and the restore, want 2: the entry key is the id and the priority, so the restored copy sits beside the moved element instead of overwriting it", countResp.Count)
		}
	})
}

// TestRestoreDrop is dump-restore-spec.md §12.5: --drop replaces the bucket
// with the dump's type and indexes, and refuses a store-type mismatch
// naming what would be lost — with or without --drop.
func TestRestoreDrop(t *testing.T) {
	s := newTestServer(t)
	ctx := rootContext()

	createTestBucket(t, s, "app", "src", BucketTypeHash)
	if _, err := s.Insert(InsertRequest{Database: "app", Bucket: "src", Key: "a", Data: map[string]interface{}{"tag": "v1"}}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	manifest, raw := dumpBucketRaw(t, s, ctx, "app", "src", 1)

	t.Run("drop_replaces_matching_type", func(t *testing.T) {
		createTestBucket(t, s, "app", "target1", BucketTypeHash)
		if _, err := s.Insert(InsertRequest{Database: "app", Bucket: "target1", Key: "stale", Data: map[string]interface{}{"tag": "should be gone"}}); err != nil {
			t.Fatalf("seed stale doc: %v", err)
		}
		m := *manifest
		m.Bucket = "target1"
		req := restoreRequestFromManifest(&m, int64(len(raw)), true)
		_, done, msg := restoreBucketRaw(t, s, ctx, req, raw, 1)
		if msg != "" {
			t.Fatalf("restore --drop refused: %s", msg)
		}
		if done.Applied != 1 {
			t.Fatalf("applied %d, want 1", done.Applied)
		}
		if resp, err := s.Find(FindRequest{Database: "app", Bucket: "target1", Key: "stale"}); err != nil || resp.Found {
			t.Fatalf("--drop did not remove the pre-existing stale document: found=%v err=%v", resp.Found, err)
		}
		if resp, err := s.Find(FindRequest{Database: "app", Bucket: "target1", Key: "a"}); err != nil || !resp.Found {
			t.Fatalf("--drop restore did not write the dump's document: found=%v err=%v", resp.Found, err)
		}
	})

	t.Run("drop_refuses_type_mismatch_and_writes_nothing", func(t *testing.T) {
		createTestBucket(t, s, "app", "target2", BucketTypeBTree)
		if _, err := s.Insert(InsertRequest{Database: "app", Bucket: "target2", Key: "keep-me", Data: map[string]interface{}{"tag": "must survive"}}); err != nil {
			t.Fatalf("seed: %v", err)
		}
		m := *manifest
		m.Bucket = "target2"
		req := restoreRequestFromManifest(&m, int64(len(raw)), true)
		_, _, msg := restoreBucketRaw(t, s, ctx, req, raw, 1)
		if msg == "" || !strings.Contains(msg, "hash") || !strings.Contains(msg, "btree") {
			t.Fatalf("--drop store-type mismatch message = %q, want it to name both types", msg)
		}
		resp, err := s.Find(FindRequest{Database: "app", Bucket: "target2", Key: "keep-me"})
		if err != nil || !resp.Found {
			t.Fatalf("--drop refusal must not have touched the existing bucket: found=%v err=%v", resp.Found, err)
		}
	})

	t.Run("no_drop_refuses_type_mismatch_and_writes_nothing", func(t *testing.T) {
		createTestBucket(t, s, "app", "target3", BucketTypeBTree)
		m := *manifest
		m.Bucket = "target3"
		req := restoreRequestFromManifest(&m, int64(len(raw)), false)
		_, _, msg := restoreBucketRaw(t, s, ctx, req, raw, 1)
		if msg == "" {
			t.Fatal("restore into a differently-typed existing bucket without --drop should be refused")
		}
		countResp, err := s.Count(CountRequest{Database: "app", Bucket: "target3"})
		if err != nil {
			t.Fatalf("count: %v", err)
		}
		if countResp.Count != 0 {
			t.Fatalf("refused restore wrote %d document(s), want 0", countResp.Count)
		}
	})
}

// blobPayloadPathOf finds blobID's payload file in the node's blob tree by
// its parsed name, failing the test if there is none.
func blobPayloadPathOf(t *testing.T, s *Server, blobID string) string {
	t.Helper()
	var found string
	err := filepath.WalkDir(s.blobRoot(), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if name, ok := store.ParsePayloadFileName(entry.Name()); ok && !name.Temporary && name.BlobID == blobID {
			found = path
		}
		return nil
	})
	if err != nil || found == "" {
		t.Fatalf("precondition: no payload file of blob %s under %s (walk: %v)", blobID, s.blobRoot(), err)
	}
	return found
}

// TestRestoreDropRefusesBlobDataLoss is the whole-branch review's Critical
// finding: dump never captures blob payloads (applyRestoreDocument's
// BucketTypeBlob case only counts and discards the metadata document), so
// --drop against an existing blob bucket recreates it empty and restore's
// own OpRestoreDone count assertion (applied + skipped == total) passes —
// the CLI reports success while every payload is gone. §9.4 requires
// --drop to refuse whenever it would discard data the dump does not
// contain; this proves the refusal fires whichever side of the drop is
// blob, and — per this project's "every negative test asserts nothing was
// written" discipline (spec §12.8) — that the original payload file and
// its content are untouched afterward, not just that an error came back.
func TestRestoreDropRefusesBlobDataLoss(t *testing.T) {
	s := newTestServer(t)
	ctx := rootContext()

	// Above the inline threshold, so the payload is a file of its own in the
	// node's blob tree and "untouched" has a file to be about.
	payload := []byte(strings.Repeat("irreplaceable blob payload — dump never captures this. ", store.InlineThreshold/32))
	createTestBucket(t, s, "app", "blobsrc", BucketTypeBlob)
	writeResp, err := s.BlobWrite(BlobWriteRequest{Database: "app", Bucket: "blobsrc", Key: "b1",
		Data: payload, Metadata: map[string]interface{}{"tag": "irreplaceable"}})
	if err != nil {
		t.Fatalf("blob write: %v", err)
	}
	blobPayloadPath := blobPayloadPathOf(t, s, writeResp.BlobID)
	manifest, raw := dumpBucketRaw(t, s, ctx, "app", "blobsrc", 1)

	assertBlobPayloadIntact := func(t *testing.T) {
		t.Helper()
		got, err := os.ReadFile(blobPayloadPath)
		if err != nil {
			t.Fatalf("--drop refusal must not have touched the blob payload file, but it is now unreadable: %v", err)
		}
		if string(got) != string(payload) {
			t.Fatalf("--drop refusal must not have touched the blob payload file, but its content changed: got %q, want %q", got, payload)
		}
		readResp, err := s.BlobRead(BlobReadRequest{Database: "app", Bucket: "blobsrc", Key: "b1"})
		if err != nil || !readResp.Found {
			t.Fatalf("--drop refusal must leave the original blob bucket readable: found=%v err=%v", readResp != nil && readResp.Found, err)
		}
	}

	t.Run("drop_into_existing_blob_bucket_same_type_refused", func(t *testing.T) {
		m := *manifest
		m.Bucket = "blobsrc" // target is the bucket itself: existing type == dump type == blob
		req := restoreRequestFromManifest(&m, int64(len(raw)), true)
		_, _, msg := restoreBucketRaw(t, s, ctx, req, raw, 1)
		if msg == "" {
			t.Fatal("--drop against an existing blob bucket must be refused even when the dump's store_type matches (blob)")
		}
		if !strings.Contains(msg, "blob") {
			t.Fatalf("refusal message = %q, want it to mention blob payloads", msg)
		}
		assertBlobPayloadIntact(t)
	})

	t.Run("drop_into_existing_blob_bucket_mismatched_type_refused_as_blob_loss", func(t *testing.T) {
		// Dump is hash-typed, target is the existing blob bucket: a plain
		// type-mismatch refusal would already stop this, but it must be
		// refused for the blob-data-loss reason specifically, per the
		// brief ("existing bucket of type blob" is enough on its own).
		createTestBucket(t, s, "app", "hashsrc", BucketTypeHash)
		if _, err := s.Insert(InsertRequest{Database: "app", Bucket: "hashsrc", Key: "k", Data: map[string]interface{}{"tag": "v"}}); err != nil {
			t.Fatalf("seed hash source: %v", err)
		}
		hashManifest, hashRaw := dumpBucketRaw(t, s, ctx, "app", "hashsrc", 1)
		m := *hashManifest
		m.Bucket = "blobsrc" // target is the existing blob bucket; dump is hash
		req := restoreRequestFromManifest(&m, int64(len(hashRaw)), true)
		_, _, msg := restoreBucketRaw(t, s, ctx, req, hashRaw, 1)
		if msg == "" {
			t.Fatal("--drop against an existing blob bucket from a non-blob dump must be refused")
		}
		if !strings.Contains(msg, "blob") {
			t.Fatalf("refusal message = %q, want it to name blob payloads as the reason (not merely a type mismatch)", msg)
		}
		assertBlobPayloadIntact(t)
	})

	t.Run("drop_into_fresh_target_from_blob_dump_refused_even_without_existing_bucket_conflict", func(t *testing.T) {
		createTestBucket(t, s, "app", "blobtarget", BucketTypeBlob)
		if _, err := s.BlobWrite(BlobWriteRequest{Database: "app", Bucket: "blobtarget", Key: "b1",
			Data: []byte("target's own payload, also irreplaceable"), Metadata: nil}); err != nil {
			t.Fatalf("seed target blob: %v", err)
		}
		m := *manifest
		m.Bucket = "blobtarget"
		req := restoreRequestFromManifest(&m, int64(len(raw)), true)
		_, _, msg := restoreBucketRaw(t, s, ctx, req, raw, 1)
		if msg == "" {
			t.Fatal("--drop against a differently-populated existing blob bucket must be refused")
		}
		assertBlobPayloadIntact(t)
	})
}

// TestRestoreDryRunWritesNothing proves --dry-run validates and reports
// without creating a database, a bucket, or any document.
func TestRestoreDryRunWritesNothing(t *testing.T) {
	s := newTestServer(t)
	ctx := rootContext()

	req := RestoreStartRequest{
		Database: "dryapp", Bucket: "dryb",
		FormatVersion: dumpManifestFormatVersion, StoreType: BucketTypeHash,
		Bytes: 100, DryRun: true,
	}
	frame, msg := dumpDispatch(t, s, ctx, transport.OpRestoreStart, req)
	if msg != "" {
		t.Fatalf("dry-run refused: %s", msg)
	}
	var resp RestoreStartResponse
	if err := bson.Unmarshal(frame.Payload, &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !resp.Success || resp.SessionID != "" {
		t.Fatalf("dry-run response = %+v, want Success and no SessionID (no session opened)", resp)
	}
	if s.serverDataAccessor().getDB("dryapp") != nil {
		t.Fatal("--dry-run created database \"dryapp\"; it must write nothing")
	}
	if _, err := os.Stat(filepath.Join(s.config.Storage.DataDir, "dryapp")); err == nil {
		t.Fatal("--dry-run created a directory on disk; it must write nothing")
	}
}

// TestRestoreRefusesFormatVersionMismatch pins dump-restore-spec.md §9.1/§10:
// OpRestoreStart itself checks format_version, not only the CLI, naming
// both versions.
func TestRestoreRefusesFormatVersionMismatch(t *testing.T) {
	s := newTestServer(t)
	ctx := rootContext()
	req := RestoreStartRequest{Database: "app", Bucket: "b", FormatVersion: dumpManifestFormatVersion + 1, StoreType: BucketTypeHash, Bytes: 10}
	_, msg := dumpDispatch(t, s, ctx, transport.OpRestoreStart, req)
	if !strings.Contains(msg, fmt.Sprint(dumpManifestFormatVersion+1)) || !strings.Contains(msg, fmt.Sprint(dumpManifestFormatVersion)) {
		t.Fatalf("format version refusal = %q, want it to name both versions", msg)
	}
	if s.serverDataAccessor().getDB("app") != nil {
		t.Fatal("a format_version refusal must not create the database")
	}
}

// TestRestoreHostilePaths is dump-restore-spec.md §12.12: a manifest naming
// "../", an absolute path, a NUL byte, or a name that canonicalises outside
// the data directory is refused before any write — and nothing is written,
// checked on disk, not just "an error came back" (this task's brief).
func TestRestoreHostilePaths(t *testing.T) {
	dataDirParent := func(s *Server) string { return filepath.Dir(s.config.Storage.DataDir) }

	cases := []struct {
		name     string
		database string
		bucket   string
	}{
		{"dotdot_database", "../escaped", "b"},
		{"dotdot_bucket", "app", "../escaped"},
		{"absolute_database", "/etc/passwd", "b"},
		{"absolute_bucket", "app", "/etc/passwd"},
		{"nul_byte_database", "app\x00evil", "b"},
		{"nul_byte_bucket", "app", "b\x00evil"},
		{"reserved_system_database", "_system", "b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestServer(t)
			ctx := rootContext()
			before, err := os.ReadDir(dataDirParent(s))
			if err != nil {
				t.Fatalf("listing data dir parent before: %v", err)
			}

			req := RestoreStartRequest{Database: tc.database, Bucket: tc.bucket, FormatVersion: dumpManifestFormatVersion, StoreType: BucketTypeHash, Bytes: 10}
			_, msg := dumpDispatch(t, s, ctx, transport.OpRestoreStart, req)
			if msg == "" {
				t.Fatalf("hostile database=%q bucket=%q was NOT refused", tc.database, tc.bucket)
			}

			if s.serverDataAccessor().getDB(tc.database) != nil {
				t.Fatalf("hostile request created an in-memory database %q", tc.database)
			}
			after, err := os.ReadDir(dataDirParent(s))
			if err != nil {
				t.Fatalf("listing data dir parent after: %v", err)
			}
			if len(after) != len(before) {
				t.Fatalf("refusal changed the data directory's parent: before=%d entries after=%d entries — something was written outside the data directory",
					len(before), len(after))
			}
		})
	}
}

// TestRestoreRefusesTraversalNames is build doc Step 19's: on the shared
// layout a bucket's name becomes a key, never a path, so what is left of the
// restore path check is a check of names — and it still refuses a traversal,
// a separator and an empty name, as a database or as a bucket.
func TestRestoreRefusesTraversalNames(t *testing.T) {
	for _, hostile := range []string{"..", "a/b", ""} {
		if err := validateBucketName(hostile, "b"); !errors.Is(err, ErrRestorePathEscapesRoot) {
			t.Errorf("database name %q: validateBucketName = %v, want a refusal", hostile, err)
		}
		if err := validateBucketName("app", hostile); !errors.Is(err, ErrRestorePathEscapesRoot) {
			t.Errorf("bucket name %q: validateBucketName = %v, want a refusal", hostile, err)
		}
	}
	if err := validateBucketName("app", "users"); err != nil {
		t.Fatalf("an ordinary name was refused: %v", err)
	}
}

// TestRestoreBucketPinsCoordinateWithDump is dump-restore-spec.md §9.7/
// §12.13: dump and restore are mutually exclusive on one bucket in either
// direction; a --drop restore conflicts with anything else on its bucket;
// two concurrent merge restores into the same bucket both succeed.
func TestRestoreBucketPinsCoordinateWithDump(t *testing.T) {
	t.Run("drop_restore_refused_while_bucket_is_dumping", func(t *testing.T) {
		s := newTestServer(t)
		ctx := rootContext()
		createTestBucket(t, s, "app", "b", BucketTypeHash)
		if _, err := s.Insert(InsertRequest{Database: "app", Bucket: "b", Key: "a", Data: map[string]interface{}{"tag": "v"}}); err != nil {
			t.Fatalf("insert: %v", err)
		}
		// Claim the dump pin directly (a FIFO/LIFO bucket would also work,
		// but any bucket type occupies s.dump — the pin this test checks).
		session, err := s.claimDump("app", "b", BucketTypeHash, "admin")
		if err != nil {
			t.Fatalf("claimDump: %v", err)
		}
		defer s.releaseDump(session, nil)

		req := RestoreStartRequest{Database: "app", Bucket: "b", FormatVersion: dumpManifestFormatVersion, StoreType: BucketTypeHash, Bytes: 10, Drop: true}
		_, msg := dumpDispatch(t, s, ctx, transport.OpRestoreStart, req)
		if !strings.Contains(msg, "dumped") {
			t.Fatalf("restore while dumping = %q, want a refusal naming the dump", msg)
		}
	})

	t.Run("dump_refused_while_bucket_is_being_restored", func(t *testing.T) {
		s := newTestServer(t)
		ctx := rootContext()
		createTestBucket(t, s, "app", "b", BucketTypeHash)

		release, err := s.claimBucketRestorePin("app", "b", false)
		if err != nil {
			t.Fatalf("claimBucketRestorePin: %v", err)
		}
		defer release()

		_, msg := dumpDispatch(t, s, ctx, transport.OpDumpStart, DumpStartRequest{Database: "app", Bucket: "b"})
		if !strings.Contains(msg, "restored") {
			t.Fatalf("dump while restoring = %q, want a refusal naming the restore", msg)
		}
	})

	t.Run("drop_restore_refused_against_an_in_progress_merge", func(t *testing.T) {
		s := newTestServer(t)
		release, err := s.claimBucketRestorePin("app", "b", false)
		if err != nil {
			t.Fatalf("claimBucketRestorePin (merge): %v", err)
		}
		defer release()

		if _, err := s.claimBucketRestorePin("app", "b", true); !errors.Is(err, ErrRestoreBucketConflict) {
			t.Fatalf("--drop against an in-progress merge = %v, want ErrRestoreBucketConflict", err)
		}
	})

	t.Run("two_concurrent_merges_into_the_same_bucket_both_succeed", func(t *testing.T) {
		s := newTestServer(t)
		release1, err := s.claimBucketRestorePin("app", "b", false)
		if err != nil {
			t.Fatalf("first merge claim: %v", err)
		}
		release2, err := s.claimBucketRestorePin("app", "b", false)
		if err != nil {
			t.Fatalf("second concurrent merge claim: %v", err)
		}
		release1()
		release2()
		if _, ok := s.bucketHasActiveRestore("app", "b"); ok {
			t.Fatal("both releases ran; the bucket should show no active restore")
		}
	})
}

// TestRestoreAbortedTransferReleasesAndAudits is dump-restore-spec.md
// §12.14: closing the connection mid-restore releases the pin (proved by a
// second restore claim succeeding immediately after) and writes
// EventRestoreAborted.
func TestRestoreAbortedTransferReleasesAndAudits(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "b", BucketTypeHash)

	auditPath := filepath.Join(t.TempDir(), "audit.jsonl")
	al, err := NewAuditLogger(auditPath)
	if err != nil {
		t.Fatalf("NewAuditLogger: %v", err)
	}
	// newTestServer's own cleanup (qs.Stop) closes s.audit exactly once when
	// the test ends; closing it again here would double-close the queue
	// channel, so this test polls the file instead of forcing a flush via
	// Close.
	s.audit = al

	connCtx, cancel := context.WithCancel(rootContext())
	req := RestoreStartRequest{Database: "app", Bucket: "b", FormatVersion: dumpManifestFormatVersion, StoreType: BucketTypeHash, Bytes: 10}
	frame, msg := dumpDispatch(t, s, connCtx, transport.OpRestoreStart, req)
	if msg != "" {
		t.Fatalf("restore start: %s", msg)
	}
	var start RestoreStartResponse
	if err := bson.Unmarshal(frame.Payload, &start); err != nil {
		t.Fatalf("decode: %v", err)
	}

	cancel() // simulate the connection closing mid-restore

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := s.bucketHasActiveRestore("app", "b"); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("restore pin was not released after the connection closed")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// The drain goroutine writes asynchronously; poll rather than assume it
	// has landed by the time bucketHasActiveRestore's release is visible.
	deadline = time.Now().Add(2 * time.Second)
	for {
		auditRaw, err := os.ReadFile(auditPath)
		if err == nil && strings.Contains(string(auditRaw), EventRestoreAborted) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("audit log never recorded %q after connection close (last read error: %v)", EventRestoreAborted, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestRestoreDiskHeadroomRefusesWithoutFillingADisk is dump-restore-spec.md
// §9.1/§11/§19 A-15: free space below the configured multiplier of the
// bucket's raw dump bytes refuses before the bucket starts. diskFreeBytes
// is overridden rather than filling a real disk (dump-restore-build.md
// trap #10).
func TestRestoreDiskHeadroomRefusesWithoutFillingADisk(t *testing.T) {
	s := newTestServer(t)
	ctx := rootContext()

	orig := diskFreeBytes
	diskFreeBytes = func(string) (int64, error) { return 1000, nil } // 1000 bytes free
	t.Cleanup(func() { diskFreeBytes = orig })

	req := RestoreStartRequest{Database: "app", Bucket: "b", FormatVersion: dumpManifestFormatVersion, StoreType: BucketTypeHash, Bytes: 10_000} // needs 20,000 at 2.0x
	_, msg := dumpDispatch(t, s, ctx, transport.OpRestoreStart, req)
	if !strings.Contains(msg, "free") && !strings.Contains(msg, "disk") {
		t.Fatalf("disk headroom refusal = %q, want it to mention free space", msg)
	}
	if s.serverDataAccessor().getDB("app") != nil {
		t.Fatal("a disk-headroom refusal must not create the database")
	}

	// Control: raise the reported free space and the identical request
	// succeeds, proving the refusal above was the headroom check and not
	// something else about the request.
	diskFreeBytes = func(string) (int64, error) { return 1 << 30, nil }
	_, msg2 := dumpDispatch(t, s, ctx, transport.OpRestoreStart, req)
	if msg2 != "" {
		t.Fatalf("restore with ample free space refused: %s", msg2)
	}
}

// TestRestoreTokenNotCachedAcrossChunks is dump-restore-spec.md §9.8/§12.17:
// the server does not cache a token/identity decision across
// OpRestoreChunk calls in a way that would make a legitimately refreshed
// token look invalid, or let a revoked one keep working. Each call re-runs
// requireRootIdentity against the connection's *current* session — proved
// here by changing the session's identity between Start and Chunk (the
// same effect a real token refresh or expiry has on transport.Session) and
// observing that the change takes effect immediately rather than being
// masked by anything cached in the restore session.
func TestRestoreTokenNotCachedAcrossChunks(t *testing.T) {
	s := newTestServer(t)
	sess := &transport.Session{}
	sess.SetIdentity(transport.Identity{Username: "admin"}) // "admin" is newTestServer's configured root user
	ctx := transport.WithSession(context.Background(), sess)

	createTestBucket(t, s, "app", "b", BucketTypeHash)
	req := RestoreStartRequest{Database: "app", Bucket: "b", FormatVersion: dumpManifestFormatVersion, StoreType: BucketTypeHash, Bytes: 10}
	frame, msg := dumpDispatch(t, s, ctx, transport.OpRestoreStart, req)
	if msg != "" {
		t.Fatalf("restore start: %s", msg)
	}
	var start RestoreStartResponse
	if err := bson.Unmarshal(frame.Payload, &start); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// A legitimate refresh on the same connection: the session's identity
	// is updated in place (exactly what finishAuth does on a token
	// refresh), still root. The next chunk must succeed — nothing about
	// restoreChunk may depend on the *old* identity object.
	sess.SetIdentity(transport.Identity{Username: "admin"})
	one, err := bson.Marshal(map[string]interface{}{"_id_": "x", "tag": "v"})
	if err != nil {
		t.Fatalf("marshal doc: %v", err)
	}
	_, cmsg := dumpDispatch(t, s, ctx, transport.OpRestoreChunk, RestoreChunkRequest{
		Database: "app", Bucket: "b", SessionID: start.SessionID, Data: one,
	})
	if cmsg != "" {
		t.Fatalf("chunk after a same-identity refresh was refused: %s", cmsg)
	}

	// A client that "cannot refresh" — the session drops to no identity at
	// all (the effect of an expired token with nothing to replace it) —
	// must fail loudly rather than being served from a stale decision.
	sess.SetIdentity(transport.Identity{})
	_, cmsg2 := dumpDispatch(t, s, ctx, transport.OpRestoreChunk, RestoreChunkRequest{
		Database: "app", Bucket: "b", SessionID: start.SessionID, Data: one,
	})
	if cmsg2 == "" {
		t.Fatal("a chunk sent after the session lost its identity was accepted; the pin would be held past a client that cannot refresh")
	}
}

// TestClaimBucketRestorePinConcurrent exercises the pin bookkeeping under
// -race: many goroutines claiming and releasing merge pins on the same
// bucket, plus one that tries (and must fail) a --drop claim while merges
// are active, concurrently.
func TestClaimBucketRestorePinConcurrent(t *testing.T) {
	s := newTestServer(t)
	const workers = 16
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			release, err := s.claimBucketRestorePin("app", "concurrent", false)
			if err != nil {
				return // a --drop claim may have won the race; not this test's concern
			}
			time.Sleep(time.Millisecond)
			release()
		}()
	}
	wg.Wait()
	if _, ok := s.bucketHasActiveRestore("app", "concurrent"); ok {
		t.Fatal("all merge claims released; the bucket should show no active restore")
	}
}

// TestClaimDumpAndClaimBucketRestorePinMutuallyExclusive is fix round 1's
// Important #1: server/dump.go's claimDump and this file's
// claimBucketRestorePin must never both win the same bucket, even when
// launched concurrently. The original implementation checked the dump slot
// and inserted the restore pin under two separately-acquired mutexes,
// leaving a window between them; not data-race-visible (each field access
// was individually synchronized), so this test proves the *logical*
// invariant directly — start both claims from a closed barrier channel, many
// times, and assert exactly one wins each trial — rather than relying on
// -race to catch it, the way TestDumpPinCeilingForceReleases' own history
// (server/dump.go) proves a synchronization bug can hide from the race
// detector while still being real.
func TestClaimDumpAndClaimBucketRestorePinMutuallyExclusive(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "race", BucketTypeHash)

	const trials = 200
	for trial := 0; trial < trials; trial++ {
		var wg sync.WaitGroup
		start := make(chan struct{})
		var (
			session    *dumpSession
			dumpErr    error
			restoreRel func()
			restoreErr error
		)

		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			session, dumpErr = s.claimDump("app", "race", BucketTypeHash, "admin")
		}()
		go func() {
			defer wg.Done()
			<-start
			restoreRel, restoreErr = s.claimBucketRestorePin("app", "race", false)
		}()
		close(start)
		wg.Wait()

		dumpWon := dumpErr == nil
		restoreWon := restoreErr == nil
		if dumpWon && restoreWon {
			t.Fatalf("trial %d: both a dump claim and a restore claim succeeded concurrently on the same bucket (dumpErr=%v restoreErr=%v)",
				trial, dumpErr, restoreErr)
		}
		if !dumpWon && !restoreWon {
			t.Fatalf("trial %d: neither claim succeeded, want exactly one (dumpErr=%v restoreErr=%v)", trial, dumpErr, restoreErr)
		}
		if dumpWon {
			s.releaseDump(session, nil)
		}
		if restoreWon {
			restoreRel()
		}
	}
}

// TestRestoredBlobBucketMatchesWhatTheWarningSays pins BUG-0494's pairing: the
// dump and restore warning says a restored blob bucket comes back empty, with
// its keys and metadata left in the dump files. Restore skips every blob
// metadata document it is sent, so the bucket must hold nothing afterward and
// the skipped count must name each key.
func TestRestoredBlobBucketMatchesWhatTheWarningSays(t *testing.T) {
	s := newTestServer(t)
	ctx := rootContext()

	createTestBucket(t, s, "app", "files", BucketTypeBlob)
	for _, key := range []string{"k1", "k2"} {
		if _, err := s.BlobWrite(BlobWriteRequest{Database: "app", Bucket: "files", Key: key,
			Data: []byte("payload for " + key), Metadata: map[string]interface{}{"type": "text/plain"}}); err != nil {
			t.Fatalf("blob write %s: %v", key, err)
		}
	}
	manifest, raw := dumpBucketRaw(t, s, ctx, "app", "files", 2)

	if resp, err := s.DeleteBucket(DeleteBucketRequest{Database: "app", Name: "files"}); err != nil || !resp.Success {
		t.Fatalf("drop bucket files: resp=%+v err=%v", resp, err)
	}

	req := restoreRequestFromManifest(manifest, int64(len(raw)), false)
	_, done, msg := restoreBucketRaw(t, s, ctx, req, raw, 2)
	if msg != "" {
		t.Fatalf("restore refused: %s", msg)
	}
	if done.Skipped != 2 {
		t.Fatalf("restore skipped %d, want 2 (one per blob key)", done.Skipped)
	}

	count, err := s.Count(CountRequest{Database: "app", Bucket: "files"})
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count.Count != 0 {
		t.Fatalf("restored blob bucket holds %d document(s), want 0: the warning says it comes back empty", count.Count)
	}
}
