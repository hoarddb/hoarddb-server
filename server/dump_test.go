package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// rootContext builds a context carrying an authenticated session for the
// server's configured root user ("admin" — newTestServer's default config),
// the way a real AUTH'd connection's session would look once handleAuth has
// run.
func rootContext() context.Context {
	sess := &transport.Session{}
	sess.SetIdentity(transport.Identity{Username: "admin"})
	return transport.WithSession(context.Background(), sess)
}

// namedContext builds a context authenticated as some other user, to prove
// the root gate refuses everyone but root.
func namedContext(username string) context.Context {
	sess := &transport.Session{}
	sess.SetIdentity(transport.Identity{Username: username})
	return transport.WithSession(context.Background(), sess)
}

// dumpDispatch marshals req, sends it through the server's real dispatch
// table (the same one a live connection's frame reaches), and reports the
// server's error message when it answered OpError instead of failing the
// test outright — callers assert on refusals as well as successes.
func dumpDispatch(t *testing.T, s *Server, ctx context.Context, op transport.Opcode, req interface{}) (*transport.Frame, string) {
	t.Helper()
	payload, err := bson.Marshal(req)
	if err != nil {
		t.Fatalf("marshal %s request: %v", op, err)
	}
	frame := transport.NewFrame(op, payload)
	resp, err := s.dispatch(ctx, frame)
	if err != nil {
		t.Fatalf("dispatch %s: unexpected transport error: %v", op, err)
	}
	if resp.Opcode == transport.OpError {
		var errResp ErrorResponse
		_ = bson.Unmarshal(resp.Payload, &errResp)
		return resp, errResp.Message
	}
	return resp, ""
}

// splitBSONDocuments splits a concatenation of BSON documents back into
// individual document byte slices, the same way bsondump/any BSON reader
// would: each document's own leading 4-byte little-endian length says how
// much of the stream belongs to it. Used by tests to prove the dumped
// bytes are exactly what dump-restore-spec.md §4 promises — "no length
// prefix, no framing, no wrapper" beyond BSON's own.
func splitBSONDocuments(data []byte) ([][]byte, error) {
	var docs [][]byte
	for len(data) > 0 {
		if len(data) < 4 {
			return nil, fmt.Errorf("trailing %d byte(s) are not a full BSON length prefix", len(data))
		}
		n := int(data[0]) | int(data[1])<<8 | int(data[2])<<16 | int(data[3])<<24
		if n < 5 || n > len(data) {
			return nil, fmt.Errorf("invalid BSON document length %d in %d remaining byte(s)", n, len(data))
		}
		docs = append(docs, data[:n])
		data = data[n:]
	}
	return docs, nil
}

// dumpBucketAndDecode drives OpDumpStart/Chunk/Done for one bucket exactly
// the way the CLI does, asserts the manifest's authoritative document count,
// verifies the server's digest acknowledgement, and returns the decoded
// documents in wire order — so callers can assert value-by-value, not just
// a count (dump-restore-spec.md §12.1: "a count-only test passes on a dump
// that wrote the same key N times").
func dumpBucketAndDecode(t *testing.T, s *Server, ctx context.Context, database, bucket string, wantDocs int64) []bson.M {
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
	// OpDumpStart's manifest deliberately carries no document count
	// (fix-round-1: avoiding a second Badger transaction) — the
	// authoritative count is asserted below, from OpDumpDone's response,
	// which is what the same real walk actually emitted.

	var all []byte
	reportedDocs := 0
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
		reportedDocs += chunk.Documents
		if chunk.EOF {
			break
		}
	}

	sum := sha256.Sum256(all)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	doneFrame, dmsg := dumpDispatch(t, s, ctx, transport.OpDumpDone, DumpDoneRequest{Database: database, Bucket: bucket, Digest: digest})
	if dmsg != "" {
		t.Fatalf("dump done %s/%s: %s", database, bucket, dmsg)
	}
	var done DumpDoneResponse
	if err := bson.Unmarshal(doneFrame.Payload, &done); err != nil {
		t.Fatalf("decode dump done response: %v", err)
	}
	if done.Digest != digest {
		t.Fatalf("server digest %s disagrees with the client's own %s over the identical bytes", done.Digest, digest)
	}
	if done.Documents != wantDocs {
		t.Fatalf("dump done reported %d documents, want %d", done.Documents, wantDocs)
	}

	rawDocs, err := splitBSONDocuments(all)
	if err != nil {
		t.Fatalf("splitting dumped bytes as a BSON stream: %v", err)
	}
	if int64(len(rawDocs)) != wantDocs {
		t.Fatalf("decoded %d BSON documents, want %d", len(rawDocs), wantDocs)
	}
	docs := make([]bson.M, len(rawDocs))
	for i, raw := range rawDocs {
		if err := bson.Unmarshal(raw, &docs[i]); err != nil {
			t.Fatalf("unmarshal document %d: %v", i, err)
		}
	}
	return docs
}

// tagsOf extracts the "tag" field from each document, in order.
func tagsOf(t *testing.T, docs []bson.M) []string {
	t.Helper()
	tags := make([]string, len(docs))
	for i, d := range docs {
		tag, _ := d["tag"].(string)
		if tag == "" {
			t.Fatalf("document %d has no string \"tag\" field: %v", i, d)
		}
		tags[i] = tag
	}
	return tags
}

func sortedCopy(s []string) []string {
	out := append([]string(nil), s...)
	sort.Strings(out)
	return out
}

// TestDumpRequiresRoot pins the root-only gate (dump-restore-spec.md §10,
// §11): no session, a non-root session, and root, in that order, against
// the same request.
func TestDumpRequiresRoot(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "users", BucketTypeHash)
	req := DumpStartRequest{Database: "app", Bucket: "users"}

	_, msg := dumpDispatch(t, s, context.Background(), transport.OpDumpStart, req)
	if !strings.Contains(msg, "root") {
		t.Fatalf("unauthenticated dump start: got %q, want a message naming the root requirement", msg)
	}

	_, msg = dumpDispatch(t, s, namedContext("alice"), transport.OpDumpStart, req)
	if !strings.Contains(msg, "root") || !strings.Contains(msg, "alice") {
		t.Fatalf("non-root dump start: got %q, want a message naming both the root requirement and the caller", msg)
	}

	_, msg = dumpDispatch(t, s, rootContext(), transport.OpDumpStart, req)
	if msg != "" {
		t.Fatalf("root dump start refused: %s", msg)
	}
	if _, msg := dumpDispatch(t, s, rootContext(), transport.OpDumpDone, DumpDoneRequest{Database: "app", Bucket: "users"}); msg != "" {
		t.Fatalf("dump done: %s", msg)
	}

	// Restore opcodes are not implemented yet (a later commit); dispatch
	// must answer a clean "unsupported opcode" rather than crash, and the
	// root gate (once Task 3 wires it) must not be bypassable by using
	// restore instead of dump. For now this just pins "does not panic".
	frame := transport.NewFrame(transport.OpRestoreStart, nil)
	resp, err := s.dispatch(rootContext(), frame)
	if err != nil {
		t.Fatalf("dispatching an unimplemented opcode returned a transport error instead of an OpError frame: %v", err)
	}
	if resp.Opcode != transport.OpError {
		t.Fatalf("OpRestoreStart (not yet implemented) answered %s, want OpError", resp.Opcode)
	}
}

// TestDumpConcurrentRefused proves dump-restore-spec.md §10's "one live
// dump per server": a second OpDumpStart while one is running is refused,
// naming the bucket already being dumped.
func TestDumpConcurrentRefused(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "a", BucketTypeHash)
	createTestBucket(t, s, "app", "b", BucketTypeHash)
	ctx := rootContext()

	if _, msg := dumpDispatch(t, s, ctx, transport.OpDumpStart, DumpStartRequest{Database: "app", Bucket: "a"}); msg != "" {
		t.Fatalf("first dump start refused: %s", msg)
	}
	t.Cleanup(func() {
		dumpDispatch(t, s, ctx, transport.OpDumpDone, DumpDoneRequest{Database: "app", Bucket: "a"})
	})

	_, msg := dumpDispatch(t, s, ctx, transport.OpDumpStart, DumpStartRequest{Database: "app", Bucket: "b"})
	if !strings.Contains(msg, "app/a") || !strings.Contains(msg, "already running") {
		t.Fatalf("second concurrent dump start: got %q, want a refusal naming the first dump (app/a) as already running", msg)
	}

	// A chunk/done call for the bucket that never started must also be
	// refused by name, not silently served against the wrong session.
	_, msg = dumpDispatch(t, s, ctx, transport.OpDumpChunk, DumpChunkRequest{Database: "app", Bucket: "b"})
	if !strings.Contains(msg, "app/b") {
		t.Fatalf("chunk on unstarted bucket: got %q, want it to name app/b", msg)
	}
}

// TestDumpDoneReleasesForNextDump proves a completed dump's slot is free:
// dumping the same bucket twice in a row must both succeed.
func TestDumpDoneReleasesForNextDump(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "a", BucketTypeHash)
	ctx := rootContext()

	for i := 0; i < 2; i++ {
		if _, msg := dumpDispatch(t, s, ctx, transport.OpDumpStart, DumpStartRequest{Database: "app", Bucket: "a"}); msg != "" {
			t.Fatalf("round %d: dump start refused: %s", i, msg)
		}
		if _, msg := dumpDispatch(t, s, ctx, transport.OpDumpDone, DumpDoneRequest{Database: "app", Bucket: "a"}); msg != "" {
			t.Fatalf("round %d: dump done refused: %s", i, msg)
		}
	}
}

// TestDumpRoundTripEachStoreType dumps one bucket of every store type and
// checks the decoded documents value-by-value (or, for FIFO/LIFO, in the
// specified order) rather than trusting a count.
func TestDumpRoundTripEachStoreType(t *testing.T) {
	s := newTestServer(t)
	ctx := rootContext()
	const n = 5

	t.Run("hash", func(t *testing.T) {
		createTestBucket(t, s, "app", "hashb", BucketTypeHash)
		var want []string
		for i := 0; i < n; i++ {
			tag := fmt.Sprintf("hash-%d", i)
			if _, err := s.Insert(InsertRequest{Database: "app", Bucket: "hashb", Key: fmt.Sprintf("k%d", i),
				Data: map[string]interface{}{"tag": tag}}); err != nil {
				t.Fatalf("insert: %v", err)
			}
			want = append(want, tag)
		}
		docs := dumpBucketAndDecode(t, s, ctx, "app", "hashb", n)
		got := tagsOf(t, docs)
		if !equalUnordered(sortedCopy(got), sortedCopy(want)) {
			t.Fatalf("hash dump tags = %v, want (in any order) %v", got, want)
		}
		for _, d := range docs {
			if _, ok := d["_id_"]; !ok {
				t.Fatalf("hash document missing _id_: %v", d)
			}
		}
	})

	t.Run("btree", func(t *testing.T) {
		createTestBucket(t, s, "app", "btreeb", BucketTypeBTree)
		var want []string
		for i := 0; i < n; i++ {
			tag := fmt.Sprintf("btree-%d", i)
			if _, err := s.Insert(InsertRequest{Database: "app", Bucket: "btreeb", Key: fmt.Sprintf("k%d", i),
				Data: map[string]interface{}{"tag": tag}}); err != nil {
				t.Fatalf("insert: %v", err)
			}
			want = append(want, tag)
		}
		docs := dumpBucketAndDecode(t, s, ctx, "app", "btreeb", n)
		got := tagsOf(t, docs)
		if !equalUnordered(sortedCopy(got), sortedCopy(want)) {
			t.Fatalf("btree dump tags = %v, want (in any order) %v", got, want)
		}
	})

	t.Run("heap", func(t *testing.T) {
		createTestBucket(t, s, "app", "heapb", BucketTypeHeap)
		var want []string
		for i := 0; i < n; i++ {
			tag := fmt.Sprintf("heap-%d", i)
			if _, err := s.Push(PushRequest{Database: "app", Bucket: "heapb", Priority: int64(i),
				Data: map[string]interface{}{"tag": tag}}); err != nil {
				t.Fatalf("push: %v", err)
			}
			want = append(want, tag)
		}
		docs := dumpBucketAndDecode(t, s, ctx, "app", "heapb", n)
		// The heap store wraps each pushed document under "value" alongside
		// its "_id_" and "priority" (store/heap.go); unwrap before comparing.
		unwrapped := make([]bson.M, len(docs))
		for i, d := range docs {
			value, ok := d["value"].(bson.M)
			if !ok {
				t.Fatalf("heap document %d has no \"value\" sub-document: %v", i, d)
			}
			unwrapped[i] = value
		}
		got := tagsOf(t, unwrapped)
		if !equalUnordered(sortedCopy(got), sortedCopy(want)) {
			t.Fatalf("heap dump tags = %v, want (in any order) %v", got, want)
		}
	})

	t.Run("fifo_order_and_nondestructive", func(t *testing.T) {
		createTestBucket(t, s, "app", "fifob", BucketTypeFIFO)
		var want []string
		for i := 0; i < n; i++ {
			tag := fmt.Sprintf("fifo-%d", i)
			if _, err := s.Push(PushRequest{Database: "app", Bucket: "fifob", Data: map[string]interface{}{"tag": tag}}); err != nil {
				t.Fatalf("push: %v", err)
			}
			want = append(want, tag)
		}
		docs := dumpBucketAndDecode(t, s, ctx, "app", "fifob", n)
		got := tagsOf(t, docs)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("FIFO dump order = %v, want push order %v", got, want)
		}

		// The dump must not have consumed the queue: every element still
		// pops in the same order (dump-restore-spec.md §7.2, §12.2).
		for i := 0; i < n; i++ {
			popResp, err := s.Pop(PopRequest{Database: "app", Bucket: "fifob"})
			if err != nil {
				t.Fatalf("pop %d after dump: %v", i, err)
			}
			var m bson.M
			if err := bson.Unmarshal(popResp.Items[0], &m); err != nil {
				t.Fatalf("decode popped doc %d: %v", i, err)
			}
			if m["tag"] != want[i] {
				t.Fatalf("pop %d after dump = %v, want tag %q", i, m, want[i])
			}
		}
	})

	t.Run("lifo_pop_order", func(t *testing.T) {
		createTestBucket(t, s, "app", "lifob", BucketTypeLIFO)
		var pushed []string
		for i := 0; i < n; i++ {
			tag := fmt.Sprintf("lifo-%d", i)
			if _, err := s.Push(PushRequest{Database: "app", Bucket: "lifob", Data: map[string]interface{}{"tag": tag}}); err != nil {
				t.Fatalf("push: %v", err)
			}
			pushed = append(pushed, tag)
		}
		docs := dumpBucketAndDecode(t, s, ctx, "app", "lifob", n)
		got := tagsOf(t, docs)
		want := make([]string, n)
		for i, tag := range pushed {
			want[n-1-i] = tag
		}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("LIFO dump order = %v, want pop order %v", got, want)
		}
	})

	t.Run("blob_metadata_only", func(t *testing.T) {
		createTestBucket(t, s, "app", "blobb", BucketTypeBlob)
		payload := []byte("this payload must never appear in the dump")
		if _, err := s.BlobWrite(BlobWriteRequest{Database: "app", Bucket: "blobb", Key: "b1",
			Data: payload, Metadata: map[string]interface{}{"tag": "blob-0"}}); err != nil {
			t.Fatalf("blob write: %v", err)
		}
		docs := dumpBucketAndDecode(t, s, ctx, "app", "blobb", 1)
		extra, _ := docs[0]["extra"].(bson.M)
		if extra == nil || extra["tag"] != "blob-0" {
			t.Fatalf("blob metadata document = %v, want extra.tag = blob-0", docs[0])
		}
		size, _ := docs[0]["size"].(int64)
		if size != int64(len(payload)) {
			t.Fatalf("blob metadata size = %v, want %d", docs[0]["size"], len(payload))
		}
	})
}

// TestDumpOversizedDocumentRefusesWholeBucket pins the decided behaviour
// (task-2 brief, dump-restore-spec.md §4): a document too large to fit in a
// single wire frame refuses the whole bucket, naming the bucket and the
// limit, before any bytes are sent — a queue bucket is used because an entry
// larger than one frame is one push away on it.
func TestDumpOversizedDocumentRefusesWholeBucket(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "big", BucketTypeFIFO)

	oversized := make([]byte, maxDumpDocumentBytes+1)
	if _, err := s.Push(PushRequest{Database: "app", Bucket: "big",
		Data: map[string]interface{}{"payload": oversized}}); err != nil {
		t.Fatalf("push oversized element: %v", err)
	}

	// OpDumpStart itself now only claims the slot and launches the one real
	// walk (fix-round-1: the size check moved into that walk instead of a
	// separate upfront pass, so it can no longer refuse synchronously
	// before the first chunk is pulled — see runDumpWalk's doc comment).
	ctx := rootContext()
	if _, msg := dumpDispatch(t, s, ctx, transport.OpDumpStart, DumpStartRequest{Database: "app", Bucket: "big"}); msg != "" {
		t.Fatalf("dump start: %s", msg)
	}
	_, msg := dumpDispatch(t, s, ctx, transport.OpDumpChunk, DumpChunkRequest{Database: "app", Bucket: "big"})
	if !strings.Contains(msg, "big") || !strings.Contains(msg, "exceeds") {
		t.Fatalf("dump chunk over an oversized document: got %q, want a refusal naming the bucket and the limit", msg)
	}

	// The refusal must release the slot: a normal-sized dump of a different
	// bucket must not be blocked by the refused attempt.
	createTestBucket(t, s, "app", "small", BucketTypeHash)
	if _, err := s.Insert(InsertRequest{Database: "app", Bucket: "small", Key: "k", Data: map[string]interface{}{"tag": "ok"}}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	_ = dumpBucketAndDecode(t, s, rootContext(), "app", "small", 1)
}

// TestDumpPinCeilingForceReleases proves the queue dump ceiling
// (dump-restore-spec.md §16.3, task-2 brief): a queue dump that does not
// call OpDumpDone within the ceiling is force-released with a named error,
// and a subsequent dump of the same bucket then succeeds.
func TestDumpPinCeilingForceReleases(t *testing.T) {
	s := newTestServer(t)
	dumpPinCeilingOverrideForTest = 50 * time.Millisecond
	t.Cleanup(func() { dumpPinCeilingOverrideForTest = 0 })
	createTestBucket(t, s, "app", "queue", BucketTypeFIFO)
	if _, err := s.Push(PushRequest{Database: "app", Bucket: "queue", Data: map[string]interface{}{"tag": "x"}}); err != nil {
		t.Fatalf("push: %v", err)
	}
	ctx := rootContext()

	if _, msg := dumpDispatch(t, s, ctx, transport.OpDumpStart, DumpStartRequest{Database: "app", Bucket: "queue"}); msg != "" {
		t.Fatalf("dump start refused: %s", msg)
	}

	// Never call OpDumpChunk/OpDumpDone: wait past the (shortened) ceiling.
	deadline := time.Now().Add(2 * time.Second)
	for {
		s.dumpMu.Lock()
		released := s.dump == nil
		s.dumpMu.Unlock()
		if released {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("dump pin was not force-released within 2s of a 50ms ceiling")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// A fresh dump of the same bucket now succeeds — the pin actually let go.
	_ = dumpBucketAndDecode(t, s, ctx, "app", "queue", 1)
}

// TestDumpConnectionCloseReleasesPin exercises watchDumpConnection directly
// (the TCP-level version, TestTCPServerDumpKillConnectionReleasesPin, drives
// it through a real socket): cancelling the context OpDumpStart was called
// with must release the session promptly, without an OpDumpDone ever
// arriving.
func TestDumpConnectionCloseReleasesPin(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "queue", BucketTypeFIFO)
	if _, err := s.Push(PushRequest{Database: "app", Bucket: "queue", Data: map[string]interface{}{"tag": "x"}}); err != nil {
		t.Fatalf("push: %v", err)
	}

	connCtx, cancel := context.WithCancel(context.Background())
	sess := &transport.Session{}
	sess.SetIdentity(transport.Identity{Username: "admin"})
	connCtx = transport.WithSession(connCtx, sess)

	if _, msg := dumpDispatch(t, s, connCtx, transport.OpDumpStart, DumpStartRequest{Database: "app", Bucket: "queue"}); msg != "" {
		t.Fatalf("dump start refused: %s", msg)
	}

	cancel() // simulate the connection closing

	var wg sync.WaitGroup
	wg.Add(1)
	deadline := time.Now().Add(2 * time.Second)
	go func() {
		defer wg.Done()
		for {
			s.dumpMu.Lock()
			released := s.dump == nil
			s.dumpMu.Unlock()
			if released || time.Now().After(deadline) {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	wg.Wait()

	s.dumpMu.Lock()
	stillHeld := s.dump != nil
	s.dumpMu.Unlock()
	if stillHeld {
		t.Fatal("dump session was not released after its connection context was cancelled")
	}

	// Prove it for real: dump again immediately, same bucket, must succeed.
	_ = dumpBucketAndDecode(t, s, rootContext(), "app", "queue", 1)
}

// TestDumpStartPublishesSessionOnlyAfterFullyWired reproduces, with an
// explicit synchronization barrier rather than relying on goroutine
// scheduling luck, the whole-branch review's Important #3.
//
// Before the fix, claimDump set s.dump = session — making the session
// reachable via activeDumpFor — before the session was fully wired and before
// the walk goroutine existed. A concurrent OpDumpDone landing in that window
// resolved the session, called stopWalk (a no-op: walkDone was still nil) and
// releaseDump — and because releaseDump's sync.Once had already fired, the
// eventual walk goroutine would block forever trying to send a document to a
// channel nobody would read. The segment store's compaction pin, which the
// race also left stuck, went with the segment store (build doc Step 12); the
// slot and the walk are what is left to prove.
//
// This test uses startDumpTestBarrier to force a concurrent OpDumpDone into
// the earliest possible instant after claimDump returns and proves the slot is
// released and the walk goroutine actually exits.
func TestDumpStartPublishesSessionOnlyAfterFullyWired(t *testing.T) {
	s := newTestServer(t)
	dumpPinCeilingOverrideForTest = 50 * time.Millisecond
	t.Cleanup(func() { dumpPinCeilingOverrideForTest = 0 })
	createTestBucket(t, s, "app", "queue", BucketTypeFIFO)
	if _, err := s.Push(PushRequest{Database: "app", Bucket: "queue", Data: map[string]interface{}{"tag": "x"}}); err != nil {
		t.Fatalf("push: %v", err)
	}

	ctx, cancel := context.WithCancel(rootContext())
	defer cancel() // let any straggler watchDumpConnection goroutine exit

	reachedBarrier := make(chan struct{})
	releaseRaced := make(chan struct{})
	startDumpTestBarrier = func() {
		close(reachedBarrier)
		<-releaseRaced
	}
	t.Cleanup(func() { startDumpTestBarrier = nil })

	type startResult struct {
		resp *DumpStartResponse
		err  error
	}
	startCh := make(chan startResult, 1)
	go func() {
		resp, err := s.startDump(ctx, DumpStartRequest{Database: "app", Bucket: "queue"})
		startCh <- startResult{resp, err}
	}()

	<-reachedBarrier // startDump has claimed the session; s.dump is now visible

	type doneResult struct {
		resp *DumpDoneResponse
		err  error
	}
	doneCh := make(chan doneResult, 1)
	go func() {
		// This lands in exactly the window the pre-fix code left open:
		// s.dump is visible (claimDump already returned), but startDump has
		// not yet launched the walk goroutine, called watchDumpConnection,
		// or armed the pin-ceiling timer.
		resp, err := s.dumpDone(ctx, DumpDoneRequest{Database: "app", Bucket: "queue"})
		doneCh <- doneResult{resp, err}
	}()

	// Give dumpDone a chance to actually reach activeDumpFor/stopWalk before
	// unblocking startDump — not required for correctness (activeDumpFor
	// only needs s.dump != nil, already true), but makes the interleaving
	// this test targets more likely on every run rather than only some.
	time.Sleep(10 * time.Millisecond)
	close(releaseRaced)

	startRes := <-startCh
	doneRes := <-doneCh
	// The barrier's job is done: clear it now, not only in t.Cleanup, so
	// the "fresh dump" call below (and any other startDump this test
	// triggers) runs unbarriered rather than re-invoking a closure that
	// would double-close reachedBarrier.
	startDumpTestBarrier = nil

	if startRes.err != nil {
		t.Fatalf("dump start returned an error after racing a concurrent OpDumpDone: %v", startRes.err)
	}
	if doneRes.err != nil {
		t.Fatalf("concurrent dump done was refused: %v", doneRes.err)
	}

	// The server's single dump slot must not be stuck.
	deadline := time.Now().Add(2 * time.Second)
	for {
		s.dumpMu.Lock()
		released := s.dump == nil
		s.dumpMu.Unlock()
		if released {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("s.dump was not released within 2s of the racing OpDumpDone completing")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// And the walk goroutine must have actually exited, not be blocked
	// forever on a send: a fresh dump of the same bucket must succeed, which
	// requires the server slot to be genuinely free.
	docs := dumpBucketAndDecode(t, s, rootContext(), "app", "queue", 1)
	if got := tagsOf(t, docs); len(got) != 1 || got[0] != "x" {
		t.Fatalf("post-race dump of app/queue = %v, want [\"x\"]", got)
	}
}

func equalUnordered(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// dumpWithUnreadableIndexes opens a bucket whose catalog record holds the
// given index elements, one of them written by a newer binary, on a server
// restarted over it, and starts a dump of it as root.
func dumpWithUnreadableIndexes(t *testing.T, indices []bson.M) (message string) {
	t.Helper()
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	if _, err := first.core.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if resp, err := first.core.CreateBucket(CreateBucketRequest{Database: "app", Name: "users", Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("CreateBucket = %+v, %v", resp, err)
	}
	mustInsert(t, first.core, "app", "users", "u1", map[string]interface{}{"Email": "a@example.com", "Name": "a"})
	writeCatalogIndices(t, first.core, "app", "users", indices)
	s := restartCatalog(t, first, dataDir).core

	_, message = dumpDispatch(t, s, rootContext(), transport.OpDumpStart, DumpStartRequest{Database: "app", Bucket: "users"})
	return message
}

// TestDumpOfAnUnreadableIndexIsRefusedOrRestoredIntact: a dump
// that carried an index this binary cannot read as a plain one, or not at
// all, would restore a unique constraint as a non-unique index or lose it,
// with nothing said (rule 10). This build refuses the dump, naming the index
// and the way out; it never writes a manifest that misdescribes the bucket.
func TestDumpOfAnUnreadableIndexIsRefusedOrRestoredIntact(t *testing.T) {
	newerUnique := bson.M{"id": int32(0), "name": "Email_1", "fields": bson.A{"Email"}, "unique": true, "version": int32(3)}
	readable := bson.M{"id": int32(1), "name": "Name_1", "fields": bson.A{"Name"}}
	tests := []struct {
		name    string
		indices []bson.M
	}{
		{"every index unreadable", []bson.M{newerUnique}},
		{"one index unreadable among readable ones", []bson.M{newerUnique, readable}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			message := dumpWithUnreadableIndexes(t, tt.indices)
			if !strings.Contains(message, "Email_1") || !strings.Contains(message, "newer") {
				t.Fatalf("dump start answered %q, want a refusal naming the index Email_1 and the newer binary that wrote it", message)
			}
		})
	}
}

// TestDumpOfAnIndexWithNoReadableFieldsDoesNotPanic is a
// unconfirmed hazard: an index element whose typed decode failed is held with
// no Fields, and bucketIndexNames read Fields[0] of every descriptor.
func TestDumpOfAnIndexWithNoReadableFieldsDoesNotPanic(t *testing.T) {
	broken := bson.M{"id": int32(0), "name": "Email_1", "fields": "Email", "unique": true, "version": int32(3)}
	message := dumpWithUnreadableIndexes(t, []bson.M{broken})
	if !strings.Contains(message, "Email_1") {
		t.Fatalf("dump start answered %q, want a refusal naming the index Email_1", message)
	}
}
