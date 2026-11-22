package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/transport"
)

// newSocialCountingServer is newSocialServer on the fault store with its
// Syncs counted (ack_sync_test.go).
func newSocialCountingServer(t *testing.T, users int) (*Server, *syncCountingKV) {
	t.Helper()
	kv := &syncCountingKV{Store: kvfault.New()}
	s := newKVFaultServer(t.TempDir(), kv)
	seedSocialUsers(t, s, 1, users)
	return s, kv
}

// TestBatchGroupCommits is T-10 on one node: 21 pushes to 21 buckets cost
// exactly one kv.Sync, and none under unsafe_ack_before_sync. Break: one
// acknowledgeDurably per item, in series, gives 21.
func TestBatchGroupCommits(t *testing.T) {
	s, kv := newSocialCountingServer(t, 21)
	items := make([]BatchItem, 0, 21)
	for u := 1; u <= 21; u++ {
		items = append(items, pushItem(t, u, "Feed", map[string]interface{}{"n": u}))
	}
	before := kv.syncs.Load()
	sendBatchOK(t, context.Background(), s, batchFrameOf(t, items...))
	if got := kv.syncs.Load() - before; got != 1 {
		t.Fatalf("21 pushes in one batch took %d syncs; the batch's writes share one", got)
	}
	s.config.Storage.UnsafeAckBeforeSync = true
	before = kv.syncs.Load()
	sendBatchOK(t, context.Background(), s, batchFrameOf(t, items...))
	if got := kv.syncs.Load() - before; got != 0 {
		t.Fatalf("under unsafe_ack_before_sync a batch took %d syncs; it takes none", got)
	}
}

// TestBatchSyncFailureMarksWrites is T-18: when the batch's one sync fails,
// every write answered a success is answered unknown with the not-durable
// text, and reads are unchanged.
func TestBatchSyncFailureMarksWrites(t *testing.T) {
	s, kv := newSocialCountingServer(t, 2)
	kv.failWith = errors.New("input/output error")
	answers := sendBatchOK(t, context.Background(), s, batchFrameOf(t,
		pushItem(t, 1, "Feed", map[string]interface{}{"n": 1}),
		lenItem(t, 2, "Feed"),
		pushItem(t, 2, "Feed", map[string]interface{}{"n": 2}),
	))
	for _, i := range []int{0, 2} {
		refusal := batchAnswerError(t, answers[i], fmt.Sprintf("write item %d", i+1))
		if refusal.WriteOutcome != outcomeUnknown || !strings.Contains(refusal.Message, "could not be written to disk") {
			t.Errorf("write item %d after a failed sync: outcome %q, %q; want unknown with the not-durable text", i+1, refusal.WriteOutcome, refusal.Message)
		}
	}
	batchAnswerField(t, answers[1], "the read")
}

// TestBatchReplyBudget is T-15 on one node, at the smallest budget (1 MiB):
// (a) reads past the budget answer RESULT_TOO_LARGE with the budget and the
// bytes charged before them, while the writes beside them are answered
// applied; (b) keyed updates past the budget are applied and answered
// doc_omitted, and the answers the batch held at once never pass the budget
// plus 64 times the largest answer. Break: charge at assembly instead of as a
// prefix, and (b)'s held bytes reach every answer's.
func TestBatchReplyBudget(t *testing.T) {
	s := newSocialServer(t, 20)
	s.config.Query.MaxResultBytes = 1 << 20
	budget := s.batchReplyBudget()
	large := strings.Repeat("r", 200<<10)
	for u := 1; u <= 10; u++ {
		answers := sendBatchOK(t, context.Background(), s, batchFrameOf(t, putItem(t, u, "Posts", "big", map[string]interface{}{"text": large})))
		batchAnswerField(t, answers[0], "seeding a large element")
	}

	t.Run("reads", func(t *testing.T) {
		items := make([]BatchItem, 0, 20)
		for u := 1; u <= 10; u++ {
			items = append(items, getItem(t, u, "Posts", "big"), pushItem(t, 10+u, "Feed", map[string]interface{}{"n": u}))
		}
		answers := sendBatchOK(t, context.Background(), s, batchFrameOf(t, items...))
		var replyBytes int64
		trimmed := 0
		for i, answer := range answers {
			replyBytes += int64(len(answer.Payload))
			if i%2 == 1 {
				batchAnswerField(t, answer, fmt.Sprintf("write item %d", i+1))
				continue
			}
			if transport.Opcode(answer.Opcode) != transport.OpError {
				continue
			}
			trimmed++
			refusal := batchAnswerError(t, answer, fmt.Sprintf("read item %d", i+1))
			if transport.ErrorCode(refusal.Code) != transport.ErrResultTooLarge || refusal.MaxResultBytes != budget || refusal.RetainedBytes == 0 {
				t.Errorf("read item %d: %+v; want RESULT_TOO_LARGE with max_result_bytes %d and the bytes retained before it", i+1, refusal, budget)
			}
			if !strings.Contains(refusal.Message, "the batch's reply is full (1 MiB), so this read's answer was left out; nothing changed") {
				t.Errorf("read item %d's text: %s", i+1, refusal.Message)
			}
		}
		if trimmed == 0 {
			t.Fatal("no read was answered RESULT_TOO_LARGE though the reads total about 2 MiB against a 1 MiB budget")
		}
		if replyBytes > budget+64<<10 {
			t.Fatalf("the answers total %d bytes against a budget of %d", replyBytes, budget)
		}
	})

	t.Run("updates", func(t *testing.T) {
		const keys = 200
		document := strings.Repeat("d", 100<<10)
		for k := 0; k < keys; k++ {
			if resp, err := s.Insert(InsertRequest{Database: batchTestDB, Bucket: userBucket(1 + k%20), Key: fmt.Sprintf("doc-%d", k),
				Data: map[string]interface{}{"body": document, "posts_n": int64(0)}}); err != nil || !resp.Success {
				t.Fatalf("inserting doc-%d: %+v, %v", k, resp, err)
			}
		}
		var peak atomic.Int64
		hook := func(held int64) { peak.Store(held) }
		batchHeldBytesHook.Store(&hook)
		t.Cleanup(func() { batchHeldBytesHook.Store(nil) })
		items := make([]BatchItem, 0, keys)
		for k := 0; k < keys; k++ {
			items = append(items, incItem(t, 1+k%20, fmt.Sprintf("doc-%d", k)))
		}
		answers := sendBatchOK(t, context.Background(), s, batchFrameOf(t, items...))
		omitted, largest := 0, int64(0)
		for i, answer := range answers {
			var update UpdateResponse
			if err := bson.Unmarshal(answer.Payload, &update); err != nil || transport.Opcode(answer.Opcode) != transport.OpUpdate || !update.Success {
				t.Fatalf("update %d answered %s: %+v, %v", i+1, transport.Opcode(answer.Opcode), update, err)
			}
			if update.DocOmitted {
				omitted++
				if update.Doc != nil {
					t.Fatalf("update %d is doc_omitted and carries its document", i+1)
				}
			}
			largest = max(largest, int64(len(answer.Payload)))
		}
		if omitted == 0 || omitted == keys {
			t.Fatalf("%d of %d updates answered doc_omitted; the budget should keep some documents and leave out the rest", omitted, keys)
		}
		largest = max(largest, int64(len(document))+1024)
		if bound := budget + batchWindow*largest; peak.Load() > bound {
			t.Fatalf("the batch held %d answer bytes at once; the bound is the budget plus 64 times the largest answer, %d", peak.Load(), bound)
		}
		resp, err := s.Find(FindRequest{Database: batchTestDB, Bucket: userBucket(1 + (keys-1)%20), Key: fmt.Sprintf("doc-%d", keys-1)})
		if err != nil || !resp.Found {
			t.Fatalf("reading the last updated document: %+v, %v", resp, err)
		}
		if n, _ := resp.Data.Lookup("posts_n").AsInt64OK(); n != 1 {
			t.Fatalf("the last updated document holds posts_n %d; an update answered doc_omitted is applied", n)
		}
	})
}

// TestBatchReadsTakeQuerySlots is T-16 on one node.
func TestBatchReadsTakeQuerySlots(t *testing.T) {
	t.Run("a read waits for a slot while its batch's writes run", func(t *testing.T) {
		s := newSocialServer(t, 2)
		gate := s.queryGate()
		pool := cap(gate.execute)
		releases := make([]func(), 0, pool)
		for i := 0; i < pool; i++ {
			release, err := gate.acquire(context.Background(), gate.executePool())
			if err != nil {
				t.Fatal(err)
			}
			releases = append(releases, release)
		}
		done := make(chan []BatchAnswer, 1)
		go func() {
			done <- sendBatchOK(t, context.Background(), s, batchFrameOf(t, lenItem(t, 1, "Feed"), pushItem(t, 2, "Feed", map[string]interface{}{"n": 1})))
		}()
		deadline := time.Now().Add(5 * time.Second)
		for feedLen(t, s, 2, "Feed") != 1 {
			if time.Now().After(deadline) {
				t.Fatal("the batch's write did not run while its read waited for a slot")
			}
			time.Sleep(5 * time.Millisecond)
		}
		select {
		case <-done:
			t.Fatal("the batch answered while every query slot was held: its read ran without one")
		case <-time.After(50 * time.Millisecond):
		}
		for _, release := range releases {
			release()
		}
		<-done
	})
	t.Run("pool + 1 reads in one batch complete", func(t *testing.T) {
		s := newSocialServer(t, 1)
		pool := cap(s.queryGate().execute)
		seedSocialUsers(t, s, 2, pool+1)
		items := make([]BatchItem, 0, pool+1)
		for u := 1; u <= pool+1; u++ {
			items = append(items, getItem(t, u, "Posts", "none"))
		}
		requireWithin(t, 10*time.Second, "a batch of pool + 1 reads", func() {
			sendBatchOK(t, context.Background(), s, batchFrameOf(t, items...))
		})
	})
	t.Run("pool/2 + 1 post-page batches complete beside a lone query", func(t *testing.T) {
		s := newSocialServer(t, 1)
		pool := cap(s.queryGate().execute)
		var wg sync.WaitGroup
		requireWithin(t, 10*time.Second, "pool/2 + 1 concurrent post-page batches", func() {
			for b := 0; b < pool/2+1; b++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					sendBatchOK(t, context.Background(), s, batchFrameOf(t, getItem(t, 1, "Posts", "p"), lenItem(t, 1, "Feed"), lenItem(t, 1, "Inbox")))
				}()
			}
			handlerSend(t, NewHandler(s), transport.OpCount, CountRequest{Database: batchTestDB, Bucket: userBucket(1)})
			wg.Wait()
		})
	})
}

// requireWithin fails the test when run does not return within limit.
func requireWithin(t *testing.T, limit time.Duration, what string, run func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		run()
	}()
	select {
	case <-done:
	case <-time.After(limit):
		t.Fatalf("%s did not complete within %s", what, limit)
	}
}

// TestBatchNodeWideItemBound is T-17: five concurrent batches of 100 items,
// each item held at the apply hook, put at most 256 items in flight on the
// node and at most 64 of any one batch. Break: drop the semaphore, and the
// five windows put 320 in flight.
func TestBatchNodeWideItemBound(t *testing.T) {
	const batches, perBatch = 5, 100
	s := newSocialServer(t, batches*perBatch)
	var inUse, peakSlots atomic.Int64
	slotHook := func(n int) {
		inUse.Store(int64(n))
		for {
			peak := peakSlots.Load()
			if int64(n) <= peak || peakSlots.CompareAndSwap(peak, int64(n)) {
				return
			}
		}
	}
	batchSlotTakenHook.Store(&slotHook)
	t.Cleanup(func() { batchSlotTakenHook.Store(nil) })

	release := make(chan struct{})
	var mu sync.Mutex
	held := make(map[int32]int)
	peakPerBatch := 0
	applyHook := func(frame *transport.Frame) {
		if frame.Opcode != transport.OpFieldPush {
			return
		}
		tag, _ := bson.Raw(frame.Payload).Lookup("data", "b").Int32OK()
		mu.Lock()
		held[tag]++
		peakPerBatch = max(peakPerBatch, held[tag])
		mu.Unlock()
		<-release
		mu.Lock()
		held[tag]--
		mu.Unlock()
	}
	dispatchApplyHook.Store(&applyHook)
	t.Cleanup(func() { dispatchApplyHook.Store(nil) })

	var wg sync.WaitGroup
	for b := 0; b < batches; b++ {
		items := make([]BatchItem, 0, perBatch)
		for i := 0; i < perBatch; i++ {
			items = append(items, pushItem(t, 1+b*perBatch+i, "Feed", map[string]interface{}{"b": int32(b)}))
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			sendBatchOK(t, context.Background(), s, batchFrameOf(t, items...))
		}()
	}
	deadline := time.Now().Add(10 * time.Second)
	for inUse.Load() < batchNodeSlots && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	if got := peakSlots.Load(); got != batchNodeSlots {
		t.Fatalf("at most %d batch items were in flight; five batches held at the hook fill the node's %d slots, and never more", got, batchNodeSlots)
	}
	if peakPerBatch > batchWindow {
		t.Fatalf("%d items of one batch were in flight at once; the window is %d", peakPerBatch, batchWindow)
	}
}

// FuzzBatchDecode is T-12: arbitrary bytes and item arrays never panic the
// decode, the write scan or the depth refusal, and a refusal is a batch
// refusal with a code.
func FuzzBatchDecode(f *testing.F) {
	seed := func(v any) {
		raw, err := bson.Marshal(v)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(raw)
	}
	seed(bson.D{{Key: "items", Value: bson.A{}}})
	seed(bson.D{{Key: "items", Value: bson.A{bson.D{{Key: "opcode", Value: int32(0xD0)}, {Key: "payload", Value: bson.D{{Key: "bucket", Value: "b"}}}}}}})
	seed(bson.D{{Key: "items", Value: bson.A{int32(4), "x", bson.D{{Key: "opcode", Value: "z"}}}}})
	seed(bson.D{{Key: "items", Value: "nope"}, {Key: "writeconcern", Value: int32(3)}})
	f.Add([]byte{0x05, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, payload []byte) {
		batchCarriesWrite(payload)
		_ = batchDepthRefusal(payload, errBatchShape)
		decoded, err := decodeBatch(payload)
		if err != nil {
			var refusal *batchRefusal
			if !errors.As(err, &refusal) || refusal.code == transport.ErrOK {
				t.Fatalf("decode refused with %T %v; every refusal is a batch refusal with a code", err, err)
			}
			return
		}
		if len(decoded.items) == 0 || len(decoded.items) > batchMaxItems {
			t.Fatalf("decoded %d items", len(decoded.items))
		}
	})
}
