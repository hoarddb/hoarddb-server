package server

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/transport"
)

// syncCountingKV is the fault store with its Syncs counted, and failed on demand.
type syncCountingKV struct {
	*kvfault.Store
	syncs    atomic.Int64
	failWith error
}

func (kv *syncCountingKV) Sync() error {
	kv.syncs.Add(1)
	if kv.failWith != nil {
		return kv.failWith
	}
	return kv.Store.Sync()
}

func newCountingServer(t *testing.T) (*Server, *syncCountingKV) {
	t.Helper()
	kv := &syncCountingKV{Store: kvfault.New()}
	s := newKVFaultServer(t.TempDir(), kv)
	createDurabilityBucket(t, s, "users", BucketTypeHash)
	createDurabilityBucket(t, s, "jobs", BucketTypeFIFO)
	return s, kv
}

// TestAClientFrameSyncsOnceAndAReadNever: a batch of documents is one sync,
// and a frame that commits nothing — a read, a pop of an empty queue — waits
// for none.
func TestAClientFrameSyncsOnceAndAReadNever(t *testing.T) {
	s, kv := newCountingServer(t)
	handler := NewHandler(s)
	cases := []struct {
		name  string
		op    transport.Opcode
		req   any
		syncs int64
	}{
		{"batch of three", transport.OpBatchPut, BatchInsertRequest{Database: "app", Bucket: "users", Data: []map[string]interface{}{{"_id_": "a"}, {"_id_": "b"}, {"_id_": "c"}}}, 1},
		{"read", transport.OpGet, FindRequest{Database: "app", Bucket: "users", Key: "a"}, 0},
		{"pop of an empty queue", transport.OpFIFOPop, PopRequest{Database: "app", Bucket: "jobs"}, 0},
	}
	for _, tc := range cases {
		before := kv.syncs.Load()
		handlerSend(t, handler, tc.op, tc.req)
		if got := kv.syncs.Load() - before; got != tc.syncs {
			t.Errorf("%s: %d syncs, want %d", tc.name, got, tc.syncs)
		}
	}
}

// TestAFailedSyncIsNotAcknowledged: a write whose sync fails is answered
// with the failure, never with success, both in process and to a client.
func TestAFailedSyncIsNotAcknowledged(t *testing.T) {
	s, kv := newCountingServer(t)
	kv.failWith = errors.New("input/output error")

	resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "users", Key: "ada", Data: map[string]interface{}{"name": "Ada"}})
	if !errors.Is(err, errWriteNotDurable) || resp != nil {
		t.Fatalf("Insert with a failing sync = %+v, %v; want no answer and errWriteNotDurable", resp, err)
	}
	if !strings.Contains(err.Error(), "input/output error") {
		t.Errorf("the error %q does not carry the sync's cause", err)
	}

	frame, err := NewHandler(s)(t.Context(), transport.NewFrame(transport.OpPut,
		mustMarshalBSON(t, InsertRequest{Database: "app", Bucket: "users", Key: "bob", Data: map[string]interface{}{"name": "Bob"}})))
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if frame.Opcode != transport.OpError || !strings.Contains(string(frame.Payload), "not acknowledged") {
		t.Fatalf("a client PUT whose sync failed was answered %s: %.300s", frame.Opcode, frame.Payload)
	}
	// The write committed in memory, so a retry is not known to be safe.
	if outcome, _ := bson.Raw(frame.Payload).Lookup("write_outcome").StringValueOK(); outcome != outcomeUnknown {
		t.Fatalf("a client PUT whose sync failed carries write_outcome %q, want %q (it may be stored)", outcome, outcomeUnknown)
	}
}

// syncFailurePop is one kind of pop on a node that does not route: a bucket
// pop of a fifo, a lifo or a heap bucket, or the pop of a fifo store field.
type syncFailurePop struct {
	kind   string
	op     transport.Opcode
	req    any
	target string
}

// newSyncFailurePops is a counting server with two entries, marked by their
// n, in each kind of pop BUG-0548's path 1 covers.
func newSyncFailurePops(t *testing.T) (*Server, *syncCountingKV, transport.RequestHandler, []syncFailurePop) {
	t.Helper()
	s, kv := newCountingServer(t)
	createDurabilityBucket(t, s, "stack", BucketTypeLIFO)
	createDurabilityBucket(t, s, "ranked", BucketTypeHeap)
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "posts", Type: BucketTypeHash,
		Options: storesOption(StoreDeclaration{Name: "Comments", Engine: "fifoStore"})}); err != nil || !resp.Success {
		t.Fatalf("create posts: %+v, %v", resp, err)
	}
	handler := NewHandler(s)
	handlerSend(t, handler, transport.OpPut, InsertRequest{Database: "app", Bucket: "posts", Key: "p1", Data: map[string]interface{}{"title": "p1"}})
	for priority, n := range []string{"first", "second"} {
		handlerSend(t, handler, transport.OpFIFOPush, PushRequest{Database: "app", Bucket: "jobs", Data: map[string]interface{}{"n": "job-" + n}})
		handlerSend(t, handler, transport.OpLIFOPush, PushRequest{Database: "app", Bucket: "stack", Data: map[string]interface{}{"n": "plate-" + n}})
		handlerSend(t, handler, transport.OpHeapPush, PushRequest{Database: "app", Bucket: "ranked", Data: map[string]interface{}{"n": "task-" + n},
			Priority: int64(priority + 1)})
		handlerSend(t, handler, transport.OpFieldPush, FieldRequest{Database: "app", Bucket: "posts", Key: "p1", Field: "Comments",
			Data: map[string]interface{}{"n": "comment-" + n}})
	}
	pops := []syncFailurePop{
		{"fifo pop", transport.OpFIFOPop, PopRequest{Database: "app", Bucket: "jobs"}, "app.jobs"},
		{"lifo pop", transport.OpLIFOPop, PopRequest{Database: "app", Bucket: "stack"}, "app.stack"},
		{"heap pop", transport.OpHeapPop, PopRequest{Database: "app", Bucket: "ranked"}, "app.ranked"},
		{"store-field pop", transport.OpFieldPop, FieldRequest{Database: "app", Bucket: "posts", Key: "p1", Field: "Comments"}, `app.posts["p1"].Comments`},
	}
	return s, kv, handler, pops
}

// queueHeadOf is the n of the entry the next pop would take, read from s's
// own store, or "" when there is none.
func queueHeadOf(t *testing.T, s *Server, pop syncFailurePop) string {
	t.Helper()
	if fieldRequest, isField := pop.req.(FieldRequest); isField {
		peeked, err := s.FieldPeek(FieldPeekRequest{FieldRequest: fieldRequest})
		if err != nil {
			t.Fatalf("peeking %s: %v", pop.target, err)
		}
		if len(peeked.Items) == 0 {
			return ""
		}
		return markerOf(peeked.Items[0].Element)
	}
	popRequest := pop.req.(PopRequest)
	peeked, err := s.Peek(PeekRequest{Database: popRequest.Database, Bucket: popRequest.Bucket})
	if err != nil {
		t.Fatalf("peeking %s: %v", pop.target, err)
	}
	if len(peeked.Items) == 0 {
		return ""
	}
	return markerOf(peeked.Items[0])
}

// TestAPopWhoseSyncFailsDoesNotLoseItsEntry is BUG-0548's path 1. A pop on a
// node that does not route commits its removal, then the sync after the
// commit fails. The entry must then be in the caller's hands or still at the
// head of the queue, for every kind of pop; before the fix the node answered
// an error without it and the removal stood, so it was in neither.
func TestAPopWhoseSyncFailsDoesNotLoseItsEntry(t *testing.T) {
	s, kv, handler, pops := newSyncFailurePops(t)
	for _, pop := range pops {
		head := queueHeadOf(t, s, pop)
		if head == "" {
			t.Fatalf("test setup: %s has nothing to pop", pop.target)
		}
		kv.failWith = errors.New("input/output error")
		answer, err := handler(t.Context(), transport.NewFrame(pop.op, mustMarshalBSON(t, pop.req)))
		kv.failWith = nil
		if err != nil {
			t.Fatalf("%s: handler: %v", pop.kind, err)
		}
		if poppedMarker(answer) == head {
			continue
		}
		if now := queueHeadOf(t, s, pop); now != head {
			t.Errorf("a %s whose sync failed removed the entry %q, which is in neither the answer nor the queue (its head is now %q); the answer was %s: %.400s",
				pop.kind, head, now, answer.Opcode, answer.Payload)
		}
	}
}

// TestAPopWhoseSyncFailsSaysItsRemovalIsNotDurable is the contract of
// BUG-0548's path 1 (owner ruling, 2026-10-14): a pop whose removal committed
// but could not be synced is answered success with its entry, a pop-only
// exception to the rule that an unsynced write is not acknowledged. Its
// write_outcome is applied (the removal is committed and every reader sees
// it) and its message names what was popped, the sync's error, that the entry
// may be served again after a restart or a power loss, and the disk to check.
// A pop of an empty queue commits nothing and waits for no sync, so it is
// answered not found as before; a put keeps its error
// (TestAFailedSyncIsNotAcknowledged).
func TestAPopWhoseSyncFailsSaysItsRemovalIsNotDurable(t *testing.T) {
	s, kv, handler, pops := newSyncFailurePops(t)
	createDurabilityBucket(t, s, "idle", BucketTypeFIFO)
	kv.failWith = errors.New("input/output error")
	for _, pop := range pops {
		head := queueHeadOf(t, s, pop)
		answer, err := handler(t.Context(), transport.NewFrame(pop.op, mustMarshalBSON(t, pop.req)))
		if err != nil {
			t.Fatalf("%s: handler: %v", pop.kind, err)
		}
		if answer.Opcode == transport.OpError {
			t.Errorf("a %s whose sync failed was answered an error: %.400s", pop.kind, answer.Payload)
			continue
		}
		if got := poppedMarker(answer); got != head {
			t.Errorf("a %s whose sync failed answered the entry %q, want %q", pop.kind, got, head)
		}
		reply := bson.Raw(answer.Payload)
		if outcome, _ := reply.Lookup("write_outcome").StringValueOK(); outcome != outcomeApplied {
			t.Errorf("a %s whose sync failed carries write_outcome %q, want %q", pop.kind, outcome, outcomeApplied)
		}
		if _, counted := reply.Lookup("copies_confirmed").Int32OK(); counted {
			t.Errorf("a %s on a single node carries copies_confirmed: %s", pop.kind, reply)
		}
		message, _ := reply.Lookup("message").StringValueOK()
		for _, want := range []string{"popped from " + pop.target + ", but", "input/output error",
			"may be served again after a restart or a power loss", "Check the node's disk"} {
			if !strings.Contains(message, want) {
				t.Errorf("the answer to a %s whose sync failed does not say %q: %s", pop.kind, want, message)
			}
		}
	}

	answer := handlerSend(t, handler, transport.OpFIFOPop, PopRequest{Database: "app", Bucket: "idle"})
	if found, _ := bson.Raw(answer.Payload).Lookup("found").BooleanOK(); found {
		t.Errorf("a pop of an empty queue answered found: %s", bson.Raw(answer.Payload))
	}
	if message, _ := bson.Raw(answer.Payload).Lookup("message").StringValueOK(); strings.Contains(message, "popped") {
		t.Errorf("a pop of an empty queue says it popped: %s", message)
	}
}

// TestUnsafeAckBeforeSyncCanLoseAnAcknowledgedWrite pins what the knob is
// documented to do: the node answers before the sync, so a power loss can
// take the write, and the node says so when it starts.
func TestUnsafeAckBeforeSyncCanLoseAnAcknowledgedWrite(t *testing.T) {
	dataDir := t.TempDir()
	kv := kvfault.New()
	s := newKVFaultServer(dataDir, kv)
	s.config.Storage.UnsafeAckBeforeSync = true
	if warning := s.unsafeAckWarning(); !strings.Contains(warning, "power cut") || !strings.Contains(warning, "HOARDDB_UNSAFE_ACK_BEFORE_SYNC") {
		t.Errorf("start-up warning %q does not name the risk and the variable", warning)
	}
	createDurabilityBucket(t, s, "users", BucketTypeHash)
	if err := kv.Sync(); err != nil {
		t.Fatalf("sync after setup: %v", err)
	}
	mustInsert(t, s, "app", "users", "ada", map[string]interface{}{"name": "Ada"})

	kv.PowerLoss()
	restarted := newKVFaultServer(dataDir, kv)
	if err := restarted.LoadCatalog(); err != nil {
		t.Fatalf("LoadCatalog after the power loss: %v", err)
	}
	if found, err := restarted.Find(FindRequest{Database: "app", Bucket: "users", Key: "ada"}); err != nil || found.Found {
		t.Fatalf("with unsafe_ack_before_sync the unsynced insert survived a power loss (%+v, %v): the knob does not relax anything", found, err)
	}
}

func TestUnsafeAckWarningIsSilentByDefault(t *testing.T) {
	if warning := newKVFaultServer(t.TempDir(), kvfault.New()).unsafeAckWarning(); warning != "" {
		t.Fatalf("a node with the default configuration warns %q", warning)
	}
}

// slowSyncKV counts commits and records, for each Sync, the commits made
// before it started; a Sync makes those durable when it ends.
type slowSyncKV struct {
	storage.KV
	committed atomic.Uint64
	durable   atomic.Uint64
	syncs     atomic.Int64
}

func (kv *slowSyncKV) Sync() error {
	kv.syncs.Add(1)
	covered := kv.committed.Load()
	time.Sleep(2 * time.Millisecond)
	for {
		was := kv.durable.Load()
		if covered <= was || kv.durable.CompareAndSwap(was, covered) {
			return nil
		}
	}
}

// TestAckSyncGroupSharesASyncAndCoversEveryCaller: concurrent callers share
// Syncs, and none returns before a Sync that started after its commit ended.
func TestAckSyncGroupSharesASyncAndCoversEveryCaller(t *testing.T) {
	const callers = 64
	kv := &slowSyncKV{}
	var group ackSyncGroup
	var wg sync.WaitGroup
	uncovered := make(chan string, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mine := kv.committed.Add(1)
			if err := group.syncAfter(kv); err != nil {
				uncovered <- fmt.Sprintf("commit %d: %v", mine, err)
				return
			}
			if durable := kv.durable.Load(); durable < mine {
				uncovered <- fmt.Sprintf("commit %d answered with only %d durable", mine, durable)
			}
		}()
	}
	wg.Wait()
	close(uncovered)
	for failure := range uncovered {
		t.Error(failure)
	}
	if syncs := kv.syncs.Load(); syncs >= callers {
		t.Errorf("%d callers took %d syncs: concurrent writes did not share one", callers, syncs)
	}
}

// TestASyncWaitsForAWriteBeingApplied: a Sync does not start while another
// write is still being applied, so that write's commit is answered by it too
// rather than by the next one.
func TestASyncWaitsForAWriteBeingApplied(t *testing.T) {
	kv := &slowSyncKV{}
	var group ackSyncGroup
	group.writeStarted()
	answered := make(chan error, 1)
	go func() {
		kv.committed.Add(1)
		answered <- group.syncAfter(kv)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		group.mu.Lock()
		gathering := group.drained != nil
		group.mu.Unlock()
		if gathering {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the Sync never waited for the write being applied")
		}
		runtime.Gosched()
	}
	second := kv.committed.Add(1)
	group.writeEnded()
	if err := <-answered; err != nil {
		t.Fatalf("syncAfter: %v", err)
	}
	if durable := kv.durable.Load(); durable < second || kv.syncs.Load() != 1 {
		t.Fatalf("one Sync should have covered both commits: %d syncs, %d of %d commits durable", kv.syncs.Load(), durable, second)
	}
}

// TestALoggedWriteIsNotSyncedAgain: a write applied with an apply sequence
// was logged, and the log was synced before the apply; the store is not
// synced a second time for it.
func TestALoggedWriteIsNotSyncedAgain(t *testing.T) {
	s, kv := newCountingServer(t)
	before := kv.syncs.Load()
	ctx := withApplySeq(t.Context(), 7)
	resp, err := acknowledgeDurably(ctx, s, func(ctx context.Context) (*InsertResponse, error) {
		return s.InsertCtx(ctx, InsertRequest{Database: "app", Bucket: "users", Key: "ada", Data: map[string]interface{}{"name": "Ada"}})
	})
	if err != nil || !resp.Success {
		t.Fatalf("logged insert: %+v, %v", resp, err)
	}
	if got := kv.syncs.Load() - before; got != 0 {
		t.Fatalf("a logged write took %d syncs of the store, want 0", got)
	}
}

// TestAWriteWaitingOnAGroupSyncIsNotAcknowledgedAfterAFailedSync (BUG-0809 round 2, S2): a write
// committed unsynced before another commit's sync failed waits on ackSyncGroup.syncAfter; the
// Sync it then runs must fail, because it cannot show the first write is on disk.
// [BREAK: Sync of a poisoned store answers nil]
func TestAWriteWaitingOnAGroupSyncIsNotAcknowledgedAfterAFailedSync(t *testing.T) {
	fault := kvfault.WrapSyncFault(kvfault.New())
	waiting := fault.NewBatch()
	if err := waiting.Set([]byte("waiting"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := waiting.Commit(false); err != nil {
		t.Fatal(err)
	}
	failing := fault.NewBatch()
	if err := failing.Set([]byte("failing"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	fault.FailNextSync()
	if err := failing.Commit(true); !errors.Is(err, storage.ErrCommittedNotSynced) {
		t.Fatalf("the failing commit answered %v", err)
	}
	var group ackSyncGroup
	if err := group.syncAfter(fault); err == nil {
		t.Fatal("a write committed before a failed sync was acknowledged by a later Sync")
	}
}
