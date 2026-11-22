package wire

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

// batch sends one RING_BATCH frame and returns the frame that answered it.
func (c *u66Conn) batch(req RingBatchRequest) *transport.Frame {
	c.t.Helper()
	return c.batchWithID(u66ID(), req)
}

func (c *u66Conn) batchWithID(id [16]byte, req RingBatchRequest) *transport.Frame {
	c.t.Helper()
	payload, err := EncodeRingBatch(req)
	if err != nil {
		c.t.Fatal(err)
	}
	return c.send(transport.OpRingBatch, id, payload)
}

// ringBatchReply decodes a RING_REPLY that must be a batch reply of count answers.
func ringBatchReply(t *testing.T, frame *transport.Frame, count int) RingReply {
	t.Helper()
	reply := u66Reply(t, frame)
	if len(reply.Answers) != count {
		t.Fatalf("batch reply has %d answers, want %d: %+v", len(reply.Answers), count, reply)
	}
	return reply
}

func ringBatchOpsOf(requests ...RingKeyedRequest) []cluster.Op {
	ops := make([]cluster.Op, len(requests))
	for i, request := range requests {
		ops[i] = request.Op
	}
	return ops
}

// ringBatchDistinctPairIDs returns n document ids whose slots differ, so n writes are on n pairs.
func ringBatchDistinctPairIDs(t *testing.T, env *u66Env, n int) []string {
	t.Helper()
	seen := map[uint16]bool{}
	var ids []string
	for i := 0; len(ids) < n && i < 10_000; i++ {
		id := fmt.Sprintf("doc-%d", i)
		slot := env.ref.slot(u66EncID(t, id))
		if !seen[slot] {
			seen[slot] = true
			ids = append(ids, id)
		}
	}
	if len(ids) != n {
		t.Fatalf("found %d ids on distinct slots, want %d", len(ids), n)
	}
	return ids
}

func TestARingBatchAppliesEveryOpAndAnswersInOrder(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	ids := ringBatchDistinctPairIDs(t, env, 6)
	var requests []RingKeyedRequest
	for _, id := range ids {
		requests = append(requests, env.doc(t, cluster.OpUpsert, id, "name-"+id))
	}
	reply := ringBatchReply(t, c.batch(RingBatchRequest{Prologue: RingPrologue{}, Ops: ringBatchOpsOf(requests...)}), len(ids))
	if reply.Outcome != "ok" {
		t.Fatalf("outcome %q, want ok", reply.Outcome)
	}
	for i, answer := range reply.Answers {
		if answer.Reply == nil || answer.Reply.WriteOutcome != "applied" {
			t.Fatalf("answer %d = %+v, want an applied reply", i, answer)
		}
		if got := string(answer.Reply.ID.Value[4 : len(answer.Reply.ID.Value)-1]); got != ids[i] {
			t.Fatalf("answer %d is for document %q, want %q: the answers are not in request order", i, got, ids[i])
		}
	}
	for _, id := range ids {
		if got := u66Reply(t, c.keyed(env.read(t, RingClassGet, id, "", 0))); got.Found == nil || !*got.Found {
			t.Fatalf("document %s was not written", id)
		}
	}
}

func TestARingBatchKeepsOrderWithinOnePair(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	const n = 12
	var requests []RingKeyedRequest
	for i := range n {
		arg := u66Arg(t, func() ([]byte, error) { return encodeElementArg(u66Item(t, fmt.Sprint("item", i))) })
		requests = append(requests, env.verb(t, "b1", "Queue", cluster.VerbPush, arg))
	}
	for range n {
		requests = append(requests, env.verb(t, "b1", "Queue", cluster.VerbPop, nil))
	}
	reply := ringBatchReply(t, c.batch(RingBatchRequest{Ops: ringBatchOpsOf(requests...)}), 2*n)
	for i := range n {
		popped := reply.Answers[n+i].Reply
		if popped == nil || popped.Found == nil || !*popped.Found || len(popped.Elements) != 1 || !bytes.Equal(popped.Elements[0], u66Item(t, fmt.Sprint("item", i))) {
			t.Fatalf("pop %d = %+v, want item%d: operations on one pair did not apply in request order", i, reply.Answers[n+i], i)
		}
	}
}

// TestARingBatchSharesOneGroupCommitAcrossPairs: [GUARD a batch is not a regression against the
// pipelined single-request fan-out it replaces]. Eight writes on eight pairs overlap, so their
// appends are covered by one or two flushes, not eight.
func TestARingBatchSharesOneGroupCommitAcrossPairs(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	ids := ringBatchDistinctPairIDs(t, env, 8)
	var requests []RingKeyedRequest
	for _, id := range ids {
		requests = append(requests, env.doc(t, cluster.OpUpsert, id, "x"))
	}
	var flushes atomic.Int64
	env.node.wal.SetFsyncHook(func() {
		flushes.Add(1)
		time.Sleep(200 * time.Millisecond)
	})
	t.Cleanup(func() { env.node.wal.SetFsyncHook(nil) })
	reply := ringBatchReply(t, c.batch(RingBatchRequest{Ops: ringBatchOpsOf(requests...)}), len(ids))
	if reply.Outcome != "ok" {
		t.Fatalf("outcome %q: %+v", reply.Outcome, reply.Answers)
	}
	t.Logf("8 writes on 8 pairs took %d flushes", flushes.Load())
	// Serial groups take one flush each, 8. Overlapping groups share flushes: 2 on an idle host, a
	// few more when a loaded host starts a worker late. Anything near 8 is a serial loop.
	if got := flushes.Load(); got > 3 {
		t.Fatalf("8 writes on 8 pairs took %d flushes, want at most 3 (a serial loop takes 8)", got)
	}
}

func TestARingBatchSpentDeadlineAnswersNotAppliedForTheRest(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	var requests []RingKeyedRequest
	for i := range 4 {
		arg := u66Arg(t, func() ([]byte, error) { return encodeElementArg(u66Item(t, fmt.Sprint("late", i))) })
		requests = append(requests, env.verb(t, "b1", "Queue", cluster.VerbPush, arg))
	}
	env.node.wal.SetFsyncHook(func() { time.Sleep(150 * time.Millisecond) })
	t.Cleanup(func() { env.node.wal.SetFsyncHook(nil) })
	reply := ringBatchReply(t, c.batch(RingBatchRequest{Prologue: RingPrologue{DeadlineMs: 60}, Ops: ringBatchOpsOf(requests...)}), 4)
	env.node.wal.SetFsyncHook(nil)
	if reply.Outcome != "partial" {
		t.Fatalf("outcome %q, want partial", reply.Outcome)
	}
	if first := reply.Answers[0]; first.Reply == nil || first.Reply.WriteOutcome != "applied" {
		t.Fatalf("the first operation started inside the deadline and must be applied: %+v", first)
	}
	for i := 1; i < 4; i++ {
		refusal := reply.Answers[i].Error
		if refusal == nil || refusal.Code != cluster.CodeUnavailable {
			t.Fatalf("operation %d = %+v, want the deadline refusal", i, reply.Answers[i])
		}
		for _, word := range []string{"not attempted", "not applied", "same request id"} {
			if !bytes.Contains([]byte(refusal.Message), []byte(word)) {
				t.Fatalf("operation %d message %q does not say %q", i, refusal.Message, word)
			}
		}
	}
	if count := u66Reply(t, c.keyed(env.read(t, RingClassStoreCount, "b1", "Queue", 0))); count.Count == nil || *count.Count != 1 {
		t.Fatalf("the queue holds %v elements, want 1: an operation answered not applied must not have been written", count.Count)
	}
}

// ringBatchSlowAllow admits everything, slowly: each ask takes 15 ms, as a lookup that has to
// ask a remote database for a bucket id would.
type ringBatchSlowAllow struct{}

func (ringBatchSlowAllow) Authorize(context.Context, RingAccess) error {
	time.Sleep(15 * time.Millisecond)
	return nil
}

// TestARingBatchDeadlineBoundsAdmissionToo: [GUARD the frame's deadline is enforced on the work
// that precedes the writes]. Six operations whose admission takes 30 ms each under a 40 ms
// deadline: admission stops once the deadline is spent, the rest answer not attempted, and the
// handler returns long before six admissions would.
func TestARingBatchDeadlineBoundsAdmissionToo(t *testing.T) {
	node, _ := u63Fresh(t)
	handler, err := NewRingHandler(node, u66Next, ringBatchSlowAllow{})
	if err != nil {
		t.Fatal(err)
	}
	env := &u66Env{node: node}
	env.ref, err = node.bucket(context.Background(), u30iDB, "Box")
	if err != nil {
		t.Fatal(err)
	}
	var requests []RingKeyedRequest
	for i := range 6 {
		requests = append(requests, env.doc(t, cluster.OpUpsert, fmt.Sprint("slow-", i), "x"))
	}
	payload, err := EncodeRingBatch(RingBatchRequest{Prologue: RingPrologue{DeadlineMs: 40}, Ops: ringBatchOpsOf(requests...)})
	if err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	frame, err := handler(context.Background(), transport.NewFrameWithID(transport.OpRingBatch, [16]byte{7}, payload))
	took := time.Since(began)
	if err != nil {
		t.Fatal(err)
	}
	reply := ringBatchReply(t, frame, 6)
	for i, answer := range reply.Answers {
		if answer.Error == nil || !strings.Contains(answer.Error.Message, "not attempted") {
			t.Fatalf("operation %d = %+v, want not attempted", i, answer)
		}
	}
	if took > 140*time.Millisecond {
		t.Fatalf("the handler took %s: admission was not bounded by the 40 ms deadline (six admissions take 180 ms)", took)
	}
	if _, err := node.Get(context.Background(), u30iDB, "Box", "slow-0"); err == nil {
		t.Fatal("an operation past the deadline was written")
	}
}

// TestARingBatchOf255IsSentAndAnswered: [GUARD the largest frame works end to end]. 255 writes on
// 255 documents are encoded, sent, applied and answered with 255 answers in request order.
func TestARingBatchOf255IsSentAndAnswered(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	var requests []RingKeyedRequest
	for i := range 255 {
		requests = append(requests, env.doc(t, cluster.OpUpsert, fmt.Sprint("big-", i), "x"))
	}
	reply := ringBatchReply(t, c.batch(RingBatchRequest{Ops: ringBatchOpsOf(requests...)}), 255)
	if reply.Outcome != "ok" {
		t.Fatalf("outcome %q", reply.Outcome)
	}
	for i, answer := range reply.Answers {
		if answer.Reply == nil || answer.Reply.WriteOutcome != "applied" || string(answer.Reply.ID.Value[4:len(answer.Reply.ID.Value)-1]) != fmt.Sprint("big-", i) {
			t.Fatalf("answer %d = %+v", i, answer)
		}
	}
}
