package wire

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// ringOrderPush is a push of item label into field of document id.
func ringOrderPush(t *testing.T, env *u66Env, id, field, label string) RingKeyedRequest {
	t.Helper()
	arg := u66Arg(t, func() ([]byte, error) { return encodeElementArg(u66Item(t, label)) })
	return env.verb(t, id, field, cluster.VerbPush, arg)
}

// ringOrderStore lists field of document id and fails unless it holds exactly labels, in order.
func ringOrderStore(t *testing.T, env *u66Env, c *u66Conn, id, field string, labels ...string) {
	t.Helper()
	listed := u66Reply(t, c.keyed(env.read(t, RingClassStoreList, id, field, 50)))
	if len(listed.Elements) != len(labels) {
		t.Fatalf("%s.%s holds %d elements, want %v", id, field, len(listed.Elements), labels)
	}
	for i, label := range labels {
		if !bytes.Equal(listed.Elements[i], u66Item(t, label)) {
			t.Fatalf("%s.%s element %d is not %s: the store's order is wrong", id, field, i, label)
		}
	}
}

// ringOrderRefusedBehind fails unless answer is the retryable refusal of an operation held back
// because an earlier operation on its store was refused.
func ringOrderRefusedBehind(t *testing.T, answer RingAnswer, index, earlier int) {
	t.Helper()
	if answer.Error == nil {
		t.Fatalf("operation %d = %+v, want it refused: operation %d on the same store was refused before it, so applying it puts the store out of order", index, answer, earlier)
	}
	if answer.Error.Code != cluster.CodeUnavailable {
		t.Fatalf("operation %d refused %s, want UNAVAILABLE (retryable): %s", index, answer.Error.Code, answer.Error.Message)
	}
	for _, phrase := range []string{"operation " + strconv.Itoa(earlier), "same store", "not applied", "in order"} {
		if !strings.Contains(answer.Error.Message, phrase) {
			t.Fatalf("operation %d refusal %q does not say %q", index, answer.Error.Message, phrase)
		}
	}
}

// TestARingBatchHoldsBackLaterOpsOnAStoreAfterARefusalAtExecution: [GUARD BUG-0924] push B is
// refused when it runs (its declaration version is stale, as after a lease blip or an alter);
// push C on the same store must then be refused too and not applied, or a resend of B puts it
// behind C. Ops on another store of the same document and on another document apply. Resending
// the frame under the same request id with B fixed applies B then C, in order.
func TestARingBatchHoldsBackLaterOpsOnAStoreAfterARefusalAtExecution(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	pushA := ringOrderPush(t, env, "b1", "Queue", "A")
	pushB := ringOrderPush(t, env, "b1", "Queue", "B")
	staleB := pushB
	staleB.Op.DeclVersion = env.ref.version + 1
	pushC := ringOrderPush(t, env, "b1", "Queue", "C")
	stack := ringOrderPush(t, env, "b1", "Stack", "S")
	other := env.doc(t, cluster.OpUpsert, "order-other", "other")
	id := [16]byte{0xB0, 0x92, 4}
	reply := ringBatchReply(t, c.batchWithID(id, RingBatchRequest{Ops: ringBatchOpsOf(pushA, staleB, pushC, stack, other)}), 5)
	if reply.Answers[0].Reply == nil {
		t.Fatalf("push A = %+v, want applied", reply.Answers[0])
	}
	if refused := reply.Answers[1].Error; refused == nil || refused.Code != cluster.CodeSchemaNotCurrent {
		t.Fatalf("push B = %+v, want SCHEMA_NOT_CURRENT", reply.Answers[1])
	}
	ringOrderRefusedBehind(t, reply.Answers[2], 2, 1)
	if reply.Answers[3].Reply == nil || reply.Answers[4].Reply == nil {
		t.Fatalf("the ops on another store and another document = %+v, %+v; want both applied", reply.Answers[3], reply.Answers[4])
	}
	ringOrderStore(t, env, c, "b1", "Queue", "A")
	ringOrderStore(t, env, c, "b1", "Stack", "S")

	retry := ringBatchReply(t, c.batchWithID(id, RingBatchRequest{Ops: ringBatchOpsOf(pushA, pushB, pushC, stack, other)}), 5)
	if retry.Outcome != "ok" {
		t.Fatalf("the resend outcome %q: %+v", retry.Outcome, retry.Answers)
	}
	ringOrderStore(t, env, c, "b1", "Queue", "A", "B", "C")
	ringOrderStore(t, env, c, "b1", "Stack", "S")
}

// TestARingBatchHoldsBackLaterOpsOnAStoreAfterARefusalAtAdmission: the first lookup of the
// bucket fails UNAVAILABLE (no database could answer), so push A is refused at admission while
// the lookups for B and C succeed. B and C are on A's store and must be refused; push S on
// another store of the document applies.
func TestARingBatchHoldsBackLaterOpsOnAStoreAfterARefusalAtAdmission(t *testing.T) {
	node, _ := u63Fresh(t)
	var lookups atomic.Int32
	failFirst := func(s *ringServer) {
		s.bucketLookup = func(ctx context.Context, bucketID uint64, admit func(database, bucket string) error) (bucketRef, error) {
			if lookups.Add(1) == 1 {
				return bucketRef{}, &cluster.Error{Code: cluster.CodeUnavailable, Message: "no database could answer"}
			}
			return node.bucketByID(ctx, bucketID, admit)
		}
	}
	env := u66ServeWith(t, node, failFirst)
	c := env.login(t)
	ops := ringBatchOpsOf(ringOrderPush(t, env, "b1", "Queue", "A"), ringOrderPush(t, env, "b1", "Queue", "B"),
		ringOrderPush(t, env, "b1", "Stack", "S"), ringOrderPush(t, env, "b1", "Queue", "C"))
	reply := ringBatchReply(t, c.batch(RingBatchRequest{Ops: ops}), 4)
	if refused := reply.Answers[0].Error; refused == nil || refused.Code != cluster.CodeUnavailable {
		t.Fatalf("push A = %+v, want the lookup's UNAVAILABLE", reply.Answers[0])
	}
	ringOrderRefusedBehind(t, reply.Answers[1], 1, 0)
	ringOrderRefusedBehind(t, reply.Answers[3], 3, 0)
	if reply.Answers[2].Reply == nil {
		t.Fatalf("push S on another store = %+v, want applied", reply.Answers[2])
	}
	ringOrderStore(t, env, c, "b1", "Queue")
	ringOrderStore(t, env, c, "b1", "Stack", "S")
}

// TestARingBatchHoldsBackLaterOpsOnAStoreAfterAnUnvouchedOutcome: [GUARD BUG-0924] a push the
// owner applied but cannot vouch for answers write_outcome "unknown" (machine.Answer rows E and
// E': the lease lapsed at answer time, or the node stopped being the owner before its successor
// acknowledged). It is not a confirmed apply: when the node is no longer the owner, the successor
// may not hold it, and a later push applied there would land in front of its resend. Push C on the
// same store is therefore held back like one behind a refusal, and the resend applies B (or
// answers it deduped) before C. The node here is a one-member cluster, which always vouches, so
// the test runs B for real and reports it unknown.
func TestARingBatchHoldsBackLaterOpsOnAStoreAfterAnUnvouchedOutcome(t *testing.T) {
	node, _ := u63Fresh(t)
	var unvouched atomic.Bool
	unvouched.Store(true)
	reportB := func(s *ringServer) {
		s.batchExecute = func(ctx context.Context, ref bucketRef, requestID [16]byte, req RingKeyedRequest, pos ringBatchPos) (RingReply, error) {
			reply, err := s.execute(ctx, ref, requestID, req, pos)
			if err == nil && pos.index == 1 && unvouched.Load() {
				reply.WriteOutcome = outcomeUnknown
			}
			return reply, err
		}
	}
	env := u66ServeWith(t, node, reportB)
	c := env.login(t)
	ops := ringBatchOpsOf(ringOrderPush(t, env, "b1", "Queue", "A"), ringOrderPush(t, env, "b1", "Queue", "B"),
		ringOrderPush(t, env, "b1", "Queue", "C"), ringOrderPush(t, env, "b1", "Stack", "S"))
	id := [16]byte{0xB0, 0x92, 5}
	reply := ringBatchReply(t, c.batchWithID(id, RingBatchRequest{Ops: ops}), 4)
	if b := reply.Answers[1].Reply; b == nil || b.WriteOutcome != outcomeUnknown {
		t.Fatalf("push B = %+v, want the unvouched outcome", reply.Answers[1])
	}
	ringOrderRefusedBehind(t, reply.Answers[2], 2, 1)
	if reply.Answers[3].Reply == nil {
		t.Fatalf("push S on another store = %+v, want applied", reply.Answers[3])
	}
	ringOrderStore(t, env, c, "b1", "Queue", "A", "B")

	unvouched.Store(false)
	retry := ringBatchReply(t, c.batchWithID(id, RingBatchRequest{Ops: ops}), 4)
	if retry.Outcome != "ok" || !retry.Answers[1].Reply.Deduped {
		t.Fatalf("the resend: %+v, want every op answered and B deduped", retry.Answers)
	}
	ringOrderStore(t, env, c, "b1", "Queue", "A", "B", "C")
}
