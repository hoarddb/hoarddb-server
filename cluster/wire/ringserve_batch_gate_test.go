package wire

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// ringBatchGateHarness serves a small-budget node and hands back its server, a gate whose
// reservation is already held (so any pop waits behind it) and a pop on a queue holding one element.
func ringBatchGateHarness(t *testing.T) (*ringServer, *u66Env, *ringBatchGate, RingKeyedRequest) {
	t.Helper()
	var server *ringServer
	node, _ := u63Fresh(t)
	env := u66ServeWith(t, node, WithRingResultBudget(1800), func(s *ringServer) { s.elementCeiling = 1100; server = s })
	c := env.login(t)
	ringBatchPushBig(t, env, c, 1)
	gate := newRingBatchGate(server.resultBudget, server.elementCeiling)
	if got := gate.enter(context.Background(), time.Time{}); got != ringGateEntered {
		t.Fatalf("first reservation = %v, want entered", got)
	}
	return server, env, gate, env.verb(t, "b1", "Queue", cluster.VerbPop, nil)
}

func ringBatchQueueCount(t *testing.T, env *u66Env) int64 {
	t.Helper()
	count := u66Reply(t, env.login(t).keyed(env.read(t, RingClassStoreCount, "b1", "Queue", 0)))
	if count.Count == nil {
		t.Fatal("count reply has no count")
	}
	return *count.Count
}

// ringBatchAnswerWithin runs the operation and fails when it is still parked after the limit.
func ringBatchAnswerWithin(t *testing.T, gate *ringBatchGate, run func() RingAnswer) RingAnswer {
	t.Helper()
	done := make(chan RingAnswer, 1)
	go func() { done <- run() }()
	select {
	case answer := <-done:
		return answer
	case <-time.After(3 * time.Second):
		gate.leave(0) // frees the parked goroutine so the test does not leak it
		<-done
		t.Fatal("the pop is still parked in the gate: nothing ends its wait")
		return RingAnswer{}
	}
}

// TestARingBatchPopThatWaitsPastTheDeadlineIsNotAttempted: [GUARD a pop that queued in the gate is
// held to the frame's deadline, not the budget it had before it queued]. The pop waits behind a
// reservation that is never released; the 150 ms deadline ends the wait, the answer says it was not
// attempted and the element is still in the queue.
func TestARingBatchPopThatWaitsPastTheDeadlineIsNotAttempted(t *testing.T) {
	server, env, gate, pop := ringBatchGateHarness(t)
	frame := ringBatchFrame{requestID: [16]byte{7}, size: 1, deadlineMs: 150, started: time.Now(), gate: gate}
	op := batchOp{index: 0, keyed: pop, ref: env.ref}
	answer := ringBatchAnswerWithin(t, gate, func() RingAnswer { return server.runBatchOp(context.Background(), frame, op) })
	if answer.Error == nil || answer.Error.Code != cluster.CodeUnavailable || !strings.Contains(answer.Error.Message, "not attempted") {
		t.Fatalf("answer = %+v, want a not-attempted refusal", answer)
	}
	if got := ringBatchQueueCount(t, env); got != 1 {
		t.Fatalf("the queue holds %d elements, want 1: the pop was applied", got)
	}
}

// TestARingBatchCancelledContextUnparksAGateWaiter: a dropped connection cancels the context, which
// must wake a pop parked in the gate without applying it.
func TestARingBatchCancelledContextUnparksAGateWaiter(t *testing.T) {
	server, env, gate, pop := ringBatchGateHarness(t)
	frame := ringBatchFrame{requestID: [16]byte{8}, size: 1, started: time.Now(), gate: gate}
	op := batchOp{index: 0, keyed: pop, ref: env.ref}
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	defer cancel()
	answer := ringBatchAnswerWithin(t, gate, func() RingAnswer { return server.runBatchOp(ctx, frame, op) })
	if answer.Error == nil || answer.Error.Code != cluster.CodeUnavailable || !strings.Contains(answer.Error.Message, "not attempted") {
		t.Fatalf("answer = %+v, want a not-attempted refusal", answer)
	}
	if got := ringBatchQueueCount(t, env); got != 1 {
		t.Fatalf("the queue holds %d elements, want 1: the pop was applied", got)
	}
}

// TestARingBatchGateWaiterRunsOnceRoomIsReleased: [GUARD the normal hand-over survives the wake-up
// path] a waiter that gets room before any deadline still runs and returns its element.
func TestARingBatchGateWaiterRunsOnceRoomIsReleased(t *testing.T) {
	server, env, gate, pop := ringBatchGateHarness(t)
	frame := ringBatchFrame{requestID: [16]byte{9}, size: 1, deadlineMs: 5000, started: time.Now(), gate: gate}
	op := batchOp{index: 0, keyed: pop, ref: env.ref}
	time.AfterFunc(100*time.Millisecond, func() { gate.leave(0) })
	answer := ringBatchAnswerWithin(t, gate, func() RingAnswer { return server.runBatchOp(context.Background(), frame, op) })
	if answer.Reply == nil || len(answer.Reply.Elements) != 1 {
		t.Fatalf("answer = %+v, want the popped element", answer)
	}
}
