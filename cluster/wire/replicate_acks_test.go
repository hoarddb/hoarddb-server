package wire

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// ackBatchLink is a Link that batches acknowledgements and records every AckBatch call. A test
// holds the link to keep the ack lane inside one call while it queues more acks behind it.
type ackBatchLink struct {
	*u32Link
	mu       sync.Mutex
	calls    [][]WriteAck
	gate     chan struct{}
	failNext error
	events   *queueEvents
}

func newAckBatchLink() *ackBatchLink { return &ackBatchLink{u32Link: newU32Link()} }

// hold makes every AckBatch call wait until release or until its context ends.
func (l *ackBatchLink) hold() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.gate = make(chan struct{})
}

func (l *ackBatchLink) release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.gate != nil {
		select {
		case <-l.gate:
		default:
			close(l.gate)
		}
	}
}

func (l *ackBatchLink) setEvents(events *queueEvents) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = events
}

func (l *ackBatchLink) AckBatch(ctx context.Context, _ string, acks []WriteAck) error {
	l.mu.Lock()
	l.calls = append(l.calls, slices.Clone(acks))
	gate, failure, events := l.gate, l.failNext, l.events
	l.failNext = nil
	l.mu.Unlock()
	if events != nil {
		for _, ack := range acks {
			events.add(queueEventName("ack", ack.Slot, ack.Pos.Seq))
		}
	}
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return failure
}

func (l *ackBatchLink) recorded() [][]WriteAck {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.calls)
}

func (l *ackBatchLink) entryCount() int {
	total := 0
	for _, call := range l.recorded() {
		total += len(call)
	}
	return total
}

// waitCalls waits until the link has seen n AckBatch calls.
func waitCalls(t *testing.T, link *ackBatchLink, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for len(link.recorded()) < n {
		if time.Now().After(deadline) {
			t.Fatalf("the link saw %d AckBatch calls, want %d", len(link.recorded()), n)
		}
		time.Sleep(time.Millisecond)
	}
}

// waitAckIdle waits until the ack lane has gone idle: no lane is left for any destination.
func waitAckIdle(t *testing.T, rep *Replicator) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		rep.ackMu.Lock()
		idle := len(rep.ackLanes) == 0
		rep.ackMu.Unlock()
		if idle {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the ack lane never went idle")
		}
		time.Sleep(time.Millisecond)
	}
}

func ackFor(slot uint16, seq uint64, outcome AckOutcome) WriteAck {
	return WriteAck{From: "copy", Slot: slot, DB: u32DB, Pos: u32Pos(u32Epoch, seq), Outcome: outcome}
}

// newAckRig is a replicator over a batching link whose Run is not started: the tests drive the
// ack lane through sendAck, with a context the cleanup ends.
func newAckRig(t *testing.T) (*u32Node, *ackBatchLink, context.Context) {
	t.Helper()
	link := newAckBatchLink()
	node := newU32Node(t, "copy", func(cfg *ReplicatorConfig) { cfg.Link = link })
	ctx, stop := context.WithCancel(context.Background())
	t.Cleanup(func() {
		link.release()
		stop()
		node.rep.ackWG.Wait()
	})
	return node, link, ctx
}

func nextAckError(t *testing.T, node *u32Node) error {
	t.Helper()
	select {
	case err := <-node.errs:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("no error was reported")
		return nil
	}
}

// TestAcksOfSeveralPairsLeaveInOneCall (BUG-0745): while one call is out, the acks of several
// pairs queue behind it and leave together in the next call.
// [GUARD: call AckBatch once per ack]
func TestAcksOfSeveralPairsLeaveInOneCall(t *testing.T) {
	node, link, ctx := newAckRig(t)
	link.hold()
	node.rep.sendAck(ctx, "owner", ackFor(1, 1, AckApplied))
	waitCalls(t, link, 1)
	for slot := uint16(2); slot <= 4; slot++ {
		node.rep.sendAck(ctx, "owner", ackFor(slot, 1, AckApplied))
	}
	link.release()
	waitCalls(t, link, 2)
	calls := link.recorded()
	if len(calls) != 2 || len(calls[0]) != 1 || len(calls[1]) != 3 {
		t.Fatalf("calls %v, want one of 1 ack then one of 3", calls)
	}
	for i, ack := range calls[1] {
		if ack.Slot != uint16(i+2) {
			t.Fatalf("the batch holds slots %v in order, want 2, 3, 4", calls[1])
		}
	}
}

// TestAnAppliedAckIsQueuedOnlyAfterTheApply (BUG-0745, invariant I3): with a batching link the
// worker still hands an ack to the lane only once the record is durable and applied.
// [GUARD: queue the ack before applyQueued returns]
func TestAnAppliedAckIsQueuedOnlyAfterTheApply(t *testing.T) {
	link := newAckBatchLink()
	c := newQueueCopy(t, func(cfg *ReplicatorConfig) { cfg.Link = link })
	link.setEvents(c.events)
	release := c.log.gate(5, 1)
	if _, queued := queueSend(t, c, 5, 1); !queued {
		t.Fatal("the frame was not queued")
	}
	queueWaitEntered(t, c)
	time.Sleep(100 * time.Millisecond)
	if n := len(link.recorded()); n != 0 {
		t.Fatalf("%d AckBatch calls before the record was durable", n)
	}
	close(release)
	waitCalls(t, link, 1)
	calls := link.recorded()
	if len(calls[0]) != 1 {
		t.Fatalf("batch %v, want one ack", calls[0])
	}
	if ack := calls[0][0]; ack.Outcome != AckApplied || ack.Pos != u32Pos(u32Epoch, 1) || ack.From != "copy" || ack.Slot != 5 {
		t.Fatalf("ack %+v, want applied at 7:1 from copy for slot 5", ack)
	}
	if d, a := c.events.index(queueEventName("durable", 5, 1)), c.events.index(queueEventName("ack", 5, 1)); d < 0 || a < d {
		t.Fatalf("the ack (event %d) was queued before the record was durable (event %d)", a, d)
	}
}

// TestCoalescingKeepsTheHighestAppliedAckOfAPair (invariant I6): acks of one pair that wait
// together leave as one entry at the highest position, and a lower one never lowers it.
// [GUARD: keep the first ack of a pair]
func TestCoalescingKeepsTheHighestAppliedAckOfAPair(t *testing.T) {
	node, link, ctx := newAckRig(t)
	link.hold()
	node.rep.sendAck(ctx, "owner", ackFor(9, 1, AckApplied))
	waitCalls(t, link, 1)
	node.rep.sendAck(ctx, "owner", ackFor(1, 3, AckApplied))
	node.rep.sendAck(ctx, "owner", ackFor(2, 2, AckApplied))
	node.rep.sendAck(ctx, "owner", ackFor(1, 5, AckApplied))
	node.rep.sendAck(ctx, "owner", ackFor(1, 4, AckApplied))
	link.release()
	waitCalls(t, link, 2)
	batch := link.recorded()[1]
	if len(batch) != 2 || batch[0] != ackFor(1, 5, AckApplied) || batch[1] != ackFor(2, 2, AckApplied) {
		t.Fatalf("batch %v, want pair 1 at 5 then pair 2 at 2", batch)
	}
}

// TestCoalescingNeverMergesAcrossANotAppliedAck (invariant I6): an applied ack after a
// not-applied answer for the same pair is a new entry, never folded into one before it.
// [GUARD: merge applied acks across a not-applied one]
func TestCoalescingNeverMergesAcrossANotAppliedAck(t *testing.T) {
	node, link, ctx := newAckRig(t)
	link.hold()
	node.rep.sendAck(ctx, "owner", ackFor(9, 1, AckApplied))
	waitCalls(t, link, 1)
	node.rep.sendAck(ctx, "owner", ackFor(1, 5, AckApplied))
	node.rep.sendAck(ctx, "owner", ackFor(1, 5, AckNotApplied))
	node.rep.sendAck(ctx, "owner", ackFor(1, 7, AckApplied))
	link.release()
	waitCalls(t, link, 2)
	batch := link.recorded()[1]
	want := []WriteAck{ackFor(1, 5, AckApplied), ackFor(1, 5, AckNotApplied), ackFor(1, 7, AckApplied)}
	if !slices.Equal(batch, want) {
		t.Fatalf("batch %v, want %v in this order", batch, want)
	}
}

// TestTheAckLaneAddsNoGoroutinePerPair: the acks of a thousand pairs queue behind one lane
// goroutine, leave in chunks of at most maxBatchEntries, and the goroutine ends when the queue is empty.
// [GUARD: start a goroutine for each pair with an ack]
func TestTheAckLaneAddsNoGoroutinePerPair(t *testing.T) {
	node, link, ctx := newAckRig(t)
	link.hold()
	node.rep.sendAck(ctx, "owner", ackFor(0, 1, AckApplied))
	waitCalls(t, link, 1)
	base := runtime.NumGoroutine()
	const pairs = 1000
	for slot := uint16(1); slot <= pairs; slot++ {
		node.rep.sendAck(ctx, "owner", ackFor(slot, 1, AckApplied))
	}
	if grown := runtime.NumGoroutine() - base; grown > 2 {
		t.Fatalf("%d more goroutines with the acks of %d pairs queued, want at most 2", grown, pairs)
	}
	link.release()
	deadline := time.Now().Add(10 * time.Second)
	for link.entryCount() < pairs+1 {
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d acks were sent", link.entryCount(), pairs+1)
		}
		time.Sleep(time.Millisecond)
	}
	for _, call := range link.recorded() {
		if len(call) > maxBatchEntries {
			t.Fatalf("a call carried %d acks, want at most %d", len(call), maxBatchEntries)
		}
	}
	waitAckIdle(t, node.rep)
	node.rep.ackWG.Wait()
	for runtime.NumGoroutine() > base-1 {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines after the queue drained, want at most %d", runtime.NumGoroutine(), base-1)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestAFailedAckBatchIsReportedAndDropped (invariant I10): a failed call is reported once and
// its acks are not sent again; the owner's ack deadline does the recovery, and the next ack goes out.
// [GUARD: keep the failed acks and retry them]
func TestAFailedAckBatchIsReportedAndDropped(t *testing.T) {
	node, link, ctx := newAckRig(t)
	boom := errors.New("the owner is gone")
	link.mu.Lock()
	link.failNext = boom
	link.mu.Unlock()
	node.rep.sendAck(ctx, "owner", ackFor(1, 1, AckApplied))
	err := nextAckError(t, node)
	if !errors.Is(err, boom) || !strings.Contains(err.Error(), "owner") {
		t.Fatalf("reported %v, want the failure naming the owner", err)
	}
	waitAckIdle(t, node.rep)
	node.rep.sendAck(ctx, "owner", ackFor(2, 1, AckApplied))
	waitCalls(t, link, 2)
	waitAckIdle(t, node.rep)
	calls := link.recorded()
	if len(calls) != 2 || len(calls[1]) != 1 || calls[1][0].Slot != 2 {
		t.Fatalf("calls %v, want the failed ack dropped and only slot 2 in the second call", calls)
	}
	select {
	case again := <-node.errs:
		t.Fatalf("a second error was reported: %v", again)
	default:
	}
}

// TestCloseWaitsForTheAckLane: an ack call that is blocked ends when Run's context ends, Close
// returns only after it, and an ack handed in afterwards is reported rather than lost.
// [GUARD: give the lane a context that outlives Run]
func TestCloseWaitsForTheAckLane(t *testing.T) {
	link := newAckBatchLink()
	node := newU32Node(t, "copy", func(cfg *ReplicatorConfig) { cfg.Link = link })
	link.hold()
	t.Cleanup(link.release)
	running := make(chan struct{})
	go func() { defer close(running); node.rep.Run(context.Background()) }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		node.rep.ackMu.Lock()
		started := node.rep.runCtx != nil
		node.rep.ackMu.Unlock()
		if started {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Run never started")
		}
		time.Sleep(time.Millisecond)
	}
	node.rep.AckAsync("owner", ackFor(1, 1, AckApplied))
	waitCalls(t, link, 1)
	closed := make(chan struct{})
	go func() { defer close(closed); node.rep.Close() }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return while an ack call was blocked")
	}
	<-running
	node.rep.AckAsync("owner", ackFor(2, 1, AckApplied))
	if err := nextAckError(t, node); !strings.Contains(err.Error(), "owner") {
		t.Fatalf("reported %v, want an ack lost after Run ended to name the owner", err)
	}
	if n := len(link.recorded()); n != 1 {
		t.Fatalf("%d AckBatch calls, want only the first", n)
	}
}

// TestWithoutABatchLinkAcksGoOneByOne: a link that cannot batch is sent one Ack per record, as before.
// [GUARD: always queue the ack]
func TestWithoutABatchLinkAcksGoOneByOne(t *testing.T) {
	c := newQueueCopy(t, nil)
	for seq := uint64(1); seq <= 3; seq++ {
		if _, queued := queueSend(t, c, 5, seq); !queued {
			t.Fatalf("5/%d was not queued", seq)
		}
	}
	for seq := uint64(1); seq <= 3; seq++ {
		if ack := queueWaitAck(t, c); ack.Outcome != AckApplied || ack.Pos != u32Pos(u32Epoch, seq) {
			t.Fatalf("ack %+v, want applied at position %d, one Ack per record", ack, seq)
		}
	}
}

// TestAckAsyncBeforeRunIsReportedNotLost: an ack handed in before Run has started has no
// context to run under, so it is reported and never sent.
// [GUARD: drop the ack silently]
func TestAckAsyncBeforeRunIsReportedNotLost(t *testing.T) {
	link := newAckBatchLink()
	node := newU32Node(t, "copy", func(cfg *ReplicatorConfig) { cfg.Link = link })
	node.rep.AckAsync("owner", ackFor(1, 1, AckApplied))
	if err := nextAckError(t, node); !strings.Contains(err.Error(), "owner") {
		t.Fatalf("reported %v, want the lost ack to name the owner", err)
	}
	if n := len(link.recorded()); n != 0 {
		t.Fatalf("%d AckBatch calls before Run", n)
	}
}
