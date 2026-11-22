package wire

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
)

// flush ships whatever is queued and waits for every lane to go idle, returning the errors
// the lanes reported. It is the test's stand-in for the Run loop, which never waits.
func (n *u32Node) flush(ctx context.Context) error {
	n.rep.dispatch(ctx)
	n.rep.laneWG.Wait()
	var failures error
	for {
		select {
		case err := <-n.errs:
			failures = errors.Join(failures, err)
		default:
			return failures
		}
	}
}

// horizonLog answers ErrBelowHorizon to a read below horizon, as a log whose retention has
// pruned those sequences does.
type horizonLog struct {
	Log
	horizon atomic.Uint64
}

func (l *horizonLog) OpenCursor(from uint64) (storage.WALCursor, error) {
	if from < l.horizon.Load() {
		return storage.WALCursor{}, storage.ErrBelowHorizon
	}
	return l.Log.OpenCursor(from)
}

func (n *u32Node) failedPairs() int {
	n.rep.mu.Lock()
	defer n.rep.mu.Unlock()
	return len(n.rep.failed)
}

// TestACopyThatAnsweredNotAppliedIsPushedToAgain (pi F1): a copy that says "not applied" (it
// is behind an epoch, or its pair is not ready) must not be abandoned. The owner re-arms and
// ships the same frames again after a backoff, without a handoff or a new write.
// [GUARD: leave behind set on a not-applied ack]
func TestACopyThatAnsweredNotAppliedIsPushedToAgain(t *testing.T) {
	owner := newU32Node(t, "owner", func(cfg *ReplicatorConfig) { cfg.RetryBackoff = 10 * time.Millisecond })
	owner.copies = []string{"copy"}
	owner.ownRun(u32Epoch, 1, 3)
	owner.rep.Push(u32Slot, u32DB, u32Pos(u32Epoch, 3))
	if err := owner.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(owner.link.sent("copy")); got != 3 {
		t.Fatalf("first shipment sent %d frames, want 3", got)
	}
	owner.rep.OnWriteAck(WriteAck{From: "copy", Slot: u32Slot, DB: u32DB, Outcome: AckNotApplied})
	time.Sleep(30 * time.Millisecond)
	owner.rep.Retry()
	if err := owner.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(owner.link.sent("copy")); got != 6 {
		t.Fatalf("after the copy answered not-applied and the backoff passed, %d frames were sent in all, want the 3 sent again (6)", got)
	}
	owner.rep.OnWriteAck(WriteAck{From: "copy", Slot: u32Slot, DB: u32DB, Pos: u32Pos(u32Epoch, 3), Outcome: AckApplied})
	if owner.failedPairs() != 0 {
		t.Fatal("an applied ack left the pair in the retry set")
	}
}

// TestAnExOwnerStopsRetryingAPairItNoLongerOwns (pi F2). [GUARD: drop the ownership check]
func TestAnExOwnerStopsRetryingAPairItNoLongerOwns(t *testing.T) {
	owner := newU32Node(t, "owner", nil)
	owner.copies = []string{"copy"}
	owner.ownRun(u32Epoch, 1, 2)
	owner.rep.Push(u32Slot, u32DB, u32Pos(u32Epoch, 2))
	owner.link.mu.Lock()
	owner.link.writeErr = errors.New("timeout")
	owner.link.mu.Unlock()
	if err := owner.flush(context.Background()); err == nil {
		t.Fatal("the failed send was not reported")
	}
	if owner.failedPairs() != 1 {
		t.Fatalf("failed pairs %d, want 1", owner.failedPairs())
	}
	owner.link.mu.Lock()
	owner.link.writeErr = nil
	owner.link.mu.Unlock()
	owner.ownerID = "elsewhere"
	owner.rep.Retry()
	if err := owner.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(owner.link.sent("copy")); got != 0 {
		t.Fatalf("a node that no longer owns the pair sent %d frames", got)
	}
	if owner.failedPairs() != 0 {
		t.Fatal("the pair stayed in the retry set after ownership moved")
	}
}

// TestACopyBelowTheLogHorizonIsShippedTheFirstRetainedRecord (pi F3): a copy whose
// acknowledged record has been truncated cannot be read from; the owner ships from the
// first retained record instead, so the copy sees a gap, asks for a catch-up and the
// source answers the horizon (the copy's own re-seed path). Today that path ends at the
// seed hook, which T3 wires: SPEC-GAP S2B-6. [GUARD: keep reading from the acked cursor]
func TestACopyBelowTheLogHorizonIsShippedTheFirstRetainedRecord(t *testing.T) {
	var log *horizonLog
	owner := newU32Node(t, "owner", func(cfg *ReplicatorConfig) {
		log = &horizonLog{Log: cfg.Log}
		cfg.Log = log
	})
	owner.copies = []string{"copy"}
	owner.ownRun(u32Epoch, 1, 6)
	owner.rep.Push(u32Slot, u32DB, u32Pos(u32Epoch, 6))
	if err := owner.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	owner.rep.OnWriteAck(WriteAck{From: "copy", Slot: u32Slot, DB: u32DB, Pos: u32Pos(u32Epoch, 2), Outcome: AckApplied})
	owner.rep.rewindAfterSendFailure(pairKey{u32Slot, u32DB}, "copy") // the copy was away: back to just after its ack
	owner.link.mu.Lock()
	owner.link.frames = map[string][]WriteFrame{}
	owner.link.mu.Unlock()
	log.horizon.Store(5) // retention pruned sequences 1 to 4; the copy's next is 3
	owner.floor.Store(5)

	for range 3 {
		owner.rep.Retry()
		_ = owner.flush(context.Background())
		if len(owner.link.sent("copy")) > 0 {
			break
		}
	}
	sent := owner.link.sent("copy")
	if len(sent) == 0 || sent[0].Pos != u32Pos(u32Epoch, 5) {
		var firstPos cluster.Position
		if len(sent) > 0 {
			firstPos = sent[0].Pos
		}
		t.Fatalf("the copy below the horizon was sent %d frames starting at %s, want a shipment from the first retained position 7:5", len(sent), firstPos)
	}

	// The copy's side: it holds 7:1 and 7:2, finds the gap, refuses and asks for a catch-up,
	// and the source answers the horizon, never a short range that looks caught up.
	copyNode := newU32Node(t, "copy", nil)
	copyNode.write(u32Frame(t, u32Epoch, u32Pos(u32Epoch, 1), cluster.Position{}))
	copyNode.write(u32Frame(t, u32Epoch, u32Pos(u32Epoch, 2), cluster.Position{}))
	reply, err := copyNode.rep.OnWrite(sent[0])
	if err != nil || reply.Ack.Outcome != AckRefused || reply.CatchUpFrom == nil {
		t.Fatalf("the copy answered %+v (%v) to a frame past a gap, want a refusal that asks for a catch-up", reply, err)
	}
	chunk, err := owner.rep.ServeCatchUp(context.Background(), CatchUpRequest{Slot: u32Slot, DB: u32DB, Node: "copy", From: *reply.CatchUpFrom})
	if u32Code(err) != cluster.CodeLogBelowHorizon && (err != nil || chunk.CaughtUp || len(chunk.Records) > 0) {
		t.Fatalf("the source answered %+v (%v) below its horizon, want LOG_BELOW_HORIZON or a chunk that is not caught up and holds nothing", chunk, err)
	}
}

// TestCloseEndsALaneStuckOnACopy (pi F5): Close bounds shutdown itself; it does not wait for
// the caller's context to end. [GUARD: derive the lane context from the caller's only]
func TestCloseEndsALaneStuckOnACopy(t *testing.T) {
	var link *stepTwoBLink
	owner := newU32Node(t, "owner", func(cfg *ReplicatorConfig) {
		link = &stepTwoBLink{u32Link: newU32Link(), stuck: "stuck", release: make(chan struct{}), entered: make(chan struct{})}
		cfg.Link = link
	})
	owner.copies = []string{"stuck"}
	owner.ownRun(u32Epoch, 1, 2)
	ctx, stop := context.WithCancel(context.Background())
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(link.release) }); stop() })
	go owner.rep.Run(ctx)
	owner.rep.Push(u32Slot, u32DB, u32Pos(u32Epoch, 2))
	select {
	case <-link.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the send never started")
	}
	closed := make(chan struct{})
	go func() { owner.rep.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not return within a second while a send to a stuck copy was in flight")
	}
}

// fencedCatchUpLink refuses its first calls the way a source that has not yet applied the
// epoch change does: the uniform EPOCH_FENCED text.
type fencedCatchUpLink struct {
	*u32Link
	mu    sync.Mutex
	fails int
	calls int
}

func (l *fencedCatchUpLink) CatchUp(ctx context.Context, source string, req CatchUpRequest) (CatchUpChunk, error) {
	l.mu.Lock()
	l.calls++
	refuse := l.calls <= l.fails
	l.mu.Unlock()
	if refuse {
		return CatchUpChunk{}, &cluster.Error{Code: cluster.CodeEpochFenced, Message: "refused CATCHUP_REQ from copy at epoch 8: this node is at epoch 7"}
	}
	return l.u32Link.CatchUp(ctx, source, req)
}

// TestARefusedCatchUpIsRetriedAFewTimes (pi F15): the source answers EPOCH_FENCED while it
// has not yet applied an epoch change, and the copy must ask again, not give up until the
// next gap. [GUARD: return the first error]
func TestARefusedCatchUpIsRetriedAFewTimes(t *testing.T) {
	source := newU32Node(t, "source", nil)
	source.ownRun(u32Epoch, 1, 3)
	source.copies = []string{"copy"}
	var link *fencedCatchUpLink
	copyNode := newU32Node(t, "copy", func(cfg *ReplicatorConfig) {
		cfg.RetryBackoff = time.Millisecond
		link = &fencedCatchUpLink{u32Link: newU32Link(), fails: 2}
		cfg.Link = link
	})
	copyNode.sourceID, copyNode.ownerID = source.id, source.id
	link.source, link.owner = source.rep, source.rep
	if err := copyNode.rep.CatchUp(context.Background(), u32Slot, u32DB, u32Pos(0, 1), 1); err != nil {
		t.Fatalf("a catch-up refused twice and then served: %v", err)
	}
	if got := copyNode.applied(); got != u32Pos(u32Epoch, 3) {
		t.Fatalf("applied %s, want 7:3", got)
	}

	if link.calls != 3 {
		t.Fatalf("a catch-up refused twice made %d calls, want 3", link.calls)
	}
	var stubborn *fencedCatchUpLink
	hopeless := newU32Node(t, "copy", func(cfg *ReplicatorConfig) {
		cfg.RetryBackoff = time.Millisecond
		stubborn = &fencedCatchUpLink{u32Link: newU32Link(), fails: 1 << 20}
		cfg.Link = stubborn
	})
	hopeless.sourceID, hopeless.ownerID = source.id, source.id
	if err := hopeless.rep.CatchUp(context.Background(), u32Slot, u32DB, u32Pos(0, 1), 1); err == nil {
		t.Fatal("a catch-up refused for good reported success")
	}
	if stubborn.calls != catchUpAttempts || catchUpAttempts != 5 {
		t.Fatalf("a catch-up refused for good made %d calls, want %d (five)", stubborn.calls, catchUpAttempts)
	}
}

// notAppliedLink answers the first push of a batch the way a copy that cannot apply does:
// it reports not-applied to the replicator and counts what it was sent.
type notAppliedLink struct {
	*u32Link
	rep   *Replicator
	mu    sync.Mutex
	count int
}

func (l *notAppliedLink) Write(ctx context.Context, peer string, frame WriteFrame) error {
	l.mu.Lock()
	l.count++
	l.mu.Unlock()
	l.rep.OnWriteAck(WriteAck{From: peer, Slot: frame.Slot, DB: frame.DB, Outcome: AckNotApplied})
	return nil
}

func (l *notAppliedLink) sentCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.count
}

// TestABatchStopsAtTheFirstNotAppliedAck (pi re-review F1): a copy that cannot apply must
// cost one frame per attempt, not the whole window. [GUARD: send the rest of the batch]
func TestABatchStopsAtTheFirstNotAppliedAck(t *testing.T) {
	var link *notAppliedLink
	owner := newU32Node(t, "owner", func(cfg *ReplicatorConfig) {
		link = &notAppliedLink{u32Link: newU32Link()}
		cfg.Link = link
	})
	link.rep = owner.rep
	owner.copies = []string{"copy"}
	owner.ownRun(u32Epoch, 1, 10)
	owner.rep.Push(u32Slot, u32DB, u32Pos(u32Epoch, 10))
	if err := owner.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := link.sentCount(); got != 1 {
		t.Fatalf("a copy answering not-applied was sent %d frames in one attempt, want 1", got)
	}
	// Only what was sent is in flight (BUG-0745 review D5); counting the unsent nine would
	// over-fill the window and mislead the count of answers still owed.
	if held := owner.rep.heldFrames(u32Slot, u32DB, "copy"); held != 1 {
		t.Fatalf("%d frames are counted in flight after one was sent, want 1", held)
	}
}

// TestTheReArmBacksOffAndHasACeiling (pi re-review F1): each not-applied answer doubles the
// wait, up to a ceiling, and an applied ack starts it over. [GUARD: fixed wait]
func TestTheReArmBacksOffAndHasACeiling(t *testing.T) {
	owner := newU32Node(t, "owner", func(cfg *ReplicatorConfig) { cfg.RetryBackoff = 100 * time.Millisecond })
	owner.copies = []string{"copy"}
	key := pairKey{u32Slot, u32DB}
	wait := func() time.Duration {
		before := time.Now()
		owner.rep.OnWriteAck(WriteAck{From: "copy", Slot: u32Slot, DB: u32DB, Outcome: AckNotApplied})
		owner.rep.mu.Lock()
		defer owner.rep.mu.Unlock()
		return owner.rep.pairs[key].peers["copy"].rearmAt.Sub(before)
	}
	prev := wait()
	for i := 0; i < 4; i++ {
		next := wait()
		if next < prev*3/2 {
			t.Fatalf("attempt %d waits %s after %s, want it to grow", i+2, next, prev)
		}
		prev = next
	}
	var last time.Duration
	for i := 0; i < 20; i++ {
		last = wait()
	}
	if last > retryCeiling+time.Second {
		t.Fatalf("the wait grew to %s, past the ceiling %s", last, retryCeiling)
	}
	owner.rep.OnWriteAck(WriteAck{From: "copy", Slot: u32Slot, DB: u32DB, Pos: u32Pos(u32Epoch, 1), Outcome: AckApplied})
	if again := wait(); again > 150*time.Millisecond {
		t.Fatalf("after an applied ack the wait is %s, want the first wait again", again)
	}
}

// TestACopyThatLeavesTheCopySetIsForgotten (pi re-review F1): a pair whose failing copy was
// removed (RF change, dead member) leaves the retry set and the copy's state is dropped.
// [GUARD: never prune]
func TestACopyThatLeavesTheCopySetIsForgotten(t *testing.T) {
	owner := newU32Node(t, "owner", func(cfg *ReplicatorConfig) { cfg.RetryBackoff = time.Millisecond })
	owner.copies = []string{"gone", "stays"}
	owner.ownRun(u32Epoch, 1, 2)
	owner.rep.Push(u32Slot, u32DB, u32Pos(u32Epoch, 2))
	if err := owner.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	owner.rep.OnWriteAck(WriteAck{From: "gone", Slot: u32Slot, DB: u32DB, Outcome: AckNotApplied})
	owner.rep.OnWriteAck(WriteAck{From: "stays", Slot: u32Slot, DB: u32DB, Pos: u32Pos(u32Epoch, 2), Outcome: AckApplied})
	if owner.failedPairs() != 1 {
		t.Fatalf("failed pairs %d, want 1", owner.failedPairs())
	}
	owner.copies = []string{"stays"}
	time.Sleep(5 * time.Millisecond)
	owner.rep.Retry()
	if owner.failedPairs() != 0 {
		t.Fatal("the pair stayed in the retry set after its failing copy left")
	}
	owner.rep.mu.Lock()
	_, kept := owner.rep.pairs[pairKey{u32Slot, u32DB}].peers["gone"]
	owner.rep.mu.Unlock()
	if kept {
		t.Fatal("the departed copy's send state was kept")
	}
}
