package wire

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

const (
	copyPairA uint16 = 1
	copyPairB uint16 = 2
)

// copyEntry is the record at seq of slot as the owner would put it in a WRITE_BATCH.
func copyEntry(t *testing.T, slot uint16, seq uint64) WriteEntry {
	t.Helper()
	return WriteEntry{Slot: slot, DB: u32DB, PairRecord: queueFrame(t, slot, seq).PairRecord}
}

// newBatchCopy is newQueueCopy once Run has started: an answer sent before that is refused as
// "not running", and newQueueCopy starts Run on its own goroutine.
func newBatchCopy(t *testing.T, mutate func(*ReplicatorConfig)) *queueCopy {
	t.Helper()
	qc := newQueueCopy(t, mutate)
	deadline := time.Now().Add(10 * time.Second)
	for {
		qc.rep.ackMu.Lock()
		running := qc.rep.runCtx != nil
		qc.rep.ackMu.Unlock()
		if running {
			return qc
		}
		if time.Now().After(deadline) {
			t.Fatal("the replicator did not start running")
		}
		time.Sleep(time.Millisecond)
	}
}

func ownsEverything(uint16, uint64) bool { return true }

// holdAppends keeps every append of the copy at the log until cleanup, so a queued frame stays
// queued and a test can count what the batch queued without racing the workers.
func holdAppends(c *queueCopy) {
	c.log.mu.Lock()
	defer c.log.mu.Unlock()
	c.log.all = make(chan struct{})
}

// queuedFrames is how many frames wait in, or are being applied from, the copy's queue.
func queuedFrames(rep *Replicator) int {
	rep.copyQ.mu.Lock()
	defer rep.copyQ.mu.Unlock()
	total := 0
	for _, pq := range rep.copyQ.pairs {
		total += pq.depth
	}
	return total
}

// answeredNow takes every ack the copy has sent by now: the answers OnWriteBatch gives
// to entries it did not queue are sent before it returns.
func answeredNow(c *queueCopy) []WriteAck {
	var acks []WriteAck
	for {
		select {
		case ack := <-c.link.acks:
			acks = append(acks, ack)
		default:
			return acks
		}
	}
}

// reportedNow takes every error the copy has reported by now.
func reportedNow(c *queueCopy) []error {
	var errs []error
	for {
		select {
		case err := <-c.errs:
			errs = append(errs, err)
		default:
			return errs
		}
	}
}

// onlySlots returns the slots of acks in order.
func onlySlots(acks []WriteAck) []uint16 {
	slots := make([]uint16, 0, len(acks))
	for _, ack := range acks {
		slots = append(slots, ack.Slot)
	}
	return slots
}

// requireNotApplied fails unless acks are exactly one not-applied answer for each of slots, in order.
func requireNotApplied(t *testing.T, acks []WriteAck, slots ...uint16) {
	t.Helper()
	if len(acks) != len(slots) {
		t.Fatalf("the copy answered %d entries (slots %v), want one answer for each of slots %v", len(acks), onlySlots(acks), slots)
	}
	for index, ack := range acks {
		if ack.Slot != slots[index] || ack.Outcome != AckNotApplied || ack.Pos != (cluster.Position{}) {
			t.Errorf("answer %d is %+v, want not applied, no position, slot %d", index, ack, slots[index])
		}
	}
}

// TestAFrameStopsOnePairAtItsFirstRefusedEntryAndOthersProceed (BUG-0745, I5, I8): pair A is not
// ready, so the copy answers its first entry not applied and never offers its later entries,
// and pair B in the same frame is queued. [GUARD: drop the stopped set]
func TestAFrameStopsOnePairAtItsFirstRefusedEntryAndOthersProceed(t *testing.T) {
	var offeredA atomic.Int64
	qc := newBatchCopy(t, func(cfg *ReplicatorConfig) {
		cfg.Ready = func(slot uint16, _ uint64) bool {
			if slot == copyPairA {
				offeredA.Add(1)
				return false
			}
			return true
		}
	})
	holdAppends(qc)

	qc.rep.OnWriteBatch("owner", u32Epoch, []WriteEntry{
		copyEntry(t, copyPairA, 1), copyEntry(t, copyPairB, 1), copyEntry(t, copyPairA, 2), copyEntry(t, copyPairA, 3),
	}, ownsEverything)

	requireNotApplied(t, answeredNow(qc), copyPairA)
	if got := offeredA.Load(); got != 1 {
		t.Errorf("pair A was offered %d times, want 1: its later entries must be skipped once it is stopped", got)
	}
	if got := queuedFrames(qc.rep); got != 1 {
		t.Errorf("the copy queued %d frames, want 1 (the entry of pair B)", got)
	}
	if errs := reportedNow(qc); len(errs) != 0 {
		t.Errorf("a not-ready pair is load, not a fault, but the copy reported %v", errs)
	}
}

// TestAnAdmissionErrorOfOnePairDoesNotPoisonTheFrame (BUG-0745, I8): the schema check fails for
// the first entry, which is reported once and answered not applied, and the entry of pair B
// behind it is still queued. [GUARD: return on the first admission error]
func TestAnAdmissionErrorOfOnePairDoesNotPoisonTheFrame(t *testing.T) {
	var checks atomic.Int64
	qc := newBatchCopy(t, func(cfg *ReplicatorConfig) {
		cfg.SchemaReady = func(uint64, cluster.Op) error {
			if checks.Add(1) == 1 {
				return &cluster.Error{Code: cluster.CodeUnavailable, Message: "the declaration of pair A has not been published yet"}
			}
			return nil
		}
	})
	holdAppends(qc)

	qc.rep.OnWriteBatch("owner", u32Epoch, []WriteEntry{
		copyEntry(t, copyPairA, 1), copyEntry(t, copyPairB, 1), copyEntry(t, copyPairA, 2),
	}, ownsEverything)

	requireNotApplied(t, answeredNow(qc), copyPairA)
	if errs := reportedNow(qc); len(errs) != 1 || !strings.Contains(errs[0].Error(), "declaration of pair A") {
		t.Errorf("the copy reported %v, want exactly the admission error of pair A", errs)
	}
	if got := queuedFrames(qc.rep); got != 1 {
		t.Errorf("the copy queued %d frames, want 1: an admission error of pair A must not fail pair B", got)
	}
	if got := checks.Load(); got != 2 {
		t.Errorf("the schema check ran %d times, want 2 (A1, B1; A2 is skipped)", got)
	}
}

// TestASlowAdmissionEndsTheFrameInsideTheCallTimeout (BUG-0745): every admission takes 400 ms
// of the fake clock, so the entries that begin after half the call timeout are answered not
// applied and nothing past the budget is admitted. [GUARD: no budget]
func TestASlowAdmissionEndsTheFrameInsideTheCallTimeout(t *testing.T) {
	if batchAdmitBudget*2 > linkCallTimeout {
		t.Fatalf("the admit budget %s is more than half of the call timeout %s", batchAdmitBudget, linkCallTimeout)
	}
	var elapsed, checks atomic.Int64
	base := time.Unix(1_700_000_000, 0)
	qc := newBatchCopy(t, func(cfg *ReplicatorConfig) {
		cfg.Now = func() time.Time { return base.Add(time.Duration(elapsed.Load())) }
		cfg.SchemaReady = func(uint64, cluster.Op) error {
			checks.Add(1)
			elapsed.Add(int64(400 * time.Millisecond))
			return nil
		}
	})
	holdAppends(qc)
	slots := []uint16{1, 2, 3, 4, 5}
	entries := make([]WriteEntry, 0, len(slots))
	for _, slot := range slots {
		entries = append(entries, copyEntry(t, slot, 1))
	}

	qc.rep.OnWriteBatch("owner", u32Epoch, entries, ownsEverything)

	// 0 ms, 400 ms and 800 ms are inside the 1 s budget; 1200 ms and 1600 ms are not.
	requireNotApplied(t, answeredNow(qc), 4, 5)
	if got := checks.Load(); got != 3 {
		t.Errorf("%d entries were admitted, want 3: nothing past the budget may be admitted", got)
	}
	if got := queuedFrames(qc.rep); got != 3 {
		t.Errorf("the copy queued %d frames, want 3", got)
	}
	if errs := reportedNow(qc); len(errs) != 0 {
		t.Errorf("running out of budget is load, not a fault, but the copy reported %v", errs)
	}
}

// TestABatchAtAStaleEpochAppliesNothing (BUG-0745, I9): a frame fenced by an epoch below the
// copy's queues nothing, answers each pair once, reports the fence once per pair, and moves no
// position. [GUARD: skip AdmitWrite]
func TestABatchAtAStaleEpochAppliesNothing(t *testing.T) {
	qc := newBatchCopy(t, nil)
	holdAppends(qc)

	qc.rep.OnWriteBatch("owner", u32Epoch-1, []WriteEntry{
		copyEntry(t, copyPairA, 1), copyEntry(t, copyPairA, 2), copyEntry(t, copyPairB, 1),
	}, ownsEverything)

	requireNotApplied(t, answeredNow(qc), copyPairA, copyPairB)
	if errs := reportedNow(qc); len(errs) != 2 {
		t.Errorf("the copy reported %d errors (%v), want one fence per pair", len(errs), errs)
	}
	if got := queuedFrames(qc.rep); got != 0 {
		t.Errorf("the copy queued %d frames of a stale epoch, want 0", got)
	}
	for _, slot := range []uint16{copyPairA, copyPairB} {
		if applied := qc.pairs.Snapshot(slot, u32DB).AppliedPos; applied != (cluster.Position{}) {
			t.Errorf("pair %d moved to %s on a stale epoch", slot, applied)
		}
	}
}

// TestAFrameFromANonOwnerQueuesNothing (BUG-0745, I4, I5): a frame from a node that does not
// own the pair is refused for every pair in it, reported with the sender's name and the slot,
// and nothing is queued. [GUARD: skip the owner check]
func TestAFrameFromANonOwnerQueuesNothing(t *testing.T) {
	qc := newBatchCopy(t, nil)
	holdAppends(qc)

	qc.rep.OnWriteBatch("intruder", u32Epoch, []WriteEntry{
		copyEntry(t, copyPairA, 1), copyEntry(t, copyPairA, 2), copyEntry(t, copyPairB, 1),
	}, func(uint16, uint64) bool { return false })

	requireNotApplied(t, answeredNow(qc), copyPairA, copyPairB)
	errs := reportedNow(qc)
	if len(errs) != 2 {
		t.Fatalf("the copy reported %d errors (%v), want one refusal per pair", len(errs), errs)
	}
	for index, slot := range []string{"slot 1", "slot 2"} {
		if text := errs[index].Error(); !strings.Contains(text, "intruder") || !strings.Contains(text, slot) {
			t.Errorf("refusal %d is %q, want it to name the sender and %s", index, text, slot)
		}
	}
	if got := queuedFrames(qc.rep); got != 0 {
		t.Errorf("the copy queued %d frames of a non-owner, want 0", got)
	}
}

// TestABatchedEntriesApplyInFrameOrderPerPair (BUG-0745, I2): entries A1 B1 A2 B2 A3 of one frame
// are applied in each pair's own order and acknowledged in ascending positions.
// [GUARD: enqueue in reverse]
func TestABatchedEntriesApplyInFrameOrderPerPair(t *testing.T) {
	qc := newBatchCopy(t, nil)

	qc.rep.OnWriteBatch("owner", u32Epoch, []WriteEntry{
		copyEntry(t, copyPairA, 1), copyEntry(t, copyPairB, 1), copyEntry(t, copyPairA, 2), copyEntry(t, copyPairB, 2), copyEntry(t, copyPairA, 3),
	}, ownsEverything)

	acked := map[uint16][]uint64{}
	for range 5 {
		ack := queueWaitAck(t, qc)
		if ack.Outcome != AckApplied {
			t.Fatalf("the copy answered %+v, want every entry applied", ack)
		}
		acked[ack.Slot] = append(acked[ack.Slot], ack.Pos.Seq)
	}
	wantA, wantB := []uint64{1, 2, 3}, []uint64{1, 2}
	if got := acked[copyPairA]; len(got) != len(wantA) || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Errorf("pair A was acknowledged at %v, want %v", got, wantA)
	}
	if got := acked[copyPairB]; len(got) != len(wantB) || got[0] != 1 || got[1] != 2 {
		t.Errorf("pair B was acknowledged at %v, want %v", got, wantB)
	}
	if applied := qc.pairs.Snapshot(copyPairA, u32DB).AppliedPos; applied != u32Pos(u32Epoch, 3) {
		t.Errorf("pair A applied %s, want 3", applied)
	}
	if applied := qc.pairs.Snapshot(copyPairB, u32DB).AppliedPos; applied != u32Pos(u32Epoch, 2) {
		t.Errorf("pair B applied %s, want 2", applied)
	}
	if errs := reportedNow(qc); len(errs) != 0 {
		t.Errorf("the copy reported %v", errs)
	}
}
