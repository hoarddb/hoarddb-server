package machine

import (
	"reflect"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// The three nodes of every scenario: a is the old owner (the sending side), b
// the new owner (the awaiting side), c a bystander.
const (
	nodeA = "a"
	nodeB = "b"
	nodeC = "c"

	t0         = 10 * time.Second
	stopBound  = 2100 * time.Millisecond // step_down_ms + heartbeat_ms at the defaults (conv §15)
	retryEvery = 200 * time.Millisecond  // replication.handoff_retry_ms
)

func barrierPos(epoch, seq uint64) cluster.Position { return cluster.Position{Epoch: epoch, Seq: seq} }

func barrierRid(n byte) [16]byte {
	var id [16]byte
	id[0] = n
	return id
}

func heldWrite(n byte) HeldOp { return HeldOp{RequestID: barrierRid(n), Body: []byte{n}} }
func heldRead(n byte) HeldOp  { return HeldOp{RequestID: barrierRid(n), Read: true} }

// sendingPair is node a, the old owner, one commit into a barrier toward b at
// epoch 6; it had appended 7 records at epoch 5.
func sendingPair() BarrierPair {
	return BarrierPair{Hold: HoldSending, Peer: nodeB, DrainTo: 7, Epoch: 6, ToEpoch: 6, fromEpoch: 5}
}

// awaitingPair is node b, the new owner, one commit into the same barrier. It
// is neither current nor proved yet.
func awaitingPair() BarrierPair {
	return BarrierPair{
		Hold: HoldAwaiting, Peer: nodeA, Epoch: 6, ToEpoch: 6, Rank1: true,
		Rank1At: t0, Bound: stopBound, rank1Seen: true,
	}
}

// readyPair is awaitingPair with both facts true except the one named by the test.
func readyPair() BarrierPair {
	b := awaitingPair()
	b.Current = true
	b.AppliedPos = barrierPos(5, 9)
	b.Target = barrierPos(5, 9)
	b.StopProved = true
	return b
}

// idlePair is a pair with no barrier, at epoch e.
func idlePair(e uint64) BarrierPair { return BarrierPair{Epoch: e} }

// noAction reports whether an action asks the caller to do nothing.
func noAction(a BarrierAction) bool { return reflect.DeepEqual(a, BarrierAction{}) }

func clonePair(b BarrierPair) BarrierPair {
	b.Held = append([]HeldOp(nil), b.Held...)
	if len(b.Held) == 0 {
		b.Held = nil
	}
	return b
}

// barrierRow is one line of the decision table in barrier.go's Apply godoc.
// row is that table's id (E1, Q4, ...), so TestBarrierDecisionTable can prove
// that every line has at least one case.
type barrierRow struct {
	row  string
	name string
	pre  BarrierPair
	ev   BarrierEvent
	want BarrierAction
	// change turns pre into the state the row must leave; nil means unchanged.
	change func(*BarrierPair)
}

var allBarrierRows = []string{
	"E1", "E2", "E3", "E4", "E5", "E6",
	"H1", "H2",
	"Q1", "Q2", "Q3", "Q4", "Q5", "Q6",
	"S1", "S2", "S3",
	"C1", "C2",
	"D1",
	"R1", "R2",
	"T1", "T2", "T3",
	"W1", "W2",
	"X1", "X2", "X3", "X4",
}

func commit(at time.Duration, epoch uint64, self, old, now string, moved bool, appended uint64) EpochCommitted {
	return EpochCommitted{At: at, Epoch: epoch, SelfID: self, OldRank1ID: old, Rank1ID: now, Moved: moved, Appended: appended, Bound: stopBound}
}

func withHeld(b BarrierPair, ops ...HeldOp) BarrierPair {
	b.Held = ops
	return b
}

func barrierRows() []barrierRow {
	fromA := func(toEpoch uint64) HandoffReqArrived {
		return HandoffReqArrived{From: nodeA, To: nodeB, FromEpoch: toEpoch - 1, ToEpoch: toEpoch, Valid: true}
	}
	stoppedFromA := func(toEpoch uint64) HandoffStoppedArrived {
		return HandoffStoppedArrived{From: nodeA, To: nodeB, ToEpoch: toEpoch, Valid: true}
	}
	readyFromB := func(toEpoch uint64, p cluster.Position) HandoffReadyArrived {
		return HandoffReadyArrived{From: nodeB, To: nodeA, ToEpoch: toEpoch, AppliedPos: p}
	}
	bothSent := BarrierAction{SendHandoffReq: true, SendStopped: true}
	readyOut := func(held ...HeldOp) BarrierAction {
		return BarrierAction{SendReady: true, ClearHold: true, ForwardHeld: held}
	}
	// What the awaiting side looks like once its READY went out.
	readySentState := func(b *BarrierPair) {
		b.Hold = HoldNone
		b.ReadySent = true
		b.Held = nil
	}

	return []barrierRow{
		// ---- RestartHold
		{row: "H1", name: "no hold: awaiting at the committed epoch, no peer, timing from boot",
			pre: idlePair(0), ev: RestartHold{At: t0, Bound: stopBound, Epoch: 6},
			change: func(b *BarrierPair) {
				b.Epoch, b.ToEpoch, b.Rank1 = 6, 6, true
				b.Hold, b.Peer = HoldAwaiting, ""
				b.Rank1At, b.Bound, b.rank1Seen = t0, stopBound, true
			}},
		{row: "H2", name: "a hold is open: unchanged",
			pre: awaitingPair(), ev: RestartHold{At: t0 + time.Second, Bound: 0, Epoch: 6}},
		{row: "H2", name: "a sending hold is open: unchanged",
			pre: sendingPair(), ev: RestartHold{At: t0, Bound: stopBound, Epoch: 6}},
		// ---- EpochCommitted
		{row: "E1", name: "moved away from the old owner: sending, REQ and STOPPED at once",
			pre: idlePair(5), ev: commit(t0, 6, nodeA, nodeA, nodeB, true, 7),
			want: bothSent,
			change: func(b *BarrierPair) {
				b.Epoch, b.ToEpoch, b.fromEpoch = 6, 6, 5
				b.Hold, b.Peer, b.DrainTo = HoldSending, nodeB, 7
			}},
		{row: "E2", name: "became rank 1: awaiting, timing starts at the commit",
			pre: idlePair(5), ev: commit(t0, 6, nodeB, nodeA, nodeB, true, 0),
			change: func(b *BarrierPair) {
				b.Epoch, b.ToEpoch, b.Rank1 = 6, 6, true
				b.Hold, b.Peer = HoldAwaiting, nodeA
				b.Rank1At, b.Bound, b.rank1Seen = t0, stopBound, true
			}},
		{row: "E3", name: "an unmoved epoch with no hold changes only the epoch and clears a sent READY",
			pre:    func() BarrierPair { b := idlePair(5); b.Rank1, b.ReadySent, b.ToEpoch = true, true, 5; return b }(),
			ev:     commit(t0, 6, nodeB, nodeB, nodeB, false, 0),
			change: func(b *BarrierPair) { b.Epoch, b.ReadySent = 6, false }},
		{row: "E3", name: "a pair that moved between two other nodes is not ours",
			pre: idlePair(5), ev: commit(t0, 6, nodeB, nodeA, nodeC, true, 3),
			change: func(b *BarrierPair) { b.Epoch = 6 }},
		{row: "E4", name: "sending, an unrelated epoch: hold dropped, held ops forwarded in order, nothing proved",
			pre: withHeld(sendingPair(), heldWrite(1), heldRead(2)), ev: commit(t0, 7, nodeA, nodeB, nodeB, false, 0),
			want: BarrierAction{ClearHold: true, ForwardHeld: []HeldOp{heldWrite(1), heldRead(2)}},
			change: func(b *BarrierPair) {
				*b = idlePair(7)
				b.ToEpoch = 6
			}},
		{row: "E4", name: "awaiting and still rank 1: re-enter awaiting with the proof and the evidence cleared",
			pre:  func() BarrierPair { b := withHeld(readyPair(), heldWrite(3)); b.Current = true; return b }(),
			ev:   commit(t0+time.Second, 7, nodeB, nodeB, nodeB, false, 0),
			want: BarrierAction{ForwardHeld: []HeldOp{heldWrite(3)}},
			change: func(b *BarrierPair) {
				*b = awaitingPair()
				b.Epoch, b.ToEpoch, b.Peer, b.Rank1At = 7, 7, "", t0+time.Second
			}},
		{row: "E4", name: "awaiting, then ranked out: sending toward the new rank 1",
			pre: withHeld(awaitingPair(), heldRead(4)), ev: commit(t0+time.Second, 7, nodeB, nodeB, nodeC, true, 0),
			want: BarrierAction{SendHandoffReq: true, SendStopped: true, ForwardHeld: []HeldOp{heldRead(4)}},
			change: func(b *BarrierPair) {
				*b = idlePair(7)
				b.Hold, b.Peer, b.ToEpoch, b.fromEpoch = HoldSending, nodeC, 7, 6
			}},
		{row: "E4", name: "sending, then rank 1 again: awaiting from the new commit",
			pre: withHeld(sendingPair(), heldWrite(5)), ev: commit(t0+time.Second, 7, nodeA, nodeB, nodeA, true, 0),
			want: BarrierAction{ForwardHeld: []HeldOp{heldWrite(5)}},
			change: func(b *BarrierPair) {
				*b = awaitingPair()
				b.Epoch, b.ToEpoch, b.Peer, b.Rank1At = 7, 7, nodeB, t0+time.Second
			}},
		{row: "E5", name: "the commit a HANDOFF_REQ announced: timing starts, an early STOPPED stays",
			pre: func() BarrierPair {
				b := idlePair(6)
				b.Hold, b.Peer, b.ToEpoch, b.Rank1, b.StopProved = HoldAwaiting, nodeA, 7, true, true
				return b
			}(),
			ev: commit(t0+time.Second, 7, nodeB, nodeA, nodeB, true, 0),
			change: func(b *BarrierPair) {
				b.Epoch, b.Rank1At, b.Bound, b.rank1Seen = 7, t0+time.Second, stopBound, true
			}},
		{row: "E5", name: "the announced commit with both facts already in: READY now",
			pre: func() BarrierPair {
				b := idlePair(6)
				b.Hold, b.Peer, b.ToEpoch, b.Rank1, b.StopProved = HoldAwaiting, nodeA, 7, true, true
				b.Current, b.AppliedPos, b.Target = true, barrierPos(6, 4), barrierPos(6, 4)
				b.Held = []HeldOp{heldWrite(6)}
				return b
			}(),
			ev:   commit(t0+time.Second, 7, nodeB, nodeA, nodeB, true, 0),
			want: readyOut(heldWrite(6)),
			change: func(b *BarrierPair) {
				b.Epoch, b.Rank1At, b.Bound, b.rank1Seen = 7, t0+time.Second, stopBound, true
				readySentState(b)
			}},
		{row: "E6", name: "a commit at or below the applied epoch is a duplicate",
			pre: withHeld(awaitingPair(), heldWrite(7)), ev: commit(t0, 6, nodeB, nodeA, nodeB, true, 0)},

		// ---- HandoffReqArrived
		{row: "Q1", name: "invalid ranks: dropped, never a refusal",
			pre: idlePair(6), ev: HandoffReqArrived{From: nodeA, To: nodeB, FromEpoch: 6, ToEpoch: 7}},
		{row: "Q1", name: "valid ranks but from >= to: dropped",
			pre: idlePair(6), ev: HandoffReqArrived{From: nodeA, To: nodeB, FromEpoch: 7, ToEpoch: 7, Valid: true}},
		{row: "Q2", name: "sending side: keep sending both",
			pre: sendingPair(), ev: fromA(6), want: bothSent},
		{row: "Q2", name: "sending side, a REQ for another epoch: ignored",
			pre: sendingPair(), ev: fromA(9)},
		{row: "Q3", name: "awaiting side: idempotent",
			pre: awaitingPair(), ev: fromA(6)},
		{row: "Q4", name: "READY already sent and the sender still asks: send READY again, change nothing",
			pre: func() BarrierPair { b := readyPair(); readySentState(&b); return b }(), ev: fromA(6),
			want: BarrierAction{SendReady: true}},
		{row: "Q5", name: "ahead of the commit: awaiting at once, exempt from the frame-epoch rule",
			pre: idlePair(6), ev: HandoffReqArrived{From: nodeA, To: nodeB, FromEpoch: 2, ToEpoch: 7, Valid: true},
			change: func(b *BarrierPair) {
				b.Hold, b.Peer, b.ToEpoch, b.Rank1 = HoldAwaiting, nodeA, 7, true
			}},
		{row: "Q6", name: "stale REQ after the hold ended: ignored",
			pre: idlePair(6), ev: fromA(6)},
		{row: "Q6", name: "stale REQ two epochs back: ignored",
			pre: idlePair(6), ev: fromA(4)},

		// ---- HandoffStoppedArrived
		{row: "S1", name: "invalid: dropped",
			pre: readyPair(), ev: HandoffStoppedArrived{From: nodeA, To: nodeB, ToEpoch: 6}},
		{row: "S2", name: "from the old owner at this ToEpoch while current: proved, READY, hold ends",
			pre: func() BarrierPair { b := readyPair(); b.StopProved = false; b.Held = []HeldOp{heldWrite(8)}; return b }(),
			ev:  stoppedFromA(6), want: readyOut(heldWrite(8)),
			change: func(b *BarrierPair) { b.StopProved = true; readySentState(b) }},
		{row: "S2", name: "proved but not current: wait for the catch-up",
			pre: awaitingPair(), ev: stoppedFromA(6),
			change: func(b *BarrierPair) { b.StopProved = true }},
		{row: "S3", name: "a proof for another epoch is not a proof for this one",
			pre: awaitingPair(), ev: stoppedFromA(7)},
		{row: "S3", name: "a STOPPED from a node that was not the predecessor proves nothing",
			pre: awaitingPair(), ev: HandoffStoppedArrived{From: nodeC, To: nodeB, ToEpoch: 6, Valid: true}},
		{row: "S3", name: "no hold: ignored",
			pre: idlePair(6), ev: stoppedFromA(6)},
		{row: "S3", name: "the sending side does not take a proof",
			pre: sendingPair(), ev: stoppedFromA(6)},

		// ---- CatchUpDone
		{row: "C1", name: "current through the target with the proof: READY, hold ends",
			pre:  func() BarrierPair { b := awaitingPair(); b.StopProved = true; b.Held = []HeldOp{heldRead(9)}; return b }(),
			ev:   CatchUpDone{At: t0, AppliedPos: barrierPos(5, 9), Target: barrierPos(5, 9), Current: true},
			want: readyOut(heldRead(9)),
			change: func(b *BarrierPair) {
				b.AppliedPos, b.Target, b.Current, b.StopProved = barrierPos(5, 9), barrierPos(5, 9), true, true
				readySentState(b)
			}},
		{row: "C1", name: "current and past the target but not proved: copied, no READY",
			pre: awaitingPair(), ev: CatchUpDone{AppliedPos: barrierPos(5, 10), Target: barrierPos(5, 9), Current: true},
			change: func(b *BarrierPair) { b.AppliedPos, b.Target, b.Current = barrierPos(5, 10), barrierPos(5, 9), true }},
		{row: "C1", name: "proved, current, but behind the target: copied, no READY",
			pre:    func() BarrierPair { b := awaitingPair(); b.StopProved = true; return b }(),
			ev:     CatchUpDone{AppliedPos: barrierPos(5, 8), Target: barrierPos(5, 9), Current: true},
			change: func(b *BarrierPair) { b.AppliedPos, b.Target, b.Current = barrierPos(5, 8), barrierPos(5, 9), true }},
		{row: "C1", name: "proved and at the target but the source did not say current: no READY",
			pre:    func() BarrierPair { b := awaitingPair(); b.StopProved = true; return b }(),
			ev:     CatchUpDone{AppliedPos: barrierPos(5, 9), Target: barrierPos(5, 9)},
			change: func(b *BarrierPair) { b.AppliedPos, b.Target = barrierPos(5, 9), barrierPos(5, 9) }},
		{row: "C2", name: "no hold: ignored",
			pre: idlePair(6), ev: CatchUpDone{AppliedPos: barrierPos(5, 9), Target: barrierPos(5, 9), Current: true}},
		{row: "C2", name: "sending side: ignored",
			pre: sendingPair(), ev: CatchUpDone{AppliedPos: barrierPos(5, 9), Target: barrierPos(5, 9), Current: true}},

		// ---- DrainDeadline
		{row: "D1", name: "the sending side never waits past the drain deadline",
			pre: withHeld(sendingPair(), heldWrite(10)), ev: DrainDeadline{At: t0 + 2*time.Second}},
		{row: "D1", name: "nor does the awaiting side care",
			pre: awaitingPair(), ev: DrainDeadline{At: t0 + 2*time.Second}},

		// ---- HandoffReadyArrived
		{row: "R1", name: "READY covering drainTo: hold ends, latch, held ops forwarded",
			pre: withHeld(sendingPair(), heldWrite(11), heldRead(12)), ev: readyFromB(6, barrierPos(5, 7)),
			want: BarrierAction{ClearHold: true, ForwardHeld: []HeldOp{heldWrite(11), heldRead(12)}},
			change: func(b *BarrierPair) {
				b.Hold, b.ReadyRecv, b.AppliedPos, b.Held = HoldNone, true, barrierPos(5, 7), nil
			}},
		{row: "R1", name: "nothing was appended at the old epoch: any READY covers it",
			pre: func() BarrierPair { b := sendingPair(); b.DrainTo = 0; return b }(), ev: readyFromB(6, barrierPos(4, 2)),
			want: BarrierAction{ClearHold: true},
			change: func(b *BarrierPair) {
				b.Hold, b.ReadyRecv, b.AppliedPos = HoldNone, true, barrierPos(4, 2)
			}},
		{row: "R2", name: "READY below drainTo: the pair stays sending (G2)",
			pre: sendingPair(), ev: readyFromB(6, barrierPos(5, 6))},
		{row: "R2", name: "a duplicate READY after the hold ended",
			pre: func() BarrierPair {
				b := sendingPair()
				b.Hold, b.ReadyRecv, b.AppliedPos = HoldNone, true, barrierPos(5, 7)
				return b
			}(), ev: readyFromB(6, barrierPos(5, 99))},
		{row: "R2", name: "READY for another epoch",
			pre: sendingPair(), ev: readyFromB(7, barrierPos(5, 7))},
		{row: "R2", name: "READY from a node that is not the new owner",
			pre: sendingPair(), ev: HandoffReadyArrived{From: nodeC, To: nodeA, ToEpoch: 6, AppliedPos: barrierPos(5, 7)}},
		{row: "R2", name: "the awaiting side does not take a READY",
			pre: awaitingPair(), ev: readyFromB(6, barrierPos(5, 7))},

		// ---- RetryTick
		{row: "T1", name: "sending: REQ and STOPPED again until the hold ends",
			pre: sendingPair(), ev: RetryTick{At: t0 + retryEvery}, want: bothSent},
		{row: "T2", name: "awaiting, the bound elapsed since the commit, current: proved by time, READY",
			pre: func() BarrierPair {
				b := readyPair()
				b.StopProved = false
				b.Held = []HeldOp{heldWrite(13)}
				return b
			}(),
			ev: RetryTick{At: t0 + stopBound}, want: readyOut(heldWrite(13)),
			change: func(b *BarrierPair) { b.StopProved = true; readySentState(b) }},
		{row: "T2", name: "the bound proves, but the pair is not current: wait",
			pre: awaitingPair(), ev: RetryTick{At: t0 + stopBound},
			change: func(b *BarrierPair) { b.StopProved = true }},
		{row: "T3", name: "one nanosecond short of the bound proves nothing",
			pre: func() BarrierPair { b := readyPair(); b.StopProved = false; return b }(),
			ev:  RetryTick{At: t0 + stopBound - 1}},
		{row: "T3", name: "a hold opened by a REQ ahead of the commit has no clock yet",
			pre: func() BarrierPair {
				b := idlePair(6)
				b.Hold, b.Peer, b.ToEpoch, b.Rank1 = HoldAwaiting, nodeA, 7, true
				b.Current, b.AppliedPos, b.Target = true, barrierPos(6, 4), barrierPos(6, 4)
				return b
			}(), ev: RetryTick{At: 100 * time.Hour}},
		{row: "T3", name: "no hold: nothing to resend",
			pre: idlePair(6), ev: RetryTick{At: t0}},
		{row: "T3", name: "awaiting and already proved: nothing to resend",
			pre: func() BarrierPair { b := awaitingPair(); b.StopProved = true; return b }(),
			ev:  RetryTick{At: t0 + time.Hour}},
		{row: "T3", name: "after READY the new owner is out of its hold: the sender's REQ drives the retransmission",
			pre: func() BarrierPair { b := readyPair(); readySentState(&b); return b }(), ev: RetryTick{At: t0 + time.Hour}},

		// ---- WriteArrived
		{row: "W1", name: "sending: the write joins the hold, unpositioned",
			pre: sendingPair(), ev: WriteArrived{RequestID: barrierRid(20), Body: []byte("w")},
			change: func(b *BarrierPair) { b.Held = []HeldOp{{RequestID: barrierRid(20), Body: []byte("w")}} }},
		{row: "W1", name: "awaiting: the write joins the hold, after the earlier one",
			pre: withHeld(awaitingPair(), heldWrite(1)), ev: WriteArrived{RequestID: barrierRid(21), Body: []byte("v")},
			change: func(b *BarrierPair) { b.Held = []HeldOp{heldWrite(1), {RequestID: barrierRid(21), Body: []byte("v")}} }},
		{row: "W2", name: "no hold: the ordinary path takes it",
			pre: idlePair(6), ev: WriteArrived{RequestID: barrierRid(22), Body: []byte("w")}},

		// ---- ReadArrived
		{row: "X1", name: "sending: held, even when the new owner is unreachable and the caller will not wait",
			pre: sendingPair(), ev: ReadArrived{RequestID: barrierRid(30)},
			change: func(b *BarrierPair) { b.Held = []HeldOp{heldRead(30)} }},
		{row: "X1", name: "sending: held when everything is fine",
			pre: sendingPair(), ev: ReadArrived{RequestID: barrierRid(31), PeerReachable: true, CallerWillWait: true},
			change: func(b *BarrierPair) { b.Held = []HeldOp{heldRead(31)} }},
		{row: "X2", name: "awaiting, peer unreachable, caller will not wait: NOT_OWNER, nothing held",
			pre: awaitingPair(), ev: ReadArrived{RequestID: barrierRid(32)},
			want: BarrierAction{Refuse: cluster.CodeNotOwner}},
		{row: "X3", name: "awaiting, peer unreachable but the caller waits: held",
			pre: awaitingPair(), ev: ReadArrived{RequestID: barrierRid(33), CallerWillWait: true},
			change: func(b *BarrierPair) { b.Held = []HeldOp{heldRead(33)} }},
		{row: "X3", name: "awaiting, peer reachable, the caller will not wait: held",
			pre: awaitingPair(), ev: ReadArrived{RequestID: barrierRid(34), PeerReachable: true},
			change: func(b *BarrierPair) { b.Held = []HeldOp{heldRead(34)} }},
		{row: "X4", name: "no hold: answered by the ordinary path",
			pre: idlePair(6), ev: ReadArrived{RequestID: barrierRid(35), PeerReachable: true}},
	}
}

// TestBarrierDecisionTable runs one case per line of the decision table in
// Apply's godoc and fails if a line of that table has no case. A row that
// changed state it should not have, or missed an action, fails here by name.
func TestBarrierDecisionTable(t *testing.T) {
	covered := map[string]bool{}
	for _, r := range barrierRows() {
		covered[r.row] = true
		t.Run(r.row+"/"+r.name, func(t *testing.T) {
			got := clonePair(r.pre)
			act := got.Apply(r.ev)
			want := clonePair(r.pre)
			if r.change != nil {
				r.change(&want)
			}
			if !reflect.DeepEqual(act, r.want) {
				t.Errorf("action = %+v, want %+v", act, r.want)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("state = %+v\nwant   %+v", got, want)
			}
		})
	}
	for _, id := range allBarrierRows {
		if !covered[id] {
			t.Errorf("decision table line %s has no case", id)
		}
	}
	known := map[string]bool{}
	for _, id := range allBarrierRows {
		known[id] = true
	}
	for id := range covered {
		if !known[id] {
			t.Errorf("case for %s, which is not a line of the decision table", id)
		}
	}
}

// TestNeitherSideAnswersDuringABarrier pins the property the whole barrier
// exists for (write §8, conv §8 check 2): while a hold is open, no write gets
// a position and no read is answered by either node. The sending node may no
// longer be the owner; the awaiting node may not be current.
func TestNeitherSideAnswersDuringABarrier(t *testing.T) {
	t.Run("the sending side holds its reads and its writes", func(t *testing.T) {
		a := idlePair(5)
		if act := a.Apply(commit(t0, 6, nodeA, nodeA, nodeB, true, 7)); !act.SendHandoffReq || !act.SendStopped {
			t.Fatalf("the commit must send REQ and STOPPED, got %+v", act)
		}
		for _, in := range []ReadArrived{
			{RequestID: barrierRid(1), PeerReachable: true, CallerWillWait: true},
			{RequestID: barrierRid(2)}, // even a read that may not wait is not answered by the superseded node
		} {
			if act := a.Apply(in); !noAction(act) || a.Held[len(a.Held)-1].RequestID != in.RequestID {
				t.Fatalf("read %v: want it held with no action, got %+v held=%v", in.RequestID, act, a.Held)
			}
		}
		a.Apply(WriteArrived{RequestID: barrierRid(3), Body: []byte("w")})
		if len(a.Held) != 3 {
			t.Fatalf("held = %d ops, want 3 (two reads, one write)", len(a.Held))
		}
		act := a.Apply(HandoffReadyArrived{From: nodeB, To: nodeA, ToEpoch: 6, AppliedPos: barrierPos(5, 7)})
		wantIDs := []byte{1, 2, 3}
		if len(act.ForwardHeld) != 3 {
			t.Fatalf("READY must return all three held ops, got %d", len(act.ForwardHeld))
		}
		for i, op := range act.ForwardHeld {
			if op.RequestID[0] != wantIDs[i] {
				t.Fatalf("forwarded op %d has request id %d, want %d: the id must be kept and the order too", i, op.RequestID[0], wantIDs[i])
			}
		}
		if !act.ForwardHeld[0].Read || !act.ForwardHeld[1].Read || act.ForwardHeld[2].Read {
			t.Fatalf("a held read must come back as a read and a held write as a write: %+v", act.ForwardHeld)
		}
	})

	t.Run("the awaiting side holds both until it is current and the old owner stopped", func(t *testing.T) {
		b := idlePair(5)
		b.Apply(commit(t0, 6, nodeB, nodeA, nodeB, true, 0))
		b.Apply(WriteArrived{RequestID: barrierRid(4), Body: []byte("w")})
		b.Apply(ReadArrived{RequestID: barrierRid(5), PeerReachable: true})
		// Current alone is not enough to answer a read: the old owner may still serve.
		b.Apply(CatchUpDone{AppliedPos: barrierPos(5, 9), Target: barrierPos(5, 9), Current: true})
		if act := b.Apply(ReadArrived{RequestID: barrierRid(6), PeerReachable: true}); !noAction(act) || len(b.Held) != 3 {
			t.Fatalf("a read while only current must be held: act=%+v held=%d", act, len(b.Held))
		}
		act := b.Apply(HandoffStoppedArrived{From: nodeA, To: nodeB, ToEpoch: 6, Valid: true})
		if !act.SendReady || !act.ClearHold || len(act.ForwardHeld) != 3 {
			t.Fatalf("both facts: want READY, the hold cleared and the three held ops handed back, got %+v", act)
		}
		// Out of the hold, the new owner answers by the ordinary path.
		if act := b.Apply(ReadArrived{RequestID: barrierRid(7), PeerReachable: true}); !noAction(act) || len(b.Held) != 0 {
			t.Fatalf("after READY a read is the ordinary path's: act=%+v held=%d", act, len(b.Held))
		}
	})

	t.Run("a read that may not wait is refused rather than forwarded to a dead peer", func(t *testing.T) {
		b := awaitingPair()
		act := b.Apply(ReadArrived{RequestID: barrierRid(8)})
		if act.Refuse != cluster.CodeNotOwner || len(b.Held) != 0 {
			t.Fatalf("want NOT_OWNER and nothing held, got %+v held=%d", act, len(b.Held))
		}
	})
}

// TestAHoldEndsAtAFurtherEpochChange pins that no hold outlives the next
// epoch, for either side, and that an epoch alone proves nothing about the
// node that was serving (write §8, conv §8 check 2).
func TestAHoldEndsAtAFurtherEpochChange(t *testing.T) {
	t.Run("sending: dropped, held ops go to the current owner, a late READY is ignored", func(t *testing.T) {
		a := sendingPair()
		a.Apply(WriteArrived{RequestID: barrierRid(1), Body: []byte("w")})
		a.Apply(ReadArrived{RequestID: barrierRid(2)})
		act := a.Apply(commit(t0+time.Second, 7, nodeA, nodeC, nodeC, false, 0))
		if a.Hold != HoldNone || !act.ClearHold || len(act.ForwardHeld) != 2 || len(a.Held) != 0 {
			t.Fatalf("the hold must end with both ops returned: hold=%v act=%+v held=%d", a.Hold, act, len(a.Held))
		}
		before := clonePair(a)
		if late := a.Apply(HandoffReadyArrived{From: nodeB, To: nodeA, ToEpoch: 6, AppliedPos: barrierPos(5, 99)}); !noAction(late) || !reflect.DeepEqual(a, before) {
			t.Fatalf("a READY for the dropped barrier must change nothing: act=%+v", late)
		}
	})

	t.Run("awaiting: dropped, and still rank 1 means a new hold with the proof cleared", func(t *testing.T) {
		b := readyPair() // current and already proved at epoch 6, READY not yet sent
		b.Apply(WriteArrived{RequestID: barrierRid(3), Body: []byte("w")})
		act := b.Apply(commit(t0+500*time.Millisecond, 7, nodeB, nodeB, nodeB, false, 0))
		if b.Hold != HoldAwaiting || b.StopProved || b.Current || b.ReadySent {
			t.Fatalf("an epoch bump alone proves nothing: want awaiting with no proof and no evidence, got %+v", b)
		}
		if len(act.ForwardHeld) != 1 || act.SendReady || b.ToEpoch != 7 || b.Rank1At != t0+500*time.Millisecond {
			t.Fatalf("want the held write returned and the clock restarted at the new commit, got act=%+v pair=%+v", act, b)
		}
		// Even with the catch-up redone, no READY until the proof is re-established.
		if act := b.Apply(CatchUpDone{AppliedPos: barrierPos(6, 1), Target: barrierPos(6, 1), Current: true}); act.SendReady {
			t.Fatalf("READY without a proof at the new epoch: %+v", act)
		}
	})

	t.Run("awaiting, then ranked out: the held read goes back and this node becomes the sending side", func(t *testing.T) {
		b := awaitingPair()
		b.Apply(ReadArrived{RequestID: barrierRid(4), CallerWillWait: true})
		act := b.Apply(commit(t0+time.Second, 7, nodeB, nodeB, nodeC, true, 0))
		if b.Hold != HoldSending || len(act.ForwardHeld) != 1 || !act.SendStopped || act.ClearHold {
			t.Fatalf("want the read returned and a new sending hold toward c, got hold=%v act=%+v", b.Hold, act)
		}
	})

	t.Run("a hold never needs a READY to end", func(t *testing.T) {
		for _, name := range []string{"sending", "awaiting"} {
			p := sendingPair()
			if name == "awaiting" {
				p = awaitingPair()
			}
			p.Apply(WriteArrived{RequestID: barrierRid(9), Body: []byte("w")})
			p.Apply(commit(t0+time.Second, 7, nodeA, nodeC, nodeC, false, 0))
			if p.Hold != HoldNone || len(p.Held) != 0 {
				t.Fatalf("%s: still holding after a further epoch: %+v", name, p)
			}
		}
	})
}

// TestBarrierMessagesAreExemptFromTheFrameEpochRule pins that HANDOFF_REQ and
// HANDOFF_STOPPED are judged by their own two epochs and two ranks, not by the
// receiver's committed epoch (conv §7): the old owner sends them the instant
// it applies the commit, which the new owner may not have applied yet.
func TestBarrierMessagesAreExemptFromTheFrameEpochRule(t *testing.T) {
	b := idlePair(6) // this node is still at epoch 6
	// The data rule would drop both: their epoch (7) is not the committed epoch (6).
	b.Apply(HandoffReqArrived{From: nodeA, To: nodeB, FromEpoch: 2, ToEpoch: 7, Valid: true})
	if b.Hold != HoldAwaiting || b.ToEpoch != 7 {
		t.Fatalf("a REQ ahead of the commit must open the hold: %+v", b)
	}
	b.Apply(HandoffStoppedArrived{From: nodeA, To: nodeB, ToEpoch: 7, Valid: true})
	if !b.StopProved {
		t.Fatalf("a STOPPED ahead of the commit must be kept: %+v", b)
	}
	b.Apply(CatchUpDone{AppliedPos: barrierPos(6, 3), Target: barrierPos(6, 3), Current: true})
	if b.ReadySent {
		t.Fatalf("READY must wait for the commit itself: the node may not vouch for an epoch it has not applied")
	}
	act := b.Apply(commit(t0, 7, nodeB, nodeA, nodeB, true, 0))
	if !act.SendReady || !b.StopProved {
		t.Fatalf("the early proof must survive the commit that confirms it: act=%+v pair=%+v", act, b)
	}

	// An invalid pair of ranks is a logged drop, and it is never a refusal.
	c := idlePair(6)
	if act := c.Apply(HandoffReqArrived{From: nodeA, To: nodeB, FromEpoch: 7, ToEpoch: 7, Valid: true}); !noAction(act) || c.Hold != HoldNone {
		t.Fatalf("from >= to must be dropped silently: act=%+v pair=%+v", act, c)
	}
}

// TestDuplicateReadyIsIdempotent pins that a second HANDOFF_READY for the same
// (From, ToEpoch) changes nothing: the first one ended the hold and latched
// the drain, a repeat must not drain again or forward anything twice.
func TestDuplicateReadyIsIdempotent(t *testing.T) {
	a := sendingPair()
	a.Apply(WriteArrived{RequestID: barrierRid(1), Body: []byte("w")})
	first := a.Apply(HandoffReadyArrived{From: nodeB, To: nodeA, ToEpoch: 6, AppliedPos: barrierPos(5, 7)})
	if len(first.ForwardHeld) != 1 || !first.ClearHold || !a.ReadyRecv {
		t.Fatalf("the first READY must end the hold and return the write: %+v", first)
	}
	// A write that arrives after the hold ended is the ordinary path's; it must not be re-held.
	a.Apply(WriteArrived{RequestID: barrierRid(2), Body: []byte("later")})
	after := clonePair(a)
	second := a.Apply(HandoffReadyArrived{From: nodeB, To: nodeA, ToEpoch: 6, AppliedPos: barrierPos(5, 90)})
	if !noAction(second) {
		t.Fatalf("a duplicate READY must do nothing, got %+v", second)
	}
	if !reflect.DeepEqual(a, after) {
		t.Fatalf("a duplicate READY must not move the drain latch: %+v, was %+v", a, after)
	}
}

// TestHandoffReadyNeedsBothFacts pins that READY goes out only when the pair
// is current through its target AND the old owner is proved stopped. The two
// are independent inputs; either alone is not enough (write §8 step 5).
func TestHandoffReadyNeedsBothFacts(t *testing.T) {
	cases := []struct {
		name       string
		current    bool
		applied    cluster.Position
		stopProved bool
		wantReady  bool
	}{
		{"neither", false, barrierPos(5, 1), false, false},
		{"the last chunk alone: current, at target, old owner not proved", true, barrierPos(5, 9), false, false},
		{"the proof alone: stopped, not current", false, barrierPos(5, 9), true, false},
		{"stopped and current but short of the target", true, barrierPos(5, 8), true, false},
		{"stopped, current, past the target", true, barrierPos(6, 1), true, true},
		{"stopped, current, exactly at the target", true, barrierPos(5, 9), true, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Whichever event arrives last must reach the same verdict.
			byChunk := awaitingPair()
			byChunk.StopProved = c.stopProved
			actChunk := byChunk.Apply(CatchUpDone{AppliedPos: c.applied, Target: barrierPos(5, 9), Current: c.current})

			byProof := awaitingPair()
			byProof.Apply(CatchUpDone{AppliedPos: c.applied, Target: barrierPos(5, 9), Current: c.current})
			var actProof BarrierAction
			if c.stopProved {
				actProof = byProof.Apply(HandoffStoppedArrived{From: nodeA, To: nodeB, ToEpoch: 6, Valid: true})
			}
			for who, act := range map[string]BarrierAction{"chunk last": actChunk, "proof last": actProof} {
				if act.SendReady != c.wantReady {
					t.Errorf("%s: SendReady = %v, want %v", who, act.SendReady, c.wantReady)
				}
				if act.SendReady && !act.ClearHold {
					t.Errorf("%s: READY must end the awaiting hold in the same step", who)
				}
			}
		})
	}
}

// TestStopProvedByTheBoundAfterAnUnrelatedBump pins that a proof is per
// barrier: after an epoch change that is not about this pair, the old owner is
// not known to be stopped, and the lease bound runs again from the NEW commit
// (record §10.5). The first commit's proof must not carry over.
func TestStopProvedByTheBoundAfterAnUnrelatedBump(t *testing.T) {
	b := idlePair(5)
	b.Apply(commit(t0, 6, nodeB, nodeA, nodeB, true, 0))
	// The old owner's STOPPED arrives while the catch-up is still running: proved for epoch 6.
	b.Apply(HandoffStoppedArrived{From: nodeA, To: nodeB, ToEpoch: 6, Valid: true})
	if !b.StopProved {
		t.Fatalf("setup: a STOPPED for the barrier's epoch must prove it: %+v", b)
	}
	// 1.5 s in, an unrelated bump arrives before the pair is current. The proof
	// was about the node that served at epoch 6, and that proves nothing about 7.
	bump := t0 + 1500*time.Millisecond
	act := b.Apply(commit(bump, 7, nodeB, nodeB, nodeB, false, 0))
	if b.StopProved || b.Hold != HoldAwaiting || act.SendReady {
		t.Fatalf("the bump must re-enter awaiting with nothing proved: %+v act=%+v", b, act)
	}
	// The catch-up is redone at the new epoch.
	b.Apply(CatchUpDone{AppliedPos: barrierPos(6, 2), Target: barrierPos(6, 2), Current: true})

	// The first commit's bound has elapsed here. It proves nothing any more.
	if act := b.Apply(RetryTick{At: t0 + stopBound}); act.SendReady || b.StopProved {
		t.Fatalf("the old commit's bound must not prove the new barrier: %+v act=%+v", b, act)
	}
	if act := b.Apply(RetryTick{At: bump + stopBound - 1}); act.SendReady || b.StopProved {
		t.Fatalf("one nanosecond short of the new bound: %+v act=%+v", b, act)
	}
	act = b.Apply(RetryTick{At: bump + stopBound})
	if !act.SendReady || !act.ClearHold || !b.StopProved || b.Hold != HoldNone {
		t.Fatalf("the bound from the new commit proves it and, being current, READY goes out: %+v act=%+v", b, act)
	}

	t.Run("a STOPPED at the old epoch is not a proof for the new one", func(t *testing.T) {
		p := idlePair(5)
		p.Apply(commit(t0, 6, nodeB, nodeA, nodeB, true, 0))
		p.Apply(commit(t0+time.Second, 7, nodeB, nodeB, nodeB, false, 0))
		p.Apply(CatchUpDone{AppliedPos: barrierPos(6, 1), Target: barrierPos(6, 1), Current: true})
		if act := p.Apply(HandoffStoppedArrived{From: nodeA, To: nodeB, ToEpoch: 6, Valid: true}); act.SendReady || p.StopProved {
			t.Fatalf("a proof for epoch 6 is not a proof for epoch 7: %+v act=%+v", p, act)
		}
	})
}

// TestReadyIsRetransmittedUntilTheHoldEnds drives the two sides across a
// channel that loses messages. Each side's messages are repeated until the
// other side's hold is known to have ended: A repeats REQ and STOPPED every
// retry tick until a READY arrives, and B answers every REQ with READY until
// the next epoch (write §8, retransmission).
func TestReadyIsRetransmittedUntilTheHoldEnds(t *testing.T) {
	a := idlePair(5)
	b := idlePair(5)
	a.Apply(commit(t0, 6, nodeA, nodeA, nodeB, true, 7))
	b.Apply(commit(t0, 6, nodeB, nodeA, nodeB, true, 0))
	b.Apply(CatchUpDone{AppliedPos: barrierPos(5, 7), Target: barrierPos(5, 7), Current: true})
	a.Apply(WriteArrived{RequestID: barrierRid(1), Body: []byte("held at a")})

	readyLost, readyDelivered, readySends := 2, 0, 0
	now := t0
	for tick := 0; tick < 6; tick++ {
		now += retryEvery
		act := a.Apply(RetryTick{At: now})
		if a.Hold == HoldNone {
			if !noAction(act) {
				t.Fatalf("tick %d: A's hold ended, it must send nothing more: %+v", tick, act)
			}
			continue
		}
		if !act.SendHandoffReq || !act.SendStopped {
			t.Fatalf("tick %d: A must repeat REQ and STOPPED while its hold is open: %+v", tick, act)
		}
		// Both messages reach B (this test loses only the READY).
		reqAct := b.Apply(HandoffReqArrived{From: nodeA, To: nodeB, FromEpoch: 5, ToEpoch: 6, Valid: true})
		stoppedAct := b.Apply(HandoffStoppedArrived{From: nodeA, To: nodeB, ToEpoch: 6, Valid: true})
		sent := reqAct.SendReady || stoppedAct.SendReady
		if tick == 0 && !stoppedAct.SendReady {
			t.Fatalf("B must send READY on the STOPPED that completes both facts: %+v", stoppedAct)
		}
		if !sent {
			t.Fatalf("tick %d: B must answer A's retransmission with READY again", tick)
		}
		readySends++
		if readyLost > 0 {
			readyLost--
			continue
		}
		fwd := a.Apply(HandoffReadyArrived{From: nodeB, To: nodeA, ToEpoch: 6, AppliedPos: barrierPos(5, 7)})
		readyDelivered++
		if len(fwd.ForwardHeld) != 1 {
			t.Fatalf("the READY that finally arrives must return A's held write: %+v", fwd)
		}
	}
	if readyDelivered != 1 || readySends != 3 {
		t.Fatalf("READY was sent %d times and delivered %d times, want 3 and 1: it must be repeated until delivered, then stop", readySends, readyDelivered)
	}
	if a.Hold != HoldNone || b.Hold != HoldNone {
		t.Fatalf("both holds must be over: a=%v b=%v", a.Hold, b.Hold)
	}

	// After both holds ended, a delayed or duplicated barrier message changes nothing.
	for name, ev := range map[string]BarrierEvent{
		"REQ":     HandoffReqArrived{From: nodeA, To: nodeB, FromEpoch: 5, ToEpoch: 6, Valid: true},
		"STOPPED": HandoffStoppedArrived{From: nodeA, To: nodeB, ToEpoch: 6, Valid: true},
	} {
		before := clonePair(b)
		b.Apply(ev)
		if !reflect.DeepEqual(b, before) {
			t.Errorf("a duplicate %s after the hold ended changed B: %+v, was %+v", name, b, before)
		}
	}
	// The next epoch ends the retransmission window itself.
	b.Apply(commit(now+retryEvery, 7, nodeB, nodeB, nodeB, false, 0))
	if act := b.Apply(HandoffReqArrived{From: nodeA, To: nodeB, FromEpoch: 5, ToEpoch: 6, Valid: true}); !noAction(act) {
		t.Errorf("after a further epoch a late REQ gets no READY: %+v", act)
	}
}

// restartedPair is a pair one RestartHold into epoch 6 at boot time t0.
func restartedPair(bound time.Duration) BarrierPair {
	var b BarrierPair
	b.Apply(RestartHold{At: t0, Bound: bound, Epoch: 6})
	return b
}

// TestRestartHoldProvesStopOnlyFromBoot: the bound is the only proof of a peer-less
// hold; it is not early, not by a stranger's STOPPED, and READY needs the catch-up too.
func TestRestartHoldProvesStopOnlyFromBoot(t *testing.T) {
	b := restartedPair(stopBound)
	if act := b.Apply(RetryTick{At: t0 + stopBound - time.Millisecond}); !noAction(act) || b.StopProved {
		t.Fatalf("before the bound: action %+v, StopProved %v", act, b.StopProved)
	}
	if act := b.Apply(HandoffStoppedArrived{From: "", To: nodeB, ToEpoch: 6, Valid: true}); !noAction(act) || b.StopProved {
		t.Fatalf("a STOPPED from no named peer proved a stop: %+v", act)
	}
	if act := b.Apply(HandoffStoppedArrived{From: nodeA, To: nodeB, ToEpoch: 6, Valid: true}); !noAction(act) || b.StopProved {
		t.Fatalf("a STOPPED from a stranger proved a stop: %+v", act)
	}
	if act := b.Apply(RetryTick{At: t0 + stopBound}); !noAction(act) || !b.StopProved {
		t.Fatalf("at the bound: action %+v, StopProved %v; want proved with no READY (not current)", act, b.StopProved)
	}
	if b.Hold != HoldAwaiting {
		t.Fatalf("hold %v ended without the catch-up", b.Hold)
	}
	act := b.Apply(CatchUpDone{AppliedPos: barrierPos(5, 9), Target: barrierPos(5, 9), Current: true})
	if !act.ClearHold || !act.SendReady || b.Hold != HoldNone {
		t.Fatalf("catch-up after the proof: action %+v, hold %v; want the hold cleared", act, b.Hold)
	}
}

// TestRestartHoldCatchUpBeforeTheBound: a finished catch-up alone does not open the pair.
func TestRestartHoldCatchUpBeforeTheBound(t *testing.T) {
	b := restartedPair(stopBound)
	if act := b.Apply(CatchUpDone{AppliedPos: barrierPos(5, 9), Target: barrierPos(5, 9), Current: true}); !noAction(act) || b.Hold != HoldAwaiting {
		t.Fatalf("caught up but unproved: action %+v, hold %v", act, b.Hold)
	}
	if act := b.Apply(RetryTick{At: t0 + stopBound}); !act.ClearHold || b.Hold != HoldNone {
		t.Fatalf("at the bound with the catch-up in: action %+v, hold %v", act, b.Hold)
	}
}

// TestRestartHoldStaleAndFutureRequests: a REQ from an old epoch is ignored, one for the
// next epoch finds the hold open and changes nothing (Q3), and the commit it announces
// re-enters awaiting with a real peer (E4).
func TestRestartHoldStaleAndFutureRequests(t *testing.T) {
	b := restartedPair(stopBound)
	b.Apply(HandoffReqArrived{From: nodeA, To: nodeB, FromEpoch: 3, ToEpoch: 4, Valid: true})
	if b.Peer != "" || b.ToEpoch != 6 {
		t.Fatalf("a stale REQ changed the hold: peer %q to-epoch %d", b.Peer, b.ToEpoch)
	}
	b.Apply(HandoffReqArrived{From: nodeA, To: nodeB, FromEpoch: 6, ToEpoch: 7, Valid: true})
	if b.Hold != HoldAwaiting || b.Peer != "" || b.ToEpoch != 6 {
		t.Fatalf("a REQ for the next epoch changed the open hold: %+v", b)
	}
	c := restartedPair(stopBound)
	c.Apply(commit(t0+time.Second, 7, nodeB, nodeA, nodeB, true, 0))
	if c.Hold != HoldAwaiting || c.Peer != nodeA || c.Rank1At != t0+time.Second || c.StopProved {
		t.Fatalf("a further epoch: %+v; want awaiting from the new commit with peer a", c)
	}
}
