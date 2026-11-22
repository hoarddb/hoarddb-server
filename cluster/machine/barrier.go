// Package machine holds the entry-ring's pure state machines: values with an
// Apply method that takes an event and returns a decision. A machine never
// reads a clock, opens a socket, takes a lock or starts a goroutine; time is
// a time.Duration the caller passes in the event.
package machine

import (
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// Hold says which side of a primary-handoff barrier a (slot, database) pair
// is on (write §8). A hold means no position is assigned and no read is
// answered for the pair.
type Hold uint8

// The three hold states.
const (
	// HoldNone is no barrier: the pair orders and answers as its owner status says.
	HoldNone Hold = iota
	// HoldSending is the old owner: it neither orders nor answers a read, because
	// a newer epoch may already be serving the pair.
	HoldSending
	// HoldAwaiting is the new owner: it orders and answers only once it is
	// current and has proved the old owner stopped.
	HoldAwaiting
)

// HeldOp is one operation parked by a hold. It never carries a position, so
// it can be answered or forwarded exactly once, by whoever owns the pair when
// the hold ends. The request id is kept so the new owner dedupes a retry.
type HeldOp struct {
	RequestID [16]byte
	Body      []byte
	Read      bool
}

// BarrierPair is the barrier state of one (slot, database) pair on one node.
// The zero value is a pair with no barrier at epoch 0. The caller feeds the
// pair every committed epoch, in order, including the ones that move nothing
// for it: the pair needs them to tell "a further epoch change" from "the
// change that opened this barrier".
type BarrierPair struct {
	// Hold is the side this pair is on, or HoldNone.
	Hold Hold
	// Peer is the other node of the barrier: the new owner on the sending
	// side, the old owner on the awaiting side. A HANDOFF_STOPPED or
	// HANDOFF_READY from anyone else proves nothing.
	Peer string
	// DrainTo is the sending side's appended seq at the old epoch.
	DrainTo uint64
	// AppliedPos is the applied position of the awaiting side; on the sending
	// side it latches the applied position of the HANDOFF_READY that ended the hold.
	AppliedPos cluster.Position
	// Target is the position the awaiting side must apply through (conv §10 rule 2).
	Target cluster.Position
	// Current is true once caughtUp == 1 and lastPosition == sourceAppendedPos.
	Current bool
	// StopProved is true once a HANDOFF_STOPPED for this pair at this ToEpoch
	// arrived from Peer, or Bound elapsed since Rank1At.
	StopProved bool
	// Rank1At is when this node applied the commit that made it rank 1 for the pair.
	Rank1At time.Duration
	// Bound is step_down_ms + heartbeat_ms (conv §15).
	Bound time.Duration
	// ReadySent is true once this node, as the new owner, sent HANDOFF_READY
	// for ToEpoch. It stays true until the next epoch change, so a
	// retransmitted HANDOFF_REQ is answered with READY again.
	ReadySent bool
	// ReadyRecv is true once the sending side got the HANDOFF_READY that ended
	// its hold (the drain latch).
	ReadyRecv bool
	// Held is the operations parked by the open hold, in arrival order.
	Held []HeldOp
	// Epoch is the last committed epoch this pair applied.
	Epoch uint64
	// ToEpoch is the epoch the barrier moves the pair to.
	ToEpoch uint64
	// Rank1 is true when this node is rank 1 for the pair at Epoch.
	Rank1 bool

	fromEpoch uint64 // the epoch DrainTo was appended in
	rank1Seen bool   // the commit that made this node rank 1 was applied; Rank1At and Bound are real
}

// BarrierEvent is one input of the barrier machine. Only this package defines them.
type BarrierEvent interface{ isBarrierEvent() }

// EpochCommitted is the FSM applying a committed epoch to this pair. Rank1ID
// and OldRank1ID are the pair's rank 1 at the new and the previous epoch;
// Moved says they differ. Appended is this node's appended seq for the pair
// at the previous epoch, and Bound is step_down_ms + heartbeat_ms.
type EpochCommitted struct {
	At                          time.Duration
	Epoch                       uint64
	SelfID, OldRank1ID, Rank1ID string
	Moved                       bool
	Appended                    uint64
	Bound                       time.Duration
}

// RestartHold is a node that booted into a committed state it did not observe
// committing: it may own the pair by a move it never saw, so it waits the stop
// bound from its own start and catches up before it orders. There is no peer: the
// predecessor of a move this node never saw is not knowable at boot (BUG-0743).
// At is the process's start, Bound is step_down_ms + heartbeat_ms (zero for a
// single-member cluster, which has no other owner), Epoch the committed epoch.
type RestartHold struct {
	At    time.Duration
	Bound time.Duration
	Epoch uint64
}

// HandoffReqArrived is HANDOFF_REQ. Valid is the caller's rank check: the
// receiver is rank 1 at ToEpoch and the sender was rank 1 at FromEpoch. The
// frame-epoch rule does not apply to it (conv §7).
type HandoffReqArrived struct {
	From, To  string
	FromEpoch uint64
	ToEpoch   uint64
	Slots     []uint16
	Valid     bool
}

// HandoffStoppedArrived is HANDOFF_STOPPED: From has stopped ordering and
// serving reads for the pair at ToEpoch.
type HandoffStoppedArrived struct {
	From, To string
	ToEpoch  uint64
	Slots    []uint16
	Valid    bool
}

// HandoffReadyArrived is HANDOFF_READY: From, the new owner, is current
// through AppliedPos and has proved the old owner stopped.
type HandoffReadyArrived struct {
	From, To   string
	ToEpoch    uint64
	Slots      []uint16
	AppliedPos cluster.Position
}

// CatchUpDone reports the awaiting side's catch-up progress: where it applied
// through, the target it must reach, and whether the source said it is current.
type CatchUpDone struct {
	At                 time.Duration
	AppliedPos, Target cluster.Position
	Current            bool
}

// WriteArrived is a write that reached the pair.
type WriteArrived struct {
	RequestID [16]byte
	Body      []byte
}

// ReadArrived is a read that reached the pair. PeerReachable says whether the
// other node of the barrier answers; CallerWillWait whether the caller's
// deadline lets it wait for the hold.
type ReadArrived struct {
	RequestID                     [16]byte
	PeerReachable, CallerWillWait bool
}

// DrainDeadline is handoff_drain_ms elapsing on the sending side. The machine
// never waits for the drain, so it decides nothing here.
type DrainDeadline struct{ At time.Duration }

// RetryTick is handoff_retry_ms elapsing.
type RetryTick struct{ At time.Duration }

func (EpochCommitted) isBarrierEvent()        {}
func (RestartHold) isBarrierEvent()           {}
func (HandoffReqArrived) isBarrierEvent()     {}
func (HandoffStoppedArrived) isBarrierEvent() {}
func (HandoffReadyArrived) isBarrierEvent()   {}
func (CatchUpDone) isBarrierEvent()           {}
func (WriteArrived) isBarrierEvent()          {}
func (ReadArrived) isBarrierEvent()           {}
func (DrainDeadline) isBarrierEvent()         {}
func (RetryTick) isBarrierEvent()             {}

// BarrierAction is what the caller must do after an event.
type BarrierAction struct {
	// SendHandoffReq and SendStopped send the two sending-side messages to Peer.
	SendHandoffReq bool
	SendStopped    bool
	// SendReady sends HANDOFF_READY{ToEpoch, slots, AppliedPos} to Peer.
	SendReady bool
	// ClearHold says the pair is out of its hold: the sending side stops
	// holding, the awaiting side may take its first position.
	ClearHold bool
	// ForwardHeld is the operations the hold parked, unpositioned, request ids
	// kept, to be routed to the current owner by the ordinary path (check 3).
	// After READY that is the new owner itself, on the awaiting side.
	ForwardHeld []HeldOp
	// Refuse is NOT_OWNER for a read that may not wait; zero otherwise.
	Refuse cluster.Code
}

// Apply feeds one event to the pair and returns the decision. A hold that
// holds a refusal never changes state, and every row of the table is one
// branch below:
//
//	EpochCommitted   E1 Moved, self was rank 1, no longer: sending, REQ+STOPPED
//	                 E2 Moved, self is rank 1: awaiting, no position, no read
//	                 E3 nothing for this pair: unchanged
//	                 E4 a hold is open: dropped; held ops forwarded; if self is
//	                    rank 1 now, awaiting again with StopProved cleared (E4
//	                    followed by E1 or E2 when those also hold)
//	                 E5 the commit a HANDOFF_REQ announced first: timing starts
//	                 E6 a commit at or below Epoch: ignored
//	RestartHold      H1 no hold: awaiting at Epoch with no peer, timing from At
//	                 H2 a hold is open: unchanged
//	HandoffReq       Q1 invalid: dropped   Q2 sending: send both again
//	                 Q3 awaiting: unchanged   Q4 READY already sent: send it again
//	                 Q5 ahead of the commit: awaiting   Q6 stale: ignored
//	HandoffStopped   S1 invalid: dropped   S2 awaiting, from Peer, same ToEpoch:
//	                 StopProved, READY if current   S3 anything else: ignored
//	CatchUpDone      C1 awaiting: copied, READY if both facts   C2 else: ignored
//	DrainDeadline    D1 nothing
//	HandoffReady     R1 sending, from Peer, same ToEpoch, covers DrainTo: hold
//	                 ends, held ops forwarded   R2 anything else: ignored
//	RetryTick        T1 sending: send both   T2 awaiting, Bound elapsed since
//	                 Rank1At: StopProved, READY if current   T3 else: nothing
//	WriteArrived     W1 hold open: held   W2 else: nothing
//	ReadArrived      X1 sending: held   X2 awaiting, peer unreachable and the
//	                 caller will not wait: NOT_OWNER   X3 awaiting: held
//	                 X4 else: nothing
func (b *BarrierPair) Apply(ev BarrierEvent) BarrierAction {
	switch e := ev.(type) {
	case EpochCommitted:
		return b.onEpochCommitted(e)
	case RestartHold:
		return b.onRestartHold(e)
	case HandoffReqArrived:
		return b.onHandoffReq(e)
	case HandoffStoppedArrived:
		return b.onHandoffStopped(e)
	case HandoffReadyArrived:
		return b.onHandoffReady(e)
	case CatchUpDone:
		return b.onCatchUpDone(e)
	case WriteArrived:
		return b.onWrite(e)
	case ReadArrived:
		return b.onRead(e)
	case RetryTick:
		return b.onRetryTick(e)
	case DrainDeadline:
		// D1: the drain wait is the caller's; the machine already sent
		// REQ and STOPPED at the commit and never waits past the deadline.
		return BarrierAction{}
	}
	return BarrierAction{}
}

func (b *BarrierPair) onEpochCommitted(e EpochCommitted) BarrierAction {
	var act BarrierAction
	if e.Epoch <= b.Epoch {
		return act
	}
	selfRank1 := e.SelfID == e.Rank1ID

	// E5: a HANDOFF_REQ opened this hold before the commit reached us.
	if b.Hold == HoldAwaiting && !b.rank1Seen && e.Epoch == b.ToEpoch && selfRank1 {
		b.Epoch = e.Epoch
		b.Rank1 = true
		b.rank1Seen = true
		b.Rank1At = e.At
		b.Bound = e.Bound
		// The announcing REQ was checked before this node could see the ranks;
		// a proof from anyone but the real predecessor is no proof.
		if predecessor := b.predecessorOf(e); predecessor != b.Peer {
			b.Peer = predecessor
			b.StopProved = false
		}
		b.maybeReady(&act)
		return act
	}

	holdWasOpen := b.Hold != HoldNone
	if holdWasOpen {
		act.ForwardHeld = b.endHold()
	} else {
		// A further epoch ends the retransmission window of a READY already sent.
		b.ReadySent = false
	}
	from := b.Epoch
	b.Epoch = e.Epoch
	b.Rank1 = selfRank1

	switch {
	case e.Moved && e.SelfID == e.OldRank1ID && !selfRank1:
		b.enterSending(e, from)
		act.SendHandoffReq = true
		act.SendStopped = true
	case (e.Moved || holdWasOpen) && selfRank1:
		b.enterAwaiting(e)
	default:
		act.ClearHold = holdWasOpen
	}
	return act
}

// endHold drops the hold, its proof and its latch, and hands back the held
// operations.
func (b *BarrierPair) endHold() []HeldOp {
	held := b.Held
	b.Held = nil
	b.clearBarrier()
	return held
}

// clearBarrier forgets everything a barrier gathered. Catch-up evidence and a
// proof belong to the barrier they were gathered for: a new barrier starts
// from none of it.
func (b *BarrierPair) clearBarrier() {
	b.Hold = HoldNone
	b.Peer = ""
	b.DrainTo = 0
	b.fromEpoch = 0
	b.AppliedPos = cluster.Position{}
	b.Target = cluster.Position{}
	b.Current = false
	b.StopProved = false
	b.ReadySent = false
	b.ReadyRecv = false
	b.rank1Seen = false
	b.Rank1At, b.Bound = 0, 0
}

func (b *BarrierPair) enterSending(e EpochCommitted, from uint64) {
	b.clearBarrier()
	b.Hold = HoldSending
	b.Peer = e.Rank1ID
	b.DrainTo = e.Appended
	b.fromEpoch = from
	b.ToEpoch = e.Epoch
}

func (b *BarrierPair) enterAwaiting(e EpochCommitted) {
	b.clearBarrier()
	b.Hold = HoldAwaiting
	b.Peer = b.predecessorOf(e)
	b.ToEpoch = e.Epoch
	b.Rank1At = e.At
	b.Bound = e.Bound
	b.rank1Seen = true
}

// onRestartHold opens the peer-less awaiting hold of a restarted owner. Epoch and
// ToEpoch are the same committed epoch, so a stale HANDOFF_REQ (ToEpoch not above
// Epoch) is ignored while a genuine REQ for a later epoch still supersedes this hold.
func (b *BarrierPair) onRestartHold(e RestartHold) BarrierAction {
	if b.Hold != HoldNone {
		return BarrierAction{}
	}
	b.clearBarrier()
	b.Hold = HoldAwaiting
	b.Epoch, b.ToEpoch = e.Epoch, e.Epoch
	b.Rank1 = true
	b.rank1Seen = true
	b.Rank1At = e.At
	b.Bound = e.Bound
	return BarrierAction{}
}

// predecessorOf names the node whose HANDOFF_STOPPED can prove anything for the
// commit e, or "" when there is none: a further epoch that left this node rank
// 1 has no other node to hear from, so only the lease bound can prove it.
func (b *BarrierPair) predecessorOf(e EpochCommitted) string {
	if e.OldRank1ID == e.SelfID {
		return ""
	}
	return e.OldRank1ID
}

func (b *BarrierPair) onHandoffReq(e HandoffReqArrived) BarrierAction {
	var act BarrierAction
	if !e.Valid || e.FromEpoch >= e.ToEpoch {
		return act
	}
	switch {
	case b.Hold == HoldSending:
		if e.ToEpoch == b.ToEpoch {
			act.SendHandoffReq = true
			act.SendStopped = true
		}
	case b.Hold == HoldAwaiting:
		// Idempotent: the hold is already open for this barrier.
	case b.ReadySent && e.ToEpoch == b.ToEpoch:
		// The sender is still asking, so it did not get our READY.
		act.SendReady = true
	case e.ToEpoch > b.Epoch:
		// Ahead of the commit: barrier messages are exempt from the frame-epoch rule.
		b.clearBarrier()
		b.Hold = HoldAwaiting
		b.Peer = e.From
		b.ToEpoch = e.ToEpoch
		b.Rank1 = true
	}
	return act
}

func (b *BarrierPair) onHandoffStopped(e HandoffStoppedArrived) BarrierAction {
	var act BarrierAction
	if !e.Valid || b.Hold != HoldAwaiting || e.ToEpoch != b.ToEpoch {
		return act
	}
	if b.Peer == "" || e.From != b.Peer {
		return act
	}
	b.StopProved = true
	b.maybeReady(&act)
	return act
}

func (b *BarrierPair) onCatchUpDone(e CatchUpDone) BarrierAction {
	var act BarrierAction
	if b.Hold != HoldAwaiting {
		return act
	}
	b.AppliedPos = e.AppliedPos
	b.Target = e.Target
	b.Current = e.Current
	b.maybeReady(&act)
	return act
}

// maybeReady sends READY once, and only when both facts hold: the pair is
// current through its target, and the old owner has stopped. Sending it ends
// the awaiting hold, so the held operations go back to the ordinary path.
func (b *BarrierPair) maybeReady(act *BarrierAction) {
	if b.Hold != HoldAwaiting || b.ReadySent || !b.rank1Seen {
		return
	}
	if !b.StopProved || !b.Current || b.AppliedPos.Less(b.Target) {
		return
	}
	b.ReadySent = true
	b.Hold = HoldNone
	act.SendReady = true
	act.ClearHold = true
	act.ForwardHeld = b.takeHeld()
}

func (b *BarrierPair) takeHeld() []HeldOp {
	held := b.Held
	b.Held = nil
	return held
}

func (b *BarrierPair) onHandoffReady(e HandoffReadyArrived) BarrierAction {
	var act BarrierAction
	if b.Hold != HoldSending || e.ToEpoch != b.ToEpoch || e.From != b.Peer {
		return act
	}
	if b.DrainTo != 0 && e.AppliedPos.Less(cluster.Position{Epoch: b.fromEpoch, Seq: b.DrainTo}) {
		return act
	}
	b.Hold = HoldNone
	b.ReadyRecv = true
	b.AppliedPos = e.AppliedPos
	act.ClearHold = true
	act.ForwardHeld = b.takeHeld()
	return act
}

func (b *BarrierPair) onRetryTick(e RetryTick) BarrierAction {
	var act BarrierAction
	switch b.Hold {
	case HoldSending:
		act.SendHandoffReq = true
		act.SendStopped = true
	case HoldAwaiting:
		if !b.StopProved && b.rank1Seen && e.At-b.Rank1At >= b.Bound {
			b.StopProved = true
			b.maybeReady(&act)
		}
	}
	return act
}

func (b *BarrierPair) onWrite(e WriteArrived) BarrierAction {
	if b.Hold != HoldNone {
		b.Held = append(b.Held, HeldOp{RequestID: e.RequestID, Body: e.Body})
	}
	return BarrierAction{}
}

func (b *BarrierPair) onRead(e ReadArrived) BarrierAction {
	var act BarrierAction
	switch {
	case b.Hold == HoldSending:
		b.Held = append(b.Held, HeldOp{RequestID: e.RequestID, Read: true})
	case b.Hold == HoldAwaiting && !e.PeerReachable && !e.CallerWillWait:
		act.Refuse = cluster.CodeNotOwner
	case b.Hold == HoldAwaiting:
		b.Held = append(b.Held, HeldOp{RequestID: e.RequestID, Read: true})
	}
	return act
}
