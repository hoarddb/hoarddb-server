// Package machine holds the entry-ring's pure state machines: values with an
// Apply method, no clock reads, no goroutines, no I/O. A machine is driven by
// events that carry the time the caller read.
package machine

import (
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// Clock is the caller's boot clock (cluster/bootclock.Clock is the real one).
// A machine never holds one: the caller reads it and passes the reading in the
// event.
type Clock interface{ BootNow() time.Duration }

// LeaseConfig is the timing and cluster shape the lease rule needs (conventions
// section 15).
type LeaseConfig struct {
	// Heartbeat and StepDown are cluster.heartbeat_ms and cluster.step_down_ms.
	Heartbeat, StepDown time.Duration
	// Members is the cluster's member count; 1 means no lease at all.
	Members int
	// WallClock is true where a wall clock exists for the cross-check.
	WallClock bool
}

// tickGapHeartbeats is the observed-progress upper bound in heartbeats: a tick more than this many
// heartbeats after the previous one voids the lease (conventions section 15, amended by
// lease-renewer-cadence-spec.md R2). The renewer paces each round from the previous round's start,
// so a round of up to about three heartbeats is followed by a tick inside the bound. Since the
// leader's own lease counts from its round's start (R1), no fence depends on this bound; it stays
// near the quorum bound (also three heartbeats) as defence in depth.
const tickGapHeartbeats = 3

// TickGapLimit is the longest gap between two ticks that keeps the lease.
func (c LeaseConfig) TickGapLimit() time.Duration { return tickGapHeartbeats * c.Heartbeat }

// LapseCause is why a lease stopped being valid: one cause per condition of conventions
// section 15 that ends validity. It is a record for the operator and the logs; no rule reads it.
type LapseCause uint8

// The causes. LapseNone means no lapse is open.
const (
	LapseNone LapseCause = iota
	// LapseNeverHeld is a node that has not yet been granted a lease.
	LapseNeverHeld
	// LapseTickGap is a tick more than three heartbeats after the previous one.
	LapseTickGap
	// LapseClockStopped is a tick whose boot reading did not advance.
	LapseClockStopped
	// LapseWallClock is a tick whose wall delta disagrees with the boot delta.
	LapseWallClock
	// LapseExpired is Until reached with no renewal.
	LapseExpired
	// LapseNoProgress is a tick that is not after the last renewal.
	LapseNoProgress
	// LapseRoundTooSlow is a grant, or the leader's own round, whose round trip was step_down or more.
	LapseRoundTooSlow
	// LapseEpoch is a later epoch seen in a grant or at a serve check.
	LapseEpoch
	// LapseQuorumFailed is the leader's own quorum round failing.
	LapseQuorumFailed
)

// String is the cause as the lapse log line spells it.
func (c LapseCause) String() string {
	switch c {
	case LapseNeverHeld:
		return "never_held"
	case LapseTickGap:
		return "tick_gap"
	case LapseClockStopped:
		return "clock_stopped"
	case LapseWallClock:
		return "wall_clock"
	case LapseExpired:
		return "expired"
	case LapseNoProgress:
		return "no_progress"
	case LapseRoundTooSlow:
		return "round_too_slow"
	case LapseEpoch:
		return "epoch"
	case LapseQuorumFailed:
		return "quorum_failed"
	}
	return "none"
}

// LeaseState is one node's lease.
type LeaseState struct {
	Held      bool
	Until     time.Duration // when the last accepted grant expires
	RenewedAt time.Duration // bootNow of the last accepted grant; 0 = none
	SentAt    time.Duration // bootNow of the last LEASE_REQ
	TickAt    time.Duration // bootNow of the previous Tick; 0 = none yet
	TickWall  time.Duration // wall reading at that tick; 0 when none
	Nonce     uint64        // nonce of the last LEASE_REQ
	LastNonce uint64        // nonce of the last accepted grant; a grant must be newer
	Term      uint64        // this node's Raft term, set by the caller
	Epoch     uint64        // the epoch of the last accepted grant
	Behind    bool          // a higher-epoch grant said the cluster moved
	// LapsedAt and Cause record the lapse in progress: the reading at which the machine ended
	// validity (Until for an expiry) and the first condition that did. They are written only by the
	// machine, only while no lapse is open, and cleared by the renewal that ends the lapse.
	LapsedAt time.Duration
	Cause    LapseCause
}

// LeaseEvent is one input to the lease rule.
type LeaseEvent interface{ isLeaseEvent() }

// RequestSent records that a LEASE_REQ was written at At.
type RequestSent struct {
	At    time.Duration
	Nonce uint64
}

// GrantReceived is a LEASE_GRANT arriving at At (boot clock) and Wall (wall
// clock), echoing a request nonce and stamped with the grantor's epoch, term
// and the round trip it measured. CommittedEpoch is this node's own committed
// epoch at the moment the grant is processed.
type GrantReceived struct {
	At, Wall                           time.Duration
	Nonce, Epoch, CommittedEpoch, Term uint64
	RTT                                time.Duration
}

// OwnQuorum is the leader's own quorum round (VerifyLeader) finishing at At. RTT is the round's
// length from its start (the tick before it), so the lease it renews ends step_down after that
// start, as a follower's does (lease-renewer-cadence-spec.md R1).
type OwnQuorum struct {
	At             time.Duration
	CommittedEpoch uint64 // the leader's committed epoch; the epoch its own lease names
	Term           uint64
	OK             bool
	RTT            time.Duration
}

// ServeCheck is the re-check made in the step that hands an answer to the
// transport.
type ServeCheck struct {
	At             time.Duration
	CommittedEpoch uint64
}

// Tick is the once-per-heartbeat renewer tick.
type Tick struct{ At, Wall time.Duration }

func (RequestSent) isLeaseEvent()   {}
func (GrantReceived) isLeaseEvent() {}
func (OwnQuorum) isLeaseEvent()     {}
func (ServeCheck) isLeaseEvent()    {}
func (Tick) isLeaseEvent()          {}

// LeaseDecision is what the caller does after an event.
type LeaseDecision struct {
	Valid       bool
	SendRequest bool
	StepDown    bool
	Behind      bool
	Refusal     cluster.Code
	// Cause and LapsedAt describe the lapse a decision that is not valid belongs to. The decision
	// that accepts a renewal after a lapse carries that lapse's Cause and LapsedAt with Ended set.
	Cause    LapseCause
	LapsedAt time.Duration
	Ended    bool
}

// Apply feeds one event to the lease rule and returns what the caller does
// next. It is conventions section 15 as a table, and a few readings the table
// leaves open, all of them on the side of not serving:
//
//   - StepDown is true on any event that takes Held from true to false, and
//     Refusal is LEASE_LAPSED on any decision that is not Valid, whichever row
//     it came from.
//   - A row the table calls "ignored", or "none", returns the lease as it
//     stands at the event's At (held and At < Until) and changes nothing.
//   - Every Tick, passing or failing, moves TickAt and TickWall forward to its
//     own reading, except a tick that did not advance the clock, which keeps
//     the high-water mark. Without that a node that was paused once would
//     measure every later tick against the old reading and never recover.
//   - The first Tick has no previous reading, so the three clock rules pass
//     vacuously; the expiry and renewal-progress rows still apply.
//   - A held lease whose Tick is not after the last renewal (At <= RenewedAt)
//     has seen no progress since it and is voided.
//   - SendRequest is decided on every Tick, valid or not, so a node with no
//     lease asks for one.
//   - OwnQuorum is an accepted grant at its own RTT only after the same checks
//     a grant passes for a lower term, a negative RTT, no progress since
//     RenewedAt and an RTT of StepDown or more; the epoch it names is its own
//     CommittedEpoch.
//   - A grant is judged against the node's own committed epoch, which the
//     event carries: Epoch == CommittedEpoch is a normal grant, Epoch above it
//     stops serving (Behind), Epoch below it is a stale grantor and changes
//     nothing. The caller therefore writes no field of LeaseState.
//   - An accepted grant records its nonce in LastNonce; a grant whose nonce is
//     not newer than LastNonce, or whose RTT is negative, changes nothing.
func (s *LeaseState) Apply(ev LeaseEvent, cfg LeaseConfig) LeaseDecision {
	if cfg.Members == 1 {
		return LeaseDecision{Valid: true}
	}
	switch e := ev.(type) {
	case Tick:
		return s.tick(e, cfg)
	case RequestSent:
		s.SentAt, s.Nonce = e.At, e.Nonce
		return s.standing(e.At)
	case GrantReceived:
		return s.grant(e, cfg)
	case OwnQuorum:
		return s.ownQuorum(e, cfg)
	case ServeCheck:
		return s.serveCheck(e)
	}
	return LeaseDecision{Refusal: cluster.CodeLeaseLapsed}
}

// decide builds the decision for a state change: wasHeld is Held before it.
func (s *LeaseState) decide(wasHeld, valid bool) LeaseDecision {
	d := LeaseDecision{Valid: valid, StepDown: wasHeld && !s.Held}
	if !valid {
		d.Refusal = cluster.CodeLeaseLapsed
		d.Cause, d.LapsedAt = s.lapseRecord()
	}
	return d
}

// lapseRecord is the open lapse; a lease that was never granted has no time to report.
func (s *LeaseState) lapseRecord() (LapseCause, time.Duration) {
	if s.Cause == LapseNone {
		return LapseNeverHeld, 0
	}
	return s.Cause, s.LapsedAt
}

// lapse opens a lapse at at unless one is open already: the first cause wins. A lease that was
// not held has no lapse to open (it was never granted, or its lapse is open already).
func (s *LeaseState) lapse(wasHeld bool, cause LapseCause, at time.Duration) {
	if wasHeld && s.Cause == LapseNone {
		s.Cause, s.LapsedAt = cause, at
	}
}

// noteExpiry opens an expiry lapse, stamped at Until rather than at the reading that noticed it,
// when the lease is held and at is not before Until.
func (s *LeaseState) noteExpiry(at time.Duration) {
	if s.Held && at >= s.Until {
		s.lapse(true, LapseExpired, s.Until)
	}
}

// accepted is the decision of a renewal that was accepted: it ends the open lapse, if any, and
// says so. wasHeld is Held before the renewal; a lease not held at all was in a lapse.
func (s *LeaseState) accepted(wasHeld bool, at, left time.Duration, epoch, term uint64) LeaseDecision {
	s.noteExpiry(at)
	d := LeaseDecision{Valid: true}
	if !wasHeld || s.Cause != LapseNone {
		d.Cause, d.LapsedAt = s.lapseRecord()
		d.Ended = true
	}
	s.accept(at, left, epoch, term)
	return d
}

// standing is the decision for an event that changes nothing: the lease as it
// stands at the given time.
func (s *LeaseState) standing(at time.Duration) LeaseDecision {
	s.noteExpiry(at)
	return s.decide(s.Held, s.Held && at < s.Until)
}

func (s *LeaseState) tick(e Tick, cfg LeaseConfig) LeaseDecision {
	wasHeld := s.Held
	s.noteExpiry(e.At)
	clocksOK := true
	if s.TickAt != 0 {
		elapsed := e.At - s.TickAt
		if elapsed <= 0 {
			// The clock did not advance: TickAt stays at its high-water mark.
			s.lapse(wasHeld, LapseClockStopped, e.At)
			s.Held = false
			return s.withRequest(s.decide(wasHeld, false), e.At, cfg)
		}
		wallOff := (e.Wall - s.TickWall) - elapsed
		if wallOff < 0 {
			wallOff = -wallOff
		}
		switch {
		case elapsed > cfg.TickGapLimit():
			clocksOK = false
			s.lapse(wasHeld, LapseTickGap, e.At)
		case cfg.WallClock && wallOff > cfg.Heartbeat:
			clocksOK = false
			s.lapse(wasHeld, LapseWallClock, e.At)
		}
	}
	s.TickAt, s.TickWall = e.At, e.Wall
	valid := clocksOK && s.Held && e.At < s.Until && e.At > s.RenewedAt
	if clocksOK && wasHeld && !valid {
		s.lapse(wasHeld, LapseNoProgress, e.At) // expiry was noted first, so this is a tick at or before the renewal
	}
	s.Held = valid
	return s.withRequest(s.decide(wasHeld, valid), e.At, cfg)
}

// withRequest adds the once-per-heartbeat request to a tick's decision.
func (s *LeaseState) withRequest(d LeaseDecision, at time.Duration, cfg LeaseConfig) LeaseDecision {
	d.SendRequest = at-s.SentAt >= cfg.Heartbeat
	return d
}

func (s *LeaseState) grant(e GrantReceived, cfg LeaseConfig) LeaseDecision {
	wasHeld := s.Held
	s.noteExpiry(e.At)
	switch {
	case e.Nonce != s.Nonce, e.Nonce <= s.LastNonce, e.Term < s.Term, e.RTT < 0, e.Epoch < e.CommittedEpoch:
		return s.standing(e.At)
	case e.Epoch > e.CommittedEpoch:
		s.lapse(wasHeld, LapseEpoch, e.At)
		s.Held, s.Behind = false, true
		d := s.decide(wasHeld, false)
		d.Behind = true
		return d
	case s.RenewedAt != 0 && e.At <= s.RenewedAt:
		return s.standing(e.At)
	case e.RTT >= cfg.StepDown:
		s.lapse(wasHeld, LapseRoundTooSlow, e.At)
		s.Held, s.Until = false, e.At
		return s.decide(wasHeld, false)
	}
	d := s.accepted(wasHeld, e.At, cfg.StepDown-e.RTT, e.Epoch, e.Term)
	s.LastNonce = e.Nonce
	return d
}

func (s *LeaseState) ownQuorum(e OwnQuorum, cfg LeaseConfig) LeaseDecision {
	wasHeld := s.Held
	s.noteExpiry(e.At)
	switch {
	case !e.OK:
		s.lapse(wasHeld, LapseQuorumFailed, e.At)
		s.Held = false
		return s.decide(wasHeld, false)
	case e.Term < s.Term, e.RTT < 0, s.RenewedAt != 0 && e.At <= s.RenewedAt:
		return s.standing(e.At)
	case e.RTT >= cfg.StepDown:
		s.lapse(wasHeld, LapseRoundTooSlow, e.At)
		s.Held, s.Until = false, e.At
		return s.decide(wasHeld, false)
	}
	return s.accepted(wasHeld, e.At, cfg.StepDown-e.RTT, e.CommittedEpoch, e.Term)
}

func (s *LeaseState) serveCheck(e ServeCheck) LeaseDecision {
	wasHeld := s.Held
	s.noteExpiry(e.At)
	if e.CommittedEpoch != s.Epoch {
		s.lapse(wasHeld, LapseEpoch, e.At)
		s.Held = false
		return s.decide(wasHeld, false)
	}
	return s.standing(e.At)
}

// accept records an accepted renewal that lasts left from at.
func (s *LeaseState) accept(at, left time.Duration, epoch, term uint64) {
	s.Held, s.Until, s.RenewedAt = true, at+left, at
	s.Cause, s.LapsedAt = LapseNone, 0
	s.Epoch, s.Term, s.Behind = epoch, term, false
}
