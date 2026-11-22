// Package machine holds the entry-ring's pure state machines: values and
// functions with no clock, no goroutine, no socket and no lock. The caller
// reads the world and passes facts in; a machine returns a decision.
package machine

import "github.com/hoarddb/hoarddb-server/cluster"

// Concern is a write concern level (conventions §17). The zero value is not a
// level: a write whose level was never set can never be called met.
type Concern uint8

// The three write concern levels. Majority is the default when the database's
// replication factor is above 1, else One.
const (
	ConcernOne      Concern = 1
	ConcernMajority Concern = 2
	ConcernAll      Concern = 3
)

// Outcome is the write_outcome of an acknowledged write (conventions §17). The
// zero value means "no write_outcome": a refusal carries none.
type Outcome uint8

// The three write_outcome values.
const (
	// OutcomeApplied: durable and applied, and the write concern was met.
	OutcomeApplied Outcome = iota + 1
	// OutcomeAppliedDegraded: durable on the owner and on fewer copies than
	// the concern needs. The answer carries copies_confirmed.
	OutcomeAppliedDegraded
	// OutcomeUnknown: durable here, but this node can no longer vouch that it
	// reached enough copies.
	OutcomeUnknown
)

// String returns the wire word of conventions §17, or "" for no outcome.
func (o Outcome) String() string {
	switch o {
	case OutcomeApplied:
		return "applied"
	case OutcomeAppliedDegraded:
		return "applied_degraded"
	case OutcomeUnknown:
		return "unknown"
	default:
		return ""
	}
}

// WriteState is what a node knows about one write at the instant it answers.
// The caller fills it in; Answer reads nothing else.
type WriteState struct {
	// Durable is true once the append returned (write path §3.2). False means
	// the append refused and the write consumed no position.
	Durable bool
	// OwnerNow is true when this node is still rank 1 at the committed epoch.
	OwnerNow bool
	// DrainedOrAcked is true when the position is covered by the appliedPos of
	// a HANDOFF_READY this node received, or by an ack from the copy that is
	// the owner now (write path §9).
	DrainedOrAcked bool
	// LevelMet is the concern waiter's verdict when the wait ended. Answer does
	// not believe it alone: the count below has to back it.
	LevelMet bool
	// WCTimeout is true when the wait ended at replication.wc_timeout_ms (or the
	// client's deadline) short of the level. A level reached later changes
	// nothing, so it overrides LevelMet.
	WCTimeout bool
	// LeaseValidAtAnswer is the lease read immediately before the answer is
	// handed to the transport (conventions §15), not the one read at admission.
	LeaseValidAtAnswer bool

	// Level is the concern the write asked for.
	Level Concern
	// CopiesConfirmed counts the owner, so it is 1 for an acknowledged write
	// and never 0.
	CopiesConfirmed uint8
	// RF is the database's configured replication factor, never the number of
	// copies currently in sync.
	RF uint8
}

// MajorityOf is floor(rf/2)+1 of the configured replication factor. It is the
// only place a majority is computed: from the configuration, never from how
// many copies happen to be in sync.
func MajorityOf(rf uint8) uint8 {
	return rf/2 + 1
}

// copiesNeeded is the number of confirmed copies, owner included, the level
// requires. It is 0 for a level that is not one of the three or an RF of 0, and
// callers treat 0 as "can never be met".
func copiesNeeded(level Concern, rf uint8) uint8 {
	if rf == 0 {
		return 0
	}
	switch level {
	case ConcernOne:
		return 1
	case ConcernMajority:
		return MajorityOf(rf)
	case ConcernAll:
		return rf
	default:
		return 0
	}
}

// levelReallyMet is true only when the wait saw the level met, did not time
// out, and the confirmed count reaches the target for the configured RF. A
// concern miss is never reported as met.
func levelReallyMet(state WriteState) bool {
	if !state.LevelMet || state.WCTimeout {
		return false
	}
	needed := copiesNeeded(state.Level, state.RF)
	return needed >= 1 && state.CopiesConfirmed >= needed
}

// Answer is the write-outcome answer table (write path §9, conventions §15 and
// §17). It returns the outcome to send, or a non-zero Code for a refusal, in
// which case there is no write_outcome.
//
//	A  durable, owner now, level met, lease valid          applied
//	B  durable, owner now, level missed, lease valid       applied_degraded
//	C  durable, not owner, drained or acked, level met     applied
//	D  durable, not owner, drained or acked, level missed  applied_degraded
//	E  durable, not owner, not drained, not acked          unknown
//	E' durable, owner now, lease lapsed at answer time     unknown
//	F  not durable                                         UNAVAILABLE, no outcome
//
// An epoch change inside the critical section is row A/B or C/D, never E: the
// write was sequenced and applied, so the READY covers it.
func Answer(state WriteState) (Outcome, cluster.Code) {
	if !state.Durable {
		return 0, cluster.CodeUnavailable
	}

	var cannotVouch bool
	if state.OwnerNow {
		// The lease is re-read in the same step as the answer: a node that
		// lost it while the concern waited may already have been replaced.
		cannotVouch = !state.LeaseValidAtAnswer
	} else {
		cannotVouch = !state.DrainedOrAcked
	}
	if cannotVouch {
		return OutcomeUnknown, 0
	}

	if levelReallyMet(state) {
		return OutcomeApplied, 0
	}
	return OutcomeAppliedDegraded, 0
}
