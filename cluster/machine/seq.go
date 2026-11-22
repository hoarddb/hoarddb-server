// Package machine holds the pure state machines of the entry ring: values with
// an Apply method, no clock, no I/O and no goroutine. Callers pass every input
// in an event, so each decision table is testable row by row.
package machine

import (
	"bytes"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// PairSeq is the sequence state of one (slot, database) pair on one node:
// where the node has appended and applied, what it is waiting for, and the
// flags that gate assignment (write §2, §3; conv §10 rule 5).
//
// Owner is an input the caller keeps equal to "rank 1 at Epoch": no event
// demotes an owner, so the caller clears Owner before sending the
// EpochAdvanced that moves the pair away from this node.
type PairSeq struct {
	Epoch       uint64
	AppendedSeq uint64           // highest seq appended at Epoch; 0 = none
	PrevEnd     cluster.Position // the boundary the next (Epoch,1) carries (conv §10 rule 5)
	AppliedPos  cluster.Position
	AppliedID   [16]byte         // the request id of the record at AppliedPos
	Target      cluster.Position // highest position any reachable ready holder reports
	Owner       bool             // rank 1 at Epoch
	Hold        bool             // sending, awaiting, stopProved not yet true, or replay not at the tail
	Replayed    bool
	LeaseOK     bool
	Members     int
	Held        []byte // unpositioned held operations, opaque to the machine
}

// Assignment is what an accepted OrderRequest returns: the position to append
// and the boundary the record must carry. PrevEnd is meaningful only when
// Pos.Seq == 1.
type Assignment struct{ Pos, PrevEnd cluster.Position }

// SeqEvent is one input to PairSeq.Apply.
type SeqEvent interface{ isSeqEvent() }

// OwnershipAcquired says this node became rank 1 and gives the position it had
// to apply through (Target) and the cut its seed ended at (zero when it did not
// seed).
type OwnershipAcquired struct {
	Epoch, Appended uint64
	Target, Cut     cluster.Position
}

// EpochAdvanced says the committed epoch moved.
type EpochAdvanced struct{ Epoch uint64 }

// RecordApplied offers one record to this node: a live push, a catch-up record
// or a drained seed buffer. PrevEnd and PrevEndID are the opener's boundary
// (meaningful when Pos.Seq == 1); PrevEndID is zero when the caller does not
// know the request id held at PrevEnd (SPEC-GAP G16).
type RecordApplied struct {
	Pos       cluster.Position
	RequestID [16]byte
	PrevEnd   cluster.Position
	PrevEndID [16]byte
}

// RecordRejected reports a record the fence or the decoder refused before it
// reached the sequence rules. It changes nothing.
type RecordRejected struct {
	Got     cluster.Position
	From    string
	Because uint8
}

// OrderRequest asks for the next position for one write.
type OrderRequest struct{ RequestID [16]byte }

// HoldChanged copies the gate flags the caller computed elsewhere.
type HoldChanged struct {
	Hold, Replayed, LeaseOK bool
	Members                 int
}

// TargetRaised reports a higher position a reachable ready holder holds.
type TargetRaised struct{ Target cluster.Position }

func (OwnershipAcquired) isSeqEvent() {}
func (EpochAdvanced) isSeqEvent()     {}
func (RecordApplied) isSeqEvent()     {}
func (RecordRejected) isSeqEvent()    {}
func (OrderRequest) isSeqEvent()      {}
func (HoldChanged) isSeqEvent()       {}
func (TargetRaised) isSeqEvent()      {}

// SeqOutcome is what one event asks the caller to do.
type SeqOutcome struct {
	Assign       *Assignment       // nil means no position consumed
	Refusal      cluster.Code      // NOT_OWNER, LEASE_LAPSED, or SEQ_GAP when this copy is behind the record
	AskCatchUp   *cluster.Position // from AppliedPos+1
	ResetAndSeed bool              // a tail nobody owns: reset, seed, SEED_RESTART reason 4
}

// recordRow is one row of the RecordApplied decision table. Rows are mutually
// exclusive, so which row fires never depends on their order; a test counts
// the matches for every input class and requires exactly one.
type recordRow struct {
	name  string
	match func(s *PairSeq, ev RecordApplied) bool
	run   func(s *PairSeq, ev RecordApplied) SeqOutcome
}

var noID [16]byte

// catchUpFrom is the position after p inside p's own epoch, the first record a
// copy that holds p still needs.
func catchUpFrom(p cluster.Position) *cluster.Position {
	next := cluster.Position{Epoch: p.Epoch, Seq: p.Seq + 1}
	return &next
}

// boundaryHolds reports whether the opener's prevEnd names the record this copy
// holds: the position must be equal, and the request id too when the caller
// knows the one held at prevEnd (SPEC-GAP G16: zero means unknown, so numeric
// equality decides).
func boundaryHolds(s *PairSeq, ev RecordApplied) bool {
	return ev.PrevEnd == s.AppliedPos && (ev.PrevEndID == noID || ev.PrevEndID == s.AppliedID)
}

// candidate is every record the sequence rules judge: it names a record
// (seq >= 1), is not the applied position and is not from a later epoch than
// this machine's, so the fence need not have refused it.
func candidate(s *PairSeq, ev RecordApplied) bool {
	return ev.Pos.Seq >= 1 && ev.Pos != s.AppliedPos && ev.Pos.Epoch <= s.Epoch
}

// openerAfter is a candidate (e,1) whose epoch starts after the applied
// position, so the boundary comparison decides it. An opener at or before the
// applied position was already passed.
func openerAfter(s *PairSeq, ev RecordApplied) bool {
	return candidate(s, ev) && ev.Pos.Seq == 1 && s.AppliedPos.Less(ev.Pos)
}

// openerBefore is a candidate (e,1) at or behind the applied position.
func openerBefore(s *PairSeq, ev RecordApplied) bool {
	return candidate(s, ev) && ev.Pos.Seq == 1 && ev.Pos.Less(s.AppliedPos)
}

// follower is a candidate that is not an opener: (e,n) with n > 1.
func follower(s *PairSeq, ev RecordApplied) bool {
	return candidate(s, ev) && ev.Pos.Seq > 1
}

func none(*PairSeq, RecordApplied) SeqOutcome { return SeqOutcome{} }

func accept(s *PairSeq, ev RecordApplied) SeqOutcome {
	s.AppliedPos, s.AppliedID, s.PrevEnd = ev.Pos, ev.RequestID, ev.Pos
	return SeqOutcome{}
}

func gapFromApplied(s *PairSeq, _ RecordApplied) SeqOutcome {
	return SeqOutcome{Refusal: cluster.CodeSeqGap, AskCatchUp: catchUpFrom(s.AppliedPos)}
}

// recordRows is the whole RecordApplied table (build doc §4.3, as amended by
// the coordinator's rulings A and B). Epoch relations use the record's own
// epoch, so a catch-up record of an earlier epoch is judged by the same rules
// as a live one: one apply mechanism (write §6).
var recordRows = []recordRow{
	{"record/seq-zero", // seq 0 names no record, because the first seq of an epoch is 1
		func(_ *PairSeq, ev RecordApplied) bool { return ev.Pos.Seq == 0 }, none},
	{"record/idempotent", // the applied position again
		func(s *PairSeq, ev RecordApplied) bool { return ev.Pos.Seq >= 1 && ev.Pos == s.AppliedPos }, none},
	{"record/future-epoch", // this machine's epoch is stale; the caller re-reads committed state
		func(s *PairSeq, ev RecordApplied) bool {
			return ev.Pos.Seq >= 1 && ev.Pos != s.AppliedPos && ev.Pos.Epoch > s.Epoch
		}, none},

	// The accepting row and the two boundary rows, for an opener after the applied position.
	{"record/opener-accept", // the opener is contiguous with what this node holds
		func(s *PairSeq, ev RecordApplied) bool { return openerAfter(s, ev) && boundaryHolds(s, ev) }, accept},
	{"record/opener-behind", // catch up to prevEnd and then accept; apply nothing now
		func(s *PairSeq, ev RecordApplied) bool { return openerAfter(s, ev) && s.AppliedPos.Less(ev.PrevEnd) },
		func(s *PairSeq, _ RecordApplied) SeqOutcome { return SeqOutcome{AskCatchUp: catchUpFrom(s.AppliedPos)} }},
	{"record/opener-ahead", // ahead, or a different record at the boundary: a tail nobody owns
		func(s *PairSeq, ev RecordApplied) bool {
			return openerAfter(s, ev) && (ev.PrevEnd.Less(s.AppliedPos) || (ev.PrevEnd == s.AppliedPos && !boundaryHolds(s, ev)))
		},
		func(*PairSeq, RecordApplied) SeqOutcome { return SeqOutcome{ResetAndSeed: true} }},

	// An opener at or behind the applied position.
	{"record/opener-duplicate", // already passed: seen again inside its epoch, or an older epoch's
		func(s *PairSeq, ev RecordApplied) bool {
			return openerBefore(s, ev) && (ev.Pos.Epoch != s.Epoch || s.AppliedPos.Epoch <= s.Epoch)
		}, none},
	{"record/opener-copy-ahead-of-epoch", // applied beyond the epoch of the sender: reset, as "ahead"
		func(s *PairSeq, ev RecordApplied) bool {
			return openerBefore(s, ev) && ev.Pos.Epoch == s.Epoch && s.AppliedPos.Epoch > s.Epoch
		},
		func(*PairSeq, RecordApplied) SeqOutcome { return SeqOutcome{ResetAndSeed: true} }},

	// (e,n) with n > 1, judged against the epoch of the applied position.
	{"record/continuity", // from (e,n-1) only
		func(s *PairSeq, ev RecordApplied) bool {
			return follower(s, ev) && ev.Pos.Epoch == s.AppliedPos.Epoch && ev.Pos.Seq == s.AppliedPos.Seq+1
		}, accept},
	{"record/gap", // a record that skips one is never accepted
		func(s *PairSeq, ev RecordApplied) bool {
			return follower(s, ev) && ev.Pos.Epoch == s.AppliedPos.Epoch && ev.Pos.Seq > s.AppliedPos.Seq+1
		}, gapFromApplied},
	{"record/older-in-epoch", // already applied
		func(s *PairSeq, ev RecordApplied) bool {
			return follower(s, ev) && ev.Pos.Epoch == s.AppliedPos.Epoch && ev.Pos.Seq < s.AppliedPos.Seq
		}, none},
	{"record/missed-opener", // the copy never applied (e,1), so (e,n) is a gap whatever n is
		func(s *PairSeq, ev RecordApplied) bool { return follower(s, ev) && ev.Pos.Epoch > s.AppliedPos.Epoch },
		gapFromApplied},
	{"record/older-epoch", // an epoch the copy has already left
		func(s *PairSeq, ev RecordApplied) bool {
			return follower(s, ev) && ev.Pos.Epoch < s.AppliedPos.Epoch && s.AppliedPos.Epoch <= s.Epoch
		}, none},
	{"record/copy-ahead-of-epoch", // applied beyond this machine's epoch: refuse and report, never apply
		func(s *PairSeq, ev RecordApplied) bool {
			return follower(s, ev) && ev.Pos.Epoch < s.AppliedPos.Epoch && s.AppliedPos.Epoch > s.Epoch
		}, gapFromApplied},
}

// matchingRecordRows names every row whose condition holds for ev.
func matchingRecordRows(s *PairSeq, ev RecordApplied) []string {
	var names []string
	for _, row := range recordRows {
		if row.match(s, ev) {
			names = append(names, row.name)
		}
	}
	return names
}

func (s *PairSeq) applyRecord(ev RecordApplied) SeqOutcome {
	for _, row := range recordRows {
		if row.match(s, ev) {
			return row.run(s, ev)
		}
	}
	// Unreachable: TestEveryRecordClassHasExactlyOneRow proves every input has a row.
	return SeqOutcome{}
}

func (s *PairSeq) applyOrder() SeqOutcome {
	switch {
	case !s.Owner:
		return SeqOutcome{Refusal: cluster.CodeNotOwner}
	case s.Hold || !s.Replayed || s.AppliedPos.Less(s.Target):
		return SeqOutcome{} // the caller holds the write unpositioned
	case s.Members > 1 && !s.LeaseOK:
		return SeqOutcome{Refusal: cluster.CodeLeaseLapsed}
	}
	s.AppendedSeq++
	return SeqOutcome{Assign: &Assignment{Pos: cluster.Position{Epoch: s.Epoch, Seq: s.AppendedSeq}, PrevEnd: s.PrevEnd}}
}

func (s *PairSeq) applyEpoch(epoch uint64) {
	if epoch <= s.Epoch {
		return
	}
	// The next opener's boundary is the last position that exists for this
	// node: what it appended if it ordered, else what it applied, so a quiet
	// epoch reuses the last position instead of 0:0 (conv §10 rule 5).
	if s.Owner && s.AppendedSeq > 0 {
		s.PrevEnd = cluster.Position{Epoch: s.Epoch, Seq: s.AppendedSeq}
	} else {
		s.PrevEnd = s.AppliedPos
	}
	s.Epoch, s.AppendedSeq, s.Target = epoch, 0, cluster.Position{}
	if !s.Owner {
		s.Hold = true // ownership moved; M4 owns the hold
	}
}

func (s *PairSeq) applyOwnership(ev OwnershipAcquired) {
	if ev.Epoch < s.Epoch {
		return // a stale event never lowers the epoch
	}
	s.Owner, s.Epoch, s.AppendedSeq, s.Target = true, ev.Epoch, ev.Appended, ev.Target
	if !ev.Cut.Zero() {
		// A completed seed ended at the cut: that is this node's own applied
		// position now, and the boundary its copies compare with.
		s.AppliedPos, s.AppliedID, s.PrevEnd = ev.Cut, noID, ev.Cut
		return
	}
	s.PrevEnd = s.AppliedPos
}

// Apply feeds one event to the machine. The bool reports whether any field
// changed. A refusal never changes state, so a refused order consumes no
// position (write §2).
func (s *PairSeq) Apply(ev SeqEvent) (SeqOutcome, bool) {
	before := *s
	var out SeqOutcome
	switch ev := ev.(type) {
	case OrderRequest:
		out = s.applyOrder()
	case EpochAdvanced:
		s.applyEpoch(ev.Epoch)
	case OwnershipAcquired:
		s.applyOwnership(ev)
	case RecordApplied:
		out = s.applyRecord(ev)
	case HoldChanged:
		s.Hold, s.Replayed, s.LeaseOK, s.Members = ev.Hold, ev.Replayed, ev.LeaseOK, ev.Members
	case TargetRaised:
		if s.Target.Less(ev.Target) {
			s.Target = ev.Target
		}
	case RecordRejected:
		// The fence or decoder refused the record before the sequence rules saw it.
	}
	return out, !before.sameAs(s)
}

func (s PairSeq) sameAs(o *PairSeq) bool {
	return s.Epoch == o.Epoch && s.AppendedSeq == o.AppendedSeq && s.PrevEnd == o.PrevEnd &&
		s.AppliedPos == o.AppliedPos && s.AppliedID == o.AppliedID && s.Target == o.Target &&
		s.Owner == o.Owner && s.Hold == o.Hold && s.Replayed == o.Replayed && s.LeaseOK == o.LeaseOK &&
		s.Members == o.Members && bytes.Equal(s.Held, o.Held)
}
