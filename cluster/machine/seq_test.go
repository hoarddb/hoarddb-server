package machine

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

func pos(epoch, seq uint64) cluster.Position { return cluster.Position{Epoch: epoch, Seq: seq} }

// idOf is the request id every test gives the record at p, so a test can name
// "the id held at prevEnd" without a lookup.
func idOf(p cluster.Position) [16]byte {
	var id [16]byte
	id[0] = byte(p.Epoch)
	id[1] = byte(p.Epoch >> 8)
	id[8] = byte(p.Seq)
	id[9] = byte(p.Seq >> 8)
	return id
}

func rid(b byte) [16]byte { return [16]byte{0: b, 15: 0xEE} }

func with(s PairSeq, edit func(*PairSeq)) PairSeq {
	edit(&s)
	return s
}

func record(p, prevEnd cluster.Position) RecordApplied {
	return RecordApplied{Pos: p, RequestID: idOf(p), PrevEnd: prevEnd}
}

// seqCase is one row of the decision table: a start state, one event, and the
// exact outcome and end state the table names. The end state is compared whole,
// so a row that also moves a field the table does not name fails.
type seqCase struct {
	name    string
	row     string
	start   PairSeq
	ev      SeqEvent
	want    SeqOutcome
	end     PairSeq
	changed bool
}

// Non-record row names. Record rows are named by recordRows in seq.go.
var otherRows = []string{
	"order/not-owner", "order/hold", "order/not-replayed", "order/behind-target", "order/lease-lapsed",
	"order/single-member-no-lease", "order/assign",
	"epoch/same", "epoch/owner-bump", "epoch/copy-bump", "epoch/stale",
	"owner/acquire", "owner/acquire-seeded", "owner/acquire-stale-epoch",
	"hold/changed", "hold/unchanged", "target/raised", "target/not-raised", "rejected/inert",
}

func seqCases() []seqCase {
	ready := PairSeq{Epoch: 5, Owner: true, Replayed: true, LeaseOK: true, Members: 3}
	copyAt := func(a cluster.Position) PairSeq {
		return PairSeq{Epoch: 5, AppliedPos: a, AppliedID: idOf(a), PrevEnd: a}
	}
	accepted := func(s PairSeq, ev RecordApplied) PairSeq {
		return with(s, func(s *PairSeq) { s.AppliedPos, s.AppliedID, s.PrevEnd = ev.Pos, ev.RequestID, ev.Pos })
	}
	var cases []seqCase
	add := func(c seqCase) { cases = append(cases, c) }
	rec := func(row, name string, start PairSeq, ev RecordApplied, want SeqOutcome, end *PairSeq) {
		c := seqCase{name: name, row: row, start: start, ev: ev, want: want, end: start}
		if end != nil {
			c.end, c.changed = *end, true
		}
		add(c)
	}
	acc := func(s PairSeq, ev RecordApplied) *PairSeq { e := accepted(s, ev); return &e }
	gap := func(a cluster.Position) SeqOutcome {
		return SeqOutcome{Refusal: cluster.CodeSeqGap, AskCatchUp: catchUpFrom(a)}
	}
	reset := SeqOutcome{ResetAndSeed: true}

	// OrderRequest.
	add(seqCase{name: "not the owner", row: "order/not-owner", start: with(ready, func(s *PairSeq) { s.Owner = false }),
		ev: OrderRequest{}, want: SeqOutcome{Refusal: cluster.CodeNotOwner}, end: with(ready, func(s *PairSeq) { s.Owner = false })})
	add(seqCase{name: "not the owner wins over a hold", row: "order/not-owner",
		start: with(ready, func(s *PairSeq) { s.Owner, s.Hold = false, true }), ev: OrderRequest{},
		want: SeqOutcome{Refusal: cluster.CodeNotOwner}, end: with(ready, func(s *PairSeq) { s.Owner, s.Hold = false, true })})
	add(seqCase{name: "held", row: "order/hold", start: with(ready, func(s *PairSeq) { s.Hold = true }), ev: OrderRequest{},
		end: with(ready, func(s *PairSeq) { s.Hold = true })})
	add(seqCase{name: "hold wins over a lapsed lease", row: "order/hold",
		start: with(ready, func(s *PairSeq) { s.Hold, s.LeaseOK = true, false }), ev: OrderRequest{},
		end: with(ready, func(s *PairSeq) { s.Hold, s.LeaseOK = true, false })})
	add(seqCase{name: "replay not at the tail", row: "order/not-replayed", start: with(ready, func(s *PairSeq) { s.Replayed = false }),
		ev: OrderRequest{}, end: with(ready, func(s *PairSeq) { s.Replayed = false })})
	add(seqCase{name: "applied below target", row: "order/behind-target",
		start: with(ready, func(s *PairSeq) { s.AppliedPos, s.Target = pos(4, 9), pos(4, 10) }), ev: OrderRequest{},
		end: with(ready, func(s *PairSeq) { s.AppliedPos, s.Target = pos(4, 9), pos(4, 10) })})
	add(seqCase{name: "lease lapsed with peers", row: "order/lease-lapsed", start: with(ready, func(s *PairSeq) { s.LeaseOK = false }),
		ev: OrderRequest{}, want: SeqOutcome{Refusal: cluster.CodeLeaseLapsed}, end: with(ready, func(s *PairSeq) { s.LeaseOK = false })})
	single := with(ready, func(s *PairSeq) { s.LeaseOK, s.Members, s.PrevEnd = false, 1, pos(4, 9) })
	add(seqCase{name: "one member holds no lease", row: "order/single-member-no-lease", start: single, ev: OrderRequest{},
		want: SeqOutcome{Assign: &Assignment{Pos: pos(5, 1), PrevEnd: pos(4, 9)}},
		end:  with(single, func(s *PairSeq) { s.AppendedSeq = 1 }), changed: true})
	owned := with(ready, func(s *PairSeq) { s.AppendedSeq, s.PrevEnd, s.AppliedPos = 7, pos(4, 9), pos(5, 7) })
	add(seqCase{name: "assigns the next seq", row: "order/assign", start: owned, ev: OrderRequest{},
		want: SeqOutcome{Assign: &Assignment{Pos: pos(5, 8), PrevEnd: pos(4, 9)}},
		end:  with(owned, func(s *PairSeq) { s.AppendedSeq = 8 }), changed: true})

	// EpochAdvanced.
	add(seqCase{name: "same epoch", row: "epoch/same", start: ready, ev: EpochAdvanced{Epoch: 5}, end: ready})
	ownerAt := with(ready, func(s *PairSeq) { s.AppendedSeq, s.AppliedPos, s.Target = 7, pos(5, 7), pos(5, 7) })
	add(seqCase{name: "still-owner bump", row: "epoch/owner-bump", start: ownerAt, ev: EpochAdvanced{Epoch: 6},
		end: with(ownerAt, func(s *PairSeq) { s.Epoch, s.AppendedSeq, s.PrevEnd, s.Target = 6, 0, pos(5, 7), cluster.Position{} }), changed: true})
	quiet := with(ready, func(s *PairSeq) { s.AppliedPos, s.PrevEnd = pos(4, 40), pos(4, 40) })
	add(seqCase{name: "quiet owner bump keeps the last position", row: "epoch/owner-bump", start: quiet, ev: EpochAdvanced{Epoch: 7},
		end: with(quiet, func(s *PairSeq) { s.Epoch = 7 }), changed: true})
	copyState := with(copyAt(pos(5, 7)), func(s *PairSeq) { s.Target = pos(5, 9) })
	add(seqCase{name: "copy bump enters the hold", row: "epoch/copy-bump", start: copyState, ev: EpochAdvanced{Epoch: 6},
		end: with(copyState, func(s *PairSeq) { s.Epoch, s.Hold, s.Target = 6, true, cluster.Position{} }), changed: true})
	add(seqCase{name: "stale epoch event", row: "epoch/stale", start: ready, ev: EpochAdvanced{Epoch: 4}, end: ready})

	// OwnershipAcquired.
	promoted := with(copyAt(pos(4, 100)), func(s *PairSeq) { s.Epoch = 6 })
	add(seqCase{name: "promotion without a seed", row: "owner/acquire", start: promoted,
		ev:  OwnershipAcquired{Epoch: 6, Appended: 0, Target: pos(4, 100)},
		end: with(promoted, func(s *PairSeq) { s.Owner, s.Target = true, pos(4, 100) }), changed: true})
	add(seqCase{name: "promotion by a seed adopts the cut", row: "owner/acquire-seeded", start: promoted,
		ev: OwnershipAcquired{Epoch: 6, Target: pos(4, 98), Cut: pos(4, 100)},
		end: with(promoted, func(s *PairSeq) {
			s.Owner, s.Target, s.AppliedPos, s.AppliedID, s.PrevEnd = true, pos(4, 98), pos(4, 100), [16]byte{}, pos(4, 100)
		}), changed: true})
	add(seqCase{name: "a stale ownership event", row: "owner/acquire-stale-epoch", start: promoted,
		ev: OwnershipAcquired{Epoch: 5, Appended: 3, Target: pos(4, 100)}, end: promoted})

	// HoldChanged and TargetRaised and RecordRejected.
	add(seqCase{name: "flags change", row: "hold/changed", start: ready, ev: HoldChanged{Hold: true, Replayed: false, LeaseOK: false, Members: 2},
		end: with(ready, func(s *PairSeq) { s.Hold, s.Replayed, s.LeaseOK, s.Members = true, false, false, 2 }), changed: true})
	add(seqCase{name: "flags repeat", row: "hold/unchanged", start: ready, ev: HoldChanged{Replayed: true, LeaseOK: true, Members: 3}, end: ready})
	add(seqCase{name: "target rises", row: "target/raised", start: ready, ev: TargetRaised{Target: pos(5, 9)},
		end: with(ready, func(s *PairSeq) { s.Target = pos(5, 9) }), changed: true})
	high := with(ready, func(s *PairSeq) { s.Target = pos(5, 9) })
	add(seqCase{name: "target never falls", row: "target/not-raised", start: high, ev: TargetRaised{Target: pos(5, 8)}, end: high})
	add(seqCase{name: "rejection is inert", row: "rejected/inert", start: ready, ev: RecordRejected{Got: pos(4, 2), From: "node-b", Because: 1}, end: ready})

	// RecordApplied: one or more cases for every row of recordRows.
	a := copyAt(pos(5, 3))
	rec("record/seq-zero", "seq 0 names no record", a, record(pos(5, 0), cluster.Position{}), SeqOutcome{}, nil)
	rec("record/idempotent", "the applied position again", a, record(pos(5, 3), cluster.Position{}), SeqOutcome{}, nil)
	rec("record/future-epoch", "a record from a later epoch", a, record(pos(6, 1), pos(5, 3)), SeqOutcome{}, nil)

	m95 := copyAt(pos(4, 95))
	m95.Epoch = 5
	open := record(pos(5, 1), pos(4, 95))
	rec("record/opener-accept", "opener at the boundary", m95, open, SeqOutcome{}, acc(m95, open))
	openID := open
	openID.PrevEndID = idOf(pos(4, 95))
	rec("record/opener-accept", "opener, same id at the boundary", m95, openID, SeqOutcome{}, acc(m95, openID))
	fresh := PairSeq{Epoch: 1}
	first := record(pos(1, 1), cluster.Position{})
	rec("record/opener-accept", "the first record of an empty pair", fresh, first, SeqOutcome{}, acc(fresh, first))
	late := with(copyAt(pos(4, 100)), func(s *PairSeq) { s.Epoch = 6 })
	oldOpener := record(pos(5, 1), pos(4, 100))
	rec("record/opener-accept", "an earlier epoch's opener, by catch-up", late, oldOpener, SeqOutcome{}, acc(late, oldOpener))

	behind := record(pos(5, 1), pos(4, 100))
	rec("record/opener-behind", "opener past the copy", m95, behind, SeqOutcome{AskCatchUp: catchUpFrom(pos(4, 95))}, nil)
	rec("record/opener-behind", "opener over an empty copy", PairSeq{Epoch: 5}, behind, SeqOutcome{AskCatchUp: catchUpFrom(cluster.Position{})}, nil)

	ahead := record(pos(5, 1), pos(4, 95))
	rec("record/opener-ahead", "copy past the boundary", with(copyAt(pos(4, 100)), func(s *PairSeq) { s.Epoch = 5 }), ahead, reset, nil)
	otherID := open
	otherID.PrevEndID = rid(7)
	rec("record/opener-ahead", "a different record at the boundary", m95, otherID, reset, nil)
	rec("record/opener-ahead", "opener claims no earlier record", m95, record(pos(5, 1), cluster.Position{}), reset, nil)

	rec("record/opener-duplicate", "an opener seen again inside its epoch", a, open, SeqOutcome{}, nil)
	rec("record/opener-duplicate", "an older epoch's opener, already passed",
		with(copyAt(pos(6, 2)), func(s *PairSeq) { s.Epoch = 6 }), record(pos(4, 1), pos(3, 9)), SeqOutcome{}, nil)
	// (e,1) against seq 0 of its own epoch is judged by prevEnd, never around
	// it: prevEnd 4:95 is below (5,0), so the copy is ahead of it.
	rec("record/opener-ahead", "an opener against seq 0 of its own epoch", PairSeq{Epoch: 5, AppliedPos: pos(5, 0)}, open, reset, nil)

	rec("record/opener-copy-ahead-of-epoch", "applied beyond the opener's epoch and this machine's",
		with(copyAt(pos(6, 2)), func(s *PairSeq) { s.Epoch = 5 }), ahead, reset, nil)

	next := record(pos(5, 4), cluster.Position{})
	rec("record/continuity", "the next seq", a, next, SeqOutcome{}, acc(a, next))
	oldNext := record(pos(5, 96), cluster.Position{})
	oldA := with(copyAt(pos(5, 95)), func(s *PairSeq) { s.Epoch = 6 })
	rec("record/continuity", "the next seq of an older epoch, by catch-up", oldA, oldNext, SeqOutcome{}, acc(oldA, oldNext))

	rec("record/gap", "skips one", a, record(pos(5, 5), cluster.Position{}), gap(pos(5, 3)), nil)
	rec("record/older-in-epoch", "an older seq of this epoch", a, record(pos(5, 2), cluster.Position{}), SeqOutcome{}, nil)
	rec("record/missed-opener", "a mid-epoch record over an earlier epoch's tail", m95, record(pos(5, 3), cluster.Position{}), gap(pos(4, 95)), nil)
	rec("record/missed-opener", "a mid-epoch record over an empty copy", PairSeq{Epoch: 5}, record(pos(5, 2), cluster.Position{}), gap(cluster.Position{}), nil)
	rec("record/older-epoch", "a record older than the applied epoch",
		with(copyAt(pos(5, 10)), func(s *PairSeq) { s.Epoch = 6 }), record(pos(4, 50), cluster.Position{}), SeqOutcome{}, nil)
	rec("record/copy-ahead-of-epoch", "applied beyond this machine's epoch, non-opener",
		with(copyAt(pos(6, 2)), func(s *PairSeq) { s.Epoch = 5 }), record(pos(5, 4), cluster.Position{}), gap(pos(6, 2)), nil)

	return cases
}

// TestEveryDecisionTableRow runs one case per row of the decision table and
// compares the whole outcome and the whole end state.
// Guard: any row's change or outcome edited in seq.go fails its own case.
func TestEveryDecisionTableRow(t *testing.T) {
	cases := seqCases()
	covered := map[string]bool{}
	for _, c := range cases {
		covered[c.row] = true
		t.Run(c.row+"/"+c.name, func(t *testing.T) {
			s := c.start
			before := s
			if rec, ok := c.ev.(RecordApplied); ok {
				got := matchingRecordRows(&s, rec)
				if len(got) != 1 || got[0] != c.row {
					t.Fatalf("matching rows = %v, want exactly [%s]", got, c.row)
				}
			}
			out, changed := s.Apply(c.ev)
			if !reflect.DeepEqual(out, c.want) {
				t.Errorf("outcome = %+v, want %+v", describe(out), describe(c.want))
			}
			if changed != c.changed {
				t.Errorf("changed = %v, want %v", changed, c.changed)
			}
			if !reflect.DeepEqual(s, c.end) {
				t.Errorf("end state = %+v, want %+v", s, c.end)
			}
			if out.Refusal != 0 && !reflect.DeepEqual(s, before) {
				t.Errorf("a refusal (%v) changed state: %+v -> %+v", out.Refusal, before, s)
			}
		})
	}
	var want []string
	want = append(want, otherRows...)
	for _, r := range recordRows {
		want = append(want, r.name)
	}
	if len(recordRows) == 0 {
		t.Errorf("recordRows is empty: the RecordApplied table is not implemented")
	}
	sort.Strings(want)
	for _, name := range want {
		if !covered[name] {
			t.Errorf("decision-table row %q has no case", name)
		}
	}
	for row := range covered {
		known := false
		for _, name := range want {
			known = known || name == row
		}
		if !known {
			t.Errorf("case names unknown row %q", row)
		}
	}
}

func describe(o SeqOutcome) string {
	s := fmt.Sprintf("{refusal %v reset %v", o.Refusal, o.ResetAndSeed)
	if o.Assign != nil {
		s += fmt.Sprintf(" assign %v prevEnd %v", o.Assign.Pos, o.Assign.PrevEnd)
	}
	if o.AskCatchUp != nil {
		s += fmt.Sprintf(" catchUp %v", *o.AskCatchUp)
	}
	return s + "}"
}

// TestEveryRecordClassHasExactlyOneRow enumerates the cross product of the
// record's epoch against Epoch, its epoch against AppliedPos.Epoch, and its seq
// against AppliedPos.Seq, with every prevEnd relation, and requires exactly one
// matching row for each input.
// Guard: drop the AppliedPos.Epoch guard from the duplicate row and its overlap
// with the boundary rows is counted here as two rows.
func TestEveryRecordClassHasExactlyOneRow(t *testing.T) {
	const machineEpoch = 5
	checked := 0
	for _, recordEpoch := range []uint64{3, 5, 7} {
		for _, appliedEpoch := range []uint64{2, 3, 4, 5, 6, 7, 8} {
			for _, appliedSeq := range []uint64{0, 1, 5, 6} {
				for _, recordSeq := range []uint64{0, 1, 2, 3, 5, 6, 7, 8} {
					applied := pos(appliedEpoch, appliedSeq)
					recordPos := pos(recordEpoch, recordSeq)
					prevEnds := []cluster.Position{
						{}, applied, pos(applied.Epoch, applied.Seq+3), pos(applied.Epoch, applied.Seq+1),
						pos(1, 1), pos(recordEpoch-1, 40),
					}
					if applied.Seq > 0 {
						prevEnds = append(prevEnds, pos(applied.Epoch, applied.Seq-1))
					}
					for _, prevEnd := range prevEnds {
						for _, prevID := range [][16]byte{{}, idOf(applied), rid(9)} {
							s := PairSeq{Epoch: machineEpoch, AppliedPos: applied, AppliedID: idOf(applied)}
							ev := RecordApplied{Pos: recordPos, RequestID: idOf(recordPos), PrevEnd: prevEnd, PrevEndID: prevID}
							got := matchingRecordRows(&s, ev)
							checked++
							if len(got) != 1 {
								t.Fatalf("Epoch %d applied %v record %v prevEnd %v prevID %x: %d rows match %v, want exactly 1",
									machineEpoch, applied, recordPos, prevEnd, prevID[:2], len(got), got)
							}
							before := s
							s.Apply(ev)
							if s.AppliedPos.Less(before.AppliedPos) {
								t.Fatalf("applied position moved backwards: %v -> %v on %+v", before.AppliedPos, s.AppliedPos, ev)
							}
						}
					}
				}
			}
		}
	}
	if checked < 1000 {
		t.Fatalf("only %d combinations were checked", checked)
	}
}

// TestARefusalConsumesNoPosition: a refused order leaves AppendedSeq alone, so
// the next accepted order is the very next seq.
// Guard: increment AppendedSeq before the refusal checks.
func TestARefusalConsumesNoPosition(t *testing.T) {
	s := PairSeq{Epoch: 5, Owner: true, Replayed: true, LeaseOK: true, Members: 3, AppendedSeq: 4}
	for _, refusing := range []func(*PairSeq){
		func(s *PairSeq) { s.Owner = false },
		func(s *PairSeq) { s.Hold = true },
		func(s *PairSeq) { s.Replayed = false },
		func(s *PairSeq) { s.Target = pos(5, 1) },
		func(s *PairSeq) { s.LeaseOK = false },
	} {
		blocked := with(s, refusing)
		before := blocked
		out, changed := blocked.Apply(OrderRequest{})
		if out.Assign != nil || changed || !reflect.DeepEqual(blocked, before) {
			t.Fatalf("a refused order consumed a position or changed state: %+v changed=%v state=%+v", out, changed, blocked)
		}
		blocked = before
		blocked.Owner, blocked.Hold, blocked.Replayed, blocked.LeaseOK, blocked.Target = true, false, true, true, cluster.Position{}
		out, _ = blocked.Apply(OrderRequest{})
		if out.Assign == nil || out.Assign.Pos != pos(5, 5) {
			t.Fatalf("after a refusal the next order = %+v, want 5:5", out.Assign)
		}
	}
}

// TestFirstPositionAfterAnEpochChangeIsOne: a still-owner's bump restarts seq
// at 1 and the opener carries the last position of the old epoch.
// Guard: keep AppendedSeq across the bump.
func TestFirstPositionAfterAnEpochChangeIsOne(t *testing.T) {
	s := PairSeq{Epoch: 41, Owner: true, Replayed: true, LeaseOK: true, Members: 1}
	var last cluster.Position
	for i := 0; i < 3; i++ {
		out, _ := s.Apply(OrderRequest{})
		if out.Assign == nil {
			t.Fatalf("setup: order %d assigned nothing: %s", i+1, describe(out))
		}
		last = out.Assign.Pos
		s.Apply(record(last, out.Assign.PrevEnd))
	}
	if last != pos(41, 3) {
		t.Fatalf("setup: last = %v", last)
	}
	s.Apply(EpochAdvanced{Epoch: 42})
	out, _ := s.Apply(OrderRequest{})
	if out.Assign == nil || out.Assign.Pos != pos(42, 1) || out.Assign.PrevEnd != pos(41, 3) {
		t.Fatalf("first order after the bump = %+v, want 42:1 carrying prevEnd 41:3", out.Assign)
	}
}

// TestACopyRefusesAGapAndCatchesUp: a skipped seq is refused SEQ_GAP and never
// applied, and the copy then takes the missing records in order.
// Guard: apply the record out of order.
func TestACopyRefusesAGapAndCatchesUp(t *testing.T) {
	s := PairSeq{Epoch: 5, AppliedPos: pos(5, 3), AppliedID: idOf(pos(5, 3))}
	out, changed := s.Apply(record(pos(5, 5), cluster.Position{}))
	if out.Refusal != cluster.CodeSeqGap || out.AskCatchUp == nil || *out.AskCatchUp != pos(5, 4) || changed || s.AppliedPos != pos(5, 3) {
		t.Fatalf("gap: out=%s changed=%v applied=%v", describe(out), changed, s.AppliedPos)
	}
	for _, seq := range []uint64{4, 5} {
		out, changed = s.Apply(record(pos(5, seq), cluster.Position{}))
		if out.Refusal != 0 || !changed || s.AppliedPos != pos(5, seq) {
			t.Fatalf("seq %d: out=%s changed=%v applied=%v", seq, describe(out), changed, s.AppliedPos)
		}
	}
}

// TestPromotionNeverAdoptsAPeerPosition: OwnershipAcquired records the target a
// peer reported but never makes it the applied position; ordering resumes only
// when this node's own applies reach it.
// Guard: set AppliedPos = Target on OwnershipAcquired.
func TestPromotionNeverAdoptsAPeerPosition(t *testing.T) {
	s := PairSeq{Epoch: 6, AppliedPos: pos(5, 98), AppliedID: idOf(pos(5, 98)), Replayed: true, LeaseOK: true, Members: 3}
	s.Apply(OwnershipAcquired{Epoch: 6, Target: pos(5, 100)})
	if s.AppliedPos != pos(5, 98) {
		t.Fatalf("promotion adopted a peer position: applied %v", s.AppliedPos)
	}
	out, _ := s.Apply(OrderRequest{})
	if out.Assign != nil || out.Refusal != 0 {
		t.Fatalf("ordered before applying through the target: %s", describe(out))
	}
	s.Apply(record(pos(5, 99), cluster.Position{}))
	s.Apply(record(pos(5, 100), cluster.Position{}))
	out, _ = s.Apply(OrderRequest{})
	if out.Assign == nil || out.Assign.Pos != pos(6, 1) || out.Assign.PrevEnd != pos(5, 100) {
		t.Fatalf("after applying through the target: %+v, want 6:1 carrying 5:100", out.Assign)
	}
}

// TestEpochOpenerRequiresPrevEnd: (e,1) is accepted only when prevEnd equals
// the copy's applied position.
// Guard: accept (e,1) from any (e-1,*).
func TestEpochOpenerRequiresPrevEnd(t *testing.T) {
	start := PairSeq{Epoch: 5, AppliedPos: pos(4, 95), AppliedID: idOf(pos(4, 95))}
	for _, tc := range []struct {
		prevEnd           cluster.Position
		accepted, catchUp bool
		reset             bool
	}{
		{prevEnd: pos(4, 95), accepted: true},
		{prevEnd: pos(4, 100), catchUp: true},
		{prevEnd: pos(4, 90), reset: true},
		{prevEnd: cluster.Position{}, reset: true},
	} {
		s := start
		out, _ := s.Apply(record(pos(5, 1), tc.prevEnd))
		gotAccepted := s.AppliedPos == pos(5, 1)
		if gotAccepted != tc.accepted || (out.AskCatchUp != nil) != tc.catchUp || out.ResetAndSeed != tc.reset {
			t.Errorf("prevEnd %v: accepted=%v out=%s", tc.prevEnd, gotAccepted, describe(out))
		}
		if !tc.accepted && s.AppliedPos != start.AppliedPos {
			t.Errorf("prevEnd %v moved the applied position to %v", tc.prevEnd, s.AppliedPos)
		}
	}
}

// TestEpochOpenerAcceptingRowComesBeforeTheStaleReset: a copy at (e-1,95) has
// 1 <= 95, which a naive stale test would treat as old and reset a tail that
// is exactly right. The opener at its own boundary must be accepted.
// Guard: let the duplicate/stale row claim the opener before the boundary rows.
func TestEpochOpenerAcceptingRowComesBeforeTheStaleReset(t *testing.T) {
	s := PairSeq{Epoch: 5, AppliedPos: pos(4, 95), AppliedID: idOf(pos(4, 95))}
	out, changed := s.Apply(record(pos(5, 1), pos(4, 95)))
	if out.ResetAndSeed || !changed || s.AppliedPos != pos(5, 1) || s.AppliedID != idOf(pos(5, 1)) || s.PrevEnd != pos(5, 1) {
		t.Fatalf("opener at its boundary: out=%s state=%+v", describe(out), s)
	}
	if s.AppendedSeq != 0 {
		t.Fatalf("a copy never appends: AppendedSeq = %d", s.AppendedSeq)
	}
}

// TestBehindCopyCatchesUpToPrevEndThenAccepts is the whole behind path: the
// opener asks for catch-up, the old epoch's records arrive after the machine's
// epoch moved, and the retransmitted opener is then accepted.
// Guard: accept the opener while AppliedPos < PrevEnd, or ignore a record of an
// earlier epoch.
func TestBehindCopyCatchesUpToPrevEndThenAccepts(t *testing.T) {
	s := PairSeq{Epoch: 5, AppliedPos: pos(4, 95), AppliedID: idOf(pos(4, 95))}
	opener := record(pos(5, 1), pos(4, 100))
	out, changed := s.Apply(opener)
	if out.AskCatchUp == nil || *out.AskCatchUp != pos(4, 96) || out.Refusal != 0 || changed || out.ResetAndSeed {
		t.Fatalf("behind: out=%s changed=%v", describe(out), changed)
	}
	for seq := uint64(96); seq <= 100; seq++ {
		out, changed = s.Apply(record(pos(4, seq), cluster.Position{}))
		if out.Refusal != 0 || !changed || s.AppliedPos != pos(4, seq) {
			t.Fatalf("catch-up record 4:%d: out=%s changed=%v applied=%v", seq, describe(out), changed, s.AppliedPos)
		}
	}
	out, changed = s.Apply(opener)
	if out.ResetAndSeed || out.AskCatchUp != nil || !changed || s.AppliedPos != pos(5, 1) {
		t.Fatalf("retransmitted opener: out=%s applied=%v", describe(out), s.AppliedPos)
	}
}

// TestAheadCopyResetsAndSeeds: a copy past the boundary, or holding a different
// record at it, holds a tail nobody owns and resets instead of merging it.
// Guard: merge the tail (accept) instead of resetting.
func TestAheadCopyResetsAndSeeds(t *testing.T) {
	ahead := PairSeq{Epoch: 5, AppliedPos: pos(4, 100), AppliedID: idOf(pos(4, 100))}
	s := ahead
	out, changed := s.Apply(record(pos(5, 1), pos(4, 95)))
	if !out.ResetAndSeed || changed || !reflect.DeepEqual(s, ahead) {
		t.Fatalf("ahead: out=%s changed=%v state=%+v", describe(out), changed, s)
	}
	diverged := PairSeq{Epoch: 5, AppliedPos: pos(4, 95), AppliedID: idOf(pos(4, 95))}
	s = diverged
	ev := record(pos(5, 1), pos(4, 95))
	ev.PrevEndID = rid(3)
	out, changed = s.Apply(ev)
	if !out.ResetAndSeed || changed || !reflect.DeepEqual(s, diverged) {
		t.Fatalf("different record at the boundary: out=%s changed=%v", describe(out), changed)
	}
}

// TestQuietEpochReusesTheLastPosition: an epoch that ordered nothing leaves the
// next opener carrying the last position that exists, never 0:0.
// Guard: stamp 0:0 after an epoch that ordered nothing.
func TestQuietEpochReusesTheLastPosition(t *testing.T) {
	s := PairSeq{Epoch: 6, AppliedPos: pos(5, 40), AppliedID: idOf(pos(5, 40)), PrevEnd: pos(5, 40),
		Owner: true, Replayed: true, LeaseOK: true, Members: 3}
	s.Apply(EpochAdvanced{Epoch: 7})
	s.Apply(EpochAdvanced{Epoch: 8})
	out, _ := s.Apply(OrderRequest{})
	if out.Assign == nil || out.Assign.Pos != pos(8, 1) || out.Assign.PrevEnd != pos(5, 40) {
		t.Fatalf("opener after two quiet epochs = %+v, want 8:1 carrying 5:40", out.Assign)
	}
}

// TestStillOwnerKeepsOwnershipAcrossABump: a bump that leaves the node rank 1
// keeps Owner and the hold clear; one that moves ownership holds.
// Guard: clear Owner on every EpochAdvanced.
func TestStillOwnerKeepsOwnershipAcrossABump(t *testing.T) {
	owner := PairSeq{Epoch: 5, Owner: true, AppendedSeq: 9, Replayed: true, LeaseOK: true, Members: 3}
	owner.Apply(EpochAdvanced{Epoch: 6})
	if !owner.Owner || owner.Hold || owner.AppendedSeq != 0 || owner.Epoch != 6 {
		t.Fatalf("still-owner after the bump: %+v", owner)
	}
	moved := PairSeq{Epoch: 5, AppendedSeq: 9}
	moved.Apply(EpochAdvanced{Epoch: 6})
	if moved.Owner || !moved.Hold || moved.AppendedSeq != 0 {
		t.Fatalf("copy after the bump: %+v", moved)
	}
}

// TestACopyInsideAnEpochNeverResetsOnARetransmittedOpener: ruling B. (e,1) seen
// again after (e,5) is a duplicate, not an ahead copy.
// Guard: drop the AppliedPos.Epoch < Epoch guard from the ahead row.
func TestACopyInsideAnEpochNeverResetsOnARetransmittedOpener(t *testing.T) {
	s := PairSeq{Epoch: 5, AppliedPos: pos(5, 5), AppliedID: idOf(pos(5, 5))}
	out, changed := s.Apply(record(pos(5, 1), pos(4, 95)))
	if out.ResetAndSeed || out.AskCatchUp != nil || out.Refusal != 0 || changed {
		t.Fatalf("retransmitted opener: out=%s changed=%v", describe(out), changed)
	}
}

// TestACopyThatMissedTheOpenerRefusesAGap: (e,n>1) over a tail of an earlier
// epoch is a gap, even though n <= the old epoch's seq.
// Guard: treat n <= AppliedPos.Seq as already applied without checking the epoch.
func TestACopyThatMissedTheOpenerRefusesAGap(t *testing.T) {
	s := PairSeq{Epoch: 5, AppliedPos: pos(4, 95), AppliedID: idOf(pos(4, 95))}
	for _, seq := range []uint64{2, 3, 95, 96, 200} {
		out, changed := s.Apply(record(pos(5, seq), cluster.Position{}))
		if out.Refusal != cluster.CodeSeqGap || out.AskCatchUp == nil || *out.AskCatchUp != pos(4, 96) || changed {
			t.Fatalf("5:%d: out=%s changed=%v", seq, describe(out), changed)
		}
	}
}

// TestAnOpenerAgainstSeqZeroGoesThroughPrevEnd: (e,1) against AppliedPos (e,0)
// is compared with prevEnd like any opener.
// Guard: take (e,1) through the Seq == AppliedPos.Seq+1 continuity row.
func TestAnOpenerAgainstSeqZeroGoesThroughPrevEnd(t *testing.T) {
	s := PairSeq{Epoch: 5, AppliedPos: pos(5, 0)}
	if rows := matchingRecordRows(&s, record(pos(5, 1), pos(4, 95))); len(rows) != 1 || rows[0] != "record/opener-ahead" {
		t.Fatalf("(e,1) against seq 0 matches %v, want only the boundary row", rows)
	}
	out, changed := s.Apply(record(pos(5, 1), pos(4, 95)))
	if !out.ResetAndSeed || changed || s.AppliedPos != pos(5, 0) {
		t.Fatalf("out=%s changed=%v applied=%v", describe(out), changed, s.AppliedPos)
	}
	// With prevEnd equal to the applied position it is accepted.
	s = PairSeq{Epoch: 5, AppliedPos: pos(5, 0)}
	s.Apply(record(pos(5, 1), pos(5, 0)))
	if s.AppliedPos != pos(5, 1) {
		t.Fatalf("applied = %v", s.AppliedPos)
	}
}

// TestASeededOwnerAdoptsTheCutAsItsBoundary: after a seed the cut is both the
// applied position and the boundary, so its copies need not re-seed.
// Guard: ignore Cut on OwnershipAcquired.
func TestASeededOwnerAdoptsTheCutAsItsBoundary(t *testing.T) {
	s := PairSeq{Epoch: 6, Replayed: true, LeaseOK: true, Members: 3}
	s.Apply(OwnershipAcquired{Epoch: 6, Target: pos(5, 100), Cut: pos(5, 100)})
	out, _ := s.Apply(OrderRequest{})
	if out.Assign == nil || out.Assign.Pos != pos(6, 1) || out.Assign.PrevEnd != pos(5, 100) {
		t.Fatalf("seeded owner's opener = %+v, want 6:1 carrying 5:100", out.Assign)
	}
}

// TestAStaleEpochEventNeverLowersTheEpoch: neither EpochAdvanced nor
// OwnershipAcquired may move Epoch down.
// Guard: set Epoch from OwnershipAcquired without comparing.
func TestAStaleEpochEventNeverLowersTheEpoch(t *testing.T) {
	s := PairSeq{Epoch: 8, AppendedSeq: 2}
	s.Apply(EpochAdvanced{Epoch: 3})
	s.Apply(OwnershipAcquired{Epoch: 3, Appended: 9})
	if s.Epoch != 8 || s.AppendedSeq != 2 || s.Owner {
		t.Fatalf("stale events changed the machine: %+v", s)
	}
}
