package machine

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// history is the one true log of a pair: the epochs that hold records and how
// many each holds. Quiet epochs hold none and do not appear. It is the oracle
// for "next position" and for what an opener's prevEnd must be.
type history struct {
	epochs []uint64
	counts map[uint64]uint64
	all    []cluster.Position
}

func newHistory(rng *rand.Rand) *history {
	h := &history{counts: map[uint64]uint64{}}
	epoch := uint64(1 + rng.Intn(3))
	for i := 0; i < 12; i++ {
		n := uint64(1 + rng.Intn(6))
		h.epochs = append(h.epochs, epoch)
		h.counts[epoch] = n
		for seq := uint64(1); seq <= n; seq++ {
			h.all = append(h.all, pos(epoch, seq))
		}
		epoch += uint64(1 + rng.Intn(2))
	}
	return h
}

// next is the position that follows a in the log, or false at the tail.
func (h *history) next(a cluster.Position) (cluster.Position, bool) {
	for i, p := range h.all {
		if a.Less(p) {
			return h.all[i], true
		}
	}
	return cluster.Position{}, false
}

// prevEnd is the position just before the opener at p: 0:0 for the first record.
func (h *history) prevEnd(p cluster.Position) cluster.Position {
	for i, q := range h.all {
		if q == p {
			if i == 0 {
				return cluster.Position{}
			}
			return h.all[i-1]
		}
	}
	return cluster.Position{}
}

// refState is the reference model's state: plain fields and plain ifs, written
// from the prose of conv §10 rule 5 and write §2 §3 §4 rather than from the
// row table, so a transcription slip in seq.go shows up as a difference.
type refState struct {
	epoch, appended       uint64
	prevEnd, applied      cluster.Position
	appliedID             [16]byte
	target                cluster.Position
	owner, hold, replayed bool
	leaseOK               bool
	members               int
}

type refOut struct {
	assign  *Assignment
	refusal cluster.Code
	catchUp *cluster.Position
	reset   bool
}

func (r *refState) record(h *history, ev RecordApplied) refOut {
	p := ev.Pos
	switch {
	case p.Seq == 0, p == r.applied, p.Epoch > r.epoch:
		return refOut{}
	case p.Less(r.applied):
		return refOut{}
	}
	// p is after the applied position and inside a known epoch.
	if p.Seq == 1 {
		switch {
		case ev.PrevEnd == r.applied:
			if ev.PrevEndID != ([16]byte{}) && ev.PrevEndID != r.appliedID {
				return refOut{reset: true}
			}
			r.applied, r.appliedID, r.prevEnd = p, ev.RequestID, p
			return refOut{}
		case r.applied.Less(ev.PrevEnd):
			return refOut{catchUp: catchUpFrom(r.applied)}
		default:
			return refOut{reset: true}
		}
	}
	if want, ok := h.next(r.applied); ok && want == p {
		r.applied, r.appliedID, r.prevEnd = p, ev.RequestID, p
		return refOut{}
	}
	return refOut{refusal: cluster.CodeSeqGap, catchUp: catchUpFrom(r.applied)}
}

func (r *refState) order() refOut {
	switch {
	case !r.owner:
		return refOut{refusal: cluster.CodeNotOwner}
	case r.hold || !r.replayed || r.applied.Less(r.target):
		return refOut{}
	case r.members > 1 && !r.leaseOK:
		return refOut{refusal: cluster.CodeLeaseLapsed}
	}
	r.appended++
	return refOut{assign: &Assignment{Pos: pos(r.epoch, r.appended), PrevEnd: r.prevEnd}}
}

func (r *refState) advance(epoch uint64) {
	if epoch <= r.epoch {
		return
	}
	if r.owner && r.appended > 0 {
		r.prevEnd = pos(r.epoch, r.appended)
	} else {
		r.prevEnd = r.applied
	}
	r.epoch, r.appended, r.target = epoch, 0, cluster.Position{}
	if !r.owner {
		r.hold = true
	}
}

func (r *refState) acquire(ev OwnershipAcquired) {
	if ev.Epoch < r.epoch {
		return
	}
	r.owner, r.epoch, r.appended, r.target = true, ev.Epoch, ev.Appended, ev.Target
	r.prevEnd = r.applied
}

func (r *refState) matches(s *PairSeq) error {
	got := refState{epoch: s.Epoch, appended: s.AppendedSeq, prevEnd: s.PrevEnd, applied: s.AppliedPos, appliedID: s.AppliedID,
		target: s.Target, owner: s.Owner, hold: s.Hold, replayed: s.Replayed, leaseOK: s.LeaseOK, members: s.Members}
	if got != *r {
		return fmt.Errorf("machine %+v, reference %+v", got, *r)
	}
	return nil
}

// TestSeqModelAgainstAReferenceHistory drives 20 fixed seeds of random event
// sequences (orders, refusals, epoch changes, openers with true and forged
// prevEnd, gaps, duplicates, out-of-order catch-up, a second epoch bump during
// catch-up) through PairSeq and an independent reference, and checks the
// invariants of the brief after every event.
// Guard: break any row of seq.go and some seed's outcome or end state differs
// from the reference, or an invariant below fires.
func TestSeqModelAgainstAReferenceHistory(t *testing.T) {
	rowHits := map[string]int{}
	finished := 0
	for seed := int64(1); seed <= 20; seed++ {
		rng := rand.New(rand.NewSource(seed))
		h := newHistory(rng)
		s := &PairSeq{Epoch: uint64(rng.Intn(3)), Replayed: true, LeaseOK: true, Members: 3}
		ref := &refState{epoch: s.Epoch, replayed: true, leaseOK: true, members: 3}
		assigned := map[cluster.Position]bool{}
		mustBeOne := false

		for step := 0; step < 400; step++ {
			label := fmt.Sprintf("seed %d step %d", seed, step)
			before := *s
			var ev SeqEvent
			var want refOut
			switch r := rng.Intn(100); {
			case r < 52: // a record
				ev = randomRecord(rng, h, s)
				want = ref.record(h, ev.(RecordApplied))
				if rows := matchingRecordRows(s, ev.(RecordApplied)); len(rows) == 1 {
					rowHits[rows[0]]++
				}
			case r < 66: // an order
				ev = OrderRequest{}
				want = ref.order()
			case r < 76: // an epoch change, sometimes stale or repeated
				e := s.Epoch + uint64(rng.Intn(3))
				if rng.Intn(5) == 0 && s.Epoch > 0 {
					e = s.Epoch - uint64(rng.Intn(int(s.Epoch)+1))
				}
				ev = EpochAdvanced{Epoch: e}
				ref.advance(e)
				if e > s.Epoch {
					mustBeOne = true
				}
			case r < 84: // the gate flags
				f := HoldChanged{Hold: rng.Intn(5) == 0, Replayed: rng.Intn(4) != 0, LeaseOK: rng.Intn(4) != 0, Members: 1 + rng.Intn(3)}
				ev = f
				ref.hold, ref.replayed, ref.leaseOK, ref.members = f.Hold, f.Replayed, f.LeaseOK, f.Members
			case r < 90: // ownership moves by the caller's rank computation
				s.Owner = !s.Owner
				ref.owner = s.Owner
				before = *s
				ev = HoldChanged{Hold: s.Hold, Replayed: s.Replayed, LeaseOK: s.LeaseOK, Members: s.Members}
			case r < 95: // promotion
				e := s.Epoch + uint64(rng.Intn(2))
				appended := uint64(rng.Intn(3))
				if e == s.Epoch && appended < s.AppendedSeq {
					appended = s.AppendedSeq
				}
				target := cluster.Position{}
				if rng.Intn(2) == 0 {
					target = s.AppliedPos
				}
				o := OwnershipAcquired{Epoch: e, Appended: appended, Target: target}
				ev = o
				ref.acquire(o)
				mustBeOne = false
			default: // a raised target
				t2 := h.all[rng.Intn(len(h.all))]
				ev = TargetRaised{Target: t2}
				if ref.target.Less(t2) {
					ref.target = t2
				}
			}

			out, changed := s.Apply(ev)

			if err := ref.matches(s); err != nil {
				t.Fatalf("%s: %T %+v: %v", label, ev, ev, err)
			}
			if got := (refOut{assign: out.Assign, refusal: out.Refusal, catchUp: out.AskCatchUp, reset: out.ResetAndSeed}); !reflect.DeepEqual(got, want) {
				t.Fatalf("%s: %T %+v: outcome %s, reference %+v", label, ev, ev, describe(out), want)
			}
			if changed != !reflect.DeepEqual(before, *s) {
				t.Fatalf("%s: changed=%v but state differs=%v", label, changed, !reflect.DeepEqual(before, *s))
			}

			// Invariants.
			if s.Epoch < before.Epoch {
				t.Fatalf("%s: the epoch fell from %d to %d", label, before.Epoch, s.Epoch)
			}
			if out.Assign != nil {
				if assigned[out.Assign.Pos] {
					t.Fatalf("%s: position %v assigned twice", label, out.Assign.Pos)
				}
				assigned[out.Assign.Pos] = true
				if out.Assign.Pos.Epoch != s.Epoch || out.Assign.Pos.Seq != before.AppendedSeq+1 {
					t.Fatalf("%s: assigned %v after AppendedSeq %d at epoch %d", label, out.Assign.Pos, before.AppendedSeq, s.Epoch)
				}
				if mustBeOne && out.Assign.Pos.Seq != 1 {
					t.Fatalf("%s: first position after an epoch change is %v, want seq 1", label, out.Assign.Pos)
				}
				mustBeOne = false
			}
			if _, promoted := ev.(OwnershipAcquired); !promoted && out.Assign == nil && s.AppendedSeq > before.AppendedSeq {
				t.Fatalf("%s: AppendedSeq grew without a position", label)
			}
			if out.Refusal != 0 && !reflect.DeepEqual(before, *s) {
				t.Fatalf("%s: a refusal (%v) changed state", label, out.Refusal)
			}
			if _, isEpoch := ev.(EpochAdvanced); isEpoch && s.Epoch > before.Epoch && s.AppendedSeq != 0 {
				t.Fatalf("%s: an epoch change left AppendedSeq %d", label, s.AppendedSeq)
			}
			if s.AppliedPos.Less(before.AppliedPos) {
				t.Fatalf("%s: AppliedPos fell from %v to %v", label, before.AppliedPos, s.AppliedPos)
			}
			if s.AppliedPos != before.AppliedPos {
				want, ok := h.next(before.AppliedPos)
				if !ok || s.AppliedPos != want {
					t.Fatalf("%s: AppliedPos jumped from %v to %v, the next position is %v", label, before.AppliedPos, s.AppliedPos, want)
				}
			}
			if rec, isRecord := ev.(RecordApplied); isRecord && rec.Pos.Seq == 1 && before.AppliedPos.Less(rec.Pos) && rec.Pos.Epoch <= before.Epoch {
				accepted := s.AppliedPos == rec.Pos
				idOK := rec.PrevEndID == ([16]byte{}) || rec.PrevEndID == before.AppliedID
				if accepted != (rec.PrevEnd == before.AppliedPos && idOK) {
					t.Fatalf("%s: opener %v prevEnd %v accepted=%v with applied %v", label, rec.Pos, rec.PrevEnd, accepted, before.AppliedPos)
				}
			}
			if rec, isRecord := ev.(RecordApplied); isRecord && rec.Pos.Seq > 1 && s.AppliedPos == rec.Pos && before.AppliedPos != rec.Pos {
				if before.AppliedPos.Epoch != rec.Pos.Epoch || before.AppliedPos.Seq+1 != rec.Pos.Seq {
					t.Fatalf("%s: %v accepted over a gap from %v", label, rec.Pos, before.AppliedPos)
				}
			}
		}
		if s.AppliedPos != (cluster.Position{}) {
			finished++
		}
	}
	for _, name := range []string{"record/opener-accept", "record/opener-behind", "record/opener-ahead", "record/continuity",
		"record/gap", "record/missed-opener", "record/older-in-epoch", "record/opener-duplicate", "record/older-epoch"} {
		if rowHits[name] == 0 {
			t.Errorf("no seed reached row %s; hits: %v", name, rowHits)
		}
	}
	if finished < 15 {
		t.Errorf("only %d of 20 seeds applied any record; hits: %v", finished, rowHits)
	}
	t.Logf("row hits across 20 seeds: %v", rowHits)
}

// randomRecord offers the machine a record from the history: usually the next
// one, sometimes a skip, a duplicate, an older record or an out-of-order one,
// and openers sometimes carry a forged prevEnd or a mismatching id.
func randomRecord(rng *rand.Rand, h *history, s *PairSeq) RecordApplied {
	var p cluster.Position
	next, hasNext := h.next(s.AppliedPos)
	switch r := rng.Intn(100); {
	case r < 45 && hasNext:
		p = next
	case r < 60 && hasNext:
		if skip, ok := h.next(next); ok {
			p = skip
		} else {
			p = next
		}
	case r < 70:
		p = s.AppliedPos
		if p.Seq == 0 {
			p = h.all[0]
		}
	default:
		p = h.all[rng.Intn(len(h.all))]
	}
	ev := RecordApplied{Pos: p, RequestID: idOf(p), PrevEnd: h.prevEnd(p)}
	if p.Seq == 1 {
		switch rng.Intn(10) {
		case 0:
			ev.PrevEnd = h.all[rng.Intn(len(h.all))]
		case 1:
			ev.PrevEnd = cluster.Position{}
		}
		switch rng.Intn(10) {
		case 0, 1:
			ev.PrevEndID = idOf(ev.PrevEnd)
		case 2:
			ev.PrevEndID = rid(byte(rng.Intn(200)))
		}
	}
	return ev
}
