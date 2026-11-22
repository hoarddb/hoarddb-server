package machine

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// row builds the facts of one line of the answer table (write path §9).
// Every field a row does not name keeps the value that makes the row's own
// answer the interesting one: a lease that is valid, a level that is met by
// a count that backs it.
func row(mutate func(*WriteState)) WriteState {
	facts := WriteState{
		Durable:            true,
		OwnerNow:           true,
		DrainedOrAcked:     false,
		LevelMet:           true,
		WCTimeout:          false,
		LeaseValidAtAnswer: true,
		Level:              ConcernMajority,
		CopiesConfirmed:    2,
		RF:                 3,
	}
	mutate(&facts)
	return facts
}

// TestAnswerTableRows covers every line of write path §9 (rows A to F) and
// the lease re-check row of conventions §15 (E'), one case each.
func TestAnswerTableRows(t *testing.T) {
	cases := []struct {
		name        string
		facts       WriteState
		wantOutcome Outcome
		wantCode    cluster.Code
	}{
		{
			name:        "A durable, owner now, level met, lease valid",
			facts:       row(func(f *WriteState) {}),
			wantOutcome: OutcomeApplied,
		},
		{
			name: "B durable, owner now, level missed at wc_timeout, lease valid",
			facts: row(func(f *WriteState) {
				f.LevelMet = false
				f.WCTimeout = true
				f.CopiesConfirmed = 1
			}),
			wantOutcome: OutcomeAppliedDegraded,
		},
		{
			name: "C no longer the owner, drained or acked, level met",
			facts: row(func(f *WriteState) {
				f.OwnerNow = false
				f.DrainedOrAcked = true
			}),
			wantOutcome: OutcomeApplied,
		},
		{
			name: "C with the lease lapsed (lease is any)",
			facts: row(func(f *WriteState) {
				f.OwnerNow = false
				f.DrainedOrAcked = true
				f.LeaseValidAtAnswer = false
			}),
			wantOutcome: OutcomeApplied,
		},
		{
			name: "D no longer the owner, drained or acked, level not met",
			facts: row(func(f *WriteState) {
				f.OwnerNow = false
				f.DrainedOrAcked = true
				f.LevelMet = false
				f.WCTimeout = true
				f.CopiesConfirmed = 1
			}),
			wantOutcome: OutcomeAppliedDegraded,
		},
		{
			name: "D with the lease lapsed (lease is any)",
			facts: row(func(f *WriteState) {
				f.OwnerNow = false
				f.DrainedOrAcked = true
				f.LevelMet = false
				f.CopiesConfirmed = 1
				f.LeaseValidAtAnswer = false
			}),
			wantOutcome: OutcomeAppliedDegraded,
		},
		{
			name: "E no longer the owner, nothing drained, no ack, level met",
			facts: row(func(f *WriteState) {
				f.OwnerNow = false
			}),
			wantOutcome: OutcomeUnknown,
		},
		{
			name: "E no longer the owner, nothing drained, level not met",
			facts: row(func(f *WriteState) {
				f.OwnerNow = false
				f.LevelMet = false
				f.CopiesConfirmed = 1
			}),
			wantOutcome: OutcomeUnknown,
		},
		{
			name: "E with the lease lapsed too",
			facts: row(func(f *WriteState) {
				f.OwnerNow = false
				f.LeaseValidAtAnswer = false
			}),
			wantOutcome: OutcomeUnknown,
		},
		{
			name: "E' owner now, level met, lease lapsed at answer time",
			facts: row(func(f *WriteState) {
				f.LeaseValidAtAnswer = false
			}),
			wantOutcome: OutcomeUnknown,
		},
		{
			name: "E' owner now, level not met, lease lapsed at answer time",
			facts: row(func(f *WriteState) {
				f.LevelMet = false
				f.WCTimeout = true
				f.CopiesConfirmed = 1
				f.LeaseValidAtAnswer = false
			}),
			wantOutcome: OutcomeUnknown,
		},
		{
			name: "F the append refused: not durable, no write_outcome",
			facts: row(func(f *WriteState) {
				f.Durable = false
				f.CopiesConfirmed = 0
			}),
			wantOutcome: 0,
			wantCode:    cluster.CodeUnavailable,
		},
		{
			name: "F with every other fact favourable still refuses",
			facts: row(func(f *WriteState) {
				f.Durable = false
				f.DrainedOrAcked = true
			}),
			wantOutcome: 0,
			wantCode:    cluster.CodeUnavailable,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotOutcome, gotCode := Answer(c.facts)
			if gotOutcome != c.wantOutcome || gotCode != c.wantCode {
				t.Fatalf("Answer(%+v) = (%v, %v), want (%v, %v)",
					c.facts, gotOutcome, gotCode, c.wantOutcome, c.wantCode)
			}
		})
	}
}

// TestAppliedThenUndrainedAnswersUnknown: the write was applied at 42, a
// further epoch change came before a READY covered it, and no copy that is now
// the owner acked it. Only the fences changed, and the new owner may never have
// had the write, so the answer is unknown whether or not the level was met.
// [Guard: answer applied because only the fences changed.]
func TestAppliedThenUndrainedAnswersUnknown(t *testing.T) {
	for _, levelMet := range []bool{true, false} {
		facts := row(func(f *WriteState) {
			f.OwnerNow = false
			f.DrainedOrAcked = false
			f.LevelMet = levelMet
			if !levelMet {
				f.CopiesConfirmed = 1
			}
		})
		gotOutcome, gotCode := Answer(facts)
		if gotOutcome != OutcomeUnknown || gotCode != 0 {
			t.Fatalf("undrained, not owner, level met %v: got (%v, %v), want (unknown, 0)",
				levelMet, gotOutcome, gotCode)
		}
	}
}

// TestEpochChangeInsideTheSectionIsAnsweredNotUnknown: an epoch flipped while
// the write was inside the section. The position is in appliedPos and the
// READY covers it, so the facts are "no longer the owner, drained", and the
// answer is applied or applied_degraded, never unknown.
// [Guard: move the hold flag outside the lock, so the write is never drained
// and row E fires on a write the new owner has.]
func TestEpochChangeInsideTheSectionIsAnsweredNotUnknown(t *testing.T) {
	met := row(func(f *WriteState) {
		f.OwnerNow = false
		f.DrainedOrAcked = true
	})
	if gotOutcome, gotCode := Answer(met); gotOutcome != OutcomeApplied || gotCode != 0 {
		t.Fatalf("drained, level met: got (%v, %v), want (applied, 0)", gotOutcome, gotCode)
	}
	missed := row(func(f *WriteState) {
		f.OwnerNow = false
		f.DrainedOrAcked = true
		f.LevelMet = false
		f.CopiesConfirmed = 1
	})
	if gotOutcome, gotCode := Answer(missed); gotOutcome != OutcomeAppliedDegraded || gotCode != 0 {
		t.Fatalf("drained, level missed: got (%v, %v), want (applied_degraded, 0)", gotOutcome, gotCode)
	}
}

// TestConcernIsARefusalToLowerTheBar: RF=3 with only the owner reachable
// answers applied_degraded, never applied, even when the caller's LevelMet is
// wrong. The count has to back a met level, and majority comes from the
// configured RF (floor(3/2)+1 = 2), not from the one copy in sync
// (floor(1/2)+1 = 1). [Guard: compute majority from copies in sync.]
func TestConcernIsARefusalToLowerTheBar(t *testing.T) {
	cases := []struct {
		name  string
		facts WriteState
	}{
		{"the wait timed out with only the owner", row(func(f *WriteState) {
			f.LevelMet = false
			f.WCTimeout = true
			f.CopiesConfirmed = 1
		})},
		{"LevelMet claimed but one copy cannot be a majority of three", row(func(f *WriteState) {
			f.CopiesConfirmed = 1
		})},
		{"LevelMet claimed after a timeout: a level reached later changes nothing", row(func(f *WriteState) {
			f.WCTimeout = true
		})},
		{"all over RF 3 with two confirmed", row(func(f *WriteState) {
			f.Level = ConcernAll
			f.CopiesConfirmed = 2
		})},
		{"a level that was never set cannot be met", row(func(f *WriteState) {
			f.Level = 0
		})},
		{"no confirmed copy at all: the owner counts, so zero is never met", row(func(f *WriteState) {
			f.Level = ConcernOne
			f.CopiesConfirmed = 0
		})},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotOutcome, gotCode := Answer(c.facts)
			if gotOutcome != OutcomeAppliedDegraded || gotCode != 0 {
				t.Fatalf("Answer(%+v) = (%v, %v), want (applied_degraded, 0)", c.facts, gotOutcome, gotCode)
			}
		})
	}

	owner := row(func(f *WriteState) {
		f.Level = ConcernOne
		f.CopiesConfirmed = 1
	})
	if gotOutcome, gotCode := Answer(owner); gotOutcome != OutcomeApplied || gotCode != 0 {
		t.Fatalf("level one with the owner's own append: got (%v, %v), want (applied, 0)", gotOutcome, gotCode)
	}
}

// TestMajorityIsFloorHalfPlusOneOfTheConfiguredRF pins the arithmetic the
// worked example in conventions §17 states.
func TestMajorityIsFloorHalfPlusOneOfTheConfiguredRF(t *testing.T) {
	cases := []struct{ rf, want uint8 }{{1, 1}, {2, 2}, {3, 2}, {4, 3}, {5, 3}}
	for _, c := range cases {
		if got := MajorityOf(c.rf); got != c.want {
			t.Fatalf("MajorityOf(%d) = %d, want %d", c.rf, got, c.want)
		}
	}
}

// TestLeaseLapseAtAnswerTimeIsUnknown: the lease is re-read immediately
// before the answer, and a node that lost it while the concern waited cannot
// vouch that the position reached anybody, whether or not the level was met.
// [Guard: answer applied_degraded.]
func TestLeaseLapseAtAnswerTimeIsUnknown(t *testing.T) {
	for _, levelMet := range []bool{true, false} {
		facts := row(func(f *WriteState) {
			f.OwnerNow = true
			f.LeaseValidAtAnswer = false
			f.LevelMet = levelMet
			f.WCTimeout = !levelMet
			if !levelMet {
				f.CopiesConfirmed = 1
			}
		})
		gotOutcome, gotCode := Answer(facts)
		if gotOutcome != OutcomeUnknown || gotCode != 0 {
			t.Fatalf("lapsed lease, level met %v: got (%v, %v), want (unknown, 0)", levelMet, gotOutcome, gotCode)
		}
	}
}

// TestARefusalBeforeTheAppendIsNeverUnknown: a write that was not made
// durable gets UNAVAILABLE and no write_outcome, whatever the other facts say.
func TestARefusalBeforeTheAppendIsNeverUnknown(t *testing.T) {
	for _, owner := range []bool{true, false} {
		for _, drained := range []bool{true, false} {
			for _, lease := range []bool{true, false} {
				facts := row(func(f *WriteState) {
					f.Durable = false
					f.OwnerNow = owner
					f.DrainedOrAcked = drained
					f.LeaseValidAtAnswer = lease
				})
				gotOutcome, gotCode := Answer(facts)
				if gotOutcome != 0 || gotCode != cluster.CodeUnavailable {
					t.Fatalf("Answer(%+v) = (%v, %v), want (no outcome, UNAVAILABLE)", facts, gotOutcome, gotCode)
				}
			}
		}
	}
}

// TestOutcomeNamesAreTheWireWords pins the three values of conventions §17.
func TestOutcomeNamesAreTheWireWords(t *testing.T) {
	cases := map[Outcome]string{
		OutcomeApplied:         "applied",
		OutcomeAppliedDegraded: "applied_degraded",
		OutcomeUnknown:         "unknown",
	}
	for outcome, want := range cases {
		if got := outcome.String(); got != want {
			t.Fatalf("Outcome(%d).String() = %q, want %q", uint8(outcome), got, want)
		}
	}
}

// TestEveryCombinationHasExactlyOneAnswer walks the whole input space: the six
// booleans (64) by every level including the unset one and a value past the
// last (5), by RF 0 to 5 (6), by a confirmed count 0 to 6 (7) -- 13,440
// combinations. For each one Answer must be total and deterministic, and must
// agree with an oracle written from the decision record's words instead of the
// table's shape. Above all, no combination may say applied when the write is
// not durable on enough copies for its level.
func TestEveryCombinationHasExactlyOneAnswer(t *testing.T) {
	const wantSpace = 64 * 5 * 6 * 7
	seen := 0
	for bits := 0; bits < 64; bits++ {
		for level := Concern(0); level <= 4; level++ {
			for rf := uint8(0); rf <= 5; rf++ {
				for copies := uint8(0); copies <= 6; copies++ {
					facts := WriteState{
						Durable:            bits&1 != 0,
						OwnerNow:           bits&2 != 0,
						DrainedOrAcked:     bits&4 != 0,
						LevelMet:           bits&8 != 0,
						WCTimeout:          bits&16 != 0,
						LeaseValidAtAnswer: bits&32 != 0,
						Level:              level,
						CopiesConfirmed:    copies,
						RF:                 rf,
					}
					seen++
					checkOneCombination(t, facts)
				}
			}
		}
	}
	if seen != wantSpace {
		t.Fatalf("enumerated %d combinations, want %d", seen, wantSpace)
	}
	t.Logf("enumerated %d combinations", seen)
}

func checkOneCombination(t *testing.T, facts WriteState) {
	t.Helper()
	gotOutcome, gotCode := Answer(facts)
	if againOutcome, againCode := Answer(facts); againOutcome != gotOutcome || againCode != gotCode {
		t.Fatalf("Answer(%+v) is not deterministic: (%v, %v) then (%v, %v)",
			facts, gotOutcome, gotCode, againOutcome, againCode)
	}

	if !facts.Durable {
		if gotOutcome != 0 || gotCode != cluster.CodeUnavailable {
			t.Fatalf("Answer(%+v) = (%v, %v), want a bare UNAVAILABLE refusal", facts, gotOutcome, gotCode)
		}
		return
	}
	if gotCode != 0 {
		t.Fatalf("Answer(%+v) refused a durable write with %v", facts, gotCode)
	}

	// The oracle. The level is met only when the wait saw it met, did not time
	// out, and the count of confirmed copies reaches the target computed from
	// the configured RF (never from copies in sync).
	var target uint8
	switch facts.Level {
	case ConcernOne:
		target = 1
	case ConcernMajority:
		target = facts.RF/2 + 1
	case ConcernAll:
		target = facts.RF
	}
	levelReallyMet := facts.Level >= ConcernOne && facts.Level <= ConcernAll &&
		facts.RF >= 1 && target >= 1 && facts.CopiesConfirmed >= target &&
		facts.LevelMet && !facts.WCTimeout

	cannotVouch := (facts.OwnerNow && !facts.LeaseValidAtAnswer) ||
		(!facts.OwnerNow && !facts.DrainedOrAcked)

	var want Outcome
	switch {
	case cannotVouch:
		want = OutcomeUnknown
	case levelReallyMet:
		want = OutcomeApplied
	default:
		want = OutcomeAppliedDegraded
	}
	if gotOutcome != want {
		t.Fatalf("Answer(%+v) = %v, want %v", facts, gotOutcome, want)
	}

	// The two safety properties, stated again on their own so a wrong oracle
	// cannot hide them.
	if gotOutcome == OutcomeApplied && !levelReallyMet {
		t.Fatalf("Answer(%+v) said applied but the level is not met by %d of %d copies",
			facts, facts.CopiesConfirmed, facts.RF)
	}
	if gotOutcome == OutcomeApplied && cannotVouch {
		t.Fatalf("Answer(%+v) said applied from a node that cannot vouch for the position", facts)
	}
}
