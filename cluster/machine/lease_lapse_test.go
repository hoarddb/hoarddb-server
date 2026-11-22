package machine

import (
	"testing"
	"time"
)

// lapseRow is one way a lease stops being valid and the record the machine keeps of it.
type lapseRow struct {
	name      string
	before    LeaseState
	ev        LeaseEvent
	cause     LapseCause
	lapsedAt  time.Duration
	wantValid bool
}

func lapseRows() []lapseRow {
	h := held()
	lateGrant := GrantReceived{At: base + 2500*leaseMillisecond, Wall: wall0 + 2500*leaseMillisecond, Nonce: 7, Epoch: 5, CommittedEpoch: 5, Term: 3, RTT: 10 * leaseMillisecond}
	nearExpiry := leaseWith(h, func(s *LeaseState) { s.TickAt, s.TickWall = base+1950*leaseMillisecond, wall0+1950*leaseMillisecond })
	return []lapseRow{
		{name: "(a) a tick more than three heartbeats after the last", before: h, ev: tickAt(base + 400*leaseMillisecond), cause: LapseTickGap, lapsedAt: base + 400*leaseMillisecond},
		{name: "(a) a tick on a clock that did not advance", before: h, ev: tickAt(base), cause: LapseClockStopped, lapsedAt: base},
		{name: "(a) a tick whose wall delta disagrees", before: h, ev: Tick{At: base + 100*leaseMillisecond, Wall: wall0 + 260*leaseMillisecond}, cause: LapseWallClock, lapsedAt: base + 100*leaseMillisecond},
		{name: "(b) expiry noticed by a tick is stamped at Until", before: nearExpiry, ev: tickAt(base + 2050*leaseMillisecond), cause: LapseExpired, lapsedAt: base + 2*time.Second},
		{name: "(b) expiry noticed by a serve check is stamped at Until", before: h, ev: ServeCheck{At: base + 2300*leaseMillisecond, CommittedEpoch: 5}, cause: LapseExpired, lapsedAt: base + 2*time.Second},
		{name: "(c) a tick that is not after the last renewal", before: leaseWith(h, func(s *LeaseState) {
			s.RenewedAt, s.TickAt, s.TickWall = base+500*leaseMillisecond, base+400*leaseMillisecond, wall0+400*leaseMillisecond
		}), ev: tickAt(base + 500*leaseMillisecond), cause: LapseNoProgress, lapsedAt: base + 500*leaseMillisecond},
		{name: "(d) a grant whose round trip is step_down or more", before: h, ev: GrantReceived{At: base + 600*leaseMillisecond, Nonce: 7, Epoch: 5, CommittedEpoch: 5, Term: 3, RTT: 2 * time.Second}, cause: LapseRoundTooSlow, lapsedAt: base + 600*leaseMillisecond},
		{name: "(e) a grant naming a later epoch", before: h, ev: GrantReceived{At: base + 600*leaseMillisecond, Nonce: 7, Epoch: 6, CommittedEpoch: 5, Term: 3, RTT: 10 * leaseMillisecond}, cause: LapseEpoch, lapsedAt: base + 600*leaseMillisecond},
		{name: "(f) the leader's own round failed", before: h, ev: OwnQuorum{At: base + 700*leaseMillisecond, CommittedEpoch: 5, Term: 3}, cause: LapseQuorumFailed, lapsedAt: base + 700*leaseMillisecond},
		{name: "(g) a serve check at another epoch", before: h, ev: ServeCheck{At: base + 800*leaseMillisecond, CommittedEpoch: 6}, cause: LapseEpoch, lapsedAt: base + 800*leaseMillisecond},
		{name: "a lease never held", before: LeaseState{Term: 3}, ev: ServeCheck{At: base, CommittedEpoch: 0}, cause: LapseNeverHeld, lapsedAt: 0},
		{name: "expiry then a late grant: the accepting decision carries the ended cause", before: h, ev: lateGrant, cause: LapseExpired, lapsedAt: base + 2*time.Second, wantValid: true},
	}
}

// TestALapseRecordsWhenAndWhy is spec lease-renewer-cadence section 6, O1: one row per condition
// that ends validity, with the cause and the time the machine fixed on the event itself.
func TestALapseRecordsWhenAndWhy(t *testing.T) {
	t.Run("the first cause wins inside one lapse", theFirstCauseWinsInsideOneLapse)
	t.Run("the first grant ends the never held lapse", theFirstGrantEndsTheNeverHeldLapse)
	for _, row := range lapseRows() {
		t.Run(row.name, func(t *testing.T) {
			s := row.before
			d := s.Apply(row.ev, cfg3)
			if d.Valid != row.wantValid {
				t.Fatalf("Valid = %v, want %v (%+v)", d.Valid, row.wantValid, d)
			}
			if d.Cause != row.cause || d.LapsedAt != row.lapsedAt {
				t.Errorf("decision records cause %v at %v, want %v at %v", d.Cause, d.LapsedAt, row.cause, row.lapsedAt)
			}
			if d.Ended != row.wantValid {
				t.Errorf("Ended = %v, want %v: only the decision that accepts a renewal ends a lapse", d.Ended, row.wantValid)
			}
			if row.wantValid {
				if s.Cause != LapseNone || s.LapsedAt != 0 {
					t.Errorf("an accepted renewal left the lapse open: %v at %v", s.Cause, s.LapsedAt)
				}
				return
			}
			if row.cause != LapseNeverHeld && (s.Cause != row.cause || s.LapsedAt != row.lapsedAt) {
				t.Errorf("state keeps cause %v at %v, want %v at %v", s.Cause, s.LapsedAt, row.cause, row.lapsedAt)
			}
		})
	}
}

// theFirstCauseWinsInsideOneLapse: a later event that is also not valid does not rewrite
// the lapse, and the accepting decision names the first cause with its time.
func theFirstCauseWinsInsideOneLapse(t *testing.T) {
	s := held()
	first := s.Apply(tickAt(base+400*leaseMillisecond), cfg3)
	if first.Cause != LapseTickGap {
		t.Fatalf("first cause = %v, want tick_gap", first.Cause)
	}
	for _, ev := range []LeaseEvent{
		OwnQuorum{At: base + 420*leaseMillisecond, CommittedEpoch: 5, Term: 3},
		ServeCheck{At: base + 450*leaseMillisecond, CommittedEpoch: 9},
		tickAt(base + 480*leaseMillisecond),
	} {
		again := s.Apply(ev, cfg3)
		if again.Valid || again.Cause != LapseTickGap || again.LapsedAt != base+400*leaseMillisecond {
			t.Fatalf("%T after the first cause: %+v, want tick_gap at %v", ev, again, base+400*leaseMillisecond)
		}
	}
	renewed := s.Apply(OwnQuorum{At: base + 500*leaseMillisecond, CommittedEpoch: 5, Term: 3, OK: true}, cfg3)
	if !renewed.Valid || !renewed.Ended || renewed.Cause != LapseTickGap || renewed.LapsedAt != base+400*leaseMillisecond {
		t.Fatalf("accepting decision = %+v, want an ended tick_gap lapse at %v", renewed, base+400*leaseMillisecond)
	}
	if next := s.Apply(OwnQuorum{At: base + 600*leaseMillisecond, CommittedEpoch: 5, Term: 3, OK: true}, cfg3); next.Ended {
		t.Fatalf("a second renewal reports an ended lapse: %+v", next)
	}
}

// theFirstGrantEndsTheNeverHeldLapse: a node that has never held a lease reports it, and
// the grant that first accepts one ends it.
func theFirstGrantEndsTheNeverHeldLapse(t *testing.T) {
	s := LeaseState{Term: 3}
	if d := s.Apply(RequestSent{At: base, Nonce: 1}, cfg3); d.Valid || d.Cause != LapseNeverHeld {
		t.Fatalf("before the first grant: %+v", d)
	}
	d := s.Apply(GrantReceived{At: base + 50*leaseMillisecond, Nonce: 1, Epoch: 0, CommittedEpoch: 0, Term: 3, RTT: 10 * leaseMillisecond}, cfg3)
	if !d.Valid || !d.Ended || d.Cause != LapseNeverHeld {
		t.Fatalf("first grant = %+v, want an ended never_held lapse", d)
	}
}

func TestLapseCausesHaveTheirLogNames(t *testing.T) {
	want := map[LapseCause]string{
		LapseNeverHeld: "never_held", LapseTickGap: "tick_gap", LapseClockStopped: "clock_stopped", LapseWallClock: "wall_clock",
		LapseExpired: "expired", LapseNoProgress: "no_progress", LapseRoundTooSlow: "round_too_slow", LapseEpoch: "epoch", LapseQuorumFailed: "quorum_failed",
	}
	for cause, name := range want {
		if cause.String() != name {
			t.Errorf("%d prints %q, want %q", cause, cause.String(), name)
		}
	}
}
