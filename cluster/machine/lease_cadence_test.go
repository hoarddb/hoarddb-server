package machine

import (
	"testing"
	"time"
)

// The tests of spec lease-renewer-cadence section 7, S2a: the leader's own lease counts from its
// round's start (R1) and a tick may come up to three heartbeats after the previous one (R2).

// ownOK is a successful own quorum round that ended at at and took rtt.
func ownOK(at, rtt time.Duration) OwnQuorum {
	return OwnQuorum{At: at, CommittedEpoch: 5, Term: 3, OK: true, RTT: rtt}
}

// TestALeaderRoundEnding150msLateKeepsTheLease is L1: the leader's round ends 10 ms after its
// tick and the next tick comes 250 ms after the first, 150 ms later than the heartbeat. Two
// heartbeats void that; three do not.
func TestALeaderRoundEnding150msLateKeepsTheLease(t *testing.T) {
	s := LeaseState{Term: 3}
	mustApply(t, &s, tickAt(base), cfg3)
	if d := mustApply(t, &s, ownOK(base+10*leaseMillisecond, 10*leaseMillisecond), cfg3); !d.Valid {
		t.Fatalf("own round: %+v, want valid", d)
	}
	if d := mustApply(t, &s, tickAt(base+250*leaseMillisecond), cfg3); !d.Valid || d.StepDown {
		t.Errorf("a tick 250 ms after the last, 240 ms after the round: %+v held=%v, want valid", d, s.Held)
	}
}

// TestAFollowerRoundEnding150msLateKeepsTheLease is L2: the follower's grant arrives 160 ms after
// its tick (RTT 160 ms) and the next tick comes 260 ms after the first.
func TestAFollowerRoundEnding150msLateKeepsTheLease(t *testing.T) {
	s := LeaseState{Term: 3}
	if d := mustApply(t, &s, tickAt(base), cfg3); !d.SendRequest {
		t.Fatalf("first tick: %+v, want a request", d)
	}
	mustApply(t, &s, RequestSent{At: base, Nonce: 1}, cfg3)
	grant := GrantReceived{At: base + 160*leaseMillisecond, Wall: wall0 + 160*leaseMillisecond, Nonce: 1, Epoch: 5, CommittedEpoch: 5, Term: 3, RTT: 160 * leaseMillisecond}
	if d := mustApply(t, &s, grant, cfg3); !d.Valid {
		t.Fatalf("grant: %+v, want valid", d)
	}
	if d := mustApply(t, &s, tickAt(base+260*leaseMillisecond), cfg3); !d.Valid || d.StepDown {
		t.Errorf("a tick 260 ms after the last, 100 ms after the grant: %+v held=%v, want valid", d, s.Held)
	}
}

// TestATickBeyondThreeHeartbeatsVoidsTheLease is L3, replacing the two-heartbeat test: a tick
// exactly three heartbeats after the previous one keeps the lease, one a millisecond later voids
// it. A process that froze for longer than three heartbeats still steps down at its next tick,
// whatever the lease's Until says.
func TestATickBeyondThreeHeartbeatsVoidsTheLease(t *testing.T) {
	s := settle(t)
	mustApply(t, &s, tickAt(base+400*leaseMillisecond), cfg3)
	if d := mustApply(t, &s, tickAt(base+700*leaseMillisecond), cfg3); !d.Valid {
		t.Fatalf("exactly 3 heartbeats: %+v, want valid", d)
	}
	d := mustApply(t, &s, tickAt(base+700*leaseMillisecond+301*leaseMillisecond), cfg3)
	if d.Valid || !d.StepDown || d.Refusal != lapse || s.Held || d.Cause != LapseTickGap {
		t.Errorf("301 ms between ticks: %+v held=%v, want a tick_gap step-down with LEASE_LAPSED", d, s.Held)
	}
}

// TestTheLeadersOwnLeaseCountsFromItsRoundsStart is L4 (R1): the own round's RTT shortens the
// lease as a grant's does, an RTT of step_down or more voids it as (d) does, and a negative RTT
// changes nothing.
func TestTheLeadersOwnLeaseCountsFromItsRoundsStart(t *testing.T) {
	s := LeaseState{Term: 3}
	mustApply(t, &s, tickAt(base), cfg3)
	if d := mustApply(t, &s, ownOK(base+1500*leaseMillisecond, 1500*leaseMillisecond), cfg3); !d.Valid {
		t.Fatalf("a 1500 ms own round: %+v, want valid", d)
	}
	if want := base + 2000*leaseMillisecond; s.Until != want || s.RenewedAt != base+1500*leaseMillisecond {
		t.Errorf("after a 1500 ms own round ending at +1500 ms: Until %v RenewedAt %v, want Until %v (the round's start plus step_down) and RenewedAt at the round's end",
			s.Until-base, s.RenewedAt-base, want-base)
	}

	slow := s
	d := mustApply(t, &slow, ownOK(base+1600*leaseMillisecond, cfg3.StepDown), cfg3)
	if d.Valid || !d.StepDown || slow.Held || d.Cause != LapseRoundTooSlow || slow.Until != base+1600*leaseMillisecond {
		t.Errorf("an own round of step_down: %+v held=%v until %v, want a round_too_slow step-down", d, slow.Held, slow.Until-base)
	}

	negative := s
	d = mustApply(t, &negative, ownOK(base+1600*leaseMillisecond, -time.Millisecond), cfg3)
	if negative != s || !d.Valid || d.StepDown {
		t.Errorf("an own round with a negative RTT changed the lease: %+v -> %+v (%+v), want it ignored", s, negative, d)
	}
}

// TestALateOwnStampCannotOutliveTheFence is L5, the window R1 closes: a leader whose round
// confirmed the majority 10 ms after its tick, then stalled 180 ms before it stamped, has a lease
// that ends step_down after the round's start, not after the stamp. With the three-heartbeat
// bound the next tick, 290 ms on, no longer catches the stall, so only R1 keeps the fence.
func TestALateOwnStampCannotOutliveTheFence(t *testing.T) {
	s := LeaseState{Term: 3, Epoch: 5}
	mustApply(t, &s, tickAt(base), cfg3)
	mustApply(t, &s, ownOK(base+190*leaseMillisecond, 190*leaseMillisecond), cfg3)
	if d := mustApply(t, &s, tickAt(base+290*leaseMillisecond), cfg3); !d.Valid {
		t.Fatalf("a tick 290 ms after the last: %+v, want valid under the three-heartbeat bound", d)
	}
	if d := mustApply(t, &s, ServeCheck{At: base + 1999*leaseMillisecond, CommittedEpoch: 5}, cfg3); !d.Valid {
		t.Errorf("a serve check 1 ms before the round's start plus step_down: %+v, want valid", d)
	}
	if d := mustApply(t, &s, ServeCheck{At: base + 2050*leaseMillisecond, CommittedEpoch: 5}, cfg3); d.Valid {
		t.Errorf("a serve check 50 ms after the round's start plus step_down: %+v (Until %v), want refused: the lease outlived its fence",
			d, s.Until-base)
	}
}

// TestATickRightAfterARenewalKeepsTheLease is L6: the renewer starts its next round at once after
// an overlong one, so its tick can follow the renewal by nanoseconds. A tick after the renewal
// keeps the lease; a tick at the renewal's own reading has seen no progress and voids it (row c).
func TestATickRightAfterARenewalKeepsTheLease(t *testing.T) {
	renewed := LeaseState{Term: 3}
	mustApply(t, &renewed, tickAt(base), cfg3)
	mustApply(t, &renewed, ownOK(base+10*leaseMillisecond, 10*leaseMillisecond), cfg3)

	after := renewed
	if d := mustApply(t, &after, tickAt(base+10*leaseMillisecond+1), cfg3); !d.Valid {
		t.Errorf("a tick 1 ns after the renewal: %+v, want valid", d)
	}
	same := renewed
	d := mustApply(t, &same, tickAt(base+10*leaseMillisecond), cfg3)
	if d.Valid || d.Cause != LapseNoProgress {
		t.Errorf("a tick at the renewal's reading: %+v, want a no_progress void", d)
	}
}
