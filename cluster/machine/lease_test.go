package machine

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

const (
	leaseMillisecond = time.Millisecond
	base             = 10 * time.Second // an arbitrary boot reading; never 0, which means "none"
	wall0            = 5000 * time.Hour // an arbitrary wall reading
	lapse            = cluster.CodeLeaseLapsed
)

var cfg3 = LeaseConfig{Heartbeat: 100 * leaseMillisecond, StepDown: 2000 * leaseMillisecond, Members: 3, WallClock: true}

// held is a node three members deep with a live lease: granted at base, good
// until base+2s, one tick behind it, request nonce 7, term 3, epoch 5.
func held() LeaseState {
	return LeaseState{
		Held: true, Until: base + 2*time.Second, RenewedAt: base, SentAt: base + 100*leaseMillisecond,
		TickAt: base, TickWall: wall0, Nonce: 7, Term: 3, Epoch: 5,
	}
}

func leaseWith(s LeaseState, edit func(*LeaseState)) LeaseState {
	edit(&s)
	return s
}

func tickAt(at time.Duration) Tick { return Tick{At: at, Wall: wall0 + (at - base)} }

func mustApply(t *testing.T, s *LeaseState, ev LeaseEvent, cfg LeaseConfig) LeaseDecision {
	t.Helper()
	return s.Apply(ev, cfg)
}

type leaseRow struct {
	name   string
	cfg    LeaseConfig
	before LeaseState
	ev     LeaseEvent
	after  LeaseState
	want   LeaseDecision
}

func leaseRows() []leaseRow {
	one := cfg3
	one.Members = 1
	noWall := cfg3
	noWall.WallClock = false
	grant := GrantReceived{At: base + 500*leaseMillisecond, Wall: wall0 + 500*leaseMillisecond, Nonce: 7, Epoch: 5, CommittedEpoch: 5, Term: 3, RTT: 40 * leaseMillisecond}
	g := func(edit func(*GrantReceived)) GrantReceived {
		c := grant
		edit(&c)
		return c
	}
	h := held()
	notHeld := leaseWith(h, func(s *LeaseState) { s.Held = false })
	settled := leaseWith(h, func(s *LeaseState) { s.SentAt = base + 100*leaseMillisecond })

	return []leaseRow{
		// --- any event, Members == 1: no lease at all
		{name: "single node: tick", cfg: one, before: LeaseState{}, ev: tickAt(base), after: LeaseState{}, want: LeaseDecision{Valid: true}},
		{name: "single node: grant", cfg: one, before: LeaseState{}, ev: g(func(e *GrantReceived) { e.Epoch = 9 }), after: LeaseState{}, want: LeaseDecision{Valid: true}},
		{name: "single node: own quorum failed", cfg: one, before: LeaseState{}, ev: OwnQuorum{At: base}, after: LeaseState{}, want: LeaseDecision{Valid: true}},
		{name: "single node: serve check", cfg: one, before: LeaseState{}, ev: ServeCheck{At: base, CommittedEpoch: 4}, after: LeaseState{}, want: LeaseDecision{Valid: true}},
		{name: "single node: request sent", cfg: one, before: LeaseState{}, ev: RequestSent{At: base, Nonce: 2}, after: LeaseState{}, want: LeaseDecision{Valid: true}},

		// --- Tick
		{name: "tick: first tick, held, nothing to measure", cfg: cfg3,
			before: leaseWith(h, func(s *LeaseState) { s.TickAt, s.TickWall, s.SentAt = 0, 0, base+50*leaseMillisecond }), ev: tickAt(base + 100*leaseMillisecond),
			after: leaseWith(h, func(s *LeaseState) {
				s.TickAt, s.TickWall, s.SentAt = base+100*leaseMillisecond, wall0+100*leaseMillisecond, base+50*leaseMillisecond
			}),
			want: LeaseDecision{Valid: true}},
		{name: "tick: first tick, not held", cfg: cfg3,
			before: LeaseState{Term: 3, Epoch: 5}, ev: tickAt(base),
			after: LeaseState{Term: 3, Epoch: 5, TickAt: base, TickWall: wall0},
			want:  LeaseDecision{SendRequest: true, Refusal: lapse}},
		{name: "tick: first tick, held but already expired", cfg: cfg3,
			before: leaseWith(h, func(s *LeaseState) { s.TickAt, s.TickWall, s.Until = 0, 0, base+50*leaseMillisecond }), ev: tickAt(base + 100*leaseMillisecond),
			after: leaseWith(h, func(s *LeaseState) {
				s.Held, s.TickAt, s.TickWall, s.Until = false, base+100*leaseMillisecond, wall0+100*leaseMillisecond, base+50*leaseMillisecond
			}),
			want: LeaseDecision{StepDown: true, Refusal: lapse}},
		{name: "tick: clock did not advance", cfg: cfg3,
			before: leaseWith(h, func(s *LeaseState) { s.TickAt, s.SentAt = base+100*leaseMillisecond, base+100*leaseMillisecond }), ev: tickAt(base + 100*leaseMillisecond),
			after: leaseWith(h, func(s *LeaseState) {
				s.Held, s.TickAt, s.SentAt = false, base+100*leaseMillisecond, base+100*leaseMillisecond
			}),
			want: LeaseDecision{StepDown: true, Refusal: lapse}},
		{name: "tick: clock went backwards, high-water mark kept", cfg: cfg3,
			before: leaseWith(h, func(s *LeaseState) { s.TickAt, s.SentAt = base+100*leaseMillisecond, base+100*leaseMillisecond }), ev: tickAt(base + 50*leaseMillisecond),
			after: leaseWith(h, func(s *LeaseState) {
				s.Held, s.TickAt, s.SentAt = false, base+100*leaseMillisecond, base+100*leaseMillisecond
			}),
			want: LeaseDecision{StepDown: true, Refusal: lapse}},
		{name: "tick: clock did not advance, not held", cfg: cfg3,
			before: leaseWith(notHeld, func(s *LeaseState) { s.TickAt, s.SentAt = base+100*leaseMillisecond, base+100*leaseMillisecond }), ev: tickAt(base + 100*leaseMillisecond),
			after: leaseWith(notHeld, func(s *LeaseState) { s.TickAt, s.SentAt = base+100*leaseMillisecond, base+100*leaseMillisecond }),
			want:  LeaseDecision{Refusal: lapse}},
		{name: "tick: exactly 3 heartbeats since the last tick is fine", cfg: cfg3,
			before: leaseWith(h, func(s *LeaseState) { s.SentAt = base + 300*leaseMillisecond }), ev: tickAt(base + 300*leaseMillisecond),
			after: leaseWith(h, func(s *LeaseState) {
				s.SentAt, s.TickAt, s.TickWall = base+300*leaseMillisecond, base+300*leaseMillisecond, wall0+300*leaseMillisecond
			}),
			want: LeaseDecision{Valid: true}},
		{name: "tick: more than 3 heartbeats since the last tick", cfg: cfg3,
			before: leaseWith(h, func(s *LeaseState) { s.SentAt = base }), ev: tickAt(base + 301*leaseMillisecond),
			after: leaseWith(h, func(s *LeaseState) {
				s.Held, s.SentAt, s.TickAt, s.TickWall = false, base, base+301*leaseMillisecond, wall0+301*leaseMillisecond
			}),
			want: LeaseDecision{SendRequest: true, StepDown: true, Refusal: lapse}},
		{name: "tick: wall clock one heartbeat off is fine", cfg: cfg3,
			before: settled, ev: Tick{At: base + 100*leaseMillisecond, Wall: wall0 + 200*leaseMillisecond},
			after: leaseWith(settled, func(s *LeaseState) { s.TickAt, s.TickWall = base+100*leaseMillisecond, wall0+200*leaseMillisecond }),
			want:  LeaseDecision{Valid: true}},
		{name: "tick: wall clock stepped forward", cfg: cfg3,
			before: settled, ev: Tick{At: base + 100*leaseMillisecond, Wall: wall0 + 201*leaseMillisecond},
			after: leaseWith(settled, func(s *LeaseState) {
				s.Held, s.TickAt, s.TickWall = false, base+100*leaseMillisecond, wall0+201*leaseMillisecond
			}),
			want: LeaseDecision{StepDown: true, Refusal: lapse}},
		{name: "tick: wall clock stepped back", cfg: cfg3,
			before: settled, ev: Tick{At: base + 100*leaseMillisecond, Wall: wall0 - time.Second},
			after: leaseWith(settled, func(s *LeaseState) {
				s.Held, s.TickAt, s.TickWall = false, base+100*leaseMillisecond, wall0-time.Second
			}),
			want: LeaseDecision{StepDown: true, Refusal: lapse}},
		{name: "tick: no wall clock, no cross-check", cfg: noWall,
			before: settled, ev: Tick{At: base + 100*leaseMillisecond, Wall: wall0 + 10*time.Minute},
			after: leaseWith(settled, func(s *LeaseState) { s.TickAt, s.TickWall = base+100*leaseMillisecond, wall0+10*time.Minute }),
			want:  LeaseDecision{Valid: true}},
		{name: "tick: lease reaches Until", cfg: cfg3,
			before: leaseWith(settled, func(s *LeaseState) { s.Until = base + 100*leaseMillisecond }), ev: tickAt(base + 100*leaseMillisecond),
			after: leaseWith(settled, func(s *LeaseState) {
				s.Held, s.Until, s.TickAt, s.TickWall = false, base+100*leaseMillisecond, base+100*leaseMillisecond, wall0+100*leaseMillisecond
			}),
			want: LeaseDecision{StepDown: true, Refusal: lapse}},
		{name: "tick: one nanosecond before Until", cfg: cfg3,
			before: leaseWith(settled, func(s *LeaseState) { s.Until = base + 100*leaseMillisecond + 1 }), ev: tickAt(base + 100*leaseMillisecond),
			after: leaseWith(settled, func(s *LeaseState) {
				s.Until, s.TickAt, s.TickWall = base+100*leaseMillisecond+1, base+100*leaseMillisecond, wall0+100*leaseMillisecond
			}),
			want: LeaseDecision{Valid: true}},
		{name: "tick: valid, request due", cfg: cfg3,
			before: leaseWith(h, func(s *LeaseState) { s.SentAt = base }), ev: tickAt(base + 100*leaseMillisecond),
			after: leaseWith(h, func(s *LeaseState) {
				s.SentAt, s.TickAt, s.TickWall = base, base+100*leaseMillisecond, wall0+100*leaseMillisecond
			}),
			want: LeaseDecision{Valid: true, SendRequest: true}},
		{name: "tick: valid, request not due", cfg: cfg3,
			before: leaseWith(h, func(s *LeaseState) { s.SentAt = base + 1*leaseMillisecond }), ev: tickAt(base + 100*leaseMillisecond),
			after: leaseWith(h, func(s *LeaseState) {
				s.SentAt, s.TickAt, s.TickWall = base+1*leaseMillisecond, base+100*leaseMillisecond, wall0+100*leaseMillisecond
			}),
			want: LeaseDecision{Valid: true}},
		{name: "tick: not held, clock rules pass", cfg: cfg3,
			before: leaseWith(notHeld, func(s *LeaseState) { s.SentAt = base }), ev: tickAt(base + 100*leaseMillisecond),
			after: leaseWith(notHeld, func(s *LeaseState) {
				s.SentAt, s.TickAt, s.TickWall = base, base+100*leaseMillisecond, wall0+100*leaseMillisecond
			}),
			want: LeaseDecision{SendRequest: true, Refusal: lapse}},
		{name: "tick: at the renewal instant there is no progress since it", cfg: cfg3,
			before: leaseWith(settled, func(s *LeaseState) { s.RenewedAt = base + 100*leaseMillisecond }), ev: tickAt(base + 100*leaseMillisecond),
			after: leaseWith(settled, func(s *LeaseState) {
				s.Held, s.RenewedAt, s.TickAt, s.TickWall = false, base+100*leaseMillisecond, base+100*leaseMillisecond, wall0+100*leaseMillisecond
			}),
			want: LeaseDecision{StepDown: true, Refusal: lapse}},

		// --- RequestSent
		{name: "request sent records the time and nonce", cfg: cfg3,
			before: h, ev: RequestSent{At: base + 300*leaseMillisecond, Nonce: 8},
			after: leaseWith(h, func(s *LeaseState) { s.SentAt, s.Nonce = base+300*leaseMillisecond, 8 }),
			want:  LeaseDecision{Valid: true}},

		// --- GrantReceived
		{name: "grant: late grant for an older request", cfg: cfg3,
			before: h, ev: g(func(e *GrantReceived) { e.Nonce = 6 }), after: h, want: LeaseDecision{Valid: true}},
		{name: "grant: deposed leader's term", cfg: cfg3,
			before: h, ev: g(func(e *GrantReceived) { e.Term = 2 }), after: h, want: LeaseDecision{Valid: true}},
		{name: "grant: stale grantor's epoch is ignored, not refused", cfg: cfg3,
			before: h, ev: g(func(e *GrantReceived) { e.Epoch = 4 }), after: h, want: LeaseDecision{Valid: true}},
		{name: "grant: higher epoch stops a held node", cfg: cfg3,
			before: h, ev: g(func(e *GrantReceived) { e.Epoch = 6 }),
			after: leaseWith(h, func(s *LeaseState) { s.Held, s.Behind = false, true }),
			want:  LeaseDecision{StepDown: true, Behind: true, Refusal: lapse}},
		{name: "grant: higher epoch, node already not held", cfg: cfg3,
			before: notHeld, ev: g(func(e *GrantReceived) { e.Epoch = 6 }),
			after: leaseWith(notHeld, func(s *LeaseState) { s.Behind = true }),
			want:  LeaseDecision{Behind: true, Refusal: lapse}},
		{name: "grant: no progress since the last renewal", cfg: cfg3,
			before: leaseWith(h, func(s *LeaseState) { s.RenewedAt = base + 500*leaseMillisecond }), ev: grant,
			after: leaseWith(h, func(s *LeaseState) { s.RenewedAt = base + 500*leaseMillisecond }), want: LeaseDecision{Valid: true}},
		{name: "grant: arriving before the last renewal", cfg: cfg3,
			before: leaseWith(h, func(s *LeaseState) { s.RenewedAt = base + 600*leaseMillisecond }), ev: grant,
			after: leaseWith(h, func(s *LeaseState) { s.RenewedAt = base + 600*leaseMillisecond }), want: LeaseDecision{Valid: true}},
		{name: "grant: round trip equals step-down, held node", cfg: cfg3,
			before: h, ev: g(func(e *GrantReceived) { e.RTT = 2000 * leaseMillisecond }),
			after: leaseWith(h, func(s *LeaseState) { s.Held, s.Until = false, base+500*leaseMillisecond }),
			want:  LeaseDecision{StepDown: true, Refusal: lapse}},
		{name: "grant: round trip above step-down, node not held", cfg: cfg3,
			before: notHeld, ev: g(func(e *GrantReceived) { e.RTT = 3 * time.Second }),
			after: leaseWith(notHeld, func(s *LeaseState) { s.Until = base + 500*leaseMillisecond }),
			want:  LeaseDecision{Refusal: lapse}},
		{name: "grant: accepted, until counts from the receive time less the round trip", cfg: cfg3,
			before: h, ev: grant,
			after: leaseWith(h, func(s *LeaseState) {
				s.Until, s.RenewedAt, s.LastNonce = base+500*leaseMillisecond+1960*leaseMillisecond, base+500*leaseMillisecond, 7
			}),
			want: LeaseDecision{Valid: true}},
		{name: "grant: accepted from not held, behind, at a newer term", cfg: cfg3,
			before: leaseWith(notHeld, func(s *LeaseState) { s.Behind = true }), ev: g(func(e *GrantReceived) { e.Term = 4 }),
			after: leaseWith(h, func(s *LeaseState) {
				s.Until, s.RenewedAt, s.Term, s.LastNonce = base+500*leaseMillisecond+1960*leaseMillisecond, base+500*leaseMillisecond, 4, 7
			}),
			want: LeaseDecision{Valid: true}},
		{name: "grant: a round trip one millisecond under step-down leaves 1 leaseMillisecond", cfg: cfg3,
			before: h, ev: g(func(e *GrantReceived) { e.RTT = 1999 * leaseMillisecond }),
			after: leaseWith(h, func(s *LeaseState) {
				s.Until, s.RenewedAt, s.LastNonce = base+500*leaseMillisecond+1*leaseMillisecond, base+500*leaseMillisecond, 7
			}),
			want: LeaseDecision{Valid: true}},
		{name: "grant: the first grant ever (RenewedAt 0)", cfg: cfg3,
			before: LeaseState{Nonce: 7, Term: 3}, ev: grant,
			after: LeaseState{Held: true, Until: base + 500*leaseMillisecond + 1960*leaseMillisecond, RenewedAt: base + 500*leaseMillisecond, Nonce: 7, LastNonce: 7, Term: 3, Epoch: 5},
			want:  LeaseDecision{Valid: true}},
		{name: "grant: a duplicate of the nonce already accepted", cfg: cfg3,
			before: leaseWith(h, func(s *LeaseState) { s.LastNonce = 7 }), ev: grant,
			after: leaseWith(h, func(s *LeaseState) { s.LastNonce = 7 }), want: LeaseDecision{Valid: true}},
		{name: "grant: a nonce older than the last accepted one", cfg: cfg3,
			before: leaseWith(h, func(s *LeaseState) { s.LastNonce = 8 }), ev: grant,
			after: leaseWith(h, func(s *LeaseState) { s.LastNonce = 8 }), want: LeaseDecision{Valid: true}},
		{name: "grant: a negative round trip is rejected, nothing changes", cfg: cfg3,
			before: h, ev: g(func(e *GrantReceived) { e.RTT = -1 * leaseMillisecond }), after: h, want: LeaseDecision{Valid: true}},
		{name: "grant: a negative round trip does not even set behind", cfg: cfg3,
			before: h, ev: g(func(e *GrantReceived) { e.RTT, e.Epoch = -1*leaseMillisecond, 6 }), after: h, want: LeaseDecision{Valid: true}},
		{name: "grant: naming the committed epoch is accepted on a fresh state (epoch 0)", cfg: cfg3,
			before: LeaseState{Nonce: 7, Term: 3}, ev: g(func(e *GrantReceived) { e.Epoch, e.CommittedEpoch = 6, 6 }),
			after: LeaseState{Held: true, Until: base + 500*leaseMillisecond + 1960*leaseMillisecond, RenewedAt: base + 500*leaseMillisecond, Nonce: 7, LastNonce: 7, Term: 3, Epoch: 6},
			want:  LeaseDecision{Valid: true}},
		{name: "grant: naming the committed epoch after it moved from 5 to 6", cfg: cfg3,
			before: notHeld, ev: g(func(e *GrantReceived) { e.Epoch, e.CommittedEpoch = 6, 6 }),
			after: leaseWith(h, func(s *LeaseState) {
				s.Until, s.RenewedAt, s.LastNonce, s.Epoch = base+500*leaseMillisecond+1960*leaseMillisecond, base+500*leaseMillisecond, 7, 6
			}),
			want: LeaseDecision{Valid: true}},
		{name: "grant: naming 7 while committed is 6 stops a held node", cfg: cfg3,
			before: leaseWith(h, func(s *LeaseState) { s.Epoch = 6 }), ev: g(func(e *GrantReceived) { e.Epoch, e.CommittedEpoch = 7, 6 }),
			after: leaseWith(h, func(s *LeaseState) { s.Epoch, s.Held, s.Behind = 6, false, true }),
			want:  LeaseDecision{StepDown: true, Behind: true, Refusal: lapse}},
		{name: "grant: naming 5 while committed is 6 is a stale grantor", cfg: cfg3,
			before: leaseWith(notHeld, func(s *LeaseState) { s.Epoch = 6 }), ev: g(func(e *GrantReceived) { e.Epoch, e.CommittedEpoch = 5, 6 }),
			after: leaseWith(notHeld, func(s *LeaseState) { s.Epoch = 6 }), want: LeaseDecision{Refusal: lapse}},
		{name: "grant: naming the node's old grant epoch once committed moved is ignored", cfg: cfg3,
			before: h, ev: g(func(e *GrantReceived) { e.CommittedEpoch = 6 }), after: h, want: LeaseDecision{Valid: true}},

		// --- OwnQuorum
		{name: "own quorum: ok renews with a round trip of 0", cfg: cfg3,
			before: notHeld, ev: OwnQuorum{At: base + 500*leaseMillisecond, CommittedEpoch: 5, Term: 3, OK: true},
			after: leaseWith(h, func(s *LeaseState) {
				s.Until, s.RenewedAt = base+500*leaseMillisecond+2000*leaseMillisecond, base+500*leaseMillisecond
			}),
			want: LeaseDecision{Valid: true}},
		{name: "own quorum: ok at a newer term adopts it", cfg: cfg3,
			before: notHeld, ev: OwnQuorum{At: base + 500*leaseMillisecond, CommittedEpoch: 5, Term: 4, OK: true},
			after: leaseWith(h, func(s *LeaseState) {
				s.Until, s.RenewedAt, s.Term = base+500*leaseMillisecond+2000*leaseMillisecond, base+500*leaseMillisecond, 4
			}),
			want: LeaseDecision{Valid: true}},
		{name: "own quorum: ok clears behind like an accepted grant", cfg: cfg3,
			before: leaseWith(notHeld, func(s *LeaseState) { s.Behind = true }), ev: OwnQuorum{At: base + 500*leaseMillisecond, CommittedEpoch: 5, Term: 3, OK: true},
			after: leaseWith(h, func(s *LeaseState) {
				s.Until, s.RenewedAt = base+500*leaseMillisecond+2000*leaseMillisecond, base+500*leaseMillisecond
			}),
			want: LeaseDecision{Valid: true}},
		{name: "own quorum: ok at a lower term is ignored", cfg: cfg3,
			before: notHeld, ev: OwnQuorum{At: base + 500*leaseMillisecond, CommittedEpoch: 5, Term: 2, OK: true}, after: notHeld,
			want: LeaseDecision{Refusal: lapse}},
		{name: "own quorum: ok with no progress since the last renewal is ignored", cfg: cfg3,
			before: leaseWith(h, func(s *LeaseState) { s.RenewedAt = base + 500*leaseMillisecond }), ev: OwnQuorum{At: base + 500*leaseMillisecond, CommittedEpoch: 5, Term: 3, OK: true},
			after: leaseWith(h, func(s *LeaseState) { s.RenewedAt = base + 500*leaseMillisecond }), want: LeaseDecision{Valid: true}},
		{name: "own quorum: its round trip shortens the lease to the round's start plus step_down", cfg: cfg3,
			before: notHeld, ev: OwnQuorum{At: base + 500*leaseMillisecond, CommittedEpoch: 5, Term: 3, OK: true, RTT: 40 * leaseMillisecond},
			after: leaseWith(h, func(s *LeaseState) {
				s.Until, s.RenewedAt = base+460*leaseMillisecond+2000*leaseMillisecond, base+500*leaseMillisecond
			}),
			want: LeaseDecision{Valid: true}},
		{name: "own quorum: a round trip of step_down voids the lease", cfg: cfg3,
			before: h, ev: OwnQuorum{At: base + 500*leaseMillisecond, CommittedEpoch: 5, Term: 3, OK: true, RTT: 2000 * leaseMillisecond},
			after: leaseWith(notHeld, func(s *LeaseState) { s.Until = base + 500*leaseMillisecond }),
			want:  LeaseDecision{StepDown: true, Refusal: lapse}},
		{name: "own quorum: a negative round trip is ignored", cfg: cfg3,
			before: notHeld, ev: OwnQuorum{At: base + 500*leaseMillisecond, CommittedEpoch: 5, Term: 3, OK: true, RTT: -leaseMillisecond}, after: notHeld,
			want: LeaseDecision{Refusal: lapse}},
		{name: "own quorum: failed, held node steps down", cfg: cfg3,
			before: h, ev: OwnQuorum{At: base + 500*leaseMillisecond, CommittedEpoch: 5, Term: 3},
			after: notHeld, want: LeaseDecision{StepDown: true, Refusal: lapse}},
		{name: "own quorum: failed, node not held", cfg: cfg3,
			before: notHeld, ev: OwnQuorum{At: base + 500*leaseMillisecond, CommittedEpoch: 5, Term: 3}, after: notHeld,
			want: LeaseDecision{Refusal: lapse}},

		{name: "own quorum: ok takes the committed epoch as the grant's", cfg: cfg3,
			before: LeaseState{Term: 3}, ev: OwnQuorum{At: base + 500*leaseMillisecond, CommittedEpoch: 6, Term: 3, OK: true},
			after: LeaseState{Held: true, Until: base + 500*leaseMillisecond + 2000*leaseMillisecond, RenewedAt: base + 500*leaseMillisecond, Term: 3, Epoch: 6},
			want:  LeaseDecision{Valid: true}},

		// --- ServeCheck
		{name: "serve check: epoch moved past the grant", cfg: cfg3,
			before: h, ev: ServeCheck{At: base + 500*leaseMillisecond, CommittedEpoch: 6}, after: notHeld,
			want: LeaseDecision{StepDown: true, Refusal: lapse}},
		{name: "serve check: epoch moved, node not held", cfg: cfg3,
			before: notHeld, ev: ServeCheck{At: base + 500*leaseMillisecond, CommittedEpoch: 6}, after: notHeld,
			want: LeaseDecision{Refusal: lapse}},
		{name: "serve check: live lease", cfg: cfg3,
			before: h, ev: ServeCheck{At: base + time.Second, CommittedEpoch: 5}, after: h, want: LeaseDecision{Valid: true}},
		{name: "serve check: at Until the lease is over", cfg: cfg3,
			before: h, ev: ServeCheck{At: base + 2*time.Second, CommittedEpoch: 5}, after: h,
			want: LeaseDecision{Refusal: lapse}},
		{name: "serve check: one nanosecond before Until", cfg: cfg3,
			before: h, ev: ServeCheck{At: base + 2*time.Second - 1, CommittedEpoch: 5}, after: h, want: LeaseDecision{Valid: true}},
		{name: "serve check: not held", cfg: cfg3,
			before: notHeld, ev: ServeCheck{At: base + 500*leaseMillisecond, CommittedEpoch: 5}, after: notHeld,
			want: LeaseDecision{Refusal: lapse}},
	}
}

// TestLeaseDecisionTable runs every line of the brief's decision table (plus
// the readings recorded in lease.go for the cells the table is silent on).
func TestLeaseDecisionTable(t *testing.T) {
	for _, row := range leaseRows() {
		t.Run(row.name, func(t *testing.T) {
			s := row.before
			got := s.Apply(row.ev, row.cfg)
			// The rows state the rule's outcome; the lapse record each one also leaves is
			// checked by TestALapseRecordsWhenAndWhy and by the reference model.
			got.Cause, got.LapsedAt, got.Ended = LapseNone, 0, false
			if got != row.want {
				t.Errorf("decision = %+v, want %+v", got, row.want)
			}
			if ruleState(s) != row.after {
				t.Errorf("state = %+v, want %+v", ruleState(s), row.after)
			}
		})
	}
}

// settle drives a fresh node through its first tick, one request and one
// accepted grant, and returns it holding a lease granted at base+300ms.
func settle(t *testing.T) LeaseState {
	t.Helper()
	s := LeaseState{Term: 3}
	mustApply(t, &s, tickAt(base), cfg3)
	mustApply(t, &s, tickAt(base+100*leaseMillisecond), cfg3)
	mustApply(t, &s, RequestSent{At: base + 100*leaseMillisecond, Nonce: 11}, cfg3)
	mustApply(t, &s, tickAt(base+200*leaseMillisecond), cfg3)
	mustApply(t, &s, tickAt(base+300*leaseMillisecond), cfg3)
	d := mustApply(t, &s, GrantReceived{At: base + 300*leaseMillisecond, Wall: wall0 + 300*leaseMillisecond, Nonce: 11, Epoch: 5, CommittedEpoch: 5, Term: 3, RTT: 100 * leaseMillisecond}, cfg3)
	if !d.Valid || !s.Held {
		t.Fatalf("setup: the grant was not accepted: %+v %+v", d, s)
	}
	return s
}

func TestLeaseUsesBootTimeAndObservedProgress(t *testing.T) {
	s := settle(t)
	if s.RenewedAt != base+300*leaseMillisecond {
		t.Errorf("RenewedAt = %v, want the receive time %v", s.RenewedAt, base+300*leaseMillisecond)
	}
	// Request went out at base+100ms; the lease counts from the receive time
	// base+300ms, less the 100 leaseMillisecond round trip.
	if want := base + 300*leaseMillisecond + 1900*leaseMillisecond; s.Until != want {
		t.Errorf("Until = %v, want %v", s.Until, want)
	}
	// Every tick that observed progress keeps it valid, up to Until.
	until := s.Until
	for at := base + 400*leaseMillisecond; at < until; at += 100 * leaseMillisecond {
		if d := mustApply(t, &s, tickAt(at), cfg3); !d.Valid || d.StepDown {
			t.Fatalf("tick at %v: %+v, want valid", at, d)
		}
	}
	d := mustApply(t, &s, tickAt(until), cfg3)
	if d.Valid || !d.StepDown || d.Refusal != lapse || s.Held {
		t.Errorf("tick at Until: %+v held=%v, want a step-down", d, s.Held)
	}
}

func TestLeaderOwnsItsOwnLease(t *testing.T) {
	s := LeaseState{Term: 3}
	mustApply(t, &s, tickAt(base), cfg3)
	if d := mustApply(t, &s, OwnQuorum{At: base + 100*leaseMillisecond, CommittedEpoch: 5, Term: 3, OK: true}, cfg3); !d.Valid {
		t.Fatalf("own quorum ok: %+v, want valid", d)
	}
	// With no further quorum rounds the leader's lease runs out at Until, even
	// though its own clock keeps ticking: liveness alone renews nothing.
	until := s.Until
	for at := base + 200*leaseMillisecond; at < until; at += 100 * leaseMillisecond {
		if d := mustApply(t, &s, tickAt(at), cfg3); !d.Valid {
			t.Fatalf("tick at %v: %+v, want valid before Until", at, d)
		}
	}
	d := mustApply(t, &s, tickAt(until), cfg3)
	if d.Valid || !d.StepDown {
		t.Errorf("tick at Until with no quorum round: %+v, want a step-down", d)
	}
	// A quorum round renews it, and a failed one ends it at once.
	mustApply(t, &s, OwnQuorum{At: until + 100*leaseMillisecond, CommittedEpoch: 5, Term: 3, OK: true}, cfg3)
	if !s.Held {
		t.Fatalf("own quorum ok after a lapse did not renew")
	}
	d = mustApply(t, &s, OwnQuorum{At: until + 200*leaseMillisecond, CommittedEpoch: 5, Term: 3}, cfg3)
	if d.Valid || !d.StepDown || d.Refusal != lapse {
		t.Errorf("failed own quorum: %+v, want a step-down with LEASE_LAPSED", d)
	}
}

func TestSingleNodeHoldsNoLease(t *testing.T) {
	one := cfg3
	one.Members = 1
	var s LeaseState
	events := []LeaseEvent{
		tickAt(base), tickAt(base), tickAt(base - time.Second), tickAt(base + time.Hour),
		Tick{At: base + time.Hour + 100*leaseMillisecond, Wall: 0},
		GrantReceived{At: base, Nonce: 9, Epoch: 2, Term: 1, RTT: time.Hour},
		OwnQuorum{At: base},
		ServeCheck{At: base, CommittedEpoch: 99},
		RequestSent{At: base, Nonce: 1},
	}
	for i, ev := range events {
		d := s.Apply(ev, one)
		if d != (LeaseDecision{Valid: true}) {
			t.Errorf("event %d (%T): %+v, want exactly Valid", i, ev, d)
		}
		if s != (LeaseState{}) {
			t.Errorf("event %d (%T): state changed to %+v", i, ev, s)
		}
	}
}

func TestGrantWhenRTTExceedsStepDownIsExpired(t *testing.T) {
	for _, rtt := range []time.Duration{2000 * leaseMillisecond, 2001 * leaseMillisecond, 10 * time.Second} {
		s := settle(t)
		at := base + 400*leaseMillisecond
		mustApply(t, &s, RequestSent{At: at, Nonce: 12}, cfg3)
		d := mustApply(t, &s, GrantReceived{At: at, Wall: wall0 + 400*leaseMillisecond, Nonce: 12, Epoch: 5, CommittedEpoch: 5, Term: 3, RTT: rtt}, cfg3)
		if d.Valid || s.Held || s.Until != at {
			t.Errorf("rtt %v: %+v held=%v until=%v, want an expired lease", rtt, d, s.Held, s.Until)
		}
		if c := mustApply(t, &s, ServeCheck{At: at, CommittedEpoch: 5}, cfg3); c.Valid {
			t.Errorf("rtt %v: a serve check right after the grant was valid", rtt)
		}
	}
}

func TestLateGrantIsDroppedByNonce(t *testing.T) {
	s := LeaseState{Term: 3}
	mustApply(t, &s, tickAt(base), cfg3)
	mustApply(t, &s, RequestSent{At: base + 100*leaseMillisecond, Nonce: 1}, cfg3)
	mustApply(t, &s, RequestSent{At: base + 200*leaseMillisecond, Nonce: 2}, cfg3)
	before := s
	// Same epoch and term as the node, so only the nonce can reject it.
	d := mustApply(t, &s, GrantReceived{At: base + 250*leaseMillisecond, Wall: wall0 + 250*leaseMillisecond, Nonce: 1, Epoch: 5, CommittedEpoch: 5, Term: 3, RTT: 10 * leaseMillisecond}, cfg3)
	if d.Valid || s != before {
		t.Errorf("late grant: %+v state %+v, want ignored (state %+v)", d, s, before)
	}
	d = mustApply(t, &s, GrantReceived{At: base + 260*leaseMillisecond, Wall: wall0 + 260*leaseMillisecond, Nonce: 2, Epoch: 5, CommittedEpoch: 5, Term: 3, RTT: 10 * leaseMillisecond}, cfg3)
	if !d.Valid || !s.Held {
		t.Errorf("grant for the current nonce: %+v held=%v, want accepted", d, s.Held)
	}
}

func TestEpochAdvanceVoidsTheGrant(t *testing.T) {
	s := settle(t)
	if d := mustApply(t, &s, ServeCheck{At: base + 500*leaseMillisecond, CommittedEpoch: 5}, cfg3); !d.Valid {
		t.Fatalf("same epoch: %+v, want valid", d)
	}
	d := mustApply(t, &s, ServeCheck{At: base + 600*leaseMillisecond, CommittedEpoch: 6}, cfg3)
	if d.Valid || !d.StepDown || d.Refusal != lapse || s.Held {
		t.Errorf("epoch 6 against a grant at 5: %+v held=%v, want a step-down", d, s.Held)
	}
	// The grant stays void: asking again at the old epoch does not revive it.
	if d := mustApply(t, &s, ServeCheck{At: base + 700*leaseMillisecond, CommittedEpoch: 5}, cfg3); d.Valid {
		t.Errorf("a voided grant came back: %+v", d)
	}
}

func TestATickThatDoesNotAdvanceTheClockVoidsTheLease(t *testing.T) {
	// One nanosecond of progress is enough ...
	s := settle(t)
	mustApply(t, &s, tickAt(base+400*leaseMillisecond), cfg3)
	if d := mustApply(t, &s, Tick{At: base + 400*leaseMillisecond + 1, Wall: wall0 + 400*leaseMillisecond + 1}, cfg3); !d.Valid {
		t.Errorf("a 1 ns advance: %+v, want valid", d)
	}
	// ... none is not, and neither is going back.
	for _, name := range []string{"same reading", "reading went back"} {
		s := settle(t)
		mustApply(t, &s, tickAt(base+400*leaseMillisecond), cfg3)
		at := base + 400*leaseMillisecond
		if name == "reading went back" {
			at -= 50 * leaseMillisecond
		}
		d := mustApply(t, &s, tickAt(at), cfg3)
		if d.Valid || !d.StepDown || d.Refusal != lapse || s.Held {
			t.Errorf("%s: %+v held=%v, want a step-down", name, d, s.Held)
		}
		if s.TickAt != base+400*leaseMillisecond {
			t.Errorf("%s: TickAt = %v, want the high-water mark %v", name, s.TickAt, base+400*leaseMillisecond)
		}
	}
}

func TestWallClockStepVoidsTheLease(t *testing.T) {
	for _, step := range []time.Duration{time.Hour, -time.Hour, 101 * leaseMillisecond} {
		s := settle(t)
		mustApply(t, &s, tickAt(base+400*leaseMillisecond), cfg3)
		d := mustApply(t, &s, Tick{At: base + 500*leaseMillisecond, Wall: wall0 + 500*leaseMillisecond + step}, cfg3)
		if d.Valid || !d.StepDown || d.Refusal != lapse || s.Held {
			t.Errorf("wall step %v: %+v held=%v, want a step-down", step, d, s.Held)
		}
	}
	// The same tick with no wall clock to compare passes.
	noWall := cfg3
	noWall.WallClock = false
	s := settle(t)
	mustApply(t, &s, tickAt(base+400*leaseMillisecond), noWall)
	if d := mustApply(t, &s, Tick{At: base + 500*leaseMillisecond, Wall: wall0 + time.Hour}, noWall); !d.Valid {
		t.Errorf("no wall clock: %+v, want valid", d)
	}
}

func TestHigherEpochGrantStopsServing(t *testing.T) {
	s := settle(t)
	mustApply(t, &s, RequestSent{At: base + 400*leaseMillisecond, Nonce: 12}, cfg3)
	d := mustApply(t, &s, GrantReceived{At: base + 500*leaseMillisecond, Wall: wall0 + 500*leaseMillisecond, Nonce: 12, Epoch: 6, CommittedEpoch: 5, Term: 3, RTT: 10 * leaseMillisecond}, cfg3)
	if d.Valid || !d.StepDown || !d.Behind || d.Refusal != lapse || s.Held || !s.Behind {
		t.Fatalf("higher-epoch grant: %+v state %+v, want stop, behind, LEASE_LAPSED", d, s)
	}
	if s.LastNonce != 11 {
		t.Errorf("LastNonce = %d, a refused grant must not consume its nonce", s.LastNonce)
	}
	if c := mustApply(t, &s, ServeCheck{At: base + 510*leaseMillisecond, CommittedEpoch: 6}, cfg3); c.Valid {
		t.Errorf("served after a higher-epoch grant: %+v", c)
	}
	// Once the node has applied epoch 6 itself, a grant at 6 is a normal one:
	// it is accepted and ends behind, with nothing written by the caller.
	mustApply(t, &s, RequestSent{At: base + 520*leaseMillisecond, Nonce: 13}, cfg3)
	d = mustApply(t, &s, GrantReceived{At: base + 600*leaseMillisecond, Wall: wall0 + 600*leaseMillisecond, Nonce: 13, Epoch: 6, CommittedEpoch: 6, Term: 3, RTT: 10 * leaseMillisecond}, cfg3)
	if !d.Valid || d.Behind || s.Behind || !s.Held || s.Epoch != 6 {
		t.Errorf("grant at the committed epoch: %+v state %+v, want accepted", d, s)
	}
}

func TestFirstGrantOfAFreshStateIsAccepted(t *testing.T) {
	s := LeaseState{Term: 3} // Epoch 0: the caller presets nothing
	mustApply(t, &s, RequestSent{At: base, Nonce: 1}, cfg3)
	d := mustApply(t, &s, GrantReceived{At: base + 50*leaseMillisecond, Wall: wall0 + 50*leaseMillisecond, Nonce: 1, Epoch: 5, CommittedEpoch: 5, Term: 3, RTT: 10 * leaseMillisecond}, cfg3)
	if !d.Valid || !s.Held || s.Epoch != 5 || d.Behind {
		t.Errorf("first grant: %+v state %+v, want accepted at epoch 5", d, s)
	}
	if c := mustApply(t, &s, ServeCheck{At: base + 60*leaseMillisecond, CommittedEpoch: 5}, cfg3); !c.Valid {
		t.Errorf("serve check after the first grant: %+v", c)
	}
}

func TestGrantAfterTheCommittedEpochMovesIsAccepted(t *testing.T) {
	s := settle(t)
	if d := mustApply(t, &s, ServeCheck{At: base + 400*leaseMillisecond, CommittedEpoch: 6}, cfg3); d.Valid || !d.StepDown {
		t.Fatalf("committed epoch moved 5 to 6: %+v, want the lease voided", d)
	}
	mustApply(t, &s, RequestSent{At: base + 410*leaseMillisecond, Nonce: 12}, cfg3)
	d := mustApply(t, &s, GrantReceived{At: base + 450*leaseMillisecond, Wall: wall0 + 450*leaseMillisecond, Nonce: 12, Epoch: 6, CommittedEpoch: 6, Term: 3, RTT: 10 * leaseMillisecond}, cfg3)
	if !d.Valid || !s.Held || s.Epoch != 6 || s.Behind {
		t.Errorf("grant naming 6: %+v state %+v, want accepted", d, s)
	}
	if c := mustApply(t, &s, ServeCheck{At: base + 460*leaseMillisecond, CommittedEpoch: 6}, cfg3); !c.Valid {
		t.Errorf("serve check at 6 after the grant: %+v", c)
	}
}

func TestGrantAheadOfTheCommittedEpochStopsServing(t *testing.T) {
	s := settle(t)
	mustApply(t, &s, ServeCheck{At: base + 400*leaseMillisecond, CommittedEpoch: 6}, cfg3)
	mustApply(t, &s, RequestSent{At: base + 410*leaseMillisecond, Nonce: 12}, cfg3)
	mustApply(t, &s, GrantReceived{At: base + 450*leaseMillisecond, Wall: wall0 + 450*leaseMillisecond, Nonce: 12, Epoch: 6, CommittedEpoch: 6, Term: 3, RTT: 10 * leaseMillisecond}, cfg3)
	mustApply(t, &s, RequestSent{At: base + 460*leaseMillisecond, Nonce: 13}, cfg3)
	until := s.Until
	d := mustApply(t, &s, GrantReceived{At: base + 500*leaseMillisecond, Wall: wall0 + 500*leaseMillisecond, Nonce: 13, Epoch: 7, CommittedEpoch: 6, Term: 3, RTT: 10 * leaseMillisecond}, cfg3)
	if d.Valid || !d.StepDown || !d.Behind || s.Held || !s.Behind || s.Until != until {
		t.Errorf("grant naming 7 while committed is 6: %+v state %+v, want stop with no extension", d, s)
	}
}

func TestGrantBehindTheCommittedEpochIsIgnored(t *testing.T) {
	s := settle(t)
	mustApply(t, &s, ServeCheck{At: base + 400*leaseMillisecond, CommittedEpoch: 6}, cfg3)
	mustApply(t, &s, RequestSent{At: base + 410*leaseMillisecond, Nonce: 12}, cfg3)
	before := s
	d := mustApply(t, &s, GrantReceived{At: base + 450*leaseMillisecond, Wall: wall0 + 450*leaseMillisecond, Nonce: 12, Epoch: 5, CommittedEpoch: 6, Term: 3, RTT: 10 * leaseMillisecond}, cfg3)
	if d.Valid || d.Behind || d.StepDown || s != before {
		t.Errorf("grant naming 5 while committed is 6: %+v state %+v, want ignored (%+v)", d, s, before)
	}
}

func TestAnAcceptedGrantConsumesItsNonce(t *testing.T) {
	s := settle(t) // accepted the grant for nonce 11
	mustApply(t, &s, tickAt(base+400*leaseMillisecond), cfg3)
	before := s
	// The same grant again, later: it would extend the lease if replayed.
	d := mustApply(t, &s, GrantReceived{At: base + 450*leaseMillisecond, Wall: wall0 + 450*leaseMillisecond, Nonce: 11, Epoch: 5, CommittedEpoch: 5, Term: 3, RTT: 0}, cfg3)
	if s != before || !d.Valid {
		t.Errorf("replayed nonce 11: %+v state %+v, want ignored (%+v)", d, s, before)
	}
	// A request numbered below the last accepted one is not newer either.
	mustApply(t, &s, RequestSent{At: base + 460*leaseMillisecond, Nonce: 9}, cfg3)
	before = s
	mustApply(t, &s, GrantReceived{At: base + 470*leaseMillisecond, Wall: wall0 + 470*leaseMillisecond, Nonce: 9, Epoch: 5, CommittedEpoch: 5, Term: 3, RTT: 0}, cfg3)
	if s != before {
		t.Errorf("nonce 9 below the last accepted 11: state changed to %+v", s)
	}
	// A newer nonce is accepted.
	mustApply(t, &s, RequestSent{At: base + 480*leaseMillisecond, Nonce: 12}, cfg3)
	mustApply(t, &s, GrantReceived{At: base + 490*leaseMillisecond, Wall: wall0 + 490*leaseMillisecond, Nonce: 12, Epoch: 5, CommittedEpoch: 5, Term: 3, RTT: 0}, cfg3)
	if s.LastNonce != 12 || s.RenewedAt != base+490*leaseMillisecond {
		t.Errorf("nonce 12: state %+v, want accepted", s)
	}
}

func TestNegativeRTTGrantIsRejected(t *testing.T) {
	s := settle(t)
	mustApply(t, &s, RequestSent{At: base + 400*leaseMillisecond, Nonce: 12}, cfg3)
	before := s
	d := mustApply(t, &s, GrantReceived{At: base + 450*leaseMillisecond, Wall: wall0 + 450*leaseMillisecond, Nonce: 12, Epoch: 5, CommittedEpoch: 5, Term: 3, RTT: -time.Hour}, cfg3)
	if s != before || !d.Valid {
		t.Errorf("negative RTT: %+v state %+v, want rejected with no change (%+v)", d, s, before)
	}
	// On a node with no lease it must not create one either.
	f := LeaseState{Term: 3}
	mustApply(t, &f, RequestSent{At: base, Nonce: 1}, cfg3)
	d = mustApply(t, &f, GrantReceived{At: base + 50*leaseMillisecond, Wall: wall0 + 50*leaseMillisecond, Nonce: 1, Epoch: 5, CommittedEpoch: 5, Term: 3, RTT: -1}, cfg3)
	if d.Valid || f.Held || f.Until != 0 {
		t.Errorf("negative RTT on a fresh node: %+v state %+v, want no lease", d, f)
	}
}

func TestAFailedTickDoesNotLockTheLeaseOut(t *testing.T) {
	s := settle(t)
	mustApply(t, &s, tickAt(base+400*leaseMillisecond), cfg3)
	// A 10 s pause voids the lease; the ticks after it are 100 leaseMillisecond apart and a
	// new grant must be accepted and survive them.
	resume := base + 400*leaseMillisecond + 10*time.Second
	if d := mustApply(t, &s, tickAt(resume), cfg3); d.Valid || !d.StepDown {
		t.Fatalf("after the pause: %+v, want a step-down", d)
	}
	mustApply(t, &s, tickAt(resume+100*leaseMillisecond), cfg3)
	mustApply(t, &s, RequestSent{At: resume + 100*leaseMillisecond, Nonce: 12}, cfg3)
	if d := mustApply(t, &s, GrantReceived{At: resume + 150*leaseMillisecond, Wall: wall0 + resume - base + 150*leaseMillisecond, Nonce: 12, Epoch: 5, CommittedEpoch: 5, Term: 3, RTT: 20 * leaseMillisecond}, cfg3); !d.Valid {
		t.Fatalf("grant after the pause: %+v, want accepted", d)
	}
	if d := mustApply(t, &s, tickAt(resume+200*leaseMillisecond), cfg3); !d.Valid {
		t.Errorf("tick after the new grant: %+v, want valid (TickAt must follow a failed tick)", d)
	}
}

func TestAGrantFromALowerTermOrEpochNeverExtends(t *testing.T) {
	for _, name := range []string{"lower term", "lower epoch"} {
		s := settle(t)
		mustApply(t, &s, RequestSent{At: base + 800*leaseMillisecond, Nonce: 12}, cfg3)
		before := s
		ev := GrantReceived{At: base + 900*leaseMillisecond, Wall: wall0 + 900*leaseMillisecond, Nonce: 12, Epoch: 5, CommittedEpoch: 5, Term: 3, RTT: 0}
		if name == "lower term" {
			ev.Term = 2
		} else {
			ev.Epoch = 4
		}
		mustApply(t, &s, ev, cfg3)
		if s != before {
			t.Errorf("%s: state changed from %+v to %+v", name, before, s)
		}
	}
}

func TestLeaseSendsARequestWhenOneIsDue(t *testing.T) {
	s := LeaseState{Term: 3}
	if d := mustApply(t, &s, tickAt(base), cfg3); !d.SendRequest {
		t.Errorf("a node with no lease and no request must ask for one: %+v", d)
	}
	mustApply(t, &s, RequestSent{At: base, Nonce: 1}, cfg3)
	if d := mustApply(t, &s, tickAt(base+50*leaseMillisecond), cfg3); d.SendRequest {
		t.Errorf("50 leaseMillisecond after a request: %+v, want no second request", d)
	}
	if d := mustApply(t, &s, tickAt(base+100*leaseMillisecond), cfg3); !d.SendRequest {
		t.Errorf("a heartbeat after a request: %+v, want a new request", d)
	}
}

// ---------------------------------------------------------------------------
// The model test: 20 fixed seeds of random events against a reference that is
// the decision table written out as straight-line code.

// refLease is the trivially correct reference: one pure function from (state,
// event) to (state, decision, row label), written row by row from the table.
func refLease(s LeaseState, ev LeaseEvent, cfg LeaseConfig) (LeaseState, LeaseDecision, string) {
	if cfg.Members == 1 {
		return s, LeaseDecision{Valid: true}, "single"
	}
	n := s
	// record opens a lapse for a lease that was held; the first cause of a lapse stays.
	record := func(cause LapseCause, at time.Duration) {
		if s.Held && n.Cause == LapseNone {
			n.Cause, n.LapsedAt = cause, at
		}
	}
	// Whatever the event, a held lease whose time has come is expired, at Until.
	var at time.Duration
	switch e := ev.(type) {
	case Tick:
		at = e.At
	case RequestSent:
		at = e.At
	case GrantReceived:
		at = e.At
	case OwnQuorum:
		at = e.At
	case ServeCheck:
		at = e.At
	}
	if s.Held && at >= s.Until {
		record(LapseExpired, s.Until)
	}
	lapsedFor := func(valid bool) LeaseDecision {
		d := LeaseDecision{Valid: valid, StepDown: s.Held && !n.Held}
		if !valid {
			d.Refusal = lapse
			d.Cause, d.LapsedAt = n.Cause, n.LapsedAt
			if n.Cause == LapseNone {
				d.Cause = LapseNeverHeld
			}
		}
		return d
	}
	// accepted is the decision of a renewal; it ends the lapse the lease was in, if any.
	accepted := func() LeaseDecision {
		d := lapsedFor(true)
		if !s.Held || n.Cause != LapseNone {
			d.Ended, d.Cause, d.LapsedAt = true, n.Cause, n.LapsedAt
			if n.Cause == LapseNone {
				d.Cause = LapseNeverHeld
			}
		}
		n.Cause, n.LapsedAt = LapseNone, 0
		return d
	}
	liveAt := func(at time.Duration) bool { return n.Held && at < n.Until }
	switch e := ev.(type) {
	case Tick:
		label := "tick/ok"
		cause := LapseNone
		if s.TickAt != 0 {
			gap := e.At - s.TickAt
			wallOff := (e.Wall - s.TickWall) - gap
			if wallOff < 0 {
				wallOff = -wallOff
			}
			switch {
			case gap <= 0:
				label, cause = "tick/stopped", LapseClockStopped
			case gap > 3*cfg.Heartbeat:
				label, cause = "tick/gap", LapseTickGap
			case cfg.WallClock && wallOff > cfg.Heartbeat:
				label, cause = "tick/wall", LapseWallClock
			}
		}
		if cause != LapseNone {
			record(cause, e.At)
		}
		if label != "tick/stopped" {
			n.TickAt, n.TickWall = e.At, e.Wall
		}
		valid := label == "tick/ok" && s.Held && e.At < s.Until && e.At > s.RenewedAt
		if label == "tick/ok" && s.Held && !valid {
			label = "tick/expired"
			record(LapseNoProgress, e.At) // expiry was recorded first; this is a tick not after the renewal
		}
		if label == "tick/ok" && !s.Held {
			label = "tick/idle"
		}
		n.Held = valid
		d := lapsedFor(valid)
		d.SendRequest = e.At-s.SentAt >= cfg.Heartbeat
		return n, d, label
	case RequestSent:
		n.SentAt, n.Nonce = e.At, e.Nonce
		return n, lapsedFor(liveAt(e.At)), "request"
	case GrantReceived:
		switch {
		case e.Nonce != s.Nonce:
			return n, lapsedFor(liveAt(e.At)), "grant/nonce"
		case e.Nonce <= s.LastNonce:
			return n, lapsedFor(liveAt(e.At)), "grant/replay"
		case e.Term < s.Term:
			return n, lapsedFor(liveAt(e.At)), "grant/term"
		case e.RTT < 0:
			return n, lapsedFor(liveAt(e.At)), "grant/rtt-negative"
		case e.Epoch < e.CommittedEpoch:
			return n, lapsedFor(liveAt(e.At)), "grant/epoch-lower"
		case e.Epoch > e.CommittedEpoch:
			record(LapseEpoch, e.At)
			n.Held, n.Behind = false, true
			d := lapsedFor(false)
			d.Behind = true
			return n, d, "grant/epoch-higher"
		case s.RenewedAt != 0 && e.At <= s.RenewedAt:
			return n, lapsedFor(liveAt(e.At)), "grant/no-progress"
		case e.RTT >= cfg.StepDown:
			record(LapseRoundTooSlow, e.At)
			n.Held, n.Until = false, e.At
			return n, lapsedFor(false), "grant/rtt"
		}
		n.Held, n.Until, n.RenewedAt = true, e.At+(cfg.StepDown-e.RTT), e.At
		n.Epoch, n.Term, n.Behind, n.LastNonce = e.Epoch, e.Term, false, e.Nonce
		return n, accepted(), "grant/accepted"
	case OwnQuorum:
		switch {
		case !e.OK:
			record(LapseQuorumFailed, e.At)
			n.Held = false
			return n, lapsedFor(false), "own/fail"
		case e.Term < s.Term:
			return n, lapsedFor(liveAt(e.At)), "own/term"
		case e.RTT < 0:
			return n, lapsedFor(liveAt(e.At)), "own/rtt-negative"
		case s.RenewedAt != 0 && e.At <= s.RenewedAt:
			return n, lapsedFor(liveAt(e.At)), "own/no-progress"
		case e.RTT >= cfg.StepDown:
			record(LapseRoundTooSlow, e.At)
			n.Held, n.Until = false, e.At
			return n, lapsedFor(false), "own/rtt"
		}
		n.Held, n.Until, n.RenewedAt, n.Term, n.Behind, n.Epoch = true, e.At+(cfg.StepDown-e.RTT), e.At, e.Term, false, e.CommittedEpoch
		return n, accepted(), "own/ok"
	case ServeCheck:
		if e.CommittedEpoch != s.Epoch {
			record(LapseEpoch, e.At)
			n.Held = false
			return n, lapsedFor(false), "serve/epoch"
		}
		return n, lapsedFor(liveAt(e.At)), "serve/ok"
	}
	return n, lapsedFor(false), "unknown"
}

// ruleState is the state without its lapse record, which only describes the rule's outcome.
func ruleState(s LeaseState) LeaseState {
	s.Cause, s.LapsedAt = LapseNone, 0
	return s
}

func pick(rng *rand.Rand, xs ...int) int { return xs[rng.Intn(len(xs))] }

// runLeaseSeed drives the machine and the reference with one seed's events
// and checks the four invariants after every step.
func runLeaseSeed(t *testing.T, seed int64, rows map[string]int) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	cfg := cfg3
	cfg.WallClock = seed%2 == 0
	machine := LeaseState{Term: 3} // Epoch 0: the caller presets nothing
	model := machine
	now, wall := base, wall0
	nonce, committed := uint64(0), uint64(5)
	clockBadSinceRenewal := false

	for step := 0; step < 600; step++ {
		var ev LeaseEvent
		switch r := rng.Intn(100); {
		case r < 33: // a normal tick, one heartbeat with jitter
			dt := 100*leaseMillisecond + time.Duration(rng.Intn(21)-10)*leaseMillisecond
			now, wall = now+dt, wall+dt+time.Duration(rng.Intn(11)-5)*leaseMillisecond
			ev = Tick{At: now, Wall: wall}
		case r < 38: // a pause: a long gap on both clocks, across the three-heartbeat bound or past it
			dt := 201*leaseMillisecond + time.Duration(rng.Intn(4000))*leaseMillisecond
			now, wall = now+dt, wall+dt
			ev = Tick{At: now, Wall: wall}
		case r < 41: // a frozen guest: wall moves, boot clock does not
			wall += time.Duration(1+rng.Intn(3000)) * leaseMillisecond
			now += time.Duration(pick(rng, 0, 0, 1)) * leaseMillisecond
			ev = Tick{At: now, Wall: wall}
		case r < 45: // a clock jump, either way, then a tick that sees it
			jump := time.Duration(pick(rng, -2000, -300, -50, 50, 300, 2000)) * leaseMillisecond
			now, wall = now+jump, wall+jump*time.Duration(pick(rng, 0, 1))
			if now <= 0 {
				now = 100 * leaseMillisecond
			}
			ev = Tick{At: now, Wall: wall}
		case r < 58: // a request goes out
			now += time.Duration(rng.Intn(50)) * leaseMillisecond
			nonce++
			ev = RequestSent{At: now, Nonce: nonce}
		case r < 80: // a grant, usually for the latest request and the right epoch and term
			now += time.Duration(rng.Intn(60)) * leaseMillisecond
			wall += 30 * leaseMillisecond
			n := nonce
			switch x := rng.Intn(100); {
			case x < 10: // a late or early nonce
				n = uint64(int64(nonce) + int64(pick(rng, -1, 1)))
			case x < 20: // the grant that was already accepted, replayed
				n = machine.LastNonce
			}
			rtt := time.Duration(rng.Intn(2300)) * leaseMillisecond
			if rng.Intn(100) < 7 {
				rtt = -time.Duration(1+rng.Intn(500)) * leaseMillisecond
			}
			ev = GrantReceived{
				At: now, Wall: wall, Nonce: n,
				Epoch:          uint64(int64(committed) + int64(pick(rng, -1, 0, 0, 0, 0, 0, 0, 1))),
				CommittedEpoch: committed,
				Term:           uint64(int64(machine.Term) + int64(pick(rng, -1, 0, 0, 0, 0, 0, 1))),
				RTT:            rtt,
			}
		case r < 86: // the leader's own quorum round
			now += time.Duration(rng.Intn(60)) * leaseMillisecond
			rtt := time.Duration(rng.Intn(2300)) * leaseMillisecond
			if rng.Intn(100) < 7 {
				rtt = -time.Duration(1+rng.Intn(500)) * leaseMillisecond
			}
			ev = OwnQuorum{At: now, CommittedEpoch: committed, Term: uint64(int64(machine.Term) + int64(pick(rng, -1, 0, 0, 0, 1))), OK: rng.Intn(100) < 70, RTT: rtt}
		case r < 89: // the committed epoch moves; the next serve check sees it
			committed++
			now += time.Duration(rng.Intn(60)) * leaseMillisecond
			ev = ServeCheck{At: now, CommittedEpoch: committed}
		default:
			now += time.Duration(rng.Intn(60)) * leaseMillisecond
			ev = ServeCheck{At: now, CommittedEpoch: committed}
		}

		before := machine
		got := machine.Apply(ev, cfg)
		wantState, wantDecision, row := refLease(model, ev, cfg)
		model = wantState
		rows[row]++
		if got != wantDecision || machine != wantState {
			t.Fatalf("seed %d step %d (%s) %T %+v:\n got  %+v %+v\n want %+v %+v", seed, step, row, ev, ev, got, machine, wantDecision, wantState)
		}

		// Invariant: time never goes backwards in the machine.
		if machine.TickAt < before.TickAt || machine.RenewedAt < before.RenewedAt || machine.LastNonce < before.LastNonce {
			t.Fatalf("seed %d step %d: time went backwards: %+v -> %+v", seed, step, before, machine)
		}
		// Invariant: a lease only gets longer through an accepted renewal.
		if machine.Held && machine.Until > before.Until && machine.RenewedAt == before.RenewedAt {
			t.Fatalf("seed %d step %d (%s): Until extended without a renewal: %+v -> %+v", seed, step, row, before, machine)
		}
		// Invariant: a lease is never valid without observed progress since
		// its last renewal.
		if machine.RenewedAt != before.RenewedAt {
			clockBadSinceRenewal = false
		}
		if row == "tick/stopped" || row == "tick/gap" || row == "tick/wall" {
			clockBadSinceRenewal = true
		}
		if got.Valid && clockBadSinceRenewal {
			t.Fatalf("seed %d step %d (%s): valid after a tick that saw no progress", seed, step, row)
		}
		if tk, ok := ev.(Tick); ok && got.Valid && tk.At <= before.RenewedAt {
			t.Fatalf("seed %d step %d: valid tick at %v not after the renewal at %v", seed, step, tk.At, before.RenewedAt)
		}
		// Invariant: a grant from a lower term or epoch, a replayed nonce or a
		// negative round trip never changes the lease.
		if g, ok := ev.(GrantReceived); ok && (g.Term < before.Term || g.Epoch < g.CommittedEpoch || g.Nonce <= before.LastNonce || g.RTT < 0) && ruleState(machine) != ruleState(before) {
			t.Fatalf("seed %d step %d (%s): a stale grant changed the state: %+v -> %+v", seed, step, row, before, machine)
		}
		// Invariant: a grant ahead of the committed epoch stops serving.
		if g, ok := ev.(GrantReceived); ok && g.Nonce == before.Nonce && g.Nonce > before.LastNonce && g.RTT >= 0 && g.Term >= before.Term && g.Epoch > g.CommittedEpoch {
			if got.Valid || !got.Behind || before.Held != got.StepDown || machine.Held {
				t.Fatalf("seed %d step %d: higher-epoch grant did not stop serving: %+v %+v", seed, step, got, machine)
			}
		}
	}
}

func TestLeaseModelAgainstTheReference(t *testing.T) {
	rows := map[string]int{}
	for seed := int64(1); seed <= 20; seed++ {
		t.Run(fmt.Sprintf("seed%02d", seed), func(t *testing.T) { runLeaseSeed(t, seed, rows) })
	}
	// The generator must reach every row, or the comparison proves little.
	for _, row := range []string{
		"tick/ok", "tick/idle", "tick/stopped", "tick/gap", "tick/wall", "tick/expired",
		"request", "grant/nonce", "grant/replay", "grant/rtt-negative", "grant/term", "grant/epoch-lower", "grant/epoch-higher",
		"grant/no-progress", "grant/rtt", "grant/accepted", "own/ok", "own/fail", "own/term", "own/rtt", "own/rtt-negative",
		"serve/epoch", "serve/ok",
	} {
		if rows[row] < 5 {
			t.Errorf("row %q was reached %d times over 20 seeds, want at least 5", row, rows[row])
		}
	}
}

// TestLeaseModelFreshSeeds runs 100 more seeds than the 20 fixed ones above.
func TestLeaseModelFreshSeeds(t *testing.T) {
	rows := map[string]int{}
	for seed := int64(101); seed <= 200; seed++ {
		t.Run(fmt.Sprintf("seed%03d", seed), func(t *testing.T) { runLeaseSeed(t, seed, rows) })
	}
}
