package wire

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster/control"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// The renewer's pacing tests (spec lease-renewer-cadence section 7, W1 and W2): the loop runs over
// a scripted lease clock, a scripted wait and a scripted control channel whose grants go to a real
// lease machine, so every pacing decision is deterministic.

// scriptClock is a lease clock that advances one microsecond on every reading, so no two readings
// are equal, and by exactly what a wait or a round spends.
type scriptClock struct {
	mu  sync.Mutex
	now time.Duration
}

func (c *scriptClock) BootNow() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now += time.Microsecond
	return c.now
}

func (c *scriptClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now += d
	c.mu.Unlock()
}

// scriptControl is the control channel and Raft seat of a follower (or a leader) whose rounds take
// a scripted time. Its lease is a real machine.LeaseState.
type scriptControl struct {
	clock  *scriptClock
	round  time.Duration
	leader bool

	lease  machine.LeaseState
	nonce  uint64
	ticks  []time.Duration // the reading of each tick, which is each round's start
	asks   []time.Duration // the sentAt of each ask
	waits  []time.Duration // every wait the loop asked for
	valid  []bool          // the decision of every renewal
	voided int             // ticks that voided a held lease
	stop   func()
	rounds int
	quorum *scriptQuorum
}

func (s *scriptControl) Tick(context.Context) machine.Tick {
	at := s.clock.BootNow()
	s.ticks = append(s.ticks, at)
	return machine.Tick{At: at, Wall: at}
}

func (s *scriptControl) Apply(ev machine.LeaseEvent, cfg machine.LeaseConfig) machine.LeaseDecision {
	s.lease.Term = 3
	held := s.lease.Held
	decision := s.lease.Apply(ev, cfg)
	switch ev.(type) {
	case machine.Tick:
		if held && !decision.Valid {
			s.voided++
		}
	case machine.GrantReceived, machine.OwnQuorum:
		s.valid = append(s.valid, decision.Valid)
		if len(s.ticks) >= s.rounds && s.round > cfg.Heartbeat {
			s.stop() // an overlong round never waits, so the run stops here
		}
	}
	return decision
}

func (s *scriptControl) AskLeaseRound(_ context.Context, sentAt time.Duration) (machine.GrantReceived, control.LeaseRound, error) {
	s.nonce++
	s.asks = append(s.asks, sentAt)
	s.lease.SentAt, s.lease.Nonce = sentAt, s.nonce // what Control's RequestSent hook applies
	s.clock.advance(s.round)
	at := s.clock.BootNow()
	return machine.GrantReceived{At: at, Wall: at, Nonce: s.nonce, Epoch: 5, CommittedEpoch: 5, Term: 3, RTT: at - sentAt}, control.LeaseRound{}, nil
}

func (s *scriptControl) IsLeader() bool { return s.leader }

func (s *scriptControl) VerifyLeader() (time.Duration, error) {
	s.clock.advance(s.round)
	return s.round, nil
}

func (s *scriptControl) Term() uint64 { return 3 }

// scriptQuorum stands in for the node's controlLease and records the sentAt of every epoch
// confirmation, so a test can tell the round's start from a fresh clock reading.
type scriptQuorum struct {
	confirmed []time.Duration
}

func (*scriptQuorum) noteQuorum() {}

func (q *scriptQuorum) confirmEpoch(_ uint64, sentAt time.Duration) {
	q.confirmed = append(q.confirmed, sentAt)
}

// runScripted runs the renewer for rounds rounds of round each at heartbeat hb and returns the script.
func runScripted(t *testing.T, leader bool, hb, round time.Duration, rounds int) *scriptControl {
	t.Helper()
	clock := &scriptClock{now: 10 * time.Second}
	script := &scriptControl{clock: clock, round: round, leader: leader}
	cfg := machine.LeaseConfig{Heartbeat: hb, StepDown: 2 * time.Second, Members: 3, WallClock: true}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	script.stop, script.rounds = cancel, rounds
	script.quorum = &scriptQuorum{}
	r := newRenewer(renewerDeps{
		Self: "a", Clock: clock, Control: script, Seat: script, Lease: script.quorum,
		Config:    func() machine.LeaseConfig { return cfg },
		Committed: func() uint64 { return 5 },
		Report:    func(error) {},
		Warn:      func(string) {},
	})
	r.freeze = newFreezeWatch(clock, time.Now)
	r.wait = func(ctx context.Context, d time.Duration) bool {
		script.waits = append(script.waits, d)
		clock.advance(d)
		if len(script.ticks) >= rounds {
			cancel()
			return false
		}
		return ctx.Err() == nil
	}
	r.run(ctx)
	return script
}

// TestTheRenewerPacesFromEachRoundsStart is W1. Rounds of 40 ms at a heartbeat of 100 ms: each round
// starts one heartbeat after the previous one started, and every round asks. Rounds of 150 ms: the
// next round starts at once, with no wait.
func TestTheRenewerPacesFromEachRoundsStart(t *testing.T) {
	const hb = 100 * time.Millisecond
	short := runScripted(t, false, hb, 40*time.Millisecond, 8)
	if len(short.asks) != len(short.ticks) {
		t.Fatalf("40 ms rounds: %d ticks but %d asks, want an ask every round (ticks %v, asks %v)", len(short.ticks), len(short.asks), short.ticks, short.asks)
	}
	for i := 1; i < len(short.ticks); i++ {
		if gap := short.ticks[i] - short.ticks[i-1]; gap < hb || gap > hb+20*time.Microsecond {
			t.Errorf("40 ms rounds: round %d started %v after round %d, want one heartbeat (%v) after the previous start", i, gap, i-1, hb)
		}
		if short.asks[i] != short.ticks[i] {
			t.Errorf("round %d asked with sentAt %v, want the round's tick reading %v", i, short.asks[i], short.ticks[i])
		}
	}
	for i, wait := range short.waits {
		if wait <= 0 || wait > hb-40*time.Millisecond {
			t.Errorf("40 ms rounds: wait %d was %v, want the rest of the heartbeat after the round (at most %v)", i, wait, hb-40*time.Millisecond)
		}
	}

	long := runScripted(t, false, hb, 150*time.Millisecond, 6)
	if len(long.waits) != 0 {
		t.Errorf("150 ms rounds: the loop waited %v, want no wait after a round longer than the heartbeat", long.waits)
	}
	for i := 1; i < len(long.ticks); i++ {
		if gap := long.ticks[i] - long.ticks[i-1]; gap < 150*time.Millisecond || gap > 150*time.Millisecond+20*time.Microsecond {
			t.Errorf("150 ms rounds: round %d started %v after round %d, want at once after the round (150 ms)", i, gap, i-1)
		}
	}
	if len(long.asks) != len(long.ticks) {
		t.Errorf("150 ms rounds: %d ticks but %d asks, want an ask every round", len(long.ticks), len(long.asks))
	}
}

// TestAnOverlongRoundIsFollowedByAnAcceptedRound is W2: rounds of 150 ms at a heartbeat of 100 ms,
// for a follower and for a leader. Every renewal is accepted and no tick voids the lease between
// rounds, although each tick follows the renewal before it by microseconds.
func TestAnOverlongRoundIsFollowedByAnAcceptedRound(t *testing.T) {
	for _, leader := range []bool{false, true} {
		script := runScripted(t, leader, 100*time.Millisecond, 150*time.Millisecond, 8)
		if len(script.valid) < 7 {
			t.Fatalf("leader %v: %d renewals, want at least 7", leader, len(script.valid))
		}
		for i, ok := range script.valid {
			if !ok {
				t.Errorf("leader %v: renewal %d was not accepted (renewals %v)", leader, i, script.valid)
			}
		}
		if script.voided != 0 {
			t.Errorf("leader %v: %d ticks voided the lease between 150 ms rounds, want none", leader, script.voided)
		}
		// R1 and R4: every renewal counts from its round's start, so the last one ends step_down
		// after the last tick, not after the round's end.
		if last := script.ticks[len(script.ticks)-1]; script.lease.Until > last+2*time.Second {
			t.Errorf("leader %v: the last renewal lasts until %v, after its round's start plus step_down (%v)", leader, script.lease.Until, last+2*time.Second)
		}
	}
}

// TestAnEpochIsConfirmedFromTheRoundsStart pins the sentAt of confirmEpoch: the epoch a grantor or a
// quorum confirmed is good from when the round started, not from when its answer was read, so a
// confirmation must carry the tick reading, as a lease request does (R4). The scripted clock moves
// 150 ms inside each round, so a fresh reading would differ from the tick by that much.
func TestAnEpochIsConfirmedFromTheRoundsStart(t *testing.T) {
	for _, leader := range []bool{false, true} {
		script := runScripted(t, leader, 100*time.Millisecond, 150*time.Millisecond, 6)
		if len(script.quorum.confirmed) < 5 {
			t.Fatalf("leader %v: %d epoch confirmations, want one per round", leader, len(script.quorum.confirmed))
		}
		for i, sentAt := range script.quorum.confirmed {
			if sentAt != script.ticks[i] {
				t.Errorf("leader %v: round %d confirmed its epoch with sentAt %v, want the round's tick reading %v", leader, i, sentAt, script.ticks[i])
			}
		}
	}
}
