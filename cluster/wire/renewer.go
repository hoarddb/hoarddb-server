package wire

import (
	"context"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster/bootclock"
	"github.com/hoarddb/hoarddb-server/cluster/control"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/transport"
)

// renewer is the lease renewer of conventions section 15 on a goroutine of its own (spec
// lease-renewer-cadence R3): each round is a tick, then the leader's own quorum round or a
// follower's ask, and the next round starts one heartbeat after this one started, or at once when
// the round ran longer. Nothing else the node does in a supervisor step delays a renewal.
//
// It also writes the lapse line (lease_trace.go): the gap record, the lapse snapshot and the line
// are read and written by this goroutine alone; the freeze watchdog has a lock of its own.
//
// Cost: one goroutine per node and the fixed records below; nothing per database, slot, pair or
// bucket. A round reads the member count, so a membership change costs it nothing.
type renewer struct {
	self      string
	clock     bootclock.Clock
	ctrl      leaseControl
	seat      leaseSeat
	lease     quorumStamp
	config    func() machine.LeaseConfig
	committed func() uint64
	report    func(error)
	warn      func(string)
	// wait sleeps d or until ctx ends, and says whether ctx is still live. It is a field so a test
	// can drive the pacing over a scripted clock.
	wait func(ctx context.Context, d time.Duration) bool

	trace   gapTrace
	lastGap gapRecord
	lapse   lapseSnapshot
	freeze  *freezeWatch
	deltas  *runtimeDeltas
	limit   lapseLimiter
}

// lapseSnapshot is what the renewer knew when it first saw a lapse begin, kept until the lapse
// ends and the line is written.
type lapseSnapshot struct {
	open   bool
	gap    gapRecord
	leader bool
	err    string
}

// leaseControl is the part of the control channel the renewer drives: *control.Control.
type leaseControl interface {
	Tick(ctx context.Context) machine.Tick
	Apply(ev machine.LeaseEvent, cfg machine.LeaseConfig) machine.LeaseDecision
	AskLeaseRound(ctx context.Context, sentAt time.Duration) (machine.GrantReceived, control.LeaseRound, error)
}

// leaseSeat is the node's Raft seat as the renewer sees it: *cluster.Raft.
type leaseSeat interface {
	IsLeader() bool
	VerifyLeader() (time.Duration, error)
	Term() uint64
}

// quorumStamp is what a renewal tells the node's lease beside the machine: *controlLease.
type quorumStamp interface {
	noteQuorum()
	confirmEpoch(epoch uint64, sentAt time.Duration)
}

// renewerDeps is everything a renewer reads from the node around it.
type renewerDeps struct {
	Self      string
	Clock     bootclock.Clock
	Control   leaseControl
	Seat      leaseSeat
	Lease     quorumStamp
	Config    func() machine.LeaseConfig
	Committed func() uint64
	Report    func(error)
	Warn      func(string)
}

// defaultLeaseHeartbeat is the pacing before a configuration names heartbeat_ms: the default.
const defaultLeaseHeartbeat = 100 * time.Millisecond

func newRenewer(deps renewerDeps) *renewer {
	return &renewer{
		self: deps.Self, clock: deps.Clock, ctrl: deps.Control, seat: deps.Seat, lease: deps.Lease,
		config: deps.Config, committed: deps.Committed, report: deps.Report, warn: deps.Warn,
		wait: waitTimer, freeze: newFreezeWatch(deps.Clock, time.Now), deltas: newRuntimeDeltas(),
	}
}

// waitTimer is the real wait: a timer of d, ended early by ctx.
func waitTimer(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// heartbeatOf is the pacing of cfg.
func heartbeatOf(cfg machine.LeaseConfig) time.Duration {
	if cfg.Heartbeat > 0 {
		return cfg.Heartbeat
	}
	return defaultLeaseHeartbeat
}

// run renews until ctx ends. A node of one member holds no lease and is polled at the default
// heartbeat until it has company.
func (r *renewer) run(ctx context.Context) {
	for ctx.Err() == nil {
		cfg := r.config()
		if cfg.Members <= 1 {
			if !r.wait(ctx, defaultLeaseHeartbeat) {
				return
			}
			continue
		}
		r.trace.phase(phWake, r.clock.BootNow())
		start := r.round(ctx, cfg)
		ended := r.clock.BootNow()
		r.trace.phase(phRound, ended)
		// Paced from the round's start on the lease clock: a round longer than the heartbeat is
		// followed by the next at once, never by a further heartbeat of sleep.
		left := start + heartbeatOf(cfg) - ended
		if left <= 0 {
			continue
		}
		if !r.wait(ctx, left) {
			return
		}
		r.trace.slept(left, false, r.clock.BootNow())
	}
}

// round is one renewal: the tick, then the leader's own quorum round or a follower's ask when the
// machine says one is due. It returns the tick's reading, the round's start.
func (r *renewer) round(ctx context.Context, cfg machine.LeaseConfig) time.Duration {
	tick := r.ctrl.Tick(ctx)
	r.closeGap(tick.At)
	decision := r.ctrl.Apply(tick, cfg)
	leader := r.seat.IsLeader()
	r.noteDecision(decision, leader, false, cfg)
	if leader {
		r.ownRound(tick.At, cfg)
	} else if decision.SendRequest {
		r.ask(ctx, tick.At, cfg)
	}
	return tick.At
}

// ownRound is the leader's own quorum round. Its lease counts from the round's start (R1): the RTT
// it reports is the whole time since the tick, or VerifyLeader's own measure if that is longer.
func (r *renewer) ownRound(start time.Duration, cfg machine.LeaseConfig) {
	verifying := r.clock.BootNow()
	took, err := r.seat.VerifyLeader()
	ended := r.clock.BootNow()
	r.trace.cur.Round = roundSplit{Leader: true, Grantor: ended - verifying}
	if err == nil {
		r.lease.noteQuorum()
		r.lease.confirmEpoch(r.committed(), start)
		r.forgetRoundError()
	} else if r.keepRoundError(err) {
		r.report(err) // once per lapse: a cut leader fails every round until it is healed
	}
	own := r.ctrl.Apply(machine.OwnQuorum{
		At: ended, CommittedEpoch: r.committed(), Term: r.seat.Term(), OK: err == nil, RTT: max(ended-start, took),
	}, cfg)
	r.noteDecision(own, true, true, cfg)
}

// ask is a follower's lease request, sent with the round's tick reading as its sentAt (R4).
func (r *renewer) ask(ctx context.Context, start time.Duration, cfg machine.LeaseConfig) {
	askCtx, cancel := context.WithTimeout(ctx, cfg.StepDown)
	defer cancel()
	grant, round, err := r.ctrl.AskLeaseRound(askCtx, start)
	r.trace.cur.Round = roundSplit{Grantor: round.Grantor, TurnWait: round.Turn.Waited, TurnHolder: transport.HolderName(round.Turn.Holder)}
	if err != nil {
		// The next round asks again; a node with no lease refuses writes by itself.
		r.keepRoundError(err)
		r.report(err)
		return
	}
	r.forgetRoundError()
	accepted := r.ctrl.Apply(grant, cfg)
	r.noteDecision(accepted, false, true, cfg)
	if accepted.Valid && !accepted.Behind {
		r.lease.confirmEpoch(grant.Epoch, start)
	}
	r.lease.noteQuorum()
}

// keepRoundError remembers the first error of a failed round for the lapse line and says whether
// it was the first since the last good round.
func (r *renewer) keepRoundError(err error) bool {
	if r.lapse.err != "" {
		return false
	}
	r.lapse.err = err.Error()
	return true
}

// forgetRoundError drops the error of an earlier failed round that no lapse followed, so it is
// not written into the line of a later one.
func (r *renewer) forgetRoundError() {
	if !r.lapse.open {
		r.lapse.err = ""
	}
}

// closeGap closes the gap that ends at the tick read at at, and notes what the process and the
// runtime did inside it.
func (r *renewer) closeGap(at time.Duration) {
	gap, had := r.trace.tick(at)
	sched, gcPause := r.deltas.read()
	if !had {
		return
	}
	gap.SchedLat, gap.GCPause = sched, gcPause
	r.lastGap = gap
}

// noteDecision keeps the first sight of a lapse, and when a decision ends one writes its line.
// It reads the lease decision and writes nothing back to the lease. afterRound is true for a
// decision the round just produced.
func (r *renewer) noteDecision(decision machine.LeaseDecision, leader, afterRound bool, cfg machine.LeaseConfig) {
	switch {
	case !decision.Valid && !r.lapse.open:
		r.lapse.open, r.lapse.leader = true, leader
		r.lapse.gap = lapseGap(r.lastGap, r.trace.cur.Round, r.clock.BootNow()-r.trace.mark, afterRound)
	case decision.Ended:
		if r.lapse.open && decision.Cause != machine.LapseNeverHeld {
			r.writeLapseLine(decision, cfg)
		}
		r.lapse = lapseSnapshot{}
	}
}

// writeLapseLine writes the one line of a lapse that has just ended, within the cap.
func (r *renewer) writeLapseLine(decision machine.LeaseDecision, cfg machine.LeaseConfig) {
	now := r.clock.BootNow()
	ok, dropped := r.limit.allow(now)
	if !ok {
		return
	}
	gap := r.lapse.gap
	phase, took := gap.Longest()
	// The freeze is read now, not at the tick: after a stall the tick can come before the watchdog's
	// first wake has recorded it.
	freeze := r.freeze.longestOverlap(gap.From, gap.To)
	role := "follower"
	if r.lapse.leader {
		role = "leader"
	}
	r.warn(lapseLine{
		Node: r.self, Role: role, Cause: decision.Cause.String(), Follower: !gap.Round.Leader,
		Began: time.Now().Add(-(now - decision.LapsedAt)), Lasted: now - decision.LapsedAt,
		Gap: gap.Gap, Round: gap.Phase[phRound], Grantor: gap.Round.Grantor, TurnWait: gap.Round.TurnWait, TurnHolder: gap.Round.TurnHolder,
		PhaseMax: phase.String(), PhaseMaxTook: took, PreTick: gap.PreTick(), Sleep: gap.Phase[phSleep], WakeCut: gap.WakeCut,
		Unaccounted: gap.Unaccounted, Freeze: freeze, TickLate: gap.Gap > cfg.TickGapLimit(), GCPauseMax: gap.GCPause, SchedLatMax: gap.SchedLat,
		VerifyErr: r.lapse.err, Dropped: dropped,
	}.String())
}
