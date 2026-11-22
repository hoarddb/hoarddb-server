package wire

import (
	"context"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/bootclock"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// supervisor is a networked node's housekeeping goroutine: each heartbeat, or at once when the
// committed state moved, it hands the new state to the handoff, ticks the handoff retry and runs
// the failure detector. The lease renewer runs beside it on a goroutine of its own (renewer.go),
// which the supervisor starts and joins, so no work of a step delays a renewal
// (lease-renewer-cadence-spec.md R3). The probes it starts are bounded to one in flight per member
// and end with the node.
//
// Cost: one goroutine, the renewer's, the freeze watchdog's, plus at most one probe goroutine per
// other member; no state per database, slot or bucket.
type supervisor struct {
	n     *Node
	clock bootclock.Clock

	mu      sync.Mutex
	det     machine.DetectorState
	cfg     machine.DetectorConfig
	leader  string
	seenSeq int64
	state   cluster.ClusterState
	probing map[string]bool
	// reachable is the last probe's verdict per member; absent means not probed yet.
	reachable map[string]bool
	downSeen  map[string]bool
	// unsent are failure reports that could not be delivered (no leader yet); the detector
	// reports a failure run once, so delivery is retried here until it succeeds or the
	// member answers a probe again.
	unsent    map[string]bool
	reporting map[string]bool
	// ledger keeps the detector's once-per-run decisions until their effect is seen, and
	// stall notices a long gap between steps; both are guarded by mu.
	ledger   *failureLedger
	stall    stallGuard
	proposed map[string]bool
	// tailsShipped says the owner's tails were queued after boot; only step uses it.
	tailsShipped bool
	// returned says a member became active since the last committed state: back from down,
	// or a joiner activated (BUG-0834); guarded by mu.
	returned bool
	// activeSeen is the active members of the last committed state, nil before the first;
	// guarded by mu.
	activeSeen map[string]bool
	wg         sync.WaitGroup
}

func newSupervisor(n *Node) *supervisor {
	return &supervisor{n: n, clock: bootclock.New(), probing: map[string]bool{}, reachable: map[string]bool{}, downSeen: map[string]bool{}, unsent: map[string]bool{}, reporting: map[string]bool{}, seenSeq: -1,
		ledger: newFailureLedger(), proposed: map[string]bool{}}
}

// peerReachable reports the last probe verdict, true when the member was never probed.
func (s *supervisor) peerReachable(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	ok, probed := s.reachable[id]
	return !probed || ok
}

func (s *supervisor) run(ctx context.Context) {
	defer s.wg.Wait()
	// The renewer and its watchdog end with ctx and are joined by the deferred Wait, which every
	// spawn's Add precedes.
	renew := s.n.newRenewer()
	s.spawn(ctx, renew.freeze.runTimer)
	s.spawn(ctx, renew.run)
	for {
		s.step(ctx)
		select {
		case <-ctx.Done():
			return
		case <-s.n.cl.wake:
		case <-time.After(s.heartbeat()):
		}
	}
}

func (s *supervisor) heartbeat() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.heartbeatLocked()
}

func (s *supervisor) heartbeatLocked() time.Duration {
	if hb := time.Duration(s.state.Settings.HeartbeatMs) * time.Millisecond; hb > 0 {
		return hb
	}
	return 100 * time.Millisecond
}

// stallLimitLocked is how long may pass between two supervisor steps before the gap is a
// stall rather than a slow step. It is step_down_ms plus a heartbeat: a step no longer waits a
// lease ask (the renewer runs apart), but its control calls are bounded the same way, and the
// limit is kept as it was.
func (s *supervisor) stallLimitLocked() time.Duration {
	if limit := time.Duration(s.state.Settings.StepDownMs+s.state.Settings.HeartbeatMs) * time.Millisecond; limit > 0 {
		return limit
	}
	return 3 * time.Second
}

func (s *supervisor) step(ctx context.Context) {
	n := s.n
	s.mu.Lock()
	s.stall.observe(s.clock.BootNow(), s.stallLimitLocked())
	s.mu.Unlock()
	if seq := n.raft.StateSeq(); seq != s.seenSeqLocked() {
		state := n.raft.State()
		n.topology.Refresh()
		for _, queued := range n.cl.takeEpochs() {
			n.handoff.OnEpoch(queued)
		}
		n.handoff.OnEpoch(state)
		s.onState(state)
	}
	if hook := n.cl.opts.BeforeSupervisorWork; hook != nil {
		hook(ctx)
	}
	if _, err := n.raft.ReconcileVoters(); err != nil && ctx.Err() == nil {
		n.cl.report(err)
	}
	n.askActivation(ctx)
	s.mu.Lock()
	members := len(s.state.Members)
	s.mu.Unlock()
	if members > 1 {
		s.detect(ctx)
		s.resendReports(ctx)
		n.cl.repl.Retry()
		if returned := s.copyReturned(); !s.tailsShipped || returned {
			// What a restarted owner had sent to its copies is gone: ship the owned tails once the
			// committed state is settled, and again when a member becomes active: a copy committed
			// down had no lane and returns without a seed, and a joiner was sent nothing after its
			// seed (BUG-0834).
			s.tailsShipped = n.shipOwnedTails(ctx)
		}
	}
	if err := n.handoff.Tick(ctx); err != nil && ctx.Err() == nil {
		n.cl.report(err)
	}
}

func (s *supervisor) seenSeqLocked() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seenSeq
}

// onState records a new committed state: the lease and detector configurations follow it,
// and a member that is down starts its stage-2 clock.
func (s *supervisor) onState(state cluster.ClusterState) {
	n := s.n
	cfg := machine.LeaseConfig{
		Heartbeat: time.Duration(state.Settings.HeartbeatMs) * time.Millisecond,
		StepDown:  time.Duration(state.Settings.StepDownMs) * time.Millisecond,
		Members:   len(state.Members), WallClock: true,
	}
	n.cl.leaseCfg.Store(&cfg)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state, s.seenSeq = state, state.StateSeq
	s.ledger.noteState(state)
	s.ledger.forgetDown(state)
	det := machine.DetectorConfig{
		SelfID: n.cfg.NodeID, ProbeInterval: time.Duration(state.Settings.ProbeIntervalMs) * time.Millisecond,
		ProbeTimeout: time.Duration(state.Settings.ProbeTimeoutMs) * time.Millisecond, Failures: int(state.Settings.ProbeFailures),
		Failover: time.Duration(state.Settings.FailoverMs) * time.Millisecond, Dead: time.Duration(state.Settings.DeadMs) * time.Millisecond,
	}
	for _, m := range state.Members {
		if m.State == cluster.MemberDead || m.State == cluster.MemberJoining {
			continue
		}
		det.MemberIDs = append(det.MemberIDs, m.ID)
	}
	det.VoterIDs = append(det.VoterIDs, state.Voters...)
	for _, v := range state.Voters {
		det.Voter = det.Voter || v == n.cfg.NodeID
	}
	s.cfg = det
	now := s.clock.BootNow()
	seen := make(map[string]bool, len(state.Members))
	for _, m := range state.Members {
		if m.State != cluster.MemberDown {
			continue
		}
		seen[m.ID] = true
		if !s.downSeen[m.ID] {
			s.det.Apply(machine.DownApplied{Subject: m.ID, At: s.ledger.noteDown(m.ID, now)}, s.cfg)
		}
	}
	if wentActive(s.downSeen, state) {
		s.returned = true // down to active at the same incarnation: no seed follows (BUG-0746)
		if n.cl.repl != nil {
			clearReturnedSuspects(n.cl.repl, s.downSeen, state)
		}
	}
	s.downSeen = seen
	active := activeMembers(state)
	if s.activeSeen != nil && gainedActive(s.activeSeen, active) {
		// A joiner pulled its pairs while joining, and nothing is pushed to a member that is
		// not a copy, so the writes ordered since its seed reach the copy sets it now enters
		// only as a tail: ship it, as for a member back from down (BUG-0834). The first state
		// a node sees is the boot's, whose tails step ships anyway.
		s.returned = true
	}
	s.activeSeen = active
	if n.cl.repl != nil {
		// topology.Refresh has run, so a member now down is no longer a copy: a write already
		// waiting for it would otherwise sleep to wc_timeout.
		n.cl.repl.copySetChanged()
	}
}

// wentActive reports whether a member that was down in the previous state is active in state.
func wentActive(wasDown map[string]bool, state cluster.ClusterState) bool {
	return len(returnedMembers(wasDown, state)) > 0
}

// activeMembers is the set of members state commits active.
func activeMembers(state cluster.ClusterState) map[string]bool {
	active := make(map[string]bool, len(state.Members))
	for _, member := range state.Members {
		if member.State == cluster.MemberActive {
			active[member.ID] = true
		}
	}
	return active
}

// gainedActive reports whether now holds an active member that was not active before.
func gainedActive(before, now map[string]bool) bool {
	for id := range now {
		if !before[id] {
			return true
		}
	}
	return false
}

// returnedMembers lists the members that were down in the previous state and are active in state.
func returnedMembers(wasDown map[string]bool, state cluster.ClusterState) []string {
	var returned []string
	for _, member := range state.Members {
		if wasDown[member.ID] && member.State == cluster.MemberActive {
			returned = append(returned, member.ID)
		}
	}
	return returned
}

// clearReturnedSuspects ends the breaker's hold on every member that went from down to active:
// the committed state says it is back, so a write need not wait for a probe to find that out.
func clearReturnedSuspects(repl *Replicator, wasDown map[string]bool, state cluster.ClusterState) {
	for _, id := range returnedMembers(wasDown, state) {
		repl.clearSuspect(id, "the committed state made it active again")
	}
}

// copyReturned reports, once, that a member became active since the last call: back from down,
// or activated after its join.
func (s *supervisor) copyReturned() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	returned := s.returned
	s.returned = false
	return returned
}

func (s *supervisor) detect(ctx context.Context) {
	s.mu.Lock()
	s.refreshLeaderLocked()
	act := s.det.Apply(machine.ProbeTick{At: s.clock.BootNow()}, s.cfg)
	cfg := s.cfg
	s.mu.Unlock()
	s.perform(ctx, act, cfg)
	s.retryLostDownProposals(ctx)
}

// refreshLeaderLocked reads the Raft leader now and tells the detector when it changed.
// Every event reads it first: a report or a probe result that arrives right after an
// election must be judged by the leader that exists, or a down proposal that only this
// event could have triggered is lost.
func (s *supervisor) refreshLeaderLocked() { s.onLeaderLocked(s.n.raft.Leader()) }

// onLeaderLocked is the detector's reaction to the Raft leader being leader now.
func (s *supervisor) onLeaderLocked(leader string) {
	s.cfg.Leader = leader
	if s.cfg.Leader != s.leader {
		s.leader = s.cfg.Leader
		s.det.Apply(machine.LeaderChanged{LeaderID: s.leader, At: s.clock.BootNow()}, s.cfg)
		// The detector clears the second-observer window and returns each decision once, so
		// whatever this node reported to the old leader is lost with it: say it again to
		// the new one (the resend finds it through unsent), and give the detector
		// the time each down member was first seen down again, never a later one: a leader
		// that changes faster than dead_ms must not keep a member from reaching dead.
		for _, subject := range s.ledger.reportsForNewLeader() {
			s.unsent[subject] = true
		}
		s.refeedDownLocked()
	}
}

// refeedDownLocked tells the detector that every member the committed state shows down went
// down now. The detector forgets a stage-2 timer when it proposes dead, so a proposal lost to
// a leader change or a failed commit would otherwise never be made again; starting the timer
// over is late, never early.
func (s *supervisor) refeedDownLocked() {
	now := s.clock.BootNow()
	for _, member := range s.state.Members {
		if member.State == cluster.MemberDown {
			s.det.Apply(machine.DownApplied{Subject: member.ID, At: s.ledger.noteDown(member.ID, now)}, s.cfg)
		}
	}
}

// onReport is a FAILURE_REPORT arriving on the control channel.
func (s *supervisor) onReport(ev machine.ReportReceived) {
	s.mu.Lock()
	s.refreshLeaderLocked()
	act := s.det.Apply(ev, s.cfg)
	cfg := s.cfg
	s.mu.Unlock()
	s.perform(s.n.cl.ctx, act, cfg)
}

func (s *supervisor) perform(ctx context.Context, act machine.DetectorAction, cfg machine.DetectorConfig) {
	for _, subject := range act.Probe {
		s.mu.Lock()
		busy := s.probing[subject]
		s.probing[subject] = true
		s.mu.Unlock()
		if busy {
			continue
		}
		s.wg.Add(1)
		go s.probe(ctx, subject, cfg)
	}
	for _, subject := range act.Report {
		s.mu.Lock()
		s.ledger.noteReported(subject)
		s.mu.Unlock()
		s.sendReport(ctx, subject)
	}
	for _, subject := range act.ProposeDown {
		s.mu.Lock()
		s.ledger.noteProposedDown(subject, s.clock.BootNow())
		s.mu.Unlock()
		s.proposeOnce(ctx, subject, "down")
	}
	for _, subject := range act.ProposeDead {
		s.proposeOnce(ctx, subject, "dead")
	}
}

// sendReport delivers FAILURE_REPORT for subject to the leader, remembering it for a retry
// when it cannot be delivered.
func (s *supervisor) sendReport(ctx context.Context, subject string) {
	s.mu.Lock()
	busy := s.reporting[subject]
	s.reporting[subject] = true
	s.mu.Unlock()
	if busy {
		return // one report per subject in flight: a hung leader must not collect goroutines
	}
	s.spawn(ctx, func(ctx context.Context) {
		err := s.n.cl.ctrl.SendReport(ctx, subject)
		s.mu.Lock()
		delete(s.reporting, subject)
		s.unsent[subject] = err != nil
		if err == nil {
			delete(s.unsent, subject)
		}
		s.mu.Unlock()
		if err != nil && ctx.Err() == nil {
			s.n.cl.report(err)
		}
	})
}

// resendReports retries the reports that found no leader or no route.
func (s *supervisor) resendReports(ctx context.Context) {
	s.mu.Lock()
	var again []string
	for subject, pending := range s.unsent {
		if pending && !s.reachable[subject] {
			again = append(again, subject)
		}
	}
	s.mu.Unlock()
	for _, subject := range again {
		s.sendReport(ctx, subject)
	}
}

func (s *supervisor) spawn(ctx context.Context, fn func(context.Context)) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		fn(ctx)
	}()
}

// proposeOnce commits subject's state from a goroutine of its own, never two at once for the
// same subject and state: the retry of a lost proposal must not pile up behind a slow commit.
func (s *supervisor) proposeOnce(ctx context.Context, subject, state string) {
	key := subject + "/" + state
	s.mu.Lock()
	busy := s.proposed[key]
	s.proposed[key] = true
	s.mu.Unlock()
	if busy {
		return
	}
	s.spawn(ctx, func(ctx context.Context) {
		defer func() {
			s.mu.Lock()
			delete(s.proposed, key)
			s.mu.Unlock()
		}()
		s.proposeState(ctx, subject, state)
	})
}

func (s *supervisor) proposeState(ctx context.Context, subject, state string) {
	if ctx.Err() != nil || !s.n.raft.IsLeader() {
		return
	}
	if err := s.n.setMemberState(ctx, subject, state); err != nil && ctx.Err() == nil {
		if state == "dead" {
			// The detector proposes dead once and forgets the timer: put it back, or the
			// member is never declared dead (membership section 5, U27).
			s.mu.Lock()
			s.refeedDownLocked()
			s.mu.Unlock()
		}
		s.n.cl.report(err)
	}
}

// retryLostDownProposals proposes down again for a member the detector decided is down, that
// no committed state shows down yet and that still does not answer, while this node leads.
func (s *supervisor) retryLostDownProposals(ctx context.Context) {
	if !s.n.raft.IsLeader() {
		return
	}
	s.mu.Lock()
	subjects := s.lostDownLocked()
	s.mu.Unlock()
	for _, subject := range subjects {
		s.proposeOnce(ctx, subject, "down")
	}
}

// lostDownLocked lists the down proposals to make again now.
func (s *supervisor) lostDownLocked() []string {
	every := max(time.Duration(s.state.Settings.ProbeIntervalMs)*time.Millisecond, s.heartbeatLocked())
	return s.ledger.downToRetry(s.clock.BootNow(), every, func(subject string) bool { return s.reachable[subject] })
}

// probeVerdictLocked feeds one probe result to the detector, unless this node was away while
// the probe was in flight (a failed result then says nothing about the subject), and returns
// what the detector decided. ok is false for a discarded result.
func (s *supervisor) probeVerdictLocked(result machine.ProbeResult) (act machine.DetectorAction, ok bool) {
	subject := result.Subject
	s.probing[subject] = false
	if s.stall.unreliable(result.StartedAt, result.Answered) {
		return machine.DetectorAction{}, false
	}
	if result.Answered {
		s.ledger.noteAnswered(subject)
	}
	act = s.det.Apply(result, s.cfg)
	s.reachable[subject] = result.Answered
	if result.Answered {
		delete(s.unsent, subject)
	}
	return act, true
}

func (s *supervisor) probe(ctx context.Context, subject string, cfg machine.DetectorConfig) {
	defer s.wg.Done()
	probeCtx, cancel := context.WithTimeout(ctx, max(cfg.ProbeTimeout, 50*time.Millisecond))
	result := s.n.cl.ctrl.Probe(probeCtx, subject)
	cancel()
	s.mu.Lock()
	s.refreshLeaderLocked()
	was, probed := s.reachable[subject]
	act, ok := s.probeVerdictLocked(result)
	current := s.cfg
	s.mu.Unlock()
	if !ok {
		return
	}
	if result.Answered && probed && !was {
		// The member is back: whatever was queued for it while it was away goes now.
		s.n.cl.repl.Resync(subject)
	}
	if ctx.Err() == nil {
		s.perform(ctx, act, current)
	}
}
