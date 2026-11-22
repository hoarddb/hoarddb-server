package wire

import (
	"slices"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// failureLedger is what the supervisor remembers around machine.DetectorState, whose
// decisions are once per failure run: a report is returned once and a down proposal once,
// and the detector cannot know whether either took effect. A leader change, a deposed
// leader that was handed the report, or a proposal lost to STATE_MOVED therefore lost the
// decision for good (membership section 5, D1). The ledger keeps the decision until the
// run ends, so the supervisor can make it again to whoever leads now.
//
// Cost: two sets of node ids, one entry per member that is failing right now.
type failureLedger struct {
	reported map[string]bool
	// downFirst is when this node first applied the commit that set a member down; a leader
	// change feeds the detector that time again, never a later one, so a leader that changes
	// faster than dead_ms cannot keep a down member from reaching dead.
	downFirst map[string]time.Duration
	// proposedDown maps a subject to the boot-clock time of the last attempt to commit it.
	proposedDown map[string]time.Duration
}

func newFailureLedger() *failureLedger {
	return &failureLedger{reported: map[string]bool{}, downFirst: map[string]time.Duration{}, proposedDown: map[string]time.Duration{}}
}

// noteDown records that a member was seen down at now, keeping the earliest time of a run,
// and returns that earliest time.
func (l *failureLedger) noteDown(subject string, now time.Duration) time.Duration {
	if first, known := l.downFirst[subject]; known {
		return first
	}
	l.downFirst[subject] = now
	return now
}

// forgetDown drops the down time of every member the state does not show down.
func (l *failureLedger) forgetDown(state cluster.ClusterState) {
	down := map[string]bool{}
	for _, member := range state.Members {
		if member.State == cluster.MemberDown {
			down[member.ID] = true
		}
	}
	for subject := range l.downFirst {
		if !down[subject] {
			delete(l.downFirst, subject)
		}
	}
}

// noteReported records that the detector told this node to report subject to the leader.
func (l *failureLedger) noteReported(subject string) { l.reported[subject] = true }

// noteProposedDown records that the detector, on the leader, decided subject is down and
// that a commit was attempted at now.
func (l *failureLedger) noteProposedDown(subject string, now time.Duration) {
	l.proposedDown[subject] = now
}

// noteAnswered ends subject's failure run: it answered a probe, so no earlier report or
// proposal about it holds.
func (l *failureLedger) noteAnswered(subject string) {
	delete(l.reported, subject)
	delete(l.proposedDown, subject)
}

// noteState drops what a committed state has settled: a member that is down or dead (or
// gone) needs no further report or proposal for this run.
func (l *failureLedger) noteState(state cluster.ClusterState) {
	stillRunning := map[string]bool{}
	for _, member := range state.Members {
		if member.State != cluster.MemberDown && member.State != cluster.MemberDead {
			stillRunning[member.ID] = true
		}
	}
	for subject := range l.reported {
		if !stillRunning[subject] {
			delete(l.reported, subject)
		}
	}
	for subject := range l.proposedDown {
		if !stillRunning[subject] {
			delete(l.proposedDown, subject)
		}
	}
}

// reportsForNewLeader lists the subjects this node reported under a leader that is gone: the
// new leader has seen none of them and its second-observer window is empty, so each is sent
// again.
func (l *failureLedger) reportsForNewLeader() []string {
	subjects := make([]string, 0, len(l.reported))
	for subject := range l.reported {
		subjects = append(subjects, subject)
	}
	slices.Sort(subjects)
	return subjects
}

// downToRetry lists the subjects the detector decided are down that no committed state shows
// down yet, that still do not answer, and whose last attempt is at least every old. Called on
// the leader each heartbeat, it makes a proposal that was lost (STATE_MOVED, a Raft hiccup)
// again instead of never, without hammering a commit that keeps being refused.
func (l *failureLedger) downToRetry(now, every time.Duration, answers func(subject string) bool) []string {
	var subjects []string
	for subject, attempted := range l.proposedDown {
		if now-attempted >= every && !answers(subject) {
			subjects = append(subjects, subject)
			l.proposedDown[subject] = now
		}
	}
	slices.Sort(subjects)
	return subjects
}

// stallGuard notices that this process did not run for a long time (a SIGSTOP, a VM pause,
// a starved scheduler) and so cannot trust a probe that was in flight across the gap: that
// probe timed out because this node was away, not because the subject was.
//
// Cost: two durations.
type stallGuard struct {
	last, stalledAt time.Duration
	seen            bool
}

// observe records a supervisor step at now (the boot clock). A gap longer than limit marks
// everything started before now as unreliable.
func (g *stallGuard) observe(now, limit time.Duration) {
	if g.seen && now-g.last > limit {
		g.stalledAt = now
	}
	g.last, g.seen = now, true
}

// unreliable reports that a probe started at startedAt and failed was in flight across a
// stall. A probe that answered is never discarded: an answer needs no excuse.
func (g *stallGuard) unreliable(startedAt time.Duration, answered bool) bool {
	return !answered && g.stalledAt != 0 && startedAt <= g.stalledAt
}
