package cluster

import (
	"fmt"
	"slices"
	"strings"
	"time"

	hraft "github.com/hashicorp/raft"
)

// ReconcileVoters makes the state's voter list match the Raft configuration, the real source of
// quorum. Promote and RemoveServer commit the configuration change first and propose SET_VOTERS
// second, so a leader that dies or loses leadership between the two leaves the state behind; the
// new leader calls this on each supervisor step and proposes SET_VOTERS for every difference. It
// does nothing on a node that is not the leader, and returns how many commands it proposed.
//
// It also removes an orphan nonvoter: a configured nonvoter with no member row and no join running
// on this leader (the leader that was adding it died before REGISTER_MEMBER committed). A witness
// is a nonvoter in the configuration but has its row, so it is not one.
//
// A witness is exempt: it joins as a Raft non-voter and is granted its vote by SET_VOTERS alone
// (membership-and-failure section 87), so it is a state voter by design and is never removed.
//
// A configured voter with no committed member row (a join whose REGISTER_MEMBER was refused after
// the library added it) cannot be recorded, because SET_VOTERS needs the row. It is returned as an
// error naming the node, once per node and process, so the operator hears of it and the
// supervisor does not repeat it every heartbeat. Lock order: voterMu is taken before anything
// else the node locks, and no proposal serializer is held inside it.
func (r *Raft) ReconcileVoters() (int, error) {
	if !r.IsLeader() || r.IsDiverged() {
		return 0, nil // a flagged leader changes no raft configuration
	}
	if !r.voterMu.TryLock() {
		return 0, nil // a promotion or removal is mid-way; the next step sees its result
	}
	defer r.voterMu.Unlock()
	conf, err := r.configuration()
	if err != nil {
		return 0, err
	}
	configured := make([]string, 0, len(conf.Servers))
	for _, server := range conf.Servers {
		if server.Suffrage == hraft.Voter {
			configured = append(configured, string(server.ID))
		}
	}
	state := r.State()
	if r.hasUnrecordedServer(conf, state) {
		// A leader that has just been elected may not have applied the entries of the leaders
		// before it, so a row can be committed and not yet in this state. Judging a server by
		// that state would remove a joiner whose row is on its way: wait for the log to be applied
		// first, and look again.
		if !r.barrierWithin(reconcileBarrier) {
			return 0, nil // not settled yet; the next step looks again
		}
		state = r.State()
	}
	changed := 0
	var orphaned []string
	for _, server := range conf.Servers {
		if server.Suffrage == hraft.Voter {
			continue
		}
		// An orphan nonvoter: the library holds the server, no member row names it, and no join
		// is running here. A leader that died between AddNonvoter and REGISTER_MEMBER left it. It
		// does not count in quorum, so removing it costs nothing.
		if _, member := state.memberIndex(string(server.ID)); member {
			continue
		}
		removed, err := r.removeOrphan(string(server.ID))
		if err != nil {
			return changed, err
		}
		if removed {
			changed++
		}
	}
	for _, id := range configured {
		_, member := state.memberIndex(id)
		if !member {
			if !r.orphans[id] {
				if r.orphans == nil {
					r.orphans = map[string]bool{}
				}
				r.orphans[id] = true
				orphaned = append(orphaned, id)
			}
			continue
		}
		if slices.Contains(state.Voters, id) {
			continue
		}
		if _, err := r.propose(CommandSetVoters, SetVotersBody(id, true)); err != nil {
			return changed, fmt.Errorf("record %s as a voter: %w", id, err)
		}
		changed++
	}
	for _, id := range state.Voters {
		if slices.Contains(configured, id) {
			continue
		}
		if i, member := state.memberIndex(id); member && state.Members[i].Witness {
			continue
		}
		if _, err := r.propose(CommandSetVoters, SetVotersBody(id, false)); err != nil {
			return changed, fmt.Errorf("record %s as no longer a voter: %w", id, err)
		}
		changed++
	}
	if len(orphaned) > 0 {
		return changed, fmt.Errorf("%s is a voter in the Raft configuration but has no member row, so the cluster state cannot record it: "+
			"run 'remove_node %s' to drop it, or join it again", strings.Join(orphaned, ", "), orphaned[0])
	}
	return changed, nil
}

// removeOrphan removes a nonvoter no member row names, and does it atomically against AddJoiner's
// mark: the check and the library's RemoveServer both run under joinsMu, and AddJoiner takes the
// same lock before it marks, so a join either marked first (the sweep sees the mark and leaves
// the server) or waits until the removal has committed and then adds the server again. The row is
// read afresh under the lock because a join may have registered since the caller read its state.
// Lock order: voterMu (held by the sweep), then joinsMu; nothing takes them the other way round,
// and the library's RemoveServer calls back into neither. It reports whether it removed the server.
func (r *Raft) removeOrphan(id string) (bool, error) {
	if r.beforeOrphanCheck != nil {
		r.beforeOrphanCheck(id)
	}
	r.joinsMu.Lock()
	defer r.joinsMu.Unlock()
	fresh := r.State()
	if _, member := fresh.memberIndex(id); member {
		return false, nil
	}
	if _, running := r.joins[id]; running {
		return false, nil
	}
	if r.afterOrphanCheck != nil {
		r.afterOrphanCheck(id)
	}
	if err := r.raft.RemoveServer(hraft.ServerID(id), 0, raftApplyTimeout).Error(); err != nil {
		return false, fmt.Errorf("remove %s, a nonvoter no member row names: %w", id, err)
	}
	return true, nil
}

// reconcileBarrier bounds the wait for the log to be applied before a server is judged to have no
// row. It is short because the supervisor that calls it also renews leases: a wait here delays
// every grant, so the bound is a fraction of the lease and the sweep simply looks again next step.
const reconcileBarrier = 250 * time.Millisecond

// barrierWithin waits at most d for the log to be applied and reports whether it was. The library's
// timeout bounds only the enqueue of the barrier, not the wait for the apply, so the wait is
// timed here. The goroutine ends when the future resolves, which it always does (on shutdown it
// resolves with an error), and the channel holds the one answer so it never blocks.
func (r *Raft) barrierWithin(d time.Duration) bool {
	future := r.raft.Barrier(d)
	settled := make(chan error, 1)
	go func() { settled <- future.Error() }()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case err := <-settled:
		return err == nil
	case <-timer.C:
		return false
	}
}

// hasUnrecordedServer reports whether a server of the configuration has no member row in state and
// no join running here, and is not a voter already reported as an orphan: that one is known and
// stays unrecorded until an operator acts, so waiting for the log on its account every step would
// put a barrier in the lease path for good.
func (r *Raft) hasUnrecordedServer(conf hraft.Configuration, state ClusterState) bool {
	for _, server := range conf.Servers {
		id := string(server.ID)
		if _, member := state.memberIndex(id); member || r.joinInFlight(id) {
			continue
		}
		if server.Suffrage == hraft.Voter && r.orphans[id] {
			continue
		}
		return true
	}
	return false
}
