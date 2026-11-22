package wire

import (
	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// The causes a held pair reports, shared by the read gate, the parked-operation refusal and
// every writer's refusal so that one condition reads the same wherever it is met.
const (
	// restartHoldCause is a peer-less awaiting hold: nobody is waited for, the node proves by
	// the lease bound counted from its own start that a previous owner has stopped.
	restartHoldCause = "this node restarted and is proving that its copies have stopped before it serves the pair, which takes up to step_down_ms + heartbeat_ms from its start (2.1 s at the defaults) and then a catch-up to its copies"
	// handoffHoldCause is a primary handoff with a named previous owner.
	handoffHoldCause = "a primary handoff for it is still open"
	// bootReplayHoldCause is a pair whose log tail the restart could not replay yet because a
	// bucket's declaration is read through the cluster state, which a booting node has not applied
	// (BUG-0760); the node retries on its own.
	bootReplayHoldCause = "this node restarted and is replaying this slot's log tail, which waits for the cluster state to give it a bucket's declaration; it retries on its own"
)

// restartUndecidedPrefix starts every cause of the window after a restart in which the node does not
// yet know which pairs it owns (BUG-0813): its log is not applied, or its ownership check has not seen
// the state that was applied.
const restartUndecidedPrefix = "this node restarted and does not know yet which pairs it owns: "

// restartUndecidedCause says what the undecided window waits for, "" once it is over.
func restartUndecidedCause(logApplied, stateSeen bool) string {
	switch {
	case !logApplied:
		return restartUndecidedPrefix + "it has not applied its own log yet"
	case !stateSeen:
		return restartUndecidedPrefix + "it has applied its log, but its ownership check has not yet caught up with the state that log ended in"
	}
	return ""
}

// awaitingCause is the cause of a pair's awaiting hold: peer-less, or opened inside the stop
// bound after a restart, it is the restart hold.
func awaitingCause(pair cluster.Pair) string {
	if pair.Peer == "" || pair.Flags&flagRestarted != 0 {
		return restartHoldCause
	}
	return handoffHoldCause
}

// HoldCause says why the pair is held on this node, or "" when no barrier is open for it.
// It takes the Handoff's lock, so it must not be called under a pair lock.
func (h *Handoff) HoldCause(slot uint16, db uint64) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	hp := h.pairs[pairKey{slot, db}]
	if hp == nil {
		return ""
	}
	switch hp.barrier.Hold {
	case machine.HoldAwaiting:
		return awaitingCause(cluster.Pair{Peer: hp.barrier.Peer, Flags: restartedFlag(hp)})
	case machine.HoldSending:
		return handoffHoldCause
	}
	return ""
}

// notOrderedBecause is the reason a write came back neither positioned nor applied, in words
// a developer can act on. A caller prefixes what was not done and suffixes how to retry.
func (a WriteAnswer) notOrderedBecause() string {
	switch {
	case a.Held && a.HoldCause != "":
		return "its pair is held: " + a.HoldCause
	case a.Held:
		return "its pair is held while a handoff or a replay finishes"
	case a.Forward:
		return "this node is not the owner of its pair"
	}
	return "it was not ordered"
}

// restartedFlag is the pair flag bit for a barrier opened in the restart window.
func restartedFlag(hp *handoffPair) uint8 {
	if hp.restartWindow {
		return flagRestarted
	}
	return 0
}
