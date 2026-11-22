package wire

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// TestAJoinersActivationMakesTheOwnerShipItsTailsOnce (BUG-0834): a joiner pulled its pairs
// while joining and the owner pushed nothing to it after its seed, because a joining member is
// not a copy. Its activation makes it a copy of pairs whose owner stays, so the owner must send
// it the tails, once per activation, the way it does for a member back from down.
// [BREAK: onState notices only a member back from down]
func TestAJoinersActivationMakesTheOwnerShipItsTailsOnce(t *testing.T) {
	s := onStateRig(t, newHealthRig(t, "copy"))
	s.onState(stateWith(cluster.MemberActive)) // the first state a node sees: the boot pass ships
	if s.copyReturned() {
		t.Fatal("the first committed state was taken for an activation")
	}
	s.onState(stateWith(cluster.MemberJoining))
	if s.copyReturned() {
		t.Fatal("a member that is joining was taken for a new copy")
	}
	s.onState(stateWith(cluster.MemberJoining))
	if s.copyReturned() {
		t.Fatal("a member that is still joining was taken for a new copy")
	}
	s.onState(stateWith(cluster.MemberActive))
	if !s.copyReturned() {
		t.Fatal("the joiner's activation did not make the owner ship its tails: the new copy stays at the position its seed reached until the pair's next write")
	}
	if s.copyReturned() {
		t.Fatal("one activation asked for the tails twice")
	}
	s.onState(stateWith(cluster.MemberActive))
	if s.copyReturned() {
		t.Fatal("a member that stayed active was taken for a new copy")
	}
}

// TestAMemberAbsentFromTheLastStateAndActiveNowMakesTheOwnerShipItsTails (BUG-0834): a member
// registered and activated between two states the node saw was not in the earlier state at all.
// A fix keyed on a joining-to-active transition would miss it.
// [BREAK: onState notices only a joining member that became active]
func TestAMemberAbsentFromTheLastStateAndActiveNowMakesTheOwnerShipItsTails(t *testing.T) {
	s := onStateRig(t, newHealthRig(t, "copy"))
	s.onState(cluster.ClusterState{Members: []cluster.Member{{ID: "owner", State: cluster.MemberActive}}})
	if s.copyReturned() {
		t.Fatal("the first committed state was taken for an activation")
	}
	s.onState(stateWith(cluster.MemberActive))
	if !s.copyReturned() {
		t.Fatal("a member that was not in the last state and is active now did not make the owner ship its tails")
	}
	if s.copyReturned() {
		t.Fatal("one appearance asked for the tails twice")
	}
}

// TestAMemberBackFromDownStillMakesTheOwnerShipItsTails (BUG-0746, kept by BUG-0834): the
// down-to-active return is noticed as before, once.
func TestAMemberBackFromDownStillMakesTheOwnerShipItsTails(t *testing.T) {
	s := onStateRig(t, newHealthRig(t, "copy"))
	s.onState(stateWith(cluster.MemberActive))
	s.onState(stateWith(cluster.MemberDown))
	if s.copyReturned() {
		t.Fatal("a member going down was taken for a return")
	}
	s.onState(stateWith(cluster.MemberActive))
	if !s.copyReturned() || s.copyReturned() {
		t.Fatal("a member back from down did not ask for the tails exactly once")
	}
}
