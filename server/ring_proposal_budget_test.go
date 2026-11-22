package server

import (
	"testing"
	"time"
)

// The proposal budget as a test option of the server host (ring user store step M2b.2, spec 11): a
// test that runs a cluster without a quorum sets one budget, and both the ring node's proposals and
// the user binding's deadline end at it, so a no-quorum case costs seconds, not the 20 s
// production deadline. Production never sets it.

// TestTheProposalBudgetTestOptionReachesTheNodeAndTheUserBinding: the budget set on the service is
// the node's ProposalBudget and the user binding's deadline; unset, the binding keeps the
// production 20 s and the node its own default. Break: leave either unplumbed; red.
func TestTheProposalBudgetTestOptionReachesTheNodeAndTheUserBinding(t *testing.T) {
	listen := ringClusterPorts(t)
	qs := ringClusterService(t, listen, nil, nil)
	if got := qs.userDeadline(); got != userProposalDeadline {
		t.Fatalf("with no budget set the user deadline is %v, want the production %v", got, userProposalDeadline)
	}
	qs.proposalBudget = 2 * time.Second
	settings, listener, err := qs.ringNodeSettings()
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if settings.Cluster == nil {
		t.Fatal("a cluster member's node settings carry no cluster options")
	}
	if got := settings.Cluster.ProposalBudget; got != 2*time.Second {
		t.Fatalf("the node opens with proposal budget %v, want the 2 s test budget", got)
	}
	if got := qs.userDeadline(); got != 2*time.Second {
		t.Fatalf("the user binding's deadline is %v, want the 2 s test budget", got)
	}
}
