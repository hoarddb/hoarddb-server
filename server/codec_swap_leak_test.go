package server

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestClusterHelperStopsItsNodesWhenItsTestReturns is the failing-first
// regression test for leaked test nodes. newTestCluster started real nodes (internode
// listeners, peer senders, the retention and auth-config loops, one reclaimer
// per node) and left stopping them to every caller. TestDeleteOfAMissingKey-
// StillReplicates and TestCreateExistingDatabaseAnswersAlreadyExists never
// did, so their clusters ran for the rest of the test binary's life, and the
// allocation-counting tests that ran next measured against nodes still busy on
// their own goroutines.
//
// The cluster is built inside a subtest, so a t.Cleanup it registered has
// run by the time t.Run returns; each node's context must then be cancelled.
func TestClusterHelperStopsItsNodesWhenItsTestReturns(t *testing.T) {
	var nodeContextErrs []func() error
	t.Run("leak", func(t *testing.T) {
		tc := newTestCluster(t, 2, 2)
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		for _, node := range tc.nodes {
			nodeContextErrs = append(nodeContextErrs, node.server.core.ctx.Err)
		}
	})

	for index, contextErr := range nodeContextErrs {
		if contextErr() == nil {
			t.Errorf("node %d of the helper's cluster is still running after its test returned: newTestCluster must register the stop itself", index+1)
		}
	}
}
