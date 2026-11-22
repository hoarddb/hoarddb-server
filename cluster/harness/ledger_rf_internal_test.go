package harness

import "testing"

// TestARestartedNodesEmptyStateDoesNotMakeTheReplicationFactorZero: a node that has just booted
// reports no database, and RF 0 would make I4 call a sound applied_degraded ack a violation
// (seen in the restart storm). The ledger keeps the last factor it saw.
// [BREAK: settleRF returns found].
func TestARestartedNodesEmptyStateDoesNotMakeTheReplicationFactorZero(t *testing.T) {
	l := NewLedger(t, LedgerOptions{DB: "app", Bucket: "users"})
	if got := l.settleRF(2); got != 2 {
		t.Fatalf("a reported factor of 2 settled as %d", got)
	}
	if got := l.settleRF(0); got != 2 {
		t.Fatalf("a node with no state loaded settled the factor as %d, want the remembered 2", got)
	}
}

// TestAKilledNodeIsNotRunningAndDoesNotEndTheTest: a schedule can kill a node between a
// client's Running check and its use of the node, and Node would then Fatalf from the
// client's goroutine (seen under load in the fixed-seed schedule: `node "n2" is not
// running`). nodeIfRunning answers false for a killed node and leaves the test running, so
// the ledger records the write as refused.
// [BREAK: nodeIfRunning calls c.Node, which Fatalfs for a killed node].
func TestAKilledNodeIsNotRunningAndDoesNotEndTheTest(t *testing.T) {
	c := NewCluster(t, 3, Options{})
	if node, ok := c.nodeIfRunning("n2"); !ok || node == nil {
		t.Fatalf("n2 is running but nodeIfRunning answered %v, %v", node, ok)
	}
	c.Kill("n2")
	if node, ok := c.nodeIfRunning("n2"); ok || node != nil {
		t.Fatalf("n2 was killed but nodeIfRunning answered %v, %v", node, ok)
	}
	if node, ok := c.nodeIfRunning("n9"); ok || node != nil {
		t.Fatalf("n9 never existed but nodeIfRunning answered %v, %v", node, ok)
	}
}
