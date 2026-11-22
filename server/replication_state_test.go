package server

import "testing"

// newStateTestCluster builds the least a Cluster needs to keep its replication
// positions, over a node store of its own.
func newStateTestCluster(t *testing.T, dataDir string) *Cluster {
	t.Helper()
	return newPositionTestCluster(t, dataDir, openPositionTestStore(t, t.TempDir()), nil)
}
