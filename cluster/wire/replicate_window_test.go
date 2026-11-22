package wire

import (
	"context"
	"net"
	"testing"
)

// TestReplWindowReachesTheReplicator (BUG-0745): replication.repl_window is the number of
// unacknowledged frames a copy may hold, and a node opened with ReplWindow N runs a
// replicator with that window; before the fix the key was read and dropped, so every node
// ran the default 64. [GUARD: leave Window out of the ReplicatorConfig]
func TestReplWindowReachesTheReplicator(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	node, err := OpenNode(context.Background(), NodeConfig{
		Dir: t.TempDir(), NodeID: "node-a", ReplWindow: 5,
		Cluster: &ClusterOptions{Listener: listener, InsecureTestOnly: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })
	if got := node.cl.repl.cfg.Window; got != 5 {
		t.Fatalf("the replicator window is %d, want the configured 5", got)
	}
}

// TestDedupeWindowReachesTheReplicator (BUG-0749): a copy writes the dedupe entry of every
// record with the node's replication.dedupe_window and result cap, the values its own
// Writer holds. A node opened with DedupeWindow 3 whose replicator fell back to the default
// 8192 would keep entries the owner side had already let go of. [GUARD: leave DedupeWindow
// out of the ReplicatorConfig]
func TestDedupeWindowReachesTheReplicator(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	node, err := OpenNode(context.Background(), NodeConfig{
		Dir: t.TempDir(), NodeID: "node-a", DedupeWindow: 3,
		Cluster: &ClusterOptions{Listener: listener, InsecureTestOnly: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })
	got := node.cl.repl.cfg.DedupeWindow
	if got.Window != 3 || got.MaxResult != maxResultBytes {
		t.Fatalf("the replicator dedupe window is %+v, want window 3 and result cap %d", got, maxResultBytes)
	}
}
