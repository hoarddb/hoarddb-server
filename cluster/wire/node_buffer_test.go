package wire

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
)

// TestTheNodeWiresTheBuffer: a networked node's replicator and seed puller hold the same
// live-write buffer, its bound is replication.seed_buffer_bytes, and the replicator is the
// one that offers a frame pushed to a pair that is not ready. [BREAK: do not set
// ReplicatorConfig.Buffer]
func TestTheNodeWiresTheBuffer(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const configured = 4096
	node, err := OpenNode(ctx, NodeConfig{
		Dir: t.TempDir(), NodeID: "n1",
		Cluster: &ClusterOptions{Listener: listener, InsecureTestOnly: true, RaftLogger: hclog.NewNullLogger(), Seed: SeedOptions{BufferBytes: configured}},
	})
	if err != nil {
		t.Fatalf("opening the cluster node: %v", err)
	}
	t.Cleanup(func() { _ = node.Close() })
	replicator := node.cl.repl.cfg.Buffer
	if replicator == nil {
		t.Fatal("ReplicatorConfig.Buffer is nil: a write pushed to a pair that is not ready is dropped instead of kept for its drain")
	}
	parts := node.cl.seeding.Load()
	if parts == nil {
		t.Fatal("the node has no seed parts")
	}
	puller := parts.puller.cfg.Buffers
	if puller == nil {
		t.Fatal("PullerConfig.Buffers is nil: the pull has no buffer to drain after its snapshot")
	}
	offered, ok := replicator.(*seedBuffers)
	if !ok || offered != puller {
		t.Fatalf("the replicator offers %T %p and the puller drains %p: a write kept by one is never seen by the other", replicator, replicator, puller)
	}
	if offered.bound != configured {
		t.Errorf("the buffer bound is %d bytes, want the configured replication.seed_buffer_bytes %d", offered.bound, configured)
	}
}
