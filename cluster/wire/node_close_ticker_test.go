package wire

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// TestClosingANodeStopsItsRetentionTicker: the node's one ticker must not outlive Close.
func TestClosingANodeStopsItsRetentionTicker(t *testing.T) {
	node, err := OpenNode(context.Background(), NodeConfig{Dir: filepath.Join(t.TempDir(), "ring"), RetentionCheck: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("OpenNode: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select { // a tick already buffered before the stop is not a leak
	case <-node.retention.C:
	default:
	}
	time.Sleep(60 * time.Millisecond)
	select {
	case <-node.retention.C:
		t.Fatal("the retention ticker still ticks after Close: Node.Close must stop it")
	default:
	}
}
