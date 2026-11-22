package wire

import (
	"context"
	"testing"
	"time"
)

// TestNodeOpensItsLogWithTheConfiguredPreallocation: NodeConfig.WALPreallocate reaches the
// log. Before it did, storage.wal_preallocate=on was accepted and silently ignored by the
// entry-ring node, so every flush of the log extended the file.
func TestNodeOpensItsLogWithTheConfiguredPreallocation(t *testing.T) {
	for _, preallocate := range []bool{false, true} {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		node, err := OpenNode(ctx, NodeConfig{Dir: t.TempDir(), WALPreallocate: preallocate})
		cancel()
		if err != nil {
			t.Fatalf("OpenNode with WALPreallocate=%v: %v", preallocate, err)
		}
		got := node.wal.Stats().Preallocated
		if err := node.Close(); err != nil {
			t.Errorf("closing the node: %v", err)
		}
		if got != preallocate {
			t.Fatalf("WALPreallocate=%v opened a log with Preallocated=%v", preallocate, got)
		}
	}
}
