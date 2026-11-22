package server

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestReplicaSettleAcrossRotationWithPreallocatedLog: a replica whose log
// preallocates and rotates every few entries applies each replicated write
// only after the flush that covers it has returned, and applies every write
// exactly once. Writes arrive one at a time, so at an apply the replica's last
// logged sequence is the entry's own: it must already be synced.
func TestReplicaSettleAcrossRotationWithPreallocatedLog(t *testing.T) {
	previousResolver, previousSegment := walPreallocateResolver, walSegmentBytes
	walPreallocateResolver = func(cfg *config.Config, walDir string) walModeResolution {
		return walModeResolution{On: true, Configured: config.WALPreallocateOn}
	}
	walSegmentBytes = 16 << 10
	t.Cleanup(func() { walPreallocateResolver, walSegmentBytes = previousResolver, previousSegment })

	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	for index, node := range tc.nodes {
		if !node.server.core.wal.Stats().Preallocated {
			t.Fatalf("node %d opened a log that does not preallocate", index+1)
		}
	}
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})

	var applied, unsynced atomic.Int64
	hook := func(nodeID string, repl ReplEntry) {
		for _, node := range tc.nodes {
			core := node.server.core
			if core.cluster.self.ID != nodeID || transport.Opcode(repl.Op) != transport.OpPut {
				continue
			}
			applied.Add(1)
			if core.wal.SyncedSequence() < core.wal.LastSequence() {
				unsynced.Add(1)
			}
		}
	}
	beforeLocalApplyHook.Store(&hook)
	t.Cleanup(func() { beforeLocalApplyHook.Store(nil) })

	const writes = 120
	padding := strings.Repeat("p", 400)
	for i := 0; i < writes; i++ {
		requireEveryCopy(t, tc.frame(0, transport.OpPut, InsertRequest{
			Database: testDatabase, Bucket: testBucket, Key: fmt.Sprintf("k%03d", i),
			Data: map[string]interface{}{"n": i, "pad": padding}, WriteConcern: "all",
		}), fmt.Sprintf("write %d", i))
	}

	if got := unsynced.Load(); got != 0 {
		t.Errorf("%d applies ran before the flush covering them had returned", got)
	}
	if got := applied.Load(); got != 2*writes {
		t.Errorf("%d applies for %d writes on 2 copies, want %d: each exactly once", got, writes, 2*writes)
	}
	for index, node := range tc.nodes {
		if count := tc.localCount(index); count != writes {
			t.Errorf("node %d holds %d documents, want %d", index+1, count, writes)
		}
		if rotations := node.server.core.wal.Stats().Rotations; rotations == 0 {
			t.Errorf("node %d's log never rotated; the test did not span a rotation", index+1)
		}
	}
}
