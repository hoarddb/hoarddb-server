package server

import (
	"errors"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// TestFencedReplicaWALDoesNotGrowPerRetry is the first half of the fix. A replica
// that is in a bucket's set but has no open store for it yet (a newcomer
// mid-backfill) refuses every push of that bucket, and the owner re-sends the
// push every replRetryInterval. The refusal is correct; appending the refused
// entry to this node's WAL on every re-send is not — the log grows by one
// entry per retry for as long as the replica stays fenced.
//
// The control arm pins that the fix does not stop a replica logging what it
// can apply: a push to a bucket this node serves still appends exactly once.
func TestFencedReplicaWALDoesNotGrowPerRetry(t *testing.T) {
	dataDir := t.TempDir()
	kv := kvfault.New()
	s := newKVFaultServer(dataDir, kv)
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "served", Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("creating the served bucket: %+v, %v", resp, err)
	}
	c := retentionTestCluster(t, s, dataDir)
	defer c.cancel()

	const retries = 5
	before := c.walLastSequence()
	for retry := 0; retry < retries; retry++ {
		err := putThrough(c, asReplica, "fenced", "k", retry)
		if err == nil {
			t.Fatalf("retry %d: a push to a bucket this node has no store for was accepted", retry)
		}
		if !errors.Is(err, ErrJoinSyncIncomplete) {
			t.Fatalf("retry %d: refused with %v, want the join-sync refusal the owner retries on", retry, err)
		}
	}
	if grown := c.walLastSequence() - before; grown > 1 {
		t.Errorf("the WAL grew by %d entries over %d refused re-sends of one push; a retried push must be logged at most once",
			grown, retries)
	}

	before = c.walLastSequence()
	if err := putThrough(c, asReplica, "served", "k", "v"); err != nil {
		t.Fatalf("a push to a served bucket: %v", err)
	}
	if grown := c.walLastSequence() - before; grown != 1 {
		t.Errorf("an applied push grew the WAL by %d entries, want 1", grown)
	}
}
