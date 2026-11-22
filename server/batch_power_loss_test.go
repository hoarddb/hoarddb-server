package server

import (
	"context"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestClusterBatchPowerLoss is T-C7 on the harness the power-loss tests share
// (the fault store, which drops every commit made without a sync): a batch of
// pushes to 20 buckets, two to each, is answered, the power goes, and the node
// restarts on the same store. Every push the reply acknowledged is in its
// bucket's queue, in order; a push the reply did not acknowledge may be absent.
// The ledger is the reply itself, as the social bench's acknowledged-write
// ledger is the server's answers. The fault store takes the place of a cluster
// owner's log: a node with the ring to itself has none, and its batch is made
// durable by the one sync it takes before the reply (§5.1). Break: answer the
// batch before that sync, and the acknowledged pushes are gone.
func TestClusterBatchPowerLoss(t *testing.T) {
	const buckets = 20
	dataDir := t.TempDir()
	kv := kvfault.New()
	s := newKVFaultServer(dataDir, kv)
	seedSocialUsers(t, s, 1, buckets)
	if err := kv.Sync(); err != nil {
		t.Fatalf("syncing the setup: %v", err)
	}

	var items []BatchItem
	var owners []int
	for number := 1; number <= 2; number++ {
		for u := 1; u <= buckets; u++ {
			items = append(items, pushItem(t, u, "Feed", map[string]interface{}{"n": number}))
			owners = append(owners, u)
		}
	}
	answers := sendBatchOK(t, context.Background(), s, batchFrameOf(t, items...))
	acknowledged := make(map[int]int, buckets)
	for i, answer := range answers {
		if replySucceeded(transport.Opcode(answer.Opcode), answer.Payload) {
			acknowledged[owners[i]]++
		}
	}
	if len(acknowledged) != buckets {
		t.Fatalf("setup: the batch acknowledged pushes to %d buckets, want %d", len(acknowledged), buckets)
	}

	kv.PowerLoss()
	restarted := newKVFaultServer(dataDir, kv)
	if err := restarted.LoadCatalog(); err != nil {
		t.Fatalf("LoadCatalog after the power loss: %v", err)
	}
	for u := 1; u <= buckets; u++ {
		held, err := restarted.FieldLen(userField(u, "Feed"))
		if err != nil {
			t.Fatalf("reading %s's feed after the power loss: %v", userBucket(u), err)
		}
		if held.Len < int64(acknowledged[u]) {
			t.Fatalf("after a power loss %s's feed holds %d pushes, but the batch's reply acknowledged %d: it answered before its writes were synced",
				userBucket(u), held.Len, acknowledged[u])
		}
	}
}
