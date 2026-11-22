package server

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// shipRecord is every entry sequence a peer was sent in a replication push
// that it answered, in the order sent.
type shipRecord struct {
	mu   sync.Mutex
	seqs []uint64
}

func (r *shipRecord) shipped() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint64(nil), r.seqs...)
}

// frozenRecordingConn holds every replication push until release is closed or
// the push's context ends, as heldPushConn does, and records the entries of
// each push the peer answers. Everything else passes.
type frozenRecordingConn struct {
	peerConn
	release <-chan struct{}
	record  *shipRecord
}

func (f *frozenRecordingConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	if op != transport.OpReplPush {
		return f.peerConn.SendRequest(ctx, lane, op, payload)
	}
	select {
	case <-f.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	var push ReplPushRequest
	if err := bson.Unmarshal(payload, &push); err != nil {
		return nil, fmt.Errorf("test connection: decoding a replication push: %w", err)
	}
	answer, err := f.peerConn.SendRequest(ctx, lane, op, payload)
	if err != nil {
		return nil, err
	}
	f.record.mu.Lock()
	defer f.record.mu.Unlock()
	for _, entry := range push.Entries {
		f.record.seqs = append(f.record.seqs, entry.Seq)
	}
	return answer, nil
}

// TestAStallEmptiesTheQueueAndTheLogDeliversEachEntryOnce is the stall's
// contract, step by step and without timing: a peer's pushes are frozen, so
// the sender sits in one ship and the queue fills; the write that finds it
// full stalls the stream instead of waiting, and the stall empties the queue;
// writes made while stalled are not queued at all; when the peer answers again
// the sender delivers everything from the log, each entry once and in
// sequence order. An entry left on the queue across the stall would be shipped
// a second time, out of order, after the replay that already carried it.
func TestAStallEmptiesTheQueueAndTheLogDeliversEachEntryOnce(t *testing.T) {
	tc := newTestClusterTuned(t, 2, 2, nil, func(_ int, c *Cluster) {
		// The frozen ship must outlast the writes that fill the queue; it gives
		// up at half this.
		c.wcTimeout = time.Minute
	})
	defer tc.stop()
	const bucket = "stalled"
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	createBucketAt(tc, 0, testDatabase, bucket, 0)
	waitForServing(t, tc, testDatabase, bucket, 2)
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, bucket))
	if err != nil {
		t.Fatalf("owner of %s: %v", bucket, err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	replica := tc.nodes[1-ownerIndex]
	ownerCluster := tc.nodes[ownerIndex].server.core.cluster
	toReplica := ownerCluster.peerFor(mustNode(t, ownerCluster, replica.addr))

	settle, cancelSettle := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelSettle()
	if err := catchUpPeer(settle, toReplica); err != nil {
		t.Fatalf("settle the stream before freezing it: %v", err)
	}

	release := make(chan struct{})
	var unfreezeOnce sync.Once
	unfreeze := func() { unfreezeOnce.Do(func() { close(release) }) }
	defer unfreeze()
	record := &shipRecord{}
	wrapPeerConn(toReplica, func(inner peerConn) peerConn {
		return &frozenRecordingConn{peerConn: inner, release: release, record: record}
	})

	var keys []string
	write := func() {
		key := fmt.Sprintf("k%05d", len(keys))
		keys = append(keys, key)
		putDocumentAt(tc, ownerIndex, testDatabase, bucket, key, "one")
	}
	// The sender holds at most one frame in its frozen ship; the queue holds
	// entryQueueDepth more. The write after those finds the queue full.
	bound := entryQueueDepth + maxReplBatch + 1
	for {
		if state, _ := toReplica.streamStatus(); state != streamStreaming {
			break
		}
		if len(keys) == bound {
			t.Fatalf("the stream to the frozen replica is still streaming after %d writes; its queue holds %d of %d",
				len(keys), len(toReplica.sendCh), entryQueueDepth)
		}
		write()
	}
	state, reason := toReplica.streamStatus()
	if state != streamStalled || reason != "queue full" {
		t.Fatalf("after %d writes the stream to the frozen replica is %s (%q), want stalled for a full queue", len(keys), state, reason)
	}
	queuedAtStall := len(toReplica.sendCh)
	if queuedAtStall != 0 {
		t.Errorf("the stall left %d entries on the queue; it must empty it", queuedAtStall)
	}
	stalledAt := len(keys)
	for i := 0; i < 10; i++ {
		write()
	}
	if queued := len(toReplica.sendCh); queued != queuedAtStall {
		t.Errorf("%d of the 10 writes made while the stream was stalled were queued; they are delivered from the log", queued-queuedAtStall)
	}
	if state, reason := toReplica.streamStatus(); state != streamStalled {
		t.Fatalf("the stream left the stall while its peer was frozen: %s (%q)", state, reason)
	}

	unfreeze()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := catchUpPeer(ctx, toReplica); err != nil {
		t.Fatalf("the stream did not recover after the peer answered: %v", err)
	}
	// One write more, queued behind anything still on the queue: once the
	// replica has acknowledged it, the queue has been served in full, so
	// every shipment the stall could have left behind is in the record.
	write()
	if err := catchUpPeer(ctx, toReplica); err != nil {
		t.Fatalf("the replica did not acknowledge a write made after the recovery: %v", err)
	}
	for _, key := range keys {
		if !holdsKeyLocallyIn(replica, bucket, key) {
			t.Fatalf("the replica lacks %s of the %d writes (the stall came at write %d)", key, len(keys), stalledAt)
		}
	}
	shipped := record.shipped()
	for i := 1; i < len(shipped); i++ {
		if shipped[i] <= shipped[i-1] {
			t.Fatalf("after the stall the replica was sent sequence %d after %d (shipment %d of %d): an entry was shipped twice or out of order",
				shipped[i], shipped[i-1], i, len(shipped))
		}
	}
	if len(shipped) < len(keys) {
		t.Fatalf("the replica was sent %d entries for %d writes", len(shipped), len(keys))
	}
}
