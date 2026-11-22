package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestAReplicaReplayingItsBacklogIsNotWaitedForByAllWrites is BUG-0602's
// in-sync half. A replica stopped while writes run comes back with a backlog;
// the owner replays the log to it and enters streaming once a read reaches the
// tail, then ships the range up to the boundary it recorded there. Until that
// range is shipped the replica is behind, so it is not in sync: an `all` write
// made meanwhile is answered at once with the copies that hold it, not after
// the write-concern timeout. Once the replica has reached the tail, `all`
// writes count it again.
//
// The replay's last stretch is made deterministic with beforeReplayShipHook:
// while the stream is still replaying its backlog, every frame is preceded by
// a few writes at `one`, which land above the read's bound and so after the
// boundary the stream enters streaming at; the first frame of that boundary
// range is then held until the `all` write has been answered.
func TestAReplicaReplayingItsBacklogIsNotWaitedForByAllWrites(t *testing.T) {
	const wcTimeout = 10 * time.Second
	tc := newTestClusterTuned(t, 3, 3, nil, func(_ int, c *Cluster) { c.wcTimeout = wcTimeout })
	defer tc.stop()
	ownerIndex, replicaIndex := ownerAndReplica(t, tc)
	otherIndex := 3 - ownerIndex - replicaIndex
	ownerCluster := tc.nodes[ownerIndex].server.core.cluster
	toReplica := ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[replicaIndex]))
	toOther := ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[otherIndex]))

	start := stopNode(t, tc, replicaIndex)
	for i := 0; i < 100; i++ {
		putDocumentAt(tc, ownerIndex, testDatabase, testBucket, fmt.Sprintf("backlog-%03d", i), "one")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := waitForStream(ctx, toReplica, func() bool { state, _ := toReplica.streamStatus(); return state == streamStalled }); err != nil {
		t.Fatalf("the stream to the stopped replica did not stall: %v", err)
	}

	hookWriter := tc.dial(ownerIndex)
	defer func() { _ = hookWriter.Close() }()
	var (
		hookWrites   atomic.Int64
		hookErrMu    sync.Mutex
		hookErr      error
		inBoundary   = make(chan struct{})
		release      = make(chan struct{})
		enteredOnce  sync.Once
		releasedOnce sync.Once
	)
	releaseHold := func() { releasedOnce.Do(func() { close(release) }) }
	defer releaseHold()
	hold := func(peerID string, frame []ReplEntry) {
		if peerID != toReplica.node.ID || frame[0].Bucket != testBucket {
			return
		}
		if state, _ := toReplica.streamStatus(); state == streamStreaming {
			enteredOnce.Do(func() { close(inBoundary) })
			<-release
			return
		}
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("moving-tail-%04d", hookWrites.Add(1))
			if err := putAtOne(hookWriter, key); err != nil {
				hookErrMu.Lock()
				hookErr = err
				hookErrMu.Unlock()
				return
			}
		}
	}
	beforeReplayShipHook.Store(&hold)
	defer beforeReplayShipHook.Store(nil)

	start()
	if err := waitForStream(ctx, toReplica, func() bool {
		select {
		case <-inBoundary:
			return true
		default:
			return false
		}
	}); err != nil {
		t.Fatalf("the returned replica's replay never reached the range up to its boundary: %v", err)
	}
	hookErrMu.Lock()
	if hookErr != nil {
		t.Fatalf("a write at one made during the replay failed: %v", hookErr)
	}
	hookErrMu.Unlock()
	if err := catchUpPeer(ctx, toOther); err != nil {
		t.Fatalf("the replica that stayed up is not caught up: %v", err)
	}

	if toReplica.inSync() {
		t.Errorf("the stream to the returned replica counts as in sync while it is still replaying its backlog " +
			"(streaming, boundary not yet shipped)")
	}
	client := tc.dial(ownerIndex)
	defer func() { _ = client.Close() }()
	during, err := writeConfirmed(client, "all-while-replaying", "all")
	if err != nil {
		t.Fatal(err)
	}
	if during.refusal != "" {
		t.Fatalf("an all write made while the returned replica replays was refused after %s: %s; "+
			"it waited for a replica that is not in sync", during.took.Round(time.Millisecond), during.refusal)
	}
	if bound := wcTimeout / 5; during.took > bound {
		t.Errorf("an all write made while the returned replica replays took %s, want under %s: it waited for that replica",
			during.took.Round(time.Millisecond), bound)
	}
	if during.confirmed != 2 {
		t.Errorf("an all write made while the returned replica replays answered copies_confirmed %d, want 2 "+
			"(the owner and the replica that stayed up)", during.confirmed)
	}
	if holdsKeyLocallyIn(tc.nodes[replicaIndex], testBucket, "all-while-replaying") {
		t.Errorf("the returned replica holds the write while its replay is held; the test did not hold it")
	}

	releaseHold()
	if err := catchUpPeer(ctx, toReplica); err != nil {
		t.Fatalf("the returned replica did not reach the tail after the replay was released: %v", err)
	}
	if !toReplica.inSync() {
		t.Errorf("the stream to the returned replica is caught up but not in sync")
	}
	after, err := writeConfirmed(client, "all-after-catch-up", "all")
	if err != nil {
		t.Fatal(err)
	}
	if after.refusal != "" || after.confirmed != 3 {
		t.Errorf("an all write after the returned replica reached the tail answered copies_confirmed %d (refusal %q), want 3",
			after.confirmed, after.refusal)
	}
}

// putAtOne writes key to the test bucket at `one` through client.
func putAtOne(client *transport.DriverClient, key string) error {
	payload, err := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: testBucket, Key: key,
		Data: map[string]interface{}{"n": 1}, WriteConcern: "one"})
	if err != nil {
		return fmt.Errorf("encoding %s: %w", key, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	frame, err := client.SendRequest(ctx, transport.OpPut, payload)
	if err != nil {
		return fmt.Errorf("writing %s: %w", key, err)
	}
	if frame.Opcode == transport.OpError {
		return fmt.Errorf("writing %s: the owner refused it: %s", key, frame.Payload)
	}
	return nil
}
