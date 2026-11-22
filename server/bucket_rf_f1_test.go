package server

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Rule F1 (bucket-replication-rev8-spec §6.2; build doc step 3.1): an entry's
// addressees and the factor its concern resolves at are computed inside the
// bucket's fence, after the ownership check, and every later use in the
// write's path uses those values. A drop is stamped inside its fence too.

// holdAt installs a hook that holds the first entry of op for bucket on node
// nodeID, closes held when it does, and lets it go when release is closed.
func holdAt(t *testing.T, hook *atomic.Pointer[func(nodeID string, entry ReplEntry)], nodeID string, op transport.Opcode, bucket string) (held, release chan struct{}) {
	t.Helper()
	held, release = make(chan struct{}), make(chan struct{})
	var taken atomic.Bool
	hold := func(at string, entry ReplEntry) {
		if at != nodeID || transport.Opcode(entry.Op) != op || entry.Bucket != bucket || !taken.CompareAndSwap(false, true) {
			return
		}
		close(held)
		<-release
	}
	hook.Store(&hold)
	t.Cleanup(func() { hook.Store(nil) })
	return held, release
}

// awaitHeld fails the test unless held closes in time.
func awaitHeld(t *testing.T, held <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-held:
	case <-time.After(20 * time.Second):
		t.Fatalf("%s was never held", what)
	}
}

// awaitAnswer waits for a background request's answer and fails on an error.
func awaitAnswer(t *testing.T, answered <-chan backgroundAnswer, what string) *transport.Frame {
	t.Helper()
	select {
	case answer := <-answered:
		if answer.err != nil || answer.frame.Opcode == transport.OpError {
			t.Fatalf("%s failed: %v %s", what, answer.err, answer.payload())
		}
		return answer.frame
	case <-time.After(30 * time.Second):
		t.Fatalf("%s was never answered", what)
		return nil
	}
}

// TestAnEntryIsAddressedAtTheFactorInForceWhenItIsLogged is T-150a (RF-5): a
// write W held at beforeOwnerFenceHook, before its fence, while a decrease
// from 3 to 2 commits; W is logged after the alter, at 2, so the node the
// decrease demotes is not sent it. Read from the demoted node's own log, once
// a later database create shows its stream has carried everything before it.
// Addressed before the fence, W would be sent to the set at 3.
func TestAnEntryIsAddressedAtTheFactorInForceWhenItIsLogged(t *testing.T) {
	// The test polls for the demoted copy's outside mark, and the rearm of a
	// demoted copy releases it, which clears the mark, as soon as its survivors
	// cover it; on a loaded host the poll can miss the mark altogether. With
	// auto-rebalance off the rearm is idle and the mark stays (BUG-0487).
	tc := newTestClusterWith(t, 3, 3, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 3)
	owner := clusterOf(tc, 0)
	demoted := nodeIndexByAddr(t, tc.addrs, mustReplicaIDsAt(t, owner, testDatabase, bucket, 3)[2])

	held, release := holdAt(t, &beforeOwnerFenceHook, owner.self.ID, transport.OpPut, bucket)
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	write := sendInBackground(t, owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "w", Data: map[string]interface{}{"n": 1}})
	awaitHeld(t, held, "the write")
	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2}))
	// The decrease marks the demoted copy outside (rule F4), so W, if it were
	// sent there, would be refused, before the demoted node logs it, and the
	// owner's stream to it would stall until a replay, which addresses W at 2,
	// moved past it. What the owner pushes to it is read on the wire.
	demotedCore := tc.nodes[demoted].server.core
	pollUntil(t, "the demoted node marks its copy outside", func() bool { return outsideMarkOf(demotedCore, bucket) != 0 })
	pushed := recordPushes(peerOf(t, tc, 0, demoted))
	close(release)
	awaitAnswer(t, write, "the write")

	waitForBarrier(t, tc, 0, "barrier", demoted)
	if sent := pushed.entriesOf(transport.OpPut, bucket); sent != 0 {
		t.Fatalf("the owner pushed the write to node %d, %d times, after the alter logged before it demoted the node: "+
			"the node refuses it, its copy being marked outside, and the owner's stream to it stalls until a replay", demoted+1, sent)
	}
	if sent := walOpCount(t, demotedCore.wal, transport.OpPut, bucket); sent != 0 {
		t.Fatalf("node %d, demoted by the alter logged before the write, was sent the write: its log holds %d puts of %q", demoted+1, sent, bucket)
	}
}

// TestAnEntryReportsAndResolvesTheFactorItWasLoggedAt is T-150b (RF-5, R-1):
// a write W logged at 3 and held at replicaEnqueueHook, after its fence is
// released and before its concern is resolved, while a decrease from 3 to 2
// commits. W reports replication_factor 3, the factor it was logged at, read
// with its addressees inside the fence; read after the release, it reports 2.
// Its default concern is resolved from the same read, but the concern is
// majority at 3 and at 2 alike, so only the reported factor can tell the two
// reads apart. The alter's own routing waits behind W's turn while W is held,
// so this test runs for about one write-concern timeout.
func TestAnEntryReportsAndResolvesTheFactorItWasLoggedAt(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForServing(t, tc, testDatabase, bucket, 3)
	owner := clusterOf(tc, 0)

	held, release := make(chan struct{}), make(chan struct{})
	var taken atomic.Bool
	hold := func(entry ReplEntry) {
		if transport.Opcode(entry.Op) != transport.OpPut || entry.Bucket != bucket || !taken.CompareAndSwap(false, true) {
			return
		}
		close(held)
		<-release
	}
	replicaEnqueueHook.Store(&hold)
	t.Cleanup(func() { replicaEnqueueHook.Store(nil) })
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()

	write := sendInBackground(t, owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "w", Data: map[string]interface{}{"n": 1}})
	awaitHeld(t, held, "the write")
	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2}))
	close(release)
	frame := awaitAnswer(t, write, "the write")

	raw := bson.Raw(frame.Payload)
	if factor, ok := raw.Lookup("replication_factor").Int32OK(); !ok || factor != 3 {
		t.Errorf("the write logged at 3 reports replication_factor %v: %s", raw.Lookup("replication_factor"), raw)
	}
}

// TestADropIsStampedWithTheFactorInForceInsideItsFence is T-191 (RF-4): a drop
// held at beforeOwnerFenceHook while an increase from 2 to 3 commits is
// stamped inside its fence, after the increase: its logged previous_factor is
// 3, and so its concernSet, which deliveryTargets computes from that stamp, is
// the set at 3 without the owner. Stamped before the fence, it says 2.
func TestADropIsStampedWithTheFactorInForceInsideItsFence(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	createBucketAt(tc, 0, testDatabase, bucket, 2)
	waitForServing(t, tc, testDatabase, bucket, 2)
	owner := clusterOf(tc, 0)

	held, release := holdAt(t, &beforeOwnerFenceHook, owner.self.ID, transport.OpDeleteBucket, bucket)
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	drop := sendInBackground(t, owner, transport.OpDeleteBucket, DeleteBucketRequest{Database: testDatabase, Name: bucket})
	awaitHeld(t, held, "the drop")
	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}))
	close(release)
	awaitAnswer(t, drop, "the drop")

	entries, err := tc.nodes[0].server.core.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading the owner's WAL: %v", err)
	}
	for _, entry := range entries {
		if transport.Opcode(entry.Op) == transport.OpDeleteBucket && entry.Bucket == bucket {
			if stamped := previousFactorOf(entry.Data); stamped != 3 {
				t.Fatalf("the drop logged after the increase to 3 is stamped previous_factor %d", stamped)
			}
			return
		}
	}
	t.Fatal("the owner logged no drop of the bucket")
}
