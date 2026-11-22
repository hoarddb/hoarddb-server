package server

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Dispatch 6, step 6.5: ownership that moved under a write, and what it
// stranded (spec §19.3.7, §19.3.12; T-102).

// TestAStretchExaminedBeforeOwnershipMovedIsStillCountedAfter: a replay that
// reads a stretch of the log while this node still owns the bucket finds
// nothing stranded in it; if its ship is then refused (an epoch refusal during
// a join) the next replay, after the adoption, finds entries of that stretch
// stranded and must count them. Only stranded sequences are counted, never a
// stretch merely read. Guard: marking the whole examined range counted — the
// second call then answered 1 for the verifier's case, not 2.
func TestAStretchExaminedBeforeOwnershipMovedIsStillCountedAfter(t *testing.T) {
	var ledger strandedLedger
	if got := ledger.count(nil, 0); got != 0 {
		t.Fatalf("a replay that stranded nothing counted %d", got)
	}
	if got := ledger.count([]uint64{150, 220}, 0); got != 2 {
		t.Errorf("entries 150 and 220, stranded after a replay had read 101 to 200 while this node owned the bucket, counted %d; want 2", got)
	}
	if got := ledger.count([]uint64{150, 220}, 0); got != 0 {
		t.Errorf("the same two stranded entries counted again (%d) by a later replay; each is counted once", got)
	}
	if total := ledger.total.Load(); total != 2 {
		t.Errorf("the ledger's total is %d, want 2", total)
	}
}

// TestTheStrandedLedgerStaysBoundedAcrossResets: every stream reset replays
// and calls count. The ledger holds only stranded sequences, merged where they
// are adjacent, and forgets those below the log's first sequence, which no
// replay can read again. Guard: a range per examined stretch, never pruned.
func TestTheStrandedLedgerStaysBoundedAcrossResets(t *testing.T) {
	var ledger strandedLedger
	for reset := uint64(0); reset < 10_000; reset++ {
		ledger.count([]uint64{500, 501, 502}, 1)
	}
	if got := len(ledger.counted); got != 1 {
		t.Errorf("after 10,000 replays over the same three adjacent stranded entries the ledger holds %d ranges, want 1", got)
	}
	// The log is pruned past the stranded entries: what lies below its first
	// sequence is forgotten.
	ledger.count([]uint64{2500}, 2000)
	if got := len(ledger.counted); got != 1 || ledger.counted[0] != (seqRange{first: 2500, last: 2500}) {
		t.Errorf("after the log was pruned to 2000 the ledger holds %v, want only [2500, 2500]", ledger.counted)
	}
	if total := ledger.total.Load(); total != 4 {
		t.Errorf("the ledger's total is %d, want 4", total)
	}
}

// localCountOf is a node's own count of a bucket, or -1 when it holds none.
func localCountOf(t *testing.T, s *Server, bucket string) int64 {
	t.Helper()
	if !s.bucketStoreOpen(testDatabase, bucket) {
		return -1
	}
	counted, err := s.Count(CountRequest{Database: testDatabase, Bucket: bucket})
	if err != nil {
		t.Fatalf("counting %s on %s: %v", bucket, s.cluster.self.ID, err)
	}
	return counted.Count
}

// bucketMovingTo returns a bucket name that the three nodes of tc place on
// a set and the ring with joiner added places on joiner, together with the
// index of a node that is a replica on both rings and not the old owner.
func bucketMovingTo(t *testing.T, tc *testCluster, joiner string) (string, int) {
	t.Helper()
	before, after := ring.New(3), ring.New(3)
	for _, id := range append(append([]string(nil), tc.addrs...), joiner) {
		if id != joiner {
			if err := before.AddNode(ring.Node{ID: id, Addr: id}); err != nil {
				t.Fatalf("building a ring: %v", err)
			}
		}
		if err := after.AddNode(ring.Node{ID: id, Addr: id}); err != nil {
			t.Fatalf("building a ring: %v", err)
		}
	}
	for i := 0; i < 1000; i++ {
		name := fmt.Sprintf("stranded%03d", i)
		key := placementOf(testDatabase, name).encoded
		oldSet, err := before.Replicas(key, 3)
		if err != nil {
			t.Fatalf("placing %s: %v", name, err)
		}
		newSet, err := after.Replicas(key, 3)
		if err != nil || newSet[0].ID != joiner {
			continue
		}
		for _, stays := range newSet[1:] {
			if stays.ID != oldSet[0].ID {
				return name, nodeIndexByAddr(t, tc.addrs, stays.ID)
			}
		}
	}
	t.Fatalf("no bucket name of 1000 moves to %s with a replica that stays", joiner)
	return "", 0
}

// heldDoneConn holds a pull's MIGRATE_DONE until release is closed or the
// request's context ends, and signals reached when it holds the first. The
// copy has landed and opened by then; the puller has not heard the source's
// answer. Everything else passes.
type heldDoneConn struct {
	peerConn
	reached chan<- struct{}
	release <-chan struct{}
}

func (h *heldDoneConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	if op == transport.OpMigrateDone {
		select {
		case h.reached <- struct{}{}:
		default:
		}
		select {
		case <-h.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return h.peerConn.SendRequest(ctx, lane, op, payload)
}

// heldDone is a hold on the MIGRATE_DONE requests sent on one peer.
type heldDone struct {
	reached <-chan struct{}
	release func()
}

// holdMigrateDone holds every MIGRATE_DONE sent to p until release is
// called; release is safe to call more than once.
func holdMigrateDone(p *peer) heldDone {
	reached := make(chan struct{}, 1)
	gate := make(chan struct{})
	wrapPeerConn(p, func(inner peerConn) peerConn { return &heldDoneConn{peerConn: inner, reached: reached, release: gate} })
	var once sync.Once
	return heldDone{reached: reached, release: func() { once.Do(func() { close(gate) }) }}
}

// TestAnEntryStrandedByAnOwnerChangeIsCountedAndNotShipped is T-102 (A-6.4),
// re-aimed by replication dispatch 10b (rev 7 step 10.4): a replica's
// connection is frozen, ten writes at `one` are made to a bucket its owner
// holds, and a fourth node joins and becomes the bucket's owner. The old owner
// has ceded and finishes delivering its term (spec §20.8.3): once the replica
// answers again the ten are on it and nothing is stranded. When the old owner
// is stopped before it delivers, the replica is copied afresh from the new
// owner instead, and that copy is counted. Guard: strand a ceded owner's
// entries — the replica is then copied whole in the first case too.
func TestAnEntryStrandedByAnOwnerChangeIsCountedAndNotShipped(t *testing.T) {
	type frozenOwnerChange struct {
		tc                       *testCluster
		owner, replica           *Cluster
		ownerIndex               int
		replicaIndex             int
		bucket                   string
		joiner                   *testNode
		unfreeze                 func()
		heldByTheReplica         int64
		copiesBeforeOnTheReplica int64
	}
	setUp := func(t *testing.T) *frozenOwnerChange {
		tc := newTestCluster(t, 3, 3)
		ports := reserveNodePorts(t)
		bucket, replicaIndex := bucketMovingTo(t, tc, ports.ringAddr)
		tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeFIFO})
		waitForServing(t, tc, testDatabase, bucket, 3)
		ownerIndex := ownerIndexOf(t, tc, bucket)
		change := &frozenOwnerChange{tc: tc, owner: clusterOf(tc, ownerIndex), replica: clusterOf(tc, replicaIndex),
			ownerIndex: ownerIndex, replicaIndex: replicaIndex, bucket: bucket}
		change.unfreeze = freezePeer(change.owner.peerFor(mustNode(t, change.owner, tc.addrs[replicaIndex])))
		for n := 0; n < 10; n++ {
			tc.ok(ownerIndex, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: bucket,
				Data: map[string]interface{}{"n": n}, WriteConcern: "one"})
		}
		change.heldByTheReplica = localCountOf(t, change.replica.s, bucket)
		change.copiesBeforeOnTheReplica = change.replica.takeOverCopies.Load()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := change.owner.proposeJoin(ctx, ports.ringNode()); err != nil {
			t.Fatalf("proposeJoin: %v", err)
		}
		change.joiner = startJoinTarget(t, tc, ports, tc.token)
		t.Cleanup(func() { _ = change.joiner.client.Close() })
		waitTakenOver(t, change.joiner.server.core, testDatabase, bucket)
		return change
	}

	t.Run("the old owner finishes delivering", func(t *testing.T) {
		change := setUp(t)
		defer change.tc.stop()
		defer change.unfreeze()
		if change.heldByTheReplica >= 10 {
			t.Fatalf("setup: the frozen replica holds %d elements before the join", change.heldByTheReplica)
		}
		change.unfreeze()
		waitFollowing(t, change.replica.s, change.joiner.server.core, testDatabase, change.bucket)
		if got := localCountOf(t, change.replica.s, change.bucket); got != 10 {
			t.Errorf("the replica that was frozen holds %d elements after the owner change, want the 10 the old owner finished delivering", got)
		}
		if got := change.owner.stranded.total.Load(); got != 0 {
			t.Errorf("the old owner counts %d stranded entries, want 0: it finished delivering its term", got)
		}
		if copies := change.replica.takeOverCopies.Load() - change.copiesBeforeOnTheReplica; copies != 0 {
			t.Errorf("the replica was copied afresh %d times; the old owner's stream delivered what it lacked", copies)
		}
	})

	// followsByOneCopy waits for the replica's copy afresh, then for it to
	// follow the new owner, and checks it was copied exactly once and holds
	// the ten. The copy is waited for on its counter: copyAfresh counts it
	// only when the pull's MIGRATE_DONE round trip to the new owner returns,
	// after the landed copy is already open on the owner's term with the ten,
	// so the follow condition alone can hold while the counter still reads 0
	// (BUG-0487).
	followsByOneCopy := func(t *testing.T, change *frozenOwnerChange) {
		t.Helper()
		copied := func() int64 { return change.replica.takeOverCopies.Load() - change.copiesBeforeOnTheReplica }
		if !holdsWithin(takeOverWait, func() bool { return copied() >= 1 }) {
			t.Fatalf("the replica was not copied afresh within %v (copies %d, holding %d elements): its stream from the stopped owner never delivered, "+
				"so it owes a whole copy from the new owner", takeOverWait, copied(), localCountOf(t, change.replica.s, change.bucket))
		}
		waitFollowing(t, change.replica.s, change.joiner.server.core, testDatabase, change.bucket)
		if got := localCountOf(t, change.replica.s, change.bucket); got != 10 {
			t.Errorf("the replica holds %d elements after it followed the new owner, want 10", got)
		}
		if copies := copied(); copies != 1 {
			t.Errorf("the replica was copied afresh %d times, want 1: its stream from the stopped owner never delivered", copies)
		}
	}

	t.Run("the old owner is stopped before it delivers", func(t *testing.T) {
		change := setUp(t)
		defer change.tc.stop()
		change.tc.stopNodeKeepingData(change.ownerIndex)
		// Released only once the test is done: a held push let go before the
		// stopped owner's context ends would still deliver.
		defer change.unfreeze()
		followsByOneCopy(t, change)
	})

	// The copy's done handshake is held until the landed copy is visible: the
	// replica follows the new owner with the ten while its counter still reads
	// 0, the state a run under -race reached on its own in about one run in
	// seven. The wait must see the copy through it. Guard: read the counter
	// once the replica follows, as the subtest above did — 0 copies.
	t.Run("the copy is counted once its done handshake returns", func(t *testing.T) {
		change := setUp(t)
		defer change.tc.stop()
		done := holdMigrateDone(change.replica.peerFor(mustNode(t, change.replica, change.joiner.addr)))
		defer done.release()
		change.tc.stopNodeKeepingData(change.ownerIndex)
		defer change.unfreeze()
		select {
		case <-done.reached:
		case <-time.After(takeOverWait):
			t.Fatalf("setup: the replica sent no MIGRATE_DONE to the new owner within %v: it was not copied afresh", takeOverWait)
		}
		waitFollowing(t, change.replica.s, change.joiner.server.core, testDatabase, change.bucket)
		if copies := change.replica.takeOverCopies.Load() - change.copiesBeforeOnTheReplica; copies != 0 {
			t.Fatalf("setup: the copy was counted (%d) while its done handshake was held", copies)
		}
		// Let go after the wait has begun reading the counter.
		time.AfterFunc(500*time.Millisecond, done.release)
		followsByOneCopy(t, change)
	})

	// A write that was in flight when the bucket moved is answered "unknown"
	// (A-6.5): unavailable, GAP-24's exit 8, and not a conflict.
	if code := codeFor(ownershipMovedErr(testDatabase, "any")); code != transport.ErrUnavailable {
		t.Errorf("ErrOwnershipMoved reaches the wire as %v, want unavailable", code)
	}
}

// TestTheOwnershipMovedMessageMakesNoClaimAboutReplication: since BUG-0528 a
// ceded owner queues a data write for the members the sender's rule addresses
// it to, so the old sentence "stored on this node only ... will not be
// replicated from here" was false. The message must name the bucket, say the
// outcome is unknown, give the only correct instruction (read it back, then
// retry if it is missing), and claim neither delivery nor non-delivery: a
// create or a drop is not delivered. Guard: the old sentence, whose reason is
// wrong for every path — a developer treats a write that was replicated as an
// unreplicated copy and escalates, or retries a non-idempotent write without
// reading back first.
func TestTheOwnershipMovedMessageMakesNoClaimAboutReplication(t *testing.T) {
	message := ownershipMovedErr(testDatabase, "users").Error()
	if !strings.Contains(message, testDatabase+".users") {
		t.Errorf("the ownership-moved message does not name %s.users: %q", testDatabase, message)
	}
	if !strings.Contains(message, "outcome is unknown") {
		t.Errorf("the ownership-moved message does not say the outcome is unknown: %q", message)
	}
	if !strings.Contains(message, "Read it back, then retry if it is missing") {
		t.Errorf("the ownership-moved message does not give the read-back-then-retry instruction: %q", message)
	}
	for _, claim := range []string{"this node only", "will not be replicated"} {
		if strings.Contains(message, claim) {
			t.Errorf("the ownership-moved message still claims %q, false for a data write a ceded owner delivers: %q", claim, message)
		}
	}
}
