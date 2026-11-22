package server

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// walSeqs reads the retained sequences of a WAL from `from` on, the exact
// range catch-up would ship to a peer positioned just below `from`.
func walSeqs(t *testing.T, w *storage.WAL, from uint64) []uint64 {
	t.Helper()
	entries, err := w.ReadFrom(from)
	if err != nil {
		t.Fatalf("read wal from seq %d: %v", from, err)
	}
	seqs := make([]uint64, 0, len(entries))
	for _, e := range entries {
		seqs = append(seqs, e.Seq)
	}
	return seqs
}

// TestRetentionNeverPrunesPastALaggingReplica pins the retention gate: while
// a replica is down, a retention pass must leave every entry that replica
// still needs readable from the log, and the safe prune bound must be exactly
// the lagging replica's confirmed position. Only after the replica catches up
// may the pass advance to the tail.
func TestRetentionNeverPrunesPastALaggingReplica(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase,
		Name:     testBucket,
		Type:     BucketTypeHash,
	})

	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve owner: %v", err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	downID := ""
	for _, id := range mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, testBucket) {
		if id != owner.ID {
			downID = id
			break
		}
	}
	if downID == "" {
		t.Fatalf("bucket %q has no replica other than its owner %s", testBucket, owner.ID)
	}
	downIndex := nodeIndexByAddr(t, tc.addrs, downID)

	// Five writes at w=all: every node, including the one about to go down,
	// confirms them.
	const before = 5
	for i := 0; i < before; i++ {
		key := fmt.Sprintf("pre%02d", i)
		if frame := tc.writeConcerned(0, key, "all"); frame.Opcode == transport.OpError {
			t.Fatalf("write %s at w=all: %s", key, string(frame.Payload))
		}
	}

	_ = tc.nodes[downIndex].client.Close()
	tc.nodes[downIndex].client = nil
	if err := tc.nodes[downIndex].server.Stop(); err != nil {
		t.Fatalf("stop node %d: %v", downIndex+1, err)
	}
	tc.nodes[downIndex].server = nil

	coordIndex := tc.coordinatorIndex(t, downIndex, owner.ID)
	const after = 4
	for i := 0; i < after; i++ {
		key := fmt.Sprintf("post%02d", i)
		if frame := tc.writeConcerned(coordIndex, key, "majority"); frame.Opcode == transport.OpError {
			t.Fatalf("write %s at w=majority: %s", key, string(frame.Payload))
		}
	}

	oc := tc.nodes[ownerIndex].server.core.cluster
	ownerWal := tc.nodes[ownerIndex].server.core.wal
	if ownerWal == nil {
		t.Fatal("owner has no WAL")
	}
	lagPeer := oc.peerFor(mustNode(t, oc, tc.addrs[downIndex]))
	lagAck := lagPeer.ackedThrough(oc.self.ID)
	if lagAck == 0 {
		t.Fatal("the replica confirmed nothing at w=all; retention cannot be tested from a zero position")
	}
	if lagAck >= ownerWal.LastSequence() {
		t.Fatalf("replica is not behind: acked %d, log tail %d", lagAck, ownerWal.LastSequence())
	}
	if bound := oc.replicaAckBound(); bound != lagAck {
		t.Fatalf("replicaAckBound = %d, want the lagging replica's position %d", bound, lagAck)
	}

	// The tail the lagging replica needs must survive a retention pass
	// byte-for-byte (this is what catch-up ships from ack+1).
	wantSeqs := walSeqs(t, ownerWal, lagAck+1)
	if err := oc.pruneWAL(); err != nil {
		t.Fatalf("pruneWAL with a replica down: %v", err)
	}
	if gotSeqs := walSeqs(t, ownerWal, lagAck+1); !equalSeqs(wantSeqs, gotSeqs) {
		t.Fatalf("pruneWAL removed entries a lagging replica needs: before %v after %v", wantSeqs, gotSeqs)
	}
	first, err := ownerWal.FirstSequence()
	if err != nil {
		t.Fatalf("first sequence: %v", err)
	}
	if first > lagAck+1 {
		t.Fatalf("retained horizon %d is past what the lagging replica needs (position %d)", first, lagAck)
	}

	// Bring the replica back and repair it from the owner's log.
	restarted := tc.startNode(downIndex, filepath.Dir(tc.nodes[downIndex].cfg.Storage.DataDir), tc.token)
	restarted.cfg = tc.nodes[downIndex].cfg
	tc.nodes[downIndex] = restarted

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := replayToPeer(ctx, lagPeer); err != nil {
		t.Fatalf("catch-up after restart: %v", err)
	}
	if a := lagPeer.ackedThrough(oc.self.ID); a != ownerWal.LastSequence() {
		t.Fatalf("replica acked %d after catch-up, want the log tail %d", a, ownerWal.LastSequence())
	}

	// Once nothing is behind, the gate may advance to the tail.
	if err := oc.pruneWAL(); err != nil {
		t.Fatalf("pruneWAL after catch-up: %v", err)
	}
	if bound := oc.replicaAckBound(); bound != ownerWal.LastSequence() {
		t.Fatalf("after catch-up replicaAckBound = %d, want %d", bound, ownerWal.LastSequence())
	}
}

func equalSeqs(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestCatchUpRefusesWhenPeerIsBelowRetainedHorizon pins the horizon guard: a
// peer whose confirmed position is below the log's earliest retained entry
// must be refused, not silently skipped. 20 MB payloads overflow the 64 MB
// segment alone, so each entry gets its own segment and a truncation can
// actually delete whole files and move the horizon.
// horizonScenario is a three-node cluster whose bucket's owner holds two
// entries every node confirmed and two more only the owner and one replica
// have, with the other replica stopped.
type horizonScenario struct {
	tc        *testCluster
	oc        *Cluster
	ownerWal  *storage.WAL
	lagPeer   *peer
	downIndex int
	lagAck    uint64
}

// stopOneReplicaBehind builds a horizonScenario: the replica is stopped and
// lagAck is the owner's cursor for it as sampled just after.
func stopOneReplicaBehind(t *testing.T, tc *testCluster) horizonScenario {
	t.Helper()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase,
		Name:     testBucket,
		Type:     BucketTypeHash,
	})

	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve owner: %v", err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	downID := ""
	for _, id := range mustReplicaIDs(t, tc.nodes[0].server.core.cluster, testDatabase, testBucket) {
		if id != owner.ID {
			downID = id
			break
		}
	}
	if downID == "" {
		t.Fatalf("bucket %q has no replica other than its owner %s", testBucket, owner.ID)
	}
	downIndex := nodeIndexByAddr(t, tc.addrs, downID)

	blob := strings.Repeat("x", 20<<20)
	put := func(index int, key, wc string) {
		t.Helper()
		if frame := tc.frame(index, transport.OpPut, InsertRequest{
			Database:     testDatabase,
			Bucket:       testBucket,
			Key:          key,
			Data:         map[string]interface{}{"blob": blob},
			WriteConcern: wc,
		}); frame.Opcode == transport.OpError {
			t.Fatalf("put %s at w=%s: %s", key, wc, string(frame.Payload))
		}
	}

	// Two writes confirmed by every node, then the replica goes down.
	put(0, "big-1", "all")
	put(0, "big-2", "all")

	_ = tc.nodes[downIndex].client.Close()
	tc.nodes[downIndex].client = nil
	if err := tc.nodes[downIndex].server.Stop(); err != nil {
		t.Fatalf("stop node %d: %v", downIndex+1, err)
	}
	tc.nodes[downIndex].server = nil

	coordIndex := tc.coordinatorIndex(t, downIndex, owner.ID)
	put(coordIndex, "big-3", "majority")
	put(coordIndex, "big-4", "majority")

	oc := tc.nodes[ownerIndex].server.core.cluster
	ownerWal := tc.nodes[ownerIndex].server.core.wal
	if ownerWal == nil {
		t.Fatal("owner has no WAL")
	}
	lagPeer := oc.peerFor(mustNode(t, oc, tc.addrs[downIndex]))
	lagAck := lagPeer.ackedThrough(oc.self.ID)
	return horizonScenario{tc: tc, oc: oc, ownerWal: ownerWal, lagPeer: lagPeer, downIndex: downIndex, lagAck: lagAck}
}

// requireAckWithinReplicaAndHorizon asserts the invariant behind refusing a
// replica below the retained horizon.
func requireAckWithinReplicaAndHorizon(t *testing.T, scenario horizonScenario, first uint64) (ack, reported uint64) {
	t.Helper()
	reported = scenario.tc.nodes[scenario.downIndex].server.core.cluster.recordedPosition(scenario.oc.self.ID).AppliedThrough
	ack = scenario.lagPeer.ackedThrough(scenario.oc.self.ID)
	// The handshake sets the cursor to the replica's own report, so the ack may
	// differ from a sample taken earlier. Only a move past that report, or to
	// the horizon, is an acknowledgement across a hole.
	if ack > reported {
		t.Fatalf("the sender moved the peer's ack to %d, above the %d the restarted replica reports holding", ack, reported)
	}
	if ack >= first {
		t.Fatalf("the sender moved the peer's ack to %d across a hole: the log's first retained sequence is %d", ack, first)
	}
	return ack, reported
}

func TestCatchUpRefusesWhenPeerIsBelowRetainedHorizon(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	scenario := stopOneReplicaBehind(t, tc)
	oc, ownerWal, lagPeer, downIndex, lagAck := scenario.oc, scenario.ownerWal, scenario.lagPeer, scenario.downIndex, scenario.lagAck

	// Simulate a broken retention policy that pruned the whole log. The peer
	// is now below the retained horizon.
	if err := ownerWal.Truncate(ownerWal.LastSequence()); err != nil {
		t.Fatalf("forced truncation: %v", err)
	}
	first, err := ownerWal.FirstSequence()
	if err != nil {
		t.Fatalf("first sequence: %v", err)
	}
	if first <= lagAck {
		t.Fatalf("test setup: retained horizon %d did not pass the peer position %d; the payloads did not force segment rotation", first, lagAck)
	}

	// While the replica is down nothing is read for it: the sender reads the
	// log only after the peer answers its probe (spec §19.3.5, S-5), so the
	// hole is found when it returns. The acknowledgement must not move.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if a := lagPeer.ackedThrough(oc.self.ID); a != lagAck {
		t.Fatalf("the sender advanced the peer's ack from %d to %d across a hole while it was down", lagAck, a)
	}

	// The restarted replica is refused, not skipped: the log no longer covers
	// it, and pretending otherwise is how it would silently stay stale.
	restarted := tc.startNode(downIndex, filepath.Dir(tc.nodes[downIndex].cfg.Storage.DataDir), tc.token)
	restarted.cfg = tc.nodes[downIndex].cfg
	tc.nodes[downIndex] = restarted

	if err := waitStalledOn(ctx, lagPeer, horizonRefusal); err != nil {
		t.Fatalf("the stream to the restarted peer past the horizon was not refused naming the horizon: %v", err)
	}
	_, reason := lagPeer.streamStatus()
	for _, want := range []string{"remove-node", "empty its data directory"} {
		if !strings.Contains(reason, want) {
			t.Errorf("the horizon refusal does not name the remedy %q: %s", want, reason)
		}
	}
	// migrate re-seeds nothing on a node that holds a copy, so the
	// refusal must not offer it as the remedy.
	if strings.Contains(reason, "with 'migrate") || strings.Contains(reason, "run 'migrate") {
		t.Errorf("the horizon refusal tells the operator to run migrate, which re-seeds nothing on a node that holds a copy: %s", reason)
	}
	requireAckWithinReplicaAndHorizon(t, scenario, first)
}

// TestACursorBehindTheReplicasOwnPositionIsRaisedNotRefusedAsAHole sets the
// owner's cursor below what the replica holds, as a cursor that missed an
// acknowledgement is. The restarted replica's handshake raises it to the
// replica's report; the refusal at the horizon stays, and the move is not
// treated as an acknowledgement across a hole.
func TestACursorBehindTheReplicasOwnPositionIsRaisedNotRefusedAsAHole(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	scenario := stopOneReplicaBehind(t, tc)
	ownerWal, lagPeer, downIndex, lagAck := scenario.ownerWal, scenario.lagPeer, scenario.downIndex, scenario.lagAck
	if lagAck == 0 {
		t.Fatal("test setup: the replica never confirmed anything, so there is no cursor to put behind it")
	}
	lagPeer.setCursor(lagAck - 1)
	scenario.lagAck = lagAck - 1

	if err := ownerWal.Truncate(ownerWal.LastSequence()); err != nil {
		t.Fatalf("forced truncation: %v", err)
	}
	first, err := ownerWal.FirstSequence()
	if err != nil {
		t.Fatalf("first sequence: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	restarted := tc.startNode(downIndex, filepath.Dir(tc.nodes[downIndex].cfg.Storage.DataDir), tc.token)
	restarted.cfg = tc.nodes[downIndex].cfg
	tc.nodes[downIndex] = restarted
	if err := waitStalledOn(ctx, lagPeer, horizonRefusal); err != nil {
		t.Fatalf("the stream to the restarted peer past the horizon was not refused naming the horizon: %v", err)
	}

	ack, reported := requireAckWithinReplicaAndHorizon(t, scenario, first)
	if ack != reported {
		t.Fatalf("the cursor is %d after the handshake, want the replica's reported position %d", ack, reported)
	}
}

// irrelevantPeer returns a peer of oc that hosts none of oc's buckets: at RF=1
// that is every other member.
func irrelevantPeer(t *testing.T, oc *Cluster, addrs []string) *peer {
	t.Helper()
	for _, addr := range addrs {
		if n := mustNode(t, oc, addr); n.ID != oc.self.ID {
			return oc.peerFor(n)
		}
	}
	t.Fatal("test setup: expected at least one other member")
	return nil
}

// TestAPeerThatHostsNoneOfThisNodesBucketsIsRefusedBelowTheHorizon: a peer
// that hosts none of this node's buckets is a member like any other. Database
// DDL is addressed to it, so a hole below the log's horizon is a hole in what
// it was sent, and its replay is refused with the remedy, never started at the
// horizon as if nothing were missing (spec §19.3.10). Retention does
// not make this state (it holds the log for every member's cursor); the test
// makes it by truncating by hand, as a broken retention policy would.
func TestAPeerThatHostsNoneOfThisNodesBucketsIsRefusedBelowTheHorizon(t *testing.T) {
	tc := newTestCluster(t, 3, 1) // RF=1: no other member replicates any bucket
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase, Name: testBucket, Type: BucketTypeHash,
	})

	// The bucket's own owner may not be node 0 (ring placement, not request
	// target); its WAL is the log that is truncated here.
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve owner: %v", err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	oc := tc.nodes[ownerIndex].server.core.cluster
	ownerWal := tc.nodes[ownerIndex].server.core.wal

	peer := irrelevantPeer(t, oc, tc.addrs)
	caughtUpWithin(t, peer, 20*time.Second)
	before := peer.ackedThrough(oc.self.ID)

	ownerWal.SetMaxSegmentBytes(256)
	for i := 0; i < 6; i++ {
		putDocument(tc, ownerIndex, testDatabase, testBucket, fmt.Sprintf("k%02d", i))
	}
	if err := ownerWal.Truncate(ownerWal.LastSequence()); err != nil {
		t.Fatalf("forced truncation: %v", err)
	}
	first, err := ownerWal.FirstSequence()
	if err != nil {
		t.Fatalf("first sequence: %v", err)
	}
	if first <= before+1 {
		t.Fatalf("test setup: retained horizon %d did not pass the peer's cursor %d", first, before)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	peer.stall("a test asked for a replay")
	if err := waitStalledOn(ctx, peer, horizonRefusal); err != nil {
		t.Fatalf("the stream to a peer that hosts none of this node's buckets, below the horizon, was not refused naming it: %v", err)
	}
}

// TestShipDoesNotRefuseOrDropIrrelevantPeerBehindHorizon pins the other half
// of the same fix: ship must still deliver a further cluster-wide broadcast
// (a second bucket's CREATE_BUCKET) to a peer that holds none of this node's
// buckets, even once the log has pruned past that peer's frozen position —
// and, critically, it must never call dropClient over this, since the
// connection ship refuses to tear down may be carrying unrelated in-flight
// client traffic (this was the actual mechanism behind bench/RESULTS.md's
// "three failing legs": a background replication guard closing a socket a
// client request was using).
func TestShipDoesNotRefuseOrDropIrrelevantPeerBehindHorizon(t *testing.T) {
	tc := newTestCluster(t, 3, 1) // RF=1: no other member replicates any bucket
	defer tc.stop()

	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{
		Database: testDatabase, Name: testBucket, Type: BucketTypeHash,
	})

	// The bucket's own owner may not be node 0 (ring placement, not request
	// target); its WAL is the log that actually needs pruning here, and its
	// Cluster is the one whose relevance/retention view matters.
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, testBucket))
	if err != nil {
		t.Fatalf("resolve owner: %v", err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	oc := tc.nodes[ownerIndex].server.core.cluster
	ownerWal := tc.nodes[ownerIndex].server.core.wal
	if ownerWal == nil {
		t.Fatal("owner has no WAL")
	}

	peer := irrelevantPeer(t, oc, tc.addrs)

	// Establish a real, non-zero acknowledgement deterministically, standing
	// in for the cluster-wide broadcasts (CREATE_DB/CREATE_BUCKET) this peer
	// would ordinarily have received and acked by now: those are
	// fire-and-forget (write_path.go's broadcastMutation: "Broadcast entries
	// are not acknowledged"), so waiting on them here raced their delivery.
	// OpPing carries no payload and needs no prior state on the peer, so it
	// exercises ship's horizon logic without depending on anything else.
	seq0, err := oc.appendWAL(ReplEntry{Op: int32(transport.OpPing)}, storage.WALOrigin{Kind: storage.WALOriginOwn})
	if err != nil {
		t.Fatalf("append WAL: %v", err)
	}
	if err := peer.ship([]ReplEntry{{Seq: seq0, Op: int32(transport.OpPing)}}); err != nil {
		t.Fatalf("test setup: could not ship the initial entry to the peer: %v", err)
	}
	before := peer.ackedThrough(oc.self.ID)
	if before < seq0 {
		t.Fatalf("test setup: peer acked %d after the initial ship, want at least %d", before, seq0)
	}
	if peer.client == nil {
		t.Fatal("test setup: peer must have a live connection after the initial ship")
	}

	// 20 MB payloads overflow a 64 MB segment (TestCatchUpRefusesWhenPeerIsBelowRetainedHorizon's
	// technique above): a small-payload write never rotates the active
	// segment, so a truncation has nothing to actually delete and
	// FirstSequence never moves.
	blob := strings.Repeat("x", 20<<20)
	for i := 0; i < 6; i++ {
		key := fmt.Sprintf("big%02d", i)
		if frame := tc.frame(ownerIndex, transport.OpPut, InsertRequest{
			Database: testDatabase, Bucket: testBucket, Key: key,
			Data: map[string]interface{}{"blob": blob},
		}); frame.Opcode == transport.OpError {
			t.Fatalf("put %s: %s", key, string(frame.Payload))
		}
	}
	// Force the truncation directly (TestCatchUpRefusesWhenPeerIsBelowRetainedHorizon's
	// technique) rather than going through pruneWAL/replicaAckBound: at RF=1
	// the natural bound is already the log tail, but relying on pruneWAL's
	// own sync-then-truncate sequencing here raced Badger's write path often
	// enough to be flaky. Truncating directly to the tail is deterministic
	// and is exactly the state a real retention pass would reach anyway.
	if err := ownerWal.Truncate(ownerWal.LastSequence()); err != nil {
		t.Fatalf("forced truncation: %v", err)
	}
	first, err := ownerWal.FirstSequence()
	if err != nil {
		t.Fatalf("first sequence: %v", err)
	}
	if first <= before {
		t.Fatalf("test setup: retained horizon %d did not pass the peer's frozen position %d", first, before)
	}

	// A further cluster-wide broadcast (OpPing standing in for e.g. a second
	// bucket's CREATE_BUCKET — the guard below does not look at the opcode)
	// must still reach this irrelevant peer.
	seq, err := oc.appendWAL(ReplEntry{Op: int32(transport.OpPing)}, storage.WALOrigin{Kind: storage.WALOriginOwn})
	if err != nil {
		t.Fatalf("append WAL: %v", err)
	}
	if err := peer.ship([]ReplEntry{{Seq: seq, Op: int32(transport.OpPing)}}); err != nil {
		t.Fatalf("ship refused a broadcast to an irrelevant peer merely because the log had pruned past its frozen position: %v", err)
	}
	if a := peer.ackedThrough(oc.self.ID); a != seq {
		t.Fatalf("ship did not advance the peer's acknowledgement to %d, got %d", seq, a)
	}
	if peer.client == nil {
		t.Fatal("ship dropped the peer's connection as collateral damage from the horizon check")
	}
}

// retentionHeldLines returns the WARN lines that say a member holds this
// node's WAL retention and name nodeID.
func retentionHeldLines(logged *syncBuffer, nodeID string) []string {
	var lines []string
	for _, line := range strings.Split(logged.String(), "\n") {
		if strings.Contains(line, "level=WARN") && strings.Contains(line, `msg="WAL retention on this node is held`) &&
			strings.Contains(line, nodeID) {
			lines = append(lines, line)
		}
	}
	return lines
}

// TestAMemberHoldingRetentionIsNamedOncePerInterval: While a
// member is down, every other node's log grows; each says so in one WARN per
// interval that names the member and the fix — whether the member's stream
// has stalled at a cursor, or (after a restart of the node that warns) the
// member has not answered a handshake at all, which holds the bound at 0.
// Nothing is said while every member is in sync.
func TestAMemberHoldingRetentionIsNamedOncePerInterval(t *testing.T) {
	logs := make([]*syncBuffer, 3)
	for i := range logs {
		logs[i] = &syncBuffer{}
	}
	const interval = 500 * time.Millisecond
	tc := newTestClusterTuned(t, 3, 3, func(_ int, cfg *config.Config) {
		// The passes are the test's: no ticker runs one.
		cfg.Server.WALRetentionSeconds = 3600
	}, func(index int, c *Cluster) {
		c.logger = slog.New(slog.NewTextHandler(logs[index], &slog.HandlerOptions{Level: slog.LevelInfo}))
		c.retentionHeldWarnEvery = interval
	})
	defer tc.stop()
	ownerIndex, downIndex := ownerAndReplica(t, tc)
	downID := tc.addrs[downIndex]
	if answer, err := writeConfirmed(tc.nodes[ownerIndex].client, "before", "all"); err != nil || answer.confirmed != 3 {
		t.Fatalf("test setup: the write before the stop was answered %+v (%v), want copies_confirmed 3", answer, err)
	}
	owner := tc.nodes[ownerIndex].server.core
	if err := owner.cluster.pruneWAL(); err != nil {
		t.Fatalf("a retention pass with every member in sync: %v", err)
	}
	if held := retentionHeldLines(logs[ownerIndex], downID); len(held) != 0 {
		t.Fatalf("every member in sync, yet the owner says retention is held:\n%s", strings.Join(held, "\n"))
	}

	tc.stopNodeKeepingData(downIndex)
	for i := 0; i < 10; i++ {
		if _, err := writeConfirmed(tc.nodes[ownerIndex].client, fmt.Sprintf("down-%d", i), "one"); err != nil {
			t.Fatal(err)
		}
	}
	toDown := owner.cluster.peerFor(mustNode(t, owner.cluster, downID))
	if !eventually(func() bool { return !toDown.inSync() }) {
		t.Fatal("test setup: the stream to the stopped member never stalled")
	}
	for pass := 0; pass < 3; pass++ {
		if err := owner.cluster.pruneWAL(); err != nil {
			t.Fatalf("retention pass %d: %v", pass, err)
		}
	}
	held := retentionHeldLines(logs[ownerIndex], downID)
	if len(held) != 1 {
		t.Fatalf("three passes inside one interval with a member down: %d WARN lines name it, want 1:\n%s", len(held), strings.Join(held, "\n"))
	}
	for _, want := range []string{"remove-node", "held_at_seq=", "Restart"} {
		if !strings.Contains(held[0], want) {
			t.Errorf("the WARN does not carry %q: %s", want, held[0])
		}
	}
	time.Sleep(interval + 100*time.Millisecond)
	if err := owner.cluster.pruneWAL(); err != nil {
		t.Fatalf("the retention pass after the interval: %v", err)
	}
	if held := retentionHeldLines(logs[ownerIndex], downID); len(held) != 2 {
		t.Fatalf("a pass after the interval: %d WARN lines name the member, want 2:\n%s", len(held), strings.Join(held, "\n"))
	}

	// The owner restarts with the member still down: the member's cursor is
	// unknown to the new process, the bound is 0, and the WARN says what the
	// node is waiting for.
	before := len(retentionHeldLines(logs[ownerIndex], downID))
	restarted := tc.restartNode(ownerIndex)
	if err := restarted.server.core.cluster.pruneWAL(); err != nil {
		t.Fatalf("a retention pass after the owner restarted: %v", err)
	}
	held = retentionHeldLines(logs[ownerIndex], downID)
	if len(held) != before+1 {
		t.Fatalf("a pass after the owner restarted with the member down: %d new WARN lines name it, want 1:\n%s",
			len(held)-before, strings.Join(held[before:], "\n"))
	}
	if !strings.Contains(held[before], "handshake") {
		t.Errorf("the WARN at bound 0 does not say the node is waiting for the member's handshake: %s", held[before])
	}
}

// TestRetentionHeldWarningForARefusedReplicaDoesNotAdviseARestart is
// BUG-0605: since BUG-0313 a replica the owner refuses for its plaintext store
// holds the log exactly as a member that is away does, but it is running and a
// restart leaves it plaintext, so the WARN must give the refusal's fix instead
// of telling the operator to restart it.
func TestRetentionHeldWarningForARefusedReplicaDoesNotAdviseARestart(t *testing.T) {
	logged := &syncBuffer{}
	previous := clusterTestLogger.Swap(slog.New(slog.NewTextHandler(logged, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { clusterTestLogger.Store(previous) })

	r := newRefusedMidWrite(t)
	defer r.tc.stop()
	replicaID := r.tc.addrs[1]

	// The stream is momentarily connecting while the sender retries it, and a
	// pass that finds it so reports nothing; a later pass is the one that sees
	// the stall. Nothing is warned before the refusal, so the first line that
	// names the replica is the one under test.
	deadline := time.Now().Add(20 * time.Second)
	var held []string
	for time.Now().Before(deadline) {
		r.owner.warnIfAMemberHoldsRetention(time.Now())
		if held = retentionHeldLines(logged, replicaID); len(held) != 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(held) == 0 {
		t.Fatalf("the owner refused its replica %s an entry and holds the log, yet no retention warning names it", replicaID)
	}
	line := held[0]
	if strings.Contains(line, "Restart") {
		t.Errorf("the WARN tells the operator to restart a replica that is running and refused for its plaintext store, which changes nothing: %s", line)
	}
	for _, want := range []string{"not encrypted", "encryption.mode=on", "'remove-node " + replicaID + "'"} {
		if !strings.Contains(line, want) {
			t.Errorf("the WARN for a refused replica does not carry %q: %s", want, line)
		}
	}
}
