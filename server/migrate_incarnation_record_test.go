package server

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// restartingSourceConn stands in for a source that restarts after it has
// answered MIGRATE_START but before the pull's chunks are read. It remembers
// the snapshot's sequence from the start answer, and on the pull's first
// MIGRATE_DATA request it hands that sequence to onFirstChunk, which rebases
// the puller's record for the source as the restarted source's stream would.
type restartingSourceConn struct {
	peerConn
	onFirstChunk func(snapshotSeq uint64)
	snapshotSeq  atomic.Uint64
	once         sync.Once
}

func (c *restartingSourceConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	if op == transport.OpMigrateData {
		c.once.Do(func() { c.onFirstChunk(c.snapshotSeq.Load()) })
	}
	frame, err := c.peerConn.SendRequest(ctx, lane, op, payload)
	if err == nil && op == transport.OpMigrateStart {
		var start MigrateStartResponse
		if bson.Unmarshal(frame.Payload, &start) == nil {
			c.snapshotSeq.Store(start.HighWater)
		}
	}
	return frame, err
}

// pullAcrossASourceRestart pulls a FIFO bucket onto a node that is not its
// owner, and has the owner's incarnation move after MIGRATE_START with a
// rebase that keeps its log valid through validThrough(snapshotSeq). It
// returns the puller, the owner's id and the snapshot's sequence.
func pullAcrossASourceRestart(t *testing.T, validThrough func(snapshotSeq uint64) uint64) (target *Cluster, ownerID string, snapshotSeq uint64) {
	t.Helper()
	tc := newTestClusterTuned(t, 3, 1, autoRebalanceOff, nil)
	t.Cleanup(tc.stop)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeFIFO})
	ownerIndex := ownerIndexOf(t, tc, testBucket)
	targetIndex := -1
	for i := range tc.addrs {
		if i != ownerIndex {
			targetIndex = i
			break
		}
	}
	if targetIndex < 0 {
		t.Fatal("no non-owner node to act as the migration target")
	}
	for i := 0; i < 5; i++ {
		tc.ok(ownerIndex, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: testBucket,
			Data: map[string]interface{}{"e": fmt.Sprintf("e%d", i)}, WriteConcern: "all"})
	}

	target = clusterOf(tc, targetIndex)
	ownerID = tc.addrs[ownerIndex]
	owner := mustNode(t, target, ownerID)
	// The pull is served only to a peer whose stream opened in this process,
	// so the target must hold the owner's incarnation before it starts.
	openedStream(t, target, ownerID)

	// Freeze the owner's stream to the target: the owner's next handshake
	// would otherwise rebase the record again before the test reads it.
	never := make(chan struct{})
	ownerCluster := clusterOf(tc, ownerIndex)
	wrapPeerConn(ownerCluster.peerFor(mustNode(t, ownerCluster, tc.addrs[targetIndex])), func(inner peerConn) peerConn {
		return &heldStreamConn{peerConn: inner, openRelease: never, entryRelease: never}
	})

	fresh := bytes.Repeat([]byte{0x5a}, 16)
	rebased := atomic.Bool{}
	wrapPeerConn(target.peerFor(owner), func(inner peerConn) peerConn {
		return &restartingSourceConn{peerConn: inner, onFirstChunk: func(seq uint64) {
			snapshotSeq = seq
			stale := target.recordedPosition(ownerID).Incarnation
			if _, err := target.rebaseStream(ownerID, fresh,
				ReplRebase{From: stale, ValidThrough: validThrough(seq), Log: fresh}); err != nil {
				t.Errorf("moving the target's record for %s onto a new incarnation mid-pull: %v", ownerID, err)
				return
			}
			rebased.Store(true)
		}}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := target.pullBucket(ctx, testDatabase, testBucket, owner); err != nil {
		t.Fatalf("the pull failed rather than landing the copy: %v", err)
	}
	if !rebased.Load() {
		t.Fatal("setup: the source's incarnation was never moved during the pull")
	}
	if snapshotSeq < 2 {
		t.Fatalf("setup: the snapshot was taken at sequence %d; the test needs room below it", snapshotSeq)
	}
	return target, ownerID, snapshotSeq
}

func fifoEntry(t *testing.T, seq uint64) ReplEntry {
	t.Helper()
	return ReplEntry{Seq: seq, Op: int32(transport.OpFIFOPush), Bucket: testBucket, Payload: putPayload(t, testDatabase, testBucket, "k")}
}

// TestAPullAcrossASourceRestartIsCappedAtWhatTheNewLogKeeps: a snapshot
// position is a sequence of the source's log. When the source restarts
// mid-pull, the handshake's rebase has already run before the position
// exists, and it is the rebase's ValidThrough that says how much of the old
// log the new one still holds: the recorded position must be capped at it, as
// the rebase caps a position that already exists. Recorded uncapped, the
// position names sequences the new log reuses for other entries, and those are
// passed over as held by the copy and lost. Dropped outright, the source
// re-ships what lies between the applied position and the snapshot, and a
// FIFO applies it twice (BUG-0311).
//
// Guard: record the snapshot position as read at MIGRATE_START, ignoring the
// rebases that ran during the pull.
func TestAPullAcrossASourceRestartIsCappedAtWhatTheNewLogKeeps(t *testing.T) {
	tests := []struct {
		name         string
		validThrough func(snapshotSeq uint64) uint64
		want         func(snapshotSeq uint64) uint64
	}{
		{"valid through below the snapshot",
			func(seq uint64) uint64 { return seq - 1 }, func(seq uint64) uint64 { return seq - 1 }},
		{"valid through at the snapshot",
			func(seq uint64) uint64 { return seq }, func(seq uint64) uint64 { return seq }},
		{"valid through above the snapshot",
			func(seq uint64) uint64 { return seq + 1000 }, func(seq uint64) uint64 { return seq }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target, ownerID, snapshotSeq := pullAcrossASourceRestart(t, tt.validThrough)
			want := tt.want(snapshotSeq)
			if got := target.seededThroughFor(ownerID, testDatabase, testBucket); got != want {
				t.Fatalf("the snapshot position after a restart mid-pull is %d, want %d (snapshot at %d, log valid through %d)",
					got, want, snapshotSeq, tt.validThrough(snapshotSeq))
			}
			// The entry at the position is held by the copy and must not be
			// applied again; the one after it is the new log's own and must be.
			if !target.seededCovers(ownerID, fifoEntry(t, want)) {
				t.Errorf("entry %d of %s is not covered by the snapshot position: a FIFO would apply it twice", want, ownerID)
			}
			if target.seededCovers(ownerID, fifoEntry(t, want+1)) {
				t.Errorf("entry %d of %s is covered by the snapshot position: the new log's entry would be lost", want+1, ownerID)
			}
		})
	}
}

// fifoCount is the number of elements database/bucket holds on target.
func fifoCount(t *testing.T, target *Cluster) int {
	t.Helper()
	counted, err := target.s.Count(CountRequest{Database: testDatabase, Bucket: testBucket})
	if err != nil {
		t.Fatalf("counting %s/%s on %s: %v", testDatabase, testBucket, target.self.ID, err)
	}
	return int(counted.Count)
}

// pulledThenRebased lands a pull whose source does not restart, then moves the
// target's record for the source onto a new incarnation whose log is valid
// through validThrough(snapshotSeq). The rebase runs after the snapshot
// position exists, so it has a position to lower: the route a restart of the
// source takes in production, since a pull in flight does not survive one
// (Cluster.setPending).
func pulledThenRebased(t *testing.T, validThrough func(snapshotSeq uint64) uint64) (target *Cluster, ownerID string, snapshotSeq uint64) {
	t.Helper()
	target, ownerID, snapshotSeq = pullAcrossASourceRestart(t, func(seq uint64) uint64 { return seq + 1000 })
	if got := target.seededThroughFor(ownerID, testDatabase, testBucket); got != snapshotSeq {
		t.Fatalf("setup: the snapshot position is %d, want the snapshot's %d", got, snapshotSeq)
	}
	valid := validThrough(snapshotSeq)
	if applied := target.recordedPosition(ownerID).AppliedThrough; applied > valid {
		t.Fatalf("setup: the stream position %d is above the log's valid prefix %d, so the rebase would clamp by the stream position", applied, valid)
	}
	incarnation := bytes.Repeat([]byte{0x6b}, 16)
	stale := target.recordedPosition(ownerID).Incarnation
	if _, err := target.rebaseStream(ownerID, incarnation, ReplRebase{From: stale, ValidThrough: valid, Log: incarnation}); err != nil {
		t.Fatalf("rebasing the target's record for %s after the pull landed: %v", ownerID, err)
	}
	return target, ownerID, snapshotSeq
}

// TestANewLogEntryAtThePulledSequenceIsAppliedAfterARebaseThatLowersTheSnapshot:
// a replica that holds a source's bucket only by a pulled snapshot has the
// snapshot's sequence as the bucket's lineage position. When the source's log
// goes backwards below it and the new log reuses that sequence, the entry is
// the new log's own and must be applied; judged by the copy's position it is
// passed over as held, acknowledged and lost on this copy (BUG-0621).
//
// Guard: set the clamp from the stream position alone (the rebase's `applied
// < appliedFrom`), ignoring the snapshot positions it lowered.
func TestANewLogEntryAtThePulledSequenceIsAppliedAfterARebaseThatLowersTheSnapshot(t *testing.T) {
	target, ownerID, snapshotSeq := pulledThenRebased(t, func(seq uint64) uint64 { return seq - 1 })
	if !target.sourceClamped(ownerID) {
		t.Errorf("the rebase lowered the snapshot position of %s and left the source unclamped", ownerID)
	}
	before := fifoCount(t, target)
	resp := pushToReplica(t, target, ownerID, []ReplEntry{fifoEntry(t, snapshotSeq)})
	if !resp.Success {
		t.Fatalf("the push of the new log's entry %d was refused: %s", snapshotSeq, resp.Error)
	}
	if after := fifoCount(t, target); after != before+1 {
		t.Fatalf("new-log entry at sequence %d acknowledged but not applied: %d -> %d elements", snapshotSeq, before, after)
	}
}

// TestAnEntryTheSnapshotCoversIsStillSkippedAfterTheClamp: clamping a source
// turns the copy's position check off for all of its entries, so what keeps an
// entry the snapshot really holds from applying twice (BUG-0017) is the
// snapshot position, checked before the store is reached. The lowered position
// still covers the log's valid prefix.
func TestAnEntryTheSnapshotCoversIsStillSkippedAfterTheClamp(t *testing.T) {
	target, ownerID, snapshotSeq := pulledThenRebased(t, func(seq uint64) uint64 { return seq - 1 })
	if !target.sourceClamped(ownerID) {
		t.Fatalf("setup: the source is not clamped")
	}
	covered := snapshotSeq - 1
	before := fifoCount(t, target)
	resp := pushToReplica(t, target, ownerID, []ReplEntry{fifoEntry(t, covered)})
	if !resp.Success {
		t.Fatalf("the push of the covered entry %d was refused: %s", covered, resp.Error)
	}
	if after := fifoCount(t, target); after != before {
		t.Fatalf("entry %d is at or below the snapshot position and was applied again: %d -> %d elements", covered, before, after)
	}
}

// TestARebaseClampsASourceOnlyWhenItLowersAPosition: a log valid through the
// snapshot, or past it, lowers nothing and leaves the source judged by the
// copy's position, and a clamp set by a rebase survives a restart of the
// replica (positionRecord.Clamped).
func TestARebaseClampsASourceOnlyWhenItLowersAPosition(t *testing.T) {
	tests := []struct {
		name         string
		validThrough func(snapshotSeq uint64) uint64
		wantClamped  bool
	}{
		{"valid through below the snapshot", func(seq uint64) uint64 { return seq - 1 }, true},
		{"valid through at the snapshot", func(seq uint64) uint64 { return seq }, false},
		{"valid through above the snapshot", func(seq uint64) uint64 { return seq + 1000 }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target, ownerID, _ := pulledThenRebased(t, tt.validThrough)
			if got := target.sourceClamped(ownerID); got != tt.wantClamped {
				t.Fatalf("source clamped after the rebase = %v, want %v", got, tt.wantClamped)
			}
			target.appliedMu.Lock()
			delete(target.recordedStream, ownerID)
			target.appliedMu.Unlock()
			if err := target.loadStoredPositions(); err != nil {
				t.Fatalf("reloading the stored positions: %v", err)
			}
			if got := target.sourceClamped(ownerID); got != tt.wantClamped {
				t.Fatalf("source clamped after a restart = %v, want %v", got, tt.wantClamped)
			}
		})
	}
}

// TestANewLogEntryAtTheSnapshotSequenceIsAppliedWhenTheSourceRestartedMidPull:
// the in-flight form of BUG-0621. The source's log goes backwards between
// MIGRATE_START and the last chunk, so the rebase lowers nothing (the position
// does not exist yet) and the position is recorded capped. The copy is still
// the snapshot, whose lineage position is the sequence the new log reuses, so
// the source must be clamped when the capped position is recorded.
//
// Guard: record the capped position without setting the source's clamp.
func TestANewLogEntryAtTheSnapshotSequenceIsAppliedWhenTheSourceRestartedMidPull(t *testing.T) {
	target, ownerID, snapshotSeq := pullAcrossASourceRestart(t, func(seq uint64) uint64 { return seq - 1 })
	if !target.sourceClamped(ownerID) {
		t.Errorf("a position recorded below the snapshot's sequence left %s unclamped", ownerID)
	}
	before := fifoCount(t, target)
	resp := pushToReplica(t, target, ownerID, []ReplEntry{fifoEntry(t, snapshotSeq)})
	if !resp.Success {
		t.Fatalf("the push of the new log's entry %d was refused: %s", snapshotSeq, resp.Error)
	}
	if after := fifoCount(t, target); after != before+1 {
		t.Fatalf("new-log entry at sequence %d acknowledged but not applied: %d -> %d elements", snapshotSeq, before, after)
	}
}

// TestAPositionRecordedBelowTheSnapshotClampsAndSurvivesARestart: a position
// recorded lower than the snapshot was taken at clamps its source; one with a
// log valid at or past the snapshot does not. The clamp is persisted with the
// position and read back by loadStoredPositions.
func TestAPositionRecordedBelowTheSnapshotClampsAndSurvivesARestart(t *testing.T) {
	tests := []struct {
		name         string
		validThrough func(snapshotSeq uint64) uint64
		wantClamped  bool
	}{
		{"valid through below the snapshot", func(seq uint64) uint64 { return seq - 1 }, true},
		{"valid through at the snapshot", func(seq uint64) uint64 { return seq }, false},
		{"valid through above the snapshot", func(seq uint64) uint64 { return seq + 1000 }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target, ownerID, _ := pullAcrossASourceRestart(t, tt.validThrough)
			if got := target.sourceClamped(ownerID); got != tt.wantClamped {
				t.Fatalf("source clamped after the pull = %v, want %v", got, tt.wantClamped)
			}
			target.appliedMu.Lock()
			delete(target.recordedStream, ownerID)
			target.appliedMu.Unlock()
			if err := target.loadStoredPositions(); err != nil {
				t.Fatalf("reloading the stored positions: %v", err)
			}
			if got := target.sourceClamped(ownerID); got != tt.wantClamped {
				t.Fatalf("source clamped after a restart = %v, want %v", got, tt.wantClamped)
			}
		})
	}
}

// TestAPullWithNoRebaseDuringItDoesNotClamp: the ordinary pull, with the
// source's incarnation unchanged, records the snapshot's own sequence and
// leaves the source judged by the copy's position.
func TestAPullWithNoRebaseDuringItDoesNotClamp(t *testing.T) {
	tc := newTestClusterTuned(t, 3, 1, autoRebalanceOff, nil)
	t.Cleanup(tc.stop)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeFIFO})
	ownerIndex := ownerIndexOf(t, tc, testBucket)
	targetIndex := (ownerIndex + 1) % len(tc.addrs)
	tc.ok(ownerIndex, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: testBucket,
		Data: map[string]interface{}{"e": "e0"}, WriteConcern: "all"})
	target := clusterOf(tc, targetIndex)
	ownerID := tc.addrs[ownerIndex]
	openedStream(t, target, ownerID)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := target.pullBucket(ctx, testDatabase, testBucket, mustNode(t, target, ownerID)); err != nil {
		t.Fatalf("the pull failed: %v", err)
	}
	if got := target.seededThroughFor(ownerID, testDatabase, testBucket); got == 0 {
		t.Fatal("setup: no snapshot position was recorded")
	}
	if target.sourceClamped(ownerID) {
		t.Fatalf("a pull no rebase crossed clamped %s", ownerID)
	}
}
