package server

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// replicaSequencesOf is the local sequence of every entry node index's log
// holds for a put of key that it took as a replica, oldest first.
func (tc *testCluster) replicaSequencesOf(index int, key string) []uint64 {
	tc.t.Helper()
	entries, err := tc.nodes[index].server.core.wal.ReadFrom(1)
	if err != nil {
		tc.t.Fatalf("reading node %d's log: %v", index+1, err)
	}
	var sequences []uint64
	for _, entry := range entries {
		if entry.Origin.Kind != storage.WALOriginReplica || entry.Bucket != testBucket || transport.Opcode(entry.Op) != transport.OpPut {
			continue
		}
		var put InsertRequest
		if err := bson.Unmarshal(entry.Data, &put); err != nil {
			tc.t.Fatalf("decoding the replicated put at %d in node %d's log: %v", entry.Seq, index+1, err)
		}
		if put.Key == key {
			sequences = append(sequences, entry.Seq)
		}
	}
	return sequences
}

// TestALostLogWithItsDataKeptDoesNotSkipAReplicatedEntry is BUG-0609: a
// replica's applied_seq for a bucket it does not own is a position in its own
// log that no peer holds, so the start hold's fast-forward does not cover it.
// The replica's log entries are lost and its data kept; on the starting tree
// its log gave the next replicated entry a local sequence at or below the
// bucket's stored mark, which the apply gate reads as already applied. Every
// copy of the entry the replica logs must be above the mark, and the replica
// must hold it.
//
// Three ways a log comes back with no entry: the whole wal/ directory lost;
// its segment files lost and its incarnation file kept; and wal/ lost, then
// opened and closed before the start that would have scanned (a crash between
// OpenWAL, which writes the incarnation file first, and the fast-forward).
// The first was the only one the first fix covered (Opus verifier, 2026-10-18).
func TestALostLogWithItsDataKeptDoesNotSkipAReplicatedEntry(t *testing.T) {
	cases := []struct {
		name    string
		loseLog func(t *testing.T, walDir string)
	}{
		{"the whole wal directory", func(t *testing.T, walDir string) {
			if err := os.RemoveAll(walDir); err != nil {
				t.Fatalf("removing the replica's log: %v", err)
			}
		}},
		{"the segments with the incarnation file kept", func(t *testing.T, walDir string) {
			segments, err := filepath.Glob(filepath.Join(walDir, "wal_*.log"))
			if err != nil || len(segments) == 0 {
				t.Fatalf("finding the replica's log segments: %v (found %d)", err, len(segments))
			}
			for _, segment := range segments {
				if err := os.Remove(segment); err != nil {
					t.Fatalf("removing %s: %v", segment, err)
				}
			}
		}},
		{"the wal directory, reopened before the scan", func(t *testing.T, walDir string) {
			if err := os.RemoveAll(walDir); err != nil {
				t.Fatalf("removing the replica's log: %v", err)
			}
			reopened, err := storage.OpenWAL(walDir, nil)
			if err != nil {
				t.Fatalf("opening a new log in %s: %v", walDir, err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatalf("closing the new log: %v", err)
			}
		}},
	}
	for _, loss := range cases {
		t.Run(loss.name, func(t *testing.T) { lostLogDoesNotSkipAReplicatedEntry(t, loss.loseLog) })
	}
}

func lostLogDoesNotSkipAReplicatedEntry(t *testing.T, loseLog func(t *testing.T, walDir string)) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	owner := ownerIndexOf(t, tc, testBucket)
	// The replica is neither the owner nor node 1, which took the DDL: it
	// owns no entry of its own, so its peers hold next to nothing of its log.
	replica := 1
	if owner == 1 {
		replica = 2
	}
	for i := 0; i < 30; i++ {
		tc.mustPutValue(owner, fmt.Sprintf("before%02d", i), "v", "all")
	}
	state, err := tc.nodes[replica].server.core.bucketStateNamed(testDatabase, testBucket)
	if err != nil {
		t.Fatalf("reading the replica's state of %s: %v", testBucket, err)
	}
	mark := state.AppliedSeq
	if mark < 30 {
		t.Fatalf("the replica's applied_seq for %s is %d after 30 replicated puts; the test needs it at 30 or above", testBucket, mark)
	}

	walDir := filepath.Join(tc.nodes[replica].cfg.Storage.DataDir, "wal")
	base := tc.stopNodeKeepingData(replica)
	loseLog(t, walDir)
	tc.nodes[replica] = tc.startNode(replica, base, tc.token)
	if scans := tc.nodes[replica].server.core.newLogMarkScans.Load(); scans != 1 {
		t.Errorf("the replica found its log new and walked its buckets' marks %d times; want once", scans)
	}

	tc.mustPutValue(owner, "after", "v", "all")
	deadline := time.Now().Add(20 * time.Second)
	for {
		if _, _, found := tc.heldValue(replica, "after"); found {
			break
		}
		if time.Now().After(deadline) {
			t.Errorf("node %d does not hold \"after\", which the owner acknowledged at all", replica+1)
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	sequences := tc.replicaSequencesOf(replica, "after")
	if len(sequences) == 0 {
		t.Fatalf("node %d's log holds no replicated put of \"after\"", replica+1)
	}
	for _, seq := range sequences {
		if seq <= mark {
			t.Errorf("node %d logged the replicated put of \"after\" at local sequence %d, at or below the applied_seq %d its kept data "+
				"holds for %s: the apply gate reads it as already applied", replica+1, seq, mark, testBucket)
		}
	}
}

// TestAHealthyRestartDoesNotScanBucketMarks guards BUG-0609's cost: only an
// open that finds no log walks the buckets' marks. A node restarted with its
// log intact walks nothing.
func TestAHealthyRestartDoesNotScanBucketMarks(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	owner := ownerIndexOf(t, tc, testBucket)
	tc.mustPutValue(owner, "before", "v", "all")
	for index := range tc.nodes {
		tc.restartNode(index)
		if scans := tc.nodes[index].server.core.newLogMarkScans.Load(); scans != 0 {
			t.Errorf("node %d restarted with its log intact and walked its buckets' marks %d times; want none", index+1, scans)
		}
	}
	tc.mustPutValue(owner, "after", "v", "all")
}

// ownDropSequencesOf is the sequence of every drop of database that node
// index's log holds as its own, oldest first.
func (tc *testCluster) ownDropSequencesOf(index int, database string) []uint64 {
	tc.t.Helper()
	var sequences []uint64
	for _, entry := range tc.dropEntriesOf(index, database, storage.WALOriginOwn) {
		sequences = append(sequences, entry.Seq)
	}
	return sequences
}

// dropEntriesOf is every drop of database that node index's log holds with
// origin kind, oldest first.
func (tc *testCluster) dropEntriesOf(index int, database string, kind storage.WALOriginKind) []storage.WALEntry {
	tc.t.Helper()
	entries, err := tc.nodes[index].server.core.wal.ReadFrom(1)
	if err != nil {
		tc.t.Fatalf("reading node %d's log: %v", index+1, err)
	}
	var drops []storage.WALEntry
	for _, entry := range entries {
		if entry.Origin.Kind != kind || transport.Opcode(entry.Op) != transport.OpDeleteDB {
			continue
		}
		var drop DeleteDatabaseRequest
		if err := bson.Unmarshal(entry.Data, &drop); err != nil {
			tc.t.Fatalf("decoding the drop at %d in node %d's log: %v", entry.Seq, index+1, err)
		}
		if drop.Name == database {
			drops = append(drops, entry)
		}
	}
	return drops
}

// replicaDropSourcesOf is every node whose drop of database node index's log
// holds as a replica. A re-sent push is logged twice, so the sources are
// counted and not the entries.
func (tc *testCluster) replicaDropSourcesOf(index int, database string) map[string]bool {
	tc.t.Helper()
	sources := make(map[string]bool)
	for _, entry := range tc.dropEntriesOf(index, database, storage.WALOriginReplica) {
		sources[entry.Origin.Source] = true
	}
	return sources
}

// TestReplayRelogDropWaitsForTheStartHold is BUG-0610: a node whose replay
// learns a dropped generation logs a drop of it as its own (G-12). Start
// replays before it arms the start hold, so on a node whose log went backwards
// that drop took the sequence after the log's end, which its peers already
// hold another entry at. The node's log holds a peer's drop of a generation
// its store does not hold dropped (the tombstone is removed by hand, as a
// store that lost the commit would leave it); its data directory is copied,
// it writes 20 entries of its own at all, and the copy is put back. The drop
// it logs again after that start must be above every position its peers hold.
func TestReplayRelogDropWaitsForTheStartHold(t *testing.T) {
	const gone, generation = "gone", int64(4242)
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	owner := ownerIndexOf(t, tc, testBucket)
	tc.mustPutValue(owner, "before", "v", "all")

	const source = "test-source"
	node := tc.nodes[owner]
	pushToReceiver(t, node, source, []ReplEntry{{Seq: sourcePosition(node, source) + 1, Op: int32(transport.OpDeleteDB),
		Payload: requestBody(t, DeleteDatabaseRequest{Name: gone, Generations: []int64{generation}})}})
	// The owner logs the drop again and ships it; each peer, learning it,
	// logs it as its own and ships it back. Those echoes must have landed
	// before the tombstone goes, or the owner learns the drop again live and
	// its re-log lands in the copy.
	pollUntil(t, "both peers' re-logs of the drop reached the owner", func() bool {
		return len(tc.replicaDropSourcesOf(owner, gone)) == 3
	})
	batch := node.server.core.kv.NewBatch()
	if err := batch.Delete(tombstoneKey(gone, generation)); err != nil {
		t.Fatalf("removing the tombstone: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing the tombstone's removal: %v", err)
	}
	batch.Discard()
	snapshotEnd := node.server.core.wal.LastSequence()

	dataDir := node.cfg.Storage.DataDir
	snapshot := filepath.Join(t.TempDir(), "snapshot")
	base := tc.stopNodeKeepingData(owner)
	copyTree(t, dataDir, snapshot)
	tc.nodes[owner] = tc.startNode(owner, base, tc.token)
	for i := 0; i < 20; i++ {
		tc.mustPutValue(owner, fmt.Sprintf("lost%02d", i), "v", "all")
	}
	highWater := tc.highestPositionFor(owner)
	if highWater <= snapshotEnd {
		t.Fatalf("setup: the peers hold the owner's log through %d, not above the snapshot's end %d", highWater, snapshotEnd)
	}

	tc.stopNodeKeepingData(owner)
	if err := os.RemoveAll(dataDir); err != nil {
		t.Fatalf("removing the owner's data directory: %v", err)
	}
	copyTree(t, snapshot, dataDir)
	tc.nodes[owner] = tc.startNode(owner, base, tc.token)

	var relogged []uint64
	deadline := time.Now().Add(20 * time.Second)
	for {
		relogged = relogged[:0]
		for _, seq := range tc.ownDropSequencesOf(owner, gone) {
			if seq > snapshotEnd {
				relogged = append(relogged, seq)
			}
		}
		if len(relogged) > 0 {
			break
		}
		if time.Now().After(deadline) {
			core := tc.nodes[owner].server.core
			dropped, _ := core.generationDropped(gone, generation)
			t.Fatalf("the restored owner did not log again the drop of generation %d of %q its replay learned "+
				"(start hold open %v, log at %d, own drops %v, drop known %v)", generation, gone,
				core.cluster.startHold.open.Load(), core.wal.LastSequence(), tc.ownDropSequencesOf(owner, gone), dropped)
		}
		time.Sleep(100 * time.Millisecond)
	}
	for _, seq := range relogged {
		if seq <= highWater {
			t.Errorf("the restored owner logged the drop its replay learned at sequence %d; its peers hold its log through %d, "+
				"so that sequence is one they hold another entry at", seq, highWater)
		}
	}
}

// TestADropReplayLearnedReachesThePeersWhenItsRelogIsLost is the loss the
// owner's ruling on BUG-0610 accepts, and why it is safe: a drop start-up
// replay learned is logged again only once the start hold opens, so a node
// stopped before that never logs it, and its next replay finds it known. The
// peers must still learn it, from this node's answer to an entry of the
// database it passes over (G-11 (a)).
//
// The node learns a drop no peer has (G-12 off for that push, so it is not
// logged again live), its tombstone is removed by hand as a store that lost
// the commit would leave it, and it restarts with both peers down: replay
// learns the drop, the hold cannot open, and the node is stopped. Restarted
// with its peers up, it logs nothing. A bucket created in the dropped
// generation through a peer then reaches it, and both peers must learn the
// drop.
func TestADropReplayLearnedReachesThePeersWhenItsRelogIsLost(t *testing.T) {
	const gone, source, learner = "gone", "test-source", 0
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(learner, transport.OpCreateDB, CreateDatabaseRequest{Name: gone})
	waitListedBy(t, tc, gone, 0, 1, 2)
	live, err := tc.nodes[learner].server.core.liveGenerations(gone)
	if err != nil || len(live) != 1 {
		t.Fatalf("setup: live generations of %q are %v (%v)", gone, live, err)
	}
	generation := live[0]

	generationSeamsHook.Store(&generationSeams{noRelog: true})
	node := tc.nodes[learner]
	pushToReceiver(t, node, source, []ReplEntry{{Seq: sourcePosition(node, source) + 1, Op: int32(transport.OpDeleteDB),
		Payload: requestBody(t, DeleteDatabaseRequest{Name: gone, Generations: []int64{generation}})}})
	generationSeamsHook.Store(nil)
	batch := node.server.core.kv.NewBatch()
	if err := batch.Delete(tombstoneKey(gone, generation)); err != nil {
		t.Fatalf("removing the tombstone: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing the tombstone's removal: %v", err)
	}
	batch.Discard()

	restartFirst := stopNode(t, tc, 1)
	restartSecond := stopNode(t, tc, 2)
	base := tc.stopNodeKeepingData(learner)
	tc.nodes[learner] = tc.startNode(learner, base, tc.token)
	if dropped, err := tc.nodes[learner].server.core.generationDropped(gone, generation); err != nil || !dropped {
		t.Fatalf("setup: the restarted node's replay did not apply the drop of generation %d (%v)", generation, err)
	}
	tc.stopNodeKeepingData(learner)

	restartFirst()
	restartSecond()
	tc.nodes[learner] = tc.startNode(learner, base, tc.token)
	pollUntil(t, "the restarted node's start hold opened", func() bool {
		return tc.nodes[learner].server.core.cluster.startHold.open.Load()
	})
	if relogged := tc.ownDropSequencesOf(learner, gone); len(relogged) != 0 {
		t.Fatalf("setup: the node logged the drop as its own at %v; the stop before its start hold opened must have lost that", relogged)
	}
	for _, peer := range []int{1, 2} {
		if dropped, _ := tc.nodes[peer].server.core.generationDropped(gone, generation); dropped {
			t.Fatalf("setup: node %d learned the drop before anything of the database reached the node that holds it", peer+1)
		}
	}

	late := bucketOwnedIn(t, tc, 1, gone, "late")
	tc.frame(1, transport.OpCreateBucket, CreateBucketRequest{Database: gone, Name: late, Type: BucketTypeHash})
	for _, peer := range []int{1, 2} {
		pollUntil(t, fmt.Sprintf("node %d learned the drop of generation %d of %q", peer+1, generation, gone), func() bool {
			dropped, err := tc.nodes[peer].server.core.generationDropped(gone, generation)
			if err != nil {
				t.Fatalf("reading node %d's tombstones: %v", peer+1, err)
			}
			return dropped
		})
	}
}
