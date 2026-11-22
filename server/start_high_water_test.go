package server

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0481 part 2: an owner whose log went backwards must not issue a sequence
// its peers already hold. These tests drive whole nodes; the log's own half
// (the skip it records, and the readers that accept it) is tested in storage.

// putValue writes key = value into the test bucket through node index at the
// given write concern and returns the answer.
func (tc *testCluster) putValue(index int, key, value, concern string) *transport.Frame {
	tc.t.Helper()
	return tc.frame(index, transport.OpPut, InsertRequest{
		Database: testDatabase, Bucket: testBucket, Key: key,
		Data: map[string]interface{}{"v": value}, WriteConcern: concern,
	})
}

// mustPutValue is putValue failing the test on an error answer.
func (tc *testCluster) mustPutValue(index int, key, value, concern string) {
	tc.t.Helper()
	if frame := tc.putValue(index, key, value, concern); frame.Opcode == transport.OpError {
		tc.t.Fatalf("put %s=%s through node %d at %s: %s", key, value, index+1, concern, frame.Payload)
	}
}

// mustUpsertValue is mustPutValue for a key the bucket already holds: insert
// refuses an existing id, so a write that means to replace sends an upsert.
func (tc *testCluster) mustUpsertValue(index int, key, value, concern string) {
	tc.t.Helper()
	frame := tc.frame(index, transport.OpUpsert, InsertRequest{
		Database: testDatabase, Bucket: testBucket, Key: key,
		Data: map[string]interface{}{"v": value}, WriteConcern: concern,
	})
	if frame.Opcode == transport.OpError {
		tc.t.Fatalf("upsert %s=%s through node %d at %s: %s", key, value, index+1, concern, frame.Payload)
	}
}

// heldValue is key's value and version on node index's own copy.
func (tc *testCluster) heldValue(index int, key string) (value string, version uint64, found bool) {
	tc.t.Helper()
	found, raw, version, err := tc.nodes[index].server.core.versionedFind(testDatabase, testBucket, key)
	if err != nil {
		tc.t.Fatalf("reading %s on node %d: %v", key, index+1, err)
	}
	if !found {
		return "", 0, false
	}
	var document struct {
		V string `bson:"v"`
	}
	if err := bson.Unmarshal(raw, &document); err != nil {
		tc.t.Fatalf("decoding %s on node %d: %v", key, index+1, err)
	}
	return document.V, version, true
}

// highestPositionFor is the highest position any running node other than
// source has applied of source's log.
func (tc *testCluster) highestPositionFor(source int) uint64 {
	var highest uint64
	for index, node := range tc.nodes {
		if index == source || node.server == nil {
			continue
		}
		highest = max(highest, appliedSnapshot(node.server.core.cluster)[tc.addrs[source]])
	}
	return highest
}

// ownSequencesOf is the sequence of every entry node index's log holds for a
// put of one of keys that it originated.
func (tc *testCluster) ownSequencesOf(index int, keys map[string]bool) map[string]uint64 {
	tc.t.Helper()
	entries, err := tc.nodes[index].server.core.wal.ReadFrom(1)
	if err != nil {
		tc.t.Fatalf("reading node %d's log: %v", index+1, err)
	}
	sequences := make(map[string]uint64)
	for _, entry := range entries {
		if entry.Origin.Kind == storage.WALOriginReplica || entry.Bucket != testBucket || (transport.Opcode(entry.Op) != transport.OpPut && transport.Opcode(entry.Op) != transport.OpUpsert) {
			continue
		}
		var put InsertRequest
		if err := bson.Unmarshal(entry.Data, &put); err != nil {
			tc.t.Fatalf("decoding the put at %d in node %d's log: %v", entry.Seq, index+1, err)
		}
		if keys[put.Key] {
			sequences[put.Key] = entry.Seq
		}
	}
	return sequences
}

// requireRestoredOwnerReusedNothing is the assertion every rollback test of
// this file ends with. Before the rollback the replicas held the owner's log
// through highWater and key "x" at lostVersion; after it the owner wrote
// "x" = "after" and the keys of written. Every replica must hold each of them,
// "x" above lostVersion, and the owner must have logged each above highWater.
func (tc *testCluster) requireRestoredOwnerReusedNothing(t *testing.T, owner int, highWater, lostVersion uint64, written []string) {
	t.Helper()
	wanted := map[string]bool{"x": true}
	for _, key := range written {
		wanted[key] = true
	}
	for key, seq := range tc.ownSequencesOf(owner, wanted) {
		if seq <= highWater {
			t.Errorf("the owner logged %s at sequence %d after its log went backwards; the replicas held its log through %d, "+
				"so the sequence is one they hold another write at", key, seq, highWater)
		}
	}
	for index := range tc.nodes {
		if index == owner {
			continue
		}
		deadline := time.Now().Add(20 * time.Second)
		for {
			value, version, found := tc.heldValue(index, "x")
			if found && value == "after" && version > lostVersion {
				break
			}
			if time.Now().After(deadline) {
				t.Errorf("node %d holds x = %q (found %v) at version %d; want \"after\" above the lost write's version %d: "+
					"the write the restored owner acknowledged orders below a write its log no longer has", index+1, value, found, version, lostVersion)
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		for _, key := range written {
			if _, _, found := tc.heldValue(index, key); !found {
				t.Errorf("node %d does not hold %s, which the restored owner acknowledged at all", index+1, key)
			}
		}
	}
}

// TestAVMSnapshotRestoreReusesNoSequence is BUG-0481 part 2's case: a VM
// snapshot restore rolls a node's log and its data back together, so nothing
// on the node shows the rollback. The owner's whole data directory is copied,
// it writes 21 more entries at all, and the copy is put back. On the starting
// tree the restored owner issued its next sequences from where the copy's log
// ended, which the replicas held other writes at: its write of x ordered below
// the write of x it had lost. Each write it makes after the restore must be
// above every position the replicas hold, and on every replica.
func TestAVMSnapshotRestoreReusesNoSequence(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	owner := ownerIndexOf(t, tc, testBucket)
	tc.mustPutValue(owner, "x", "before", "all")
	for i := 0; i < 10; i++ {
		tc.mustPutValue(owner, fmt.Sprintf("kept%02d", i), "kept", "all")
	}

	dataDir := tc.nodes[owner].cfg.Storage.DataDir
	snapshot := filepath.Join(t.TempDir(), "snapshot")
	base := tc.stopNodeKeepingData(owner)
	copyTree(t, dataDir, snapshot)
	tc.nodes[owner] = tc.startNode(owner, base, tc.token)

	tc.mustUpsertValue(owner, "x", "lost", "all")
	for i := 0; i < 20; i++ {
		tc.mustPutValue(owner, fmt.Sprintf("lost%02d", i), "lost", "all")
	}
	highWater := tc.highestPositionFor(owner)
	_, lostVersion, _ := tc.heldValue((owner+1)%3, "x")

	tc.stopNodeKeepingData(owner)
	if err := os.RemoveAll(dataDir); err != nil {
		t.Fatalf("removing the owner's data directory: %v", err)
	}
	copyTree(t, snapshot, dataDir)
	tc.nodes[owner] = tc.startNode(owner, base, tc.token)

	tc.mustUpsertValue(owner, "x", "after", "all")
	written := []string{"after00", "after01", "after02"}
	for _, key := range written {
		tc.mustPutValue(owner, key, "after", "all")
	}
	tc.requireRestoredOwnerReusedNothing(t, owner, highWater, lostVersion, written)
}

// TestALostLogWithItsDataKeptReusesNoSequence is the other half: the owner's
// wal/ directory is lost and its data kept, so its new log starts again at 1
// under buckets whose replicas hold the old log far above it.
func TestALostLogWithItsDataKeptReusesNoSequence(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	owner := ownerIndexOf(t, tc, testBucket)
	tc.mustPutValue(owner, "x", "lost", "all")
	for i := 0; i < 20; i++ {
		tc.mustPutValue(owner, fmt.Sprintf("kept%02d", i), "kept", "all")
	}
	highWater := tc.highestPositionFor(owner)
	_, lostVersion, _ := tc.heldValue((owner+1)%3, "x")

	walDir := filepath.Join(tc.nodes[owner].cfg.Storage.DataDir, "wal")
	base := tc.stopNodeKeepingData(owner)
	if err := os.RemoveAll(walDir); err != nil {
		t.Fatalf("removing the owner's log: %v", err)
	}
	tc.nodes[owner] = tc.startNode(owner, base, tc.token)

	tc.mustUpsertValue(owner, "x", "after", "all")
	written := []string{"after00", "after01", "after02"}
	for _, key := range written {
		tc.mustPutValue(owner, key, "after", "all")
	}
	tc.requireRestoredOwnerReusedNothing(t, owner, highWater, lostVersion, written)
}

// TestARestartedOwnerHoldsWritesUntilAMajorityOfPeersAnswers is the owner's
// ruling for a peer that is down at start (BUG-0481, 2026-10-17): five nodes,
// two of the owner's four peers stopped, the owner restarted. Two of four is
// not a majority: a write is held and, past the write-concern timeout, refused
// with a message naming both stopped peers. With one of them back, three of
// four have answered and the owner writes again, though the other is still
// down. On the starting tree the first write was taken at once.
func TestARestartedOwnerHoldsWritesUntilAMajorityOfPeersAnswers(t *testing.T) {
	const timeout = 3 * time.Second
	tc := newTestClusterTuned(t, 5, 3, nil, func(index int, c *Cluster) { c.wcTimeout = timeout })
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	owner := ownerIndexOf(t, tc, testBucket)
	tc.mustPutValue(owner, "before", "v", "one")

	var stopped []int
	for index := range tc.nodes {
		if index != owner && len(stopped) < 2 {
			stopped = append(stopped, index)
		}
	}
	restartFirst := stopNode(t, tc, stopped[0])
	stopNode(t, tc, stopped[1])
	tc.restartNode(owner)

	began := time.Now()
	frame := tc.putValue(owner, "held", "v", "one")
	took := time.Since(began)
	if frame.Opcode != transport.OpError {
		t.Fatalf("a write to the restarted owner with two of its four peers down was taken after %v; want it held and refused", took)
	}
	refusal := string(frame.Payload)
	for _, index := range stopped {
		if !strings.Contains(refusal, tc.addrs[index]) {
			t.Errorf("the refusal does not name the unreachable peer %s: %s", tc.addrs[index], refusal)
		}
	}
	if took < timeout/2 {
		t.Errorf("the write was refused after %v; it must be held until the write-concern timeout (%v) first", took, timeout)
	}
	// A peer that answered is not asked to rebase while the hold lasts: the
	// rebase lowers what it holds, and the log has not moved past it yet.
	current := tc.nodes[owner].server.core.wal.Incarnation()
	for index, node := range tc.nodes {
		if index == owner || node.server == nil {
			continue
		}
		if recorded := node.server.core.cluster.recordedPosition(tc.addrs[owner]).Incarnation; string(recorded) == string(current[:]) {
			t.Errorf("node %d was rebased onto the restarted owner's log before a majority of its peers had answered", index+1)
		}
	}

	restartFirst()
	deadline := time.Now().Add(45 * time.Second)
	for {
		frame := tc.putValue(owner, "after", "v", "one")
		if frame.Opcode != transport.OpError {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("with three of its four peers answering, the restarted owner still refuses writes: %s", frame.Payload)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestANormalRestartReusesNothingAndSkipsNothing guards the common case: an
// owner restarted with its log intact and every peer up takes its first write
// within a moment, at the sequence after its log's last, with no skip.
func TestANormalRestartReusesNothingAndSkipsNothing(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	owner := ownerIndexOf(t, tc, testBucket)
	for i := 0; i < 5; i++ {
		tc.mustPutValue(owner, fmt.Sprintf("before%02d", i), "v", "all")
	}
	tc.restartNode(owner)
	last := tc.nodes[owner].server.core.wal.LastSequence()

	began := time.Now()
	tc.mustPutValue(owner, "first", "v", "all")
	if took := time.Since(began); took > 10*time.Second {
		t.Errorf("the first write after a normal restart took %v", took)
	}
	if seq := tc.ownSequencesOf(owner, map[string]bool{"first": true})["first"]; seq != last+1 {
		t.Errorf("the first write after a normal restart was logged at %d; the log ended at %d, so it must be %d", seq, last, last+1)
	}
}
