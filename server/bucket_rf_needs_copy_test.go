package server

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The member's side of the factor alter for a member that holds nothing
// (bucket-replication-rev8-spec §3.3, §5.3 rows 4 and 5 and the pull window,
// §7.2; G3 build doc step 5). A member an increase newly places writes one
// needs-a-copy record and nothing else; one an alter does not place deletes
// it and cancels every pull of the name in flight. The landed copy, the
// bucket's drop and a database drop of the record's generation delete it.
// With auto-rebalance off, as most of these tests run, a pull is an
// operator's migrate; with it on, the pending-copy worker's
// (bucket_rf_worker_test.go).

// storedNeedsCopy is the value of one 'n' record as a test reads it.
type storedNeedsCopy struct {
	Database          string `bson:"database"`
	Bucket            string `bson:"bucket"`
	ReplicationFactor int32  `bson:"replication_factor"`
	Epoch             int64  `bson:"epoch"`
	Generation        int64  `bson:"generation"`
	Since             int64  `bson:"since"`
}

// needsCopyOnDisk reads node s's needs-a-copy record of the test database's
// bucket, and whether there is one.
func needsCopyOnDisk(t *testing.T, s *Server, bucket string) (storedNeedsCopy, bool) {
	t.Helper()
	key := storage.NodeKey(storage.NSReplication, []byte{'n'}, []byte(placementOf(testDatabase, bucket).encoded))
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	stored, err := snap.Get(key)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return storedNeedsCopy{}, false
	}
	if err != nil {
		t.Fatalf("reading %s's needs-a-copy record of %q: %v", s.cluster.self.ID, bucket, err)
	}
	var record storedNeedsCopy
	if err := bson.Unmarshal(stored, &record); err != nil {
		t.Fatalf("decoding %s's needs-a-copy record of %q: %v", s.cluster.self.ID, bucket, err)
	}
	return record, true
}

// requireNoNeedsCopy fails if node s holds a needs-a-copy record of bucket.
func requireNoNeedsCopy(t *testing.T, s *Server, bucket, when string) {
	t.Helper()
	if record, found := needsCopyOnDisk(t, s, bucket); found {
		t.Errorf("%s, %s still holds a needs-a-copy record of %q: %+v", when, s.cluster.self.ID, bucket, record)
	}
}

// requireNeedsCopyOf fails unless node s holds a needs-a-copy record of
// bucket carrying the alter's factor, epoch and generation.
func requireNeedsCopyOf(t *testing.T, s *Server, bucket string, factor int32, epoch uint64, generation int64) {
	t.Helper()
	record, found := needsCopyOnDisk(t, s, bucket)
	if !found {
		t.Errorf("%s, newly placed and holding nothing, wrote no needs-a-copy record of %q", s.cluster.self.ID, bucket)
		return
	}
	want := storedNeedsCopy{Database: testDatabase, Bucket: bucket, ReplicationFactor: factor, Epoch: int64(epoch), Generation: generation, Since: record.Since}
	if record != want || record.Since == 0 {
		t.Errorf("%s's needs-a-copy record of %q is %+v, want %+v with its write time", s.cluster.self.ID, bucket, record, want)
	}
}

// increasedOnEmptyMembers is three nodes at default 1 with auto-rebalance off,
// so nothing pulls an increase's new copies but a migrate, and a bucket owned
// by node 1 at 1 with keys written, raised to 3: the other two hold nothing
// and await their copies. It returns the owner's epoch and the bucket's
// generation, which the alter carries.
func increasedOnEmptyMembers(t *testing.T, bucketType BucketType) (tc *testCluster, bucket string, keys []string, epoch uint64, generation int64) {
	t.Helper()
	return increasedOnEmptyMembersWith(t, bucketType, false)
}

// increasedOnEmptyMembersWith is increasedOnEmptyMembers with auto-rebalance
// as autoRebalance says on every node: on, each new member's pending-copy
// worker copies the bucket by itself.
func increasedOnEmptyMembersWith(t *testing.T, bucketType BucketType, autoRebalance bool) (tc *testCluster, bucket string, keys []string, epoch uint64, generation int64) {
	t.Helper()
	tc = newTestClusterWith(t, 3, 1, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = autoRebalance })
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket = bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: bucketType})
	for i := 0; i < 5; i++ {
		keys = append(keys, writeOneTo(t, tc, 0, bucket, bucketType, i))
	}
	owner := tc.nodes[0].server.core
	facts, _ := owner.peekSettled(testDatabase, bucket)
	answer := alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}))
	if len(answer.Pending) != 2 {
		t.Fatalf("the increase from 1 to 3 names %v as pending, want the two members", answer.Pending)
	}
	waitForBarrier(t, tc, 0, "after-the-increase", 1, 2)
	return tc, bucket, keys, owner.cluster.Epoch(), facts.generation
}

// writeOneTo writes the i-th element to bucket through node index with the
// opcode its engine takes, and returns its key ("" for a queue or a heap).
func writeOneTo(t *testing.T, tc *testCluster, index int, bucket string, bucketType BucketType, i int) string {
	t.Helper()
	key := fmt.Sprintf("k%03d", i)
	data := map[string]interface{}{"n": i}
	switch bucketType {
	case BucketTypeFIFO:
		tc.ok(index, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: bucket, Data: data})
	case BucketTypeLIFO:
		tc.ok(index, transport.OpLIFOPush, PushRequest{Database: testDatabase, Bucket: bucket, Data: data})
	case BucketTypeHeap:
		tc.ok(index, transport.OpHeapPush, PushRequest{Database: testDatabase, Bucket: bucket, Data: data, Priority: int64(i)})
	case BucketTypeBlob:
		tc.ok(index, transport.OpBlobWrite, BlobWriteRequest{Database: testDatabase, Bucket: bucket, Key: key, Data: []byte(key)})
	case BucketTypeBTree:
		tc.ok(index, transport.OpBTreeInsert, InsertRequest{Database: testDatabase, Bucket: bucket, Key: key, Data: data})
	default:
		tc.ok(index, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: key, Data: data})
	}
	return key
}

// TestIncreaseBackfillsEachNewHolderFromItsOwnStorage is T-05's G4 form
// (OQ-G3-5; G4 to G7 build doc step 1): an increase from 1 to 3 onto members
// that hold nothing, with auto-rebalance on and no migrate. Each member's
// pending-copy worker copies the bucket from its owner by itself, its
// needs-a-copy record goes with the landing, and every acknowledged write,
// before the increase, while the copies are pending and after they landed, is
// then read from the member's own storage.
func TestIncreaseBackfillsEachNewHolderFromItsOwnStorage(t *testing.T) {
	tc, bucket, keys, _, _ := increasedOnEmptyMembersWith(t, BucketTypeHash, true)
	defer tc.stop()
	keys = append(keys, writeOneTo(t, tc, 0, bucket, BucketTypeHash, 5))
	for member := 1; member <= 2; member++ {
		waitForPendingCopyLanded(t, tc, member, bucket)
	}
	keys = append(keys, writeOneTo(t, tc, 0, bucket, BucketTypeHash, 6))
	waitForBarrier(t, tc, 0, "after-the-copies", 1, 2)
	for member := 1; member <= 2; member++ {
		requireHeldLocally(t, tc.nodes[member].server.core, bucket, keys)
	}
}

// TestIncreaseBackfillsEveryEngine is T-35, a freeze: T-05's G4 shape for each
// of the six engines. Every engine is a range on the shared store, so each new
// member's landed copy, made by its own worker, holds as many keys as the
// owner's.
func TestIncreaseBackfillsEveryEngine(t *testing.T) {
	for _, bucketType := range []BucketType{BucketTypeHash, BucketTypeBTree, BucketTypeFIFO, BucketTypeLIFO, BucketTypeHeap, BucketTypeBlob} {
		t.Run(string(bucketType), func(t *testing.T) {
			tc, bucket, _, _, _ := increasedOnEmptyMembersWith(t, bucketType, true)
			defer tc.stop()
			owner := tc.nodes[0].server.core
			for member := 1; member <= 2; member++ {
				core := tc.nodes[member].server.core
				waitForPendingCopyLanded(t, tc, member, bucket)
				ownerCopy, memberCopy := lookupBucket(owner, testDatabase, bucket), lookupBucket(core, testDatabase, bucket)
				if memberCopy == nil {
					t.Fatalf("node %d serves no copy of the %s bucket once its worker landed it", member+1, bucketType)
				}
				held := liveKeysIn(t, core.kv, memberCopy.slot, memberCopy.databaseID, memberCopy.id.Load())
				if want := liveKeysIn(t, owner.kv, ownerCopy.slot, ownerCopy.databaseID, ownerCopy.id.Load()); held != want {
					t.Errorf("node %d's landed copy of the %s bucket holds %d keys, the owner's %d", member+1, bucketType, held, want)
				}
			}
		})
	}
}

// TestReadOnAPlacedButUnseededMemberForwardsAndNeverAnswersNotFound is T-06,
// guard-only: a member an increase places, which holds nothing yet, forwards
// a read of the bucket to its owner (holderServes's open-store half). The
// nodes' default is 3 and the bucket was created at 1, so a member that never
// held it places it by its default and hosts it: only the open-store half
// keeps it from answering the read.
func TestReadOnAPlacedButUnseededMemberForwardsAndNeverAnswersNotFound(t *testing.T) {
	tc := newTestClusterWith(t, 3, 3, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	createBucketAt(tc, 0, testDatabase, bucket, 1)
	keys := []string{writeOneTo(t, tc, 0, bucket, BucketTypeHash, 0), writeOneTo(t, tc, 0, bucket, BucketTypeHash, 1)}
	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}))
	waitForBarrier(t, tc, 0, "after-the-increase", 1, 2)
	for member := 1; member <= 2; member++ {
		core := tc.nodes[member].server.core
		if core.bucketStoreOpen(testDatabase, bucket) || !core.cluster.Hosts(placementOf(testDatabase, bucket)) {
			t.Fatalf("setup: node %d holds %q (%v) or does not host it by its own placement", member+1, bucket, core.bucketStoreOpen(testDatabase, bucket))
		}
		requireReadsFound(t, tc, member, bucket, keys, "on a placed member that holds nothing")
	}
}

// needsCopyWrites is the batch every apply of an alter on a member that holds
// nothing writes through, captured by needsCopyBatchHook.
func needsCopyWrites(t *testing.T, member string, apply func()) *countingBatch {
	t.Helper()
	var counted *countingBatch
	hook := func(nodeID string, b storage.KVBatch) storage.KVBatch {
		if nodeID != member {
			return b
		}
		counted = &countingBatch{KVBatch: b}
		return counted
	}
	needsCopyBatchHook.Store(&hook)
	defer needsCopyBatchHook.Store(nil)
	apply()
	if counted == nil {
		t.Fatalf("%s's apply of the alter wrote through no needs-a-copy batch", member)
	}
	return counted
}

// TestAnAltersApplyOnANewMemberWritesOneRecordAndDoesNotGrowWithOtherBuckets
// is T-55, the member's half of rule 11 (spec §13): an increase's apply on a
// newly placed member that holds nothing writes one key, its needs-a-copy
// record of 107 bytes plus the names, makes no catalog entry or record of the
// bucket, and writes the same with 100,000 other buckets on the member as with
// none.
func TestAnAltersApplyOnANewMemberWritesOneRecordAndDoesNotGrowWithOtherBuckets(t *testing.T) {
	if testing.Short() {
		t.Skip("creates 100,000 buckets")
	}
	tc := newTestClusterWith(t, 3, 1, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	first := bucketOwnedBy(t, tc, 0, "sessions")
	second := bucketOwnedBy(t, tc, 0, "accounts")
	member := nodeIndexByAddr(t, tc.addrs, mustReplicaIDsAt(t, clusterOf(tc, 0), testDatabase, first, 2)[1])
	if walk := mustReplicaIDsAt(t, clusterOf(tc, 0), testDatabase, second, 2); walk[1] != tc.addrs[member] {
		t.Fatalf("setup: %q and %q are placed second on different members", first, second)
	}
	for _, bucket := range []string{first, second} {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	}
	memberCore := tc.nodes[member].server.core
	measure := func(bucket string) *countingBatch {
		t.Helper()
		return needsCopyWrites(t, tc.addrs[member], func() {
			alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2}))
			// Through the handler, not a client session, which the minutes
			// spent creating the other buckets can leave idle and closed.
			barrier := sendInBackground(t, clusterOf(tc, 0), transport.OpCreateDB, CreateDatabaseRequest{Name: "after-" + bucket})
			awaitAnswer(t, barrier, "the barrier's database create")
			waitForWALCount(t, tc, member, transport.OpCreateDB, "", walOpCount(t, tc.nodes[0].server.core.wal, transport.OpCreateDB, ""))
		})
	}
	alone := measure(first)

	const others = 100_000
	if created, err := memberCore.CreateDatabase("others"); err != nil || !created.Success {
		t.Fatalf("creating the other buckets' database: %+v, %v", created, err)
	}
	started := time.Now()
	for i := 0; i < others; i++ {
		if created, err := memberCore.CreateBucket(CreateBucketRequest{Database: "others", Name: fmt.Sprintf("other-%06d", i), Type: BucketTypeHash}); err != nil || !created.Success {
			t.Fatalf("creating other bucket %d: %+v, %v", i, created, err)
		}
	}
	t.Logf("created %d other buckets on node %d in %s", others, member+1, time.Since(started).Round(time.Millisecond))
	crowded := measure(second)

	if alone.writes != 1 {
		t.Errorf("an increase's apply on a member that holds nothing wrote %d keys, want 1 (its needs-a-copy record)", alone.writes)
	}
	if crowded.writes != alone.writes || crowded.bytes-alone.bytes != len(second)-len(first) {
		t.Errorf("with %d other buckets the apply wrote %d keys and %d bytes; with none, %d keys and %d bytes (the names differ by %d)",
			others, crowded.writes, crowded.bytes, alone.writes, alone.bytes, len(second)-len(first))
	}
	for _, bucket := range []string{first, second} {
		if _, held := memberCore.peekBucket(testDatabase, bucket); held {
			t.Errorf("node %d's apply made a record of %q, which it does not hold", member+1, bucket)
		}
		if _, catalogued, err := memberCore.readCatalogEntry(testDatabase, bucket); err != nil || catalogued {
			t.Errorf("node %d's apply catalogued %q, which it does not hold: %v", member+1, bucket, err)
		}
	}
	record, found := needsCopyOnDisk(t, memberCore, second)
	if !found {
		t.Fatalf("node %d holds no needs-a-copy record of %q", member+1, second)
	}
	encoded, err := bson.Marshal(record)
	if err != nil {
		t.Fatalf("encoding the record: %v", err)
	}
	t.Logf("one apply on a new member: %d key, %d bytes of key and value, alone and beside %d buckets; the needs-a-copy record's value is %d bytes "+
		"with a %d-byte database and a %d-byte bucket name", alone.writes, alone.bytes, others, len(encoded), len(testDatabase), len(second))
	if want := 107 + len(testDatabase) + len(second); len(encoded) != want {
		t.Errorf("a needs-a-copy record's value is %d bytes, spec §13 says 107 plus the names, %d", len(encoded), want)
	}
}

// TestADatabaseDropDeletesTheNeedsACopyRecordsOfItsGeneration is T-190's
// member half (spec §3.3): a database drop deletes, in its own batch, the
// needs-a-copy records of the generations it names, live and when it is
// replayed at start, and a drop of another generation leaves the record of the
// bucket's live one. T-54 proper, with the worker's queue, is G4's.
func TestADatabaseDropDeletesTheNeedsACopyRecordsOfItsGeneration(t *testing.T) {
	t.Run("live", func(t *testing.T) {
		tc, bucket, _, _, _ := increasedOnEmptyMembers(t, BucketTypeHash)
		defer tc.stop()
		tc.ok(0, transport.OpDeleteDB, DeleteDatabaseRequest{Name: testDatabase})
		waitForBarrier(t, tc, 0, "after-the-drop", 1, 2)
		for member := 1; member <= 2; member++ {
			requireNoNeedsCopy(t, tc.nodes[member].server.core, bucket, "after the database drop")
		}
	})

	// The member is sent nothing once the drop is logged: its log is given
	// the owner's drop entry while it is stopped, as a member that logged the
	// drop and stopped before its batch committed has it, and its start
	// replays it. Node 3's stream to it is frozen too: node 3 relogs the drop
	// as its own (G-12) and ships it, and node 2 would then apply it live,
	// which is the next case (BUG-0593).
	t.Run("replayed at start", func(t *testing.T) {
		tc, bucket, _, _, _ := increasedOnEmptyMembers(t, BucketTypeHash)
		defer tc.stop()
		restarted := replayDatabaseDropOn(t, tc, 1)
		requireNoNeedsCopy(t, restarted.server.core, bucket, "once node 2 replayed the database drop at start")
	})

	// BUG-0593: the member applies the drop live, then restarts. Replay runs
	// its whole log, so the increase's apply comes again after the drop's
	// tombstone is on disk, and the drop's own replay learns nothing and
	// commits nothing. The record of the dropped generation must not come
	// back.
	t.Run("applied live, then replayed at start", func(t *testing.T) {
		tc, bucket, _, _, _ := increasedOnEmptyMembers(t, BucketTypeHash)
		defer tc.stop()
		tc.ok(0, transport.OpDeleteDB, DeleteDatabaseRequest{Name: testDatabase})
		waitForBarrier(t, tc, 0, "after-the-drop", 1, 2)
		requireNoNeedsCopy(t, tc.nodes[1].server.core, bucket, "after the database drop, before the restart")
		restarted := restartNodeAfterEditingItsWAL(t, tc, 1, func(*storage.WAL) {})
		requireNoNeedsCopy(t, restarted.server.core, bucket, "once node 2, which applied the database drop live, replayed its log at start")
	})

	t.Run("a drop of another generation", func(t *testing.T) {
		tc, bucket, _, _, generation := increasedOnEmptyMembers(t, BucketTypeHash)
		defer tc.stop()
		member := tc.nodes[1].server.core
		other := unknownGenerationOtherThan(t, member, generation)
		if _, err := member.commitGenerationDrop(testDatabase, []int64{other}, 0, true); err != nil {
			t.Fatalf("committing a drop of generation %d on node 2: %v", other, err)
		}
		if _, found := needsCopyOnDisk(t, member, bucket); !found {
			t.Errorf("a drop of generation %d deleted node 2's needs-a-copy record of %q, which is of generation %d", other, bucket, generation)
		}
	})
}

// replayDatabaseDropOn drops the test database through node 1 and has node
// index, a member, apply the drop only by replaying it at start. The member is
// sent nothing once the drop is logged: its log, pruned to its tail, is given
// the owner's drop entry while it is stopped, as a member that logged the drop
// and stopped before its batch committed has it, and its start replays it. Node 3's
// stream to it is frozen too: node 3 relogs the drop as its own (G-12) and
// ships it, and the member would then apply it live (BUG-0593). It returns
// the restarted node.
func replayDatabaseDropOn(t *testing.T, tc *testCluster, index int) *testNode {
	t.Helper()
	owner := clusterOf(tc, 0)
	unfreeze := freezePeer(owner.peerFor(mustNode(t, owner, tc.addrs[index])))
	t.Cleanup(unfreeze)
	third := clusterOf(tc, 3-index)
	unfreezeThird := freezePeer(third.peerFor(mustNode(t, third, tc.addrs[index])))
	t.Cleanup(unfreezeThird)
	tc.frame(0, transport.OpDeleteDB, DeleteDatabaseRequest{Name: testDatabase})
	entries, err := tc.nodes[0].server.core.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading the owner's WAL: %v", err)
	}
	var drop *storage.WALEntry
	for i := range entries {
		if transport.Opcode(entries[i].Op) == transport.OpDeleteDB {
			drop = &entries[i]
		}
	}
	if drop == nil {
		t.Fatal("the owner logged no database drop")
	}
	return restartNodeAfterEditingItsWAL(t, tc, index, func(wal *storage.WAL) {
		// Pruned first, as retention would have: a replayed alter earlier in
		// the log would queue the bucket again before the drop, and the
		// replay must delete the record when nothing but the record says the
		// copy is pending (spec §3.3).
		pruneToTheTail(t, wal)
		origin := storage.WALOrigin{Kind: storage.WALOriginReplica, Source: owner.self.ID, Seq: drop.Seq}
		if _, err := wal.AppendFrom(drop.Bucket, drop.Op, drop.Key, drop.Data, origin); err != nil {
			t.Fatalf("appending the owner's drop to node %d's WAL: %v", index+1, err)
		}
	})
}

// unknownGenerationOtherThan is a generation of the test database, not
// generation, that node s has not recorded dropped.
func unknownGenerationOtherThan(t *testing.T, s *Server, generation int64) int64 {
	t.Helper()
	for candidate := int64(1); candidate < generation+100; candidate++ {
		unknown, err := s.unknownGenerations(testDatabase, []int64{candidate})
		if err != nil {
			t.Fatalf("reading %s's dropped generations: %v", s.cluster.self.ID, err)
		}
		if candidate != generation && len(unknown) == 1 {
			return candidate
		}
	}
	t.Fatalf("setup: no generation other than the bucket's is unknown to %s", s.cluster.self.ID)
	return 0
}

// TestADropLearnedFromAThirdNodeBeforeTheAlterLeavesNoNeedsACopyRecord is
// BUG-0593's other order: node 2 learns the database drop from node 3 before
// the owner's increase reaches it (the owner's stream to it is frozen), then
// the owner's stream delivers the increase and the drop. The increase's apply
// comes after the tombstone, live, and the owner's drop then learns nothing,
// so only alterOfNoCopy's check of the tombstone keeps the record out; and a
// restart, which replays both, keeps it out too.
func TestADropLearnedFromAThirdNodeBeforeTheAlterLeavesNoNeedsACopyRecord(t *testing.T) {
	tc := newTestClusterWith(t, 3, 1, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	waitForBarrier(t, tc, 0, "after-the-create", 1, 2)
	facts, _ := tc.nodes[0].server.core.peekSettled(testDatabase, bucket)
	owner := clusterOf(tc, 0)
	unfreeze := freezePeer(owner.peerFor(mustNode(t, owner, tc.addrs[1])))
	defer unfreeze()
	alterAccepted(t, alterFrame(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3}))
	tc.ok(0, transport.OpDeleteDB, DeleteDatabaseRequest{Name: testDatabase})
	member := tc.nodes[1].server.core
	droppedHere := func() bool {
		dropped, err := member.generationDropped(testDatabase, facts.generation)
		return err == nil && dropped
	}
	if !eventually(droppedHere) {
		// Push something on node 3's stream to carry its relogged drop.
		waitForBarrier(t, tc, 2, "through-node-3", 1)
		pollUntil(t, "node 2 learns the drop from node 3", droppedHere)
	}
	requireNoNeedsCopy(t, member, bucket, "node 2 learned the drop, before the owner's increase reached it")
	unfreeze()
	waitForBarrier(t, tc, 0, "after-the-unfreeze", 1, 2)
	if walOpCount(t, member.wal, transport.OpAlterBucket, bucket) == 0 {
		t.Fatalf("setup: node 2 logged no alter of %q, so the increase never reached it after the drop", bucket)
	}
	requireNoNeedsCopy(t, member, bucket, "the owner's increase reached node 2 after it had learned the drop")
	restarted := restartNodeAfterEditingItsWAL(t, tc, 1, func(*storage.WAL) {})
	requireNoNeedsCopy(t, restarted.server.core, bucket, "after node 2 replayed at start")
}

// TestAnAlterAppliedWhileADropCommitsLeavesNoNeedsACopyRecord guards the
// allocator's mutex in alterOfNoCopy (BUG-0593). The alter has checked that
// its generation is not dropped; at needsCopyBatchHook, still before it writes,
// a drop of that generation is started on the same node. Under the mutex the
// drop waits for the alter's commit and then deletes the record it wrote;
// without it, the drop commits its tombstone and finds no record to delete,
// and the alter then writes one for a generation that is dropped, for good.
func TestAnAlterAppliedWhileADropCommitsLeavesNoNeedsACopyRecord(t *testing.T) {
	tc := newTestClusterWith(t, 3, 1, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	facts, _ := tc.nodes[0].server.core.peekSettled(testDatabase, bucket)
	member := tc.nodes[1].server.core
	alter := AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3, PreviousFactor: 1, Epoch: member.cluster.Epoch(),
		Generation: facts.generation}

	dropDone := make(chan error, 1)
	var droppedEarly bool
	var earlyErr error
	hook := func(nodeID string, b storage.KVBatch) storage.KVBatch {
		if nodeID != member.cluster.self.ID {
			return b
		}
		go func() {
			_, err := member.commitGenerationDrop(testDatabase, []int64{facts.generation}, 0, true)
			dropDone <- err
		}()
		// Without the mutex the drop commits within milliseconds; with it, it
		// is still waiting when this gives up, and the alter goes on to write.
		select {
		case earlyErr = <-dropDone:
			droppedEarly = true
		case <-time.After(500 * time.Millisecond):
		}
		return b
	}
	needsCopyBatchHook.Store(&hook)
	defer needsCopyBatchHook.Store(nil)

	if err := member.alterOfNoCopy(alter); err != nil {
		t.Fatalf("applying the alter on node 2: %v", err)
	}
	dropErr := earlyErr
	if !droppedEarly {
		select {
		case dropErr = <-dropDone:
		case <-time.After(30 * time.Second):
			t.Fatal("the drop never committed")
		}
	}
	if dropErr != nil {
		t.Fatalf("committing the drop of generation %d on node 2: %v", facts.generation, dropErr)
	}
	requireNoNeedsCopy(t, member, bucket, "after an alter and a drop of its generation raced on node 2")
}

// pullHeldAtMaterialize is T-34's case 3: an increase from 2 to 3 places a
// member that holds nothing; its migrate is held after the owner's snapshot
// and its recorded seed, before the copy lands; a decrease from 3 to 2 is
// logged meanwhile and reaches the member, which it un-places (row 5). Once
// released, the pull is cancelled: the member holds no copy, serves no read,
// has no needs-a-copy record, and has forgotten the factor its pull recorded.
func pullHeldAtMaterialize(t *testing.T) {
	tc, bucket, member := pendingOnOneMember(t)
	defer tc.stop()
	putDocumentAt(tc, 0, testDatabase, bucket, "before", "all")
	memberCore := tc.nodes[member].server.core

	held, release := holdBucketAt(t, &beforeMaterializeHook, bucket)
	defer release()
	pulled := migrateInBackground(memberCore.cluster, bucket)
	awaitHeld(t, held, fmt.Sprintf("node %d's pull of %q", member+1, bucket))
	alterWithin(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2})
	waitForWALCount(t, tc, member, transport.OpAlterBucket, bucket, 2)
	release()
	select {
	case err := <-pulled:
		if err == nil {
			t.Errorf("node %d's migrate of %q succeeded after the decrease un-placed it", member+1, bucket)
		}
	case <-time.After(60 * time.Second):
		t.Fatalf("node %d's migrate of %q never returned", member+1, bucket)
	}
	if memberCore.bucketStoreOpen(testDatabase, bucket) {
		t.Errorf("node %d, un-placed by the decrease while its pull was in flight, landed a copy of %q", member+1, bucket)
	}
	requireReadsFound(t, tc, member, bucket, []string{"before"}, "after the cancelled pull")
	requireNoNeedsCopy(t, memberCore, bucket, "after the decrease")
	if factor, registered := registeredFactor(memberCore.cluster, testDatabase, bucket); registered {
		t.Errorf("node %d's registry still places %q at %d after its pull was cancelled", member+1, bucket, factor)
	}
}

// pullHeldBeforeTheSnapshot is T-34's case 5 (RF-1, the pull window's case
// 3): the member's migrate passes the owner's placement check, read from the
// owner's registry at 3, and is held at beforeMigrateSnapshotHook; a decrease
// to 2 commits on the owner meanwhile, and its push to the member is held
// until the pull has answered, so the decrease cannot cancel the pull first.
// Released, the snapshot ships the record's factor, 2, and the member's
// second opinion refuses the pull before it records a seed: no copy lands, no
// seed position is recorded, the member's registry is not left at 3, it
// serves no read, and its needs-a-copy record is gone once the decrease
// reaches it. Partly guard-only: the second opinion and the record's factor
// are built.
func pullHeldBeforeTheSnapshot(t *testing.T) {
	tc, bucket, member := pendingOnOneMember(t)
	defer tc.stop()
	putDocumentAt(tc, 0, testDatabase, bucket, "before", "all")
	memberCore := tc.nodes[member].server.core
	ownerID := clusterOf(tc, 0).self.ID

	held, release := holdBucketAt(t, &beforeMigrateSnapshotHook, bucket)
	defer release()
	pulled := migrateInBackground(memberCore.cluster, bucket)
	awaitHeld(t, held, fmt.Sprintf("the owner's snapshot of %q for node %d", bucket, member+1))
	owner := clusterOf(tc, 0)
	unfreeze := freezePeer(owner.peerFor(mustNode(t, owner, tc.addrs[member])))
	defer unfreeze()
	alterWithin(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 2})
	release()
	select {
	case err := <-pulled:
		if !errors.Is(err, errNotPlacedHere) {
			t.Errorf("node %d's migrate of %q answered %v; want its second opinion's refusal (%v)", member+1, bucket, err, errNotPlacedHere)
		}
	case <-time.After(60 * time.Second):
		t.Fatalf("node %d's migrate of %q never returned", member+1, bucket)
	}
	unfreeze()
	if memberCore.bucketStoreOpen(testDatabase, bucket) {
		t.Errorf("node %d landed a copy of %q from a snapshot taken after the decrease un-placed it", member+1, bucket)
	}
	if seeded := memberCore.cluster.seededThroughFor(ownerID, testDatabase, bucket); seeded != 0 {
		t.Errorf("node %d recorded a seed position of %q at %d: the decrease is then skipped as held by the snapshot", member+1, bucket, seeded)
	}
	if factor, registered := registeredFactor(memberCore.cluster, testDatabase, bucket); registered && factor == 3 {
		t.Errorf("node %d's registry is left placing %q at 3", member+1, bucket)
	}
	waitForWALCount(t, tc, member, transport.OpAlterBucket, bucket, 2)
	requireReadsFound(t, tc, member, bucket, []string{"before"}, "after the refused pull")
	requireNoNeedsCopy(t, memberCore, bucket, "once the decrease reached it")
}

// emptyRows is T-174's rows 4 and 5: an increase from 1 to 3 is applied by
// two members that hold nothing, row 4, and a decrease back to 1 by the same
// two, row 5. Each acknowledges, so a bucket every node holds keeps every copy
// at concern all; row 4 writes the needs-a-copy record and row 5 deletes it.
func emptyRows(t *testing.T) {
	tc := newTestClusterWith(t, 3, 1, func(_ int, cfg *config.Config) { cfg.Server.AutoRebalance = false })
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForAlterFeature(t, tc)
	bucket := bucketOwnedBy(t, tc, 0, "sessions")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	createBucketAt(tc, 0, testDatabase, "other", 3)
	waitForServing(t, tc, testDatabase, "other", 3)

	alterWithin(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 3})
	waitForBarrier(t, tc, 0, "after-the-increase", 1, 2)
	requireWriteReachesEveryCopy(t, tc, 0, "other", "after-the-increase", 3)
	for member := 1; member <= 2; member++ {
		if _, found := needsCopyOnDisk(t, tc.nodes[member].server.core, bucket); !found {
			t.Errorf("node %d, row 4, wrote no needs-a-copy record of %q", member+1, bucket)
		}
	}
	alterWithin(t, tc, 0, AlterBucketRequest{Database: testDatabase, Name: bucket, ReplicationFactor: 1})
	waitForBarrier(t, tc, 0, "after-the-decrease", 1, 2)
	requireWriteReachesEveryCopy(t, tc, 0, "other", "after-the-decrease", 3)
	for member := 1; member <= 2; member++ {
		requireNoNeedsCopy(t, tc.nodes[member].server.core, bucket, "after the decrease (row 5)")
	}
}
