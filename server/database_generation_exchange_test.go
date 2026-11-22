package server

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// pushRefused delivers entries to receiver as a replication push from source
// and returns its answer, which must be a refusal.
func pushRefused(t *testing.T, receiver *testNode, source string, entries []ReplEntry) ReplPushResponse {
	t.Helper()
	core := receiver.server.core
	push, err := bson.Marshal(ReplPushRequest{Token: core.cluster.localToken(), FromNode: source, Entries: entries,
		Epoch: core.cluster.Epoch(), Stream: streamOf(t, core.cluster, source)})
	if err != nil {
		t.Fatalf("marshal replication push: %v", err)
	}
	respFrame, err := handleReplPush(context.Background(), transport.NewFrame(transport.OpReplPush, push), core)
	if err != nil {
		t.Fatalf("replication push: %v", err)
	}
	var resp ReplPushResponse
	if err := bson.Unmarshal(respFrame.Payload, &resp); err != nil {
		t.Fatalf("decode replication ack: %v", err)
	}
	if resp.Success {
		t.Fatalf("the receiver accepted %d entries it should have refused", len(entries))
	}
	return resp
}

// TestADatabaseDropRemovesOnlyTheGenerationsItNames replaces the rework's
// boundary test (TestADatabaseEntryIsPassedOverOnlyBelowASnapshotInItsDatabase):
// database DDL is no longer decided by snapshot positions but by the
// generations it names. Each row is pushed to one receiver.
//
// Guards: G-4's generation filter (row "two generations") and G-6 (rows
// "tombstoned" and "replaced").
func TestADatabaseDropRemovesOnlyTheGenerationsItNames(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	receiver := tc.nodes[1]
	core := receiver.server.core
	const source = "test-source"
	seq := sourcePosition(receiver, source)
	next := func() uint64 { seq++; return seq }
	createDB := func(database string, generation int64) ReplEntry {
		return ReplEntry{Seq: next(), Op: int32(transport.OpCreateDB), Payload: requestBody(t, CreateDatabaseRequest{Name: database, Generation: generation})}
	}
	dropDB := func(database string, generations ...int64) ReplEntry {
		return ReplEntry{Seq: next(), Op: int32(transport.OpDeleteDB), Payload: requestBody(t, DeleteDatabaseRequest{Name: database, Generations: generations})}
	}
	createBucket := func(database, bucket string, generation int64) ReplEntry {
		return ReplEntry{Seq: next(), Op: int32(transport.OpCreateBucket), Bucket: bucket,
			Payload: requestBody(t, CreateBucketRequest{Database: database, Name: bucket, Type: BucketTypeHash, DatabaseGeneration: generation})}
	}
	// Names the other node does not own, so the receiver applies them as a
	// replica and never as their owner.
	name := func(database, prefix string) string { return bucketOwnedIn(t, tc, 0, database, prefix) }

	t.Run("two generations", func(t *testing.T) {
		const database = "two"
		older, newer := name(database, "older"), name(database, "newer")
		pushToReceiver(t, receiver, source, []ReplEntry{
			createDB(database, 11), createDB(database, 12),
			createBucket(database, older, 11), createBucket(database, newer, 12),
			dropDB(database, 11),
		})
		if core.bucketStoreOpen(database, older) || !core.bucketStoreOpen(database, newer) || !listsDatabase(t, receiver, database) {
			t.Errorf("after a drop of generation 11: %s open %v, %s open %v, database listed %v; want false, true, true",
				older, core.bucketStoreOpen(database, older), newer, core.bucketStoreOpen(database, newer), listsDatabase(t, receiver, database))
		}
	})
	t.Run("not live", func(t *testing.T) {
		const database = "notlive"
		kept := name(database, "kept")
		pushToReceiver(t, receiver, source, []ReplEntry{createDB(database, 21), createBucket(database, kept, 21), dropDB(database, 99)})
		dropped, err := core.generationDropped(database, 99)
		if !core.bucketStoreOpen(database, kept) || !dropped || err != nil {
			t.Errorf("a drop of a generation not live here: %s open %v, generation 99 recorded dropped %v (%v); want true and true",
				kept, core.bucketStoreOpen(database, kept), dropped, err)
		}
	})
	t.Run("repeated create", func(t *testing.T) {
		const database = "repeat"
		pushToReceiver(t, receiver, source, []ReplEntry{createDB(database, 31), createDB(database, 31)})
		live, err := core.liveGenerations(database)
		if err != nil || !slices.Equal(live, []int64{31}) {
			t.Errorf("a create repeated: live generations %v (%v); want [31]", live, err)
		}
	})
	t.Run("unknown database", func(t *testing.T) {
		const database = "unknown"
		bucket := name(database, "first")
		pushToReceiver(t, receiver, source, []ReplEntry{createBucket(database, bucket, 41)})
		live, err := core.liveGenerations(database)
		if !core.bucketStoreOpen(database, bucket) || err != nil || !slices.Equal(live, []int64{41}) {
			t.Errorf("a bucket create in a database unknown here: open %v, live generations %v (%v); want true and [41]",
				core.bucketStoreOpen(database, bucket), live, err)
		}
	})
	t.Run("tombstoned", func(t *testing.T) {
		const database = "tombstoned"
		bucket := name(database, "late")
		pushToReceiver(t, receiver, source, []ReplEntry{dropDB(database, 51)})
		resp := pushRefused(t, receiver, source, []ReplEntry{createBucket(database, bucket, 51)})
		if core.bucketStoreOpen(database, bucket) || listsDatabase(t, receiver, database) {
			t.Errorf("a bucket create in a dropped generation created something: open %v, listed %v", core.bucketStoreOpen(database, bucket), listsDatabase(t, receiver, database))
		}
		if len(resp.DroppedGenerations) != 1 || !slices.Equal(resp.DroppedGenerations[0].Generations, []int64{51}) {
			t.Errorf("the refusal carries %+v; want the database's dropped generation 51", resp.DroppedGenerations)
		}
	})
	// G-6 (d) as the create-beside-copy fix narrowed it: a held copy is replaced when the create
	// carries its generation as dropped, and kept, with the create answered as
	// already existing, when nobody has dropped it. A refusal would stall the
	// sender's stream for ever (verifier D-2).
	t.Run("replaced", func(t *testing.T) {
		const database = "replaced"
		bucket := name(database, "same")
		pushToReceiver(t, receiver, source, []ReplEntry{createDB(database, 61), createBucket(database, bucket, 61)})
		pushToReceiver(t, receiver, source, []ReplEntry{{Seq: next(), Op: int32(transport.OpPut), Bucket: bucket,
			Payload: requestBody(t, InsertRequest{Database: database, Bucket: bucket, Key: "old", Data: map[string]interface{}{"n": 1}})}})
		recreateDB := createDB(database, 62)
		recreate := ReplEntry{Seq: next(), Op: int32(transport.OpCreateBucket), Bucket: bucket, Payload: requestBody(t, CreateBucketRequest{
			Database: database, Name: bucket, Type: BucketTypeHash, DatabaseGeneration: 62, DroppedGenerations: []int64{61},
		})}
		pushToReceiver(t, receiver, source, []ReplEntry{recreateDB, recreate})
		if generation := heldBucketGeneration(receiver, database, bucket); generation != 62 || holdsKeyIn(receiver, database, bucket, "old") {
			t.Errorf("a create of a held name in another generation: held in generation %d, old document there %v; want 62 and false",
				generation, holdsKeyIn(receiver, database, bucket, "old"))
		}
	})
	// Kept on a replica, and on the bucket's owner: a stream push reaches a node
	// that owns the bucket now, and the rule in decideGenerationOfCreate is its
	// only defence.
	for _, kept := range []struct {
		row, database string
		owner         int
	}{{"kept", "kept", 0}, {"kept at the owner", "keptowner", 1}} {
		t.Run(kept.row, func(t *testing.T) {
			bucket := bucketOwnedIn(t, tc, kept.owner, kept.database, "same")
			pushToReceiver(t, receiver, source, []ReplEntry{createDB(kept.database, 71), createBucket(kept.database, bucket, 71)})
			pushToReceiver(t, receiver, source, []ReplEntry{{Seq: next(), Op: int32(transport.OpPut), Bucket: bucket,
				Payload: requestBody(t, InsertRequest{Database: kept.database, Bucket: bucket, Key: "old", Data: map[string]interface{}{"n": 1}})}})
			pushToReceiver(t, receiver, source, []ReplEntry{createBucket(kept.database, bucket, 72)})
			if generation := heldBucketGeneration(receiver, kept.database, bucket); generation != 71 || !holdsKeyIn(receiver, kept.database, bucket, "old") {
				t.Errorf("a create in a generation nobody dropped replaced the live copy: held in generation %d, old document there %v; want 71 and true",
					generation, holdsKeyIn(receiver, kept.database, bucket, "old"))
			}
		})
	}
}

// TestADatabaseDDLEntryWithoutAGenerationIsPassedOverByAReceiver: database
// DDL logged before generations existed is acknowledged and never applied on
// a stream (G-8), and the receiver's position moves past it durably.
//
// Guard: legacy database DDL applied on a stream.
func TestADatabaseDDLEntryWithoutAGenerationIsPassedOverByAReceiver(t *testing.T) {
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	receiver := tc.nodes[1]
	const source = "test-source"
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "kept"})
	waitListedBy(t, tc, "kept", 1)
	base := sourcePosition(receiver, source)

	pushToReceiver(t, receiver, source, []ReplEntry{
		{Seq: base + 1, Op: int32(transport.OpCreateDB), Payload: requestBody(t, CreateDatabaseRequest{Name: "legacy"})},
		{Seq: base + 2, Op: int32(transport.OpDeleteDB), Payload: requestBody(t, DeleteDatabaseRequest{Name: "kept"})},
	})
	if listsDatabase(t, receiver, "legacy") || !listsDatabase(t, receiver, "kept") {
		t.Fatalf("legacy database DDL was applied from a stream: legacy listed %v, kept listed %v",
			listsDatabase(t, receiver, "legacy"), listsDatabase(t, receiver, "kept"))
	}
	if position := sourcePosition(receiver, source); position != base+2 {
		t.Fatalf("the receiver's position on the source is %d; want %d, past both entries", position, base+2)
	}
	snap := receiver.server.core.kv.NewSnapshot()
	encoded, err := snap.Get(positionKey(source))
	snap.Discard()
	if err != nil {
		t.Fatalf("the position moved past entries passed over is not checkpointed: %v", err)
	}
	var record positionRecord
	if err := bson.Unmarshal(encoded, &record); err != nil || record.AppliedThrough != base+2 {
		t.Fatalf("the checkpointed position is %d (%v); want %d", record.AppliedThrough, err, base+2)
	}
}

// TestADropWhoseCoordinatorIsLostStillReachesTheOwner is review F-4's first
// case by the refusal path alone: a drop reaches a replica of a bucket and not
// the bucket's owner, and its coordinator is lost. The re-logged drop is held
// back from the owner, so the owner learns the drop from the replica's refusal
// of the next write to the bucket: its stream to the replica streams again,
// it stops serving the dropped bucket, and its other buckets reach the
// replica.
//
// Guard: a refusal that carries no dropped generations. The owner keeps the
// bucket and its stream to the replica stays stalled.
func TestADropWhoseCoordinatorIsLostStillReachesTheOwner(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	coordinator, owner, replica := 0, 1, 2
	const other = "other"
	bucket := bucketOwnedIn(t, tc, owner, testDatabase, "dropped")
	elsewhere := bucketOwnedIn(t, tc, owner, other, "kept")

	tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: other})
	waitListedBy(t, tc, testDatabase, 0, 1, 2)
	waitListedBy(t, tc, other, 0, 1, 2)
	createBucketAt(tc, owner, testDatabase, bucket, 0)
	createBucketAt(tc, owner, other, elsewhere, 0)
	waitForServing(t, tc, testDatabase, bucket, 3)
	waitForServing(t, tc, other, elsewhere, 3)

	toOwner := holdPushes(peerOf(t, tc, coordinator, owner))
	defer toOwner.Release()
	relogToOwner := holdPushes(peerOf(t, tc, replica, owner))
	defer relogToOwner.Release()
	tc.ok(coordinator, transport.OpDeleteDB, DeleteDatabaseRequest{Name: testDatabase})
	pollUntil(t, "the replica applied the drop", func() bool { return !listsDatabase(t, tc.nodes[replica], testDatabase) })
	_ = stopNode(t, tc, coordinator)

	tc.frame(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "after-the-drop", Data: map[string]interface{}{"n": 1}, WriteConcern: "one"})
	toReplica := peerOf(t, tc, owner, replica)
	deadline := time.Now().Add(10 * time.Second)
	for {
		state, _ := toReplica.streamStatus()
		if state == streamStreaming && !tc.nodes[owner].server.core.bucketStoreOpen(testDatabase, bucket) {
			break
		}
		if time.Now().After(deadline) {
			_, reason := toReplica.streamStatus()
			t.Fatalf("ten seconds after the replica refused a write to %s the owner's stream to it is %s (%q) and the owner serves the bucket: %v",
				bucket, state, reason, tc.nodes[owner].server.core.bucketStoreOpen(testDatabase, bucket))
		}
		toReplica.wakeSender()
		time.Sleep(50 * time.Millisecond)
	}
	putIn(tc, owner, other, elsewhere, "after-the-refusal", "one")
	pollUntil(t, "the replica holds the owner's write to another bucket", func() bool {
		return holdsKeyIn(tc.nodes[replica], other, elsewhere, "after-the-refusal")
	})
}

// TestAPullCarriesTombstonesBothWays is G-11 (b), each direction with no other
// way for the drop to travel. A replica that applied a drop pulls a bucket of
// the dropped incarnation from its owner, which never received the drop: the
// owner learns it from the request and refuses the pull, holding the bucket no
// longer. And a node pulling a bucket of a later incarnation from a source
// that holds the drop learns it from the answer.
//
// Guard: no dropped generations on a pull (G-11 (b) off).
func TestAPullCarriesTombstonesBothWays(t *testing.T) {
	t.Run("request", func(t *testing.T) {
		tc := newTestCluster(t, 3, 3)
		defer tc.stop()
		coordinator, owner, replica := 0, 1, 2
		bucket := bucketOwnedIn(t, tc, owner, testDatabase, "pulled")
		tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
		waitListedBy(t, tc, testDatabase, 0, 1, 2)
		createBucketAt(tc, owner, testDatabase, bucket, 0)
		waitForServing(t, tc, testDatabase, bucket, 3)

		toOwner := holdPushes(peerOf(t, tc, coordinator, owner))
		defer toOwner.Release()
		relogToOwner := holdPushes(peerOf(t, tc, replica, owner))
		defer relogToOwner.Release()
		tc.ok(coordinator, transport.OpDeleteDB, DeleteDatabaseRequest{Name: testDatabase})
		pollUntil(t, "the replica applied the drop", func() bool { return !listsDatabase(t, tc.nodes[replica], testDatabase) })

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		err := clusterOf(tc, replica).pullBucket(ctx, testDatabase, bucket, clusterOf(tc, owner).self)
		if err == nil {
			t.Fatalf("the replica pulled %s, a bucket of the incarnation it dropped", bucket)
		}
		if tc.nodes[owner].server.core.bucketStoreOpen(testDatabase, bucket) || tc.nodes[replica].server.core.bucketStoreOpen(testDatabase, bucket) {
			t.Fatalf("after the pull (%v) the owner serves %s %v and the replica %v; want neither",
				err, bucket, tc.nodes[owner].server.core.bucketStoreOpen(testDatabase, bucket), tc.nodes[replica].server.core.bucketStoreOpen(testDatabase, bucket))
		}
	})
	t.Run("response", func(t *testing.T) {
		tc := newTestCluster(t, 2, 2)
		defer tc.stop()
		const database = "tenant"
		source, receiver := 0, 1
		before := bucketOwnedIn(t, tc, source, database, "before")
		after := bucketOwnedIn(t, tc, source, database, "after")

		hold := holdPushes(peerOf(t, tc, source, receiver))
		defer hold.Release()
		tc.ok(source, transport.OpCreateDB, CreateDatabaseRequest{Name: database})
		createBucketAtOne(tc, source, database, before)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := clusterOf(tc, receiver).pullBucket(ctx, database, before, clusterOf(tc, source).self); err != nil {
			t.Fatalf("pull %s: %v", before, err)
		}
		dropped, err := tc.nodes[source].server.core.liveGenerations(database)
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		tc.ok(source, transport.OpDeleteDB, DeleteDatabaseRequest{Name: database})
		tc.ok(source, transport.OpCreateDB, CreateDatabaseRequest{Name: database})
		createBucketAtOne(tc, source, database, after)
		if err := clusterOf(tc, receiver).pullBucket(ctx, database, after, clusterOf(tc, source).self); err != nil {
			t.Fatalf("pull %s: %v", after, err)
		}
		learned, err := tc.nodes[receiver].server.core.droppedGenerations(database)
		if err != nil || !slices.Equal(learned, dropped) {
			t.Fatalf("after pulling from a source that dropped %v the receiver holds dropped %v (%v)", dropped, learned, err)
		}
		if tc.nodes[receiver].server.core.bucketStoreOpen(database, before) {
			t.Fatalf("the receiver still serves %s, of the incarnation the source's answer says was dropped", before)
		}
	})
}

// TestACreateBucketInADroppedGenerationIsRefusedByAReplica is rev 3's A-1 (c):
// a bucket's owner that has not received a drop stamps a create with the
// dropped generation; its replicas hold the drop and refuse the create rather
// than acknowledge it. The client is told nothing was created, with the
// command that fixes it, and no node holds the bucket. The owner's inbound
// pushes are held and the re-logged and carried drops switched off, so the
// refusal is the only way the owner learns.
//
// Guard: G-6 (a) on a replica acknowledging the create. The create then
// succeeds at majority and the bucket is later dropped with its writes.
func TestACreateBucketInADroppedGenerationIsRefusedByAReplica(t *testing.T) {
	generationSeamsHook.Store(&generationSeams{noRelog: true, noCarried: true})
	defer generationSeamsHook.Store(nil)
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	coordinator, owner, replica := 0, 1, 2
	bucket := bucketOwnedIn(t, tc, owner, testDatabase, "late")

	tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, 0, 1, 2)
	toOwner := holdPushes(peerOf(t, tc, coordinator, owner))
	defer toOwner.Release()
	tc.ok(coordinator, transport.OpDeleteDB, DeleteDatabaseRequest{Name: testDatabase})
	pollUntil(t, "the replica applied the drop", func() bool { return !listsDatabase(t, tc.nodes[replica], testDatabase) })
	if !listsDatabase(t, tc.nodes[owner], testDatabase) {
		t.Fatal("setup: the owner received the drop")
	}

	answer := tc.frame(owner, transport.OpCreateBucket, bson.M{
		"database": testDatabase, "name": bucket, "type": string(BucketTypeHash), "write_concern": "majority",
	})
	reply := createRefusalOf(t, answer)
	if !strings.Contains(reply, "was dropped while bucket") || !strings.Contains(reply, "create database "+testDatabase) {
		t.Errorf("the refusal does not say the database was dropped and how to create it again: %q", reply)
	}
	for index, node := range tc.nodes {
		if node.server.core.bucketStoreOpen(testDatabase, bucket) {
			t.Errorf("node %d holds %s, created in a dropped incarnation", index+1, bucket)
		}
	}
	write := tc.frame(owner, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "k", Data: map[string]interface{}{"n": 1}})
	if write.Opcode != transport.OpError {
		t.Errorf("a write to %s after its create was refused was answered %s", bucket, write.Opcode)
	}
}

// TestDroppingOneLegacyDatabaseLeavesAnotherReplicating is review F-2 with
// rev 3's A-5 seam: two databases that predate generations, on every node,
// forced to derive one legacy generation. One is dropped; a bucket created in
// the other and written at all is on every node, a node pulls it, and the
// other database has no dropped generation anywhere.
//
// Guard: a dropped generation keyed by generation alone. With the forced
// equal value the first database's drop refuses the second's create and pull.
func TestDroppingOneLegacyDatabaseLeavesAnotherReplicating(t *testing.T) {
	const dropped, kept, forced = "legacy-a", "legacy-b", int64(555)
	derive := func(database string) (int64, bool) { return forced, database == dropped || database == kept }
	legacyGenerationHook.Store(&derive)
	defer legacyGenerationHook.Store(nil)

	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	// A store written before generations existed: a marker with none, and
	// nothing in the log. Each node is restarted on it.
	for index := range tc.nodes {
		core := tc.nodes[index].server.core
		batch := core.kv.NewBatch()
		for _, database := range []string{dropped, kept} {
			if err := core.putDatabaseMarker(batch, database, databaseMarker{CreatedAt: time.Now().UTC()}); err != nil {
				t.Fatalf("setup: %v", err)
			}
		}
		if err := batch.Commit(true); err != nil {
			t.Fatalf("setup: %v", err)
		}
		batch.Discard()
		tc.restartNode(index)
	}
	for _, database := range []string{dropped, kept} {
		waitListedBy(t, tc, database, 0, 1, 2)
	}

	tc.ok(0, transport.OpDeleteDB, DeleteDatabaseRequest{Name: dropped})
	pollUntil(t, "every node applied the drop", func() bool {
		for _, node := range tc.nodes {
			if listsDatabase(t, node, dropped) {
				return false
			}
		}
		return true
	})
	bucket := bucketOwnedIn(t, tc, 1, kept, "after")
	createBucketAt(tc, 1, kept, bucket, 0)
	putIn(tc, 1, kept, bucket, "written", "all")
	for index, node := range tc.nodes {
		if !holdsKeyIn(node, kept, bucket, "written") {
			t.Errorf("node %d does not hold the write to %s/%s", index+1, kept, bucket)
		}
		if tombstones, err := node.server.core.droppedGenerations(kept); err != nil || len(tombstones) != 0 {
			t.Errorf("node %d holds dropped generations %v of %s (%v); want none", index+1, tombstones, kept, err)
		}
	}

	// A copy pulled by a node that drops it first, as a fresh holder would.
	puller := tc.nodes[2].server.core
	if resp, err := puller.DeleteBucketCtx(context.Background(), DeleteBucketRequest{Database: kept, Name: bucket}); err != nil || !resp.Success {
		t.Fatalf("setup: drop the puller's copy: %+v %v", resp, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := puller.cluster.pullBucket(ctx, kept, bucket, clusterOf(tc, 1).self); err != nil {
		t.Fatalf("a pull of %s/%s after %s was dropped: %v", kept, bucket, dropped, err)
	}
	if !holdsKeyIn(tc.nodes[2], kept, bucket, "written") {
		t.Fatalf("the pulled copy of %s/%s lacks its write", kept, bucket)
	}
}
