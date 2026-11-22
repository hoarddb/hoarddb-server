package server

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The tests of database generations (the replication-database-ddl-design record
// §5). A database name is a sequence of incarnations; a drop names the
// incarnations it removes, every bucket records the one it was created in, and
// the dropped ones are remembered and travel. Each negative test names the
// rule it guards.

// TestAReplayedDropDatabaseFromAnotherCoordinatorDoesNotDeleteAPulledCopy is
// the verifier's probe (§5.1), made deterministic: the database's DDL goes
// through a member that is not the bucket's owner, that member's pushes to the
// newcomer are held and recorded from before the join, and the end waits until
// the member's stream to the newcomer is acknowledged past the drop. The
// newcomer's snapshot position is in the owner's stream, not in the drop's.
//
// Guard: G-4's generation filter. A drop that removes every bucket of the
// database deletes the pulled copy, which is of a later incarnation.
func TestAReplayedDropDatabaseFromAnotherCoordinatorDoesNotDeleteAPulledCopy(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	bucket, owner := bucketKeptByItsOwnerAndPlacedOn(t, tc, newcomer)
	coordinator := (owner + 1) % 3
	coordinatorCluster := clusterOf(tc, coordinator)
	ownerCluster := clusterOf(tc, owner)

	tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, 0, 1, 2)
	createBucketAt(tc, owner, testDatabase, bucket, 0)
	waitForServing(t, tc, testDatabase, bucket, 3)
	putDocumentAt(tc, owner, testDatabase, bucket, "gen1", "all")
	tc.ok(coordinator, transport.OpDeleteDB, DeleteDatabaseRequest{Name: testDatabase})
	waitForServing(t, tc, testDatabase, bucket, 0)
	tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, 0, 1, 2)
	createBucketAt(tc, owner, testDatabase, bucket, 0)
	waitForServing(t, tc, testDatabase, bucket, 3)
	keys := make([]string, 0, 20)
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("gen2-%02d", i)
		keys = append(keys, key)
		putDocumentAt(tc, owner, testDatabase, bucket, key, "all")
	}

	toNewcomer := coordinatorCluster.peerFor(newcomer)
	hold := holdPushes(toNewcomer)
	defer hold.Release()
	// The owner's stream is held until the pull has landed, so the copy is
	// the pull's and not a replay of the bucket's history.
	ownerHold := holdPushes(ownerCluster.peerFor(newcomer))
	defer ownerHold.Release()
	joiner := joinNewcomer(t, tc, owner, ports)
	pulledFromOwner(t, joiner, ownerCluster, testDatabase, bucket)
	ownerHold.Release()
	pulledID := heldBucketID(joiner.node, testDatabase, bucket)
	for _, key := range keys {
		if !holdsKeyLocallyIn(joiner.node, bucket, key) {
			t.Fatalf("setup: the pulled copy lacks %s", key)
		}
	}
	if !eventually(func() bool {
		toNewcomer.wakeSender()
		_, held := hold.heldEntry(transport.OpDeleteDB, testDatabase)
		return held
	}) {
		t.Fatal("setup: the drop was not held")
	}
	drop, _ := hold.heldEntry(transport.OpDeleteDB, testDatabase)
	requireUndelivered(t, toNewcomer, drop.Seq, "the drop")

	hold.Release()
	waitAcked(t, toNewcomer, drop.Seq)
	missing := 0
	for _, key := range keys {
		if !holdsKeyLocallyIn(joiner.node, bucket, key) {
			missing++
		}
	}
	if id := heldBucketID(joiner.node, testDatabase, bucket); missing > 0 || id != pulledID {
		t.Fatalf("the coordinator's replayed drop (its sequence %d) deleted the joiner's pulled copy of a later incarnation: bucket id %d (the pull's was %d), missing %d of %d keys",
			drop.Seq, id, pulledID, missing, len(keys))
	}
}

// TestARecreatedBucketOfTheSameNameIsNotMergedIntoTheDroppedCopy is review
// F-1: a replica that missed a drop still holds the dropped incarnation's
// bucket when the bucket's owner creates the same name in the database's next
// incarnation. The held copy is replaced, so the dropped document is never
// served inside the new incarnation and the late drop finds nothing of its own
// to remove.
//
// Guard: G-6 (d). Answering "already exists" for a held copy of another
// generation merges the new incarnation's writes into the dropped copy, which
// the late drop then deletes.
func TestARecreatedBucketOfTheSameNameIsNotMergedIntoTheDroppedCopy(t *testing.T) {
	// The owner, taught the drop by its coordinator, would log it again and
	// ship it to the lagging replica (G-12), and the owner's create of the
	// bucket carries the drop (SA-DG-14): either way the replica would hold no
	// copy of the name when the re-create reaches it, and G-6 (d) could not
	// fail. Both are switched off.
	generationSeamsHook.Store(&generationSeams{noRelog: true, noCarried: true})
	defer generationSeamsHook.Store(nil)
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	owner := ownerIndexOf(t, tc, testBucket)
	coordinator, lagging := (owner+1)%3, (owner+2)%3

	tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, 0, 1, 2)
	createBucketAt(tc, owner, testDatabase, testBucket, 0)
	waitForServing(t, tc, testDatabase, testBucket, 3)
	putDocumentAt(tc, owner, testDatabase, testBucket, "gen1", "all")

	hold := holdPushes(peerOf(t, tc, coordinator, lagging))
	defer hold.Release()
	tc.ok(coordinator, transport.OpDeleteDB, DeleteDatabaseRequest{Name: testDatabase})
	pollUntil(t, "the owner and the coordinator no longer serve the bucket", func() bool {
		return !tc.nodes[owner].server.core.bucketStoreOpen(testDatabase, testBucket) &&
			!tc.nodes[coordinator].server.core.bucketStoreOpen(testDatabase, testBucket)
	})
	tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, owner)
	createBucketAt(tc, owner, testDatabase, testBucket, 0)
	keys := make([]string, 0, 20)
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("gen2-%02d", i)
		keys = append(keys, key)
		putDocumentAt(tc, owner, testDatabase, testBucket, key, "all")
	}

	// The owner's stream to the lagging replica is acknowledged past the
	// create: that is how the test knows the create reached it (rev 3, A-5).
	create := lastLoggedSeq(t, tc, owner, transport.OpCreateBucket, testBucket)
	waitAcked(t, peerOf(t, tc, owner, lagging), create)
	if holdsKeyLocallyIn(tc.nodes[lagging], testBucket, "gen1") {
		t.Fatalf("the lagging replica serves the dropped incarnation's document gen1 inside the new incarnation")
	}

	toLagging := peerOf(t, tc, coordinator, lagging)
	drop := loggedDatabaseDDL(t, tc, coordinator, transport.OpDeleteDB, testDatabase)
	requireUndelivered(t, toLagging, drop, "the drop")
	hold.Release()
	waitAcked(t, toLagging, drop)
	missing := 0
	for _, key := range keys {
		if !holdsKeyLocallyIn(tc.nodes[lagging], testBucket, key) {
			missing++
		}
	}
	if missing > 0 {
		t.Fatalf("after the late drop the lagging replica is missing %d of %d keys acknowledged at all", missing, len(keys))
	}
}

// TestADropDatabaseBetweenTwoSnapshotsRemovesOnlyThePreDropCopy: one source,
// two pulls of two buckets of one database, a drop and a re-create between
// them. The drop is replayed after both landed. It removes the bucket pulled
// before it and spares the one pulled after it, in whichever order the three
// arrive (§1.3).
//
// Guard: no rule by snapshot position. Passing the drop over below the
// database's highest snapshot position (the rework's seededCoversDatabaseDDL)
// keeps the bucket the source dropped.
func TestADropDatabaseBetweenTwoSnapshotsRemovesOnlyThePreDropCopy(t *testing.T) {
	// The second pull's answer carries the source's dropped generations
	// (G-11 (b)), which would drop the first copy before the replayed drop
	// arrives and leave the receiver's rule for that drop untested.
	generationSeamsHook.Store(&generationSeams{noPullExchange: true})
	defer generationSeamsHook.Store(nil)
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	const database = "tenant"
	source, receiver := 0, 1
	first := bucketOwnedIn(t, tc, source, database, "first")
	second := bucketOwnedIn(t, tc, source, database, "second")
	sourceCluster := clusterOf(tc, source)
	receiverCluster := clusterOf(tc, receiver)

	hold := holdPushes(peerOf(t, tc, source, receiver))
	defer hold.Release()
	tc.ok(source, transport.OpCreateDB, CreateDatabaseRequest{Name: database})
	createBucketAtOne(tc, source, database, first)
	putIn(tc, source, database, first, "before-the-drop", "one")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := receiverCluster.pullBucket(ctx, database, first, sourceCluster.self); err != nil {
		t.Fatalf("pull %s: %v", first, err)
	}

	tc.ok(source, transport.OpDeleteDB, DeleteDatabaseRequest{Name: database})
	tc.ok(source, transport.OpCreateDB, CreateDatabaseRequest{Name: database})
	createBucketAtOne(tc, source, database, second)
	keys := make([]string, 0, 20)
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("after-%02d", i)
		keys = append(keys, key)
		putIn(tc, source, database, second, key, "one")
	}
	if err := receiverCluster.pullBucket(ctx, database, second, sourceCluster.self); err != nil {
		t.Fatalf("pull %s: %v", second, err)
	}
	toReceiver := peerOf(t, tc, source, receiver)
	requireUndelivered(t, toReceiver, loggedDatabaseDDL(t, tc, source, transport.OpDeleteDB, database), "the drop")

	hold.Release()
	waitAckedEverywhere(t, tc, source)
	if tc.nodes[receiver].server.core.bucketStoreOpen(database, first) {
		t.Errorf("the receiver still serves %s, pulled before the drop that removed it from its source", first)
	}
	missing := 0
	for _, key := range keys {
		if !holdsKeyIn(tc.nodes[receiver], database, second, key) {
			missing++
		}
	}
	if missing > 0 {
		t.Errorf("the receiver's copy of %s, pulled after the drop, is missing %d of %d keys", second, missing, len(keys))
	}
}

// TestALateDropDatabaseOnALaggingMemberSparesABucketCreatedAfterIt is §1.5
// with a new name: a member's stream from the drop's coordinator lags; the
// database is re-created and a bucket created in it, written at all; the late
// drop then arrives. It removes the dropped incarnation and nothing else, and
// the owner's stream to the member is undisturbed.
//
// Guard: G-4's generation filter.
func TestALateDropDatabaseOnALaggingMemberSparesABucketCreatedAfterIt(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	owner := ownerIndexOf(t, tc, testBucket)
	coordinator, lagging := (owner+1)%3, (owner+2)%3

	tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, 0, 1, 2)
	hold := holdPushes(peerOf(t, tc, coordinator, lagging))
	defer hold.Release()
	tc.ok(coordinator, transport.OpDeleteDB, DeleteDatabaseRequest{Name: testDatabase})
	pollUntil(t, "the owner applied the drop", func() bool { return !listsDatabase(t, tc.nodes[owner], testDatabase) })
	tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, owner)
	createBucketAt(tc, owner, testDatabase, testBucket, 0)
	keys := make([]string, 0, 20)
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("after-%02d", i)
		keys = append(keys, key)
		putDocumentAt(tc, owner, testDatabase, testBucket, key, "all")
	}

	toLagging := peerOf(t, tc, coordinator, lagging)
	drop := loggedDatabaseDDL(t, tc, coordinator, transport.OpDeleteDB, testDatabase)
	requireUndelivered(t, toLagging, drop, "the drop")
	hold.Release()
	waitAcked(t, toLagging, drop)
	missing := 0
	for _, key := range keys {
		if !holdsKeyLocallyIn(tc.nodes[lagging], testBucket, key) {
			missing++
		}
	}
	if missing > 0 {
		t.Fatalf("the late drop deleted the lagging member's copy of a bucket created after it: missing %d of %d keys acknowledged at all", missing, len(keys))
	}
	putDocumentAt(tc, owner, testDatabase, testBucket, "after-the-late-drop", "all")
}

// TestADropThroughANodeThatDoesNotKnowTheDatabaseDropsNothing is §1.6's first
// bullet (review F-8: no bucket, so the node learns the database from nothing
// but its create). A drop through a node that has not received the database's
// create answers "no database" and logs nothing; every node then lists it.
//
// Guard: G-3. A drop logged with an empty set of generations reaches every
// other member and removes the database there.
func TestADropThroughANodeThatDoesNotKnowTheDatabaseDropsNothing(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	coordinator, unaware := 0, 1

	hold := holdPushes(peerOf(t, tc, coordinator, unaware))
	defer hold.Release()
	tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, 0, 2)
	answer := tc.frame(unaware, transport.OpDeleteDB, DeleteDatabaseRequest{Name: testDatabase})
	if !strings.Contains(string(answer.Payload), "not found") {
		t.Fatalf("setup: the unaware node answered the drop with %s %q; want a not-found refusal", answer.Opcode, answer.Payload)
	}

	toUnaware := peerOf(t, tc, coordinator, unaware)
	create := loggedDatabaseDDL(t, tc, coordinator, transport.OpCreateDB, testDatabase)
	requireUndelivered(t, toUnaware, create, "the create")
	hold.Release()
	waitAcked(t, toUnaware, create)
	waitAckedEverywhere(t, tc, unaware)
	for index, node := range tc.nodes {
		if !listsDatabase(t, node, testDatabase) {
			t.Errorf("node %d does not list %q after a drop through a node that did not know it", index+1, testDatabase)
		}
	}
}

// TestALateCreateDatabaseDoesNotResurrectADroppedOne: a create that reaches a
// member after the drop of its incarnation is ignored there. The member holds
// nothing of the database when the drop arrives, so the drop writes only its
// tombstones; the create's coordinator learns of the drop only later.
//
// Guards: G-2's tombstone check (the late create brings the database back on
// the member) and G-4's tombstone write on a node without the database (placed
// after DeleteDatabaseCtx's "no database" return, it writes nothing there).
func TestALateCreateDatabaseDoesNotResurrectADroppedOne(t *testing.T) {
	// The late member's answer to the create carries the drop back to the
	// creator (G-11 a), which would log it again and ship it (G-12) before
	// the test looks: the late member would drop the database a second time
	// and G-2's check could not fail. G-12 is switched off.
	generationSeamsHook.Store(&generationSeams{noRelog: true})
	defer generationSeamsHook.Store(nil)
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	creator, dropper, late := 0, 1, 2

	toLate := holdPushes(peerOf(t, tc, creator, late))
	defer toLate.Release()
	dropToCreator := holdPushes(peerOf(t, tc, dropper, creator))
	defer dropToCreator.Release()
	lateToCreator := holdPushes(peerOf(t, tc, late, creator))
	defer lateToCreator.Release()

	tc.ok(creator, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, dropper)
	tc.ok(dropper, transport.OpDeleteDB, DeleteDatabaseRequest{Name: testDatabase})
	waitDelivered(t, tc, dropper, peerOf(t, tc, dropper, late))

	creatorToLate := peerOf(t, tc, creator, late)
	create := loggedDatabaseDDL(t, tc, creator, transport.OpCreateDB, testDatabase)
	requireUndelivered(t, creatorToLate, create, "the create")
	toLate.Release()
	waitAcked(t, creatorToLate, create)
	if listsDatabase(t, tc.nodes[late], testDatabase) {
		t.Fatalf("a create that arrived after the drop of its incarnation brought %q back on node %d", testDatabase, late+1)
	}

	dropToCreator.Release()
	lateToCreator.Release()
	waitAckedEverywhere(t, tc, dropper)
	waitAckedEverywhere(t, tc, late)
	for index, node := range tc.nodes {
		if listsDatabase(t, node, testDatabase) {
			t.Errorf("node %d lists %q after every delivery of its create and its drop", index+1, testDatabase)
		}
	}
}

// TestAReplayDoesNotShipACreateOfATombstonedIncarnation: a database created
// through one member and dropped through another, whose log is then pruned
// past the drop. A joiner replays the creator's log and must not list the
// database.
//
// Guard: G-10's row in Cluster.deliverTo. A create of a generation the sender
// holds tombstoned is addressed to nobody.
func TestAReplayDoesNotShipACreateOfATombstonedIncarnation(t *testing.T) {
	// The creator, taught the drop, would log it again after its create
	// (G-12), and a replay of its log would then bring the joiner the drop
	// too, whatever G-10 did.
	generationSeamsHook.Store(&generationSeams{noRelog: true})
	defer generationSeamsHook.Store(nil)
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	const database = "tenant"
	creator, dropper := 0, 1

	tc.ok(dropper, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, 0, 1, 2)
	pruned := bucketOwnedIn(t, tc, dropper, testDatabase, "prune")
	createBucketAt(tc, dropper, testDatabase, pruned, 0)
	waitForServing(t, tc, testDatabase, pruned, 3)

	tc.ok(creator, transport.OpCreateDB, CreateDatabaseRequest{Name: database})
	waitListedBy(t, tc, database, dropper)
	tc.ok(dropper, transport.OpDeleteDB, DeleteDatabaseRequest{Name: database})
	pollUntil(t, "no member lists the dropped database", func() bool {
		for _, node := range tc.nodes {
			if listsDatabase(t, node, database) {
				return false
			}
		}
		return true
	})
	pruneLogPast(t, tc, dropper, pruned)
	creatorLog, err := tc.nodes[creator].server.core.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading the creator's log: %v", err)
	}
	retained := false
	for _, entry := range creatorLog {
		if _, named := decodeFrameMeta(entry.Data).scope(transport.OpCreateDB); transport.Opcode(entry.Op) == transport.OpCreateDB && named == database {
			retained = true
		}
	}
	if !retained {
		t.Fatal("setup: the creator's log no longer holds the create; the test needs it retained")
	}

	ports := reserveNodePorts(t)
	joiner := joinNewcomer(t, tc, creator, ports)
	for index := range tc.nodes {
		waitDelivered(t, tc, index, clusterOf(tc, index).peerFor(ports.ringNode()))
	}
	if listsDatabase(t, joiner.node, database) {
		t.Fatalf("the joiner lists %q: a replay shipped the create of an incarnation its sender had dropped", database)
	}
	for index, node := range tc.nodes {
		if listsDatabase(t, node, database) {
			t.Errorf("node %d lists the dropped database %q", index+1, database)
		}
	}
}

// TestAPartiallyDeliveredDropReachesEveryMember is rev 3's A-2: a drop that
// reached one member only, before its coordinator was lost, reaches every
// member, the database idle (no write after the drop gives anyone a refusal to
// learn from).
//
// Guard: G-12. A learned tombstone that is not re-logged stays on the one
// member that learned it.
func TestAPartiallyDeliveredDropReachesEveryMember(t *testing.T) {
	tc := newTestCluster(t, 4, 3)
	defer tc.stop()
	coordinator, owner, reached := 0, 1, 2
	bucket := bucketOwnedIn(t, tc, owner, testDatabase, "idle")

	tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, 0, 1, 2, 3)
	createBucketAt(tc, owner, testDatabase, bucket, 0)
	waitForServing(t, tc, testDatabase, bucket, 3)
	putIn(tc, owner, testDatabase, bucket, "before-the-drop", "all")

	var holds []*pushHold
	for index := range tc.nodes {
		if index != coordinator && index != reached {
			holds = append(holds, holdPushes(peerOf(t, tc, coordinator, index)))
		}
	}
	defer func() {
		for _, hold := range holds {
			hold.Release()
		}
	}()
	tc.ok(coordinator, transport.OpDeleteDB, DeleteDatabaseRequest{Name: testDatabase})
	pollUntil(t, "the one member the drop reached applied it", func() bool { return !listsDatabase(t, tc.nodes[reached], testDatabase) })
	_ = stopNode(t, tc, coordinator)

	deadline := time.Now().Add(10 * time.Second)
	for {
		var stillListing, stillServing []int
		for index, node := range tc.nodes {
			if index == coordinator {
				continue
			}
			if listsDatabase(t, node, testDatabase) {
				stillListing = append(stillListing, index+1)
			}
			if node.server.core.bucketStoreOpen(testDatabase, bucket) {
				stillServing = append(stillServing, index+1)
			}
		}
		if len(stillListing) == 0 && len(stillServing) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("ten seconds after a drop reached one member and its coordinator was lost, nodes %v still list %q and nodes %v still serve %s",
				stillListing, testDatabase, stillServing, bucket)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestACreateBucketStampedWithAGenerationDroppedBeforeItsApplyIsRefused is
// review F-6: a bucket create stamped with the database's live generation is
// held between the stamp and the owner's apply while a drop of that
// generation is applied on the owner. The client is told nothing was created,
// with the command that fixes it, and no node holds the bucket.
//
// Guard: G-6 on the owner's own apply. Without it the owner creates the
// bucket in a dropped incarnation, and its replicas with it.
func TestACreateBucketStampedWithAGenerationDroppedBeforeItsApplyIsRefused(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	owner := ownerIndexOf(t, tc, testBucket)
	other := (owner + 1) % 3

	tc.ok(other, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, 0, 1, 2)

	reached := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	hook := func(op transport.Opcode, database, bucket string) {
		if op != transport.OpCreateBucket || database != testDatabase || bucket != testBucket {
			return
		}
		once.Do(func() {
			close(reached)
			<-release
		})
	}
	afterJoinFenceHook.Store(&hook)
	defer afterJoinFenceHook.Store(nil)

	payload, err := bson.Marshal(CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	if err != nil {
		t.Fatalf("marshal the create: %v", err)
	}
	type answer struct {
		frame *transport.Frame
		err   error
	}
	answered := make(chan answer, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		frame, err := tc.nodes[owner].client.SendRequest(ctx, transport.OpCreateBucket, payload)
		answered <- answer{frame: frame, err: err}
	}()
	select {
	case <-reached:
	case <-time.After(20 * time.Second):
		t.Fatal("setup: the create never reached the hook")
	}
	tc.ok(other, transport.OpDeleteDB, DeleteDatabaseRequest{Name: testDatabase})
	pollUntil(t, "the owner applied the drop", func() bool { return !listsDatabase(t, tc.nodes[owner], testDatabase) })
	close(release)

	got := <-answered
	if got.err != nil {
		t.Fatalf("the create's request failed: %v", got.err)
	}
	reply := createRefusalOf(t, got.frame)
	if !strings.Contains(reply, "was dropped while bucket") || !strings.Contains(reply, "create database "+testDatabase) {
		t.Errorf("the create was not refused with the reason and the command that fixes it: %q", reply)
	}
	waitAckedEverywhere(t, tc, owner)
	for index, node := range tc.nodes {
		if node.server.core.bucketStoreOpen(testDatabase, testBucket) {
			t.Errorf("node %d holds %s/%s, created in an incarnation dropped before the owner applied the create", index+1, testDatabase, testBucket)
		}
		if listsDatabase(t, node, testDatabase) {
			t.Errorf("node %d lists %q, which the drop removed", index+1, testDatabase)
		}
	}
}

// TestAJoinerThatPulledNothingEndsWithTheClustersDatabaseSet: two members
// create and drop two databases through each other; a joiner that holds no
// bucket ends with the databases every member lists, whichever member's
// stream reaches it first (here the one whose order would mislead it).
//
// Guard: G-2's tombstone check.
func TestAJoinerThatPulledNothingEndsWithTheClustersDatabaseSet(t *testing.T) {
	// Each member re-logs the drops it learns from the other (G-12) and a
	// re-create carries them (G-2): with either on, every member's log ends
	// with the drop after the create it drops, and G-2's check could not
	// fail. Both are switched off; the result must not depend on them.
	generationSeamsHook.Store(&generationSeams{noRelog: true, noCarried: true})
	defer generationSeamsHook.Store(nil)
	tc := newTestCluster(t, 2, 2)
	defer tc.stop()
	first, second := 0, 1
	const kept, dropped = "kept", "gone"

	tc.ok(first, transport.OpCreateDB, CreateDatabaseRequest{Name: kept})
	waitListedBy(t, tc, kept, second)
	tc.ok(second, transport.OpDeleteDB, DeleteDatabaseRequest{Name: kept})
	pollUntil(t, "the first member applied the drop", func() bool { return !listsDatabase(t, tc.nodes[first], kept) })
	tc.ok(first, transport.OpCreateDB, CreateDatabaseRequest{Name: kept})
	waitListedBy(t, tc, kept, second)
	tc.ok(second, transport.OpCreateDB, CreateDatabaseRequest{Name: dropped})
	waitListedBy(t, tc, dropped, first)
	tc.ok(first, transport.OpDeleteDB, DeleteDatabaseRequest{Name: dropped})
	pollUntil(t, "the second member applied the drop", func() bool { return !listsDatabase(t, tc.nodes[second], dropped) })
	waitAckedEverywhere(t, tc, first)
	waitAckedEverywhere(t, tc, second)

	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	secondHold := holdPushes(clusterOf(tc, second).peerFor(newcomer))
	defer secondHold.Release()
	joiner := joinNewcomer(t, tc, first, ports)
	waitDelivered(t, tc, first, clusterOf(tc, first).peerFor(newcomer))
	secondHold.Release()
	waitDelivered(t, tc, second, clusterOf(tc, second).peerFor(newcomer))

	for index, node := range append(append([]*testNode{}, tc.nodes...), joiner.node) {
		if !listsDatabase(t, node, kept) || listsDatabase(t, node, dropped) {
			t.Errorf("node %d lists %q %v and %q %v; every member lists exactly %q",
				index+1, kept, listsDatabase(t, node, kept), dropped, listsDatabase(t, node, dropped), kept)
		}
	}
}

// TestAPulledCopySurvivesRestartsBetweenThePullAndTheReplay is §5.1's scenario
// with both ends restarted: the coordinator after the newcomer's pull, and the
// newcomer after it applied the replayed drop and before the re-create
// reaches it. Nothing the outcome depends on is in memory: generations are in
// the logged payloads, the catalog and the marker, and dropped generations in
// the store.
//
// Guards: G-4's generation filter, and the durability of what a node knows
// to be dropped.
func TestAPulledCopySurvivesRestartsBetweenThePullAndTheReplay(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()

	ports := reserveNodePorts(t)
	newcomer := ports.ringNode()
	bucket, owner := bucketKeptByItsOwnerAndPlacedOn(t, tc, newcomer)
	coordinator := (owner + 1) % 3
	ownerCluster := clusterOf(tc, owner)

	tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, 0, 1, 2)
	createBucketAt(tc, owner, testDatabase, bucket, 0)
	waitForServing(t, tc, testDatabase, bucket, 3)
	putDocumentAt(tc, owner, testDatabase, bucket, "gen1", "all")
	tc.ok(coordinator, transport.OpDeleteDB, DeleteDatabaseRequest{Name: testDatabase})
	waitForServing(t, tc, testDatabase, bucket, 0)
	tc.ok(coordinator, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitListedBy(t, tc, testDatabase, 0, 1, 2)
	createBucketAt(tc, owner, testDatabase, bucket, 0)
	waitForServing(t, tc, testDatabase, bucket, 3)
	keys := make([]string, 0, 20)
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("gen2-%02d", i)
		keys = append(keys, key)
		putDocumentAt(tc, owner, testDatabase, bucket, key, "all")
	}
	drop := loggedDatabaseDDL(t, tc, coordinator, transport.OpDeleteDB, testDatabase)

	hold := holdPushes(clusterOf(tc, coordinator).peerFor(newcomer))
	defer hold.Release()
	ownerHold := holdPushes(ownerCluster.peerFor(newcomer))
	defer ownerHold.Release()
	joiner := joinNewcomer(t, tc, owner, ports)
	pulledFromOwner(t, joiner, ownerCluster, testDatabase, bucket)
	ownerHold.Release()
	pulledID := heldBucketID(joiner.node, testDatabase, bucket)
	requireUndelivered(t, clusterOf(tc, coordinator).peerFor(newcomer), drop, "the drop")

	// The restarted coordinator delivers its log to the newcomer through the
	// drop and holds the re-create behind it.
	recreate := make(chan struct{})
	var releaseRecreate sync.Once
	defer releaseRecreate.Do(func() { close(recreate) })
	tc.tune = func(index int, c *Cluster) {
		if index != coordinator {
			return
		}
		wrapPeerConn(c.peerFor(newcomer), func(inner peerConn) peerConn {
			return &splitHoldConn{peerConn: inner, upTo: drop, released: recreate}
		})
	}
	tc.restartNode(coordinator)
	waitAcked(t, clusterOf(tc, coordinator).peerFor(newcomer), drop)
	if id := heldBucketID(joiner.node, testDatabase, bucket); id != pulledID {
		t.Fatalf("the replayed drop at sequence %d deleted the joiner's pulled copy of a later incarnation (bucket id %d, now %d)", drop, pulledID, id)
	}

	joiner.restart(t)
	releaseRecreate.Do(func() { close(recreate) })
	waitDelivered(t, tc, coordinator, clusterOf(tc, coordinator).peerFor(newcomer))
	waitDelivered(t, tc, owner, ownerCluster.peerFor(newcomer))
	missing := 0
	for _, key := range keys {
		if !holdsKeyLocallyIn(joiner.node, bucket, key) {
			missing++
		}
	}
	if id := heldBucketID(joiner.node, testDatabase, bucket); missing > 0 || id != pulledID {
		t.Fatalf("after both restarts and the re-create the joiner holds bucket id %d (the pull's was %d) missing %d of %d keys",
			id, pulledID, missing, len(keys))
	}

	// What the joiner knows to be dropped survived its restart: a create of
	// the dropped incarnation arriving now, from anywhere, is ignored.
	var logged DeleteDatabaseRequest
	if err := bson.Unmarshal(loggedPayload(t, tc, coordinator, transport.OpDeleteDB, testDatabase), &logged); err != nil || len(logged.Generations) == 0 {
		t.Fatalf("decode the coordinator's drop: %+v %v", logged, err)
	}
	const source = "test-source"
	pushToReceiver(t, joiner.node, source, []ReplEntry{{Seq: sourcePosition(joiner.node, source) + 1, Op: int32(transport.OpCreateDB),
		Payload: requestBody(t, CreateDatabaseRequest{Name: testDatabase, Generation: logged.Generations[0]})}})
	live, err := joiner.node.server.core.liveGenerations(testDatabase)
	if err != nil || slices.Contains(live, logged.Generations[0]) {
		t.Fatalf("after its restart the joiner took a create of the dropped generation %d back: live generations %v (%v)",
			logged.Generations[0], live, err)
	}
}
