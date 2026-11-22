package server

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// A merge restore sent to a joining owner before its pull of the
// bucket succeeded, and afterwards no node held the bucket's documents. The
// joiner had not learned the database, so the restore created it again — a new
// incarnation — and then created the bucket in that incarnation as its owner;
// each holder, told by the owner that the name was made afresh, dropped its copy
// (decideGenerationOfCreate, rule G-6 (d)). A `create bucket` at the joiner did
// the same.

// generationsOn returns each member's live generations of database.
func generationsOn(t *testing.T, nodes []*testNode, database string) [][]int64 {
	t.Helper()
	live := make([][]int64, 0, len(nodes))
	for i, node := range nodes {
		generations, err := node.server.core.liveGenerations(database)
		if err != nil {
			t.Fatalf("reading node %d's generations of %s: %v", i+1, database, err)
		}
		live = append(live, generations)
	}
	return live
}

// sameGenerations fails the test unless every member still holds exactly the
// incarnations of database it held before.
func sameGenerations(t *testing.T, what string, nodes []*testNode, database string, before [][]int64) {
	t.Helper()
	after := generationsOn(t, nodes, database)
	for i := range nodes {
		if !slices.Equal(before[i], after[i]) {
			t.Errorf("%s started a new incarnation of %s: node %d held %v and now holds %v", what, database, i+1, before[i], after[i])
		}
	}
}

// bucketGeneration is the incarnation node's copy of bucket was created in, or
// 0 when it holds none.
func bucketGeneration(node *testNode, database, bucket string) int64 {
	db := node.server.core.serverDataAccessor().getDB(database)
	if db == nil {
		return 0
	}
	held := db.getBucket(bucket)
	if held == nil {
		return 0
	}
	return held.generation.Load()
}

// pullAndCheckEveryoneHolds lands the joiner's copy of bucket and fails the test
// unless every node, the joiner too, holds the document written before the join.
func pullAndCheckEveryoneHolds(t *testing.T, s joinerReadingUnpulledBuckets, bucket string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := s.joiner.ensureMigrated(ctx, testDatabase, bucket); err != nil {
		t.Fatalf("the joiner's pull of %s: %v", bucket, err)
	}
	for i, node := range append(append([]*testNode{}, s.tc.nodes...), s.joinerNode) {
		if !holdsKeyIn(node, testDatabase, bucket, "held") {
			t.Errorf("node %d no longer holds the document written to %s before the join", i+1, bucket)
		}
	}
	get := s.tc.frame(0, transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: "held"})
	var found FindResponse
	if err := bson.Unmarshal(get.Payload, &found); err != nil || !found.Found {
		t.Errorf("a get of %s through a member does not find the document written before the join (err %v): %s", bucket, err, get.Payload)
	}
}

// TestAMergeRestoreAtAJoinerBeforeItsPullLosesNothing is the
// reproduction. The joiner owns a bucket its two holders hold, and no pull of it
// has started. A merge restore started at the joiner is refused with retry and
// creates nothing; the pull then lands the holders' documents everywhere, and
// the restore, retried, merges into the bucket.
func TestAMergeRestoreAtAJoinerBeforeItsPullLosesNothing(t *testing.T) {
	// The line of defence under test is behind the join freeze, which would refuse
	// this schema change during the join first (join-freeze build doc, trap 5).
	switchJoinFreezeOff(t)
	s := setUpJoinerReadingUnpulledBuckets(t)
	before := generationsOn(t, s.tc.nodes, testDatabase)
	start := RestoreStartRequest{
		Database: testDatabase, Bucket: s.restoredBucket, FormatVersion: dumpManifestFormatVersion, StoreType: BucketTypeHash, Bytes: 1,
	}

	refusedAwaitingCopy(t, "a merge restore start at an owner that has not copied the bucket", s.tc.frameOn(s.joinerNode, transport.OpRestoreStart, start))
	if s.joiner.s.bucketStoreOpen(testDatabase, s.restoredBucket) {
		t.Errorf("the refused restore start created %s at the joiner", s.restoredBucket)
	}
	sameGenerations(t, "a refused restore start", s.tc.nodes, testDatabase, before)

	pullAndCheckEveryoneHolds(t, s, s.restoredBucket)

	retried := s.tc.frameOn(s.joinerNode, transport.OpRestoreStart, start)
	answered(t, "the restore start retried once the copy landed", retried)
	var resp RestoreStartResponse
	if err := bson.Unmarshal(retried.Payload, &resp); err != nil {
		t.Fatalf("decoding the retried restore start: %v", err)
	}
	if resp.Created {
		t.Errorf("the retried restore start created %s again instead of merging into it", s.restoredBucket)
	}
	sameGenerations(t, "the retried restore start", s.tc.nodes, testDatabase, before)
}

// TestACreateBucketAtAJoinerOfABucketItHasNotCopiedLosesNothing is the same
// defect through `create bucket`: the joiner, as the owner, would create the
// bucket afresh and its holders would drop theirs. It is refused with retry.
func TestACreateBucketAtAJoinerOfABucketItHasNotCopiedLosesNothing(t *testing.T) {
	// The line of defence under test is behind the join freeze, which would refuse
	// this schema change during the join first (join-freeze build doc, trap 5).
	switchJoinFreezeOff(t)
	s := setUpJoinerReadingUnpulledBuckets(t)
	before := generationsOn(t, s.tc.nodes, testDatabase)

	refusedAwaitingCopy(t, "a create at an owner of a bucket it has not copied", s.tc.frameOn(s.joinerNode, transport.OpCreateBucket,
		CreateBucketRequest{Database: testDatabase, Name: s.hashBucket, Type: BucketTypeHash}))
	sameGenerations(t, "a refused bucket create", s.tc.nodes, testDatabase, before)

	// Sent to a member, the create is forwarded to the joiner as the bucket's
	// owner, which refuses it the same way.
	refusedAwaitingCopy(t, "a create forwarded to an owner of a bucket it has not copied", s.tc.frame(1, transport.OpCreateBucket,
		CreateBucketRequest{Database: testDatabase, Name: s.hashBucket, Type: BucketTypeHash}))
	sameGenerations(t, "a refused forwarded bucket create", s.tc.nodes, testDatabase, before)

	pullAndCheckEveryoneHolds(t, s, s.hashBucket)
}

// TestACreateAtAJoinerOfADatabaseItHasNotLearnedStartsNoNewIncarnation is the
// general defect under the create-beside-copy fix: a joiner that has not learned a database the
// members hold answered `create database` by starting a second incarnation of
// it, and created a new bucket in yet another. Both go to the incarnation the
// members hold.
func TestACreateAtAJoinerOfADatabaseItHasNotLearnedStartsNoNewIncarnation(t *testing.T) {
	// The line of defence under test is behind the join freeze, which would refuse
	// this schema change during the join first (join-freeze build doc, trap 5).
	switchJoinFreezeOff(t)
	var joinerOwned, memberOwned string
	s := setUpJoinerReadingUnpulledBuckets(t)
	after := ringOf(t, s.members, 3)
	joinerID := s.members[len(s.members)-1].ID
	joinerOwned = bucketNamed(t, "freshjoin", func(name string) bool { return ownerOn(after, name) == joinerID })
	memberOwned = bucketNamed(t, "freshmemb", func(name string) bool { return ownerOn(after, name) != joinerID })
	before := generationsOn(t, s.tc.nodes, testDatabase)
	if len(before[0]) != 1 {
		t.Fatalf("setup: the members hold %v incarnations of %s, want one", before[0], testDatabase)
	}
	incarnation := before[0][0]

	frame := s.tc.frameOn(s.joinerNode, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	var created CreateDatabaseResponse
	if err := bson.Unmarshal(frame.Payload, &created); err != nil {
		t.Fatalf("decoding the create-database answer: %v (%s %s)", err, frame.Opcode, frame.Payload)
	}
	if created.Success {
		t.Errorf("create database %s at the joiner succeeded, though every member holds it", testDatabase)
	}
	sameGenerations(t, "create database at the joiner", s.tc.nodes, testDatabase, before)

	for _, bucket := range []string{joinerOwned, memberOwned} {
		answered(t, "a create at the joiner of a bucket that exists nowhere", s.tc.frameOn(s.joinerNode, transport.OpCreateBucket,
			CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash}))
		for i, node := range s.tc.nodes {
			pollUntil(t, "a member holds the new bucket", func() bool { return bucketGeneration(node, testDatabase, bucket) != 0 })
			if generation := bucketGeneration(node, testDatabase, bucket); generation != incarnation {
				t.Errorf("node %d created %s in incarnation %d, not the database's %d", i+1, bucket, generation, incarnation)
			}
		}
	}
	sameGenerations(t, "a bucket create at the joiner", s.tc.nodes, testDatabase, before)
}

// TestARestoreAtAJoinerOfANewBucketJoinsTheDatabasesIncarnation: a restore at
// the joiner of a bucket that exists nowhere, in a database the members hold
// and the joiner has not learned, creates the bucket in the members'
// incarnation. Before the fix it created the database again first.
func TestARestoreAtAJoinerOfANewBucketJoinsTheDatabasesIncarnation(t *testing.T) {
	// The line of defence under test is behind the join freeze, which would refuse
	// this schema change during the join first (join-freeze build doc, trap 5).
	switchJoinFreezeOff(t)
	s := setUpJoinerReadingUnpulledBuckets(t)
	after := ringOf(t, s.members, 3)
	joinerID := s.members[len(s.members)-1].ID
	bucket := bucketNamed(t, "freshrest", func(name string) bool { return ownerOn(after, name) == joinerID })
	before := generationsOn(t, s.tc.nodes, testDatabase)

	frame := s.tc.frameOn(s.joinerNode, transport.OpRestoreStart, RestoreStartRequest{
		Database: testDatabase, Bucket: bucket, FormatVersion: dumpManifestFormatVersion, StoreType: BucketTypeHash, Bytes: 1,
	})
	answered(t, "a restore at the joiner of a bucket that exists nowhere", frame)
	var resp RestoreStartResponse
	if err := bson.Unmarshal(frame.Payload, &resp); err != nil {
		t.Fatalf("decoding the restore start: %v", err)
	}
	if !resp.Created {
		t.Errorf("the restore of a bucket that exists nowhere did not create %s", bucket)
	}
	sameGenerations(t, "a restore of a new bucket at the joiner", s.tc.nodes, testDatabase, before)
	for i, node := range append(append([]*testNode{}, s.tc.nodes...), s.joinerNode) {
		pollUntil(t, "a node holds the restored bucket", func() bool { return bucketGeneration(node, testDatabase, bucket) != 0 })
		if generation := bucketGeneration(node, testDatabase, bucket); generation != before[0][0] {
			t.Errorf("node %d created %s in incarnation %d, not the database's %d", i+1, bucket, generation, before[0][0])
		}
	}
}

// TestARestoreAtAJoinerIsRefusedWhileTheMembersAreSilent (BUG-0362): a restore
// at a joiner that has not learned the database, while every member is silent
// to its catalog question, is refused UNAVAILABLE naming the members, and
// restores nothing. ensureDatabase asks the members first and refuses when
// they are silent (refuseWhenSilent); minting here would start a second
// incarnation of the database beside theirs. Guard: ensureDatabase's
// refuseWhenSilent switched to mintWhenSilent, so the restore creates the
// database and the bucket in a new incarnation.
func TestARestoreAtAJoinerIsRefusedWhileTheMembersAreSilent(t *testing.T) {
	// The line of defence under test is behind the join freeze, which would refuse
	// this schema change during the join first (join-freeze build doc, trap 5).
	switchJoinFreezeOff(t)
	s := setUpJoinerReadingUnpulledBuckets(t)
	after := ringOf(t, s.members, 3)
	joinerID := s.members[len(s.members)-1].ID
	bucket := bucketNamed(t, "silentrest", func(name string) bool { return ownerOn(after, name) == joinerID })
	before := generationsOn(t, s.tc.nodes, testDatabase)
	// The owner's question about the bucket is passed over, so the refusal
	// can only come from the question the restore asks about the database.
	ownerQuestionPassOverHook.Store(true)
	t.Cleanup(func() { ownerQuestionPassOverHook.Store(false) })
	for _, node := range s.tc.nodes {
		node.server.core.cluster.mismatch.Store(true)
	}
	frame := s.tc.frameOn(s.joinerNode, transport.OpRestoreStart, RestoreStartRequest{
		Database: testDatabase, Bucket: bucket, FormatVersion: dumpManifestFormatVersion, StoreType: BucketTypeHash, Bytes: 1,
	})
	for _, node := range s.tc.nodes {
		node.server.core.cluster.mismatch.Store(false)
	}
	refused := refusalOf(t, frame)
	if refused.Code != int(transport.ErrUnavailable) {
		t.Errorf("the restore was refused with code %d, want UNAVAILABLE (%d): %s", refused.Code, transport.ErrUnavailable, refused.Message)
	}
	if !strings.Contains(refused.Message, "has not learned database") {
		t.Errorf("the refusal is not the database question's (the restore did not ask the members whether they hold the database): %s", refused.Message)
	}
	for _, node := range s.tc.nodes {
		if id := node.server.core.cluster.self.ID; !strings.Contains(refused.Message, id) {
			t.Errorf("the refusal does not name the silent member %s: %s", id, refused.Message)
		}
	}
	if s.joiner.s.bucketStoreOpen(testDatabase, bucket) {
		t.Errorf("the refused restore created %s at the joiner", bucket)
	}
	sameGenerations(t, "a restore refused while the members are silent", s.tc.nodes, testDatabase, before)
}

// TestAnOwnerRecreatesABucketItDroppedWhileAReplicaStillHoldsIt guards the
// fix: an owner that dropped a bucket asks before it creates the name again,
// and a replica that has not applied the drop yet still answers that it holds
// the bucket. That copy is the dropped one; the owner's stream delivers the drop
// before the create, so the create goes ahead.
func TestAnOwnerRecreatesABucketItDroppedWhileAReplicaStillHoldsIt(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	bucket := bucketOwnedBy(t, tc, 0, "redo")
	createBucketAt(tc, 0, testDatabase, bucket, 0)
	putDocument(tc, 0, testDatabase, bucket, "old")
	waitForLocalCopies(t, tc, testDatabase, bucket, "old", 3)

	hold := holdPushes(peerOf(t, tc, 0, 1))
	defer hold.Release()
	tc.ok(0, transport.OpDeleteBucket, bson.M{"database": testDatabase, "name": bucket, "write_concern": "one"})
	if !tc.nodes[1].server.core.bucketStoreOpen(testDatabase, bucket) {
		t.Fatalf("setup: the held replica applied the drop of %s", bucket)
	}
	frame := tc.frame(0, transport.OpCreateBucket, bson.M{
		"database": testDatabase, "name": bucket, "type": string(BucketTypeHash), "write_concern": "one",
	})
	answered(t, "an owner's create of a bucket it dropped while a replica has not applied the drop", frame)

	hold.Release()
	pollUntil(t, "the replica drops the old document", func() bool { return !holdsKeyIn(tc.nodes[1], testDatabase, bucket, "old") })
}

// TestARestoreAtAJoinerIntoADatabaseThatExistsNowhereCreatesIt guards the fix:
// asked first, no member knows the database or holds the bucket, so the
// restore creates both, as on a settled cluster.
func TestARestoreAtAJoinerIntoADatabaseThatExistsNowhereCreatesIt(t *testing.T) {
	// The line of defence under test is behind the join freeze, which would refuse
	// this schema change during the join first (join-freeze build doc, trap 5).
	switchJoinFreezeOff(t)
	const fresh = "freshdb"
	var bucket string
	s := setUpJoinerWithoutCopies(t, func(t *testing.T, joined []ring.Node) []string {
		after := ringOf(t, joined, 3)
		joiner := joined[len(joined)-1].ID
		bucket = bucketNamed(t, "restored", func(name string) bool {
			owner, err := after.Owner(placementOf(fresh, name).encoded)
			return err == nil && owner.ID == joiner
		})
		return nil
	})
	frame := s.tc.frameOn(s.joinerNode, transport.OpRestoreStart, RestoreStartRequest{
		Database: fresh, Bucket: bucket, FormatVersion: dumpManifestFormatVersion, StoreType: BucketTypeHash, Bytes: 1,
	})
	answered(t, "a restore into a database that exists nowhere", frame)
	var resp RestoreStartResponse
	if err := bson.Unmarshal(frame.Payload, &resp); err != nil {
		t.Fatalf("decoding the restore start: %v", err)
	}
	if !resp.Created {
		t.Errorf("the restore into a database that exists nowhere did not create %s.%s", fresh, bucket)
	}
	waitListedBy(t, s.tc, fresh, 0, 1)
	joinerGenerations, err := s.joiner.s.liveGenerations(fresh)
	if err != nil {
		t.Fatalf("reading the joiner's generations of %s: %v", fresh, err)
	}
	for i, generations := range generationsOn(t, s.tc.nodes, fresh) {
		if !slices.Equal(generations, joinerGenerations) {
			t.Errorf("node %d holds incarnations %v of %s; the joiner that created it holds %v", i+1, generations, fresh, joinerGenerations)
		}
	}
}
