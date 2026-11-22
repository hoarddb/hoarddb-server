package server

// The create-beside-copy mechanism as the replication concurrency review of 2026-10-04
// found it (R-1, R-2, R-3), lifted from the review's probes. Each test fails
// when the tree loses or hides data a holder had before the join.

import (
	"context"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// joinerWaitUntil polls cond for at most wait and reports whether it held.
func joinerWaitUntil(wait time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(wait)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// holdersStillHold fails the test unless both members still hold key in bucket
// after the time a shipped create or drop needs to reach them.
func holdersStillHold(t *testing.T, s joinerReadingUnpulledBuckets, bucket, key, what string) {
	t.Helper()
	joinerWaitUntil(10*time.Second, func() bool {
		return !holdsKeyLocally(s.tc.nodes[0], bucket, key) || !holdsKeyLocally(s.tc.nodes[1], bucket, key)
	})
	for i := range s.tc.nodes {
		if !holdsKeyLocally(s.tc.nodes[i], bucket, key) {
			t.Errorf("%s: member %d no longer holds %q of %s, which it held before the join", what, i+1, key, bucket)
		}
	}
}

// foundAtJoiner sends a get to the joiner and reports whether the document was
// found, with the frame for the message.
func foundAtJoiner(t *testing.T, s joinerReadingUnpulledBuckets, bucket, key string) (bool, *transport.Frame) {
	t.Helper()
	frame := s.tc.frameOn(s.joinerNode, transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: key})
	if frame.Opcode == transport.OpError {
		return false, frame
	}
	var found FindResponse
	if err := bson.Unmarshal(frame.Payload, &found); err != nil {
		t.Fatalf("decoding the get at the joiner: %v", err)
	}
	return found.Found, frame
}

// TestReviewACreateAtAJoinerOwnerWipesTheHoldersCopies (review R-1): a joiner
// owns a bucket whose two holders hold "held" and has never seen the database.
// A create of that name at the joiner, with or without its pull in flight, and
// a merge restore started there, must leave every holder's copy, and the
// joiner's pull must land it.
func TestReviewACreateAtAJoinerOwnerWipesTheHoldersCopies(t *testing.T) {
	// The line of defence under test is behind the join freeze, which would refuse
	// this schema change during the join first (join-freeze build doc, trap 5).
	switchJoinFreezeOff(t)
	s := setUpJoinerReadingUnpulledBuckets(t)

	t.Run("no pull in flight", func(t *testing.T) {
		bucket := s.restoredBucket
		frame := s.tc.frameOn(s.joinerNode, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
		t.Logf("create bucket at the joiner answered %s: %s", frame.Opcode, frame.Payload)
		holdersStillHold(t, s, bucket, "held", "a create at the joiner")

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		t.Logf("the joiner's pull of %s: %v", bucket, s.joiner.ensureMigrated(ctx, testDatabase, bucket))
		if found, answer := foundAtJoiner(t, s, bucket, "held"); !found {
			t.Errorf("after the create and the pull, a get at the owner does not find %q of %s: %s", "held", bucket, answer.Payload)
		}
	})

	// The joiner's own apply of the create is refused with its pull in flight;
	// the create must not have been logged and shipped before that.
	t.Run("pull in flight", func(t *testing.T) {
		bucket := s.landingBucket
		done, release := s.holdPull(t, bucket)
		defer release()
		frame := s.tc.frameOn(s.joinerNode, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
		t.Logf("create bucket at the joiner, pull in flight, answered %s: %s", frame.Opcode, frame.Payload)
		holdersStillHold(t, s, bucket, "held", "a create at the joiner with its pull in flight")
		release()
		select {
		case err := <-done:
			t.Logf("the held pull ended: %v", err)
		case <-time.After(60 * time.Second):
			t.Fatal("the held pull never ended")
		}
		if found, answer := foundAtJoiner(t, s, bucket, "held"); !found {
			t.Errorf("after the create and the pull, a get at the owner does not find %q of %s: %s", "held", bucket, answer.Payload)
		}
	})

	// As first filed: a merge restore's start at the joiner before any pull.
	t.Run("restore start before any pull", func(t *testing.T) {
		bucket := s.hashBucket
		frame := s.tc.frameOn(s.joinerNode, transport.OpRestoreStart, RestoreStartRequest{
			Database: testDatabase, Bucket: bucket, FormatVersion: dumpManifestFormatVersion, StoreType: BucketTypeHash, Bytes: 1,
		})
		t.Logf("restore start at the joiner before any pull answered %s: %s", frame.Opcode, frame.Payload)
		holdersStillHold(t, s, bucket, "held", "a merge restore started at the joiner")

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		t.Logf("the joiner's pull of %s: %v", bucket, s.joiner.ensureMigrated(ctx, testDatabase, bucket))
		if found, answer := foundAtJoiner(t, s, bucket, "held"); !found {
			t.Errorf("after the restore start and the pull, a get at the owner does not find %q of %s: %s", "held", bucket, answer.Payload)
		}
	})
}

// TestAHolderKeepsALiveCopyAgainstACreateOfAnIncarnationItHasNotSeen (review
// R-1, change 1): whatever the coordinator and the owner checked, a holder
// replaces its copy of a name only when that copy's incarnation is known
// dropped — carried by the create or in its own tombstones. Here the joiner, as
// the owner, logs a create stamped with an incarnation minted from its empty
// view, as a coordinator did before the fix; each holder keeps its copy.
func TestAHolderKeepsALiveCopyAgainstACreateOfAnIncarnationItHasNotSeen(t *testing.T) {
	// The line of defence under test is behind the join freeze, which would refuse
	// this schema change during the join first (join-freeze build doc, trap 5).
	switchJoinFreezeOff(t)
	s := setUpJoinerReadingUnpulledBuckets(t)
	bucket := s.restoredBucket
	minted, err := mintGeneration(generationsOn(t, s.tc.nodes, testDatabase)...)
	if err != nil {
		t.Fatalf("minting a generation: %v", err)
	}
	payload, err := bson.Marshal(bson.D{
		{Key: "database", Value: testDatabase}, {Key: "name", Value: bucket}, {Key: "type", Value: string(BucketTypeHash)},
		{Key: "db_gen", Value: minted}, {Key: "dropped_generations", Value: []int64{}},
	})
	if err != nil {
		t.Fatalf("encoding the create: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp, err := s.joiner.applyAsOwner(ctx, ReplEntry{Op: int32(transport.OpCreateBucket), Bucket: bucket, Payload: payload}, placementOf(testDatabase, bucket), 0)
	t.Logf("the owner's create in a minted incarnation: %s %v", transport.Opcode(resp.Opcode), err)
	holdersStillHold(t, s, bucket, "held", "a create in an incarnation the holders have not seen")
}

// TestReviewACreateThroughAHolderAtAJoinerOwnerLeavesAnEmptyOwnerCopy (review
// R-2): the same create coordinated at a holder carries the holders' own
// generation and is forwarded to the joiner as the owner. It must not leave an
// empty owner copy that the pull then skips.
func TestReviewACreateThroughAHolderAtAJoinerOwnerLeavesAnEmptyOwnerCopy(t *testing.T) {
	// The line of defence under test is behind the join freeze, which would refuse
	// this schema change during the join first (join-freeze build doc, trap 5).
	switchJoinFreezeOff(t)
	s := setUpJoinerReadingUnpulledBuckets(t)
	bucket := s.hashBucket

	frame := s.tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	t.Logf("create bucket through member 1 answered %s: %s", frame.Opcode, frame.Payload)
	holdersStillHold(t, s, bucket, "held", "a create through a holder")
	servedBeforePull := s.joiner.s.bucketStoreOpen(testDatabase, bucket)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	t.Logf("the joiner's pull of %s: %v", bucket, s.joiner.ensureMigrated(ctx, testDatabase, bucket))
	if found, answer := foundAtJoiner(t, s, bucket, "held"); !found {
		t.Errorf("after a create through a holder and the joiner's pull, a get at the owner does not find %q of %s (served before the pull: %t): %s",
			"held", bucket, servedBeforePull, answer.Payload)
	}
}

// TestReviewACreateDatabaseAtAJoinerSaysCreatedAndMintsASecondIncarnation
// (review R-3): `create database` of a database every member holds, sent to a
// joiner that has not learned it, must not be answered "created" nor give the
// members a second live incarnation; and a later create of an existing name
// through a holder must leave the holders' copies.
func TestReviewACreateDatabaseAtAJoinerSaysCreatedAndMintsASecondIncarnation(t *testing.T) {
	// The line of defence under test is behind the join freeze, which would refuse
	// this schema change during the join first (join-freeze build doc, trap 5).
	switchJoinFreezeOff(t)
	s := setUpJoinerReadingUnpulledBuckets(t)

	frame := s.tc.frameOn(s.joinerNode, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	var created CreateDatabaseResponse
	if frame.Opcode != transport.OpError {
		if err := bson.Unmarshal(frame.Payload, &created); err != nil {
			t.Fatalf("decoding the create-database answer: %v", err)
		}
	}
	if created.Success {
		t.Errorf("a create of database %q, which every member holds, was answered success at the joiner: %s", testDatabase, created.Message)
	}
	for i := range s.tc.nodes {
		live, err := s.tc.nodes[i].server.core.liveGenerations(testDatabase)
		if err != nil {
			t.Fatalf("reading member %d's live generations: %v", i+1, err)
		}
		if len(live) > 1 {
			t.Errorf("member %d now holds %d live incarnations of %q after a create at the joiner", i+1, len(live), testDatabase)
		}
	}

	bucket := s.hashBucket
	frame = s.tc.frame(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	t.Logf("create bucket through member 1 after the create database at the joiner answered %s: %s", frame.Opcode, frame.Payload)
	holdersStillHold(t, s, bucket, "held", "a create through a holder after a create database at the joiner")
}
