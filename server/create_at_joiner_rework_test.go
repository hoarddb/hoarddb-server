package server

// The create-beside-copy fix, the verifier's second round: the catalog question must not crash a
// member, a create that cannot hear the members must not start an incarnation,
// a holder's answer to a create over its live copy must not stall the owner's
// stream, and a pull registered after the owner's fence must still stop the
// create before it is logged.

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestAnAnswerToTheCatalogQuestionSurvivesAConcurrentDrop (verifier D-1): the
// answer read a bucket it had just seen open, and a drop in between made the
// second lookup nil, which crashed the member.
func TestAnAnswerToTheCatalogQuestionSurvivesAConcurrentDrop(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	c := tc.nodes[0].server.core.cluster
	var stop atomic.Bool
	var askers sync.WaitGroup
	for range 4 {
		askers.Add(1)
		go func() {
			defer askers.Done()
			for !stop.Load() {
				c.answerCatalog(ReplPushRequest{Catalog: &ReplCatalogAsk{Database: testDatabase, Bucket: "racer"}})
			}
		}()
	}
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		tc.frame(0, transport.OpCreateBucket, bson.M{"database": testDatabase, "name": "racer", "type": string(BucketTypeHash)})
		tc.frame(0, transport.OpDeleteBucket, bson.M{"database": testDatabase, "name": "racer"})
	}
	stop.Store(true)
	askers.Wait()
}

// streamsDrained fails the test unless every stream from the joiner to a member
// acknowledges the joiner's whole log within the wait: a stream stuck on a
// refused entry never does.
func streamsDrained(t *testing.T, s joinerReadingUnpulledBuckets, what string) {
	t.Helper()
	last := s.joiner.walLastSequence()
	for i, node := range s.tc.nodes {
		stream := s.joiner.peerFor(node.server.core.cluster.self)
		if !joinerWaitUntil(30*time.Second, func() bool { return stream.ackedThrough(s.joiner.self.ID) >= last }) {
			stream.streamMu.Lock()
			reason := stream.stallReason
			stream.streamMu.Unlock()
			t.Errorf("%s: the joiner's stream to member %d is stuck at %d of %d: %.300s", what, i+1, stream.ackedThrough(s.joiner.self.ID), last, reason)
		}
	}
}

// TestACreateAtAJoinerThatCannotHearTheMembersIsRefused (verifier D-2): a joiner
// that has not learned the database and cannot hear the members must not start
// an incarnation of its own. Created in it, the bucket was refused by its
// holders once they answered again, for ever, and the joiner served an empty
// copy. The create is refused with retry and names the members it did not hear.
func TestACreateAtAJoinerThatCannotHearTheMembersIsRefused(t *testing.T) {
	// The line of defence under test is behind the join freeze, which would refuse
	// this schema change during the join first (join-freeze build doc, trap 5).
	switchJoinFreezeOff(t)
	s := setUpJoinerReadingUnpulledBuckets(t)
	bucket := s.restoredBucket
	before := generationsOn(t, s.tc.nodes, testDatabase)
	for _, node := range s.tc.nodes {
		node.server.core.cluster.mismatch.Store(true)
	}
	frame := s.tc.frameOn(s.joinerNode, transport.OpCreateBucket, bson.M{
		"database": testDatabase, "name": bucket, "type": string(BucketTypeHash), "write_concern": "one",
	})
	for _, node := range s.tc.nodes {
		node.server.core.cluster.mismatch.Store(false)
	}
	refusedAwaitingCopy(t, "a create at a joiner that cannot hear the members", frame)
	var refusal ErrorResponse
	if err := bson.Unmarshal(frame.Payload, &refusal); err != nil {
		t.Fatalf("decoding the refusal: %v", err)
	}
	// Which members the joiner did not hear is timing-dependent: the divergence
	// flag that silences them is cleared by a gossip round, and under load a live
	// member's own answer can miss the question's deadline. The refusal names
	// every member it did not hear, so it may name one member or several;
	// requiring a particular member, or all of them, is the flake. It must name
	// at least one member it did not hear.
	named := false
	for _, node := range s.tc.nodes {
		if strings.Contains(refusal.Message, node.server.core.cluster.self.ID) {
			named = true
			break
		}
	}
	if !named {
		t.Errorf("the refusal names no member the joiner did not hear: %s", refusal.Message)
	}
	sameGenerations(t, "a create that could not hear the members", s.tc.nodes, testDatabase, before)
	streamsDrained(t, s, "after the refused create")
	pullAndCheckEveryoneHolds(t, s, bucket)
}

// TestAHolderAnswersACreateOverItsLiveCopyWithoutStallingTheStream (verifier
// D-2, the backstop): whatever reaches a holder as a create over a copy it holds
// in a live incarnation, the holder keeps its copy.
//
// Its second clause is reversed by replication dispatch 10a.6 (review M-7):
// the stream no longer moves on. D-2 answered "already exists" so that a
// refusal would not hold every later entry behind it; but then the stray's
// later writes landed in the holder's copy of another incarnation. The holder
// now refuses the create, records it, and the owner's stream stalls on it
// until an operator repairs it, so the write after the create never reaches
// the holders. The name is kept: the catalog-question file and the join-freeze build doc
// cite it. So it now asserts the stall (review M-7); its exits are owner ruling
// 53's — a drop of the refused incarnation through this owner
// (incarnation_stall_exit_test.go:TestADropOfTheRefusedIncarnationEndsTheStall)
// or a migrate on the holder (divergence_test.go:
// TestMigrateReSeedsAHolderOfAnotherIncarnation) — and this test takes neither.
func TestAHolderAnswersACreateOverItsLiveCopyWithoutStallingTheStream(t *testing.T) {
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
	// The create reaches the owner as if its question had passed the door
	// (R8-D): this is the holder's backstop, behind the owner's guard.
	ctx = withSurveyEpoch(ctx, s.joiner.Epoch())
	resp, err := s.joiner.applyAsOwner(ctx, ReplEntry{Op: int32(transport.OpCreateBucket), Bucket: bucket, Payload: payload}, placementOf(testDatabase, bucket), 0)
	t.Logf("the owner's create in a minted incarnation: %s %v", transport.Opcode(resp.Opcode), err)

	answered(t, "a write at the owner after the create", s.tc.frameOn(s.joinerNode, transport.OpPut, InsertRequest{
		Database: testDatabase, Bucket: bucket, Key: "after", Data: map[string]interface{}{"n": 2}, WriteConcern: "one"}))
	joinerID := s.joiner.self.ID
	refusedEverywhere := eventually(func() bool {
		for _, node := range s.tc.nodes {
			if record, recorded := node.server.core.cluster.divergenceFrom(joinerID); !recorded || record.Reason != refusalAnotherIncarnation {
				return false
			}
		}
		return true
	})
	if !refusedEverywhere {
		t.Error("the holders did not record the create over their live copies as another incarnation")
	}
	for i, node := range s.tc.nodes {
		if holdsKeyLocally(node, bucket, "after") {
			t.Errorf("member %d stores the write the owner accepted after a create of another incarnation", i+1)
		}
		if !holdsKeyLocally(node, bucket, "held") {
			t.Errorf("member %d no longer holds %q, which it held before the join", i+1, "held")
		}
	}
}

// TestAPullRegisteredAfterTheOwnersFenceStillStopsTheCreate: a create of a
// bucket the joiner owns passed the door, and the take-over's pull of the
// bucket registered before the create was logged. The create is not logged:
// inside the fence it finds the take-over running, waits for it holding no
// fence, and is answered from the copy it landed (re-aimed by replication
// dispatch 10b, step 10b.3: it was refused with retry before; a create is no
// longer exempt from the take-over, and waits for it).
func TestAPullRegisteredAfterTheOwnersFenceStillStopsTheCreate(t *testing.T) {
	// The line of defence under test is behind the join freeze, which would refuse
	// this schema change during the join first (join-freeze build doc, trap 5).
	switchJoinFreezeOff(t)
	s := setUpJoinerReadingUnpulledBuckets(t)
	bucket := s.hashBucket
	// The door's question would refuse the create, the holders answering; the
	// test is about the check inside the fence, which a take-over registered
	// after the door meets.
	ownerQuestionPassOverHook.Store(true)
	defer ownerQuestionPassOverHook.Store(false)

	registered, release := make(chan struct{}), make(chan struct{})
	var started atomic.Bool
	pullHook := func(_, pulled string) {
		if pulled == bucket && started.CompareAndSwap(false, true) {
			close(registered)
			<-release
		}
	}
	pullStartedHook.Store(&pullHook)
	defer pullStartedHook.Store(nil)
	pulled := make(chan error, 1)
	var fenced atomic.Bool
	fenceHook := func(op transport.Opcode, _, named string) {
		if op != transport.OpCreateBucket || named != bucket || !fenced.CompareAndSwap(false, true) {
			return
		}
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			pulled <- s.joiner.ensureMigrated(ctx, testDatabase, bucket)
		}()
		select {
		case <-registered:
		case <-time.After(20 * time.Second):
		}
		// The pull lands while the create waits for it, after the fence.
		time.AfterFunc(time.Second, func() { close(release) })
	}
	afterJoinFenceHook.Store(&fenceHook)
	defer afterJoinFenceHook.Store(nil)

	frame := s.tc.frameOn(s.joinerNode, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	if !started.Load() {
		t.Fatalf("setup: the pull of %s never registered after the fence", bucket)
	}
	if err := <-pulled; err != nil {
		t.Errorf("the take-over's pull of %s, which its holders hold: %v", bucket, err)
	}
	var created CreateBucketResponse
	if frame.Opcode == transport.OpError || bson.Unmarshal(frame.Payload, &created) != nil || !created.Success || !strings.Contains(created.Message, "already exists") {
		t.Errorf("a create whose take-over registered after the owner's fence was answered %s %s; want the bucket that already exists",
			frame.Opcode, frame.Payload)
	}
	entries, err := s.joiner.s.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading the joiner's log: %v", err)
	}
	for _, entry := range entries {
		if transport.Opcode(entry.Op) == transport.OpCreateBucket && entry.Bucket == bucket {
			t.Errorf("the joiner logged the create of %s at sequence %d, though its take-over was running", bucket, entry.Seq)
		}
	}
	if found, answer := foundAtJoiner(t, s, bucket, "held"); !found {
		t.Errorf("after the create and the take-over, a get at the owner does not find %q of %s: %s", "held", bucket, answer.Payload)
	}
}
