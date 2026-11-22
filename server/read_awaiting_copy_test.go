package server

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// A read at an owner that holds no copy of its bucket yet answered
// "database not found" (INTERNAL, with advice to create the database) instead of
// the join fence's "not ready, retry".

// joinerReadingUnpulledBuckets is a joiner that owns a hash bucket holding one
// document and a FIFO bucket holding one element, and holds neither yet: the
// members' streams to it are held, so no replay creates them there, and
// nothing rebalances on its own, so the test's pulls are the only ones. The
// joiner has never seen the database either; it learns it with its first
// copied bucket.
type joinerReadingUnpulledBuckets struct {
	joinerWithoutCopies
	hashBucket, fifoBucket string
	// landingBucket is a hash bucket holding one document, pulled with its
	// materialise held.
	landingBucket string
	// restoredBucket is a hash bucket holding one document, which a restore
	// names while its pull is in flight.
	restoredBucket string
}

func setUpJoinerReadingUnpulledBuckets(t *testing.T) joinerReadingUnpulledBuckets {
	t.Helper()
	return setUpJoinerReadingUnpulledBucketsWith(t, false)
}

// setUpJoinerReadingUnpulledBucketsWith is setUpJoinerReadingUnpulledBuckets;
// noHistory has the joiner adopt its join with no history, so it does not
// know the buckets' previous owners (on-demand pull build doc step 9): a read
// whose pull is held is then undecided and answers retry. A joiner that knows
// its previous owner forwards such a read instead
// (TestAPeekAtAJoinerPastTheBoundIsForwardedToThePreviousOwner).
func setUpJoinerReadingUnpulledBucketsWith(t *testing.T, noHistory bool) joinerReadingUnpulledBuckets {
	t.Helper()
	tc := newTestClusterWith(t, 2, 3, func(_ int, cfg *config.Config) { noSweepNoRetention(cfg) })
	t.Cleanup(tc.stop)
	ports := reserveNodePorts(t)
	joined := append(append([]ring.Node{}, tc.nodes[0].server.core.cluster.Members()...), ports.ringNode())
	after := ringOf(t, joined, 3)
	joinerID := joined[len(joined)-1].ID
	ownedByJoiner := func(name string) bool { return ownerOn(after, name) == joinerID }
	hashBucket := bucketNamed(t, "awaitread", ownedByJoiner)
	fifoBucket := bucketNamed(t, "awaitpeek", ownedByJoiner)
	landingBucket := bucketNamed(t, "awaitland", ownedByJoiner)
	restoredBucket := bucketNamed(t, "awaitrest", ownedByJoiner)
	buckets := []string{hashBucket, fifoBucket, landingBucket, restoredBucket}

	// Everything is written before the join: afterwards a write to any of the
	// buckets is the joiner's to apply, and the join fence refuses it.
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: hashBucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: fifoBucket, Type: BucketTypeFIFO})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: landingBucket, Type: BucketTypeHash})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: restoredBucket, Type: BucketTypeHash})
	for _, bucket := range buckets {
		waitForServing(t, tc, testDatabase, bucket, 2)
	}
	for _, bucket := range []string{hashBucket, landingBucket, restoredBucket} {
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: bucket, Key: "held",
			Data: map[string]interface{}{"n": 1}, WriteConcern: "majority"})
	}
	tc.ok(0, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: fifoBucket,
		Data: map[string]interface{}{"n": 1}, WriteConcern: "majority"})

	proposer := tc.nodes[0].server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := proposer.proposeJoin(ctx, ports.ringNode()); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	for _, node := range tc.nodes {
		t.Cleanup(holdStreamPushesOnly(node.server.core.cluster.peerFor(ports.ringNode())))
	}
	if noHistory {
		skipGossipOf(t, ports.ringAddr)
	}
	joinerNode := startJoinTarget(t, tc, ports, tc.token, noSweepNoRetention)
	t.Cleanup(func() { _ = joinerNode.client.Close() })
	joiner := joinerNode.server.core.cluster
	adoptFrom(t, joiner, proposer)
	for _, bucket := range buckets {
		if !joiner.actsAsOwner(placementOf(testDatabase, bucket)) {
			t.Fatalf("setup: the joiner does not own %s", bucket)
		}
		if joiner.s.bucketStoreOpen(testDatabase, bucket) {
			t.Fatalf("setup: the joiner holds %s before any pull", bucket)
		}
	}
	return joinerReadingUnpulledBuckets{
		joinerWithoutCopies: joinerWithoutCopies{tc: tc, joiner: joiner, joinerNode: joinerNode, members: joined},
		hashBucket:          hashBucket,
		fifoBucket:          fifoBucket,
		landingBucket:       landingBucket,
		restoredBucket:      restoredBucket,
	}
}

// holdPull starts the joiner's pull of bucket and holds it as it starts, once it
// is registered, before anything is copied. It returns the pull's outcome and the
// function that releases it.
func (s joinerReadingUnpulledBuckets) holdPull(t *testing.T, bucket string) (<-chan error, func()) {
	t.Helper()
	held, release := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	hold := func(_, pulled string) {
		if pulled == bucket && once.CompareAndSwap(false, true) {
			close(held)
			<-release
		}
	}
	pullStartedHook.Store(&hold)
	t.Cleanup(func() { pullStartedHook.Store(nil) })
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		done <- s.joiner.ensureMigrated(ctx, testDatabase, bucket)
	}()
	select {
	case <-held:
	case err := <-done:
		t.Fatalf("the pull of %s ended before it was held: %v", bucket, err)
	case <-time.After(30 * time.Second):
		t.Fatalf("the pull of %s never started", bucket)
	}
	var released atomic.Bool
	return done, func() {
		if released.CompareAndSwap(false, true) {
			close(release)
		}
	}
}

// awaitPull releases a held pull and waits for it to land.
func awaitPull(t *testing.T, done <-chan error, release func()) {
	t.Helper()
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the pull, released: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the released pull never landed")
	}
}

// refusedAwaitingCopy fails the test unless frame is the join fence's refusal:
// UNAVAILABLE, and not a "not found" that tells the user to create what exists.
func refusedAwaitingCopy(t *testing.T, what string, frame *transport.Frame) {
	t.Helper()
	var refusal ErrorResponse
	if frame.Opcode != transport.OpError || bson.Unmarshal(frame.Payload, &refusal) != nil {
		t.Errorf("%s was answered %s, want the join fence's refusal (UNAVAILABLE): %s", what, frame.Opcode, frame.Payload)
		return
	}
	if transport.ErrorCode(refusal.Code) != transport.ErrUnavailable {
		t.Errorf("%s was answered code %d, want UNAVAILABLE (%d): %s", what, refusal.Code, transport.ErrUnavailable, refusal.Message)
	}
	if strings.Contains(refusal.Message, "not found") || strings.Contains(refusal.Message, "create database") {
		t.Errorf("%s tells the user the data does not exist, though its copy is on its way: %s", what, refusal.Message)
	}
}

// answered fails the test unless frame is a successful answer.
func answered(t *testing.T, what string, frame *transport.Frame) {
	t.Helper()
	if frame.Opcode == transport.OpError {
		var refusal ErrorResponse
		_ = bson.Unmarshal(frame.Payload, &refusal)
		t.Errorf("%s was refused, code %d: %s", what, refusal.Code, refusal.Message)
	}
}

// TestAReadAtAnOwnerWhoseCopyHasNotLandedSaysRetry: A joiner owns a
// bucket it is still pulling. A get, a find, a count and a queue peek sent to
// it, and a read forwarded to it by another member, are refused with the join
// fence's ErrJoinSyncIncomplete (UNAVAILABLE, awaiting_copy), as a write is.
// On the starting tree they were answered "database not found … or 'create
// database …'", INTERNAL. A member that holds an open copy keeps answering, and
// once the pull lands the joiner answers every one of them.
func TestAReadAtAnOwnerWhoseCopyHasNotLandedSaysRetry(t *testing.T) {
	// The restore start at the joiner tests the join fence, which is
	// behind the join freeze (join-freeze build doc, trap 5).
	switchJoinFreezeOff(t)
	// The joiner does not know the buckets' previous owners, so a read whose
	// pull is held is undecided (spec §11, "Tests that move"); one that knows
	// forwards it (step 9).
	s := setUpJoinerReadingUnpulledBucketsWith(t, true)

	t.Run("a get, a find and a count", func(t *testing.T) {
		done, release := s.holdPull(t, s.hashBucket)
		defer release()

		get := FindRequest{Database: testDatabase, Bucket: s.hashBucket, Key: "held"}
		refusedAwaitingCopy(t, "a get at the owner", s.tc.frameOn(s.joinerNode, transport.OpGet, get))
		refusedAwaitingCopy(t, "a find at the owner", s.tc.frameOn(s.joinerNode, transport.OpScan, ScanRequest{Database: testDatabase, Bucket: s.hashBucket}))
		refusedAwaitingCopy(t, "a count at the owner", s.tc.frameOn(s.joinerNode, transport.OpCount, CountRequest{Database: testDatabase, Bucket: s.hashBucket}))

		// A read forwarded to the owner by a member: the owner answers the
		// internode awaiting_copy refusal, and the member's error matches the
		// sentinel and names the owner.
		entry := ReplEntry{Op: int32(transport.OpGet), Bucket: s.hashBucket, Payload: mustMarshalBSON(t, get)}
		push := mustMarshalBSON(t, ReplPushRequest{Token: s.joiner.localToken(), FromNode: s.members[0].ID,
			ReturnResponse: true, Epoch: s.joiner.Epoch(), Entries: []ReplEntry{entry}})
		ack, err := handleReplPush(context.Background(), transport.NewFrame(transport.OpReplPush, push), s.joiner.s)
		if err != nil {
			t.Fatalf("forwarding the get: %v", err)
		}
		var resp ReplPushResponse
		if err := bson.Unmarshal(ack.Payload, &resp); err != nil {
			t.Fatalf("decoding the answer to the forwarded get: %v", err)
		}
		if resp.Success || resp.Refusal != refusalAwaitingCopy {
			t.Errorf("a get forwarded to the owner was answered success=%t refusal=%q error=%q; want %q",
				resp.Success, resp.Refusal, resp.Error, refusalAwaitingCopy)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		coordinator := s.tc.nodes[0].server.core.cluster
		_, err = coordinator.forward(ctx, s.joiner.self, entry, 0, readForward{})
		if !errors.Is(err, ErrJoinSyncIncomplete) {
			t.Errorf("a get forwarded to the owner returned %v at the member; want it to match ErrJoinSyncIncomplete", err)
		} else if !strings.Contains(err.Error(), s.joiner.self.ID) || strings.Contains(err.Error(), "write") {
			t.Errorf("the member's error for a forwarded read should name the owner %s and speak of a read: %v", s.joiner.self.ID, err)
		}

		// A member that holds an open copy answers from it, as before.
		answered(t, "a get at a member with an open copy", s.tc.frame(0, transport.OpGet, get))

		awaitPull(t, done, release)
		frame := s.tc.frameOn(s.joinerNode, transport.OpGet, get)
		answered(t, "a get at the owner once its copy landed", frame)
		var found FindResponse
		if err := bson.Unmarshal(frame.Payload, &found); err != nil || !found.Found {
			t.Errorf("a get at the owner once its copy landed did not find the document (err %v): %s", err, frame.Payload)
		}
		answered(t, "a find at the owner once its copy landed", s.tc.frameOn(s.joinerNode, transport.OpScan, ScanRequest{Database: testDatabase, Bucket: s.hashBucket}))
		answered(t, "a count at the owner once its copy landed", s.tc.frameOn(s.joinerNode, transport.OpCount, CountRequest{Database: testDatabase, Bucket: s.hashBucket}))
	})

	t.Run("a queue peek", func(t *testing.T) {
		done, release := s.holdPull(t, s.fifoBucket)
		defer release()

		peek := PeekRequest{Database: testDatabase, Bucket: s.fifoBucket}
		refusedAwaitingCopy(t, "a peek at the owner", s.tc.frameOn(s.joinerNode, transport.OpFIFOPeek, peek))
		// A peek is answered by the owner alone, so through a member
		// it is forwarded to the joiner and refused there.
		refusedAwaitingCopy(t, "a peek through a member", s.tc.frame(0, transport.OpFIFOPeek, peek))

		awaitPull(t, done, release)
		frame := s.tc.frameOn(s.joinerNode, transport.OpFIFOPeek, peek)
		answered(t, "a peek at the owner once its copy landed", frame)
		var peeked PeekResponse
		if err := bson.Unmarshal(frame.Payload, &peeked); err != nil || !peeked.Found {
			t.Errorf("a peek at the owner once its copy landed found no element (err %v): %s", err, frame.Payload)
		}
	})

	// The pull is held during its materialise: the copy's keys are written and
	// its catalog entry is not. A get sent then either waits for the copy and
	// finds the document, or is told to retry; it is never told the bucket or
	// its database does not exist.
	t.Run("a get while the copy is put in place", func(t *testing.T) {
		held, release := make(chan struct{}), make(chan struct{})
		var once, released atomic.Bool
		hold := func(uint64) error {
			if once.CompareAndSwap(false, true) {
				close(held)
				<-release
			}
			return nil
		}
		releaseOnce := func() {
			if released.CompareAndSwap(false, true) {
				close(release)
			}
		}
		materializeCatalogHook.Store(&hold)
		defer materializeCatalogHook.Store(nil)
		defer releaseOnce()
		done := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			done <- s.joiner.ensureMigrated(ctx, testDatabase, s.landingBucket)
		}()
		select {
		case <-held:
		case err := <-done:
			t.Fatalf("the pull ended before its catalog commit: %v", err)
		case <-time.After(30 * time.Second):
			t.Fatal("the pull never reached its catalog commit")
		}

		type getAnswer struct {
			frame *transport.Frame
			err   error
		}
		payload := mustMarshalBSON(t, FindRequest{Database: testDatabase, Bucket: s.landingBucket, Key: "held"})
		got := make(chan getAnswer, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			frame, err := s.joinerNode.client.SendRequest(ctx, transport.OpGet, payload)
			got <- getAnswer{frame, err}
		}()
		var answer getAnswer
		select {
		case answer = <-got:
			awaitPull(t, done, releaseOnce)
		case <-time.After(2 * time.Second):
			// Waiting on the copy, as a write does: let the pull land.
			awaitPull(t, done, releaseOnce)
			select {
			case answer = <-got:
			case <-time.After(30 * time.Second):
				t.Fatal("the get was never answered once the copy landed")
			}
		}
		if answer.err != nil {
			t.Fatalf("sending the get: %v", answer.err)
		}
		frame := answer.frame
		if frame.Opcode == transport.OpError {
			refusedAwaitingCopy(t, "a get while the copy is put in place", frame)
			return
		}
		var found FindResponse
		if err := bson.Unmarshal(frame.Payload, &found); err != nil || !found.Found {
			t.Errorf("a get while the copy is put in place did not find the document (err %v): %s", err, frame.Payload)
		}
	})

	// A restore's start creates the bucket it names. At an owner whose pull of
	// that bucket is in flight, the pull then found the bucket served and
	// failed, and the bucket's data was left on holders that release it once
	// the join settles. The start is refused with retry, and the pull lands.
	t.Run("a restore start", func(t *testing.T) {
		done, release := s.holdPull(t, s.restoredBucket)
		defer release()

		refusedAwaitingCopy(t, "a restore start at the owner", s.tc.frameOn(s.joinerNode, transport.OpRestoreStart, RestoreStartRequest{
			Database: testDatabase, Bucket: s.restoredBucket, FormatVersion: dumpManifestFormatVersion, StoreType: BucketTypeHash, Bytes: 1,
		}))

		awaitPull(t, done, release)
		frame := s.tc.frameOn(s.joinerNode, transport.OpGet, FindRequest{Database: testDatabase, Bucket: s.restoredBucket, Key: "held"})
		answered(t, "a get at the owner once its copy landed", frame)
		var found FindResponse
		if err := bson.Unmarshal(frame.Payload, &found); err != nil || !found.Found {
			t.Errorf("the owner's landed copy of %s lacks the document its holders had (err %v): %s", s.restoredBucket, err, frame.Payload)
		}
	})
}

// TestAReadOfADatabaseThatDoesNotExistIsStillNotFound guards the fix: an owner
// that holds no copy because there is none, and is pulling nothing, still says
// the database does not exist and how to create it.
func TestAReadOfADatabaseThatDoesNotExistIsStillNotFound(t *testing.T) {
	const missing = "neverdb"
	var bucket string
	s := setUpJoinerWithoutCopies(t, func(t *testing.T, joined []ring.Node) []string {
		after := ringOf(t, joined, 3)
		joiner := joined[len(joined)-1].ID
		bucket = bucketNamed(t, "nowhere", func(name string) bool {
			owner, err := after.Owner(placementOf(missing, name).encoded)
			return err == nil && owner.ID == joiner
		})
		return nil
	})
	if !s.joiner.actsAsOwner(placementOf(missing, bucket)) {
		t.Fatalf("setup: the joiner does not own %s.%s", missing, bucket)
	}
	frame := s.tc.frameOn(s.joinerNode, transport.OpGet, FindRequest{Database: missing, Bucket: bucket, Key: "k"})
	var refusal ErrorResponse
	if frame.Opcode != transport.OpError || bson.Unmarshal(frame.Payload, &refusal) != nil {
		t.Fatalf("a get in a database that does not exist was answered %s: %s", frame.Opcode, frame.Payload)
	}
	if transport.ErrorCode(refusal.Code) == transport.ErrUnavailable {
		t.Errorf("a get in a database that does not exist was answered UNAVAILABLE, as if a copy were on its way: %s", refusal.Message)
	}
	if !strings.Contains(refusal.Message, "not found") || !strings.Contains(refusal.Message, "create database "+missing) {
		t.Errorf("a get in a database that does not exist does not say so with the create hint: %s", refusal.Message)
	}
}
