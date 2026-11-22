package server

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// The outcome cache (on-demand pull build doc step 4; spec §4.1, §4.2, R-2,
// F-6, R2-7): a take-over's finding about a name nobody here holds answers the
// reads of it, so a loop reading a missing name does not ask every member on
// every read.

// outcomeCount is how many findings c's take-over keeps.
func outcomeCount(c *Cluster) int {
	c.takeOver.mu.Lock()
	defer c.takeOver.mu.Unlock()
	return len(c.takeOver.foundNothing)
}

// missingOwnedBy is a bucket name in testDatabase that nobody created and c
// owns.
func missingOwnedBy(t *testing.T, c *Cluster, prefix string) string {
	t.Helper()
	return bucketNamed(t, prefix, func(name string) bool { return c.actsAsOwner(placementOf(testDatabase, name)) })
}

// pauseWorkerOf stops c's take-over worker until the returned resume runs or
// the test ends.
func pauseWorkerOf(t *testing.T, c *Cluster) (resume func()) {
	t.Helper()
	paused := func(nodeID string) bool { return nodeID == c.self.ID }
	takeOverPausedHook.Store(&paused)
	t.Cleanup(func() { takeOverPausedHook.Store(nil) })
	return func() { takeOverPausedHook.Store(nil) }
}

// errorFrameText is the message of an error frame, or the empty string.
func errorFrameText(frame *transport.Frame) string {
	var refusal ErrorResponse
	if frame.Opcode != transport.OpError || bson.Unmarshal(frame.Payload, &refusal) != nil {
		return ""
	}
	return refusal.Message
}

// TestAMissingBucketInTheWindowIsNotFoundOnceEveryMemberAnswers is R3: a name
// that moved here and that no member holds answers not found once every
// member has answered, and a hundred reads of it ask one round of questions,
// not one each. Guard: the read's cache lookup.
func TestAMissingBucketInTheWindowIsNotFoundOnceEveryMemberAnswers(t *testing.T) {
	s := setUpJoinerBeforeAnyPull(t)
	bucket := missingOwnedBy(t, s.joiner, "nowhere")
	asked := countQuestions(t, s.joiner.self.ID)
	for index := 0; index < 100; index++ {
		refusedNotFound(t, fmt.Sprintf("read %d of a name no member holds", index+1),
			s.tc.frameOn(s.joinerNode, transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: "k"}))
	}
	if n, members := asked.Load(), int64(len(s.members)-1); n != members {
		t.Errorf("100 reads of a missing name sent %d questions; want one round, one to each of the %d other members", n, members)
	}
}

// TestAMovedNameThatDoesNotExistWithAMemberStoppedAnswersRetryNamingIt is O-4:
// with a member silent, a read of a moved name nobody else holds answers retry
// naming the member and remove-node, never not found; within
// onDemandUnheardTTL the next read is answered at once from the finding, and
// after it a read asks again. Guard: retry, not "not found", on a partial
// answer.
func TestAMovedNameThatDoesNotExistWithAMemberStoppedAnswersRetryNamingIt(t *testing.T) {
	s := setUpJoinerBeforeAnyPull(t)
	silent := s.members[1].ID
	hook := func(askerID, nodeID string) error {
		if askerID == s.joiner.self.ID && nodeID == silent {
			return errSeamFailsHandOff
		}
		return nil
	}
	askHolderHook.Store(&hook)
	t.Cleanup(func() { askHolderHook.Store(nil) })
	bucket := missingOwnedBy(t, s.joiner, "silentmissing")
	get := FindRequest{Database: testDatabase, Bucket: bucket, Key: "k"}
	refusedAwaitingCopy(t, "the first read with a member silent", s.tc.frameOn(s.joinerNode, transport.OpGet, get))

	// The worker's own rounds re-ask; with it paused only a read could.
	resume := pauseWorkerOf(t, s.joiner)
	started := time.Now()
	second := s.tc.frameOn(s.joinerNode, transport.OpGet, get)
	took := time.Since(started)
	refusedAwaitingCopy(t, "a read within the finding's life", second)
	if text := errorFrameText(second); !strings.Contains(text, silent) || !strings.Contains(text, "remove-node") {
		t.Errorf("a read with a member silent does not name it and remove-node: %s", text)
	}
	if took >= onDemandWait/2 {
		t.Errorf("a read within the finding's life took %s; it is answered from the finding at once", took)
	}

	time.Sleep(onDemandUnheardTTL)
	asked := countQuestions(t, s.joiner.self.ID)
	resume()
	refusedAwaitingCopy(t, "a read after the finding's life", s.tc.frameOn(s.joinerNode, transport.OpGet, get))
	// The read is past the take-over's bound, which read one spent, so it is
	// answered at once; the question it named follows.
	if !eventually(func() bool { return asked.Load() > 0 }) {
		t.Errorf("a read after the finding's life asked nobody; it asks again")
	}
}

// TestTheNothingCacheIsEmptyAfterAnAdoption is R2-7: a "nothing" recorded in
// the window is about the epoch it was found at, and an adoption forgets it.
// Guard: the clear in adoptMembershipLocked.
func TestTheNothingCacheIsEmptyAfterAnAdoption(t *testing.T) {
	s := setUpJoinerBeforeAnyPull(t)
	bucket := missingOwnedBy(t, s.joiner, "adoptmissing")
	refusedNotFound(t, "a read of a name no member holds",
		s.tc.frameOn(s.joinerNode, transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: "k"}))
	if outcomeCount(s.joiner) == 0 {
		t.Fatalf("setup: the take-over recorded no finding for the missing name")
	}
	if err := s.adoptNext(s.members); err != nil {
		t.Fatalf("adopting the next epoch: %v", err)
	}
	if n := outcomeCount(s.joiner); n != 0 {
		t.Errorf("%d findings outlive the adoption", n)
	}
}

// TestTheNothingCacheIsEmptyAfterTheWindowCloses is R2-7: once every member
// has settled, recomputeJoinFreeze publishes the closed window and the
// findings recorded in it go. Guard: the clear in recomputeJoinFreeze.
func TestTheNothingCacheIsEmptyAfterTheWindowCloses(t *testing.T) {
	lowerSweepBounds(t, 10*time.Minute)
	var failing atomic.Bool
	failing.Store(true)
	setAskNodeToMigrateHook(t, func(_, _, _, _ string) error {
		if failing.Load() {
			return errSeamFailsHandOff
		}
		return nil
	})
	s := startSettlingCluster(t)
	s.join(t, 0)
	joiner := s.joiner.server.core.cluster
	if !joiner.readWindowOpen(joiner.Epoch()) {
		t.Fatalf("setup: the joiner's settling window is closed while every hand-off fails")
	}
	bucket := missingOwnedBy(t, joiner, "closemissing")
	// A read before the epoch's placement pass finishes is answered the
	// hand-off's retry (onDemandRead), never from a finding.
	pollUntil(t, "the joiner's placement pass finishes", joiner.placementPassFinished)
	refusedNotFound(t, "a read of a name no member holds",
		s.tc.frameOn(s.joiner, transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: "k"}))
	if outcomeCount(joiner) == 0 {
		t.Fatalf("setup: the take-over recorded no finding for the missing name")
	}
	failing.Store(false)
	pollUntil(t, "every member settles the join at the joiner", func() bool {
		wakeAll(s.members())
		return !joiner.readWindowOpen(joiner.Epoch())
	})
	if n := outcomeCount(joiner); n != 0 {
		t.Errorf("%d findings outlive the settling window", n)
	}
}

// migrateWaiter starts c.awaitOwnCopy for bucket and returns its answer once
// it has taken its start time.
func migrateWaiter(c *Cluster, bucket string) <-chan error {
	answer := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		answer <- c.awaitOwnCopy(ctx, placementOf(testDatabase, bucket), testDatabase, bucket)
	}()
	time.Sleep(200 * time.Millisecond)
	return answer
}

// TestTheWindowCloseClearKeepsALeaveGatedEntry is F-6: a migrate gave leave
// and its take-over found nothing; the window closes before the migrate polls.
// The clear keeps that finding, and the migrate is answered
// ErrMigrateBucketNotFound, not its hand-off refusal at its deadline. Guard:
// the clear skips leave entries.
func TestTheWindowCloseClearKeepsALeaveGatedEntry(t *testing.T) {
	const writeConcern = 3 * time.Second
	tc := newTestClusterTuned(t, 3, 2, nil, func(_ int, c *Cluster) { c.wcTimeout = writeConcern })
	defer tc.stop()
	owner := clusterOf(tc, 0)
	answer := migrateWaiter(owner, "leavegated")
	owner.takeOver.mu.Lock()
	owner.takeOver.foundNothing[placementOf(testDatabase, "leavegated")] = foundOutcome{at: time.Now(), epoch: owner.Epoch(), leave: true}
	owner.clearReadOutcomesLocked()
	owner.takeOver.mu.Unlock()
	select {
	case err := <-answer:
		if !errors.Is(err, ErrMigrateBucketNotFound) {
			t.Errorf("the migrate was answered %v; want the take-over's found-nothing answer", err)
		}
	case <-time.After(writeConcern / 2):
		t.Errorf("the migrate did not hear its take-over found nothing within %s: the window's close forgot it", writeConcern/2)
	}
}

// TestAnUnheardEntryNeverAnswersAMigrateNotFound is R-2: a finding that a
// member did not answer, newer than a migrate's start, is not "no member holds
// a live copy": the migrate is never answered ErrMigrateBucketNotFound from
// it. Guard: awaitOwnCopy takes only a leave-gated "nothing".
func TestAnUnheardEntryNeverAnswersAMigrateNotFound(t *testing.T) {
	const writeConcern = 3 * time.Second
	tc := newTestClusterTuned(t, 3, 2, nil, func(_ int, c *Cluster) { c.wcTimeout = writeConcern })
	defer tc.stop()
	owner := clusterOf(tc, 0)
	answer := migrateWaiter(owner, "unheardonly")
	owner.takeOver.mu.Lock()
	owner.takeOver.foundNothing[placementOf(testDatabase, "unheardonly")] = foundOutcome{at: time.Now(), epoch: owner.Epoch(),
		unheard: []string{tc.addrs[2]}}
	owner.takeOver.mu.Unlock()
	select {
	case err := <-answer:
		if errors.Is(err, ErrMigrateBucketNotFound) {
			t.Errorf("a migrate was answered %v from a finding that a member did not answer", err)
		}
	case <-time.After(writeConcern + 10*time.Second):
		t.Errorf("the migrate was never answered")
	}
}

// TestAReadOfADatabaseThatDoesNotExistWithARemovedNodeIsStillNotFound is the
// BUG-0338 guard with a node removed within the history: the removed node is
// asked and not waited for as a member is, so a read in the window of a name
// it used to own, in a database nobody has, is not found once every member
// answered, never retry; and the next read asks nobody. Guard: the cache.
func TestAReadOfADatabaseThatDoesNotExistWithARemovedNodeIsStillNotFound(t *testing.T) {
	const missing = "neverdb"
	tc := newTestCluster(t, 4, 3)
	t.Cleanup(tc.stop)
	removedID := tc.addrs[3]
	holdSweepStages(t, tc.addrs[0]).hold(InitialEpoch+1, sweepStarting)
	proposeRemovalVia(t, clusterOf(tc, 0), removedID)
	// The reader is whichever of nodes 2 and 3 (node 1's sweep is held) takes
	// over a name the removed node owned: on some rings no partition of the
	// removed node moves to a fixed one of them.
	candidates := []int{1, 2}
	for _, index := range candidates {
		pollUntil(t, fmt.Sprintf("node %d adopts the removal", index+1), func() bool { return clusterOf(tc, index).Epoch() == InitialEpoch+1 })
	}
	founding, recorded := clusterOf(tc, 1).ringAt(InitialEpoch)
	if !recorded {
		t.Fatalf("setup: node 2 does not hold the founding ring")
	}
	readerIndex := -1
	bucket := bucketNamed(t, "removedowned", func(name string) bool {
		key := placementOf(missing, name)
		owner, err := founding.Owner(key.encoded)
		if err != nil || owner.ID != removedID {
			return false
		}
		for _, index := range candidates {
			if clusterOf(tc, index).actsAsOwner(key) {
				readerIndex = index
				return true
			}
		}
		return false
	})
	reader, readerNode := clusterOf(tc, readerIndex), tc.nodes[readerIndex]
	// A read before the epoch's placement pass finishes is answered the
	// hand-off's retry (onDemandRead), never from a finding.
	pollUntil(t, "the reader's placement pass finishes", reader.placementPassFinished)
	if !reader.readWindowOpen(reader.Epoch()) {
		t.Fatalf("setup: the reader's settling window is closed")
	}
	get := FindRequest{Database: missing, Bucket: bucket, Key: "k"}
	refusedNotFound(t, "a read of a name a removed node owned, in a database nobody has", tc.frameOn(readerNode, transport.OpGet, get))
	asked := countQuestions(t, reader.self.ID)
	refusedNotFound(t, "the next read", tc.frameOn(readerNode, transport.OpGet, get))
	if n := asked.Load(); n != 0 {
		t.Errorf("the next read of the missing name asked %d questions; the finding answers it", n)
	}
}

// plantOrphan records a create of bucket as in flight at c (registerCreate's
// orphan), until the test ends.
func plantOrphan(t *testing.T, c *Cluster, bucket string) {
	t.Helper()
	key := heldBucket{database: testDatabase, bucket: bucket}
	c.sweepLeftMu.Lock()
	if c.orphans == nil {
		c.orphans = make(map[heldBucket]uint64)
	}
	c.orphans[key] = c.Epoch()
	c.sweepLeftMu.Unlock()
	t.Cleanup(func() {
		c.sweepLeftMu.Lock()
		delete(c.orphans, key)
		c.sweepLeftMu.Unlock()
	})
}

// hasOutcome reports whether c's take-over keeps a finding for bucket.
func hasOutcome(c *Cluster, bucket string) bool {
	c.takeOver.mu.Lock()
	defer c.takeOver.mu.Unlock()
	_, recorded := c.takeOver.foundNothing[placementOf(testDatabase, bucket)]
	return recorded
}

// TestANothingIsNotCachedWhileACreateIsInFlight is D-10: a founder has a
// create of a name in flight (an orphan), so it answers the question that it
// holds no copy and that a create is in flight. The take-over finds no copy,
// and does not record "nothing": the create is about to land, and a read is
// told to retry, not that the name does not exist. Guard: the joiner reads
// ReplCopy.CreateInFlight.
func TestANothingIsNotCachedWhileACreateIsInFlight(t *testing.T) {
	s := setUpJoinerBeforeAnyPull(t)
	bucket := missingOwnedBy(t, s.joiner, "inflight")
	plantOrphan(t, clusterOf(s.tc, 0), bucket)
	refusedAwaitingCopy(t, "a read of a name whose create is in flight at a founder",
		s.tc.frameOn(s.joinerNode, transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: "k"}))
	if hasOutcome(s.joiner, bucket) {
		t.Errorf("the take-over recorded a finding for a name whose create is in flight")
	}
}

// TestANothingIsNotCachedForThisNodesOwnOrphan is R2-6: the create in flight
// is this node's own. The take-over decides "nothing" and records no finding.
// Guard: completeTakeOver's sawCreateInFlight check.
func TestANothingIsNotCachedForThisNodesOwnOrphan(t *testing.T) {
	s := setUpJoinerBeforeAnyPull(t)
	bucket := missingOwnedBy(t, s.joiner, "ownorphan")
	plantOrphan(t, s.joiner, bucket)
	refusedAwaitingCopy(t, "a read of a name whose create is in flight here",
		s.tc.frameOn(s.joinerNode, transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: "k"}))
	if hasOutcome(s.joiner, bucket) {
		t.Errorf("the take-over recorded a finding for a name whose create is in flight here")
	}
}

// TestAStoppedMemberRecordedDownIsAskedAndCountsAsUnheard is R2-4 under
// owner ruling R2 (2026-10-20): a current member this node records as down is
// still asked, with catalogQuestionWait for its answer; one that does not
// answer is recorded as a member that did not answer, and a read is told to
// retry naming it.
func TestAStoppedMemberRecordedDownIsAskedAndCountsAsUnheard(t *testing.T) {
	s := setUpJoinerBeforeAnyPull(t)
	down := s.members[1].ID
	s.tc.stopNodeKeepingData(1)
	pollUntil(t, "the joiner finds the stopped member down", func() bool {
		s.joiner.wakeGossip()
		s.joiner.peersMu.RLock()
		p := s.joiner.peers[down]
		s.joiner.peersMu.RUnlock()
		_, isDown := p.downSince()
		return isDown
	})
	var asked atomic.Int64
	hook := func(askerID, nodeID string) error {
		if askerID == s.joiner.self.ID && nodeID == down {
			asked.Add(1)
		}
		return nil
	}
	askHolderHook.Store(&hook)
	t.Cleanup(func() { askHolderHook.Store(nil) })
	frame := s.tc.frameOn(s.joinerNode, transport.OpGet, FindRequest{Database: testDatabase, Bucket: s.bucket, Key: "held"})
	refusedAwaitingCopy(t, "a read while a member that may hold the bucket is down", frame)
	if text := errorFrameText(frame); !strings.Contains(text, down) {
		t.Errorf("the refusal does not name the down member %s: %s", down, text)
	}
	if asked.Load() == 0 {
		t.Errorf("the fallback did not ask the member recorded down; it must ask it with a short wait (owner ruling R2)")
	}
	status := s.joiner.takeOverStatusOf(placementOf(testDatabase, s.bucket))
	if !slices.Contains(status.waitingOn, down) {
		t.Errorf("the take-over waits on %v, want the down member %s among them", status.waitingOn, down)
	}
}
