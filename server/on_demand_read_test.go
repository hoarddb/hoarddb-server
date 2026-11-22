package server

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The on-demand read (on-demand pull build doc, step 1; BUG-0340): a read at
// an owner without its copy, in the settling window, names the bucket to the
// take-over worker, waits a bound, and answers from the copy it lands.

// countQuestions counts the copy questions node askerID sends from now until
// the test ends (askCopiesOnceHook).
func countQuestions(t *testing.T, askerID string) *atomic.Int64 {
	t.Helper()
	var asked atomic.Int64
	hook := func(nodeID string, _ int) {
		if nodeID == askerID {
			asked.Add(1)
		}
	}
	askCopiesOnceHook.Store(&hook)
	t.Cleanup(func() { askCopiesOnceHook.Store(nil) })
	return &asked
}

// joinerBeforeAnyPull is a joiner that owns a hash bucket holding one document
// and has pulled nothing: the founders' streams to it are held, so no replay
// creates the bucket there, and every founder's hand-off to it fails, so no
// pull starts. Auto-rebalance is on, so the joiner's own take-over may copy.
type joinerBeforeAnyPull struct {
	joinerWithoutCopies
	bucket string
}

func setUpJoinerBeforeAnyPull(t *testing.T) joinerBeforeAnyPull {
	t.Helper()
	s := setUpJoinerOwningUnpulled(t, "ondemand", 1)
	return joinerBeforeAnyPull{joinerWithoutCopies: s.joinerWithoutCopies, bucket: s.buckets[0]}
}

// joinerOwningUnpulled is joinerBeforeAnyPull with count buckets, in name
// order; the first holds the document "held", the others are empty.
type joinerOwningUnpulled struct {
	joinerWithoutCopies
	buckets []string
}

func setUpJoinerOwningUnpulled(t *testing.T, prefix string, count int) joinerOwningUnpulled {
	t.Helper()
	return setUpJoinerOwningUnpulledWith(t, joinerSetup{prefix: prefix, count: count})
}

// joinerSetup shapes setUpJoinerOwningUnpulledWith: also, when set, narrows the
// joiner's buckets to names it accepts on the membership the join makes
// (joiner last), and joinerOptions configure the joiner. storeType is the
// buckets' type (hash by default; a FIFO's first bucket holds three
// elements). How the joiner adopts its join: by default its gossip
// reconciliation is skipped and the test hands it the proposer's membership
// with no history (adoptFrom), so it holds no record of the epoch before its
// join and its take-over decides by the fallback, which the tests written for
// steps 1 to 8 rely on (a silent member leaves a take-over undecided);
// adoptByGossip waits for a member's gossip answer, which carries that record
// (step 9); newcomerFirst starts the joiner before the join is proposed and
// skips its gossip, so it adopts from the join's commit.
type joinerSetup struct {
	prefix        string
	count         int
	also          func(joined []ring.Node, name string) bool
	joinerOptions []func(*config.Config)
	storeType     BucketType
	adoptByGossip bool
	newcomerFirst bool
	// reseed, with also, gives the later node also places names for fresh
	// ports when no name fits both rings: some port draws leave no partition
	// the joiner owns and the later node takes from it.
	reseed func()
}

func setUpJoinerOwningUnpulledWith(t *testing.T, setup joinerSetup) joinerOwningUnpulled {
	t.Helper()
	prefix, count := setup.prefix, setup.count
	var failing atomic.Bool
	failing.Store(true)
	setAskNodeToMigrateHook(t, func(_, _, _, _ string) error {
		if failing.Load() {
			return errSeamFailsHandOff
		}
		return nil
	})
	tc := newTestClusterWith(t, 2, 3, func(_ int, cfg *config.Config) { cfg.Server.WALRetentionSeconds = 0 })
	t.Cleanup(tc.stop)
	ports := reserveNodePorts(t)
	joined := append(append([]ring.Node{}, tc.nodes[0].server.core.cluster.Members()...), ports.ringNode())
	after := ringOf(t, joined, 3)
	joinerID := joined[len(joined)-1].ID
	wanted := func(name string) bool {
		return ownerOn(after, name) == joinerID && (setup.also == nil || setup.also(joined, name))
	}
	var buckets []string
	if count == 1 && setup.also == nil {
		buckets = []string{bucketNamed(t, prefix, wanted)}
	}
	// A filter over two rings can leave few names in 2,000; search further.
	for index, reseeds := 0, 0; len(buckets) < count; index++ {
		if index > 100*count+20000 && setup.reseed != nil && reseeds < 5 {
			setup.reseed()
			index, reseeds = 0, reseeds+1
		}
		if index > 100*count+20000 {
			t.Fatalf("setup: fewer than %d names of the form %s%%05d are owned by the joiner", count, prefix)
		}
		if name := fmt.Sprintf("%s%05d", prefix, index); wanted(name) {
			buckets = append(buckets, name)
		}
	}
	storeType := setup.storeType
	if storeType == "" {
		storeType = BucketTypeHash
	}
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	for _, bucket := range buckets {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: bucket, Type: storeType})
	}
	for _, bucket := range buckets {
		waitForServing(t, tc, testDatabase, bucket, 2)
	}
	if storeType == BucketTypeFIFO {
		for n := 1; n <= 3; n++ {
			tc.ok(0, transport.OpFIFOPush, PushRequest{Database: testDatabase, Bucket: buckets[0], Data: map[string]interface{}{"n": n}, WriteConcern: "majority"})
		}
	} else {
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: buckets[0], Key: "held",
			Data: map[string]interface{}{"n": 1}, WriteConcern: "majority"})
	}

	proposer := tc.nodes[0].server.core.cluster
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if !setup.adoptByGossip {
		skipGossipOf(t, ports.ringAddr)
	}
	var joinerNode *testNode
	if setup.newcomerFirst {
		joinerNode = startJoinTarget(t, tc, ports, tc.token, setup.joinerOptions...)
	}
	if err := proposer.proposeJoin(ctx, ports.ringNode()); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	// Held once the join is committed, on the peer the members ship to the
	// joiner with from then on.
	for _, node := range tc.nodes {
		t.Cleanup(holdStreamPushesOnly(node.server.core.cluster.peerFor(ports.ringNode())))
	}
	if !setup.newcomerFirst {
		joinerNode = startJoinTarget(t, tc, ports, tc.token, setup.joinerOptions...)
	}
	t.Cleanup(func() { _ = joinerNode.client.Close() })
	joiner := joinerNode.server.core.cluster
	switch {
	case setup.adoptByGossip, setup.newcomerFirst:
		adoptedByGossip(t, joiner, proposer.Epoch())
	default:
		adoptFrom(t, joiner, proposer)
	}
	// A read before the epoch's placement pass has finished is answered the
	// hand-off's retry at once (N-1); every test here starts after it.
	pollUntil(t, "the joiner's placement pass finished", joiner.placementPassFinished)
	for _, bucket := range buckets {
		if !joiner.actsAsOwner(placementOf(testDatabase, bucket)) {
			t.Fatalf("setup: the joiner does not own %s", bucket)
		}
		if joiner.s.bucketStoreOpen(testDatabase, bucket) || joiner.pullPending(testDatabase, bucket) {
			t.Fatalf("setup: the joiner holds or pulls %s before any read", bucket)
		}
	}
	return joinerOwningUnpulled{
		joinerWithoutCopies: joinerWithoutCopies{tc: tc, joiner: joiner, joinerNode: joinerNode, members: joined},
		buckets:             buckets,
	}
}

// foundHeld fails the test unless frame is a get that found the document
// "held".
func foundHeld(t *testing.T, what string, frame *transport.Frame) {
	t.Helper()
	answered(t, what, frame)
	var found FindResponse
	if err := bson.Unmarshal(frame.Payload, &found); err != nil || !found.Found {
		t.Errorf("%s did not find the document (err %v): %s", what, err, frame.Payload)
	}
}

// refusedNotFound fails the test unless frame is a "not found" refusal, not a
// retry.
func refusedNotFound(t *testing.T, what string, frame *transport.Frame) {
	t.Helper()
	var refusal ErrorResponse
	if frame.Opcode != transport.OpError || bson.Unmarshal(frame.Payload, &refusal) != nil {
		t.Errorf("%s was answered %s, want not found: %s", what, frame.Opcode, frame.Payload)
		return
	}
	if transport.ErrorCode(refusal.Code) == transport.ErrUnavailable || !strings.Contains(refusal.Message, "not found") {
		t.Errorf("%s was answered code %d, want not found: %s", what, refusal.Code, refusal.Message)
	}
}

// TestAReadAtAJoinerBeforeItsPullStartsAnswersFromTheBucket is BUG-0340's
// first criterion: a joiner owns a bucket whose pull has not started. A get
// there names the bucket to the take-over worker, which copies it in, and the
// get is answered from the copy, which is kept. On the starting tree it was
// answered "database not found ... create database". Guard: §3.3's moved
// test.
func TestAReadAtAJoinerBeforeItsPullStartsAnswersFromTheBucket(t *testing.T) {
	s := setUpJoinerBeforeAnyPull(t)
	get := FindRequest{Database: testDatabase, Bucket: s.bucket, Key: "held"}
	foundHeld(t, "a get at the joiner before its pull", s.tc.frameOn(s.joinerNode, transport.OpGet, get))
	if !s.joiner.s.bucketStoreOpen(testDatabase, s.bucket) {
		t.Fatalf("the joiner did not keep the copy the read landed")
	}
	foundHeld(t, "a second get at the joiner", s.tc.frameOn(s.joinerNode, transport.OpGet, get))
}

// TestAReadBeforeThePlacementPassFinishesAnswersRetry is N-1: before the
// placement pass of its epoch has finished, the worker takes nothing over, so
// a read in the window names its bucket and answers retry at once, never "not
// found". On the starting tree it was answered "database not found". Guard:
// the placementPassFinished check in onDemandRead.
func TestAReadBeforeThePlacementPassFinishesAnswersRetry(t *testing.T) {
	s := setUpJoinerBeforeAnyPull(t)
	epoch := s.joiner.Epoch()
	s.joiner.placementMu.Lock()
	finished := s.joiner.passFinished
	s.joiner.passFinished = epoch - 1
	s.joiner.placementMu.Unlock()
	t.Cleanup(func() {
		s.joiner.placementMu.Lock()
		s.joiner.passFinished = finished
		s.joiner.placementMu.Unlock()
	})
	started := time.Now()
	frame := s.tc.frameOn(s.joinerNode, transport.OpGet, FindRequest{Database: testDatabase, Bucket: s.bucket, Key: "held"})
	refusedAwaitingCopy(t, "a get before the placement pass finished", frame)
	if took := time.Since(started); took >= onDemandWait {
		t.Errorf("a read before the placement pass finished took %s; it answers retry at once, without the bound", took)
	}
}

// TestAnUnmovedMissingBucketAsksNobodyInTheWindow is §3.3: in the window, a
// name this node already owned on the settled floor's ring and holds nothing
// of did not exist here when the window opened. It answers not found and asks
// nobody. A pin: green on the starting tree. Guard: the moved test.
func TestAnUnmovedMissingBucketAsksNobodyInTheWindow(t *testing.T) {
	s := setUpJoinerBeforeAnyPull(t)
	founder := s.tc.nodes[0].server.core.cluster
	if !founder.readWindowOpen(founder.Epoch()) {
		t.Fatalf("setup: the founder's settling window is closed")
	}
	// Until the founder has heard every member's Settled, its floor is 0, an
	// epoch no ring is recorded at, and every name counts as moved.
	var floor *ring.Ring
	pollUntil(t, "the founder's settled floor is an epoch it holds the ring of", func() bool {
		founder.wakeGossip()
		var recorded bool
		floor, recorded = founder.ringAt(founder.joinFreeze.Load().SettledAt)
		return recorded
	})
	bucket := bucketNamed(t, "unmoved", func(name string) bool {
		key := placementOf(testDatabase, name)
		owner, err := floor.Owner(key.encoded)
		return err == nil && owner.ID == founder.self.ID && founder.actsAsOwner(key)
	})
	asked := countQuestions(t, founder.self.ID)
	refusedNotFound(t, "a get of an unmoved missing bucket in the window",
		s.tc.frameOn(s.tc.nodes[0], transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: "k"}))
	if n := asked.Load(); n != 0 {
		t.Errorf("an unmoved missing bucket asked %d questions; it asks nobody", n)
	}
}

// TestASettledNodeAnswersNotFoundWithoutAQuestion is BUG-0340's third
// criterion: once every member has settled the join, a bucket that does not
// exist answers not found at once, and no question is sent. A pin: green on
// the starting tree.
func TestASettledNodeAnswersNotFoundWithoutAQuestion(t *testing.T) {
	s := startSettlingCluster(t)
	s.join(t, 0)
	joiner := s.joiner.server.core.cluster
	pollUntil(t, "every member settles the join at the joiner", func() bool {
		wakeAll(s.members())
		return !joiner.readWindowOpen(joiner.Epoch())
	})
	bucket := bucketNamed(t, "settledmissing", func(name string) bool {
		return joiner.actsAsOwner(placementOf(testDatabase, name))
	})
	asked := countQuestions(t, joiner.self.ID)
	refusedNotFound(t, "a get of a missing bucket at a settled node",
		s.tc.frameOn(s.joiner, transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: "k"}))
	if n := asked.Load(); n != 0 {
		t.Errorf("a settled node asked %d questions for a missing bucket; it asks nobody", n)
	}
}

// pausedWorker is a one-node cluster whose take-over worker is paused, so
// nothing drains takeOver.named while a test fills it.
func pausedWorker(t *testing.T) *Cluster {
	t.Helper()
	tc := newTestCluster(t, 1, 1)
	t.Cleanup(tc.stop)
	c := clusterOf(tc, 0)
	paused := func(nodeID string) bool { return nodeID == c.self.ID }
	takeOverPausedHook.Store(&paused)
	t.Cleanup(func() { takeOverPausedHook.Store(nil) })
	return c
}

// fillNamed puts count distinct names in c's take-over queue.
func fillNamed(c *Cluster, count int) {
	c.takeOver.mu.Lock()
	defer c.takeOver.mu.Unlock()
	for index := 0; index < count; index++ {
		bucket := fmt.Sprintf("filler%05d", index)
		c.takeOver.named[placementOf(testDatabase, bucket)] = heldBucket{database: testDatabase, bucket: bucket}
	}
}

// namedCount is the length of c's take-over queue.
func namedCount(c *Cluster) int {
	c.takeOver.mu.Lock()
	defer c.takeOver.mu.Unlock()
	return len(c.takeOver.named)
}

// TestConcurrentReadsAtTheNamedCapInsertOnce is R2-2: the queue holds
// onDemandNamedCap − 1 names. One read is parked between its length check and
// its insert; a second read of another name must not finish while it is
// parked (both hold takeOver.mu across the check and the insert), and once
// both are done the queue holds exactly the cap and exactly one read was
// refused. Guard: the check and the insert in one takeOver.mu section.
func TestConcurrentReadsAtTheNamedCapInsertOnce(t *testing.T) {
	c := pausedWorker(t)
	fillNamed(c, onDemandNamedCap-1)
	parked, release := make(chan struct{}), make(chan struct{})
	var fired atomic.Int64
	hook := func(_, _, bucket string) {
		if bucket == "firstread" && fired.Add(1) == 1 {
			close(parked)
			<-release
		}
	}
	takeOverBeforeNamedInsertHook.Store(&hook)
	t.Cleanup(func() { takeOverBeforeNamedInsertHook.Store(nil) })

	var refused atomic.Int64
	firstDone, secondDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(firstDone)
		if !c.wakeTakeOverCapped(testDatabase, "firstread") {
			refused.Add(1)
		}
	}()
	<-parked
	go func() {
		defer close(secondDone)
		if !c.wakeTakeOverCapped(testDatabase, "secondread") {
			refused.Add(1)
		}
	}()
	select {
	case <-secondDone:
		t.Errorf("the second read finished while the first was parked between its check and its insert")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	<-firstDone
	<-secondDone
	if n := namedCount(c); n > onDemandNamedCap {
		t.Errorf("the take-over queue holds %d names, over the cap of %d", n, onDemandNamedCap)
	}
	if n := refused.Load(); n != 1 {
		t.Errorf("%d reads were refused at the cap, want exactly 1", n)
	}
}

// TestTenThousandDistinctNamesKeepTheQueueBounded is §4.3: ten thousand reads
// of distinct missing names, with the worker taking nothing (paused, the worst
// case for the queue), add at most onDemandNamedCap names; every read past the
// cap is answered at once, without the bound's wait. Guard: the cap.
func TestTenThousandDistinctNamesKeepTheQueueBounded(t *testing.T) {
	c := pausedWorker(t)
	const reads = 10000
	epoch := c.Epoch()
	var atOnce atomic.Int64
	var wg sync.WaitGroup
	for index := 0; index < reads; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bucket := fmt.Sprintf("missing%05d", index)
			started := time.Now()
			answer := c.onDemandRead(context.Background(), transport.OpGet, placementOf(testDatabase, bucket), epoch, testDatabase, bucket)
			if answer.refusal == nil {
				t.Errorf("a read of missing %s was not refused", bucket)
			}
			if time.Since(started) < onDemandWait/2 {
				atOnce.Add(1)
			}
		}()
	}
	wg.Wait()
	if n := namedCount(c); n > onDemandNamedCap {
		t.Errorf("%d reads of distinct names left %d names in the take-over queue, over the cap of %d", reads, n, onDemandNamedCap)
	}
	if n := atOnce.Load(); n < reads-onDemandNamedCap {
		t.Errorf("%d reads were answered at once, want at least the %d past the cap", n, reads-onDemandNamedCap)
	}
}

// TestDistinctNameReadsWithTheWorkerRunningKeepTheQueueBounded is §4.3 with
// the worker running (owner ruling R3, 2026-10-20): one member is silent, so
// every read-named missing name stays unsettled round after round. For at
// least eight seconds reads name distinct moved, missing names; the take-over
// queue never holds more than onDemandNamedCap names. Guard: takeOverRound
// puts an unsettled name back only while the queue has room, unless it is
// marked, a migrate's leave or a holder's wait.
func TestDistinctNameReadsWithTheWorkerRunningKeepTheQueueBounded(t *testing.T) {
	s := setUpJoinerBeforeAnyPull(t)
	silent := s.members[1].ID
	hook := func(askerID, nodeID string) error {
		if askerID == s.joiner.self.ID && nodeID == silent {
			return fmt.Errorf("test: %s is silent", silent)
		}
		return nil
	}
	askHolderHook.Store(&hook)
	t.Cleanup(func() { askHolderHook.Store(nil) })

	var largest, largestAfterRound atomic.Int64
	afterRound := func(namedAfter int) {
		if n := int64(namedAfter); n > largestAfterRound.Load() {
			largestAfterRound.Store(n)
		}
	}
	requeueUnsettledDoneHook.Store(&afterRound)
	t.Cleanup(func() { requeueUnsettledDoneHook.Store(nil) })
	sampled := make(chan struct{})
	stopSampling := make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			if n := int64(namedCount(s.joiner)); n > largest.Load() {
				largest.Store(n)
			}
			select {
			case <-stopSampling:
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()
	const storm = 8 * time.Second
	named, refused := 0, 0
	until := time.Now().Add(storm)
	for index := 0; time.Now().Before(until); index++ {
		bucket := fmt.Sprintf("storm%07d", index)
		if !s.joiner.actsAsOwner(placementOf(testDatabase, bucket)) {
			continue
		}
		if s.joiner.wakeTakeOverCapped(testDatabase, bucket) {
			named++
			continue
		}
		refused++
		time.Sleep(time.Millisecond)
	}
	close(stopSampling)
	<-sampled
	if n := int64(namedCount(s.joiner)); n > largest.Load() {
		largest.Store(n)
	}
	t.Logf("%s of distinct-name reads with a member silent: %d names named, %d refused at the cap, the queue reached %d (cap %d)",
		storm, named, refused, max(largest.Load(), largestAfterRound.Load()), onDemandNamedCap)
	if named <= 2*onDemandNamedCap {
		t.Fatalf("test setup: only %d names were named in %s; the worker did not take rounds, so the queue was never re-filled", named, storm)
	}
	if n := largest.Load(); n > onDemandNamedCap {
		t.Errorf("the take-over queue reached %d names with the worker running, over the cap of %d", n, onDemandNamedCap)
	}
	// The sampler misses the moment a round re-inserts its names: the queue is
	// over the cap only inside that one lock section, so the hook reads it there.
	if n := largestAfterRound.Load(); n > onDemandNamedCap {
		t.Errorf("a round's re-insert left %d names in the take-over queue, over the cap of %d", n, onDemandNamedCap)
	}
}

// TestRequeueAtTheCapKeepsMarkedLeftAndWaitedNamesAndDropsReadNamed is owner
// ruling R3's other half: with the queue at the cap, a name in c.unsettled, a
// name with a migrate leave and a name with a holder wait always go back, and
// an ordinary read-named name does not (and loses its status and bound).
// Guard: the marked/leave/waiting branch of requeueUnsettled.
func TestRequeueAtTheCapKeepsMarkedLeftAndWaitedNamesAndDropsReadNamed(t *testing.T) {
	c := pausedWorker(t)
	fillNamed(c, onDemandNamedCap)
	held := func(bucket string) heldBucket { return heldBucket{database: testDatabase, bucket: bucket} }
	markedName, leaveName, waitedName, readName := held("marked"), held("leave"), held("waited"), held("readnamed")
	key := func(h heldBucket) placementKey { return placementOf(h.database, h.bucket) }

	c.placementMu.Lock()
	c.unsettled[key(markedName)] = placementMarks{}
	c.noteUnsettledLocked()
	c.placementMu.Unlock()
	c.takeOver.mu.Lock()
	c.takeOver.leave[key(leaveName)] = true
	c.takeOver.waiting[key(waitedName)] = originWait{began: time.Now()}
	c.takeOver.status[key(readName)] = takeOverStatus{}
	c.takeOver.bounds[key(readName)] = waitBound{}
	c.takeOver.mu.Unlock()

	c.requeueUnsettled([]heldBucket{markedName, leaveName, waitedName, readName})

	c.takeOver.mu.Lock()
	defer c.takeOver.mu.Unlock()
	for name, want := range map[string]struct {
		held     heldBucket
		wantBack bool
	}{
		"a name marked unsettled": {markedName, true},
		"a name with a leave":     {leaveName, true},
		"a name with a wait":      {waitedName, true},
		"a read-named name":       {readName, false},
	} {
		_, back := c.takeOver.named[key(want.held)]
		if back != want.wantBack {
			t.Errorf("%s at the cap: back in the queue = %v, want %v", name, back, want.wantBack)
		}
	}
	if _, kept := c.takeOver.status[key(readName)]; kept {
		t.Errorf("the dropped read-named name kept its status")
	}
}

// TestTheUnheardFindingLivesAtLeastAsLongAsTheCatalogQuestionWait pins the two
// 2 s constants together: the build doc's owner ruling R2 justifies the short
// wait for a member recorded down by the finding outliving one question.
func TestTheUnheardFindingLivesAtLeastAsLongAsTheCatalogQuestionWait(t *testing.T) {
	if onDemandUnheardTTL < catalogQuestionWait {
		t.Errorf("onDemandUnheardTTL %s is shorter than catalogQuestionWait %s", onDemandUnheardTTL, catalogQuestionWait)
	}
}
