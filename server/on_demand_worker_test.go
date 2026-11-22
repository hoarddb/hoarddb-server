package server

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// One opener (on-demand pull build doc step 7; spec §5): a read names its
// bucket and the worker opens it. Before each completion of a round the worker
// takes the names: one of the round's moves to the front, one outside it runs
// at once in a nested pass on the worker's own goroutine, and whole copies run
// after opens, resumes and nothings.

// workerGate is a one-shot hold a hook parks the worker on: reached closes when the
// worker gets there, and the worker waits until release.
type workerGate struct {
	reached     chan struct{}
	gate        chan struct{}
	reachedOnce sync.Once
	releaseOnce sync.Once
}

func newWorkerGate(t *testing.T) *workerGate {
	t.Helper()
	g := &workerGate{reached: make(chan struct{}), gate: make(chan struct{})}
	t.Cleanup(g.release)
	return g
}

// hold parks the caller until release; only the first caller is parked.
func (g *workerGate) hold() {
	first := false
	g.reachedOnce.Do(func() { first = true; close(g.reached) })
	if first {
		<-g.gate
	}
}

func (g *workerGate) release() { g.releaseOnce.Do(func() { close(g.gate) }) }

// await fails the test unless the worker reaches the gate within limit.
func (g *workerGate) await(t *testing.T, what string, limit time.Duration) {
	t.Helper()
	select {
	case <-g.reached:
	case <-time.After(limit):
		t.Fatalf("setup: the worker never reached %s", what)
	}
}

// onBucketHook stores hook, called with the bucket's name, as the node's
// take-over hook of that kind until the test ends.
func onBucketHook(t *testing.T, seam *atomic.Pointer[func(nodeID, database, bucket string)], nodeID string, hook func(bucket string)) {
	t.Helper()
	wrapped := func(at, _, bucket string) {
		if at == nodeID {
			hook(bucket)
		}
	}
	seam.Store(&wrapped)
	t.Cleanup(func() { seam.Store(nil) })
}

// startRound names buckets to the joiner's worker together, so that one round
// takes them all: the worker is paused while they are named, then woken.
func startRound(t *testing.T, c *Cluster, buckets ...string) {
	t.Helper()
	resume := pauseWorkerOf(t, c)
	for _, bucket := range buckets {
		if !c.wakeTakeOverCapped(testDatabase, bucket) {
			t.Fatalf("setup: %s was refused a place in the take-over queue", bucket)
		}
	}
	resume()
	c.signalTakeOver()
}

// openOrder records the buckets whose completion reaches the open, in order.
type openOrder struct {
	mu      sync.Mutex
	buckets []string
}

func (o *openOrder) add(bucket string) {
	o.mu.Lock()
	o.buckets = append(o.buckets, bucket)
	o.mu.Unlock()
}

func (o *openOrder) list() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.Clone(o.buckets)
}

// servedWithin reports whether c serves bucket from an opened copy within
// limit.
func servedWithin(c *Cluster, bucket string, limit time.Duration) bool {
	return holdsWithin(limit, func() bool { return c.s.bucketServed(testDatabase, bucket) })
}

// TestANameReadMidRoundIsTakenInANestedPass is spec §5's nested pass: a round
// of two buckets is held in its first completion while a read names a third
// bucket outside the round. The third is opened before the round's second
// completion, not in the next round. Guard: the nested pass.
func TestANameReadMidRoundIsTakenInANestedPass(t *testing.T) {
	s := setUpJoinerOwningUnpulled(t, "nested", 3)
	outside, first, second := s.buckets[0], s.buckets[1], s.buckets[2]
	held := newWorkerGate(t)
	var order openOrder
	onBucketHook(t, &takeOverBeforeOpenHook, s.joiner.self.ID, func(bucket string) {
		order.add(bucket)
		if bucket == first {
			held.hold()
		}
	})
	startRound(t, s.joiner, first, second)
	held.await(t, "the open of the round's first bucket", takeOverWait)
	if !s.joiner.wakeTakeOverCapped(testDatabase, outside) {
		t.Fatalf("setup: %s was refused a place in the take-over queue", outside)
	}
	held.release()
	if !servedWithin(s.joiner, second, takeOverWait) || !servedWithin(s.joiner, outside, takeOverWait) {
		t.Fatalf("the joiner did not land every bucket; opens reached %v", order.list())
	}
	if got, want := order.list(), []string{first, outside, second}; !slices.Equal(got, want) {
		t.Errorf("the worker opened %v; a name read mid-round runs in a nested pass before the round's next completion, want %v", got, want)
	}
}

// TestAReadOfABucketClaimedBehindABigCopyIsServedWithinTheBound is spec §5's
// named-first move: a round of two buckets, the first of which is held in its
// copy for longer than a read's bound. A read of the second, named while the
// round claims its buckets, is answered from the copy the worker lands within
// the bound: the named bucket moves to the front of the round's completions.
// Every bucket at a joiner that holds nothing is a whole copy, so both are.
// Guard: the named-first move.
func TestAReadOfABucketClaimedBehindABigCopyIsServedWithinTheBound(t *testing.T) {
	s := setUpJoinerOwningUnpulled(t, "behind", 2)
	big, read := s.buckets[0], s.buckets[1]
	claiming := newWorkerGate(t)
	onBucketHook(t, &takeOverBeforeClaimHook, s.joiner.self.ID, func(bucket string) {
		if bucket == read {
			claiming.hold()
		}
	})
	bigCopy := newWorkerGate(t)
	onBucketHook(t, &takeOverBeforeOpenHook, s.joiner.self.ID, func(bucket string) {
		if bucket == big {
			bigCopy.hold()
		}
	})
	startRound(t, s.joiner, big, read)
	claiming.await(t, "the claim of the read's bucket", takeOverWait)

	key := placementOf(testDatabase, read)
	answered := make(chan onDemandAnswer, 1)
	var took time.Duration
	go func() {
		began := time.Now()
		answer := s.joiner.onDemandRead(context.Background(), transport.OpGet, key, s.joiner.Epoch(), testDatabase, read)
		took = time.Since(began)
		answered <- answer
	}()
	if !holdsWithin(takeOverWait, func() bool {
		s.joiner.takeOver.mu.Lock()
		defer s.joiner.takeOver.mu.Unlock()
		_, named := s.joiner.takeOver.named[key]
		return named
	}) {
		t.Fatalf("setup: the read did not name %s", read)
	}
	claiming.release()
	answer := <-answered
	bigCopy.release()
	if !answer.serve || answer.outcome != onDemandLanded {
		t.Errorf("a read of %s named behind %s's held copy was answered %v (outcome %s) after %s; want served from the landed copy within %s",
			read, big, answer.refusal, answer.outcome, took, onDemandWait)
	}
	if took >= onDemandWait {
		t.Errorf("the read of %s took %s, past its bound of %s", read, took, onDemandWait)
	}
}

// TestTheWorkerIsTheOnlyOpener is spec §5's invariant: a nested bucket reads
// its own copy before it claims it, at every boundary it is named at. Bucket
// B, outside a round of three, is named while the round's first completion is
// held, and its nested pass is held between its read and its claim
// (takeOverBeforeClaimHook) while B is named again — a second nested pass for
// the same bucket. B lands; a write to it is made while the round's second
// completion is held; the round's next boundary runs B's second nested pass.
// That pass finds B's copy opened and leaves it: B is claimed once, and its
// term, its position and the write are unchanged. Guard: each nested pass
// reads B's copy afresh, not from a read taken before the round's last
// completion.
func TestTheWorkerIsTheOnlyOpener(t *testing.T) {
	s := setUpJoinerOwningUnpulled(t, "opener", 4)
	nested, first, second, third := s.buckets[0], s.buckets[1], s.buckets[2], s.buckets[3]
	ref := ReplBucketRef{Database: testDatabase, Bucket: nested}

	var claims atomic.Int64
	claiming := newWorkerGate(t)
	onBucketHook(t, &takeOverBeforeClaimHook, s.joiner.self.ID, func(bucket string) {
		if bucket == nested {
			claims.Add(1)
			claiming.hold()
		}
	})
	firstHeld, secondHeld := newWorkerGate(t), newWorkerGate(t)
	onBucketHook(t, &takeOverBeforeOpenHook, s.joiner.self.ID, func(bucket string) {
		switch bucket {
		case first:
			firstHeld.hold()
		case second:
			secondHeld.hold()
		}
	})
	startRound(t, s.joiner, first, second, third)
	firstHeld.await(t, "the open of the round's first bucket", takeOverWait)
	s.joiner.wakeTakeOverCapped(testDatabase, nested)
	firstHeld.release()

	claiming.await(t, "the nested pass's claim", takeOverWait)
	if s.joiner.s.bucketStoreOpen(testDatabase, nested) {
		t.Errorf("%s was opened between its nested pass's read and its claim", nested)
	}
	s.joiner.wakeTakeOverCapped(testDatabase, nested)
	claiming.release()

	secondHeld.await(t, "the open of the round's second bucket", takeOverWait)
	landed, err := s.joiner.answerCopy(ref)
	if err != nil || !landed.Holds || !landed.Opened {
		t.Fatalf("%s did not land before the round's second completion (copy %+v, err %v)", nested, landed, err)
	}
	answered(t, "a put of \"after\" to the landed copy", s.tc.frameOn(s.joinerNode, transport.OpPut,
		InsertRequest{Database: testDatabase, Bucket: nested, Key: "after", Data: map[string]interface{}{"n": 2}}))
	written, err := s.joiner.answerCopy(ref)
	if err != nil {
		t.Fatalf("reading %s's copy after the write: %v", nested, err)
	}
	secondHeld.release()

	if !servedWithin(s.joiner, third, takeOverWait) {
		t.Fatalf("the round's third bucket never landed")
	}
	if n := claims.Load(); n != 1 {
		t.Errorf("%s was claimed %d times; a nested pass that reads its opened copy afresh leaves it unclaimed", nested, n)
	}
	after, err := s.joiner.answerCopy(ref)
	if err != nil {
		t.Fatalf("reading %s's copy after the round: %v", nested, err)
	}
	if after.Term != written.Term || after.Through != written.Through {
		t.Errorf("%s's copy moved from term %d through %d to term %d through %d after it was opened", nested,
			written.Term, written.Through, after.Term, after.Through)
	}
	for _, docKey := range []string{"held", "after"} {
		foundKey(t, s, nested, docKey)
	}
}

// foundKey fails the test unless a get of key in bucket at the joiner finds it.
func foundKey(t *testing.T, s joinerOwningUnpulled, bucket, key string) {
	t.Helper()
	frame := s.tc.frameOn(s.joinerNode, transport.OpGet, FindRequest{Database: testDatabase, Bucket: bucket, Key: key})
	what := fmt.Sprintf("a get of %q in %s at the joiner", key, bucket)
	answered(t, what, frame)
	var found FindResponse
	if err := bson.Unmarshal(frame.Payload, &found); err != nil || !found.Found {
		t.Errorf("%s did not find it (err %v): %s", what, err, frame.Payload)
	}
}

// TestANestedBucketLeftPendingIsRenamedWithoutASecondRead is ODP-5: a bucket
// named once, mid-round, whose nested pass cannot decide (a member is silent
// from the round's first completion on) is put back in the queue after the
// round, so the next round reads it again with no read naming it a second
// time. Guard: the nested pass's re-insert.
func TestANestedBucketLeftPendingIsRenamedWithoutASecondRead(t *testing.T) {
	s := setUpJoinerOwningUnpulled(t, "pending", 3)
	nested, first, second := s.buckets[0], s.buckets[1], s.buckets[2]
	silent := s.members[1].ID
	var silenced atomic.Bool
	ask := func(askerID, nodeID string) error {
		if silenced.Load() && askerID == s.joiner.self.ID && nodeID == silent {
			return fmt.Errorf("test: %s is silent", silent)
		}
		return nil
	}
	askHolderHook.Store(&ask)
	t.Cleanup(func() { askHolderHook.Store(nil) })
	var reads atomic.Int64
	onBucketHook(t, &takeOverBeforeReadHook, s.joiner.self.ID, func(bucket string) {
		if bucket == nested {
			reads.Add(1)
		}
	})
	held := newWorkerGate(t)
	onBucketHook(t, &takeOverBeforeOpenHook, s.joiner.self.ID, func(bucket string) {
		if bucket == first {
			held.hold()
		}
	})
	startRound(t, s.joiner, first, second)
	held.await(t, "the open of the round's first bucket", takeOverWait)
	silenced.Store(true)
	s.joiner.wakeTakeOverCapped(testDatabase, nested)
	held.release()
	if !servedWithin(s.joiner, second, takeOverWait) {
		t.Fatalf("the round's second bucket never landed")
	}
	if !holdsWithin(takeOverWait, func() bool { return reads.Load() >= 2 }) {
		t.Errorf("%s, left undecided by its nested pass, was read %d times; it must go back in the queue and be read again with no second read naming it",
			nested, reads.Load())
	}
	if s.joiner.s.bucketStoreOpen(testDatabase, nested) {
		t.Errorf("%s was taken over though a member it must hear from is silent", nested)
	}
}

// TestNameToOpenLatencyWithABusyWorker is spec §5's third row, measured: the
// worker is landing copies of two hundred buckets in one round, and buckets
// outside the round are named one at a time; each is served at most one
// completion plus its own nested pass later. The p99 from naming to served
// must be at most 50 ms on loopback (P2 measured 4.67 ms at an idle worker).
// Copies of empty buckets land in well under a millisecond on loopback, so
// each of the round's completions is made to cost busyCompletion more, or the
// round ends before the probes and the worker measured is idle.
func TestNameToOpenLatencyWithABusyWorker(t *testing.T) {
	const busy, probes = 200, 50
	const busyCompletion = 2 * time.Millisecond
	s := setUpJoinerOwningUnpulled(t, "latency", busy+probes)
	round, probed := s.buckets[probes:], s.buckets[:probes]
	inRound := make(map[string]bool, len(round))
	for _, bucket := range round {
		inRound[bucket] = true
	}
	onBucketHook(t, &takeOverBeforeOpenHook, s.joiner.self.ID, func(bucket string) {
		if inRound[bucket] {
			time.Sleep(busyCompletion)
		}
	})
	startRound(t, s.joiner, round...)
	if !holdsWithin(takeOverWait, func() bool { return s.joiner.s.bucketServed(testDatabase, round[0]) }) {
		t.Fatalf("setup: the round's first bucket never landed")
	}
	var latencies []time.Duration
	stillBusy := 0
	for _, bucket := range probed {
		if !s.joiner.s.bucketServed(testDatabase, round[len(round)-1]) {
			stillBusy++
		}
		named := time.Now()
		if !s.joiner.wakeTakeOverCapped(testDatabase, bucket) {
			t.Fatalf("setup: %s was refused a place in the take-over queue", bucket)
		}
		for !s.joiner.s.bucketServed(testDatabase, bucket) {
			if time.Since(named) > takeOverWait {
				t.Fatalf("%s was never served after it was named", bucket)
			}
			time.Sleep(100 * time.Microsecond)
		}
		latencies = append(latencies, time.Since(named))
	}
	slices.Sort(latencies)
	p99 := latencies[(len(latencies)*99+99)/100-1]
	t.Logf("naming to served with the worker busy on %d copies of %s each (%d of %d probes while it was busy): p50 %s, p99 %s, max %s (P2, idle: 4.67ms)",
		busy, busyCompletion, stillBusy, probes, latencies[len(latencies)/2], p99, latencies[len(latencies)-1])
	if stillBusy < probes/2 {
		t.Errorf("setup: only %d of %d probes were named while the round was still landing copies", stillBusy, probes)
	}
	// A wall-clock bound steps aside under the race detector (the same idiom as filter_cost_test.go
	// and the sighting recorded in BUG-0487): with 50 probes p99 is the worst one, and the detector
	// on a loaded host puts a single scheduling stall past 50 ms. The plain run keeps the bound.
	if !raceEnabled && p99 > 50*time.Millisecond {
		t.Errorf("naming to served p99 is %s with a busy worker, over the bound of 50ms", p99)
	}
}

// TestAWaitingReadNamesItsBucketAgainEachStep is step 7's note on the cap: a
// read waiting in onDemandRead names its bucket again every takeOverWaitStep,
// as awaitTakeOver does, so a round that drops its name at the cap costs it
// one step, not its bound. Guard: the re-naming in the read's wait.
func TestAWaitingReadNamesItsBucketAgainEachStep(t *testing.T) {
	c := pausedWorker(t)
	const bucket = "waitingread"
	key := placementOf(testDatabase, bucket)
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.onDemandRead(context.Background(), transport.OpGet, key, c.Epoch(), testDatabase, bucket)
	}()
	queued := func() bool {
		c.takeOver.mu.Lock()
		defer c.takeOver.mu.Unlock()
		_, named := c.takeOver.named[key]
		return named
	}
	if !holdsWithin(time.Second, queued) {
		t.Fatalf("setup: the read did not name %s", bucket)
	}
	// What requeueUnsettled does to a read-named name at the cap.
	c.takeOver.mu.Lock()
	delete(c.takeOver.named, key)
	c.takeOver.mu.Unlock()
	if !holdsWithin(3*takeOverWaitStep, queued) {
		t.Errorf("a waiting read whose name was dropped did not name %s again within %s", bucket, 3*takeOverWaitStep)
	}
	<-done
}
