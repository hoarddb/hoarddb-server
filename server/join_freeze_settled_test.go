package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Join-freeze build step 5: a node's Settled epoch, the generation-keyed sweep
// residue, the single-flight rearm, orphan creates, and the sweep a start
// resumes (join-freeze spec §3.1, §8.1).

// settlingBuckets is how many buckets settlingCluster creates.
const settlingBuckets = 6

// settlingCluster is three members at factor 3 holding settlingBuckets
// buckets of one document each, and a fourth node reserved to join them.
type settlingCluster struct {
	tc      *testCluster
	ports   *testNodePorts
	joiner  *testNode
	buckets []string
}

// startSettlingCluster starts the members and their buckets; the joiner is
// started by join.
func startSettlingCluster(t *testing.T) *settlingCluster {
	t.Helper()
	return startSettlingClusterPlaced(t, func([]string, string) bool { return true })
}

// startSettlingClusterPlaced is startSettlingCluster with the joiner at a ring
// address for which fits, given the founders' addresses, holds.
func startSettlingClusterPlaced(t *testing.T, fits func(founders []string, joiner string) bool) *settlingCluster {
	t.Helper()
	tc := newTestCluster(t, 3, 3)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	s := &settlingCluster{tc: tc, ports: reserveJoinerPlaced(t, func(joiner string) bool { return fits(tc.addrs, joiner) })}
	for index := 0; index < settlingBuckets; index++ {
		name := fmt.Sprintf("settle%03d", index)
		s.buckets = append(s.buckets, name)
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: name, Key: "doc", Data: map[string]interface{}{"n": index}})
	}
	return s
}

// joinedRing is the placement ring once the joiner is a member.
func (s *settlingCluster) joinedRing(t *testing.T) *ring.Ring {
	t.Helper()
	return ringOf(t, append(clusterOf(s.tc, 0).Members(), s.ports.ringNode()), 3)
}

// keptBucket creates a bucket that node index stays a member of at the join
// and that the joiner joins the set of: the node's hand-off of it asks the
// joiner, and no other node's hand-off releases the node's copy.
func (s *settlingCluster) keptBucket(t *testing.T, index int, prefix string) string {
	t.Helper()
	joined := s.joinedRing(t)
	name := bucketNamed(t, prefix, func(name string) bool {
		set, err := joined.Replicas(placementOf(testDatabase, name).encoded, 3)
		return err == nil && containsID(nodeIDs(set), s.tc.addrs[index]) && containsID(nodeIDs(set), s.ports.ringAddr)
	})
	s.tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
	s.tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: name, Key: "doc", Data: map[string]interface{}{"n": 1}})
	waitForServing(t, s.tc, testDatabase, name, 3)
	s.buckets = append(s.buckets, name)
	return name
}

// join starts the joiner the documented way and proposes it through node via;
// it returns once the joiner has adopted the join.
func (s *settlingCluster) join(t *testing.T, via int) uint64 {
	t.Helper()
	if s.joiner == nil {
		s.startJoiner(t)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := clusterOf(s.tc, via).proposeJoin(ctx, s.ports.ringNode()); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	joinEpoch := clusterOf(s.tc, via).Epoch()
	joiner := s.joiner.server.core.cluster
	pollUntil(t, "the joiner adopts the join", func() bool {
		return joiner.Epoch() == joinEpoch && !joiner.bootstrapped.Load()
	})
	return joinEpoch
}

// startJoiner starts the joiner the documented way, waiting for 'join'; join
// proposes it.
func (s *settlingCluster) startJoiner(t *testing.T) {
	t.Helper()
	s.joiner = startJoinTarget(t, s.tc, s.ports, s.tc.token)
}

// members is every running node's cluster, the joiner last when it runs.
func (s *settlingCluster) members() []*Cluster {
	var clusters []*Cluster
	for _, node := range s.tc.nodes {
		if node.server != nil {
			clusters = append(clusters, node.server.core.cluster)
		}
	}
	if s.joiner != nil && s.joiner.server != nil {
		clusters = append(clusters, s.joiner.server.core.cluster)
	}
	return clusters
}

// lowerSweepBounds shortens the sweep's retry, give-up and re-arm bounds for
// the test.
func lowerSweepBounds(t *testing.T, giveUp time.Duration) {
	t.Helper()
	floor, ceiling, gaveUp, rearm := sweepRetryFloor.get(), sweepRetryCeiling.get(), sweepRetryGiveUp.get(), sweepRearmInterval.get()
	sweepRetryFloor.set(50 * time.Millisecond)
	sweepRetryCeiling.set(200 * time.Millisecond)
	sweepRetryGiveUp.set(giveUp)
	sweepRearmInterval.set(100 * time.Millisecond)
	t.Cleanup(func() {
		sweepRetryFloor.set(floor)
		sweepRetryCeiling.set(ceiling)
		sweepRetryGiveUp.set(gaveUp)
		sweepRearmInterval.set(rearm)
	})
}

// quickenSweepRetries shortens the sweep's retry backoff, and nothing else, so
// a test that waits for a sweep to settle waits on the sweep's passes and not
// on its production backoff (1 s doubling to 15 s). The give-up bound and the
// rearm interval stay as they are.
func quickenSweepRetries(t *testing.T) {
	t.Helper()
	floor, ceiling := sweepRetryFloor.get(), sweepRetryCeiling.get()
	sweepRetryFloor.set(50 * time.Millisecond)
	sweepRetryCeiling.set(200 * time.Millisecond)
	t.Cleanup(func() {
		sweepRetryFloor.set(floor)
		sweepRetryCeiling.set(ceiling)
	})
}

// setAskNodeToMigrateHook installs askNodeToMigrateHook until the test ends.
func setAskNodeToMigrateHook(t *testing.T, hook func(fromID, memberID, database, bucket string) error) {
	t.Helper()
	askNodeToMigrateHook.Store(&hook)
	t.Cleanup(func() { askNodeToMigrateHook.Store(nil) })
}

// errSeamFailsHandOff is the refusal a test's seam answers a hand-off with.
var errSeamFailsHandOff = errors.New("test seam: this hand-off fails")

// failHandOffs fails every hand-off from node fromID of the named buckets
// while failing reports true.
func failHandOffs(t *testing.T, fromID string, failing *atomic.Bool, buckets ...string) {
	t.Helper()
	named := make(map[string]bool, len(buckets))
	for _, bucket := range buckets {
		named[bucket] = true
	}
	setAskNodeToMigrateHook(t, func(from, _, _, bucket string) error {
		if from == fromID && failing.Load() && (len(named) == 0 || named[bucket]) {
			return errSeamFailsHandOff
		}
		return nil
	})
}

// residueSnapshot is a copy of what the newest recorded sweep left.
func (c *Cluster) residueSnapshot() sweepResidue {
	c.sweepLeftMu.Lock()
	defer c.sweepLeftMu.Unlock()
	residue := c.residue
	residue.list = append([]heldBucket(nil), residue.list...)
	return residue
}

// waitSettled waits until node c's own Settled epoch reaches epoch.
func waitSettled(t *testing.T, c *Cluster, epoch uint64) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for c.settled.Load() < epoch {
		if time.Now().After(deadline) {
			t.Fatalf("node %s is settled at %d, want %d; its residue holds %d buckets", c.self.ID, c.settled.Load(), epoch, c.sweepLeftCount())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// stageHold holds the sweeps of one node at chosen stages and epochs.
type stageHold struct {
	nodeID  string
	mu      sync.Mutex
	gates   map[stageKey]chan struct{}
	reached map[stageKey]chan struct{}
	// arrivals counts how many times a sweep reached each stage.
	arrivals map[stageKey]int
}

type stageKey struct {
	epoch uint64
	stage sweepStage
}

// holdSweepStages installs a sweepStageHook for nodeID; hold and release
// pick the stages.
func holdSweepStages(t *testing.T, nodeID string) *stageHold {
	t.Helper()
	h := &stageHold{nodeID: nodeID, gates: make(map[stageKey]chan struct{}), reached: make(map[stageKey]chan struct{}), arrivals: make(map[stageKey]int)}
	hook := func(node string, epoch uint64, stage sweepStage) {
		if node != h.nodeID {
			return
		}
		key := stageKey{epoch: epoch, stage: stage}
		h.mu.Lock()
		reached := h.reachedLocked(key)
		gate := h.gates[key]
		h.arrivals[key]++
		h.mu.Unlock()
		select {
		case <-reached:
		default:
			close(reached)
		}
		if gate != nil {
			<-gate
		}
	}
	sweepStageHook.Store(&hook)
	t.Cleanup(func() {
		sweepStageHook.Store(nil)
		h.releaseAll()
	})
	return h
}

func (h *stageHold) reachedLocked(key stageKey) chan struct{} {
	if h.reached[key] == nil {
		h.reached[key] = make(chan struct{})
	}
	return h.reached[key]
}

// hold makes the sweep that reaches stage at epoch wait for release.
func (h *stageHold) hold(epoch uint64, stage sweepStage) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.gates[stageKey{epoch: epoch, stage: stage}] = make(chan struct{})
}

// release lets the held stage go on, and every later arrival at it.
func (h *stageHold) release(epoch uint64, stage sweepStage) {
	h.mu.Lock()
	defer h.mu.Unlock()
	key := stageKey{epoch: epoch, stage: stage}
	if gate := h.gates[key]; gate != nil {
		close(gate)
		delete(h.gates, key)
	}
}

func (h *stageHold) releaseAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for key, gate := range h.gates {
		close(gate)
		delete(h.gates, key)
	}
}

// reachedChan is closed once a sweep of the node has reached stage at epoch.
func (h *stageHold) reachedChan(epoch uint64, stage sweepStage) <-chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.reachedLocked(stageKey{epoch: epoch, stage: stage})
}

// arrivalsAt is how many times a sweep of the node has reached stage at epoch.
func (h *stageHold) arrivalsAt(epoch uint64, stage sweepStage) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.arrivals[stageKey{epoch: epoch, stage: stage}]
}

// awaitStage waits until a sweep of the node reaches stage at epoch.
func (h *stageHold) awaitStage(t *testing.T, epoch uint64, stage sweepStage) {
	t.Helper()
	h.mu.Lock()
	reached := h.reachedLocked(stageKey{epoch: epoch, stage: stage})
	h.mu.Unlock()
	select {
	case <-reached:
	case <-time.After(30 * time.Second):
		t.Fatalf("node %s's sweep never reached stage %d at epoch %d", h.nodeID, stage, epoch)
	}
}

// TestASweepThatLeavesNothingMarksTheNodeSettledAtItsEpoch: after a join's
// sweeps every member, the joiner included, is settled at the join epoch.
func TestASweepThatLeavesNothingMarksTheNodeSettledAtItsEpoch(t *testing.T) {
	s := startSettlingCluster(t)
	defer s.tc.stop()
	joinEpoch := s.join(t, 0)
	for _, member := range s.members() {
		waitSettled(t, member, joinEpoch)
	}
}

// TestASweepThatGaveUpLeavesTheNodeUnsettled: a holder whose hand-off of one
// bucket fails until its sweep gives up is not settled at the join epoch.
// Guard: mark on any return of the sweep.
func TestASweepThatGaveUpLeavesTheNodeUnsettled(t *testing.T) {
	lowerSweepBounds(t, 500*time.Millisecond)
	s := startSettlingCluster(t)
	defer s.tc.stop()
	holder := clusterOf(s.tc, 0)
	var failing atomic.Bool
	failing.Store(true)
	failHandOffs(t, holder.self.ID, &failing, s.keptBucket(t, 0, "kept"))
	stages := holdSweepStages(t, holder.self.ID)
	defer stages.releaseAll()
	joinEpoch := s.join(t, 1)

	stages.awaitStage(t, joinEpoch, sweepEnded)
	if left := holder.sweepLeftCount(); left == 0 {
		t.Fatal("test setup: the holder's sweep gave up on nothing")
	}
	if settled := holder.settled.Load(); settled >= joinEpoch {
		t.Fatalf("the holder gave up handing off %d buckets and is settled at %d, the join epoch %d", holder.sweepLeftCount(), settled, joinEpoch)
	}
}

// TestASweepOfAnOlderEpochDoesNotMarkTheNewerOneSettled: a sweep of epoch E
// that ends empty after the node adopted E+1, while E+1's sweep has not begun,
// marks nothing; E+1 is marked when its own sweep ends. Guard: mark the node's
// current epoch when a sweep ends empty, whatever epoch it swept.
//
// The sweep of E runs its passes against E+1's members, which are still taking
// the change over: the first pass is often refused ("has not taken the bucket
// over yet") and the sweep asks again after its retry backoff. A loaded host
// stretches that to several retries, so the table refuses six passes, which on
// the production backoff (1, 2, 4, 8, 15 s) ends after 30 s: the bound the test
// waits for the sweep (BUG-0487).
func TestASweepOfAnOlderEpochDoesNotMarkTheNewerOneSettled(t *testing.T) {
	for _, tc := range []struct {
		name          string
		refusedPasses int
	}{
		{name: "hand-offs confirmed as the members take over", refusedPasses: 0},
		{name: "six passes refused before the members have taken over", refusedPasses: 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			quickenSweepRetries(t)
			s := startSettlingCluster(t)
			defer s.tc.stop()
			held := clusterOf(s.tc, 0)
			stages := holdSweepStages(t, held.self.ID)
			defer stages.releaseAll()
			joinEpoch := clusterOf(s.tc, 0).Epoch() + 1
			setAskNodeToMigrateHook(t, func(fromID, _, _, _ string) error {
				if fromID == held.self.ID && stages.arrivalsAt(joinEpoch, passFinished) < tc.refusedPasses {
					return errSeamFailsHandOff
				}
				return nil
			})
			stages.hold(joinEpoch, sweepPassing)
			stages.hold(joinEpoch+1, sweepStarting)
			s.join(t, 1)
			stages.awaitStage(t, joinEpoch, sweepPassing)

			// The next change: the joiner removed again, proposed by another member.
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			if err := clusterOf(s.tc, 1).proposeRemoval(ctx, s.ports.ringAddr); err != nil {
				t.Fatalf("proposeRemoval: %v", err)
			}
			pollUntil(t, "node 1 adopts the removal", func() bool { return held.Epoch() == joinEpoch+1 })
			stages.awaitStage(t, joinEpoch+1, sweepStarting)

			stages.release(joinEpoch, sweepPassing)
			stages.awaitStage(t, joinEpoch, sweepEnded)
			if settled := held.settled.Load(); settled >= joinEpoch+1 {
				t.Fatalf("a sweep of epoch %d that ended on epoch %d marked the node settled at %d", joinEpoch, joinEpoch+1, settled)
			}
			stages.release(joinEpoch+1, sweepStarting)
			waitSettled(t, held, joinEpoch+1)
		})
	}
}

// TestARearmThatEndsAfterANewerSweepNeitherMarksNorClobbers is review F-4: a
// rearm of an older residue that ends after a newer full sweep recorded its
// own residue neither marks the node settled nor replaces that residue.
// Guard: record the residue whatever the generation.
func TestARearmThatEndsAfterANewerSweepNeitherMarksNorClobbers(t *testing.T) {
	lowerSweepBounds(t, 500*time.Millisecond)
	s := startSettlingCluster(t)
	defer s.tc.stop()
	holder := clusterOf(s.tc, 0)
	var failing atomic.Bool
	failing.Store(true)
	failHandOffs(t, holder.self.ID, &failing, s.keptBucket(t, 0, "kept"))
	// Kept through the join, so the next change's sweep hands it off itself.
	second := s.keptBucket(t, 0, "second")
	stages := holdSweepStages(t, holder.self.ID)
	defer stages.releaseAll()
	joinEpoch := s.join(t, 1)
	stages.awaitStage(t, joinEpoch, sweepEnded)
	if holder.sweepLeftCount() == 0 {
		t.Fatal("test setup: the join's sweep left nothing to re-arm")
	}

	// R: a rearm of the join's residue, held before its pass.
	stages.hold(joinEpoch, rearmPassing)
	failing.Store(false)
	pollUntil(t, "a rearm of the join's residue starts", func() bool {
		holder.rearmSweep()
		select {
		case <-stages.reachedChan(joinEpoch, rearmPassing):
			return true
		default:
			return false
		}
	})

	// S: the next change's full sweep, which leaves another bucket behind.
	nextEpoch := joinEpoch + 1
	failHandOffs(t, holder.self.ID, &failing, second)
	failing.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := clusterOf(s.tc, 1).proposeRemoval(ctx, s.ports.ringAddr); err != nil {
		t.Fatalf("proposeRemoval: %v", err)
	}
	stages.awaitStage(t, nextEpoch, sweepEnded)
	residue := holder.residueSnapshot()
	if len(residue.list) == 0 || residue.epoch != nextEpoch {
		t.Fatalf("test setup: the full sweep of epoch %d left residue %+v, want one bucket of that epoch", nextEpoch, residue)
	}

	stages.release(joinEpoch, rearmPassing)
	pollUntil(t, "the rearm ends", func() bool { return !holder.rearmRunning.Load() })
	if settled := holder.settled.Load(); settled >= nextEpoch {
		t.Errorf("a rearm superseded by a newer sweep marked the node settled at %d", settled)
	}
	after := holder.residueSnapshot()
	if after.generation != residue.generation || len(after.list) != len(residue.list) {
		t.Errorf("a rearm superseded by a newer sweep replaced its residue: %+v, want %+v", after, residue)
	}
}

// TestRearmIsSingleFlight: while one rearm is held mid-pass, more calls past
// the re-arm interval start none. Guard: remove the single-flight flag.
func TestRearmIsSingleFlight(t *testing.T) {
	lowerSweepBounds(t, 500*time.Millisecond)
	s := startSettlingCluster(t)
	defer s.tc.stop()
	holder := clusterOf(s.tc, 0)
	var failing atomic.Bool
	failing.Store(true)
	failHandOffs(t, holder.self.ID, &failing, s.keptBucket(t, 0, "kept"))
	stages := holdSweepStages(t, holder.self.ID)
	defer stages.releaseAll()
	joinEpoch := s.join(t, 1)
	stages.awaitStage(t, joinEpoch, sweepEnded)

	stages.hold(joinEpoch, rearmPassing)
	before := holder.sweepRearms.Load()
	pollUntil(t, "a rearm starts", func() bool {
		holder.rearmSweep()
		return holder.sweepRearms.Load() > before
	})
	stages.awaitStage(t, joinEpoch, rearmPassing)
	for call := 0; call < 3; call++ {
		time.Sleep(2 * sweepRearmInterval.get())
		holder.rearmSweep()
	}
	if started := holder.sweepRearms.Load() - before; started != 1 {
		t.Fatalf("%d rearms started while one was running, want 1", started)
	}
}

// TestSettledIsNotTheEmptySweepLeftList: during the first pass of a new
// epoch's sweep nothing is remembered as unsettled, and the node is not
// settled. Guard: mark from sweepLeftCount() == 0.
func TestSettledIsNotTheEmptySweepLeftList(t *testing.T) {
	s := startSettlingCluster(t)
	defer s.tc.stop()
	held := clusterOf(s.tc, 0)
	stages := holdSweepStages(t, held.self.ID)
	defer stages.releaseAll()
	joinEpoch := held.Epoch() + 1
	stages.hold(joinEpoch, sweepPassing)
	s.join(t, 1)
	stages.awaitStage(t, joinEpoch, sweepPassing)
	if left := held.sweepLeftCount(); left != 0 {
		t.Fatalf("test setup: %d buckets remembered as unsettled before the first pass", left)
	}
	if settled := held.settled.Load(); settled >= joinEpoch {
		t.Fatalf("the node is settled at %d while its first pass of epoch %d has not run", settled, joinEpoch)
	}
	stages.release(joinEpoch, sweepPassing)
	waitSettled(t, held, joinEpoch)
}

// TestAnOrphanCreateHoldsSettledUntilHandedOff is review F-11: a create the
// owner logged under the epoch before a join, and that the join took the
// bucket from, holds the node's Settled below the join epoch until the bucket
// is handed off; then the joiner holds it. Guard: do not record orphans.
func TestAnOrphanCreateHoldsSettledUntilHandedOff(t *testing.T) {
	lowerSweepBounds(t, 500*time.Millisecond)
	s := startSettlingClusterPlaced(t, func(founders []string, joiner string) bool {
		return joinerFrom(t, "orphan", founders, joiner, 0)
	})
	defer s.tc.stop()
	owner := clusterOf(s.tc, 0)
	joined := s.joinedRing(t)
	orphan := bucketNamed(t, "orphan", func(name string) bool {
		return owner.ownsBucket(placementOf(testDatabase, name)) && ownerOn(joined, name) == s.ports.ringAddr
	})

	reached := make(chan struct{})
	release := make(chan struct{})
	var reachedOnce, releasedOnce sync.Once
	hold := func(nodeID string, op transport.Opcode, _, bucket string, _ uint64) {
		if nodeID == owner.self.ID && op == transport.OpCreateBucket && bucket == orphan {
			reachedOnce.Do(func() { close(reached) })
			<-release
		}
	}
	afterOwnerViewHook.Store(&hold)
	t.Cleanup(func() { afterOwnerViewHook.Store(nil) })
	// Deferred after the cluster's stop, so it runs first: a create held in the
	// hook keeps its node from stopping.
	defer releasedOnce.Do(func() { close(release) })
	created := make(chan *transport.Frame, 1)
	payload, err := bson.Marshal(CreateBucketRequest{Database: testDatabase, Name: orphan, Type: BucketTypeHash})
	if err != nil {
		t.Fatalf("encoding the create: %v", err)
	}
	client := s.tc.nodes[0].client
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		// A send that fails leaves the channel empty; the test then fails on
		// the wait below, not from this goroutine.
		if frame, sendErr := client.SendRequest(ctx, transport.OpCreateBucket, payload); sendErr == nil {
			created <- frame
		}
	}()
	select {
	case <-reached:
	case <-time.After(30 * time.Second):
		t.Fatal("test setup: the create never reached its owner's view")
	}

	joinEpoch := s.join(t, 1)
	// The owner's sweep of the join walks a catalog without the bucket and
	// ends with nothing left: only the create in flight holds Settled.
	pollUntil(t, "the owner's sweep of the join ends", func() bool {
		residue := owner.residueSnapshot()
		return residue.epoch == joinEpoch && residue.generation != 0
	})
	if settled := owner.settled.Load(); settled >= joinEpoch {
		t.Fatalf("the owner is settled at %d with a create of the epoch before the join in flight", settled)
	}

	releasedOnce.Do(func() { close(release) })
	var answer *transport.Frame
	select {
	case answer = <-created:
	case <-time.After(60 * time.Second):
		t.Fatal("the create the join overtook was never answered")
	}
	refused := refusalOf(t, answer)
	if refused.Code != int(codeFor(ErrOwnershipMoved)) {
		t.Fatalf("the create the join overtook answered code %d %q, want ErrOwnershipMoved's", refused.Code, refused.Message)
	}
	if settled := owner.settled.Load(); settled >= joinEpoch {
		t.Fatalf("the owner is settled at %d holding an orphan create it has not handed off", settled)
	}
	pollUntil(t, "the orphan is handed off and the owner settles", func() bool {
		owner.rearmSweep()
		return owner.settled.Load() >= joinEpoch
	})
	pollUntil(t, "the joiner holds the orphan", func() bool {
		return s.joiner.server.core.bucketStoreOpen(testDatabase, orphan)
	})
}

// TestSettledSurvivesARestart: a member's Settled epoch is read back at start.
// Guard: do not load it.
func TestSettledSurvivesARestart(t *testing.T) {
	s := startSettlingCluster(t)
	defer s.tc.stop()
	joinEpoch := s.join(t, 0)
	waitSettled(t, clusterOf(s.tc, 1), joinEpoch)

	// Any sweep the restarted node starts waits, so what it reports is what it
	// loaded.
	stages := holdSweepStages(t, s.tc.addrs[1])
	defer stages.releaseAll()
	for _, stage := range []sweepStage{sweepStarting, rearmPassing} {
		stages.hold(joinEpoch, stage)
	}
	restarted := s.tc.restartNode(1).server.core.cluster
	if settled := restarted.settled.Load(); settled != joinEpoch {
		t.Fatalf("after a restart node 2 is settled at %d, want %d", settled, joinEpoch)
	}
}

// TestARestartMidSettlingResumesTheSweep: a holder stopped while its sweep of
// a join was still retrying hand-offs runs that sweep again at its next start,
// and settles. Only the start's sweep can: the buckets it stays a member of are
// handed off by nothing else (start-time relocation moves only the buckets
// outside its set).
func TestARestartMidSettlingResumesTheSweep(t *testing.T) {
	lowerSweepBounds(t, time.Minute)
	s := startSettlingCluster(t)
	defer s.tc.stop()
	holder := clusterOf(s.tc, 0)
	// Every hand-off of the holder fails until it stops, and its sweep keeps
	// retrying (the give-up is a minute away): it is mid-settling then.
	var failing atomic.Bool
	failing.Store(true)
	failHandOffs(t, holder.self.ID, &failing)
	joinEpoch := s.join(t, 1)
	pollUntil(t, "the holder adopts the join", func() bool { return holder.Epoch() == joinEpoch })
	time.Sleep(time.Second)
	if settled := holder.settled.Load(); settled >= joinEpoch {
		t.Fatalf("test setup: the holder settled at %d although every hand-off failed", settled)
	}
	start := stopNode(t, s.tc, 0)
	failing.Store(false)

	restarted := start().server.core.cluster
	waitSettled(t, restarted, joinEpoch)
}

// TestAGivenUpSweepIsRetriedWhileItsPeersAnswer: a hand-off that failed until
// the sweep gave up is asked again from the healthy gossip round, with no peer
// ever down: it completes and the node settles.
func TestAGivenUpSweepIsRetriedWhileItsPeersAnswer(t *testing.T) {
	lowerSweepBounds(t, 500*time.Millisecond)
	s := startSettlingCluster(t)
	defer s.tc.stop()
	holder := clusterOf(s.tc, 0)
	var failing atomic.Bool
	failing.Store(true)
	failHandOffs(t, holder.self.ID, &failing, s.keptBucket(t, 0, "kept"))
	joinEpoch := s.join(t, 1)
	pollUntil(t, "the holder's sweep gives up on a bucket", func() bool { return holder.sweepLeftCount() > 0 })
	// The sweep's retries have ended; only a rearm asks again.
	time.Sleep(2 * sweepRetryGiveUp.get())

	failing.Store(false)
	pollUntil(t, "the given-up hand-off completes while every peer answers", func() bool {
		holder.wakeGossip()
		return holder.sweepLeftCount() == 0
	})
	waitSettled(t, holder, joinEpoch)
}

// TestAHolderOutsideTheNewSetKeepsItsCopyUntilTheJoinerConfirms is spec §6.1's
// all-members release: a holder the join moved out of a bucket's set keeps its
// copy while the joiner, in the new set, has not confirmed it holds one — even
// though the other members of the set have. Guard: release on one
// confirmation.
func TestAHolderOutsideTheNewSetKeepsItsCopyUntilTheJoinerConfirms(t *testing.T) {
	lowerSweepBounds(t, time.Minute)
	s := startSettlingCluster(t)
	defer s.tc.stop()
	joined := s.joinedRing(t)
	joiner := s.ports.ringAddr
	const holder = 0
	leaving := bucketNamed(t, "leave", func(name string) bool {
		set, err := joined.Replicas(placementOf(testDatabase, name).encoded, 3)
		return err == nil && containsID(nodeIDs(set), joiner) && !containsID(nodeIDs(set), s.tc.addrs[holder])
	})
	s.tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: leaving, Type: BucketTypeHash})
	s.tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: leaving, Key: "doc", Data: map[string]interface{}{"n": 1}})
	waitForServing(t, s.tc, testDatabase, leaving, 3)
	// Every hand-off to the joiner fails, so it never pulls and never
	// confirms; every other member answers. The streams to it are held too: an
	// owner's stream would replay the bucket's create to its new replica.
	defer holdStreamsTo(t, joiner)()
	setAskNodeToMigrateHook(t, func(_, memberID, _, _ string) error {
		if memberID == joiner {
			return errSeamFailsHandOff
		}
		return nil
	})
	s.join(t, 1)
	keeper := clusterOf(s.tc, holder)
	pollUntil(t, "the holder's sweep has asked the set", func() bool { return keeper.ownHandOff() != nil })
	time.Sleep(10 * sweepRetryFloor.get())
	if !keeper.s.bucketStoreOpen(testDatabase, leaving) {
		t.Fatalf("node %d, outside %s's new set, released its copy while the joiner had not confirmed holding one", holder+1, leaving)
	}
	if s.joiner.server.core.bucketStoreOpen(testDatabase, leaving) {
		t.Fatalf("test setup: the joiner holds %s, so it could have confirmed", leaving)
	}
}

// holdStreamsTo holds every replication ship and replay to node id until the
// returned release, which the caller defers after the cluster's stop so it
// runs first.
func holdStreamsTo(t *testing.T, id string) (release func()) {
	t.Helper()
	gate := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	holdReplay := func(peerID string, _ []ReplEntry) {
		if peerID == id {
			<-gate
		}
	}
	holdShip := func(peerID string) func() {
		if peerID == id {
			<-gate
		}
		return func() {}
	}
	beforeReplayShipHook.Store(&holdReplay)
	beforeShipHook.Store(&holdShip)
	t.Cleanup(func() {
		beforeReplayShipHook.Store(nil)
		beforeShipHook.Store(nil)
	})
	return release
}

// heldCreate is a create of one bucket sent through node 0 and held at its
// owner right after the owner read its view of ownership.
type heldCreate struct {
	reached  chan struct{}
	release  func()
	answered chan *transport.Frame
}

// holdCreateAtOwnerView sends a create of bucket through node index of tc and
// holds it at owner's view until release. The caller defers release after the
// cluster's stop, so it runs first.
func holdCreateAtOwnerView(t *testing.T, tc *testCluster, index int, owner *Cluster, bucket string) *heldCreate {
	t.Helper()
	held := &heldCreate{reached: make(chan struct{}), answered: make(chan *transport.Frame, 1)}
	gate := make(chan struct{})
	var reachedOnce, releasedOnce sync.Once
	held.release = func() { releasedOnce.Do(func() { close(gate) }) }
	hold := func(nodeID string, op transport.Opcode, _, name string, _ uint64) {
		if nodeID == owner.self.ID && op == transport.OpCreateBucket && name == bucket {
			reachedOnce.Do(func() { close(held.reached) })
			<-gate
		}
	}
	afterOwnerViewHook.Store(&hold)
	t.Cleanup(func() { afterOwnerViewHook.Store(nil) })
	payload, err := bson.Marshal(CreateBucketRequest{Database: testDatabase, Name: bucket, Type: BucketTypeHash})
	if err != nil {
		t.Fatalf("encoding the create: %v", err)
	}
	client := tc.nodes[index].client
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		// A send that fails leaves the channel empty; the test fails on its
		// wait, not from this goroutine.
		if frame, sendErr := client.SendRequest(ctx, transport.OpCreateBucket, payload); sendErr == nil {
			held.answered <- frame
		}
	}()
	select {
	case <-held.reached:
	case <-time.After(30 * time.Second):
		t.Fatal("test setup: the create never reached its owner's view")
	}
	return held
}

// awaitAnswer waits for the held create's answer.
func (h *heldCreate) awaitAnswer(t *testing.T) *transport.Frame {
	t.Helper()
	select {
	case frame := <-h.answered:
		return frame
	case <-time.After(60 * time.Second):
		t.Fatal("the held create was never answered")
	}
	return nil
}

// allLifted reports whether no member's report pauses schema changes.
func allLifted(members []*Cluster) bool {
	for _, member := range members {
		if member.joinFreeze.Load().Paused {
			return false
		}
	}
	return true
}

// waitLifted waits, waking gossip but asking no rearm, until every member
// allows schema changes again: the lift must come from the nodes themselves.
func waitLifted(t *testing.T, members []*Cluster, within time.Duration, what string) {
	t.Helper()
	for deadline := time.Now().Add(within); !allLifted(members); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			for _, member := range members {
				t.Logf("%s: settled %d, report %+v", member.self.ID, member.settled.Load(), *member.joinFreeze.Load())
			}
			t.Fatalf("%s: schema changes still paused %s later", what, within)
		}
		wakeAll(members)
	}
}

// TestACreateInFlightAcrossAJoinDoesNotHoldThePauseForever is the verifier's
// probe of deviation 2: a create of a bucket its owner keeps through a join,
// in flight while the join's sweep ends, kept the owner from settling (the
// sweep's mark was refused for the create in flight) and, once it completed
// as an ordinary create, nothing marked the node settled again: every node
// stayed paused. Now a create that a membership change overtook is handed off
// like an orphan, and the node settles when it is.
func TestACreateInFlightAcrossAJoinDoesNotHoldThePauseForever(t *testing.T) {
	s := startSettlingCluster(t)
	defer s.tc.stop()
	owner := clusterOf(s.tc, 0)
	joined := s.joinedRing(t)
	// A name the owner keeps through the join; on some rings no such name also
	// gains the joiner, so whether it does is read, not chosen.
	kept := bucketNamed(t, "keptown", func(name string) bool {
		return owner.ownsBucket(placementOf(testDatabase, name)) && ownerOn(joined, name) == owner.self.ID
	})
	set, err := joined.Replicas(placementOf(testDatabase, kept).encoded, 3)
	if err != nil {
		t.Fatalf("placing %s after the join: %v", kept, err)
	}
	joinerInSet := containsID(nodeIDs(set), s.ports.ringAddr)
	create := holdCreateAtOwnerView(t, s.tc, 0, owner, kept)
	defer create.release()
	joinEpoch := s.join(t, 1)
	pollUntil(t, "the owner's sweep of the join ends", func() bool {
		residue := owner.residueSnapshot()
		return residue.epoch == joinEpoch && residue.generation != 0
	})
	if owner.settled.Load() >= joinEpoch {
		t.Fatal("test setup: the owner settled with a create of the epoch before in flight")
	}
	create.release()
	if answer := create.awaitAnswer(t); answer.Opcode == transport.OpError {
		t.Logf("the create answered %s", string(answer.Payload))
	}
	waitLifted(t, s.members(), 30*time.Second, "after the create the join overtook completed")
	if joinerInSet && !s.joiner.server.core.bucketStoreOpen(testDatabase, kept) {
		t.Errorf("the pause lifted while the joiner, in %s's set, holds no copy of it", kept)
	}
}
