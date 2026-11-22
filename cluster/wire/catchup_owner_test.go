package wire

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// TestAStragglingCatchUpDoesNotResetANodeThatBecameOwner is BUG-0816. A catch-up that started
// while this node was a copy, more than one chunk behind, is still running when the node has
// caught up by another route (the handoff's own catch-up), the epoch that makes it the owner
// has committed, and it has ordered its first record. The straggler's next request would name
// a position its source never ordered; the source would answer Divergent and the catch-up
// would reset the pair the node now owns and has taken a write for. The interleaving is
// scripted at the Replicator level: no cluster, no handoff.
func TestAStragglingCatchUpDoesNotResetANodeThatBecameOwner(t *testing.T) {
	source := newU32Node(t, "source", func(cfg *ReplicatorConfig) { cfg.ChunkRecords = 2 })
	source.ownRun(u32Epoch, 1, 6)
	promoted := newU32Node(t, "promoted", nil)
	// The catch-up starts on a copy: the source is the pair's owner at epoch 7.
	promoted.catchUpFrom(source, source)

	var once sync.Once
	promoted.link.mu.Lock()
	promoted.link.afterCatchUp = func(CatchUpRequest) {
		once.Do(func() {
			// What the handoff's own catch-up does meanwhile: apply the old owner's tail.
			for seq := uint64(1); seq <= 6; seq++ {
				promoted.write(u32Frame(t, u32Epoch, u32Pos(u32Epoch, seq), cluster.Position{}))
			}
			// The epoch that makes this node the owner commits, and it orders its first record.
			// The hook runs on the catch-up's own goroutine, so the owner is read in order.
			promoted.committed.Store(u32Epoch + 1)
			promoted.ownerID = promoted.id
			promoted.ownAppend(u32Slot, u32Pos(u32Epoch+1, 1), u32Pos(u32Epoch, 6))
		})
	}
	promoted.link.mu.Unlock()

	err := promoted.rep.CatchUp(context.Background(), u32Slot, u32DB, cluster.Position{}, 0)

	promoted.link.mu.Lock()
	requests := append([]CatchUpRequest(nil), promoted.link.requests...)
	promoted.link.mu.Unlock()
	for i, req := range requests {
		t.Logf("request %d: epoch %d from %s", i+1, req.Epoch, req.From)
	}
	if err != nil || promoted.seeds.count() != 0 {
		t.Fatalf("a catch-up that began on a copy reset the pair after the node became its owner: CatchUp returned %v with %d resets, want no error and no reset", err, promoted.seeds.count())
	}
	if len(requests) != 1 {
		t.Fatalf("the catch-up asked its source %d times, want 1: it must stop before the request that follows the promotion", len(requests))
	}
	if got := promoted.applied(); got != u32Pos(u32Epoch+1, 1) {
		t.Fatalf("the owner's applied position is %s, want its own record 8:1", got)
	}
	if promoted.rep.IsCurrent(u32Slot, u32DB) {
		t.Fatal("a catch-up that stopped because the node became the owner marked the pair current; only the handoff's own catch-up may")
	}
}

// TestACopysCatchUpDoesNothingOnANodeThatOwnsThePair: the catch-up a copy asks for after a
// gap can be started late, after the epoch that made the node the owner. It then asks nobody
// and resets nothing, although its source would answer Divergent (BUG-0816).
func TestACopysCatchUpDoesNothingOnANodeThatOwnsThePair(t *testing.T) {
	source := newU32Node(t, "source", nil)
	source.ownRun(u32Epoch, 1, 2)
	owner := newU32Node(t, "owner", nil)
	owner.committed.Store(u32Epoch + 1)
	owner.ownRun(u32Epoch+1, 1, 3)
	owner.catchUpFrom(source, owner)

	if err := owner.rep.CatchUpAsCopy(context.Background(), u32Slot, u32DB); err != nil {
		t.Fatalf("CatchUpAsCopy on the owner: %v", err)
	}
	if asked := len(owner.link.requests); asked != 0 || owner.seeds.count() != 0 {
		t.Fatalf("a copy's catch-up on the pair's owner asked its source %d times and reset the pair %d times, want neither", asked, owner.seeds.count())
	}
}

// TestAnOwnersCatchUpStillResetsADivergentTail: the handoff's catch-up begins on the node
// that owns the pair, before the pair opens. A tail nobody owns found there is still reset:
// the owner check of BUG-0816 is for a catch-up that began on a copy.
func TestAnOwnersCatchUpStillResetsADivergentTail(t *testing.T) {
	source := newU32Node(t, "source", nil)
	source.ownRun(u32Epoch, 1, 2)
	source.committed.Store(8)
	source.ownAppend(u32Slot, u32Pos(8, 1), u32Pos(7, 2))
	awaiting := newU32Node(t, "awaiting", nil)
	for seq := uint64(1); seq <= 3; seq++ {
		awaiting.write(u32Frame(t, u32Epoch, u32Pos(7, seq), cluster.Position{}))
	}
	awaiting.committed.Store(9)
	awaiting.catchUpFrom(source, awaiting)

	err := awaiting.rep.CatchUp(context.Background(), u32Slot, u32DB, u32Pos(7, 3), 1)
	if !errors.Is(err, ErrResetAndSeed) || awaiting.seeds.count() != 1 {
		t.Fatalf("the handoff's catch-up of a pair holding a tail nobody owns: err %v, resets %d, want ErrResetAndSeed and one reset", err, awaiting.seeds.count())
	}
}

// TestAPushedRecordJudgedDivergentDoesNotResetAnOwner: a pushed record whose verdict is a
// reset was judged for a copy. When the node owns the pair by the time the verdict is acted
// on, the pair is not reset; on a copy it is (BUG-0816, the push path's half).
func TestAPushedRecordJudgedDivergentDoesNotResetAnOwner(t *testing.T) {
	for _, role := range []struct {
		name   string
		owner  bool
		resets int
	}{{"a copy", false, 1}, {"the owner", true, 0}} {
		t.Run(role.name, func(t *testing.T) {
			node := newU32Node(t, "node", nil)
			for seq := uint64(1); seq <= 5; seq++ {
				node.write(u32Frame(t, u32Epoch, u32Pos(7, seq), cluster.Position{}))
			}
			node.committed.Store(8)
			if role.owner {
				node.ownerID = node.id
			}
			// An opener whose boundary is behind this node's position: a tail nobody owns.
			reply := node.write(u32Frame(t, 8, u32Pos(8, 1), u32Pos(7, 3)))
			if node.seeds.count() != role.resets || reply.Ack.Outcome != AckRefused {
				t.Fatalf("resets %d, ack %+v, want %d resets and a refusal", node.seeds.count(), reply.Ack, role.resets)
			}
		})
	}
}

// TestAnOwnersCatchUpCancelsACopysAndWaitsForIt: one catch-up runs per pair. A copy's
// catch-up is held inside a request when the node becomes the owner and the handoff's
// catch-up starts: the handoff's cancels it, waits until it has ended and only then asks,
// so the two never ask or apply at the same time; the cancelled one reports no error.
func TestAnOwnersCatchUpCancelsACopysAndWaitsForIt(t *testing.T) {
	var promotedNow atomic.Bool
	source := newU32Node(t, "source", func(cfg *ReplicatorConfig) { cfg.ChunkRecords = 2 })
	source.ownRun(u32Epoch, 1, 6)
	source.copies = []string{"node"}
	node := newU32Node(t, "node", func(cfg *ReplicatorConfig) {
		cfg.Owner = func(uint16, uint64) (string, bool) {
			if promotedNow.Load() {
				return "node", true
			}
			return "source", true
		}
	})
	node.sourceID = source.id
	node.link.mu.Lock()
	node.link.source, node.link.owner = source.rep, source.rep
	held, release := make(chan struct{}), make(chan struct{})
	var inFlight, overlapped atomic.Int32
	var first sync.Once
	node.link.afterCatchUp = func(CatchUpRequest) {
		if inFlight.Add(1) > 1 {
			overlapped.Store(1)
		}
		first.Do(func() {
			close(held)
			<-release
		})
		inFlight.Add(-1)
	}
	node.link.mu.Unlock()

	copyDone := make(chan error, 1)
	go func() { copyDone <- node.rep.CatchUpAsCopy(context.Background(), u32Slot, u32DB) }()
	<-held
	promotedNow.Store(true)
	ownerDone := make(chan error, 1)
	go func() { ownerDone <- node.rep.CatchUp(context.Background(), u32Slot, u32DB, cluster.Position{}, 0) }()

	select {
	case err := <-ownerDone:
		t.Fatalf("the owner's catch-up returned %v while the copy's was still inside a request", err)
	case <-time.After(100 * time.Millisecond):
	}
	node.link.mu.Lock()
	asked := len(node.link.requests)
	node.link.mu.Unlock()
	if asked != 1 {
		t.Fatalf("%d requests were sent while the copy's catch-up held the pair, want its 1", asked)
	}
	close(release)
	if err := <-copyDone; err != nil {
		t.Fatalf("the cancelled copy's catch-up returned %v, want no error", err)
	}
	if err := <-ownerDone; err != nil {
		t.Fatalf("the owner's catch-up: %v", err)
	}
	if overlapped.Load() != 0 {
		t.Fatal("two catch-ups of one pair were inside a request at the same time")
	}
	if got := node.applied(); got != u32Pos(u32Epoch, 6) || !node.rep.IsCurrent(u32Slot, u32DB) || node.seeds.count() != 0 {
		t.Fatalf("after the owner's catch-up: applied %s, current %v, resets %d, want 7:6, current and no reset", got, node.rep.IsCurrent(u32Slot, u32DB), node.seeds.count())
	}
}

// blockingSeeds is a Seeder whose reset waits, as the node's does while it clears the pair's
// seed bit through the leader (up to 5 s).
type blockingSeeds struct {
	entered, release chan struct{}
	done             atomic.Bool
}

func (s *blockingSeeds) ResetAndSeed(uint16, uint64, ReseedCause) error {
	close(s.entered)
	<-s.release
	s.done.Store(true)
	return nil
}

// promotable is a node whose owner a test flips from another goroutine.
func promotable(t *testing.T, promoted *atomic.Bool, mutate func(*ReplicatorConfig)) *u32Node {
	t.Helper()
	return newU32Node(t, "node", func(cfg *ReplicatorConfig) {
		cfg.Owner = func(uint16, uint64) (string, bool) {
			if promoted.Load() {
				return "node", true
			}
			return "source", true
		}
		if mutate != nil {
			mutate(cfg)
		}
	})
}

// TestAPushedRecordsResetIsFinishedBeforeTheHandoffsCatchUpRuns (BUG-0816, review F2): a pushed
// record is judged divergent on a copy and its reset waits inside the Seeder. The epoch that
// makes the node the owner commits and the handoff's catch-up starts. It must wait for the
// reset: if it ran, it would find no source ahead, mark the pair current and let the pair
// open, and the reset would then land on a serving owner.
func TestAPushedRecordsResetIsFinishedBeforeTheHandoffsCatchUpRuns(t *testing.T) {
	var promoted atomic.Bool
	seeds := &blockingSeeds{entered: make(chan struct{}), release: make(chan struct{})}
	node := promotable(t, &promoted, func(cfg *ReplicatorConfig) {
		cfg.Seeds = seeds
		cfg.CatchUpSource = func(uint16, uint64) (string, error) { return "", ErrNoCatchUpSource }
	})
	for seq := uint64(1); seq <= 5; seq++ {
		node.write(u32Frame(t, u32Epoch, u32Pos(7, seq), cluster.Position{}))
	}
	node.committed.Store(8)

	pushed := make(chan struct{})
	frame := u32Frame(t, 8, u32Pos(8, 1), u32Pos(7, 3)) // an opener behind this copy: a tail nobody owns
	go func() {
		defer close(pushed)
		if _, err := node.rep.OnWrite(frame); err != nil {
			t.Errorf("OnWrite: %v", err)
		}
	}()
	<-seeds.entered
	promoted.Store(true)

	handoff := make(chan error, 1)
	var resetDoneFirst atomic.Bool
	go func() {
		err := node.rep.CatchUp(context.Background(), u32Slot, u32DB, cluster.Position{}, 0)
		resetDoneFirst.Store(seeds.done.Load())
		handoff <- err
	}()
	select {
	case err := <-handoff:
		t.Fatalf("the handoff's catch-up returned %v (pair current: %v) while a copy's reset of the pair was still running: the pair can open and then be reset", err, node.rep.IsCurrent(u32Slot, u32DB))
	case <-time.After(150 * time.Millisecond):
	}
	close(seeds.release)
	<-pushed
	if err := <-handoff; err != nil {
		t.Fatalf("the handoff's catch-up after the reset: %v", err)
	}
	if !resetDoneFirst.Load() {
		t.Fatal("the handoff's catch-up returned before the copy's reset had finished")
	}
}

// TestAPushedRecordsResetLeavesThePairToACatchUpThatHoldsIt: while a catch-up holds the pair,
// a pushed record judged divergent resets nothing itself; the catch-up meets the same tail.
func TestAPushedRecordsResetLeavesThePairToACatchUpThatHoldsIt(t *testing.T) {
	var promoted atomic.Bool
	source := newU32Node(t, "source", nil)
	source.ownRun(u32Epoch, 1, 5)
	node := promotable(t, &promoted, nil)
	for seq := uint64(1); seq <= 5; seq++ {
		node.write(u32Frame(t, u32Epoch, u32Pos(7, seq), cluster.Position{}))
	}
	node.committed.Store(8)
	node.sourceID = source.id
	held, release := make(chan struct{}), make(chan struct{})
	node.link.mu.Lock()
	node.link.source, node.link.owner = source.rep, source.rep
	node.link.beforeCatchUp = func(context.Context, CatchUpRequest) error {
		close(held)
		<-release
		return nil
	}
	node.link.mu.Unlock()
	running := make(chan error, 1)
	go func() { running <- node.rep.CatchUpAsCopy(context.Background(), u32Slot, u32DB) }()
	<-held

	reply := node.write(u32Frame(t, 8, u32Pos(8, 1), u32Pos(7, 3)))
	if node.seeds.count() != 0 || reply.Ack.Outcome != AckRefused {
		t.Fatalf("a pushed divergent record while a catch-up holds the pair: resets %d, ack %+v, want no reset and a refusal", node.seeds.count(), reply.Ack)
	}
	close(release)
	if err := <-running; err != nil {
		t.Fatalf("the catch-up that held the pair: %v", err)
	}
}

// TestAHandoffsCatchUpCancelsACopysRequest: the copy's request waits on its context, as a
// call on a real link does. The handoff's catch-up cancels it, so the copy's catch-up ends at
// once and with no error, and the handoff's then runs. Without the cancel the handoff would
// wait for the request's own deadline.
func TestAHandoffsCatchUpCancelsACopysRequest(t *testing.T) {
	var promoted atomic.Bool
	source := newU32Node(t, "source", nil)
	source.ownRun(u32Epoch, 1, 4)
	node := promotable(t, &promoted, nil)
	node.sourceID = source.id
	held := make(chan struct{})
	var first sync.Once
	node.link.mu.Lock()
	node.link.source, node.link.owner = source.rep, source.rep
	node.link.beforeCatchUp = func(ctx context.Context, _ CatchUpRequest) error {
		blocked := false
		first.Do(func() { blocked = true })
		if !blocked {
			return nil
		}
		close(held)
		<-ctx.Done()
		return ctx.Err()
	}
	node.link.mu.Unlock()

	copyDone := make(chan error, 1)
	go func() { copyDone <- node.rep.CatchUpAsCopy(context.Background(), u32Slot, u32DB) }()
	<-held
	promoted.Store(true)
	ownerDone := make(chan error, 1)
	go func() { ownerDone <- node.rep.CatchUp(context.Background(), u32Slot, u32DB, cluster.Position{}, 0) }()

	select {
	case err := <-copyDone:
		if err != nil {
			t.Fatalf("the cancelled copy's catch-up returned %v, want no error: a request the handoff cut short is not a failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handoff's catch-up did not cancel the copy's request")
	}
	select {
	case err := <-ownerDone:
		if err != nil {
			t.Fatalf("the handoff's catch-up: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handoff's catch-up did not run after the copy's ended")
	}
	if got := node.applied(); got != u32Pos(u32Epoch, 4) || !node.rep.IsCurrent(u32Slot, u32DB) {
		t.Fatalf("after the handoff's catch-up: applied %s, current %v, want 7:4 and current", got, node.rep.IsCurrent(u32Slot, u32DB))
	}
}

// TestACopysCatchUpCancelledInsideAChunk: the handoff's catch-up cancels a copy's while it
// applies a chunk. The cancellation itself is not a failure, so the copy's catch-up returns no
// error; but when the apply fails for a reason of its own (here the copy refuses the record),
// that error is returned and not hidden behind the cancel (flash 5).
func TestACopysCatchUpCancelledInsideAChunk(t *testing.T) {
	refusal := errors.New("the disk refused the append")
	for _, tc := range []struct {
		name    string
		failure error
	}{{"the apply succeeds", nil}, {"the apply fails", refusal}} {
		t.Run(tc.name, func(t *testing.T) {
			var promoted atomic.Bool
			source := newU32Node(t, "source", nil)
			source.ownRun(u32Epoch, 1, 2)
			cancelled, held := make(chan struct{}), make(chan struct{})
			var applying sync.Once
			node := promotable(t, &promoted, func(cfg *ReplicatorConfig) {
				// The node is still a copy when its first record is checked; the apply then
				// waits until the handoff has cancelled the catch-up.
				cfg.SchemaReady = func(uint64, cluster.Op) error {
					applying.Do(func() { close(held) })
					<-cancelled
					return tc.failure
				}
			})
			node.catchUpFrom(source, source)

			copyDone := make(chan error, 1)
			go func() { copyDone <- node.rep.CatchUpAsCopy(context.Background(), u32Slot, u32DB) }()
			<-held
			promoted.Store(true)
			ownerDone := make(chan error, 1)
			go func() { ownerDone <- node.rep.CatchUp(context.Background(), u32Slot, u32DB, cluster.Position{}, 0) }()
			waitPreempted(t, node.rep)
			close(cancelled)
			if err := <-copyDone; !errors.Is(err, tc.failure) || (tc.failure == nil && err != nil) {
				t.Fatalf("the cancelled copy's catch-up returned %v, want %v", err, tc.failure)
			}
			<-ownerDone
		})
	}
}

// waitPreempted waits until the pair's flight has been cancelled by an owner's catch-up.
func waitPreempted(t *testing.T, r *Replicator) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		r.mu.Lock()
		flight := r.catching[pairKey{u32Slot, u32DB}]
		r.mu.Unlock()
		if flight != nil && flight.preempted.Load() {
			return
		}
	}
	t.Fatal("no owner's catch-up cancelled the pair's flight")
}

// TestASecondCopysCatchUpLeavesWhenOneIsRunning: a gap can ask for a catch-up while one is
// already fetching. The second returns at once and asks nobody.
func TestASecondCopysCatchUpLeavesWhenOneIsRunning(t *testing.T) {
	source := newU32Node(t, "source", nil)
	source.ownRun(u32Epoch, 1, 2)
	node := newU32Node(t, "node", nil)
	node.catchUpFrom(source, source)
	held, release := make(chan struct{}), make(chan struct{})
	var first sync.Once
	node.link.mu.Lock()
	node.link.beforeCatchUp = func(context.Context, CatchUpRequest) error {
		first.Do(func() {
			close(held)
			<-release
		})
		return nil
	}
	node.link.mu.Unlock()
	running := make(chan error, 1)
	go func() { running <- node.rep.CatchUpAsCopy(context.Background(), u32Slot, u32DB) }()
	<-held

	second := make(chan error, 1)
	go func() { second <- node.rep.CatchUpAsCopy(context.Background(), u32Slot, u32DB) }()
	select {
	case err := <-second:
		if err != nil {
			t.Fatalf("the second catch-up: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a second copy's catch-up waited for the one that was running")
	}
	node.link.mu.Lock()
	asked := len(node.link.requests)
	node.link.mu.Unlock()
	if asked != 1 {
		t.Fatalf("%d requests were sent, want the first catch-up's 1", asked)
	}
	close(release)
	if err := <-running; err != nil {
		t.Fatalf("the first catch-up: %v", err)
	}
}

// TestACopysChunkStopsAtTheRecordAfterThePromotion: the node becomes the owner between two
// records of one chunk. The record being applied is finished; the next is not applied.
func TestACopysChunkStopsAtTheRecordAfterThePromotion(t *testing.T) {
	var promoted atomic.Bool
	node := promotable(t, &promoted, func(cfg *ReplicatorConfig) {
		cfg.SchemaReady = func(uint64, cluster.Op) error {
			promoted.Store(true) // during the first record's apply
			return nil
		}
	})
	chunk := CatchUpChunk{Records: []PairRecord{
		u32Record(t, u32Pos(7, 1), cluster.Position{}),
		u32Record(t, u32Pos(7, 2), cluster.Position{}),
	}}
	whole, err := node.rep.applyChunk(context.Background(), "source", CatchUpRequest{Slot: u32Slot, DB: u32DB}, chunk, true)
	if err != nil || whole {
		t.Fatalf("applyChunk: whole %v, err %v, want a chunk cut short and no error", whole, err)
	}
	if got := node.applied(); got != u32Pos(7, 1) {
		t.Fatalf("the node applied through %s, want 7:1: a copy's catch-up applied a record after the node became the owner", got)
	}
}

// TestACopysChunkDoesNotResetAfterThePromotion: a record of a copy's chunk is judged
// divergent, and the node becomes the owner while it is judged. The pair is not reset. On a
// node that stays a copy the same record resets it.
func TestACopysChunkDoesNotResetAfterThePromotion(t *testing.T) {
	for _, role := range []struct {
		name     string
		promotes bool
		resets   int
	}{{"stays a copy", false, 1}, {"becomes the owner", true, 0}} {
		t.Run(role.name, func(t *testing.T) {
			var promoted, armed atomic.Bool
			node := promotable(t, &promoted, func(cfg *ReplicatorConfig) {
				cfg.SchemaReady = func(uint64, cluster.Op) error {
					if armed.Load() && role.promotes {
						promoted.Store(true)
					}
					return nil
				}
			})
			for seq := uint64(1); seq <= 3; seq++ {
				node.write(u32Frame(t, u32Epoch, u32Pos(7, seq), cluster.Position{}))
			}
			node.committed.Store(8)
			armed.Store(true)
			// An opener whose boundary is behind this node's position: a tail nobody owns.
			chunk := CatchUpChunk{Records: []PairRecord{u32Record(t, u32Pos(8, 1), u32Pos(7, 2))}}
			whole, err := node.rep.applyChunk(context.Background(), "source", CatchUpRequest{Slot: u32Slot, DB: u32DB}, chunk, true)
			if whole || node.seeds.count() != role.resets {
				t.Fatalf("whole %v, err %v, resets %d, want a chunk cut short and %d resets", whole, err, node.seeds.count(), role.resets)
			}
			if role.promotes && err != nil {
				t.Fatalf("a copy's chunk on a node that became the owner returned %v, want no error", err)
			}
		})
	}
}

// TestACopysCatchUpThatFindsNoSourceAfterThePromotionDoesNotMarkThePairCurrent (flash 4):
// choosing a source asks every copy for its position. The node becomes the owner meanwhile and
// nobody holds more than it. Whether the pair is current is then the handoff's catch-up to
// say, so the copy's catch-up leaves the mark alone.
func TestACopysCatchUpThatFindsNoSourceAfterThePromotionDoesNotMarkThePairCurrent(t *testing.T) {
	var promoted atomic.Bool
	node := promotable(t, &promoted, func(cfg *ReplicatorConfig) {
		cfg.CatchUpSource = func(uint16, uint64) (string, error) {
			promoted.Store(true)
			return "", ErrNoCatchUpSource
		}
	})
	if err := node.rep.CatchUpAsCopy(context.Background(), u32Slot, u32DB); err != nil {
		t.Fatalf("CatchUpAsCopy: %v", err)
	}
	if node.rep.IsCurrent(u32Slot, u32DB) {
		t.Fatal("a copy's catch-up marked the pair current after the node became its owner")
	}
}

// TestAPushedRecordsResetReadsTheOwnerAgainInsideItsFlight: between the owner check that lets
// a pushed record's reset start and the moment the reset holds the pair's flight, a whole
// handoff can run. The owner is read again inside the flight, where no handoff can pass
// unseen, and a node that owns the pair by then is not reset.
func TestAPushedRecordsResetReadsTheOwnerAgainInsideItsFlight(t *testing.T) {
	var armed atomic.Bool
	var asked atomic.Int32
	node := newU32Node(t, "node", func(cfg *ReplicatorConfig) {
		cfg.Owner = func(uint16, uint64) (string, bool) {
			// A copy at the first read after arming, the owner from the second on.
			if armed.Load() && asked.Add(1) > 1 {
				return "node", true
			}
			return "source", true
		}
	})
	for seq := uint64(1); seq <= 5; seq++ {
		node.write(u32Frame(t, u32Epoch, u32Pos(7, seq), cluster.Position{}))
	}
	node.committed.Store(8)
	armed.Store(true)

	reply := node.write(u32Frame(t, 8, u32Pos(8, 1), u32Pos(7, 3)))
	if asked.Load() != 2 {
		t.Fatalf("the owner was read %d times, want 2: once before the flight and once inside it", asked.Load())
	}
	if node.seeds.count() != 0 || reply.Ack.Outcome != AckRefused {
		t.Fatalf("resets %d, ack %+v, want no reset and a refusal", node.seeds.count(), reply.Ack)
	}
}
