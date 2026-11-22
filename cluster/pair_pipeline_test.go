package cluster

import (
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// pipelineWriter drives the two sections of one owner write the way the wire writer does, with a
// pause standing in for the group commit between them.
type pipelineWriter struct {
	set  *PairSet
	slot uint16
	db   uint64
	cap  int
	// doc gives each write a document no other write shares.
	doc atomic.Uint64
	// orderViolations counts a second section that met an out-of-order head.
	orderViolations atomic.Int64
}

func (w *pipelineWriter) write(pause time.Duration) {
	var cursor uint64
	ref := w.set.WithPipeline(w.slot, w.db, func(p *Pair, pl *Pipeline) bool {
		if !pl.WaitTurn(w.cap) {
			return false
		}
		p.AppendedSeq++
		cursor = p.AppendedSeq
		pl.Push(PendingWrite{Pos: Position{Epoch: 1, Seq: p.AppendedSeq}, Cursor: cursor, DocKey: w.doc.Add(1) + 1, DedupeKey: w.doc.Load() + 1<<40})
		return true
	})
	time.Sleep(pause)
	ref.With(func(p *Pair, pl *Pipeline) {
		for head := pl.Head(); head != nil && head.Cursor <= cursor; head = pl.Head() {
			if head.Pos.Seq != p.AppliedPos.Seq+1 {
				w.orderViolations.Add(1)
			}
			p.AppliedPos, p.Ordinal = head.Pos, p.Ordinal+1
			pl.PopHead()
		}
	})
}

// TestPairPipelineRace: [GUARD]. Pipelined writers on one pair, a mix of With, Snapshot and
// WithHeld callers on it, and Drop calls on another pair, with random pauses: no data race (run
// with -race), no deadlock, no section of the lock-taking entry points ever sees a write that is
// appended and not applied, and every position is applied once, in order.
func TestPairPipelineRace(t *testing.T) {
	set := &PairSet{}
	w := &pipelineWriter{set: set, slot: 1, db: 1, cap: 8}
	const writers, perWriter = 16, 40
	var wg sync.WaitGroup
	var torn atomic.Int64
	stop := make(chan struct{})
	check := func(p *Pair) {
		if p.AppendedSeq != p.AppliedPos.Seq {
			torn.Add(1)
		}
	}
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWriter {
				w.write(time.Duration(rand.IntN(400)) * time.Microsecond)
			}
		}()
	}
	var observers sync.WaitGroup
	for range 4 {
		observers.Add(3)
		go func() {
			defer observers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					set.With(1, 1, check)
				}
			}
		}()
		go func() {
			defer observers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					if p := set.Snapshot(1, 1); p.AppendedSeq != p.AppliedPos.Seq {
						torn.Add(1)
					}
				}
			}
		}()
		go func() {
			defer observers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					set.WithHeld(1, 1, check)
					set.With(2, 1, func(*Pair) {})
					set.Drop(2, 1)
				}
			}
		}()
	}
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(30 * time.Second):
		t.Fatal("the writers did not finish within 30 s: a deadlock between the pipeline and the lock-taking callers")
	}
	close(stop)
	observers.Wait()
	if got := torn.Load(); got != 0 {
		t.Fatalf("%d lock-taking sections saw a pair with an appended and unapplied write", got)
	}
	if got := w.orderViolations.Load(); got != 0 {
		t.Fatalf("%d second sections met a head that was not the next position", got)
	}
	if p := set.Snapshot(1, 1); p.AppliedPos.Seq != writers*perWriter || p.Ordinal != writers*perWriter {
		t.Fatalf("pair = %+v, want %d applied once each", p, writers*perWriter)
	}
}

// TestADrainWaiterIsNotStarved: [GUARD: no waiter count]. Under a continuous stream of pipelined
// writers a With call returns within a few group commits, not after the stream ends.
func TestADrainWaiterIsNotStarved(t *testing.T) {
	set := &PairSet{}
	w := &pipelineWriter{set: set, slot: 1, db: 1, cap: 16}
	const pause = 20 * time.Millisecond
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					w.write(pause)
				}
			}
		}()
	}
	time.Sleep(3 * pause)
	for range 5 {
		start := time.Now()
		set.With(1, 1, func(p *Pair) {
			if p.AppendedSeq != p.AppliedPos.Seq {
				t.Errorf("With ran on a pair holding an unapplied write: appended %d applied %d", p.AppendedSeq, p.AppliedPos.Seq)
			}
		})
		if took := time.Since(start); took > 20*pause {
			t.Errorf("With took %s under a stream of writers, want a few group commits (pause %s)", took, pause)
		}
		time.Sleep(pause)
	}
	close(stop)
	wg.Wait()
}

// TestWritersProgressUnderARunOfDrainCallers: [GUARD: the first section blocked while any waiter
// exists]. Four goroutines call Snapshot in a loop on a pair with pipelined writers: every write
// still completes within a few flushes plus a few sections.
func TestWritersProgressUnderARunOfDrainCallers(t *testing.T) {
	set := &PairSet{}
	w := &pipelineWriter{set: set, slot: 1, db: 1, cap: 16}
	const pause = 20 * time.Millisecond
	stop := make(chan struct{})
	var observers sync.WaitGroup
	for range 4 {
		observers.Add(1)
		go func() {
			defer observers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					set.Snapshot(1, 1)
				}
			}
		}()
	}
	var wg sync.WaitGroup
	var slowest atomic.Int64
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				start := time.Now()
				w.write(pause)
				for took := int64(time.Since(start)); took > slowest.Load(); {
					if slowest.CompareAndSwap(slowest.Load(), took) {
						break
					}
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	observers.Wait()
	if time.Duration(slowest.Load()) > 25*pause {
		t.Fatalf("the slowest write took %s with drain callers running, want a few flushes (pause %s)", time.Duration(slowest.Load()), pause)
	}
}

// TestDropDrainsBeforeItForgetsThePair: [GUARD: the drain after the map deletion]. Drop on a pair with
// pending writes waits for their second sections; a With that arrives meanwhile does not return a
// fresh zero Pair before the drain ends; and the dropped entry's list is empty when it is marked
// dropped.
func TestDropDrainsBeforeItForgetsThePair(t *testing.T) {
	set := &PairSet{}
	var cursors [3]uint64
	var ref *PipelineRef
	for i := range cursors {
		ref = set.WithPipeline(1, 1, func(p *Pair, pl *Pipeline) bool {
			p.AppendedSeq++
			cursors[i] = p.AppendedSeq
			pl.Push(PendingWrite{Pos: Position{Epoch: 1, Seq: p.AppendedSeq}, Cursor: p.AppendedSeq, DocKey: uint64(i + 2)})
			return true
		})
	}
	var secondSectionsDone atomic.Bool
	dropped := make(chan struct{})
	go func() { set.Drop(1, 1); close(dropped) }()
	withSaw := make(chan Pair, 1)
	time.Sleep(50 * time.Millisecond)
	go func() {
		var seen Pair
		set.With(1, 1, func(p *Pair) {
			if !secondSectionsDone.Load() {
				t.Error("a With ran before the pending second sections finished")
			}
			seen = *p
		})
		withSaw <- seen
	}()
	select {
	case <-dropped:
		t.Fatal("Drop returned while writes were pending")
	case <-time.After(100 * time.Millisecond):
	}
	ref.With(func(p *Pair, pl *Pipeline) {
		for head := pl.Head(); head != nil; head = pl.Head() {
			p.AppliedPos, p.Ordinal = head.Pos, p.Ordinal+1
			pl.PopHead()
		}
		secondSectionsDone.Store(true)
		if pl.Len() != 0 {
			t.Error("the list is not empty after every second section ran")
		}
	})
	select {
	case <-dropped:
	case <-time.After(5 * time.Second):
		t.Fatal("Drop did not return after the pending writes applied")
	}
	// The With that waited ran either before the drop, on the fully applied pair, or after it, on the fresh
	// zero pair; it never ran on a half-applied pair, which the flag check above would have caught.
	got := <-withSaw
	applied := got.AppliedPos == (Position{Epoch: 1, Seq: 3}) && got.AppendedSeq == 3
	fresh := got.AppliedPos.Zero() && got.AppendedSeq == 0
	if !applied && !fresh {
		t.Fatalf("the With that waited saw %+v, want the applied pair or the fresh one that follows the drop", got)
	}
}

// TestWaitTurnHoldsTheCap: a first section waits while the list is full and goes when a second section
// frees a slot.
func TestWaitTurnHoldsTheCap(t *testing.T) {
	set := &PairSet{}
	var first *PipelineRef
	push := func() *PipelineRef {
		return set.WithPipeline(1, 1, func(p *Pair, pl *Pipeline) bool {
			if !pl.WaitTurn(1) {
				return false
			}
			p.AppendedSeq++
			pl.Push(PendingWrite{Pos: Position{Epoch: 1, Seq: p.AppendedSeq}, Cursor: p.AppendedSeq, DocKey: p.AppendedSeq + 1})
			return true
		})
	}
	first = push()
	second := make(chan struct{})
	go func() { push(); close(second) }()
	select {
	case <-second:
		t.Fatal("a second write was appended with a cap of 1")
	case <-time.After(100 * time.Millisecond):
	}
	first.With(func(p *Pair, pl *Pipeline) { p.AppliedPos = pl.Head().Pos; pl.PopHead() })
	select {
	case <-second:
	case <-time.After(5 * time.Second):
		t.Fatal("the held-back write did not go after a slot was freed")
	}
}

// TestPipelineConflicts: the conflict rule of the first section.
func TestPipelineConflicts(t *testing.T) {
	set := &PairSet{}
	set.WithPipeline(1, 1, func(p *Pair, pl *Pipeline) bool {
		if pl.Conflicts(5, 6) {
			t.Error("an empty list conflicts with nothing")
		}
		pl.Push(PendingWrite{Pos: Position{Epoch: 1, Seq: 1}, Cursor: 1, DocKey: 5, DedupeKey: 6})
		for _, tc := range []struct {
			doc, dedupe uint64
			want        bool
			name        string
		}{
			{5, 99, true, "same document"},
			{99, 6, true, "same dedupe key"},
			{99, 98, false, "unrelated"},
			{0, 98, true, "a write that is not one document"},
		} {
			if got := pl.Conflicts(tc.doc, tc.dedupe); got != tc.want {
				t.Errorf("%s: Conflicts = %v, want %v", tc.name, got, tc.want)
			}
		}
		if pl.Epoch() != 1 {
			t.Errorf("Epoch = %d, want 1", pl.Epoch())
		}
		return true
	})
}

// TestTheSecondSectionPublishesTheView: [GUARD: WithPipeline does not publish on the second section].
// ReadView returns the applied position right after the second section, never before.
func TestTheSecondSectionPublishesTheView(t *testing.T) {
	set := &PairSet{}
	ref := set.WithPipeline(1, 1, func(p *Pair, pl *Pipeline) bool {
		p.AppendedSeq = 1
		pl.Push(PendingWrite{Pos: Position{Epoch: 1, Seq: 1}, Cursor: 1, DocKey: 2})
		return true
	})
	if view := set.ReadView(1, 1); !view.AppliedPos.Zero() {
		t.Fatalf("the view says applied %s before the second section", view.AppliedPos)
	}
	ref.With(func(p *Pair, pl *Pipeline) { p.AppliedPos = pl.Head().Pos; pl.PopHead() })
	if view := set.ReadView(1, 1); view.AppliedPos != (Position{Epoch: 1, Seq: 1}) {
		t.Fatalf("the view says applied %s after the second section, want 1:1", view.AppliedPos)
	}
}
