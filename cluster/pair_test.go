package cluster

import (
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func TestPairStateIsOneValuePerPair(t *testing.T) {
	var set PairSet
	set.With(7, 3, func(p *Pair) { p.AppendedSeq = 10 })
	set.With(7, 3, func(p *Pair) { p.AppendedSeq++ })
	got := set.Snapshot(7, 3)
	if got.AppendedSeq != 11 {
		t.Fatalf("two With calls on one pair must share one state: AppendedSeq = %d, want 11", got.AppendedSeq)
	}
	got.AppendedSeq = 999
	if again := set.Snapshot(7, 3); again.AppendedSeq != 11 {
		t.Fatalf("Snapshot must return a copy: stored value became %d", again.AppendedSeq)
	}
	if other := set.Snapshot(7, 4); other != (Pair{}) {
		t.Fatalf("a different database must have its own state, got %+v", other)
	}
	// 80 B: the owner state with the append's epoch beside its seq (write path §3.1) and the
	// peer string header; a copy per bucket would show up as growth here, so pin the
	// ceiling.
	if size := unsafe.Sizeof(Pair{}); size > 80 {
		t.Fatalf("Pair is %d bytes, want at most 80", size)
	}
}

func TestPairLockIsPerPairNotPerNode(t *testing.T) {
	var set PairSet
	held := make(chan struct{})
	release := make(chan struct{})
	var holder sync.WaitGroup
	holder.Add(1)
	go func() {
		defer holder.Done()
		set.With(1, 1, func(*Pair) {
			close(held)
			<-release
		})
	}()
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("With never ran its function")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		set.With(2, 1, func(p *Pair) { p.AppendedSeq = 1 })
		set.With(1, 2, func(p *Pair) { p.AppendedSeq = 1 })
		set.Snapshot(3, 1)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		close(release)
		holder.Wait()
		t.Fatal("work on other pairs stalled behind one held pair lock: the lock is not per pair")
	}
	close(release)
	holder.Wait()
}

func TestNoPairStateForABucket(t *testing.T) {
	keyType := reflect.TypeOf(pairKey{})
	if keyType.NumField() != 2 ||
		keyType.Field(0).Type != reflect.TypeOf(uint16(0)) ||
		keyType.Field(1).Type != reflect.TypeOf(uint64(0)) {
		t.Fatalf("pairKey must be exactly (slot uint16, db uint64), got %v", keyType)
	}
	var set PairSet
	for round := 0; round < 1000; round++ {
		for slot := uint16(0); slot < 3; slot++ {
			set.With(slot, 9, func(p *Pair) { p.AppendedSeq++ })
		}
	}
	if got := set.pairCount(); got != 3 {
		t.Fatalf("map holds %d entries after 3000 writes to 3 pairs, want 3", got)
	}
	set.Drop(0, 9)
	if got := set.pairCount(); got != 2 {
		t.Fatalf("map holds %d entries after Drop, want 2", got)
	}
	set.Snapshot(5, 5)
	if got := set.pairCount(); got != 2 {
		t.Fatalf("Snapshot of an unknown pair must not create it, map holds %d", got)
	}
}

func TestPairCarriesTheAppliedOrdinal(t *testing.T) {
	var set PairSet
	set.With(0x1362, 1, func(p *Pair) {
		p.AppliedPos = Position{Epoch: 41, Seq: 98213}
		p.Ordinal = 98213
	})
	got := set.Snapshot(0x1362, 1)
	if got.Ordinal != 98213 || got.AppliedPos != (Position{Epoch: 41, Seq: 98213}) {
		t.Fatalf("applied position and ordinal must travel together: %+v", got)
	}
	// An epoch change resets seq-side fields, never the ordinal.
	set.With(0x1362, 1, func(p *Pair) {
		p.AppendedSeq = 0
		p.AppliedPos = Position{Epoch: 42, Seq: 1}
		p.Ordinal++
	})
	if got := set.Snapshot(0x1362, 1); got.Ordinal != 98214 {
		t.Fatalf("ordinal after an epoch change = %d, want 98214", got.Ordinal)
	}
}

func TestPairSetRaceManyPairs(t *testing.T) {
	var set PairSet
	const workers, pairs, rounds = 32, 64, 200
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := 0; round < rounds; round++ {
				slot := uint16((worker + round) % pairs)
				set.With(slot, 1, func(p *Pair) { p.Ordinal++ })
				set.Snapshot(slot, 1)
				if round%50 == 49 {
					set.Drop(slot, 2)
				}
			}
		}()
	}
	wg.Wait()
	var total uint64
	for slot := uint16(0); slot < pairs; slot++ {
		total += set.Snapshot(slot, 1).Ordinal
	}
	if total != workers*rounds {
		t.Fatalf("lost updates: total ordinal %d, want %d", total, workers*rounds)
	}
}

func TestPairDropIsSafeAgainstAHeldLock(t *testing.T) {
	var set PairSet
	held := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		set.With(1, 1, func(p *Pair) {
			close(held)
			<-release
			p.Ordinal = 5
		})
	}()
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("With never ran its function")
	}
	dropped := make(chan struct{})
	go func() {
		defer close(dropped)
		set.Drop(1, 1)
	}()
	close(release)
	<-finished
	<-dropped
	set.With(1, 1, func(p *Pair) {
		if p.Ordinal != 0 {
			t.Errorf("a dropped pair must come back empty, ordinal = %d", p.Ordinal)
		}
	})
}

// A read of the pair must not wait for a holder of the pair's lock that is inside a flush. Two paths hold
// it across an fsync: an RF 2 and larger write (PairSet.With around AppendFrom) and a copy apply
// (replicate_apply.go). A plain With stands in for both here. The read must see the holder's state once
// the holder has released the lock.
func TestReadViewDoesNotWaitForARunningWith(t *testing.T) {
	var set PairSet
	set.With(5, 1, func(p *Pair) { p.AppendedSeq = 1 })
	inside, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		set.With(5, 1, func(p *Pair) {
			p.AppendedSeq = 2
			close(inside)
			<-release
		})
	}()
	<-inside
	got := make(chan Pair, 1)
	go func() { got <- set.ReadView(5, 1) }()
	select {
	case view := <-got:
		if view.AppendedSeq != 1 {
			t.Fatalf("ReadView during a running With: AppendedSeq = %d, want the published 1", view.AppendedSeq)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ReadView waited for a running With")
	}
	close(release)
	<-done
	if view := set.ReadView(5, 1); view.AppendedSeq != 2 {
		t.Fatalf("ReadView after With: AppendedSeq = %d, want 2", view.AppendedSeq)
	}
	set.Drop(5, 1)
	if view := set.ReadView(5, 1); view != (Pair{}) {
		t.Fatalf("ReadView of a dropped pair = %+v, want the zero Pair", view)
	}
}
