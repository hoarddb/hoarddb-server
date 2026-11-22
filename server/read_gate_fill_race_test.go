package server

import (
	"github.com/hoarddb/hoarddb-server/storage"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// gateFillFixture is a three-node cluster with one bucket, and the gate's view
// of it at node 0.
type gateFillFixture struct {
	cluster  *Cluster
	key      placementKey
	record   *bucketRec
	stopFunc func()
}

func newGateFillFixture(t *testing.T) *gateFillFixture {
	t.Helper()
	tc := newTestCluster(t, 3, 3)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: testBucket, Type: BucketTypeHash})
	c := clusterOf(tc, 0)
	// The create reaches node 0 by replication when another node owns it.
	var served *inMemoryBucket
	var err error
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if served, err = c.s.resolveBucket(testDatabase, testBucket); err == nil {
			break
		}
	}
	if err != nil {
		tc.stop()
		t.Fatalf("setup: resolving the bucket: %v", err)
	}
	return &gateFillFixture{cluster: c, key: placementOf(testDatabase, testBucket), record: served.rec, stopFunc: tc.stop}
}

// forgetCache empties the lineage cache under the bucket's lock, as an attach
// or an originated apply on an unknown cache leaves it.
func (f *gateFillFixture) forgetCache() {
	f.record.mu.Lock()
	f.record.lineageFlags.Store(0)
	f.record.mu.Unlock()
}

func (f *gateFillFixture) document(t testing.TB) store.BucketState {
	t.Helper()
	st, err := f.cluster.s.bucketStateNamed(testDatabase, testBucket)
	if err != nil {
		t.Fatalf("reading the state document: %v", err)
	}
	return st
}

func (f *gateFillFixture) rewrite(t testing.TB, change func(*store.BucketState) bool) {
	t.Helper()
	if err := f.cluster.s.rewriteLineageIf(testDatabase, testBucket, change); err != nil {
		t.Errorf("rewriting the lineage: %v", err)
	}
}

// assertCacheUnderLock checks, holding the bucket's lock so that no writer is
// between its commit and its cache update, that a filled cache equals the
// document.
func (f *gateFillFixture) assertCacheUnderLock(t testing.TB, when string) {
	t.Helper()
	f.record.mu.Lock()
	defer f.record.mu.Unlock()
	snapshot := f.cluster.s.kv.NewSnapshot()
	defer snapshot.Discard()
	doc, err := store.LoadBucketState(snapshot, f.record.slot, storage.LegacyDatabaseID(testDatabase), f.record.id.Load())
	if err != nil {
		t.Errorf("reading the state document: %v", err)
		return
	}
	f.compareCache(t, when, doc)
}

// assertCacheNeverContradicts fails when the cache is filled with something
// other than the document says.
func (f *gateFillFixture) assertCacheNeverContradicts(t testing.TB, when string) {
	t.Helper()
	f.compareCache(t, when, f.document(t))
}

func (f *gateFillFixture) compareCache(t testing.TB, when string, doc store.BucketState) {
	t.Helper()
	flags := f.record.lineageFlags.Load()
	if flags&lineageKnown == 0 {
		return
	}
	term, opened := f.record.term.Load(), flags&lineageOpened != 0
	if term != doc.Term || opened != doc.Opened {
		t.Errorf("%s: the cache says term %d opened %v (flags %b); the document says term %d opened %v",
			when, term, opened, flags, doc.Term, doc.Opened)
	}
}

// TestALateFillFromAnOlderDocumentDoesNotRegressTheCache is the BUG-0566
// rework's regression: a read that holds a state document, then loses a race
// to a lineage writer, must not write what it holds into the cache. The writer
// commits term 7 opened and leaves the cache unknown (as an originated apply
// does); the read still carries the older document, term 0. Guard: fill the
// cache from the document the caller carries instead of reading it under the
// lock.
func TestALateFillFromAnOlderDocumentDoesNotRegressTheCache(t *testing.T) {
	f := newGateFillFixture(t)
	defer f.stopFunc()
	older := f.document(t)
	f.rewrite(t, func(st *store.BucketState) bool {
		st.Term, st.Opened = 7, true
		return true
	})
	f.forgetCache()

	awaits, err := f.cluster.ownerReadAwaitsTakeOverState(f.key, f.cluster.Epoch(), testDatabase, testBucket, &older)
	if err != nil {
		t.Fatalf("the read gate: %v", err)
	}
	if awaits {
		t.Errorf("the gate refused a read of a copy whose document says term 7 opened")
	}
	f.assertCacheNeverContradicts(t, "after a late fill from an older document")
	if flags := f.record.lineageFlags.Load(); flags&lineageKnown != 0 && (f.record.term.Load() != 7 || flags&lineageOpened == 0) {
		t.Errorf("cache regressed to term %d flags %b after a late fill from an older document; the document says term 7 opened",
			f.record.term.Load(), flags)
	}
}

// TestLineageWritersAndGatedReadsLeaveTheCacheEqualToTheDocument runs lineage
// writers (term accepts and opened marks), cache resets and gated owner reads
// that carry stale documents against one bucket, under the race detector.
// Afterwards the cache is unknown or equals the document, and one more read
// fills it with the document's value (BUG-0566).
func TestLineageWritersAndGatedReadsLeaveTheCacheEqualToTheDocument(t *testing.T) {
	f := newGateFillFixture(t)
	defer f.stopFunc()
	const rounds = 300
	var termCounter atomic.Uint64
	var group sync.WaitGroup
	run := func(body func()) {
		group.Add(1)
		go func() {
			defer group.Done()
			body()
		}()
	}
	for writer := 0; writer < 2; writer++ {
		run(func() {
			for i := 0; i < rounds; i++ {
				term, opened := termCounter.Add(1), i%2 == 0
				f.rewrite(t, func(st *store.BucketState) bool {
					st.Term, st.Opened = term, opened
					return true
				})
			}
		})
	}
	run(func() {
		for i := 0; i < rounds*4; i++ {
			f.forgetCache()
			runtime.Gosched()
		}
	})
	run(func() {
		for i := 0; i < rounds*4; i++ {
			f.assertCacheUnderLock(t, "while writers and readers run")
			runtime.Gosched()
		}
	})
	for reader := 0; reader < 4; reader++ {
		run(func() {
			for i := 0; i < rounds*4; i++ {
				var carried *store.BucketState
				if i%2 == 0 {
					older := f.document(t)
					runtime.Gosched()
					carried = &older
				}
				if _, err := f.cluster.ownerReadAwaitsTakeOverState(f.key, f.cluster.Epoch(), testDatabase, testBucket, carried); err != nil {
					t.Errorf("the read gate: %v", err)
					return
				}
			}
		})
	}
	group.Wait()

	f.assertCacheNeverContradicts(t, "after the writers and readers stopped")
	f.forgetCache()
	doc := f.document(t)
	if _, err := f.cluster.ownerReadAwaitsTakeOverState(f.key, f.cluster.Epoch(), testDatabase, testBucket, nil); err != nil {
		t.Fatalf("the final read: %v", err)
	}
	flags := f.record.lineageFlags.Load()
	if flags&lineageKnown == 0 {
		t.Fatalf("the final read left the cache unknown with no writer running")
	}
	if f.record.term.Load() != doc.Term || (flags&lineageOpened != 0) != doc.Opened {
		t.Errorf("after the final read the cache says term %d flags %b, the document says term %d opened %v",
			f.record.term.Load(), flags, doc.Term, doc.Opened)
	}
}

// TestNoOwnerReadIsServedWhileTheDocumentSaysNotOpened: the copy is in a term
// it has not opened; one writer marks it opened while readers, whose carried
// documents are stale and whose cache is reset under them, ask the gate. A
// read the gate lets through must find the document opened by the time it
// returns, because the opened mark only ever turns on here (BUG-0566).
func TestNoOwnerReadIsServedWhileTheDocumentSaysNotOpened(t *testing.T) {
	f := newGateFillFixture(t)
	defer f.stopFunc()
	f.rewrite(t, func(st *store.BucketState) bool {
		st.Term, st.Opened = 5, false
		return true
	})
	f.forgetCache()
	var servedBeforeOpen atomic.Int64
	var opened atomic.Bool
	var group sync.WaitGroup
	for reader := 0; reader < 4; reader++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for i := 0; i < 3000; i++ {
				var carried *store.BucketState
				if i%2 == 0 {
					older := f.document(t)
					runtime.Gosched()
					carried = &older
				}
				awaits, err := f.cluster.ownerReadAwaitsTakeOverState(f.key, f.cluster.Epoch(), testDatabase, testBucket, carried)
				if err != nil {
					t.Errorf("the read gate: %v", err)
					return
				}
				if !awaits && !f.document(t).Opened {
					servedBeforeOpen.Add(1)
				}
			}
		}()
	}
	group.Add(2)
	go func() {
		defer group.Done()
		for i := 0; i < 3000 && !opened.Load(); i++ {
			f.forgetCache()
			f.assertCacheUnderLock(t, "while the opened mark is pending")
			runtime.Gosched()
		}
	}()
	go func() {
		defer group.Done()
		for i := 0; i < 200; i++ {
			runtime.Gosched()
		}
		f.rewrite(t, func(st *store.BucketState) bool {
			st.Opened = true
			return true
		})
		opened.Store(true)
	}()
	group.Wait()
	if n := servedBeforeOpen.Load(); n != 0 {
		t.Errorf("%d reads were served while the document said the copy was not opened", n)
	}
	f.assertCacheNeverContradicts(t, "after the opened mark")
}

// TestAnApplyThatMovesThePositionClearsTheCachedZeroPosition: the cache keeps
// "the position is zero" for a term-0 copy, and an apply that carries the
// origin's sequence moves the position in the state document, so the cache
// must stop saying zero or a term-0 copy that has applied entries would be
// judged opened by the owner-with-no-lineage rule (BUG-0566).
func TestAnApplyThatMovesThePositionClearsTheCachedZeroPosition(t *testing.T) {
	var rec bucketRec
	rec.setLineageStateLocked(store.BucketState{})
	rec.noteAppliedLocked(0, false)
	if rec.lineageFlags.Load()&lineagePositionZero == 0 {
		t.Fatalf("an apply with no origin sequence cleared the zero position")
	}
	rec.noteAppliedLocked(9, false)
	if rec.lineageFlags.Load()&lineagePositionZero != 0 {
		t.Errorf("an apply at origin sequence 9 left the cache saying the position is zero")
	}
	rec.noteAppliedLocked(9, true)
	if rec.lineageFlags.Load()&lineageOpened == 0 {
		t.Errorf("an apply this node originated did not mark the cache opened")
	}
	var unknown bucketRec
	unknown.noteAppliedLocked(9, true)
	if unknown.lineageFlags.Load() != 0 {
		t.Errorf("an apply filled a cache that was unknown")
	}
}
