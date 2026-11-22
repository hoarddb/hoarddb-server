package wire

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The drop of a seeding pair that left the copy set (BUG-0817): the record is voided, the
// range is deleted by the pair's own pull run, and the record is removed last. The order
// itself is pinned in reconcile_test.go (TestAWriteIsNotAdmittedWhileALeftSeedsRangeIsBeingDeleted
// and its neighbours); these tests pin who runs the drop and when an order is stale.

// leftSeedPuller is a puller over a reconciler rig, with no source: a pair it is asked to
// pull waits for one, so only a drop can end a run.
func leftSeedPuller(l *leftSeed, reported *atomic.Int64) *Puller {
	return leftSeedPullerOn(l, l.kv, func(error) { reported.Add(1) })
}

// leftSeedPullerOn is leftSeedPuller deleting through kv and handing its errors to onError.
func leftSeedPullerOn(l *leftSeed, kv storage.KV, onError func(error)) *Puller {
	return NewPuller(PullerConfig{
		Self: testSelf, Recon: l.g.rec, KV: kv, Options: SeedOptions{RetryBackoff: 5 * time.Millisecond},
		Committed: func() uint64 { return l.epoch }, StateSeq: func() uint64 { return uint64(l.shrunk.StateSeq) },
		Sources: func(uint16, uint64) []string { return nil },
		OnError: onError,
	})
}

// reportedErrors collects what a puller hands to OnError.
type reportedErrors struct {
	mu   sync.Mutex
	seen []string
}

func (r *reportedErrors) take(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, err.Error())
}

func (r *reportedErrors) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.seen...)
}

// runPuller runs p until done reports true, and fails the test after ten seconds.
func runPuller(t *testing.T, p *Puller, what string, done func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		p.Run(ctx)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for !done() && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	ok := done()
	cancel()
	<-finished
	if !ok {
		t.Fatalf("timed out waiting for %s", what)
	}
}

// TestTheSinkVoidsALeftSeedAndThePullersWorkerDeletesIt: the sink's arm writes the voided
// attempt and queues the pair, and deletes nothing itself, so the seeding driver never waits
// for the rate-limited delete; the puller's worker then empties the range and removes the
// record. Until the worker has run the pair says seeding and admits nothing.
// [guard: the sink only queues, with no void; the worker does not look for a left seed]
func TestTheSinkVoidsALeftSeedAndThePullersWorkerDeletesIt(t *testing.T) {
	l := newLeftSeed(t, t.TempDir())
	t.Cleanup(func() { _ = l.kv.Close() })
	var reported atomic.Int64
	puller := leftSeedPuller(l, &reported)
	parts := newSeedPartsFor(l.g.rec, l.shrunk, testSelf)
	parts.puller = puller
	before, _ := l.g.rec.Record(l.slot, reconDB)

	if err := parts.queueLeftSeed(l.key); err != nil {
		t.Fatalf("queueLeftSeed: %v", err)
	}
	rec, ok := diskRecord(t, l.kv, l.slot)
	if !ok || rec.State != machine.SeedSeeding || rec.Phase != machine.PhaseReset || len(rec.Cursor) != 0 || !rec.Cut.Zero() || rec.SeedID == before.SeedID {
		t.Fatalf("after the sink's arm the record on disk is %+v (present %v), want a new seeding attempt in phase reset", rec, ok)
	}
	if held := rangeKeys(t, l.kv, l.slot); held != 2 {
		t.Fatalf("the sink's arm left %d keys in the range, want both: the delete is the worker's", held)
	}
	if (recordSlots{parts: parts}).Admits(l.slot, reconDB) {
		t.Fatal("the pair admits a write while its drop is queued")
	}
	if puller.Pending() != 1 {
		t.Fatalf("the puller holds %d pairs, want the left seed", puller.Pending())
	}
	// The pass repeats its order on every state_seq; the attempt the delete runs under stays.
	if err := parts.queueLeftSeed(l.key); err != nil {
		t.Fatalf("queueLeftSeed again: %v", err)
	}
	if again, _ := diskRecord(t, l.kv, l.slot); again.SeedID != rec.SeedID {
		t.Fatal("a repeated order minted a new attempt over the one the delete runs under")
	}

	runPuller(t, puller, "the worker to drop the left seed", func() bool {
		_, held := l.g.rec.Record(l.slot, reconDB)
		return !held && puller.Pending() == 0
	})
	if rec, ok := diskRecord(t, l.kv, l.slot); ok {
		t.Errorf("the worker left the record %+v on disk", rec)
	}
	if held := rangeKeys(t, l.kv, l.slot); held != 0 {
		t.Errorf("the worker left %d keys in the range", held)
	}
	if reported.Load() != 0 {
		t.Errorf("the drop reported %d errors", reported.Load())
	}
}

// TestADropThatMeetsAPassesBatchWaitsWithoutAnError: the delete runs on a worker, so a
// reconcile pass can hold the pair busy (staged in a batch it is committing) when the drop
// wants to write its record. The drop waits and tries again; it reports no error, because a
// node that drops thousands of left seeds beside a pass would fill its log with them.
// [guard: dropLeft reports ErrPairBusy]
func TestADropThatMeetsAPassesBatchWaitsWithoutAnError(t *testing.T) {
	l := newLeftSeed(t, t.TempDir())
	t.Cleanup(func() { _ = l.kv.Close() })
	var reported atomic.Int64
	puller := leftSeedPuller(l, &reported)
	parts := newSeedPartsFor(l.g.rec, l.shrunk, testSelf)
	parts.puller = puller
	if err := parts.queueLeftSeed(l.key); err != nil {
		t.Fatalf("queueLeftSeed: %v", err)
	}
	setBusy := func(busy bool) {
		l.g.rec.mu.Lock()
		defer l.g.rec.mu.Unlock()
		if busy {
			l.g.rec.busy[l.key] = struct{}{}
			return
		}
		delete(l.g.rec.busy, l.key)
	}
	setBusy(true)
	runPuller(t, puller, "the drop to meet the busy pair", func() bool {
		status, held := puller.Status(l.slot, reconDB)
		return held && strings.Contains(status.Detail, "being changed")
	})
	if _, held := l.g.rec.Record(l.slot, reconDB); !held {
		t.Fatal("the record of a busy pair was removed")
	}
	if reported.Load() != 0 {
		t.Fatalf("a drop that met a busy pair reported %d errors, want a quiet retry", reported.Load())
	}
	setBusy(false)
	runPuller(t, puller, "the drop to finish once the pass's batch landed", func() bool {
		_, held := l.g.rec.Record(l.slot, reconDB)
		return !held && puller.Pending() == 0
	})
	if held := rangeKeys(t, l.kv, l.slot); held != 0 {
		t.Errorf("the drop left %d keys in the range", held)
	}
	if reported.Load() != 0 {
		t.Errorf("the drop reported %d errors", reported.Load())
	}
}

// TestADropThatFailsIsReportedAndTriedAgain: the other half of the quiet retry. A busy pair
// is a wait, but a delete that the store refuses, or a record commit that fails, is a fault
// the node must report, and the drop is tried again: the record stays a voided attempt until
// a later run empties the range and removes it. [guard: dropLeft reports nothing]
func TestADropThatFailsIsReportedAndTriedAgain(t *testing.T) {
	// dropped runs the worker until the pair has no record and the puller is done with it.
	dropped := func(t *testing.T, l *leftSeed, puller *Puller) {
		t.Helper()
		runPuller(t, puller, "the drop to finish once the store took its writes", func() bool {
			_, held := l.g.rec.Record(l.slot, reconDB)
			return !held && puller.Pending() == 0
		})
		if held := rangeKeys(t, l.kv, l.slot); held != 0 {
			t.Errorf("the retried drop left %d keys in the range", held)
		}
	}
	// voidedStill checks that a failed drop left the record a voided attempt.
	voidedStill := func(t *testing.T, l *leftSeed) {
		t.Helper()
		rec, ok := diskRecord(t, l.kv, l.slot)
		if !ok || rec.State != machine.SeedSeeding || rec.Phase != machine.PhaseReset {
			t.Fatalf("after the failed drop the record is %+v (present %v), want the voided attempt", rec, ok)
		}
	}

	t.Run("the delete fails", func(t *testing.T) {
		l := newLeftSeed(t, t.TempDir())
		t.Cleanup(func() { _ = l.kv.Close() })
		fail := true
		var reported reportedErrors
		puller := leftSeedPullerOn(l, failingStore{l.kv, &fail, l.d1}, reported.take)
		parts := newSeedPartsFor(l.g.rec, l.shrunk, testSelf)
		parts.puller = puller
		if err := parts.queueLeftSeed(l.key); err != nil {
			t.Fatalf("queueLeftSeed: %v", err)
		}
		runPuller(t, puller, "the failed delete to be reported", func() bool { return len(reported.all()) > 0 })
		if first := reported.all()[0]; !strings.Contains(first, "left seed") || !strings.Contains(first, "injected commit failure") {
			t.Fatalf("the failed delete was reported as %q, want the drop named and the store's error kept", first)
		}
		if named := strings.Count(reported.all()[0], "slot "); named != 1 {
			t.Errorf("the report names the slot %d times, want once: %s", named, reported.all()[0])
		}
		voidedStill(t, l)
		if held := rangeKeys(t, l.kv, l.slot); held != 2 {
			t.Fatalf("the range holds %d keys after a delete whose commit failed, want both", held)
		}
		fail = false
		dropped(t, l, puller)
	})
	t.Run("the removal's commit fails", func(t *testing.T) {
		fail := false
		l := newLeftSeedOn(t, t.TempDir(), func(kv *storage.BadgerKV) Store {
			return failingStore{kv, &fail, SeedRecordKey(findSlots(t, threeNodes(t)).copyOnly, reconDB)}
		})
		t.Cleanup(func() { _ = l.kv.Close() })
		var reported reportedErrors
		puller := leftSeedPullerOn(l, l.kv, reported.take)
		parts := newSeedPartsFor(l.g.rec, l.shrunk, testSelf)
		parts.puller = puller
		if err := parts.queueLeftSeed(l.key); err != nil {
			t.Fatalf("queueLeftSeed: %v", err)
		}
		fail = true // the void is committed; the removal is the record key's next write
		runPuller(t, puller, "the failed removal to be reported", func() bool { return len(reported.all()) > 0 })
		if first := reported.all()[0]; !strings.Contains(first, "left seed") || !strings.Contains(first, "injected commit failure") {
			t.Fatalf("the failed removal was reported as %q, want the drop named and the store's error kept", first)
		}
		if named := strings.Count(reported.all()[0], "slot "); named != 1 {
			t.Errorf("the report names the slot %d times, want once: %s", named, reported.all()[0])
		}
		voidedStill(t, l)
		fail = false
		dropped(t, l, puller)
	})
}

// TestARecordThatIsNotSeedingIsNeverALeftSeed: the puller asks about any pair it holds an
// entry for, and a pull that ended ready stays queued when it was asked for again. A ready
// copy or a retired one that the last pass saw outside the copy set waits for the retire
// gate: it is not a left seed, so nothing voids its record (which would zero its position)
// and nothing deletes its range. [guard: leftSeedLocked does not look at the state]
func TestARecordThatIsNotSeedingIsNeverALeftSeed(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	applied := cluster.Position{Epoch: uint64(st.Epoch), Seq: 7}
	for name, c := range map[string]struct {
		slot  uint16
		state uint8
	}{
		// The pass retires a ready copy outside the copy set and keeps the view it saw.
		"retired": {slots.outside, machine.SeedRetired},
		// A ready record keeps a view that says outside only until the pass that retires it has
		// committed; the test sets that view by hand on a copy the pass left ready.
		"ready": {slots.copyOnly, machine.SeedReady},
	} {
		t.Run(name, func(t *testing.T) {
			kv := openKV(t, t.TempDir())
			t.Cleanup(func() { _ = kv.Close() })
			putRecord(t, kv, c.slot, readyRecord(applied))
			data := storage.Key(c.slot, reconDB, 5, storage.NSDocuments, []byte("d1"))
			held := kv.NewBatch()
			defer held.Discard()
			if err := held.Set(data, []byte("kept")); err != nil {
				t.Fatal(err)
			}
			if err := held.Commit(true); err != nil {
				t.Fatal(err)
			}
			g := newRig(t, kv, st, testSelf, true)
			g.boot(t)
			key := reconcileKey{c.slot, reconDB}
			g.rec.mu.Lock()
			g.rec.pairs[key].seen.inTarget, g.rec.pairs[key].hasSeen = false, true
			g.rec.mu.Unlock()
			before, ok := diskRecord(t, kv, c.slot)
			if !ok || before.State != c.state {
				t.Fatalf("the record is %+v (present %v), want state %d outside the copy set", before, ok, c.state)
			}

			if g.rec.leftSeed(c.slot, reconDB) {
				t.Error("a record that is not seeding is called a left seed")
			}
			deleted := false
			if err := dropLeftSeed(t.Context(), g.rec, key, func(context.Context, reconcileKey) error {
				deleted = true
				return nil
			}); err != nil {
				t.Fatalf("dropLeftSeed: %v", err)
			}
			if deleted {
				t.Error("the range of a record that is not seeding was deleted as a left seed's")
			}
			var reported atomic.Int64
			puller := NewPuller(PullerConfig{
				Self: testSelf, Recon: g.rec, KV: kv, Options: SeedOptions{RetryBackoff: 5 * time.Millisecond},
				Committed: func() uint64 { return uint64(st.Epoch) }, StateSeq: func() uint64 { return uint64(st.StateSeq) },
				Sources: func(uint16, uint64) []string { return nil },
				OnError: func(error) { reported.Add(1) },
			})
			puller.Queue(c.slot, reconDB)
			runPuller(t, puller, "the puller to be done with the pair", func() bool { return puller.Pending() == 0 })
			after, ok := diskRecord(t, kv, c.slot)
			if !ok || !reflect.DeepEqual(after, before) {
				t.Errorf("the record changed from %+v to %+v (present %v)", before, after, ok)
			}
			if held := rangeKeys(t, kv, c.slot); held != 1 {
				t.Errorf("the range holds %d keys, want the one the copy held", held)
			}
			if reported.Load() != 0 {
				t.Errorf("the puller reported %d errors", reported.Load())
			}
		})
	}
}

// TestEachRecordWriteOfADropWaitsForAPassesBatch: the void and the removal each refuse a
// pair a reconcile pass has staged in a batch it has not committed. Either write beside the
// pass's own write of the same record would leave memory and disk apart, so each is pinned
// alone: the other one refusing must not hide it.
// [guard: voidLeftSeed does not check busy; removeLeftSeed does not check busy]
func TestEachRecordWriteOfADropWaitsForAPassesBatch(t *testing.T) {
	setBusy := func(l *leftSeed, busy bool) {
		l.g.rec.mu.Lock()
		defer l.g.rec.mu.Unlock()
		if busy {
			l.g.rec.busy[l.key] = struct{}{}
			return
		}
		delete(l.g.rec.busy, l.key)
	}
	t.Run("the void", func(t *testing.T) {
		l := newLeftSeed(t, t.TempDir())
		t.Cleanup(func() { _ = l.kv.Close() })
		before, _ := diskRecord(t, l.kv, l.slot)
		setBusy(l, true)
		deleted := false
		err := dropLeftSeed(t.Context(), l.g.rec, l.key, func(context.Context, reconcileKey) error {
			deleted = true
			return nil
		})
		if !errors.Is(err, ErrPairBusy) {
			t.Fatalf("a drop of a busy pair returned %v, want ErrPairBusy", err)
		}
		if deleted {
			t.Error("the range was deleted although the pair was busy at the void")
		}
		if after, ok := diskRecord(t, l.kv, l.slot); !ok || !reflect.DeepEqual(after, before) {
			t.Errorf("the void wrote %+v (present %v) over a busy pair's %+v", after, ok, before)
		}
	})
	t.Run("the removal", func(t *testing.T) {
		l := newLeftSeed(t, t.TempDir())
		t.Cleanup(func() { _ = l.kv.Close() })
		err := dropLeftSeed(t.Context(), l.g.rec, l.key, func(context.Context, reconcileKey) error {
			setBusy(l, true) // a pass staged the pair while the delete ran
			return nil
		})
		if !errors.Is(err, ErrPairBusy) {
			t.Fatalf("a drop whose pair turned busy during the delete returned %v, want ErrPairBusy", err)
		}
		if rec, ok := diskRecord(t, l.kv, l.slot); !ok || rec.State != machine.SeedSeeding || rec.Phase != machine.PhaseReset {
			t.Errorf("the removal changed a busy pair's record to %+v (present %v), want the voided attempt kept", rec, ok)
		}
		if _, ok := l.g.rec.Record(l.slot, reconDB); !ok {
			t.Error("the removal took a busy pair's record out of memory")
		}
	})
}

// TestADropOrderedBeforeThePairCameBackIsNotCarriedOut: the order is a pass's answer to one
// state, and the delete may start after a later pass saw the pair in the copy set again and
// wrote its new attempt. That record is not a left seed: nothing is voided, nothing deleted.
// [guard: voidLeftSeed does not ask what the last pass saw]
func TestADropOrderedBeforeThePairCameBackIsNotCarriedOut(t *testing.T) {
	l := newLeftSeed(t, t.TempDir())
	t.Cleanup(func() { _ = l.kv.Close() })
	l.g.state = l.back
	if err := l.g.rec.OnStateSeq(t.Context(), l.back); err != nil {
		t.Fatal(err)
	}
	back, _ := l.g.rec.Record(l.slot, reconDB)

	deleted := false
	if err := dropLeftSeed(t.Context(), l.g.rec, l.key, func(context.Context, reconcileKey) error {
		deleted = true
		return nil
	}); err != nil {
		t.Fatalf("dropLeftSeed: %v", err)
	}
	if deleted {
		t.Error("the range of a pair that is back in the copy set was deleted on a stale order")
	}
	if rec, ok := l.g.rec.Record(l.slot, reconDB); !ok || rec.SeedID != back.SeedID || rec.State != machine.SeedSeeding {
		t.Errorf("the stale order changed the record to %+v (present %v), want the attempt the pass wrote", rec, ok)
	}
	var reported atomic.Int64
	parts := newSeedPartsFor(l.g.rec, l.back, testSelf)
	parts.puller = leftSeedPuller(l, &reported)
	if err := parts.queueLeftSeed(l.key); err != nil {
		t.Fatal(err)
	}
	if parts.puller.Pending() != 0 {
		t.Error("the sink queued a drop for a pair that is back in the copy set")
	}
}

// TestASeedingRecordNoPassHasJudgedIsNotALeftSeed: after a restart a seeding record is only a
// record. Whether its node is in the copy set is the first pass's answer, against the loaded
// committed state, so nothing is dropped before that pass. [guard: leftSeedLocked ignores hasSeen]
func TestASeedingRecordNoPassHasJudgedIsNotALeftSeed(t *testing.T) {
	st := threeNodes(t)
	slot := findSlots(t, st).outside
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	pulled := seedingRecord(machine.PhaseSnapshot, cluster.Position{Epoch: uint64(st.Epoch), Seq: 5}, "k")
	putRecord(t, kv, slot, pulled)
	g := newRig(t, kv, st, testSelf, true)
	if err := g.rec.LoadRecords(); err != nil {
		t.Fatal(err)
	}
	if g.rec.leftSeed(slot, reconDB) {
		t.Fatal("a record no pass has judged is called a left seed")
	}
	deleted := false
	if err := dropLeftSeed(t.Context(), g.rec, reconcileKey{slot, reconDB}, func(context.Context, reconcileKey) error {
		deleted = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if rec, _ := diskRecord(t, kv, slot); deleted || rec.SeedID != pulled.SeedID || rec.Phase != machine.PhaseSnapshot {
		t.Fatalf("a drop before the first pass deleted the range (%v) or changed the record (%+v)", deleted, rec)
	}
}

// TestAPassThatSeesThePairComeBackDuringTheDeleteKeepsItsNewAttempt: the delete holds no pair
// lock, so a pass can run beside it. One that sees the node in the copy set again writes a
// new attempt and queues its pull; the drop must not remove that record when its delete
// ends, or the pair would be absent in the copy set with no pass left to seed it.
// [guard: removeLeftSeed does not compare the attempt]
func TestAPassThatSeesThePairComeBackDuringTheDeleteKeepsItsNewAttempt(t *testing.T) {
	l := newLeftSeed(t, t.TempDir())
	t.Cleanup(func() { _ = l.kv.Close() })
	var voided, renewed SeedRecord
	if err := dropLeftSeed(t.Context(), l.g.rec, l.key, func(context.Context, reconcileKey) error {
		voided, _ = l.g.rec.Record(l.slot, reconDB)
		l.g.sink.actions = nil
		l.g.state = l.back
		if err := l.g.rec.OnStateSeq(t.Context(), l.back); err != nil {
			return err
		}
		renewed, _ = l.g.rec.Record(l.slot, reconDB)
		return nil
	}); err != nil {
		t.Fatalf("dropLeftSeed: %v", err)
	}
	if acts := l.g.sink.of(l.slot); len(acts) != 1 || acts[0].Do != machine.SlotRestartSeed {
		t.Fatalf("the pass beside the delete answered %+v, want one RestartSeed", acts)
	}
	if renewed.SeedID == voided.SeedID {
		t.Fatal("the pass that saw the pair come back kept the voided attempt's seed id")
	}
	rec, ok := diskRecord(t, l.kv, l.slot)
	if !ok || rec.SeedID != renewed.SeedID || rec.State != machine.SeedSeeding || rec.Phase != machine.PhaseReset {
		t.Fatalf("after the drop the record is %+v (present %v), want the attempt the pass wrote", rec, ok)
	}
}

// TestARegistrationThatLandsDuringTheDeleteKeepsTheRecord: the voided attempt keeps its seed
// id when a source accepts it, so the id alone does not say the record is untouched. A record
// that registered while the delete ran describes a pull, and the drop must not remove it; the
// pair's next run finds a left seed again and drops it whole. The puller never lets this
// happen (one run per pair); the removal does not rely on that.
// [guard: removeLeftSeed does not look at the phase]
func TestARegistrationThatLandsDuringTheDeleteKeepsTheRecord(t *testing.T) {
	l := newLeftSeed(t, t.TempDir())
	t.Cleanup(func() { _ = l.kv.Close() })
	cut := cluster.Position{Epoch: l.epoch, Seq: 9}
	if err := dropLeftSeed(t.Context(), l.g.rec, l.key, func(context.Context, reconcileKey) error {
		_, err := l.g.rec.FeedSeed(l.slot, reconDB, FeedSeedRequest{
			Event: machine.RegisterAccepted{Cut: cut, WindowBytes: 1 << 20}, Cfg: machine.SeedConfig{Epoch: l.epoch},
		})
		return err
	}); err != nil {
		t.Fatalf("dropLeftSeed: %v", err)
	}
	rec, ok := diskRecord(t, l.kv, l.slot)
	if !ok || rec.State != machine.SeedSeeding || rec.Phase != machine.PhaseSnapshot || rec.Cut != cut {
		t.Fatalf("after the drop the record is %+v (present %v), want the registered attempt", rec, ok)
	}
	if !l.g.rec.leftSeed(l.slot, reconDB) {
		t.Fatal("the registered record of a pair outside the copy set is no longer a left seed, so nothing would drop it")
	}
}

// TestADropThatKeptItsRecordIsDroppedByTheNextRun: when the removal declines (the record is
// no longer the unregistered attempt the delete ran under) the worker's run must not end as
// if the pair were gone. Nothing else would look at the pair again on a cluster whose state
// holds still, and the record it kept may be a registered cut over the range the drop just
// emptied. The run looks at the pair once more and drops it whole.
// [guard: dropLeft ends the run without looking at the record]
func TestADropThatKeptItsRecordIsDroppedByTheNextRun(t *testing.T) {
	l := newLeftSeed(t, t.TempDir())
	t.Cleanup(func() { _ = l.kv.Close() })
	var registered atomic.Bool
	hooked := &snapshotHookKV{BadgerKV: l.kv}
	hooked.afterSnapshot = func() {
		// A registration lands on the voided attempt while its range is being deleted.
		_, err := l.g.rec.FeedSeed(l.slot, reconDB, FeedSeedRequest{
			Event: machine.RegisterAccepted{Cut: cluster.Position{Epoch: l.epoch, Seq: 9}, WindowBytes: 1 << 20}, Cfg: machine.SeedConfig{Epoch: l.epoch},
		})
		registered.Store(err == nil)
	}
	var reported atomic.Int64
	puller := leftSeedPullerOn(l, hooked, func(error) { reported.Add(1) })
	parts := newSeedPartsFor(l.g.rec, l.shrunk, testSelf)
	parts.puller = puller
	if err := parts.queueLeftSeed(l.key); err != nil {
		t.Fatalf("queueLeftSeed: %v", err)
	}
	runPuller(t, puller, "the pair whose first drop kept its record to be dropped by the next run", func() bool {
		_, held := l.g.rec.Record(l.slot, reconDB)
		return registered.Load() && !held && puller.Pending() == 0
	})
	if rec, ok := diskRecord(t, l.kv, l.slot); ok {
		t.Errorf("the record is still on disk: %+v", rec)
	}
	if held := rangeKeys(t, l.kv, l.slot); held != 0 {
		t.Errorf("the range holds %d keys", held)
	}
	if reported.Load() != 0 {
		t.Errorf("the drop reported %d errors", reported.Load())
	}
}

// t3Out is the rig's state with the database at RF 1, which leaves n2 out of the pair's copy set.
func t3Out(t *testing.T, rig *t3Rig) cluster.ClusterState {
	t.Helper()
	out := rig.state
	alterRF(t, &out, 1)
	return out
}

// t3Leave makes the rig's pair leave n2's copy set the way the node does: the pass over out,
// then the sink's arm for its answer. It runs on the puller's worker, inside a call.
func t3Leave(t *testing.T, rig *t3Rig, out cluster.ClusterState) {
	rig.state = out
	if err := rig.rec.OnStateSeq(context.Background(), out); err != nil {
		t.Errorf("the pass that sees the pair leave: %v", err)
	}
	if !rig.rec.leftSeed(rig.slot, reconDB) {
		t.Error("the pass did not leave the pair a left seed")
	}
	parts := &seedParts{recon: rig.rec, puller: rig.puller}
	if err := parts.queueLeftSeed(reconcileKey{rig.slot, reconDB}); err != nil {
		t.Errorf("queueLeftSeed: %v", err)
	}
}

// t3Dropped reports whether the rig's pair has no record and the puller is done with it.
func t3Dropped(rig *t3Rig) bool {
	_, held := rig.rec.Record(rig.slot, reconDB)
	return !held && rig.puller.Pending() == 0
}

// TestAPullWhosePairLeavesTheCopySetStopsAndItsRangeIsDeleted: the pair leaves while its pull
// is in the middle of the snapshot. The void makes the next chunk unwanted, the same run then
// finds a left seed and deletes the range, and the pull never completes into a ready copy
// that no pass would retire. [guard: queueLeftSeed does not void; attempt pulls a left seed]
func TestAPullWhosePairLeavesTheCopySetStopsAndItsRangeIsDeleted(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 3})
	rig.owner = "x"
	rig.sources = map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 10, t3Cut(t, rig), 42)
	out := t3Out(t, rig)
	var chunks atomic.Int64
	rig.wrap = func(_ string, _ transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
		if err == nil && reply != nil && reply.Env.Kind == transport.KindSeedChunk && chunks.Add(1) == 2 {
			t3Leave(t, rig, out) // one chunk is applied, the second is on its way
		}
		return reply, err
	}

	rig.pullUntil("the left seed to be dropped", func() bool { return chunks.Load() >= 2 && t3Dropped(rig) })

	if left := t3Dump(t, rig.kv, rig.slot, reconDB); len(left) != 0 {
		t.Errorf("the dropped pair's range still holds %d keys", len(left))
	}
	if rig.ready.Load() != 0 {
		t.Errorf("the pull of a pair that left the copy set completed %d times", rig.ready.Load())
	}
	if got := chunks.Load(); got != 2 {
		t.Errorf("the pull asked for %d chunks, want none after the one that met the void", got)
	}
}

// TestAnAcceptThatLandsOnAVoidedAttemptAsksForNoChunk: the pair leaves while SEED_REGISTER is
// in flight. The record is still an unregistered attempt, so the void keeps it and the
// accept registers it; the pull must not go on to stream chunks into a range that is about
// to be deleted. [guard: attempt does not look for a left seed after registering]
func TestAnAcceptThatLandsOnAVoidedAttemptAsksForNoChunk(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 3})
	rig.owner = "x"
	rig.sources = map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 10, t3Cut(t, rig), 42)
	out := t3Out(t, rig)
	var accepts atomic.Int64
	rig.wrap = func(_ string, _ transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
		if err == nil && reply != nil && reply.Env.Kind == transport.KindSeedAccept && accepts.Add(1) == 1 {
			t3Leave(t, rig, out)
		}
		return reply, err
	}

	rig.pullUntil("the left seed to be dropped", func() bool { return accepts.Load() >= 1 && t3Dropped(rig) })

	if acks := len(rig.requests(transport.KindSeedAck)); acks != 0 {
		t.Errorf("the pull sent %d SEED_ACKs for a pair that had left the copy set, want none", acks)
	}
	if left := t3Dump(t, rig.kv, rig.slot, reconDB); len(left) != 0 {
		t.Errorf("the dropped pair's range still holds %d keys", len(left))
	}
	if rig.ready.Load() != 0 {
		t.Errorf("the pull of a pair that left the copy set completed %d times", rig.ready.Load())
	}
}
