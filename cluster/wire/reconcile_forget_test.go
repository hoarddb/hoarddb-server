package wire

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// retiringRecord is a copy that is being taken out of the set: its delete has
// begun, so Forget must leave it to the delete that is running.
func retiringRecord() SeedRecord {
	rec := SeedRecord{}
	rec.State, rec.Applied = machine.SeedRetiring, cluster.Position{Epoch: 1, Seq: 6}
	return rec
}

// TestForgetRemovesOnlyARetiredRecord: Forget removes a retired record from disk
// and memory and nothing else. A seeding, a ready or a retiring record is
// ForgetHeld with the state it has and stays as it was, and a pair with no
// record is ForgetNoRecord. [BREAK: let retiring through]
func TestForgetRemovesOnlyARetiredRecord(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	cut := cluster.Position{Epoch: uint64(st.Epoch), Seq: 40}

	held := []struct {
		name string
		rec  SeedRecord
	}{
		{"seeding", seedingRecord(machine.PhaseSnapshot, cut, "key-0042")},
		{"ready", readyRecord(cluster.Position{Epoch: uint64(st.Epoch), Seq: 41})},
		{"retiring", retiringRecord()},
	}
	for _, tc := range held {
		t.Run(tc.name, func(t *testing.T) {
			kv := openKV(t, t.TempDir())
			t.Cleanup(func() {
				if err := kv.Close(); err != nil {
					t.Errorf("closing the store: %v", err)
				}
			})
			putRecord(t, kv, slots.copyOnly, tc.rec)
			g := newRig(t, kv, st, testSelf, true)
			if err := g.rec.LoadRecords(); err != nil {
				t.Fatalf("LoadRecords: %v", err)
			}

			got, err := g.rec.Forget(slots.copyOnly, reconDB, false)
			if err != nil {
				t.Fatalf("Forget: %v", err)
			}
			if got.Outcome != ForgetHeld || got.State != machine.SlotState(tc.rec.State) {
				t.Fatalf("Forget = %+v, want ForgetHeld with state %d", got, tc.rec.State)
			}
			disk, ok := diskRecord(t, kv, slots.copyOnly)
			if !ok || disk.State != tc.rec.State {
				t.Fatalf("the %s record after Forget is %+v (present %v), want it untouched", tc.name, disk, ok)
			}
		})
	}

	t.Run("retired", func(t *testing.T) {
		kv := openKV(t, t.TempDir())
		t.Cleanup(func() {
			if err := kv.Close(); err != nil {
				t.Errorf("closing the store: %v", err)
			}
		})
		putRecord(t, kv, slots.copyOnly, retiredRecord())
		g := newRig(t, kv, st, testSelf, true)
		if err := g.rec.LoadRecords(); err != nil {
			t.Fatalf("LoadRecords: %v", err)
		}

		got, err := g.rec.Forget(slots.copyOnly, reconDB, true)
		if err != nil {
			t.Fatalf("Forget: %v", err)
		}
		if got.Outcome != ForgetRemoved || got.State != machine.SlotRetired {
			t.Fatalf("Forget = %+v, want ForgetRemoved of a retired record", got)
		}
		if _, ok := diskRecord(t, kv, slots.copyOnly); ok {
			t.Fatal("the retired record is still on disk after Forget")
		}
		if _, ok := g.rec.Record(slots.copyOnly, reconDB); ok {
			t.Fatal("the retired record is still in memory after Forget")
		}
	})

	// The only-copy check is made on the record the removal acts on, under the pair lock
	// (BUG-0795): a retired record that no member has confirmed a copy of stays without anyway.
	t.Run("retired with no confirmed copy", func(t *testing.T) {
		kv := openKV(t, t.TempDir())
		t.Cleanup(func() {
			if err := kv.Close(); err != nil {
				t.Errorf("closing the store: %v", err)
			}
		})
		putRecord(t, kv, slots.copyOnly, retiredRecord())
		g := newRig(t, kv, st, testSelf, true)
		if err := g.rec.LoadRecords(); err != nil {
			t.Fatalf("LoadRecords: %v", err)
		}
		got, err := g.rec.Forget(slots.copyOnly, reconDB, false)
		if err != nil {
			t.Fatalf("Forget: %v", err)
		}
		if got.Outcome != ForgetOnlyCopy {
			t.Fatalf("Forget = %+v, want ForgetOnlyCopy", got)
		}
		if _, ok := diskRecord(t, kv, slots.copyOnly); !ok {
			t.Fatal("a refused forget removed the record from disk")
		}
	})

	t.Run("no record", func(t *testing.T) {
		kv := openKV(t, t.TempDir())
		t.Cleanup(func() {
			if err := kv.Close(); err != nil {
				t.Errorf("closing the store: %v", err)
			}
		})
		g := newRig(t, kv, st, testSelf, true)
		if err := g.rec.LoadRecords(); err != nil {
			t.Fatalf("LoadRecords: %v", err)
		}

		got, err := g.rec.Forget(slots.outside, reconDB, false)
		if err != nil {
			t.Fatalf("Forget: %v", err)
		}
		if got.Outcome != ForgetNoRecord {
			t.Fatalf("Forget of a pair with no record = %+v, want ForgetNoRecord", got)
		}
	})
}

// TestForgetKeepsTheRange: Forget removes the record and leaves the pair's range
// and its 's' fence alone; the range is the data the delete of a retired copy
// has not reached yet, and the fence is what a later pass reads. [BREAK: delete
// the pair's range in Forget]
func TestForgetKeepsTheRange(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Errorf("closing the store: %v", err)
		}
	})
	const keys = 40
	putRecord(t, kv, slots.copyOnly, retiredRecord())
	putFence(t, kv, slots.copyOnly, cluster.Position{Epoch: uint64(st.Epoch), Seq: 4})
	s1FillRange(t, kv, slots.copyOnly, reconDB, keys)
	g := newRig(t, kv, st, testSelf, true)
	if err := g.rec.LoadRecords(); err != nil {
		t.Fatalf("LoadRecords: %v", err)
	}

	got, err := g.rec.Forget(slots.copyOnly, reconDB, true)
	if err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if got.Outcome != ForgetRemoved {
		t.Fatalf("Forget = %+v, want ForgetRemoved", got)
	}
	if left := s1CountRange(t, kv, slots.copyOnly, reconDB); left != keys {
		t.Fatalf("%d of %d keys of the range survive Forget", left, keys)
	}
	if _, ok := hasFence(t, kv, slots.copyOnly); !ok {
		t.Fatal("the 's' fence of the pair is gone after Forget")
	}
}

// forgetStore records, for each commit that deletes the pair's record key, the
// sync flag it was committed with.
type forgetStore struct {
	Store
	key     []byte
	removes []bool
}

func (s *forgetStore) NewBatch() storage.KVBatch {
	return &forgetBatch{KVBatch: s.Store.NewBatch(), store: s}
}

type forgetBatch struct {
	storage.KVBatch
	store   *forgetStore
	deletes bool
}

func (b *forgetBatch) Delete(key []byte) error {
	if bytes.Equal(key, b.store.key) {
		b.deletes = true
	}
	return b.KVBatch.Delete(key)
}

func (b *forgetBatch) Commit(sync bool) error {
	if b.deletes {
		b.store.removes = append(b.store.removes, sync)
	}
	return b.KVBatch.Commit(sync)
}

// TestForgetIsDurableBeforeItReturns: the commit that removes the record is a
// synced commit, and the record is gone when the store is reopened, so a crash
// after Forget returns cannot bring back a record the caller was told is gone.
// [BREAK: return before the commit]
func TestForgetIsDurableBeforeItReturns(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	dir := t.TempDir()
	kv := openKV(t, dir)
	putRecord(t, kv, slots.copyOnly, retiredRecord())
	fs := &forgetStore{Store: kv, key: SeedRecordKey(slots.copyOnly, reconDB)}
	g := newRigOn(t, kv, fs, st, testSelf, true)
	if err := g.rec.LoadRecords(); err != nil {
		t.Fatalf("LoadRecords: %v", err)
	}

	got, err := g.rec.Forget(slots.copyOnly, reconDB, true)
	if err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if got.Outcome != ForgetRemoved {
		t.Fatalf("Forget = %+v, want ForgetRemoved", got)
	}
	if len(fs.removes) != 1 || !fs.removes[0] {
		t.Fatalf("commits that delete the record, by sync flag, before Forget returned: %v; want one synced commit", fs.removes)
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openKV(t, dir)
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("closing the reopened store: %v", err)
		}
	})
	if _, ok := diskRecord(t, reopened, slots.copyOnly); ok {
		t.Fatal("the record came back after the store was reopened")
	}
}

// forgottenPair is a pair n2 (an active member) held retired while it was out of the copy
// set (RF 1), with forgottenKeys keys of the retired copy still in its range, after `seeding
// forget ... anyway` removed the record and kept the range. back is the state that names n2
// a copy of the pair again (RF 1 to 2) with no member's SEEDED bit set.
type forgottenPair struct {
	g     *rig
	kv    *storage.BadgerKV
	slot  uint16
	epoch uint64
	back  cluster.ClusterState
}

const forgottenKeys = 40

func newForgottenPair(t *testing.T) *forgottenPair {
	t.Helper()
	st := threeNodes(t)
	f := &forgottenPair{slot: findSlots(t, st).copyOnly, epoch: uint64(st.Epoch)}
	out := st
	alterRF(t, &out, 1)
	f.back = out
	alterRF(t, &f.back, 2)
	f.kv = openKV(t, t.TempDir())
	t.Cleanup(func() {
		if err := f.kv.Close(); err != nil {
			t.Errorf("closing the store: %v", err)
		}
	})
	putRecord(t, f.kv, f.slot, retiredRecord())
	putFence(t, f.kv, f.slot, cluster.Position{Epoch: f.epoch, Seq: 4})
	s1FillRange(t, f.kv, f.slot, reconDB, forgottenKeys)
	f.g = newRig(t, f.kv, out, testSelf, true)
	if err := f.g.rec.LoadRecords(); err != nil {
		t.Fatalf("LoadRecords: %v", err)
	}
	if err := f.g.rec.OnStateSeq(t.Context(), out); err != nil {
		t.Fatalf("the pass over the state that leaves the node out: %v", err)
	}
	got, err := f.g.rec.Forget(f.slot, reconDB, true)
	if err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if got.Outcome != ForgetRemoved {
		t.Fatalf("Forget = %+v, want ForgetRemoved", got)
	}
	if left := s1CountRange(t, f.kv, f.slot, reconDB); left != forgottenKeys {
		t.Fatalf("%d of %d keys of the range survive Forget", left, forgottenKeys)
	}
	f.g.sink.actions = nil
	return f
}

// TestAForgottenPairNamedAgainIsSeededFromAResetNotWrittenReadyOverItsKeptRange (BUG-0819):
// forget removes the record and keeps the range, and the copy set then names the node again
// with no other member's bit set. The pass must not take the pair for a new, empty one: it
// writes no ready record over the kept keys and it queues a seed, whose range reset deletes
// what forget kept once a source has accepted the pull
// (TestTheKeptRangeOfAForgottenPairIsDeletedOnlyOnceASourceAcceptsThePull).
// [guard: the absent-in-target row does not look at the range]
func TestAForgottenPairNamedAgainIsSeededFromAResetNotWrittenReadyOverItsKeptRange(t *testing.T) {
	f := newForgottenPair(t)
	f.g.state = f.back
	if err := f.g.rec.OnStateSeq(t.Context(), f.back); err != nil {
		t.Fatalf("the pass over the state that names the node again: %v", err)
	}

	rec, ok := diskRecord(t, f.kv, f.slot)
	if ok && rec.State == machine.SeedReady {
		t.Fatalf("the pass wrote the forgotten pair ready at %s over the %d keys forget kept, with no seed and no delete",
			rec.Applied, s1CountRange(t, f.kv, f.slot, reconDB))
	}
	if !ok || rec.State != machine.SeedSeeding || rec.Phase != machine.PhaseReset {
		t.Fatalf("after the pass the record on disk is %+v (present %v), want a seeding attempt that owes a range reset", rec, ok)
	}
	if f.g.rec.Serves(f.slot, reconDB) {
		t.Error("the pair serves reads from the range forget kept")
	}
	if acts := f.g.sink.of(f.slot); len(acts) != 1 || acts[0].Do != machine.SlotQueueSeed {
		t.Fatalf("the pass answered %+v, want one QueueSeed", acts)
	}
}

// TestNoWriteIsAppliedOverAForgottenPairsKeptRangeBeforeThePass (BUG-0819): the copy set
// names the node again and a write, or a pushed record, reaches the pair before any pass
// has run. A pair with no record is "never written here, so empty" on an active member,
// which is false for a range forget kept: the step that would create the pair's record is
// refused, at the owner's first section and at the apply, and nothing is written.
// [guard: a record-less pair is given a record without a look at its range]
func TestNoWriteIsAppliedOverAForgottenPairsKeptRangeBeforeThePass(t *testing.T) {
	f := newForgottenPair(t)
	f.g.state = f.back

	err := f.g.rec.ApplyAdvance(f.slot, reconDB, advanceTo(f.kv, f.slot, 6, f.epoch)) // would write d1 = "v"
	if !errors.Is(err, ErrPairNotReady) {
		t.Errorf("a live step over the range forget kept returned %v, want ErrPairNotReady", err)
	}
	var prepareErr error
	f.g.pairs.With(f.slot, reconDB, func(*cluster.Pair) {
		_, prepareErr = f.g.rec.PrepareAdvanceLocked(f.slot, reconDB)
	})
	if !errors.Is(prepareErr, ErrPairNotReady) {
		t.Errorf("an owner write's first section over the range forget kept returned %v, want ErrPairNotReady", prepareErr)
	}
	if rec, ok := diskRecord(t, f.kv, f.slot); ok {
		t.Errorf("a write before the pass left the record %+v (applied %s) over the %d keys forget kept", rec, rec.Applied, forgottenKeys)
	}
	if held := rangeKeys(t, f.kv, f.slot); held != forgottenKeys {
		t.Errorf("the range holds %d keys after the refused write, want the %d forget kept and no more", held, forgottenKeys)
	}
}

// TestAnAdmittedSecondSectionIsNotJudgedByTheRangeAgain (BUG-0819): the look at the range
// belongs to the step that decides to create the record, which for a pipelined owner write is
// its first section. The second section's record is already in the log, so it applies; a
// refusal there would leave a logged write that no replay creates a record for.
// [guard: the range is looked at for an admitted step too]
func TestAnAdmittedSecondSectionIsNotJudgedByTheRangeAgain(t *testing.T) {
	f := newForgottenPair(t)
	f.g.state = f.back
	second := advanceTo(f.kv, f.slot, 6, f.epoch)
	second.Admitted = true
	if err := f.g.rec.ApplyAdvance(f.slot, reconDB, second); err != nil {
		t.Fatalf("an admitted second section returned %v, want it applied", err)
	}
	if rec, ok := diskRecord(t, f.kv, f.slot); !ok || rec.State != machine.SeedReady {
		t.Fatalf("after the admitted section the record on disk is %+v (present %v), want ready", rec, ok)
	}
}

// TestARangeEmptiedAfterThePassBeganIsWrittenReadyEmpty (BUG-0819): the pass looks at the
// range on its own snapshot first, which is as old as the pass. A range that held keys then
// and holds none now (a left seed's delete finished and removed its record in between) is
// an empty pair: it is written ready empty, and no seed is queued for it, which on a pair
// with no other copy would wait for a source that never comes.
// [guard: the pass trusts its own snapshot for a range that holds keys]
func TestARangeEmptiedAfterThePassBeganIsWrittenReadyEmpty(t *testing.T) {
	st := threeNodes(t)
	slot := findSlots(t, st).copyOnly
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Errorf("closing the store: %v", err)
		}
	})
	s1FillRange(t, kv, slot, reconDB, 3)
	hooked := &snapshotHookKV{BadgerKV: kv}
	g := newRigOn(t, kv, hooked, st, testSelf, true)
	if err := g.rec.LoadRecords(); err != nil {
		t.Fatalf("LoadRecords: %v", err)
	}
	hooked.afterSnapshot = func() { // the pass has its snapshot; the range is emptied behind it
		emptied := kv.NewBatch()
		defer emptied.Discard()
		for _, entry := range t3Dump(t, kv, slot, reconDB) {
			if err := emptied.Delete(entry.Key); err != nil {
				t.Fatal(err)
			}
		}
		if err := emptied.Commit(true); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.rec.OnStateSeq(t.Context(), st); err != nil {
		t.Fatalf("OnStateSeq: %v", err)
	}
	if hooked.afterSnapshot != nil {
		t.Fatal("the pass took no snapshot, so the range was never emptied behind it")
	}
	if held := rangeKeys(t, kv, slot); held != 0 {
		t.Fatalf("the range still holds %d keys: the test did not empty it", held)
	}
	if rec, ok := diskRecord(t, kv, slot); !ok || rec.State != machine.SeedReady {
		t.Fatalf("the record of a pair whose range is empty is %+v (present %v), want ready empty", rec, ok)
	}
	for _, act := range g.sink.of(slot) {
		if act.Do == machine.SlotQueueSeed {
			t.Fatal("the pass queued a seed for a pair whose range is empty and that no member holds")
		}
	}
}

// TestTheRangeLookStopsAtThePairsOwnDatabase (BUG-0819): the look covers the pair's range and
// no more. Keys of another database in the same slot say nothing about this pair: it is still
// a new, empty one, written ready empty by the pass and given its record by a first write.
// [guard: the look runs to the end of the slot]
func TestTheRangeLookStopsAtThePairsOwnDatabase(t *testing.T) {
	st := threeNodes(t)
	slots := findSlots(t, st)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Errorf("closing the store: %v", err)
		}
	})
	for _, slot := range []uint16{slots.copyOnly, slots.owner} {
		s1FillRange(t, kv, slot, reconDB+1, 3)
	}
	g := newRig(t, kv, st, testSelf, true)
	if err := g.rec.LoadRecords(); err != nil {
		t.Fatalf("LoadRecords: %v", err)
	}
	if err := g.rec.ApplyAdvance(slots.owner, reconDB, advanceTo(kv, slots.owner, 6, uint64(st.Epoch))); err != nil {
		t.Fatalf("a first write to a pair whose slot holds another database's keys returned %v", err)
	}
	if err := g.rec.OnStateSeq(t.Context(), st); err != nil {
		t.Fatalf("OnStateSeq: %v", err)
	}
	if rec, ok := diskRecord(t, kv, slots.copyOnly); !ok || rec.State != machine.SeedReady {
		t.Fatalf("the record of a pair whose slot holds another database's keys is %+v (present %v), want ready empty", rec, ok)
	}
}

// TestForgetRunsAPassAlthoughTheStateDidNotMove (BUG-0819): a retired pair is forgotten while
// the node is in its copy set. The pair then has no record over a kept range and applies no
// write, and only a pass queues the seed that resets it; the state_seq does not move, so the
// forget itself must ask the driver for that pass, once.
// [guard: forget does not ask for a pass; the request is never taken back]
func TestForgetRunsAPassAlthoughTheStateDidNotMove(t *testing.T) {
	raft := openDivergedRaft(t, t.TempDir())
	if _, err := raft.Propose(cluster.Command{Version: cluster.CommandVersion, Kind: cluster.CommandCreateDatabase,
		BaseSeq: uint64(raft.State().StateSeq), Body: plainCreate("app", int64(reconDB))}); err != nil {
		t.Fatal(err)
	}
	n := bareNode(raft)
	g, parts := seedingRig(t, n, true)
	n.cl.seeding.Store(parts)
	const slot = 7
	putRecord(t, g.kv, slot, retiredRecord())
	s1FillRange(t, g.kv, slot, reconDB, forgottenKeys)
	if err := g.rec.LoadRecords(); err != nil {
		t.Fatal(err)
	}
	// driveUntil ticks the driver until done, as its ticker does.
	driveUntil := func(what string, done func() bool) {
		t.Helper()
		for deadline := time.Now().Add(10 * time.Second); !done(); time.Sleep(10 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			if err := n.driveSeeding(t.Context()); err != nil {
				t.Fatalf("driveSeeding: %v", err)
			}
		}
	}
	driveUntil("the first pass", func() bool { return parts.passes.Load() > 0 })
	driveTicks(t, n, 3)
	settled, seq := parts.passes.Load(), raft.StateSeq()
	if rec, ok := diskRecord(t, g.kv, slot); !ok || rec.State != machine.SeedRetired {
		t.Fatalf("before the forget the record is %+v (present %v), want retired", rec, ok)
	}

	outcome, err := parts.forget(slot, reconDB, true)
	if err != nil || outcome != ForgetRemoved {
		t.Fatalf("forget = %v, %v, want ForgetRemoved", outcome, err)
	}
	driveUntil("the pass the forget asked for", func() bool { return parts.passes.Load() > settled })
	if now := raft.StateSeq(); now != seq {
		t.Fatalf("the state_seq moved from %d to %d, so the pass proves nothing about the forget", seq, now)
	}
	rec, ok := diskRecord(t, g.kv, slot)
	if !ok || rec.State != machine.SeedSeeding || rec.Phase != machine.PhaseReset {
		t.Fatalf("after the forget's pass the record is %+v (present %v), want a seeding attempt that owes a range reset", rec, ok)
	}
	driveTicks(t, n, 3)
	if got := parts.passes.Load(); got != settled+1 {
		t.Fatalf("the forget made the driver run %d passes, want one", got-settled)
	}

	// A forget that removes nothing asks for nothing.
	if outcome, err := parts.forget(slot, reconDB, true); err != nil || outcome != ForgetHeld {
		t.Fatalf("forget of a seeding pair = %v, %v, want ForgetHeld", outcome, err)
	}
	driveTicks(t, n, 3)
	if got := parts.passes.Load(); got != settled+1 {
		t.Fatalf("a forget that removed nothing made the driver run %d more passes", got-settled-1)
	}
}

// forgottenPuller is a puller over a forgotten pair's rig whose seed channel is call; sources
// are the nodes it may ask.
func forgottenPuller(t *testing.T, f *forgottenPair, sources []string, call func(context.Context, string, transport.Message) (*transport.Message, error)) *Puller {
	t.Helper()
	return NewPuller(PullerConfig{
		Self: testSelf, Recon: f.g.rec, KV: f.kv, Options: SeedOptions{RetryBackoff: 5 * time.Millisecond}, Call: call,
		Committed: func() uint64 { return f.epoch }, StateSeq: func() uint64 { return uint64(f.back.StateSeq) },
		Sources: func(uint16, uint64) []string { return sources },
		OnError: func(err error) { t.Errorf("the puller reported %v", err) },
	})
}

// keysHeld counts the pair's keys without failing the test, for a call made on the puller's worker.
func keysHeld(kv storage.KV, slot uint16) int64 {
	prefix := storage.PairPrefix(slot, reconDB)
	snap := kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: keyRangeEnd(prefix)})
	defer it.Close()
	held := int64(0)
	for it.Seek(prefix); it.Valid(); it.Next() {
		held++
	}
	return held
}

// TestTheKeptRangeOfAForgottenPairIsDeletedOnlyOnceASourceAcceptsThePull (BUG-0819, the
// owner's ruling on the review's F1): the seed queued for a forgotten pair resets the range,
// and that reset deletes what may be the last copy of the pair's data. It must not run while
// there is nothing to pull from: with no other copy in the copy set, or with a source that
// does not accept the registration, the kept keys stay and the pair stays seeding with the
// reason in its status row. Once a source accepts, the range is reset before the attempt is
// registered. [guard: the reset runs before a source is asked; no reset after the accept]
func TestTheKeptRangeOfAForgottenPairIsDeletedOnlyOnceASourceAcceptsThePull(t *testing.T) {
	seeding := func(t *testing.T) *forgottenPair {
		t.Helper()
		f := newForgottenPair(t)
		f.g.state = f.back
		if err := f.g.rec.OnStateSeq(t.Context(), f.back); err != nil {
			t.Fatalf("the pass over the state that names the node again: %v", err)
		}
		return f
	}
	// waits runs the puller until the pair's status row gives a reason that holds want, and
	// requires the kept range and the unregistered attempt to be as the pass left them.
	waits := func(t *testing.T, f *forgottenPair, puller *Puller, want string) {
		t.Helper()
		puller.Queue(f.slot, reconDB)
		var detail string
		runPuller(t, puller, "the pull to say why it waits", func() bool {
			status, _ := puller.Status(f.slot, reconDB)
			detail = status.Detail
			return strings.Contains(detail, want)
		})
		if held := rangeKeys(t, f.kv, f.slot); held != forgottenKeys {
			t.Errorf("the range holds %d of the %d keys forget kept, and no source has accepted the pull (status: %s)", held, forgottenKeys, detail)
		}
		if rec, ok := diskRecord(t, f.kv, f.slot); !ok || rec.State != machine.SeedSeeding || rec.Phase != machine.PhaseReset {
			t.Errorf("the record on disk is %+v (present %v), want the seeding attempt that still owes its range reset", rec, ok)
		}
		if !strings.Contains(detail, "kept") {
			t.Errorf("the status row does not say the slot's data is kept meanwhile: %q", detail)
		}
	}

	t.Run("no other copy to pull from", func(t *testing.T) {
		f := seeding(t)
		waits(t, f, forgottenPuller(t, f, nil, nil), "no node to pull from")
	})

	// The sentence about kept data is for a range that holds some: an ordinary seed that
	// waits for a source over an empty range does not carry it.
	t.Run("no other copy and nothing kept", func(t *testing.T) {
		f := seeding(t)
		emptied := f.kv.NewBatch()
		defer emptied.Discard()
		for _, entry := range t3Dump(t, f.kv, f.slot, reconDB) {
			if err := emptied.Delete(entry.Key); err != nil {
				t.Fatal(err)
			}
		}
		if err := emptied.Commit(true); err != nil {
			t.Fatal(err)
		}
		puller := forgottenPuller(t, f, nil, nil)
		puller.Queue(f.slot, reconDB)
		var detail string
		runPuller(t, puller, "the pull to say why it waits", func() bool {
			status, _ := puller.Status(f.slot, reconDB)
			detail = status.Detail
			return strings.Contains(detail, "no node to pull from")
		})
		if strings.Contains(detail, "kept") {
			t.Errorf("the status row of a pair whose range is empty speaks of kept data: %q", detail)
		}
	})

	t.Run("a source that does not accept", func(t *testing.T) {
		f := seeding(t)
		busy := func(_ context.Context, _ string, msg transport.Message) (*transport.Message, error) {
			return messageOf(transport.EncodeSeedBusy(transport.SeedBusy{Header: msg.Env, RetryAfterMs: 5}))
		}
		waits(t, f, forgottenPuller(t, f, []string{"n1"}, busy), "SEED_BUSY")
	})

	// accepting is a source that accepts every registration and serves no chunk; atRegister
	// is how many keys the range held when it was first asked.
	accepting := func(f *forgottenPair, atRegister *atomic.Int64) func(context.Context, string, transport.Message) (*transport.Message, error) {
		atRegister.Store(-1)
		return func(_ context.Context, _ string, msg transport.Message) (*transport.Message, error) {
			if msg.Env.Kind != transport.KindSeedRegister {
				return nil, errors.New("the test's source serves no chunk")
			}
			register, err := transport.DecodeSeedRegister(payloadOf(msg))
			if err != nil {
				return nil, err
			}
			atRegister.CompareAndSwap(-1, keysHeld(f.kv, f.slot))
			return messageOf(transport.EncodeSeedAccept(transport.SeedAccept{Header: msg.Env, SeedID: register.SeedID, WindowBytes: 1 << 20}))
		}
	}

	t.Run("a source accepts", func(t *testing.T) {
		f := seeding(t)
		var atRegister atomic.Int64
		puller := forgottenPuller(t, f, []string{"n1"}, accepting(f, &atRegister))
		puller.Queue(f.slot, reconDB)
		runPuller(t, puller, "the attempt to register", func() bool {
			rec, ok := f.g.rec.Record(f.slot, reconDB)
			return ok && rec.Phase == machine.PhaseSnapshot
		})
		if got := atRegister.Load(); got != forgottenKeys {
			t.Errorf("when the source was asked the range held %d keys, want the %d forget kept: the reset ran before any source accepted", got, forgottenKeys)
		}
		if held := rangeKeys(t, f.kv, f.slot); held != 0 {
			t.Errorf("the attempt registered over %d of the keys forget kept: the range was not reset after the accept", held)
		}
	})

	// The accept is not the registration: the attempt is registered only after its reset,
	// so a reset that fails leaves an attempt that still owes it, and the next try redoes it.
	t.Run("the reset fails after the accept", func(t *testing.T) {
		f := seeding(t)
		fail := true
		var atRegister atomic.Int64
		var reported reportedErrors
		puller := NewPuller(PullerConfig{
			Self: testSelf, Recon: f.g.rec, KV: failingStore{f.kv, &fail, t3Key(f.slot, reconDB, 0)},
			Options: SeedOptions{RetryBackoff: 5 * time.Millisecond}, Call: accepting(f, &atRegister),
			Committed: func() uint64 { return f.epoch }, StateSeq: func() uint64 { return uint64(f.back.StateSeq) },
			Sources: func(uint16, uint64) []string { return []string{"n1"} }, OnError: reported.take,
		})
		puller.Queue(f.slot, reconDB)
		runPuller(t, puller, "the failed reset to be reported", func() bool { return len(reported.all()) > 0 })
		if rec, ok := diskRecord(t, f.kv, f.slot); !ok || rec.State != machine.SeedSeeding || rec.Phase != machine.PhaseReset {
			t.Fatalf("after a reset that failed the record is %+v (present %v), want the attempt still owing its reset: it registered over a range that was not reset", rec, ok)
		}
		if held := rangeKeys(t, f.kv, f.slot); held != forgottenKeys {
			t.Fatalf("the range holds %d keys after a reset whose commit failed, want all %d", held, forgottenKeys)
		}
		if status, _ := puller.Status(f.slot, reconDB); strings.Contains(status.Detail, "is kept") {
			t.Errorf("after a source accepted and the reset failed, the status row says the data is kept until a copy accepts: %q", status.Detail)
		}
		fail = false
		runPuller(t, puller, "the attempt to register once the store takes the reset", func() bool {
			rec, ok := f.g.rec.Record(f.slot, reconDB)
			return ok && rec.Phase == machine.PhaseSnapshot
		})
		if held := rangeKeys(t, f.kv, f.slot); held != 0 {
			t.Errorf("the attempt registered over %d keys", held)
		}
	})
}

// forgottenInItsCopySet is a one-member cluster node whose pair (slot 7) was retired and then
// forgotten while the node is in its copy set, after the forget and before the pass it asked
// for. settled is the driver's pass count before the forget.
func forgottenInItsCopySet(t *testing.T) (n *Node, g *rig, parts *seedParts, slot uint16, settled int64) {
	t.Helper()
	raft := openDivergedRaft(t, t.TempDir())
	if _, err := raft.Propose(cluster.Command{Version: cluster.CommandVersion, Kind: cluster.CommandCreateDatabase,
		BaseSeq: uint64(raft.State().StateSeq), Body: plainCreate("app", int64(reconDB))}); err != nil {
		t.Fatal(err)
	}
	n = bareNode(raft)
	g, parts = seedingRig(t, n, true)
	n.cl.seeding.Store(parts)
	slot = 7
	putRecord(t, g.kv, slot, retiredRecord())
	s1FillRange(t, g.kv, slot, reconDB, forgottenKeys)
	if err := g.rec.LoadRecords(); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); parts.passes.Load() == 0; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the first pass")
		}
		if err := n.driveSeeding(t.Context()); err != nil {
			t.Fatalf("driveSeeding: %v", err)
		}
	}
	driveTicks(t, n, 3)
	settled = parts.passes.Load()
	if outcome, err := parts.forget(slot, reconDB, true); err != nil || outcome != ForgetRemoved {
		t.Fatalf("forget = %v, %v, want ForgetRemoved", outcome, err)
	}
	g.sink.actions = nil
	return n, g, parts, slot, settled
}

// TestThePassAForgetAskedForIsRunAgainWhenItFails (BUG-0819, the review's F2): the pass a
// forget asks for is the only thing that queues the forgotten pair's seed while the state
// stands still. A pass that fails, before its commit or in its dispatch, must leave the
// request standing, as a failed state_seq pass leaves the state_seq unreconciled; otherwise
// the pair applies no write until the cluster state happens to move.
// [guard: the request is dropped when the pass fails]
func TestThePassAForgetAskedForIsRunAgainWhenItFails(t *testing.T) {
	queued := func(t *testing.T, g *rig, slot uint16) {
		t.Helper()
		if rec, ok := diskRecord(t, g.kv, slot); !ok || rec.State != machine.SeedSeeding || rec.Phase != machine.PhaseReset {
			t.Errorf("the record on disk is %+v (present %v), want the seeding attempt the pass writes", rec, ok)
		}
		acts := g.sink.of(slot)
		if len(acts) == 0 || acts[len(acts)-1].Do != machine.SlotQueueSeed {
			t.Errorf("the sink was handed %+v for the pair, want its seed queued", acts)
		}
	}

	t.Run("the pass fails before its commit", func(t *testing.T) {
		n, g, parts, slot, settled := forgottenInItsCopySet(t)
		stopped, stop := context.WithCancel(context.Background())
		stop()
		if err := n.driveSeeding(stopped); err == nil {
			t.Fatal("the pass over a cancelled context did not fail, so the test proves nothing")
		}
		driveTicks(t, n, 3)
		if got := parts.passes.Load() - settled; got != 1 {
			t.Fatalf("%d passes ran after the one that failed, want one: the forget's request was dropped with the failed pass", got)
		}
		queued(t, g, slot)
	})

	t.Run("the dispatch fails after the commit", func(t *testing.T) {
		n, g, parts, slot, settled := forgottenInItsCopySet(t)
		g.sink.fail = errors.New("the sink failed")
		err := n.driveSeeding(t.Context())
		g.sink.fail = nil
		if err == nil {
			t.Fatal("the pass whose dispatch failed returned no error, so the test proves nothing")
		}
		g.sink.actions = nil
		driveTicks(t, n, 3)
		if got := parts.passes.Load() - settled; got != 1 {
			t.Fatalf("%d passes ran after the one whose dispatch failed, want one: the queue request was never handed over again", got)
		}
		queued(t, g, slot)
	})
}

// deleteHookKV runs onDelete once, right after the first batch that carries a delete has
// committed: the point at which a range reset has begun and is not finished.
type deleteHookKV struct {
	storage.KV
	onDelete func()
}

func (k *deleteHookKV) NewBatch() storage.KVBatch {
	return &deleteHookBatch{KVBatch: k.KV.NewBatch(), kv: k}
}

type deleteHookBatch struct {
	storage.KVBatch
	kv      *deleteHookKV
	deletes bool
}

func (b *deleteHookBatch) Delete(key []byte) error {
	b.deletes = true
	return b.KVBatch.Delete(key)
}

func (b *deleteHookBatch) Commit(sync bool) error {
	err := b.KVBatch.Commit(sync)
	if hook := b.kv.onDelete; b.deletes && hook != nil {
		b.kv.onDelete = nil
		hook()
	}
	return err
}

// TestTheStatusRowStopsSayingTheDataIsKeptOnceTheResetRuns (BUG-0819, the second review's
// F4): a pair that waited for a source carries "the data ... is kept" in its status row. Once
// a source has accepted and the reset is deleting that data, the row must say so instead of
// repeating the sentence of the wait that is over. [guard: register notes nothing before the reset]
func TestTheStatusRowStopsSayingTheDataIsKeptOnceTheResetRuns(t *testing.T) {
	f := newForgottenPair(t)
	f.g.state = f.back
	if err := f.g.rec.OnStateSeq(t.Context(), f.back); err != nil {
		t.Fatalf("the pass over the state that names the node again: %v", err)
	}
	var appeared atomic.Bool
	var during atomic.Value
	hooked := &deleteHookKV{KV: f.kv}
	puller := NewPuller(PullerConfig{
		Self: testSelf, Recon: f.g.rec, KV: hooked, Options: SeedOptions{RetryBackoff: 5 * time.Millisecond},
		Call: func(_ context.Context, _ string, msg transport.Message) (*transport.Message, error) {
			if msg.Env.Kind != transport.KindSeedRegister {
				return nil, errors.New("the test's source serves no chunk")
			}
			register, err := transport.DecodeSeedRegister(payloadOf(msg))
			if err != nil {
				return nil, err
			}
			return messageOf(transport.EncodeSeedAccept(transport.SeedAccept{Header: msg.Env, SeedID: register.SeedID, WindowBytes: 1 << 20}))
		},
		Committed: func() uint64 { return f.epoch }, StateSeq: func() uint64 { return uint64(f.back.StateSeq) },
		Sources: func(uint16, uint64) []string {
			if appeared.Load() {
				return []string{"n1"}
			}
			return nil
		},
		OnError: func(err error) { t.Errorf("the puller reported %v", err) },
	})
	puller.Queue(f.slot, reconDB)
	runPuller(t, puller, "the pull to wait with the data kept", func() bool {
		status, _ := puller.Status(f.slot, reconDB)
		return strings.Contains(status.Detail, "kept")
	})
	hooked.onDelete = func() {
		status, _ := puller.Status(f.slot, reconDB)
		during.Store(status.Detail)
	}
	appeared.Store(true)
	runPuller(t, puller, "the attempt to register", func() bool {
		rec, ok := f.g.rec.Record(f.slot, reconDB)
		return ok && rec.Phase == machine.PhaseSnapshot
	})
	detail, _ := during.Load().(string)
	if strings.Contains(detail, "is kept") {
		t.Errorf("while the reset deletes the slot's data the status row still says it is kept: %q", detail)
	}
	if !strings.Contains(detail, "deleting") || !strings.Contains(detail, "n1") {
		t.Errorf("while the reset runs the status row is %q, want it to say that n1 accepted the pull and the data is being deleted", detail)
	}
}

// TestACrashInsideTheResetLeavesAnAttemptThatStillOwesIt (the seed machine's contract 1, with
// a real source): the reset comes after SEED_ACCEPT and before RegisterAccepted is fed. A node
// that stops inside the reset must find, at its next start, an attempt in phase reset: a
// record that says "registered" there would resume over the old keys the reset had not
// reached, and they would end in the ready copy. [guard: RegisterAccepted is fed before the reset]
func TestACrashInsideTheResetLeavesAnAttemptThatStillOwesIt(t *testing.T) {
	badger := t3OpenKV(t, t.TempDir())
	t.Cleanup(func() { _ = badger.Close() })
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	hooked := &deleteHookKV{KV: badger, onDelete: stop} // the node stops after the reset's first batch
	rig := newT3Rig(t, t3Options{kv: hooked})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 3})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 6, t3Cut(t, rig), 0)
	const old = 3 * resetRangeBatch
	s1FillRange(t, badger, rig.slot, reconDB, old)

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		rig.puller.Run(ctx)
	}()
	rig.puller.Queue(rig.slot, reconDB)
	select {
	case <-stopped:
	case <-time.After(20 * time.Second):
		t.Fatal("the node never stopped inside the reset")
	}
	left := s1CountRange(t, badger, rig.slot, reconDB)
	if left == 0 || left == old {
		t.Fatalf("%d of %d old keys are left at the stop, so it was not inside the reset and the test proves nothing", left, old)
	}
	if rec, ok := diskRecord(t, badger, rig.slot); !ok || rec.State != machine.SeedSeeding || rec.Phase != machine.PhaseReset {
		t.Fatalf("at the stop, with %d old keys left, the record is %+v (present %v), want an attempt that still owes its reset", left, rec, ok)
	}

	again := newT3Rig(t, t3Options{kv: badger})
	again.owner, again.sources = "x", map[string]*t3Source{"x": owner}
	again.pullUntil("the pair to be ready after the restart", again.isReady)
	t3SameEntries(t, t3Dump(t, badger, again.slot, reconDB), owner.dump(t, again.slot, reconDB))
}

// TestAForgottenPairIsSeededOnceASourceAppears (BUG-0819): the whole way out. The pair
// waits with its range kept and serves nothing while no copy can be pulled from; when a
// source appears the pull registers, deletes the kept keys and ends ready with the
// source's entries and none of the old ones.
func TestAForgottenPairIsSeededOnceASourceAppears(t *testing.T) {
	f := newForgottenPair(t)
	f.g.state = f.back
	if err := f.g.rec.OnStateSeq(t.Context(), f.back); err != nil {
		t.Fatalf("the pass over the state that names the node again: %v", err)
	}
	owner := t3NewSource(t, "x", "x", f.epoch, SeedOptions{ChunkEntries: 3})
	owner.fill(t, f.slot, reconDB, 5, cluster.Position{Epoch: f.epoch, Seq: 40}, 0)
	link := &t3Rig{t: t, slot: f.slot, sources: map[string]*t3Source{"x": owner}}
	rep, err := NewReplicator(ReplicatorConfig{
		Self: testSelf, Committed: func() uint64 { return f.epoch }, Pairs: f.g.pairs,
		Advancer: t3Advancer{}, Link: t3Link{rig: link},
	})
	if err != nil {
		t.Fatal(err)
	}
	var appeared atomic.Bool
	puller := NewPuller(PullerConfig{
		Self: testSelf, Recon: f.g.rec, KV: f.kv, Options: SeedOptions{RetryBackoff: 5 * time.Millisecond},
		Call: func(_ context.Context, _ string, msg transport.Message) (*transport.Message, error) {
			return owner.src.Serve(testSelf, msg)
		},
		Committed: func() uint64 { return f.epoch }, StateSeq: func() uint64 { return uint64(f.back.StateSeq) },
		Sources: func(uint16, uint64) []string {
			if appeared.Load() {
				return []string{"x"}
			}
			return nil
		},
		Replicator: rep, OnError: func(err error) { t.Errorf("the puller reported %v", err) },
	})
	puller.Queue(f.slot, reconDB)
	runPuller(t, puller, "the pull to wait with the range kept", func() bool {
		status, _ := puller.Status(f.slot, reconDB)
		return strings.Contains(status.Detail, "kept")
	})
	if held := rangeKeys(t, f.kv, f.slot); held != forgottenKeys {
		t.Fatalf("the range holds %d keys while nothing can be pulled, want the %d forget kept", held, forgottenKeys)
	}
	if f.g.rec.Serves(f.slot, reconDB) {
		t.Fatal("the pair serves reads from the kept range while it waits")
	}
	appeared.Store(true)
	runPuller(t, puller, "the pair to be ready once a source appeared", func() bool {
		rec, ok := f.g.rec.Record(f.slot, reconDB)
		return ok && rec.State == machine.SeedReady
	})
	t3SameEntries(t, t3Dump(t, f.kv, f.slot, reconDB), owner.dump(t, f.slot, reconDB))
}
