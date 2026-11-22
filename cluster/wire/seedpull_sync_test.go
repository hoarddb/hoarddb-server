package wire

import (
	"bytes"
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/internal/storeimage"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// syncCountingKV counts the explicit store syncs (KV.Sync) made through it and keeps, in order,
// the store events a reset is judged by: a committed batch that deleted a key of a pair range, an
// explicit sync, and any other committed batch. A synced Commit is not a KV.Sync: it is counted
// as a commit, as the puller's own record writes are.
type syncCountingKV struct {
	storage.KV
	syncs atomic.Int64

	mu     sync.Mutex
	events []string
}

const (
	eventDeletes = "deletes"
	eventSync    = "sync"
	eventCommit  = "commit"
)

func (k *syncCountingKV) Sync() error {
	if err := k.KV.Sync(); err != nil {
		return err
	}
	k.syncs.Add(1)
	k.record(eventSync)
	return nil
}

func (k *syncCountingKV) NewBatch() storage.KVBatch {
	return &syncCountingBatch{KVBatch: k.KV.NewBatch(), kv: k}
}

func (k *syncCountingKV) record(event string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.events = append(k.events, event)
}

func (k *syncCountingKV) snapshotEvents() []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]string(nil), k.events...)
}

type syncCountingBatch struct {
	storage.KVBatch
	kv      *syncCountingKV
	deletes bool
}

func (b *syncCountingBatch) Delete(key []byte) error {
	if len(key) > 10 && key[0] != 0xFF { // a pair-range key; node keys live under 0xFFFF
		b.deletes = true
	}
	return b.KVBatch.Delete(key)
}

func (b *syncCountingBatch) Commit(sync bool) error {
	if err := b.KVBatch.Commit(sync); err != nil {
		return err
	}
	if b.deletes {
		b.kv.record(eventDeletes)
	} else {
		b.kv.record(eventCommit)
	}
	return nil
}

// seedingSlots returns up to n slots whose record on the rig's node is a seeding attempt that
// still owes its range reset.
func seedingSlots(t *testing.T, rig *t3Rig, n int) []uint16 {
	t.Helper()
	var slots []uint16
	for slot := 0; slot < storage.SlotCount && len(slots) < n; slot++ {
		record, found := rig.rec.Record(uint16(slot), reconDB)
		if found && record.State == machine.SeedSeeding && record.Phase == machine.PhaseReset {
			slots = append(slots, uint16(slot))
		}
	}
	if len(slots) < n {
		t.Fatalf("SCENARIO NOT REACHED: the joiner holds %d seeding pairs that owe their reset, want %d", len(slots), n)
	}
	return slots
}

// pullAll queues every slot and runs the puller until each pair's record is ready.
func pullAll(t *testing.T, rig *t3Rig, slots []uint16) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		rig.puller.Run(ctx)
	}()
	for _, slot := range slots {
		rig.puller.Queue(slot, reconDB)
	}
	allReady := func() bool {
		for _, slot := range slots {
			record, found := rig.rec.Record(slot, reconDB)
			if !found || record.State != machine.SeedReady {
				return false
			}
		}
		return true
	}
	deadline := time.Now().Add(30 * time.Second)
	for !allReady() && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	ok := allReady()
	cancel()
	<-finished
	if !ok {
		t.Fatalf("timed out waiting for %d pairs to be ready", len(slots))
	}
}

// TestSeedingEmptyPairsCostsOneStoreSyncEachNotTwo: seeding N pairs whose ranges are empty costs
// at most N + 1 explicit store syncs on the puller node. Each pull syncs once before its ready
// record (Puller.finishSeed); the reset of an empty range deleted nothing and has nothing to make
// durable, so a sync there is the second one per pair the vouched cut made cost a marker write.
// The total of every sync, synced commits included (VouchedCounts), is logged beside it.
func TestSeedingEmptyPairsCostsOneStoreSyncEachNotTwo(t *testing.T) {
	const pairs = 12
	badger := t3OpenKV(t, t.TempDir())
	t.Cleanup(func() {
		if err := badger.Close(); err != nil {
			t.Errorf("closing the receiver store: %v", err)
		}
	})
	counting := &syncCountingKV{KV: badger}
	rig := newT3Rig(t, t3Options{kv: counting})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	cut := t3Cut(t, rig)
	slots := seedingSlots(t, rig, pairs)
	for _, slot := range slots {
		owner.pairs.With(slot, reconDB, func(pair *cluster.Pair) { pair.AppliedPos = cut })
	}

	syncsBefore := counting.syncs.Load()
	allBefore, _, _ := badger.VouchedCounts()
	pullAll(t, rig, slots)
	syncs := counting.syncs.Load() - syncsBefore
	allAfter, _, _ := badger.VouchedCounts()
	t.Logf("%d empty pairs: %d explicit store syncs, %d syncs in all (synced commits included)", pairs, syncs, allAfter-allBefore)

	for _, slot := range slots {
		if got := len(t3Dump(t, badger, slot, reconDB)); got != 0 {
			t.Errorf("slot %d holds %d keys after seeding an empty pair", slot, got)
		}
	}
	if syncs > pairs+1 {
		t.Fatalf("seeding %d empty pairs made %d explicit store syncs, want at most %d: the reset of a range that held no key synced the store anyway",
			pairs, syncs, pairs+1)
	}
}

// TestAResetThatDeletedKeysSyncsBeforeThePullGoesOn: when the reset deleted keys, the store is
// synced after the last delete and before any other write of the attempt (the accepted
// registration's record), so a cut can never keep the record's "the range is empty" and lose
// the deletes (seeding section 4 step 3, conventions 12 contract 1).
func TestAResetThatDeletedKeysSyncsBeforeThePullGoesOn(t *testing.T) {
	badger := t3OpenKV(t, t.TempDir())
	t.Cleanup(func() {
		if err := badger.Close(); err != nil {
			t.Errorf("closing the receiver store: %v", err)
		}
	})
	counting := &syncCountingKV{KV: badger}
	rig := newT3Rig(t, t3Options{kv: counting})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 3})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 5, t3Cut(t, rig), 0)

	strays := make([][]byte, 0, 3)
	batch := badger.NewBatch()
	for number := 900; number < 903; number++ {
		stray := t3Key(rig.slot, reconDB, number)
		strays = append(strays, stray)
		if err := batch.Set(stray, []byte("old")); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	batch.Discard()

	rig.pullUntil("the pair to be ready", rig.isReady)

	events := counting.snapshotEvents()
	last := -1
	for i, event := range events {
		if event == eventDeletes {
			last = i
		}
	}
	if last < 0 {
		t.Fatalf("SCENARIO NOT REACHED: the reset deleted nothing; events %v", events)
	}
	if last+1 >= len(events) || events[last+1] != eventSync {
		t.Fatalf("after the reset's last delete the next store event is %v, want an explicit sync before any other write of the attempt; events %v",
			events[last+1:min(len(events), last+3)], events)
	}
	for _, stray := range strays {
		if kvHas(t, badger, stray) {
			t.Errorf("the stray key %q survived the reset", stray)
		}
	}
}

// TestACutBetweenTheResetAndTheFinalSyncKeepsNoKeyTheResetRemoved plays a power cut on a
// vouched store at the second chunk request of an attempt, after the reset, the accepted
// registration and the first chunk and before the ready record's sync, once for an empty range
// (its reset does not sync) and once for a range that held keys. The accepted registration
// commits without a sync (seedStepCommitsUnsynced), so seed_sync_bytes is one byte here: the
// first chunk's synced commit is what vouches the record past the reset. The image opened with
// the cut must hold no key the reset removed whenever its record has left the reset phase, and
// a pull run on the image again ends with exactly the source's keys.
func TestACutBetweenTheResetAndTheFinalSyncKeepsNoKeyTheResetRemoved(t *testing.T) {
	for _, strays := range []int{0, 3} {
		t.Run(map[int]string{0: "empty range", 3: "range with keys"}[strays], func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "badger")
			vouched, err := openVouchedTestKV(t, dir, storage.BadgerTuning{MemTableMB: 32})
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			t.Cleanup(func() {
				if !closed {
					if err := vouched.Close(); err != nil {
						t.Errorf("closing the receiver store: %v", err)
					}
				}
			})
			rig := newT3Rig(t, t3Options{kv: vouched, seed: SeedOptions{SyncBytes: 1}})
			owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 2})
			rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
			cut := t3Cut(t, rig)
			owner.fill(t, rig.slot, reconDB, 4, cut, 0)

			var removed [][]byte
			if strays > 0 {
				batch := vouched.NewBatch()
				for number := range strays {
					stray := t3Key(rig.slot, reconDB, 900+number)
					removed = append(removed, stray)
					if err := batch.Set(stray, []byte("old")); err != nil {
						t.Fatal(err)
					}
				}
				if err := batch.Commit(true); err != nil {
					t.Fatal(err)
				}
				batch.Discard()
			}

			var (
				image   storeimage.Capture
				takeErr error
				acks    atomic.Int64
			)
			rig.wrap = func(_ string, msg transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
				if msg.Env.Kind == transport.KindSeedAck && acks.Add(1) == 2 {
					image, takeErr = storeimage.Take(dir)
				}
				return reply, err
			}
			rig.pullUntil("the pair to be ready", rig.isReady)
			if takeErr != nil {
				t.Fatal(takeErr)
			}
			if image == nil {
				t.Fatal("SCENARIO NOT REACHED: no second chunk was asked for, so no image was taken")
			}
			if err := vouched.Close(); err != nil {
				t.Fatal(err)
			}
			closed = true

			cutStore := openImageStoreVouched(t, image)
			rebooted := newT3Rig(t, t3Options{kv: cutStore})
			rebooted.owner, rebooted.sources = "x", map[string]*t3Source{"x": owner}
			if rebooted.slot != rig.slot {
				t.Fatalf("SCENARIO NOT REACHED: the rebooted rig picked slot %d, want %d", rebooted.slot, rig.slot)
			}
			record, found := rebooted.rec.Record(rig.slot, reconDB)
			if !found {
				t.Fatal("the image holds no record for the pair")
			}
			if record.State != machine.SeedSeeding || record.Phase == machine.PhaseReset {
				t.Fatalf("SCENARIO NOT REACHED: the image's record is %v in phase %v, want a seeding attempt past its reset", record.State, record.Phase)
			}
			for _, stray := range removed {
				if kvHas(t, cutStore, stray) {
					t.Fatalf("the image's record is in phase %v, past the reset, and the key %q the reset removed came back", record.Phase, stray)
				}
			}
			rebooted.pullUntil("the pair to be ready on the image", rebooted.isReady)
			got, want := t3Dump(t, cutStore, rig.slot, reconDB), owner.dump(t, rig.slot, reconDB)
			t3SameEntries(t, got, want)
			for _, stray := range removed {
				for _, entry := range got {
					if bytes.Equal(entry.Key, stray) {
						t.Fatalf("the key %q the reset removed is in the seeded range", stray)
					}
				}
			}
		})
	}
}
