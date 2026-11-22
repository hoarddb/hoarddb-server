package wire

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/internal/storeimage"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// openVouchedReceiver opens the receiver's store with the vouched cut, as Node.open does, in a
// fresh directory, and closes it at cleanup unless the test closed it (closed set true).
func openVouchedReceiver(t *testing.T) (kv *storage.BadgerKV, dir string, closed *bool) {
	t.Helper()
	dir = filepath.Join(t.TempDir(), "badger")
	kv, err := openVouchedTestKV(t, dir, storage.BadgerTuning{MemTableMB: 32})
	if err != nil {
		t.Fatal(err)
	}
	closed = new(bool)
	t.Cleanup(func() {
		if !*closed {
			if err := kv.Close(); err != nil {
				t.Errorf("closing the receiver store: %v", err)
			}
		}
	})
	return kv, dir, closed
}

// recordOnDisk reads the pair's seed record from kv as it lies there, without a Reconciler:
// a joining node's boot rewrites its records (it starts every pair again from the reset), so
// only the bytes say what an image holds.
func recordOnDisk(t *testing.T, kv storage.KV, slot uint16) (SeedRecord, bool) {
	t.Helper()
	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	raw, err := snapshot.Get(SeedRecordKey(slot, reconDB))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return SeedRecord{}, false
	}
	if err != nil {
		t.Fatalf("reading the seed record of slot %d: %v", slot, err)
	}
	record, err := DecodeSeedRecord(raw)
	if err != nil {
		t.Fatalf("decoding the seed record of slot %d: %v", slot, err)
	}
	return record, true
}

// TestSeedingAPairCostsOneStoreSyncAndOneMarkerAdvance: seeding N small pairs (half of them
// empty, half three chunks each, all under seed_sync_bytes) costs at most N + 2 store syncs and
// N + 2 sync-marker advances on the puller node, synced commits included: one per pair, the
// commit that writes its ready record. Every earlier step of an attempt (the accepted
// registration, the final chunk, the drained record, and the explicit sync before the ready
// record) used to sync too, about five syncs and four marker writes per pair, which made a
// seeded join two to three times slower under the vouched cut (seeding-speed-note.md). The
// count, not a wall time, is the claim.
func TestSeedingAPairCostsOneStoreSyncAndOneMarkerAdvance(t *testing.T) {
	const pairs = 12
	vouched, _, _ := openVouchedReceiver(t)
	rig := newT3Rig(t, t3Options{kv: vouched})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 2})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	cut := t3Cut(t, rig)
	slots := seedingSlots(t, rig, pairs)
	for i, slot := range slots {
		if i%2 == 0 {
			owner.fill(t, slot, reconDB, 5, cut, 0)
			continue
		}
		owner.pairs.With(slot, reconDB, func(pair *cluster.Pair) { pair.AppliedPos = cut })
	}

	syncsBefore, advancesBefore, _ := vouched.VouchedCounts()
	pullAll(t, rig, slots)
	syncsAfter, advancesAfter, markerTime := vouched.VouchedCounts()
	syncs, advances := syncsAfter-syncsBefore, advancesAfter-advancesBefore
	t.Logf("%d pairs: %d store syncs, %d marker advances (marker writes %v in all, for the whole store)", pairs, syncs, advances, markerTime)

	for i, slot := range slots {
		got, want := t3Dump(t, vouched, slot, reconDB), owner.dump(t, slot, reconDB)
		if i%2 == 0 && len(want) == 0 {
			t.Fatalf("SCENARIO NOT REACHED: the source holds no key in slot %d", slot)
		}
		t3SameEntries(t, got, want)
	}
	if syncs > pairs+2 || advances > pairs+2 {
		t.Fatalf("seeding %d small pairs made %d store syncs and %d sync-marker advances, want at most %d of each: "+
			"a step of an attempt before its ready record syncs the store", pairs, syncs, advances, pairs+2)
	}
}

// TestAPairIsReadyOnlyWhenAPowerCutKeepsItsReadyRecordAndItsData plays a power cut at the
// instant the puller runs OnReady, the first thing that acts on a ready pair (the sweeps rerun,
// the node recounts; a report then tells the cluster). The image, opened with the cut, must hold
// the ready record and exactly the source's keys: the ready record's commit is the attempt's one
// sync, and it must make the record and everything the attempt wrote before it durable and
// vouched before anything acts on it (vouched cut I-4).
func TestAPairIsReadyOnlyWhenAPowerCutKeepsItsReadyRecordAndItsData(t *testing.T) {
	vouched, dir, closed := openVouchedReceiver(t)
	var (
		image   storeimage.Capture
		takeErr error
		once    sync.Once
	)
	rig := newT3Rig(t, t3Options{kv: vouched, OnReady: func(uint16, uint64) {
		once.Do(func() { image, takeErr = storeimage.Take(dir) })
	}})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 2})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 5, t3Cut(t, rig), 0)

	rig.pullUntil("the pair to be ready", rig.isReady)
	if takeErr != nil {
		t.Fatal(takeErr)
	}
	if image == nil {
		t.Fatal("SCENARIO NOT REACHED: OnReady never ran, so no image was taken")
	}
	if err := vouched.Close(); err != nil {
		t.Fatal(err)
	}
	*closed = true

	cutStore := openImageStoreVouched(t, image)
	record, found := recordOnDisk(t, cutStore, rig.slot)
	if !found || record.State != machine.SeedReady {
		t.Fatalf("a power cut at the pair's OnReady reopens with its record %+v (found %v), want ready: the pair was acted on "+
			"as ready before its ready record was vouched", record, found)
	}
	t3SameEntries(t, t3Dump(t, cutStore, rig.slot, reconDB), owner.dump(t, rig.slot, reconDB))
}

// TestAPowerCutAtAnyCallOfAPullReopensToAPullThatEndsWithTheSourcesKeys plays a power cut at
// every call the puller makes to its source, over a range that held stray keys: the steps of an
// attempt before its ready record commit without a sync, so an image may reopen at any earlier
// step of the attempt. Each image, opened with the cut, must hold no key the reset removed once
// its record has left the reset phase, and a pull run on it must end ready with exactly the
// source's keys. seed_sync_bytes is one byte so that each chunk syncs and the images reach the
// snapshot phase too.
func TestAPowerCutAtAnyCallOfAPullReopensToAPullThatEndsWithTheSourcesKeys(t *testing.T) {
	vouched, dir, closed := openVouchedReceiver(t)
	rig := newT3Rig(t, t3Options{kv: vouched, seed: SeedOptions{SyncBytes: 1}})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 2})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 7, t3Cut(t, rig), 0)

	var strays [][]byte
	batch := vouched.NewBatch()
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

	var (
		mu      sync.Mutex
		images  []storeimage.Capture
		takeErr error
	)
	rig.wrap = func(_ string, _ transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
		image, imageErr := storeimage.Take(dir)
		mu.Lock()
		images = append(images, image)
		takeErr = imageErr
		mu.Unlock()
		return reply, err
	}
	rig.pullUntil("the pair to be ready", rig.isReady)
	if takeErr != nil {
		t.Fatal(takeErr)
	}
	if err := vouched.Close(); err != nil {
		t.Fatal(err)
	}
	*closed = true

	pastReset := 0
	for i, image := range images {
		cutStore := openImageStoreVouched(t, image)
		record, found := recordOnDisk(t, cutStore, rig.slot)
		if !found {
			t.Fatalf("image %d holds no record for the pair", i)
		}
		if record.State == machine.SeedReady || record.Phase != machine.PhaseReset {
			pastReset++
			for _, stray := range strays {
				if kvHas(t, cutStore, stray) {
					t.Fatalf("image %d reopens with the record %v in phase %v, past the reset, and the key %q the reset removed came back",
						i, record.State, record.Phase, stray)
				}
			}
		}
		rebooted := newT3Rig(t, t3Options{kv: cutStore})
		rebooted.owner, rebooted.sources = "x", map[string]*t3Source{"x": owner}
		if rebooted.slot != rig.slot {
			t.Fatalf("SCENARIO NOT REACHED: the rebooted rig picked slot %d, want %d", rebooted.slot, rig.slot)
		}
		rebooted.pullUntil("the pair to be ready on the image", rebooted.isReady)
		got := t3Dump(t, cutStore, rig.slot, reconDB)
		t3SameEntries(t, got, owner.dump(t, rig.slot, reconDB))
		for _, stray := range strays {
			for _, entry := range got {
				if bytes.Equal(entry.Key, stray) {
					t.Fatalf("image %d: the key %q the reset removed is in the seeded range", i, stray)
				}
			}
		}
	}
	if len(images) < 3 || pastReset == 0 {
		t.Fatalf("SCENARIO NOT REACHED: %d images, %d of them past the reset phase; want at least 3 and one past the reset",
			len(images), pastReset)
	}
}

// recordSyncKV records, for every committed batch that writes a seed record, whether it was
// committed with a sync and the record it wrote.
type recordSyncKV struct {
	storage.KV
	mu      sync.Mutex
	commits []recordCommit
}

type recordCommit struct {
	synced bool
	record SeedRecord
}

func (k *recordSyncKV) NewBatch() storage.KVBatch {
	return &recordSyncBatch{KVBatch: k.KV.NewBatch(), kv: k}
}

func (k *recordSyncKV) snapshot() []recordCommit {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]recordCommit(nil), k.commits...)
}

type recordSyncBatch struct {
	storage.KVBatch
	kv      *recordSyncKV
	records []SeedRecord
}

func (b *recordSyncBatch) Set(key, value []byte) error {
	if len(key) == len(SeedRecordKey(0, 0)) && key[0] == 0xFF {
		if record, err := DecodeSeedRecord(value); err == nil {
			b.records = append(b.records, record)
		}
	}
	return b.KVBatch.Set(key, value)
}

func (b *recordSyncBatch) Commit(sync bool) error {
	if err := b.KVBatch.Commit(sync); err != nil {
		return err
	}
	b.kv.mu.Lock()
	defer b.kv.mu.Unlock()
	for _, record := range b.records {
		b.kv.commits = append(b.kv.commits, recordCommit{synced: sync, record: record})
	}
	return nil
}

// TestASeedRestartAndARedirectAreCommittedWithASync: only the accepted registration and the end
// of the drain may commit their record without a sync (seedStepCommitsUnsynced). A restart's
// fresh attempt (here a registration whose cut is from a later epoch, SEED_RESTART 3) and a
// redirect's new source are record changes the narrow list leaves out, and they are committed
// with a sync. [BREAK: drop the Restart check, or let every seed step but the ready one commit
// unsynced]
func TestASeedRestartAndARedirectAreCommittedWithASync(t *testing.T) {
	recording := &recordSyncKV{KV: t3OpenKV(t, t.TempDir())}
	t.Cleanup(func() {
		if err := recording.Close(); err != nil {
			t.Errorf("closing the receiver store: %v", err)
		}
	})
	rig := newT3Rig(t, t3Options{kv: recording})
	slot := seedingSlots(t, rig, 1)[0]
	before, _ := rig.rec.Record(slot, reconDB)
	cfg := machine.SeedConfig{SyncBytes: 1 << 20, BufferBytes: 1 << 20, WindowBytes: 1 << 20, Epoch: uint64(rig.state.Epoch)}

	t.Run("a redirect", func(t *testing.T) {
		attempt := before.SeedID
		out, err := rig.rec.FeedSeed(slot, reconDB, FeedSeedRequest{
			Event: machine.RegisterRefused{Reason: seedRefusedDisk, Redirect: "n9"}, Cfg: cfg, Attempt: &attempt,
		})
		if err != nil || out.Restart != 0 {
			t.Fatalf("SCENARIO NOT REACHED: the redirect gave %+v, %v", out, err)
		}
		requireSyncedRecordCommit(t, recording, func(record SeedRecord) bool { return record.Source == "n9" }, "the redirect's new source")
	})
	t.Run("a restart", func(t *testing.T) {
		attempt := before.SeedID
		later := cluster.Position{Epoch: uint64(rig.state.Epoch) + 1, Seq: 5}
		out, err := rig.rec.FeedSeed(slot, reconDB, FeedSeedRequest{
			Event: machine.RegisterAccepted{Cut: later, WindowBytes: 1 << 20}, Cfg: cfg, Attempt: &attempt,
		})
		if err != nil || out.Restart == 0 {
			t.Fatalf("SCENARIO NOT REACHED: a registration on a later epoch's cut gave %+v, %v, want a restart", out, err)
		}
		requireSyncedRecordCommit(t, recording, func(record SeedRecord) bool { return record.SeedID != attempt }, "the restart's fresh attempt")
	})
}

// requireSyncedRecordCommit fails unless a committed record that matches was committed with a sync.
func requireSyncedRecordCommit(t *testing.T, kv *recordSyncKV, matches func(SeedRecord) bool, what string) {
	t.Helper()
	found := false
	for _, commit := range kv.snapshot() {
		if !matches(commit.record) {
			continue
		}
		found = true
		if !commit.synced {
			t.Fatalf("%s was committed without a sync: only the accepted registration and the end of the drain may be", what)
		}
	}
	if !found {
		t.Fatalf("SCENARIO NOT REACHED: no commit wrote %s", what)
	}
}

// TestTheExpiryEntriesAPullDerivesSurviveACutAtOnReady: the expiry rebuild writes its entries
// with unsynced commits (expiry_rebuild.go: expiryEntrySink.flush, Commit(false)) before the
// ready record, and finishSeed makes no sync of its own: the ready record's synced commit must
// vouch them. The derive hook here writes one entry under the expiry prefix the same way, and an
// image taken at OnReady, opened with the cut, must hold it beside the ready record.
// [BREAK: derive the expiry entries after the ready commit instead of before it]
func TestTheExpiryEntriesAPullDerivesSurviveACutAtOnReady(t *testing.T) {
	vouched, dir, closed := openVouchedReceiver(t)
	var (
		image   storeimage.Capture
		takeErr error
		once    sync.Once
	)
	rig := newT3Rig(t, t3Options{kv: vouched, OnReady: func(uint16, uint64) {
		once.Do(func() { image, takeErr = storeimage.Take(dir) })
	}})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 2})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 3, t3Cut(t, rig), 0)
	entry := append(storage.ExpiryPrefix(), []byte("derived-entry")...)
	rig.puller.cfg.DeriveExpiry = func(context.Context, uint16, uint64) error {
		batch := vouched.NewBatch()
		defer batch.Discard()
		if err := batch.Set(entry, []byte{1}); err != nil {
			return err
		}
		return batch.Commit(false)
	}

	rig.pullUntil("the pair to be ready", rig.isReady)
	if takeErr != nil {
		t.Fatal(takeErr)
	}
	if image == nil {
		t.Fatal("SCENARIO NOT REACHED: OnReady never ran, so no image was taken")
	}
	if err := vouched.Close(); err != nil {
		t.Fatal(err)
	}
	*closed = true

	cutStore := openImageStoreVouched(t, image)
	if record, found := recordOnDisk(t, cutStore, rig.slot); !found || record.State != machine.SeedReady {
		t.Fatalf("SCENARIO NOT REACHED: the image at OnReady holds the record %+v (found %v), want ready", record, found)
	}
	if !kvHas(t, cutStore, entry) {
		t.Fatal("a power cut at OnReady kept the ready record and lost the expiry entry the pull derived before it: " +
			"the ready record's synced commit did not vouch the derivation's unsynced commit")
	}
}
