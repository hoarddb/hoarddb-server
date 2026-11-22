package wire

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/internal/storeimage"
	"github.com/hoarddb/hoarddb-server/storage"
)

// recordsOf lists the sweep records of one kind in kv whose key starts with prefix.
func recordsOf(t *testing.T, kv storage.KV, kind byte) [][]byte {
	t.Helper()
	prefix := storage.NodeKey(storage.NSReclaim, []byte{kind})
	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	iter := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: storage.PrefixEnd(prefix)})
	defer iter.Close()
	var keys [][]byte
	for iter.Seek(prefix); iter.Valid(); iter.Next() {
		keys = append(keys, append([]byte(nil), iter.Key()...))
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}
	return keys
}

// kvHas reports whether kv holds key.
func kvHas(t *testing.T, kv storage.KV, key []byte) bool {
	t.Helper()
	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	_, err := snapshot.Get(key)
	if err == nil {
		return true
	}
	if isNotFound(err) {
		return false
	}
	t.Fatalf("reading %x: %v", key, err)
	return false
}

func isNotFound(err error) bool { return errors.Is(err, storage.ErrKeyNotFound) }

// deleteDurably removes keys from kv in one synced batch.
func deleteDurably(t *testing.T, kv storage.KV, keys ...[]byte) {
	t.Helper()
	batch := kv.NewBatch()
	for _, key := range keys {
		if err := batch.Delete(key); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
}

// openImageStoreVouched opens a copy of a store directory as the node would, with the cut, and
// returns it with what the cut did.
func openImageStoreVouched(t *testing.T, capture storeimage.Capture) *storage.BadgerKV {
	t.Helper()
	copyDir := filepath.Join(t.TempDir(), "badger")
	if err := storeimage.Write(copyDir, capture); err != nil {
		t.Fatal(err)
	}
	kv, err := openVouchedTestKV(t, copyDir, storage.BadgerTuning{MemTableMB: 64})
	if err != nil {
		t.Fatalf("opening the image's store with the cut: %v", err)
	}
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Error(err)
		}
	})
	return kv
}

// sweepFixture is a node without a cluster whose sweep loop the test holds, with buckets that have
// documents in one pair.
type sweepFixture struct {
	node *Node
	dir  string
	db   uint64
}

func newSweepFixture(t *testing.T) *sweepFixture {
	t.Helper()
	dir := t.TempDir()
	node := openSingleNode(t, dir, false)
	ctx := context.Background()
	if _, err := node.CreateDatabase(ctx, u30iDB); err != nil {
		t.Fatal(err)
	}
	db, err := node.database(u30iDB)
	if err != nil {
		t.Fatal(err)
	}
	// The background loop is held for the whole test, so a record is worked only by the call
	// under test, never behind it.
	node.retire.loopMu.Lock()
	t.Cleanup(node.retire.loopMu.Unlock)
	return &sweepFixture{node: node, dir: dir, db: db}
}

// bucketWithDocuments creates bucket name with fields and inserts count documents into slot.
func (f *sweepFixture) bucketWithDocuments(t *testing.T, name string, slot uint16, count int, fields ...Field) uint64 {
	t.Helper()
	ctx := context.Background()
	id, err := f.node.CreateBucket(ctx, u30iDB, name, fields...)
	if err != nil {
		t.Fatal(err)
	}
	for _, doc := range idsInSlot(t, u30iDB, name, slot, count) {
		if _, err := f.node.Insert(ctx, u30iDB, name, bson.D{{Key: "_id_", Value: doc}, {Key: "Name", Value: doc}}); err != nil {
			t.Fatal(err)
		}
	}
	return id
}

// TestSweepRecordsSurviveTheCut is T11: the records RetireSweep.RerunOnReady and
// RetireSweep.SweepBucket write are the only memory of their work, and nothing derives them again
// after a cut. The image is every store file and the marker as of the last sync completed before
// the call, plus whatever the call itself made durable: the store's files taken right after the
// call (or, for RecordSweeps, right after SweepBucket's first step), with their marker. The cut discards what
// that marker does not vouch, which is exactly what no sync covered.
func TestSweepRecordsSurviveTheCut(t *testing.T) {
	t.Run("RerunOnReady's records", func(t *testing.T) {
		f := newSweepFixture(t)
		slot := uint16(300)
		bucket := f.bucketWithDocuments(t, "Rec", slot, 3, Field{Name: "Name", Type: "string"}, Field{Name: "Old", Type: "string"})
		if _, err := f.node.AlterBucket(context.Background(), u30iDB, "Rec", AlterChange{Drop: []string{"Old"}}); err != nil {
			t.Fatal(err)
		}
		// The bump's note wrote the 't' records durable; the pair is one that became ready after it, so
		// its record is gone and RerunOnReady must write it again.
		var ours [][]byte
		for _, key := range recordsOf(t, f.node.kv, reclaimRetireKind) {
			if rec, err := DecodeRetireKey(key); err == nil && rec.Slot == slot && rec.DBID == f.db && rec.BucketID == bucket {
				ours = append(ours, key)
			}
		}
		if len(ours) == 0 {
			t.Fatal("SCENARIO NOT REACHED: the alter wrote no retired-field record for the pair")
		}
		deleteDurably(t, f.node.kv, ours...)
		if err := f.node.kv.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := f.node.retire.RerunOnReady(slot, f.db); err != nil {
			t.Fatal(err)
		}
		if !kvHas(t, f.node.kv, ours[0]) {
			t.Fatal("SCENARIO NOT REACHED: RerunOnReady wrote no record for the pair")
		}
		image := openImageStoreVouched(t, takeStore(t, f.dir))
		for _, key := range ours {
			if !kvHas(t, image, key) {
				t.Errorf("the retired-field record %x that RerunOnReady wrote is gone after the cut: nothing derives it again", key)
			}
		}
	})

	t.Run("RecordSweeps's record, then a kill and a restart", func(t *testing.T) {
		f := newSweepFixture(t)
		slot := uint16(310)
		ctx := context.Background()
		f.bucketWithDocuments(t, "Gone", slot, 5, Field{Name: "Name", Type: "string"})
		bucket, err := f.node.StartDropBucket(ctx, u30iDB, "Gone")
		if err != nil {
			t.Fatal(err)
		}
		record := BucketRecord{Slot: slot, DBID: f.db, BucketID: bucket}.Key()
		if !kvHas(t, f.node.kv, record) {
			t.Fatal("SCENARIO NOT REACHED: the drop's note wrote no dropped-bucket record for the pair")
		}
		// A drop whose bump another change carried: the note did not write this pair's record, so
		// the drop's pre-pass writes it before the first sweep.
		deleteDurably(t, f.node.kv, record)
		if err := f.node.kv.Sync(); err != nil {
			t.Fatal(err)
		}
		if scopeKeys(t, f.node.kv, slot, f.db, bucket) == 0 {
			t.Fatal("SCENARIO NOT REACHED: the dropped bucket has no key in the pair")
		}
		var atFirstStep storeimage.Capture
		var takeErr error
		f.node.retire.afterStep = func() {
			if atFirstStep == nil && takeErr == nil {
				atFirstStep, takeErr = storeimage.Take(filepath.Join(f.dir, "badger"))
			}
		}
		t.Cleanup(func() { f.node.retire.afterStep = nil })
		if err := f.node.retire.RecordSweeps(f.db, bucket, []uint16{slot}); err != nil {
			t.Fatal(err)
		}
		if err := f.node.retire.SweepBucket(ctx, slot, f.db, bucket); err != nil {
			t.Fatal(err)
		}
		if takeErr != nil || atFirstStep == nil {
			t.Fatalf("SCENARIO NOT REACHED: no copy at SweepBucket's first step (%v)", takeErr)
		}
		image := openImageStoreVouched(t, atFirstStep)
		if !kvHas(t, image, record) {
			t.Errorf("the dropped-bucket record RecordSweeps wrote is gone after the cut, while the drop goes on to log the pair as swept")
		}

		// The kill at the first step and the restart: the loop must finish the pair's sweep.
		f.node.retire.afterStep = nil
		f.node.retire.loopMu.Unlock()
		if err := f.node.Close(); err != nil {
			t.Fatal(err)
		}
		f.node.retire.loopMu.Lock() // the fixture's cleanup unlocks it again
		nodeImage := t.TempDir()
		writeStoreImage(t, nodeImage, atFirstStep)
		copyNodeExceptStore(t, f.dir, nodeImage)
		restarted := openSingleNode(t, nodeImage, false)
		deadline := time.Now().Add(vouchedTestTimeout)
		left := scopeKeys(t, restarted.kv, slot, f.db, bucket)
		for left > 0 && time.Now().Before(deadline) {
			time.Sleep(100 * time.Millisecond)
			left = scopeKeys(t, restarted.kv, slot, f.db, bucket)
		}
		if left > 0 {
			t.Errorf("%d keys of the dropped bucket remain in slot %d after a kill and a restart: no record told the loop to sweep them", left, slot)
		}
	})

	t.Run("SweepBucket syncs nothing when the note wrote the record", func(t *testing.T) {
		f := newSweepFixture(t)
		slot := uint16(320)
		ctx := context.Background()
		f.bucketWithDocuments(t, "Gone2", slot, 3, Field{Name: "Name", Type: "string"})
		bucket, err := f.node.StartDropBucket(ctx, u30iDB, "Gone2")
		if err != nil {
			t.Fatal(err)
		}
		if !kvHas(t, f.node.kv, BucketRecord{Slot: slot, DBID: f.db, BucketID: bucket}.Key()) {
			t.Fatal("SCENARIO NOT REACHED: the drop's note wrote no dropped-bucket record for the pair")
		}
		before, _, _ := f.node.kv.VouchedCounts()
		var atFirstStep uint64
		stepped := false
		f.node.retire.afterStep = func() {
			if !stepped {
				stepped = true
				atFirstStep, _, _ = f.node.kv.VouchedCounts()
			}
		}
		t.Cleanup(func() { f.node.retire.afterStep = nil })
		if err := f.node.retire.SweepBucket(ctx, slot, f.db, bucket); err != nil {
			t.Fatal(err)
		}
		if !stepped {
			t.Fatal("SCENARIO NOT REACHED: SweepBucket took no step")
		}
		if atFirstStep != before {
			t.Errorf("SweepBucket made %d store syncs up to its first step although the note had written its record", atFirstStep-before)
		}
	})
}

// TestADropsRecordsSurviveTheCutAtItsFirstSweptWrite is the T11 row of BUG-0861: a drop writes
// every missing dropped-bucket record in one synced pre-pass before its first sweep, so an image
// taken right after the first bitmap write (the store files as of the last sync, with their marker,
// the cut applied on open) holds the record of every slot the bitmap does not yet name. If the
// pre-pass were unsynced those records would be above the cut, and a kill there would leave the
// bucket's keys in those slots with nothing to sweep them.
func TestADropsRecordsSurviveTheCutAtItsFirstSweptWrite(t *testing.T) {
	rig := newU42Rig(t)
	rig.pause()
	rig.crashAt(u42Bump, 1)
	if err := rig.run(); !errors.Is(err, errU42Crash) {
		t.Fatalf("run = %v, want the crash after the bump", err)
	}
	// The records of the slots the node comes to hold after the bump are the ones the pre-pass writes.
	if _, err := rig.node.List(context.Background(), u30iDB, "Order", nil, 0); err != nil {
		t.Fatal(err)
	}
	held := rig.node.pairs.HeldSlots(rig.dbID)
	if len(held) <= sweptBatchSlots {
		t.Fatalf("SCENARIO NOT REACHED: the node holds %d slots, want more than one bitmap batch of %d", len(held), sweptBatchSlots)
	}
	rig.install()
	var image storeimage.Capture
	rig.drop.crashAfter = func(step string) error {
		if step == u42Swept {
			image = takeStore(t, rig.dir)
			return errU42Crash
		}
		return nil
	}
	if err := rig.run(); !errors.Is(err, errU42Crash) {
		t.Fatalf("run = %v, want the crash at the first bitmap write", err)
	}
	if image == nil {
		t.Fatal("SCENARIO NOT REACHED: no image at the first bitmap write")
	}
	cut := openImageStoreVouched(t, image)
	for _, slot := range held[sweptBatchSlots:] {
		if key := (BucketRecord{Slot: slot, DBID: rig.dbID, BucketID: rig.bucketID}).Key(); !kvHas(t, cut, key) {
			t.Fatalf("the record of slot %d, which the bitmap does not name yet, is gone after the cut", slot)
		}
	}
}
