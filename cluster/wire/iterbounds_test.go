package wire

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"reflect"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// recordingSnapshot remembers the options of every iterator a read opens, so a
// test can prove the read bounded Badger instead of walking every table.
type recordingSnapshot struct {
	storage.KVSnapshot
	opened []storage.IterOptions
}

func (s *recordingSnapshot) NewIterator(opts storage.IterOptions) storage.KVIterator {
	s.opened = append(s.opened, opts)
	return s.KVSnapshot.NewIterator(opts)
}

// unboundedSnapshot opens every iterator with no bounds at all: what a document
// read did before it passed any. It is the reference a bounded read must equal.
type unboundedSnapshot struct{ storage.KVSnapshot }

func (s unboundedSnapshot) NewIterator(storage.IterOptions) storage.KVIterator {
	return s.KVSnapshot.NewIterator(storage.IterOptions{})
}

// minPrunablePrefix is slot(2) + database(8) + bucket(8): the shortest prefix that lets
// Badger skip every table outside one bucket's slot.
const minPrunablePrefix = 18

// sharedBoundPrefix mirrors storage.sharedPrefix (unexported, so repeated here): the bytes
// Badger is given as Prefix when an iterator has both bounds. Keep the two in step.
func sharedBoundPrefix(lower, upper []byte) []byte {
	shared := 0
	for shared < len(lower) && shared < len(upper) && lower[shared] == upper[shared] {
		shared++
	}
	return lower[:shared]
}

// requireBoundsOf fails unless opts is exactly the half-open range of every key that starts with
// prefix. Both bounds are required, and they must share a prefix, because Badger is given
// only the bytes the two bounds share (storage.sharedPrefix): two unrelated bounds hand it
// nothing to prune tables with.
func requireBoundsOf(t *testing.T, what string, opts storage.IterOptions, prefix []byte) {
	t.Helper()
	if !bytes.Equal(opts.Lower, prefix) {
		t.Errorf("%s: Lower is %x, want the document prefix %x", what, opts.Lower, prefix)
	}
	wantUpper := storage.PrefixEnd(prefix)
	if !bytes.Equal(opts.Upper, wantUpper) {
		t.Errorf("%s: Upper is %x, want the end of the document prefix %x", what, opts.Upper, wantUpper)
	}
	if shared := len(sharedBoundPrefix(opts.Lower, opts.Upper)); shared < minPrunablePrefix {
		t.Errorf("%s: bounds %x..%x share %d bytes, want at least %d (slot, database, bucket), or Badger cannot prune tables by slot and bucket", what, opts.Lower, opts.Upper, shared, minPrunablePrefix)
	}
	if opts.Reverse || opts.IncludeDeleted {
		t.Errorf("%s: opened with Reverse=%v IncludeDeleted=%v, a document read wants neither", what, opts.Reverse, opts.IncludeDeleted)
	}
}

func openSmallMemtableKV(t *testing.T, dir string) *storage.BadgerKV {
	t.Helper()
	kv, err := storage.OpenBadgerKV(dir, storage.BadgerTuning{MemTableMB: 8}, nil)
	if err != nil {
		t.Fatalf("OpenBadgerKV: %v", err)
	}
	return kv
}

func encodeDocID(t testing.TB, id string) []byte {
	t.Helper()
	encoded, err := store.EncodeComponent(id)
	if err != nil {
		t.Fatalf("EncodeComponent(%q): %v", id, err)
	}
	return encoded
}

func TestReadStoredDocumentOpensABoundedIterator(t *testing.T) {
	fx := newFixture(t)
	encID := encodeDocID(t, "u1")
	batch := fx.kv.NewBatch()
	prefix := store.DocumentPrefix(testSlot, testDB, testBucket, encID)
	for _, key := range [][]byte{prefix, store.FieldKey(prefix, 2), store.FieldKey(prefix, 3)} {
		if err := batch.Set(key, []byte{0x02, 1, 0, 0, 0, 'x'}); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
	snapshot := &recordingSnapshot{KVSnapshot: fx.kv.NewSnapshot()}
	defer snapshot.Discard()

	doc, found, err := readStoredDocument(snapshot, ReadRequest{Slot: testSlot, DBID: testDB, BucketID: testBucket, EncID: encID}, "")
	if err != nil || !found || len(doc.fields) != 2 {
		t.Fatalf("readStoredDocument = %+v, %v, %v", doc, found, err)
	}
	if len(snapshot.opened) != 1 {
		t.Fatalf("the read opened %d iterators, want 1", len(snapshot.opened))
	}
	requireBoundsOf(t, "readStoredDocument", snapshot.opened[0], prefix)
}

func TestAllScalarsOpensABoundedIterator(t *testing.T) {
	fx := newFixture(t)
	prefix := store.DocumentPrefix(testSlot, testDB, testBucket, encodeDocID(t, "u1"))
	batch := fx.kv.NewBatch()
	if err := batch.Set(prefix, []byte{0x02, 1, 0, 0, 0, 'x'}); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
	snapshot := &recordingSnapshot{KVSnapshot: fx.kv.NewSnapshot()}
	defer snapshot.Discard()
	if _, err := allScalars(snapshot, prefix); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.opened) != 1 {
		t.Fatalf("the read opened %d iterators, want 1", len(snapshot.opened))
	}
	requireBoundsOf(t, "allScalars", snapshot.opened[0], prefix)
}

// documentModel is what the test believes the store holds, kept apart from the
// store so a read is judged against it and not against itself.
type documentModel map[string]modelDoc

type modelDoc struct {
	header []byte
	fields map[uint16][]byte
}

func modelKey(bucket uint64, id string) string { return fmt.Sprintf("%d/%s", bucket, id) }

func (m documentModel) put(kv storage.KV, t *testing.T, bucket uint64, id string, header []byte, fields map[uint16][]byte) {
	t.Helper()
	prefix := store.DocumentPrefix(testSlot, testDB, bucket, encodeDocID(t, id))
	batch := kv.NewBatch()
	defer batch.Discard()
	// An overwrite replaces the whole document, as an upsert does.
	if old, ok := m[modelKey(bucket, id)]; ok {
		for fieldID := range old.fields {
			if _, kept := fields[fieldID]; !kept {
				if err := batch.Delete(store.FieldKey(prefix, fieldID)); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if err := batch.Set(prefix, header); err != nil {
		t.Fatal(err)
	}
	for fieldID, stored := range fields {
		if err := batch.Set(store.FieldKey(prefix, fieldID), stored); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
	m[modelKey(bucket, id)] = modelDoc{header: header, fields: fields}
}

// remove deletes every key of the document, leaving delete markers.
func (m documentModel) remove(kv storage.KV, t *testing.T, bucket uint64, id string) {
	t.Helper()
	prefix := store.DocumentPrefix(testSlot, testDB, bucket, encodeDocID(t, id))
	batch := kv.NewBatch()
	defer batch.Discard()
	if err := batch.Delete(prefix); err != nil {
		t.Fatal(err)
	}
	for fieldID := range m[modelKey(bucket, id)].fields {
		if err := batch.Delete(store.FieldKey(prefix, fieldID)); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
	delete(m, modelKey(bucket, id))
}

// filler writes about 10 MiB of incompressible keys into a bucket no test reads,
// enough to push an 8 MiB memtable to disk.
func filler(t *testing.T, kv storage.KV, bucket uint64) {
	t.Helper()
	for chunk := 0; chunk < 100; chunk++ {
		batch := kv.NewBatch()
		for i := 0; i < 100; i++ {
			value := make([]byte, 1000)
			if _, err := rand.Read(value); err != nil {
				t.Fatal(err)
			}
			id := encodeDocID(t, fmt.Sprintf("filler-%d-%d", chunk, i))
			if err := batch.Set(store.DocumentPrefix(testSlot, testDB, bucket, id), value); err != nil {
				t.Fatal(err)
			}
		}
		if err := batch.Commit(false); err != nil {
			t.Fatal(err)
		}
	}
}

func fieldValue(text string) []byte {
	return append([]byte{0x02, byte(len(text) + 1), 0, 0, 0}, append([]byte(text), 0)...)
}

// TestBoundedDocumentReadEqualsTheUnboundedRead builds a store across several
// generations (a reopen flushes the memtable to a table; the filler and the
// 8 MiB memtable add more flushes and compactions), then reads every document
// both ways. The bounded read must return what the model says and what the
// unbounded read returns, whichever table or memtable holds the document's
// versions.
func TestBoundedDocumentReadEqualsTheUnboundedRead(t *testing.T) {
	if testing.Short() {
		t.Skip("writes about 30 MiB to build several tables")
	}
	dir := t.TempDir()
	model := documentModel{}
	kv := openSmallMemtableKV(t, dir)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = kv.Close()
		}
	})
	reopen := func() {
		t.Helper()
		if err := kv.Close(); err != nil {
			t.Fatalf("closing a generation: %v", err)
		}
		kv = openSmallMemtableKV(t, dir)
	}
	// Buckets 1, 256 and 257 share leading id bytes; the filler bucket is far away.
	buckets := []uint64{1, 256, 257, testBucket}
	for _, bucket := range buckets {
		// u1 is a byte prefix of u10 and u100 as text; the encoding must keep them apart.
		model.put(kv, t, bucket, "u1", fieldValue("one"), map[uint16][]byte{2: fieldValue("a"), 3: fieldValue("b"), 4: fieldValue("c")})
		model.put(kv, t, bucket, "u10", fieldValue("ten"), map[uint16][]byte{2: fieldValue("ten-a"), 3: fieldValue("ten-b")})
		model.put(kv, t, bucket, "u100", fieldValue("hundred"), map[uint16][]byte{2: fieldValue("hundred-a")})
		model.put(kv, t, bucket, "u1\x00", fieldValue("nul"), map[uint16][]byte{2: fieldValue("nul-a")})
		model.put(kv, t, bucket, "u3", fieldValue("three"), map[uint16][]byte{2: fieldValue("old")})
		model.put(kv, t, bucket, "u4", fieldValue("four"), map[uint16][]byte{2: fieldValue("doomed")})
		model.put(kv, t, bucket, "u5", fieldValue("five"), map[uint16][]byte{2: fieldValue("v1")})
	}
	filler(t, kv, 900)
	reopen()
	for _, bucket := range buckets {
		// Overwrite so older versions sit in an older table; drop a field.
		model.put(kv, t, bucket, "u3", fieldValue("three"), map[uint16][]byte{2: fieldValue("new"), 3: fieldValue("added")})
		model.remove(kv, t, bucket, "u4")
		// A tombstone header, as a document delete writes it, over live fields.
		model.put(kv, t, bucket, "u5", []byte{0x00}, map[uint16][]byte{})
	}
	filler(t, kv, 901)
	reopen()
	for _, bucket := range buckets {
		model.put(kv, t, bucket, "u4", fieldValue("four-again"), map[uint16][]byte{2: fieldValue("back")})
	}
	filler(t, kv, 902) // the last generation stays partly in the memtable

	snapshot := kv.NewSnapshot()
	absent := []string{"u2", "u", "u1000", "u10\x00", ""}
	ids := []string{"u1", "u10", "u100", "u1\x00", "u3", "u4", "u5"}
	projections := [][]uint16{nil, {3}, {2, 4}, {99}}
	for _, bucket := range buckets {
		for _, id := range append(append([]string{}, ids...), absent...) {
			if id == "" {
				continue // the empty id has no encoding to read
			}
			for _, project := range projections {
				req := ReadRequest{Slot: testSlot, DBID: testDB, BucketID: bucket, EncID: encodeDocID(t, id), Fields: project}
				bounded, boundedLive, err := readStoredDocument(snapshot, req, "")
				if err != nil {
					t.Fatalf("bucket %d id %q: %v", bucket, id, err)
				}
				reference, referenceLive, err := readStoredDocument(unboundedSnapshot{snapshot}, req, "")
				if err != nil {
					t.Fatalf("bucket %d id %q unbounded: %v", bucket, id, err)
				}
				if boundedLive != referenceLive || !reflect.DeepEqual(bounded, reference) {
					t.Errorf("bucket %d id %q project %v: bounded read %+v (live %v) differs from the unbounded read %+v (live %v)",
						bucket, id, project, bounded, boundedLive, reference, referenceLive)
				}
				want, held := model[modelKey(bucket, id)]
				wantLive := held && (len(want.header) != 1 || want.header[0] != 0x00)
				if boundedLive != wantLive {
					t.Errorf("bucket %d id %q: live = %v, the model says %v", bucket, id, boundedLive, wantLive)
				}
				if !wantLive {
					continue
				}
				if !bytes.Equal(bounded.header, want.header) {
					t.Errorf("bucket %d id %q: header %x, want %x", bucket, id, bounded.header, want.header)
				}
				got := map[uint16][]byte{}
				for _, field := range bounded.fields {
					got[field.id] = field.stored
				}
				wantFields := map[uint16][]byte{}
				for fieldID, stored := range want.fields {
					if keepsField(project, fieldID) {
						wantFields[fieldID] = stored
					}
				}
				if !reflect.DeepEqual(got, wantFields) {
					t.Errorf("bucket %d id %q project %v: fields %v, want %v", bucket, id, project, got, wantFields)
				}
			}
			scalars, err := allScalars(snapshot, store.DocumentPrefix(testSlot, testDB, bucket, encodeDocID(t, id)))
			if err != nil {
				t.Fatalf("allScalars bucket %d id %q: %v", bucket, id, err)
			}
			reference, err := allScalars(unboundedSnapshot{snapshot}, store.DocumentPrefix(testSlot, testDB, bucket, encodeDocID(t, id)))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(scalars, reference) {
				t.Errorf("allScalars bucket %d id %q: %v differs from the unbounded read %v", bucket, id, scalars, reference)
			}
		}
	}

	// The slot scan keeps its one unbounded iterator (see the slot scan note in the commit history);
	// it must list the same documents however the snapshot hands out iterators.
	var mask cluster.SlotMask
	mask.Set(testSlot)
	mask.Set(testSlot + 1)
	for _, bucket := range buckets {
		collect := func(snap storage.KVSnapshot) []string {
			var seen []string
			scanner := NewSlotScanner(SlotScannerConfig{})
			plan := scanPlan{db: testDB, bucketID: bucket, mask: mask}
			err := scanner.walk(context.Background(), snap, plan, 0, nil, func(doc scannedDoc) (bool, error) {
				seen = append(seen, fmt.Sprintf("%d/%x/%x/%d", doc.slot, doc.encID, doc.header, len(doc.fields)))
				return true, nil
			})
			if err != nil {
				t.Fatalf("slot walk of bucket %d: %v", bucket, err)
			}
			return seen
		}
		bounded, reference := collect(snapshot), collect(unboundedSnapshot{snapshot})
		if len(bounded) == 0 || !reflect.DeepEqual(bounded, reference) {
			t.Errorf("bucket %d: bounded walk %v, unbounded walk %v", bucket, bounded, reference)
		}
	}

	snapshot.Discard()
	if err := kv.Close(); err != nil {
		t.Fatalf("closing: %v", err)
	}
	closed = true
}
