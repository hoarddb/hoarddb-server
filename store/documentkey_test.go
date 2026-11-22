package store

import (
	"bytes"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

const (
	docTestSlot     uint16 = 0x1362
	docTestDatabase uint64 = 1
	docTestBucket   uint64 = 41
)

// docRange reads every key of one bucket's range back out of kv.
func docRange(t *testing.T, kv storage.KV) map[string][]byte {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	scope := storage.Scope(docTestSlot, docTestDatabase, docTestBucket)
	upper := storage.Scope(docTestSlot, docTestDatabase, docTestBucket+1)
	it := snap.NewIterator(storage.IterOptions{Lower: scope, Upper: upper})
	defer it.Close()
	found := map[string][]byte{}
	for it.Seek(scope); it.Valid(); it.Next() {
		stored, err := it.Value()
		if err != nil {
			t.Fatalf("reading %x: %v", it.Key(), err)
		}
		found[string(it.Key())] = stored
	}
	if err := it.Err(); err != nil {
		t.Fatalf("walking the bucket range: %v", err)
	}
	return found
}

func docRef() storage.BucketRef {
	return storage.BucketRef{Slot: docTestSlot, DatabaseID: docTestDatabase, BucketID: docTestBucket}
}

func mustHexBytes(t *testing.T, text string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(strings.ReplaceAll(text, " ", ""))
	if err != nil {
		t.Fatalf("hex %q: %v", text, err)
	}
	return decoded
}

// TestDocumentPrefixIsExact: ids "a", "a\x00" and "ab" share a bucket, and
// each document's prefix scan returns only its own keys.
func TestDocumentPrefixIsExact(t *testing.T) {
	kv := kvfault.New()
	ids := []string{"a", "a\x00", "ab"}
	encoded := map[string][]byte{}
	batch := kv.NewBatch()
	for _, id := range ids {
		encID, err := EncodeComponent(id)
		if err != nil {
			t.Fatalf("EncodeComponent(%q): %v", id, err)
		}
		encoded[id] = encID
		req := DocumentInsert{Ref: docRef(), EncID: encID, Header: []byte{0x02, 0x01}, Fields: []DocumentField{{FieldID: 2, Stored: []byte{0x10, 0x07}}}}
		if err := WriteInsert(batch, req); err != nil {
			t.Fatalf("WriteInsert(%q): %v", id, err)
		}
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("commit: %v", err)
	}
	all := docRange(t, kv)
	if len(all) != 6 {
		t.Fatalf("three documents of two keys each: got %d keys", len(all))
	}
	for _, id := range ids {
		prefix := DocumentPrefix(docTestSlot, docTestDatabase, docTestBucket, encoded[id])
		own := 0
		for key := range all {
			if strings.HasPrefix(key, string(prefix)) {
				own++
				if key != string(prefix) && key != string(FieldKey(prefix, 2)) {
					t.Errorf("document %q's prefix reaches the foreign key %x", id, key)
				}
			}
		}
		if own != 2 {
			t.Errorf("document %q: its prefix reaches %d keys, want its own 2", id, own)
		}
	}
	// Deleting one document by prefix leaves the others whole.
	doomed := DocumentPrefix(docTestSlot, docTestDatabase, docTestBucket, encoded["a"])
	for key := range all {
		if strings.HasPrefix(key, string(doomed)) {
			delete(all, key)
		}
	}
	if len(all) != 4 {
		t.Errorf("deleting document \"a\" by prefix left %d keys, want the other two documents' 4", len(all))
	}
}

// TestInsertWritesExactlyTheExampleKeys is placement Formats 2, byte for byte.
func TestInsertWritesExactlyTheExampleKeys(t *testing.T) {
	kv := kvfault.New()
	encID := mustHexBytes(t, "50 70 31 00 00")
	req := DocumentInsert{
		Ref:    docRef(),
		EncID:  encID,
		Header: mustHexBytes(t, "02 03 00 00 00 70 31 00"),
		Fields: []DocumentField{
			{FieldID: 2, Stored: mustHexBytes(t, "02 04 00 00 00 41 64 61 00")},
			{FieldID: 3, Stored: mustHexBytes(t, "12 24 00 00 00 00 00 00 00")},
			{FieldID: 4, Store: true},
			{FieldID: 5, Store: true},
		},
	}
	batch := kv.NewBatch()
	if err := WriteInsert(batch, req); err != nil {
		t.Fatalf("WriteInsert: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("commit: %v", err)
	}
	header := mustHexBytes(t, "13 62 00 00 00 00 00 00 00 01 00 00 00 00 00 00 00 29 01 50 70 31 00 00")
	want := map[string]string{
		string(header): "02 03 00 00 00 70 31 00",
		string(append(bytes.Clone(header), 0, 2)): "02 04 00 00 00 41 64 61 00",
		string(append(bytes.Clone(header), 0, 3)): "12 24 00 00 00 00 00 00 00",
	}
	got := docRange(t, kv)
	if len(got) != len(want) {
		t.Fatalf("the insert wrote %d keys, want exactly 3 (an empty store writes none): %x", len(got), got)
	}
	keyBytes, valueBytes := 0, 0
	for key, stored := range got {
		wantValue, ok := want[key]
		if !ok {
			t.Fatalf("unexpected key %x", key)
		}
		if !bytes.Equal(stored, mustHexBytes(t, wantValue)) {
			t.Errorf("key %x holds %x, want %s", key, stored, wantValue)
		}
		keyBytes += len(key)
		valueBytes += len(stored)
	}
	if keyBytes != 76 || valueBytes != 26 {
		t.Errorf("the batch is %d key bytes + %d value bytes, want 76 + 26 = 102", keyBytes, valueBytes)
	}
	if !bytes.Equal(DocumentPrefix(docTestSlot, docTestDatabase, docTestBucket, encID), header) {
		t.Errorf("DocumentPrefix is %x, want the example header %x", DocumentPrefix(docTestSlot, docTestDatabase, docTestBucket, encID), header)
	}
}

func TestInsertRefusesFieldIDsThatAreNeverKeys(t *testing.T) {
	for _, id := range []uint16{0, 1} {
		req := DocumentInsert{Ref: docRef(), EncID: []byte{0x50, 0x00, 0x00}, Header: []byte{0x02},
			Fields: []DocumentField{{FieldID: id, Stored: []byte{0x10}}}}
		if err := WriteInsert(kvfault.New().NewBatch(), req); err == nil {
			t.Errorf("field id %d was accepted; 0 is the header marker and 1 is _id_, which has only the header key", id)
		}
	}
}

// failOnSet fails the nth Set and counts commits, standing in for a disk that
// dies part way through one insert.
type failOnSet struct {
	storage.KVBatch
	failAt  int
	sets    int
	commits int
}

var errInjectedSet = errors.New("injected Set failure")

func (b *failOnSet) Set(key, stored []byte) error {
	b.sets++
	if b.sets == b.failAt {
		return errInjectedSet
	}
	return b.KVBatch.Set(key, stored)
}

func (b *failOnSet) Commit(sync bool) error {
	b.commits++
	return b.KVBatch.Commit(sync)
}

// TestInsertIsAtomic: a failure on the last Set leaves no key of the document,
// and the writer never commits on its own account.
func TestInsertIsAtomic(t *testing.T) {
	kv := kvfault.New()
	batch := &failOnSet{KVBatch: kv.NewBatch(), failAt: 3}
	req := DocumentInsert{Ref: docRef(), EncID: []byte{0x50, 0x70, 0x31, 0x00, 0x00}, Header: []byte{0x02, 0x01},
		Fields: []DocumentField{{FieldID: 2, Stored: []byte{0x10, 0x01}}, {FieldID: 3, Stored: []byte{0x10, 0x02}}}}
	err := WriteInsert(batch, req)
	if !errors.Is(err, errInjectedSet) {
		t.Fatalf("WriteInsert error = %v, want the injected failure wrapped", err)
	}
	if batch.commits != 0 {
		t.Errorf("the writer committed %d time(s); the caller owns the one commit", batch.commits)
	}
	batch.Discard()
	if left := docRange(t, kv); len(left) != 0 {
		t.Errorf("a failed insert left %d key(s) behind: %x", len(left), left)
	}
}

// TestStorePrefixIsNotADocumentPrefix: a store field's sub-tree and a
// document's keys are two prefixes, neither containing the other.
func TestStorePrefixIsNotADocumentPrefix(t *testing.T) {
	encID := []byte{0x50, 0x70, 0x31, 0x00, 0x00}
	doc := DocumentPrefix(docTestSlot, docTestDatabase, docTestBucket, encID)
	store := StorePrefix(docTestSlot, docTestDatabase, docTestBucket, encID, 4)
	if bytes.HasPrefix(store, doc) || bytes.HasPrefix(doc, store) {
		t.Fatalf("store prefix %x and document prefix %x overlap", store, doc)
	}
	if store[18] != storage.NSFieldStore {
		t.Errorf("store prefix namespace byte = %#x, want %#x", store[18], storage.NSFieldStore)
	}
	wantStore := mustHexBytes(t, "13 62 00 00 00 00 00 00 00 01 00 00 00 00 00 00 00 29 07 50 70 31 00 00 00 04")
	if !bytes.Equal(store, wantStore) {
		t.Errorf("StorePrefix = %x, want %x", store, wantStore)
	}
	if !bytes.Equal(FieldKey(doc, 0x0102), append(bytes.Clone(doc), 0x01, 0x02)) {
		t.Errorf("FieldKey is not docPrefix ‖ fieldID big-endian")
	}
}
