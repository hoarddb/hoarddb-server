package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"log/slog"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/store"
)

// keyContaining is the one stored key of the fault store that holds fragment:
// the key a handler reads for a value the test wrote, so the test can arm the
// one-shot read fault on exactly it.
func keyContaining(t *testing.T, kv *kvfault.Store, fragment []byte) []byte {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{})
	defer it.Close()
	var found []byte
	for it.Seek(nil); it.Valid(); it.Next() {
		if bytes.Contains(it.Key(), fragment) {
			if found != nil {
				t.Fatalf("more than one stored key holds %q", fragment)
			}
			found = bytes.Clone(it.Key())
		}
	}
	if found == nil {
		t.Fatalf("no stored key holds %q", fragment)
	}
	return found
}

// requireKeyOutOfClientText fails when message carries key as hex.
func requireKeyOutOfClientText(t *testing.T, message string, key []byte) {
	t.Helper()
	if strings.Contains(message, hex.EncodeToString(key)) {
		t.Errorf("the message carries a storage key as hex: %s", message)
	}
}

// TestBTreeEdgeReadFailureOmitsStorageKey: BUG-0591 beyond the heap. A btree
// first() whose read fails answers without the key's encoding, and the key goes
// to the log.
func TestBTreeEdgeReadFailureOmitsStorageKey(t *testing.T) {
	s, kv, _, _ := newFaultNodeWithBucket(t, BucketTypeBTree)
	if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "held", Key: "alpha", Data: map[string]interface{}{"n": 1}}); err != nil || !resp.Success {
		t.Fatalf("insert: %+v, %v", resp, err)
	}
	var logged bytes.Buffer
	s.logger = slog.New(slog.NewTextHandler(&logged, nil))
	key := rawDocumentKey(t, s, "app", "held", "alpha")

	kv.FailNextReadOf(key)
	_, err := s.Edge("app", "held", false)
	if err == nil {
		t.Fatal("the armed read did not fail the first() call")
	}
	requireKeyOutOfClientText(t, explainError(err), key)
	if !strings.Contains(logged.String(), hex.EncodeToString(key)) {
		t.Errorf("the node log does not carry the key; log: %q", logged.String())
	}
}

// TestFieldGetReadFailureOmitsStorageKey is the field-store element read.
func TestFieldGetReadFailureOmitsStorageKey(t *testing.T) {
	dataDir := t.TempDir()
	kv := kvfault.New()
	s := newKVFaultServer(dataDir, kv)
	createWithStores(t, s, "app", "Post", BucketTypeHash, StoreDeclaration{Name: "Tags", Engine: "hashStore"})
	if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"title": "p1"}}); err != nil || !resp.Success {
		t.Fatalf("insert: %+v, %v", resp, err)
	}
	fieldReq := FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Tags", Element: "zz-unique-element"}
	put := fieldReq
	put.Data = map[string]interface{}{"n": 1}
	fieldOK(t)(s.FieldPutCtx(context.Background(), put))
	var logged bytes.Buffer
	s.logger = slog.New(slog.NewTextHandler(&logged, nil))
	key := keyContaining(t, kv, []byte("zz-unique-element"))

	kv.FailNextReadOf(key)
	_, err := s.FieldGet(fieldReq)
	if err == nil {
		t.Fatal("the armed element read did not fail")
	}
	requireKeyOutOfClientText(t, explainError(err), key)
	if !strings.Contains(logged.String(), hex.EncodeToString(key)) {
		t.Errorf("the node log does not carry the key; log: %q", logged.String())
	}
}

// TestFieldGetDamagedKeyOmitsStorageKey: BUG-0607. A field-store read over a
// stored key that is already damaged answers with no hex of the key, and the
// node log carries it. The verb is list rather than get: a get addresses an
// element by its key and never decodes a stored key, while list walks the
// range and decodes every element's remainder, which is the decoder the bug is
// about.
func TestFieldGetDamagedKeyOmitsStorageKey(t *testing.T) {
	dataDir := t.TempDir()
	kv := kvfault.New()
	s := newKVFaultServer(dataDir, kv)
	createWithStores(t, s, "app", "Post", BucketTypeHash, StoreDeclaration{Name: "Tags", Engine: "hashStore"})
	if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"title": "p1"}}); err != nil || !resp.Success {
		t.Fatalf("insert: %+v, %v", resp, err)
	}
	fieldReq := FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Tags", Element: "zz-damaged-element"}
	put := fieldReq
	put.Data = map[string]interface{}{"n": 1}
	fieldOK(t)(s.FieldPutCtx(context.Background(), put))

	// The element's remainder is its encoded key. Replace it with bytes that
	// are not an encoded string: the damage a corrupt or foreign writer can
	// leave under a field-store prefix.
	original := keyContaining(t, kv, []byte("zz-damaged-element"))
	encodedElement, err := store.EncodeComponent("zz-damaged-element")
	if err != nil {
		t.Fatalf("encoding the element key: %v", err)
	}
	malformed := []byte{0xaa, 0xbb, 0xcc}
	damaged := append(append([]byte(nil), original[:len(original)-len(encodedElement)]...), malformed...)
	batch := kv.NewBatch()
	if err := batch.Delete(original); err != nil {
		t.Fatalf("deleting the valid element key: %v", err)
	}
	if err := batch.Set(damaged, []byte("v")); err != nil {
		t.Fatalf("setting the damaged key: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing the damaged key: %v", err)
	}

	var logged bytes.Buffer
	s.logger = slog.New(slog.NewTextHandler(&logged, nil))
	_, readErr := s.FieldList(FieldListRequest{FieldRequest: fieldReq})
	if readErr == nil {
		t.Fatal("the damaged stored key did not fail the list read")
	}
	requireKeyOutOfClientText(t, explainError(readErr), malformed)
	requireKeyOutOfClientText(t, explainError(readErr), damaged)
	if !strings.Contains(logged.String(), hex.EncodeToString(damaged)) {
		t.Errorf("the node log does not carry the key; log: %q", logged.String())
	}
}
