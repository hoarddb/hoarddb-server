package store

import (
	"bytes"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// fieldDecodeLeak fails a test when message carries key's hex: the decoder of
// a damaged field-store key must not put that key into its text. The caller
// passes the fragment the old wording printed, which is a piece of the stored
// key, not the whole key.
func fieldDecodeLeak(t *testing.T, message string, key []byte) {
	t.Helper()
	if len(key) > 0 && strings.Contains(message, hex.EncodeToString(key)) {
		t.Errorf("the text carries the damaged key as hex: %s", message)
	}
}

// requireFieldKeyError fails a test unless err carries key in a
// *storage.KeyError, which logKeyReadFailure logs for the node operator while
// the text stays free of the encoding.
func requireFieldKeyError(t *testing.T, err error, key []byte) {
	t.Helper()
	var keyErr *storage.KeyError
	if !errors.As(err, &keyErr) {
		t.Fatalf("the error does not carry the key for the node log: %v", err)
	}
	if !bytes.Equal(keyErr.Key, key) {
		t.Errorf("the logged key is %x, want %x", keyErr.Key, key)
	}
}

// putRawFieldKey writes key directly into the node store, the way damage or a
// foreign writer does, so the decoders see a key their engine never wrote.
func putRawFieldKey(t *testing.T, kv storage.KV, key, value []byte) {
	t.Helper()
	batch := kv.NewBatch()
	if err := batch.Set(key, value); err != nil {
		t.Fatalf("setting the damaged key %x: %v", key, err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing the damaged key %x: %v", key, err)
	}
}

// TestFieldKeyDecodeErrorsKeepTheKeyOutOfTheText: BUG-0607. Every field-store
// decoder that a client verb reaches over a damaged stored key puts the key
// into a *storage.KeyError for the node log, and never into the error text a
// server sends to a client (BUG-0591's split, applied to the field-store
// decoders it left for a follow-up).
func TestFieldKeyDecodeErrorsKeepTheKeyOutOfTheText(t *testing.T) {
	t.Run("list decodes a queue remainder", func(t *testing.T) {
		p, _ := newFaultFieldParent(t)
		ref := p.ref("doc", 1, FieldFIFO)
		prefix := FieldStorePrefix(ref.Owner, 1)
		remainder := []byte{0x11, 0x22, 0x33, 0x44}
		damaged := FieldElementKey(prefix, storage.NSQueue, remainder)
		putRawFieldKey(t, p.kv, damaged, []byte("v"))

		snap := p.kv.NewSnapshot()
		defer snap.Discard()
		_, _, err := FieldList(snap, ref, FieldListRequest{Limit: 10, MaxBytes: 1 << 20})
		if err == nil {
			t.Fatal("FieldList decoded a damaged position remainder without an error")
		}
		fieldDecodeLeak(t, err.Error(), remainder)
		requireFieldKeyError(t, err, damaged)
	})

	t.Run("list decodes a keyed remainder", func(t *testing.T) {
		p, _ := newFaultFieldParent(t)
		ref := p.ref("doc", 1, FieldHash)
		prefix := FieldStorePrefix(ref.Owner, 1)
		remainder := []byte{0xaa, 0xbb, 0xcc}
		damaged := FieldElementKey(prefix, storage.NSDocuments, remainder)
		putRawFieldKey(t, p.kv, damaged, []byte("v"))

		snap := p.kv.NewSnapshot()
		defer snap.Discard()
		_, _, err := FieldList(snap, ref, FieldListRequest{Limit: 10, MaxBytes: 1 << 20})
		if err == nil {
			t.Fatal("FieldList decoded a damaged element key without an error")
		}
		fieldDecodeLeak(t, err.Error(), remainder)
		requireFieldKeyError(t, err, damaged)
	})

	t.Run("walk parses a short suffix", func(t *testing.T) {
		p, _ := newFaultFieldParent(t)
		owner, err := EncodeComponent("doc")
		if err != nil {
			t.Fatalf("encoding the owner: %v", err)
		}
		suffix := []byte{0x00, 0x01}
		damaged := storage.Key(fieldTestSlot, 1, fieldTestID, storage.NSFieldStore, owner, suffix)
		putRawFieldKey(t, p.kv, damaged, []byte("v"))

		snap := p.kv.NewSnapshot()
		defer snap.Discard()
		err = WalkFieldElements(snap, storage.BucketRef{Slot: fieldTestSlot, DatabaseID: 1, BucketID: fieldTestID}, FieldLayout{1: FieldFIFO},
			func(FieldElementRecord) error { return nil })
		if err == nil {
			t.Fatal("WalkFieldElements parsed a short suffix without an error")
		}
		fieldDecodeLeak(t, err.Error(), suffix)
		requireFieldKeyError(t, err, damaged)
	})

	t.Run("walk measures an L3 remainder", func(t *testing.T) {
		p, _ := newFaultFieldParent(t)
		owner, err := EncodeComponent("doc")
		if err != nil {
			t.Fatalf("encoding the owner: %v", err)
		}
		remainder := []byte{0xaa, 0xbb, 0xcc}
		suffix := append([]byte{0x00, 0x01, storage.NSFieldStore}, remainder...)
		damaged := storage.Key(fieldTestSlot, 1, fieldTestID, storage.NSFieldStore, owner, suffix)
		putRawFieldKey(t, p.kv, damaged, []byte("v"))

		snap := p.kv.NewSnapshot()
		defer snap.Discard()
		err = WalkFieldElements(snap, storage.BucketRef{Slot: fieldTestSlot, DatabaseID: 1, BucketID: fieldTestID}, FieldLayout{1: FieldFIFO},
			func(FieldElementRecord) error { return nil })
		if err == nil {
			t.Fatal("WalkFieldElements measured a short L3 remainder without an error")
		}
		fieldDecodeLeak(t, err.Error(), remainder)
		requireFieldKeyError(t, err, damaged)
	})

	t.Run("delete cascade reads the leaf", func(t *testing.T) {
		p, _ := newFaultFieldParent(t)
		ref := p.ref("doc", 1, FieldBlob)
		suffix := []byte{0x00, 0x01}
		damaged := append(bytes.Clone(ref.Owner), suffix...)
		putRawFieldKey(t, p.kv, damaged, []byte("v"))

		snap := p.kv.NewSnapshot()
		defer snap.Discard()
		batch := p.kv.NewBatch()
		_, err := deleteFieldStores(FieldWrite{Snap: snap, State: &BucketState{}, Batch: batch}, ref.Owner, FieldLayout{1: FieldBlob})
		if err == nil {
			t.Fatal("deleteFieldStores read a short leaf suffix without an error")
		}
		fieldDecodeLeak(t, err.Error(), suffix)
		requireFieldKeyError(t, err, damaged)
	})

	t.Run("instance state value", func(t *testing.T) {
		key := []byte("field-state-key")
		_, err := DecodeFieldInstanceState(key, []byte{0x00})
		if err == nil {
			t.Fatal("DecodeFieldInstanceState accepted a short value")
		}
		fieldDecodeLeak(t, err.Error(), key)
		requireFieldKeyError(t, err, key)
	})
}
