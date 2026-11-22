package storage_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// TestBatchRefusesAKeyOverMaxKeyBytes pins storage.MaxKeyBytes to Badger's own
// cap and the typed refusal (BUG-0181): a key of exactly the limit is accepted,
// one byte more is a KeyTooLargeError whose text carries no hex dump.
func TestBatchRefusesAKeyOverMaxKeyBytes(t *testing.T) {
	kv, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })

	batch := kv.NewBatch()
	if err := batch.Set([]byte(strings.Repeat("k", storage.MaxKeyBytes)), []byte("v")); err != nil {
		t.Fatalf("Set of a key of exactly MaxKeyBytes = %v, want nil", err)
	}
	overLimit := []byte(strings.Repeat("k", storage.MaxKeyBytes+1))
	refusal := batch.Set(overLimit, []byte("v"))
	var tooLarge *storage.KeyTooLargeError
	if !errors.As(refusal, &tooLarge) || !errors.Is(refusal, storage.ErrKeyTooLarge) {
		t.Fatalf("Set of a key one byte over MaxKeyBytes = %v, want a *KeyTooLargeError", refusal)
	}
	if tooLarge.Size != len(overLimit) || tooLarge.Limit != storage.MaxKeyBytes {
		t.Errorf("KeyTooLargeError = size %d limit %d, want %d and %d", tooLarge.Size, tooLarge.Limit, len(overLimit), storage.MaxKeyBytes)
	}
	if strings.Contains(refusal.Error(), "kkkk") {
		t.Errorf("Error() %q shows the key", refusal.Error())
	}
}
