package store

import (
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// TestIndexEntryErrorsKeepTheKeyOutOfTheText: BUG-0591. An index entry that
// cannot be read names its storage key in the error's KeyError, for the node
// log, and never in the text a server sends to a client.
func TestIndexEntryErrorsKeepTheKeyOutOfTheText(t *testing.T) {
	kv := kvfault.New()
	idx := NewSecondaryIndex(kv, 1, 1, 7, 1)
	batch := kv.NewBatch()
	// A bool component is two bytes; one byte is a damaged entry.
	if err := idx.Add(batch, []byte{tagBool}, nil); err != nil {
		t.Fatalf("adding the damaged entry: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing: %v", err)
	}
	damagedKey := idx.makeKey([]byte{tagBool}, nil)

	reads := map[string]func() error{
		"FindPrefix": func() error { _, err := idx.FindPrefix(nil, 1); return err },
		"ScanBracket": func() error {
			return idx.ScanBracket(tagBool, []byte{tagBool, 0xff}, func([]byte) error { return nil })
		},
		"FirstDuplicate": func() error { _, _, _, err := idx.FirstDuplicate(1); return err },
	}
	for name, read := range reads {
		t.Run(name, func(t *testing.T) {
			err := read()
			if err == nil {
				t.Fatal("a damaged index entry read without an error")
			}
			if strings.Contains(err.Error(), hex.EncodeToString(damagedKey)) {
				t.Errorf("the text carries the storage key as hex: %v", err)
			}
			var keyErr *storage.KeyError
			if !errors.As(err, &keyErr) || hex.EncodeToString(keyErr.Key) != hex.EncodeToString(damagedKey) {
				t.Errorf("the error does not carry the key for the log: %v", err)
			}
		})
	}
}
