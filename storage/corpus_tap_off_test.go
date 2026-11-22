//go:build !bsoncorpus

package storage_test

import (
	"os"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// In a normal build the tap is a constant-false branch: nothing is recorded
// even when the output directory is set, and the constant is false so the
// compiler removes the calls. That the calls vanish from the binary is shown
// by the build (step 1d's report), not by a test.
func TestCorpusTapIsOffWithoutTheTag(t *testing.T) {
	if storage.CorpusTapEnabled {
		t.Fatal("corpusTapEnabled is true in a build without the bsoncorpus tag")
	}

	outputDir := t.TempDir()
	t.Setenv("BSONCORPUS_OUT", outputDir)
	document := []byte{0x0c, 0, 0, 0, 0x10, 'a', 0, 5, 0, 0, 0, 0}

	kv, err := storage.OpenBadgerKV(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening the Badger store: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	batch := kv.NewBatch()
	if err := batch.Set(storage.Key(1, 1, 2, storage.NSDocuments, []byte("k")), document); err != nil {
		t.Fatalf("batch Set: %v", err)
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("batch Commit: %v", err)
	}
	instance, err := storage.OpenBadger(t.TempDir(), storage.BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("opening the Badger instance: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close() })
	if err := instance.Put([]byte("k"), document); err != nil {
		t.Fatalf("Put: %v", err)
	}

	written, err := os.ReadDir(outputDir)
	if err != nil {
		t.Fatalf("reading %s: %v", outputDir, err)
	}
	if len(written) != 0 {
		t.Errorf("a build without the tag wrote %d files to BSONCORPUS_OUT", len(written))
	}
}
