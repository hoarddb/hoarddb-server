package harness

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// TestCopyLiveStoreNeedsTheMarker: a ring store always holds its sync marker (this build writes it
// before Badger creates a file and refuses a store without one), so a directory without it is
// not one, and copying it would produce a store the restart refuses. The copy fails, naming the
// marker, instead of succeeding without it.
func TestCopyLiveStoreNeedsTheMarker(t *testing.T) {
	source := filepath.Join(t.TempDir(), "badger")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatalf("creating the source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "MANIFEST"), []byte("manifest"), 0o600); err != nil {
		t.Fatalf("writing a data file: %v", err)
	}
	err := copyLiveStore(source, filepath.Join(t.TempDir(), "copy"))
	if err == nil {
		t.Fatal("copyLiveStore copied a store that has no VOUCHED marker without an error")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the error does not carry the missing marker: %v", err)
	}

	// A real ring store: its marker is the open's format, two slots.
	source = filepath.Join(t.TempDir(), "badger")
	store, err := storage.OpenVouchedBadgerKV(source, storage.BadgerTuning{MemTableMB: 8})
	if err != nil {
		t.Fatalf("opening a ring store at the source: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("closing the source store: %v", err)
	}
	copyDir := filepath.Join(t.TempDir(), "copy")
	if err := copyLiveStore(source, copyDir); err != nil {
		t.Fatalf("copying a store with its marker: %v", err)
	}
	for _, name := range []string{liveStoreMarker, "MANIFEST"} {
		if _, err := os.Stat(filepath.Join(copyDir, name)); err != nil {
			t.Fatalf("the copy lacks %s: %v", name, err)
		}
	}

	// A marker the open would refuse (here 32 zero bytes, neither layout) is not a state a crash of
	// this build leaves: the copy fails rather than hand the harness a store that will not start
	// (store-vouched-cut review R-2 item 8).
	if err := os.WriteFile(filepath.Join(source, liveStoreMarker), make([]byte, 32), 0o600); err != nil {
		t.Fatalf("writing a marker of another layout: %v", err)
	}
	err = copyLiveStore(source, filepath.Join(t.TempDir(), "copy"))
	var damaged *storage.VouchedMarkerDamagedError
	if !errors.As(err, &damaged) {
		t.Fatalf("copyLiveStore copied a marker the open refuses: %v", err)
	}
}
