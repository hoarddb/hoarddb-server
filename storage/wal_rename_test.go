package storage

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// TestWALRenameIfAbsentRefusesAnExistingName: the fallback a filesystem
// without RENAME_NOREPLACE takes (and every other OS) refuses an existing
// target too, and renames when there is none. Break by: renaming without the
// existence check.
func TestWALRenameIfAbsentRefusesAnExistingName(t *testing.T) {
	dir := t.TempDir()
	from, to := filepath.Join(dir, "from"), filepath.Join(dir, "to")
	if err := os.WriteFile(from, []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(to, []byte("kept"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := renameIfAbsent(from, to); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("renameIfAbsent over an existing file = %v, want ErrExist", err)
	}
	if kept, err := os.ReadFile(to); err != nil || string(kept) != "kept" {
		t.Fatalf("the existing file now holds %q (%v), want it untouched", kept, err)
	}
	if err := os.Remove(to); err != nil {
		t.Fatal(err)
	}
	if err := renameIfAbsent(from, to); err != nil {
		t.Fatalf("renameIfAbsent to a free name: %v", err)
	}
	if moved, err := os.ReadFile(to); err != nil || string(moved) != "new" {
		t.Fatalf("after the rename the target holds %q (%v), want \"new\"", moved, err)
	}
}
