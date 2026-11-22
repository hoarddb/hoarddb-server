package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestTheDirectoryGuardNamesARegularFileInPlaceOfTheDirectory defends
// BUG-0568(b): os.SameFile is false for a regular file exactly as for another
// directory, and the refusal named "another directory" either way. It must say
// what the stat actually found.
func TestTheDirectoryGuardNamesARegularFileInPlaceOfTheDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("creating the store directory: %v", err)
	}
	guard, err := newDirGuard(dir)
	if err != nil {
		t.Fatalf("newDirGuard: %v", err)
	}

	if err := os.Rename(dir, dir+".moved"); err != nil {
		t.Fatalf("moving the store directory away: %v", err)
	}
	if err := os.WriteFile(dir, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("writing a regular file in its place: %v", err)
	}

	refusal := guard.check()
	if refusal == nil {
		t.Fatalf("the guard accepted a regular file in place of the directory")
	}
	if !strings.Contains(refusal.Error(), "a regular file is in its place") {
		t.Errorf("the refusal does not name the regular file: %v", refusal)
	}
}
