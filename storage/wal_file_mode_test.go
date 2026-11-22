package storage

import (
	"os"
	"path/filepath"
	"testing"
)

// TestWALDirSegmentAndStateFileModes: the WAL directory, its
// active segment file, and its incarnation chain must match the F13 hygiene
// rule's 0600/0700 house pattern (auth.token.key, users.json), not the process
// umask default (0755 for the directory, 0644 for the files) OpenWAL and the
// state file it no longer keeps used to leave behind. The directory is created fresh under t.TempDir() so
// the assertion exercises OpenWAL's own os.MkdirAll mode, not the mode
// t.TempDir() itself already sets.
func TestWALDirSegmentAndStateFileModes(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")

	w, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	appendTestEntries(t, w, 1)
	segPath := activeSegmentPath(t, dir, w)

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	dirInfo, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat WAL directory: %v", err)
	}
	if perm := dirInfo.Mode().Perm(); perm != 0o700 {
		t.Fatalf("WAL directory %s permissions = %o, want 0700", dir, perm)
	}

	segInfo, err := os.Stat(segPath)
	if err != nil {
		t.Fatalf("stat active segment: %v", err)
	}
	if perm := segInfo.Mode().Perm(); perm != 0o600 {
		t.Fatalf("WAL segment %s permissions = %o, want 0600", segPath, perm)
	}

	chainPath := filepath.Join(dir, walIncarnationFile)
	chainInfo, err := os.Stat(chainPath)
	if err != nil {
		t.Fatalf("stat WAL incarnation chain: %v", err)
	}
	if perm := chainInfo.Mode().Perm(); perm != 0o600 {
		t.Fatalf("WAL incarnation chain %s permissions = %o, want 0600", chainPath, perm)
	}
}
