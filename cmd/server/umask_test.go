//go:build unix

package main

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
)

// TestServerFilesAreOwnerOnlyUnderALooseUmask drives the real binary from a
// shell whose umask is 0002 (the Ubuntu default for a user with a private
// group). Before the start-up umask, the data directory was 0775 and Badger's
// *.vlog, *.mem and DISCARD files were 0664; none of them may carry a group or
// other bit now.
func TestServerFilesAreOwnerOnlyUnderALooseUmask(t *testing.T) {
	binary := buildServerBinary(t)
	dataDir := filepath.Join(t.TempDir(), "data")
	environment := serverEnvironment(append(listenEnvironment(t), "HOARDDB_DATA_DIR="+dataDir)...)

	started := startServerBinary(t, "/bin/sh", t.TempDir(), environment,
		"-c", `umask 002; exec "$0" "$@"`, binary)
	started.waitForFile(t, server.UsersFilePath(dataDir))

	if walkErr := walkOwnerOnly(t, dataDir, nil); walkErr != nil {
		t.Fatalf("walking %s: %v", dataDir, walkErr)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "store")); err != nil {
		t.Fatalf("the node store is missing: %v", err)
	}
}

// walkOwnerOnly reports, through t, every entry under root with a group or
// other permission bit. The server keeps running during the walk and renames
// temporary files (.users-*.json) into place, so an entry can vanish between
// the directory listing and its stat; beforeStat, when set, runs just before
// each entry is stat-ed so a test can make that happen on demand.
func walkOwnerOnly(t *testing.T, root string, beforeStat func(path string)) error {
	t.Helper()
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if beforeStat != nil {
			beforeStat(path)
		}
		info, err := entry.Info()
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode().Perm()&0o077 != 0 {
			t.Errorf("%s has mode %o, want no group or other bits", path, info.Mode().Perm())
		}
		return nil
	})
}

// TestOwnerOnlyWalkToleratesAFileRenamedAwayMidWalk: a temporary
// file listed by the walk and gone by the time it is stat-ed is not a failure.
func TestOwnerOnlyWalkToleratesAFileRenamedAwayMidWalk(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatalf("restricting the walked directory: %v", err)
	}
	temporary := filepath.Join(root, ".users-123.json")
	if err := os.WriteFile(temporary, []byte("{}"), 0o600); err != nil {
		t.Fatalf("writing the temporary file: %v", err)
	}
	renamedAway := func(path string) {
		if path != temporary {
			return
		}
		if err := os.Remove(temporary); err != nil && !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("removing the temporary file: %v", err)
		}
	}
	if err := walkOwnerOnly(t, root, renamedAway); err != nil {
		t.Fatalf("a file that vanished mid-walk failed the walk: %v", err)
	}
}
