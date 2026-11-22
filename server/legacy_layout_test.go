package server

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// makeLegacyEntry creates relPath under dataDir: a directory, or a file when
// the last element is key.wrapped.
func makeLegacyEntry(t *testing.T, dataDir, relPath string) {
	t.Helper()
	full := filepath.Join(dataDir, filepath.FromSlash(relPath))
	if filepath.Base(full) == "key.wrapped" {
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("creating %s: %v", filepath.Dir(full), err)
		}
		if err := os.WriteFile(full, []byte("wrapped"), 0o600); err != nil {
			t.Fatalf("writing %s: %v", full, err)
		}
		return
	}
	if err := os.MkdirAll(full, 0o755); err != nil {
		t.Fatalf("creating %s: %v", full, err)
	}
}

// topLevelNames lists the entries directly under dir.
func topLevelNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

// assertStartCreatedNothing holds the user page's promise that a refused start
// leaves the data directory as it found it: no keys, tokens or store.
func assertStartCreatedNothing(t *testing.T, dataDir string, before []string) {
	t.Helper()
	if after := topLevelNames(t, dataDir); !slices.Equal(before, after) {
		t.Errorf("the refused start changed %s: it held %v, now holds %v", dataDir, before, after)
	}
}

// wantLegacyLayoutRefusal is the whole refusal for a data directory in the
// per-bucket layout: the directory, what this release reads, that the start
// changed nothing, and the fix.
func wantLegacyLayoutRefusal(dataDir string) string {
	return "Data directory " + dataDir + " uses the per-bucket storage layout (catalog version 1), " +
		"and this release does not read that layout: it reads the shared layout (version 2). " +
		"Nothing in the directory was changed. " +
		"Start hoarddb-server with an empty data directory: set storage.data_dir (HOARDDB_DATA_DIR) to a new path, " +
		"or move this directory aside and keep it if you want it."
}

// TestLegacyLayoutRefusalSendsNobodyToAnotherBuild: no build but this one has
// been released, so the refusal names no other release, no dump taken with
// one, and no page about restoring it.
func TestLegacyLayoutRefusalSendsNobodyToAnotherBuild(t *testing.T) {
	message := (&legacyLayoutError{dataDir: "./data", version: 1}).Error()
	for _, removed := range []string{"previous release", "earlier", "dump", "restore", "user docs", "convert"} {
		if strings.Contains(message, removed) {
			t.Errorf("the refusal still says %q: %s", removed, message)
		}
	}
	if want := wantLegacyLayoutRefusal("./data"); message != want {
		t.Errorf("the refusal is\n  %s\nwant\n  %s", message, want)
	}
}

// TestLegacyBucketDirectoryRefusesStart is spec T-17, shape 2: a data directory
// that holds a legacy bucket directory and no catalog.json at all. The check
// the first shape relies on, the catalog file, sees nothing here, so without
// this one the new server would start beside the old directories and log a
// warning whose remedy no longer re-attaches anything.
func TestLegacyBucketDirectoryRefusesStart(t *testing.T) {
	for _, marker := range []string{"badger", "segments", "blobs", "indexes", "key.wrapped"} {
		t.Run(marker, func(t *testing.T) {
			dataDir := t.TempDir()
			makeLegacyEntry(t, dataDir, "app/users/"+marker)

			before := topLevelNames(t, dataDir)
			_, err := newServiceAt(t, dataDir)
			if err == nil {
				t.Fatalf("a data directory with app/users/%s and no catalog.json started", marker)
			}
			assertStartCreatedNothing(t, dataDir, before)
			if want := wantLegacyLayoutRefusal(dataDir); !strings.Contains(err.Error(), want) {
				t.Errorf("the refusal is\n  %v\nwant\n  %s", err, want)
			}
			if _, statErr := os.Stat(filepath.Join(dataDir, "store")); !os.IsNotExist(statErr) {
				t.Errorf("the refused start created the node store beside the legacy layout (stat: %v)", statErr)
			}
		})
	}
}

// TestSharedLayoutDirectoriesAreNotLegacy: the refusal looks at two levels,
// <database>/<bucket>/, and skips the node's own top-level directories. A
// directory named like a legacy marker anywhere else is not a legacy bucket.
func TestSharedLayoutDirectoriesAreNotLegacy(t *testing.T) {
	for _, notLegacy := range []string{
		"blobs/ab/badger",       // inside the node-wide blob tree
		"store/sub/badger",      // inside the node's store
		"keys/sub/key.wrapped",  // inside the node's keys
		"wal/sub/segments",      // inside the write-ahead log
		"app/badger",            // one level: a database holding a marker, no bucket
		"app/users/data/badger", // three levels: below the bucket's directory
	} {
		t.Run(notLegacy, func(t *testing.T) {
			dataDir := t.TempDir()
			makeLegacyEntry(t, dataDir, notLegacy)

			if _, err := newServiceAt(t, dataDir); err != nil {
				t.Fatalf("a shared-layout data directory with %s was refused: %v", notLegacy, err)
			}
		})
	}
}
