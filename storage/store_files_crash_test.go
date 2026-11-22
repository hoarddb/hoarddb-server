package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

// copyStoreFiles copies every regular file of a store directory, the way a
// crash leaves a running store: whatever was written, memtable log included.
// A memtable log that a flush removes between the listing and the read is
// skipped: a crash image taken a moment later would not hold it either, and
// the store removes it on purpose.
//
// The files are copied one by one, so a flush or a compaction that finishes
// during the copy leaves an image no crash can: a MANIFEST copied last that
// names a table written after the listing, or a table removed before its turn
// (the BUG-0880 family; the store rightly refuses such an image, "its MANIFEST
// names a table file the directory does not hold"). The copy is therefore
// repeated until the directory's tables and its MANIFEST were the same before
// and after it. A directory nothing writes to is copied once, as it is. Any
// other file that vanishes while the tables and the MANIFEST stood still is a
// failure, because an image without it is not one a crash can leave.
func copyStoreFiles(t *testing.T, fromDir, toDir string) {
	t.Helper()
	if err := os.MkdirAll(toDir, 0o700); err != nil {
		t.Fatalf("creating %s: %v", toDir, err)
	}
	deadline := time.Now().Add(90 * time.Second)
	for {
		tablesBefore, manifestBefore := storeTableNames(t, fromDir), storeManifest(t, fromDir)
		copied, vanished := copyStoreFilesOnce(t, fromDir, toDir)
		var tablesCopied []string
		for _, name := range copied {
			if strings.HasSuffix(name, ".sst") {
				tablesCopied = append(tablesCopied, name)
			}
		}
		manifestCopied, err := os.ReadFile(filepath.Join(toDir, badger.ManifestFilename))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("reading the copied MANIFEST: %v", err)
		}
		stoodStill := slices.Equal(tablesBefore, tablesCopied) && slices.Equal(tablesBefore, storeTableNames(t, fromDir)) &&
			bytes.Equal(manifestBefore, manifestCopied) && !strings.HasSuffix(vanished, ".sst")
		if stoodStill && vanished == "" {
			return
		}
		if stoodStill {
			t.Fatalf("reading %s: it vanished during the copy, and neither the tables nor the MANIFEST of %s changed", vanished, fromDir)
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("SCENARIO NOT REACHED: the tables or the MANIFEST of %s changed during every copy for 90 s", fromDir)
		}
		for _, name := range copied {
			if err := os.Remove(filepath.Join(toDir, name)); err != nil {
				t.Fatalf("removing %s of a copy the store changed under: %v", name, err)
			}
		}
	}
}

// copyStoreFilesOnce is one pass of copyStoreFiles. It returns the names it
// copied, sorted, and the first file other than a memtable log that was listed
// and gone when its turn came ("" when none was).
func copyStoreFilesOnce(t *testing.T, fromDir, toDir string) (copied []string, vanished string) {
	t.Helper()
	dirEntries, err := os.ReadDir(fromDir)
	if err != nil {
		t.Fatalf("listing %s: %v", fromDir, err)
	}
	for _, dirEntry := range dirEntries {
		if !dirEntry.Type().IsRegular() {
			continue
		}
		fileBytes, err := os.ReadFile(filepath.Join(fromDir, dirEntry.Name()))
		if errors.Is(err, os.ErrNotExist) {
			if !strings.HasSuffix(dirEntry.Name(), ".mem") && vanished == "" {
				vanished = dirEntry.Name()
			}
			continue
		}
		if err != nil {
			t.Fatalf("reading %s: %v", dirEntry.Name(), err)
		}
		if err := os.WriteFile(filepath.Join(toDir, dirEntry.Name()), fileBytes, 0o600); err != nil {
			t.Fatalf("writing %s: %v", dirEntry.Name(), err)
		}
		copied = append(copied, dirEntry.Name())
	}
	return copied, vanished
}

// storeTableNames lists the table files of a store directory, sorted.
func storeTableNames(t *testing.T, dir string) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil {
		t.Fatalf("listing the tables of %s: %v", dir, err)
	}
	for i, path := range paths {
		paths[i] = filepath.Base(path)
	}
	sort.Strings(paths)
	return paths
}

// storeManifest reads the MANIFEST of a store directory, nil when it has none.
func storeManifest(t *testing.T, dir string) []byte {
	t.Helper()
	manifest, err := os.ReadFile(filepath.Join(dir, badger.ManifestFilename))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reading the MANIFEST of %s: %v", dir, err)
	}
	return manifest
}

// writeFirstFlushCrashStore rebuilds what a kill during the very first flush
// leaves: 000001.sst written, the manifest still without a record for it, and
// the memtable log holding every document.
func writeFirstFlushCrashStore(t *testing.T, encryptionKey []byte) (dir, tableName string) {
	t.Helper()
	flushedDir := writeClosedStore(t, encryptionKey)
	tableName = filepath.Base(tableFiles(t, flushedDir)[0])

	runningDir := filepath.Join(t.TempDir(), "running")
	kv, err := OpenBadgerKV(runningDir, BadgerTuning{MemTableMB: 32}, encryptionKey)
	if err != nil {
		t.Fatalf("opening the store: %v", err)
	}
	batch := kv.NewBatch()
	for index := 0; index < corruptDocumentCount; index++ {
		if err := batch.Set(corruptKey(index), corruptValue(index)); err != nil {
			t.Fatalf("setting document %d: %v", index, err)
		}
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing: %v", err)
	}
	dir = filepath.Join(t.TempDir(), "crashed")
	copyStoreFiles(t, runningDir, dir)
	if err := kv.Close(); err != nil {
		t.Fatalf("closing the running store: %v", err)
	}

	tableBytes, err := os.ReadFile(filepath.Join(flushedDir, tableName))
	if err != nil {
		t.Fatalf("reading the flushed table: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, tableName), tableBytes, 0o600); err != nil {
		t.Fatalf("writing the leftover table: %v", err)
	}
	if memLogs, _ := filepath.Glob(filepath.Join(dir, "*.mem")); len(memLogs) == 0 {
		t.Fatal("the crashed store has no memtable log")
	}
	return dir, tableName
}
