//go:build linux

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestEvictFileFromPageCacheActuallyEvicts is the guard that matters: the whole
// cold-open measurement is a lie if POSIX_FADV_DONTNEED is a no-op here. It is
// a no-op on tmpfs (there is no page cache to drop, the file *is* the cache),
// so the test refuses to run there rather than passing vacuously.
func TestEvictFileFromPageCacheActuallyEvicts(t *testing.T) {
	dir := t.TempDir()
	fsType, err := filesystemFor(dir)
	if err != nil {
		t.Fatalf("identifying the filesystem under %q: %v", dir, err)
	}
	if fsType == "tmpfs" || fsType == "ramfs" {
		t.Skipf("TMPDIR is on %s, where page-cache eviction is meaningless; "+
			"re-run with TMPDIR pointed at a real disk to exercise this", fsType)
	}

	path := filepath.Join(dir, "evictable")
	const fileBytes = 8 << 20
	if err := os.WriteFile(path, make([]byte, fileBytes), 0o600); err != nil {
		t.Fatalf("writing the test file: %v", err)
	}
	// Without this the test fails, and that failure is the point: fadvise keeps
	// dirty pages, so a file that has only just been written is not evictable
	// at all. The measurement syncs the data directory for the same reason.
	if err := syncTree(dir); err != nil {
		t.Fatalf("syncing the test file before eviction: %v", err)
	}

	warm, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("warming the page cache: %v", err)
	}
	if len(warm) != fileBytes {
		t.Fatalf("read %d bytes, want %d", len(warm), fileBytes)
	}

	beforeEvict, err := readProcessIO(os.Getpid())
	if err != nil {
		t.Fatalf("reading /proc/self/io: %v", err)
	}
	if _, err := os.ReadFile(path); err != nil {
		t.Fatalf("re-reading the warm file: %v", err)
	}
	afterWarmRead, err := readProcessIO(os.Getpid())
	if err != nil {
		t.Fatalf("reading /proc/self/io: %v", err)
	}
	warmDeviceBytes := afterWarmRead.ReadBytes - beforeEvict.ReadBytes

	if err := evictFileFromPageCache(path); err != nil {
		t.Fatalf("evicting %q: %v", path, err)
	}
	if _, err := os.ReadFile(path); err != nil {
		t.Fatalf("re-reading the evicted file: %v", err)
	}
	afterColdRead, err := readProcessIO(os.Getpid())
	if err != nil {
		t.Fatalf("reading /proc/self/io: %v", err)
	}
	coldDeviceBytes := afterColdRead.ReadBytes - afterWarmRead.ReadBytes

	if coldDeviceBytes <= warmDeviceBytes {
		t.Fatalf("eviction did not reach the device: a warm re-read moved %d bytes and the "+
			"post-eviction read moved %d. POSIX_FADV_DONTNEED is not working on %s, so every "+
			"cold-open number this harness reports would be a warm one",
			warmDeviceBytes, coldDeviceBytes, fsType)
	}
}
