//go:build unix

package server

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// withUmask sets the process umask for one test and restores it. The umask is
// process-global, so these tests must not run in parallel.
func withUmask(t *testing.T, mask int) {
	t.Helper()
	original := syscall.Umask(mask)
	t.Cleanup(func() { syscall.Umask(original) })
}

// TestRestrictUmaskTightensALooseUmask: under umask 0002 a file asked for at
// 0666 used to land at 0664 and a directory asked for at 0777 at 0775; after
// RestrictUmask neither is readable by group or other.
func TestRestrictUmaskTightensALooseUmask(t *testing.T) {
	withUmask(t, 0o002)
	change := RestrictUmask()
	if change.Previous != 0o002 || change.Applied != 0o077 {
		t.Errorf("RestrictUmask = %+v, want previous 0002 applied 0077", change)
	}
	dir := t.TempDir()
	filePath := filepath.Join(dir, "file")
	if err := os.WriteFile(filePath, nil, 0o666); err != nil {
		t.Fatalf("creating a file: %v", err)
	}
	subdir := filepath.Join(dir, "subdir")
	if err := os.Mkdir(subdir, 0o777); err != nil {
		t.Fatalf("creating a directory: %v", err)
	}
	for _, path := range []string{filePath, subdir} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		if info.Mode().Perm()&0o077 != 0 {
			t.Errorf("%s has mode %o, want no group or other bits", path, info.Mode().Perm())
		}
	}
}

// TestRestrictUmaskNeverLoosens: an operator who already runs under a stricter
// umask (0277) keeps it; the server only ever adds bits to the mask.
func TestRestrictUmaskNeverLoosens(t *testing.T) {
	withUmask(t, 0o277)
	change := RestrictUmask()
	if change.Previous != 0o277 || change.Applied != 0o277 {
		t.Errorf("RestrictUmask = %+v, want previous and applied both 0277", change)
	}
}

// TestRestrictedUmaskReachesBadgerFiles: the store's own *.vlog and *.mem files
// were 0664 under umask 0002 because Badger creates them with 0666.
func TestRestrictedUmaskReachesBadgerFiles(t *testing.T) {
	withUmask(t, 0o002)
	RestrictUmask()
	dataDir := t.TempDir()
	kv, _, err := openNodeStore(encryptionTestConfig(dataDir, "off", "off"), nil)
	if err != nil {
		t.Fatalf("opening the node store: %v", err)
	}
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Errorf("closing the node store: %v", err)
		}
	})
	storeFiles, err := filepath.Glob(filepath.Join(dataDir, nodeStoreDirName, "*"))
	if err != nil || len(storeFiles) == 0 {
		t.Fatalf("listing the store: %v (%d files)", err, len(storeFiles))
	}
	for _, path := range storeFiles {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		if info.Mode().Perm()&0o077 != 0 {
			t.Errorf("%s has mode %o, want owner-only", path, info.Mode().Perm())
		}
	}
}
