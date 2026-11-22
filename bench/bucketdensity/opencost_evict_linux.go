//go:build linux

package main

import (
	"fmt"
	"os"
	"syscall"
)

// posixFadvDontNeed is POSIX_FADV_DONTNEED. It is not in the syscall package,
// which exposes the fadvise64 syscall number but none of its advice constants.
const posixFadvDontNeed = 4

// evictFileFromPageCache asks the kernel to drop this file's clean page-cache
// pages, so the next read of it reaches the device.
//
// This is the only way to measure a *cold* open without root: dropping the
// whole page cache needs /proc/sys/vm/drop_caches, which is root-only, and
// unmounting the filesystem under the data directory is not available either.
// POSIX_FADV_DONTNEED is advisory — dirty pages are kept, and on tmpfs it does
// nothing at all — so the harness refuses tmpfs and verifies the effect from
// /proc/<pid>/io read_bytes rather than assuming it.
func evictFileFromPageCache(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening %q to evict it from the page cache: %w", path, err)
	}
	defer func() { _ = file.Close() }()

	// Length 0 means "from the offset to end of file".
	_, _, errno := syscall.Syscall6(syscall.SYS_FADVISE64,
		file.Fd(), 0, 0, posixFadvDontNeed, 0, 0)
	if errno != 0 {
		return fmt.Errorf("fadvise(POSIX_FADV_DONTNEED) on %q: %w", path, errno)
	}
	return nil
}
