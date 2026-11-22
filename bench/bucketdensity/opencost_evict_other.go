//go:build !linux

package main

import (
	"fmt"
	"runtime"
)

// evictFileFromPageCache refuses to run off linux: cold-open measurement needs
// posix_fadvise (a linux syscall) and /proc/<pid>/io to verify the eviction
// actually reached the device, neither of which exists on this platform.
// Returning an error keeps the harness visible to `go vet` on darwin instead
// of silently measuring a warm cache (BUG-0536).
func evictFileFromPageCache(path string) error {
	return fmt.Errorf("evict %s from the page cache: the bucket-density harness needs linux (posix_fadvise and /proc), not %s", path, runtime.GOOS)
}
