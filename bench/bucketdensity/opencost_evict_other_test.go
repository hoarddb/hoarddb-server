//go:build !linux

package main

import (
	"strings"
	"testing"
)

// TestEvictFileFromPageCacheRefusesOffLinux is the darwin-only guard: the
// linux build must evict, but every other platform must fail loudly rather
// than report a warm cache as cold (BUG-0536).
func TestEvictFileFromPageCacheRefusesOffLinux(t *testing.T) {
	err := evictFileFromPageCache("/tmp/hoarddb-not-a-real-file")
	if err == nil {
		t.Fatal("evictFileFromPageCache returned no error off linux")
	}
	if got := err.Error(); !strings.Contains(got, "linux") {
		t.Errorf("error %q does not name linux", got)
	}
}
