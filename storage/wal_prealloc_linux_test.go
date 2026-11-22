//go:build linux

package storage

import (
	"fmt"
	"testing"

	"golang.org/x/sys/unix"
)

// TestWALFallocateUnsupportedIsNotAFailure: a filesystem without fallocate
// (EOPNOTSUPP) and a kernel or sandbox without the syscall (ENOSYS) both mean
// the call is unavailable, and the fill writes its zeros without it; every
// other error, ENOSPC first, fails the fill (S7 verifier ADV-1). Break by:
// dropping either errno, or ignoring ENOSPC.
func TestWALFallocateUnsupportedIsNotAFailure(t *testing.T) {
	for _, tc := range []struct {
		err         error
		unsupported bool
	}{
		{unix.EOPNOTSUPP, true},
		{unix.ENOSYS, true},
		{fmt.Errorf("fallocate: %w", unix.ENOSYS), true},
		{unix.ENOSPC, false},
		{unix.EIO, false},
		{unix.EBADF, false},
	} {
		if got := fallocateUnsupported(tc.err); got != tc.unsupported {
			t.Errorf("fallocateUnsupported(%v) = %v, want %v", tc.err, got, tc.unsupported)
		}
	}
}
