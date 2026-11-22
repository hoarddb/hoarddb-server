//go:build unix

package transport

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"syscall"
)

// openNoFollow opens path without following a link in its last component,
// and without blocking on a FIFO planted in its place.
func openNoFollow(path string, flag int, perm os.FileMode) (*os.File, error) {
	file, err := os.OpenFile(path, flag|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, perm)
	if errors.Is(err, syscall.ELOOP) {
		return nil, fmt.Errorf("%s: %w", path, errSymbolicLink)
	}
	return file, err
}

// lockExclusive takes flock(LOCK_EX), blocking until the lock is granted.
// The wait is queued in the kernel, so acquisitions follow arrival order. A
// signal interrupting the wait (EINTR) is not a failure of the lock, so the
// wait resumes.
func lockExclusive(file *os.File) error {
	for {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			return fmt.Errorf("flock %s: %w", file.Name(), err)
		}
		return nil
	}
}

// trustPathFacts reads the owner, type and permission bits from info.
func trustPathFacts(path string, info fs.FileInfo) (TrustPathFacts, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return TrustPathFacts{}, fmt.Errorf("checking %s: the file system reports no owner for it", path)
	}
	return TrustPathFacts{
		Path:       path,
		Type:       info.Mode().Type(),
		Perm:       info.Mode().Perm(),
		OwnerUID:   stat.Uid,
		CurrentUID: uint32(os.Geteuid()), //nolint:gosec // a uid is never negative on unix
	}, nil
}
