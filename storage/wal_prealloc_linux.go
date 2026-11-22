//go:build linux

package storage

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// fallocateFile reserves size bytes of blocks for file (mode 0, which also
// sets the size), so a spare's zeros are written into blocks it already owns.
// Where fallocate is not supported the fill goes on: the zero writes allocate
// the blocks themselves. On ENOSPC ext4 keeps the blocks it managed to
// allocate (NR-1), so a caller that fails here must delete the file to give
// them back.
func fallocateFile(file *os.File, size int64) error {
	err := unix.Fallocate(int(file.Fd()), 0, 0, size)
	if err != nil && !fallocateUnsupported(err) {
		return fmt.Errorf("fallocate %d bytes for %s: %w", size, file.Name(), err)
	}
	return nil
}

// fallocateUnsupported reports whether a fallocate error means the call is not
// available rather than that it failed: EOPNOTSUPP from a filesystem without
// it (ext3 without extents), ENOSYS from a kernel or a sandbox (seccomp, gVisor)
// without the syscall.
func fallocateUnsupported(err error) bool {
	return errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOSYS)
}

// freeBytesAt is the bytes an unprivileged writer can still use on the
// filesystem holding dirPath: statfs's Bavail times its block size, the count
// that excludes the root reserve. Statfs_t's fields are narrower than uint64
// on 32-bit targets, so each is converted.
func freeBytesAt(dirPath string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dirPath, &stat); err != nil {
		return 0, fmt.Errorf("statfs %s: %w", dirPath, err)
	}
	return uint64(stat.Bavail) * uint64(stat.Bsize), nil
}
