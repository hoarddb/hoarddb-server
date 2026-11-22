//go:build linux

package storage

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// renameNoReplace renames oldPath to newPath and refuses, with an error that
// matches fs.ErrExist, when newPath already exists. The kernel makes the check
// and the rename one step (renameat2 with RENAME_NOREPLACE). A filesystem that
// does not support the flag answers EINVAL, and a kernel without the call
// ENOSYS; both fall back to renameIfAbsent.
func renameNoReplace(oldPath, newPath string) error {
	err := unix.Renameat2(unix.AT_FDCWD, oldPath, unix.AT_FDCWD, newPath, unix.RENAME_NOREPLACE)
	if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOSYS) {
		return renameIfAbsent(oldPath, newPath)
	}
	if err != nil {
		return &os.LinkError{Op: "renameat2", Old: oldPath, New: newPath, Err: err}
	}
	return nil
}
