//go:build unix

package storage

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// fileUnderName returns a descriptor for the file f has open that names path,
// and closes f. A rotation opens the next segment while it is still
// wal_spare.tmp (R0), so that nothing after the outgoing segment is retired can
// fail for want of a descriptor; the segment is renamed later (R3), and every
// error a write or a flush on it reports must name the segment, not the spare it
// was born as. The duplicate is close-on-exec, as every descriptor Go opens is.
func fileUnderName(f *os.File, path string) (*os.File, error) {
	raw, err := f.SyscallConn()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("reaching the descriptor of %s: %w", f.Name(), err), f.Close())
	}
	var duplicate int
	var dupErr error
	if err := raw.Control(func(fd uintptr) {
		duplicate, dupErr = unix.FcntlInt(fd, unix.F_DUPFD_CLOEXEC, 0)
	}); err != nil {
		return nil, errors.Join(fmt.Errorf("reaching the descriptor of %s: %w", f.Name(), err), f.Close())
	}
	if dupErr != nil {
		return nil, errors.Join(fmt.Errorf("duplicating the descriptor of %s: %w", f.Name(), dupErr), f.Close())
	}
	named := os.NewFile(uintptr(duplicate), path)
	if err := f.Close(); err != nil {
		return nil, errors.Join(fmt.Errorf("closing %s once it is open as %s: %w", f.Name(), path, err), named.Close())
	}
	return named, nil
}
