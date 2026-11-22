//go:build linux

package storage

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// lockStoreDirectory takes the advisory lock Badger takes on the store
// directory inside its own open (an exclusive, non-blocking flock on the
// directory), so a repair made before that open cannot touch a store another
// process is running. held is true when another process has it. The returned
// release must be called before Badger opens the directory.
func lockStoreDirectory(dir string) (release func(), held bool, err error) {
	dirFile, err := os.Open(dir)
	if err != nil {
		return nil, false, fmt.Errorf("opening %s to lock it: %w", dir, err)
	}
	if err := syscall.Flock(int(dirFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		closeErr := dirFile.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, true, closeErr
		}
		return nil, false, errors.Join(fmt.Errorf("locking %s: %w", dir, err), closeErr)
	}
	return func() { _ = dirFile.Close() }, false, nil // closing the descriptor drops the flock
}
