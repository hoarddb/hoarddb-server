//go:build linux

package storage

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// fdatasyncFile makes the file's data, and the metadata needed to read it back,
// durable without the cost of flushing timestamps.
func fdatasyncFile(file *os.File) error {
	if err := unix.Fdatasync(int(file.Fd())); err != nil {
		return fmt.Errorf("fdatasync %s: %w", file.Name(), err)
	}
	return nil
}
