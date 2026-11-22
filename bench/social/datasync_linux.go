//go:build linux

package main

import (
	"os"
	"syscall"
)

// datasyncFile flushes file's data, and only the metadata needed to read it
// back, as the database's own log sync does.
func datasyncFile(file *os.File) error {
	return syscall.Fdatasync(int(file.Fd()))
}
