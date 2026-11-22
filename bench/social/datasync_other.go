//go:build !linux

package main

import "os"

// datasyncFile falls back to a full flush: syscall.Fdatasync exists on Linux
// only. The sync probe measures the Linux test VMs, so this variant exists to
// keep the package compiling and vetting on the other platforms the build
// matrix covers; its figure is not comparable with the Linux one.
func datasyncFile(file *os.File) error {
	return file.Sync()
}
