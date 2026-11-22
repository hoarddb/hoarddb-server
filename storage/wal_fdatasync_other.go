//go:build !linux

package storage

import "os"

// fdatasyncFile is (*os.File).Sync off Linux, which is F_FULLFSYNC on darwin:
// the strongest flush the platform offers.
func fdatasyncFile(file *os.File) error {
	return file.Sync()
}
