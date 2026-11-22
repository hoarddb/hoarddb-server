//go:build unix

package server

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// blobDirLock is a node's exclusive hold on its configured blob directory.
// The lock is flock(2) on an open file, the way Badger holds its own
// directory: it is released when the file closes, including when the process
// dies, so a crashed node never leaves a directory that refuses its restart.
type blobDirLock struct {
	file *os.File
}

// lockBlobDir takes the exclusive lock on dir, or explains that another
// running node holds it. Two nodes on one blob directory would delete each
// other's payloads: each node's orphan pass unlinks every payload file its
// own store does not own (reclaim.go:collectOrphan), and payload names carry
// node-local bucket ids, so they can also collide outright.
func lockBlobDir(dir string) (*blobDirLock, error) {
	path := filepath.Join(dir, blobDirLockName)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening the blob directory lock %s: %w", path, err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		closeErr := file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, errors.Join(fmt.Errorf("storage.blob_dir (HOARDDB_BLOB_DIR) %s is in use by another running hoardDB node, which holds %s. "+
				"Each node needs a blob directory of its own: a node deletes every payload file in its blob directory that its own store does not own, so two nodes on one directory delete each other's blobs. "+
				"Point HOARDDB_BLOB_DIR at a different directory, or stop the other node first", dir, path), closeErr)
		}
		return nil, errors.Join(fmt.Errorf("locking the blob directory %s: %w", path, err), closeErr)
	}
	return &blobDirLock{file: file}, nil
}

// release drops the lock. A nil lock is the unset-blob_dir case, which holds
// nothing.
func (l *blobDirLock) release() error {
	if l == nil {
		return nil
	}
	if err := l.file.Close(); err != nil {
		return fmt.Errorf("releasing the blob directory lock %s: %w", l.file.Name(), err)
	}
	return nil
}

// blobDiskIsSeparate reports whether the blob directory is on a filesystem of
// its own (blob_disk.go).
func blobDiskIsSeparate(dataDir, blobDir string) (bool, error) {
	return blobOnOwnFilesystem(dataDir, blobDir, fsDevice)
}

// fsDevice is the device number of the filesystem holding dir.
func fsDevice(dir string) (uint64, error) {
	info, err := os.Stat(dir)
	if err != nil {
		return 0, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, fmt.Errorf("stat %s carries no device number", dir)
	}
	// Dev is uint64 on Linux and int32 on Darwin.
	return uint64(stat.Dev), nil
}
