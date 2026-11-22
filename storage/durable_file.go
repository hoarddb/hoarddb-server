package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// syncFile and syncDirectory are the two fsyncs WriteFileDurably makes, and
// renameFile its rename. They are variables so a test can record that each
// happens, and in which order: a power cut is not available to a test.
var (
	syncFile      = func(file *os.File) error { return file.Sync() }
	syncDirectory = syncDirectoryPath
	renameFile    = os.Rename
)

// WriteFileDurably replaces path with contents so that, after a power loss at
// any point, the file holds either its previous contents or the new ones,
// never an empty or partial file: write a temporary file beside it, fsync it,
// rename it over path, then fsync the directory, which is what makes the
// rename itself durable. Without the first fsync the rename can reach the disk
// before the data does, and leave an empty file in place.
func WriteFileDurably(path string, contents []byte) error {
	temporaryPath := path + ".tmp"
	file, err := os.OpenFile(temporaryPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("creating %s: %w", temporaryPath, err)
	}
	if _, err := file.Write(contents); err != nil {
		return errors.Join(fmt.Errorf("writing %s: %w", temporaryPath, err), file.Close())
	}
	if err := syncFile(file); err != nil {
		return errors.Join(fmt.Errorf("syncing %s: %w", temporaryPath, err), file.Close())
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", temporaryPath, err)
	}
	if err := renameFile(temporaryPath, path); err != nil {
		return fmt.Errorf("renaming %s over %s: %w", temporaryPath, path, err)
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return fmt.Errorf("making the rename of %s durable: %w", path, err)
	}
	return nil
}

func syncDirectoryPath(dirPath string) error {
	dirFile, err := os.Open(dirPath)
	if err != nil {
		return fmt.Errorf("opening %s to sync it: %w", dirPath, err)
	}
	if err := dirFile.Sync(); err != nil {
		return errors.Join(fmt.Errorf("syncing %s: %w", dirPath, err), dirFile.Close())
	}
	if err := dirFile.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", dirPath, err)
	}
	return nil
}
