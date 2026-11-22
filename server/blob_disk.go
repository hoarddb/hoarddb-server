package server

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
)

// blobOnOwnFilesystem reports whether blobDir is on a different filesystem
// from dataDir, by device number. A blob directory not created yet is on the
// filesystem of its nearest existing parent, which is where it will be
// created: that is the default tree before the first blob write.
//
// The answer is taken once, at start, and both the join guard and the
// capacity sampler read it, so the gauge and the node's vote cannot disagree
// about which filesystems there are. A volume mounted over the blob directory
// while the node runs is seen at the next start.
func blobOnOwnFilesystem(dataDir, blobDir string, deviceOf func(string) (uint64, error)) (bool, error) {
	dataDevice, err := deviceOf(dataDir)
	if err != nil {
		return false, fmt.Errorf("reading which filesystem holds the data directory %s: %w", dataDir, err)
	}
	for dir := filepath.Clean(blobDir); ; dir = filepath.Dir(dir) {
		blobDevice, err := deviceOf(dir)
		if err == nil {
			return blobDevice != dataDevice, nil
		}
		if !errors.Is(err, fs.ErrNotExist) || filepath.Dir(dir) == dir {
			return false, fmt.Errorf("reading which filesystem holds the blob directory %s: %w", blobDir, err)
		}
	}
}

// diskDirs are the directories whose filesystems the join guard and the
// capacity sampler read: the data directory's always, and the blob
// directory's when it is a filesystem of its own. Each filesystem is read
// once.
func (s *Server) diskDirs() []string {
	if s.blobDiskSeparate {
		return []string{s.config.Storage.DataDir, s.blobRoot()}
	}
	return []string{s.config.Storage.DataDir}
}
