//go:build !unix

package server

import "fmt"

// blobDirLock is unavailable off unix: there is no flock(2) to hold a blob
// directory with, and a configured blob_dir without its lock would let two
// nodes delete each other's payloads.
type blobDirLock struct{}

func lockBlobDir(dir string) (*blobDirLock, error) {
	return nil, fmt.Errorf("storage.blob_dir (HOARDDB_BLOB_DIR) %s cannot be used on this platform: the node locks its blob directory with flock(2), which only unix builds have; unset it to keep payloads under the data directory", dir)
}

func (l *blobDirLock) release() error { return nil }

// blobDiskIsSeparate is false off unix: there is no device number to compare,
// and no disk measurement to take either (disk_headroom_other.go), so a
// second disk row would only read unavailable beside the first.
func blobDiskIsSeparate(dataDir, blobDir string) (bool, error) { return false, nil }
