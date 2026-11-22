//go:build !linux

package server

import "errors"

// walFilesystemType is unavailable off Linux, where zero-filling a log segment
// is unmeasured: auto then resolves to off and says why.
func walFilesystemType(dir string) (uint32, error) {
	return 0, errors.New("the filesystem type is only read on linux")
}
