//go:build linux

package server

import "syscall"

// walFilesystemType is the statfs f_type of the filesystem holding dir. The
// field is 32 bits on some architectures and 64 on others, and 0x9123683E and
// 0x58465342 do not fit a signed 32-bit value, so it is compared as a uint32.
func walFilesystemType(dir string) (uint32, error) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(dir, &fs); err != nil {
		return 0, err
	}
	return uint32(fs.Type), nil
}
