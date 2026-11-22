//go:build linux

package server

import "syscall"

// diskStatfs is one raw statfs of the filesystem holding dir. It does not
// validate the reading: diskStat.validate does, downstream of the probe
// member, so an injected fixture takes the same path a real one does.
// Statfs_t's fields are narrower than uint64 on 32-bit targets, and Frsize is
// signed, so each is converted explicitly.
func diskStatfs(dir string) (diskStat, error) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(dir, &fs); err != nil {
		return diskStat{}, err
	}
	return diskStat{
		Blocks: uint64(fs.Blocks),
		Bavail: uint64(fs.Bavail),
		Frsize: uint64(fs.Frsize),
	}, nil
}

// diskFreeBytesStatfs returns the bytes a non-root writer could actually use
// on the filesystem holding dir (statfs `Bavail * Bsize`, the same
// root-reserve-excluding count the join guard reads) — -1 on error.
func diskFreeBytesStatfs(dir string) (int64, error) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(dir, &fs); err != nil {
		return -1, err
	}
	return int64(fs.Bavail) * int64(fs.Bsize), nil
}
