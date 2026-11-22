//go:build unix

package main

import (
	"context"
	"fmt"
	"syscall"
)

// statfsFree reports the free bytes of the filesystem holding path, for a local data directory.
func statfsFree(path string) freeFunc {
	return func(context.Context) (int64, error) {
		var stat syscall.Statfs_t
		if err := syscall.Statfs(path, &stat); err != nil {
			return 0, fmt.Errorf("statfs %s: %w", path, err)
		}
		return int64(stat.Bavail) * int64(stat.Bsize), nil
	}
}
