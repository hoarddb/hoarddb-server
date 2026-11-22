//go:build !linux

package storage

import (
	"math"
	"os"
)

// fallocateFile reserves nothing off Linux: the spare's zero writes allocate
// its blocks, which is all fallocate adds there.
func fallocateFile(*os.File, int64) error {
	return nil
}

// freeBytesAt does not measure off the Linux build target it is tuned for, and
// reports no limit: the B2 check then passes, and a fill that runs out of room
// fails on its own zero writes and deletes its file, as one that fails for any
// other reason does.
func freeBytesAt(string) (uint64, error) {
	return math.MaxUint64, nil
}
