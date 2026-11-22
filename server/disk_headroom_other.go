//go:build !linux

package server

import (
	"errors"
	"fmt"
)

// diskStatfs is unavailable off the Linux build target it is tuned for. The
// error makes the join guard fail open (a membership change is not refused
// because headroom could not be measured) and the capacity sampler report the
// disk as unavailable rather than as empty.
func diskStatfs(dir string) (diskStat, error) {
	return diskStat{}, errors.New("disk capacity measurement is only implemented on linux")
}

// diskFreeBytesStatfs mirrors diskStatfs's off-Linux refusal: -1 makes the
// restore headroom check fail open rather than refuse every restore on a
// platform this build cannot measure.
func diskFreeBytesStatfs(dir string) (int64, error) {
	return -1, fmt.Errorf("disk headroom measurement is only implemented on linux")
}
