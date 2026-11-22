//go:build linux

package bootclock

import (
	"time"

	"golang.org/x/sys/unix"
)

// bootClockID is CLOCK_BOOTTIME. Unlike CLOCK_MONOTONIC it keeps counting through a VM suspend, so a lease
// measured on it is void when a suspended node wakes (conventions §15).
const bootClockID = unix.CLOCK_BOOTTIME

type linuxClock struct{}

func newClock() Clock { return linuxClock{} }

// BootNow reads CLOCK_BOOTTIME. The kernel has offered it since 2.6.39, so a failure is an invariant break; the
// code does not fall back to a clock that freezes through suspend.
func (linuxClock) BootNow() time.Duration {
	var ts unix.Timespec
	if err := unix.ClockGettime(bootClockID, &ts); err != nil {
		panic("bootclock: ClockGettime(CLOCK_BOOTTIME) failed: " + err.Error()) // invariant: kernel >= 2.6.39
	}
	return time.Duration(ts.Nano())
}
