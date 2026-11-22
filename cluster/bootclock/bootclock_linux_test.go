//go:build linux

package bootclock

import (
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// kernelBootTime reads CLOCK_BOOTTIME directly; it is the reference the clock under test must agree with.
func kernelBootTime(t *testing.T) time.Duration {
	t.Helper()
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &ts); err != nil {
		t.Fatalf("ClockGettime(CLOCK_BOOTTIME): %v", err)
	}
	return time.Duration(ts.Nano())
}

// TestBootTimeAdvancesThroughSuspend pins the clock id. A unit test cannot suspend the host, and on a host that
// never suspends CLOCK_MONOTONIC and CLOCK_BOOTTIME read the same value, so the id is the only thing that tells
// them apart. CLOCK_MONOTONIC freezes through suspend and would let a suspended node wake believing its lease is
// valid (conventions §15).
func TestBootTimeAdvancesThroughSuspend(t *testing.T) {
	if bootClockID != unix.CLOCK_BOOTTIME {
		t.Fatalf("lease clock id = %d, want CLOCK_BOOTTIME (%d): CLOCK_MONOTONIC freezes through suspend",
			bootClockID, unix.CLOCK_BOOTTIME)
	}
	clock := New()
	before := clock.BootNow()
	time.Sleep(50 * time.Millisecond)
	if advanced := clock.BootNow() - before; advanced < 50*time.Millisecond {
		t.Fatalf("BootNow advanced %v across a 50ms sleep, want at least 50ms", advanced)
	}
}

// TestWallClockStepChangesNothing checks that the reading is the kernel's boot clock and not something derived
// from the wall clock or from process start. A wall-clock step moves time.Now and not CLOCK_BOOTTIME, so a reading
// that agrees with the kernel's since-boot value cannot be moved by one.
func TestWallClockStepChangesNothing(t *testing.T) {
	const tolerance = 5 * time.Millisecond
	got := New().BootNow()
	want := kernelBootTime(t)
	if diff := want - got; diff < -tolerance || diff > tolerance {
		t.Fatalf("BootNow() = %v, kernel CLOCK_BOOTTIME = %v: the reading is not the boot clock", got, want)
	}
}
