//go:build !linux

package bootclock

import "time"

// processStart anchors the best-effort reading. The platforms without CLOCK_BOOTTIME give no since-boot monotonic
// clock through the standard library, so the reading counts from process start; lease arithmetic only takes
// differences within one process, so the origin does not matter.
var processStart = time.Now()

type bestEffortClock struct{}

func newClock() Clock { return bestEffortClock{} }

// BootNow returns the monotonic time since process start. It does not include a suspend on these platforms, which
// is the risk WarnOnce names at start-up.
func (bestEffortClock) BootNow() time.Duration {
	return time.Since(processStart)
}
