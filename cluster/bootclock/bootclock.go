// Package bootclock reads the lease clock. On Linux it is CLOCK_BOOTTIME, which keeps counting through a VM
// suspend, so a suspended node cannot wake holding a lease it no longer has. Elsewhere it is the best-effort
// monotonic reading the platform offers.
package bootclock

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// Clock reads the lease clock. Only differences between two readings from one process mean anything; the value
// is never a wall-clock time, so a wall-clock step cannot move it.
type Clock interface {
	// BootNow returns the current reading as a duration.
	BootNow() time.Duration
}

// New returns the lease clock for this platform.
func New() Clock {
	return newClock()
}

// WarnOnce writes the one start-up warning a non-Linux cluster member prints, naming the platform and the risk.
// It is called once per process, and it refuses nothing: the lease still runs on the best-effort clock. It returns
// the write error from the first call, so the node-start code can log it; later calls return nil.
func WarnOnce(w io.Writer, platform string) error {
	if platform == "linux" {
		return nil
	}
	var err error
	warnOnce.Do(func() {
		_, err = fmt.Fprintf(w, "hoardDB: warning: this cluster member runs on %s, which has no CLOCK_BOOTTIME, so its "+
			"lease clock does not count through a VM suspend: a suspended guest may wake believing its lease is still "+
			"valid. The node starts anyway; run cluster members on Linux to remove the risk.\n", platform)
		if err != nil {
			err = fmt.Errorf("writing the boot clock warning: %w", err)
		}
	})
	return err
}

// warnOnce guards WarnOnce so a process prints the warning at most once.
var warnOnce sync.Once
