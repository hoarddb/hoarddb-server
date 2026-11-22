package wire

import (
	"strconv"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// StepDownReporter is implemented by a Topology that knows the committed step_down_ms, which the
// LEASE_LAPSED text prints.
type StepDownReporter interface {
	// StepDownMs is cluster.step_down_ms as committed.
	StepDownMs() int64
}

// fillLeaseLapsed sets the figures of the LEASE_LAPSED text from the lease decision the refusal
// was made on: how long ago validity ended, by the lease clock, and the committed step_down_ms.
// A lease that was never granted served nothing before, so its age is the least a refusal can
// say, one millisecond, rather than the host's uptime.
func fillLeaseLapsed(args map[string]string, lease machine.LeaseDecision, now time.Duration, topo Topology) {
	lapsedMs := int64(1)
	if lease.Cause != machine.LapseNone && lease.Cause != machine.LapseNeverHeld {
		lapsedMs = max(1, (now - lease.LapsedAt).Milliseconds())
	}
	args["lapsed_ms"] = strconv.FormatInt(lapsedMs, 10)
	args["step_down_ms"] = "unknown"
	if reporter, ok := topo.(StepDownReporter); ok {
		args["step_down_ms"] = strconv.FormatInt(reporter.StepDownMs(), 10)
	}
}
