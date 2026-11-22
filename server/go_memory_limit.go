package server

import (
	"fmt"
	"os"
	"runtime/debug"
)

// goMemoryLimitPercent is the share of the detected memory limit the server
// gives the Go runtime as its soft memory limit when the operator sets no
// GOMEMLIMIT. Under an ingest burst the live heap is mostly Badger's
// pooled table-builder buffers, and with GOGC=100 and no limit the heap grew to
// about 2.4 times that before the GC ran. 75% sits inside the 70-80% the
// investigation proposed, below the capacity sampler's 0.80 memory red line,
// so the GC works harder before the node reports red, and it leaves a quarter
// of the limit for what the Go limit does not count: the mapped table and
// value-log pages, thread stacks and the kernel's share of the cgroup.
const goMemoryLimitPercent = 75

// goMemoryLimitDecision is what the start sets and the start-up line says.
// bytes is 0 when the server sets nothing.
type goMemoryLimitDecision struct {
	bytes int64
	text  string
}

// decideGoMemoryLimit decides the Go soft memory limit from the environment's
// GOMEMLIMIT, which always wins (the Go runtime has applied it already), and
// the node's memory limit.
func decideGoMemoryLimit(environment string, readLimit func() (memoryLimit, error)) goMemoryLimitDecision {
	if environment != "" {
		return goMemoryLimitDecision{text: "GOMEMLIMIT=" + environment + ", from the environment"}
	}
	limit, err := readLimit()
	if err != nil {
		return goMemoryLimitDecision{text: fmt.Sprintf("not set: this node's memory limit could not be read (%v). "+
			"An ingest burst can then grow the heap to several GB; set GOMEMLIMIT to about three quarters of this node's memory", err)}
	}
	limitBytes := limit.bytes * goMemoryLimitPercent / 100
	return goMemoryLimitDecision{
		bytes: limitBytes,
		text: fmt.Sprintf("%s, %d%% of this node's memory limit of %s (%s); set GOMEMLIMIT to choose another",
			formatBytesIEC(limitBytes), goMemoryLimitPercent, formatBytesIEC(limit.bytes), memtableLimitOrigin(limit)),
	}
}

// SetGoMemoryLimit gives the Go runtime a soft memory limit of 75% of the
// node's memory limit, unless GOMEMLIMIT is set, and returns the sentence the
// server's start-up line logs. The server's main calls it once, before the
// store opens; it is process-wide, so nothing else calls it.
func SetGoMemoryLimit() string {
	decision := decideGoMemoryLimit(os.Getenv("GOMEMLIMIT"), hostMemoryLimit)
	if decision.bytes > 0 {
		debug.SetMemoryLimit(decision.bytes)
	}
	return decision.text
}
