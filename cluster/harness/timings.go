package harness

import (
	"context"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/config"
)

// Profile is a set of committed cluster timings keyed by the setting names (heartbeat_ms,
// step_down_ms, failover_ms, dead_ms, probe_interval_ms, probe_timeout_ms, probe_failures).
// The failure tables quote absolute times, so their rows run at P0; the flapping, restart
// storm and randomized runs check invariants and run at P1, ten times faster.
type Profile map[string]int64

// P0 is the spec's default timings: detection in 2,500 ms, a stop bound of 2,100 ms.
var P0 = Profile{
	"heartbeat_ms":      100,
	"step_down_ms":      2000,
	"failover_ms":       5000,
	"dead_ms":           600000,
	"probe_interval_ms": 1000,
	"probe_timeout_ms":  500,
	"probe_failures":    3,
}

// P1 is P0 divided by ten. Its timings are checked against the same validator as P0, so
// a run at P1 is a run the node would accept at start-up.
var P1 = Profile{
	"heartbeat_ms":      20,
	"step_down_ms":      150,
	"failover_ms":       400,
	"dead_ms":           3000,
	"probe_interval_ms": 100,
	"probe_timeout_ms":  50,
	"probe_failures":    3,
}

// commitProfile commits every setting of timings on the leader, in key order, so a
// profile lands the same way on every run. Each commit is conditional on the value the
// leader holds now, which is what SetSetting does.
func commitProfile(tb testing.TB, leader *wire.Node, timings map[string]int64) {
	tb.Helper()
	for _, key := range slices.Sorted(maps.Keys(timings)) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := leader.SetSetting(ctx, key, timings[key])
		cancel()
		if err != nil {
			tb.Fatalf("harness: committing the profile setting %s: %v", key, err)
		}
	}
}

// Detection is how long a majority needs to commit a node down: (probe_failures - 1) x
// probe_interval_ms + probe_timeout_ms. P0 gives 2,500 ms.
func (p Profile) Detection() time.Duration {
	return time.Duration((p["probe_failures"]-1)*p["probe_interval_ms"]+p["probe_timeout_ms"]) * time.Millisecond
}

// StopBound is the longest a deposed owner can keep serving: step_down_ms + heartbeat_ms.
// P0 gives 2,100 ms.
func (p Profile) StopBound() time.Duration {
	return time.Duration(p["step_down_ms"]+p["heartbeat_ms"]) * time.Millisecond
}

// MustValidateTimings fails the test with the validator's own text when the profile
// breaks an E11 inequality of config.CommittedTimings. SET_SETTING accepts any value of at least
// one, so without this a compressed profile could quietly break the E11 inequalities the
// failure tables rely on. dead_ms is not checked by the validator, so it is not checked here.
func MustValidateTimings(tb testing.TB, p Profile) {
	tb.Helper()
	timings := config.CommittedTimings{
		HeartbeatMS:     int(p["heartbeat_ms"]),
		StepDownMS:      int(p["step_down_ms"]),
		FailoverMS:      int(p["failover_ms"]),
		ProbeIntervalMS: int(p["probe_interval_ms"]),
		ProbeTimeoutMS:  int(p["probe_timeout_ms"]),
		ProbeFailures:   int(p["probe_failures"]),
	}
	if err := timings.Validate(); err != nil {
		tb.Fatalf("harness: the timing profile is unsafe: %v", err)
	}
}
