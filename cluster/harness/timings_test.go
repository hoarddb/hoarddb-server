package harness_test

import (
	"fmt"
	"maps"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
)

// settingsOf reads the committed timings of a state under the names a profile uses.
func settingsOf(s cluster.Settings) map[string]int64 {
	return map[string]int64{
		"heartbeat_ms":      s.HeartbeatMs,
		"step_down_ms":      s.StepDownMs,
		"failover_ms":       s.FailoverMs,
		"dead_ms":           s.DeadMs,
		"probe_interval_ms": s.ProbeIntervalMs,
		"probe_timeout_ms":  s.ProbeTimeoutMs,
		"probe_failures":    int64(s.ProbeFailures),
	}
}

// TestAProfileIsCommittedBeforeNewClusterReturns: the settings a cluster holds when NewCluster
// returns equal the profile asked for, on every node, and a cluster asked for none keeps the
// defaults (which are P0's values).
func TestAProfileIsCommittedBeforeNewClusterReturns(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts harness.Options
		want harness.Profile
	}{
		{"P0", harness.Options{Timings: harness.P0}, harness.P0},
		{"P1", harness.Options{Timings: harness.P1}, harness.P1},
		{"none keeps the defaults", harness.Options{}, harness.P0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := harness.NewCluster(t, 3, tc.opts)
			for _, id := range c.IDs() {
				got := settingsOf(c.Node(id).State().Settings)
				for key, want := range tc.want {
					if got[key] != want {
						t.Fatalf("%s holds %s = %d, want %d", id, key, got[key], want)
					}
				}
			}
		})
	}
}

// failure records a tb.Fatalf instead of ending the test, so a refusal can be asserted.
type failure struct {
	testing.TB
	message string
}

func (f *failure) Fatalf(format string, args ...any) {
	f.message = fmt.Sprintf(format, args...)
	runtime.Goexit()
}

func (f *failure) Fatal(args ...any) {
	f.message = fmt.Sprint(args...)
	runtime.Goexit()
}

// refusal runs body against a recording tb and returns what it passed to Fatal.
func refusal(t *testing.T, body func(tb testing.TB)) string {
	t.Helper()
	f := &failure{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		body(f)
	}()
	<-done
	return f.message
}

func with(p harness.Profile, key string, value int64) harness.Profile {
	out := maps.Clone(p)
	out[key] = value
	return out
}

// TestAnUnsafeProfileIsRefusedWithTheValidatorsText: a profile that breaks the detection
// floor or the failover floor is refused, and the refusal names the setting.
func TestAnUnsafeProfileIsRefusedWithTheValidatorsText(t *testing.T) {
	for _, tc := range []struct {
		name    string
		profile harness.Profile
		names   string
	}{
		// detection falls to 250 ms while step_down + heartbeat stays 2,100 ms.
		{"detection floor", with(harness.P0, "probe_interval_ms", 100), "step_down_ms"},
		// the failover floor is 3 x 1000 + 500 = 3,500 ms.
		{"failover floor", with(harness.P0, "failover_ms", 3000), "failover_ms"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := refusal(t, func(tb testing.TB) { harness.MustValidateTimings(tb, tc.profile) }); !strings.Contains(got, tc.names) {
				t.Fatalf("refusal = %q, want it to name %s", got, tc.names)
			}
			got := refusal(t, func(tb testing.TB) { harness.NewCluster(tb, 3, harness.Options{Timings: tc.profile}) })
			if !strings.Contains(got, tc.names) {
				t.Fatalf("NewCluster refusal = %q, want it to name %s", got, tc.names)
			}
		})
	}
}

// TestTheTwoProfilesAreValid pins P0 and P1 against the start-up validator.
func TestTheTwoProfilesAreValid(t *testing.T) {
	for name, p := range map[string]harness.Profile{"P0": harness.P0, "P1": harness.P1} {
		if got := refusal(t, func(tb testing.TB) { harness.MustValidateTimings(tb, p) }); got != "" {
			t.Fatalf("%s is refused: %s", name, got)
		}
	}
}

// TestProfileArithmeticMatchesTheSpec pins the failure tables' quoted times.
func TestProfileArithmeticMatchesTheSpec(t *testing.T) {
	ms := time.Millisecond
	for _, tc := range []struct {
		name                         string
		p                            harness.Profile
		detection, stopBound, margin time.Duration
	}{
		{"P0", harness.P0, 2500 * ms, 2100 * ms, 400 * ms},
		{"P1", harness.P1, 250 * ms, 170 * ms, 80 * ms},
	} {
		if got := tc.p.Detection(); got != tc.detection {
			t.Errorf("%s detection = %v, want %v", tc.name, got, tc.detection)
		}
		if got := tc.p.StopBound(); got != tc.stopBound {
			t.Errorf("%s stop bound = %v, want %v", tc.name, got, tc.stopBound)
		}
		if got := tc.p.Detection() - tc.p.StopBound(); got != tc.margin {
			t.Errorf("%s margin = %v, want %v", tc.name, got, tc.margin)
		}
	}
}

// TestTimingsTogetherWithADetectionOptionIsATestBug: Timings plus FastDetection,
// NoDetection or NoProbes would commit two competing sets of probe settings.
func TestTimingsTogetherWithADetectionOptionIsATestBug(t *testing.T) {
	for name, opts := range map[string]harness.Options{
		"FastDetection": {Timings: harness.P1, FastDetection: true},
		"NoDetection":   {Timings: harness.P1, NoDetection: true},
		"NoProbes":      {Timings: harness.P1, NoProbes: true},
	} {
		got := refusal(t, func(tb testing.TB) { harness.NewCluster(tb, 3, opts) })
		if !strings.Contains(got, name) {
			t.Fatalf("%s: refusal = %q, want it to name the option", name, got)
		}
	}
}
