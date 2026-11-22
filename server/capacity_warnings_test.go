package server

import (
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
)

// bannedDiskPhrases are claims the disk texts must never make: the node votes
// against a join, it does not refuse one, and nothing here is "the last
// point" (spec §7.2, review R-5).
var bannedDiskPhrases = []string{"add a node", "refuses new members", "refuse new members", "will fail", "last point"}

// TestCapacityWarningAdviceDiffersByResource (spec T-7) drives each resource
// red by injection and reads the WARN: each names its own action, and the
// disk text never says to add a node or that a join is refused.
func TestCapacityWarningAdviceDiffersByResource(t *testing.T) {
	s := newTestServer(t)
	for _, tt := range []struct {
		resource string
		inject   func(p *capacityProbes)
		want     []string
		banned   []string
	}{
		{resource: resourceDisk, inject: func(p *capacityProbes) { p.diskStatfs = fixedDisk(diskFixture(92)) },
			want: []string{"free space", "majority"}, banned: bannedDiskPhrases},
		{resource: resourceFDs, inject: func(p *capacityProbes) {
			p.openFDs = func() (int64, int64, error) { return 950, 1000, nil }
		}, want: []string{"RLIMIT_NOFILE", "LimitNOFILE"}, banned: []string{"add a node"}},
		{resource: resourceMappings, inject: func(p *capacityProbes) {
			p.mappings = func() (int64, int64, error) { return 60000, 65530, nil }
		}, want: []string{"vm.max_map_count"}, banned: []string{"add a node"}},
		{resource: resourceMemory, inject: func(p *capacityProbes) {
			p.anonMemory = func() (int64, error) { return 950 << 20, nil }
		}, want: []string{"add a node"}},
	} {
		t.Run(tt.resource, func(t *testing.T) {
			setCapacityProbes(t, s, func(p *capacityProbes) {
				greenProbes(p)
				tt.inject(p)
			})
			cs, logs := newLoggedSampler(t, s)
			cs.sampleNow()
			warns := logs.messages(slog.LevelWarn)
			if len(warns) != 1 {
				t.Fatalf("%s red wrote %d WARNs, want 1: %q", tt.resource, len(warns), warns)
			}
			for _, phrase := range tt.want {
				if !strings.Contains(warns[0], phrase) {
					t.Fatalf("%s WARN lacks %q: %s", tt.resource, phrase, warns[0])
				}
			}
			for _, phrase := range tt.banned {
				if strings.Contains(warns[0], phrase) {
					t.Fatalf("%s WARN contains %q: %s", tt.resource, phrase, warns[0])
				}
			}
		})
	}

	t.Run("disk amber", func(t *testing.T) {
		setCapacityProbes(t, s, func(p *capacityProbes) {
			greenProbes(p)
			p.diskStatfs = fixedDisk(diskFixture(80))
		})
		cs, logs := newLoggedSampler(t, s)
		cs.sampleNow()
		warns := logs.messages(slog.LevelWarn, "approaching the high-water mark")
		if len(warns) != 1 {
			t.Fatalf("disk at 80%% against 85 wrote %d amber WARNs, want 1: %q", len(warns), logs.messages(slog.LevelWarn))
		}
		for _, phrase := range bannedDiskPhrases {
			if strings.Contains(warns[0], phrase) {
				t.Fatalf("amber disk WARN contains %q: %s", phrase, warns[0])
			}
		}
		if !strings.Contains(warns[0], "votes against joins") {
			t.Fatalf("amber disk WARN does not describe the vote: %s", warns[0])
		}
	})
}

// TestMemoryHighWarningSaysThrottledNotFull (build doc BT-9) pins OQ-12's
// wording: against memory.high the node is throttled, not full and not about
// to be killed; against memory.max it is near being killed, or over the limit.
func TestMemoryHighWarningSaysThrottledNotFull(t *testing.T) {
	high := memoryLimit{bytes: 1 << 30, source: limitCgroupHigh, cgroup: "/system.slice/hoarddb.service", hardBytes: 2 << 30, hardCgroup: "/system.slice"}
	hard := memoryLimit{bytes: 1 << 30, source: limitCgroupMax, cgroup: "/system.slice/hoarddb.service", hardBytes: 1 << 30, hardCgroup: "/system.slice/hoarddb.service"}
	for _, ratio := range []float64{0.9, 1.2} {
		throttled := memoryRedText(ratio, high)
		for _, want := range []string{"throttl", "not a hard limit", "add a node"} {
			if !strings.Contains(throttled, want) {
				t.Fatalf("memory.high WARN at %v lacks %q: %s", ratio, want, throttled)
			}
		}
		for _, banned := range []string{"full", "is killed"} {
			if strings.Contains(throttled, banned) {
				t.Fatalf("memory.high WARN at %v says %q: %s", ratio, banned, throttled)
			}
		}

		full := memoryRedText(ratio, hard)
		want := "killed"
		if ratio >= 1 {
			want = "over the limit"
		}
		for _, phrase := range []string{want, "add a node"} {
			if !strings.Contains(full, phrase) {
				t.Fatalf("memory.max WARN at %v lacks %q: %s", ratio, phrase, full)
			}
		}
	}
}

// TestCapacityWarningIsRateLimitedWithHysteresis (spec T-8) proves a red
// resource warns once per crossing and then hourly, recovers only below
// threshold − 0.05, and warns again when it crosses back.
func TestCapacityWarningIsRateLimitedWithHysteresis(t *testing.T) {
	s := newTestServer(t)
	clock := newFakeClock()
	fds := func(open int64) {
		setCapacityProbes(t, s, func(p *capacityProbes) {
			greenProbes(p)
			p.now = clock.Now
			p.openFDs = func() (int64, int64, error) { return open, 1000, nil }
		})
	}
	cs, logs := newLoggedSampler(t, s)
	red := func() int { return logs.count(slog.LevelWarn, "RLIMIT_NOFILE") }
	recovered := func() int { return logs.count(slog.LevelInfo, "capacity for fds is back below its threshold") }

	fds(900)
	for range 10 {
		cs.sampleNow()
		clock.Advance(capacitySampleInterval)
	}
	if got := red(); got != 1 {
		t.Fatalf("10 samples above the threshold wrote %d WARNs, want 1", got)
	}
	clock.Advance(time.Hour)
	cs.sampleNow()
	if got := red(); got != 2 {
		t.Fatalf("an hour later: %d WARNs, want 2", got)
	}

	fds(760) // under the line, but not under threshold − 0.05: still red
	cs.sampleNow()
	if got := recovered(); got != 0 {
		t.Fatalf("0.76 against 0.80 wrote a recovery INFO; hysteresis holds it red until below 0.75")
	}
	fds(700)
	cs.sampleNow()
	if got := recovered(); got != 1 {
		t.Fatalf("falling below threshold − 0.05 wrote %d recovery INFOs, want 1", got)
	}
	fds(900)
	cs.sampleNow()
	if got := red(); got != 3 {
		t.Fatalf("re-crossing wrote %d WARNs in total, want 3", got)
	}
}

// TestCapacityProbeFailureIsAnnounced (spec T-9, part 5 and the rest) proves a
// failure is written to the log with metrics disabled, once per episode and
// then hourly; that red → unavailable writes no recovery INFO; and that
// reading again writes one "can be read again" INFO.
func TestCapacityProbeFailureIsAnnounced(t *testing.T) {
	s := newTestServer(t)
	if s.config.Metrics.Enabled {
		t.Fatal("this test proves the WARN reaches the log with metrics disabled; the server has them on")
	}
	for _, row := range probeFailureRows() {
		t.Run(row.name, func(t *testing.T) {
			setCapacityProbes(t, s, greenProbes)
			cs, logs := newLoggedSampler(t, s)
			cs.sampleNow()
			setCapacityProbes(t, s, func(p *capacityProbes) {
				greenProbes(p)
				row.inject(t, p)
			})
			cs.sampleNow()
			want := "capacity for " + row.resource + " can no longer be read"
			if row.resource == "" {
				want = "capacity data bytes for"
			}
			if got := logs.count(slog.LevelWarn, want); got != 1 {
				t.Fatalf("(5) %d WARNs %q, want exactly 1: %q", got, want, logs.messages(slog.LevelWarn))
			}
		})
	}

	failing := errors.New("injected disk failure")
	clock := newFakeClock()
	setCapacityProbes(t, s, func(p *capacityProbes) {
		greenProbes(p)
		p.now = clock.Now
		p.diskStatfs = fixedDisk(diskFixture(92))
	})
	cs, logs := newLoggedSampler(t, s)
	cs.sampleNow()
	if logs.count(slog.LevelWarn, "disk on this node is at") != 1 {
		t.Fatal("the disk did not start red")
	}
	setCapacityProbes(t, s, func(p *capacityProbes) {
		greenProbes(p)
		p.now = clock.Now
		p.diskStatfs = func(string) (diskStat, error) { return diskStat{}, failing }
	})
	cs.sampleNow()
	if got := logs.count(slog.LevelInfo, "back below its threshold"); got != 0 {
		t.Fatal("red → unavailable wrote a recovery INFO: the disk stopped being measured, it did not recover")
	}
	clock.Advance(capacitySampleInterval)
	cs.sampleNow()
	if got := logs.count(slog.LevelWarn, "capacity for disk can no longer be read"); got != 1 {
		t.Fatalf("a second consecutive failure: %d failure WARNs, want 1", got)
	}
	clock.Advance(time.Hour)
	cs.sampleNow()
	if got := logs.count(slog.LevelWarn, "capacity for disk can no longer be read"); got != 2 {
		t.Fatalf("an hour into the episode: %d failure WARNs, want 2", got)
	}
	setCapacityProbes(t, s, func(p *capacityProbes) {
		greenProbes(p)
		p.now = clock.Now
	})
	cs.sampleNow()
	if got := logs.count(slog.LevelInfo, "capacity for disk can be read again"); got != 1 {
		t.Fatalf("recovery wrote %d \"can be read again\" INFOs, want 1", got)
	}
	if _, ok := seriesValue(gatherSampler(t, cs), familyRatio, "resource", resourceDisk); !ok {
		t.Fatal("the disk ratio did not come back when the disk could be read again")
	}
}

// TestCapacitySamplerRunsWithMetricsDisabled (spec T-11) proves the sampler
// and its WARN do not depend on /metrics: with metrics off a red reading
// WARNs, one INFO says the gauges are not exported, and nothing binds the
// metrics address.
func TestCapacitySamplerRunsWithMetricsDisabled(t *testing.T) {
	metricsAddr := listenLoopback(t, "metrics")
	metricsListen := metricsAddr.Addr().String()
	if err := metricsAddr.Close(); err != nil {
		t.Fatalf("release the metrics port: %v", err)
	}
	logger, logs := newLogCapture()
	qs := newCapacityService(t, func(cfg *config.Config) {
		cfg.Metrics.Enabled = false
		cfg.Metrics.Listen = metricsListen
	}, logger)
	ports := reserveNodePorts(t)
	qs.driverListener, qs.internodeListener = ports.driver, ports.internode
	if err := qs.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = qs.Stop() })

	if qs.core.capacity == nil {
		t.Fatal("with metrics disabled the capacity sampler was never started")
	}
	waitFirstSample(t, qs.core.capacity)
	setCapacityProbes(t, qs.core, func(p *capacityProbes) {
		greenProbes(p)
		p.openFDs = func() (int64, int64, error) { return 950, 1000, nil }
	})
	qs.core.capacity.sampleNow()
	if got := logs.count(slog.LevelWarn, "RLIMIT_NOFILE"); got != 1 {
		t.Fatalf("a red fds reading with metrics off wrote %d WARNs, want 1", got)
	}
	if got := logs.count(slog.LevelInfo, "capacity gauges are not exported"); got != 1 {
		t.Fatalf("the metrics-disabled INFO was written %d times, want once", got)
	}
	probe, err := net.Listen("tcp", metricsListen)
	if err != nil {
		t.Fatalf("the metrics address %s is bound with metrics disabled: %v", metricsListen, err)
	}
	if err := probe.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestHealthCheckCarriesCapacityRows (spec T-13) proves status carries the
// four rows of the last sample, and none is stale on a fresh node.
func TestHealthCheckCarriesCapacityRows(t *testing.T) {
	s := newTestServer(t)
	waitFirstSample(t, s.capacity)
	health, err := s.HealthCheck()
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if health.Capacity == nil || len(health.Capacity.Rows) != len(capacityResources) {
		t.Fatalf("HealthCheck capacity = %+v, want %d rows", health.Capacity, len(capacityResources))
	}
	for i, row := range health.Capacity.Rows {
		if row.Resource != capacityResources[i] || row.State == stateStale {
			t.Fatalf("row %d = %+v, want resource %s and a fresh state", i, row, capacityResources[i])
		}
	}
	if health.Capacity.SampledAt == 0 {
		t.Fatal("sampled_at is 0 after a sample")
	}
}

// TestDiskGuardOffWarnsWithoutTheVote (spec T-19, WARN half) proves a guard
// of 0 still warns at 90%, without the sentences about votes and joins, and
// that 50% and 78% write nothing.
func TestDiskGuardOffWarnsWithoutTheVote(t *testing.T) {
	qs := newCapacityService(t, func(cfg *config.Config) { cfg.Server.DiskHighWaterPct = 0 }, nil)
	t.Cleanup(func() { _ = qs.Stop() })
	s := qs.core
	cs, logs := newLoggedSampler(t, s)
	for _, pct := range []int{50, 78} {
		setCapacityProbes(t, s, func(p *capacityProbes) {
			greenProbes(p)
			p.diskStatfs = fixedDisk(diskFixture(pct))
		})
		cs.sampleNow()
	}
	if warns := logs.messages(slog.LevelWarn); len(warns) != 0 {
		t.Fatalf("50%% and 78%% with the guard off wrote %q", warns)
	}
	setCapacityProbes(t, s, func(p *capacityProbes) {
		greenProbes(p)
		p.diskStatfs = fixedDisk(diskFixture(90))
	})
	cs.sampleNow()
	warns := logs.messages(slog.LevelWarn)
	if len(warns) != 1 || !strings.Contains(warns[0], "free space") {
		t.Fatalf("90%% with the guard off wrote %q, want one disk WARN", warns)
	}
	for _, phrase := range append([]string{"votes against joins", "majority", "Do not grow the cluster"}, bannedDiskPhrases...) {
		if strings.Contains(warns[0], phrase) {
			t.Fatalf("the guard-off disk WARN contains %q: %s", phrase, warns[0])
		}
	}
}

// blockingDisk is a diskStatfs member that, once armed, blocks every call
// until released, standing in for a statfs stuck on a dead mount.
type blockingDisk struct {
	armed    chan struct{}
	entered  chan struct{}
	release  chan struct{}
	armOnce  sync.Once
	fireOnce sync.Once
}

func newBlockingDisk() *blockingDisk {
	return &blockingDisk{armed: make(chan struct{}), entered: make(chan struct{}), release: make(chan struct{})}
}

func (b *blockingDisk) statfs(string) (diskStat, error) {
	select {
	case <-b.armed:
		b.fireOnce.Do(func() { close(b.entered) })
		<-b.release
	default:
	}
	return diskFixture(10), nil
}

func (b *blockingDisk) arm() { b.armOnce.Do(func() { close(b.armed) }) }

// TestStatusShowsStaleWhenTheSamplerStops (spec T-20) proves a sampler stuck
// in a probe is visible: after 45s every status row reads stale, whatever the
// last sample said, and a sample that completes brings the rows back.
func TestStatusShowsStaleWhenTheSamplerStops(t *testing.T) {
	s := newTestServer(t)
	waitFirstSample(t, s.capacity)
	clock := newFakeClock()
	disk := newBlockingDisk()
	setCapacityProbes(t, s, func(p *capacityProbes) {
		greenProbes(p)
		p.now = clock.Now
		p.diskStatfs = disk.statfs
	})
	s.capacity.sampleNow()
	assertRowStates(t, s, "after a good sample", func(state string) bool { return state != stateStale })

	disk.arm()
	sampled := make(chan struct{})
	go func() {
		s.capacity.sampleNow()
		close(sampled)
	}()
	<-disk.entered
	released := false
	release := func() {
		if !released {
			released = true
			close(disk.release)
			<-sampled
		}
	}
	// Release before the test's cleanup stops the server, or Stop waits on the
	// blocked sample.
	defer release()

	clock.Advance(46 * time.Second)
	assertRowStates(t, s, "46s into a stuck sample", func(state string) bool { return state == stateStale })

	// The stuck sample read its clock before it blocked, so it completes 46s
	// stale; the next one brings the rows back to their computed states.
	release()
	s.capacity.sampleNow()
	assertRowStates(t, s, "after the probe is released", func(state string) bool { return state != stateStale })
}

// TestHealthCheckDoesNotWaitForTheSampler (build doc BT-10) proves status
// answers while a probe is stuck: healthCapacity takes lastMu, never sampleMu.
func TestHealthCheckDoesNotWaitForTheSampler(t *testing.T) {
	s := newTestServer(t)
	waitFirstSample(t, s.capacity)
	disk := newBlockingDisk()
	setCapacityProbes(t, s, func(p *capacityProbes) {
		greenProbes(p)
		p.diskStatfs = disk.statfs
	})
	disk.arm()
	sampled := make(chan struct{})
	go func() {
		s.capacity.sampleNow()
		close(sampled)
	}()
	<-disk.entered
	defer func() {
		close(disk.release)
		<-sampled
	}()

	answered := make(chan error, 1)
	go func() {
		_, err := s.HealthCheck()
		answered <- err
	}()
	select {
	case err := <-answered:
		if err != nil {
			t.Fatalf("HealthCheck: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("HealthCheck waited on a sample stuck in a probe")
	}
}

// TestUnavailableRowCarriesNoRatio (build doc BT-11) proves an unavailable row
// carries no ratio at all, rather than a healthy-looking 0.
func TestUnavailableRowCarriesNoRatio(t *testing.T) {
	s := newTestServer(t)
	waitFirstSample(t, s.capacity)
	setCapacityProbes(t, s, func(p *capacityProbes) {
		greenProbes(p)
		p.diskStatfs = func(string) (diskStat, error) { return diskStat{}, errors.New("injected disk failure") }
	})
	s.capacity.sampleNow()
	health, err := s.HealthCheck()
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	for _, row := range health.Capacity.Rows {
		if row.Resource != resourceDisk {
			continue
		}
		if row.State != stateUnavailable.String() || row.Ratio != nil {
			t.Fatalf("disk row %+v, want unavailable with no ratio", row)
		}
		return
	}
	t.Fatal("no disk row")
}

// assertRowStates checks every status row's state.
func assertRowStates(t *testing.T, s *Server, when string, ok func(state string) bool) {
	t.Helper()
	health, err := s.HealthCheck()
	if err != nil {
		t.Fatalf("HealthCheck %s: %v", when, err)
	}
	for _, row := range health.Capacity.Rows {
		if !ok(row.State) {
			t.Fatalf("%s: row %s reads %q", when, row.Resource, row.State)
		}
	}
}
