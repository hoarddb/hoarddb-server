package server

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/hoarddb/hoarddb-server/config"
)

// setCapacityProbes replaces one server's probe set with an edited copy. The
// loaded value is never mutated: the sampler may be reading it on another
// goroutine, so the edit goes into a fresh struct that is then Stored. No
// cleanup is registered, because the server's own cleanup stops it and the
// probe set dies with it.
func setCapacityProbes(t testing.TB, s *Server, edit func(*capacityProbes)) {
	t.Helper()
	edited := *s.probes.Load()
	edit(&edited)
	s.probes.Store(&edited)
}

// diskFixture is a filesystem pctUsed percent full, in round block counts, so
// usedPercent is pctUsed within one ulp. Its blocks are 1 GiB, a 100 GiB
// filesystem: a pull that replaces a copy is refused when the copy would take
// the filesystem to the mark (refuseReplacingPastTheMark), and the test
// clusters' copies must count as the few bytes they are, not as a share of a
// 400 KiB disk.
func diskFixture(pctUsed int) diskStat {
	return diskStat{Blocks: 100, Bavail: uint64(100 - pctUsed), Frsize: 1 << 30}
}

// fixedDisk is a diskStatfs member that reports st for any directory.
func fixedDisk(st diskStat) func(string) (diskStat, error) {
	return func(string) (diskStat, error) { return st, nil }
}

// fixtureMemoryLimit is the green fixtures' memory limit: 1 GiB from a
// memory.max, so a ratio against it means "full" in the WARN's words.
var fixtureMemoryLimit = memoryLimit{
	bytes: 1 << 30, source: limitCgroupMax, cgroup: "/fixture.slice",
	hardBytes: 1 << 30, hardCgroup: "/fixture.slice",
}

// greenProbes sets every member to a healthy reading, so a test starts from a
// successful sample whatever the test host's real disk, descriptors or
// cgroup say.
func greenProbes(p *capacityProbes) {
	p.diskStatfs = fixedDisk(diskFixture(10))
	p.openFDs = func() (int64, int64, error) { return 100, 1000, nil }
	p.mappings = func() (int64, int64, error) { return 100, 65530, nil }
	p.anonMemory = func() (int64, error) { return 100 << 20, nil }
	p.memoryLimit = func() (memoryLimit, error) { return fixtureMemoryLimit, nil }
	p.storeSize = func() (int64, int64, error) { return 1000, 2000, nil }
	p.walBytes = func() (int64, bool, error) { return 500, true, nil }
}

// newIdleSampler builds a second sampler over s that is never started: its
// own registry and its own state, sampled only when the test calls
// sampleNow. It reads s's probes, so setCapacityProbes reaches it, and the
// server's running sampler cannot interleave with the test's samples.
func newIdleSampler(t testing.TB, s *Server) *capacitySampler {
	t.Helper()
	cs, err := newCapacitySampler(s)
	if err != nil {
		t.Fatalf("newCapacitySampler: %v", err)
	}
	return cs
}

// newCapacityService builds a server through NewService on a default config
// (never config.Load, which reads HOARDDB_* from the environment). edit, when
// non-nil, changes the config before NewService reads it; logger, when
// non-nil, is the server's logger. The caller stops the service.
func newCapacityService(t testing.TB, edit func(*config.Config), logger *slog.Logger) *Service {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Metrics.Enabled = false
	cfg.Audit.Enabled = false
	if edit != nil {
		edit(cfg)
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	qs, err := NewService(cfg, logger)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return qs
}

// gatherSampler returns a sampler's registry as metric family name → series.
func gatherSampler(t testing.TB, cs *capacitySampler) map[string][]*dto.Metric {
	t.Helper()
	families, err := cs.metrics.registry.Gather()
	if err != nil {
		t.Fatalf("gathering the capacity registry: %v", err)
	}
	byName := make(map[string][]*dto.Metric, len(families))
	for _, family := range families {
		byName[family.GetName()] = family.GetMetric()
	}
	return byName
}

// gatherCapacity gathers a server's running sampler's registry.
func gatherCapacity(t testing.TB, s *Server) map[string][]*dto.Metric {
	t.Helper()
	return gatherSampler(t, s.capacity)
}

// metricValue is a gauge's or counter's value.
func metricValue(m *dto.Metric) float64 {
	if m.GetGauge() != nil {
		return m.GetGauge().GetValue()
	}
	return m.GetCounter().GetValue()
}

// labelValue is one label's value on a series, or "" when it has none.
func labelValue(m *dto.Metric, name string) string {
	for _, pair := range m.GetLabel() {
		if pair.GetName() == name {
			return pair.GetValue()
		}
	}
	return ""
}

// seriesValue finds a family's series whose label (when labelName is not
// empty) has labelWant, and reports its value.
func seriesValue(gathered map[string][]*dto.Metric, family, labelName, labelWant string) (float64, bool) {
	for _, m := range gathered[family] {
		if labelName == "" || labelValue(m, labelName) == labelWant {
			return metricValue(m), true
		}
	}
	return 0, false
}

// rowFor is a resource's row in the sampler's last sample.
func rowFor(t testing.TB, cs *capacitySampler, resource string) capacityRowSample {
	t.Helper()
	cs.lastMu.Lock()
	defer cs.lastMu.Unlock()
	for _, row := range cs.last.rows {
		if row.resource == resource {
			return row
		}
	}
	t.Fatalf("no %s row in the last sample (sampled at %v)", resource, cs.last.sampledAt)
	return capacityRowSample{}
}

// waitFirstSample waits for a running sampler's immediate first sample, so a
// test's own injections cannot be read by it.
func waitFirstSample(t testing.TB, cs *capacitySampler) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		cs.lastMu.Lock()
		sampled := !cs.last.sampledAt.IsZero()
		cs.lastMu.Unlock()
		if sampled {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the capacity sampler took no sample within 5s of start")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// fakeClock is a probe clock a test moves by hand.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// capturedLog is an slog handler that keeps every record, so a test can count
// the WARN and INFO lines a sample wrote.
type capturedLog struct {
	mu      sync.Mutex
	records []capturedRecord
}

type capturedRecord struct {
	level slog.Level
	msg   string
}

func newLogCapture() (*slog.Logger, *capturedLog) {
	captured := &capturedLog{}
	return slog.New(captured), captured
}

func (c *capturedLog) Enabled(context.Context, slog.Level) bool { return true }

func (c *capturedLog) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, capturedRecord{level: r.Level, msg: r.Message})
	return nil
}

func (c *capturedLog) WithAttrs([]slog.Attr) slog.Handler { return c }

func (c *capturedLog) WithGroup(string) slog.Handler { return c }

// messages returns the messages logged at level that contain every one of
// substrings.
func (c *capturedLog) messages(level slog.Level, substrings ...string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var matched []string
	for _, record := range c.records {
		if record.level != level || !containsAll(record.msg, substrings) {
			continue
		}
		matched = append(matched, record.msg)
	}
	return matched
}

// count is how many messages at level contain every one of substrings.
func (c *capturedLog) count(level slog.Level, substrings ...string) int {
	return len(c.messages(level, substrings...))
}

func containsAll(s string, substrings []string) bool {
	for _, sub := range substrings {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}

// newLoggedSampler is newIdleSampler with its log captured.
func newLoggedSampler(t testing.TB, s *Server) (*capacitySampler, *capturedLog) {
	t.Helper()
	cs := newIdleSampler(t, s)
	logger, captured := newLogCapture()
	cs.logger = logger
	return cs, captured
}
