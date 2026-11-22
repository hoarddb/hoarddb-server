package server

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestWALPreallocateAutoByFilesystem pins what storage.wal_preallocate=auto
// resolves to for each filesystem type statfs can report, and the startup line
// each answer logs, verbatim. XFS stays off until it is measured (spec §11
// item 5): this test is the place that changes when it is.
func TestWALPreallocateAutoByFilesystem(t *testing.T) {
	const prefixOn = "write-ahead log segments are preallocated "
	const prefixOff = "write-ahead log segments are not preallocated "
	dir := t.TempDir()
	probeOf := func(fsType uint32, err error) func(string) (uint32, error) {
		return func(string) (uint32, error) { return fsType, err }
	}
	for _, tc := range []struct {
		name       string
		fsType     uint32
		probeErr   error
		wantOn     bool
		wantFS     string
		wantLine   string
		wantReason string
	}{
		{"ext2/3/4", 0xEF53, nil, true, "ext2/3/4", prefixOn + "(storage.wal_preallocate=auto: ext2/3/4)", "ext2/3/4"},
		{"xfs", 0x58465342, nil, false, "xfs", prefixOff + "(storage.wal_preallocate=auto: XFS is not measured yet)", "XFS is not measured yet"},
		{"btrfs", 0x9123683E, nil, false, "btrfs", prefixOff + "(storage.wal_preallocate=auto: btrfs is copy-on-write, so it would not help)", "btrfs is copy-on-write, so it would not help"},
		{"zfs", 0x2FC12FC1, nil, false, "zfs", prefixOff + "(storage.wal_preallocate=auto: ZFS is copy-on-write, so it would not help)", "ZFS is copy-on-write, so it would not help"},
		{"tmpfs", 0x01021994, nil, false, "tmpfs", prefixOff + "(storage.wal_preallocate=auto: tmpfs is memory, so it would cost RAM)", "tmpfs is memory, so it would cost RAM"},
		{"overlayfs", 0x794C7630, nil, false, "0x794c7630", prefixOff + "(storage.wal_preallocate=auto: filesystem 0x794c7630 is not ext2/3/4)", "filesystem 0x794c7630 is not ext2/3/4"},
		{"unknown", 0x1234, nil, false, "0x1234", prefixOff + "(storage.wal_preallocate=auto: filesystem 0x1234 is not ext2/3/4)", "filesystem 0x1234 is not ext2/3/4"},
		{"statfs error", 0, errors.New("no such device"), false, "",
			prefixOff + "(storage.wal_preallocate=auto: could not read the filesystem of " + dir + ": no such device)", "could not read the filesystem of " + dir + ": no such device"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveWALPreallocate(config.WALPreallocateAuto, dir, probeOf(tc.fsType, tc.probeErr))
			if got.On != tc.wantOn {
				t.Errorf("auto on %s resolved On=%v, want %v", tc.name, got.On, tc.wantOn)
			}
			if got.Filesystem != tc.wantFS {
				t.Errorf("filesystem = %q, want %q", got.Filesystem, tc.wantFS)
			}
			if got.Reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", got.Reason, tc.wantReason)
			}
			if line := got.startLine(); line != tc.wantLine {
				t.Errorf("startup line:\n got %q\nwant %q", line, tc.wantLine)
			}
		})
	}

	// "on" and "off" never ask the filesystem.
	failing := func(string) (uint32, error) {
		t.Fatal("the filesystem was probed for a mode that is not auto")
		return 0, nil
	}
	for _, tc := range []struct {
		configured string
		wantOn     bool
		wantLine   string
	}{
		{config.WALPreallocateOn, true, prefixOn + "(storage.wal_preallocate=on)"},
		{config.WALPreallocateOff, false, prefixOff + "(storage.wal_preallocate=off)"},
	} {
		got := resolveWALPreallocate(tc.configured, dir, failing)
		if got.On != tc.wantOn || got.startLine() != tc.wantLine {
			t.Errorf("%s: On=%v line %q, want On=%v line %q", tc.configured, got.On, got.startLine(), tc.wantOn, tc.wantLine)
		}
	}

	// A first start has no WAL directory yet; auto creates it so there is
	// something to statfs.
	fresh := filepath.Join(t.TempDir(), "wal")
	var probed string
	resolveWALPreallocate(config.WALPreallocateAuto, fresh, func(path string) (uint32, error) { probed = path; return 0xEF53, nil })
	if info, err := os.Stat(fresh); err != nil || !info.IsDir() {
		t.Errorf("auto left %s uncreated before probing it (stat error %v)", fresh, err)
	}
	if probed != fresh {
		t.Errorf("auto probed %q, want %q", probed, fresh)
	}
}

// startServiceLogging starts a core server with the given edit and returns what
// it logged.
func startServiceLogging(t *testing.T, edit func(*config.Config)) (*Server, *syncBuffer) {
	t.Helper()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("loading default config: %v", err)
	}
	cfg.Storage.DataDir = t.TempDir()
	cfg.Server.ReplicationFactor = 1
	cfg.Server.Listen = "127.0.0.1:0"
	cfg.TCP.Listen = "127.0.0.1:0"
	edit(cfg)
	if cfg.Server.ReplicationFactor > 1 {
		if err := SaveClusterToken(cfg.Storage.DataDir, ClusterToken{Token: strings.Repeat("a", 64)}); err != nil {
			t.Fatalf("SaveClusterToken: %v", err)
		}
	}
	logged := &syncBuffer{}
	service, err := NewService(cfg, slog.New(slog.NewTextHandler(logged, nil)))
	if err != nil {
		t.Fatalf("creating server: %v", err)
	}
	t.Cleanup(func() {
		if err := service.Stop(); err != nil {
			t.Logf("stopping server: %v", err)
		}
	})
	return service.core, logged
}

// TestWALPreallocateStartupLineNoEffectWithoutAWAL: on or auto set by the
// operator on a node with replication factor 1 and no seeds says that the
// legacy node has no log and that the entry-ring node's line is separate, so a
// single-node benchmark run with the key set cannot pass for a preallocated run
// and the line cannot be read as the ring node's mode. The default (nothing
// set) says nothing, because auto is the default and would otherwise print the
// line on every start.
func TestWALPreallocateStartupLineNoEffectWithoutAWAL(t *testing.T) {
	for _, configured := range []string{config.WALPreallocateOn, config.WALPreallocateAuto} {
		t.Run(configured, func(t *testing.T) {
			s, logged := startServiceLogging(t, func(cfg *config.Config) {
				cfg.Storage.WALPreallocate = configured
				cfg.Storage.WALPreallocateSet = true
			})
			if s.wal != nil {
				t.Fatal("a node with replication factor 1 and no seeds opened a log")
			}
			want := "storage.wal_preallocate=" + configured + " has no effect here: this node keeps no node-to-node write-ahead log (replication_factor 1, no seed nodes); the entry-ring node logs its own mode"
			if !strings.Contains(logged.String(), want) {
				t.Errorf("the start did not log %q; it logged:\n%s", want, logged.String())
			}
			if health, _ := s.HealthCheck(); health.WAL != nil {
				t.Errorf("a node with no log reports wal %+v in HEALTH_CHECK, want none", health.WAL)
			}
		})
	}
	t.Run("the default says nothing", func(t *testing.T) {
		_, logged := startServiceLogging(t, func(*config.Config) {})
		if strings.Contains(logged.String(), "has no effect") {
			t.Errorf("the default mode logged a no-effect line:\n%s", logged.String())
		}
	})
	t.Run("off says nothing", func(t *testing.T) {
		_, logged := startServiceLogging(t, func(cfg *config.Config) {
			cfg.Storage.WALPreallocate = config.WALPreallocateOff
			cfg.Storage.WALPreallocateSet = true
		})
		if strings.Contains(logged.String(), "has no effect") {
			t.Errorf("an explicit off logged a no-effect line:\n%s", logged.String())
		}
	})
}

// TestRingWALStartLineNamesTheRingNode: the entry-ring node's log line carries
// the node's name, so it cannot be read as the legacy node's mode, and says the
// same things the legacy line does for the same resolution.
func TestRingWALStartLineNamesTheRingNode(t *testing.T) {
	resolution := walModeResolution{On: true, Configured: config.WALPreallocateAuto, Filesystem: "ext2/3/4", Reason: "ext2/3/4"}
	logged := &syncBuffer{}
	resolution.logRingWALMode(slog.New(slog.NewTextHandler(logged, nil)))
	want := "entry-ring: write-ahead log segments are preallocated (storage.wal_preallocate=auto: ext2/3/4)"
	if !strings.Contains(logged.String(), want) {
		t.Errorf("the ring node logged:\n%s\nwant a line containing %q", logged.String(), want)
	}
}

// TestWALPreallocateStartupLineAndHealthOfANodeWithALog: a node with a log
// logs the mode in effect once and reports it, with the spare and the log's
// write refusal, in HEALTH_CHECK.
func TestWALPreallocateStartupLineAndHealthOfANodeWithALog(t *testing.T) {
	s, logged := startServiceLogging(t, func(cfg *config.Config) { cfg.Server.ReplicationFactor = 2 })
	if s.wal == nil {
		t.Fatal("a node with replication factor 2 opened no log")
	}
	if got := strings.Count(logged.String(), "write-ahead log segments are not preallocated"); got != 1 {
		t.Errorf("the start logged the segment mode %d times, want once:\n%s", got, logged.String())
	}
	health, err := s.HealthCheck()
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if health.WAL == nil {
		t.Fatal("a node with a log reports no wal in HEALTH_CHECK")
	}
	if health.WAL.Preallocated || health.WAL.Configured != "auto" || health.WAL.Refusal != "" {
		t.Errorf("wal = %+v, want not preallocated, configured auto (the default), no refusal", health.WAL)
	}

	// A log whose directory is gone refuses every write; HEALTH_CHECK says so in
	// the wal field, not only in the node's store refusal.
	if err := os.RemoveAll(filepath.Join(s.config.Storage.DataDir, "wal")); err != nil {
		t.Fatalf("removing the log directory: %v", err)
	}
	// The log's directory guard stats at most once per interval, so poll.
	deadline := time.Now().Add(10 * time.Second)
	for {
		health, err = s.HealthCheck()
		if err != nil {
			t.Fatalf("HealthCheck: %v", err)
		}
		if health.WAL != nil && health.WAL.Refusal != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("wal = %+v after the log directory was removed, want its refusal", health.WAL)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !strings.Contains(health.WAL.Refusal, "refuses writes") {
		t.Errorf("refusal %q does not say the log refuses writes", health.WAL.Refusal)
	}
}

// TestWALMetricsCollector: the ten series are node-wide, carry no labels, and
// are absent on a node with no log. The bytes-appended counter reads the
// append the test makes.
func TestWALMetricsCollector(t *testing.T) {
	want := []string{
		"hoarddb_wal_preallocated",
		"hoarddb_wal_flush_seconds",
		"hoarddb_wal_rotations_total",
		"hoarddb_wal_rotation_waits_total",
		"hoarddb_wal_rotation_wait_seconds_total",
		"hoarddb_wal_zero_fill_bytes_total",
		"hoarddb_wal_zero_fill_seconds_total",
		"hoarddb_wal_spare_fill_failures_total",
		"hoarddb_wal_spare_ready",
		"hoarddb_wal_bytes_appended_total",
	}
	gather := func(s *Server) map[string]*dto.MetricFamily {
		t.Helper()
		families, err := metricsGatherer(s).Gather()
		if err != nil {
			t.Fatalf("gathering: %v", err)
		}
		found := map[string]*dto.MetricFamily{}
		for _, family := range families {
			if strings.HasPrefix(family.GetName(), "hoarddb_wal_") {
				found[family.GetName()] = family
			}
		}
		return found
	}

	withLog, _ := startServiceLogging(t, func(cfg *config.Config) { cfg.Server.ReplicationFactor = 2 })
	if _, err := withLog.wal.Append("", uint8(transport.OpCreateDB), nil, []byte("x")); err != nil {
		t.Fatalf("appending: %v", err)
	}
	found := gather(withLog)
	if len(found) != len(want) {
		t.Errorf("%d hoarddb_wal_ series, want %d: %v", len(found), len(want), found)
	}
	for _, name := range want {
		family, ok := found[name]
		if !ok {
			t.Errorf("series %s is missing", name)
			continue
		}
		if len(family.GetMetric()) != 1 {
			t.Errorf("%s has %d series, want 1 (node-wide, never per bucket)", name, len(family.GetMetric()))
			continue
		}
		if labels := family.GetMetric()[0].GetLabel(); len(labels) != 0 {
			t.Errorf("%s has labels %v, want none", name, labels)
		}
	}
	if family := found["hoarddb_wal_bytes_appended_total"]; family != nil && len(family.GetMetric()) == 1 {
		if got := family.GetMetric()[0].GetCounter().GetValue(); got <= 0 {
			t.Errorf("hoarddb_wal_bytes_appended_total is %v after an append, want more than 0", got)
		}
	}
	if family := found["hoarddb_wal_flush_seconds"]; family != nil && len(family.GetMetric()) == 1 {
		histogram := family.GetMetric()[0].GetHistogram()
		if got := len(histogram.GetBucket()); got != storage.WALFlushBuckets {
			t.Errorf("flush histogram has %d buckets, want %d", got, storage.WALFlushBuckets)
		}
		if histogram.GetSampleCount() == 0 {
			t.Error("flush histogram counted no flush after a synced append")
		}
	}
	if gauge := found["hoarddb_wal_preallocated"]; gauge != nil && gauge.GetMetric()[0].GetGauge().GetValue() != 0 {
		t.Error("hoarddb_wal_preallocated is not 0 on a node pinned off")
	}

	withoutLog, _ := startServiceLogging(t, func(*config.Config) {})
	if found := gather(withoutLog); len(found) != 0 {
		t.Errorf("a node with no log exports %d hoarddb_wal_ series, want none", len(found))
	}
}

// TestWALPreallocateResolverTakesTheModeFromTheConfiguration: with nothing set the
// resolver reads the default, auto, and an off set in the YAML file is honoured
// and logs the off line. (The auto answer itself depends on the filesystem under
// the temp directory, so only the mode it started from is pinned here.)
func TestWALPreallocateResolverTakesTheModeFromTheConfiguration(t *testing.T) {
	t.Setenv("HOARDDB_WAL_PREALLOCATE", "")
	for _, tc := range []struct {
		yaml string
		want string
	}{
		{"", "auto"},
		{"storage:\n  wal_preallocate: \"off\"\n", "off"},
	} {
		path := ""
		if tc.yaml != "" {
			path = filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(tc.yaml), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		cfg, err := config.Load(path)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		got := resolveWALPreallocateFromConfig(cfg, t.TempDir())
		if got.Configured != tc.want {
			t.Errorf("yaml %q: configured %q, want %q", tc.yaml, got.Configured, tc.want)
		}
		if tc.want == "off" && got.startLine() != "write-ahead log segments are not preallocated (storage.wal_preallocate=off)" {
			t.Errorf("yaml %q: line %q", tc.yaml, got.startLine())
		}
	}
}
