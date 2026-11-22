package server

import (
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	gib               = int64(1) << 30
	mib               = int64(1) << 20
	fixtureMemTotalKB = 32 * 1024 * 1024 // 32 GiB
	fixtureMemTotal   = fixtureMemTotalKB * 1024
)

// absentFile marks a cgroup limit file the fixture must not create.
const absentFile = "<absent>"

// cgLevel is one directory of a fake cgroup v2 tree. path is relative to the
// mount point ("" is the mount point itself).
type cgLevel struct {
	path string
	// trueRoot leaves out cgroup.type, as the kernel does on the true root.
	trueRoot bool
	// noMemory leaves memory out of cgroup.controllers; the level then gets
	// no memory.max or memory.high, as on a real host.
	noMemory bool
	// max and high are the limit files' contents. Empty means "max", so every
	// level that lists memory carries both files unless a case says otherwise.
	max, high string
}

// cgFixture is a fake /proc/self/cgroup, mountinfo, meminfo and cgroup2 tree
// under one t.TempDir().
type cgFixture struct {
	procCgroup string
	// mountRoot and mountPoint are the cgroup2 mountinfo line's fields 4 and
	// 5. noMount leaves the cgroup2 line out entirely.
	mountRoot, mountPoint string
	noMount               bool
	levels                []cgLevel
	meminfo               string
}

// v2Fixture is the common case: a unified host, the process in
// /parent.slice/leaf.scope, the tree mounted at /sys/fs/cgroup.
func v2Fixture(levels ...cgLevel) cgFixture {
	return cgFixture{
		procCgroup: "0::/parent.slice/leaf.scope\n",
		mountRoot:  "/",
		mountPoint: "/sys/fs/cgroup",
		levels:     levels,
	}
}

// standardLevels is the true root, parent.slice and leaf.scope, with the
// given limit-file contents on the two non-root levels.
func standardLevels(parentMax, parentHigh, leafMax, leafHigh string) []cgLevel {
	return []cgLevel{
		{path: "", trueRoot: true},
		{path: "parent.slice", max: parentMax, high: parentHigh},
		{path: "parent.slice/leaf.scope", max: leafMax, high: leafHigh},
	}
}

func (f cgFixture) build(t *testing.T) cgroupPaths {
	t.Helper()
	root := t.TempDir()
	proc := filepath.Join(root, "proc")
	mustMkdir(t, proc)

	mountinfo := "22 1 0:21 / /proc rw,nosuid,nodev,noexec,relatime shared:12 - proc proc rw\n"
	if !f.noMount {
		mountinfo += "30 23 0:26 " + f.mountRoot + " " + f.mountPoint +
			" rw,nosuid,nodev,noexec,relatime shared:4 - cgroup2 cgroup2 rw,nsdelegate,memory_recursiveprot\n"
	}
	meminfo := f.meminfo
	if meminfo == "" {
		meminfo = "MemTotal:       " + strconv.FormatInt(fixtureMemTotalKB, 10) + " kB\nMemFree:         1000 kB\n"
	}
	paths := cgroupPaths{
		procCgroup:    filepath.Join(proc, "cgroup"),
		procMountinfo: filepath.Join(proc, "mountinfo"),
		procMeminfo:   filepath.Join(proc, "meminfo"),
		root:          root,
	}
	mustWrite(t, paths.procCgroup, f.procCgroup)
	mustWrite(t, paths.procMountinfo, mountinfo)
	mustWrite(t, paths.procMeminfo, meminfo)

	for _, level := range f.levels {
		dir := filepath.Join(root, f.mountPoint, level.path)
		mustMkdir(t, dir)
		controllers := "cpu io memory pids"
		if level.noMemory {
			controllers = "cpu io pids"
		}
		mustWrite(t, filepath.Join(dir, "cgroup.controllers"), controllers+"\n")
		if level.trueRoot {
			continue
		}
		mustWrite(t, filepath.Join(dir, "cgroup.type"), "domain\n")
		if level.noMemory {
			continue
		}
		for name, content := range map[string]string{"memory.max": level.max, "memory.high": level.high} {
			if content == absentFile {
				continue
			}
			if content == "" {
				content = "max"
			}
			mustWrite(t, filepath.Join(dir, name), content+"\n")
		}
	}
	return paths
}

func mustMkdir(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
}

func mustWrite(t *testing.T, filePath, content string) {
	t.Helper()
	if err := os.WriteFile(filePath, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", filePath, err)
	}
}

// TestMemoryLimitIsTheMinimumOverAncestors (spec T-15, probe half) proves the
// limit is the smallest memory.max or memory.high over the process's cgroup
// and every visible ancestor, read through mountinfo rather than a hard-coded
// /sys/fs/cgroup.
func TestMemoryLimitIsTheMinimumOverAncestors(t *testing.T) {
	gibText := strconv.FormatInt(gib, 10)
	for _, tt := range []struct {
		name       string
		fixture    cgFixture
		wantBytes  int64
		wantSource memoryLimitSource
		wantCgroup string
		wantHard   int64
	}{
		{
			name:      "the parent's limit binds a leaf that reads max",
			fixture:   v2Fixture(standardLevels(gibText, "", "", "")...),
			wantBytes: gib, wantSource: limitCgroupMax, wantCgroup: "/parent.slice", wantHard: gib,
		},
		{
			name: "two limited ancestors: the smaller wins",
			fixture: cgFixture{
				procCgroup: "0::/a.slice/b.slice/leaf.scope\n", mountRoot: "/", mountPoint: "/sys/fs/cgroup",
				levels: []cgLevel{
					{path: "", trueRoot: true},
					{path: "a.slice", max: strconv.FormatInt(1*gib, 10)},
					{path: "a.slice/b.slice", max: strconv.FormatInt(2*gib, 10)},
					{path: "a.slice/b.slice/leaf.scope"},
				},
			},
			wantBytes: gib, wantSource: limitCgroupMax, wantCgroup: "/a.slice", wantHard: gib,
		},
		{
			name:      "the leaf lower than the parent: the leaf wins",
			fixture:   v2Fixture(standardLevels(strconv.FormatInt(2*gib, 10), "", strconv.FormatInt(512*mib, 10), "")...),
			wantBytes: 512 * mib, wantSource: limitCgroupMax, wantCgroup: "/parent.slice/leaf.scope", wantHard: 512 * mib,
		},
		{
			name: "a level without the memory controller is skipped, not a failure",
			fixture: v2Fixture(
				cgLevel{path: "", trueRoot: true},
				cgLevel{path: "parent.slice", max: gibText},
				cgLevel{path: "parent.slice/leaf.scope", noMemory: true},
			),
			wantBytes: gib, wantSource: limitCgroupMax, wantCgroup: "/parent.slice", wantHard: gib,
		},
		{
			name: "a non-standard mount point is still found through mountinfo",
			fixture: cgFixture{
				procCgroup: "0::/parent.slice/leaf.scope\n", mountRoot: "/", mountPoint: "/run/unusual/cg2",
				levels: standardLevels(gibText, "", "", ""),
			},
			wantBytes: gib, wantSource: limitCgroupMax, wantCgroup: "/parent.slice", wantHard: gib,
		},
		{
			name:      "OQ-12: memory.high below memory.max at one level is the limit",
			fixture:   v2Fixture(standardLevels("", "", strconv.FormatInt(2*gib, 10), gibText)...),
			wantBytes: gib, wantSource: limitCgroupHigh, wantCgroup: "/parent.slice/leaf.scope", wantHard: 2 * gib,
		},
		{
			name:      "OQ-12: the parent's memory.high below the leaf's memory.max wins",
			fixture:   v2Fixture(standardLevels("", gibText, strconv.FormatInt(2*gib, 10), "")...),
			wantBytes: gib, wantSource: limitCgroupHigh, wantCgroup: "/parent.slice", wantHard: 2 * gib,
		},
		{
			name:      "OQ-12: memory.high equal to memory.max is the hard limit (the tie rule)",
			fixture:   v2Fixture(standardLevels("", "", gibText, gibText)...),
			wantBytes: gib, wantSource: limitCgroupMax, wantCgroup: "/parent.slice/leaf.scope", wantHard: gib,
		},
		{
			name:      "OQ-12: only memory.high is set; no hard limit is recorded",
			fixture:   v2Fixture(standardLevels("", gibText, "", "")...),
			wantBytes: gib, wantSource: limitCgroupHigh, wantCgroup: "/parent.slice", wantHard: 0,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readMemoryLimit(tt.fixture.build(t))
			if err != nil {
				t.Fatalf("readMemoryLimit: %v", err)
			}
			if got.bytes != tt.wantBytes || got.source != tt.wantSource || got.cgroup != tt.wantCgroup || got.hardBytes != tt.wantHard {
				t.Fatalf("limit = %d from source %d on %q (hard %d), want %d from %d on %q (hard %d)",
					got.bytes, got.source, got.cgroup, got.hardBytes, tt.wantBytes, tt.wantSource, tt.wantCgroup, tt.wantHard)
			}
		})
	}

	// The ratio half: through the sampler, the parent's 1 GiB is the
	// denominator, so 900 MiB of anonymous memory is red. A leaf-only read
	// would divide by MemTotal (32 GiB) and read about 0.03, ok.
	t.Run("the sampler divides by the ancestor's limit", func(t *testing.T) {
		paths := v2Fixture(standardLevels(gibText, "", "", "")...).build(t)
		s := newTestServer(t)
		setCapacityProbes(t, s, func(p *capacityProbes) {
			greenProbes(p)
			p.anonMemory = func() (int64, error) { return 900 * mib, nil }
			p.memoryLimit = func() (memoryLimit, error) { return readMemoryLimit(paths) }
		})
		cs := newIdleSampler(t, s)
		cs.sampleNow()
		row := rowFor(t, cs, resourceMemory)
		if !row.hasRatio || row.ratio < 0.87 || row.ratio > 0.89 || row.state != stateRed {
			t.Fatalf("memory row %+v, want red at about 0.88", row)
		}
	})
}

// memoryWalkFailure is one way the cgroup walk must fail.
type memoryWalkFailure struct {
	name    string
	fixture func(t *testing.T) cgroupPaths
}

// memoryWalkFailureCases are spec T-16's (a)-(g) and OQ-12's (h)-(j). T-9
// runs them through the sampler too.
func memoryWalkFailureCases() []memoryWalkFailure {
	gibText := strconv.FormatInt(gib, 10)
	return []memoryWalkFailure{
		{name: "(a) the cgroup is under no cgroup2 mount root", fixture: func(t *testing.T) cgroupPaths {
			f := v2Fixture(standardLevels(gibText, "", "", "")...)
			f.mountRoot = "/docker/0123abcd"
			return f.build(t)
		}},
		{name: "(b) the leaf directory does not exist", fixture: func(t *testing.T) cgroupPaths {
			f := v2Fixture(cgLevel{path: "", trueRoot: true}, cgLevel{path: "parent.slice", max: gibText})
			return f.build(t)
		}},
		{name: "(c) memory.max is missing where memory is listed", fixture: func(t *testing.T) cgroupPaths {
			return v2Fixture(standardLevels(gibText, "", absentFile, "")...).build(t)
		}},
		{name: "(d) memory.max is unreadable", fixture: func(t *testing.T) cgroupPaths {
			if os.Geteuid() == 0 {
				t.Skip("root ignores file modes, so an unreadable file cannot be made")
			}
			paths := v2Fixture(standardLevels(gibText, "", "", "")...).build(t)
			unreadable := filepath.Join(paths.root, "sys/fs/cgroup/parent.slice/leaf.scope/memory.max")
			if err := os.Chmod(unreadable, 0o000); err != nil {
				t.Fatalf("chmod: %v", err)
			}
			return paths
		}},
		{name: "(e) memory.max is 0", fixture: func(t *testing.T) cgroupPaths {
			return v2Fixture(standardLevels(gibText, "", "0", "")...).build(t)
		}},
		{name: "(f) memory.max is garbage", fixture: func(t *testing.T) cgroupPaths {
			return v2Fixture(standardLevels(gibText, "", "garbage", "")...).build(t)
		}},
		{name: "(g) a directory without cgroup.type that is not the mount point", fixture: func(t *testing.T) cgroupPaths {
			return v2Fixture(
				cgLevel{path: "", trueRoot: true},
				cgLevel{path: "parent.slice", trueRoot: true},
				cgLevel{path: "parent.slice/leaf.scope", max: gibText},
			).build(t)
		}},
		{name: "(h) memory.high is missing where memory is listed", fixture: func(t *testing.T) cgroupPaths {
			return v2Fixture(standardLevels(gibText, "", "", absentFile)...).build(t)
		}},
		{name: "(i) memory.high is 0", fixture: func(t *testing.T) cgroupPaths {
			return v2Fixture(standardLevels(gibText, "", "", "0")...).build(t)
		}},
		{name: "(j) memory.high is garbage", fixture: func(t *testing.T) cgroupPaths {
			return v2Fixture(standardLevels(gibText, "", "", "garbage")...).build(t)
		}},
	}
}

// TestMemoryLimitReadFailureIsUnavailableNotMemTotal (spec T-16) proves every
// walk failure is an error, and never MemTotal: at the probe, and through the
// sampler, where memory reads unavailable with no limit series and MemTotal
// appears in no gauge.
func TestMemoryLimitReadFailureIsUnavailableNotMemTotal(t *testing.T) {
	s := newTestServer(t)
	for _, tt := range memoryWalkFailureCases() {
		t.Run(tt.name, func(t *testing.T) {
			paths := tt.fixture(t)
			got, err := readMemoryLimit(paths)
			if err == nil {
				t.Fatalf("readMemoryLimit = %d from source %d with no error; a failed walk must be an error", got.bytes, got.source)
			}
			if got.bytes == fixtureMemTotal {
				t.Fatalf("a failed walk returned MemTotal (%d) alongside its error %v", got.bytes, err)
			}

			setCapacityProbes(t, s, func(p *capacityProbes) {
				greenProbes(p)
				p.memoryLimit = func() (memoryLimit, error) { return readMemoryLimit(paths) }
			})
			cs := newIdleSampler(t, s)
			cs.sampleNow()
			if row := rowFor(t, cs, resourceMemory); row.state != stateUnavailable || row.hasRatio {
				t.Fatalf("memory row %+v, want unavailable with no ratio", row)
			}
			gathered := gatherSampler(t, cs)
			if len(gathered[familyMemLimitBytes]) != 0 {
				t.Fatal("memory_limit_bytes is published after a failed walk")
			}
			if _, ok := seriesValue(gathered, familyRatio, "resource", resourceMemory); ok {
				t.Fatal("the memory ratio is published after a failed walk")
			}
			for family, series := range gathered {
				for _, m := range series {
					if metricValue(m) == float64(fixtureMemTotal) {
						t.Fatalf("%s holds MemTotal (%d) after a failed walk", family, fixtureMemTotal)
					}
				}
			}
		})
	}
}

// TestMemoryLimitSourceIsAnnounced (spec T-17) proves the walk classifies
// where a MemTotal limit came from, including the hybrid host (amendment F-1)
// that mounts cgroup2 while v1 holds the memory controller, and that the log
// announces each source once, and again when it changes (OQ-12's (f)).
func TestMemoryLimitSourceIsAnnounced(t *testing.T) {
	gibText := strconv.FormatInt(gib, 10)
	hybridCgroup := "0::/user.slice/u.scope\n7:memory:/user.slice\n1:name=systemd:/user.slice\n"
	hybridLevels := []cgLevel{
		{path: "", trueRoot: true},
		{path: "user.slice", noMemory: true},
		{path: "user.slice/u.scope", noMemory: true},
	}
	for _, tt := range []struct {
		name       string
		fixture    cgFixture
		wantSource memoryLimitSource
		wantBytes  int64
	}{
		{
			name:       "(a) every level reads max and the mount is the true root",
			fixture:    v2Fixture(standardLevels("", "", "", "")...),
			wantSource: limitMemTotalNoLimit, wantBytes: fixtureMemTotal,
		},
		{
			name: "(b) the mount point has cgroup.type: a namespace root",
			fixture: cgFixture{
				procCgroup: "0::/\n", mountRoot: "/", mountPoint: "/sys/fs/cgroup",
				levels: []cgLevel{{path: ""}},
			},
			wantSource: limitMemTotalNamespaceRoot, wantBytes: fixtureMemTotal,
		},
		{
			name: "(c) no 0:: line and no cgroup2 mount: cgroup v1 only",
			fixture: cgFixture{
				procCgroup: "7:memory:/user.slice\n1:name=systemd:/user.slice\n", noMount: true,
			},
			wantSource: limitMemTotalV1, wantBytes: fixtureMemTotal,
		},
		{
			name: "(e) F-1: a hybrid host whose memory controller is on v1",
			fixture: cgFixture{
				procCgroup: hybridCgroup, mountRoot: "/", mountPoint: "/sys/fs/unified",
				levels: hybridLevels,
			},
			wantSource: limitMemTotalV1, wantBytes: fixtureMemTotal,
		},
		{
			name: "(e) F-1 holds whatever the v2 tree says, even a limit",
			fixture: cgFixture{
				procCgroup: hybridCgroup, mountRoot: "/", mountPoint: "/sys/fs/unified",
				levels: []cgLevel{
					{path: "", trueRoot: true},
					{path: "user.slice", max: gibText},
					{path: "user.slice/u.scope"},
				},
			},
			wantSource: limitMemTotalV1, wantBytes: fixtureMemTotal,
		},
		{
			name: "(e') control: without the memory line, name=systemd does not make it v1",
			fixture: cgFixture{
				procCgroup: "0::/user.slice/u.scope\n1:name=systemd:/user.slice\n", mountRoot: "/", mountPoint: "/sys/fs/unified",
				levels: hybridLevels,
			},
			wantSource: limitMemTotalNoLimit, wantBytes: fixtureMemTotal,
		},
		{
			name: "(e') control: a v1 line naming memory only as a substring does not match",
			fixture: cgFixture{
				procCgroup: "0::/user.slice/u.scope\n5:memory_extra,name=memoryish:/user.slice\n", mountRoot: "/", mountPoint: "/sys/fs/unified",
				levels: hybridLevels,
			},
			wantSource: limitMemTotalNoLimit, wantBytes: fixtureMemTotal,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readMemoryLimit(tt.fixture.build(t))
			if err != nil {
				t.Fatalf("readMemoryLimit: %v", err)
			}
			if got.source != tt.wantSource || got.bytes != tt.wantBytes {
				t.Fatalf("limit = %d from source %d, want %d from source %d", got.bytes, got.source, tt.wantBytes, tt.wantSource)
			}
		})
	}

	// The log half: each classification is announced, once, in words an
	// operator can act on.
	s := newTestServer(t)
	announce := func(t *testing.T, paths cgroupPaths, clock *fakeClock) (*capacitySampler, *capturedLog, func(cgroupPaths)) {
		t.Helper()
		current := paths
		var mu sync.Mutex
		setCapacityProbes(t, s, func(p *capacityProbes) {
			greenProbes(p)
			p.now = clock.Now
			p.memoryLimit = func() (memoryLimit, error) {
				mu.Lock()
				defer mu.Unlock()
				return readMemoryLimit(current)
			}
		})
		cs, logs := newLoggedSampler(t, s)
		swap := func(next cgroupPaths) {
			mu.Lock()
			defer mu.Unlock()
			current = next
		}
		return cs, logs, swap
	}

	t.Run("log (a) no limit anywhere: one INFO names the host's MemTotal", func(t *testing.T) {
		cs, logs, _ := announce(t, v2Fixture(standardLevels("", "", "", "")...).build(t), newFakeClock())
		cs.sampleNow()
		cs.sampleNow()
		if got := logs.count(slog.LevelInfo, "host's MemTotal", "no cgroup memory limit is set"); got != 1 {
			t.Fatalf("%d MemTotal INFOs, want 1: %q", got, logs.messages(slog.LevelInfo))
		}
	})
	t.Run("log (b) a namespace root: the INFO says a limit above the container is not seen", func(t *testing.T) {
		f := cgFixture{procCgroup: "0::/\n", mountRoot: "/", mountPoint: "/sys/fs/cgroup", levels: []cgLevel{{path: ""}}}
		cs, logs, _ := announce(t, f.build(t), newFakeClock())
		cs.sampleNow()
		if got := logs.count(slog.LevelInfo, "above the container is not seen"); got != 1 {
			t.Fatalf("%d namespace-root INFOs, want 1: %q", got, logs.messages(slog.LevelInfo))
		}
	})
	t.Run("log (c) cgroup v1: a WARN, repeated hourly", func(t *testing.T) {
		clock := newFakeClock()
		f := cgFixture{procCgroup: "7:memory:/user.slice\n", noMount: true}
		cs, logs, _ := announce(t, f.build(t), clock)
		cs.sampleNow()
		clock.Advance(capacitySampleInterval)
		cs.sampleNow()
		if got := logs.count(slog.LevelWarn, "cgroup v1"); got != 1 {
			t.Fatalf("%d v1 WARNs in the first minute, want 1", got)
		}
		clock.Advance(time.Hour)
		cs.sampleNow()
		if got := logs.count(slog.LevelWarn, "cgroup v1"); got != 2 {
			t.Fatalf("%d v1 WARNs after an hour, want 2", got)
		}
	})
	t.Run("log (d) unchanged for 10 samples: no further INFO; changed: one", func(t *testing.T) {
		cs, logs, swap := announce(t, v2Fixture(standardLevels("", "", "", "")...).build(t), newFakeClock())
		for range 10 {
			cs.sampleNow()
		}
		if got := logs.count(slog.LevelInfo, "memory limit"); got != 1 {
			t.Fatalf("10 unchanged samples wrote %d memory-limit INFOs, want 1", got)
		}
		swap(v2Fixture(standardLevels(gibText, "", "", "")...).build(t))
		cs.sampleNow()
		cs.sampleNow()
		if got := logs.count(slog.LevelInfo, "memory limit 1.0 GiB from memory.max on cgroup /parent.slice"); got != 1 {
			t.Fatalf("a changed limit wrote %d INFOs naming it, want 1: %q", got, logs.messages(slog.LevelInfo))
		}
	})
	t.Run("log (e) F-1: the hybrid host writes the v1 WARN, not the INFO", func(t *testing.T) {
		f := cgFixture{procCgroup: hybridCgroup, mountRoot: "/", mountPoint: "/sys/fs/unified", levels: hybridLevels}
		cs, logs, _ := announce(t, f.build(t), newFakeClock())
		cs.sampleNow()
		if got := logs.count(slog.LevelWarn, "cgroup v1"); got != 1 {
			t.Fatalf("the hybrid host wrote %d v1 WARNs, want 1", got)
		}
		if got := logs.count(slog.LevelInfo, "memory limit"); got != 0 {
			t.Fatalf("the hybrid host wrote a memory-limit INFO: %q", logs.messages(slog.LevelInfo, "memory limit"))
		}
	})
	t.Run("log (f) memory.high is announced as a throttle line; memory.max below it is announced again", func(t *testing.T) {
		cs, logs, swap := announce(t, v2Fixture(standardLevels(strconv.FormatInt(4*gib, 10), gibText, "", "")...).build(t), newFakeClock())
		cs.sampleNow()
		highs := logs.messages(slog.LevelInfo, "memory.high", "throttle line")
		if len(highs) != 1 || !strings.Contains(highs[0], "The hard limit is 4.0 GiB from memory.max on cgroup /parent.slice") {
			t.Fatalf("memory.high INFOs %q, want one naming the throttle line and the hard limit", highs)
		}
		if strings.Contains(highs[0], "full") {
			t.Fatalf("the memory.high INFO says full: %s", highs[0])
		}
		swap(v2Fixture(standardLevels(strconv.FormatInt(512*mib, 10), gibText, "", "")...).build(t))
		cs.sampleNow()
		if got := logs.count(slog.LevelInfo, "memory limit 512.0 MiB from memory.max on cgroup /parent.slice"); got != 1 {
			t.Fatalf("lowering memory.max below memory.high wrote %d INFOs naming memory.max, want 1: %q", got, logs.messages(slog.LevelInfo))
		}
	})
	t.Run("log (f') memory.high with no memory.max names MemTotal as the hard limit", func(t *testing.T) {
		cs, logs, _ := announce(t, v2Fixture(standardLevels("", gibText, "", "")...).build(t), newFakeClock())
		cs.sampleNow()
		if got := logs.count(slog.LevelInfo, "The hard limit is the host's MemTotal (32.0 GiB)"); got != 1 {
			t.Fatalf("memory.high with no memory.max: %q", logs.messages(slog.LevelInfo))
		}
	})
}

// TestMemoryLimitMissingMemTotalIsAnError proves the one legitimate use of
// MemTotal fails when MemTotal cannot be read, rather than reporting 0.
func TestMemoryLimitMissingMemTotalIsAnError(t *testing.T) {
	f := v2Fixture(standardLevels("", "", "", "")...)
	f.meminfo = "MemFree: 1000 kB\n"
	if got, err := readMemoryLimit(f.build(t)); err == nil {
		t.Fatalf("readMemoryLimit with no MemTotal = %d, nil; want an error", got.bytes)
	}
}
