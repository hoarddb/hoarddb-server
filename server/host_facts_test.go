package server

import (
	"io/fs"
	"strings"
	"testing"
)

// fakeHostFiles is a map-backed hostReader: the host's /proc and /sys files as
// a test wants them. A path not in the map reads as absent, the way a file a
// kernel or a container does not provide does.
type fakeHostFiles struct {
	files map[string]string
	reads map[string]int
}

func (f *fakeHostFiles) ReadFile(name string) ([]byte, error) {
	if f.reads == nil {
		f.reads = map[string]int{}
	}
	f.reads[name]++
	content, present := f.files[name]
	if !present {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	return []byte(content), nil
}

func (f *fakeHostFiles) totalReads() int {
	var total int
	for _, count := range f.reads {
		total += count
	}
	return total
}

const (
	meminfoBody = "MemTotal:       16777216 kB\nMemFree:         1000000 kB\nMemAvailable:    4194304 kB\nBuffers: 1 kB\n"
)

func TestParseMeminfo(t *testing.T) {
	cases := []struct {
		name          string
		content       string
		total, avail  int64
		wantErrorPart string
	}{
		{name: "a normal file", content: meminfoBody, total: 16 * gib, avail: 4 * gib},
		{name: "no MemAvailable (kernel before 3.14)", content: "MemTotal: 1024 kB\n", wantErrorPart: "MemAvailable"},
		{name: "no MemTotal", content: "MemAvailable: 1024 kB\n", wantErrorPart: "MemTotal"},
		{name: "a unit that is not kB", content: "MemTotal: 1024 MB\nMemAvailable: 1 kB\n", wantErrorPart: "MemTotal"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			total, avail, err := parseMeminfo([]byte(tc.content))
			if tc.wantErrorPart != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrorPart) {
					t.Fatalf("parseMeminfo error = %v, want one naming %q", err, tc.wantErrorPart)
				}
				return
			}
			if err != nil || total != tc.total || avail != tc.avail {
				t.Errorf("parseMeminfo = %d, %d, %v; want %d, %d", total, avail, err, tc.total, tc.avail)
			}
		})
	}
}

func TestParseLoadavg(t *testing.T) {
	load, err := parseLoadavg([]byte("0.52 0.41 0.30 1/345 12345\n"))
	if err != nil {
		t.Fatalf("parseLoadavg: %v", err)
	}
	if load.OneMinute != 0.52 || load.FiveMinutes != 0.41 || load.FifteenMinutes != 0.30 {
		t.Errorf("parseLoadavg = %+v, want 0.52 0.41 0.30", load)
	}
	for _, bad := range []string{"", "0.52 0.41", "a b c d e"} {
		if _, err := parseLoadavg([]byte(bad)); err == nil {
			t.Errorf("parseLoadavg(%q) succeeded, want an error", bad)
		}
	}
}

func TestCPUsFromQuota(t *testing.T) {
	cases := []struct {
		quota, period int64
		want          int
	}{
		{200000, 100000, 2},
		{50000, 100000, 1},  // half a CPU still needs one thread to run on
		{250000, 100000, 3}, // a fraction rounds up: the process may use all of the third
		{100000, 100000, 1},
	}
	for _, tc := range cases {
		if got := cpusFromQuota(tc.quota, tc.period); got != tc.want {
			t.Errorf("cpusFromQuota(%d, %d) = %d, want %d", tc.quota, tc.period, got, tc.want)
		}
	}
}

func TestParseCPUMaxV2(t *testing.T) {
	cases := []struct {
		content string
		limited bool
		cpus    int
		wantErr bool
	}{
		{"max 100000\n", false, 0, false},
		{"150000 100000\n", true, 2, false},
		{"nonsense\n", false, 0, true},
		{"100000 0\n", false, 0, true},
	}
	for _, tc := range cases {
		cpus, limited, err := parseCPUMaxV2([]byte(tc.content))
		if (err != nil) != tc.wantErr || limited != tc.limited || cpus != tc.cpus {
			t.Errorf("parseCPUMaxV2(%q) = %d, %v, %v; want %d, %v, error %v", tc.content, cpus, limited, err, tc.cpus, tc.limited, tc.wantErr)
		}
	}
}

func TestUsableCPUs(t *testing.T) {
	cases := []struct {
		name         string
		files        map[string]string
		numCPU       int
		wantCPUs     int
		wantHostCPUs int
	}{
		{
			name:     "no cgroup files at all: the CPUs the process may run on",
			files:    map[string]string{},
			numCPU:   8,
			wantCPUs: 8,
		},
		{
			name:         "a container with a v2 quota of one and a half CPUs",
			files:        map[string]string{"/proc/self/cgroup": "0::/\n", "/sys/fs/cgroup/cpu.max": "150000 100000\n"},
			numCPU:       8,
			wantCPUs:     2,
			wantHostCPUs: 8,
		},
		{
			name: "the tightest level of a nested v2 cgroup wins",
			files: map[string]string{
				"/proc/self/cgroup":                  "0::/a/b.service\n",
				"/sys/fs/cgroup/a/b.service/cpu.max": "max 100000\n",
				"/sys/fs/cgroup/a/cpu.max":           "400000 100000\n",
				"/sys/fs/cgroup/cpu.max":             "max 100000\n",
			},
			numCPU:       16,
			wantCPUs:     4,
			wantHostCPUs: 16,
		},
		{
			name:     "a v2 quota above the CPU count limits nothing",
			files:    map[string]string{"/proc/self/cgroup": "0::/\n", "/sys/fs/cgroup/cpu.max": "800000 100000\n"},
			numCPU:   4,
			wantCPUs: 4,
		},
		{
			name: "a v2 cgroup path this mount does not show: the mount's own limit applies",
			files: map[string]string{
				"/proc/self/cgroup":      "0::/docker/abc\n",
				"/sys/fs/cgroup/cpu.max": "100000 100000\n",
			},
			numCPU:       4,
			wantCPUs:     1,
			wantHostCPUs: 4,
		},
		{
			name: "a v1 quota under cpu,cpuacct",
			files: map[string]string{
				"/proc/self/cgroup": "11:cpu,cpuacct:/docker/x\n4:memory:/docker/x\n",
				"/sys/fs/cgroup/cpu,cpuacct/docker/x/cpu.cfs_quota_us":  "300000\n",
				"/sys/fs/cgroup/cpu,cpuacct/docker/x/cpu.cfs_period_us": "100000\n",
			},
			numCPU:       8,
			wantCPUs:     3,
			wantHostCPUs: 8,
		},
		{
			name: "a v1 container view at the controller's root, quota -1 means none",
			files: map[string]string{
				"/proc/self/cgroup":                    "3:cpu:/\n",
				"/sys/fs/cgroup/cpu/cpu.cfs_quota_us":  "-1\n",
				"/sys/fs/cgroup/cpu/cpu.cfs_period_us": "100000\n",
			},
			numCPU:   2,
			wantCPUs: 2,
		},
		{
			name: "a v1 container view at the controller's root with a quota",
			files: map[string]string{
				"/proc/self/cgroup":                    "3:cpu:/\n",
				"/sys/fs/cgroup/cpu/cpu.cfs_quota_us":  "100000\n",
				"/sys/fs/cgroup/cpu/cpu.cfs_period_us": "100000\n",
			},
			numCPU:       8,
			wantCPUs:     1,
			wantHostCPUs: 8,
		},
		{
			name:     "an unreadable quota is no quota, never an error",
			files:    map[string]string{"/proc/self/cgroup": "0::/\n", "/sys/fs/cgroup/cpu.max": "garbage\n"},
			numCPU:   4,
			wantCPUs: 4,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cpus, hostCPUs := usableCPUs(&fakeHostFiles{files: tc.files}, tc.numCPU)
			if cpus != tc.wantCPUs || hostCPUs != tc.wantHostCPUs {
				t.Errorf("usableCPUs = %d, host %d; want %d, host %d", cpus, hostCPUs, tc.wantCPUs, tc.wantHostCPUs)
			}
		})
	}
}
