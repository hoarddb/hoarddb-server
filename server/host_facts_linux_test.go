//go:build linux

package server

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// factsSource builds a hostSource over fake files, with the memory-limit walk
// replaced by limit (it has its own tests in capacity_cgroup_test.go).
func factsSource(files *fakeHostFiles, numCPU int, limit func() (memoryLimit, error)) hostSource {
	return hostSource{
		files:       files,
		memoryLimit: limit,
		numCPU:      func() int { return numCPU },
		now:         func() time.Time { return time.Unix(1_000_000, 0) },
	}
}

func noLimit() (memoryLimit, error) {
	return memoryLimit{bytes: 16 * gib, source: limitMemTotalNoLimit}, nil
}

func TestHostFactsMemory(t *testing.T) {
	base := map[string]string{
		"/proc/meminfo": meminfoBody,
		"/proc/loadavg": "1.50 1.00 0.50 1/100 7\n",
	}
	with := func(extra map[string]string) map[string]string {
		merged := map[string]string{}
		for name, content := range base {
			merged[name] = content
		}
		for name, content := range extra {
			merged[name] = content
		}
		return merged
	}
	cases := []struct {
		name       string
		files      map[string]string
		limit      func() (memoryLimit, error)
		wantTotal  int64
		wantUsed   int64
		wantSource string
	}{
		{
			name:       "no cgroup limit: the host's MemTotal and MemAvailable",
			files:      with(nil),
			limit:      noLimit,
			wantTotal:  16 * gib,
			wantUsed:   12 * gib,
			wantSource: MemorySourceHost,
		},
		{
			name: "a v2 limit below MemTotal: the limit, and the cgroup's working set",
			files: with(map[string]string{
				"/sys/fs/cgroup/memory.current": fmt.Sprintf("%d\n", 3*gib/2),
				"/sys/fs/cgroup/memory.stat":    fmt.Sprintf("anon 1\ninactive_file %d\nactive_file 5\n", gib/2),
			}),
			limit: func() (memoryLimit, error) {
				return memoryLimit{bytes: 2 * gib, source: limitCgroupMax, cgroup: "/"}, nil
			},
			wantTotal:  2 * gib,
			wantUsed:   gib,
			wantSource: MemorySourceCgroup,
		},
		{
			name:  "a limit above MemTotal limits nothing",
			files: with(nil),
			limit: func() (memoryLimit, error) {
				return memoryLimit{bytes: 64 * gib, source: limitCgroupMax, cgroup: "/"}, nil
			},
			wantTotal:  16 * gib,
			wantUsed:   12 * gib,
			wantSource: MemorySourceHost,
		},
		{
			name: "a v1 limit, which the walk does not read",
			files: with(map[string]string{
				"/sys/fs/cgroup/memory/memory.limit_in_bytes": fmt.Sprintf("%d\n", gib),
				"/sys/fs/cgroup/memory/memory.usage_in_bytes": fmt.Sprintf("%d\n", gib*3/4),
				"/sys/fs/cgroup/memory/memory.stat":           fmt.Sprintf("cache 9\ntotal_inactive_file %d\n", gib/4),
			}),
			limit: func() (memoryLimit, error) {
				return memoryLimit{bytes: 16 * gib, source: limitMemTotalV1}, nil
			},
			wantTotal:  gib,
			wantUsed:   gib / 2,
			wantSource: MemorySourceCgroup,
		},
		{
			name:  "a v1 host with no limit set (the kernel's huge value)",
			files: with(map[string]string{"/sys/fs/cgroup/memory/memory.limit_in_bytes": "9223372036854771712\n"}),
			limit: func() (memoryLimit, error) {
				return memoryLimit{bytes: 16 * gib, source: limitMemTotalV1}, nil
			},
			wantTotal:  16 * gib,
			wantUsed:   12 * gib,
			wantSource: MemorySourceHost,
		},
		{
			name:       "a failed limit walk reports no memory rather than a guess",
			files:      with(nil),
			limit:      func() (memoryLimit, error) { return memoryLimit{}, errors.New("layout not understood") },
			wantTotal:  0,
			wantUsed:   0,
			wantSource: "",
		},
		{
			name:  "a cgroup limit whose usage file cannot be read keeps the limit and drops the usage",
			files: with(nil),
			limit: func() (memoryLimit, error) {
				return memoryLimit{bytes: 2 * gib, source: limitCgroupMax, cgroup: "/"}, nil
			},
			wantTotal:  2 * gib,
			wantUsed:   0,
			wantSource: MemorySourceCgroup,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			facts := readHostFacts(factsSource(&fakeHostFiles{files: tc.files}, 8, tc.limit))
			if facts.MemoryTotalBytes != tc.wantTotal || facts.MemoryUsedBytes != tc.wantUsed || facts.MemorySource != tc.wantSource {
				t.Errorf("memory = total %d used %d source %q; want total %d used %d source %q",
					facts.MemoryTotalBytes, facts.MemoryUsedBytes, facts.MemorySource, tc.wantTotal, tc.wantUsed, tc.wantSource)
			}
			if facts.CPUs != 8 {
				t.Errorf("CPUs = %d, want 8: a memory problem must not hide the CPU count", facts.CPUs)
			}
			if facts.Load == nil || facts.Load.OneMinute != 1.5 || facts.Load.FiveMinutes != 1.0 || facts.Load.FifteenMinutes != 0.5 {
				t.Errorf("Load = %+v, want 1.5 1.0 0.5", facts.Load)
			}
		})
	}
}

func TestHostFactsWithoutProcFiles(t *testing.T) {
	facts := readHostFacts(factsSource(&fakeHostFiles{files: map[string]string{}}, 4, noLimit))
	if facts.CPUs != 4 {
		t.Errorf("CPUs = %d, want 4", facts.CPUs)
	}
	if facts.MemoryTotalBytes != 0 || facts.MemoryUsedBytes != 0 || facts.MemorySource != "" || facts.Load != nil {
		t.Errorf("facts = %+v, want memory and load absent when /proc cannot be read", facts)
	}
}

// TestHostFactsAreCachedForASecond: a health probe may arrive many times a
// second; the files are read once per second, not once per probe.
func TestHostFactsAreCachedForASecond(t *testing.T) {
	files := &fakeHostFiles{files: map[string]string{"/proc/meminfo": meminfoBody, "/proc/loadavg": "0.10 0.10 0.10 1/1 1\n"}}
	clock := time.Unix(2_000_000, 0)
	source := factsSource(files, 2, noLimit)
	source.now = func() time.Time { return clock }
	cache := hostFactsCache{source: &source}

	first := cache.get()
	readsAfterFirst := files.totalReads()
	if readsAfterFirst == 0 {
		t.Fatal("the first call read no file")
	}
	clock = clock.Add(900 * time.Millisecond)
	if second := cache.get(); second != first || files.totalReads() != readsAfterFirst {
		t.Errorf("a call within the second read %d more files, want none", files.totalReads()-readsAfterFirst)
	}
	clock = clock.Add(200 * time.Millisecond)
	cache.get()
	if files.totalReads() == readsAfterFirst {
		t.Error("a call after the second still served the old reading")
	}
}
