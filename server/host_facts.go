package server

import (
	"errors"
	"fmt"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// hostFactsTTL is how long one reading of the host's files is served. A
// health probe may arrive many times a second, and the CPU count, memory and
// load it reports do not change that fast: a reading is made at most once a
// second, on the first call after the last one expired, and every file is
// opened, read and closed inside that call. Nothing is held per request, per
// connection or per bucket.
const hostFactsTTL = time.Second

// cgroupRoot is where the cgroup filesystems are mounted on every
// distribution that mounts them at all (a container sees its own cgroup as
// this directory's root through a cgroup namespace).
const cgroupRoot = "/sys/fs/cgroup"

// hostReader reads one small file of the host. The real one is the
// filesystem; a test supplies a map, so the parsers run without the host.
type hostReader interface {
	ReadFile(name string) ([]byte, error)
}

// osHostFiles is the real host's files.
type osHostFiles struct{}

func (osHostFiles) ReadFile(name string) ([]byte, error) {
	content, err := os.ReadFile(name)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", name, err)
	}
	return content, nil
}

// hostSource is everything one reading depends on, so a test can replace each
// part.
type hostSource struct {
	files hostReader
	// memoryLimit is the cgroup memory-limit walk (capacity_cgroup.go), the
	// one the capacity guard already uses, so the two cannot disagree.
	memoryLimit func() (memoryLimit, error)
	numCPU      func() int
	now         func() time.Time
}

// realHostSource reads this machine.
var realHostSource = hostSource{
	files:       osHostFiles{},
	memoryLimit: hostMemoryLimit,
	numCPU:      runtime.NumCPU,
	now:         time.Now,
}

// hostFactsCache serves the host facts, re-reading them when the last reading
// is older than hostFactsTTL. The zero value reads this machine.
type hostFactsCache struct {
	// source is nil in production, which reads this machine.
	source *hostSource

	mu     sync.Mutex
	readAt time.Time
	facts  *HealthHost
}

// get returns the current reading. The result is shared and read-only.
func (c *hostFactsCache) get() *HealthHost {
	src := c.source
	if src == nil {
		src = &realHostSource
	}
	now := src.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.facts == nil || now.Sub(c.readAt) >= hostFactsTTL || now.Before(c.readAt) {
		c.facts = readHostFacts(*src)
		c.readAt = now
	}
	return c.facts
}

// parseMeminfo reads MemTotal and MemAvailable, in bytes, from /proc/meminfo.
// MemAvailable is what the kernel estimates can be handed out without
// swapping, which is a truer "free" than MemFree (page cache counts as
// available). Both are required: a missing one is an error, never a zero.
func parseMeminfo(content []byte) (total, available int64, err error) {
	var haveTotal, haveAvailable bool
	for _, line := range strings.Split(string(content), "\n") {
		if value, ok := strings.CutPrefix(line, "MemTotal:"); ok {
			total, err = parseKilobytes("/proc/meminfo", "MemTotal", value)
			if err != nil {
				return 0, 0, err
			}
			haveTotal = true
		}
		if value, ok := strings.CutPrefix(line, "MemAvailable:"); ok {
			available, err = parseKilobytes("/proc/meminfo", "MemAvailable", value)
			if err != nil {
				return 0, 0, err
			}
			haveAvailable = true
		}
	}
	switch {
	case !haveTotal:
		return 0, 0, errors.New("/proc/meminfo has no MemTotal line")
	case !haveAvailable:
		return 0, 0, errors.New("/proc/meminfo has no MemAvailable line")
	}
	return total, available, nil
}

// parseLoadavg reads the three load averages from /proc/loadavg
// ("0.52 0.41 0.30 1/345 12345").
func parseLoadavg(content []byte) (HealthLoad, error) {
	fields := strings.Fields(string(content))
	if len(fields) < 3 {
		return HealthLoad{}, fmt.Errorf("/proc/loadavg holds %q, want three load averages", strings.TrimSpace(string(content)))
	}
	var averages [3]float64
	for position := range averages {
		parsed, err := strconv.ParseFloat(fields[position], 64)
		if err != nil {
			return HealthLoad{}, fmt.Errorf("/proc/loadavg field %d %q is not a number: %w", position+1, fields[position], err)
		}
		averages[position] = parsed
	}
	return HealthLoad{OneMinute: averages[0], FiveMinutes: averages[1], FifteenMinutes: averages[2]}, nil
}

// cpusFromQuota converts a CFS quota and period (microseconds of CPU time per
// period) to whole CPUs, rounding up: a quota of one and a half CPUs lets the
// process run on two at once, for part of each period.
func cpusFromQuota(quota, period int64) int {
	return int((quota + period - 1) / period)
}

// parseCPUMaxV2 reads a cgroup v2 cpu.max ("<quota|max> <period>"). limited
// is false for "max".
func parseCPUMaxV2(content []byte) (cpus int, limited bool, err error) {
	fields := strings.Fields(string(content))
	if len(fields) == 0 {
		return 0, false, errors.New("cpu.max is empty")
	}
	if fields[0] == "max" {
		return 0, false, nil
	}
	if len(fields) != 2 {
		return 0, false, fmt.Errorf("cpu.max holds %q, want \"<quota|max> <period>\"", strings.TrimSpace(string(content)))
	}
	quota, quotaErr := strconv.ParseInt(fields[0], 10, 64)
	period, periodErr := strconv.ParseInt(fields[1], 10, 64)
	if quotaErr != nil || periodErr != nil || quota <= 0 || period <= 0 {
		return 0, false, fmt.Errorf("cpu.max holds %q, want positive integers", strings.TrimSpace(string(content)))
	}
	return cpusFromQuota(quota, period), true, nil
}

// cgroupAncestors lists a cgroup path and each of its parents, leaf first and
// "/" last: a limit on any level binds the process.
func cgroupAncestors(cgroupPath string) []string {
	var levels []string
	for current := path.Clean("/" + cgroupPath); ; current = path.Dir(current) {
		levels = append(levels, current)
		if current == "/" {
			return levels
		}
	}
}

// readInt reads a file holding one integer, reporting an absent or unreadable
// file as not ok: for the CPU limit a file that cannot be read is no limit,
// and the node still reports its CPUs.
func readInt(files hostReader, name string) (int64, bool) {
	content, err := files.ReadFile(name)
	if err != nil {
		return 0, false
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(string(content)), 10, 64)
	if err != nil {
		return 0, false
	}
	return parsed, true
}

// cgroupCPULimit is the smallest CPU quota, in whole CPUs, on any level of
// the process's cgroup, from cgroup v2 (cpu.max) or v1 (cpu.cfs_quota_us). A
// level whose file is absent or unreadable sets no limit: a cgroup path the
// mount does not show (a container without a cgroup namespace) is covered by
// the mount's own root level, which is the container's cgroup.
func cgroupCPULimit(files hostReader) (cpus int, limited bool) {
	listing, err := files.ReadFile("/proc/self/cgroup")
	if err != nil {
		return 0, false
	}
	offer := func(levelCPUs int) {
		if !limited || levelCPUs < cpus {
			cpus, limited = levelCPUs, true
		}
	}
	for _, line := range strings.Split(string(listing), "\n") {
		fields := strings.SplitN(line, ":", 3)
		if len(fields) != 3 {
			continue
		}
		hierarchy, controllers, cgroupPath := fields[0], fields[1], fields[2]
		switch {
		case hierarchy == "0" && controllers == "":
			for _, level := range cgroupAncestors(cgroupPath) {
				content, err := files.ReadFile(path.Join(cgroupRoot, level, "cpu.max"))
				if err != nil {
					continue
				}
				if levelCPUs, levelLimited, err := parseCPUMaxV2(content); err == nil && levelLimited {
					offer(levelCPUs)
				}
			}
		case containsController(controllers, "cpu"):
			for _, level := range cgroupAncestors(cgroupPath) {
				for _, mount := range []string{"cpu,cpuacct", "cpuacct,cpu", "cpu"} {
					dir := path.Join(cgroupRoot, mount, level)
					quota, haveQuota := readInt(files, path.Join(dir, "cpu.cfs_quota_us"))
					period, havePeriod := readInt(files, path.Join(dir, "cpu.cfs_period_us"))
					if haveQuota && havePeriod && quota > 0 && period > 0 {
						offer(cpusFromQuota(quota, period))
					}
				}
			}
		}
	}
	return cpus, limited
}

// containsController reports whether a v1 line's comma-separated controller
// list names controller exactly ("cpu" is not "cpuset").
func containsController(controllers, controller string) bool {
	for _, name := range strings.Split(controllers, ",") {
		if name == controller {
			return true
		}
	}
	return false
}

// usableCPUs is the CPU count the process may use and, when a cgroup quota
// lowered it, the count it was lowered from. numCPU is runtime.NumCPU, which
// already honours the CPU affinity mask but not a CFS quota.
func usableCPUs(files hostReader, numCPU int) (cpus, hostCPUs int) {
	quotaCPUs, limited := cgroupCPULimit(files)
	if limited && quotaCPUs < numCPU {
		return max(1, quotaCPUs), numCPU
	}
	return numCPU, 0
}

// statValue finds "<key> <n>" in a cgroup memory.stat file.
func statValue(content []byte, key string) (int64, bool) {
	for _, line := range strings.Split(string(content), "\n") {
		if value, ok := strings.CutPrefix(line, key+" "); ok {
			parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
			return parsed, err == nil
		}
	}
	return 0, false
}
