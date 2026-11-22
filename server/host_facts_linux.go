//go:build linux

package server

import "path"

// v1MemoryDir is where a cgroup v1 memory controller is mounted; a container
// sees its own cgroup as its root.
const v1MemoryDir = cgroupRoot + "/memory"

// readHostFacts reads this node's CPUs, memory and load. Each part that cannot
// be read is left absent; none is an error, because a health reply that
// refuses to answer over a missing /proc file would take the node out of
// service for a cosmetic reading.
func readHostFacts(src hostSource) *HealthHost {
	facts := &HealthHost{}
	facts.CPUs, facts.HostCPUs = usableCPUs(src.files, src.numCPU())
	fillMemory(src, facts)
	if content, err := src.files.ReadFile("/proc/loadavg"); err == nil {
		if load, err := parseLoadavg(content); err == nil {
			facts.Load = &load
		}
	}
	return facts
}

// fillMemory sets the memory total, used and source. The total is the cgroup
// limit when one smaller than the host's MemTotal applies, else MemTotal. A
// limit walk that failed leaves the memory absent: reporting MemTotal would
// overstate a node whose limit the walk could not read.
func fillMemory(src hostSource, facts *HealthHost) {
	content, err := src.files.ReadFile("/proc/meminfo")
	if err != nil {
		return
	}
	hostTotal, hostAvailable, err := parseMeminfo(content)
	if err != nil {
		return
	}
	limit, err := src.memoryLimit()
	if err != nil {
		return
	}
	if scope, limited := cgroupMemoryScope(src, limit, hostTotal); limited {
		facts.MemoryTotalBytes = scope.limitBytes
		facts.MemorySource = MemorySourceCgroup
		facts.MemoryUsedBytes = cgroupWorkingSet(src.files, scope)
		return
	}
	facts.MemoryTotalBytes = hostTotal
	facts.MemoryUsedBytes = hostTotal - hostAvailable
	facts.MemorySource = MemorySourceHost
}

// cgroupMemory is a cgroup memory limit and where its usage is read.
type cgroupMemory struct {
	limitBytes int64
	dir        string
	// usageFile and inactiveKey differ between cgroup v2 and v1.
	usageFile   string
	inactiveKey string
}

// cgroupMemoryScope says whether a cgroup memory limit below the host's total
// applies. The v2 walk reports its own limit; the v1 case is the one the walk
// does not read (limitMemTotalV1), so its limit file is read here.
func cgroupMemoryScope(src hostSource, limit memoryLimit, hostTotal int64) (cgroupMemory, bool) {
	if limit.source == limitMemTotalV1 {
		limitBytes, ok := readInt(src.files, path.Join(v1MemoryDir, "memory.limit_in_bytes"))
		if ok && limitBytes > 0 && limitBytes < hostTotal {
			return cgroupMemory{limitBytes: limitBytes, dir: v1MemoryDir, usageFile: "memory.usage_in_bytes", inactiveKey: "total_inactive_file"}, true
		}
		return cgroupMemory{}, false
	}
	if (limit.source == limitCgroupMax || limit.source == limitCgroupHigh) && limit.bytes > 0 && limit.bytes < hostTotal {
		return cgroupMemory{limitBytes: limit.bytes, dir: path.Join(cgroupRoot, limit.cgroup), usageFile: "memory.current", inactiveKey: "inactive_file"}, true
	}
	return cgroupMemory{}, false
}

// cgroupWorkingSet is the cgroup's memory in use without the reclaimable file
// cache, the figure container tooling shows as "memory used". It is 0 (absent)
// when the usage file cannot be read; a missing memory.stat only means the
// cache is not subtracted.
func cgroupWorkingSet(files hostReader, scope cgroupMemory) int64 {
	usage, ok := readInt(files, path.Join(scope.dir, scope.usageFile))
	if !ok {
		return 0
	}
	if stat, err := files.ReadFile(path.Join(scope.dir, "memory.stat")); err == nil {
		if inactive, found := statValue(stat, scope.inactiveKey); found && inactive <= usage {
			usage -= inactive
		}
	}
	return usage
}
