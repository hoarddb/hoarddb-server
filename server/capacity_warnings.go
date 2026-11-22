package server

import "fmt"

// The texts below are the capacity channel's words: what the log says when a
// resource crosses its line, cannot be read, or recovers. Each red text names
// the action for its resource (AGENTS.md rule 3). The disk texts are
// node-capacity-spec.md §7.2's, verbatim; the memory texts are build doc
// §0.2's, which keep "full" and "throttled" apart by the file that set the
// limit. the "capacity" user-docs page quotes them, so a change here is a change
// there.

// formatRatio prints a ratio the way every capacity line does.
func formatRatio(ratio float64) string { return fmt.Sprintf("%.2f", ratio) }

// formatBytesIEC prints a byte count for a person: 2.0 GiB, 512.0 MiB.
func formatBytesIEC(n int64) string {
	const (
		kib = 1 << 10
		mib = 1 << 20
		gib = 1 << 30
	)
	switch {
	case n >= gib:
		return fmt.Sprintf("%.1f GiB", float64(n)/gib)
	case n >= mib:
		return fmt.Sprintf("%.1f MiB", float64(n)/mib)
	case n >= kib:
		return fmt.Sprintf("%.1f KiB", float64(n)/kib)
	}
	return fmt.Sprintf("%d B", n)
}

// The subjects the disk WARNs open with: the data directory's filesystem,
// worded as it always was, and a blob directory on a filesystem of its own,
// named by path so the operator frees space on the right volume.
const (
	diskSubjectData = "disk on this node"
	diskSubjectBlob = "the blob directory %s on this node"
)

// diskRedText is the red disk WARN. With the join guard off the node never
// votes against a join or refuses a copy, so the sentences about votes,
// copies and joins go.
func diskRedText(subject string, ratio, threshold float64, guardOn bool) string {
	where := "on this node"
	if subject != diskSubjectData {
		where = "on that filesystem"
	}
	text := fmt.Sprintf("%s is at %s of its filesystem (high-water mark %s): free space %s or grow the volume",
		subject, formatRatio(ratio), formatRatio(threshold), where)
	if guardOn {
		text += ". At this mark the node votes against joins and copies in no bucket it does not already hold. In a cluster of three or more a majority can still admit a join, and a bucket the change places on this node then refuses writes, or is a copy short, until space is freed. Do not grow the cluster while this node is red"
	}
	return text
}

// diskAmberText is the early disk WARN, written only with the guard on.
func diskAmberText(subject string, ratio, threshold float64) string {
	return fmt.Sprintf("%s is at %s, approaching the high-water mark %s: free space or grow the volume now. At the mark this node votes against joins and copies in no bucket it does not already hold",
		subject, formatRatio(ratio), formatRatio(threshold))
}

// fdsRedText is the red descriptor WARN: the limit is a kernel setting, and
// raising it is the fix.
func fdsRedText(ratio float64, open, limit int64) string {
	return fmt.Sprintf("open descriptors on this node are at %s of RLIMIT_NOFILE (%d of %d): raise the process's RLIMIT_NOFILE (LimitNOFILE= in systemd, --ulimit nofile= in Docker). It is a kernel limit, and raising it is free. If the limit is already very high and still close, the node holds a lot of data: read the disk ratio",
		formatRatio(ratio), open, limit)
}

// mappingsRedText is the red mappings WARN.
func mappingsRedText(ratio float64, count, limit int64) string {
	return fmt.Sprintf("memory mappings on this node are at %s of vm.max_map_count (%d of %d): raise vm.max_map_count (sysctl -w vm.max_map_count=262144, and persist it in /etc/sysctl.d/). The kernel default of 65530 is low for any mmap-heavy database. If it is already very high and still close, the node holds a lot of data: read the disk ratio",
		formatRatio(ratio), count, limit)
}

// memoryActions are the actions every memory WARN ends with.
const memoryActions = "If a rebalance is running, wait for it. If the block or index caches were raised, lower them. If the load is traffic-driven, add a node."

// memoryRedText is the red memory WARN. Against memory.high the node is being
// throttled, not killed, and the text says so; against memory.max or MemTotal
// it is near the point where it swaps heavily or is killed.
func memoryRedText(ratio float64, limit memoryLimit) string {
	if limit.source == limitCgroupHigh {
		where := fmt.Sprintf("memory on this node is at %s of memory.high %s (cgroup %s)",
			formatRatio(ratio), formatBytesIEC(limit.bytes), limit.cgroup)
		state := ", the line where the kernel starts throttling this process and reclaiming its memory."
		if ratio >= 1 {
			state = ": it is past the throttle line, and the kernel is holding it there by reclaiming and stalling its allocations."
		}
		return where + state + " The node is being slowed, not stopped: memory.high is not a hard limit, and the process is not killed there. If memory.high was set lower than you meant (MemoryHigh= in systemd), raise it. " + memoryActions
	}
	where := fmt.Sprintf("memory on this node is at %s of its memory limit %s (%s)",
		formatRatio(ratio), formatBytesIEC(limit.bytes), memoryLimitOrigin(limit))
	state := ": anonymous memory, resident plus swapped, is near the point where this process swaps heavily or is killed. "
	if ratio >= 1 {
		state = ": it is over the limit and swapping. "
	}
	return where + state + memoryActions + " If the limit is below the host's memory, raise it."
}

// memoryLimitOrigin names where a hard (non-throttle) limit came from.
func memoryLimitOrigin(limit memoryLimit) string {
	if limit.source == limitCgroupMax {
		return "memory.max on cgroup " + limit.cgroup
	}
	return "the host's MemTotal"
}

// memoryLimitText is the INFO that announces the memory ratio's denominator,
// at the first sample and whenever its source, cgroup or value changes, so
// the denominator never changes silently.
func memoryLimitText(limit memoryLimit) string {
	value := formatBytesIEC(limit.bytes)
	switch limit.source {
	case limitCgroupMax:
		return fmt.Sprintf("memory limit %s from memory.max on cgroup %s", value, limit.cgroup)
	case limitCgroupHigh:
		text := fmt.Sprintf("memory limit %s from memory.high on cgroup %s: this is a throttle line, not a hard limit. Past it the kernel slows this process and reclaims its memory; it does not kill it there. ", value, limit.cgroup)
		if limit.hardBytes > 0 {
			return text + fmt.Sprintf("The hard limit is %s from memory.max on cgroup %s", formatBytesIEC(limit.hardBytes), limit.hardCgroup)
		}
		return text + fmt.Sprintf("The hard limit is the host's MemTotal (%s): no memory.max is set on any cgroup this process can see", formatBytesIEC(limit.hostMemTotal))
	case limitMemTotalNamespaceRoot:
		return fmt.Sprintf("memory limit is the host's MemTotal (%s): no cgroup memory limit is visible from this cgroup namespace; a limit set above the container is not seen", value)
	}
	return fmt.Sprintf("memory limit is the host's MemTotal (%s): no cgroup memory limit is set on this process's cgroup or any ancestor it can see", value)
}

// memoryLimitV1Text is the WARN for a host whose memory controller is cgroup
// v1's, which this probe does not read (spec §3.2 step 5, verbatim).
const memoryLimitV1Text = "memory limit is the host's MemTotal because this host uses cgroup v1, whose limits hoardDB does not read; if this process runs under a v1 memory limit, the memory ratio understates how full the node is"

// probeFailureText is the WARN for entering unavailable.
func probeFailureText(resource string, readErr error, whatToCheck string) string {
	return fmt.Sprintf("capacity for %s can no longer be read: %v. The %s ratio is not being reported; %s",
		resource, readErr, resource, whatToCheck)
}

// readAgainText is the INFO for leaving unavailable.
func readAgainText(resource string, ratio float64) string {
	return fmt.Sprintf("capacity for %s can be read again: %s", resource, formatRatio(ratio))
}

// recoveryText is the INFO for leaving red.
func recoveryText(resource string, ratio, threshold float64) string {
	return fmt.Sprintf("capacity for %s is back below its threshold: %s of %s", resource, formatRatio(ratio), formatRatio(threshold))
}

// dataFailureText is the WARN for a data-bytes part that cannot be read.
func dataFailureText(part string, readErr error) string {
	return fmt.Sprintf("capacity data bytes for %s can no longer be read: %v. hoarddb_node_data_bytes for %s is not being reported", part, readErr, part)
}

// metricsDisabledText is the startup INFO when /metrics is off (spec §7.1
// item 5): the gauges are not exported, but the log and status still work.
const metricsDisabledText = "capacity gauges are not exported (HOARDDB_METRICS_ENABLED is false); capacity warnings still go to this log and to hoardDB status"
