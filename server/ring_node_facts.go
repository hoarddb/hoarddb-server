package server

import (
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/version"
)

// ringNodeFacts is what describe_node reports that only the server process knows: the build,
// when the process started, and the machine, read through the cache the legacy health reply
// uses (hostFactsCache re-reads /proc at most once a second).
func (s *Server) ringNodeFacts() wire.RingNodeFacts {
	return wire.RingNodeFacts{Version: version.Version, StartedAt: startTime, Host: s.ringHost}
}

// ringHost reads the host part of describe_node, or nil when the machine could not be read.
// The process memory row is what the legacy banner drew: the memory capacity row's ratio of
// the memtable's memory limit.
func (s *Server) ringHost() *wire.RingAdminHost {
	var ratio *float64
	if capacity := s.capacity.healthCapacity(s.probes.Load().now()); capacity != nil {
		for _, row := range capacity.Rows {
			if row.Resource == resourceMemory {
				ratio = row.Ratio
				break
			}
		}
	}
	limit := s.memtable.healthMemtable(nil).LimitBytes
	return ringAdminHost(s.hostFacts.get(), ratio, limit)
}

// ringAdminHost converts the legacy host facts. A reading with no CPUs is a host that could
// not be read, and leaves the part out; every other member is left out when it is not known,
// so no row of a banner ever shows a zero it did not measure.
func ringAdminHost(host *HealthHost, memoryRatio *float64, memoryLimit int64) *wire.RingAdminHost {
	if host == nil || host.CPUs < 1 {
		return nil
	}
	converted := &wire.RingAdminHost{CPUs: int32(host.CPUs), HostCPUs: int32(host.HostCPUs)}
	if host.Load != nil {
		converted.Load = &wire.RingAdminLoad{One: host.Load.OneMinute, Five: host.Load.FiveMinutes, Fifteen: host.Load.FifteenMinutes}
	}
	if host.MemoryTotalBytes > 0 && host.MemorySource != "" {
		converted.MemTotal, converted.MemSource = host.MemoryTotalBytes, host.MemorySource
		converted.MemUsed = min(host.MemoryUsedBytes, host.MemoryTotalBytes)
	}
	if memoryRatio != nil && memoryLimit > 0 {
		converted.Process = &wire.RingAdminProcess{Used: int64(*memoryRatio * float64(memoryLimit)), Limit: memoryLimit}
	}
	return converted
}
