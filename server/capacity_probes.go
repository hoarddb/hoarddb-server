package server

import (
	"errors"
	"sync/atomic"
	"time"
)

// capacityProbes is the set of raw readings behind the join guard and the
// capacity sampler. Each Server holds one behind an atomic pointer
// (Server.probes), so a test can change one node's readings while that node's
// sampler is running, and make one node of several red.
//
// Every member returns a raw reading, a raw limit and an error, and never a
// fallback value with a nil error (node-capacity-spec.md §6.3). Value checks
// on what a member returns (a limit of zero or less, a zero-block filesystem)
// are made downstream, in the sampler, so an injected fixture reaches them.
//
// Read with Server.probes.Load(). Never mutate a loaded value; replace it with
// a new one.
type capacityProbes struct {
	// diskStatfs is the one statfs the guard's percentage, the refusal text
	// and the disk gauges all come from (node-capacity-spec.md §3.3).
	diskStatfs func(dir string) (diskStat, error)
	// openFDs is the open descriptor count and the RLIMIT_NOFILE soft limit.
	openFDs func() (open, limit int64, err error)
	// mappings is the memory mapping count and vm.max_map_count.
	mappings func() (count, limit int64, err error)
	// anonMemory is RssAnon + VmSwap, in bytes.
	anonMemory func() (int64, error)
	// memoryLimit is the cgroup walk (capacity_cgroup.go).
	memoryLimit func() (memoryLimit, error)
	// storeSize is the node store's LSM and value-log bytes.
	storeSize func() (lsm, vlog int64, err error)
	// walBytes is the WAL's segment bytes. present is false on a node with no
	// WAL (single-node mode), which is not a failure.
	walBytes func() (bytes int64, present bool, err error)
	// now is the sampler's clock, and the clock status measures a sample's
	// age by, so a test can move time without waiting for it.
	now func() time.Time
}

// errStoreSizeUnknown is storeSize's answer for a store that cannot report
// its size (a test wrapper, say). It is an error, never a zero.
var errStoreSizeUnknown = errors.New("the node store does not report its size")

// diskStatfsForTesting, when non-nil, replaces the host statfs in every
// capacity probe set built while it is set. It is a pointer so a test can
// restore the previous value. Nil in production; unset, it costs one atomic
// load per server start.
var diskStatfsForTesting atomic.Pointer[func(dir string) (diskStat, error)]

// pinnedDiskStatfs is the statfs member a new probe set gets: the pinned test
// override when there is one, the host's statfs otherwise.
func pinnedDiskStatfs() func(dir string) (diskStat, error) {
	if override := diskStatfsForTesting.Load(); override != nil {
		return *override
	}
	return diskStatfs
}

// PinDiskUsageForTesting makes every server started while the returned restore
// is not called report the disk holding its data directory as pctUsed percent
// full (0-100). It is the seam the cli golden transcripts use: their data
// directory is a t.TempDir() on a filesystem they do not control, so the disk
// row's state and the node's health must not follow the host. Nothing in
// production calls it.
func PinDiskUsageForTesting(pctUsed int) (restore func()) {
	fixed := func(string) (diskStat, error) {
		return diskStat{Blocks: 100, Bavail: uint64(100 - pctUsed), Frsize: 4096}, nil
	}
	previous := diskStatfsForTesting.Swap(&fixed)
	return func() { diskStatfsForTesting.Store(previous) }
}

// defaultCapacityProbes returns the probes that read the real host. s is the
// server whose store and WAL the storage members measure; both are open
// before NewService stores these.
func defaultCapacityProbes(s *Server) *capacityProbes {
	return &capacityProbes{
		diskStatfs:  pinnedDiskStatfs(),
		openFDs:     readOpenFDs,
		mappings:    readMappings,
		anonMemory:  hostAnonMemory,
		memoryLimit: hostMemoryLimit,
		storeSize: func() (int64, int64, error) {
			sized, ok := s.kv.(interface{ Size() (int64, int64, error) })
			if !ok {
				return 0, 0, errStoreSizeUnknown
			}
			return sized.Size()
		},
		walBytes: func() (int64, bool, error) {
			if s.wal == nil {
				return 0, false, nil
			}
			segmentBytes, err := s.wal.SegmentBytes()
			if err != nil {
				return 0, true, err
			}
			return segmentBytes, true, nil
		},
		now: time.Now,
	}
}
