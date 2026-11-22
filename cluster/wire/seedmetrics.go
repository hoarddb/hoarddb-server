package wire

import (
	"sync/atomic"

	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// SeedCounts is what a node reports of its seeding to /metrics: five numbers and
// no per-pair detail, so no slot, database or bucket can become a label.
type SeedCounts struct {
	Ready, Seeding, Retired  int    // held records by state; Retired counts the retired and the retiring ones
	BytesTotal, RetriesTotal uint64 // chunk bytes applied; moves to another source or restarts on a fresh cut
}

// seedCounters is what a Puller adds to as it works; its zero value is ready to use.
type seedCounters struct{ bytes, retries atomic.Uint64 }

// StateCounts counts the held records by state in one walk of the pairs under
// the lock. A pair with no record on disk is not held, so it is not counted.
// Retired includes retiring: both are copies this node has given up.
func (r *Reconciler) StateCounts() (ready, seeding, retired int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.pairs {
		if !p.present {
			continue
		}
		switch p.disk.State {
		case machine.SeedReady:
			ready++
		case machine.SeedSeeding:
			seeding++
		case machine.SeedRetired, machine.SeedRetiring:
			retired++
		}
	}
	return ready, seeding, retired
}

// Counters is the chunk bytes this puller has applied and the times it moved a pull to another
// source or restarted it on a fresh cut, since the process began.
func (p *Puller) Counters() (bytes, retries uint64) {
	return p.counters.bytes.Load(), p.counters.retries.Load()
}

// SeedCounts is the node's seeding counts for /metrics: the zero value when the node has no
// cluster or its seeding is not built yet.
func (n *Node) SeedCounts() SeedCounts {
	if n.cl == nil {
		return SeedCounts{}
	}
	parts := n.cl.seeding.Load()
	if parts == nil {
		return SeedCounts{}
	}
	ready, seeding, retired := parts.recon.StateCounts()
	bytesTotal, retriesTotal := parts.puller.Counters()
	return SeedCounts{Ready: ready, Seeding: seeding, Retired: retired, BytesTotal: bytesTotal, RetriesTotal: retriesTotal}
}
