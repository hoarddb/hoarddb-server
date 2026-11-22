package ringprobe

import (
	"fmt"

	"github.com/buraksezer/consistent"

	"github.com/hoarddb/hoarddb-server/ring"
)

// replicationFactor is the number of copies of a partition's data both probes
// assume exist. See doc.go's "Replication factor is an assumption this
// package makes, not a recorded one" for why 3 and what changing it does.
const replicationFactor = 3

// nodeIDStyle names one of the two node identifier shapes the review measured
// separately, to check whether the failover figures are an artifact of one
// naming scheme's hash distribution.
type nodeIDStyle int

const (
	hostnameIDs nodeIDStyle = iota
	hexIDs
)

// String names the style for test output; it is not consistent.Member.String.
func (s nodeIDStyle) String() string {
	switch s {
	case hostnameIDs:
		return "hostname"
	case hexIDs:
		return "hex"
	default:
		return fmt.Sprintf("nodeIDStyle(%d)", int(s))
	}
}

// buildMembers returns n ring.Node values named per style. The same (n, style)
// pair always produces the same members, so before/after snapshots in the same
// test compare like for like.
func buildMembers(n int, style nodeIDStyle) []ring.Node {
	nodes := make([]ring.Node, n)
	for i := 0; i < n; i++ {
		var id string
		switch style {
		case hostnameIDs:
			id = fmt.Sprintf("node-%02d.hoarddb.internal", i)
		case hexIDs:
			id = fmt.Sprintf("%016x", ring.XXHasher{}.Sum64([]byte(fmt.Sprintf("node-%02d", i))))
		}
		nodes[i] = ring.Node{ID: id, Addr: fmt.Sprintf("10.0.0.%d:9000", i+1)}
	}
	return nodes
}

// newConsistent builds a consistent-hash circle the way ring.Ring builds one
// (ring.Config: ring.XXHasher, the production virtual-node count and load
// factor), overriding only PartitionCount, which ring.Config() hardcodes at
// the package constant ring.PartitionCount. See doc.go for why this package
// cannot instead drive ring.Ring directly.
func newConsistent(members []ring.Node, partitionCount int) *consistent.Consistent {
	cfg := ring.Config()
	cfg.PartitionCount = partitionCount
	memberList := make([]consistent.Member, len(members))
	for i, m := range members {
		memberList[i] = m
	}
	return consistent.New(memberList, cfg)
}

// holderSet returns the member IDs in holders as a set, for set-membership
// comparisons between a before and after replica list.
func holderSet(holders []consistent.Member) map[string]struct{} {
	set := make(map[string]struct{}, len(holders))
	for _, h := range holders {
		set[h.String()] = struct{}{}
	}
	return set
}
