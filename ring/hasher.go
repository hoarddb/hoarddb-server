package ring

import (
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash/v2"
)

// XXHasher is the key hasher for the ring. xxHash64 is fast and has good
// distribution for short keys; the dependency is MIT-licensed, which keeps it
// compatible with hoardDB's BSL 1.1 licence.
type XXHasher struct{}

// Sum64 returns the xxHash64 of b.
func (h XXHasher) Sum64(b []byte) uint64 {
	return xxhash.Sum64(b)
}

// Config returns the consistent-hash configuration used by the ring.
//
// Note the naming collision with hoardDB's own replication factor:
// consistent.Config.ReplicationFactor is the number of virtual nodes each
// physical node occupies on the circle, not the number of data copies.
func Config() consistent.Config {
	return consistent.Config{
		Hasher:            XXHasher{},
		PartitionCount:    PartitionCount,
		ReplicationFactor: virtualNodes,
		Load:              loadFactor,
	}
}
