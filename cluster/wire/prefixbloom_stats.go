package wire

import "github.com/hoarddb/hoarddb-server/storage"

// PrefixBloomStats returns the node store's prefix bloom counters (prefix bloom spec §3.4):
// node-wide, per rule, zero at every open. They are not part of the operator surface: tests
// and the bench read them.
func (n *Node) PrefixBloomStats() storage.PrefixBloomStats { return n.kv.PrefixBloomStats() }
