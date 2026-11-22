package server

import (
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// ringMembershipOpcode is the legacy cluster operation a membership class stands for: adding a
// node (join, promote, replace_node) is the legacy cluster add, removing one (leave,
// remove_node) the legacy cluster remove.
func ringMembershipOpcode(class wire.RingClass) (transport.Opcode, bool) {
	switch class {
	case wire.RingClassJoin, wire.RingClassPromote, wire.RingClassReplaceNode:
		return transport.OpClusterAdd, true
	case wire.RingClassLeave, wire.RingClassRemoveNode:
		return transport.OpClusterRemove, true
	}
	return 0, false
}
