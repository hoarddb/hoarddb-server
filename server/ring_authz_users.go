package server

import (
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// ringUserOpcode is the legacy user operation a user class stands for. Each needs user-manager
// (or root) with no database, as opRules gives the legacy user opcodes, so a grant on one database
// is denied: users are not a database's.
func ringUserOpcode(class wire.RingClass) (transport.Opcode, bool) {
	switch class {
	case wire.RingClassCreateUser, wire.RingClassAlterUser, wire.RingClassDropUser, wire.RingClassGrantRole,
		wire.RingClassRevokeRole, wire.RingClassShowUsers, wire.RingClassDescribeUser:
		return ringLegacyOpcode[class], true
	}
	return 0, false
}
