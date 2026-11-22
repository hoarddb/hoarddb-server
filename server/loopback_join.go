package server

import (
	"fmt"

	"github.com/hoarddb/hoarddb-server/config"
)

// ownMembershipAddr is the address this node holds in the current
// membership, which is the address its peers dial. It is not Cluster.LocalAddr:
// a node listening on 0.0.0.0 with HOARDDB_NODE_ID and id@address seeds has
// self.Addr normalised to 127.0.0.1 on first boot while the membership lists
// it at its real address.
func (c *Cluster) ownMembershipAddr() string {
	_, members := c.epochMembers()
	for _, member := range members {
		if member.ID == c.self.ID {
			return member.Addr
		}
	}
	return c.self.Addr
}

// refuseLoopbackJoin refuses a join that this node cannot complete. A node
// whose membership address is loopback would commit an epoch naming itself as
// 127.0.0.1:<port>; the remote node refuses to adopt a membership with no
// member it can reach, and the cluster is stuck at an epoch that
// `remove-node` cannot undo for want of a majority. Refusing before the
// proposal leaves the membership untouched.
func refuseLoopbackJoin(cluster *Cluster, newcomerAddr string) error {
	ownAddr := cluster.ownMembershipAddr()
	if !config.ListensOnLoopbackOnly(ownAddr) || config.ListensOnLoopbackOnly(newcomerAddr) {
		return nil
	}
	return fmt.Errorf(`cannot add %s: this node is a cluster member at %s, which only this machine can reach
       the new node would be told to find this cluster member on %s and could not reach it
       to fix it, set HOARDDB_LISTEN on this node to its real address (for example HOARDDB_LISTEN=192.0.2.1:4433),
       replace a loopback address for this node in HOARDDB_SEED_NODES with that address,
       stop the node, remove membership.json from its data directory (a node that has already joined keeps the
       old address there and ignores the setting), start it again, then run join again`,
		newcomerAddr, ownAddr, ownAddr)
}
