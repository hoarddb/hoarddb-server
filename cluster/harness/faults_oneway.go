package harness

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// BlockOneWay cuts one direction between two named nodes while on is true and lifts exactly
// that direction when it is false: the dials from from to to are refused on every channel
// and the connections from dialed to to are severed, while the connections to dialed to
// from, and every other pair, are untouched. Heal also lifts it.
//
// A test that needs one direction of one channel of one pair only (the seeding S1 end-to-end
// test) combines this with DropChannel or DropReplies, which take a channel; this one cuts
// every channel of the direction. Raft dials pass through the same dialer
// (peer.Network.DialRaft), so a one-way cut from a follower to the leader also stops the
// follower's own Raft traffic to it, while the leader's connections to that follower go on.
func (c *Cluster) BlockOneWay(from, to string, on bool) { c.faults.BlockOneWay(from, to, on) }

// DialChannel dials node to as node from would on channel ch, through the fault layer and
// the pinned TLS, and returns the secured connection. It carries no channel handshake, so a
// test uses it to observe whether a dial is refused and whether the fault layer severs or
// keeps the connection, not to send traffic. The caller closes it.
func (c *Cluster) DialChannel(from, to string, ch transport.Channel) (net.Conn, error) {
	c.mu.Lock()
	src, dst := c.members[from], c.members[to]
	c.mu.Unlock()
	if src == nil || dst == nil {
		return nil, fmt.Errorf("harness: DialChannel %s to %s: no such node", from, to)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.faults.dialer(from, src.trust)(ctx, "", ch, dst.addr)
}
