package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-server/transport"
)

// errSenderNotOfConnection refuses an internode request that names a sender
// other than the member its connection speaks for.
var errSenderNotOfConnection = errors.New("the request names a sender other than the member this connection speaks for")

// refuseSenderNotOfConnection binds an internode connection to the member its
// first request names as the sender, and refuses every later request on it
// that names another (transport.Session.BindPeerNode). It runs after the
// request's cluster token is checked and before anything of the request is
// applied, in every handler whose request names its sender: gossip
// (GossipRequest.NodeID), a replication push (ReplPushRequest.FromNode), a
// phase of a membership change, a pull's start and a versioned read (each
// FromNode). A member opens its own connections and names only itself on
// them, so a connection that names two members is not a member's.
//
// One mutex-guarded compare per request on the connection's session: nothing
// per bucket. A request with no internode session (a call made in-process)
// is not bound.
func (c *Cluster) refuseSenderNotOfConnection(ctx context.Context, op transport.Opcode, claimed string) error {
	sess, ok := transport.SessionFromContext(ctx)
	if !ok || sess == nil || !sess.Peer {
		return nil
	}
	bound, same := sess.BindPeerNode(claimed)
	if same {
		return nil
	}
	c.logger.Warn("internode request refused: it names a sender other than the member its connection speaks for",
		"opcode", op, "connection_speaks_for", bound, "named_sender", boundedIdentifier(claimed), "addr", sess.RemoteAddr)
	return fmt.Errorf("%w: this connection speaks for %q and this %s request names %q, so nothing of it was applied. "+
		"A member names only itself on the connections it opens, so the sender is not the member it names: "+
		"check which hosts hold the cluster token (<data dir>/cluster.token) and that only members can reach this node's internode port",
		errSenderNotOfConnection, bound, op, boundedIdentifier(claimed))
}
