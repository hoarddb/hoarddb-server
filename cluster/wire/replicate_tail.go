package wire

import (
	"context"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// ShipTail queues the tail record of a pair this node owns for every copy, once, from the
// position and log cursor the pair holds in memory. An owner that restarted lost what it had
// sent to each copy, a node that just became owner never sent anything, and a copy that was
// away missed whatever came while it was: a copy learns it is behind only from a frame, so
// this is the frame (BUG-0746). A copy that holds the record acknowledges the duplicate; a
// copy that is behind refuses it as a gap and runs the catch-up. It does no I/O and keeps
// nothing per bucket; a pair whose cursor is unknown (never written since it was seeded)
// sends nothing.
func (r *Replicator) ShipTail(slot uint16, db uint64) {
	pair := r.cfg.Pairs.Snapshot(slot, db)
	if pair.AppliedPos.Zero() || pair.TailCursor == 0 {
		// Nothing applied, or the record is not known to this log (a pair seeded and not
		// written since has no log record for its cut): there is no frame to send, and finding
		// one would mean reading the log from its first record for every such pair. The next
		// write of the pair ships it.
		return
	}
	r.Enqueue(slot, db, pair.AppliedPos, cluster.Position{}, pair.TailCursor)
}

// shipOwnedTails runs ShipTail for every pair of a replicated database that this node holds
// and owns at the committed epoch: one pass over the pair map, memory only, stopping when ctx
// ends. It reports false while the committed state is not settled (no database yet, or the
// handoff has not seen the committed epoch, or this node knows the cluster moved past it), so
// the caller asks again.
func (n *Node) shipOwnedTails(ctx context.Context) bool {
	committed := n.topology.Committed()
	if _, behind := n.cl.lease.BehindEpoch(committed); behind || !n.handoff.Saw(committed) {
		return false
	}
	replicated := map[uint64]bool{}
	for _, database := range n.raft.State().Databases {
		replicated[uint64(database.ID)] = database.RF > 1
	}
	if len(replicated) == 0 {
		return false
	}
	n.pairs.Held(func(slot uint16, db uint64) {
		if ctx.Err() != nil || !replicated[db] {
			return
		}
		if owner, known := n.ownerOf(slot, db); known && owner == n.cfg.NodeID {
			n.cl.repl.ShipTail(slot, db)
		}
	})
	return ctx.Err() == nil
}

// tailOpened is Handoff's Opened hook: the pair became this node's to order by a move.
func (n *Node) tailOpened(slot uint16, db uint64) {
	if n.cl != nil {
		n.cl.repl.ShipTail(slot, db)
	}
}
