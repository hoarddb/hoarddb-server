package server

import (
	"fmt"
	"time"

	"github.com/hoarddb/hoarddb-server/ring"
)

// WAL retention: the write-ahead log is the repair tool for two consumers —
// this node's own crash replay and catch-up for lagging members — and nothing
// may be pruned that either still needs. Every retention pass re-checks both
// bounds, so the log grows without bound only while a member is unreachable or
// refused, which is exactly the state where the log is doing its job.
//
// A pass prunes up to retentionBound — the lowest cursor of any member, held
// below every entry not yet applied here — after the node's store has been
// synced. The ordering is deliberate: every entry at or below the bound has
// applied, so the sync flushes it. Design record:
// the wal-retention-design record.
func (c *Cluster) runRetentionLoop() {
	defer c.storeWG.Done()

	ticker := time.NewTicker(time.Duration(c.s.config.Server.WALRetentionSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if err := c.pruneWAL(); err != nil {
				c.logger.Warn("WAL retention pass failed", "error", err)
			}
		}
	}
}

// pruneWAL performs one retention pass. It is also the entry point tests use
// to drive retention deterministically instead of waiting for the ticker.
func (c *Cluster) pruneWAL() error {
	if c.s.wal == nil {
		return nil
	}

	// An idle stream's cursor moves only by an advance notice. The senders
	// send it after this pass has read its bound, so the next pass prunes
	// what they released (spec §19.3.9).
	c.askForAdvances()

	// Sample the bound BEFORE syncing: every entry at or below it has applied
	// (retentionBound holds it below any that has not), so the sync that
	// follows makes each of them durable in the store before the log drops it.
	bound := c.retentionBound()
	c.warnIfAMemberHoldsRetention(time.Now())
	if bound == 0 {
		// Nothing is confirmed yet — e.g. right after this node restarted,
		// when per-peer positions start at zero and are restored by the
		// first catch-up round. Truncating now could remove segments a
		// restarting peer still needs.
		return nil
	}
	// The positions go into the store first, unsynced: the sync below then makes
	// them durable before the truncation removes the log entries that carried
	// them. Written after the sync, a restart between the two would rebuild a
	// position from a log that no longer holds it.
	checkpointed, err := c.checkpointAllPositions(false)
	if err != nil {
		return fmt.Errorf("checkpointing replication positions before WAL retention: %w", err)
	}
	if err := c.syncLocalStores(); err != nil {
		return fmt.Errorf("syncing stores before WAL retention: %w", err)
	}
	// Only now are the positions durable, so only now do they pay a debt.
	c.settleOwedCheckpoints(checkpointed)
	if err := c.s.wal.Truncate(bound); err != nil {
		return fmt.Errorf("truncating WAL up to seq %d: %w", bound, err)
	}
	c.logger.Info("WAL retention pruned acknowledged segments", "up_to_seq", bound)
	return nil
}

// retentionBound is the highest sequence a retention pass may cut the log to:
// the replicas' bound, held below every entry appended and not yet applied
// and below the lowest sequence this node still owes a replay (spec
// §11 Q24, SA-28). The terms compose as a minimum.
//
// The unapplied term is needed whatever the members' cursors say. With no
// other member, the replicas' bound is the log's last sequence, which counts an
// append whose apply has not run; with one, a peer's acknowledgement of a later
// write this node shipped says nothing about an entry it appended as a replica,
// which it never ships. The store sync before the truncate cannot flush an apply that
// has not happened, so either way the log is that entry's only copy.
//
// The replicas' bound is read first: it reads the log's last sequence, and
// every entry at or below that was registered as unapplied before it was
// appended, so the unapplied mark read after it sees every such entry that
// has not applied yet.
//
// The replicas acknowledge an owed write — they applied it — so without the
// owed term retention would remove the only copy before the restart that
// retries it. While it holds the bound, each pass warns: one outstanding
// failure holds the whole node's log.
func (c *Cluster) retentionBound() uint64 {
	bound := c.replicaAckBound()
	if floor, inFlight := c.unapplied.lowest(); inFlight && floor-1 < bound {
		bound = floor - 1
	}
	lowest, owner, owed := c.s.owed.lowest()
	if !owed || lowest-1 >= bound {
		return bound
	}
	c.logger.Warn(fmt.Sprintf(
		"WAL retention is held at sequence %d: bucket %q in database %q has a write at sequence %d that failed on this node and that replay still owes. "+
			"The log will grow until it is retried; restart the node to retry it.",
		lowest-1, owner.bucket, owner.database, lowest),
		"owed_seq", lowest, "database", owner.database, "bucket", owner.bucket, "owed", c.s.owed.count())
	return lowest - 1
}

// replicaAckBound returns the highest sequence no member can still need from
// this node's log: the lowest stream cursor of every member but this node
// (spec §19.3.9). The cursor is the position a member reported at its
// handshake, raised by its acknowledgements since; every entry at or below it
// that is addressed to the member has been delivered, because one sender
// delivers in order. Database DDL is addressed to every member, so the bound
// is over every member, not over the ones that host a bucket this node owns
// : a member that hosts nothing of this node's still holds back the
// log while it is down.
//
// While any member's cursor is unknown — it has not completed a handshake
// with this process — the bound is zero and nothing is pruned: the position
// it will report is the only thing that says what it needs.
//
// With no other member the only consumer is this node's own replay: the bound
// is the end of the log, and retentionBound holds it below every entry that
// has not applied.
//
// O(members): nothing here reads the buckets this node holds.
func (c *Cluster) replicaAckBound() uint64 {
	bound := c.s.wal.LastSequence()
	members := c.otherMembers()
	c.peersMu.RLock()
	defer c.peersMu.RUnlock()
	for _, member := range members {
		p, known := c.peers[member.ID]
		if !known {
			return 0
		}
		cursor, opened := p.cursor(c.self.ID)
		if !opened {
			return 0
		}
		bound = min(bound, cursor)
	}
	return bound
}

// retentionHeldWarnInterval is how often a retention pass repeats its warning
// that a member holds the log: the condition lasts until an
// operator acts, so a line per pass would scroll the log over one fact.
const retentionHeldWarnInterval = 5 * time.Minute

// retentionHolder is a member that holds this node's log back because it is
// not in sync: it has not completed a handshake with this process, or its
// stream is stalled.
type retentionHolder struct {
	nodeID     string
	node       ring.Node
	handshaken bool
	cursor     uint64
	reason     string
	// diverged is the member's refusal of an entry as diverged, when that is
	// why its stream is stalled (R8-E, review M-6).
	diverged string
}

// heldBy returns the member that is not in sync and holds the retention bound
// lowest, if one does. A member in sync below the others is not reported: its
// cursor moves with its next acknowledgement or advance notice. O(members).
func (c *Cluster) heldBy() (retentionHolder, bool) {
	var holder retentionHolder
	found := false
	// A member whose handshake answered and waits for the start hold is
	// named only when no member without an answer holds the log too: the
	// one to start or remove is the one that has not answered.
	var answeredUnopened string
	for _, member := range c.otherMembers() {
		c.peersMu.RLock()
		p, known := c.peers[member.ID]
		c.peersMu.RUnlock()
		if !known {
			return retentionHolder{nodeID: member.ID}, true
		}
		cursor, opened := p.cursor(c.self.ID)
		if !opened {
			if !c.startHold.heardOf(member.ID) {
				return retentionHolder{nodeID: member.ID}, true
			}
			if answeredUnopened == "" {
				answeredUnopened = member.ID
			}
			continue
		}
		reason, stalled := p.stalledReason()
		if !stalled || (found && cursor >= holder.cursor) {
			continue
		}
		holder = retentionHolder{nodeID: member.ID, node: member, handshaken: true, cursor: cursor, reason: reason}
		if diverged := p.divergedOn.Load(); diverged != nil {
			holder.diverged = *diverged
		}
		found = true
	}
	if answeredUnopened != "" {
		return retentionHolder{nodeID: answeredUnopened}, true
	}
	return holder, found
}

// stalledReason reports whether the stream is stalled, and why.
func (p *peer) stalledReason() (string, bool) {
	p.streamMu.Lock()
	defer p.streamMu.Unlock()
	return p.stallReason, p.state == streamStalled
}

// warnIfAMemberHoldsRetention says, at most once per retentionHeldWarnEvery
// and at the first pass after a start, which member holds this node's log
// back and what to do about it. Since the bound is over every
// member, one member that is down holds the log on every node, and the disk
// fills with nothing else said; hoarddb_replication_peers_not_in_sync and
// hoarddb_node_data_bytes{part="wal"} are the same fact as numbers.
func (c *Cluster) warnIfAMemberHoldsRetention(now time.Time) {
	holder, held := c.heldBy()
	if !held {
		return
	}
	c.retentionWarnMu.Lock()
	due := c.retentionWarnedAt.IsZero() || now.Sub(c.retentionWarnedAt) >= c.retentionHeldWarnEvery
	if due {
		c.retentionWarnedAt = now
	}
	c.retentionWarnMu.Unlock()
	if !due {
		return
	}
	if !holder.handshaken {
		c.logger.Warn(fmt.Sprintf(
			"WAL retention on this node is held: member %s has not completed a replication handshake with this node since it started, "+
				"so nothing in the log is pruned and it grows until %s answers. Restart %s, or remove it with 'remove-node %s' if it is not coming back.",
			holder.nodeID, holder.nodeID, holder.nodeID, holder.nodeID),
			"node_id", holder.nodeID, "held_at_seq", 0)
		return
	}
	if holder.diverged != "" {
		c.logger.Warn(fmt.Sprintf(
			"WAL retention on this node is held at sequence %d by member %s, which diverged: %s. "+
				"The log grows until the bucket is repaired on %s.", holder.cursor, holder.nodeID, holder.diverged, holder.nodeID),
			"node_id", holder.nodeID, "held_at_seq", holder.cursor, "reason", "diverged")
		return
	}
	// A member whose store this node knows is not sealed cannot take the log:
	// the stream is stalled at the refused entry (BUG-0313), and it is running,
	// so a restart leaves the store as it is and the refusal stands. Its own
	// fix is the only way out (BUG-0605).
	if c.s.storeSealed() {
		known := c.sealedPeers.knowledge(holder.nodeID)
		if known.answered && !known.sealed() {
			c.logger.Warn(fmt.Sprintf(
				"WAL retention on this node is held at sequence %d by member %s, which this node cannot replicate to: %s, "+
					"and this node's store is encrypted (encryption.mode=%s), so nothing in the log can be sent to it and the log grows until that changes. "+
					"%s, or remove it from the cluster with 'remove-node %s'.",
				holder.cursor, holder.nodeID, peerStoreState(holder.node, known), c.s.config.Encryption.Mode,
				peerStoreFix(holder.node, known, "encryption.mode=on"), holder.nodeID),
				"node_id", holder.nodeID, "held_at_seq", holder.cursor, "reason", holder.reason)
			return
		}
	}
	c.logger.Warn(fmt.Sprintf(
		"WAL retention on this node is held at sequence %d by member %s, whose replication stream is stalled; "+
			"the log grows until %s returns. Restart %s, or remove it with 'remove-node %s' if it is not coming back.",
		holder.cursor, holder.nodeID, holder.nodeID, holder.nodeID, holder.nodeID),
		"node_id", holder.nodeID, "held_at_seq", holder.cursor, "reason", holder.reason)
}

// syncLocalStores flushes the node's store once, so the WAL entries applied so
// far survive a process crash without the log. Every bucket lives on that one
// store, so a pass is one Sync however many buckets the node holds (spec
// §14.2). The sync runs under no bucket lock: entries at or below the bound
// are already fully applied (see retentionBound), and a Sync racing a newer
// apply is harmless — the newer entry is above the bound and stays in the log.
func (c *Cluster) syncLocalStores() error {
	if err := c.s.kv.Sync(); err != nil {
		return fmt.Errorf("syncing the node's store: %w", err)
	}
	return nil
}
