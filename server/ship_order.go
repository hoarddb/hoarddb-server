package server

import (
	"context"
	"fmt"
	"sync"
)

// shipOrder puts this node's shipped entries on every peer's queue in sequence
// order.
//
// Every judgement about a peer's position is a high-water mark: the replica's
// apply gate (Cluster.shouldApply), the owner's write-concern wait
// (peer.waitForAck) and the replay's starting point (Cluster.replayStart). A peer that
// receives N+1 before N therefore skips N as already applied, and the owner
// counts N as confirmed. The WAL assigns sequences in order, but nothing between
// the append and the enqueue did: the bucket fence is shared by every writer and
// is released before the enqueue, so a writer delayed in that window let a later
// sequence reach the queue first.
//
// The receiver cannot detect the hole on its own. A peer's stream is sparse by
// construction: it carries only the buckets that peer hosts, and this node's WAL
// also numbers entries it applied as a replica, which are never shipped. A
// missing sequence is therefore the normal case, not evidence of loss.
//
// A write opens a ticket before its WAL append, with a floor no greater than the
// sequence it will get. It may queue for a peer only when no other open ticket
// that could still reach that peer has a lower floor. A ticket opened after this
// write's append has a floor above this write's sequence, so latecomers never
// hold it up. Waits are per peer: a write stuck behind a full queue for one peer
// does not hold up writes to other peers. The WAL's group commit is untouched,
// because no lock is held across the append.
type shipOrder struct {
	mu      sync.Mutex
	pending map[*shipTicket]struct{}
	// changed is closed and replaced whenever a ticket moves, which wakes every
	// waiter to re-check its turn.
	changed chan struct{}
}

// shipTicket is one shipped write between choosing its WAL position and being on
// every peer's queue. Its fields are guarded by shipOrder.mu.
type shipTicket struct {
	// floor is the lowest sequence the write can have until the WAL assigns
	// one, and that sequence afterwards.
	floor uint64
	// peers are the peers the write has still to be queued for. Nil means
	// they are not known yet, and until then the ticket holds up every peer.
	peers map[string]struct{}
}

func newShipOrder() *shipOrder {
	return &shipOrder{
		pending: make(map[*shipTicket]struct{}),
		changed: make(chan struct{}),
	}
}

// open registers a write that is about to be appended to the WAL. The floor is
// read under the lock, after every earlier ticket's registration, and the WAL's
// last sequence only grows, so the sequence the append assigns is at least it.
func (o *shipOrder) open(lastSeq func() uint64) *shipTicket {
	o.mu.Lock()
	defer o.mu.Unlock()
	ticket := &shipTicket{floor: lastSeq() + 1}
	o.pending[ticket] = struct{}{}
	return ticket
}

// assign records the sequence the WAL gave the write.
func (o *shipOrder) assign(ticket *shipTicket, seq uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	ticket.floor = seq
	o.notifyLocked()
}

// route records the peers the write goes to. Until it is called, the ticket
// holds up every peer.
func (o *shipOrder) route(ticket *shipTicket, peerIDs []string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	ticket.peers = make(map[string]struct{}, len(peerIDs))
	for _, id := range peerIDs {
		ticket.peers[id] = struct{}{}
	}
	o.notifyLocked()
}

// waitTurn blocks until every write with a lower sequence that could still go to
// peerID has been queued for it or has given up.
func (o *shipOrder) waitTurn(ctx context.Context, ticket *shipTicket, peerID string) error {
	for {
		o.mu.Lock()
		seq := ticket.floor
		blocked := o.blockedLocked(ticket, peerID)
		changed := o.changed
		o.mu.Unlock()
		if !blocked {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return fmt.Errorf("waiting for earlier entries to be queued for peer %s before seq %d: %w",
				peerID, seq, ctx.Err())
		}
	}
}

// queued records that the write is done with peerID, whether it was queued
// there or gave up. Later writes to that peer may then go ahead.
func (o *shipOrder) queued(ticket *shipTicket, peerID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(ticket.peers, peerID)
	o.notifyLocked()
}

// close retires the ticket. Every path out of a shipped write must reach it, or
// the writes behind it would wait for their whole queue deadline, so callers
// defer it and may also call it earlier; a second call does nothing.
func (o *shipOrder) close(ticket *shipTicket) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, ok := o.pending[ticket]; !ok {
		return
	}
	delete(o.pending, ticket)
	o.notifyLocked()
}

// lowestOpenFloor returns the lowest floor of any open ticket, and whether one
// is open. Every entry below it has closed its ticket, so it has been put on
// every queue it was going to (spec §19.3.9). It reads every open ticket: one
// per write in flight, never one per bucket.
func (o *shipOrder) lowestOpenFloor() (floor uint64, open bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for ticket := range o.pending {
		if !open || ticket.floor < floor {
			floor, open = ticket.floor, true
		}
	}
	return floor, open
}

func (o *shipOrder) blockedLocked(ticket *shipTicket, peerID string) bool {
	for other := range o.pending {
		if other == ticket || other.floor >= ticket.floor {
			continue
		}
		if other.peers == nil {
			return true
		}
		if _, ok := other.peers[peerID]; ok {
			return true
		}
	}
	return false
}

func (o *shipOrder) notifyLocked() {
	close(o.changed)
	o.changed = make(chan struct{})
}

// enqueueInOrder routes entry to p's stream after every earlier shipped entry
// that is going to p. It releases the ticket's hold on p whatever happens, so a
// write that gives up does not stall the writes behind it. Nothing here waits
// for the peer: a full queue stalls the stream, and a turn that does not come
// stalls it too, because an earlier entry may not be on the queue and this one
// must not overtake it. Either way the entry is in the log and is delivered
// from there.
func (c *Cluster) enqueueInOrder(ctx context.Context, ticket *shipTicket, p *peer, entry ReplEntry) {
	defer c.shipOrder.queued(ticket, p.node.ID)
	if err := c.shipOrder.waitTurn(ctx, ticket, p.node.ID); err != nil {
		p.stall("an earlier write did not finish routing")
		return
	}
	p.route(entry)
}
