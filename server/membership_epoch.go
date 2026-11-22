package server

import (
	"errors"
	"fmt"
	"time"
)

// One epoch per exchange (spec §19.3.7). An entry is applied only between two
// nodes that agree on the membership epoch, and therefore on the ring: a push
// that changes state and a pull are refused across epochs, and either side of
// a refusal wakes its gossip, so the node that is behind learns the newer
// epoch in a round trip instead of at its next twenty-second exchange.

// minGossipWakeInterval bounds how often refusals wake the gossip loops. Under
// load every forwarded write of the window is refused, and one exchange per
// member per tenth of a second is all a node that is behind needs.
const minGossipWakeInterval = 100 * time.Millisecond

// wakeGossip makes every verifyPeer loop exchange gossip now, or, inside the
// rate limit, once the limit has passed: a wake is never dropped, because what
// it carries (an epoch, a node settling) would otherwise wait for the next
// twenty-second exchange. At most one wake is deferred at a time, so the rate
// stays one per minGossipWakeInterval.
func (c *Cluster) wakeGossip() {
	c.gossipWakeMu.Lock()
	defer c.gossipWakeMu.Unlock()
	if wait := minGossipWakeInterval - time.Since(c.lastGossipWake); wait > 0 {
		if !c.gossipWakeDeferred {
			c.gossipWakeDeferred = true
			// Not stopped when the cluster closes, and safe not to be: it
			// fires within minGossipWakeInterval and only closes and replaces
			// a channel nothing may be waiting on any more.
			time.AfterFunc(wait, c.deferredGossipWake)
		}
		return
	}
	c.closeGossipWakeLocked()
}

// deferredGossipWake is the wake wakeGossip put off to the end of the rate
// limit.
func (c *Cluster) deferredGossipWake() {
	c.gossipWakeMu.Lock()
	defer c.gossipWakeMu.Unlock()
	c.gossipWakeDeferred = false
	c.closeGossipWakeLocked()
}

// closeGossipWakeLocked wakes every gossip loop. The caller holds
// gossipWakeMu.
func (c *Cluster) closeGossipWakeLocked() {
	c.lastGossipWake = time.Now()
	close(c.gossipWake)
	c.gossipWake = make(chan struct{})
}

// gossipWakeSignal returns the channel the next wakeGossip closes.
func (c *Cluster) gossipWakeSignal() <-chan struct{} {
	c.gossipWakeMu.Lock()
	defer c.gossipWakeMu.Unlock()
	return c.gossipWake
}

// refuseAcrossEpochs is the receiver's check on a push that changes state —
// entries that change state, or an advance notice, which moves a position:
// the sender's epoch must be this node's. It returns the refusal to answer
// with, or nil. Reads, and a stream's handshake, are not gated.
func (c *Cluster) refuseAcrossEpochs(req ReplPushRequest) *ReplPushResponse {
	// A question is gated too: its answer compares only on one ring.
	if !pushChangesState(req.Entries) && req.Stream.advanceTo() == 0 && req.Handoff == nil {
		return nil
	}
	current := c.Epoch()
	if req.Epoch == current {
		return nil
	}
	c.wakeGossip()
	return &ReplPushResponse{
		Success: false,
		Refusal: refusalEpoch,
		Epoch:   current,
		Error: fmt.Sprintf("%s sent this push on membership epoch %d and this node is on epoch %d; nothing was applied. "+
			"The node that is behind learns the newer membership by gossip, which this refusal has woken", req.FromNode, req.Epoch, current),
	}
}

// errPullAcrossEpochs is a source's "not yet" to a pull from a node on
// another membership epoch: the two place the bucket differently, so a copy
// handed over now is positioned in a stream that will not be routed to it.
var errPullAcrossEpochs = errors.New("the puller and this node are on different membership epochs")

// refuseAPullAcrossEpochs refuses a pull whose puller is on another epoch than
// this node, and wakes this node's gossip. It is "not yet", never final: the
// puller asks again once the two agree.
func (c *Cluster) refuseAPullAcrossEpochs(req MigrateStartRequest) error {
	current := c.Epoch()
	if req.Epoch == current {
		return nil
	}
	c.wakeGossip()
	return fmt.Errorf("%w: %s asked for bucket %q in database %q on epoch %d and this node is on epoch %d; "+
		"nothing was sent, and the pull is tried again once both have learned the newer membership",
		errPullAcrossEpochs, req.FromNode, req.Bucket, req.Database, req.Epoch, current)
}
