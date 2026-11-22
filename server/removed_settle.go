package server

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/transport"
)

// A member waits for a removed node's hand-off before it reports a change
// settled (owner ruling R7, 2026-10-21; on-demand pull build doc §20).
//
// At factor 1 a removed owner holds the only copy of its buckets, and its
// successor holds nothing of them: nothing on the successor says a name is
// coming until the removed node names it (removed_handoff.go). The successor's
// own sweep settles about a millisecond after the removal reaches it, before
// that name arrives, and once its settled epoch is raised the settling window
// closes for good: a read of such a bucket then answered "not found", and a
// restore created the bucket beside the removed node's copy.
//
// So a member does not raise its Settled epoch while a node removed since its
// last settled epoch has neither told it that it has named every bucket it
// holds (ReplHandoff.NamedAllAt) nor been confirmed gone. Gone is the bound a
// create already waits for a removed node: no answer within
// removedNodeAnswerWait. A dead node's buckets are then genuinely unavailable.
// With the names it sent in hand, the member's take-over holds the mark until
// it has found their copies (takeOverUnfound).
//
// A read of a name a removed node owned (owner ruling R8) answers, while that
// node is awaited, a retry naming it; once its notice has arrived or it is
// confirmed gone, and after the window, an ordinary miss — not found, with the
// create advice — that names the node, never a retry: a never-created name
// must not read retry forever after a crash.
//
// The notice says the node has named every bucket it holds, not that each
// one landed (BUG-0688). A bucket is named only once its owner served it or
// recorded the ask (MigrateResponse.HandOffTaken, BUG-0698): an ask that
// reaches the owner before it adopts the epoch that makes it the owner
// records nothing here, and is asked again. A bucket it named stays awaited
// here, its owner, until the take-over ends (takeOver.handedOff): the node
// said it holds it, so a read of it is the retry naming the node, never the
// miss, and the settle mark stays down, until the copy lands, the node
// answers that it does not hold it, or the node is confirmed gone. The probe
// runs on while such a bucket is awaited, so a node that dies after its
// notice is still confirmed gone within the bound.
//
// The notice is for one membership epoch (BUG-0701): the node names each
// bucket to the bucket's owner at that epoch, and a later change can move a
// bucket on before its copy lands (a second removal, of this owner, or a
// join; the take-over then drops the entry here). So a member counts a notice
// only for its own epoch or an earlier one (removedHandOff.covers): after a
// change it awaits the node again, and probes it, until the node has followed
// the change, named the moved buckets to their new owners and told it so, or
// said nothing is left to hand over (namedAllEnded), or is confirmed gone. A
// node removed before this member's last settled epoch is awaited again only
// when this member holds a notice of it for an earlier epoch; a node it never
// heard from was settled past already.
//
// The notice is kept in memory, so a restart forgets it (BUG-0702). The
// probe carries what this member holds (ReplHandoff.HeardNamedAllAt) and the
// removed node's answer carries its notice whenever nothing of its hand-off
// can have been lost here; otherwise the removed node names this member's
// buckets to it again, which records them here again, and then tells it.
//
// A live removed node whose asks are all refused without the flag (its view
// is stale, the owner was itself removed, a held copy fails followOwnersTerm)
// never sends its notice: readers get the retry naming it, as R8 says, and
// the members do not settle. The same holds after every later change until
// it has named its buckets again: one membership change after another while
// a hand-off copies keeps the members unsettled and the moved names a retry
// for as long as the removed node runs and has not caught up. Stopping it
// releases them once it is confirmed gone (removedNodeAnswerWait). There is
// no timer.
//
// Cost (rule 11): one entry per removed node, with one probe goroutine per
// removed node while it is awaited; and one entry (a placement key and a node
// ID) per bucket a removed node named here whose take-over has not ended,
// none at rest. The check reads the member sets of the epochs since the last
// settled one, and the removed nodes of the history: O(epochs × members). A
// membership change costs nothing per bucket: at a member it may start one
// probe per removed node of the history that has not ended its hand-off; a
// node confirmed gone costs one pass over the awaited buckets.

// removedHandOff is what this member heard of one removed node's hand-off.
type removedHandOff struct {
	// namedAllAt is the highest epoch the node said it had named every bucket
	// it holds to that epoch's owners at, namedAllEnded once it said nothing
	// is left, 0 until it says so. It counts for this node's epoch only when
	// at or above it (covers): a notice of an earlier epoch says nothing of a
	// bucket a later change moved before its copy landed (BUG-0701).
	namedAllAt uint64
	// gone says the node did not answer within removedNodeAnswerWait.
	gone bool
	// probing says a probe of the node runs.
	probing bool
}

// covers reports whether the node's notice holds for epoch: it is the node
// gone, or a notice given at epoch or later.
func (s *removedHandOff) covers(epoch uint64) bool {
	return s.gone || s.namedAllAt >= epoch
}

// removedLeft is a node a membership change removed, and the first epoch whose
// members did not list it.
type removedLeft struct {
	node   ring.Node
	leftAt uint64
}

// removedHandOffOf is id's entry, made when absent. The caller holds
// removedHandOffMu.
func (c *Cluster) removedHandOffOf(id string) *removedHandOff {
	if c.removedHandOffs == nil {
		c.removedHandOffs = make(map[string]*removedHandOff)
	}
	state, known := c.removedHandOffs[id]
	if !known {
		state = &removedHandOff{}
		c.removedHandOffs[id] = state
	}
	return state
}

// noteRemovedNamedAll records a removed node's notice that it has named every
// bucket it holds, and raises the settle mark it may have been holding back.
func (c *Cluster) noteRemovedNamedAll(nodeID string, at uint64) {
	c.removedHandOffMu.Lock()
	state := c.removedHandOffOf(nodeID)
	state.namedAllAt = max(state.namedAllAt, at)
	c.removedHandOffMu.Unlock()
	c.raiseSettleIfClear()
}

// removedSince is every node a member at an epoch from this node's settled
// epoch to epoch − 1 that epoch's members do not list, with the epoch it left
// at. An epoch out of the history is passed over.
func (c *Cluster) removedSince(epoch uint64) []removedLeft {
	current, recorded := c.membersAt(epoch)
	if !recorded {
		return nil
	}
	lastListed := make(map[string]uint64)
	nodes := make(map[string]ring.Node)
	for at := max(c.settled.Load(), InitialEpoch); at < epoch; at++ {
		members, recorded := c.membersAt(at)
		if !recorded {
			continue
		}
		for _, member := range members {
			if member.ID == c.self.ID || nodeListed(current, member.ID) {
				continue
			}
			lastListed[member.ID] = at
			nodes[member.ID] = member
		}
	}
	removed := make([]removedLeft, 0, len(nodes))
	for id, node := range nodes {
		removed = append(removed, removedLeft{node: node, leftAt: lastListed[id] + 1})
	}
	return removed
}

// awaitingRemovedHandOff reports whether a node removed since this node's last
// settled epoch has neither said it named every bucket it holds, at epoch or
// later, nor been confirmed gone, and starts a probe of each such node not yet
// probed. So does a node removed earlier whose notice this node holds for an
// earlier epoch only: a change since may have moved one of its buckets on
// before the copy landed, so it is awaited until it says it named them at
// this epoch (BUG-0701). An entry for a node that is a member again is
// forgotten. O(removed nodes in the history).
func (c *Cluster) awaitingRemovedHandOff(epoch uint64) bool {
	removed := c.removedSince(epoch)
	inHistory := c.removedInHistory()
	_, current := c.epochMembers()
	c.removedHandOffMu.Lock()
	for id := range c.removedHandOffs {
		if nodeListed(current, id) {
			delete(c.removedHandOffs, id)
		}
	}
	awaiting := false
	var probe []ring.Node
	await := func(node ring.Node) {
		state := c.removedHandOffOf(node.ID)
		if state.covers(epoch) {
			return
		}
		awaiting = true
		if !state.probing {
			state.probing = true
			probe = append(probe, node)
		}
	}
	for _, left := range removed {
		await(left.node)
	}
	for _, node := range inHistory {
		if state, known := c.removedHandOffs[node.ID]; known && state.namedAllAt != 0 {
			await(node)
		}
	}
	c.removedHandOffMu.Unlock()
	for _, node := range probe {
		c.probeRemoved(node)
	}
	return awaiting
}

// probeRemoved asks a removed node, every removedNodeAnswerWait, whether it
// answers at all, until it has said it named every bucket it holds, it does
// not answer within the wait (it is gone), or the cluster stops. Either end
// raises the settle mark it may have held back.
func (c *Cluster) probeRemoved(node ring.Node) {
	if !c.startStoreGoroutine() {
		c.removedHandOffMu.Lock()
		c.removedHandOffOf(node.ID).probing = false
		c.removedHandOffMu.Unlock()
		return
	}
	go func() {
		defer c.storeWG.Done()
		for {
			c.removedHandOffMu.Lock()
			heard := c.removedHandOffOf(node.ID).namedAllAt
			c.removedHandOffMu.Unlock()
			answered, namedAllAt := c.removedNodeAnswers(node, heard)
			// Read before removedHandOffMu, a leaf.
			epoch := c.Epoch()
			// A probe this node's own stop cut short says nothing of the
			// removed node: taken for its silence, it raised and persisted a
			// settle that disabled the wait after the restart (BUG-0692).
			if !answered && c.ctx.Err() != nil {
				c.removedHandOffMu.Lock()
				c.removedHandOffOf(node.ID).probing = false
				c.removedHandOffMu.Unlock()
				return
			}
			c.removedHandOffMu.Lock()
			state := c.removedHandOffOf(node.ID)
			if !answered {
				state.gone = true
			}
			// The node's answer carries its notice when this node may be told
			// it: a notice this node's restart forgot is told again
			// (BUG-0702).
			state.namedAllAt = max(state.namedAllAt, namedAllAt)
			// Read under removedHandOffMu, as noteHandedOff records, so a
			// bucket recorded while this probe ends is never left unprobed.
			settled := state.covers(epoch) && (state.gone || !c.handedOffAwaitedFrom(node.ID))
			if settled {
				state.probing = false
			}
			c.removedHandOffMu.Unlock()
			if settled {
				if !answered {
					c.forgetHandedOffFrom(node.ID)
					c.logger.Warn("a removed node does not answer; the buckets only it held are unavailable until it is back",
						"node_id", node.ID, "waited", removedNodeAnswerWait.String())
				}
				c.raiseSettleIfClear()
				return
			}
			select {
			case <-c.ctx.Done():
				c.removedHandOffMu.Lock()
				c.removedHandOffOf(node.ID).probing = false
				c.removedHandOffMu.Unlock()
				return
			case <-time.After(removedNodeAnswerWait):
			}
		}
	}()
}

// removedNodeAnswers reports whether node answers a question that names no
// bucket within removedNodeAnswerWait: any answer, a refusal included, is one.
// heard is the notice this node holds from it, and namedAllAt the notice its
// answer carries, or 0.
func (c *Cluster) removedNodeAnswers(node ring.Node, heard uint64) (answered bool, namedAllAt uint64) {
	ctx, cancel := context.WithTimeout(c.ctx, removedNodeAnswerWait)
	defer cancel()
	resp, err := c.sendRemovedNotice(ctx, node, &ReplHandoff{HeardNamedAllAt: heard})
	return err == nil, resp.NamedAllAt
}

// sendRemovedNotice sends one hand-off frame that names no bucket — a probe,
// or a removed node's NamedAllAt notice — and returns the answer. An error is
// no answer.
func (c *Cluster) sendRemovedNotice(ctx context.Context, node ring.Node, notice *ReplHandoff) (ReplPushResponse, error) {
	cli, err := c.peerFor(node).dial(ctx)
	if err != nil {
		return ReplPushResponse{}, fmt.Errorf("reaching %s: %w", node.ID, err)
	}
	payload, err := bson.Marshal(ReplPushRequest{Token: c.localToken(), FromNode: c.self.ID, Epoch: c.Epoch(), Handoff: notice})
	if err != nil {
		return ReplPushResponse{}, fmt.Errorf("encoding the notice to %s: %w", node.ID, err)
	}
	frame, err := cli.SendRequest(ctx, transport.LaneRequest, transport.OpReplPush, payload)
	if err != nil {
		return ReplPushResponse{}, fmt.Errorf("reaching %s: %w", node.ID, err)
	}
	var resp ReplPushResponse
	if err := decodePeerAnswer(frame.Payload, &resp); err != nil {
		return ReplPushResponse{}, fmt.Errorf("decoding %s's answer: %w", node.ID, err)
	}
	return resp, nil
}

// removedFormerOwnerOf is the node, removed and in this node's membership
// history, that owned key at an epoch the history still holds, or "". O(1)
// with no removed node in the history; otherwise one ring lookup per epoch.
func (c *Cluster) removedFormerOwnerOf(key placementKey) string {
	removed := c.removedInHistory()
	if len(removed) == 0 {
		return ""
	}
	c.historyMu.Lock()
	epochs := c.rememberedEpochs()
	c.historyMu.Unlock()
	for _, epoch := range epochs {
		placed, recorded := c.ringAt(epoch)
		if !recorded {
			continue
		}
		if owner, err := placed.Owner(key.encoded); err == nil && nodeListed(removed, owner.ID) {
			return owner.ID
		}
	}
	return ""
}

// awaitedRemovedOwnerOf is removedFormerOwnerOf while this node still waits
// for that node: it has neither said it named every bucket it holds, at this
// node's epoch or later (BUG-0701), nor been confirmed gone, or it handed
// key's bucket here and the take-over has not ended (handedOffBy). ""
// otherwise.
func (c *Cluster) awaitedRemovedOwnerOf(key placementKey) string {
	node := c.removedFormerOwnerOf(key)
	if node == "" {
		return ""
	}
	if handing := c.handedOffBy(key); handing != "" {
		return handing
	}
	epoch := c.Epoch()
	c.removedHandOffMu.Lock()
	defer c.removedHandOffMu.Unlock()
	if state, known := c.removedHandOffs[node]; known && state.covers(epoch) {
		return ""
	}
	return node
}

// removedOwnerAwaited refuses a request of a name whose owner before a
// removal was node, while this node still waits for node's hand-off: retry,
// naming it. nothing says what was not done ("nothing was read").
func removedOwnerAwaited(database, bucket, node, nothing string) error {
	return &sentencedError{cause: ErrHandoffInProgress, message: fmt.Sprintf(
		"bucket %q in database %q was placed on %s, which was removed from the cluster and has not handed its buckets to this node yet; "+
			"%s. Retry in a moment: the hand-off runs on its own while %s answers ('nodes' shows the members)",
		bucket, database, node, nothing, node)}
}

// refuseRemovedOwnersName answers a mutation of a name node, a removed node,
// owned, which this node holds no copy of and whose take-over has recorded
// nothing about it (owner ruling R8, extended to writes 2026-10-23,
// BUG-0693), as a read of it is answered (answerOnDemand, answerFromOutcome):
// while this node still waits for the hand-off of the node the name was
// placed on, a retry naming it, nothing stored; after the window, the miss
// with the note naming node; in the window once nothing is awaited, the miss
// only when the take-over found no live copy anywhere, else the hand-off's
// retry, since another member may hold the bucket. A drop this node remembers
// is told as a drop. A copy that landed while the take-over was awaited is the
// hand-off's retry, never the miss: the answer is always an error, so the
// routed fence never lets a write through that it did not fence on.
//
// Cost (rule 11): paid only on the miss path of a name a removed node owned,
// never by a held bucket: the drop lookup, the history's owner lookups
// awaitedRemovedOwnerOf makes, one atomic load and at most one finding
// lookup, with no state of its own.
func (c *Cluster) refuseRemovedOwnersName(key placementKey, database, bucket, node string) error {
	if c.s.bucketStoreOpen(database, bucket) {
		return c.handoffInProgress(database, bucket)
	}
	if dropped := c.missingBucketErr(database, bucket); errors.Is(dropped, ErrBucketNotFound) {
		return dropped
	}
	if awaited := c.awaitedRemovedOwnerOf(key); awaited != "" {
		return removedOwnerAwaited(database, bucket, awaited, "nothing was stored")
	}
	if epoch := c.Epoch(); c.readWindowOpen(epoch) {
		if found, recorded := c.cachedOutcome(key, epoch); !recorded || len(found.unheard) > 0 {
			return c.handoffInProgress(database, bucket)
		}
	}
	return c.missNamingRemovedOwner(database, bucket, node)
}

// removedOwnersWindowClosed reports whether R8's bound has passed for key, a
// name a removed node owned: the settling window is closed and no removed node
// it was placed on is awaited (awaitedRemovedOwnerOf). A request of such a
// name that this node does not hold is then the miss with the note at once,
// as a routed read after the window is (answerOnDemand): it does not wake the
// take-over, whose fresh round would answer the request "moving to this node"
// while it looks for a copy nobody holds.
func (c *Cluster) removedOwnersWindowClosed(key placementKey) bool {
	return !c.readWindowOpen(c.Epoch()) && c.awaitedRemovedOwnerOf(key) == ""
}

// removedOwnerNote is a request's ordinary miss with a note naming the removed
// node the name was placed on (owner ruling R8). It is the miss for every
// purpose but its text.
type removedOwnerNote struct {
	miss error
	node string
}

func (e *removedOwnerNote) Error() string {
	return fmt.Sprintf("%v. This name was placed on %s before %s was removed from the cluster: if the bucket existed only there, "+
		"its data is on %s", e.miss, e.node, e.node, e.node)
}

func (e *removedOwnerNote) Unwrap() error { return e.miss }

// missNamingRemovedOwner is the miss the handler would answer — no such
// database, or no such bucket in it — with the removed-node note.
func (c *Cluster) missNamingRemovedOwner(database, bucket, node string) error {
	miss := bucketNotFoundErr(database, bucket)
	if c.s.serverDataAccessor().getDB(database) == nil {
		miss = databaseNotFoundErr(database)
	}
	return &removedOwnerNote{miss: miss, node: node}
}

// raiseSettleIfClear raises the settle mark a sweep could not raise while a
// removed node's hand-off or a take-over's name held it. A node a removal left
// alone runs no sweep (it is in single-node mode, so a restart does not resume
// one), and it has nothing to hand off: it raises the mark itself, under
// raiseSettled's own checks.
func (c *Cluster) raiseSettleIfClear() {
	c.markIfResidueClear()
	if c.aloneAfterARemoval() {
		c.markSettled(c.Epoch())
	}
}

// noteHandedOff records that a peer asked this node, key's owner, to take the
// bucket over, when a removed node owned the name (BUG-0688): at factor 1 that
// is the removed node's hand-off, and the node holds the bucket. The node is
// probed while the bucket is awaited, so its death still ends the wait within
// the bound; a node already confirmed gone is not waited for (R7). The entry
// goes when the take-over ends (endTakeOver), or when the node is confirmed
// gone. recorded reports that the bucket is now awaited here, which is what
// the owner's flag on its refusal says (MigrateResponse.HandOffTaken).
func (c *Cluster) noteHandedOff(key placementKey) (recorded bool) {
	nodeID := c.removedFormerOwnerOf(key)
	if nodeID == "" {
		return false
	}
	node, known := c.removedNode(nodeID)
	if !known {
		return false
	}
	c.removedHandOffMu.Lock()
	state := c.removedHandOffOf(nodeID)
	if state.gone {
		c.removedHandOffMu.Unlock()
		return false
	}
	probe := !state.probing
	state.probing = true
	// Under removedHandOffMu, so a probe that confirms the node gone forgets
	// this entry after it, never before (takeOver.mu stays the leaf).
	c.takeOver.mu.Lock()
	c.takeOver.handedOff[key] = nodeID
	c.takeOver.mu.Unlock()
	c.removedHandOffMu.Unlock()
	if probe {
		c.probeRemoved(node)
	}
	return true
}

// removedNode is the node with id among the removed nodes in this node's
// membership history.
func (c *Cluster) removedNode(id string) (ring.Node, bool) {
	for _, node := range c.removedInHistory() {
		if node.ID == id {
			return node, true
		}
	}
	return ring.Node{}, false
}

// forgetHandedOffOf forgets key's hand-off once this node holds an opened
// copy: a take-over that ended just before noteHandedOff recorded it deletes
// nothing after it.
func (c *Cluster) forgetHandedOffOf(key placementKey) {
	c.takeOver.mu.Lock()
	delete(c.takeOver.handedOff, key)
	c.takeOver.mu.Unlock()
}

// handedOffBy is the removed node that handed key's bucket here, while its
// take-over has not ended, or "". O(1).
func (c *Cluster) handedOffBy(key placementKey) string {
	c.takeOver.mu.Lock()
	defer c.takeOver.mu.Unlock()
	return c.takeOver.handedOff[key]
}

// handedOffAwaitedFrom reports whether a bucket nodeID handed here is still
// awaited. O(awaited buckets), once per probe.
func (c *Cluster) handedOffAwaitedFrom(nodeID string) bool {
	c.takeOver.mu.Lock()
	defer c.takeOver.mu.Unlock()
	for _, handing := range c.takeOver.handedOff {
		if handing == nodeID {
			return true
		}
	}
	return false
}

// forgetHandedOffFrom forgets every bucket nodeID handed here: it is
// confirmed gone, and its buckets are a miss that names it (owner ruling R8).
// The take-over then decides them without it.
func (c *Cluster) forgetHandedOffFrom(nodeID string) {
	c.takeOver.mu.Lock()
	defer c.takeOver.mu.Unlock()
	maps.DeleteFunc(c.takeOver.handedOff, func(_ placementKey, handing string) bool { return handing == nodeID })
}
