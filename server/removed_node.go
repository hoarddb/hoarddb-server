package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/storage"
)

// A node that was removed stops accepting writes (build doc step 6.5b, A-6.3;
// review F-5), remembers it, and stays a source (step 9.3; spec §20.8.7).
//
// Steps 6.2 and 6.3 took away the accident by which a removed node's writes
// were saved: on main its stream pushes reached the members and were
// re-originated there. A removed node's writes at `one` are now refused by
// every member, so the node itself must stop answering them with a success.
//
// When a node is shown a membership that does not contain it, it records that
// durably — the 'x' record under storage.NSReplication, {epoch, members},
// synced — before it marks itself removed, so a restart begins removed. It
// then takes that membership as a non-member: its ring is built from the
// members without it, so it owns nothing and is in no bucket's set, and every
// copy it holds is outside. membership.json is not rewritten: it keeps the
// last membership this node was in. A removed node refuses every mutation and
// forwards every read, whatever its ring holds: removed from a two-node cluster
// its ring is the survivor alone and Enabled is false, so Server.dispatch asks
// Cluster.removed as well and sends it to the routed door (BUG-0626, BUG-0628); it keeps its senders running and keeps serving pulls,
// and it never pulls a copy and never deletes one — a removed node may hold
// the only complete copy of a bucket, and the node an operator is told to wipe
// afterwards must hold nothing the cluster lacks. Shown a later epoch by a
// member of the membership it recorded, it records that one and follows it,
// so its answers are not refused across epochs after the next change.

// ErrRemovedNode means this node was removed from the cluster and refuses
// writes. Nothing was applied, so the write is safe to send to a member.
var ErrRemovedNode = errors.New("this node was removed from the cluster")

// errRemovedNodeDoesNotPull is a removed node's answer to being asked to pull
// a copy: it is no bucket's member, and a copy it took would be one nothing
// ships to.
var errRemovedNodeDoesNotPull = errors.New("this node was removed from the cluster and pulls no copy")

// removalRecordKind is the kind byte of the 'x' record.
const removalRecordKind byte = 'x'

// removalRecord is the value of the 'x' record: the membership, without this
// node, that it was last shown.
type removalRecord struct {
	Epoch   uint64 `bson:"epoch"`
	Members []Peer `bson:"members"`
}

// removalKey is the key of the 'x' record.
func removalKey() []byte {
	return storage.NodeKey(storage.NSReplication, []byte{removalRecordKind})
}

// readRemovalRecord reads this node's record of its removal; found is false
// on a node that was never removed.
func readRemovalRecord(kv storage.KV) (record removalRecord, found bool, err error) {
	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	stored, err := snapshot.Get(removalKey())
	if errors.Is(err, storage.ErrKeyNotFound) {
		return removalRecord{}, false, nil
	}
	if err != nil {
		return removalRecord{}, false, fmt.Errorf("reading this node's record of its removal: %w", err)
	}
	if err := bson.Unmarshal(stored, &record); err != nil {
		return removalRecord{}, false, fmt.Errorf("decoding this node's record of its removal: %w", err)
	}
	return record, true, nil
}

// writeRemovalRecord records, synced, the membership of epoch without this
// node.
func (c *Cluster) writeRemovalRecord(epoch uint64, members []ring.Node) error {
	encoded, err := bson.Marshal(removalRecord{Epoch: epoch, Members: nodesToPeers(members)})
	if err != nil {
		return fmt.Errorf("encoding the record of this node's removal at epoch %d: %w", epoch, err)
	}
	batch := c.s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(removalKey(), encoded); err != nil {
		return fmt.Errorf("recording this node's removal at epoch %d: %w", epoch, err)
	}
	if err := batch.Commit(true); err != nil {
		return fmt.Errorf("recording this node's removal at epoch %d: %w", epoch, err)
	}
	return nil
}

// markRemoved takes the membership of epoch, members, which does not contain
// this node: it records it, then builds its ring from it without itself and
// moves its view to it. The first mark logs the removal and makes every write
// refused; each restarts the streams under the new ring, and a removed node's
// replays count what it can no longer deliver as stranded.
func (c *Cluster) markRemoved(epoch uint64, members []ring.Node) error {
	// Copied before the file lock: settleMu is never taken under it.
	settledOf := c.settledOfSnapshot()
	c.membershipFileMu.Lock()
	if c.removedAt.Load() != 0 && epoch <= c.Epoch() {
		c.membershipFileMu.Unlock()
		return nil
	}
	placement, err := c.placementRing(members, true)
	if err == nil {
		err = c.writeRemovalRecord(epoch, members)
	}
	if err == nil {
		// The history holds the epoch as an adoption's does: a placement pass
		// and an entry's ownership are judged against its ring.
		err = c.recordMembership(epoch, members)
	}
	if err == nil {
		err = c.clearPendingOnRemovalLocked(settledOf)
	}
	if err != nil {
		c.membershipFileMu.Unlock()
		return fmt.Errorf("recording that this node was removed at membership epoch %d: %w", epoch, err)
	}
	first := c.takeRemovedView(epoch, members, placement)
	c.membershipFileMu.Unlock()

	if first {
		c.logger.Warn("this node was removed from the cluster; it refuses every write from now on, and clients should connect to a member. "+
			"It keeps the copies it holds and serves them to the members until an operator empties its data directory",
			"epoch", epoch, "connect_to", members[0].Addr)
	} else {
		c.logger.Info("this removed node follows a later membership", "epoch", epoch, "members", len(members))
	}
	c.resetStreams(fmt.Sprintf("this node was removed, and follows membership epoch %d", epoch))
	c.startPlacementPass(epoch)
	for _, m := range members {
		c.peerFor(m)
		c.ensurePeerRunning(m)
	}
	// Before Start, Start launches it once the catalog is loaded.
	if c.startCalled.Load() {
		c.startRemovedHandOff()
	}
	return nil
}

// clearPendingOnRemovalLocked drops this node's acceptance, on disk and in
// memory, as it is removed. A removed node refuses every phase of a
// membership change, so it never promises or accepts again at that epoch,
// and the acceptance it forgets cannot be contradicted. The record is written
// with the view still the membership this node was last a member of, which
// is what a removed node's file keeps; only the pending change goes. Written
// after the removal record, so a crash between the two leaves a node that
// starts removed, and NewCluster restores no acceptance on such a node. The
// caller holds membershipFileMu and took settledOf before it.
func (c *Cluster) clearPendingOnRemovalLocked(settledOf map[string]uint64) error {
	if c.pending == nil {
		return nil
	}
	epoch, members := c.epochMembers()
	if err := c.writeMembershipRecordLocked(membershipWrite{
		epoch: epoch, members: members, settledOf: settledOf, bootstrapped: c.bootstrapped.Load(),
	}); err != nil {
		return fmt.Errorf("dropping this node's acceptance of membership epoch %d: %w", c.pending.Epoch, err)
	}
	c.pending = nil
	return nil
}

// takeRemovedView swaps in a removed node's ring and view of the membership
// and marks it removed; it reports whether this was the first mark.
func (c *Cluster) takeRemovedView(epoch uint64, members []ring.Node, placement *ring.Ring) bool {
	c.viewMu.Lock()
	defer c.viewMu.Unlock()
	// Deferred after the unlock's defer, so it runs once the view and the
	// mark are both made.
	defer c.noteViewChanged()
	c.r = placement
	c.epoch = epoch
	c.members = members
	c.removedConnectTo = members[0].Addr
	return c.removedAt.CompareAndSwap(0, epoch)
}

// startRemoved makes a node that recorded its removal start removed: the
// record's membership is its view, and its ring has no place for it.
func (c *Cluster) startRemoved(record removalRecord) error {
	members := peersToNodes(record.Members)
	if len(members) == 0 {
		return fmt.Errorf("this node's record of its removal at epoch %d lists no member; the node store is damaged — "+
			"empty this node's data directory: it was removed from the cluster, and its members hold what it held", record.Epoch)
	}
	placement, err := c.placementRing(members, true)
	if err != nil {
		return err
	}
	c.takeRemovedView(record.Epoch, members, placement)
	c.logger.Warn("this node was removed from the cluster and refuses every write; clients should connect to a member. "+
		"Empty its data directory once the members hold what it holds",
		"epoch", record.Epoch, "connect_to", members[0].Addr)
	return nil
}

// errRejoinRemovedNode refuses the join of a node that keeps the record of its
// removal; errJoinUnanswered the join of a node that does not answer.
var (
	errRejoinRemovedNode = errors.New("the node was removed from this cluster and still holds its data directory")
	errJoinUnanswered    = errors.New("the node to be added did not answer")
)

// admitNewcomerAsked is the operator's join asking the node it adds, before
// any vote. A join adds a running node, so one that does not answer is
// refused: it could not say whether it remembers a removal, and on a plaintext
// cluster nothing else asks it. A node that remembers its removal is refused:
// it refuses every write while it keeps that record, so as a member it would
// refuse the writes of every bucket it owns.
func (c *Cluster) admitNewcomerAsked(ctx context.Context, newcomer ring.Node) error {
	resp, _, err := c.probeGossip(ctx, newcomer.Addr)
	if err != nil {
		return fmt.Errorf("%w: %s (%v). A join adds a running node: start it, configured as a member of this cluster, and run 'join' again",
			errJoinUnanswered, newcomer.Addr, err)
	}
	if resp.RemovedAt == 0 {
		return nil
	}
	return fmt.Errorf("%w: %s was removed at membership epoch %d and refuses every write while it keeps that record. "+
		"Stop it, empty its data directory, start it again and run 'join', once the members hold its buckets",
		errRejoinRemovedNode, newcomer.ID, resp.RemovedAt)
}

// refuseWhenRemoved is a removed node's answer to a write.
func (c *Cluster) refuseWhenRemoved() error {
	epoch := c.removedAt.Load()
	if epoch == 0 {
		return nil
	}
	c.viewMu.RLock()
	connectTo := c.removedConnectTo
	c.viewMu.RUnlock()
	return fmt.Errorf("%w at epoch %d and no longer accepts writes; connect to %s, a member, and send the write there",
		ErrRemovedNode, epoch, connectTo)
}

// actsAsOwner reports whether this node owns the bucket placed by key and has
// not been removed: what a write's owner re-checks while it is in flight.
func (c *Cluster) actsAsOwner(key placementKey) bool {
	return c.removedAt.Load() == 0 && c.ownsBucket(key)
}

// removed reports whether this node was removed from the cluster.
func (c *Cluster) removed() bool {
	return c.removedAt.Load() != 0
}
