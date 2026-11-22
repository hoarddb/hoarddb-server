package server

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// The drop record (replication dispatch 10, R8-C).
//
// A drop names the incarnation it removes and is delivered to every member.
// Every node that applies a drop entry — the owner, a replica, a replay — and
// the "dropped here; drop your copy" path leave a record of it:
//
//	'd' ‖ storage.PlacementKey(database, bucket) ‖ uvarint(incarnation)  →  {origin_seq, epoch}
//
// under storage.NSReplication. While it stands, nothing lands a copy of that
// incarnation on this node (a pull's materialise refuses it), a holder of it
// is not counted as the bucket's (the catalog question), and the copy
// question answers it (ReplCopy.Dropped). It is not the database-generation
// tombstone ('g', generation.go): that one records a dropped database
// incarnation, this one a dropped bucket incarnation.
//
// A release, handleMigrateDone and G-6 (d) delete a copy without being a drop
// entry, and write no record (review M-3): the copy they delete is the live
// incarnation, which this node may be asked to hold again.
//
// The tombstone sweep deletes a record once every current member, and every
// node removed within the membership history, has answered that it holds no
// copy of that incarnation; a removed node that has aged out of the history is
// gone and is not asked. There is no in-memory map: the readers are a pull, a
// question and a hand-off, none of them hot, and each does one Get.

const (
	dropRecordKind byte = 'd'
	// removedNodeQuestionWait bounds a question to a node removed from the
	// membership: it may be gone for good, and it is asked from a sweep.
	removedNodeQuestionWait = 3 * time.Second
	// dropRecordBatch is how many records one batch of the sweep deletes.
	dropRecordBatch = 1000
)

// droppedIncarnation is what a drop entry's apply knows of the drop: the
// incarnation it removes, the origin's sequence for the entry, and the
// membership epoch this node applied it at.
type droppedIncarnation struct {
	incarnation uint64
	originSeq   uint64
	epoch       uint64
}

type dropEntryCtxKey struct{}

// withDropEntry marks a bucket drop as the apply of a drop entry (or the
// "dropped here" path), which Server.DeleteBucketCtx answers by applying it
// only to a copy of that incarnation and recording it.
func withDropEntry(ctx context.Context, dropped droppedIncarnation) context.Context {
	return context.WithValue(ctx, dropEntryCtxKey{}, dropped)
}

// dropEntryFrom returns what withDropEntry attached.
func dropEntryFrom(ctx context.Context) (droppedIncarnation, bool) {
	dropped, ok := ctx.Value(dropEntryCtxKey{}).(droppedIncarnation)
	return dropped, ok
}

type loneDropCtxKey struct{}

// withLoneDrop marks a client's bucket drop on a node alone on its ring that
// remembers a removed node (recordsLoneDrops): Server.DeleteBucketCtx records
// the incarnation it deletes, in the drop's own batch.
func withLoneDrop(ctx context.Context) context.Context {
	return context.WithValue(ctx, loneDropCtxKey{}, true)
}

// loneDrop reports whether withLoneDrop marked ctx.
func loneDrop(ctx context.Context) bool {
	marked, _ := ctx.Value(loneDropCtxKey{}).(bool)
	return marked
}

// recordsLoneDrops reports whether a client's bucket drop on this node is
// recorded although the cluster did not stamp it: the node is alone on its
// ring, so it applies the drop straight to its store and logs no entry, and a
// node removed within its membership history may still hold a copy and hand it
// back (BUG-0500). The record is what refuses that copy: the take-over's
// fallback skips a dropped incarnation, and the copy question answers it. A
// node that never had another member has no one to hand a copy back and
// records nothing, as before.
func (c *Cluster) recordsLoneDrops() bool {
	return c.aloneAfterARemoval()
}

// aloneAfterARemoval reports whether this node is alone on its ring, is not
// itself removed, and remembers a node removed within its membership history:
// a removal left it alone, so the removed node may hold the only copy of a
// bucket this node now owns. A node that never had another member is not.
//
// The one-member door asks it of every request (BUG-0630), so it is cheap: a
// node that never had another member answers on one atomic load
// (historyNamedAnotherNode), and any other on three, from the answer computed
// at the current count of view and history changes (viewChanges). Only the
// first request after a change takes the view's lock and walks the history.
func (c *Cluster) aloneAfterARemoval() bool {
	alone, _ := c.aloneAfterARemovalAt()
	return alone
}

// aloneAfterARemovalAt is aloneAfterARemoval with the membership epoch its
// answer was read at, which is the current one while the answer is: an epoch
// change is a view change. A node that never had another member answers
// false and no epoch.
func (c *Cluster) aloneAfterARemovalAt() (alone bool, epoch uint64) {
	if !c.historyNamedAnotherNode.Load() {
		return false, 0
	}
	changes := c.viewChanges.Load()
	if cached := c.loneAnswer.Load(); cached != nil && cached.changes == changes {
		return cached.alone, cached.epoch
	}
	// Computed from a view read after the count: a change made meanwhile is
	// counted after it is made, so this answer is stored under a count that
	// is already old and is computed again by the next request.
	epoch = c.Epoch()
	alone = !c.Enabled() && !c.removed() && len(c.removedInHistory()) > 0
	c.loneAnswer.Store(&aloneAnswer{changes: changes, alone: alone, epoch: epoch})
	return alone, epoch
}

// aloneAnswer is one computed answer of aloneAfterARemoval, the epoch it was
// read at, and the count of view and history changes it was computed at.
type aloneAnswer struct {
	changes uint64
	epoch   uint64
	alone   bool
}

// noteViewChanged counts a change of the view or of the history, after it is
// made: aloneAfterARemoval's stored answer is then computed again.
func (c *Cluster) noteViewChanged() {
	c.viewChanges.Add(1)
}

// dropRecord is the value of a 'd' record.
type dropRecord struct {
	OriginSeq uint64 `bson:"origin_seq"`
	Epoch     uint64 `bson:"epoch"`
}

// dropRecordPrefix is the prefix of every drop record.
func dropRecordPrefix() []byte {
	return storage.NodeKey(storage.NSReplication, []byte{dropRecordKind})
}

// dropRecordNamePrefix is the prefix of every drop record of one name. The
// placement key is length-prefixed, so no name's prefix is another's.
func dropRecordNamePrefix(database, bucket string) []byte {
	return storage.NodeKey(storage.NSReplication, []byte{dropRecordKind}, storage.PlacementKey(database, bucket, nil))
}

// dropRecordKey is the key of the record of one dropped incarnation.
func dropRecordKey(database, bucket string, incarnation uint64) []byte {
	return binary.AppendUvarint(dropRecordNamePrefix(database, bucket), incarnation)
}

// setDropRecord adds the record of dropped to batch.
func (s *Server) setDropRecord(batch storage.KVBatch, database, bucket string, dropped droppedIncarnation) error {
	encoded, err := bson.Marshal(dropRecord{OriginSeq: dropped.originSeq, Epoch: dropped.epoch})
	if err != nil {
		return fmt.Errorf("encoding the drop record of bucket %q in database %q: %w", bucket, database, err)
	}
	if err := batch.Set(dropRecordKey(database, bucket, dropped.incarnation), encoded); err != nil {
		return fmt.Errorf("writing the drop record of bucket %q in database %q: %w", bucket, database, err)
	}
	if s.cluster != nil {
		s.cluster.dropRecordsMaybe.Store(true)
	}
	return nil
}

// dropRecorded reports whether this node recorded incarnation of
// database/bucket dropped.
func (s *Server) dropRecorded(database, bucket string, incarnation uint64) (bool, error) {
	snapshot := s.kv.NewSnapshot()
	defer snapshot.Discard()
	_, err := snapshot.Get(dropRecordKey(database, bucket, incarnation))
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, storage.ErrKeyNotFound):
		return false, nil
	default:
		return false, fmt.Errorf("reading the drop record of bucket %q in database %q: %w", bucket, database, err)
	}
}

// droppedIncarnations is every incarnation of database/bucket this node
// recorded dropped; usually none.
func (s *Server) droppedIncarnations(database, bucket string) ([]uint64, error) {
	prefix := dropRecordNamePrefix(database, bucket)
	snapshot := s.kv.NewSnapshot()
	defer snapshot.Discard()
	iterator := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: keyPrefixEnd(prefix)})
	defer iterator.Close()
	var incarnations []uint64
	for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		incarnation, read := binary.Uvarint(iterator.Key()[len(prefix):])
		if read <= 0 {
			return nil, fmt.Errorf("decoding a drop record of bucket %q in database %q: the incarnation is malformed", bucket, database)
		}
		incarnations = append(incarnations, incarnation)
	}
	return incarnations, nil
}

// heldIncarnation is the incarnation of the copy of database/bucket recorded
// under (slot, id).
func (s *Server) heldIncarnation(database string, slot uint16, id uint64) (uint64, error) {
	snapshot := s.kv.NewSnapshot()
	defer snapshot.Discard()
	st, err := store.LoadBucketState(snapshot, slot, storage.LegacyDatabaseID(database), id)
	if err != nil {
		return 0, err
	}
	return st.Incarnation, nil
}

// recordDropOfNoCopy is a drop entry's apply on a node that holds no copy of
// the incarnation it names: nothing is deleted, the drop is recorded, and,
// when clearPending says the node holds no copy of the name at all, the name's
// placement marks, awaiting records and needs-a-copy record go with it. A node
// that holds a newer incarnation keeps them: they are that incarnation's.
func (s *Server) recordDropOfNoCopy(database, bucket string, dropped droppedIncarnation, clearPending bool) error {
	batch := s.kv.NewBatch()
	defer batch.Discard()
	if err := s.setDropRecord(batch, database, bucket, dropped); err != nil {
		return err
	}
	if clearPending {
		if err := batch.Delete(placementMarkKey(placementOf(database, bucket))); err != nil {
			return fmt.Errorf("deleting the placement marks of bucket %q in database %q: %w", bucket, database, err)
		}
		snapshot := s.kv.NewSnapshot()
		defer snapshot.Discard()
		if err := deleteNodeRecords(snapshot, batch, storage.AwaitingRecordKind, bucketRecordScope(placementOf(database, bucket)), nil); err != nil {
			return fmt.Errorf("deleting the awaiting records of bucket %q in database %q: %w", bucket, database, err)
		}
		if err := batch.Delete(needsCopyKey(placementOf(database, bucket))); err != nil {
			return fmt.Errorf("deleting the needs-a-copy record of bucket %q in database %q: %w", bucket, database, err)
		}
	}
	if err := batch.Commit(true); err != nil {
		return fmt.Errorf("committing the drop record of bucket %q in database %q: %w", bucket, database, err)
	}
	if clearPending && s.cluster != nil {
		s.cluster.forgetPendingName(database, bucket, 0)
		s.cluster.forgetAwaitingBucket(placementOf(database, bucket))
		s.cluster.forgetPendingCopy(placementOf(database, bucket))
	}
	return nil
}

// forgetPendingName takes a dropped name off everything this node keeps
// pending for it (review M-2 (a)): its placement marks, the take-over
// worker's queue, the sweep's retry list and residue, and the orphan
// creates. The pulls of the name were cancelled by dropClearsPulls. The 'h'
// record on disk is deleted in the drop's batch. droppedID is the catalog id of
// the bucket dropped, 0 for a node that held none.
func (c *Cluster) forgetPendingName(database, bucket string, droppedID uint64) {
	c.forgetPlacementMarks(placementOf(database, bucket))
	c.forgetTakeOverOf(placementOf(database, bucket))
	held := heldBucket{database: database, bucket: bucket}
	without := func(list []heldBucket) []heldBucket {
		kept := make([]heldBucket, 0, len(list))
		for _, pending := range list {
			if pending != held {
				kept = append(kept, pending)
			}
		}
		return kept
	}
	c.sweepLeftMu.Lock()
	c.noteDroppedInRound(held, droppedID)
	c.retrying.list = without(c.retrying.list)
	c.residue.list = without(c.residue.list)
	delete(c.orphans, held)
	delete(c.orphanCopies, held)
	c.sweepLeftMu.Unlock()
	c.markIfResidueClear()
}

// errPullOfDroppedIncarnation is a pull refused because the copy it would
// land is of an incarnation this node recorded dropped (R8-C).
var errPullOfDroppedIncarnation = errors.New("the copy is of an incarnation of the bucket this node dropped; it is not landed")

// pullOfDroppedIncarnationError names the incarnation a refused pull carried,
// which handleMigrate tells the asking holder. It matches
// errPullOfDroppedIncarnation.
type pullOfDroppedIncarnationError struct {
	database, bucket string
	incarnation      uint64
}

func (e *pullOfDroppedIncarnationError) Error() string {
	return fmt.Sprintf("pulling bucket %q in database %q: incarnation %d: %v", e.bucket, e.database, e.incarnation, errPullOfDroppedIncarnation)
}

func (e *pullOfDroppedIncarnationError) Unwrap() error { return errPullOfDroppedIncarnation }

// droppedHereError is a member's answer to a hand-off that it dropped the
// incarnation it was asked to take: the asking holder's copy of it is a
// dropped bucket's.
type droppedHereError struct {
	member      string
	incarnation uint64
	message     string
}

func (e *droppedHereError) Error() string {
	return fmt.Sprintf("node %s dropped incarnation %d of this bucket: %s", e.member, e.incarnation, e.message)
}

// dropCopyDroppedElsewhere deletes this node's copy of database/bucket when
// it is the incarnation a member answered dropped: a drop being delivered by
// the hand-off, not a release, so it is recorded like one (withDropEntry). It
// reports whether it deleted the copy.
func (c *Cluster) dropCopyDroppedElsewhere(ctx context.Context, database, bucket string, incarnation uint64) (bool, error) {
	fence := c.s.acquireFence(placementOf(database, bucket), fenceExclusive)
	defer fence.release()
	if !c.s.bucketStoreOpen(database, bucket) {
		return false, nil
	}
	st, err := c.s.bucketStateNamed(database, bucket)
	if err != nil {
		return false, err
	}
	if st.Incarnation != incarnation {
		return false, nil
	}
	dropped := withDropEntry(ctx, droppedIncarnation{incarnation: incarnation, epoch: c.Epoch()})
	resp, err := c.s.DeleteBucketCtx(dropped, DeleteBucketRequest{Database: database, Name: bucket})
	if err != nil {
		return false, fmt.Errorf("dropping this node's copy of bucket %q in database %q, which a member dropped: %w", bucket, database, err)
	}
	if !resp.Success {
		return false, fmt.Errorf("dropping this node's copy of bucket %q in database %q, which a member dropped: %s", bucket, database, resp.Message)
	}
	c.logger.Info("dropped a copy of a bucket incarnation a member had dropped; the drop had not reached this node yet",
		"database", database, "bucket", bucket, "incarnation", incarnation)
	return true, nil
}

// listedDrop is one drop record the sweep found.
type listedDrop struct {
	ref         ReplBucketRef
	incarnation uint64
	key         []byte
}

// listDropRecords reads every drop record on this node.
func (s *Server) listDropRecords() ([]listedDrop, error) {
	prefix := dropRecordPrefix()
	snapshot := s.kv.NewSnapshot()
	defer snapshot.Discard()
	iterator := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: keyPrefixEnd(prefix)})
	defer iterator.Close()
	var listed []listedDrop
	for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		key := iterator.Key()
		database, bucket, incarnation, err := decodeDropRecordSuffix(key[len(prefix):])
		if err != nil {
			return nil, err
		}
		listed = append(listed, listedDrop{ref: ReplBucketRef{Database: database, Bucket: bucket}, incarnation: incarnation, key: slices.Clone(key)})
	}
	return listed, nil
}

// decodeDropRecordSuffix reads a drop record's key after its kind.
func decodeDropRecordSuffix(suffix []byte) (database, bucket string, incarnation uint64, err error) {
	malformed := errors.New("a drop record's key is malformed")
	field := func() (string, bool) {
		length, read := binary.Uvarint(suffix)
		if read <= 0 || uint64(len(suffix)-read) < length {
			return "", false
		}
		value := string(suffix[read : read+int(length)])
		suffix = suffix[read+int(length):]
		return value, true
	}
	var ok bool
	if database, ok = field(); !ok {
		return "", "", 0, malformed
	}
	if bucket, ok = field(); !ok {
		return "", "", 0, malformed
	}
	incarnation, read := binary.Uvarint(suffix)
	if read <= 0 || read != len(suffix) {
		return "", "", 0, malformed
	}
	return database, bucket, incarnation, nil
}

// rememberedEpochs is the epochs of the membership history this node keeps,
// at most gossipHistoryLimit, the latest, in order. The caller holds historyMu.
func (c *Cluster) rememberedEpochs() []uint64 {
	epochs := make([]uint64, 0, len(c.history))
	for epoch := range c.history {
		epochs = append(epochs, epoch)
	}
	slices.Sort(epochs)
	if limit := int(gossipHistoryLimit.Load()); len(epochs) > limit {
		epochs = epochs[len(epochs)-limit:]
	}
	return epochs
}

// ownedByARemovedNode reports whether a node removed within the membership
// history owned the name placed by key at an epoch the history keeps. An
// owner that is still a member holds the name's copy or is taking it over,
// which the catalog survey and the take-over's wait find; one that was
// removed may hold the only copy, and no member knows of it.
func (c *Cluster) ownedByARemovedNode(key placementKey) bool {
	removed := c.removedInHistory()
	if len(removed) == 0 {
		return false
	}
	c.historyMu.Lock()
	epochs := c.rememberedEpochs()
	c.historyMu.Unlock()
	for _, epoch := range epochs {
		placed, recorded := c.ringAt(epoch)
		if !recorded {
			continue
		}
		owner, err := placed.Owner(key.encoded)
		if err == nil && nodeListed(removed, owner.ID) {
			return true
		}
	}
	return false
}

// removedInHistory is every node in the member sets of the membership history
// this node keeps — at most gossipHistoryLimit epochs, the latest — that is
// not a member now. A node removed before that is gone.
func (c *Cluster) removedInHistory() []ring.Node {
	_, current := c.epochMembers()
	c.historyMu.Lock()
	epochs := c.rememberedEpochs()
	var removed []ring.Node
	for _, epoch := range epochs {
		for _, node := range c.history[epoch] {
			if node.ID == c.self.ID || nodeListed(current, node.ID) || nodeListed(removed, node.ID) {
				continue
			}
			removed = append(removed, node)
		}
	}
	c.historyMu.Unlock()
	return removed
}

// maybeSweepTombstones starts the tombstone sweep, at most once per
// sweepRearmInterval and only on a node that may hold a record. It runs at the
// end of every sweep and every rearm, and from the rearm's idle path, which a
// healthy gossip round calls.
func (c *Cluster) maybeSweepTombstones() {
	if !c.dropRecordsMaybe.Load() || c.removed() {
		return
	}
	if since := time.Since(time.Unix(0, c.tombstoneSweepAt.Load())); since < sweepRearmInterval.get() {
		return
	}
	if !c.tombstoneSweeping.CompareAndSwap(false, true) {
		return
	}
	c.tombstoneSweepAt.Store(time.Now().UnixNano())
	if !c.startStoreGoroutine() {
		c.tombstoneSweeping.Store(false)
		return
	}
	go func() {
		defer c.storeWG.Done()
		defer c.tombstoneSweeping.Store(false)
		if _, err := c.sweepTombstones(c.ctx); err != nil {
			c.logger.Warn("the drop-record sweep could not finish; it runs again with the next sweep", "error", err)
		}
	}()
}

// sweepTombstones deletes every drop record no node can hand back: every
// current member and every node removed within the history answered that it
// holds no copy of that incarnation. A node that does not answer keeps every
// record. It holds no fence and no lock across its questions, and returns how
// many records it deleted.
func (c *Cluster) sweepTombstones(ctx context.Context) (int, error) {
	listed, err := c.s.listDropRecords()
	if err != nil {
		return 0, err
	}
	if len(listed) == 0 {
		c.dropRecordsMaybe.Store(false)
		return 0, nil
	}
	// One question per name, whatever its dropped incarnations: a set, so a
	// sweep over N records is O(N), not O(N²) (10,000 records swept in 460 ms
	// with a slice search, most of it the search).
	refs := make([]ReplBucketRef, 0, len(listed))
	asked := make(map[ReplBucketRef]struct{}, len(listed))
	for _, drop := range listed {
		if _, seen := asked[drop.ref]; !seen {
			asked[drop.ref] = struct{}{}
			refs = append(refs, drop.ref)
		}
	}
	type heldCopy struct {
		ref         ReplBucketRef
		incarnation uint64
	}
	held := make(map[heldCopy]bool)
	ask := func(node ring.Node, wait time.Duration) error {
		askCtx, cancel := ctx, context.CancelFunc(func() {})
		if wait > 0 {
			askCtx, cancel = context.WithTimeout(ctx, wait)
		}
		defer cancel()
		copies, err := c.askCopies(askCtx, node, refs)
		if err != nil {
			return err
		}
		for i, answered := range copies {
			if answered.Holds {
				held[heldCopy{ref: refs[i], incarnation: answered.Incarnation}] = true
			}
		}
		return nil
	}
	for _, member := range c.otherMembers() {
		if err := ask(member, 0); err != nil {
			return 0, fmt.Errorf("asking member %s which dropped incarnations it holds; every drop record is kept: %w", member.ID, err)
		}
	}
	for _, removed := range c.removedInHistory() {
		if err := ask(removed, removedNodeQuestionWait); err != nil {
			return 0, fmt.Errorf("asking %s, removed from the membership, which dropped incarnations it holds; every drop record is kept until it answers or ages out of the history: %w", removed.ID, err)
		}
	}
	var gone [][]byte
	for _, drop := range listed {
		if !held[heldCopy{ref: drop.ref, incarnation: drop.incarnation}] {
			gone = append(gone, drop.key)
		}
	}
	for start := 0; start < len(gone); start += dropRecordBatch {
		batch := c.s.kv.NewBatch()
		for _, key := range gone[start:min(start+dropRecordBatch, len(gone))] {
			if err := batch.Delete(key); err != nil {
				batch.Discard()
				return start, fmt.Errorf("deleting a drop record: %w", err)
			}
		}
		if err := batch.Commit(false); err != nil {
			batch.Discard()
			return start, fmt.Errorf("committing the deletion of drop records: %w", err)
		}
		batch.Discard()
	}
	if len(gone) == len(listed) {
		c.dropRecordsMaybe.Store(false)
	}
	return len(gone), nil
}
