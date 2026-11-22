package server

import (
	"fmt"
	"slices"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The set an entry is delivered to.
//
// An entry is addressed to the replica set of the bucket it is about, at the
// factor the bucket had when the entry was placed. That is a fact about the
// entry, and it must never be recomputed from the registry as it stands when
// the entry is looked at again: a drop forgets the factor as it applies, and the
// name can be re-created at another one before a replica that was down comes
// back. Three call sites once decided this separately (the owner's live
// delivery, catch-up's filter, retention's bookkeeping) and disagreed exactly
// there (verifier D-1). deliverySet is the one function the two that remain
// call: retention no longer keeps drops of its own, because its bound is every
// member's cursor (spec §19.3.9).
//
//	create        the factor the create declares, else the node default
//	drop          previous_factor, stamped by the owner before it logs the drop
//	              (every drop, an inheriting bucket's included); a drop logged
//	              before stamping existed, the node default
//	alter         the larger of previous_factor and replication_factor, both
//	              stamped by the owner before it logs the alter: the union of
//	              the sets before and after the change
//	anything else the bucket's factor now: nothing but a create, a drop and
//	              an alter changes it
//
// The owner is the same node at every factor (the ring's prefix property), so
// the owner of an entry does not depend on any of this.

// entryFactor is the factor an entry was placed at.
func (c *Cluster) entryFactor(entry ReplEntry, key placementKey) int {
	switch transport.Opcode(entry.Op) {
	case transport.OpCreateBucket:
		var req CreateBucketRequest
		if err := bson.Unmarshal(entry.Payload, &req); err == nil && req.ReplicationFactor > 0 {
			return req.ReplicationFactor
		}
		return c.rf
	case transport.OpDeleteBucket:
		if previous := previousFactorOf(entry.Payload); previous > 0 {
			return int(previous)
		}
		return c.rf
	case transport.OpAlterBucket:
		// The union of the sets before and after, which by the ring's prefix
		// property is the set at the larger factor. Only a payload the owner
		// has stamped is passed here: before the stamp, previous_factor is
		// whatever the client sent.
		return int(max(previousFactorOf(entry.Payload), alteredFactorOf(entry.Payload)))
	default:
		return c.effectiveRF(key)
	}
}

// deliverySet is the replica set an entry is addressed to, owner first.
func (c *Cluster) deliverySet(entry ReplEntry, key placementKey) ([]ring.Node, error) {
	return c.replicasAtFactor(key, c.entryFactor(entry, key))
}

// deliveryTargets is deliverySet without this node.
func (c *Cluster) deliveryTargets(entry ReplEntry, key placementKey) ([]ring.Node, error) {
	set, err := c.deliverySet(entry, key)
	if err != nil {
		return nil, err
	}
	targets := make([]ring.Node, 0, len(set))
	for _, node := range set {
		if node.ID != c.self.ID {
			targets = append(targets, node)
		}
	}
	return targets, nil
}

// deliverTo is who an entry this node logged is addressed to, on the ring and
// the registry as they stand now (spec §19.3.4). The owner's live routing, the
// broadcast of database DDL and the sender's replay of its log call it, and
// nothing else decides it:
//
//	a create of a database, or of a
//	  bucket, in a generation this
//	  node holds dropped                 nobody (G-10; a drop is always shipped)
//	no bucket (create or drop database)  every other member
//	a bucket this node does not own      nobody
//	any entry but a create or a drop,
//	  of a bucket this node no longer
//	  holds                              nobody
//	a bucket this node owns              deliveryTargets
//
// It is evaluated when an entry is routed or replayed, never stored: an entry
// this node did not originate is never passed to it, so what is left to decide
// is only where one it did originate goes now.
//
// The fourth row is spec §20.2's (A-3.2). A replay reads entries logged before
// the bucket they are about was dropped; the drop is what a peer is owed, and a
// write to the dropped bucket would be refused by every peer that does not
// hold it, for ever, with everything behind it on the stream.
func (c *Cluster) deliverTo(op transport.Opcode, bucket string, payload []byte) ([]ring.Node, error) {
	targets, _, err := c.addressing(op, bucket, payload, 0)
	return targets, err
}

// addressing is deliverTo, and whether its answer of nobody is the third row's:
// a bucket entry of a bucket this node does not own (R-OWNS), which the replay
// counts as stranded. loggedAt is the epoch an `own` entry was logged under;
// such an entry is this node's to ship only if it has owned the bucket at every
// epoch since (spec §20.2 F-2, build doc step 6.5a). Zero — an entry being
// routed now, or one logged before entries carried an epoch — is judged by the
// ring as it stands.
func (c *Cluster) addressing(op transport.Opcode, bucket string, payload []byte, loggedAt uint64) (targets []ring.Node, notOwned bool, err error) {
	dropped, err := c.s.createOfDroppedGeneration(op, payload)
	if err != nil || dropped {
		return nil, false, err
	}
	// A removed node delivers nothing: every member refuses its pushes, and
	// what it owned is someone else's now (step 6.5b).
	if c.removedAt.Load() != 0 {
		return nil, bucket != "", nil
	}
	if bucket == "" {
		return c.otherMembers(), false, nil
	}
	_, database := decodeFrameMeta(payload).scope(op)
	key := placementOf(database, bucket)
	if !c.actsAsOwner(key) || (loggedAt > 0 && !c.ownedSince(key, loggedAt)) {
		return nil, true, nil
	}
	// A drop goes to every member, not only the set (R8-C): a copy can sit
	// outside the set — a hand-off not yet confirmed, a sweep that gave up —
	// and a member applies it only to a copy of the incarnation it names.
	// Its concern is still the set's (applyAsOwner).
	if op == transport.OpDeleteBucket {
		return c.otherMembers(), false, nil
	}
	if op != transport.OpCreateBucket && !c.s.bucketStoreOpen(database, bucket) {
		return nil, false, nil
	}
	targets, err = c.deliveryTargets(ReplEntry{Op: int32(op), Bucket: bucket, Payload: payload}, key)
	if err != nil || !filtersAwaiting(op) {
		return targets, false, err
	}
	// Rule F5: a member awaiting its copy is sent nothing of the bucket but
	// the alter. The live path reads this inside the bucket's fence (F1).
	match, err := c.awaitingMatchOfCopy(database, bucket)
	if err != nil {
		return nil, false, err
	}
	return withoutMembers(targets, c.awaitingMatching(key, match)), false, nil
}

// bucketEntryAddressing is who a bucket entry this node's replay reads back
// from its log is addressed to, a drop aside (the sender's rule, spec
// §20.8.3, build doc step 10b.4), and whether the entry is one this node may
// no longer ship: stranded, when it is its own.
//
//	a legacy entry (logged before origins)  the current set, from the ring's
//	                                        owner only, while the bucket's
//	                                        term is 0 and its copy has not
//	                                        moved (A-10.4)
//	an own entry, the copy not opened, or
//	  its epoch below the copy's term      nobody: stranded
//	an own entry, this node the owner      the current set
//	an own entry, this node ceded or was
//	  removed                              the members of the current set
//	                                       that were in the bucket's set at
//	                                       the entry's epoch: the old owner
//	                                       finishes delivering its term, and
//	                                       a newcomer copies the bucket
//	                                       instead; nobody, stranded, when
//	                                       that epoch has left the history
//
// A create is one entry of its term like any other, so a removed or ceded
// node ships its creates to the old set only, never to a node that joined
// since. st is the state of the entry's bucket as this node holds it now.
func (c *Cluster) bucketEntryAddressing(e storage.WALEntry, st store.BucketState) (targets []ring.Node, stranded bool, err error) {
	op := transport.Opcode(e.Op)
	dropped, err := c.s.createOfDroppedGeneration(op, e.Data)
	if err != nil {
		return nil, false, fmt.Errorf("reading whether entry %d creates a bucket in a dropped database generation: %w", e.Seq, err)
	}
	if dropped {
		return nil, false, nil
	}
	_, database := decodeFrameMeta(e.Data).scope(op)
	key := placementOf(database, e.Bucket)
	held := c.s.bucketStoreOpen(database, e.Bucket)
	if op != transport.OpCreateBucket && !held {
		return nil, false, nil
	}
	entry := ReplEntry{Op: int32(op), Bucket: e.Bucket, Payload: e.Data}
	// Rule F5, on every branch: a member awaiting its copy is shipped nothing
	// of the bucket but the alter, matched by the term and generation of the
	// copy this node holds now.
	var match awaitingMatch
	if filtersAwaiting(op) {
		match = c.awaitingMatchAt(database, e.Bucket, st.Term)
	}
	if e.Origin.Kind != storage.WALOriginOwn {
		// A legacy entry has no origin: this node cannot tell one it wrote
		// from one it applied as a replica, so it ships none once a term has
		// been opened, accepted or copied in, nor while it is taking the
		// bucket over — only an owner that has owned it since the upgrade
		// ships one.
		marks := c.placementMarksOf(key)
		if c.removed() || !c.actsAsOwner(key) || st.Term != 0 || marks.Moved != 0 || !c.copyIsOpened(key, st, marks) {
			return nil, false, nil
		}
		targets, err = c.deliveryTargets(entry, key)
		if err != nil {
			return nil, false, fmt.Errorf("addressing legacy entry %d of bucket %q in database %q: %w", e.Seq, e.Bucket, database, err)
		}
		return withoutMembers(targets, c.awaitingMatching(key, match)), false, nil
	}
	if held && (!c.copyIsOpened(key, st, c.placementMarksOf(key)) || e.Origin.Epoch < st.Term) {
		return nil, true, nil
	}
	if c.actsAsOwner(key) {
		current, err := c.deliveryTargets(entry, key)
		if err != nil {
			return nil, false, fmt.Errorf("addressing entry %d of bucket %q in database %q: %w", e.Seq, e.Bucket, database, err)
		}
		return withoutMembers(current, c.awaitingMatching(key, match)), false, nil
	}
	targets, recorded, err := c.cededAddressees(key, e.Origin.Epoch, c.entryFactor(entry, key), match)
	if err != nil {
		return nil, false, fmt.Errorf("addressing entry %d of bucket %q in database %q: %w", e.Seq, e.Bucket, database, err)
	}
	if !recorded {
		return nil, true, nil
	}
	return targets, len(targets) == 0, nil
}

// cededAddressees is spec §6.4's one predicate for a node that ceded or was
// removed from a bucket's ownership (S3), which a ceded owner's follower also
// waits on (S2, Cluster.termDelivered): the members of the bucket's current
// set at factor that were in its set at epoch then, at the same factor,
// without this node and without the members awaiting a copy that match
// matches (rule F5). recorded is false when then's ring has left the
// membership history; each caller says what that means for it.
func (c *Cluster) cededAddressees(key placementKey, then uint64, factor int, match awaitingMatch) (targets []ring.Node, recorded bool, err error) {
	current, err := c.replicasAtFactor(key, factor)
	if err != nil {
		return nil, false, err
	}
	thenRing, recorded := c.ringAt(then)
	if !recorded {
		return nil, false, nil
	}
	thenSet, err := thenRing.Replicas(key.encoded, factor)
	if err != nil {
		return nil, false, fmt.Errorf("placing the bucket at epoch %d: %w", then, err)
	}
	awaiting := c.awaitingMatching(key, match)
	for _, member := range current {
		if member.ID != c.self.ID && nodeListed(thenSet, member.ID) && !slices.Contains(awaiting, member.ID) {
			targets = append(targets, member)
		}
	}
	return targets, true, nil
}

// otherMembers is every member but this node.
func (c *Cluster) otherMembers() []ring.Node {
	members := c.Members()
	others := make([]ring.Node, 0, len(members))
	for _, member := range members {
		if member.ID != c.self.ID {
			others = append(others, member)
		}
	}
	return others
}

// stampDrop puts the factor a drop was placed at on its payload, under
// previous_factor, replacing anything the client sent under that name: the
// owner's word is the only one the delivery filter may read. The stamp is on
// every drop, an inheriting bucket's included, because an unstamped drop's
// addressees would be read from a registry that a re-create has since changed.
func (c *Cluster) stampDrop(entry ReplEntry, key placementKey) (ReplEntry, int32, error) {
	if transport.Opcode(entry.Op) != transport.OpDeleteBucket {
		return entry, 0, nil
	}
	placedAt := int32(c.effectiveRF(key))
	var document bson.D
	if err := bson.Unmarshal(entry.Payload, &document); err != nil {
		return entry, 0, fmt.Errorf("decoding the drop of bucket %q to stamp its factor: %w", entry.Bucket, err)
	}
	stamped := make(bson.D, 0, len(document)+1)
	for _, element := range document {
		if !matchesServerField("previous_factor", element.Key) {
			stamped = append(stamped, element)
		}
	}
	stamped = append(stamped, bson.E{Key: "previous_factor", Value: placedAt})
	payload, err := bson.Marshal(stamped)
	if err != nil {
		return entry, 0, fmt.Errorf("stamping the drop of bucket %q with its factor: %w", entry.Bucket, err)
	}
	entry.Payload = payload
	return entry, placedAt, nil
}

// previousFactorOf reads the factor a stamped drop was placed at, or 0.
func previousFactorOf(payload []byte) int32 {
	previous, _ := bson.Raw(payload).Lookup("previous_factor").Int32OK()
	return previous
}

// alteredFactorOf reads the factor an alter asks for, or 0.
func alteredFactorOf(payload []byte) int32 {
	factor, _ := bson.Raw(payload).Lookup("replication_factor").Int32OK()
	return factor
}

// nodeListed reports whether nodes names nodeID.
func nodeListed(nodes []ring.Node, nodeID string) bool {
	for _, node := range nodes {
		if node.ID == nodeID {
			return true
		}
	}
	return false
}

// stampDropIncarnation puts the incarnation of the owner's own copy on a
// drop's payload, read under the drop's exclusive fence (R8-C): the drop
// removes that incarnation and no other, on every member.
func (c *Cluster) stampDropIncarnation(entry ReplEntry) ([]byte, error) {
	_, database := decodeFrameMeta(entry.Payload).scope(transport.OpDeleteBucket)
	st, err := c.s.bucketStateNamed(database, entry.Bucket)
	if err != nil {
		return nil, fmt.Errorf("reading the incarnation the drop of bucket %q removes: %w", entry.Bucket, err)
	}
	stamped, err := stampPayload(entry.Payload, bson.D{{Key: "incarnation", Value: int64(st.Incarnation)}})
	if err != nil {
		return nil, fmt.Errorf("stamping the drop of bucket %q with incarnation %d: %w", entry.Bucket, st.Incarnation, err)
	}
	return stamped, nil
}
