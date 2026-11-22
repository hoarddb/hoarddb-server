package server

import (
	"encoding/binary"
	"fmt"
	"slices"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/storage"
)

// The membership history (build doc step 6.5a, A-6.2; review F-2).
//
// A node ships a data entry it logged as a bucket's owner only if it has owned
// the bucket at every membership epoch since the entry was logged (R-OWNS as
// amended by spec §20.2 F-2). An owner that lost a bucket and regained it does
// not ship what it logged in its first era: a node that copied the bucket in
// between holds those entries by its copy, and would apply them twice. To judge
// that, a node keeps the member set of each of the last epochs it adopted:
//
//	"m" ‖ uvarint(epoch)   BSON {members: [{id, addr}]}, under storage.NSReplication
//
// written, synced, before the ring of that epoch replaces the one before it.
// The addresses are kept because dispatch 9 dials a node that is no longer a
// member. A node that missed epochs — it learned a later one by gossip — has
// no record of them, and judges an entry logged before the gap stranded: the
// safe side (SA-6.4).
const (
	membershipRecordKind byte = 'm'
	// membershipHistoryLength is how many epochs the history keeps.
	membershipHistoryLength = 64
)

// gossipHistoryLimit is how many history records a gossip answer carries at
// most. A variable so a test can make a node miss more changes than its peer
// remembers without making 65 of them. Atomic because the cluster's own
// goroutines read it while a test may be changing it.
var gossipHistoryLimit = newGossipHistoryLimit()

func newGossipHistoryLimit() *atomic.Int64 {
	limit := new(atomic.Int64)
	limit.Store(membershipHistoryLength)
	return limit
}

// membershipRecord is the value of an "m" record.
type membershipRecord struct {
	Members []Peer `bson:"members"`
}

// membershipHistoryKey is the key of epoch's record.
func membershipHistoryKey(epoch uint64) []byte {
	return storage.NodeKey(storage.NSReplication,
		[]byte{membershipRecordKind}, binary.AppendUvarint(nil, epoch))
}

// loadMembershipHistory reads every "m" record at start, and writes the record
// of the current epoch if it is missing: a node's first start on this build,
// or a data directory older than the history.
func (c *Cluster) loadMembershipHistory() error {
	prefix := storage.NodeKey(storage.NSReplication, []byte{membershipRecordKind})
	snapshot := c.s.kv.NewSnapshot()
	defer snapshot.Discard()
	iterator := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: keyPrefixEnd(prefix)})
	defer iterator.Close()
	history := make(map[uint64][]ring.Node)
	for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		epoch, read := binary.Uvarint(iterator.Key()[len(prefix):])
		if read <= 0 || read != len(iterator.Key())-len(prefix) {
			return fmt.Errorf("the membership history record %x has no readable epoch; the node store is damaged — restore it from a backup", iterator.Key())
		}
		stored, err := iterator.Value()
		if err != nil {
			return fmt.Errorf("reading the membership of epoch %d: %w", epoch, err)
		}
		var record membershipRecord
		if err := bson.Unmarshal(stored, &record); err != nil {
			return fmt.Errorf("decoding the membership of epoch %d: %w", epoch, err)
		}
		history[epoch] = peersToNodes(record.Members)
		c.noteHistoryMembers(history[epoch])
	}
	if err := iterator.Err(); err != nil {
		return fmt.Errorf("walking the membership history: %w", err)
	}
	c.historyMu.Lock()
	c.history = history
	c.ringsAt = make(map[uint64]*ring.Ring)
	c.historyMu.Unlock()
	c.noteViewChanged()
	epoch, members := c.epochMembers()
	if _, recorded := history[epoch]; recorded {
		return nil
	}
	return c.recordMembership(epoch, members)
}

// recordMembership writes epoch's member set to the history, synced, and
// deletes the records that fall out of it.
func (c *Cluster) recordMembership(epoch uint64, members []ring.Node) error {
	encoded, err := bson.Marshal(membershipRecord{Members: nodesToPeers(members)})
	if err != nil {
		return fmt.Errorf("encoding the membership of epoch %d: %w", epoch, err)
	}
	c.historyMu.Lock()
	defer c.historyMu.Unlock()
	batch := c.s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(membershipHistoryKey(epoch), encoded); err != nil {
		return fmt.Errorf("recording the membership of epoch %d: %w", epoch, err)
	}
	var expired []uint64
	for recorded := range c.history {
		if recorded+membershipHistoryLength <= epoch {
			expired = append(expired, recorded)
			if err := batch.Delete(membershipHistoryKey(recorded)); err != nil {
				return fmt.Errorf("expiring the membership of epoch %d: %w", recorded, err)
			}
		}
	}
	if err := batch.Commit(true); err != nil {
		return fmt.Errorf("recording the membership of epoch %d: %w", epoch, err)
	}
	for _, recorded := range expired {
		delete(c.history, recorded)
		delete(c.ringsAt, recorded)
	}
	c.history[epoch] = append([]ring.Node(nil), members...)
	c.noteHistoryMembers(members)
	c.noteViewChanged()
	return nil
}

// noteHistoryMembers sets historyNamedAnotherNode when members, a member set
// entering the history, names a node other than this one. It is called before
// the set is readable from the history, or under historyMu with it, so no
// reader of the history finds another node while the flag is false.
func (c *Cluster) noteHistoryMembers(members []ring.Node) {
	for _, node := range members {
		if node.ID != c.self.ID {
			c.historyNamedAnotherNode.Store(true)
			return
		}
	}
}

// forgetMembership deletes epoch's record from the history, synced. A node
// that started on a provisional view — its seeds and itself — forgets it when
// it adopts its join: no member ever held that view, and a ring built from it
// would answer for the cluster's first epoch (owner ruling 52, R8-D (b)).
func (c *Cluster) forgetMembership(epoch uint64) error {
	c.historyMu.Lock()
	defer c.historyMu.Unlock()
	batch := c.s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Delete(membershipHistoryKey(epoch)); err != nil {
		return fmt.Errorf("forgetting the provisional membership of epoch %d: %w", epoch, err)
	}
	if err := batch.Commit(true); err != nil {
		return fmt.Errorf("forgetting the provisional membership of epoch %d: %w", epoch, err)
	}
	delete(c.history, epoch)
	delete(c.ringsAt, epoch)
	c.noteViewChanged()
	return nil
}

// overwriteMembership writes members as epoch's record over the one the
// history holds, synced: a joiner's provisional view replaced by the cluster's
// record of the same epoch, carried in the answer it adopts its join from
// (on-demand pull spec §6.7). The cached ring of the epoch is dropped with it.
func (c *Cluster) overwriteMembership(epoch uint64, members []ring.Node) error {
	encoded, err := bson.Marshal(membershipRecord{Members: nodesToPeers(members)})
	if err != nil {
		return fmt.Errorf("encoding the cluster's membership of epoch %d: %w", epoch, err)
	}
	c.historyMu.Lock()
	defer c.historyMu.Unlock()
	batch := c.s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Set(membershipHistoryKey(epoch), encoded); err != nil {
		return fmt.Errorf("writing the cluster's membership of epoch %d over the provisional one: %w", epoch, err)
	}
	if err := batch.Commit(true); err != nil {
		return fmt.Errorf("writing the cluster's membership of epoch %d over the provisional one: %w", epoch, err)
	}
	c.history[epoch] = append([]ring.Node(nil), members...)
	delete(c.ringsAt, epoch)
	c.noteHistoryMembers(members)
	c.noteViewChanged()
	return nil
}

// MembershipRecord is one epoch of a node's membership history, as a gossip
// answer carries it to a node that is behind (step 9.4).
type MembershipRecord struct {
	Epoch   uint64 `bson:"epoch" json:"epoch"`
	Members []Peer `bson:"members" json:"members"`
}

// historyAbove returns this node's records of the epochs above after, oldest
// first: at most gossipHistoryLimit of them, the latest.
func (c *Cluster) historyAbove(after uint64) []MembershipRecord {
	c.historyMu.Lock()
	epochs := make([]uint64, 0, len(c.history))
	for epoch := range c.history {
		if epoch > after {
			epochs = append(epochs, epoch)
		}
	}
	slices.Sort(epochs)
	if limit := int(gossipHistoryLimit.Load()); len(epochs) > limit {
		epochs = epochs[len(epochs)-limit:]
	}
	records := make([]MembershipRecord, 0, len(epochs))
	for _, epoch := range epochs {
		records = append(records, MembershipRecord{Epoch: epoch, Members: nodesToPeers(c.history[epoch])})
	}
	c.historyMu.Unlock()
	return records
}

// membersAt is the member set of epoch as this node's history records it;
// false when the epoch is not in the history.
func (c *Cluster) membersAt(epoch uint64) ([]ring.Node, bool) {
	c.historyMu.Lock()
	defer c.historyMu.Unlock()
	members, recorded := c.history[epoch]
	return append([]ring.Node(nil), members...), recorded
}

// committedAsProposed answers a vote or a commit whose adoption found this
// node already past the epoch (errAdoptionNotAhead): a catch-up adopted it in
// the meantime. Recorded with the proposed member set, the change is
// committed; recorded with another, two changes were made under one number.
func (c *Cluster) committedAsProposed(epoch uint64, proposed []ring.Node) error {
	recorded, known := c.membersAt(epoch)
	if !known {
		return fmt.Errorf("%w: this node moved past membership epoch %d while the change was being agreed and no longer "+
			"remembers that epoch's member set; run 'nodes' to see the membership it is on", ErrMemberMismatch, epoch)
	}
	if !memberSetsEqual(memberIDSetOf(recorded), memberIDSetOf(proposed)) {
		return fmt.Errorf("%w: membership epoch %d was committed with the members %v while this change, to %v, was being agreed; "+
			"run 'nodes' to see the membership, and propose the change again if it is still wanted",
			ErrMemberMismatch, epoch, memberIDsOf(recorded), memberIDsOf(proposed))
	}
	return nil
}

// ringAt is the placement ring of epoch's members, built once and cached; false
// when the epoch is not in the history.
func (c *Cluster) ringAt(epoch uint64) (*ring.Ring, bool) {
	c.historyMu.Lock()
	defer c.historyMu.Unlock()
	if built, cached := c.ringsAt[epoch]; cached {
		return built, true
	}
	members, recorded := c.history[epoch]
	if !recorded {
		return nil, false
	}
	built := ring.New(c.rf)
	for _, member := range members {
		if err := built.AddNode(member); err != nil {
			return nil, false
		}
	}
	c.ringsAt[epoch] = built
	return built, true
}

// ownedSince reports whether this node owned the bucket placed by key at every
// epoch from the one after loggedAt to the current one. An epoch missing from
// the history answers no.
func (c *Cluster) ownedSince(key placementKey, loggedAt uint64) bool {
	current := c.Epoch()
	for epoch := loggedAt + 1; epoch <= current; epoch++ {
		placement, recorded := c.ringAt(epoch)
		if !recorded {
			return false
		}
		owner, err := placement.Owner(key.encoded)
		if err != nil || owner.ID != c.self.ID {
			return false
		}
	}
	return true
}
