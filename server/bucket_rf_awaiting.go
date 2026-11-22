package server

import (
	"encoding/binary"
	"fmt"
	"slices"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The owner's awaiting records of the factor alter
// (bucket-replication-rev8-spec §3.3). An increase newly places members that
// hold nothing of the bucket; the owner records each one, in the alter's own
// apply batch, until that member's copy is seeded. Nothing at rest: a record
// lives only while a copy is pending, and the in-memory map holds only the
// buckets that have one. The write path reads the map through rule F5
// (awaitingFor): an awaiting member is addressed nothing of the bucket but the
// alter.

// awaitingRecord is the value of one 'a' record: the term and the sequence of
// the alter that placed the member, the database generation of the bucket's
// incarnation, and when the alter applied (wall-clock seconds, read only by the
// listings). 73 bytes of BSON.
type awaitingRecord struct {
	Term       int64 `bson:"term"`
	AlterSeq   int64 `bson:"alter_seq"`
	Generation int64 `bson:"generation"`
	Since      int64 `bson:"since"`
}

// awaitingCopy is what the in-memory map keeps of one awaiting record.
type awaitingCopy struct {
	term       uint64
	generation int64
	since      int64
}

// awaitingPrefix is the prefix of every 'a' record.
func awaitingPrefix() []byte {
	return storage.NodeKey(storage.NSReplication, []byte{storage.AwaitingRecordKind})
}

// awaitingKey is the key of the record of member for the bucket placed by key:
// the kind byte 'a', then the placement key, then uvarint(len(member)) and the
// member. The placement key starts with the database, so one database's
// records are one prefix.
func awaitingKey(key placementKey, member string) []byte {
	return storage.NodeKey(storage.NSReplication, []byte{storage.AwaitingRecordKind},
		[]byte(key.encoded), binary.AppendUvarint(nil, uint64(len(member))), []byte(member))
}

// writeAwaiting puts one awaiting record per member into b.
func writeAwaiting(b storage.KVBatch, key placementKey, members []string, record awaitingRecord) error {
	if len(members) == 0 {
		return nil
	}
	encoded, err := bson.Marshal(record)
	if err != nil {
		return fmt.Errorf("encoding an awaiting record: %w", err)
	}
	for _, member := range members {
		if err := b.Set(awaitingKey(key, member), encoded); err != nil {
			return fmt.Errorf("writing the awaiting record of member %s: %w", member, err)
		}
	}
	return nil
}

// publishAwaiting adds the members an applied increase newly placed to the
// awaiting map. The caller holds the bucket's fence exclusively and has
// committed the records.
func (c *Cluster) publishAwaiting(key placementKey, members []string, record awaitingRecord) {
	c.awaitingMu.Lock()
	defer c.awaitingMu.Unlock()
	c.addAwaitingLocked(key, members, awaitingCopy{term: uint64(record.Term), generation: record.Generation, since: record.Since})
	c.noteReplicationTransition()
}

// addAwaitingLocked records members as awaiting a copy of the bucket placed by
// key. The caller holds awaitingMu.
func (c *Cluster) addAwaitingLocked(key placementKey, members []string, copyOf awaitingCopy) {
	pending := c.awaiting[key]
	if pending == nil {
		pending = make(map[string]awaitingCopy, len(members))
		c.awaiting[key] = pending
	}
	for _, member := range members {
		pending[member] = copyOf
	}
	c.awaitingCount.Store(int64(len(c.awaiting)))
}

// supersedeAwaiting deletes into b the awaiting record, as snap holds it, of
// every member of the bucket placed by key that keeps does not hold: a later
// alter supersedes their pending copies (spec §5.2 step 4, owner R9-Q1). R8
// has refused any alter that would still place one, so it is every member
// awaited. One prefix scan of the bucket's own records, O(pending records).
// It returns the members whose records it deleted.
func supersedeAwaiting(snap storage.KVSnapshot, b storage.KVBatch, key placementKey, keeps []ring.Node) ([]string, error) {
	prefix := append(awaitingPrefix(), key.encoded...)
	iterator := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: keyPrefixEnd(prefix)})
	defer iterator.Close()
	var superseded []string
	for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		_, member, err := parseAwaitingKey(iterator.Key()[len(awaitingPrefix()):])
		if err != nil {
			return nil, err
		}
		if nodeListed(keeps, member) {
			continue
		}
		if err := b.Delete(awaitingKey(key, member)); err != nil {
			return nil, fmt.Errorf("deleting the awaiting record of member %s: %w", member, err)
		}
		superseded = append(superseded, member)
	}
	if err := iterator.Err(); err != nil {
		return nil, fmt.Errorf("walking the awaiting records of a bucket: %w", err)
	}
	return superseded, nil
}

// dropAwaiting removes members from the bucket's awaiting map once the batch
// that deleted their records has committed.
func (c *Cluster) dropAwaiting(key placementKey, members []string) {
	c.awaitingMu.Lock()
	defer c.awaitingMu.Unlock()
	pending := c.awaiting[key]
	for _, member := range members {
		delete(pending, member)
	}
	if len(pending) == 0 {
		delete(c.awaiting, key)
	}
	c.awaitingCount.Store(int64(len(c.awaiting)))
	c.noteReplicationTransition()
}

// clearSeededAwaiting deletes member's awaiting record for the bucket placed
// by key, in a synced batch, and then drops it from the map: the snapshot that
// seeds member calls it inside the bucket's exclusive fence (spec §7.3). The
// sync is one per pull, never per write, and a power loss cannot bring the
// record back to filter a member that holds the bucket (rev 4 F-6). An empty
// member, a snapshot that names no puller, clears nothing: it is never read as
// every member.
func (c *Cluster) clearSeededAwaiting(key placementKey, member string) error {
	if member == "" || !c.awaitsCopyOn(key, member) {
		return nil
	}
	batch := c.s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Delete(awaitingKey(key, member)); err != nil {
		return fmt.Errorf("deleting the awaiting record of member %s: %w", member, err)
	}
	if err := batch.Commit(true); err != nil {
		return fmt.Errorf("deleting the awaiting record of member %s: %w", member, err)
	}
	c.dropAwaiting(key, []string{member})
	return nil
}

// awaitsCopyOn reports whether the awaiting map holds a record of member for
// the bucket placed by key, of any term or generation: one atomic load when
// nothing is pending.
func (c *Cluster) awaitsCopyOn(key placementKey, member string) bool {
	if c.awaitingCount.Load() == 0 {
		return false
	}
	c.awaitingMu.Lock()
	defer c.awaitingMu.Unlock()
	_, awaits := c.awaiting[key][member]
	return awaits
}

// awaitingFor is the members rule F5 drops from the bucket placed by key's
// data entries (spec §6.2, §7.3): those whose awaiting record was written in
// term, for the incarnation of database generation generation. A record of
// another term or generation filters nothing (R9.1-6.6): the drop of its
// incarnation, or its term's clearing, deletes it. One atomic load when
// nothing is pending, one map lookup otherwise. The caller reads term and
// generation first: awaitingMu is a leaf, and they take the record's lock or
// read the store.
func (c *Cluster) awaitingFor(key placementKey, term uint64, generation int64) []string {
	if c.awaitingCount.Load() == 0 {
		return nil
	}
	c.awaitingMu.Lock()
	defer c.awaitingMu.Unlock()
	var members []string
	for member, pending := range c.awaiting[key] {
		if pending.term == term && pending.generation == generation {
			members = append(members, member)
		}
	}
	return members
}

// awaitingMatch is what rule F5 matches the awaiting records of one bucket
// against: its copy's term and database generation. The zero value, for an
// entry F5 does not filter or when nothing is pending, matches no record.
type awaitingMatch struct {
	term       uint64
	generation int64
	applies    bool
}

// filtersAwaiting reports whether rule F5 drops an awaiting member from an
// entry of op: every bucket entry but the create, which cannot meet a record
// of its own incarnation, the drop, which goes to every member, and the alter,
// which goes to its union (spec §4.4, §7.3). A declaration is filtered: the
// snapshot carries it.
func filtersAwaiting(op transport.Opcode) bool {
	return op != transport.OpCreateBucket && op != transport.OpDeleteBucket && op != transport.OpAlterBucket
}

// awaitingMatchAt is the match for this node's copy of database/bucket in
// term, its generation read from the record. Nothing is read when nothing is
// pending anywhere, so the write path pays one atomic load (rule 11).
func (c *Cluster) awaitingMatchAt(database, bucket string, term uint64) awaitingMatch {
	if c.awaitingCount.Load() == 0 {
		return awaitingMatch{}
	}
	facts, held := c.s.peekBucket(database, bucket)
	if !held {
		return awaitingMatch{}
	}
	return awaitingMatch{term: term, generation: facts.generation, applies: true}
}

// awaitingMatchOfCopy is awaitingMatchAt in the term the copy's lineage holds
// now.
func (c *Cluster) awaitingMatchOfCopy(database, bucket string) (awaitingMatch, error) {
	if c.awaitingCount.Load() == 0 {
		return awaitingMatch{}, nil
	}
	term, _, err := c.s.bucketLineage(database, bucket)
	if err != nil {
		return awaitingMatch{}, fmt.Errorf("reading the term of bucket %q in database %q to match its awaiting records: %w", bucket, database, err)
	}
	return c.awaitingMatchAt(database, bucket, term), nil
}

// awaitingMatching is awaitingFor for match, nil for a match that applies to
// nothing.
func (c *Cluster) awaitingMatching(key placementKey, match awaitingMatch) []string {
	if !match.applies {
		return nil
	}
	return c.awaitingFor(key, match.term, match.generation)
}

// withoutMembers is nodes without those ids names, in nodes' order.
func withoutMembers(nodes []ring.Node, ids []string) []ring.Node {
	if len(ids) == 0 {
		return nodes
	}
	kept := make([]ring.Node, 0, len(nodes))
	for _, node := range nodes {
		if !slices.Contains(ids, node.ID) {
			kept = append(kept, node)
		}
	}
	return kept
}

// deleteNodeRecords deletes into b every NSReplication record of kind under
// scope, as snap holds them: one bucket's (bucketRecordScope) or one
// database's (databaseRecordScope). With generations, only the records whose
// value's generation is one of them: a drop of an old generation learned late
// leaves alone the record of a bucket recreated in a later one (spec §3.3).
// One prefix scan, O(the scope's records), never O(buckets). The 'a' and 'n'
// records use it.
func deleteNodeRecords(snap storage.KVSnapshot, b storage.KVBatch, kind byte, scope []byte, generations []int64) error {
	prefix := storage.NodeKey(storage.NSReplication, []byte{kind}, scope)
	iterator := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: keyPrefixEnd(prefix)})
	defer iterator.Close()
	for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		if generations != nil {
			stored, err := iterator.Value()
			if err != nil {
				return fmt.Errorf("reading a record of kind %q: %w", kind, err)
			}
			generation, recorded := bson.Raw(stored).Lookup("generation").Int64OK()
			if !recorded || !slices.Contains(generations, generation) {
				continue
			}
		}
		if err := b.Delete(slices.Clone(iterator.Key())); err != nil {
			return fmt.Errorf("deleting a record of kind %q: %w", kind, err)
		}
	}
	if err := iterator.Err(); err != nil {
		return fmt.Errorf("walking the records of kind %q: %w", kind, err)
	}
	return nil
}

// bucketRecordScope is the part of a node record's key, after its kind, that
// every record of one bucket starts with: its placement key.
func bucketRecordScope(key placementKey) []byte {
	return []byte(key.encoded)
}

// databaseRecordScope is the part of a node record's key, after its kind, that
// every record of one database's buckets starts with: the placement key's
// first half, uvarint(len(database)) ‖ database. The length comes first, so it
// matches exactly one database.
func databaseRecordScope(database string) []byte {
	return append(binary.AppendUvarint(nil, uint64(len(database))), database...)
}

// forgetAwaitingBucket drops the bucket placed by key from the awaiting map,
// once the batch that deleted its records has committed.
func (c *Cluster) forgetAwaitingBucket(key placementKey) {
	if c.awaitingCount.Load() == 0 {
		return
	}
	c.awaitingMu.Lock()
	defer c.awaitingMu.Unlock()
	delete(c.awaiting, key)
	c.awaitingCount.Store(int64(len(c.awaiting)))
	c.noteReplicationTransition()
}

// forgetAwaitingDatabase drops from the awaiting map every member of a bucket
// of database whose record's generation is one of generations, every one when
// generations is nil, once the drop's batch that deleted the records has
// committed. It walks the map: O(pending buckets).
func (c *Cluster) forgetAwaitingDatabase(database string, generations []int64) {
	if c.awaitingCount.Load() == 0 {
		return
	}
	scope := string(databaseRecordScope(database))
	c.awaitingMu.Lock()
	defer c.awaitingMu.Unlock()
	for key, pending := range c.awaiting {
		if !strings.HasPrefix(key.encoded, scope) {
			continue
		}
		for member, copyOf := range pending {
			if generations == nil || slices.Contains(generations, copyOf.generation) {
				delete(pending, member)
			}
		}
		if len(pending) == 0 {
			delete(c.awaiting, key)
		}
	}
	c.awaitingCount.Store(int64(len(c.awaiting)))
	c.noteReplicationTransition()
}

// clearAwaitingOutsideTheSet is the placement pass's clear (spec §7.3; G3
// build doc OQ-G3-9): the record of a member that is no longer in the set at
// epoch, at the bucket's factor, of a bucket this node holds, is deleted, in
// synced batches of at most placementBatchRecords, and dropped from the map
// after each commits. It walks the map, never the held buckets: O(awaiting
// records).
func (c *Cluster) clearAwaitingOutsideTheSet(epoch uint64) error {
	if c.awaitingCount.Load() == 0 {
		return nil
	}
	pending := make(map[placementKey][]string)
	c.awaitingMu.Lock()
	for key, members := range c.awaiting {
		for member := range members {
			pending[key] = append(pending[key], member)
		}
	}
	c.awaitingMu.Unlock()
	var outside []awaitedMember
	for key, members := range pending {
		database, bucket, named := key.names()
		if !named || !c.s.bucketStoreOpen(database, bucket) {
			continue
		}
		set, err := c.setAt(epoch, key, c.effectiveRF(key))
		if err != nil {
			return fmt.Errorf("placing bucket %q in database %q to clear the awaiting records of members outside its set: %w", bucket, database, err)
		}
		for _, member := range members {
			if !nodeListed(set, member) {
				outside = append(outside, awaitedMember{key: key, member: member})
			}
		}
	}
	for start := 0; start < len(outside); start += placementBatchRecords {
		if err := c.clearAwaitingRecords(outside[start:min(start+placementBatchRecords, len(outside))]); err != nil {
			return err
		}
	}
	return nil
}

// awaitedMember names one awaiting record: the bucket and the member.
type awaitedMember struct {
	key    placementKey
	member string
}

// clearAwaitingRecords deletes records in one synced batch, then drops them
// from the map.
func (c *Cluster) clearAwaitingRecords(records []awaitedMember) error {
	batch := c.s.kv.NewBatch()
	defer batch.Discard()
	for _, record := range records {
		if err := batch.Delete(awaitingKey(record.key, record.member)); err != nil {
			return fmt.Errorf("deleting the awaiting record of member %s: %w", record.member, err)
		}
	}
	if err := batch.Commit(true); err != nil {
		return fmt.Errorf("committing the clear of %d awaiting records of members outside their sets: %w", len(records), err)
	}
	for _, record := range records {
		c.dropAwaiting(record.key, []string{record.member})
	}
	return nil
}

// loadAwaitingCopies reads every awaiting record into the map at start: one
// prefix scan, O(pending records), never O(buckets). It runs after the WAL
// replay, which can have written records, and adds to what the replay
// published.
func (c *Cluster) loadAwaitingCopies() error {
	prefix := awaitingPrefix()
	snapshot := c.s.kv.NewSnapshot()
	defer snapshot.Discard()
	iterator := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: keyPrefixEnd(prefix)})
	defer iterator.Close()
	c.awaitingMu.Lock()
	defer c.awaitingMu.Unlock()
	for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		key, member, err := parseAwaitingKey(iterator.Key()[len(prefix):])
		if err != nil {
			return err
		}
		stored, err := iterator.Value()
		if err != nil {
			return fmt.Errorf("reading the awaiting record of member %s: %w", member, err)
		}
		var record awaitingRecord
		if err := bson.Unmarshal(stored, &record); err != nil {
			return fmt.Errorf("decoding the awaiting record of member %s: %w", member, err)
		}
		c.addAwaitingLocked(key, []string{member}, awaitingCopy{term: uint64(record.Term), generation: record.Generation, since: record.Since})
	}
	if err := iterator.Err(); err != nil {
		return fmt.Errorf("walking the awaiting records: %w", err)
	}
	return nil
}

// parseAwaitingKey splits what follows the 'a' prefix into the bucket's
// placement key and the member.
func parseAwaitingKey(suffix []byte) (placementKey, string, error) {
	rest := suffix
	for part := 0; part < 2; part++ {
		length, read := binary.Uvarint(rest)
		if read <= 0 || uint64(len(rest)-read) < length {
			return placementKey{}, "", fmt.Errorf("an awaiting record's key %x does not hold a placement key", suffix)
		}
		rest = rest[read+int(length):]
	}
	placement := placementKey{encoded: string(suffix[:len(suffix)-len(rest)])}
	length, read := binary.Uvarint(rest)
	if read <= 0 || uint64(len(rest)-read) != length {
		return placementKey{}, "", fmt.Errorf("an awaiting record's key %x does not end in a member id", suffix)
	}
	return placement, string(rest[read:]), nil
}
