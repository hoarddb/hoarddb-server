package server

import (
	"github.com/hoarddb/hoarddb-server/transport"
)

// Replication is at-least-once: an acknowledgement that is lost in flight makes
// the sender re-send, and a sender that restarted starts its per-peer
// acknowledgement position at zero. A receiver therefore has to remember how far
// it has applied each source's stream, or it re-applies work it already holds.
//
// Re-applying is harmless for keyed stores (same key, same value) but destructive
// for FIFO and LIFO: a re-applied push appends a second element and a re-applied
// pop takes whatever is at the head now, which is not the element it originally
// took.
//
// The position is kept in memory and rebuilt at start from the node's own log
// and from checkpoints in the node's store (replication_positions.go). Nothing
// is written per replicated frame.

// replicationState is the layout of replication_state.bson, the file the
// previous build kept its positions in. Nothing writes it any more; it is
// decoded once, by importLegacyReplicationState.
type replicationState struct {
	AppliedFrom map[string]uint64 `bson:"applied_from"`
	// SeededThrough is, per source node and per bucket, the sequence of that
	// source's stream a snapshot pulled onto this node was taken at. It is
	// remembered for the same reason AppliedFrom is: a replica that forgot
	// what its snapshot already holds would replay the bucket's history over
	// it after a restart. Optional and additive — a state file
	// written before it existed reads as having no seeded positions.
	//
	// The inner key is "<database>/<bucket>" (seededStateKey). A file
	// written before placement by database holds bare bucket names there instead; those
	// import as legacy positions (importLegacyReplicationState).
	SeededThrough map[string]map[string]uint64 `bson:"seeded_through,omitempty"`
}

// seededStateKeySeparator joins the database and bucket of a persisted
// snapshot position's key. It is not migrateKey's NUL, because a BSON
// document key cannot contain one. Neither name can contain "/"
// (validateBucketName refuses it wherever a bucket is created — the catalog
// keys buckets the same way), so the key splits back unambiguously, and a
// bare bucket name — how a state file from before placement by database keyed a position
// — never contains one.
const seededStateKeySeparator = "/"

// seededStateKey is the persisted key of database/bucket's snapshot position.
func seededStateKey(database, bucket string) string {
	return database + seededStateKeySeparator + bucket
}

// seedFloors is one stream's snapshot positions: per bucket name, then per
// database, the sequence of that stream a snapshot of database/bucket was
// taken at. A position covers its own (database, bucket) only — two buckets
// of one name in two databases can share a replica set (by chance, since the
// ring places by (database, bucket)) and one's snapshot says nothing
// about the other's entries.
//
// Name first because a replicated entry carries only the bucket name
// (ReplEntry.Bucket); its database is in its payload. The
// name-level lookup decides whether that payload has to be decoded at all.
type seedFloors map[string]map[string]uint64

// raise records a snapshot of database/bucket taken at seq. It only raises:
// a later snapshot is taken at a later sequence, and an earlier one cannot
// un-deliver anything.
func (f seedFloors) raise(database, bucket string, seq uint64) {
	perDatabase, ok := f[bucket]
	if !ok {
		perDatabase = make(map[string]uint64)
		f[bucket] = perDatabase
	}
	if seq > perDatabase[database] {
		perDatabase[database] = seq
	}
}

// drop forgets the snapshot position of database/bucket.
func (f seedFloors) drop(database, bucket string) {
	perDatabase, ok := f[bucket]
	if !ok {
		return
	}
	delete(perDatabase, database)
	if len(perDatabase) == 0 {
		delete(f, bucket)
	}
}

// highest returns the highest snapshot position of any database's bucket
// named bucket: an entry above it is covered by none of them, whatever its
// database, which is what lets the callers skip decoding its payload.
func (f seedFloors) highest(bucket string) uint64 {
	var highest uint64
	for _, seq := range f[bucket] {
		highest = max(highest, seq)
	}
	return highest
}

// seededCovers reports whether entry e from `from` is contents a snapshot
// pulled onto this node already holds (handleReplPush skips it). It is on the
// replication apply path, so it decodes e's payload for its database only
// when e is at or below the highest snapshot position of any bucket of that
// name from that source — see seedFloors.covers — and a legacy bare-name
// position covers every database's bucket of its name, as it did before
// placement by database.
func (c *Cluster) seededCovers(from string, e ReplEntry) bool {
	c.appliedMu.Lock()
	highest := max(c.seededThrough[from].highest(e.Bucket), c.legacySeeded[from][e.Bucket])
	c.appliedMu.Unlock()
	if e.Seq > highest {
		return false
	}
	_, database := decodeFrameMeta(e.Payload).scope(transport.Opcode(e.Op))
	return e.Seq <= c.seededThroughFor(from, database, e.Bucket)
}

// seededThroughFor returns the sequence of from's stream that a snapshot of
// database/bucket delivered to this node was taken at, or 0 when no snapshot
// of it from that source has landed here. A legacy bare-name position from a
// state file written before placement by database applies to every database's bucket of
// its name.
func (c *Cluster) seededThroughFor(from, database, bucket string) uint64 {
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	return max(c.seededThrough[from][bucket][database], c.legacySeeded[from][bucket])
}

// shouldApply reports whether an entry from a source has still to be applied.
// It only reads: recording the position is markApplied's job, and it happens
// after the entry is applied, because a position recorded for an entry that
// failed to apply would make the failure permanent — the retry would be skipped.
//
// It is a high-water mark, not a gap detector, and it cannot be one: a source's
// stream to this node is sparse by construction (only the buckets this node
// hosts, and none of the entries the source applied as a replica), so a missing
// sequence is the normal case. An entry at or below the mark is treated as
// already applied. That is correct only because the source delivers its entries
// to this node in sequence order. The source's shipOrder puts them on the queue
// in that order, and runSender, the only thing that ships them, replays them
// from the log in sequence order before it serves the queue. An entry that arrives out of order is skipped for good, and
// the source counts it as applied.
func (c *Cluster) shouldApply(from string, seq uint64) bool {
	if from == "" || seq == 0 {
		return true
	}
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	last, ok := c.appliedFrom[from]
	return !ok || seq > last
}

// markApplied records that an entry from a source has been applied.
func (c *Cluster) markApplied(from string, seq uint64) {
	if from == "" || seq == 0 {
		return
	}
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	if last, ok := c.appliedFrom[from]; ok && seq <= last {
		return
	}
	c.appliedFrom[from] = seq
}
