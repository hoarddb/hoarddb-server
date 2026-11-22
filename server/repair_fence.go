package server

import (
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// A consistent get's put repair is checked by the bucket's owner before it is
// logged (BUG-0603). BUG-0480's bound, compared in every copy's apply, spares
// a write made after the read, but a delete removes the version it compares
// with. So the owner takes the bucket's fence exclusively for a repair put,
// which leaves every entry of the bucket logged before it applied and lets
// none be logged beside it, and reads its own log after the position the read
// saw. A put or delete of the key there, a log that no longer reaches back
// that far, or a copy that is not the one the read saw, drops the repair: it
// is answered not_applied and nothing is logged or shipped. A missed repair is
// made by the next consistent get; a wrong one undoes a client's write.

// repairCheckBatchBytes bounds each read of the owner's log. The span is the
// window between a read and its repair, usually a handful of entries.
const repairCheckBatchBytes = 1 << 20

// isRepairPut reports whether entry is a consistent get's put repair: an
// OpUpsert carrying repair_bound (a repair replaces), which the client door refuses
// (refuseClientRepairBound). It looks the field up in the raw payload, so an
// ordinary put pays a scan of its top-level names and no decode.
func isRepairPut(entry ReplEntry) bool {
	if transport.Opcode(entry.Op) != transport.OpUpsert {
		return false
	}
	bound, err := bson.Raw(entry.Payload).LookupErr("repair_bound")
	if err != nil {
		return false
	}
	switch bound.Type {
	case bson.TypeInt64:
		return bound.Int64() != 0
	case bson.TypeInt32:
		return bound.Int32() != 0
	}
	return false
}

// ownerFenceMode is the mode the owner takes a bucket's fence in for entry:
// exclusively for a repair put, so its log check and its append are not
// separated by another write of the bucket; otherwise fenceModeFor's.
func ownerFenceMode(entry ReplEntry) fenceMode {
	if isRepairPut(entry) {
		return fenceExclusive
	}
	return fenceModeFor(transport.Opcode(entry.Op))
}

// dropSupersededRepair is the owner's check of a repair put, made inside the
// bucket's fence held exclusively and before anything is logged. It returns
// the answer to a repair it drops, or nil for an entry to log as usual.
func (c *Cluster) dropSupersededRepair(entry ReplEntry) (*ReplResponse, error) {
	if !isRepairPut(entry) {
		return nil, nil
	}
	var req InsertRequest
	if err := bson.Unmarshal(entry.Payload, &req); err != nil {
		return c.droppedRepair(entry, "", fmt.Sprintf("its payload cannot be decoded: %v", err))
	}
	reason, err := c.repairSupersededBy(req)
	if err != nil {
		return c.droppedRepair(entry, req.Key, err.Error())
	}
	if reason != "" {
		return c.droppedRepair(entry, req.Key, reason)
	}
	return nil, nil
}

// repairSupersededBy says why the repair req must not be logged, or "" when
// nothing of the key was written since the read. Its error is a failure to
// find out, which drops the repair too.
func (c *Cluster) repairSupersededBy(req InsertRequest) (string, error) {
	read := req.RepairRead
	switch {
	case read == nil:
		return "it carries no read position", nil
	case read.Owner != c.self.ID:
		return fmt.Sprintf("it was read from %s, not this owner", read.Owner), nil
	}
	st, err := c.ownersBucketState(req.Database, req.Bucket)
	if err != nil {
		return "", err
	}
	switch {
	case st.Incarnation != read.Incarnation || st.Term != read.Term:
		return fmt.Sprintf("the copy is incarnation %d term %d, and the read saw incarnation %d term %d",
			st.Incarnation, st.Term, read.Incarnation, read.Term), nil
	case len(st.FailedSeqs) > 0 || c.s.bucketOwesApplies(req.Database, req.Bucket):
		return "the owner owes an apply of the bucket", nil
	case st.Through == read.Through:
		// Under the exclusive fence every logged entry of the bucket has
		// applied or is owed, and an applied one moves Through: nothing
		// of the bucket was written since the read.
		return "", nil
	case st.Through < read.Through:
		return fmt.Sprintf("the copy's position %d is below the read's %d", st.Through, read.Through), nil
	}
	return c.keyWrittenInLog(req, read.Through+1, st.Through)
}

// ownersBucketState reads the owner's lineage of database/bucket.
func (c *Cluster) ownersBucketState(database, bucket string) (store.BucketState, error) {
	b, snap, err := c.s.resolveForRead(database, bucket)
	if err != nil {
		return store.BucketState{}, fmt.Errorf("resolving the bucket: %w", err)
	}
	defer snap.Discard()
	st, err := store.LoadBucketState(snap, b.slot, b.databaseID, b.id.Load())
	if err != nil {
		return store.BucketState{}, fmt.Errorf("reading the bucket's lineage: %w", err)
	}
	return st, nil
}

// keyWrittenInLog reads the owner's log from first through last for an entry
// of req's bucket that writes req.Key, or that names no single key (a batch,
// an update by query, a drop), and says which it found, or "". A log that no
// longer holds first is a reason too: the span cannot be read.
func (c *Cluster) keyWrittenInLog(req InsertRequest, first, last uint64) (string, error) {
	if c.s.wal == nil {
		return "this node keeps no log to read the span from", nil
	}
	cursor, err := c.s.wal.OpenCursor(first)
	if errors.Is(err, storage.ErrBelowHorizon) {
		return fmt.Sprintf("the owner's log no longer reaches the read's position %d", first-1), nil
	}
	if err != nil {
		return "", fmt.Errorf("opening the owner's log at %d: %w", first, err)
	}
	for next := first; next <= last; {
		entries, after, err := c.s.wal.ReadBatch(cursor, last, repairCheckBatchBytes)
		if errors.Is(err, storage.ErrBelowHorizon) {
			return fmt.Sprintf("the owner's log no longer reaches sequence %d", next), nil
		}
		if err != nil {
			return "", fmt.Errorf("reading the owner's log from %d: %w", next, err)
		}
		if len(entries) == 0 {
			return fmt.Sprintf("the owner's log ends before sequence %d", next), nil
		}
		for _, logged := range entries {
			if writesKey(logged, req) {
				return fmt.Sprintf("the owner logged %s of the key at sequence %d, after the read",
					transport.Opcode(logged.Op), logged.Seq), nil
			}
		}
		cursor, next = after, entries[len(entries)-1].Seq+1
	}
	return "", nil
}

// writesKey reports whether a logged entry may have written req's key: an
// entry of the same bucket and database whose payload names the key, or names
// none, or names one that is not a string. The payload is read and not the
// log's key column, which is diagnostic: the raw value without its type.
func writesKey(logged storage.WALEntry, req InsertRequest) bool {
	if logged.Bucket != req.Bucket {
		return false
	}
	meta := decodeFrameMeta(logged.Data)
	if _, database := meta.scope(transport.Opcode(logged.Op)); database != "" && database != req.Database {
		return false
	}
	named := meta.Key
	if named.Type == 0 {
		named = meta.ID
	}
	key, isString := named.StringValueOK()
	return !isString || key == req.Key
}

// droppedRepair is the answer to a repair put the owner did not log: a
// success that says why and is not_applied, so the consistent get reports
// nothing repaired and logs no failure.
func (c *Cluster) droppedRepair(entry ReplEntry, key, reason string) (*ReplResponse, error) {
	c.logger.Debug("dropped a read repair before logging it", "bucket", entry.Bucket, "reason", reason)
	payload, err := bson.Marshal(InsertResponse{ID: key, Success: true, WriteOutcome: outcomeNotApplied,
		Message: "read repair dropped: " + reason})
	if err != nil {
		return nil, fmt.Errorf("encoding the answer to a dropped read repair of bucket %q: %w", entry.Bucket, err)
	}
	return &ReplResponse{Opcode: int32(transport.OpUpsertOK), Payload: payload}, nil
}
