package server

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
)

// The member's needs-a-copy record of the factor alter
// (bucket-replication-rev8-spec §3.3, §5.3 rows 4 and 5, §7.2). A member an
// increase newly places that holds nothing of the bucket learns nothing
// durable from the alter but this one record: no catalog entry, no record of
// the bucket, no registry entry. It lives only while the copy is pending, and
// is deleted by the batch that lands the copy, by the bucket's drop, by a
// database drop of its generation, and by a later alter that does not place
// this node. The pending-copy worker (bucket_rf_worker.go) makes the copy
// with auto-rebalance on; with it off, an operator's migrate does.

// needsCopyRecord is the value of one 'n' record: the bucket, the factor and
// the membership epoch of the alter that placed this node, the database
// generation of the bucket's incarnation, all the entry's (spec §3.2), and
// when this node wrote it (wall-clock seconds, read only by the listings).
// 107 bytes of BSON plus the two names.
type needsCopyRecord struct {
	Database          string `bson:"database"`
	Bucket            string `bson:"bucket"`
	ReplicationFactor int32  `bson:"replication_factor"`
	Epoch             int64  `bson:"epoch"`
	Generation        int64  `bson:"generation"`
	Since             int64  `bson:"since"`
}

// needsCopyKey is the key of the bucket placed by key's record: the kind
// byte 'n', then the placement key, which starts with the database.
func needsCopyKey(key placementKey) []byte {
	return storage.NodeKey(storage.NSReplication, []byte{storage.NeedsCopyRecordKind}, []byte(key.encoded))
}

// needsCopyBatchHook, when set, is handed the batch of every alter's apply on
// this node for a bucket it holds nothing of, with this node's id, and returns
// the batch the apply writes to: a test counts what one apply writes. Nil in
// production.
var needsCopyBatchHook atomic.Pointer[func(nodeID string, b storage.KVBatch) storage.KVBatch]

// alterOfNoCopy is a member's apply of an alter of a bucket it holds nothing
// of (spec §5.3 rows 4 and 5). Placed by the new factor on the current ring,
// it writes its needs-a-copy record and, once the batch has committed, queues
// the bucket to the pending-copy worker and wakes it; not placed, it deletes
// any record and, once the batch has committed, cancels every pull of the
// name in flight here, whatever started it: a copy that landed after the alter
// un-placed this node would be unmarked, unaddressed, and served frozen at its
// snapshot. The
// caller holds the bucket's fence exclusively, as a pull's materialise holds
// it shared from its last cancellation check to its catalog commit, so the
// pull either sees the cancel or has landed, and the alter then finds the copy
// held. Every row acknowledges: a placement change is never refused for the
// disk (§7.2). One record, one batch: nothing here is O(buckets).
//
// No record is written for a generation this node holds dropped (BUG-0593):
// replay applies the alter again after the drop's tombstone is durable, and
// the drop's own replay, which then learns nothing, deletes nothing. The
// check and the commit are under the allocator's mutex, the one a drop
// writes its tombstones and deletes the records under, so an alter applied
// live is ordered with the drop either way.
func (s *Server) alterOfNoCopy(req AlterBucketRequest) error {
	key := placementOf(req.Database, req.Name)
	set, err := s.cluster.replicasAtFactor(key, int(req.ReplicationFactor))
	if err != nil {
		return fmt.Errorf("placing bucket %q in database %q at the factor its alter asks for: %w", req.Name, req.Database, err)
	}
	placed := nodeListed(set, s.cluster.self.ID)
	s.bucketIDs.mu.Lock()
	defer s.bucketIDs.mu.Unlock()
	dropped, err := s.generationDropped(req.Database, req.Generation)
	if err != nil {
		return fmt.Errorf("applying the alter of bucket %q in database %q: %w", req.Name, req.Database, err)
	}
	batch := s.kv.NewBatch()
	defer batch.Discard()
	written := batch
	if hook := needsCopyBatchHook.Load(); hook != nil {
		written = (*hook)(s.cluster.self.ID, batch)
	}
	record := needsCopyRecord{Database: req.Database, Bucket: req.Name, ReplicationFactor: req.ReplicationFactor,
		Epoch: int64(req.Epoch), Generation: req.Generation, Since: time.Now().Unix()}
	if placed && !dropped {
		encoded, err := bson.Marshal(record)
		if err != nil {
			return fmt.Errorf("encoding the needs-a-copy record of bucket %q in database %q: %w", req.Name, req.Database, err)
		}
		if err := written.Set(needsCopyKey(key), encoded); err != nil {
			return fmt.Errorf("writing the needs-a-copy record of bucket %q in database %q: %w", req.Name, req.Database, err)
		}
	} else if err := written.Delete(needsCopyKey(key)); err != nil {
		return fmt.Errorf("deleting the needs-a-copy record of bucket %q in database %q: %w", req.Name, req.Database, err)
	}
	// Not synced: the alter is in this node's log, which is synced before the
	// apply, and replay applies it again for a bucket this node does not hold,
	// which writes or deletes the same record.
	if err := batch.Commit(false); err != nil {
		return fmt.Errorf("committing the needs-a-copy record of bucket %q in database %q: %w", req.Name, req.Database, err)
	}
	if !placed {
		s.cluster.forgetPendingCopy(key)
		s.cluster.cancelPullsOf(req.Database, req.Name)
		return nil
	}
	if !dropped {
		s.cluster.queuePendingCopy(record)
	}
	return nil
}
