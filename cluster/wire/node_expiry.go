package wire

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// expiryWriteBudget is how long one system removal may wait for the write path.
const expiryWriteBudget = 5 * time.Second

// RemoveDocument implements ExpiryRemover: a system delete of the expired document.
func (n *Node) RemoveDocument(ctx context.Context, entry storage.ExpiryEntry) (bool, error) {
	op := cluster.Op{Class: cluster.OpDelete, BucketID: entry.BucketID, EncID: entry.EncID, System: true}
	return n.systemWrite(ctx, entry, op)
}

// RemoveElement implements ExpiryRemover: a system take of the expired queue element by position.
func (n *Node) RemoveElement(ctx context.Context, entry storage.ExpiryEntry) (bool, error) {
	arg, err := bson.Marshal(takeKeyArgDoc{K: strconv.FormatUint(entry.Position, 10) + "@" + strconv.FormatInt(entry.AtMillis, 10)})
	if err != nil {
		return false, fmt.Errorf("encoding the position of an expired element: %w", err)
	}
	op := cluster.Op{
		Class: cluster.OpStoreVerb, BucketID: entry.BucketID, EncID: entry.EncID, System: true,
		Fields: []cluster.FieldOp{{FieldID: entry.FieldID, Verb: cluster.VerbTake, Value: arg}},
	}
	return n.systemWrite(ctx, entry, op)
}

// systemWrite sends op through the Writer every client write reaches, with no request id: the
// record gets no dedupe entry, because nothing could ever retry it. A node that does not own the
// pair, or whose pair is held, answers Forward or Held; both are a skip, the owner issues it.
func (n *Node) systemWrite(ctx context.Context, entry storage.ExpiryEntry, op cluster.Op) (bool, error) {
	view := n.schema.View(entry.DBID, entry.BucketID)
	if !view.Present {
		return true, nil
	}
	_, op.DeclVersion = machine.EffectiveTable(view.Decl, view.Published)
	op.SchemaVersionSeen = view.Seen
	answer, err := n.writer.Write(ctx, WriteRequest{Slot: entry.Slot, DBID: entry.DBID, Op: op, Deadline: expiryWriteBudget})
	if err != nil {
		return false, err
	}
	return systemAnswerOutcome(answer)
}

// systemAnswerOutcome reads the answer of a system write: a held or forwarded write is a skip, an
// apply that failed after the append is an error (the pair needs a replay; it is not a removal), and
// anything else is a removal.
func systemAnswerOutcome(answer WriteAnswer) (skipped bool, err error) {
	switch {
	case answer.Held || answer.Forward:
		return true, nil
	case answer.ApplyFailure != nil:
		return false, fmt.Errorf("the system write was appended but its apply failed: %w", answer.ApplyFailure)
	}
	return false, nil
}

// expiryTable is the expiry declaration in force for an entry's bucket at the published version,
// for the sweep's verification; false when the cache cannot say (the entry then waits).
func (n *Node) expiryTable(entry storage.ExpiryEntry) (cluster.AdmittedExpire, bool) {
	view := n.schema.View(entry.DBID, entry.BucketID)
	if !view.Present {
		return cluster.AdmittedExpire{}, false
	}
	return *admittedExpireOf(machine.EffectiveExpire(view.Decl, view.Published)), true
}

// expiryBacklogLimit bounds the walk that counts the backlog gauge at each scrape.
const expiryBacklogLimit = 10_000

// ExpiryCounts is the node's expiry counters and backlog for /metrics: the zero value before the
// node is wired. Nothing in it is per bucket.
func (n *Node) ExpiryCounts() ExpiryCounts {
	if n.expiry == nil {
		return ExpiryCounts{}
	}
	counts, err := n.expiry.Counts(expiryBacklogLimit)
	if err != nil {
		// A backlog that could not be counted is unknown, not zero: a zero would tell an operator
		// the sweep is caught up while the queue cannot even be read.
		counts.Backlog, counts.BacklogUnknown = 0, true
		if n.cl != nil {
			n.cl.report(fmt.Errorf("counting the expiry backlog for /metrics: %w. The hoarddb_expiry_backlog series is left out until the expiry queue can be read", err))
		}
	}
	return counts
}

// refuseExpiryOnDocuments refuses `set expire at <Field>` on a bucket that already holds documents,
// unless the bucket already expires at that field (a repeat). Nothing builds expiry entries for
// documents that are already stored, so reads would hide them when their time passed and nothing
// would ever remove them; a backfill is a later feature.
func (n *Node) refuseExpiryOnDocuments(ctx context.Context, db, bucket, field string) error {
	ref, err := n.bucket(ctx, db, bucket)
	if err != nil {
		return err
	}
	current := machine.EffectiveExpire(ref.entry.Decl, n.topology.Published(ref.dbID))
	if entry, found := entryNamed(ref.table, field); found && entry.ID == current.BucketField {
		return nil
	}
	held, err := n.List(ctx, db, bucket, nil, 1)
	if err != nil {
		return fmt.Errorf("checking whether bucket %q in database %q holds documents: %w", bucket, db, err)
	}
	if len(held) == 0 {
		return nil
	}
	return refuse(cluster.CodeInvalidArgument, "bucket %q in database %q already holds documents, and expiry is not applied to documents already stored: they would be hidden when their time passed and never removed. Create a new bucket with the expiry and copy the documents into it, e.g. 'create bucket %s_expiring in %s { %s datetime, expire at %s }' with your other fields added, or empty it first: run 'drop bucket %s in %s' and create it again with expire at %s",
		bucket, db, bucket, db, field, field, bucket, db, field)
}
