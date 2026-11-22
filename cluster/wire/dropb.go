package wire

import (
	"context"
	entropy "crypto/rand"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// BucketSweeper deletes the keys of a dropped bucket from the pairs a node holds.
type BucketSweeper interface {
	// HeldSlots lists, ascending, the slots of database dbID the coordinator waits for.
	HeldSlots(dbID uint64) []uint16
	// SweepBucket deletes every key of bucketID in one pair, step by step, and returns when none is left.
	SweepBucket(ctx context.Context, slot uint16, dbID, bucketID uint64) error
	// RecordSweeps writes, synced and in batches, the sweep record of every slot in slots that has
	// none, without waking the background loop.
	RecordSweeps(dbID, bucketID uint64, slots []uint16) error
	// Enqueue leaves the sweep of every held pair to the node's background loop.
	Enqueue(dbID, bucketID uint64) error
}

// DropBucketConfig is the collaborators of a DropBucket.
type DropBucketConfig struct {
	// Sender orders the declaration and reverse-index writes.
	Sender Sender
	// Declarations reads a declaration by name from its owner.
	Declarations DeclarationSource
	// Topology gives the replication factor the degraded refusal reports.
	Topology Topology
	// DatabaseName is the name a database id placed under.
	DatabaseName func(dbID uint64) (string, bool)
	// BucketName reads the name a bucket id belongs to from the reverse index.
	BucketName func(ctx context.Context, dbID, bucketID uint64) (string, bool, error)
	// Committed is the database's committed schema_version.
	Committed func(dbID uint64) int64
	// Published is the database's published schema_version on this node.
	Published func(dbID uint64) int64
	// Bumper commits the one bump.
	Bumper SchemaBumper
	// Sweeper deletes the bucket's keys from the held pairs.
	Sweeper BucketSweeper
	// ReverseState reads the state ("active" or "dropping") of the reverse-index document of
	// a bucket id from its owner.
	ReverseState func(ctx context.Context, dbID, bucketID uint64) (state string, found bool, err error)
}

// DropBucket runs conv 21's drop, in this order and no other:
//
//  1. read the declaration; none is NOT_FOUND;
//  2. write the dropping marker (declaration field 4, version s+1) and upsert the reverse-index
//     document as dropping, both at majority-or-better;
//  3. commit ONE BUMP_SCHEMA s -> s+1, only when the marker's version is above the published
//     one, so a retry of a drop whose bump landed fences nothing again;
//  4. sweep every held slot (a 'b' record each), recording the slot in the swept bitmap;
//  5. delete the declaration, then the reverse-index document.
//
// Every step is a state check followed by a write, so a run that dies anywhere is finished by
// running it again, and the deletes are the last thing it does.
//
// Cost (rule 11), per drop and never per other bucket: two small ring writes, one Raft commit,
// one pair-lock pass per node, O(documents and store elements of the bucket) of background
// deletes on each node that holds them, one 'b' record per held pair while they run, one
// bitmap write per 64 swept slots. No goroutine, file descriptor or memory is added per
// bucket, and no other bucket is read, locked or slowed: a sweep step holds one pair's lock for
// one bounded batch.
type DropBucket struct {
	cfg DropBucketConfig
	// crashAfter, when set by a test, runs after each named step and may end the run there.
	crashAfter func(step string) error
	// beforeMark, when set by a test, runs after the run has read an active declaration and
	// before it writes the marker.
	beforeMark func()
	// beforeReverse, when set by a test, runs after the run has read a dropping declaration and
	// before it writes the reverse document.
	beforeReverse func()
}

// NewDropBucket returns a DropBucket over cfg.
func NewDropBucket(cfg DropBucketConfig) *DropBucket { return &DropBucket{cfg: cfg} }

// The steps a drop reports to its crash seam.
const (
	dropStepMarker      = "marker"
	dropStepReverse     = "reverse"
	dropStepBump        = "bump"
	dropStepSweep       = "sweep"
	dropStepSwept       = "swept"
	dropStepDeclaration = "declaration"
	dropStepDone        = "done"
)

const (
	// maxDropAttempts bounds the times one run starts over when another change moves under it.
	maxDropAttempts = 8
	// sweptBatchSlots is how many swept slots one bitmap write records.
	sweptBatchSlots = 64
)

// The _buckets field ids a drop writes (conv 19).
const declFieldSwept uint16 = 6

var (
	// errDropMoved is what a closure at the declaration owner returns when the world is not the
	// one the run read: the run reads again. It is never shown to a client.
	errDropMoved = errors.New("the declaration moved during the drop")
	// errDropGone is what a closure returns when the document it was to change or delete is
	// already gone: another run finished the drop.
	errDropGone = errors.New("the declaration is already gone")
)

// Run drops bucket name of database dbID. It is idempotent: a bucket already gone is NOT_FOUND,
// which is also what a drop that finished answers, and in both the wanted end state holds.
func (d *DropBucket) Run(ctx context.Context, dbID uint64, name string) error {
	dbName, ok := d.cfg.DatabaseName(dbID)
	if !ok {
		return refuse(cluster.CodeNotFound, "dropping bucket %q: database id %d is not in the committed state. Run 'show databases' to list what exists", name, dbID)
	}
	if err := checkBucketName(dbName, name); err != nil {
		return err
	}
	run := &dropRun{DropBucket: d, ctx: ctx, dbID: dbID, dbName: dbName, name: name}
	for attempt := 0; attempt < maxDropAttempts; attempt++ {
		done, err := run.attempt()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		time.Sleep(publicationWait)
	}
	return &cluster.Error{Code: cluster.CodeSchemaNotCurrent, Message: fmt.Sprintf(
		"dropping bucket %q in database %q kept meeting another change to the database's schema and gave up after %d tries. Nothing is lost: the bucket is either still whole or already leaving. Run 'drop bucket %s in %s' again to finish it",
		name, dbName, maxDropAttempts, name, dbName)}
}

// Start begins the drop of bucket name of database dbID and returns once the bucket is dropping
// and the bump is committed: the marker, the reverse document and the one bump are done inline,
// the sweep of every held pair is left to the node's background loop, and that loop frees the
// name (declaration, then reverse document) when the last pair is swept. It returns the bucket's
// id. Running it again on a dropping bucket queues the sweep again and returns the same id;
// a bucket that is not there is NOT_FOUND.
func (d *DropBucket) Start(ctx context.Context, dbID uint64, name string) (uint64, error) {
	return d.StartExpecting(ctx, dbID, name, 0)
}

// StartExpecting is Start for a caller that knows the id of the bucket it means: a bucket that
// carries another id is refused and nothing is dropped. Zero means any.
func (d *DropBucket) StartExpecting(ctx context.Context, dbID uint64, name string, expect uint64) (uint64, error) {
	dbName, ok := d.cfg.DatabaseName(dbID)
	if !ok {
		return 0, refuse(cluster.CodeNotFound, "dropping bucket %q: database id %d is not in the committed state. Run 'show databases' to list what exists", name, dbID)
	}
	if err := checkBucketName(dbName, name); err != nil {
		return 0, err
	}
	run := &dropRun{DropBucket: d, ctx: ctx, dbID: dbID, dbName: dbName, name: name, async: true, expect: expect}
	for attempt := 0; attempt < maxDropAttempts; attempt++ {
		done, err := run.attempt()
		if err != nil {
			return 0, err
		}
		if done {
			return run.pinned, nil
		}
		time.Sleep(publicationWait)
	}
	return 0, &cluster.Error{Code: cluster.CodeSchemaNotCurrent, Message: fmt.Sprintf(
		"dropping bucket %q in database %q kept meeting another change to the database's schema and gave up after %d tries. Nothing is lost: run 'drop bucket %s in %s' again to finish it",
		name, dbName, maxDropAttempts, name, dbName)}
}

// finishSwept frees the name and the id of a bucket whose pairs are all swept: the declaration
// first and the reverse-index document last, each deleted only if it is still this bucket's.
// The node's loop calls it, and keeps the last 'b' record until it returns, so a crash between
// the two deletes is repeated and reclaims the reverse document a bare retry could not.
func (d *DropBucket) finishSwept(ctx context.Context, dbID, bucketID uint64) error {
	dbName, ok := d.cfg.DatabaseName(dbID)
	if !ok {
		return nil
	}
	name, found, err := d.cfg.BucketName(ctx, dbID, bucketID)
	if err != nil {
		return fmt.Errorf("reading the name of bucket %016x: %w", bucketID, err)
	}
	if !found {
		return nil
	}
	run := &dropRun{DropBucket: d, ctx: ctx, dbID: dbID, dbName: dbName, name: name, pinned: bucketID, reclaim: true}
	_, err = run.finish(bucketID)
	return err
}

// SweepSlot deletes the bucket's keys from one held pair and returns when none is left.
func (d *DropBucket) SweepSlot(ctx context.Context, slot uint16, dbID, bucketID uint64) error {
	if err := d.cfg.Sweeper.SweepBucket(ctx, slot, dbID, bucketID); err != nil {
		return fmt.Errorf("sweeping slot %d of bucket %016x: %w", slot, bucketID, err)
	}
	return nil
}

// needsBump reports whether the drop's bump is still to be committed: the marker was written
// at a version the database has not published. A marker at or below the published version has
// been made visible already, by this drop's earlier run or by any other bump after it.
func (d *DropBucket) needsBump(declVersion, published int64) bool { return declVersion > published }

// step reports a finished step to the crash seam.
func (d *DropBucket) step(name string) error {
	if d.crashAfter == nil {
		return nil
	}
	return d.crashAfter(name)
}

// dropRun is one run of a drop.
type dropRun struct {
	*DropBucket
	ctx    context.Context
	dbID   uint64
	dbName string
	name   string
	// pinned is the id of the bucket this run set out to drop, learned from the first
	// declaration it reads. Every later read and every owner-side rewrite is checked against it:
	// a name that now carries another id is a bucket this run never meant, and the run ends.
	pinned uint64
	// expect, when not zero, is the id the caller means; a first read that finds another is refused.
	expect uint64
	// finished is set when the run reached its end rather than stopping because its bucket was gone.
	finished bool
	// movedAway is set when the final deletes found the declaration was no longer this bucket's.
	movedAway bool
	// async ends the run at the bump and leaves the sweep to the node's loop.
	async bool
	// reclaim lets the deletes go on past a declaration that is already gone, to free the
	// reverse document of a bucket the caller knows was being dropped.
	reclaim bool
}

// attempt is the drop once. done false means something moved and the caller reads again.
func (r *dropRun) attempt() (done bool, err error) {
	done, err = r.attempt1()
	if err != nil || !done || r.finished {
		return done, err
	}
	// The run stopped because the bucket it set out to drop is gone. If the name is live as
	// another bucket, saying "dropping" would be a lie: the caller is told.
	return true, r.lostName()
}

// lostName is the refusal for a run whose bucket was dropped by someone else while the name was
// created again: nothing of the new bucket was touched, and the command that would drop it is named.
func (r *dropRun) lostName() error {
	decl, _, found, err := r.cfg.Declarations.Read(r.ctx, r.dbID, r.name)
	if err != nil {
		return fmt.Errorf("reading the declaration of bucket %q in database %q: %w", r.name, r.dbName, err)
	}
	if !found || decl.BucketID == r.pinned {
		return nil
	}
	return refuse(cluster.CodeBucketExists, "bucket %q in database %q was dropped by another request and created again as %s while this drop ran, so the new bucket was not touched. If you mean to drop the new one, run 'drop bucket %s in %s' again",
		r.name, r.dbName, id16(decl.BucketID), r.name, r.dbName)
}

func (r *dropRun) attempt1() (done bool, err error) {
	decl, _, found, err := r.cfg.Declarations.Read(r.ctx, r.dbID, r.name)
	if err != nil {
		return false, fmt.Errorf("reading the declaration of bucket %q in database %q: %w", r.name, r.dbName, err)
	}
	if !found {
		if r.pinned != 0 {
			return true, nil
		}
		return false, r.notFound()
	}
	if r.pinned == 0 {
		if r.expect != 0 && decl.BucketID != r.expect {
			return true, refuse(cluster.CodeNotFound, "bucket %q in database %q has id %s, not the %s this request named, so it was dropped and created again since. Nothing was dropped. Run 'describe bucket %s in %s' to see the bucket there now",
				r.name, r.dbName, id16(decl.BucketID), id16(r.expect), r.name, r.dbName)
		}
		r.pinned = decl.BucketID
	} else if decl.BucketID != r.pinned {
		// The bucket this run was dropping is gone and the name belongs to another.
		return true, nil
	}
	if decl.State == machine.StateActive {
		if r.beforeMark != nil {
			r.beforeMark()
		}
		if err := r.mark(); errors.Is(err, errDropMoved) {
			return false, nil
		} else if errors.Is(err, errDropGone) {
			return true, nil
		} else if err != nil {
			return false, err
		}
		if err := r.step(dropStepMarker); err != nil {
			return false, err
		}
	}
	// A dropping declaration reaches here on a retry, or right after the marker: either way the
	// reverse document is upserted again, because a crash between the two writes is repaired by
	// running the drop (conv 21 step 2).
	if r.beforeReverse != nil {
		r.beforeReverse()
	}
	if err := r.reverseDropping(decl.BucketID); errors.Is(err, errDropGone) {
		return true, nil
	} else if errors.Is(err, errDropMoved) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	if err := r.step(dropStepReverse); err != nil {
		return false, err
	}
	var swept []byte
	decl, swept, found, err = r.cfg.Declarations.Read(r.ctx, r.dbID, r.name)
	if err != nil {
		return false, fmt.Errorf("reading the declaration of bucket %q in database %q: %w", r.name, r.dbName, err)
	}
	if !found || decl.BucketID != r.pinned {
		return true, nil
	}
	if decl.State != machine.StateDropping {
		return false, nil
	}
	moved, err := r.bump(decl)
	if err != nil || moved {
		return false, err
	}
	if err := r.step(dropStepBump); err != nil {
		return false, err
	}
	if r.async {
		r.finished = true
		return true, r.cfg.Sweeper.Enqueue(r.dbID, decl.BucketID)
	}
	if err := r.sweep(decl, swept); err != nil {
		return false, err
	}
	done, err = r.finish(decl.BucketID)
	r.finished = done && err == nil && !r.movedAway
	return done, err
}

func (r *dropRun) notFound() error {
	return &cluster.Error{Code: cluster.CodeNotFound, Message: cluster.CodeNotFound.Text(cluster.CondBucketNotFound,
		map[string]string{"bucket": r.name, "db": r.dbName})}
}

// mark writes the dropping marker at the declaration's owner as one read-modify-write: field 4
// becomes dropping, field 3 the version the bump will publish, field 5 the table in force (a
// published pending table is folded and an unpublished one, inert, is dropped with field 8).
func (r *dropRun) mark() error {
	encID, err := store.EncodeComponent(r.name)
	if err != nil {
		return fmt.Errorf("encoding bucket name %q: %w", r.name, err)
	}
	slot := storage.SlotOf(storage.PlacementKey(r.dbName, systemBucketName, encID))
	placeholder := []cluster.FieldOp{
		{FieldID: declFieldVersion, OpCode: cluster.UpdateSet}, {FieldID: createFieldState, OpCode: cluster.UpdateSet},
		{FieldID: declFieldFields, OpCode: cluster.UpdateSet}, {FieldID: declFieldPending, OpCode: cluster.UpdateUnset},
		{FieldID: declFieldAggregates, OpCode: cluster.UpdateUnset},
	}
	return r.write(slot, "the dropping marker", cluster.Op{
		Class: cluster.OpUpdate, BucketID: store.BucketsBucketID, SchemaVersionSeen: r.cfg.Published(r.dbID), EncID: encID, Fields: placeholder,
	}, r.rewriteMarker)
}

// rewriteMarker is the owner's read-modify-write of the marker, run under the declaration
// pair's lock. It refuses to build while a bump is mid-publication, as an alter does, because
// the version it writes must be one above the version every node is publishing.
func (r *dropRun) rewriteMarker() ([]cluster.FieldOp, error) {
	decl, _, found, err := r.cfg.Declarations.Read(r.ctx, r.dbID, r.name)
	if err != nil {
		return nil, fmt.Errorf("reading the declaration of bucket %q at its owner: %w", r.name, err)
	}
	if !found {
		return nil, errDropGone
	}
	if decl.BucketID != r.pinned {
		return nil, errDropGone
	}
	if decl.State != machine.StateActive {
		return nil, errDropMoved
	}
	published := r.cfg.Published(r.dbID)
	if r.cfg.Committed(r.dbID) != published {
		return nil, errDropMoved
	}
	marked := machine.Effective(decl, published)
	marked.State, marked.Version = machine.StateDropping, published+1
	stored, err := store.EncodeDeclaration(marked)
	if err != nil {
		return nil, fmt.Errorf("encoding the dropping declaration of bucket %q: %w", r.name, err)
	}
	fields := []cluster.FieldOp{
		{FieldID: declFieldVersion, OpCode: cluster.UpdateSet, Value: stored[declFieldVersion]},
		{FieldID: createFieldState, OpCode: cluster.UpdateSet, Value: stored[createFieldState]},
		{FieldID: declFieldFields, OpCode: cluster.UpdateSet, Value: stored[declFieldFields]},
	}
	if decl.Pending != nil {
		fields = append(fields, cluster.FieldOp{FieldID: declFieldPending, OpCode: cluster.UpdateUnset})
	}
	if len(marked.Aggregates.Sets) != len(decl.Aggregates.Sets) {
		// The table in force no longer declares a store whose set field 12 still holds.
		if value, ok := stored[declFieldAggregates]; ok {
			fields = append(fields, cluster.FieldOp{FieldID: declFieldAggregates, OpCode: cluster.UpdateSet, Value: value})
		} else {
			fields = append(fields, cluster.FieldOp{FieldID: declFieldAggregates, OpCode: cluster.UpdateUnset})
		}
	}
	return fields, nil
}

// reverseDropping upserts the reverse-index document with state dropping and the bucket's name:
// an upsert, not a rewrite: create writes the reverse document FIRST, so a declaration normally has one, but a reverse document that is missing (damage, or a drop that raced a loser's reclaim) must not make the drop fail on a missing key.
func (r *dropRun) reverseDropping(bucketID uint64) error {
	encID, err := store.EncodeComponent(idBytes(bucketID))
	if err != nil {
		return fmt.Errorf("encoding bucket id %016x: %w", bucketID, err)
	}
	slot := storage.SlotOf(storage.PlacementKey(r.dbName, systemBucketName, encID))
	fields := []cluster.FieldOp{
		{FieldID: createFieldState, Value: stringValue(machine.StateDropping)},
		{FieldID: createFieldName, Value: stringValue(r.name)},
	}
	// Checked at the owner under the reverse pair's lock: the loop deletes the declaration and then
	// this document under the same lock, so a write that sees the declaration still dropping lands
	// before that delete, and one that does not sees it gone and writes nothing.
	err = r.write(slot, "the reverse-index marker", cluster.Op{
		Class: cluster.OpUpsert, BucketID: store.BucketsBucketID, SchemaVersionSeen: r.cfg.Published(r.dbID),
		EncID: encID, IDValue: bucketIDValue(bucketID), Fields: fields,
	}, func() ([]cluster.FieldOp, error) { return fields, r.checkDeclaration(bucketID) })
	if err != nil {
		return err
	}
	return nil
}

// bump commits the one BUMP_SCHEMA when the marker is not yet published, and reports moved when
// another change took the version first (the caller reads again, and the marker may by then
// be published by that other bump).
func (r *dropRun) bump(decl machine.Declaration) (moved bool, err error) {
	published := r.cfg.Published(r.dbID)
	if !r.needsBump(decl.Version, published) {
		return false, nil
	}
	note := cluster.BumpNote{BucketID: decl.BucketID, Drop: true}
	err = r.cfg.Bumper.BumpSchema(r.ctx, r.dbID, published, published+1, note)
	var refusal *cluster.Error
	if errors.As(err, &refusal) && (refusal.Code == cluster.CodeStateMoved || refusal.Code == cluster.CodeSchemaNotCurrent) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("committing the schema bump of the drop of bucket %q: %w", r.name, err)
	}
	return false, nil
}

// sweep deletes the bucket's keys from every held slot not yet in the bitmap, and writes the
// bitmap after every sweptBatchSlots slots and at the end.
func (r *dropRun) sweep(decl machine.Declaration, swept []byte) error {
	if swept == nil {
		swept = make([]byte, store.SweptBytes)
	}
	var unswept []uint16
	for _, slot := range r.cfg.Sweeper.HeldSlots(r.dbID) {
		if swept[slot/8]&(1<<(slot%8)) == 0 {
			unswept = append(unswept, slot)
		}
	}
	// One synced pre-pass writes every missing 'b' record before the first sweep, and so before any
	// bitmap write that names a slot (§3.7): the records are at or below the cut by then, and the
	// per-slot sweeps below find them and sync nothing. A kill in the middle of the pre-pass leaves
	// some records and no bitmap bit; the drop is run again and writes the rest.
	if err := r.cfg.Sweeper.RecordSweeps(r.dbID, decl.BucketID, unswept); err != nil {
		return fmt.Errorf("writing the sweep records of bucket %q: %w", r.name, err)
	}
	pending := 0
	for _, slot := range unswept {
		if err := r.SweepSlot(r.ctx, slot, r.dbID, decl.BucketID); err != nil {
			return err
		}
		swept[slot/8] |= 1 << (slot % 8)
		pending++
		if err := r.step(dropStepSweep); err != nil {
			return err
		}
		if pending == sweptBatchSlots {
			if err := r.recordSwept(decl, swept); err != nil {
				return err
			}
			pending = 0
		}
	}
	if pending > 0 {
		return r.recordSwept(decl, swept)
	}
	return nil
}

// recordSwept writes field 6 of the dropping declaration. A declaration already gone means
// another run finished, which is not this run's failure.
func (r *dropRun) recordSwept(decl machine.Declaration, swept []byte) error {
	stored, err := store.EncodeDeclarationWithSwept(decl, swept)
	if err != nil {
		return fmt.Errorf("encoding the swept bitmap of bucket %q: %w", r.name, err)
	}
	encID, err := store.EncodeComponent(r.name)
	if err != nil {
		return fmt.Errorf("encoding bucket name %q: %w", r.name, err)
	}
	slot := storage.SlotOf(storage.PlacementKey(r.dbName, systemBucketName, encID))
	fields := []cluster.FieldOp{{FieldID: declFieldSwept, OpCode: cluster.UpdateSet, Value: stored[declFieldSwept]}}
	// Checked at the owner under the pair's lock: field 6 on a declaration that is not this
	// bucket's dropping one would corrupt it.
	err = r.write(slot, "the swept bitmap", cluster.Op{
		Class: cluster.OpUpdate, BucketID: store.BucketsBucketID, SchemaVersionSeen: r.cfg.Published(r.dbID), EncID: encID, Fields: fields,
	}, func() ([]cluster.FieldOp, error) { return fields, r.checkDeclaration(decl.BucketID) })
	var refusal *cluster.Error
	if errors.Is(err, errDropGone) || errors.Is(err, errDropMoved) || (errors.As(err, &refusal) && refusal.Code == cluster.CodeNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return r.step(dropStepSwept)
}

// finish deletes the declaration, which frees the name, and then the reverse-index document,
// which frees the id. The order protects the id: freeing it first would let a create mint it
// while a declaration still carries it. Each delete is checked at its owner under the pair's
// lock, so a slow run cannot delete a bucket a faster run's name was re-created as.
func (r *dropRun) finish(bucketID uint64) (bool, error) {
	nameEnc, err := store.EncodeComponent(r.name)
	if err != nil {
		return false, fmt.Errorf("encoding bucket name %q: %w", r.name, err)
	}
	nameSlot := storage.SlotOf(storage.PlacementKey(r.dbName, systemBucketName, nameEnc))
	err = r.write(nameSlot, "the declaration delete", cluster.Op{
		Class: cluster.OpDelete, BucketID: store.BucketsBucketID, SchemaVersionSeen: r.cfg.Published(r.dbID), EncID: nameEnc,
	}, func() ([]cluster.FieldOp, error) { return nil, r.checkDeclaration(bucketID) })
	switch {
	case (errors.Is(err, errDropGone) || errors.Is(err, errDropMoved)) && !r.reclaim:
		// Gone: another run finished. Moved: the declaration is not this bucket's dropping one any
		// more, and the drop is not ours to finish.
		r.movedAway = errors.Is(err, errDropMoved)
		return true, nil
	case err != nil && !errors.Is(err, errDropGone) && !errors.Is(err, errDropMoved):
		return false, err
	}
	if err := r.step(dropStepDeclaration); err != nil {
		return false, err
	}
	idEnc, err := store.EncodeComponent(idBytes(bucketID))
	if err != nil {
		return false, fmt.Errorf("encoding bucket id %016x: %w", bucketID, err)
	}
	idSlot := storage.SlotOf(storage.PlacementKey(r.dbName, systemBucketName, idEnc))
	err = r.write(idSlot, "the reverse-index delete", cluster.Op{
		Class: cluster.OpDelete, BucketID: store.BucketsBucketID, SchemaVersionSeen: r.cfg.Published(r.dbID), EncID: idEnc,
	}, func() ([]cluster.FieldOp, error) { return nil, r.checkReverse(bucketID) })
	if err != nil && !errors.Is(err, errDropGone) && !errors.Is(err, errDropMoved) {
		return false, err
	}
	return true, r.step(dropStepDone)
}

// checkDeclaration is the guard of the declaration delete: it is still this drop's, dropping.
func (r *dropRun) checkDeclaration(bucketID uint64) error {
	decl, _, found, err := r.cfg.Declarations.Read(r.ctx, r.dbID, r.name)
	if err != nil {
		return fmt.Errorf("reading the declaration of bucket %q at its owner: %w", r.name, err)
	}
	switch {
	case !found:
		return errDropGone
	case decl.BucketID != bucketID || decl.State != machine.StateDropping:
		return errDropMoved
	}
	return nil
}

// checkReverse is the guard of the reverse-index delete: the document still names this bucket.
func (r *dropRun) checkReverse(bucketID uint64) error {
	name, found, err := r.cfg.BucketName(r.ctx, r.dbID, bucketID)
	if err != nil {
		return fmt.Errorf("reading the reverse-index document of bucket %016x at its owner: %w", bucketID, err)
	}
	// The id is freed only when the document is a dropping one: a create writes it active.
	state, present, err := r.cfg.ReverseState(r.ctx, r.dbID, bucketID)
	if err != nil {
		return fmt.Errorf("reading the state of the reverse-index document of bucket %016x at its owner: %w", bucketID, err)
	}
	if !present {
		return errDropGone
	}
	if state != machine.StateDropping {
		return errDropMoved
	}
	// The id is freed only when no declaration still carries it.
	if decl, _, present, err := r.cfg.Declarations.Read(r.ctx, r.dbID, r.name); err != nil {
		return fmt.Errorf("reading the declaration of bucket %q at its owner: %w", r.name, err)
	} else if present && decl.BucketID == bucketID {
		return errDropMoved
	}
	switch {
	case !found:
		return errDropGone
	case name != r.name:
		return errDropMoved
	}
	return nil
}

// write orders one ring document at majority and refuses anything less (an applied_degraded
// declaration write is not a success: a schema change must be durable on every copy that will
// serve it, conv 20).
func (r *dropRun) write(slot uint16, what string, op cluster.Op, rewrite func() ([]cluster.FieldOp, error)) error {
	var requestID [16]byte
	if _, err := entropy.Read(requestID[:]); err != nil {
		return fmt.Errorf("drawing the request id of %s: %w", what, err)
	}
	answer, err := r.cfg.Sender.Write(r.ctx, WriteRequest{
		Slot: slot, DBID: r.dbID, Op: op, Concern: machine.ConcernMajority, RequestID: requestID, Rewrite: rewrite,
	})
	if err != nil {
		if errors.Is(err, errDropMoved) || errors.Is(err, errDropGone) {
			return err
		}
		return fmt.Errorf("%s of the drop of bucket %q in database %q: %w", what, r.name, r.dbName, err)
	}
	switch {
	case answer.Held || answer.Forward:
		return refuse(cluster.CodeUnavailable, "%s of the drop of bucket %q in database %q was not ordered here because %s. Nothing was deleted. Run 'drop bucket %s in %s' again in a few seconds: it is safe to repeat",
			what, r.name, r.dbName, answer.notOrderedBecause(), r.name, r.dbName)
	case answer.ApplyFailure != nil:
		return errors.Join(refuse(cluster.CodeUnavailable, "%s of the drop of bucket %q in database %q is durable at %s but applying it failed on this node, so it was not confirmed. Run 'drop bucket %s in %s' again once the pair has recovered: it is safe to repeat",
			what, r.name, r.dbName, answer.Pos, r.name, r.dbName), answer.ApplyFailure)
	case answer.Outcome == machine.OutcomeApplied:
		return nil
	case answer.Outcome == machine.OutcomeAppliedDegraded:
		rf := 0
		if row, ok := r.cfg.Topology.Database(r.dbID); ok {
			rf = row.RF
		}
		return &cluster.Error{Code: cluster.CodeUnavailable, Message: cluster.CodeUnavailable.Text(cluster.CondDeclarationWriteFailed, map[string]string{
			"op": "drop bucket", "db": r.dbName, "bucket": r.name, "c": strconv.Itoa(int(answer.CopiesConfirmed)),
			"rf": strconv.Itoa(rf), "slot": strconv.Itoa(int(slot)),
		})}
	}
	return refuse(cluster.CodeUnavailable, "%s of the drop of bucket %q in database %q has an unknown outcome. Run 'drop bucket %s in %s' again: it is safe to repeat",
		what, r.name, r.dbName, r.name, r.dbName)
}
