package wire

import (
	"context"
	entropy "crypto/rand"
	"errors"
	"fmt"
	jitter "math/rand/v2"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// AlterChange is what an alter bucket statement asks for: fields to add, each with its
// canonical type word, and names of fields to drop. A name appears once in the whole change.
type AlterChange struct {
	Add  []FieldEntry
	Drop []string
	// SetExpire names the datetime field the bucket expires its documents at (`set expire at`);
	// DropExpire removes the bucket's expiry (`drop expire at`). At most one of them. Either rides the
	// pending table, so it is in force on every node at the bump and on none before it.
	SetExpire  string
	DropExpire bool
}

// SchemaBumper commits BUMP_SCHEMA. *Node implements it with its Raft group.
type SchemaBumper interface {
	// BumpSchema commits database dbID's move from schema_version from to to, carrying the
	// note that tells every node which bucket and which retired fields to sweep, and returns
	// once this node has published it (conv 20 step 6).
	BumpSchema(ctx context.Context, dbID uint64, from, to int64, note cluster.BumpNote) error
}

// AlterBucketConfig is the collaborators of an AlterBucket.
type AlterBucketConfig struct {
	// Sender orders the declaration write at the owner of the declaration's slot.
	Sender Sender
	// Declarations reads a declaration by name from its owner.
	Declarations DeclarationSource
	// Topology gives the replication factor the degraded refusal reports and the committed epoch.
	Topology Topology
	// DatabaseName is the name a database id placed under.
	DatabaseName func(dbID uint64) (string, bool)
	// Committed is the database's committed schema_version, the number a bump is built on.
	Committed func(dbID uint64) int64
	// Published is the database's published schema_version on this node.
	Published func(dbID uint64) int64
	// Bumper commits the bump once the new table is durable.
	Bumper SchemaBumper
	// Queue, when set, makes alters of one database from this node take turns. Alters from other
	// nodes still meet each other, which the retry loop and its backoff absorb. Share one Queue
	// among every AlterBucket of a node; nil queues nothing.
	Queue *AlterQueue
	// BackoffSeed seeds the jitter between attempts; 0 draws a seed from the system.
	BackoffSeed uint64
	// Sleep waits for d or until ctx ends; nil is a timer. A test replaces it.
	Sleep func(ctx context.Context, d time.Duration) error
}

// AlterQueue lets one alter per database run at a time on a node. Its cost is one empty channel
// per database that has ever been altered here, nothing per bucket.
type AlterQueue struct{ turns sync.Map }

// wait takes the database's turn, or returns when ctx ends. The returned function gives it back.
func (q *AlterQueue) wait(ctx context.Context, dbID uint64) (func(), error) {
	turn, _ := q.turns.LoadOrStore(dbID, make(chan struct{}, 1))
	slot := turn.(chan struct{})
	select {
	case slot <- struct{}{}:
		return func() { <-slot }, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("waiting for another alter of database %d to finish: %w", dbID, ctx.Err())
	}
}

// Backoff between attempts: exponential from alterBackoffBase, capped, with the whole delay
// jittered so contending coordinators do not retry in step.
const (
	alterBackoffBase = 5 * time.Millisecond
	alterBackoffCap  = 200 * time.Millisecond
)

// alterBackoff is the wait before retry number attempt (0-based): a uniform draw from half the
// exponential delay up to the whole of it.
func alterBackoff(attempt int, rng *jitter.Rand) time.Duration {
	delay := alterBackoffCap
	if attempt < 6 {
		delay = min(alterBackoffBase<<attempt, alterBackoffCap)
	}
	half := delay / 2
	return half + time.Duration(rng.Int64N(int64(delay-half)+1))
}

func sleepTimer(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("waiting to retry the alter: %w", ctx.Err())
	}
}

// AlterBucket runs conv 20's alter: the new field table is written as field 8 `pending` at
// the declaration's owner as one read-modify-write, then BUMP_SCHEMA is committed, and that
// commit is what makes the table effective. A crash or a refusal between the two leaves the
// old table in force and the bucket writable; running the same alter again finishes it.
//
// Cost (rule 11), per alter and never per bucket or document: one declaration write of 1 key
// (3 when it first folds a published pending table), one Raft commit and one pair-lock pass
// on each node. Adding a field to a bucket of 10^6 documents rewrites no document; dropping
// one only records a retired id. Nothing is held between alters.
type AlterBucket struct {
	cfg AlterBucketConfig
}

// NewAlterBucket returns an AlterBucket over cfg.
func NewAlterBucket(cfg AlterBucketConfig) *AlterBucket { return &AlterBucket{cfg: cfg} }

// maxAlterAttempts bounds the rebuilds one alter makes when another alter, a bump or a
// publication moves under it. Each attempt is one declaration write and one commit.
const maxAlterAttempts = 8

// publicationWait is how long a repeat waits for this node to publish a committed bump.
const publicationWait = 10 * time.Millisecond

// errAlterMoved is what the owner's read-modify-write returns when the database moved
// between the caller's read and the owner's: the caller reads again and rebuilds. It is never
// shown to a client.
var errAlterMoved = errors.New("the database's schema_version moved during the alter")

// The _buckets field ids an alter writes (conv 19).
const (
	declFieldVersion uint16 = 3
	declFieldFields  uint16 = 5
	declFieldPending uint16 = 8
	declFieldExpire  uint16 = 11
	// declFieldReferences is the declaration field of the fields declared 'of' another bucket.
	declFieldReferences uint16 = 13
	// declFieldAggregates is declaration field 12.
	declFieldAggregates = machine.SystemOptionalAggregates
)

// Run alters bucket name of database dbID and returns the field table in force afterwards.
// It is idempotent: a change the table in force already holds writes nothing, and the same
// alter run after an interrupted one writes the same pending table and commits the bump.
func (a *AlterBucket) Run(ctx context.Context, dbID uint64, name string, change AlterChange) (machine.FieldTable, error) {
	dbName, ok := a.cfg.DatabaseName(dbID)
	if !ok {
		return machine.FieldTable{}, refuse(cluster.CodeNotFound, "altering bucket %q: database id %d is not in the committed state. Run 'show databases' to list what exists", name, dbID)
	}
	if err := checkBucketName(dbName, name); err != nil {
		return machine.FieldTable{}, err
	}
	if err := checkAlterChange(dbName, name, change); err != nil {
		return machine.FieldTable{}, err
	}
	// The bucket is pinned before the alter waits for its turn: a bucket dropped and created again
	// while this alter queued is not the one it was asked for.
	var pinned uint64
	if decl, _, found, err := a.cfg.Declarations.Read(ctx, dbID, name); err != nil {
		return machine.FieldTable{}, fmt.Errorf("reading the declaration of bucket %q in database %q: %w", name, dbName, err)
	} else if found {
		pinned = decl.BucketID
		if change.DropExpire && machine.EffectiveExpire(decl, a.cfg.Committed(dbID)).BucketField == 0 {
			return machine.FieldTable{}, refuse(cluster.CodeInvalidArgument, "EXPIRE_NOT_DECLARED: bucket %q in database %q declares no expiry, so there is nothing to drop. Run 'describe bucket %s in %s' to see what it declares, or 'alter bucket %s in %s { set expire at <Field> }' to expire its documents",
				name, dbName, name, dbName, name, dbName)
		}
	}
	if a.cfg.Queue != nil {
		release, err := a.cfg.Queue.wait(ctx, dbID)
		if err != nil {
			return machine.FieldTable{}, err
		}
		defer release()
	}
	seed := a.cfg.BackoffSeed
	if seed == 0 {
		seed = jitter.Uint64()
	}
	rng := jitter.New(jitter.NewPCG(seed, seed))
	sleep := a.cfg.Sleep
	if sleep == nil {
		sleep = sleepTimer
	}
	for attempt := 0; attempt < maxAlterAttempts; attempt++ {
		if attempt > 0 {
			if err := sleep(ctx, alterBackoff(attempt-1, rng)); err != nil {
				return machine.FieldTable{}, err
			}
		}
		run := &alterRun{AlterBucket: a, ctx: ctx, dbID: dbID, dbName: dbName, name: name, change: change, pinned: &pinned}
		table, done, err := run.attempt()
		if err != nil {
			return machine.FieldTable{}, err
		}
		if done {
			return table, nil
		}
	}
	return machine.FieldTable{}, &cluster.Error{Code: cluster.CodeSchemaNotCurrent, Message: fmt.Sprintf(
		"altering bucket %q in database %q kept meeting another change to the database's schema and gave up after %d tries, so the change may be only partly applied. Nothing is lost. Run 'describe bucket %s in %s' to see the fields in force, then run the same 'alter bucket %s in %s { … }' again to finish it",
		name, dbName, maxAlterAttempts, name, dbName, name, dbName)}
}

// Bump commits the move of database dbID from schema_version from to to. The caller has made
// the new table durable first (conv 20 step 3), so this is only ever the second half.
func (a *AlterBucket) Bump(ctx context.Context, dbID uint64, from, to int64, note cluster.BumpNote) error {
	return a.cfg.Bumper.BumpSchema(ctx, dbID, from, to, note)
}

// alterRun is one attempt: a read, the owner's write, the bump and the check.
type alterRun struct {
	*AlterBucket
	ctx    context.Context
	dbID   uint64
	dbName string
	name   string
	change AlterChange
	// pinned is the id of the bucket the first read found, shared by every attempt of one alter:
	// a name that carries another id was dropped and created again, and this alter is not for it.
	pinned *uint64
	// wroteAt is the schema_version the owner built the pending table on, set by the owner's
	// read-modify-write; the bump goes from it.
	wroteAt int64
}

// attempt is conv 20's steps 1 to 4 once. done false means something moved and the caller
// should build again from a fresh read.
func (r *alterRun) attempt() (table machine.FieldTable, done bool, err error) {
	decl, err := r.read()
	if err != nil {
		return machine.FieldTable{}, false, err
	}
	committed := r.cfg.Committed(r.dbID)
	base, _ := machine.EffectiveTable(decl, committed)
	epoch := r.cfg.Topology.Committed()
	plan := planInput{db: r.dbName, bucket: r.name, base: base, change: r.change, epoch: epoch}
	planned, changed, err := planAlter(plan)
	if err != nil {
		return machine.FieldTable{}, false, err
	}
	if _, expireChanged, err := planExpire(plan, planned, machine.EffectiveExpire(decl, committed)); err != nil {
		return machine.FieldTable{}, false, err
	} else if expireChanged {
		changed = true
	}
	if !changed {
		// Everything asked is already in force: a repeat of an alter that finished. The reply
		// is described from the published table, so answer only once this node publishes the
		// version the committed table came from; until then the caller reads again.
		if r.cfg.Published(r.dbID) != committed {
			time.Sleep(publicationWait)
			return machine.FieldTable{}, false, nil
		}
		return base, true, nil
	}
	err = r.writePending()
	if errors.Is(err, errAlterMoved) {
		return machine.FieldTable{}, false, nil
	}
	if err != nil {
		return machine.FieldTable{}, false, err
	}
	// The note names the bucket and the ids this alter retired, so every node sweeps them
	// (U43). It is a claim to be checked, not an order: a node deletes an id only if the
	// declaration in force retires it, so two alters at one version cannot make the loser's
	// ids a sweep target (SPEC-GAP U40-2).
	var note cluster.BumpNote
	if retired := retiredBy(base, planned); len(retired) > 0 {
		note = cluster.BumpNote{BucketID: decl.BucketID, Retired: retired, Epoch: epoch}
	}
	err = r.Bump(r.ctx, r.dbID, r.wroteAt, r.wroteAt+1, note)
	var refusal *cluster.Error
	if errors.As(err, &refusal) && (refusal.Code == cluster.CodeStateMoved || refusal.Code == cluster.CodeSchemaNotCurrent) {
		// Another change took the version first. The pending table we wrote may have been
		// overwritten by theirs; read again and see what is in force.
		return machine.FieldTable{}, false, nil
	}
	if err != nil {
		return machine.FieldTable{}, false, err
	}
	return r.confirm()
}

// retiredBy lists, ascending, the field ids planned retires beyond those base already had.
func retiredBy(base, planned machine.FieldTable) []uint16 {
	var ids []uint16
	for _, retired := range planned.Retired[len(base.Retired):] {
		ids = append(ids, retired.ID)
	}
	slices.Sort(ids)
	return ids
}

// confirm reads the table in force after the bump and checks that it holds this alter's
// change. Two alters that wrote the same declaration at the same version leave one table
// published, and the other one's change is missing from it: that one runs again.
func (r *alterRun) confirm() (machine.FieldTable, bool, error) {
	decl, err := r.read()
	if err != nil {
		return machine.FieldTable{}, false, err
	}
	table, _ := machine.EffectiveTable(decl, r.cfg.Committed(r.dbID))
	plan := planInput{db: r.dbName, bucket: r.name, base: table, change: r.change, epoch: r.cfg.Topology.Committed()}
	planned, changed, err := planAlter(plan)
	if err != nil {
		return machine.FieldTable{}, false, err
	}
	if _, expireChanged, err := planExpire(plan, planned, machine.EffectiveExpire(decl, r.cfg.Committed(r.dbID))); err != nil {
		return machine.FieldTable{}, false, err
	} else if expireChanged {
		changed = true
	}
	return table, !changed, nil
}

// read resolves conv 20 step 1: the declaration, refused when the bucket is absent or dropping.
func (r *alterRun) read() (machine.Declaration, error) {
	decl, _, found, err := r.cfg.Declarations.Read(r.ctx, r.dbID, r.name)
	if err != nil {
		return machine.Declaration{}, fmt.Errorf("reading the declaration of bucket %q in database %q: %w", r.name, r.dbName, err)
	}
	if err := r.usable(decl, found); err != nil {
		return decl, err
	}
	if *r.pinned == 0 {
		*r.pinned = decl.BucketID
	}
	return decl, r.checkPinned(decl)
}

// checkPinned refuses a declaration whose bucket id is not the one this alter began on.
func (r *alterRun) checkPinned(decl machine.Declaration) error {
	if decl.BucketID != *r.pinned {
		return refuse(cluster.CodeNotFound, "bucket %q in database %q was dropped and created again while this alter ran, so the change was not applied to the new bucket. Run 'describe bucket %s in %s' to see the new bucket, then run the alter again if you still want it",
			r.name, r.dbName, r.name, r.dbName)
	}
	return nil
}

// usable refuses a declaration that is absent or dropping.
func (r *alterRun) usable(decl machine.Declaration, found bool) error {
	switch {
	case !found:
		return &cluster.Error{Code: cluster.CodeNotFound, Message: cluster.CodeNotFound.Text(cluster.CondBucketNotFound,
			map[string]string{"bucket": r.name, "db": r.dbName})}
	case decl.State != machine.StateActive:
		return &cluster.Error{Code: cluster.CodeBucketDropping, Message: cluster.CodeBucketDropping.Text(cluster.CondBucketDropping,
			map[string]string{"bucket": r.name, "db": r.dbName, "n": "0"})}
	}
	return nil
}

// writePending is conv 20 step 2: one write at the declaration's owner, at the concern the
// declaration writes use, whose fields the owner builds itself from the declaration it holds.
func (r *alterRun) writePending() error {
	encID, err := store.EncodeComponent(r.name)
	if err != nil {
		return fmt.Errorf("encoding bucket name %q: %w", r.name, err)
	}
	var requestID [16]byte
	if _, err := entropy.Read(requestID[:]); err != nil {
		return fmt.Errorf("drawing the request id of the alter of bucket %q: %w", r.name, err)
	}
	slot := storage.SlotOf(storage.PlacementKey(r.dbName, systemBucketName, encID))
	placeholder := make([]cluster.FieldOp, 0, 3)
	for _, id := range []uint16{declFieldVersion, declFieldFields, declFieldPending} {
		placeholder = append(placeholder, cluster.FieldOp{FieldID: id, OpCode: cluster.UpdateSet})
	}
	// Field 12 is pruned only when a fold drops a store field's set; the owner decides in rewrite,
	// and the placeholder names it so the write's class admits it either way.
	placeholder = append(placeholder, cluster.FieldOp{FieldID: declFieldAggregates, OpCode: cluster.UpdateSet})
	answer, err := r.cfg.Sender.Write(r.ctx, WriteRequest{
		Slot: slot, DBID: r.dbID, Concern: machine.ConcernMajority, RequestID: requestID,
		Op: cluster.Op{
			Class: cluster.OpUpdate, BucketID: store.BucketsBucketID, SchemaVersionSeen: r.cfg.Published(r.dbID),
			EncID: encID, Fields: placeholder,
		},
		Rewrite: r.rewrite,
	})
	if err != nil {
		if errors.Is(err, errAlterMoved) {
			return err
		}
		return fmt.Errorf("the declaration write of the alter of bucket %q in database %q: %w", r.name, r.dbName, err)
	}
	switch {
	case answer.Held || answer.Forward:
		return refuse(cluster.CodeUnavailable, "the declaration write of the alter of bucket %q in database %q was not ordered here because %s. Nothing was changed. Run 'alter bucket %s in %s { … }' again in a few seconds: it is safe to repeat",
			r.name, r.dbName, answer.notOrderedBecause(), r.name, r.dbName)
	case answer.ApplyFailure != nil:
		return errors.Join(refuse(cluster.CodeUnavailable, "the declaration write of the alter of bucket %q in database %q is durable at %s but applying it failed on this node, so it was not confirmed. Run 'alter bucket %s in %s { … }' again once the pair has recovered: it is safe to repeat",
			r.name, r.dbName, answer.Pos, r.name, r.dbName), answer.ApplyFailure)
	case answer.Outcome == machine.OutcomeApplied:
		return nil
	case answer.Outcome == machine.OutcomeAppliedDegraded:
		rf := 0
		if row, ok := r.cfg.Topology.Database(r.dbID); ok {
			rf = row.RF
		}
		return &cluster.Error{Code: cluster.CodeUnavailable, Message: cluster.CodeUnavailable.Text(cluster.CondDeclarationWriteFailed, map[string]string{
			"op": "alter bucket", "db": r.dbName, "bucket": r.name, "c": strconv.Itoa(int(answer.CopiesConfirmed)),
			"rf": strconv.Itoa(rf), "slot": strconv.Itoa(int(slot)),
		})}
	}
	return refuse(cluster.CodeUnavailable, "the declaration write of the alter of bucket %q in database %q has an unknown outcome. Run 'alter bucket %s in %s { … }' again: it is safe to repeat",
		r.name, r.dbName, r.name, r.dbName)
}

// rewrite is the read-modify-write, run by the owner under the declaration pair's lock. It
// reads the declaration the owner holds, builds the new table from the one in force, and
// returns the fields to set: field 8 alone, or fields 3, 5 and 8 when a published pending
// table is folded first. Field 5 is not touched otherwise, which is what keeps the bucket on
// its old table until the bump publishes the new one.
func (r *alterRun) rewrite() ([]cluster.FieldOp, error) {
	decl, _, found, err := r.cfg.Declarations.Read(r.ctx, r.dbID, r.name)
	if err != nil {
		return nil, fmt.Errorf("reading the declaration of bucket %q at its owner: %w", r.name, err)
	}
	if err := r.usable(decl, found); err != nil {
		return nil, err
	}
	if err := r.checkPinned(decl); err != nil {
		return nil, err
	}
	// The committed and the published version must agree, or a bump is mid-publication: a
	// pending table written now could become effective before it is built on the table that
	// is. The caller reads again.
	published := r.cfg.Published(r.dbID)
	if r.cfg.Committed(r.dbID) != published {
		return nil, errAlterMoved
	}
	base, _ := machine.EffectiveTable(decl, published)
	plan := planInput{db: r.dbName, bucket: r.name, base: base, change: r.change, epoch: r.cfg.Topology.Committed()}
	next, changed, err := planAlter(plan)
	if err != nil {
		return nil, err
	}
	plannedExpire, expireChanged, err := planExpire(plan, next, machine.EffectiveExpire(decl, published))
	if err != nil {
		return nil, err
	}
	if !changed && !expireChanged {
		return nil, errAlterMoved
	}
	// A published pending table is folded into field 5 first (field 3 := its version, field
	// 5 := its table), so a quiet declaration is 5 keys again. An unpublished one is inert
	// and is simply replaced: field 5 stays in force.
	declared, ids := decl, []uint16{declFieldPending}
	if decl.Pending != nil && decl.Pending.Version <= published {
		declared, ids = machine.Fold(decl), []uint16{declFieldVersion, declFieldFields, declFieldPending}
		if decl.Pending.Expire != nil {
			// The fold moved the pending expiry into field 11, which is then written, or removed when it is empty.
			ids = append(ids, declFieldExpire)
		}
		if len(declared.Aggregates.Sets) != len(decl.Aggregates.Sets) {
			ids = append(ids, declFieldAggregates)
		}
	}
	// An unpublished pending is replaced whole, its table and its expiry together: its expiry named field
	// ids of its own table, which this alter does not build on, so nothing of it is carried. The alter that
	// wrote it plans its expiry again when it is run again.
	var pendingExpire *machine.ExpireTable
	if expireChanged {
		pendingExpire = &plannedExpire
	}
	declared.Pending = &machine.Pending{Version: published + 1, Table: next, Expire: pendingExpire}
	prunedReferences := len(declared.References) != len(decl.References)
	if prunedReferences {
		ids = append(ids, declFieldReferences)
	}
	stored, err := store.EncodeDeclaration(declared)
	if err != nil {
		return nil, fmt.Errorf("encoding the pending table of bucket %q: %w", r.name, err)
	}
	if r.cfg.Published(r.dbID) != published {
		return nil, errAlterMoved
	}
	fields := make([]cluster.FieldOp, 0, len(ids))
	for _, id := range ids {
		if scalar, written := stored[id]; !written {
			fields = append(fields, cluster.FieldOp{FieldID: id, OpCode: cluster.UpdateUnset})
		} else {
			fields = append(fields, cluster.FieldOp{FieldID: id, OpCode: cluster.UpdateSet, Value: scalar})
		}
	}
	r.wroteAt = published
	return fields, nil
}

// checkAlterChange refuses a change that can never apply, before any read.
func checkAlterChange(dbName, bucket string, change AlterChange) error {
	if change.SetExpire != "" && change.DropExpire {
		return refuse(cluster.CodeInvalidArgument, "EXPIRE_TWICE: alter bucket %s in %s both sets and drops the expiry, and one alter changes it once. Run 'alter bucket %s in %s { set expire at <Field> }' or 'alter bucket %s in %s { drop expire at }', not both",
			bucket, dbName, bucket, dbName, bucket, dbName)
	}
	if len(change.Add) == 0 && len(change.Drop) == 0 && change.SetExpire == "" && !change.DropExpire {
		return refuse(cluster.CodeInvalidArgument, "alter bucket %s in %s names nothing to change. Run 'alter bucket %s in %s { add <Field> <type> }' to add a field, '{ drop <Field> }' to remove one, or '{ set expire at <Field> }' to expire its documents",
			bucket, dbName, bucket, dbName)
	}
	seen := map[string]bool{}
	named := make([]string, 0, len(change.Add)+len(change.Drop))
	for _, field := range change.Add {
		if field.Name == "" || field.Type == "" {
			return refuse(cluster.CodeInvalidArgument, "alter bucket %s in %s adds a field with no name or no type. Run 'alter bucket %s in %s { add <Field> <type> }' with both",
				bucket, dbName, bucket, dbName)
		}
		named = append(named, field.Name)
	}
	named = append(named, change.Drop...)
	for _, name := range named {
		if name == "_id_" {
			return refuse(cluster.CodeInvalidArgument, "alter bucket %s in %s names _id_, which every bucket has as its document id and which never changes. Run 'alter bucket %s in %s { … }' without _id_",
				bucket, dbName, bucket, dbName)
		}
		if seen[name] {
			return refuse(cluster.CodeInvalidArgument, "alter bucket %s in %s names field %q twice, and one alter changes a field once (a type never changes: dropping a field and adding it again would lose its values). Name each field once, in an add or in a drop",
				bucket, dbName, name)
		}
		seen[name] = true
	}
	return nil
}

// planInput is everything planAlter needs.
type planInput struct {
	db, bucket string
	base       machine.FieldTable
	change     AlterChange
	// epoch is the committed epoch, stamped on every id the alter retires.
	epoch uint64
}

// planAlter applies change to base and reports the new table and whether it differs. It is a
// pure function, so the owner's read-modify-write and the caller's check cannot disagree.
// Drops come first, so one alter at the cap can drop a field and add another.
//
// A field already declared with the type asked for is not an error and changes nothing, and a
// name that is not declared drops nothing: both make the same alter run twice give the same
// result, which is what the interrupted-alter fix text relies on. The same name with another
// type is refused, because a type never changes (placement 5).
func planAlter(in planInput) (machine.FieldTable, bool, error) {
	// A copy of the base, not a table rebuilt member by member: a member added to FieldTable
	// later must survive an alter. The slices are cloned so the alter edits its own.
	next := in.base
	next.Entries = slices.Clone(in.base.Entries)
	next.Retired = slices.Clone(in.base.Retired)
	changed := false
	for _, name := range in.change.Drop {
		at := slices.IndexFunc(next.Entries, func(entry machine.FieldEntry) bool { return entry.Name == name })
		if at < 0 {
			continue
		}
		next.Retired = append(next.Retired, machine.RetiredID{ID: next.Entries[at].ID, Epoch: in.epoch})
		next.Entries = slices.Delete(next.Entries, at, at+1)
		changed = true
	}
	for _, field := range in.change.Add {
		at := slices.IndexFunc(next.Entries, func(entry machine.FieldEntry) bool { return entry.Name == field.Name })
		if at >= 0 {
			if next.Entries[at].Type == field.Type {
				continue
			}
			return machine.FieldTable{}, false, typeChangeRefusal(in, next.Entries[at], field)
		}
		if len(next.Entries) >= store.MaxFieldTableEntries {
			return machine.FieldTable{}, false, refuse(cluster.CodeInvalidArgument, "bucket %q in database %q already declares %d fields, the maximum, because one insert writes one key per field and must stay inside one batch. Drop a field first: 'alter bucket %s in %s { drop <field> }'",
				in.bucket, in.db, store.MaxFieldTableEntries-1, in.bucket, in.db)
		}
		if next.NextFieldID == 0 || len(next.Retired) >= 0xFFFF {
			return machine.FieldTable{}, false, refuse(cluster.CodeInvalidArgument, "bucket %q in database %q has used all 65535 field ids, retired ones included, and an id is never reused. Create a new bucket and copy the values: 'create bucket %s2 in %s { <Field> <type> }', copy into it, then 'drop bucket %s in %s'",
				in.bucket, in.db, in.bucket, in.db, in.bucket, in.db)
		}
		next.Entries = append(next.Entries, machine.FieldEntry{ID: next.NextFieldID, Type: field.Type, Name: field.Name})
		// 65535 is the last id; the next one wraps to 0, which means none is left.
		next.NextFieldID++
		changed = true
	}
	return next, changed, nil
}

// planExpire applies the expiry part of change to current, the expiry in force with the base table,
// against next, the table the alter builds. It returns the expiry the new pending table carries and
// whether it differs. It is pure and idempotent, as planAlter is: an alter run again, and the check
// after a bump, both find "nothing to change".
func planExpire(in planInput, next machine.FieldTable, original machine.ExpireTable) (machine.ExpireTable, bool, error) {
	change := in.change
	current := original
	// A dropped field that is part of the expiry takes that part with it, in the same pending table:
	// the bucket's datetime field clears the bucket form, a queue field leaves the store form. There is
	// nothing to clear first and nothing to refuse.
	current = dropExpiryOfDroppedFields(in, current)
	switch {
	case change.DropExpire:
		out := current
		out.BucketField = 0
		return out, !reflect.DeepEqual(out, original), nil
	case change.SetExpire != "":
		built, err := buildExpireTable(in.db, in.bucket, next, ExpireSpec{BucketField: change.SetExpire})
		if err != nil {
			return original, false, err
		}
		out := current
		out.BucketField = built.BucketField
		return out, !reflect.DeepEqual(out, original), nil
	}
	return current, !reflect.DeepEqual(current, original), nil
}

// typeChangeRefusal is the text of conv Errors for a declared type that an alter would change.
func typeChangeRefusal(in planInput, declared machine.FieldEntry, asked FieldEntry) error {
	return refuse(cluster.CodeInvalidArgument, "field %q in %s.%s is declared %s and this statement declares it %s; a declared type never changes, because stored values were written as %s. Add a new field and copy the values: 'alter bucket %s in %s { add <NewField> <type> }', update into <NewField>, then 'alter bucket %s in %s { drop %s }'",
		declared.Name, in.db, in.bucket, declared.Type, asked.Type, declared.Type, in.bucket, in.db, in.bucket, in.db, declared.Name)
}

// dropExpiryOfDroppedFields removes from current the parts of the expiry that name a field this alter
// drops. It reads the dropped names against the base table; a name the base does not declare drops
// nothing, which keeps a repeated alter idempotent.
func dropExpiryOfDroppedFields(in planInput, current machine.ExpireTable) machine.ExpireTable {
	out := machine.ExpireTable{BucketField: current.BucketField, Stores: slices.Clone(current.Stores)}
	for _, name := range in.change.Drop {
		dropped, found := entryNamed(in.base, name)
		if !found {
			continue
		}
		if dropped.ID == out.BucketField {
			out.BucketField = 0
		}
		out.Stores = slices.DeleteFunc(out.Stores, func(expiring machine.ExpireStore) bool { return expiring.FieldID == dropped.ID })
	}
	if len(out.Stores) == 0 {
		out.Stores = nil
	}
	return out
}
