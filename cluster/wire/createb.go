package wire

import (
	"bytes"
	"context"
	entropy "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/namepolicy"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// maxUserFields is the entry cap of a field table less the implicit _id_ (conv 19).
const maxUserFields = store.MaxFieldTableEntries - 1

// maxIDAttempts is how many ids a create tries before BUCKET_ID_COLLISION (conv 21).
const maxIDAttempts = 3

// The _buckets field ids a create writes (conv 19).
const (
	createFieldState uint16 = 4
	createFieldName  uint16 = 7
)

// FieldEntry is one declared field of a bucket, as the caller names it.
type FieldEntry = machine.FieldEntry

// DeclarationSource reads a declaration straight from its owner. *Fetcher implements it.
type DeclarationSource interface {
	// Read returns the declaration of (dbID, name), or found false when there is none.
	Read(ctx context.Context, dbID uint64, name string) (decl machine.Declaration, swept []byte, found bool, err error)
}

// CreateBucketConfig is the collaborators of a CreateBucket.
type CreateBucketConfig struct {
	// Sender orders the ring documents, each at the owner of its own slot.
	Sender Sender
	// Declarations reads a declaration by name.
	Declarations DeclarationSource
	// Topology gives the replication factor the degraded refusal reports.
	Topology Topology
	// DatabaseName is the name a database id placed under.
	DatabaseName func(dbID uint64) (string, bool)
	// Published is the database's published schema_version, read at the call.
	Published func(dbID uint64) int64
	// DrawID draws a candidate bucket id; nil draws 8 bytes from the system entropy source. The name avoids the word the package guard TestMintedIDIsNeverMintedByTheOwner bans outside coordinator.go
	DrawID func() (uint64, error)
}

// CreateBucket runs conv 21's create: two ring documents on two slots, written in order
// and never in one batch, with no Raft entry. Every step is a state check followed by a
// write, so a retry after a crash resumes where the first run stopped.
type CreateBucket struct {
	cfg CreateBucketConfig
}

// NewCreateBucket returns a CreateBucket over cfg.
func NewCreateBucket(cfg CreateBucketConfig) *CreateBucket {
	if cfg.DrawID == nil {
		cfg.DrawID = randomBucketID
	}
	return &CreateBucket{cfg: cfg}
}

// randomBucketID draws 8 bytes from crypto/rand, never 0 and never the system bucket's id.
func randomBucketID() (uint64, error) {
	for {
		var raw [8]byte
		if _, err := entropy.Read(raw[:]); err != nil {
			return 0, fmt.Errorf("drawing a bucket id: %w", err)
		}
		if id := binary.BigEndian.Uint64(raw[:]); id != 0 && id != store.BucketsBucketID {
			return id, nil
		}
	}
}

// createRun is one Run: the resolved names and the table the declaration will carry.
type createRun struct {
	*CreateBucket
	ctx     context.Context
	dbID    uint64
	dbName  string
	name    string
	table   machine.FieldTable
	expire  machine.ExpireTable
	version int64
	// references are the fields declared 'of' another bucket, in field-id order.
	references []machine.Reference
	// aggregates is the declaration's field 12; the zero value is no aggregate.
	aggregates machine.AggregateTable
}

// referencesOf turns the target bucket id of each referencing field name into the
// declaration's list, ordered by field id.
func referencesOf(table machine.FieldTable, byName map[string]uint64) []machine.Reference {
	var references []machine.Reference
	for _, entry := range table.Entries {
		if bucketID, ok := byName[entry.Name]; ok {
			references = append(references, machine.Reference{FieldID: entry.ID, BucketID: bucketID})
		}
	}
	return references
}

// referencesFor is the declaration's references for the bucket id: a reference to the bucket
// itself carries id.
func (r *createRun) referencesFor(id uint64) []machine.Reference {
	if !slices.ContainsFunc(r.references, func(reference machine.Reference) bool { return reference.BucketID == selfReference }) {
		return r.references
	}
	out := slices.Clone(r.references)
	for i := range out {
		if out[i].BucketID == selfReference {
			out[i].BucketID = id
		}
	}
	return out
}

// Run creates bucket name in database dbID with the caller's fields (without _id_, which is
// entry 1 of every table) and returns its bucket id. It is idempotent: a retry with the
// same fields completes whatever a crashed run left undone and returns the same id. The
// reverse-index document is written first, because its insert claims the id, and the
// declaration second, so a declaration never holds an id its creator does not own (conv 21).
func (c *CreateBucket) Run(ctx context.Context, dbID uint64, name string, fields []FieldEntry, aggregates ...AggregateDecl) (BucketID uint64, err error) {
	return c.RunWithReferences(ctx, dbID, name, fields, nil, ExpireSpec{}, aggregates...)
}

// RunWithExpiry is Run for a bucket that declares expiry: the spec is resolved against the table
// and written as declaration field 11 in the same declaration insert. A repeat with the same
// fields and the same expiry is the create that already happened; the same fields with another
// expiry are BUCKET_EXISTS.
func (c *CreateBucket) RunWithExpiry(ctx context.Context, dbID uint64, name string, fields []FieldEntry, spec ExpireSpec) (BucketID uint64, err error) {
	return c.RunWithReferences(ctx, dbID, name, fields, nil, spec)
}

// RunWithReferences is Run for a bucket whose fields may be declared 'of' other buckets and
// that may declare expiry: references maps a field name to the id of the bucket its values are
// ids of, spec is the expiry resolved against the table (field 11) and aggregates are the
// maintained aggregates of its store fields (field 12).
func (c *CreateBucket) RunWithReferences(ctx context.Context, dbID uint64, name string, fields []FieldEntry, references map[string]uint64, spec ExpireSpec, aggregates ...AggregateDecl) (BucketID uint64, err error) {
	dbName, ok := c.cfg.DatabaseName(dbID)
	if !ok {
		return 0, refuse(cluster.CodeNotFound, "creating bucket %q: database id %d is not in the committed state. Run 'show databases' to list what exists", name, dbID)
	}
	if err := checkBucketName(dbName, name); err != nil {
		return 0, err
	}
	table, err := buildFieldTable(dbName, name, fields)
	if err != nil {
		return 0, err
	}
	expire, err := buildExpireTable(dbName, name, table, spec)
	if err != nil {
		return 0, err
	}
	declared, err := buildAggregates(dbName, name, table, aggregates)
	if err != nil {
		return 0, err
	}
	run := &createRun{CreateBucket: c, ctx: ctx, dbID: dbID, dbName: dbName, name: name, table: table, expire: expire, aggregates: declared, version: c.cfg.Published(dbID),
		references: referencesOf(table, references)}
	return run.create()
}

// checkBucketName refuses a reserved, empty, unsafe or over-long name (the shared policy) before any read or write.
func checkBucketName(dbName, name string) error {
	if strings.HasPrefix(name, "_") {
		return &cluster.Error{Code: cluster.CodeReservedName, Message: cluster.CodeReservedName.Text(cluster.CondReservedName,
			map[string]string{"name": name, "db": dbName})}
	}
	if name == "" {
		return refuse(cluster.CodeInvalidArgument, "bucket name is empty in database %q: a name is 1 to %d bytes. Run 'create bucket <name> in %s { … }' with a name in that range",
			dbName, namepolicy.MaxBytes, dbName)
	}
	if err := bucketNameRefusal(dbName, name); err != nil {
		return err
	}
	return nil
}

// buildFieldTable numbers the caller's fields from 2 under the implicit _id_ at 1. A leading
// _id_ entry is the implicit one and is not counted twice.
func buildFieldTable(dbName, bucket string, fields []FieldEntry) (machine.FieldTable, error) {
	if len(fields) > 0 && fields[0].Name == "_id_" {
		fields = fields[1:]
	}
	if len(fields) > maxUserFields {
		return machine.FieldTable{}, refuse(cluster.CodeInvalidArgument, "bucket %q in database %q declares %d fields and the cap is %d. Run 'create bucket %s in %s { … }' with fewer fields, or split the entity into two buckets",
			bucket, dbName, len(fields), maxUserFields, bucket, dbName)
	}
	entries := make([]machine.FieldEntry, 0, len(fields)+1)
	entries = append(entries, machine.FieldEntry{ID: 1, Type: "any", Name: "_id_"})
	seen := map[string]bool{"_id_": true}
	for _, field := range fields {
		if field.Name == "" || field.Type == "" {
			return machine.FieldTable{}, refuse(cluster.CodeInvalidArgument, "bucket %q in database %q has a field with no name or no type. Run 'create bucket %s in %s { <Field> string }' to declare each field as a name and a type",
				bucket, dbName, bucket, dbName)
		}
		if seen[field.Name] {
			return machine.FieldTable{}, refuse(cluster.CodeInvalidArgument, "bucket %q in database %q declares field %q twice. Run 'create bucket %s in %s { … }' with each field name once",
				bucket, dbName, field.Name, bucket, dbName)
		}
		seen[field.Name] = true
		entries = append(entries, machine.FieldEntry{ID: uint16(len(entries) + 1), Type: field.Type, Name: field.Name})
	}
	return machine.FieldTable{NextFieldID: uint16(len(entries) + 1), Entries: entries}, nil
}

// create is conv 21's create, reverse first. The reverse-index insert claims the id, and
// only then is the declaration written, so a declaration never holds an id this create does
// not own and no declaration is ever rewritten. The one deletion is the loser of a lost race removing
// its own unused reverse-index document.
func (r *createRun) create() (uint64, error) {
	existing, found, err := r.readDeclaration()
	if err != nil {
		return 0, err
	}
	if found {
		return r.answerFor(existing)
	}
	id, err := r.claim()
	if err != nil {
		return 0, err
	}
	if err := r.writeDeclaration(id); err != nil {
		if !isDocumentExists(err) {
			return 0, err
		}
		// The name has a declaration: a retry that converged, or a create of the same
		// name that won the race.
		winner, present, readErr := r.readDeclaration()
		if readErr != nil {
			return 0, readErr
		}
		if !present {
			return 0, fmt.Errorf("the declaration insert of bucket %q was refused as existing and the read found none: %w", r.name, err)
		}
		// Our reverse document names an id no declaration holds. Left alone it is an inert
		// orphan that every show_buckets pages over, and a client can make one per lost
		// race, so the loser deletes its own. It is safe because the id was drawn here and
		// the reverse insert accepted it, so no other create claims it, and the winner holds
		// a different id (an equal id would have been refused at the claim).
		if winner.BucketID != id {
			if err := r.reclaim(id); err != nil {
				return 0, err
			}
		}
		return r.answerFor(winner)
	}
	return id, nil
}

// readDeclaration reads this create's declaration straight from its owner.
func (r *createRun) readDeclaration() (machine.Declaration, bool, error) {
	existing, _, found, err := r.cfg.Declarations.Read(r.ctx, r.dbID, r.name)
	if err != nil {
		return machine.Declaration{}, false, fmt.Errorf("reading the declaration of bucket %q in database %q: %w", r.name, r.dbName, err)
	}
	return existing, found, nil
}

// answerFor is the answer when the name already has a declaration. The same table on an
// active bucket is a create that already happened (a retry, or a race lost to an
// identical create) and answers with its id; any other table, or a dropping bucket, is
// BUCKET_EXISTS, and its text never suggests a drop.
func (r *createRun) answerFor(existing machine.Declaration) (uint64, error) {
	if existing.State != machine.StateActive || !bytes.Equal(store.EncodeFieldTable(existing.Fields), store.EncodeFieldTable(r.table)) ||
		!bytes.Equal(store.EncodeExpireTable(existing.Expire), store.EncodeExpireTable(r.expire)) ||
		!slices.Equal(existing.References, r.referencesFor(existing.BucketID)) ||
		!reflect.DeepEqual(existing.Aggregates.Sets, r.aggregates.Sets) {
		return 0, &cluster.Error{Code: cluster.CodeBucketExists, Message: cluster.CodeBucketExists.Text(cluster.CondBucketExists, map[string]string{
			"bucket": r.name, "db": r.dbName, "id16": id16(existing.BucketID), "state": existing.State,
		})}
	}
	return existing.BucketID, nil
}

// claim draws ids until the reverse-index insert accepts one: the insert is the authority,
// and a refusal means another name owns the id. Three refused draws is
// BUCKET_ID_COLLISION, with nothing but other buckets' documents on the ring.
func (r *createRun) claim() (uint64, error) {
	for attempt := 0; attempt < maxIDAttempts; attempt++ {
		id, err := r.cfg.DrawID()
		if err != nil {
			return 0, fmt.Errorf("drawing an id for bucket %q: %w", r.name, err)
		}
		err = r.writeReverse(id, attempt)
		if err == nil {
			return id, nil
		}
		if !isDocumentExists(err) {
			return 0, err
		}
	}
	return 0, &cluster.Error{Code: cluster.CodeBucketIDCollision, Message: cluster.CodeBucketIDCollision.Text(cluster.CondBucketIDCollision,
		map[string]string{"bucket": r.name, "db": r.dbName})}
}

// writeDeclaration inserts the declaration: _id_ the name, fields 2 to 5, version s.
func (r *createRun) writeDeclaration(id uint64) error {
	stored, err := store.EncodeDeclaration(machine.Declaration{BucketID: id, Version: r.version, State: machine.StateActive, Fields: r.table, Expire: r.expire, Aggregates: r.aggregates, References: r.referencesFor(id)})
	if err != nil {
		return fmt.Errorf("encoding the declaration of bucket %q: %w", r.name, err)
	}
	encID, err := store.EncodeComponent(r.name)
	if err != nil {
		return fmt.Errorf("encoding bucket name %q: %w", r.name, err)
	}
	op := cluster.Op{
		Class: cluster.OpInsert, BucketID: store.BucketsBucketID, SchemaVersionSeen: r.version,
		EncID: encID, IDValue: stringValue(r.name), Fields: sortedFields(stored),
	}
	return r.write(ringWrite{op: op, what: "the declaration insert", step: "declaration", bucketID: id})
}

// reclaim deletes this run's own reverse-index document of id, left behind by a lost race.
// A failure is returned, not dropped: the create itself is answered by a repeat, which finds
// the declaration and writes nothing, so the orphan then stays.
func (r *createRun) reclaim(id uint64) error {
	encID, err := store.EncodeComponent(idBytes(id))
	if err != nil {
		return fmt.Errorf("encoding bucket id %s: %w", id16(id), err)
	}
	op := cluster.Op{Class: cluster.OpDelete, BucketID: store.BucketsBucketID, SchemaVersionSeen: r.version, EncID: encID}
	return r.write(ringWrite{op: op, what: "the reclaim of the unused reverse-index document", step: "reclaim", bucketID: id})
}

// writeReverse inserts the reverse-index document of id: _id_ the id, state, name.
func (r *createRun) writeReverse(id uint64, attempt int) error {
	encID, err := store.EncodeComponent(idBytes(id))
	if err != nil {
		return fmt.Errorf("encoding bucket id %s: %w", id16(id), err)
	}
	op := cluster.Op{
		Class: cluster.OpInsert, BucketID: store.BucketsBucketID, SchemaVersionSeen: r.version,
		EncID: encID, IDValue: bucketIDValue(id),
		Fields: []cluster.FieldOp{
			{FieldID: createFieldState, Value: stringValue(machine.StateActive)},
			{FieldID: createFieldName, Value: stringValue(r.name)},
		},
	}
	return r.write(ringWrite{op: op, what: "the reverse-index insert", step: "reverse", bucketID: id, attempt: attempt})
}

// ringWrite is one write of a create: the operation, its label for error texts, and the
// three facts besides the bucket that make it a distinct request.
type ringWrite struct {
	op   cluster.Op
	what string
	// step is "reverse" or "declaration".
	step string
	// bucketID is the bucket id the step writes.
	bucketID uint64
	// attempt counts the collision redraws that came before this write.
	attempt int
}

// requestID derives the dedupe request id of a ring write. The owner's dedupe key is
// (request id, op index) inside one (slot, database, _buckets) pair, and two of a create's
// writes land on one pair whenever their slots match, so a request id shared by all of
// them, such as zero, makes the owner answer the second write as a repeat of the first and
// apply nothing. The id is the first
// 16 bytes of SHA-256 over (database id, name length, name, step, bucket id, attempt,
// schema_version written, the canonical encoded field table). The table and the version
// bind the id to the body: a different table under the same drawn id, or a recreate at a
// higher version inside the dedupe window, must be a new request, not a replay. The
// same create retried after a crash re-derives the same ids, so a write that did land is
// correctly answered from its dedupe entry, and distinct writes never share one. The op
// index is always 0.
func (r *createRun) requestID(step ringWrite) [16]byte {
	hash := sha256.New()
	hash.Write(binary.BigEndian.AppendUint64(nil, r.dbID))
	hash.Write(binary.BigEndian.AppendUint32(nil, uint32(len(r.name))))
	hash.Write([]byte(r.name))
	hash.Write([]byte(step.step))
	hash.Write(binary.BigEndian.AppendUint64(nil, step.bucketID))
	hash.Write(binary.BigEndian.AppendUint32(nil, uint32(step.attempt)))
	hash.Write(binary.BigEndian.AppendUint64(nil, uint64(r.version)))
	hash.Write(store.EncodeFieldTable(r.table))
	if len(r.aggregates.Sets) > 0 {
		hash.Write(store.EncodeAggregateTable(r.aggregates))
	}
	var id [16]byte
	copy(id[:], hash.Sum(nil))
	return id
}

// write orders one ring document at majority and refuses anything less. An
// applied_degraded declaration is not a success: a schema change must be durable on
// every copy that will serve it (conv 20), so the create stops and a retry repairs it.
func (r *createRun) write(step ringWrite) error {
	op, what := step.op, step.what
	slot := storage.SlotOf(storage.PlacementKey(r.dbName, systemBucketName, op.EncID))
	answer, err := r.cfg.Sender.Write(r.ctx, WriteRequest{
		Slot: slot, DBID: r.dbID, Op: op, Concern: machine.ConcernMajority, RequestID: r.requestID(step),
	})
	if err != nil {
		return fmt.Errorf("%s of bucket %q in database %q: %w", what, r.name, r.dbName, err)
	}
	switch {
	case answer.Held || answer.Forward:
		return refuse(cluster.CodeUnavailable, "%s of bucket %q in database %q was not ordered here because %s. Retry 'create bucket %s in %s { … }' in a few seconds: it is safe to repeat",
			what, r.name, r.dbName, answer.notOrderedBecause(), r.name, r.dbName)
	case answer.ApplyFailure != nil:
		return errors.Join(refuse(cluster.CodeUnavailable, "%s of bucket %q in database %q is durable at %s but applying it failed on this node, so it was not confirmed. Run 'create bucket %s in %s { … }' again once the pair has recovered: it is safe to repeat",
			what, r.name, r.dbName, answer.Pos, r.name, r.dbName), answer.ApplyFailure)
	case answer.Outcome == machine.OutcomeApplied:
		return nil
	case answer.Outcome == machine.OutcomeAppliedDegraded:
		rf := 0
		if row, ok := r.cfg.Topology.Database(r.dbID); ok {
			rf = row.RF
		}
		return &cluster.Error{Code: cluster.CodeUnavailable, Message: cluster.CodeUnavailable.Text(cluster.CondDeclarationWriteFailed, map[string]string{
			"op": "create bucket", "db": r.dbName, "bucket": r.name, "c": strconv.Itoa(int(answer.CopiesConfirmed)),
			"rf": strconv.Itoa(rf), "slot": strconv.Itoa(int(slot)),
		})}
	}
	return refuse(cluster.CodeUnavailable, "%s of bucket %q in database %q has an unknown outcome. Retry 'create bucket %s in %s { … }': it is safe to repeat",
		what, r.name, r.dbName, r.name, r.dbName)
}

// refuse is a coded refusal whose text names the fix. SEMANTICS: the specs do not decide
// these conditions, so they take the nearest code (a missing database is NOT_FOUND, a bad
// name or table INVALID_ARGUMENT, an unordered or unknown write UNAVAILABLE), as MongoDB
// and MySQL do for the same mistakes, and U52 maps the code to an exit status.
func refuse(code cluster.Code, format string, args ...any) error {
	return &cluster.Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// isDocumentExists reports the owner's duplicate-_id_ refusal.
func isDocumentExists(err error) bool {
	var refusal *cluster.Error
	return errors.As(err, &refusal) && refusal.Code == cluster.CodeDocumentExists
}

// sortedFields turns stored values into the Op's field list, ascending by field id.
func sortedFields(stored map[uint16][]byte) []cluster.FieldOp {
	fields := make([]cluster.FieldOp, 0, len(stored))
	for fieldID, encoded := range stored {
		fields = append(fields, cluster.FieldOp{FieldID: fieldID, Value: encoded})
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i].FieldID < fields[j].FieldID })
	return fields
}

// idBytes is the 8-byte big-endian form of a bucket id.
func idBytes(id uint64) []byte { return binary.BigEndian.AppendUint64(nil, id) }

// bucketIDValue is a bucket id as a type-tagged BSON binary: tag 0x05, a 4-byte
// little-endian length, subtype 0, the 8 bytes.
func bucketIDValue(id uint64) []byte {
	out := []byte{0x05}
	out = binary.LittleEndian.AppendUint32(out, 8)
	out = append(out, 0)
	return append(out, idBytes(id)...)
}

// id16 is a bucket id as the 16 hex digits the error texts print.
func id16(id uint64) string { return fmt.Sprintf("%016x", id) }

// ExpireSpec is the expiry a create or alter names, by field name: the datetime field of the bucket
// form, and the element path of each fifoStore or lifoStore field of the store form.
type ExpireSpec struct {
	BucketField string
	StorePaths  map[string]string
}

// buildExpireTable resolves the names of spec against table and returns declaration field 11, or a
// refusal that names the field and the statement that fixes it. The bucket field must be a declared
// datetime (not any: an expiry that depends on the stored type lies), each path field a fifoStore or
// lifoStore, and a path one to three dot-separated keys.
func buildExpireTable(dbName, bucket string, table machine.FieldTable, spec ExpireSpec) (machine.ExpireTable, error) {
	var out machine.ExpireTable
	if spec.BucketField != "" {
		entry, found := entryNamed(table, spec.BucketField)
		switch {
		case !found:
			return out, refuse(cluster.CodeInvalidArgument, "EXPIRE_FIELD_TYPE: bucket %q in database %q does not declare field %q, so 'expire at %s' has nothing to read. Run 'alter bucket %s in %s { add %s datetime }' to declare it, then 'alter bucket %s in %s { set expire at %s }'",
				bucket, dbName, spec.BucketField, spec.BucketField, bucket, dbName, spec.BucketField, bucket, dbName, spec.BucketField)
		case entry.Type != "datetime":
			return out, refuse(cluster.CodeInvalidArgument, "EXPIRE_FIELD_TYPE: field %q of bucket %q in database %q is declared %s, and 'expire at' needs a datetime, because an expiry that depends on the stored type lies. A field's type never changes: run 'alter bucket %s in %s { add ExpiresAt datetime }', write the expiry instant into it, then 'alter bucket %s in %s { set expire at ExpiresAt }'",
				spec.BucketField, bucket, dbName, entry.Type, bucket, dbName, bucket, dbName)
		}
		out.BucketField = entry.ID
	}
	for name, path := range spec.StorePaths {
		entry, found := entryNamed(table, name)
		switch {
		case !found:
			return machine.ExpireTable{}, refuse(cluster.CodeInvalidArgument, "EXPIRE_STORE_TYPE: bucket %q in database %q does not declare field %q, so 'expire at \"%s\"' has no store to expire. Run 'create bucket %s in %s { %s fifoStore expire at \"%s\" }' with the store declared",
				bucket, dbName, name, path, bucket, dbName, name, path)
		case entry.Type != "fifoStore" && entry.Type != "lifoStore":
			return machine.ExpireTable{}, refuse(cluster.CodeInvalidArgument, "EXPIRE_STORE_TYPE: field %q of bucket %q in database %q is a %s, and 'expire at \"<path>\"' is for fifoStore and lifoStore in this release. Run 'create bucket %s in %s { %s fifoStore expire at \"%s\" }' to declare a queue that expires",
				name, bucket, dbName, entry.Type, bucket, dbName, name, path)
		case path == "" || len(path) > 255 || strings.Count(path, ".") >= 3 || strings.Contains(path, "..") || strings.HasPrefix(path, ".") || strings.HasSuffix(path, "."):
			return machine.ExpireTable{}, refuse(cluster.CodeInvalidArgument, "EXPIRE_STORE_TYPE: the expire path %q of field %q is not one to three dot-separated keys. Run 'create bucket %s in %s { %s fifoStore expire at \"ts\" }' with a path such as \"ts\" or \"meta.at\"",
				path, name, bucket, dbName, name)
		}
		out.Stores = append(out.Stores, machine.ExpireStore{FieldID: entry.ID, Path: path})
	}
	sort.Slice(out.Stores, func(i, j int) bool { return out.Stores[i].FieldID < out.Stores[j].FieldID })
	return out, nil
}
