package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// restoreGroupEntities is the most entities one group of inserts holds: the operations of
// one frame (section 3.4 step 3).
const restoreGroupEntities = restoreMaxFrameOperations

// restoreOrphanNames is how many ids of documents left without their stores a failed
// restore names.
const restoreOrphanNames = 10

// restoreRefusedError is an operation the ring refused. Any refused operation stops a
// restore: a skipped push would break the order of a queue.
type restoreRefusedError struct {
	op    restoreOp
	cause error
	// resent says the frame had been sent more than once, so an insert refused as a
	// duplicate may be this restore's own earlier send, whose reply was lost.
	resent bool
}

func (e *restoreRefusedError) Error() string {
	var refusal *ringclient.Error
	if errors.As(e.cause, &refusal) {
		text := fmt.Sprintf("%s was refused: %s: %s", e.op.describe(), refusal.Code, terminalSafeText(refusal.Message))
		if e.resent && e.op.kind == restoreInsert && refusal.Code == cluster.CodeDocumentExists {
			text += " This frame was sent more than once after a lost reply, so the document may have been created by this restore's own first send and not by another writer: read it before you decide."
		}
		return text
	}
	return fmt.Sprintf("%s was refused: %s", e.op.describe(), terminalSafeText(strings.TrimPrefix(e.cause.Error(), "ringclient: ")))
}

func (e *restoreRefusedError) Unwrap() error { return e.cause }

// restoreDamageError is a dump that does not hold together the way pass 1 said it did: a
// store chunk that does not follow its document, or a file that changed while it was being
// restored. It is the input's fault, so it exits as unreadable input.
type restoreDamageError struct{ message string }

func (e *restoreDamageError) Error() string { return e.message }

// restoreEntity is one entity of a group: its insert, and the store operations of its
// elems records, held until its insert has been answered.
type restoreEntity struct {
	insert restoreOp
	stores []restoreOp
}

// restoreGroup is the entities waiting for their inserts: at most restoreGroupEntities and,
// past the first, at most restoreFrameByteCap bytes, so a restore holds a frame and not a
// bucket (section 3.4 step 3).
type restoreGroup struct {
	entities []*restoreEntity
	bytes    int
}

// restoreTally is what a restore has done so far, for the progress lines, the summary and
// the report of a restore that stopped.
type restoreTally struct {
	databases, buckets, entities, elements int64
	blobFields, aggregates                 int64
	// the current bucket's own counters, printed when it is complete.
	bucketEntities, bucketElements int64
	// orphans are entities whose insert applied in a group that stopped before their stores
	// were sent: documents without their elements.
	orphans     int64
	orphanNames []string
}

// restoreApplier is pass 3: it streams the dump in file order and writes it to the target.
type restoreApplier struct {
	run    *ringRestoreRun
	sender *restoreSender
	reader *dumpReader
	// recordStart is where the record being applied starts in the file: a failure is raised
	// while the record that closes a bucket or a database is applied, so the databases not
	// started are the ones from this position on, this record included.
	recordStart int64

	// selected is true while the records belong to a database this run restores.
	selected bool
	// database is the target name of the current database; bucket is the current bucket's
	// record, nil until its record is read.
	database string
	bucket   *dumpBucket
	group    restoreGroup
	// last is the entity the next elems record must belong to; streaming says its insert
	// and its buffered operations are already sent, so its next chunks go out as they come.
	last      *restoreEntity
	streaming bool

	// groupCreated holds the databases created with the group, and groupNames their target names.
	groupCreated map[string]bool
	groupNames   []string
	// groupBuckets is how many linked buckets the group made in each of its databases.
	groupBuckets map[string]int64
	// created holds the linked buckets created ahead of their records and not reached yet.
	created map[linkBucketKey]bool

	tally    restoreTally
	progress restoreProgress

	// footerMatched is set only when the end of the file matched pass 1's checksum: until
	// then a database the restore finished is not known to be complete, because the file
	// could have changed under it. footerMismatch says it did not match.
	footerMatched, footerMismatch bool
	// torn is set when a store frame failed or went unconfirmed: the documents of the last
	// group may hold some of their elements and not others.
	torn bool
}

// newRestoreApplier prepares pass 3 over the already verified file.
func newRestoreApplier(run *ringRestoreRun, sender *restoreSender) *restoreApplier {
	return &restoreApplier{run: run, sender: sender}
}

// apply reads the file once more, in order, and writes every record the run selects. It
// returns nil when the footer was reached and the file's checksum still matches.
func (a *restoreApplier) apply() error {
	file, err := os.Open(a.run.flags.file)
	if err != nil {
		return fmt.Errorf("opening the dump %s: %w", a.run.flags.file, err)
	}
	defer func() { _ = file.Close() }()
	if err := a.run.checkOpenedSource(file); err != nil {
		return err
	}
	a.reader = newDumpReader(file)
	if err := a.createGroup(); err != nil {
		return err
	}

	var coveredSum [32]byte
	var coveredBytes, recordsBeforeSummary int64
	for {
		if err := a.run.ctx.Err(); err != nil {
			return fmt.Errorf("stopped: %w", err)
		}
		a.recordStart = a.reader.Offset()
		kind, raw, err := a.reader.Next()
		if errors.Is(err, io.EOF) {
			return &restoreDamageError{message: "the dump ended without its footer: it changed after it was verified. Run the restore again on a new target name"}
		}
		if err != nil {
			return &restoreDamageError{message: fmt.Sprintf("the dump changed after it was verified: %v", err)}
		}
		record, err := decodeRecord(raw)
		if err != nil {
			return &restoreDamageError{message: fmt.Sprintf("the dump changed after it was verified: %v", err)}
		}
		if kind != dumpKindSummary && kind != dumpKindFooter {
			recordsBeforeSummary++
		}
		switch record := record.(type) {
		case *dumpHeader:
		case *dumpDB:
			err = a.applyDatabase(record)
		case *dumpBucket:
			err = a.applyBucket(record)
		case *dumpDoc:
			err = a.applyDoc(record)
		case *dumpElems:
			err = a.applyElems(record)
		case *dumpSummaryRecord:
			if err = a.finishDatabase(); err == nil {
				coveredSum, coveredBytes = a.reader.Sum(), a.reader.Offset()
			}
		case *dumpFooter:
			// The file must still be the one pass 1 verified, not only self-consistent: a
			// different valid dump swapped in would match its own footer.
			if !bytes.Equal(coveredSum[:], record.SHA256) || coveredBytes != record.Bytes ||
				coveredSum != a.run.verification.SHA256 || recordsBeforeSummary != a.run.verification.Records {
				a.footerMismatch = true
				return &restoreDamageError{message: "the dump changed while it was being restored: what was read no longer matches the checksum pass 1 verified, so nothing written from it is known to be right"}
			}
			a.footerMatched = true
			return nil
		default:
			return &restoreDamageError{message: fmt.Sprintf("the dump holds a %q record this restore does not apply", kind)}
		}
		if err != nil {
			return err
		}
	}
}

// applyDatabase closes the database before it and, when this run restores the new one,
// creates it. Creating an equal existing empty database is a no-op, which is the empty
// target pass 2 accepted.
func (a *restoreApplier) applyDatabase(record *dumpDB) error {
	if err := a.finishDatabase(); err != nil {
		return err
	}
	if !a.run.flags.selects(record.Name) {
		a.selected = false
		return nil
	}
	a.selected = true
	a.database = a.run.flags.restoreTargetName(record.Name)
	a.progress.attemptDatabase(record.Name, a.database)
	if a.groupCreated[record.Name] {
		// Created with the group before the first record, with its linked buckets. A writer has
		// had the length of the group's creation to add a bucket of another name, so the
		// database is read again before anything is filled.
		if err := a.run.checkGroupDatabaseUntouched(record, a.groupBuckets[record.Name]); err != nil {
			return err
		}
		a.progress.beginDatabase()
		return nil
	}
	if err := a.run.checkDatabaseStillEmpty(a.run.flags, record); err != nil {
		return err
	}
	if err := a.run.createDatabase(record, a.database); err != nil {
		return err
	}
	a.progress.beginDatabase()
	return a.createLinkedBuckets(record.Name)
}

// createGroup creates, before the first record is applied, the databases that link into each
// other across databases and then every linked bucket of theirs in one order across them,
// targets first. No order of whole databases satisfies such links, so nothing of them is
// filled until every declaration exists; the file is then applied in its own order.
func (a *restoreApplier) createGroup() error {
	plan := a.run.links
	if plan == nil || len(plan.groupDatabases) == 0 {
		return nil
	}
	a.groupCreated = map[string]bool{}
	a.groupBuckets = map[string]int64{}
	for _, record := range plan.groupOrder {
		a.groupBuckets[record.DB]++
	}
	for _, record := range plan.groupDatabases {
		target := a.run.flags.restoreTargetName(record.Name)
		if err := a.run.checkDatabaseStillEmpty(a.run.flags, record); err != nil {
			return err
		}
		a.progress.attemptDatabase(record.Name, target)
		if err := a.run.createDatabase(record, target); err != nil {
			return err
		}
		a.groupCreated[record.Name] = true
		a.groupNames = append(a.groupNames, target)
	}
	for _, record := range plan.groupOrder {
		if err := a.createAheadBucket(record); err != nil {
			return err
		}
	}
	a.progress.attemptDatabase("", "")
	return nil
}

// createLinkedBuckets creates, right after its database, the buckets of the database that
// take part in a link, targets first: a field 'of' another bucket needs that bucket to exist
// when it is created, and the file lists buckets by name, not in the order links need. They
// are created empty and filled when the file reaches their records, so the rest of the file
// is restored as before. A restore that stops leaves these buckets created and the report
// says which (section 3.5).
func (a *restoreApplier) createLinkedBuckets(dumpDatabase string) error {
	for _, record := range a.run.links.linked(dumpDatabase) {
		if err := a.createAheadBucket(record); err != nil {
			return err
		}
	}
	return nil
}

// createAheadBucket creates one linked bucket empty, in the target database its record names,
// and notes it so the fill finds it already made.
func (a *restoreApplier) createAheadBucket(record *dumpBucket) error {
	database := a.run.flags.restoreTargetName(record.DB)
	if err := a.createBucketOnce(record, database); err != nil {
		return err
	}
	if a.created == nil {
		a.created = map[linkBucketKey]bool{}
	}
	a.created[linkBucketKey{db: record.DB, name: record.Name}] = true
	a.progress.createdAhead(database, record.Name)
	return nil
}

// createBucketOnce checks that the bucket is absent from the target and creates it.
func (a *restoreApplier) createBucketOnce(record *dumpBucket, database string) error {
	if err := a.run.checkBucketAbsent(a.run.flags, record.DB, record.Name, database); err != nil {
		return err
	}
	if restoreBeforeBucketCreate != nil {
		restoreBeforeBucketCreate()
	}
	return a.run.createBucket(record, database)
}

// applyBucket closes the bucket before it and creates this one, only now, just before its
// entities: a bucket is created, filled completely and only then is the next one started.
func (a *restoreApplier) applyBucket(record *dumpBucket) error {
	if err := a.finishBucket(); err != nil {
		return err
	}
	if !a.run.flags.selects(record.DB) {
		return nil
	}
	if !a.selected {
		return &restoreDamageError{message: fmt.Sprintf("bucket %q appears in the dump before the record of its database %q", terminalSafeText(record.Name), terminalSafeText(record.DB))}
	}
	key := linkBucketKey{db: record.DB, name: record.Name}
	if a.created[key] {
		delete(a.created, key)
		// Written to by anyone since it was made, for as long as the file took to reach it:
		// merging into it would break the rule that restore never merges.
		if err := a.run.checkBucketStillEmpty(a.run.flags, record.DB, record.Name, a.database); err != nil {
			return err
		}
	} else if err := a.createBucketOnce(record, a.database); err != nil {
		return err
	}
	a.bucket = record
	a.progress.beginBucket(a.database, record.Name)
	for _, line := range restoreBlobFieldLines(record, a.database) {
		_, _ = fmt.Fprintln(a.run.stderr, line)
	}
	a.tally.blobFields += int64(len(record.Unsupported))
	a.tally.aggregates += int64(len(record.Aggregates))
	return nil
}

// applyDoc adds an entity's insert to the group, sending the group first when it is full.
func (a *restoreApplier) applyDoc(record *dumpDoc) error {
	if !a.run.flags.selects(record.DB) {
		return nil
	}
	if a.bucket == nil || record.DB != a.bucket.DB || record.B != a.bucket.Name {
		return &restoreDamageError{message: fmt.Sprintf("document %q of %s.%s appears in the dump outside its bucket's records", terminalSafeText(record.ID), terminalSafeText(record.DB), terminalSafeText(record.B))}
	}
	fields, err := restoreDocFields(record)
	if err != nil {
		return &restoreDamageError{message: fmt.Sprintf("the fields of document %q of %s.%s cannot be read: %v", terminalSafeText(record.ID), terminalSafeText(record.DB), terminalSafeText(record.B), err)}
	}
	entity := &restoreEntity{insert: restoreOp{
		kind: restoreInsert, database: a.database, bucket: record.B, id: record.ID, fields: fields,
		size: len(record.F) + len(record.ID) + restoreOperationOverhead,
	}}
	a.streaming = false
	if len(a.group.entities) > 0 && (len(a.group.entities) >= restoreGroupEntities || a.group.bytes+entity.insert.size > restoreFrameByteCap) {
		if err := a.flushGroup(); err != nil {
			return err
		}
	}
	a.group.entities = append(a.group.entities, entity)
	a.group.bytes += entity.insert.size
	a.last = entity
	return nil
}

// restoreDocFields copies a doc record's scalar members into the Doc an insert carries. The
// values are copied because the reader's buffer is reused by the next read and a group is
// held across reads.
func restoreDocFields(record *dumpDoc) (ringclient.Doc, error) {
	members, err := record.F.Elements()
	if err != nil {
		return nil, err
	}
	fields := make(ringclient.Doc, len(members))
	for _, member := range members {
		value := member.Value()
		fields[member.Key()] = bson.RawValue{Type: value.Type, Value: bytes.Clone(value.Value)}
	}
	return fields, nil
}

// applyElems adds one chunk of one store of the last entity: held with its entity while the
// group fits a frame, and sent as it comes once the group has been sent.
func (a *restoreApplier) applyElems(record *dumpElems) error {
	if !a.run.flags.selects(record.DB) {
		return nil
	}
	if a.last == nil || a.bucket == nil || record.DB != a.bucket.DB || record.B != a.bucket.Name || record.ID != a.last.insert.id {
		return &restoreDamageError{message: fmt.Sprintf("elements of field %q of %s.%s id %q appear in the dump without the document they belong to", terminalSafeText(record.Field), terminalSafeText(record.DB), terminalSafeText(record.B), terminalSafeText(record.ID))}
	}
	ops, err := restoreElemOps(record, a.database)
	if err != nil {
		return &restoreDamageError{message: err.Error()}
	}
	var chunkBytes int
	for _, op := range ops {
		chunkBytes += op.size
	}
	if a.streaming {
		return a.sendStoreOps(ops)
	}
	if a.group.bytes+chunkBytes > restoreFrameByteCap {
		// The group plus this chunk would not fit a frame: send the group now, with the
		// entity's own insert answered before its first element, and send what follows as it
		// comes. The one entity that holds more than a frame is therefore never held whole.
		a.last.stores = append(a.last.stores, ops...)
		if err := a.flushGroup(); err != nil {
			return err
		}
		a.streaming = true
		return nil
	}
	a.last.stores = append(a.last.stores, ops...)
	a.group.bytes += chunkBytes
	return nil
}

// restoreElemOps turns one elems record into its store operations, in the record's order,
// with the bytes copied out of the reader's buffer. The kind picks the write: a queue
// pushes, a keyed store puts under the record's keys, a heap pushes under its entry ids and
// priorities.
func restoreElemOps(record *dumpElems, database string) ([]restoreOp, error) {
	switch record.Kind {
	case dumpFifoStoreWord, dumpLifoStoreWord:
	case dumpHashStoreWord, dumpBTreeStoreWord:
		if len(record.Keys) != len(record.Els) {
			return nil, fmt.Errorf("a %s chunk of field %q of id %q holds %d elements and %d keys", record.Kind, terminalSafeText(record.Field), terminalSafeText(record.ID), len(record.Els), len(record.Keys))
		}
	case dumpHeapStoreWord:
		if len(record.Keys) != len(record.Els) || len(record.Prios) != len(record.Els) {
			return nil, fmt.Errorf("a heapStore chunk of field %q of id %q holds %d elements, %d entry ids and %d priorities", terminalSafeText(record.Field), terminalSafeText(record.ID), len(record.Els), len(record.Keys), len(record.Prios))
		}
	default:
		return nil, fmt.Errorf("an elements chunk of field %q of id %q names the store kind %q, which restore does not write", terminalSafeText(record.Field), terminalSafeText(record.ID), record.Kind)
	}
	ops := make([]restoreOp, len(record.Els))
	for index, element := range record.Els {
		op := restoreOp{
			database: database, bucket: record.B, id: record.ID, field: record.Field,
			element: bytes.Clone(element), size: len(element) + restoreOperationOverhead,
		}
		switch record.Kind {
		case dumpFifoStoreWord, dumpLifoStoreWord:
			op.kind = restorePush
		case dumpHashStoreWord, dumpBTreeStoreWord:
			op.kind, op.key = restorePut, record.Keys[index]
		case dumpHeapStoreWord:
			op.kind, op.key, op.priority = restorePushHeap, record.Keys[index], record.Prios[index]
		}
		ops[index] = op
	}
	return ops, nil
}

// flushGroup sends the group in the two steps of section 3.4 step 3, always in this order:
// the inserts of every entity in one frame, then, only when every insert applied, the store
// operations of the entities in file order, one frame at a time with each answered before the
// next is sent. A refused insert stops the restore before any store operation goes out, for
// any entity of the group, so no push can land in a document this restore did not create.
func (a *restoreApplier) flushGroup() error {
	entities := a.group.entities
	if len(entities) == 0 {
		return nil
	}
	inserts := make([]restoreOp, len(entities))
	for index, entity := range entities {
		inserts[index] = entity.insert
	}
	answers, err := a.sender.send(a.run.ctx, inserts)
	if err != nil {
		return err
	}
	for index, answer := range answers {
		if answer.Err != nil {
			a.noteOrphans(entities, answers)
			return &restoreRefusedError{op: inserts[index], cause: answer.Err, resent: a.sender.lastResent}
		}
	}
	a.tally.entities += int64(len(entities))
	a.tally.bucketEntities += int64(len(entities))
	var stores []restoreOp
	for _, entity := range entities {
		stores = append(stores, entity.stores...)
	}
	a.group = restoreGroup{}
	return a.sendStoreOps(stores)
}

// noteOrphans records the entities of a refused group whose insert did apply: they exist as
// documents without their stores, and the report names them.
func (a *restoreApplier) noteOrphans(entities []*restoreEntity, answers []ringDocAnswer) {
	for index, answer := range answers {
		if answer.Err != nil {
			continue
		}
		a.tally.orphans++
		if len(a.tally.orphanNames) < restoreOrphanNames {
			a.tally.orphanNames = append(a.tally.orphanNames, entities[index].insert.id)
		}
	}
}

// sendStoreOps sends store operations in frames of at most restoreMaxFrameOperations and, past
// the first operation, restoreFrameByteCap bytes, one frame at a time: a frame is answered
// before the next is sent, so the order of one queue's pushes is the order of the file.
func (a *restoreApplier) sendStoreOps(ops []restoreOp) error {
	for len(ops) > 0 {
		count, frameBytes := 0, 0
		for count < len(ops) && count < restoreMaxFrameOperations && (count == 0 || frameBytes+ops[count].size <= restoreFrameByteCap) {
			frameBytes += ops[count].size
			count++
		}
		frame := ops[:count]
		answers, err := a.sender.send(a.run.ctx, frame)
		if err != nil {
			a.torn = true
			return err
		}
		for index, answer := range answers {
			if answer.Err != nil {
				a.torn = true
				return &restoreRefusedError{op: frame[index], cause: answer.Err}
			}
		}
		a.tally.elements += int64(len(frame))
		a.tally.bucketElements += int64(len(frame))
		ops = ops[count:]
	}
	return nil
}

// finishBucket sends what the bucket still holds, then prints its progress line and counts
// it complete.
func (a *restoreApplier) finishBucket() error {
	if err := a.flushGroup(); err != nil {
		return err
	}
	a.last, a.streaming = nil, false
	if a.bucket == nil {
		return nil
	}
	_, _ = fmt.Fprintf(a.run.stderr, "Restored %s.%s: %s entities, %s elements\n",
		terminalSafeText(a.database), terminalSafeText(a.bucket.Name), formatCount(a.tally.bucketEntities), formatCount(a.tally.bucketElements))
	a.progress.completeBucket()
	a.tally.buckets++
	a.tally.bucketEntities, a.tally.bucketElements = 0, 0
	a.bucket = nil
	return nil
}

// finishDatabase closes the current bucket and counts the database complete.
func (a *restoreApplier) finishDatabase() error {
	if err := a.finishBucket(); err != nil {
		return err
	}
	if a.selected {
		a.tally.databases++
		a.progress.completeDatabase()
		a.selected = false
	}
	return nil
}
