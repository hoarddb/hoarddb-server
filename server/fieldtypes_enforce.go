package server

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Enforcement of a bucket's typed-field declaration on the writes that store
// whole documents: insert, put, push and batch insert (typed-fields spec
// §5.2). The check runs in the apply, under the bucket's lock, on the owner,
// on every replica and in replay, after now() was fixed at the door or before
// the log and after _id_ and the E-18 name scan, before the engine and the
// unique index inside it (§5.1, §5.8). The only lock it takes is warn's
// aggregate, a leaf.

// The recorded modes besides strict, which is recorded as "" (canonicalMode).
const (
	fieldTypesWarn = "warn"
	fieldTypesOff  = "off"
)

// refuseMistyped is the check of one document against holder, the bucket's
// declaration as declaredFieldTypes loaded it. A bucket that declares nothing
// pays that load and this nil test, which inlines into the handler (spec
// §9.1).
func (s *Server) refuseMistyped(ctx context.Context, holder *bucketFieldTypes, bucket *inMemoryBucket, database, name string, document map[string]interface{}) error {
	if holder == nil {
		return nil
	}
	return s.enforceDeclaration(ctx, holder, bucket, database, name, []map[string]interface{}{document})
}

// refuseMistypedBatch is the check of a batch, document by document, before
// the engine's atomic batch: the first mismatch refuses the whole batch and
// says where in it the document is (spec §5.2).
func (s *Server) refuseMistypedBatch(ctx context.Context, holder *bucketFieldTypes, bucket *inMemoryBucket, database, name string, documents []map[string]interface{}) error {
	if holder == nil {
		return nil
	}
	err := s.enforceDeclaration(ctx, holder, bucket, database, name, documents)
	var mistyped *fieldTypeRefusal
	if errors.As(err, &mistyped) {
		return inBatch(err, mistyped.position, mistyped.key)
	}
	return err
}

// enforceDeclaration applies the bucket's mode to documents, one write: off
// checks nothing; strict refuses the first mismatch; warn stores the write
// and counts it once, however many of its documents mismatch. Every write is
// refused while the declaration cannot be enforced as recorded.
func (s *Server) enforceDeclaration(ctx context.Context, holder *bucketFieldTypes, bucket *inMemoryBucket, database, name string, documents []map[string]interface{}) error {
	if unusable := holder.unusable; unusable != nil {
		return &declarationUnusableError{database: database, bucket: name, invalid: unusable.invalid, newer: unusable.recorded != nil}
	}
	mode := holder.schema.mode
	if mode == fieldTypesOff {
		return nil
	}
	for position, document := range documents {
		mismatch := holder.schema.check(document)
		if mismatch == nil {
			continue
		}
		if mode == fieldTypesWarn {
			s.countWarned(ctx, database, name)
			return nil
		}
		if answersClient(ctx) {
			fieldTypeRefusals.Inc()
		}
		key, _ := document["_id_"].(string)
		empty := bucketIsEmpty(bucket)
		return &fieldTypeRefusal{mismatch: mismatch, database: database, bucket: name, empty: empty, position: position + 1, key: key,
			conflicts: mismatch.batchConflicts(documents, position, empty)}
	}
	return nil
}

// answersClient reports whether ctx is the apply whose answer reaches the
// client, the one apply of a write the counters count (spec §9.5, SA-5): the
// owner's live apply, which records its store failures, or a standalone
// node's, which carries no cluster marker (build doc §9 E-2); never a
// replica's apply of a shipped entry and never replay.
func answersClient(ctx context.Context) bool {
	return !isReplay(ctx) && (recordsFailures(ctx) || !clusterApplied(ctx))
}

// clusterApplied reports whether ctx is an apply of an entry the cluster's
// write path stamped or shipped (generation.go's clusterApplyCtxKey).
func clusterApplied(ctx context.Context) bool {
	applied, _ := ctx.Value(clusterApplyCtxKey{}).(bool)
	return applied
}

// countWarned counts one write warn stored with a mismatch, and adds it to
// the hourly line, on the apply that answers the client.
func (s *Server) countWarned(ctx context.Context, database, name string) {
	if !answersClient(ctx) {
		return
	}
	fieldTypeMismatchesAccepted.Inc()
	if line := s.fieldTypeWarnings.note(database+"."+name, fieldTypesNow()); line != "" {
		s.logger.Warn(line)
	}
}

// flushFieldTypeWarnings logs what warn counted since its last line: at
// shutdown, so no count is lost to a stop.
func (s *Server) flushFieldTypeWarnings() {
	if line := s.fieldTypeWarnings.flush(); line != "" {
		s.logger.Warn(line)
	}
}

// fieldTypesNow is warn's clock, a seam a test moves across the hour.
var fieldTypesNow = time.Now

// The hourly line's bounds: it names at most five buckets, and counts
// distinct buckets up to a thousand, so an hour of mismatches across a
// million warn buckets holds a thousand names, not a million (rule 11).
const (
	warnLineNames       = 5
	warnDistinctBuckets = 1000
)

// fieldTypeWarnings is warn's hourly aggregate (spec §9.5, D-14): one line an
// hour at most, with no goroutine. The first counted mismatch after the hour
// boundary logs the hour before it, and a shutdown logs the rest. Its lock is
// a leaf, taken under a bucket's lock.
type fieldTypeWarnings struct {
	mu          sync.Mutex
	windowStart time.Time
	writes      int64
	// warnedBuckets are the distinct "database.bucket" names counted this
	// hour, up to warnDistinctBuckets; named are the first warnLineNames of
	// them, in the order they were first counted. (Not "buckets": that name is
	// the bucket table's, which TestNoCodeReachesIntoTheBucketMap guards.)
	warnedBuckets map[string]struct{}
	named         []string
}

// note counts one write to bucket at now, and returns the line for the hour
// before it when now is past that hour's end, or "".
func (w *fieldTypeWarnings) note(bucket string, now time.Time) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	var line string
	if w.windowStart.IsZero() {
		w.windowStart = now
	} else if now.Sub(w.windowStart) >= time.Hour {
		line = w.lineLocked()
		w.resetLocked(now)
	}
	w.writes++
	if _, counted := w.warnedBuckets[bucket]; counted || len(w.warnedBuckets) >= warnDistinctBuckets {
		return line
	}
	if w.warnedBuckets == nil {
		w.warnedBuckets = make(map[string]struct{})
	}
	w.warnedBuckets[bucket] = struct{}{}
	if len(w.named) < warnLineNames {
		w.named = append(w.named, bucket)
	}
	return line
}

// flush returns the line for what was counted since the last one, or "", and
// starts again.
func (w *fieldTypeWarnings) flush() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	line := w.lineLocked()
	w.resetLocked(time.Time{})
	return line
}

func (w *fieldTypeWarnings) resetLocked(start time.Time) {
	w.windowStart, w.writes, w.warnedBuckets, w.named = start, 0, nil, nil
}

// lineLocked is the aggregate as one line, naming buckets and counts and
// never a value, or "" when nothing was counted.
func (w *fieldTypeWarnings) lineLocked() string {
	if w.writes == 0 {
		return ""
	}
	writes := "writes"
	if w.writes == 1 {
		writes = "write"
	}
	distinct := len(w.warnedBuckets)
	bucketCount := fmt.Sprintf("%d buckets", distinct)
	names := strings.Join(w.named, ", ")
	switch {
	case distinct >= warnDistinctBuckets:
		bucketCount = fmt.Sprintf("at least %s buckets", groupThousands(warnDistinctBuckets))
		names += ", and others"
	case distinct == 1:
		bucketCount = "1 bucket"
	case distinct > len(w.named):
		names += fmt.Sprintf(", and %d more", distinct-len(w.named))
	}
	// No query lists the mismatched documents (the scan filter has no $type),
	// so the line promises none: strict only refuses from now on.
	return fmt.Sprintf("Since %s, %s %s to %s in field_types: warn mode stored a field of the wrong declared type (%s). "+
		"Set field_types: strict on the bucket to refuse them from now on: alter bucket <name> { field_types: strict }",
		w.windowStart.UTC().Format(time.RFC3339), groupThousands(int(w.writes)), writes, bucketCount, names)
}

// bucketIsEmpty reports whether the bucket holds no documents, read only on
// a refusal (D-5): the received type's own word is offered only then, since
// on a bucket that holds documents the alter it names would be scanned and
// could be refused. A count that cannot be read offers the widening, which
// is right on any bucket. The caller holds the bucket's lock.
func bucketIsEmpty(bucket *inMemoryBucket) bool {
	entries, err := countBucketEntries(bucket)
	return err == nil && entries == 0
}

// fieldTypeRefusal is E-1 to E-5 and E-8 (spec §6.2) for one document. Its
// text carries field names, types and the bucket, never a value (RD-10).
type fieldTypeRefusal struct {
	mismatch         *typeMismatch
	database, bucket string
	// empty is whether the bucket held no documents when it refused.
	empty bool
	// position is the document's 1-based place in its write, and key its
	// _id_; batchPrefix locates it in a batch.
	position    int
	key         string
	batchPrefix string
	// conflicts are the 1-based positions of the other documents in its batch
	// that the alter E-1 offers would refuse.
	conflicts []int
}

func (e *fieldTypeRefusal) Error() string {
	return e.batchPrefix + e.mismatch.refusalText(e.database, e.bucket, e.empty, e.conflicts)
}

// Unwrap makes the refusal FIELD_TYPE: a constraint's answer at replay and on
// every copy (spec §6.1).
func (e *fieldTypeRefusal) Unwrap() error { return ErrFieldType }

// refusalText is the mismatch's text from spec §6.2, in bucket of database.
// conflicts are the other documents of its batch the offered alter would
// refuse: with any, E-1 offers no alter, since taking it would only move the
// refusal to them.
func (m *typeMismatch) refusalText(database, bucket string, empty bool, conflicts []int) string {
	where := fmt.Sprintf("Field %q in bucket %q (database %q)", m.path, bucket, database)
	var refused string
	switch m.reason {
	case mismatchRequired:
		refused = fmt.Sprintf("%s is required, and this document has no %s (or it is null), so the document was not written.", where, m.path)
	case mismatchDoubleMagnitude:
		refused = fmt.Sprintf("%s is declared double, but this document's %s is %s. The document was not written.", where, m.path, m.receivedDetail())
	default:
		refused = fmt.Sprintf("%s is declared %s, but this document's %s is %s, so the document was not written.",
			where, m.declared.text(), m.path, m.receivedDetail())
	}
	if len(conflicts) > 0 {
		return fmt.Sprintf("%s\nSend %s as %s, for example %s.\nThe alter that would accept this document (alter bucket %s { %s }) would refuse %s of the batch, so no single alter admits the batch.",
			refused, m.path, m.declared.text(), exampleOf(m.declared.kind, m.path), bucket, m.alteredTo(m.offeredKind(empty)), positionsText(conflicts))
	}
	return refused + "\n" + m.fixLine(bucket, empty, "")
}

// receivedDetail is what was received, with the rule it breaks where spec
// §6.2 states one.
func (m *typeMismatch) receivedDetail() string {
	switch m.reason {
	case mismatchInt32Range:
		return "a 64-bit integer outside int32's range (-2147483648 to 2147483647)"
	case mismatchDoubleMagnitude:
		return "a 64-bit integer larger than 2^53 in magnitude, and a double field accepts integers only up to 2^53, where every integer is held exactly"
	case mismatchDoubleForInteger:
		return "a double (a number written with a decimal point or exponent)"
	case mismatchDoubleForDecimal:
		return "a double, which cannot hold most decimal fractions exactly"
	}
	return m.received
}

// fixLine is the second line of the mismatch's text: what to send instead,
// and the declaration that would accept what was sent. On an empty bucket
// the received type's own word is offered (D-5). An update names its operator, and
// each example is shaped as that operator ({$max: {Age: 36}}); a write names
// none.
func (m *typeMismatch) fixLine(bucket string, empty bool, operator string) string {
	field := m.path
	example := func(kind fieldKind) string {
		if operator == "" {
			return exampleOf(kind, field)
		}
		return fmt.Sprintf("{%s: %s}", operator, exampleOf(kind, field))
	}
	switch m.reason {
	case mismatchRequired:
		return fmt.Sprintf("Add %s to the document, for example %s; or make it optional: alter bucket %s { %s %s }",
			field, example(m.declared.kind), bucket, field, m.declared.text())
	case mismatchInt32Range:
		return fmt.Sprintf("To store 64-bit values, widen the declaration: alter bucket %s { %s }", bucket, m.alteredTo(kindInt))
	case mismatchDoubleMagnitude:
		return fmt.Sprintf("To accept it, widen the declaration: alter bucket %s { %s }", bucket, m.alteredTo(kindNumber))
	case mismatchDoubleForInteger:
		return fmt.Sprintf("Send a whole number without a decimal point, for example %s; or, to accept fractions too, "+
			"widen the declaration: alter bucket %s { %s }", example(kindInt), bucket, m.alteredTo(kindNumber))
	case mismatchDoubleForDecimal:
		return fmt.Sprintf("Send it as a decimal, for example %s; or, to accept doubles too, widen the declaration: alter bucket %s { %s }",
			example(kindDecimal), bucket, m.alteredTo(kindNumber))
	case mismatchUUIDText:
		return fmt.Sprintf("A UUID field holds a 16-byte UUID value, not its text: send %s. Document keys (_id_) are UUID text; "+
			"to accept text here too, widen the declaration: alter bucket %s { %s }", example(kindUUID), bucket, m.alteredTo(kindAny))
	}
	sent := fmt.Sprintf("Send %s as %s, for example %s", field, m.declared.text(), example(m.declared.kind))
	if empty {
		return fmt.Sprintf("%s; or, since the bucket holds no documents yet, change the declaration: alter bucket %s { %s }",
			sent, bucket, m.alteredTo(m.offeredKind(empty)))
	}
	return fmt.Sprintf("%s; or, to accept other values here, widen the declaration: alter bucket %s { %s }", sent, bucket, m.alteredTo(m.offeredKind(empty)))
}

// offeredKind is the word the alter E-1 offers names: the received type's own
// word on an empty bucket, the widening otherwise.
func (m *typeMismatch) offeredKind(empty bool) fieldKind {
	if empty {
		return m.receivedKind
	}
	return m.widening()
}

// batchConflicts are the 1-based positions of the documents in a batch, other
// than the refused one at index refused, whose value for this field the
// alter E-1 offers would refuse. Only E-1 offers an alter that depends on the
// type; the others name fixed widenings (spec §6.2). A single document has no
// others.
func (m *typeMismatch) batchConflicts(documents []map[string]interface{}, refused int, empty bool) []int {
	if m.reason != mismatchWrongType || len(documents) < 2 {
		return nil
	}
	offered := m.offeredKind(empty)
	var conflicts []int
	for index, document := range documents {
		if index == refused {
			continue
		}
		if fieldValue := document[m.path]; fieldValue != nil && accepts(offered, fieldValue) != mismatchNone {
			conflicts = append(conflicts, index+1)
		}
	}
	return conflicts
}

// positionsText is "document 2" or "documents 1 and 3" or "documents 1, 3
// and 4".
func positionsText(positions []int) string {
	words := make([]string, len(positions))
	for index, position := range positions {
		words[index] = strconv.Itoa(position)
	}
	if len(words) == 1 {
		return "document " + words[0]
	}
	return "documents " + strings.Join(words[:len(words)-1], ", ") + " and " + words[len(words)-1]
}

// widening is the word an alter that accepts the received value without
// narrowing anything names: number when both types are numbers, any
// otherwise (spec §6.2).
func (m *typeMismatch) widening() fieldKind {
	if numericKind(m.declared.kind) && numericKind(m.receivedKind) {
		return kindNumber
	}
	return kindAny
}

// alteredTo is the field's declaration as an alter to kind writes it: the
// field, the word and, when the field is required, required, so the alter
// changes the type and nothing else.
func (m *typeMismatch) alteredTo(kind fieldKind) string {
	if m.required {
		return fmt.Sprintf("%s %s required", m.path, kind)
	}
	return fmt.Sprintf("%s %s", m.path, kind)
}

// text is the node's canonical text form, as describe prints it.
func (node typeNode) text() string {
	return FieldTypeText(declarationOfType(node.catalogType()))
}

// exampleValues are spec §6.2's <example> values, by declared kind: literals
// the CLI accepts, never filled from what the client sent (D-10). any holds
// every type, so its example is a string.
var exampleValues = [...]string{
	kindString: `"text"`, kindInt32: "36", kindInt: "36", kindDouble: "9.5", kindDecimal: `{$numberDecimal: "9.99"}`,
	kindNumber: "36", kindBool: "true", kindDatetime: `{$date: "2026-10-03T12:00:00Z"}`,
	kindBSONTimestamp: "{$timestamp: {t: 1759492800, i: 1}}", kindBinary: `{$binary: {base64: "AQID", subType: "00"}}`,
	kindUUID: `{$uuid: "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b"}`, kindObjectID: `{$oid: "66fe1a2b3c4d5e6f70819203"}`,
	kindObject: `{city: "Tirana"}`, kindArray: `["a", "b"]`, kindRegex: `{$regularExpression: {pattern: "^a", options: "i"}}`,
	kindNull: "null", kindAny: `"text"`,
}

// exampleOf is the example document for field declared kind.
func exampleOf(kind fieldKind, field string) string {
	return fmt.Sprintf("{%s: %s}", field, exampleValues[kind])
}

// declarationUnusableError refuses a write to a bucket whose recorded
// declaration cannot be enforced as recorded: written by a newer hoardDB
// (spec §8.4), or refused by this version's validator. Reads are
// served. It answers UNSUPPORTED_VERSION (SA-16): it is not a constraint's
// answer, and replay counts it skipped, because an operator must act.
type declarationUnusableError struct {
	database, bucket string
	newer            bool
	invalid          error
}

func (e *declarationUnusableError) Error() string {
	lead := fmt.Sprintf("A write to bucket %q (database %q) was refused: its", e.bucket, e.database)
	if e.newer {
		return newerRefusal(lead)
	}
	return invalidRefusal(lead, e.bucket, e.invalid)
}

func (e *declarationUnusableError) Unwrap() error { return ErrDeclarationUnusable }

// writesRefused is why every write to the bucket is refused, as describe
// shows it, or "" when its declaration is enforced.
func (ft *bucketFieldTypes) writesRefused(bucket string) string {
	if ft == nil || ft.unusable == nil {
		return ""
	}
	const lead = "Writes are refused: this bucket's"
	if ft.unusable.recorded != nil {
		return newerRefusal(lead)
	}
	return invalidRefusal(lead, bucket, ft.unusable.invalid)
}

// newerRefusal and invalidRefusal are the two reasons a recorded declaration
// cannot be enforced, said after lead, which ends in the possessive naming the
// bucket. The unreadable one wins over an invalid one: the validator saw only
// the part this binary reads.
func newerRefusal(lead string) string {
	return lead + " typed-field declaration was written by a newer version of hoardDB, which this version cannot read, so it cannot check the write. " +
		"Reads still work. Upgrade this node to that version or later; the declaration is kept as recorded."
}

func invalidRefusal(lead, bucket string, invalid error) string {
	return fmt.Sprintf("%s recorded typed-field declaration fails this version's checks (%s), so it cannot be enforced. Reads still work. "+
		"To write to it again: dump the bucket, create a new bucket whose declaration passes, restore the dump into it, then drop %q.",
		lead, strings.TrimSpace(invalid.Error()), bucket)
}
