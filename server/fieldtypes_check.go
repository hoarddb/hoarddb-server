package server

import "fmt"

// typeNode is the declared type of one field, member or element. TF-5 fills
// members and elem from the declaration; the check reads them from TF-9 on.
type typeNode struct {
	kind fieldKind
	// members are an object's declared members and elem an array's element
	// type, nil for a bare object or array and for every other kind.
	members []memberNode
	elem    *typeNode
}

// memberNode is one declared field of an object, or one top-level field.
type memberNode struct {
	name     string
	required bool
	typ      typeNode
}

// declaredSchema is a bucket's declared top-level fields in declaration
// order, immutable once built: a reader holds the pointer and never a lock.
// The order is what makes every copy report the same field when two are wrong.
//
// A schema is interned (fieldtypes_schema.go): every bucket with the same
// declaration and mode holds the same one, so it is never modified.
type declaredSchema struct {
	fields []memberNode
	// mode is the recorded field_types word: "" for strict, the default, or
	// "warn" or "off".
	mode string
}

// bucketFieldTypes is the per-bucket holder: the schema a check reads, and,
// only for a declaration this binary cannot read whole, the declaration as
// the catalog recorded it. It carries no sequence (spec §5.1): the
// declaration in force is the one in the entry's catalog position.
type bucketFieldTypes struct {
	schema *declaredSchema
	// unusable is nil for every declaration this binary enforces. It is a
	// pointer so that what only the rare unenforceable declaration has costs
	// every other declared bucket 8 bytes (rule 11).
	unusable *unusableDeclaration
}

// unusableDeclaration is a recorded declaration this binary cannot enforce
// as recorded: every write to its bucket is refused, and reads are served.
type unusableDeclaration struct {
	// recorded is the catalog's declaration, in order, when one of its fields
	// (or its mode) is one this binary cannot read: every rewrite of the entry
	// writes it back byte for byte (spec §8.4), and the schema then holds only
	// the fields this binary can read. Nil for a readable declaration.
	recorded []catalogField
	// invalid is the validator's refusal of a recorded declaration,
	// nil when it passes. A create never makes one.
	invalid error
}

// recordedFields is the declaration as recorded when this binary cannot
// read all of it, nil otherwise.
func (ft *bucketFieldTypes) recordedFields() []catalogField {
	if ft.unusable == nil {
		return nil
	}
	return ft.unusable.recorded
}

// typeMismatch is the first declared field a document does not satisfy. It is
// built only on failure, so an accepted write allocates nothing.
type typeMismatch struct {
	// path is the full path of the value: a member name today, and with TF-9
	// members joined by "." and positions as [i], assembled while unwinding.
	path     string
	declared typeNode
	// required is the declared field's modifier: the alter a refusal offers
	// keeps it.
	required bool
	received string
	// receivedKind is the word that declares the received value's type, which
	// a refusal on an empty bucket suggests (spec §6.2).
	receivedKind fieldKind
	reason       mismatchReason
}

// Error is a stand-in sentence: spec §6.2's refusal texts name the bucket and
// the database, which the check does not know, and are TF-6's.
func (m *typeMismatch) Error() string {
	if m.reason == mismatchRequired {
		return fmt.Sprintf("field %q is required and the document has none (or it is null). Add it, or make the field optional in the declaration", m.path)
	}
	return fmt.Sprintf("field %q is declared %s, but the document's value is %s. Send %s as %s, or widen the declaration",
		m.path, m.declared.kind, m.received, m.path, m.declared.kind)
}

// Unwrap makes a type refusal a FIELD_TYPE refusal: deterministic, decided
// before the apply, and a constraint's answer at replay.
func (m *typeMismatch) Unwrap() error { return ErrFieldType }

// checkDocument checks a document's declared fields against the bucket's
// declaration, and returns the first field in declaration order that does not
// fit, or nil. A bucket that declares nothing pays the nil test and nothing
// else (spec §9.1): the nil test is the first statement and the rest is a
// separate function, so this one inlines into its caller and the 2 ns gate
// holds. The E-18 name scan is not here; it runs on every bucket, outside this
// gate.
func checkDocument(ft *bucketFieldTypes, doc map[string]interface{}) *typeMismatch {
	if ft == nil {
		return nil
	}
	return ft.schema.check(doc)
}

// check does one map lookup and one acceptance test per declared field. Null
// and missing satisfy an optional field and fail a required one (spec §4.1 to
// §4.3).
func (schema *declaredSchema) check(doc map[string]interface{}) *typeMismatch {
	for index := range schema.fields {
		member := &schema.fields[index]
		fieldValue := doc[member.name]
		if fieldValue == nil {
			if member.required {
				return &typeMismatch{path: member.name, declared: member.typ, required: true, received: "null", receivedKind: kindNull, reason: mismatchRequired}
			}
			continue
		}
		if reason := accepts(member.typ.kind, fieldValue); reason != mismatchNone {
			return &typeMismatch{path: member.name, declared: member.typ, required: member.required,
				received: receivedWord(fieldValue), receivedKind: kindOf(fieldValue), reason: reason}
		}
	}
	return nil
}
