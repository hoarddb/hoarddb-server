package server

import (
	"context"
	"fmt"
	"strings"
)

// Enforcement of a bucket's typed-field declaration on updates (typed-fields
// spec §4.5.6, §5.4; build doc TF-7). An update is checked in two halves, in
// the apply, under the bucket's lock, on every copy and in replay:
//
//   - the static half reads only the request and the declaration, once per
//     request: an operator the declared field cannot take, an operand that
//     does not satisfy the field (or its element type), the removal of a
//     required field, and a path through a field declared as something that
//     holds no members or positions. It is raised before any operator runs,
//     so an operator that would silently do nothing ($max of a string over an
//     int) is refused rather than accepted;
//   - the result half reads what only the result tells, after the operators
//     run: an $inc or $mul result outside the field's range or kind, and the
//     value a $rename moves onto a declared field. Fields the update does
//     not write are not checked again.
//
// Until nested types (TF-9) the declaration is read at the top level only: a
// path is checked against the declaration of its first name, and beneath a
// declared object or array nothing is declared.

// updateTyping is one update's checks against its bucket's declaration. It
// is built per request and used by one apply, under the bucket's lock.
type updateTyping struct {
	schema           *declaredSchema
	database, bucket string
	parsed           *parsedUpdate
	// static is the first refusal the request earns by itself, in the fixed
	// operator order, or nil.
	static *updateTypeRefusal
	// results are the declared top-level fields whose written value only
	// the result tells (spec §5.4): $inc and $mul on a field declared int32,
	// int or double, and $rename's one-name targets. Nothing else the update
	// does not write is checked again.
	results []resultCheck
	// warned is set when warn's mode let a mismatch through, so the update
	// is counted once however many documents it changes.
	warned bool
}

// resultCheck is one declared field an update's result is checked at.
type resultCheck struct {
	operator string
	member   *memberNode
	// factor is $mul's operand, which tells a fractional product apart.
	factor float64
	// absent is, for a $rename target, whether the field is missing when
	// $rename runs on the current document, having been missing before the
	// operators ran or removed by this update's $unset: only then does the
	// rename write it. (An operator that creates the field first makes the
	// rename a no-op, and the check then reads a value this update wrote and
	// already checked.)
	absent bool
}

// typeUpdate prepares the checks of parsed against holder, the bucket's
// declaration. A bucket that declares nothing, or whose mode is off, gets no
// checks; one whose declaration cannot be enforced refuses the update before
// any document is read.
//
// Each entry point (this, before and after) is a nil test that inlines into
// the handler, with the work in a separate function, so an undeclared
// bucket pays three nil tests per update and no call (spec §9.1).
func typeUpdate(holder *bucketFieldTypes, parsed *parsedUpdate) (*updateTyping, error) {
	if holder == nil {
		return nil, nil
	}
	return holder.typeUpdate(parsed)
}

func (holder *bucketFieldTypes) typeUpdate(parsed *parsedUpdate) (*updateTyping, error) {
	req := parsed.req
	if unusable := holder.unusable; unusable != nil {
		return nil, &declarationUnusableError{database: req.Database, bucket: req.Bucket, invalid: unusable.invalid, newer: unusable.recorded != nil}
	}
	if holder.schema.mode == fieldTypesOff {
		return nil, nil
	}
	typing := &updateTyping{schema: holder.schema, database: req.Database, bucket: req.Bucket, parsed: parsed}
	typing.static = typing.staticRefusal()
	typing.results = typing.resultChecks()
	return typing, nil
}

// resultChecks lists the fields the result half checks. $inc and $mul on
// number or any always fit, and their refusal on anything else is static.
func (u *updateTyping) resultChecks() []resultCheck {
	var checks []resultCheck
	for _, operator := range []struct {
		name  string
		paths []fieldPath
	}{{"$inc", u.parsed.inc}, {"$mul", u.parsed.mul}} {
		for _, path := range operator.paths {
			member := u.schema.member(path.text)
			if path.segs != nil || member == nil {
				continue
			}
			switch member.typ.kind {
			case kindInt32, kindInt, kindDouble:
				checks = append(checks, resultCheck{operator: operator.name, member: member, factor: u.parsed.req.Mul[path.text]})
			}
		}
	}
	for _, pair := range u.parsed.rename {
		member := u.schema.member(pair.target.text)
		if pair.target.segs == nil && member != nil && member.typ.kind != kindAny {
			checks = append(checks, resultCheck{operator: "$rename", member: member})
		}
	}
	return checks
}

// before runs the static half for the document stored under key, about to
// be updated: in strict the request's refusal is raised, naming key; in warn
// it is let through and counted once the update stores. It notes which
// $rename targets the document lacks, for the result half.
func (u *updateTyping) before(ctx context.Context, document map[string]interface{}, key string) error {
	if u == nil {
		return nil
	}
	return u.checkBefore(ctx, document, key)
}

func (u *updateTyping) checkBefore(ctx context.Context, document map[string]interface{}, key string) error {
	for index := range u.results {
		if check := &u.results[index]; check.operator == "$rename" {
			_, present := document[check.member.name]
			check.absent = !present || u.unsets(check.member.name)
		}
	}
	if u.static == nil {
		return nil
	}
	refusal := *u.static
	refusal.key = key
	if refusal.class == refusedElement {
		element := *refusal.mismatch
		element.path = fmt.Sprintf("%s[%d]", refusal.member.name, u.appendIndex(document, refusal.member.name))
		refusal.mismatch = &element
	}
	return u.raise(ctx, &refusal)
}

// after runs the result half on the document stored under key, once the
// operators have run on it: each field in results the update wrote must
// satisfy its declaration. A multi-update stops at the first document that
// does not, and nothing of it is stored.
func (u *updateTyping) after(ctx context.Context, document map[string]interface{}, key string) error {
	if u == nil {
		return nil
	}
	return u.checkAfter(ctx, document, key)
}

func (u *updateTyping) checkAfter(ctx context.Context, document map[string]interface{}, key string) error {
	for index := range u.results {
		check := &u.results[index]
		written, present := document[check.member.name]
		if !present || (check.operator == "$rename" && !check.absent) || (written == nil && !check.member.required) {
			continue
		}
		reason := accepts(check.member.typ.kind, written)
		if reason == mismatchNone {
			continue
		}
		class := refusedResult
		if check.operator == "$mul" && reason == mismatchDoubleForInteger && !wholeInt64Factor(check.factor) {
			class = refusedFraction
		}
		refusal := u.refusal(class, check.operator, fieldPath{text: check.member.name}, check.member)
		refusal.key = key
		refusal.mismatch = mismatchOf(check.member.name, check.member.typ, check.member.required, written, reason)
		return u.raise(ctx, refusal)
	}
	return nil
}

// raise applies the bucket's mode to a refusal: strict counts it, on the
// apply that answers the client, and returns it; warn lets the update
// through and counts it once it stores.
func (u *updateTyping) raise(ctx context.Context, refusal *updateTypeRefusal) error {
	if u.schema.mode == fieldTypesWarn {
		u.warned = true
		return nil
	}
	if answersClient(ctx) {
		fieldTypeRefusals.Inc()
	}
	return refusal
}

// noteUpdateWarned counts an update warn stored with a mismatch, once, on
// the apply that answers the client.
func (s *Server) noteUpdateWarned(ctx context.Context, typing *updateTyping) {
	if typing != nil && typing.warned {
		s.countWarned(ctx, typing.database, typing.bucket)
	}
}

// member is the declared top-level field name, or nil.
func (schema *declaredSchema) member(name string) *memberNode {
	for index := range schema.fields {
		if schema.fields[index].name == name {
			return &schema.fields[index]
		}
	}
	return nil
}

// rootName is the first name of a path: the top-level field it is inside.
func rootName(path fieldPath) string {
	if path.segs == nil {
		return path.text
	}
	return path.segs[0].name
}

// staticRefusal is the first refusal of the request against the
// declaration, in the operators' fixed order and each operator's key order,
// so every copy names the same one.
func (u *updateTyping) staticRefusal() *updateTypeRefusal {
	for _, operator := range []struct {
		name  string
		paths []fieldPath
	}{
		{"$set", u.parsed.set}, {"$unset", u.parsed.unset}, {"$inc", u.parsed.inc}, {"$push", u.parsed.push},
		{"$addToSet", u.parsed.addToSet}, {"$mul", u.parsed.mul}, {"$min", u.parsed.min}, {"$max", u.parsed.max},
	} {
		for _, path := range operator.paths {
			member := u.schema.member(rootName(path))
			if member == nil {
				continue
			}
			var refusal *updateTypeRefusal
			if path.segs == nil {
				refusal = u.atField(operator.name, path, member)
			} else {
				refusal = u.through(operator.name, path, member)
			}
			if refusal != nil {
				return refusal
			}
		}
	}
	for _, pair := range u.parsed.rename {
		if refusal := u.renamed(pair); refusal != nil {
			return refusal
		}
	}
	return nil
}

// atField checks one operator on a declared top-level field.
func (u *updateTyping) atField(operator string, path fieldPath, member *memberNode) *updateTypeRefusal {
	req := u.parsed.req
	switch operator {
	case "$set":
		return u.valueOperand(operator, path, member, req.Set[path.text])
	case "$min":
		return u.valueOperand(operator, path, member, req.Min[path.text])
	case "$max":
		return u.valueOperand(operator, path, member, req.Max[path.text])
	case "$unset":
		return u.removal(operator, path, member, refusedUnset)
	case "$push":
		return u.appended(operator, path, member, req.Push[path.text])
	case "$addToSet":
		return u.appended(operator, path, member, req.AddToSet[path.text])
	}
	return u.arithmetic(operator, path, member)
}

// through refuses a path whose first name is declared as something a path
// cannot go through: a name needs an object, a position an array. Such an
// update either meets a stored value it cannot enter or creates a document
// where the declaration wants something else, so its result is never the
// declared type. path has two segments or more.
func (u *updateTyping) through(operator string, path fieldPath, member *memberNode) *updateTypeRefusal {
	if member.typ.kind == kindAny {
		return nil
	}
	want := kindObject
	if path.segs[1].isIndex {
		want = kindArray
	}
	if member.typ.kind == want {
		return nil
	}
	return u.refusal(refusedThrough, operator, path, member)
}

// valueOperand checks the operand of $set, $min or $max against the field:
// null is refused on a required field, and anything else must satisfy it.
func (u *updateTyping) valueOperand(operator string, path fieldPath, member *memberNode, operand interface{}) *updateTypeRefusal {
	if operand == nil {
		if member.required {
			return u.refusal(refusedNull, operator, path, member)
		}
		return nil
	}
	reason := accepts(member.typ.kind, operand)
	if reason == mismatchNone {
		return nil
	}
	refusal := u.refusal(refusedOperand, operator, path, member)
	refusal.mismatch = mismatchOf(path.text, member.typ, member.required, operand, reason)
	return refusal
}

// removal refuses taking a required field away ($unset, $rename's source).
func (u *updateTyping) removal(operator string, path fieldPath, member *memberNode, class updateRefusalClass) *updateTypeRefusal {
	if !member.required {
		return nil
	}
	return u.refusal(class, operator, path, member)
}

// arithmetic refuses $inc and $mul on a field that is not declared a number
// they compute: decimal (OC-5), every non-numeric word, object and array.
func (u *updateTyping) arithmetic(operator string, path fieldPath, member *memberNode) *updateTypeRefusal {
	switch member.typ.kind {
	case kindInt32, kindInt, kindDouble, kindNumber, kindAny:
		return nil
	}
	return u.refusal(refusedArithmetic, operator, path, member)
}

// appended checks $push and $addToSet: they make the field an array, so it
// must be declared one (or any), and the operand must satisfy the declared
// element type. An operand $addToSet would find present is checked too.
func (u *updateTyping) appended(operator string, path fieldPath, member *memberNode, operand interface{}) *updateTypeRefusal {
	switch member.typ.kind {
	case kindAny:
		return nil
	case kindArray:
	default:
		return u.refusal(refusedAppend, operator, path, member)
	}
	element := member.typ.elem
	if element == nil {
		return nil
	}
	reason := accepts(element.kind, operand)
	if reason == mismatchNone {
		return nil
	}
	refusal := u.refusal(refusedElement, operator, path, member)
	refusal.mismatch = mismatchOf(path.text, *element, false, operand, reason)
	return refusal
}

// renamed checks one $rename: a required source cannot be moved away, and
// neither side may go through a declared field that holds no members.
func (u *updateTyping) renamed(pair renamePair) *updateTypeRefusal {
	if member := u.schema.member(rootName(pair.source)); member != nil {
		var refusal *updateTypeRefusal
		if pair.source.segs == nil {
			refusal = u.removal("$rename", pair.source, member, refusedRename)
		} else {
			refusal = u.through("$rename", pair.source, member)
		}
		if refusal != nil {
			return refusal
		}
	}
	if member := u.schema.member(rootName(pair.target)); member != nil && pair.target.segs != nil {
		return u.through("$rename", pair.target, member)
	}
	return nil
}

// appendIndex is the position $push or $addToSet at the top-level field name
// appends at in document, after the operators that run before it in the same
// update ($set and $unset of the same field).
func (u *updateTyping) appendIndex(document map[string]interface{}, name string) int {
	current, exists := document[name]
	if set, isSet := u.parsed.req.Set[name]; isSet {
		current, exists = set, true
	}
	if u.unsets(name) {
		exists = false
	}
	if !exists {
		return 0
	}
	if elements, isArray := decodedArray(current); isArray {
		return len(elements)
	}
	return 1
}

// unsets reports whether the update's $unset names the top-level field name.
func (u *updateTyping) unsets(name string) bool {
	for _, path := range u.parsed.unset {
		if path.text == name {
			return true
		}
	}
	return false
}

// mismatchOf is the mismatch of value against a declared node, at path.
func mismatchOf(path string, declared typeNode, required bool, value interface{}, reason mismatchReason) *typeMismatch {
	return &typeMismatch{path: path, declared: declared, required: required,
		received: receivedWord(value), receivedKind: kindOf(value), reason: reason}
}

// updateRefusalClass is which of an update's type refusals a refusal is.
type updateRefusalClass uint8

const (
	// refusedOperand is E-1 (or E-2, E-3, E-4, E-8) for $set, $min or $max.
	refusedOperand updateRefusalClass = iota
	// refusedElement is E-1 for the element $push or $addToSet appends.
	refusedElement
	// refusedArithmetic is E-6: $inc or $mul on a field that is not a number.
	refusedArithmetic
	// refusedAppend is $push or $addToSet on a field that is not an array.
	refusedAppend
	// refusedUnset, refusedRename and refusedNull are E-7.
	refusedUnset
	refusedRename
	refusedNull
	// refusedThrough is a path through a declared field that holds no
	// members (or positions).
	refusedThrough
	// refusedResult is a result check's refusal: an $inc or $mul result, or
	// the value a $rename moved, that the field does not accept.
	refusedResult
	// refusedFraction is E-6's $mul text: an integer times a factor that is
	// not a whole number inside int64's range is a double, which an integer
	// field refuses.
	refusedFraction
)

// updateTypeRefusal is one update's refusal for the bucket's declared types.
// Its text carries operators, paths, types, the bucket and a document's key,
// never a value (RD-10).
type updateTypeRefusal struct {
	class            updateRefusalClass
	operator         string
	path             fieldPath
	member           *memberNode
	database, bucket string
	// key is the refused document's key, set when the refusal is raised: on
	// a multi-update, the first matched document's (D-11).
	key string
	// mismatch is the operand's or the element's, for refusedOperand and
	// refusedElement.
	mismatch *typeMismatch
}

func (u *updateTyping) refusal(class updateRefusalClass, operator string, path fieldPath, member *memberNode) *updateTypeRefusal {
	return &updateTypeRefusal{class: class, operator: operator, path: path, member: member, database: u.database, bucket: u.bucket}
}

// Unwrap makes the refusal FIELD_TYPE: decided by the request, the stored
// document and the declaration alone, so a constraint's answer on every copy
// and at replay (spec §6.1).
func (e *updateTypeRefusal) Unwrap() error { return ErrFieldType }

// Error is the refusal's text: E-6 and E-7 as spec §6.2 words them, and, for
// a mistyped operand or element, E-1's facts in an update's words.
func (e *updateTypeRefusal) Error() string {
	field, declared := e.member.name, e.member.typ.text()
	where := fmt.Sprintf("in bucket %q (database %q)", e.bucket, e.database)
	setInstead := fmt.Sprintf("To change it, use $set, for example: db.%s.update({_id_: %q}, {$set: {%s: %s}})",
		e.bucket, e.key, field, exampleValues[e.member.typ.kind])
	required := fmt.Sprintf("%s is required. Nothing was changed.\nChange its value with $set instead, or make it optional first: alter bucket %s { %s %s }",
		field, e.bucket, field, declared)
	switch e.class {
	case refusedArithmetic:
		applies := "int32, int, double and number fields"
		if e.member.typ.kind == kindDecimal {
			applies += "; decimal arithmetic is not supported in this release"
		}
		return fmt.Sprintf("%s on field %q %s was refused: %s is declared %s, and %s only applies to %s. Nothing was changed.\n%s",
			e.operator, field, where, field, declared, e.operator, applies, setInstead)
	case refusedAppend:
		return fmt.Sprintf("%s on field %q %s was refused: %s is declared %s, and %s only applies to array fields: it would make %s an array. Nothing was changed.\n%s",
			e.operator, field, where, field, declared, e.operator, field, setInstead)
	case refusedUnset:
		return fmt.Sprintf("$unset of field %q %s was refused: %s", field, where, required)
	case refusedRename:
		return fmt.Sprintf("$rename of field %q away %s was refused: %s", field, where, required)
	case refusedNull:
		return fmt.Sprintf("setting required field %q to null %s was refused: %s is required. Nothing was changed.\n"+
			"Set it to %s instead, for example {$set: %s}; or make it optional first: alter bucket %s { %s %s }",
			field, where, field, kindArticle(e.member.typ.kind), exampleOf(e.member.typ.kind, field), e.bucket, field, declared)
	case refusedThrough:
		return e.throughText(where)
	case refusedFraction:
		return fmt.Sprintf("$mul on field %q %s was refused: the product is a double (the factor is fractional, not a number, infinite, "+
			"or outside int64's range), and %s is declared %s. Nothing was changed.\n"+
			"Multiply by a whole number within int64's range, set the computed whole number with $set, or widen the declaration: alter bucket %s { %s }",
			field, where, field, declared, e.bucket, alteredField(e.member, kindNumber))
	case refusedResult:
		if e.operator == "$rename" {
			return fmt.Sprintf("$rename onto field %q %s was refused: %s is declared %s, but the value moved there is %s. Nothing was changed.\n"+
				"Rename onto %s only a value it accepts, or set it directly: db.%s.update({_id_: %q}, {$set: %s}); "+
				"or, to accept other values here, widen the declaration: alter bucket %s { %s }",
				field, where, field, declared, e.mismatch.receivedDetail(),
				field, e.bucket, e.key, exampleOf(e.member.typ.kind, field), e.bucket, e.mismatch.alteredTo(e.mismatch.widening()))
		}
		return fmt.Sprintf("%s on field %q %s was refused: %s is declared %s, but the result is %s. Nothing was changed.\n%s",
			e.operator, field, where, field, declared, e.mismatch.receivedDetail(), e.mismatch.fixLine(e.bucket, false, ""))
	case refusedElement:
		element := e.mismatch
		return fmt.Sprintf("%s on field %q %s was refused: %s is declared %s (an element of %s: %s), but its operand is %s. Nothing was changed.\n"+
			"Send the element as %s, for example {%s: {%s: %s}}; or, to accept other elements, widen the declaration: alter bucket %s { %s }",
			e.operator, element.path, where, element.path, element.declared.text(), field, declared, element.receivedDetail(),
			element.declared.text(), e.operator, field, exampleValues[element.declared.kind], e.bucket, alteredField(e.member, kindArray))
	}
	return fmt.Sprintf("%s on field %q %s was refused: %s is declared %s, but its operand is %s. Nothing was changed.\n%s",
		e.operator, field, where, field, declared, e.mismatch.receivedDetail(), e.mismatch.fixLine(e.bucket, false, e.operator))
}

// kindArticle is a declared word as a value of it is named: "a string",
// "an int"; any names no type, so "a value".
func kindArticle(kind fieldKind) string {
	word := kind.String()
	switch {
	case kind == kindAny:
		return "a value"
	case strings.ContainsRune("aeio", rune(word[0])):
		return "an " + word
	}
	return "a " + word
}

// throughText is E-14's text for a path through a declared field: the
// declaration, not a stored value, is what it cannot go through.
func (e *updateTypeRefusal) throughText(where string) string {
	next := e.path.segs[1]
	kind, has, holds := "a document", "no member "+next.name, "members"
	if next.isIndex {
		kind, has, holds = "an array", fmt.Sprintf("no element %d", next.index), "elements"
	}
	refused := fmt.Sprintf("%s of %q %s was refused: %s is declared %s, not %s, so it has %s. Nothing was changed.\n",
		e.operator, e.path.text, where, e.member.name, e.member.typ.text(), kind, has)
	if e.operator == "$unset" {
		return refused + fmt.Sprintf("To remove the whole field, unset it: {$unset: [%q]}", e.member.name)
	}
	return refused + fmt.Sprintf("To replace it, set the whole value, for example: db.%s.update({_id_: %q}, {$set: %s}); "+
		"or, to store %s in it, widen the declaration: alter bucket %s { %s }",
		e.bucket, e.key, exampleOf(e.member.typ.kind, e.member.name), holds, e.bucket, alteredField(e.member, kindAny))
}

// alteredField is member's declaration as an alter to kind writes it, keeping
// required.
func alteredField(member *memberNode, kind fieldKind) string {
	if member.required {
		return fmt.Sprintf("%s %s required", member.name, kind)
	}
	return fmt.Sprintf("%s %s", member.name, kind)
}
