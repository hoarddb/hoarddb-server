package server

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// Paths: naming a value inside a value (typed-fields spec §4.9).
//
// A path is a field name followed by segments: ".name" for a member of a
// document and "[i]" for a 0-based position in an array, so Address.City and
// Lines[3].Qty. It means the same thing in every reader and every update
// operator. A key with no ".", "[" or "]" is a one-name path and keeps the
// single map lookup it always had: no segment slice is allocated for it.

// ErrPathRefused is under every refusal of a path: the grammar (E-14's grammar
// variant), a path through a value that is not a document or an array (E-14),
// a position a write cannot use (E-15) and overlapping paths in one update
// (E-16). Each depends only on the request and the stored document, so every
// copy and every replay refuses alike (spec §4.9.4). It reaches the wire as
// INVALID_ARGUMENT, carrying the constraint_refusal marker.
var ErrPathRefused = errors.New("path refused")

// maxPathSegments bounds a path's length (spec §4.9.1, review A N-7): without
// it, $set of a.a.a… could create one nested object per two bytes of key, and
// nothing else in the server bounds a document's depth.
const maxPathSegments = 100

// pathSeg is one step of a path: a member name, or an array position.
type pathSeg struct {
	name    string
	index   int
	isIndex bool
}

// fieldPath is a parsed path. segs is nil for a one-name key, which then
// resolves with one map lookup of text.
type fieldPath struct {
	segs []pathSeg
	text string
}

// simple reports whether the path is one name, the form every key had before
// paths: a single map lookup, no walk.
func (p fieldPath) simple() bool { return p.segs == nil }

// pathGrammarError is E-14's grammar variant: the key is not a path at all.
type pathGrammarError struct {
	path, reason string
}

func (e *pathGrammarError) Error() string {
	return fmt.Sprintf("%q is not a path: %s", e.path, e.reason)
}

func (e *pathGrammarError) Unwrap() error { return ErrPathRefused }

// parseFieldPath reads a field key as a path (spec §4.9.1). The cost is one
// byte scan of the key; a key without ".", "[" or "]" allocates nothing.
func parseFieldPath(key string) (fieldPath, error) {
	if !strings.ContainsAny(key, ".[]") {
		if reason := pathNameProblem(key); reason != "" {
			return fieldPath{}, &pathGrammarError{path: key, reason: reason}
		}
		return fieldPath{text: key}, nil
	}
	segs := make([]pathSeg, 0, 1+strings.Count(key, ".")+strings.Count(key, "["))
	position := 0
	readName := func() error {
		end := position
		for end < len(key) && key[end] != '.' && key[end] != '[' && key[end] != ']' {
			end++
		}
		name := key[position:end]
		if reason := pathNameProblem(name); reason != "" {
			return &pathGrammarError{path: key, reason: reason}
		}
		segs = append(segs, pathSeg{name: name})
		position = end
		return nil
	}
	if err := readName(); err != nil {
		return fieldPath{}, err
	}
	for position < len(key) {
		switch key[position] {
		case '.':
			position++
			if err := readName(); err != nil {
				return fieldPath{}, err
			}
		case '[':
			index, end, reason := readPathIndex(key, position+1)
			if reason != "" {
				return fieldPath{}, &pathGrammarError{path: key, reason: reason}
			}
			segs = append(segs, pathSeg{index: index, isIndex: true})
			position = end
		default:
			return fieldPath{}, &pathGrammarError{path: key, reason: `it has a "]" with no "[" before it`}
		}
		if len(segs) > maxPathSegments {
			return fieldPath{}, &pathGrammarError{path: key,
				reason: fmt.Sprintf("it has more than %d segments, and a path has at most %d", maxPathSegments, maxPathSegments)}
		}
	}
	if segs[0].name == "_id_" {
		return fieldPath{}, &pathGrammarError{path: key, reason: "_id_ is the document key, a string, so nothing is inside it"}
	}
	return fieldPath{segs: segs, text: key}, nil
}

// pathNameProblem says why name cannot be one name of a path, or "" when it
// can (§4.7 rule 1, as a path reads it).
func pathNameProblem(name string) string {
	switch {
	case name == "":
		return "it has an empty name (a path is names joined by \".\", with positions in brackets)"
	case name[0] == '$':
		return fmt.Sprintf("the name %q starts with \"$\", which marks an operator", name)
	case strings.IndexByte(name, 0) >= 0:
		return "a name contains a NUL byte"
	}
	return ""
}

// readPathIndex reads the digits of a position starting at start, just past
// its "[", and returns the position and the offset just past its "]".
func readPathIndex(key string, start int) (index, end int, reason string) {
	closeAt := strings.IndexByte(key[start:], ']')
	if closeAt < 0 {
		return 0, 0, `a "[" has no closing "]"`
	}
	digits := key[start : start+closeAt]
	end = start + closeAt + 1
	switch {
	case digits == "":
		return 0, 0, "it has an empty position []; a position is a whole number such as [3]"
	case digits[0] == '-' || digits[0] == '+':
		return 0, 0, "a position has no sign; positions count from [0]"
	case len(digits) > 1 && digits[0] == '0':
		return 0, 0, fmt.Sprintf("the position [%s] has a leading zero; write [%s]", digits, strings.TrimLeft(digits, "0"))
	}
	for _, digit := range []byte(digits) {
		if digit < '0' || digit > '9' {
			return 0, 0, fmt.Sprintf("a position is a whole number, 0 or more, such as [3], not [%s]", digits)
		}
	}
	index, err := strconv.Atoi(digits)
	if err != nil {
		return 0, 0, fmt.Sprintf("the position [%s] is too large to be an array position", digits)
	}
	return index, end, ""
}

// resolvePath is the decoded resolver: the value at p in doc, or absent. A
// name segment needs a document and an index segment an array; anything else
// on the way — a scalar, null, a position past the end, a name on an array —
// is absent, never an error (spec §4.9.6).
func resolvePath(doc map[string]interface{}, p fieldPath) (interface{}, bool) {
	if p.segs == nil {
		value, found := doc[p.text]
		return value, found
	}
	var current interface{} = doc
	for _, seg := range p.segs {
		next, found := pathStep(current, seg)
		if !found {
			return nil, false
		}
		current = next
	}
	return current, true
}

// pathStep takes one segment from a decoded value. A wire document decodes
// nested documents to map[string]interface{} and arrays to primitive.A; an
// update's own result holds []interface{}; bson.M and bson.D reach here only
// from in-process callers (spec T-9).
func pathStep(current interface{}, seg pathSeg) (interface{}, bool) {
	if seg.isIndex {
		elements, isArray := decodedArray(current)
		if !isArray || seg.index >= len(elements) {
			return nil, false
		}
		return elements[seg.index], true
	}
	switch document := current.(type) {
	case map[string]interface{}:
		value, found := document[seg.name]
		return value, found
	case primitive.M:
		value, found := document[seg.name]
		return value, found
	case primitive.D:
		for _, element := range document {
			if element.Key == seg.name {
				return element.Value, true
			}
		}
	}
	return nil, false
}

// decodedArray returns a decoded array's elements.
func decodedArray(value interface{}) ([]interface{}, bool) {
	switch elements := value.(type) {
	case primitive.A:
		return elements, true
	case []interface{}:
		return elements, true
	}
	return nil, false
}

// refuseUnreachableNames is E-18 (spec §4.9.8, OQ-25, OC-2): a document whose
// top-level field name contains ".", "[" or "]" is refused, because no path
// can name that field. It runs on every bucket, declared or not, in the apply,
// after now() and _id_ and before any type check, so every copy refuses alike.
// It is one range over the top-level keys with a byte search of each: 11–25
// ns a key, no allocation. The name reported is the least offending one, so
// every copy words the refusal alike. Nested names are not scanned: that
// would walk every value of every write.
func refuseUnreachableNames(doc map[string]interface{}, database, bucket string) error {
	var unreachable string
	found := false
	for name := range doc {
		if strings.ContainsAny(name, ".[]") && (!found || name < unreachable) {
			unreachable, found = name, true
		}
	}
	if !found {
		return nil
	}
	return &unreachableNameError{name: unreachable, database: database, bucket: bucket}
}

// unreachableNameError is E-18. Its example is built from the name with the
// fixed placeholder "…", never from the document's value (D-10).
type unreachableNameError struct {
	name, database, bucket string
	// batchPrefix locates the document in a batch.
	batchPrefix string
}

func (e *unreachableNameError) Error() string {
	reading, nesting := "", ""
	if path, err := parseFieldPath(e.name); err == nil && len(path.segs) > 1 {
		last := path.segs[len(path.segs)-1]
		holder := pathPrefixText(path.segs[:len(path.segs)-1])
		if last.isIndex {
			reading = fmt.Sprintf(" (%s is position %d of %s)", e.name, last.index, holder)
		} else {
			reading = fmt.Sprintf(" (%s is the member %s of %s)", e.name, last.name, holder)
		}
		nesting = "Nest it instead, for example " + nestedExample(path.segs) + ", or rename"
	} else {
		nesting = "Rename"
	}
	renamed := strings.NewReplacer(".", "_", "[", "_", "]", "").Replace(e.name)
	return fmt.Sprintf("%sField name %q in this document cannot be stored in bucket %q (database %q): a top-level field name may not "+
		"contain \".\", \"[\" or \"]\", because hoardDB reads them as a path%s. Nothing was written.\n"+
		"%s it without the character, for example %s.",
		e.batchPrefix, e.name, e.bucket, e.database, reading, nesting, renamed)
}

func (e *unreachableNameError) Unwrap() error { return ErrFieldType }

// nestedExample is the document a path's segments describe, with "…" for the
// value: {Address: {City: …}}, or {Lines: [ … ]} for a position.
func nestedExample(segs []pathSeg) string {
	if len(segs) == 0 {
		return "…"
	}
	if segs[0].isIndex {
		return "[ … ]"
	}
	return fmt.Sprintf("{%s: %s}", segs[0].name, nestedExample(segs[1:]))
}

// inBatch locates an E-18, an empty-_id_ or a type refusal in a batch: the
// document's 1-based position and its key.
func inBatch(err error, position int, key string) error {
	prefix := fmt.Sprintf("Document %d of the batch (_id_ %q): ", position, key)
	var unreachable *unreachableNameError
	if errors.As(err, &unreachable) {
		unreachable.batchPrefix = prefix
	}
	var emptyID *emptyIDError
	if errors.As(err, &emptyID) {
		emptyID.batchPrefix = prefix
	}
	var mistyped *fieldTypeRefusal
	if errors.As(err, &mistyped) {
		mistyped.batchPrefix = prefix
	}
	return err
}
