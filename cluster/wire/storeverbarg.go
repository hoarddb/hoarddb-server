package wire

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
)

const (
	// maxVerbArgBytes is the largest STORE_VERB arg (conventions section 9.1).
	maxVerbArgBytes = 1 << 20
	// verbArgDepth is the deepest nesting a verb arg may have: the layouts of conventions
	// section 9.1 are flat documents whose element is a binary, so 8 is generous.
	verbArgDepth = 8
	// maxVerbKeyBytes is the longest k or e of a STORE_VERB arg.
	maxVerbKeyBytes = 1024
	// maxIncPaths is the most dotted paths one inc names; a counter hour has a handful.
	maxIncPaths = 64
	// maxIncDepth is the deepest dotted path an inc names: d.14.t is three levels.
	maxIncDepth = 3
)

// verbShape names the layout of a STORE_VERB arg (conventions section 9.1).
type verbShape int

const (
	// shapePop is a pop: no arg (one element) or {n} (up to n elements).
	shapePop verbShape = iota + 1
	// shapeElement is {v} for a push to a fifoStore or lifoStore.
	shapeElement
	// shapeHeapPush is {p, e, v}.
	shapeHeapPush
	// shapePut is {k, v}.
	shapePut
	// shapeTakeKey is {k} for a take from a hashStore or btreeStore.
	shapeTakeKey
	// shapeTakeEntry is {e} for a take from a heapStore.
	shapeTakeEntry
	// shapeInc is {k, v[, r]} for an inc on a hashStore or btreeStore: v is a BSON document of
	// dotted paths to numbers, and r asks for the element as applied.
	shapeInc
)

// verbArg is a decoded STORE_VERB arg; only the fields of its shape are set.
type verbArg struct {
	Priority int64
	EntryID  string
	Key      string
	Element  []byte
	// Count is the n of a pop {n}; 0 means the arg was absent, a single pop.
	Count int
	// Return is the r of an inc: true asks for the element as applied.
	Return bool
}

// The wire documents. Field order is the spec's order and is the canonical byte layout.
type (
	popArgDoc struct {
		N int32 `bson:"n"`
	}
	elementArgDoc struct {
		V []byte `bson:"v"`
	}
	heapPushArgDoc struct {
		P int64  `bson:"p"`
		E string `bson:"e"`
		V []byte `bson:"v"`
	}
	putArgDoc struct {
		K string `bson:"k"`
		V []byte `bson:"v"`
	}
	takeKeyArgDoc struct {
		K string `bson:"k"`
	}
	takeEntryArgDoc struct {
		E string `bson:"e"`
	}
	incArgDoc struct {
		K string `bson:"k"`
		V []byte `bson:"v"`
		// R is omitted when false: an arg with r: false would name the same increment as one without it.
		R bool `bson:"r,omitempty"`
	}
)

// verbShapeOf is the table of conventions section 9.1: the arg layout of a verb on a
// store type, or false when the pair is not one the owner applies.
func verbShapeOf(verb byte, engine store.FieldEngine) (verbShape, bool) {
	switch verb {
	case cluster.VerbPush:
		switch engine {
		case store.FieldFIFO, store.FieldLIFO:
			return shapeElement, true
		case store.FieldHeap:
			return shapeHeapPush, true
		}
	case cluster.VerbPop:
		switch engine {
		case store.FieldFIFO, store.FieldLIFO, store.FieldHeap:
			return shapePop, true
		}
	case cluster.VerbPut:
		switch engine {
		case store.FieldHash, store.FieldBTree:
			return shapePut, true
		}
	case cluster.VerbInc:
		// Only a keyed element has a number to add to; a queue's elements are positional, and an inc
		// that fit one would be appended and then fail on every replay.
		switch engine {
		case store.FieldHash, store.FieldBTree:
			return shapeInc, true
		}
	case cluster.VerbTake:
		switch engine {
		case store.FieldHash, store.FieldBTree:
			return shapeTakeKey, true
		case store.FieldHeap:
			return shapeTakeEntry, true
		}
	}
	return 0, false
}

func verbArgRefusal(format string, args ...any) error {
	return &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(format, args...)}
}

func checkVerbKey(name, key string) error {
	if len(key) < 1 || len(key) > maxVerbKeyBytes {
		return verbArgRefusal("%q is %d bytes and must be 1 to %d bytes. Nothing was written", name, len(key), maxVerbKeyBytes)
	}
	// A key comes back in the keys member of a reply, a BSON string, which is UTF-8 (build
	// SPEC-GAP U85-5).
	if !utf8.ValidString(key) {
		return verbArgRefusal("%q is not valid UTF-8. Nothing was written. Send a key and an entry id as text; encode binary values as hex or base64 first", name)
	}
	return nil
}

func checkVerbElement(element []byte) error {
	if len(element) < 1 {
		return verbArgRefusal("%q is empty and must be at least 1 byte (a BSON document). Nothing was written", "v")
	}
	return nil
}

func checkVerbPriority(priority int64) error {
	if priority == math.MinInt64 {
		return verbArgRefusal("%q is %d and must be between %d and %d. Nothing was written", "p", priority, math.MinInt64+1, math.MaxInt64)
	}
	return nil
}

func encodeVerbDoc(doc any) ([]byte, error) {
	arg, err := bson.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("encoding a store verb arg: %w", err)
	}
	if len(arg) > maxVerbArgBytes {
		return nil, verbArgRefusal("the arg is %d bytes and at most %d bytes are taken. Nothing was written", len(arg), maxVerbArgBytes)
	}
	return arg, nil
}

// encodeElementArg is the {v} arg of a push to a fifoStore or lifoStore.
func encodeElementArg(element []byte) ([]byte, error) {
	if err := checkVerbElement(element); err != nil {
		return nil, err
	}
	return encodeVerbDoc(elementArgDoc{V: element})
}

// encodeHeapPushArg is the {p, e, v} arg of a push to a heapStore.
func encodeHeapPushArg(priority int64, entryID string, element []byte) ([]byte, error) {
	if err := checkVerbPriority(priority); err != nil {
		return nil, err
	}
	if err := checkVerbKey("e", entryID); err != nil {
		return nil, err
	}
	if err := checkVerbElement(element); err != nil {
		return nil, err
	}
	return encodeVerbDoc(heapPushArgDoc{P: priority, E: entryID, V: element})
}

// encodePutArg is the {k, v} arg of a put into a hashStore or btreeStore.
func encodePutArg(key string, element []byte) ([]byte, error) {
	if err := checkVerbKey("k", key); err != nil {
		return nil, err
	}
	if err := checkVerbElement(element); err != nil {
		return nil, err
	}
	return encodeVerbDoc(putArgDoc{K: key, V: element})
}

// encodeTakeKeyArg is the {k} arg of a take from a hashStore or btreeStore.
func encodeTakeKeyArg(key string) ([]byte, error) {
	if err := checkVerbKey("k", key); err != nil {
		return nil, err
	}
	return encodeVerbDoc(takeKeyArgDoc{K: key})
}

// encodePopArg is the {n} arg of a pop that takes up to n elements.
func encodePopArg(n int) ([]byte, error) {
	if err := checkPopCount(int64(n)); err != nil {
		return nil, err
	}
	return encodeVerbDoc(popArgDoc{N: int32(n)})
}

func checkPopCount(n int64) error {
	if n < 1 || n > maxPeekCount {
		return verbArgRefusal("%q is %d and a pop takes 1 to %d elements. Nothing was removed", "n", n, maxPeekCount)
	}
	return nil
}

// encodeTakeEntryArg is the {e} arg of a take from a heapStore.
func encodeTakeEntryArg(entryID string) ([]byte, error) {
	if err := checkVerbKey("e", entryID); err != nil {
		return nil, err
	}
	return encodeVerbDoc(takeEntryArgDoc{E: entryID})
}

// IncArg is the {k, v[, r]} arg of an inc on a hashStore or btreeStore: key names the element,
// increments is a BSON document of dotted key paths to numbers, and returnAfter asks for the
// element as applied. r is written only when true, so an arg has one byte form.
func IncArg(key string, increments []byte, returnAfter bool) ([]byte, error) {
	if err := checkVerbKey("k", key); err != nil {
		return nil, err
	}
	if err := checkIncrements(increments); err != nil {
		return nil, err
	}
	return encodeVerbDoc(incArgDoc{K: key, V: increments, R: returnAfter})
}

// checkIncrements refuses an increments document the apply could not add, before a position is
// taken: a path of more than maxIncDepth levels or with an empty level, a value that is not an
// int32, an int64 or a finite double, more than maxIncPaths paths, and no path at all.
func checkIncrements(increments []byte) error {
	if _, err := bson.Validate(increments, bson.Limits{Depth: verbArgDepth, Size: maxVerbArgBytes}); err != nil {
		return verbArgRefusal("inc's increments are not a valid BSON document: %v. Nothing was written", err)
	}
	elements, err := bson.Raw(increments).Elements()
	if err != nil {
		return verbArgRefusal("inc's increments are not a BSON document: %v. Nothing was written", err)
	}
	if len(elements) == 0 {
		return verbArgRefusal("inc takes at least one path and its number, and the increments are empty. Nothing was written. Send one, for example db.User[\"ada\"].Friends.inc(\"grace\", {visits: 1}) (help inc)")
	}
	if len(elements) > maxIncPaths {
		return verbArgRefusal("inc takes at most %d paths in one call, and this one has %d. Nothing was written; split it into more calls", maxIncPaths, len(elements))
	}
	for _, element := range elements {
		if err := checkIncPath(element.Key()); err != nil {
			return err
		}
		if err := checkIncNumber(element.Key(), element.Value()); err != nil {
			return err
		}
	}
	return checkIncConflicts(elements)
}

// checkIncConflicts refuses a path given twice and a path that is a parent of another, as MongoDB
// refuses a path conflict in $inc: the add would otherwise be applied twice or to a number that
// is also a document. At most maxIncPaths paths, so the pairwise walk is bounded.
func checkIncConflicts(elements []bson.RawElement) error {
	for i, first := range elements {
		for _, second := range elements[i+1:] {
			a, b := first.Key(), second.Key()
			switch {
			case a == b:
				return verbArgRefusal("path %q is given twice and would be added twice. Nothing was written; give each path once with the total amount", a)
			case strings.HasPrefix(a, b+"."):
				return verbArgRefusal("paths %q and %q conflict, as %q is inside %q. Nothing was written; inc one or the other in a call", b, a, a, b)
			case strings.HasPrefix(b, a+"."):
				return verbArgRefusal("paths %q and %q conflict, as %q is inside %q. Nothing was written; inc one or the other in a call", a, b, b, a)
			}
		}
	}
	return nil
}

// checkIncPath refuses a dotted path of more than maxIncDepth levels, or with an empty level.
func checkIncPath(path string) error {
	levels := strings.Split(path, ".")
	if len(levels) > maxIncDepth {
		return verbArgRefusal("path %q is %d levels deep and inc takes at most %d. Nothing was written", path, len(levels), maxIncDepth)
	}
	for _, level := range levels {
		if level == "" {
			return verbArgRefusal("path %q has an empty level. Nothing was written; a path is names joined by dots, such as \"d.14.t\"", path)
		}
	}
	return nil
}

// checkIncNumber refuses a value that is not an int32, an int64 or a finite double. A decimal is
// refused by name: money belongs in integer minor units, which an inc can add exactly.
func checkIncNumber(path string, value bson.RawValue) error {
	switch value.Type {
	case bsontype.Int32, bsontype.Int64:
		return nil
	case bsontype.Double:
		if number, ok := readNumber(value.Type, value.Value); ok && isFinite(number.real) {
			return nil
		}
		return verbArgRefusal("%q is not a finite double. Nothing was written", path)
	case bsontype.Decimal128:
		return verbArgRefusal("inc takes numbers; a decimal is refused, store money in integer minor units. %q is a decimal. Nothing was written", path)
	}
	return verbArgRefusal("inc takes an int32, an int64 or a finite double, and %q is a %s. Nothing was written", path, value.Type)
}

// decodeVerbDoc unmarshals arg into doc, then insists arg is exactly what the encoder
// would write for it: reordered, duplicate, missing or extra fields, a widened integer
// and trailing bytes all fail that comparison, so an accepted arg re-encodes identically
// (the U32c rule). Lengths are checked before anything is allocated.
func decodeVerbDoc(arg []byte, doc any) error {
	if len(arg) > maxVerbArgBytes {
		return verbArgRefusal("the arg is %d bytes and at most %d bytes are taken", len(arg), maxVerbArgBytes)
	}
	if _, err := bson.Validate(arg, bson.Limits{Depth: verbArgDepth, Size: maxVerbArgBytes}); err != nil {
		return verbArgRefusal("the arg is not a valid BSON document: %v", err)
	}
	if err := bson.Unmarshal(arg, doc); err != nil {
		return verbArgRefusal("the arg is not a BSON document of the layout the verb takes: %v", err)
	}
	again, err := bson.Marshal(doc)
	if err != nil {
		return verbArgRefusal("the arg holds an element that cannot be read back: %v", err)
	}
	if !bytes.Equal(again, arg) {
		return verbArgRefusal("the arg does not have the layout of conventions section 9.1 (fields in order, none missing, duplicated or extra, the stated types)")
	}
	return nil
}

// decodeVerbArg reads arg as shape and checks the limits of conventions section 9.1.
func decodeVerbArg(shape verbShape, arg []byte) (verbArg, error) {
	switch shape {
	case shapePop:
		if len(arg) == 0 {
			return verbArg{}, nil
		}
		var doc popArgDoc
		if err := decodeVerbDoc(arg, &doc); err != nil {
			return verbArg{}, err
		}
		return verbArg{Count: int(doc.N)}, checkPopCount(int64(doc.N))
	case shapeElement:
		var doc elementArgDoc
		if err := decodeVerbDoc(arg, &doc); err != nil {
			return verbArg{}, err
		}
		return verbArg{Element: doc.V}, checkVerbElement(doc.V)
	case shapeHeapPush:
		var doc heapPushArgDoc
		if err := decodeVerbDoc(arg, &doc); err != nil {
			return verbArg{}, err
		}
		return verbArg{Priority: doc.P, EntryID: doc.E, Element: doc.V}, firstError(checkVerbPriority(doc.P), checkVerbKey("e", doc.E), checkVerbElement(doc.V))
	case shapePut:
		var doc putArgDoc
		if err := decodeVerbDoc(arg, &doc); err != nil {
			return verbArg{}, err
		}
		return verbArg{Key: doc.K, Element: doc.V}, firstError(checkVerbKey("k", doc.K), checkVerbElement(doc.V))
	case shapeTakeKey:
		var doc takeKeyArgDoc
		if err := decodeVerbDoc(arg, &doc); err != nil {
			return verbArg{}, err
		}
		return verbArg{Key: doc.K}, checkVerbKey("k", doc.K)
	case shapeTakeEntry:
		var doc takeEntryArgDoc
		if err := decodeVerbDoc(arg, &doc); err != nil {
			return verbArg{}, err
		}
		return verbArg{EntryID: doc.E}, checkVerbKey("e", doc.E)
	case shapeInc:
		var doc incArgDoc
		if err := decodeVerbDoc(arg, &doc); err != nil {
			return verbArg{}, err
		}
		return verbArg{Key: doc.K, Element: doc.V, Return: doc.R}, firstError(checkVerbKey("k", doc.K), checkIncrements(doc.V))
	}
	return verbArg{}, fmt.Errorf("store verb arg shape %d is not one this build knows", shape)
}

func firstError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
