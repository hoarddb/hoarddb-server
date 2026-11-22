package server

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// Update operators on paths (typed-fields spec §4.9.2, §4.9.3). Every one of
// the nine operators takes a path. An update's keys are parsed once per
// request (parseUpdate); the refusals that depend only on the request — the
// grammar, a position $unset or $rename cannot use, overlapping paths — are
// made then, before any document is read; the ones that depend on the stored
// document are made as each operator walks to its value. Either way nothing
// is stored: the caller discards the document.

// pathRefusal is an update refused for a path, with the text a developer
// reads (E-14, E-15, E-16). It depends only on the request and the stored
// document, so every applier gives it alike.
type pathRefusal struct{ message string }

func (e *pathRefusal) Error() string { return e.message }

func (e *pathRefusal) Unwrap() error { return ErrPathRefused }

// renamePair is one $rename, parsed.
type renamePair struct {
	source, target fieldPath
}

// parsedUpdate is an update whose operator keys are parsed into paths, each
// operator's keys in name order so the refusal a developer reads is the same
// on every node.
type parsedUpdate struct {
	req                                     UpdateRequest
	set, inc, push, addToSet, mul, min, max []fieldPath
	// unset keeps the request's order; a duplicate is a no-op.
	unset  []fieldPath
	rename []renamePair
}

// parseUpdate parses req's keys once for the request and makes every refusal
// that depends on the request alone.
func parseUpdate(req UpdateRequest) (*parsedUpdate, error) {
	site := updateSite{database: req.Database, bucket: req.Bucket}
	parsed := &parsedUpdate{req: req}
	var err error
	for _, operator := range []struct {
		name string
		keys []string
		into *[]fieldPath
	}{
		{"$set", slices.Sorted(maps.Keys(req.Set)), &parsed.set},
		{"$unset", req.Unset, &parsed.unset},
		{"$inc", slices.Sorted(maps.Keys(req.Inc)), &parsed.inc},
		{"$push", slices.Sorted(maps.Keys(req.Push)), &parsed.push},
		{"$addToSet", slices.Sorted(maps.Keys(req.AddToSet)), &parsed.addToSet},
		{"$mul", slices.Sorted(maps.Keys(req.Mul)), &parsed.mul},
		{"$min", slices.Sorted(maps.Keys(req.Min)), &parsed.min},
		{"$max", slices.Sorted(maps.Keys(req.Max)), &parsed.max},
	} {
		if *operator.into, err = parseOperatorKeys(site, operator.name, operator.keys); err != nil {
			return nil, err
		}
	}
	for _, path := range parsed.unset {
		if path.segs != nil && path.segs[len(path.segs)-1].isIndex {
			return nil, &pathRefusal{message: fmt.Sprintf(
				"$unset of %q in bucket %q (database %q) was refused: removing an array element would move every element after it. Nothing was changed.\n"+
					"Set the whole array without that element instead: {$set: {%s: [ … ]}}",
				path.text, site.bucket, site.database, pathPrefixText(path.segs[:len(path.segs)-1]))}
		}
	}
	for _, source := range slices.Sorted(maps.Keys(req.Rename)) {
		pair, err := parseRenamePair(site, source, req.Rename[source])
		if err != nil {
			return nil, err
		}
		parsed.rename = append(parsed.rename, pair)
	}
	if err := parsed.refuseOverlaps(site); err != nil {
		return nil, err
	}
	return parsed, nil
}

func parseOperatorKeys(site updateSite, operator string, keys []string) ([]fieldPath, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	paths := make([]fieldPath, 0, len(keys))
	for _, key := range keys {
		path, err := parseFieldPath(key)
		if err != nil {
			return nil, &pathRefusal{message: fmt.Sprintf(
				"%s of %q in bucket %q (database %q) was refused: %v. Nothing was changed.\n"+
					"Write a path as names joined by \".\", with 0-based positions in brackets, for example Address.City or Lines[3].Qty.",
				operator, key, site.bucket, site.database, err)}
		}
		paths = append(paths, path)
	}
	return paths, nil
}

// parseRenamePair parses one $rename. Both sides are paths of names only: an
// array element has no name to move (E-15).
func parseRenamePair(site updateSite, source, target string) (renamePair, error) {
	parsed, err := parseOperatorKeys(site, "$rename", []string{source, target})
	if err != nil {
		return renamePair{}, err
	}
	for _, path := range parsed {
		for _, seg := range path.segs {
			if seg.isIndex {
				return renamePair{}, &pathRefusal{message: fmt.Sprintf(
					"$rename of %q in bucket %q (database %q) was refused: $rename moves a named member, and an array position is not a name. Nothing was changed.\n"+
						`Rename members of objects only, for example {$rename: {"Address.Zip": "Address.PostCode"}}`,
					path.text, site.bucket, site.database)}
			}
		}
	}
	return renamePair{source: parsed[0], target: parsed[1]}, nil
}

// pathOverlap is how two paths relate.
type pathOverlap int

const (
	pathsDisjoint pathOverlap = iota
	pathsEqual
	firstInsideSecond
	secondInsideFirst
)

// segmentsOf is a path's segments, a one-name path included.
func segmentsOf(path fieldPath) []pathSeg {
	if path.segs == nil {
		return []pathSeg{{name: path.text}}
	}
	return path.segs
}

func overlapOf(first, second fieldPath) pathOverlap {
	firstSegs, secondSegs := segmentsOf(first), segmentsOf(second)
	shared := min(len(firstSegs), len(secondSegs))
	if !slices.Equal(firstSegs[:shared], secondSegs[:shared]) {
		return pathsDisjoint
	}
	switch {
	case len(firstSegs) == len(secondSegs):
		return pathsEqual
	case len(firstSegs) > len(secondSegs):
		return firstInsideSecond
	}
	return secondInsideFirst
}

// touchedPath is one path an update writes, and the operator writing it.
type touchedPath struct {
	operator string
	path     fieldPath
}

// refuseOverlaps is spec §4.9.2's overlap rule (OC-3), checked before any
// operator runs (T-10): an operator's keys are a Go map, so two keys touching
// one path would apply in a different order on every copy. Within one
// operator a strict prefix is refused; the sources and targets of $rename are
// pairwise disjoint, equality counted; across operators an equal
// key keeps the fixed loop order and a strict prefix is refused; $unset
// duplicates are a no-op.
func (u *parsedUpdate) refuseOverlaps(site updateSite) error {
	for _, pair := range u.rename {
		var moves string
		switch overlapOf(pair.source, pair.target) {
		case pathsEqual:
			moves = "moves a field onto itself"
		case secondInsideFirst:
			moves = "moves a field inside itself"
		case firstInsideSecond:
			moves = "moves a field onto the field that holds it"
		default:
			continue
		}
		return &pathRefusal{message: fmt.Sprintf(
			"This update to bucket %q (database %q) was refused: $rename of %q to %q %s. Nothing was changed.\n"+
				"Rename it to a name outside %s, for example %s.",
			site.bucket, site.database, pair.source.text, pair.target.text, moves,
			pair.source.text, strings.NewReplacer(".", "_", "[", "_", "]", "").Replace(pair.target.text))}
	}
	for i, first := range u.rename {
		for _, second := range u.rename[i+1:] {
			for _, sides := range [][2]fieldPath{
				{first.source, second.source}, {first.source, second.target},
				{first.target, second.source}, {first.target, second.target},
			} {
				overlap := overlapOf(sides[0], sides[1])
				if overlap == pathsDisjoint {
					continue
				}
				touched := sides[0].text
				if overlap == firstInsideSecond {
					touched = sides[1].text
				}
				return &pathRefusal{message: fmt.Sprintf(
					"This update to bucket %q (database %q) was refused: $rename of %q to %q and of %q to %q both touch %q, "+
						"so the result would depend on which is applied first. Nothing was changed.\n"+
						"Split it into separate updates, or rename to names no other pair uses.",
					site.bucket, site.database, first.source.text, first.target.text, second.source.text, second.target.text, touched)}
			}
		}
	}
	touched := u.touchedPaths()
	for i, first := range touched {
		for _, second := range touched[i+1:] {
			overlap := overlapOf(first.path, second.path)
			if overlap == pathsDisjoint || overlap == pathsEqual {
				continue
			}
			outer, inner := first.path.text, second.path.text
			if overlap == firstInsideSecond {
				outer, inner = inner, outer
			}
			return &pathRefusal{message: fmt.Sprintf(
				"This update to bucket %q (database %q) was refused: it writes both %q and %q, and one is inside the other, "+
					"so the result would depend on which is applied first. Nothing was changed.\n"+
					"Write one of them: set the whole %s, or set %s alone.",
				site.bucket, site.database, outer, inner, outer, inner)}
		}
	}
	return nil
}

// touchedPaths is every path the update writes, in the fixed operator order.
func (u *parsedUpdate) touchedPaths() []touchedPath {
	var touched []touchedPath
	for _, operator := range []struct {
		name  string
		paths []fieldPath
	}{
		{"$set", u.set}, {"$unset", u.unset}, {"$inc", u.inc}, {"$push", u.push}, {"$addToSet", u.addToSet},
		{"$mul", u.mul}, {"$min", u.min}, {"$max", u.max},
	} {
		for _, path := range operator.paths {
			touched = append(touched, touchedPath{operator: operator.name, path: path})
		}
	}
	for _, pair := range u.rename {
		touched = append(touched, touchedPath{operator: "$rename", path: pair.source}, touchedPath{operator: "$rename", path: pair.target})
	}
	return touched
}

// apply runs the operators on doc in the fixed order — $set, $unset, $inc,
// $push, $addToSet, $mul, $min, $max, $rename — and returns the first
// refusal; the caller then discards doc without storing it.
func (u *parsedUpdate) apply(doc map[string]interface{}, key string) error {
	site := updateSite{database: u.req.Database, bucket: u.req.Bucket, key: key}
	walk := pathWalk{doc: doc, site: site}
	for _, path := range u.set {
		target, _, err := walk.locate("$set", path, walkCreate)
		if err != nil {
			return err
		}
		target.set(u.req.Set[path.text])
	}
	for _, path := range u.unset {
		target, found, err := walk.locate("$unset", path, walkLenient)
		if err != nil {
			return err
		}
		if found {
			delete(target.document, target.name)
		}
	}
	for _, path := range u.inc {
		target, _, err := walk.locate("$inc", path, walkCreate)
		if err != nil {
			return err
		}
		if err := incrementField(target, site, path.text, u.req.Inc[path.text]); err != nil {
			return err
		}
	}
	for _, path := range u.push {
		target, _, err := walk.locate("$push", path, walkCreate)
		if err != nil {
			return err
		}
		applyPush(target, u.req.Push[path.text])
	}
	for _, path := range u.addToSet {
		target, _, err := walk.locate("$addToSet", path, walkCreate)
		if err != nil {
			return err
		}
		applyAddToSet(target, u.req.AddToSet[path.text])
	}
	for _, path := range u.mul {
		target, found, err := walk.locate("$mul", path, walkExisting)
		if err != nil {
			return err
		}
		if found {
			if err := multiplyField(target, site, path.text, u.req.Mul[path.text]); err != nil {
				return err
			}
		}
	}
	for _, path := range u.min {
		target, _, err := walk.locate("$min", path, walkCreate)
		if err != nil {
			return err
		}
		applyMin(target, u.req.Min[path.text])
	}
	for _, path := range u.max {
		target, _, err := walk.locate("$max", path, walkCreate)
		if err != nil {
			return err
		}
		applyMax(target, u.req.Max[path.text])
	}
	for _, pair := range u.rename {
		if err := walk.rename(pair); err != nil {
			return err
		}
	}
	return nil
}

// pathSlot is where an operator reads and writes the value a path names: a
// member of a document, or a position of an array. A slot of an array writes
// into the array's own storage, which the document holds.
type pathSlot struct {
	document map[string]interface{}
	name     string
	array    []interface{}
	index    int
}

// get is the value in the slot, and whether there is one. A position always
// holds one: locate refuses a position past the end.
func (s pathSlot) get() (interface{}, bool) {
	if s.document != nil {
		value, found := s.document[s.name]
		return value, found
	}
	return s.array[s.index], true
}

func (s pathSlot) set(value interface{}) {
	if s.document != nil {
		s.document[s.name] = value
		return
	}
	s.array[s.index] = value
}

// walkMode is what a walk does at a missing segment.
type walkMode int

const (
	// walkCreate creates a missing intermediate document ($set, $inc, $min,
	// $max, $push, $addToSet, $rename's target). A missing array before a
	// position, and a position past the end, are refused (E-15).
	walkCreate walkMode = iota
	// walkExisting finds nothing at a missing segment and creates nothing
	// ($mul, $rename's source); a position past the end is refused.
	walkExisting
	// walkLenient finds nothing at a missing segment or a position past the
	// end ($unset: a missing path does nothing).
	walkLenient
)

// pathWalk walks the paths of one update over one document.
type pathWalk struct {
	doc  map[string]interface{}
	site updateSite
}

// locate walks to the slot path names. found is false when a segment is
// missing and the mode creates nothing. A value on the way that is not a
// document (for a name) or an array (for a position) — null included — is
// refused (E-14): replacing it would destroy a value the update did not name.
func (w pathWalk) locate(operator string, path fieldPath, mode walkMode) (pathSlot, bool, error) {
	if path.segs == nil {
		return pathSlot{document: w.doc, name: path.text}, true, nil
	}
	last := len(path.segs) - 1
	var current interface{} = w.doc
	for position := range last {
		child, found, err := w.descend(operator, path, position, current, mode)
		if err != nil || !found {
			return pathSlot{}, false, err
		}
		current = child
	}
	seg := path.segs[last]
	if !seg.isIndex {
		document, isDocument := writableDocument(current)
		if !isDocument {
			return pathSlot{}, false, w.notAContainer(operator, path, last, current)
		}
		return pathSlot{document: document, name: seg.name}, true, nil
	}
	elements, isArray := decodedArray(current)
	switch {
	case !isArray:
		return pathSlot{}, false, w.notAContainer(operator, path, last, current)
	case seg.index < len(elements):
		return pathSlot{array: elements, index: seg.index}, true, nil
	case mode == walkLenient:
		return pathSlot{}, false, nil
	}
	return pathSlot{}, false, w.pastTheEnd(operator, path, last, len(elements))
}

// descend takes one intermediate segment of path from current, creating a
// missing document when the mode creates.
func (w pathWalk) descend(operator string, path fieldPath, position int, current interface{}, mode walkMode) (interface{}, bool, error) {
	seg := path.segs[position]
	if seg.isIndex {
		elements, isArray := decodedArray(current)
		switch {
		case !isArray:
			return nil, false, w.notAContainer(operator, path, position, current)
		case seg.index < len(elements):
			return elements[seg.index], true, nil
		case mode == walkLenient:
			return nil, false, nil
		}
		return nil, false, w.pastTheEnd(operator, path, position, len(elements))
	}
	document, isDocument := writableDocument(current)
	if !isDocument {
		return nil, false, w.notAContainer(operator, path, position, current)
	}
	if child, found := document[seg.name]; found {
		return child, true, nil
	}
	if mode != walkCreate {
		return nil, false, nil
	}
	if path.segs[position+1].isIndex {
		return nil, false, &pathRefusal{message: fmt.Sprintf(
			"%s of %q in bucket %q (database %q) was refused: %s does not exist, and a missing array is not created by a position. Nothing was changed.\n"+
				"Create the array first: {$set: {%s: [ … ]}}",
			operator, path.text, w.site.bucket, w.site.database,
			pathPrefixText(path.segs[:position+1]), quotedPathKey(path.segs[:position+1]))}
	}
	created := map[string]interface{}{}
	document[seg.name] = created
	return created, true, nil
}

// rename moves the value at pair.source to pair.target. A missing source and
// an existing target do nothing (today's rule, along the path); a missing
// intermediate document of the target is created.
func (w pathWalk) rename(pair renamePair) error {
	source, found, err := w.locate("$rename", pair.source, walkExisting)
	if err != nil || !found {
		return err
	}
	value, exists := source.get()
	if !exists {
		return nil
	}
	existing, found, err := w.locate("$rename", pair.target, walkExisting)
	if err != nil {
		return err
	}
	if found {
		if _, taken := existing.get(); taken {
			return nil
		}
	}
	target, _, err := w.locate("$rename", pair.target, walkCreate)
	if err != nil {
		return err
	}
	target.set(value)
	delete(source.document, source.name)
	return nil
}

// notAContainer is E-14: the path goes through a value that is not a document
// (before a name) or an array (before a position).
func (w pathWalk) notAContainer(operator string, path fieldPath, position int, found interface{}) error {
	seg := path.segs[position]
	holder := pathPrefixText(path.segs[:position])
	kind, has := "a document", fmt.Sprintf("no member %s", seg.name)
	replacement := fmt.Sprintf("{%s: …}", seg.name)
	if seg.isIndex {
		kind, has = "an array", fmt.Sprintf("no element %d", seg.index)
		replacement = "[ … ]"
	}
	return &pathRefusal{message: fmt.Sprintf(
		"%s of %q in bucket %q (database %q) was refused: %s is %s, not %s, so it has %s. Nothing was changed.\n"+
			"To replace it, set the whole value, for example: db.%s.update({_id_: %q}, {$set: {%s: %s}})",
		operator, path.text, w.site.bucket, w.site.database, holder, receivedWord(found), kind, has,
		w.site.bucket, w.site.key, quotedPathKey(path.segs[:position]), replacement)}
}

// pastTheEnd is E-15: a position at or past an array's length. $set never
// extends an array or fills a hole; appending is $push.
func (w pathWalk) pastTheEnd(operator string, path fieldPath, position, length int) error {
	array := pathPrefixText(path.segs[:position])
	held := "has no elements"
	if length > 0 {
		held = fmt.Sprintf("has %d elements (positions 0 to %d)", length, length-1)
	}
	return &pathRefusal{message: fmt.Sprintf(
		"%s of %q in bucket %q (database %q) was refused: %s %s, and a position past the end is not added. Nothing was changed.\n"+
			"To add an element, use $push, for example: db.%s.update({_id_: %q}, {$push: {%s: …}})",
		operator, path.text, w.site.bucket, w.site.database, array, held,
		w.site.bucket, w.site.key, quotedPathKey(path.segs[:position]))}
}

// writableDocument returns a decoded document an operator can write into. A
// wire document's members decode to map[string]interface{}; bson.M reaches
// here only from an in-process caller's $set value.
func writableDocument(value interface{}) (map[string]interface{}, bool) {
	switch document := value.(type) {
	case map[string]interface{}:
		return document, true
	case primitive.M:
		return document, true
	}
	return nil, false
}

// pathPrefixText spells segments as a path: Address, Lines[3], Orders[0].Lines.
func pathPrefixText(segs []pathSeg) string {
	var text strings.Builder
	for position, seg := range segs {
		switch {
		case seg.isIndex:
			text.WriteString("[" + strconv.Itoa(seg.index) + "]")
		case position > 0:
			text.WriteString("." + seg.name)
		default:
			text.WriteString(seg.name)
		}
	}
	return text.String()
}

// quotedPathKey is segments as a key in an example document: bare when it is
// one name, quoted when it is a path.
func quotedPathKey(segs []pathSeg) string {
	text := pathPrefixText(segs)
	if len(segs) == 1 {
		return text
	}
	return strconv.Quote(text)
}
