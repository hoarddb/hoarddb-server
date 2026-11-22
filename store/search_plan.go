package store

import (
	"bytes"
	"fmt"

	"github.com/hoarddb/hoarddb-server/storage"
)

// The search planner (GAP-14 build doc Step 11, spec §5.2–§5.3). A compound
// index's key is its fields' EncodeKey in declaration order, and every
// component is self-delimiting, so the encoded values of a prefix of its
// fields are a byte prefix of every matching entry: a filter covering the
// first k fields is served by one bounded walk.

// SearchPlan is how an index serves a search: the chosen index and the prefix
// of its fields the filter covers. Every filter predicate is re-applied to
// what the walk returns; the plan only narrows.
type SearchPlan struct {
	Spec IndexSpec
	// Covered is the fields of the index's prefix the filter fixes by
	// equality, in declaration order.
	Covered []string
	prefix  []byte
	index   *SecondaryIndex
}

// DocumentKeys returns the key of every document under the plan's prefix.
func (p *SearchPlan) DocumentKeys() ([][]byte, error) {
	return p.index.FindPrefix(p.prefix, len(p.Spec.Fields))
}

// candidate is one index considered for a filter.
type candidate struct {
	spec    IndexSpec
	covered []interface{}
}

// better orders candidates: the longest covered prefix, then unique — at most
// one document per full key — then the lowest id, so the choice never
// depends on map order and survives a restart.
func (c candidate) better(other *candidate) bool {
	switch {
	case other == nil:
		return true
	case len(c.covered) != len(other.covered):
		return len(c.covered) > len(other.covered)
	case c.spec.Unique != other.spec.Unique:
		return c.spec.Unique
	}
	return c.spec.ID < other.spec.ID
}

// PlanSearch chooses the index that serves an equality filter. A building,
// conflicted or unreadable index is never chosen. It returns nil and no error
// when no index covers the filter — the caller scans, and says so — and the
// refusal of the best index that covers it when no ready one does: an index
// being rebuilt or holding a duplicate is the reason the query is refused,
// not a reason to scan in silence.
func (im *IndexManager) PlanSearch(filter map[string]interface{}) (*SearchPlan, error) {
	var ready, unready *candidate
	for _, spec := range im.specs {
		covered := coveredPrefix(spec, filter)
		if len(covered) == 0 {
			continue
		}
		considered := candidate{spec: spec, covered: covered}
		if spec.State == IndexReady {
			if considered.better(ready) {
				ready = &considered
			}
			continue
		}
		if considered.better(unready) {
			unready = &considered
		}
	}
	if ready == nil && unready != nil {
		return nil, im.notReadyError(unready.spec)
	}
	if ready == nil {
		return nil, nil
	}
	prefix, err := EncodeKey(ready.covered)
	if err != nil {
		return nil, fmt.Errorf("encoding the search's key for index %s: %w", ready.spec.Name, err)
	}
	return &SearchPlan{Spec: ready.spec, Covered: ready.spec.FieldNames()[:len(ready.covered)], prefix: prefix, index: im.indexes[ready.spec.ID]}, nil
}

// coveredPrefix is the values of the longest prefix of spec's fields that
// filter fixes by equality. A value that is an operator document, an array,
// or anything else no key can hold is not an equality.
func coveredPrefix(spec IndexSpec, filter map[string]interface{}) []interface{} {
	var covered []interface{}
	for _, part := range spec.Fields {
		value, present := filter[part.Field]
		if !present {
			break
		}
		if _, err := EncodeComponent(value); err != nil {
			break
		}
		covered = append(covered, value)
	}
	return covered
}

// notReadyError is why an index that is not ready cannot serve a search.
func (im *IndexManager) notReadyError(spec IndexSpec) error {
	switch spec.State {
	case IndexConflicted:
		return im.conflictedIndexError(spec)
	case IndexUnreadable:
		return &UnreadableIndexError{Index: spec.Name, Version: spec.Version}
	default:
		return &IndexNotReadyError{Index: spec.Name, State: spec.State}
	}
}

// FindPrefix returns the key of every document whose entry begins with
// encodedPrefix, the encoded values of the first fields of an index of arity
// fields: a bounded walk of that prefix's entries.
func (s *SecondaryIndex) FindPrefix(encodedPrefix []byte, arity int) ([][]byte, error) {
	lower := s.makePrefix(encodedPrefix)
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: lower, Upper: prefixEnd(lower)})
	defer it.Close()
	var documents [][]byte
	for it.Seek(nil); it.Valid(); it.Next() {
		entry := it.Key()[len(s.prefix):]
		keyLength := 0
		for component := 0; component < arity; component++ {
			length, err := componentLen(entry[keyLength:])
			if err != nil {
				return nil, &storage.KeyError{Operation: "reading an index entry", Key: bytes.Clone(it.Key()), Cause: err}
			}
			keyLength += length
		}
		documents = append(documents, append([]byte(nil), entry[keyLength:]...))
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("walking the index entries under a prefix: %w", err)
	}
	return documents, nil
}
