package server

import (
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-server/transport"
)

// repeatedIDError is the refusal of a BATCH_PUT that names one _id_ at more
// than one position. MySQL refuses a multi-row INSERT with a duplicate key as
// a whole statement and MongoDB's insertMany reports the failing position;
// neither drops a duplicate silently, and neither does this. results carries
// the per-position answer the ERROR frame sends (build doc BD-5, BD-6).
type repeatedIDError struct {
	database, bucket string
	// repeats are the repeated ids with every 1-based position, in order of
	// first appearance.
	repeats []repeatedID
	results []WriteResult
}

type repeatedID struct {
	id        string
	positions []int
}

func (e *repeatedIDError) Error() string {
	const named = 3
	parts := make([]string, 0, named)
	for index, repeat := range e.repeats {
		if index == named {
			break
		}
		parts = append(parts, fmt.Sprintf("%q at positions %s", repeat.id, joinPositions(repeat.positions)))
	}
	more := ""
	if len(e.repeats) > named {
		more = fmt.Sprintf(" (and %d more repeated ids)", len(e.repeats)-named)
	}
	return fmt.Sprintf("This batch for bucket %q in database %q names an _id_ more than once: %s%s. "+
		"Nothing was stored. Send each _id_ once, or send the batch as BATCH_UPSERT to keep the last document for each _id_.",
		e.bucket, e.database, strings.Join(parts, "; "), more)
}

func joinPositions(positions []int) string {
	text := make([]string, len(positions))
	for index, position := range positions {
		text[index] = fmt.Sprint(position)
	}
	return strings.Join(text, ", ")
}

// insertRefusesRepeatedIDs reports whether a BATCH_PUT into a bucket of this
// type is an insert keyed by _id_. A hash or btree bucket stores one document
// per _id_, so a repeat inside the batch is a mistake to refuse. A queue or a
// heap takes each document as its own element (spec section 6.6 verb table:
// "per-item push, as today"), so a repeated _id_ there is two elements.
func insertRefusesRepeatedIDs(kind BucketType) bool {
	return kind == BucketTypeHash || kind == BucketTypeBTree
}

// refuseRepeatedIDs refuses a bulk insert that names one _id_ twice, or
// returns nil. A document without an _id_ is minted a distinct one later, so
// only named ids can repeat. The scan is O(documents) in one map, per request,
// never per bucket.
func refuseRepeatedIDs(documents []map[string]interface{}, database, bucket string) error {
	firstAt := make(map[string]int, len(documents))
	var repeated *repeatedIDError
	byID := map[string]int{}
	var refused []int
	for index, document := range documents {
		id, _ := document["_id_"].(string)
		if id == "" {
			continue
		}
		first, seen := firstAt[id]
		if !seen {
			firstAt[id] = index
			continue
		}
		if repeated == nil {
			repeated = &repeatedIDError{database: database, bucket: bucket}
		}
		slot, listed := byID[id]
		if !listed {
			slot = len(repeated.repeats)
			byID[id] = slot
			repeated.repeats = append(repeated.repeats, repeatedID{id: id, positions: []int{first + 1}})
		}
		repeated.repeats[slot].positions = append(repeated.repeats[slot].positions, index+1)
		refused = append(refused, index)
	}
	if repeated == nil {
		return nil
	}
	repeated.results = make([]WriteResult, len(documents))
	for index, document := range documents {
		id, _ := document["_id_"].(string)
		repeated.results[index] = WriteResult{ID: id, WriteOutcome: outcomeNotApplied}
	}
	for _, index := range refused {
		id, _ := documents[index]["_id_"].(string)
		repeated.results[index].Code = int32(transport.ErrDocumentExists)
		repeated.results[index].Message = fmt.Sprintf("the same `_id_` appears earlier in this batch at position %d", firstAt[id]+1)
	}
	return repeated
}

// appliedResults is the per-position answer of an applied bulk write: one
// applied entry per id, in request order. Where an id repeats, the store kept
// the last document, and every earlier position says which one replaced it.
func appliedResults(ids []string) []WriteResult {
	lastAt := make(map[string]int, len(ids))
	for index, id := range ids {
		lastAt[id] = index
	}
	results := make([]WriteResult, len(ids))
	for index, id := range ids {
		results[index] = WriteResult{ID: id, WriteOutcome: outcomeApplied}
		if last := lastAt[id]; last != index {
			results[index].Message = fmt.Sprintf("replaced by the document at position %d of this batch", last+1)
		}
	}
	return results
}
