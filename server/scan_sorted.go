package server

import (
	"errors"
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
)

// sortedScanPassHook, when set, runs for every entry each pass of a sorted
// scan visits, with the entry's position in the walk. T-5 uses it to prove
// the two passes read the same entry at every position. Nil in production.
var sortedScanPassHook atomic.Pointer[func(bucket string, pass int, position int64, rawEntry []byte)]

// errSortedPageComplete ends pass 2's walk once it is past the last position
// the page needs. It never leaves scanSortedPage.
var errSortedPageComplete = errors.New("the sorted page is complete")

// sortPage is what a sorted scan asks for: the field and direction it ranks
// by, and the slice of the ranking it returns.
type sortPage struct {
	path      fieldPath
	ascending bool
	skip      int
	limit     int
	// projection is the query's, compiled once; nil when it projects nothing.
	projection *compiledProjection
}

// documentSortKey is a stored document's sort key, read from its bytes with
// the resolver, and what the budget charges for holding it (spec §3.2 as
// corrected in §13.5): the value's bytes plus 16 for the candidate that holds
// it. A missing field costs the 16.
func documentSortKey(document bson.Document, lookup bson.Path) (sortKey, int64, error) {
	view, found, err := document.Lookup(lookup)
	if err != nil {
		return sortKey{}, 0, fmt.Errorf("resolving the sort key: %w", err)
	}
	return sortKeyOfView(view, found), int64(len(view.Bytes())) + 16, nil
}

// sortKeyOfView classifies a stored value as sortKeyOf classifies its decode
// into map[string]interface{}: the decode gives int32, int64 and float64 for
// the numbers, primitive.DateTime for a date, nil for null, a map and
// primitive.A for a document and an array, and for every other type the
// primitive type whose BSON type byte otherBSONType names, which is the
// stored type byte itself.
func sortKeyOfView(view bson.View, found bool) sortKey {
	if !found {
		return sortKey{class: sortRankMissing << 8}
	}
	switch view.Type() {
	case bsontype.Null:
		return sortKey{class: sortRankMissing << 8}
	case bsontype.DateTime:
		return sortKey{class: sortRankDate << 8, millis: dateTimeMillis(view)}
	case bsontype.Double, bsontype.Int32, bsontype.Int64:
		return sortKey{class: sortRankNumber << 8, number: viewNumber(view)}
	case bsontype.String:
		return sortKey{class: sortRankString << 8, text: string(stringBytes(view))}
	case bsontype.Boolean:
		return sortKey{class: sortRankBoolean << 8, flag: booleanOf(view)}
	case bsontype.EmbeddedDocument:
		return sortKey{class: sortRankDocument << 8}
	case bsontype.Array:
		return sortKey{class: sortRankArray << 8}
	}
	return sortKey{class: sortRankOther<<8 | uint16(view.Type())}
}

// scanSortedPage answers a sorted scan in two passes over the one snapshot the
// scan opened (query-pipelining spec §3.2, "Two passes over the one
// snapshot"). Pass 1 filters every entry on its stored bytes, counts the matches, and
// keeps only the skip+limit candidates that sort first, as (key, position).
// Pass 2 walks the same snapshot again and takes the page's documents by
// position. Sorting used to decode and keep every match and sort them all;
// this keeps skip+limit small keys and limit documents.
//
// Position is the index of an entry among the non-nil entries the walk
// visits, counted the same way in both passes. The view's engines read one
// snapshot (resolve.go:snapshotKV), so both walks see the same entries in the
// same order.
//
// The budget holds each kept key while pass 1 ranks — its raw value's bytes
// plus 16, released when a better key evicts it — and then the page's
// documents as pass 2 takes them. A refusal in pass 1 is the ranking shape of
// W-2's message; in pass 2, the find shape.
func scanSortedPage(bucket *inMemoryBucket, req ScanRequest, order sortPage, budget *resultBudget, walk *queryWalk) (*ScanResponse, error) {
	passHook := sortedScanPassHook.Load()
	capacity := int64(order.skip) + int64(order.limit)
	selected := &sortCandidateHeap{ascending: order.ascending}
	filter := compileFilter(req.Filter)
	if err := filter.refusal(); err != nil {
		return nil, err
	}
	lookup := libraryPath(order.path)
	budget.refusal.shape = resultShapeRanking
	var position, total int64

	rankMatches := func(rawEntry []byte) error {
		if rawEntry == nil {
			return nil
		}
		if err := walk.visit(); err != nil {
			return err
		}
		current := position
		position++
		if passHook != nil {
			(*passHook)(req.Bucket, 1, current, rawEntry)
		}
		// The walk hands out this node's own stored bytes, so only their
		// framing is checked; the filter and the key read them in place.
		document, err := bson.TrustStored(rawEntry)
		if err != nil {
			return fmt.Errorf("decoding scanned document in %s.%s: %w", req.Database, req.Bucket, err)
		}
		matched, err := filter.matchesDocument(document)
		if err != nil {
			return err
		}
		if !matched {
			return nil
		}
		total++
		key, keyBytes, err := documentSortKey(document, lookup)
		if err != nil {
			return fmt.Errorf("sorting %s.%s: %w", req.Database, req.Bucket, err)
		}
		return selected.offer(sortCandidate{key: key, position: current, keyBytes: keyBytes}, capacity, budget)
	}
	if err := walkBucketDocuments(bucket, req.Database, req.Bucket, rankMatches); err != nil {
		return nil, err
	}

	ranked := selected.candidates
	sort.Slice(ranked, func(i, j int) bool { return sortsBefore(ranked[i], ranked[j], order.ascending) })
	var page []sortCandidate
	if order.skip < len(ranked) {
		page = ranked[order.skip:]
	}

	budget.refusal.shape = resultShapeFind
	documents, err := collectSortedPage(bucket, req, page, order.projection, budget, walk)
	if err != nil {
		return nil, err
	}

	var message string
	if total > 100 {
		message = fmt.Sprintf("note: sorting %d documents without an index", total)
	}
	return &ScanResponse{
		Found:     true,
		Database:  req.Database,
		Bucket:    req.Bucket,
		Documents: documents,
		Total:     total,
		HasMore:   total > capacity,
		Message:   message,
	}, nil
}

// collectSortedPage is pass 2: it walks the snapshot again and returns the
// documents at page's positions, in page's order, projected when the request
// asks for it. It stops after the largest position the page needs.
func collectSortedPage(bucket *inMemoryBucket, req ScanRequest, page []sortCandidate, projection *compiledProjection, budget *resultBudget, walk *queryWalk) ([]bson.Raw, error) {
	if len(page) == 0 {
		return nil, nil
	}
	passHook := sortedScanPassHook.Load()
	pageIndex := make(map[int64]int, len(page))
	var lastPosition int64
	for index, candidate := range page {
		pageIndex[candidate.position] = index
		lastPosition = max(lastPosition, candidate.position)
	}

	documents := make([]bson.Raw, len(page))
	found := 0
	var position int64
	takePage := func(rawEntry []byte) error {
		if rawEntry == nil {
			return nil
		}
		if err := walk.visit(); err != nil {
			return err
		}
		current := position
		position++
		if passHook != nil {
			(*passHook)(req.Bucket, 2, current, rawEntry)
		}
		if index, ok := pageIndex[current]; ok {
			encoded, err := sortedPageDocument(rawEntry, projection, req)
			if err != nil {
				return err
			}
			if err := budget.chargeDocument(elementHeader(index) + int64(len(encoded))); err != nil {
				return err
			}
			documents[index] = encoded
			found++
		}
		if current >= lastPosition {
			return errSortedPageComplete
		}
		return nil
	}
	if err := walkBucketDocuments(bucket, req.Database, req.Bucket, takePage); err != nil && !errors.Is(err, errSortedPageComplete) {
		return nil, err
	}
	// Both passes read one snapshot, so every position pass 1 selected is
	// there in pass 2. If a store's walk ever stopped being a pure function of
	// the snapshot, this is where it would show, as an error rather than a
	// page with holes in it.
	if found != len(page) {
		return nil, fmt.Errorf("sorting %s.%s: the second pass over the snapshot found %d of the page's %d documents; please report this as a bug",
			req.Database, req.Bucket, found, len(page))
	}
	return documents, nil
}

// sortedPageDocument is one page document as the wire carries it: the stored
// bytes when nothing is projected, and the projected document otherwise.
// The walked bytes are a copy (KVIterator.Value), so they are the page's to
// keep.
func sortedPageDocument(rawEntry []byte, projection *compiledProjection, req ScanRequest) (bson.Raw, error) {
	if projection == nil {
		return bson.Raw(rawEntry), nil
	}
	var document map[string]interface{}
	if err := bson.Unmarshal(rawEntry, &document); err != nil {
		return nil, fmt.Errorf("decoding scanned document in %s.%s: %w", req.Database, req.Bucket, err)
	}
	encoded, err := bson.Marshal(projection.apply(document))
	if err != nil {
		return nil, fmt.Errorf("encoding scanned document in %s.%s: %w", req.Database, req.Bucket, err)
	}
	return encoded, nil
}
