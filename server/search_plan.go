package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/store"
)

// search: the planner's index, or a scan that says so (GAP-14 build doc
// Steps 11–13, S-8 — recommendation taken: a filter no index serves is
// answered by a full scan, with the limit applied and IndexUsed saying so,
// which is what find does and what the field exists to make visible).

// indexUsedFullScan is IndexUsed for an answer no index served.
const indexUsedFullScan = "none — full scan"

// errSearchLimitReached stops a scan once it has seen a match past the search's limit.
var errSearchLimitReached = errors.New("search limit reached")

// searchPlanned answers a search under a context and a result budget. Once it
// holds Limit results it looks for one further real match and stops there,
// setting HasMore if it finds one; Total stays the number returned, because a
// match count would mean fetching every candidate. The ids an index returns
// stay outside the budget (R-4's stated exception): they are the index's
// answer, not the query's result.
func (s *Server) searchPlanned(ctx context.Context, req SearchRequest) (*SearchResponse, error) {
	if len(req.Filter) == 0 {
		return &SearchResponse{Message: "filter required"}, nil
	}

	bucket, snap, err := s.resolveForRead(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	defer snap.Discard()

	var plan *store.SearchPlan
	if bucket.idxMgr != nil {
		plan, err = bucket.idxMgr.PlanSearch(req.Filter)
		if err != nil {
			return &SearchResponse{Message: s.readFailureMessage(req.Database, req.Bucket, "search failed", err)}, nil
		}
	}

	budget := s.newResultBudget(queryMethodSearch, resultTooLargeError{
		query: fmt.Sprintf("search() on %s.%s", req.Database, req.Bucket), shape: resultShapeFind, limit: req.Limit,
	})
	defer budget.report()
	walk := newQueryWalk(ctx, queryMethodSearch, req.Database, req.Bucket)
	collected := &searchResults{limit: req.Limit, budget: budget}

	used := indexUsedFullScan
	if plan == nil {
		err = s.searchByScan(bucket, req, walk, collected)
	} else {
		used = plan.Spec.Name
		err = s.searchByIndex(bucket, req, plan, walk, collected)
	}
	if err != nil {
		s.logKeyReadFailure(err)
		return nil, err
	}
	if err := walk.check(); err != nil {
		s.logKeyReadFailure(err)
		return nil, err
	}
	return &SearchResponse{Results: collected.results, Total: len(collected.results), HasMore: collected.hasMore, IndexUsed: used}, nil
}

// searchResults holds a search's answer. Nothing here reads a hit's fields
// beyond the filter, so each stored document goes to the wire as its stored
// BSON, as Find's does — decoding it into a map only to re-encode it cost
// ~2.6 µs and ~59 allocations per hit.
type searchResults struct {
	limit   int
	budget  *resultBudget
	results []bson.Raw
	// hasMore is set when a real match arrived after the limit was reached.
	hasMore bool
}

func (r *searchResults) full() bool { return r.limit > 0 && len(r.results) == r.limit }

// offer takes one document that matched the filter. At the limit it keeps
// nothing and records that a further match exists, and reports done so the
// caller stops: one match past the limit is all HasMore needs.
func (r *searchResults) offer(raw []byte) (done bool, err error) {
	if r.full() {
		r.hasMore = true
		return true, nil
	}
	return false, r.add(raw)
}

func (r *searchResults) add(raw []byte) error {
	if err := r.budget.chargeDocument(elementHeader(len(r.results)) + int64(len(raw))); err != nil {
		return err
	}
	r.results = append(r.results, bson.Raw(raw))
	return nil
}

// searchByIndex resolves the documents under the plan's prefix and re-applies
// every predicate of the filter to each — the ones the index covers too, with
// the equality find uses — so an entry that disagrees with its document is
// never an answer (spec §5.3). A document two entries name is
// answered once.
func (s *Server) searchByIndex(bucket *inMemoryBucket, req SearchRequest, plan *store.SearchPlan, walk *queryWalk, collected *searchResults) error {
	// A limit refusal belongs to the filter, not the data: answer it before
	// the index or any document is read (BUG-0690).
	filter := compileFilter(req.Filter)
	if err := filter.refusal(); err != nil {
		return err
	}
	// The index and the documents are read from one snapshot, so every id
	// the index returns names a document the same view holds, or none.
	dbIDs, err := plan.DocumentKeys()
	if err != nil {
		return fmt.Errorf("searching index %s: %w", plan.Spec.Name, err)
	}
	served := make(map[string]bool, len(dbIDs))
	for _, id := range dbIDs {
		key := string(id)
		if served[key] {
			continue
		}
		served[key] = true
		if err := walk.visit(); err != nil {
			return err
		}
		raw, err := bucket.hashStore.Get(key)
		if err != nil {
			return fmt.Errorf("reading document %q that index %s names: %w", key, plan.Spec.Name, err)
		}
		if raw == nil {
			s.noteOrphanEntry(req, plan.Spec.Name, key)
			continue
		}
		matched, err := residualMatches(raw, filter)
		if err != nil {
			return err
		}
		if !matched {
			continue
		}
		done, err := collected.offer(raw)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
	return nil
}

// residualMatches reports whether a stored document satisfies filter, with
// find's matcher. The candidate is this node's own stored bytes, read from
// the bucket's snapshot, so only their framing is checked, and the compiled
// filter reads the values its paths resolve to in place: a hit is never
// decoded, whether the filter names fields or holds a top-level $and or $or.
func residualMatches(raw []byte, filter *compiledFilter) (bool, error) {
	document, err := bson.TrustStored(raw)
	if err != nil {
		return false, fmt.Errorf("decoding a search hit: %w", err)
	}
	return filter.matchesDocument(document)
}

// searchByScan answers a filter no index serves by walking the bucket and
// matching every document, as find does, up to the limit.
func (s *Server) searchByScan(bucket *inMemoryBucket, req SearchRequest, walk *queryWalk, collected *searchResults) error {
	filter := compileFilter(req.Filter)
	if err := filter.refusal(); err != nil {
		return err
	}
	err := walkBucketDocuments(bucket, req.Database, req.Bucket, func(raw []byte) error {
		if raw == nil {
			return nil
		}
		if err := walk.visit(); err != nil {
			return err
		}
		matched, err := residualMatches(raw, filter)
		if err != nil || !matched {
			return err
		}
		done, err := collected.offer(raw)
		if err != nil {
			return err
		}
		if done {
			return errSearchLimitReached
		}
		return nil
	})
	if err != nil && !errors.Is(err, errSearchLimitReached) {
		return err
	}
	return nil
}

// noteOrphanEntry counts an index entry whose document does not exist and
// logs the first one per bucket per start, naming the index and the document
// key — never the value (RD-10). It is not repaired here: a read path that
// deleted what it could not resolve would race the writer about to resolve
// it (spec §6.5).
func (s *Server) noteOrphanEntry(req SearchRequest, index, key string) {
	indexOrphanSkips.Inc()
	if _, warned := s.orphanWarned.LoadOrStore(req.Database+"/"+req.Bucket, true); warned {
		return
	}
	s.logger.Warn(fmt.Sprintf("Index %s of bucket %q in database %q names document %q, which does not exist; search skipped it. "+
		"Further skips in this bucket are counted in hoarddb_index_orphan_skips_total, not logged, until the node restarts.",
		index, req.Bucket, req.Database, key),
		"database", req.Database, "bucket", req.Bucket, "index", index, "document", key)
}
