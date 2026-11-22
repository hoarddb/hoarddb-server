package server

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/store"
)

// defaultScanLimit is the page size a ScanRequest gets when Limit is 0. It is
// documented in the "hql-spec" user-docs page; the CLI sends 0 and carries no second
// copy of the number, so this is the one place the default is decided.
const defaultScanLimit = 100

// ErrMultiFieldSort is returned when a scan asks to sort by more than one
// field. Only single-field sort is implemented, and a sort spec is a map, so
// picking "the first" field would pick whichever one Go's randomised map
// iteration yielded on that call — the same query would come back in
// different orders. Refusing is the honest answer until a real
// comparator chain exists; the feature is a post-1.0 candidate, because it
// also needs a settled answer on index-assisted prefix sorting.
var ErrMultiFieldSort = errors.New("sorting by more than one field is not supported yet")

// Scan returns one page of a bucket's documents in the store's natural
// order, after applying filter, plus the total number of matches.
//
// Order per store type, per the scan-find-all-spec record 4.2: hash and
// btree ascending key, fifo oldest first, lifo newest first, heap highest
// priority first. A blob bucket refuses, because a blob's value is binary
// content rather than a document. Total counts every match in the bucket, not
// just the page, so the CLI's "showing N of M" line is truthful.
//
// A scan reads one snapshot, opened as the name resolved (resolveForRead), so
// concurrent scans of one bucket do not serialise against each other and a
// writer cannot move the page mid-read.
func (s *Server) Scan(req ScanRequest) (*ScanResponse, error) {
	return s.ScanCtx(context.Background(), req)
}

// ScanCtx is Scan under a context, which its walk observes, and a result
// budget, which bounds what it holds to the node's ceiling (query-pipelining
// spec §3.2): the page's documents, and for a sorted scan the keys it ranks.
func (s *Server) ScanCtx(ctx context.Context, req ScanRequest) (*ScanResponse, error) {
	bucket, snap, err := s.resolveForRead(req.Database, req.Bucket)
	if err != nil {
		// A client that asked to continue a scan of a particular bucket is
		// told its bucket is gone, not that the name has no rows (T-25).
		if req.ExpectBucketID != 0 && s.nameNoLongerResolves(req.Database, err) {
			return nil, &bucketGenerationError{database: req.Database, bucket: req.Bucket}
		}
		// A missing database or bucket is the "does not exist" half of the
		// Found contract, not a failure of the scan.
		return &ScanResponse{Found: false, Database: req.Database, Bucket: req.Bucket, Message: err.Error()}, nil
	}
	defer snap.Discard()

	// resolveForRead built the view from the id the name resolved to, so this
	// is the generation the page is served from.
	bucketID := int64(bucket.id.Load())
	if req.ExpectBucketID != 0 && req.ExpectBucketID != bucketID {
		return nil, &bucketGenerationError{database: req.Database, bucket: req.Bucket}
	}

	limit := req.Limit
	if limit <= 0 {
		limit = defaultScanLimit
	}
	skip := req.Skip
	if skip < 0 {
		skip = 0
	}

	// Refuse a malformed sort before walking the bucket: the request is bad
	// whatever the data says, and the caller should not pay for a full scan
	// to be told so.
	sortField, sortAscending, err := singleSortField(req.Sort)
	if err != nil {
		return nil, err
	}
	var sortPath fieldPath
	if sortField != "" {
		if sortPath, err = parseFieldPath(sortField); err != nil {
			return nil, fmt.Errorf("the sort key %w. Write a path as names joined by \".\", with 0-based positions in brackets, for example {sort: {\"Address.City\": 1}}", err)
		}
	}
	projection, err := compileProjection(req.Projection)
	if err != nil {
		return nil, err
	}

	// Substitute now() sentinels in the filter before matching
	if req.Filter != nil {
		substituted := substituteNowSentinels(req.Filter)
		if filter, ok := substituted.(map[string]interface{}); ok {
			req.Filter = filter
		}
	}

	if bucket.bucketType == BucketTypeBlob {
		return nil, wrongTypeSentence(fmt.Sprintf(
			`db.%s.find() does not scan blob buckets: a blob's value is binary content, not a document. Use db.%s.info("<key>") for one key's metadata, or db.%s.get("<key>") to read the content.`,
			req.Bucket, req.Bucket, req.Bucket), BucketTypeBlob)
	}

	budget := s.newResultBudget(queryMethodScan, resultTooLargeError{
		query: fmt.Sprintf("find() on %s.%s", req.Database, req.Bucket), shape: resultShapeFind,
		limit: limit, skip: skip, sortField: sortField,
	})
	defer budget.report()
	walk := newQueryWalk(ctx, queryMethodScan, req.Database, req.Bucket)

	var resp *ScanResponse
	switch {
	case len(req.Filter) == 0 && len(req.Sort) == 0 && len(req.Projection) == 0:
		// Only a filter, sort or projection needs a document's fields.
		// Without one — find() or find({}) — the stored bytes go to the wire
		// untouched, as Find's do; decoding them into a map only to re-encode
		// it cost ~2.6 µs and ~59 allocations per returned document.
		resp, err = scanRawPage(bucket, req, skip, limit, budget, walk)
	case sortField != "":
		resp, err = scanSortedPage(bucket, req, sortPage{path: sortPath, ascending: sortAscending, skip: skip, limit: limit, projection: projection}, budget, walk)
	default:
		resp, err = scanFilteredPage(bucket, req, filteredPage{skip: skip, limit: limit, projection: projection}, budget, walk)
	}
	if err != nil {
		return nil, err
	}
	if err := walk.check(); err != nil {
		return nil, err
	}
	resp.IndexUsed = indexUsedFullScan
	resp.BucketID = bucketID
	return resp, nil
}

// nameNoLongerResolves reports whether err from resolveForRead means the
// database or bucket name resolves to nothing now, as opposed to a store
// failure or a bucket held by another node.
func (s *Server) nameNoLongerResolves(database string, err error) bool {
	return errors.Is(err, ErrBucketNotFound) || s.serverDataAccessor().getDB(database) == nil
}

// bucketGenerationError is the refusal of a scan page that would not continue
// the previous one: the bucket it was taken from was dropped, and the name now
// resolves to a different bucket or to none (spec §3.1). It is an
// ErrBucketNotFound, so it reaches the wire as ErrNotFound, and its text is the
// spec's sentence alone, without the sentinel's prefix.
type bucketGenerationError struct{ database, bucket string }

func (e *bucketGenerationError) Error() string {
	return fmt.Sprintf(`Bucket %q in database %q was dropped and re-created since the previous page `+
		`(it is a different bucket now), so this page would not continue your scan. `+
		`Start again from the first page: find({}, {limit: %d}).`, e.bucket, e.database, defaultScanLimit)
}

func (e *bucketGenerationError) Is(target error) bool { return target == ErrBucketNotFound }

// scanFilteredPage answers a filtered or projected scan in natural order. It
// keeps only the page — each page match's stored bytes, or its projection —
// and counts the rest for Total, so it holds the page and never the match
// set.
func scanFilteredPage(bucket *inMemoryBucket, req ScanRequest, page filteredPage, budget *resultBudget, walk *queryWalk) (*ScanResponse, error) {
	// An empty page has always answered an empty array here, not null.
	documents := []bson.Raw{}
	filter := compileFilter(req.Filter)
	if err := filter.refusal(); err != nil {
		return nil, err
	}
	var total int64
	skip := page.skip
	pageEnd := int64(page.skip) + int64(page.limit)

	visit := func(rawEntry []byte) error {
		if rawEntry == nil {
			return nil
		}
		if err := walk.visit(); err != nil {
			return err
		}
		// The walk hands out this node's own stored bytes, so only their
		// framing is checked; the filter reads the fields it names in place.
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
		if total >= int64(skip) && total < pageEnd {
			encoded, err := pageDocument(document, page.projection, req)
			if err != nil {
				return err
			}
			if err := budget.chargeDocument(elementHeader(len(documents)) + int64(len(encoded))); err != nil {
				return err
			}
			documents = append(documents, encoded)
		}
		total++
		return nil
	}
	if err := walkBucketDocuments(bucket, req.Database, req.Bucket, visit); err != nil {
		return nil, err
	}

	return &ScanResponse{
		Found:     true,
		Database:  req.Database,
		Bucket:    req.Bucket,
		Documents: documents,
		Total:     total,
		HasMore:   pageEnd < total,
	}, nil
}

// filteredPage is the slice of a natural-order scan a filtered or projected
// find returns, and its projection, compiled once for the query (nil when it
// projects nothing).
type filteredPage struct {
	skip, limit int
	projection  *compiledProjection
}

// pageDocument is one page document as the wire carries it: the stored bytes
// when nothing is projected, and otherwise the projection of the document,
// which is decoded for it, so only a page document is ever decoded. The
// walked bytes are a copy (KVIterator.Value), so they are the page's to keep.
func pageDocument(document bson.Document, projection *compiledProjection, req ScanRequest) (bson.Raw, error) {
	if projection == nil {
		return bson.Raw(document.Bytes()), nil
	}
	var fields map[string]interface{}
	if err := document.Decode(&fields); err != nil {
		return nil, fmt.Errorf("decoding scanned document in %s.%s: %w", req.Database, req.Bucket, err)
	}
	encoded, err := bson.Marshal(projection.apply(fields))
	if err != nil {
		return nil, fmt.Errorf("encoding scanned document in %s.%s: %w", req.Database, req.Bucket, err)
	}
	return encoded, nil
}

// scanRawPage answers an unqualified scan: every stored document matches, the
// order is the store's natural order, and the page is handed to the wire as
// the stored BSON. Because every document matches, Total is the bucket's entry
// count, which every engine keeps in its state document; reading it from the
// page's own snapshot lets the walk stop at the page's end. The page costs
// O(skip + limit), not O(bucket).
func scanRawPage(bucket *inMemoryBucket, req ScanRequest, skip, limit int, budget *resultBudget, walk *queryWalk) (*ScanResponse, error) {
	var documents []bson.Raw
	total, err := pageBucketDocuments(bucket, req.Database+"."+req.Bucket, store.PageRequest{
		Skip:  skip,
		Limit: limit,
		Step:  walk.visit,
		Visit: func(rawEntry []byte) error {
			// KVIterator.Value returns a copy, so the bytes are the page's to keep.
			if err := budget.chargeDocument(elementHeader(len(documents)) + int64(len(rawEntry))); err != nil {
				return err
			}
			documents = append(documents, bson.Raw(rawEntry))
			return nil
		},
	})
	if err != nil {
		return nil, err
	}

	return &ScanResponse{
		Found:     true,
		Database:  req.Database,
		Bucket:    req.Bucket,
		Documents: documents,
		Total:     total,
		HasMore:   int64(skip)+int64(limit) < total,
	}, nil
}

// pageBucketDocuments reads one unfiltered page of a bucket in its natural
// order, with its entry count. The request's own errors come back unchanged,
// because they are the caller's, already worded for the client; only a store
// failure is wrapped, naming location.
func pageBucketDocuments(bucket *inMemoryBucket, location string, req store.PageRequest) (int64, error) {
	var callerErr error
	engineReq := store.PageRequest{
		Skip:  req.Skip,
		Limit: req.Limit,
		Step: func() error {
			callerErr = req.Step()
			return callerErr
		},
		Visit: func(entry []byte) error {
			callerErr = req.Visit(entry)
			return callerErr
		},
	}
	var total int64
	var err error
	switch bucket.bucketType {
	case BucketTypeHash:
		total, err = bucket.hashStore.Page(engineReq)
	case BucketTypeBTree:
		total, err = bucket.btreeStore.Page(engineReq)
	case BucketTypeFIFO, BucketTypeLIFO:
		total, err = bucket.queueStore.Page(engineReq)
	case BucketTypeHeap:
		total, err = bucket.heapStore.Page(engineReq)
	default:
		return 0, unsupportedForTypeErr("scan", bucket.bucketType)
	}
	if callerErr != nil {
		return 0, callerErr
	}
	if err != nil {
		return 0, fmt.Errorf("reading a page of %s: %w", location, err)
	}
	return total, nil
}

// walkBucketDocuments calls visit with each stored document's BSON bytes, in
// the store's natural order (the scan-find-all-spec record 4.2): hash and
// btree ascending key, fifo oldest first, lifo newest first, heap highest
// priority first.
func walkBucketDocuments(bucket *inMemoryBucket, database, bucketName string, visit func(rawEntry []byte) error) error {
	switch bucket.bucketType {
	case BucketTypeHash:
		if err := walkHashDocuments(bucket, database+"."+bucketName, visit); err != nil {
			return err
		}

	case BucketTypeBTree:
		if err := walkBTreeDocuments(bucket, database+"."+bucketName, visit); err != nil {
			return err
		}

	case BucketTypeFIFO, BucketTypeLIFO:
		if err := bucket.queueStore.Walk(visit); err != nil {
			return err
		}

	case BucketTypeHeap:
		if err := bucket.heapStore.Walk(visit); err != nil {
			return err
		}

	default:
		return unsupportedForTypeErr("scan", bucket.bucketType)
	}
	return nil
}

// walkHashDocuments calls visit with each stored document of a hash bucket, in
// ascending key order, from one iterator over one snapshot. Listing the keys
// and then reading each one cost a point lookup per key — and, on a bucket
// that is not a read view, a snapshot per key. The walk hands out
// the stored bytes only: every caller finds the key it needs in the document.
//
// A visit's error comes back unchanged, because it is the caller's own and
// already worded for the client; only a store failure is wrapped, naming
// location.
func walkHashDocuments(bucket *inMemoryBucket, location string, visit func(rawEntry []byte) error) error {
	var visitErr error
	walkErr := bucket.hashStore.IterateDocuments(func(_, rawEntry []byte) error {
		visitErr = visit(rawEntry)
		return visitErr
	})
	if visitErr != nil {
		return visitErr
	}
	if walkErr != nil {
		return fmt.Errorf("reading the documents of %s: %w", location, walkErr)
	}
	return nil
}

// walkBTreeDocuments is walkHashDocuments for a btree bucket: visit sees each
// stored document in key order, one at a time, over one snapshot, so the walk
// retains one document rather than the bucket, as Range(nil, nil, 0) would
// (query-pipelining spec §3.2). IterateDocuments wraps a visit's error in
// "iterating key %q:", so the visit's own error is captured and returned in
// its place: it is the caller's, already worded for the client.
func walkBTreeDocuments(bucket *inMemoryBucket, location string, visit func(rawEntry []byte) error) error {
	var visitErr error
	walkErr := bucket.btreeStore.IterateDocuments(func(_, rawEntry []byte) error {
		visitErr = visit(rawEntry)
		return visitErr
	})
	if visitErr != nil {
		return visitErr
	}
	if walkErr != nil {
		return fmt.Errorf("reading the documents of %s: %w", location, walkErr)
	}
	return nil
}

// singleSortField reduces a sort spec to the one field a sorted scan can
// order by. An empty spec means "no sort" and yields an empty field name; a
// spec naming two or more fields is refused, since the map has no order to
// choose from. Direction is 1 for ascending, anything else descending, which
// is the spec's {field: 1/-1} form.
func singleSortField(sortSpec map[string]int) (field string, ascending bool, err error) {
	switch len(sortSpec) {
	case 0:
		return "", false, nil
	case 1:
		for name, direction := range sortSpec {
			return name, direction > 0, nil
		}
	}

	// Sorted, so the message reads the same on every call — a message whose
	// field order varied would repeat the defect it is reporting.
	names := make([]string, 0, len(sortSpec))
	for name := range sortSpec {
		names = append(names, name)
	}
	sort.Strings(names)

	return "", false, fmt.Errorf(
		"%w: this query sorts by %s. To fix it, sort by one field (for example {sort: {%s: %d}}), or sort the result in your client",
		ErrMultiFieldSort, strings.Join(names, ", "), names[0], sortSpec[names[0]])
}
