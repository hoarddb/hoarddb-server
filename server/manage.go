package server

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/hoarddb/hoarddb-server/ids"
	"sort"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// noDatabaseErr builds the "no such database" message: per AGENTS.md rule 3
// every error must tell the developer how to recover.
func noDatabaseErr(name string) string {
	return fmt.Sprintf(
		"database %q not found on this node. Run 'show databases' to list what exists, or 'create database %s' to create it",
		name, name)
}

// noBucketErr builds the "no such bucket" recovery message. It names the
// real create syntax — with `in <db>` — because the CLI adds that clause;
// the server cannot enumerate the types the CLI accepts (cli imports server,
// not the other way round), so the hint says hash and defers the list.
func noBucketErr(database, bucket string) string {
	bucket = displayBucket(bucket)
	return fmt.Sprintf(
		"bucket %q not found in database %q. Run 'show buckets in %s' to list what exists, or 'create bucket %s in %s { type: hash }' to create it",
		bucket, database, database, bucket, database)
}

// notHolderErr builds the "this node is not a holder" message for a bucket the
// ring places elsewhere. It exists because noBucketErr is a wrong answer that
// looks like a right one here: a client that asked a non-holder for a full
// bucket is told the bucket does not exist, and an empty result set from a
// misrouted read is indistinguishable from a correct one.
//
// The owner's address is labelled as the cluster address, not offered as a
// dial string: ring.Node.Addr is Server.Listen (4433), while a driver client
// connects to that node's TCP.Listen (7433), which this node has no way to
// know. Naming the node and pointing at 'nodes' is the honest hint. 'nodes'
// and 'create bucket <name> in <db>' are both real verbs
// (cli/parser.go:Parse) -- the CLI has no 'ring' verb to send anyone to.
func notHolderErr(database, bucket string, owner ring.Node) string {
	bucket = displayBucket(bucket)
	return fmt.Sprintf(
		"bucket %q in database %q is held by node %s (cluster address %s) and its replicas; this node does not hold it, "+
			"so it cannot answer for it. Connect to that node and retry — run 'nodes' to see every member and its address. "+
			"If the bucket is absent there too it was never created: 'create bucket %s in %s { type: hash }'",
		bucket, database, owner.ID, owner.Addr, bucket, database)
}

// bucketHeldElsewhere reports the ring owner of a bucket that is missing
// locally and that this node is not a designated holder of. Placement is a
// hash of the bucket's (database, bucket) placement key, so the answer costs
// no network hop and is available for a bucket this node has never seen.
//
// The gate is Hosts, not "owner is someone else": under a replication factor
// above 1 this node can be a replica without being the owner, and a replica
// with no local copy is the join fence's condition
// (cluster_join.go:ErrJoinSyncIncomplete), not a misrouted read. Telling that
// operator to connect to another node would send them away from the node that
// is supposed to have the data.
func (s *Server) bucketHeldElsewhere(database, bucket string) (ring.Node, bool) {
	key := placementOf(database, bucket)
	if !s.cluster.Enabled() || s.cluster.Hosts(key) {
		return ring.Node{}, false
	}
	owner, err := s.cluster.Owner(key)
	if err != nil {
		// An empty ring has no placement to report. The plain "not found"
		// message is still correct, and more useful than a ring diagnostic.
		s.logger.Debug("ring owner lookup for a bucket missing locally failed",
			"bucket", bucket, "error", err)
		return ring.Node{}, false
	}
	return owner, true
}

// wrongTypeErr explains that an operation needs a different bucket type. The
// sentence is the whole answer, so it is carried as is under ErrWrongBucketType:
// the code tells a script the caller sent the wrong verb (INVALID_ARGUMENT),
// where an unclassified error would read as a server fault (INTERNAL).
func wrongTypeErr(bucket string, got BucketType, wants ...BucketType) error {
	names := make([]string, 0, len(wants))
	for _, w := range wants {
		names = append(names, string(w))
	}
	return wrongTypeSentence(fmt.Sprintf("bucket %q is type %s; this operation requires type %s.",
		bucket, got, strings.Join(names, " or ")), got)
}

// unsupportedForTypeErr is the refusal of a walk-based verb (distinct, a
// filtered count, multi-update, scan) for a store type it does not walk. The
// sentence is the one these refusals always had; only its class is new.
func unsupportedForTypeErr(verb string, got BucketType) error {
	return wrongTypeSentence(fmt.Sprintf("%s is not supported for bucket type %s.", verb, got), got)
}

// afterResolveHook, when set, runs in lockBucket once a name has resolved,
// before the writer locks the bucket. A test holds a writer there across a
// drop. Nil in production.
var afterResolveHook atomic.Pointer[func(database, bucket string)]

// requireNames refuses an empty database or bucket name with the fix.
func requireNames(database, bucket string) error {
	if database == "" {
		return fmt.Errorf("no database selected — run 'use <database>' first, or pass 'in <database>'")
	}
	if bucket == "" {
		return fmt.Errorf("no bucket given — use db.<bucket>.<operation>(), e.g. db.users.count()")
	}
	return nil
}

// missingBucketErr is the error for a name this node does not resolve: the
// node that holds it when the ring puts it elsewhere, not found otherwise.
func (s *Server) missingBucketErr(database, bucket string) error {
	if owner, heldElsewhere := s.bucketHeldElsewhere(database, bucket); heldElsewhere {
		return &sentencedError{message: notHolderErr(database, bucket, owner), cause: ErrNotHolder}
	}
	return bucketNotFoundErr(database, bucket)
}

// ListDatabases returns every database on this node with the number of
// buckets it holds in each.
func (s *Server) ListDatabases(_ ListDatabasesRequest) (*ListDatabasesResponse, error) {
	return s.listDatabases(false)
}

// listDatabases is ListDatabases, counting with ownedCounts on a cluster member
// only the buckets this node is the ring owner of (memberListDatabasesRequest).
func (s *Server) listDatabases(ownedCounts bool) (*ListDatabasesResponse, error) {
	data := s.serverDataAccessor()

	data.mu.RLock()
	names := make([]string, 0, len(data.dbs))
	for name := range data.dbs {
		names = append(names, name)
	}
	data.mu.RUnlock()

	sort.Strings(names)

	resp := &ListDatabasesResponse{Databases: make([]DatabaseInfo, 0, len(names))}
	for _, name := range names {
		count := 0
		if db := data.getDB(name); db != nil {
			var err error
			if count, err = s.bucketCountOf(name, db, ownedCounts); err != nil {
				return nil, err
			}
		}
		resp.Databases = append(resp.Databases, DatabaseInfo{
			Name:        name,
			BucketCount: count,
		})
	}
	if len(resp.Databases) == 0 {
		resp.Message = NoDatabasesMessage
	}
	return resp, nil
}

// bucketCountOf is the number of buckets of database db that ListDatabases
// reports: all it holds, or with owned on a cluster member, those whose ring
// owner it is. The owned count walks the database's names, O(buckets held);
// the summed owned counts of every member are the cluster's count with no
// bucket counted twice, which the held counts are not at a factor above 1.
func (s *Server) bucketCountOf(name string, db *inMemoryDB, owned bool) (int, error) {
	if !owned || !s.cluster.Enabled() {
		db.mu.RLock()
		defer db.mu.RUnlock()
		return db.table.clientCount(), nil
	}
	count := 0
	var ownerErr error
	db.table.scan(func(step []string) {
		for _, bucket := range step {
			if hasPartSeparator(bucket) {
				continue
			}
			owner, err := s.cluster.Owner(placementOf(name, bucket))
			if err != nil {
				ownerErr = fmt.Errorf("placing bucket %q of database %q to count it: %w", bucket, name, err)
				return
			}
			if owner.ID == s.cluster.self.ID {
				count++
			}
		}
	})
	return count, ownerErr
}

// ListBuckets returns every bucket in a database with its type and entry count.
func (s *Server) ListBuckets(req ListBucketsRequest) (*ListBucketsResponse, error) {
	if req.Database == "" {
		return nil, fmt.Errorf("database required — run 'show buckets in <database>'")
	}
	data := s.serverDataAccessor()
	db := data.getDB(req.Database)
	if db == nil {
		return nil, databaseNotFoundErr(req.Database)
	}

	var names []string
	db.table.scan(func(step []string) {
		names = append(names, step...)
	})
	sort.Strings(names)

	resp := &ListBucketsResponse{
		Found:    true,
		Database: req.Database,
		Buckets:  make([]BucketInfo, 0, len(names)),

		Encrypted: s.encryptedLabel(),
	}
	for _, name := range names {
		// A part is not a bucket a client made: its collection is listed once.
		if hasPartSeparator(name) {
			continue
		}
		// The declared factor is on the bucket itself: resolveForRead hands
		// back a per-request view that copies only what a read needs.
		facts, held := s.peekBucketIn(req.Database, db, name)
		if !held {
			// Dropped since the names were read.
			continue
		}
		declared := int(facts.replicationFactor)
		b, snap, err := s.resolveForRead(req.Database, name)
		if errors.Is(err, ErrBucketNotFound) {
			// Dropped since the names were read, or not yet served.
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("listing bucket %q: %w", name, err)
		}
		counts, err := bucketCountsOf(b)
		snap.Discard()
		if err != nil {
			return nil, fmt.Errorf("counting bucket %q: %w", name, err)
		}
		resp.Buckets = append(resp.Buckets, s.bucketInfoOf(req.Database, name, b, counts, declared))
	}
	if len(resp.Buckets) == 0 {
		resp.Message = fmt.Sprintf(
			"database %q has no buckets yet — create one with 'create bucket <name> in %s { type: hash }'",
			req.Database, req.Database)
	}
	return resp, nil
}

// effectiveFactorOf is the number of copies this node reports for a bucket
// that declared declaredFactor (0 when it declared none): its own, else the
// node's configured default. BucketInfo and BucketStats share it so the two
// answers cannot disagree.
func (s *Server) effectiveFactorOf(declaredFactor int) int {
	if declaredFactor == 0 {
		return max(1, s.config.Server.ReplicationFactor)
	}
	return declaredFactor
}

// bucketInfoOf is one bucket's listing row, from the view the caller resolved,
// its entry count and the replication factor it declared (0 when it declared
// none). ListBuckets and BucketInfo both build the row here so the two cannot
// drift.
func (s *Server) bucketInfoOf(database, name string, view *inMemoryBucket, counts bucketCounts, declaredFactor int) BucketInfo {
	effective := s.effectiveFactorOf(declaredFactor)
	owners := s.replicationViewOf(database, name)
	return BucketInfo{
		Name:    displayBucket(name),
		Type:    view.bucketType,
		Entries: counts.entries,
		Pending: counts.delivery.Pending,
		Dead:    counts.delivery.Dead,
		Stores:  storeNames(view),

		ReplicationFactor: effective,
		Inherited:         declaredFactor == 0,
		Outside:           s.cluster != nil && s.cluster.placementMarksOf(placementOf(database, name)).Outside != 0,

		InSync:        owners.inSync,
		State:         owners.state,
		NotInSync:     owners.notInSync,
		PendingCopies: owners.pending,
	}
}

// BucketInfo describes one bucket's declaration: what ListBuckets says of it,
// without listing the database. It reads the bucket's state document for the
// entry count, the catalog for the declared names, engines and factor, and nothing
// else: no store, index or element is walked, so it costs O(declared names)
// whatever the stores hold, and a caller may ask on every read command.
// BucketStats is the call that counts elements.
func (s *Server) BucketInfo(req BucketInfoRequest) (*BucketInfoResponse, error) {
	view, snap, err := s.resolveForRead(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	defer snap.Discard()
	facts, held := s.peekBucket(req.Database, req.Bucket)
	if !held {
		// Dropped since the view was resolved.
		return nil, bucketNotFoundErr(req.Database, req.Bucket)
	}
	counts, err := bucketCountsOf(view)
	if err != nil {
		return nil, fmt.Errorf("counting the entries of bucket %q in database %q: %w", req.Bucket, req.Database, err)
	}
	return &BucketInfoResponse{
		Found:    true,
		Database: req.Database,
		Bucket:   s.bucketInfoOf(req.Database, req.Bucket, view, counts, int(facts.replicationFactor)),

		StoreFields: storeFieldInfos(view),
	}, nil
}

// BucketStats describes one bucket: type, entry count, bytes on disk, indexes.
func (s *Server) BucketStats(req BucketStatsRequest) (*BucketStatsResponse, error) {
	b, snap, err := s.resolveForRead(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	defer snap.Discard()

	entries, err := countBucketEntries(b)
	if err != nil {
		return nil, fmt.Errorf("counting the entries of bucket %q in database %q: %w", req.Bucket, req.Database, err)
	}

	indexes := describeIndexes(b.declaredIndexes())

	// A bucket has no directory of its own to measure: its size is the logical
	// figure its state document keeps (spec §11 Q22), read from the snapshot
	// the entry count came from.
	state, err := store.LoadBucketState(snap, b.slot, b.databaseID, b.id.Load())
	if err != nil {
		return nil, fmt.Errorf("reading the size of bucket %q in database %q: %w", req.Bucket, req.Database, err)
	}
	stores, err := storeStatsOf(b, snap)
	if err != nil {
		return nil, fmt.Errorf("counting the store fields of bucket %q: %w", req.Bucket, err)
	}
	declaration := b.declaredFieldTypes()
	fields, fieldTypes := describeFieldTypes(declaration)
	// A second peek, not the facts resolveForRead copied: the declared factor
	// is live — an alter stores a new one on the record while this call runs —
	// so the answer names the factor as of now, not the one the view resolved
	// with. The peek also notices a drop since the view was resolved.
	facts, held := s.peekBucket(req.Database, req.Bucket)
	if !held {
		// Dropped since the view was resolved.
		return nil, bucketNotFoundErr(req.Database, req.Bucket)
	}
	view := s.replicationViewOf(req.Database, req.Bucket)
	return &BucketStatsResponse{
		Found:    true,
		Database: req.Database,
		Bucket:   displayBucket(req.Bucket),
		Type:     b.bucketType,
		Entries:  entries,
		// Bytes and Path stay empty for one release (spec §11 Q16).
		LogicalBytes:  state.LogicalBytes,
		Indexes:       indexes,
		Stores:        stores,
		Encrypted:     s.encryptedLabel(),
		Fields:        fields,
		FieldTypes:    fieldTypes,
		WritesRefused: declaration.writesRefused(req.Bucket),

		ReplicationFactor: s.effectiveFactorOf(int(facts.replicationFactor)),
		Inherited:         facts.replicationFactor == 0,
		Placement:         placementLabel(b.bucketType, facts.spread),

		InSync:        view.inSync,
		State:         view.state,
		Nodes:         nodeIDs(view.set),
		NotInSync:     view.notInSync,
		PendingCopies: view.pending,
	}, nil
}

// Count returns the number of entries in a bucket.
func (s *Server) Count(req CountRequest) (*CountResponse, error) {
	return s.CountCtx(context.Background(), req)
}

// CountCtx is Count under a context, which a filtered count's walk observes.
// A count keeps nothing but its number, so its budget stays empty; it reports
// one all the same, so every query method is accounted alike.
func (s *Server) CountCtx(ctx context.Context, req CountRequest) (*CountResponse, error) {
	b, snap, err := s.resolveForRead(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	defer snap.Discard()
	budget := s.newResultBudget(queryMethodCount, resultTooLargeError{})
	defer budget.report()
	walk := newQueryWalk(ctx, queryMethodCount, req.Database, req.Bucket)

	var n int64
	var held bucketCounts
	if len(req.Filter) == 0 {
		held, err = bucketCountsOf(b)
		n = held.entries
	} else {
		n, err = countFilteredEntries(b, req.Filter, walk)
	}
	if err != nil {
		return nil, err
	}
	if err := walk.check(); err != nil {
		return nil, err
	}
	answer := &CountResponse{Count: n}
	if held.queued {
		answer.Pending, answer.Dead, answer.Expired = held.delivery.Pending, held.delivery.Dead, s.leaseHasEnded(held.delivery)
	}
	return answer, nil
}

// countBucketEntries counts the entries of a bucket through its handle: a
// read view's engines read its one snapshot, so the count is of that
// snapshot and needs no lock.
func countBucketEntries(b *inMemoryBucket) (int64, error) {
	counts, err := bucketCountsOf(b)
	return counts.entries, err
}

// bucketCounts is a bucket's entry count and, for a queue or heap, the claimed
// and dead entries and the soonest lease deadline read with it.
type bucketCounts struct {
	entries int64
	// delivery is set when the bucket is a queue or a heap.
	delivery store.DeliveryCounts
	queued   bool
}

// bucketCountsOf is countBucketEntries for a caller that also reports what a
// queue holds back. A queue's and a heap's counts come from the one state read
// the entry count needs.
func bucketCountsOf(b *inMemoryBucket) (bucketCounts, error) {
	switch b.bucketType {
	case BucketTypeFIFO, BucketTypeLIFO:
		if b.queueStore == nil {
			return bucketCounts{}, nil
		}
		delivery, err := b.queueStore.Counts()
		if err != nil {
			return bucketCounts{}, fmt.Errorf("counting queue entries: %w", err)
		}
		return bucketCounts{entries: delivery.Entries, delivery: delivery, queued: true}, nil
	case BucketTypeHeap:
		if b.heapStore == nil {
			return bucketCounts{}, nil
		}
		delivery, err := b.heapStore.Counts()
		if err != nil {
			return bucketCounts{}, fmt.Errorf("counting heap entries: %w", err)
		}
		return bucketCounts{entries: delivery.Entries, delivery: delivery, queued: true}, nil
	}
	entries, err := countPlainEntries(b)
	return bucketCounts{entries: entries}, err
}

// countPlainEntries counts the entries of a hash, btree or blob bucket; a queue
// and a heap are bucketCountsOf's.
func countPlainEntries(b *inMemoryBucket) (int64, error) {
	switch b.bucketType {
	case BucketTypeHash:
		if b.hashStore == nil {
			return 0, nil
		}
		return b.hashStore.Len()
	case BucketTypeBTree:
		if b.btreeStore == nil {
			return 0, nil
		}
		n, err := b.btreeStore.Len()
		if err != nil {
			return 0, fmt.Errorf("counting btree entries: %w", err)
		}
		return n, nil
	case BucketTypeBlob:
		if b.blobStore == nil {
			return 0, nil
		}
		n, err := b.blobStore.Len()
		if err != nil {
			return 0, fmt.Errorf("counting blobs: %w", err)
		}
		return n, nil
	default:
		return 0, unsupportedForTypeErr("counting entries", b.bucketType)
	}
}

// countFilteredEntries counts documents matching a filter. The caller must hold
// the bucket lock.
func countFilteredEntries(b *inMemoryBucket, filter map[string]interface{}, walk *queryWalk) (int64, error) {
	var count int64
	compiled := compileFilter(filter)
	if err := compiled.refusal(); err != nil {
		return 0, err
	}

	// visit counts each matching document
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
			return fmt.Errorf("decoding filtered document: %w", err)
		}
		matched, err := compiled.matchesDocument(document)
		if err != nil {
			return err
		}
		if matched {
			count++
		}
		return nil
	}

	// Walk through all entries, same logic as Scan
	switch b.bucketType {
	case BucketTypeHash:
		if err := walkHashDocuments(b, "the bucket", visit); err != nil {
			return 0, err
		}

	case BucketTypeBTree:
		if err := walkBTreeDocuments(b, "the bucket", visit); err != nil {
			return 0, err
		}

	case BucketTypeFIFO, BucketTypeLIFO:
		if err := b.queueStore.Walk(visit); err != nil {
			return 0, err
		}

	case BucketTypeHeap:
		if err := b.heapStore.Walk(visit); err != nil {
			return 0, err
		}

	default:
		return 0, unsupportedForTypeErr("counting", b.bucketType)
	}

	return count, nil
}

// Distinct returns the distinct values of a field in a bucket, optionally filtered.
func (s *Server) Distinct(req DistinctRequest) (*DistinctResponse, error) {
	return s.DistinctCtx(context.Background(), req)
}

// DistinctCtx is Distinct under a context, which its walk observes, and a
// result budget, which bounds the values and their seen-set keys to the
// node's ceiling.
func (s *Server) DistinctCtx(ctx context.Context, req DistinctRequest) (*DistinctResponse, error) {
	b, snap, err := s.resolveForRead(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	defer snap.Discard()
	budget := s.newResultBudget(queryMethodDistinct, resultTooLargeError{
		query: fmt.Sprintf("distinct(%q) on %s.%s", req.Field, req.Database, req.Bucket), shape: resultShapeDistinct,
	})
	defer budget.report()
	walk := newQueryWalk(ctx, queryMethodDistinct, req.Database, req.Bucket)

	values, err := distinctValues(b, req, budget, walk)
	if err != nil {
		return nil, err
	}
	if err := walk.check(); err != nil {
		return nil, err
	}
	return &DistinctResponse{Values: values}, nil
}

// distinctValues walks b once and returns the distinct values of req.Field
// among the documents req.Filter matches, in the order first seen. Each new
// value is charged to budget: its encoded element in the answer, and the
// seen-set key that remembers it.
func distinctValues(b *inMemoryBucket, req DistinctRequest, budget *resultBudget, walk *queryWalk) ([]interface{}, error) {
	path, err := parseFieldPath(req.Field)
	if err != nil {
		return nil, fmt.Errorf("distinct's field %w. Write a path as names joined by \".\", with 0-based positions in brackets, for example distinct(\"Address.City\")", err)
	}
	// Use a map to track seen values (type-aware)
	seen := make(map[string]interface{})
	filter := compileFilter(req.Filter)
	if err := filter.refusal(); err != nil {
		return nil, err
	}
	lookup := libraryPath(path)
	var values []interface{}

	// visit collects distinct values of the requested field
	visit := func(rawEntry []byte) error {
		if rawEntry == nil {
			return nil
		}
		if err := walk.visit(); err != nil {
			return err
		}
		// The walk hands out this node's own stored bytes, so only their
		// framing is checked; the filter and the field are read in place.
		document, err := bson.TrustStored(rawEntry)
		if err != nil {
			return fmt.Errorf("decoding document for distinct: %w", err)
		}

		// Apply filter if provided
		if len(req.Filter) > 0 {
			matched, err := filter.matchesDocument(document)
			if err != nil {
				return err
			}
			if !matched {
				return nil
			}
		}

		// Check if field exists and collect distinct value. Only the field's
		// value is decoded, as the whole-document decode would give it.
		view, ok, err := document.Lookup(lookup)
		if err != nil {
			return fmt.Errorf("reading the field %q for distinct: %w", req.Field, err)
		}
		if ok {
			fieldValue, err := store.DecodeStoredValue(view.Type(), view.Bytes())
			if err != nil {
				return fmt.Errorf("decoding a value of %q for distinct: %w", req.Field, err)
			}
			// Create a type-aware key for the value
			key := valueToDistinctKey(fieldValue)
			if _, alreadySeen := seen[key]; !alreadySeen {
				_, encoded, err := bson.MarshalValue(fieldValue)
				if err != nil {
					return fmt.Errorf("encoding a distinct value of %q: %w", req.Field, err)
				}
				if err := budget.chargeDocument(elementHeader(len(values)) + int64(len(encoded)) + int64(len(key))); err != nil {
					return err
				}
				seen[key] = fieldValue
				values = append(values, fieldValue)
			}
		}
		return nil
	}

	// Walk through all entries, same logic as Scan and Count
	switch b.bucketType {
	case BucketTypeHash:
		if err := walkHashDocuments(b, req.Database+"."+req.Bucket, visit); err != nil {
			return nil, err
		}

	case BucketTypeBTree:
		if err := walkBTreeDocuments(b, req.Database+"."+req.Bucket, visit); err != nil {
			return nil, err
		}

	case BucketTypeFIFO, BucketTypeLIFO:
		if err := b.queueStore.Walk(visit); err != nil {
			return nil, err
		}

	case BucketTypeHeap:
		if err := b.heapStore.Walk(visit); err != nil {
			return nil, err
		}

	default:
		return nil, unsupportedForTypeErr("distinct", b.bucketType)
	}

	return values, nil
}

// valueToDistinctKey creates a type-aware key for use in distinct value tracking.
// Numbers key by their mathematical value: 5, int64(5) and 5.0 are one key, and
// 2^53 and 2^53+1 are two, which a float64 image could not tell apart.
func valueToDistinctKey(v interface{}) string {
	if numVal, ok := numberOf(v); ok {
		return "num:" + numVal.distinctKey()
	}
	if str, ok := v.(string); ok {
		return fmt.Sprintf("str:%s", str)
	}
	// For other types, use reflect.DeepEqual-based comparison
	return fmt.Sprintf("other:%T:%v", v, v)
}

// Update applies the nine update operators ($set, $unset, $inc, $push,
// $addToSet, $mul, $min, $max, $rename) to one existing document.
func (s *Server) Update(req UpdateRequest) (*UpdateResponse, error) {
	return acknowledgeDurably(context.Background(), s, func(ctx context.Context) (*UpdateResponse, error) {
		return s.UpdateCtx(ctx, req)
	})
}

// UpdateCtx is Update with a context, which carries the apply sequence of a
// replicated or replayed update down to an engine on the shared store.
func (s *Server) UpdateCtx(ctx context.Context, req UpdateRequest) (*UpdateResponse, error) {
	b, unlock, err := s.lockBucket(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	defer unlock()
	// Handle multi-document update
	if req.Multi {
		return s.updateMultiple(ctx, b, req)
	}
	// The keys are parsed once, and the refusals that depend only on the
	// request are made before the document is read.
	parsed, err := parseUpdate(req)
	if err != nil {
		return nil, err
	}

	// Handle single-document update
	if req.Key == "" {
		return nil, fmt.Errorf(`update needs a key — use db.%s.update({_id_: "key"}, {$set: {field: value}})`, req.Bucket)
	}
	// The born set of a spread template is the router's own document, which
	// the collection's declaration does not judge (UpdateRequest.Admitted).
	declared := b.declaredFieldTypes()
	if req.Admitted {
		declared = nil
	}
	typing, err := typeUpdate(declared, parsed)
	if err != nil {
		return nil, err
	}

	var raw []byte
	switch b.bucketType {
	case BucketTypeHash:
		raw, err = b.hashStore.Get(req.Key)
	case BucketTypeBTree:
		raw, err = b.btreeStore.Get(req.Key)
	default:
		return nil, wrongTypeErr(req.Bucket, b.bucketType, BucketTypeHash, BucketTypeBTree)
	}
	if err != nil {
		// The store failed the read, not the update: mark it and record it as a
		// failed apply, so a replica leaves the entry unapplied for its owner
		// to re-send (BUG-0583) and the owner's replay retries it (BUG-0584).
		readErr := markReadApplyFailure(fmt.Errorf("reading document %q: %w", req.Key, err))
		s.recordReadApplyFailure(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: b, op: "UPDATE"}, readErr)
		return nil, readErr
	}
	if raw == nil {
		if err := s.settleReplayedNoEffect(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: b, op: "UPDATE"}, false); err != nil {
			return nil, err
		}
		return &UpdateResponse{
			Success: false,
			Message: fmt.Sprintf("document %q not found in %s.%s — insert it first or use find to check the key",
				req.Key, req.Database, req.Bucket),
		}, nil
	}

	var doc map[string]interface{}
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("decoding document %q: %w", req.Key, err)
	}

	if err := typing.before(ctx, doc, req.Key); err != nil {
		return nil, err
	}
	if err := parsed.apply(doc, req.Key); err != nil {
		return nil, err
	}
	if err := typing.after(ctx, doc, req.Key); err != nil {
		return nil, err
	}
	doc["_id_"] = req.Key
	if err := newUpdateGuard(req).check(req.Key, raw, doc); err != nil {
		return nil, err
	}
	s.noteUpdateWarned(ctx, typing)

	switch b.bucketType {
	case BucketTypeHash:
		err = b.hashStore.Put(applyInfoFrom(ctx, s), req.Key, doc, writeVersionFrom(ctx))
		s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: b, op: "UPDATE"}, err)
	case BucketTypeBTree:
		err = b.btreeStore.Put(applyInfoFrom(ctx, s), req.Key, doc, writeVersionFrom(ctx))
		s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: b, op: "UPDATE"}, err)
	}
	if refusal := uniqueRefusal(req.Bucket, err); refusal != nil {
		return nil, refusal
	}
	if err != nil {
		return nil, fmt.Errorf("writing document %q: %w", req.Key, err)
	}

	return &UpdateResponse{Success: true, Doc: doc}, nil
}

// multiUpdateWalkHook, when set, runs once inside updateMultiple's walk, at
// its first entry and before its batch, with the bucket's lock held. A test
// holds an update there to prove a read of the same bucket does not wait for
// it (query-pipelining spec §3.5, T-2). Nil in production.
var multiUpdateWalkHook atomic.Pointer[func(database, bucket string)]

// updateMultiple updates all documents matching a filter. On the shared store
// every matched document is written in one apply: the update is one WAL entry
// with one sequence, and a second apply at the same sequence would be an
// ordering fault. The caller holds b's lock, from lockBucket.
func (s *Server) updateMultiple(ctx context.Context, b *inMemoryBucket, req UpdateRequest) (*UpdateResponse, error) {
	// One parse for every matched document: a refusal of the request itself
	// is answered before the walk.
	parsed, err := parseUpdate(req)
	if err != nil {
		return nil, err
	}
	typing, err := typeUpdate(b.declaredFieldTypes(), parsed)
	if err != nil {
		return nil, err
	}

	// Each match is updated as the walk reaches it. Reading every match again
	// by key cost a second point lookup per match — on this live bucket a
	// snapshot per match — and read nothing new: b.mu is held across the walk
	// and the write, and every writer of the bucket takes it.
	var updated []map[string]interface{}
	guard := newUpdateGuard(req)
	filter := compileFilter(req.Filter)
	if err := filter.refusal(); err != nil {
		return nil, err
	}
	walkHook := multiUpdateWalkHook.Load()
	collectUpdates := func(rawEntry []byte) error {
		if walkHook != nil {
			(*walkHook)(req.Database, req.Bucket)
			walkHook = nil
		}
		if rawEntry == nil {
			return nil
		}
		// The walk hands out this node's own stored bytes: the filter reads
		// them in place, and only a match is decoded, to be updated.
		stored, err := bson.TrustStored(rawEntry)
		if err != nil {
			return fmt.Errorf("decoding document: %w", err)
		}
		matched, err := filter.matchesDocument(stored)
		if err != nil {
			return err
		}
		if !matched {
			return nil
		}
		var doc map[string]interface{}
		if err := stored.Decode(&doc); err != nil {
			return fmt.Errorf("decoding document: %w", err)
		}
		key, ok := doc["_id_"].(string)
		if !ok {
			return nil
		}
		if err := typing.before(ctx, doc, key); err != nil {
			return err
		}
		if err := parsed.apply(doc, key); err != nil {
			return err
		}
		if err := typing.after(ctx, doc, key); err != nil {
			return err
		}
		doc["_id_"] = key
		if err := guard.check(key, rawEntry, doc); err != nil {
			return err
		}
		updated = append(updated, doc)
		return nil
	}

	// Walk through all entries to find matches. The wrapper records whether
	// the callback failed: its errors are decodes and refusals, not reads of
	// the store, and only the walk's own failure is marked a store failure.
	var visitErr error
	visit := func(rawEntry []byte) error {
		visitErr = collectUpdates(rawEntry)
		return visitErr
	}
	switch b.bucketType {
	case BucketTypeHash:
		if err := walkHashDocuments(b, req.Database+"."+req.Bucket, visit); err != nil {
			if visitErr != nil {
				return nil, err
			}
			readErr := markReadApplyFailure(err)
			s.recordReadApplyFailure(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: b, op: "UPDATE"}, readErr)
			return nil, readErr
		}

	case BucketTypeBTree:
		entries, err := b.btreeStore.Range(nil, nil, 0)
		if err != nil {
			readErr := markReadApplyFailure(fmt.Errorf("ranging btree: %w", err))
			s.recordReadApplyFailure(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: b, op: "UPDATE"}, readErr)
			return nil, readErr
		}
		for _, entry := range entries {
			if err := collectUpdates(entry.Value); err != nil {
				return nil, err
			}
		}

	default:
		return nil, unsupportedForTypeErr("multi-update", b.bucketType)
	}

	if len(updated) > 0 {
		s.noteUpdateWarned(ctx, typing)
		var err error
		if b.bucketType == BucketTypeHash {
			_, err = b.hashStore.PutBatch(applyInfoFrom(ctx, s), store.PutBatchRequest{Items: updated, Version: writeVersionFrom(ctx)})
		} else {
			_, err = b.btreeStore.PutBatch(applyInfoFrom(ctx, s), store.PutBatchRequest{Items: updated, Version: writeVersionFrom(ctx)})
		}
		s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: b, op: "UPDATE"}, err)
		if refusal := uniqueRefusal(req.Bucket, err); refusal != nil {
			return nil, refusal
		}
		if err != nil {
			return nil, fmt.Errorf("writing %d updated documents: %w", len(updated), err)
		}
	}
	if len(updated) == 0 {
		if err := s.settleReplayedNoEffect(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: b, op: "UPDATE"}, true); err != nil {
			return nil, err
		}
	}
	return &UpdateResponse{Success: true, Message: multiUpdateMessage(len(updated))}, nil
}

// multiUpdateMessage reports how many documents a multi-update changed.
func multiUpdateMessage(updated int) string {
	message := fmt.Sprintf("updated %d documents", updated)
	if updated > 1000 {
		message += "; consider batching for safety"
	}
	return message
}

// applyPush appends a value to the array in target, or creates the array if
// absent.
func applyPush(target pathSlot, value interface{}) {
	target.set(append(arrayForAppend(target), value))
}

// applyAddToSet appends a value to the array in target only if not already
// present.
func applyAddToSet(target pathSlot, value interface{}) {
	elements := arrayForAppend(target)
	for _, existing := range elements {
		if valuesEqual(existing, value) {
			return // Value already present, do nothing
		}
	}
	target.set(append(elements, value))
}

// arrayForAppend is the array $push and $addToSet append to: the one in
// target, none when it is absent, and a value that is not an array wrapped as
// the array's first element, as it always was.
func arrayForAppend(target pathSlot) []interface{} {
	current, exists := target.get()
	if !exists {
		return nil
	}
	if elements, isArray := decodedArray(current); isArray {
		return elements
	}
	return []interface{}{current}
}

// applyMin keeps the minimum of the current value and the operand.
func applyMin(target pathSlot, value interface{}) {
	current, exists := target.get()
	if !exists {
		target.set(value)
		return
	}
	// Compare values using type-aware comparison; values that do not compare
	// are left as they are.
	if order, comparable := compareValues(current, value); comparable && order > 0 {
		target.set(value)
	}
}

// applyMax keeps the maximum of the current value and the operand.
func applyMax(target pathSlot, value interface{}) {
	current, exists := target.get()
	if !exists {
		target.set(value)
		return
	}
	if order, comparable := compareValues(current, value); comparable && order < 0 {
		target.set(value)
	}
}

// RangeQuery returns the entries of a btree bucket inside [Low, High].
// An empty bound is unbounded on that side.
func (s *Server) RangeQuery(req BTreeRangeRequest) (*BTreeRangeResponse, error) {
	return s.RangeQueryCtx(context.Background(), req)
}

// RangeQueryCtx is RangeQuery under a context, which its walk observes, and a
// result budget: a range with no limit is stopped by the node's ceiling
// rather than by the band's size.
func (s *Server) RangeQueryCtx(ctx context.Context, req BTreeRangeRequest) (*BTreeRangeResponse, error) {
	b, snap, err := s.resolveForRead(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	defer snap.Discard()
	if b.bucketType != BucketTypeBTree {
		return nil, wrongTypeErr(req.Bucket, b.bucketType, BucketTypeBTree)
	}
	budget := s.newResultBudget(queryMethodBTreeRange, resultTooLargeError{
		query: fmt.Sprintf("range() on %s.%s", req.Database, req.Bucket), shape: resultShapeFind, limit: req.Limit,
	})
	defer budget.report()
	walk := newQueryWalk(ctx, queryMethodBTreeRange, req.Database, req.Bucket)

	entries, err := rangeQueryEntries(b, req, budget, walk)
	if err != nil {
		return nil, err
	}
	if err := walk.check(); err != nil {
		return nil, err
	}
	return &BTreeRangeResponse{Entries: entries}, nil
}

// rangeQueryEntries reads the entries of the btree bucket b inside
// [req.Low, req.High], at most req.Limit of them when it is positive. The
// limit goes to the store's walk, which stops there; truncating the whole
// band after reading it cost what the band held. Each entry is
// charged to budget as it is kept, so a band with no limit stops at the
// ceiling.
func rangeQueryEntries(b *inMemoryBucket, req BTreeRangeRequest, budget *resultBudget, walk *queryWalk) ([]BTreeEntry, error) {
	// The bounds stay strings: every key a client can write is a string
	// (InsertRequest.Key), and the store's keys are typed, so a bound read as a
	// number would ask for a type no wire-written key has.
	var low, high interface{}
	if req.Low != "" {
		low = req.Low
	}
	if req.High != "" {
		high = req.High
	}

	var entries []BTreeEntry
	keep := func(stored store.BTreeEntry) error {
		if err := walk.visit(); err != nil {
			return err
		}
		entry := BTreeEntry{Key: wireKey(stored), Value: bson.Raw(stored.Value)}
		if err := budget.chargeDocument(elementHeader(len(entries)) + btreeEntryBytes(entry)); err != nil {
			return err
		}
		entries = append(entries, entry)
		return nil
	}
	if err := b.btreeStore.RangeEntries(low, high, req.Limit, keep); err != nil {
		return nil, fmt.Errorf("scanning bucket %q: %w", req.Bucket, err)
	}
	if entries == nil {
		// An empty band has always answered an empty array, not null
		// (btreeEntries built a non-nil slice); keep the reply's shape.
		entries = []BTreeEntry{}
	}
	return entries, nil
}

// btreeEntryBytes is one range entry's encoded subdocument, as the reply
// marshals BTreeEntry: its length and terminator, the "key" string element
// and the "value" document element. The names are the default lower-cased
// field names, because BTreeEntry carries only json tags.
func btreeEntryBytes(entry BTreeEntry) int64 {
	return int64(4 + (1 + 4 + 4 + len(entry.Key) + 1) + (1 + 6 + len(entry.Value)) + 1)
}

// Edge returns the first (lowest key) or last (highest key) entry of a btree
// bucket.
func (s *Server) Edge(database, bucket string, last bool) (*BTreeRangeResponse, error) {
	b, snap, err := s.resolveForRead(database, bucket)
	if err != nil {
		return nil, err
	}
	defer snap.Discard()
	if b.bucketType != BucketTypeBTree {
		return nil, wrongTypeErr(bucket, b.bucketType, BucketTypeBTree)
	}

	var entry store.BTreeEntry
	if last {
		entry, err = b.btreeStore.Last()
		if err != nil {
			s.logKeyReadFailure(err)
			return nil, fmt.Errorf("reading last entry of %q: %w", bucket, err)
		}
	} else {
		entry, err = b.btreeStore.First()
		if err != nil {
			s.logKeyReadFailure(err)
			return nil, fmt.Errorf("reading first entry of %q: %w", bucket, err)
		}
	}

	if entry.Value == nil {
		resp := &BTreeRangeResponse{Entries: []BTreeEntry{}}
		resp.Message = fmt.Sprintf("bucket %q is empty — insert a document first", bucket)
		return resp, nil
	}
	return &BTreeRangeResponse{Entries: btreeEntries([]store.BTreeEntry{entry})}, nil
}

// BlobWrite stores binary data in a blob bucket under a key.
func (s *Server) BlobWrite(req BlobWriteRequest) (*BlobWriteResponse, error) {
	if err := s.checkBlobWrite(blobWriteShape{bucket: req.Bucket, key: req.Key, dataBytes: len(req.Data), payloadBytes: len(req.Data)}); err != nil {
		return nil, err
	}
	return acknowledgeDurably(context.Background(), s, func(ctx context.Context) (*BlobWriteResponse, error) {
		return s.BlobWriteCtx(ctx, req)
	})
}

// BlobWriteCtx is BlobWrite with a context, which carries the apply sequence
// of a replicated or replayed write down to the blob store, so a replayed
// write is gated by the bucket's applied_seq like every other.
func (s *Server) BlobWriteCtx(ctx context.Context, req BlobWriteRequest) (*BlobWriteResponse, error) {
	b, unlock, err := s.lockBucket(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if b.bucketType != BucketTypeBlob {
		return nil, wrongTypeErr(req.Bucket, b.bucketType, BucketTypeBlob)
	}
	if len(req.Data) == 0 {
		// sentencedError keeps the wire message the fix alone while errors.Is still finds the sentinel.
		return nil, &sentencedError{
			message: fmt.Sprintf(`no data to store — use db.%s.put({_id_: "key", data: "..."})`, req.Bucket),
			cause:   ErrBlobEmpty,
		}
	}

	key := req.Key
	if key == "" {
		var err error
		key, err = ids.NewDocumentID()
		if err != nil {
			return &BlobWriteResponse{Success: false, Message: failureMessage("generating blob id", err)}, nil
		}
	}

	blobID, err := b.blobStore.Put(applyInfoFrom(ctx, s), key, req.Data, req.Metadata)
	err = s.notePayloadLeft(req.Database, req.Bucket, err)
	s.noteApplyOutcome(ctx, appliedWrite{database: req.Database, bucket: req.Bucket, b: b, op: "BLOB WRITE"}, err)
	if err != nil {
		return nil, fmt.Errorf("writing blob: %w", err)
	}

	return &BlobWriteResponse{Success: true, Key: key, BlobID: blobID, Size: len(req.Data)}, nil
}

// BlobRead returns the blob stored under a key together with its metadata.
func (s *Server) BlobRead(req BlobReadRequest) (*BlobReadResponse, error) {
	b, snap, err := s.resolveForRead(req.Database, req.Bucket)
	if err != nil {
		return nil, err
	}
	defer snap.Discard()
	if b.bucketType != BucketTypeBlob {
		return nil, wrongTypeErr(req.Bucket, b.bucketType, BucketTypeBlob)
	}
	if req.Key == "" {
		return nil, fmt.Errorf(`blob read needs a key — use db.%s.get({_id_: "key"})`, req.Bucket)
	}

	stored, err := b.blobStore.Get(req.Key)
	if err != nil {
		return nil, fmt.Errorf("reading blob %q in %s.%s: %w%s", req.Key, req.Database, req.Bucket, err, blobReadFix(err))
	}
	if stored == nil {
		return &BlobReadResponse{
			Found: false,
			Message: fmt.Sprintf("blob %q not found in %s.%s — keys are exact; db.%s.count() shows how many the bucket holds",
				req.Key, req.Database, req.Bucket, req.Bucket),
		}, nil
	}

	return &BlobReadResponse{
		Found:    true,
		BlobID:   stored.BlobID,
		Data:     stored.Content,
		Size:     len(stored.Content),
		Metadata: stored.Metadata,
	}, nil
}

// notePayloadLeft turns store.ErrPayloadLeft into the success it is — the
// blob mutation committed; only the unlink of a payload nothing names any
// more failed — and logs the file so an operator can see it. Any other error
// is returned as it is.
func (s *Server) notePayloadLeft(database, bucket string, err error) error {
	if !errors.Is(err, store.ErrPayloadLeft) {
		return err
	}
	s.logger.Warn("a blob payload file nothing names could not be removed: it takes disk space and is never served",
		"database", database, "bucket", bucket, "error", err)
	return nil
}

// DeleteDatabase drops a database: its catalog keys go in one synced batch,
// then every bucket engine is closed and the database directory removed.
func (s *Server) DeleteDatabase(req DeleteDatabaseRequest) (*DeleteDatabaseResponse, error) {
	return s.DeleteDatabaseCtx(context.Background(), req)
}

// DeleteDatabaseCtx drops a database's incarnations (spec §5.2.2 "drop
// database"; the replication-database-ddl-design record G-3, G-4, G-12). A
// drop names the generations it removes, and removes exactly the buckets
// created in them, on any node and whenever it arrives: a later incarnation's
// buckets are untouched. It does O(1) store work however many buckets the
// database holds — one synced batch records the drop, its tombstones and the
// marker's remaining generations — then removes those buckets from the
// registry and closes them, and their catalog entries become reclaim records in
// the background, one bucket fence at a time (the reclaimer's expander).
//
// The drop's batch also cancels the pulls of the database's buckets in flight
// on this node (Cluster.dropDatabaseClearsPulls): the database's
// fence does not exclude a materialise, which holds its bucket's, and the drop
// record is gone once expanded, long before a slow pull's catalog commit.
func (s *Server) DeleteDatabaseCtx(ctx context.Context, req DeleteDatabaseRequest) (*DeleteDatabaseResponse, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("database name required — run 'drop database <name>'")
	}
	// Validate the name before any filesystem operations (path traversal prevention).
	if err := validRestoreNameComponent("database", req.Name); err != nil {
		return nil, fmt.Errorf("invalid database name: %v", err)
	}
	generations := req.Generations
	if !generationsTrusted(ctx) {
		// A client of a node that coordinates for itself: the drop names the
		// incarnations live here, whatever the request said (G-3, G-9).
		live, err := s.liveGenerations(req.Name)
		if err != nil {
			return &DeleteDatabaseResponse{Success: false, Message: failureMessage(fmt.Sprintf("database %q was not dropped: reading its incarnations failed", req.Name), err)}, nil
		}
		generations = live
	}
	if len(generations) == 0 {
		return s.deleteLegacyDatabase(ctx, req.Name)
	}
	// A drop another node logged can name generations this node has not
	// seen dropped: it logs them again as its own first (G-12). The entry is
	// then applied as it is and finds them dropped.
	if fromPeer(ctx) {
		if err := s.learnTombstones(ctx, req.Name, generations); err != nil {
			return &DeleteDatabaseResponse{Success: false, Message: failureMessage(fmt.Sprintf("database %q was not dropped: logging the drop it taught this node failed", req.Name), err)}, nil
		}
	}
	return s.applyGenerationDrop(ctx, req.Name, generations)
}

// deleteLegacyDatabase is the drop of a database that names no generation:
// one logged before generations existed, met again by a replay of this node's
// own log (a receiver passes such an entry over, G-8). It removes what the
// database holds when it is applied, as every drop did then: the record's
// max_id says which catalog entries are the database's, and no tombstone is
// written, so a re-create logged after it in the same log is not refused.
func (s *Server) deleteLegacyDatabase(ctx context.Context, database string) (*DeleteDatabaseResponse, error) {
	data := s.serverDataAccessor()
	if data.getDB(database) == nil {
		return nil, databaseNotFoundErr(database)
	}

	// The record first: a drop the store did not record must not already
	// have hidden anything. The reclaimer leaves the database's records until
	// the detach has unlinked its buckets (BUG-0527).
	window := s.unlinkWindow.holdDatabase(database)
	defer window.release()
	if err := s.commitDatabaseDrop(database, applySeqFrom(ctx)); err != nil {
		return &DeleteDatabaseResponse{
			Success: false,
			Message: failureMessage(fmt.Sprintf("database %q was not dropped: recording the drop failed", database), err),
		}, nil
	}
	window.recordCommitted()
	removed := s.detachDroppedGenerations(database, nil, true)
	window.namesGone()
	// Nothing on disk is named after a database, so nothing is removed here:
	// the reclaimer frees its keys. A RemoveAll(<data dir>/<name>) once lived
	// here, and "drop database keys" deleted the node's keys directory.
	s.wakeReclaimer()

	return &DeleteDatabaseResponse{
		Success: true,
		Message: fmt.Sprintf("database %q dropped (%d bucket(s)). Its space is reclaimed in the background; watch hoarddb_reclaim_pending_records to follow it.", database, removed),
	}, nil
}

// DeleteBucket drops a bucket, with no apply sequence: its reclaim record's
// dropped_seq is 0.
func (s *Server) DeleteBucket(req DeleteBucketRequest) (*DeleteBucketResponse, error) {
	return s.DeleteBucketCtx(context.Background(), req)
}

// dropPhase names a point of a bucket drop before its unlink, for
// dropPhaseHook.
type dropPhase int

const (
	// dropBeforeCommit is step 2 about to commit: nothing durable has changed.
	dropBeforeCommit dropPhase = iota
	// dropCommitted is step 2 committed and synced, the name not yet unlinked.
	dropCommitted
)

// dropPhaseHook, when set, runs at each dropPhase of a bucket drop. Tests cut
// a drop there, as a crash would. Nil in production.
var dropPhaseHook atomic.Pointer[func(phase dropPhase, database, bucket string)]

// afterDropHiddenHook, when set, runs once a drop has removed the name,
// marked the bucket closed and freed its record (step 3), and before it hands
// the id to the reclaimer (step 4). Nil in production.
var afterDropHiddenHook atomic.Pointer[func(database, bucket string, id uint64, slot uint16)]

// DeleteBucketCtx drops a bucket (spec §5.2.2). It runs inside the exclusive
// apply fence its caller took, so every write to the bucket has finished and
// none starts:
//
//  1. resolve the name to its id and slot under db.mu;
//  2. commit one synced batch that deletes the catalog entry and puts the
//     reclaim record — synced, because the reclaimer unlinks blob payloads
//     through the filesystem's own journal, and a hide lost to a power loss
//     after an unlink would bring the bucket back naming missing files. The
//     id is in s.unlinkWindow from before this commit until step 3's unlink,
//     so the reclaimer, which reads the record from the store, leaves it;
//  3. remove the name under db.mu held for writing, and mark the bucket
//     closed so a writer that resolved it before the drop cannot write;
//  4. hand the id to the reclaimer;
//  5. return.
//
// No store I/O is done under db.mu. The range stays until the reclaimer
// deletes it; nothing resolves to it meanwhile.
func (s *Server) DeleteBucketCtx(ctx context.Context, req DeleteBucketRequest) (*DeleteBucketResponse, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("bucket name required — run 'drop bucket <name>'")
	}
	if req.Database == "" {
		return nil, fmt.Errorf("no database selected — run 'use <database>' first, or pass 'in <database>'")
	}
	if err := validateBucketName(req.Database, req.Name); err != nil {
		return nil, errors.New(invalidBucketNameMessage(err))
	}

	// A drop entry is applied only to a copy of the incarnation it names
	// (R8-C, trap 11). Applied by a replica or a replay to a node with no such
	// copy — no database, no bucket, or another incarnation — it is an
	// acknowledged no-op that still records the drop. The owner's own live
	// apply keeps its answers: it stamped the incarnation from its own copy.
	dropped, isDropEntry := dropEntryFrom(ctx)
	// A pull of the name in flight on this node must not bring it back once
	// the drop returns. The drop runs inside the exclusive hold its caller
	// took, so a materialise is either finished or has yet to re-check. A drop
	// that is no entry — a release, G-6 (d), a client's drop on a node with no
	// cluster — removes whatever copy is here, so it cancels by name now. A
	// drop entry cancels below, and only when it deletes this node's copy of
	// its incarnation: a pull here, or a snapshot served from here, may be of
	// a newer incarnation, and a pull of the dropped one is refused at its
	// materialise by the record this drop writes (trap 11).
	if s.cluster != nil && !isDropEntry {
		s.cluster.dropClearsPulls(req.Database, req.Name)
	}
	mayBeANoOp := isDropEntry && (isReplay(ctx) || !lineageFrom(ctx).originated)
	db := s.serverDataAccessor().getDB(req.Database)
	if db == nil {
		if mayBeANoOp {
			return s.dropOfNoCopy(req, dropped, true)
		}
		return nil, databaseNotFoundErr(req.Database)
	}
	db.mu.RLock()
	held, found := db.table.get(req.Name)
	var record reclaimRecord
	if found {
		held.rec.mu.RLock()
		record = reclaimRecord{ID: int64(held.rec.id.Load()), Slot: int32(held.rec.slot), Type: bucketTypeOfCode(held.rec.bucketType)}
		held.rec.mu.RUnlock()
	}
	db.mu.RUnlock()
	if !found {
		if mayBeANoOp {
			return s.dropOfNoCopy(req, dropped, true)
		}
		return nil, bucketNotFoundErr(req.Database, req.Name)
	}
	record.Database, record.Name, record.DroppedSeq = req.Database, req.Name, int64(applySeqFrom(ctx))
	record.DroppedAt = time.Now().UTC()

	// A bucket whose attach failed at load is catalogued with an id the
	// in-memory struct never took: its range is the catalog entry's.
	if record.ID == 0 {
		entry, catalogued, err := s.readCatalogEntry(req.Database, req.Name)
		if err != nil {
			return &DeleteBucketResponse{Success: false, Message: failureMessage(fmt.Sprintf("bucket %q was not dropped: reading its catalog entry failed", req.Name), err)}, nil
		}
		if catalogued {
			record.ID, record.Slot, record.Type = entry.ID, entry.Slot, entry.Type
		}
	}
	var recorded *droppedIncarnation
	if isDropEntry && record.ID != 0 {
		held, err := s.heldIncarnation(req.Database, uint16(record.Slot), uint64(record.ID))
		if err != nil {
			return &DeleteBucketResponse{Success: false, Message: failureMessage(fmt.Sprintf("bucket %q was not dropped: reading its incarnation failed", req.Name), err)}, nil
		}
		if held != dropped.incarnation {
			// Another incarnation: the drop is recorded and the live copy, and
			// what is pending for it, are left alone.
			return s.dropOfNoCopy(req, dropped, false)
		}
		recorded = &dropped
	}
	if !isDropEntry && record.ID != 0 && loneDrop(ctx) {
		held, err := s.heldIncarnation(req.Database, uint16(record.Slot), uint64(record.ID))
		if err != nil {
			return &DeleteBucketResponse{Success: false, Message: failureMessage(fmt.Sprintf("bucket %q was not dropped: reading its incarnation failed", req.Name), err)}, nil
		}
		recorded = &droppedIncarnation{incarnation: held, epoch: s.cluster.Epoch()}
	}
	if s.cluster != nil && isDropEntry {
		s.cluster.dropClearsPulls(req.Database, req.Name)
	}
	var window *windowHold
	if record.ID != 0 {
		// The reclaimer can read the record from the commit on, and must not
		// step it before the unlink below (BUG-0527).
		window = s.unlinkWindow.holdBucket(uint64(record.ID))
		defer window.release()
		if hook := dropPhaseHook.Load(); hook != nil {
			(*hook)(dropBeforeCommit, req.Database, req.Name)
		}
		if err := s.commitDropBatch(record, recorded); err != nil {
			return &DeleteBucketResponse{
				Success: false,
				Message: failureMessage(fmt.Sprintf("bucket %q was not dropped: recording the drop failed", req.Name), err),
			}, nil
		}
		window.recordCommitted()
		if s.cluster != nil {
			s.cluster.forgetAwaitingBucket(placementOf(req.Database, req.Name))
		}
		if hook := dropPhaseHook.Load(); hook != nil {
			(*hook)(dropCommitted, req.Database, req.Name)
		}
	}

	// The drop's three phases (spec §5.3): the name stops finding the record,
	// the record is closed with no db.mu held, and only then is its slot
	// given back. A record another drop unlinked first (a database drop
	// takes no fence) is that drop's to close and free.
	db.mu.Lock()
	unlinked, unlinkErr := db.table.unlink(req.Name, held)
	db.mu.Unlock()
	if unlinkErr != nil {
		// The catalog entry is already gone, but the name still finds the
		// record: answering success would leave a bucket served that the
		// catalog says was dropped. A restart rebuilds the registry from the
		// catalog, and that is the end of this drop, not a step before a retry:
		// after the restart the bucket is not in the catalog, so a second drop
		// answers not-found.
		return &DeleteBucketResponse{
			Success: false,
			Message: failureMessage(fmt.Sprintf("bucket %q was dropped from the catalog but its in-memory record could not be unlinked, so it is still served; restart the node to clear it. The drop is already complete: after the restart the bucket is gone ('show buckets in %s' will not list it), and no retry is needed", req.Name, req.Database), unlinkErr),
		}, nil
	}
	if window != nil {
		// Every reader that resolved the name took its snapshot before the
		// unlink's write hold, so the range is the reclaimer's from here.
		window.namesGone()
	}
	if unlinked {
		s.noteBucketDetached(s.retireRecords(db, []*bucketRec{held.rec}))
	}
	if hook := afterDropHiddenHook.Load(); hook != nil {
		(*hook)(req.Database, req.Name, uint64(record.ID), uint16(record.Slot))
	}

	s.handToReclaimer(req.Database, req.Name, uint64(record.ID))
	if recorded != nil && s.cluster != nil {
		s.cluster.forgetPendingName(req.Database, req.Name, uint64(record.ID))
	}
	return &DeleteBucketResponse{
		Success: true,
		Message: fmt.Sprintf("bucket %q dropped from %q. Its space is reclaimed in the background; watch hoarddb_reclaim_pending_records to follow it.", req.Name, req.Database),
	}, nil
}

// commitDropBatch is the drop's one synced batch: the catalog entry deleted
// and the reclaim record put, together, with the bucket's awaiting records
// deleted. A drop entry and a release alike delete them: a released copy logs
// in no term any more (spec §7.3). The caller drops them from the map once the
// batch has committed.
func (s *Server) commitDropBatch(record reclaimRecord, dropped *droppedIncarnation) error {
	encoded, err := bson.Marshal(record)
	if err != nil {
		return fmt.Errorf("encoding the reclaim record of bucket %d: %w", record.ID, err)
	}
	batch := s.kv.NewBatch()
	defer batch.Discard()
	snapshot := s.kv.NewSnapshot()
	defer snapshot.Discard()
	if err := deleteNodeRecords(snapshot, batch, storage.AwaitingRecordKind, bucketRecordScope(placementOf(record.Database, record.Name)), nil); err != nil {
		return fmt.Errorf("deleting the awaiting records of bucket %d: %w", record.ID, err)
	}
	// Only a drop entry records the drop and clears the name's marks (review
	// M-3): a release deletes the live incarnation, which this node may be
	// asked to hold again.
	if dropped != nil {
		if err := s.setDropRecord(batch, record.Database, record.Name, *dropped); err != nil {
			return err
		}
		if err := batch.Delete(placementMarkKey(placementOf(record.Database, record.Name))); err != nil {
			return fmt.Errorf("deleting the placement marks of bucket %d: %w", record.ID, err)
		}
	}
	if err := batch.Delete(catalogKey(record.Database, record.Name)); err != nil {
		return fmt.Errorf("deleting the catalog entry of bucket %d: %w", record.ID, err)
	}
	if err := batch.Set(reclaimKey(uint64(record.ID)), encoded); err != nil {
		return fmt.Errorf("writing the reclaim record of bucket %d: %w", record.ID, err)
	}
	if err := batch.Commit(true); err != nil {
		return fmt.Errorf("committing the drop of bucket %d: %w", record.ID, err)
	}
	return nil
}

// handToReclaimer is step 4 of a drop: the reclaim record is in the store
// already and the id has left the unlink window, so the hand-off is a wake of
// the reclaimer. Sequences the node still
// owed the bucket are forgotten — its state document goes with the range.
//
// It also notes the drop in recentDrops: the reclaim record itself
// can be gone by the time a racing or following write checks for it — an
// empty bucket's one reclaim lap can finish before the next request lands —
// and a missing signal there must not fall back to the join fence's
// "rebalancing has not moved the data here yet" guess for a drop that just
// happened on this node.
func (s *Server) handToReclaimer(database, bucket string, id uint64) {
	s.recentDrops.record(database, bucket)
	s.owed.dropBucketID(id)
	s.ttl.forget(database, bucket)
	s.unregisterBucketFactor(database, bucket)
	s.wakeReclaimer()
}

// btreeEntries turns store entries into wire entries.
func btreeEntries(entries []store.BTreeEntry) []BTreeEntry {
	out := make([]BTreeEntry, 0, len(entries))
	for _, e := range entries {
		// The stored value is already BSON; hand it through untouched, as Find
		// does, rather than decoding to a map and re-encoding it per entry.
		out = append(out, BTreeEntry{Key: wireKey(e), Value: bson.Raw(e.Value)})
	}
	return out
}

// wireKey is a store entry's key for the wire. A number, date, bool or null
// key arrives as text the store formatted, valid UTF-8 by construction, so it
// is used as is; only string and binary keys need displayKey's check (BUG-0617).
func wireKey(entry store.BTreeEntry) string {
	if entry.Text != "" {
		return entry.Text
	}
	return displayKey(entry.Key)
}

// displayKey renders a B+tree key for the wire exactly as it was written: the
// store has already turned numbers, dates and booleans into their text, and
// every valid UTF-8 key, NUL and other control bytes included, travels as the
// BSON string it is (BUG-0613). Only bytes that are not valid UTF-8 cannot be a
// BSON string, so those keep the "0x" + hex form.
func displayKey(key []byte) string {
	if utf8.Valid(key) {
		return string(key)
	}
	return "0x" + hex.EncodeToString(key)
}

// dropOfNoCopy answers a drop entry on a node that holds no copy of the
// incarnation it names: the drop is recorded, and — when the node holds no
// copy of the name at all — the name's pending state goes with it.
func (s *Server) dropOfNoCopy(req DeleteBucketRequest, dropped droppedIncarnation, clearPending bool) (*DeleteBucketResponse, error) {
	if err := s.recordDropOfNoCopy(req.Database, req.Name, dropped, clearPending); err != nil {
		return &DeleteBucketResponse{Success: false, Message: failureMessage(fmt.Sprintf("the drop of bucket %q was not recorded", req.Name), err)}, nil
	}
	return &DeleteBucketResponse{Success: true, Message: fmt.Sprintf(
		"bucket %q in %q: this node holds no copy of incarnation %d, so nothing was deleted here; the drop is recorded", req.Name, req.Database, dropped.incarnation)}, nil
}
