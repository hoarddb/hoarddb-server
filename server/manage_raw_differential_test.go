package server

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/store"
)

// The manage differential (build doc step 7a, path "manage"). Count with a
// filter, distinct and the multi-document update each walked the bucket and
// decoded every document into a map, to match it and, for distinct, to read
// one field. The oracles below are those functions as they were, copied from
// server/manage.go before the move (next at 8fc40221), and the tests run
// them beside the server's over the scan differential's stored documents
// and filters: the same count, the same distinct values in the same order,
// and for updates the same answer and the same stored documents afterwards.
// The ruled duplicate-key difference cannot reach a stored document (every
// write encodes a map), so the stored buckets here have none.

func oracleCountFilteredEntries(b *inMemoryBucket, filter map[string]interface{}, walk *queryWalk) (int64, error) {
	var count int64
	compiled := compileFilter(filter)
	if err := compiled.refusal(); err != nil {
		return 0, err
	}
	visit := func(rawEntry []byte) error {
		if rawEntry == nil {
			return nil
		}
		if err := walk.visit(); err != nil {
			return err
		}
		var document map[string]interface{}
		if err := bson.Unmarshal(rawEntry, &document); err != nil {
			return fmt.Errorf("decoding filtered document: %w", err)
		}
		matched, err := oracleMatches(compiled, document)
		if err != nil {
			return err
		}
		if matched {
			count++
		}
		return nil
	}
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

func oracleDistinctValues(b *inMemoryBucket, req DistinctRequest, budget *resultBudget, walk *queryWalk) ([]interface{}, error) {
	path, err := parseFieldPath(req.Field)
	if err != nil {
		return nil, fmt.Errorf("distinct's field %w. Write a path as names joined by \".\", with 0-based positions in brackets, for example distinct(\"Address.City\")", err)
	}
	seen := make(map[string]interface{})
	filter := compileFilter(req.Filter)
	if err := filter.refusal(); err != nil {
		return nil, err
	}
	var values []interface{}
	visit := func(rawEntry []byte) error {
		if rawEntry == nil {
			return nil
		}
		if err := walk.visit(); err != nil {
			return err
		}
		var document map[string]interface{}
		if err := bson.Unmarshal(rawEntry, &document); err != nil {
			return fmt.Errorf("decoding document for distinct: %w", err)
		}
		if len(req.Filter) > 0 {
			matched, err := oracleMatches(filter, document)
			if err != nil {
				return err
			}
			if !matched {
				return nil
			}
		}
		if fieldValue, ok := resolvePath(document, path); ok {
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
	if err := walkBucketDocuments(b, req.Database, req.Bucket, visit); err != nil {
		return nil, err
	}
	return values, nil
}

// oracleUpdateMultiple is updateMultiple as it was, for a hash or btree
// bucket: every document decoded and matched as a map, every match updated.
func (s *Server) oracleUpdateMultiple(ctx context.Context, b *inMemoryBucket, req UpdateRequest) (*UpdateResponse, error) {
	parsed, err := parseUpdate(req)
	if err != nil {
		return nil, err
	}
	typing, err := typeUpdate(b.declaredFieldTypes(), parsed)
	if err != nil {
		return nil, err
	}
	var updated []map[string]interface{}
	guard := newUpdateGuard(req)
	filter := compileFilter(req.Filter)
	if err := filter.refusal(); err != nil {
		return nil, err
	}
	collectUpdates := func(rawEntry []byte) error {
		if rawEntry == nil {
			return nil
		}
		var doc map[string]interface{}
		if err := bson.Unmarshal(rawEntry, &doc); err != nil {
			return fmt.Errorf("decoding document: %w", err)
		}
		matched, err := oracleMatches(filter, doc)
		if err != nil {
			return err
		}
		if !matched {
			return nil
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
	switch b.bucketType {
	case BucketTypeHash:
		var visitErr error
		if err := walkHashDocuments(b, req.Database+"."+req.Bucket, func(rawEntry []byte) error {
			visitErr = collectUpdates(rawEntry)
			return visitErr
		}); err != nil {
			return nil, err
		}
	case BucketTypeBTree:
		entries, err := b.btreeStore.Range(nil, nil, 0)
		if err != nil {
			return nil, err
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
		var err error
		if b.bucketType == BucketTypeHash {
			_, err = b.hashStore.PutBatch(applyInfoFrom(ctx, s), store.PutBatchRequest{Items: updated, Version: writeVersionFrom(ctx)})
		} else {
			_, err = b.btreeStore.PutBatch(applyInfoFrom(ctx, s), store.PutBatchRequest{Items: updated, Version: writeVersionFrom(ctx)})
		}
		if refusal := uniqueRefusal(req.Bucket, err); refusal != nil {
			return nil, refusal
		}
		if err != nil {
			return nil, fmt.Errorf("writing %d updated documents: %w", len(updated), err)
		}
	}
	return &UpdateResponse{Success: true, Message: multiUpdateMessage(len(updated))}, nil
}

// manageFilters are the page differential's filters, a sample derived from
// the stored documents, and ranges whose bounds are stored values, so an
// operator read one way too wide changes what an update writes.
func manageFilters(documents []probeDocument, random *rand.Rand) []map[string]interface{} {
	filters := pageFilters(documents, random)
	for _, operator := range []string{"$gt", "$gte", "$lt", "$lte", "$ne"} {
		filters = append(filters,
			map[string]interface{}{"seq": map[string]interface{}{operator: int32(2)}},
			map[string]interface{}{"name": map[string]interface{}{operator: "alpha"}},
		)
	}
	return filters
}

// withBucket runs read against a bucket's read view, with the walk and
// budget a query gets.
func withBucket(t *testing.T, s *Server, bucket string, read func(*inMemoryBucket, *resultBudget, *queryWalk)) {
	t.Helper()
	view, snap, err := s.resolveForRead("app", bucket)
	if err != nil {
		t.Fatalf("resolving %s: %v", bucket, err)
	}
	defer snap.Discard()
	budget := s.newResultBudget(queryMethodDistinct, resultTooLargeError{query: "differential", shape: resultShapeDistinct})
	read(view, budget, newQueryWalk(context.Background(), queryMethodDistinct, "app", bucket))
}

// TestManageMatchesTheMapPath runs count and distinct, old and new, on every
// store type, and the multi-document update on the two that have it.
func TestManageMatchesTheMapPath(t *testing.T) {
	s := newTestServer(t)
	documents := pageDifferentialDocuments(t)
	buckets := seededDifferentialBuckets(t, s, documents)
	random := rand.New(rand.NewSource(74))
	filters := manageFilters(documents, random)
	fields := []string{"seq", "_id_", "name", "a", "b", "c", "0", "k", "nested", "nested.a", "Lines[1]", "tags", "arr[0]", "double", "zz"}
	counts, distincts, nonEmpty := 0, 0, 0
	for _, bucket := range buckets {
		for _, filterSpec := range filters {
			var wantCount, gotCount int64
			var wantErr, gotErr error
			withBucket(t, s, bucket, func(view *inMemoryBucket, _ *resultBudget, walk *queryWalk) {
				wantCount, wantErr = oracleCountFilteredEntries(view, filterSpec, walk)
			})
			withBucket(t, s, bucket, func(view *inMemoryBucket, _ *resultBudget, walk *queryWalk) {
				gotCount, gotErr = countFilteredEntries(view, filterSpec, walk)
			})
			counts++
			if wantCount != gotCount || !sameRefusal(wantErr, gotErr) {
				t.Errorf("%s, count %v: the map path counts (%d, %v), the server (%d, %v)", bucket, filterSpec, wantCount, wantErr, gotCount, gotErr)
			}
			for _, field := range fields {
				req := DistinctRequest{Database: "app", Bucket: bucket, Field: field, Filter: filterSpec}
				var wantValues, gotValues []interface{}
				withBucket(t, s, bucket, func(view *inMemoryBucket, budget *resultBudget, walk *queryWalk) {
					wantValues, wantErr = oracleDistinctValues(view, req, budget, walk)
				})
				withBucket(t, s, bucket, func(view *inMemoryBucket, budget *resultBudget, walk *queryWalk) {
					gotValues, gotErr = distinctValues(view, req, budget, walk)
				})
				distincts++
				if len(wantValues) > 0 {
					nonEmpty++
				}
				if !sameRefusal(wantErr, gotErr) || !reflect.DeepEqual(distinctKeys(wantValues), distinctKeys(gotValues)) {
					t.Errorf("%s, distinct(%q) of %v: the map path gives (%v, %v), the server (%v, %v)",
						bucket, field, filterSpec, wantValues, wantErr, gotValues, gotErr)
				}
			}
		}
	}
	if nonEmpty == 0 {
		t.Fatalf("every one of %d distinct answers was empty; the comparison read nothing", distincts)
	}
	t.Logf("%d counts and %d distincts (%d non-empty) compared over %d stored documents per bucket", counts, distincts, nonEmpty, len(documents))
}

// distinctKeys is a distinct answer as comparable text: each value's
// distinct key, which names its Go type and value, NaN included.
func distinctKeys(values []interface{}) []string {
	keys := make([]string, len(values))
	for position, value := range values {
		keys[position] = valueToDistinctKey(value)
	}
	return keys
}

// storedDocuments is every stored document of a bucket, by key, as bytes.
func storedDocuments(t *testing.T, s *Server, bucket string) map[string][]byte {
	t.Helper()
	stored := map[string][]byte{}
	withBucket(t, s, bucket, func(view *inMemoryBucket, _ *resultBudget, _ *queryWalk) {
		if err := walkBucketDocuments(view, "app", bucket, func(raw []byte) error {
			var keyed struct {
				ID string `bson:"_id_"`
			}
			if err := bson.Unmarshal(raw, &keyed); err != nil {
				return err
			}
			stored[keyed.ID] = raw
			return nil
		}); err != nil {
			t.Fatalf("walking %s: %v", bucket, err)
		}
	})
	return stored
}

// TestMultiUpdateMatchesTheMapPath applies the same sequence of filtered
// updates to two identical buckets, one through the map path and one through
// the server's, and compares every answer and, after each, every stored
// document.
func TestMultiUpdateMatchesTheMapPath(t *testing.T) {
	s := newTestServer(t)
	documents := pageDifferentialDocuments(t)
	random := rand.New(rand.NewSource(75))
	filters := manageFilters(documents, random)
	updates := []UpdateRequest{
		{Set: map[string]interface{}{"touched": int32(1)}},
		{Inc: map[string]int64{"counter": 2}},
		{Unset: []string{"b"}},
		{Push: map[string]interface{}{"log": "x"}},
		{Set: map[string]interface{}{"nested.mark": "m"}},
	}
	for _, bucketType := range []BucketType{BucketTypeHash, BucketTypeBTree} {
		oracleBucket, serverBucket := "oracle"+string(bucketType), "server"+string(bucketType)
		for _, name := range []string{oracleBucket, serverBucket} {
			createTestBucket(t, s, "app", name, bucketType)
			for position, document := range documents {
				var fields map[string]interface{}
				if err := bson.Unmarshal(document.raw, &fields); err != nil {
					continue
				}
				key := fmt.Sprintf("d%05d", position)
				fields["_id_"] = key
				if _, err := s.Insert(InsertRequest{Database: "app", Bucket: name, Key: key, Data: fields}); err != nil {
					continue
				}
			}
		}
		applied, changing := 0, 0
		for step, filterSpec := range filters {
			update := updates[step%len(updates)]
			update.Database, update.Filter, update.Multi = "app", filterSpec, true
			oracleRequest, serverRequest := update, update
			oracleRequest.Bucket, serverRequest.Bucket = oracleBucket, serverBucket
			want, wantErr := s.runMultiUpdate(t, oracleRequest, s.oracleUpdateMultiple)
			got, gotErr := s.runMultiUpdate(t, serverRequest, s.updateMultiple)
			applied++
			if wantErr == nil && want.Message != multiUpdateMessage(0) {
				changing++
			}
			if !sameRefusal(wantErr, gotErr) || (wantErr == nil && (want.Success != got.Success || want.Message != got.Message)) {
				t.Fatalf("%s, update %d with filter %v: the map path answers (%+v, %v), the server (%+v, %v)",
					bucketType, step, filterSpec, want, wantErr, got, gotErr)
			}
			oracleStored, serverStored := storedDocuments(t, s, oracleBucket), storedDocuments(t, s, serverBucket)
			if len(oracleStored) != len(serverStored) {
				t.Fatalf("%s, after update %d: %d documents through the map path, %d through the server", bucketType, step, len(oracleStored), len(serverStored))
			}
			for key, oracleBytes := range oracleStored {
				if !bytes.Equal(oracleBytes, serverStored[key]) {
					t.Fatalf("%s, after update %d with filter %v: document %q differs:\n  map path %s\n  server   %s",
						bucketType, step, filterSpec, key, bson.Raw(oracleBytes), bson.Raw(serverStored[key]))
				}
			}
		}
		if changing == 0 {
			t.Fatalf("%s: none of %d updates changed a document; the comparison wrote nothing", bucketType, applied)
		}
		t.Logf("%s: %d filtered updates compared, %d of them changing documents, %d documents per bucket", bucketType, applied, changing, len(storedDocuments(t, s, serverBucket)))
	}
}

// runMultiUpdate runs one multi-document update through either path under
// the bucket's lock, as UpdateCtx does.
func (s *Server) runMultiUpdate(t *testing.T, req UpdateRequest, update func(context.Context, *inMemoryBucket, UpdateRequest) (*UpdateResponse, error)) (*UpdateResponse, error) {
	t.Helper()
	b, unlock, err := s.lockBucket(req.Database, req.Bucket)
	if err != nil {
		t.Fatalf("locking %s: %v", req.Bucket, err)
	}
	defer unlock()
	response, err := update(context.Background(), b, req)
	if err != nil && strings.Contains(err.Error(), "ordering fault") {
		t.Fatalf("%s: %v", req.Bucket, err)
	}
	return response, err
}
