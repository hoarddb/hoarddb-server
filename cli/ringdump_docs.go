package cli

import (
	"context"
	"fmt"
	"slices"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// ringDumpListPageSize is how many documents one List page asks for. A page is one
// round trip, so a larger page trades memory for fewer trips; a document can be as large
// as a page of frames, which is why the dump does not ask for the server's maximum either.
const ringDumpListPageSize = 100

// dumpDocuments writes one bucket's doc records: every document once, one List page at a
// time, in the order the pages return them (section 3.2 item 4). It is handed the
// declaration the bucket record was written from and keeps nothing about the bucket
// afterwards, so a run holds one page, one declaration and one document at a time
// whatever the ring contains; with --parallel N it holds the N entity buffers of one page
// as well (section 3.3).
func (r *ringDumpRun) dumpDocuments(database, bucket string, declaration ringclient.BucketInfo) error {
	// What the bucket's stores cannot carry is a fact about the bucket, not about one
	// entity, so it is said once here, as the entity pass starts after the bucket record
	// (section 3.1).
	r.noteBucketStores(database, bucket, declaration)
	// The two name lists are derived once per bucket, not once per entity: the entity
	// pass of a bucket of 100,000 documents must not allocate them 100,000 times.
	projected, storeFields := ringDumpDeclarationFields(declaration)
	pageWhat := fmt.Sprintf("the documents of %s.%s", database, bucket)
	var cursor []byte
	for {
		var page ringclient.EntryPage
		// The closure captures the request, so a retried page is asked for with the same
		// cursor: a refusal read nothing, and the cursor is (slot, encoded id) order, which
		// does not depend on the owner (section 3.5).
		call := func() error {
			answered, err := r.source.ListDocuments(r.ctx, database, bucket, ringclient.PageRequest{
				Limit:  ringDumpListPageSize,
				Cursor: cursor,
				Fields: projected,
			})
			if err != nil {
				return err
			}
			page = answered
			return nil
		}
		if err := r.retryMovedPage(pageWhat, call); err != nil {
			if bucketLeftDuringTheDump(err, true) {
				// A document page that finds no bucket is the bucket leaving, not a failed page.
				return fmt.Errorf("listing the documents of %s.%s: %w", database, bucket, errBucketLeftDuringTheDump)
			}
			return fmt.Errorf("listing the documents of %s.%s: %w", database, bucket, err)
		}
		if err := r.dumpEntryPage(database, bucket, declaration, storeFields, page.Entries); err != nil {
			return err
		}
		if !page.More {
			break
		}
		cursor = page.Cursor
	}
	return nil
}

// dumpEntityRecord is one entity being written: what it is, the declaration its stores
// follow, the sink its records go to and the counters its records add to. The serial path
// points the sink at the dump writer itself; the parallel path points it at the entity's
// own buffer, which is copied into the file when the entity's turn comes (section 3.3).
// That sink and those counters are the only difference between reading an entity serially
// and reading it in a worker, so both paths write the same records in the same order.
type dumpEntityRecord struct {
	// ctx is the run's context serially and the page's derived one in a worker, so a
	// cancelled run stops a worker in the middle of a store chain.
	ctx      context.Context
	database string
	bucket   string
	id       string
	body     bson.Raw
	// declaration is the bucket's fields, whose store fields are read in declared order.
	declaration ringclient.BucketInfo
	// storeFields is the store field names a List body can still carry as its marker
	// member, derived from declaration once per bucket.
	storeFields []string
	sink        dumpRecordSink
	counters    *dumpCounters
}

// readEntity writes one entity's records into entity.sink: its doc record, then its elems
// records, counting into entity.counters. It is the one place an entity is read, so the
// serial and the parallel path cannot read one differently (section 3.2 item 7: once).
func (r *ringDumpRun) readEntity(entity dumpEntityRecord) error {
	body, err := ringDumpDocumentBody(entity.body, entity.storeFields)
	if err != nil {
		return fmt.Errorf("reading document %q of %s.%s: %w", terminalSafeText(entity.id), entity.database, entity.bucket, err)
	}
	record := &dumpDoc{T: dumpKindDoc, DB: entity.database, B: entity.bucket, ID: entity.id, F: body}
	if err := entity.sink.WriteRecord(record); err != nil {
		return fmt.Errorf("writing document %q of %s.%s: %w", terminalSafeText(entity.id), entity.database, entity.bucket, err)
	}
	entity.counters.Entities++
	// The entity's stores follow its doc, so an entity is a run of records and is never
	// interleaved with another entity's (section 3.1).
	return r.dumpStoreFields(entity)
}

// ringDumpDeclarationFields splits a declaration into the scalar field names a List
// projects and the store field names whose marker member a body can still carry, both in
// declared order. projected is never nil: a bucket with no scalar field asks for an empty
// list, so a reader of the stub call can tell "none declared" from "not projected".
//
// Plan item 7.4 item 15 asked whether an empty non-nil Fields means "none" or "all". It
// means neither on this base: cluster/wire.EncodeRingList refuses it before anything is
// sent ("fields is empty. Send at least one field name such as ["Name"], or leave fields
// out to get every field", cluster/wire/projection.go:checkFieldNames). So a bucket with
// no scalar field is asked for with the member left out
// (cli/ringdump_source.go:ringClientSource.ListDocuments), the node then answers every
// field with each store field as its marker text (cluster/wire/slotscan.go:scanPlan.body
// calls appendStoreSummaries only when the projection is nil), and the dump removes every
// declared store field's member by name: the fallback section 3.2 item 4 names.
// TestDumpOfADocumentWithStoreFieldsHoldsNoMarkerStrings pins the live host's answer.
func ringDumpDeclarationFields(declaration ringclient.BucketInfo) (projected, storeFields []string) {
	projected = make([]string, 0, len(declaration.Fields))
	storeFields = make([]string, 0, len(declaration.Fields))
	for _, field := range declaration.Fields {
		switch {
		case field.Name == dumpBucketIDField:
		case ringStoreType(field.Type):
			storeFields = append(storeFields, field.Name)
		default:
			projected = append(projected, field.Name)
		}
	}
	return projected, storeFields
}

// ringDumpDocumentBody rebuilds a List body as the doc record's f: the body's own members
// in the body's own order, without _id_ (the record's id carries it) and without a store
// field's marker member. It is rebuilt from the raw elements rather than decoded into a
// map, so the order the declaration gives is the order in the file; a body with nothing
// left is the 5-byte empty document, which is a valid f.
func ringDumpDocumentBody(body bson.Raw, storeFields []string) (bson.Raw, error) {
	elements, err := body.Elements()
	if err != nil {
		return nil, fmt.Errorf("the document is not BSON: %w", err)
	}
	kept := make([][]byte, 0, len(elements))
	for _, element := range elements {
		name := element.Key()
		if name == dumpBucketIDField || slices.Contains(storeFields, name) {
			continue
		}
		kept = append(kept, element)
	}
	return bson.Raw(bsoncore.BuildDocument(nil, kept...)), nil
}
