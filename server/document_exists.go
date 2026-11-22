package server

import (
	"errors"
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// documentExistsError is the refusal of an insert whose _id_ the bucket
// already holds (document-placement spec §6.3). MySQL's INSERT refuses a
// duplicate primary key and MongoDB's insertOne a duplicate _id; neither
// replaces the stored row, and the refusal names the verb that does. It
// reaches the wire as DOCUMENT_EXISTS, a constraint refusal every copy and
// every replay gives alike (constraintRefusedUnder). Its text is built from
// the bucket, the database and the id only: a refusal never echoes a
// document's content (T-02).
type documentExistsError struct {
	database, bucket string
	// ids are the held ids, in request order; empty when the owner refused
	// the write and this copy did not read which (store.OwnerRefused).
	ids []string
	// positions are the 1-based batch positions of ids; nil for one insert.
	positions []int
	// results is a refused batch's per-position answer (build doc BD-5).
	results []WriteResult
}

func (e *documentExistsError) Error() string {
	if e.results == nil {
		id := ""
		if len(e.ids) > 0 {
			id = e.ids[0]
		}
		return fmt.Sprintf("Document %q already exists in bucket %q (database %q). To replace it, run db.%s.upsert({_id_: %q, ...}); "+
			"to change some fields, run db.%s.update(%q, {$set: {...}}). "+
			"If you are retrying an insert whose answer you did not get, it may have been stored: retry with upsert.",
			id, e.bucket, e.database, e.bucket, id, e.bucket, id)
	}
	if len(e.ids) == 0 {
		return fmt.Sprintf("This batch for bucket %q in database %q was refused by the bucket's owner because a document in it already exists. "+
			"Nothing was stored.", e.bucket, e.database)
	}
	const named = 3
	parts := make([]string, 0, named)
	for index, id := range e.ids {
		if index == named {
			break
		}
		parts = append(parts, fmt.Sprintf("%q at position %d", id, e.positions[index]))
	}
	more := ""
	if len(e.ids) > named {
		more = fmt.Sprintf(" (and %d more)", len(e.ids)-named)
	}
	return fmt.Sprintf("This batch for bucket %q in database %q names documents that already exist: %s%s. Nothing was stored. "+
		"To replace them, send the batch as BATCH_UPSERT (db.%s.upsert for each document). "+
		"If you are retrying a batch whose answer you did not get, it may have been stored: retry with BATCH_UPSERT.",
		e.bucket, e.database, strings.Join(parts, ", "), more, e.bucket)
}

// insertExistsRefusal turns the store's refusal of an insert of key into the
// wire's, or returns nil when err is not that refusal.
func insertExistsRefusal(database, bucket, key string, err error) error {
	var exists *store.DocumentExistsError
	if !errors.As(err, &exists) {
		return nil
	}
	return &documentExistsError{database: database, bucket: bucket, ids: []string{key}}
}

// batchExistsRefusal is insertExistsRefusal for a BATCH_PUT of documents:
// every held position carries DOCUMENT_EXISTS and every other one is not
// applied, because the batch is atomic (spec §6.6).
func batchExistsRefusal(database, bucket string, documents []map[string]interface{}, err error) error {
	var exists *store.DocumentExistsError
	if !errors.As(err, &exists) {
		return nil
	}
	refusal := &documentExistsError{database: database, bucket: bucket, results: make([]WriteResult, len(documents))}
	for index, document := range documents {
		id, _ := document["_id_"].(string)
		refusal.results[index] = WriteResult{ID: id, WriteOutcome: outcomeNotApplied,
			Message: "not applied: another document in this batch already exists"}
	}
	// Positions are 1-based places in this request (store.PutBatch).
	for _, position := range exists.Positions {
		index := position - 1
		id := refusal.results[index].ID
		refusal.ids = append(refusal.ids, id)
		refusal.positions = append(refusal.positions, position)
		refusal.results[index].Code = int32(transport.ErrDocumentExists)
		refusal.results[index].Message = fmt.Sprintf("document %q already exists in this bucket", id)
	}
	return refusal
}
