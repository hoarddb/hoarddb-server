package wire

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// batchEntryOverhead is charged per write on top of its key and value, so the
// size estimate below matches the store's own accounting for many small keys.
const batchEntryOverhead = 16

// tombstoneHeader is the header value of a deleted document (placement §3).
var tombstoneHeader = []byte{0x00}

// DocumentKey names one document: its bucket range and its encoded _id_.
type DocumentKey struct {
	// Ref is the bucket's slot, database and bucket id.
	Ref storage.BucketRef
	// EncID is the document id's EncodeComponent form.
	EncID []byte
}

func (d DocumentKey) header() []byte {
	return store.DocumentPrefix(d.Ref.Slot, d.Ref.DatabaseID, d.Ref.BucketID, d.EncID)
}

func (d DocumentKey) storePrefix() []byte {
	return storage.Key(d.Ref.Slot, d.Ref.DatabaseID, d.Ref.BucketID, storage.NSFieldStore, d.EncID)
}

// HeaderState is what a document's header key says (placement §3).
type HeaderState int

const (
	// HeaderAbsent means no header key.
	HeaderAbsent HeaderState = iota
	// HeaderLive means a header holding the document's _id_ value.
	HeaderLive
	// HeaderTombstone means the single byte 0x00: the document is deleted and
	// its remaining keys wait for the sweep or for a re-insert's cleanup.
	HeaderTombstone
)

// ReadHeaderState reads the header of doc. "Absent" in the operation tables
// means HeaderAbsent or HeaderTombstone (conv §11 rule 2); callers that treat
// 0x00 as present acknowledge a write the sweep then deletes.
func ReadHeaderState(snap storage.KVSnapshot, doc DocumentKey) (HeaderState, error) {
	header, err := snap.Get(doc.header())
	if errors.Is(err, storage.ErrKeyNotFound) {
		return HeaderAbsent, nil
	}
	if err != nil {
		return HeaderAbsent, fmt.Errorf("reading the header of document %x: %w", doc.EncID, err)
	}
	switch {
	case bytes.Equal(header, tombstoneHeader):
		return HeaderTombstone, nil
	case len(header) == 0:
		return HeaderAbsent, fmt.Errorf("the header of document %x is empty, which is neither a tombstone nor an _id_ value", doc.EncID)
	}
	return HeaderLive, nil
}

// ApplyDelete fills batch with a delete (conv §11 rule 1): the header becomes
// the tombstone and one node-local 'd' record is stamped with pos, and nothing
// else is touched, so a large store never forces an over-limit batch. A
// document that is absent or already a tombstone writes nothing; the caller
// still consumes the position. The header is never deleted here.
func ApplyDelete(snap storage.KVSnapshot, batch storage.KVBatch, doc DocumentKey, pos cluster.Position) error {
	state, err := ReadHeaderState(snap, doc)
	if err != nil {
		return fmt.Errorf("deleting document %x: %w", doc.EncID, err)
	}
	if state != HeaderLive {
		return nil
	}
	if err := batch.Set(doc.header(), tombstoneHeader); err != nil {
		return fmt.Errorf("writing the tombstone of document %x: %w", doc.EncID, err)
	}
	record := ReclaimRecord{Doc: doc, Pos: pos}
	if err := batch.Set(record.Key(), []byte{}); err != nil {
		return fmt.Errorf("writing the 'd' record of document %x at %s: %w", doc.EncID, pos, err)
	}
	return nil
}

// InsertPath is an insert or an upsert's insert path for one document, with the
// largest batch the node commits atomically.
type InsertPath struct {
	// Insert is the document to write.
	Insert store.DocumentInsert
	// Limit is storage.KV.MaxBatchBytes.
	Limit int64
}

func (r InsertPath) document() DocumentKey {
	return DocumentKey{Ref: r.Insert.Ref, EncID: r.Insert.EncID}
}

// insertBytes is the estimated size of the insert's own writes.
func (r InsertPath) insertBytes() int64 {
	doc := r.document()
	total := int64(len(doc.header()) + len(r.Insert.Header) + batchEntryOverhead)
	for _, field := range r.Insert.Fields {
		if field.Store {
			continue
		}
		total += int64(len(doc.header()) + 2 + len(field.Stored) + batchEntryOverhead)
	}
	return total
}

// CleanupTooLargeError refuses an insert over a tombstone whose cleanup and
// insert do not fit one batch (conv §11 rule 2). Nothing was written.
type CleanupTooLargeError struct {
	// Ref is the bucket of the document.
	Ref storage.BucketRef
	// EncID is the encoded _id_ of the deleted document.
	EncID []byte
	// Bytes is the size of the batch this insert would need: the deletes of the
	// deleted document's keys and record, and the insert's own writes.
	Bytes int64
	// Limit is the most one batch holds.
	Limit int64
}

// Error describes the refusal with the ids the node has; Message is the text a
// client sees, once the caller supplies the names.
func (e *CleanupTooLargeError) Error() string {
	return fmt.Sprintf("an insert into bucket %d over the tombstone of document %x needs a batch of %d bytes and one batch holds at most %d", e.Ref.BucketID, e.EncID, e.Bytes, e.Limit)
}

// Message is the BATCH_TOO_LARGE text of the placement spec's Errors table.
func (e *CleanupTooLargeError) Message(bucket, id string) string {
	return fmt.Sprintf("this insert into %q must first remove %d bytes left by the deleted document %q, and one batch holds at most %d bytes. Nothing was written. Finish the delete with db.%s.delete({_id_: %q}), wait for it, then insert again",
		bucket, e.Bytes, id, e.Limit, bucket, id)
}

// cleanupKeys lists the keys an insert over doc's tombstone deletes: every key
// under both prefixes except the header, which the insert rewrites. It keeps
// keys only while the running size stays inside limit, and keeps counting after
// that so a refusal can name the whole size.
func cleanupKeys(snap storage.KVSnapshot, doc DocumentKey, limit int64) ([][]byte, int64, error) {
	var keys [][]byte
	var size int64
	header := doc.header()
	for _, prefix := range [][]byte{doc.storePrefix(), header} {
		it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: storage.PrefixEnd(prefix)})
		for it.Seek(prefix); it.Valid(); it.Next() {
			key := it.Key()
			if bytes.Equal(key, header) {
				continue
			}
			size += int64(len(key) + batchEntryOverhead)
			if size <= limit {
				keys = append(keys, append([]byte(nil), key...))
			}
		}
		err := it.Err()
		it.Close()
		if err != nil {
			return nil, 0, fmt.Errorf("listing the keys of deleted document %x: %w", doc.EncID, err)
		}
	}
	return keys, size, nil
}

// CheckTombstoneCleanup is check 6a for an insert or upsert that lands on a
// tombstone: it returns a *CleanupTooLargeError when cleanup and insert cannot
// share one batch, and nil for any other header state. It runs before a
// position is taken, so a refusal consumes none.
func CheckTombstoneCleanup(snap storage.KVSnapshot, req InsertPath) error {
	doc := req.document()
	state, err := ReadHeaderState(snap, doc)
	if err != nil {
		return fmt.Errorf("checking the cleanup of document %x: %w", doc.EncID, err)
	}
	if state != HeaderTombstone {
		return nil
	}
	return checkCleanup(snap, req)
}

// checkCleanup is the size check of CheckTombstoneCleanup without its state test, for a
// document that is live but past its expire time and is replaced as a tombstone is.
func checkCleanup(snap storage.KVSnapshot, req InsertPath) error {
	doc := req.document()
	_, cleanup, err := cleanupKeys(snap, doc, req.Limit)
	if err != nil {
		return fmt.Errorf("checking the cleanup of document %x: %w", doc.EncID, err)
	}
	record := ReclaimRecord{Doc: doc}
	total := cleanup + int64(len(record.Key())+batchEntryOverhead) + req.insertBytes()
	if total > req.Limit {
		return &CleanupTooLargeError{Ref: doc.Ref, EncID: doc.EncID, Bytes: total, Limit: req.Limit}
	}
	return nil
}

// InsertOverTombstone fills batch with an insert that lands on a tombstone
// (conv §11 rule 2): a delete of every key under both of the document's
// prefixes and of its 'd' record, then the insert's own writes, all in this one
// batch and ordered deletes first, so every copy that applies the position
// reaches the same state and no later sweep can delete what the insert
// acknowledged. A document whose header is not a tombstone is refused, so a
// live document is never cleaned. An over-size cleanup returns a
// *CleanupTooLargeError and leaves batch empty.
func InsertOverTombstone(snap storage.KVSnapshot, batch storage.KVBatch, req InsertPath) error {
	doc := req.document()
	state, err := ReadHeaderState(snap, doc)
	if err != nil {
		return fmt.Errorf("cleaning up document %x: %w", doc.EncID, err)
	}
	if state != HeaderTombstone {
		return fmt.Errorf("document %x is not a tombstone, so cleaning up its keys would delete a live document", doc.EncID)
	}
	return insertOverStale(snap, batch, req)
}

// insertOverStale is the cleanup and insert of InsertOverTombstone for any document whose
// keys are to be replaced whole: a tombstone, or a live document past its expire time. The
// caller has decided that; this only does it, in the one batch.
func insertOverStale(snap storage.KVSnapshot, batch storage.KVBatch, req InsertPath) error {
	doc := req.document()
	if err := checkCleanup(snap, req); err != nil {
		return err
	}
	keys, _, err := cleanupKeys(snap, doc, req.Limit)
	if err != nil {
		return fmt.Errorf("cleaning up document %x: %w", doc.EncID, err)
	}
	records, err := reclaimRecordKeys(snap, doc)
	if err != nil {
		return fmt.Errorf("cleaning up document %x: %w", doc.EncID, err)
	}
	for _, key := range append(keys, records...) {
		if err := batch.Delete(key); err != nil {
			return fmt.Errorf("deleting key %x left by document %x: %w", key, doc.EncID, err)
		}
	}
	if err := store.WriteInsert(batch, req.Insert); err != nil {
		return fmt.Errorf("writing the insert over the tombstone of document %x: %w", doc.EncID, err)
	}
	return nil
}

// WriteInsertPath writes the insert path of an insert or an upsert (placement
// §6): a header that is absent is a plain insert, and a tombstone is an insert
// over a tombstone with its cleanup. It returns the state it found; for
// HeaderLive it writes nothing, because that is the update path's (or, for an
// insert, DOCUMENT_EXISTS). 0x00 is never treated as present.
func WriteInsertPath(snap storage.KVSnapshot, batch storage.KVBatch, req InsertPath) (HeaderState, error) {
	doc := req.document()
	state, err := ReadHeaderState(snap, doc)
	if err != nil {
		return HeaderAbsent, fmt.Errorf("writing the insert path of document %x: %w", doc.EncID, err)
	}
	switch state {
	case HeaderAbsent:
		err = store.WriteInsert(batch, req.Insert)
	case HeaderTombstone:
		err = InsertOverTombstone(snap, batch, req)
	case HeaderLive:
	}
	if err != nil {
		return state, fmt.Errorf("writing the insert path of document %x: %w", doc.EncID, err)
	}
	return state, nil
}

// CheckStoreVerbTarget is check 6a for a store verb (placement §7 rule 5): a
// tombstone is refused with INVALID_ARGUMENT and an absent document with
// NOT_FOUND, never recreated, because the sweep would delete what the verb
// wrote. The message for the first is StoreVerbOnTombstoneMessage.
func CheckStoreVerbTarget(snap storage.KVSnapshot, doc DocumentKey) (cluster.Code, error) {
	state, err := ReadHeaderState(snap, doc)
	if err != nil {
		return 0, fmt.Errorf("checking the target of a store verb: %w", err)
	}
	switch state {
	case HeaderTombstone:
		return cluster.CodeInvalidArgument, nil
	case HeaderAbsent:
		return cluster.CodeNotFound, nil
	case HeaderLive:
	}
	return 0, nil
}

// StoreVerbOnTombstoneMessage is the INVALID_ARGUMENT text of the placement
// spec's Errors table.
func StoreVerbOnTombstoneMessage(database, bucket, id string) string {
	return fmt.Sprintf("document %q in %s.%s is deleted and its remaining keys are still being removed, so its store fields cannot be changed. Retry in a moment, or finish the delete with db.%s.delete({_id_: %q}) and insert the document again",
		id, database, bucket, bucket, id)
}
