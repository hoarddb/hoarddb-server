package server

import "fmt"

// writeVerb is the intent of a document write, taken from the opcode of the
// handler that decoded the request and never from the wire (document-placement
// spec §1.3). It lives in an unexported field of InsertRequest and
// BatchInsertRequest, which BSON neither encodes nor decodes, so no client byte
// can set it and it never reaches the log: a replica and a replay derive it
// again from the logged opcode, through the same handler.
type writeVerb uint8

const (
	// verbUnset is the zero value: a Go caller that built the request itself.
	// It keeps the replace semantics a direct call has always had.
	verbUnset writeVerb = iota
	// verbInsert is set by the PUT, BTREE_INSERT and BATCH_PUT handlers.
	verbInsert
	// verbUpsert is set by the UPSERT and BATCH_UPSERT handlers.
	verbUpsert
)

// refuseUpsertOn is the refusal of an upsert into a store type that has no
// document to replace by key (document-placement spec §6.3): a queue or a heap
// has no key, and a blob bucket's replace verb is put. It names the verb that
// does work. It is nil for hash and btree buckets, which upsert.
func refuseUpsertOn(bucket string, kind BucketType) error {
	switch kind {
	case BucketTypeFIFO, BucketTypeLIFO, BucketTypeHeap:
		return wrongTypeSentence(fmt.Sprintf("Bucket %q is a %s bucket: a queue has no key to replace; use push, db.%s.push({...}).",
			bucket, kind, bucket), kind)
	case BucketTypeBlob:
		return wrongTypeSentence(fmt.Sprintf("Bucket %q is a blob bucket: use put, db.%s.put(\"key\", ...).", bucket, bucket), kind)
	}
	return nil
}

// refuseInsertOnBlob is the refusal of an insert (PUT, BTREE_INSERT) into a
// blob bucket (document-placement spec §6.3, build doc BD-7). A blob bucket's
// write is put (BLOB_WRITE), which replaces; it has no insert, as it has no
// upsert. It is nil for every other type.
func refuseInsertOnBlob(bucket string, kind BucketType) error {
	if kind != BucketTypeBlob {
		return nil
	}
	return wrongTypeSentence(fmt.Sprintf("Bucket %q is a blob bucket, and a blob bucket is written with put: db.%s.put(\"key\", ...).",
		bucket, bucket), kind)
}
