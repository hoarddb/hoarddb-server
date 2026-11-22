// Package store provides store type implementations for hoardDB buckets.
package store

// Enumerator is the read-only capability a dump needs. Nothing else in the
// server uses it, so it stays one method and a count.
//
// IterateDocuments walks every document the store holds and invokes fn once
// per document with the document's key and its stored bytes. The stored
// bytes are emitted verbatim — never unmarshalled and re-marshalled — so a
// caller comparing dump output against the source store sees byte-identical
// documents. Where an engine has no per-document key of its own (the
// segment-backed queues), key is nil; see each implementation's doc comment
// for what key and value actually carry. fn's error stops the walk; the
// returned error wraps it with the position at which the walk stopped.
//
// DocCount reports how many documents the store holds, read from storage
// itself rather than from any in-memory counter. This repository has
// twice shipped a cached counter that lied about a bucket's true size
// (HashStore.Recount and BTreeStore.Recount both exist to correct exactly
// that class of bug), and a dump's manifest is exactly the kind of promise
// that bug class breaks — so DocCount always re-derives its answer from
// storage, per engine, and never trusts a counter kept for something else.
type Enumerator interface {
	IterateDocuments(fn func(key, value []byte) error) error
	DocCount() (int64, error)
}
