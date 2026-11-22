package main

import (
	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// upsert builds an UPSERT of doc under key.
func upsert(db, bucket, key string, doc map[string]any) call {
	return call{op: transport.OpUpsert, payload: server.InsertRequest{Database: db, Bucket: bucket, Key: key, Data: doc}}
}

// upsertCases pins UPSERT (0x16) and BATCH_UPSERT (0x24) on the wire: a new
// id and an existing one on a hash and a btree bucket, a batch of two, and one
// refusal per store type that has no document to replace by key. They are the
// newest cases, so every earlier exchange keeps its number, database name and
// request ID.
func upsertCases() []func(db string) exchangeCase {
	replaceNote := "UPSERT replaces the scalar fields of the document stored under the id (a field the new document " +
		"lacks is gone) and keeps the document's own stores; an absent id is stored. The answer is UPSERT_OK."
	refusal := func(name, storeType, verb string, kind server.BucketType, op transport.Opcode, wantMessage ...string) func(db string) exchangeCase {
		return func(db string) exchangeCase {
			bucket := "target"
			request := upsert(db, bucket, "ada", ada())
			if op == transport.OpBatchUpsert {
				request = call{op: op, payload: server.BatchInsertRequest{Database: db, Bucket: bucket,
					Data: []map[string]any{{"_id_": "ada", "Name": "Ada"}}}}
			}
			return exchangeCase{
				name: name, group: "write", storeType: storeType,
				notes: verb + " is an ERROR frame INVALID_ARGUMENT, " +
					"and nothing is stored: a bucket with no key to replace by has no upsert (handbook operations.md section 1).",
				setup: bucketSetup(db, bucket, kind), request: request,
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument, wantMessage: wantMessage,
			}
		}
	}
	return []func(db string) exchangeCase{
		func(db string) exchangeCase {
			return exchangeCase{
				name: "upsert of a new id on a hash bucket", group: "write", storeType: "hash", notes: replaceNote,
				setup:      bucketSetup(db, "users", server.BucketTypeHash),
				request:    upsert(db, "users", "ada", ada()),
				wantOpcode: transport.OpUpsertOK, wantField: map[string]any{"success": true, "id": "ada"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "upsert of an existing id on a hash bucket replaces it", group: "write", storeType: "hash", notes: replaceNote,
				setup:      append(bucketSetup(db, "users", server.BucketTypeHash), put(db, "users", "ada", map[string]any{"Name": "Ada", "Draft": true})),
				request:    upsert(db, "users", "ada", map[string]any{"Name": "Ada Lovelace", "Born": int32(1815)}),
				wantOpcode: transport.OpUpsertOK, wantField: map[string]any{"success": true, "id": "ada"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "upsert of a new key on a btree bucket", group: "write", storeType: "btree", notes: replaceNote,
				setup:      bucketSetup(db, "b", server.BucketTypeBTree),
				request:    upsert(db, "b", "ada", ada()),
				wantOpcode: transport.OpUpsertOK, wantField: map[string]any{"success": true, "id": "ada"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "upsert of an existing key on a btree bucket replaces it", group: "write", storeType: "btree", notes: replaceNote,
				setup:      append(bucketSetup(db, "b", server.BucketTypeBTree), put(db, "b", "ada", map[string]any{"Name": "Ada", "Draft": true})),
				request:    upsert(db, "b", "ada", map[string]any{"Name": "Ada Lovelace"}),
				wantOpcode: transport.OpUpsertOK, wantField: map[string]any{"success": true, "id": "ada"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "upsert many on a hash bucket", group: "write", storeType: "hash",
				notes: "BATCH_UPSERT is UPSERT per document, in one frame: a new id is stored and an existing one replaced. " +
					"The answer is BATCH_UPSERT_OK with the ids in order.",
				setup: append(bucketSetup(db, "users", server.BucketTypeHash), put(db, "users", "ada", map[string]any{"Name": "Ada", "Draft": true})),
				request: call{op: transport.OpBatchUpsert, payload: server.BatchInsertRequest{
					Database: db, Bucket: "users",
					Data: []map[string]any{
						{"_id_": "ada", "Name": "Ada Lovelace"},
						{"_id_": "grace", "Name": "Grace", "Born": int32(1906)},
					},
				}},
				wantOpcode: transport.OpBatchUpsertOK, wantField: map[string]any{"success": true, "count": 2, "ids": []any{"ada", "grace"}},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "put many repeating an id is refused whole", group: "write", storeType: "hash",
				notes: "A BATCH_PUT that names one _id_ at two positions is refused before anything is logged or stored: " +
					"an ERROR frame DOCUMENT_EXISTS (0x12), write_outcome not_applied, with results[] of one entry per position, " +
					"the repeated position carrying the code and every other position not_applied. Send each _id_ once, or BATCH_UPSERT.",
				setup: bucketSetup(db, "users", server.BucketTypeHash),
				request: call{op: transport.OpBatchPut, payload: server.BatchInsertRequest{
					Database: db, Bucket: "users",
					Data: []map[string]any{
						{"_id_": "ada", "Name": "Ada"},
						{"_id_": "grace", "Name": "Grace"},
						{"_id_": "ada", "Name": "Ada Lovelace"},
					},
				}},
				wantOpcode: transport.OpError, wantCode: transport.ErrDocumentExists,
				wantMessage: []string{"more than once", "positions 1, 3"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "upsert many repeating an id keeps the last", group: "write", storeType: "hash",
				notes: "BATCH_UPSERT keeps the last document for a repeated _id_. The answer carries results[] with one applied " +
					"entry per position; an earlier occurrence says which later position replaced it.",
				setup: bucketSetup(db, "users", server.BucketTypeHash),
				request: call{op: transport.OpBatchUpsert, payload: server.BatchInsertRequest{
					Database: db, Bucket: "users",
					Data: []map[string]any{
						{"_id_": "ada", "Name": "Ada"},
						{"_id_": "grace", "Name": "Grace"},
						{"_id_": "ada", "Name": "Ada Lovelace"},
					},
				}},
				wantOpcode: transport.OpBatchUpsertOK, wantField: map[string]any{"success": true, "count": 3, "ids": []any{"ada", "grace", "ada"}},
			}
		},
		refusal("upsert on a fifo bucket is refused", "fifo", "UPSERT on a fifo bucket", server.BucketTypeFIFO, transport.OpUpsert,
			"a queue has no key to replace", "use push"),
		refusal("upsert on a lifo bucket is refused", "lifo", "UPSERT on a lifo bucket", server.BucketTypeLIFO, transport.OpUpsert,
			"a queue has no key to replace", "use push"),
		refusal("upsert on a heap bucket is refused", "heap", "UPSERT on a heap bucket", server.BucketTypeHeap, transport.OpUpsert,
			"a queue has no key to replace", "use push"),
		refusal("upsert on a blob bucket is refused", "blob", "UPSERT on a blob bucket", server.BucketTypeBlob, transport.OpUpsert,
			"use put"),
		refusal("upsert many on a fifo bucket is refused", "fifo", "BATCH_UPSERT on a fifo, lifo or heap bucket", server.BucketTypeFIFO, transport.OpBatchUpsert,
			"a queue has no key to replace", "use push"),
		refusal("upsert many on a blob bucket is refused", "blob", "BATCH_UPSERT on a blob bucket", server.BucketTypeBlob, transport.OpBatchUpsert,
			"use put"),
	}
}
