package main

import (
	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// verbMatrixCases pins, on the wire, the cells of the handbook's verb matrix
// (operations.md section 1) that a driver might have read as "refused": the
// verbs a store type answers beyond its own family, and the refusals that
// used to look like answers (BUG-0516). They come last so every earlier
// exchange keeps its number, database name and request ID.
func verbMatrixCases() []func(db string) exchangeCase {
	return []func(db string) exchangeCase{
		func(db string) exchangeCase {
			return exchangeCase{
				name: "put on a blob bucket is refused", group: "write", storeType: "blob",
				notes: "A blob bucket is written with BLOB_WRITE (the CLI's put), which replaces; a document PUT into one is an " +
					"ERROR frame INVALID_ARGUMENT naming put, and nothing is stored (handbook operations.md section 1).",
				setup: bucketSetup(db, "files", server.BucketTypeBlob), request: put(db, "files", "ada", ada()),
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument,
				wantMessage: []string{"a blob bucket is written with put"}, wantField: notApplied(),
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "push on a hash bucket is a keyed put", group: "write", storeType: "hash",
				notes: "FIFO_PUSH on a hash or btree bucket is an insert: it stores the document under its _id_ (here chosen by " +
					"the caller) and refuses an _id_ the bucket holds with DOCUMENT_EXISTS. POP on a hash is still refused.",
				setup:      bucketSetup(db, "users", server.BucketTypeHash),
				request:    push(transport.OpFIFOPush, db, "users", map[string]any{"_id_": "ada", "Name": "Ada"}, 0),
				wantOpcode: transport.OpFIFOPush, wantField: map[string]any{"success": true, "id": "ada"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "insert many on a fifo bucket reports what it pushed", group: "write", storeType: "fifo",
				notes: "BATCH_PUT on a fifo or lifo answers the count and the ids of the documents it pushed, in order " +
					"(the ids are the callers' _id_ values here).",
				setup: bucketSetup(db, "q", server.BucketTypeFIFO),
				request: call{op: transport.OpBatchPut, payload: server.BatchInsertRequest{
					Database: db, Bucket: "q",
					Data: []map[string]any{{"_id_": "first", "n": int32(1)}, {"_id_": "second", "n": int32(2)}},
				}},
				wantOpcode: transport.OpBatchPutOK, wantField: map[string]any{"success": true, "count": 2, "ids": []any{"first", "second"}},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "exists on a fifo bucket is refused", group: "read", storeType: "fifo",
				notes: "A bucket type with no keys has no yes-or-no answer: EXISTS is an ERROR frame INVALID_ARGUMENT, " +
					"never exists:false. An EXISTS with an empty key is refused the same way, on any type.",
				setup:      append(bucketSetup(db, "q", server.BucketTypeFIFO), push(transport.OpFIFOPush, db, "q", ada(), 0)),
				request:    find(transport.OpExists, db, "q", "ada"),
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument, wantMessage: []string{"fifo"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "exists on a blob bucket", group: "read", storeType: "blob",
				notes:      "EXISTS and GET on a blob bucket answer; BLOB_READ is the verb for the payload.",
				setup:      append(bucketSetup(db, "files", server.BucketTypeBlob), call{op: transport.OpBlobWrite, payload: server.BlobWriteRequest{Database: db, Bucket: "files", Key: "ada", Data: []byte("ada's notes")}}),
				request:    find(transport.OpExists, db, "files", "ada"),
				wantOpcode: transport.OpExistsResponse, wantField: map[string]any{"exists": true},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "get on a blob bucket", group: "read", storeType: "blob",
				notes: "GET on a blob bucket answers found:true with the stored document when the payload is BSON, " +
					"else a synthesised {_id_, blob_id, size, ...metadata} document (as here).",
				setup:      append(bucketSetup(db, "files", server.BucketTypeBlob), call{op: transport.OpBlobWrite, payload: server.BlobWriteRequest{Database: db, Bucket: "files", Key: "ada", Data: []byte("ada's notes")}}),
				request:    find(transport.OpGet, db, "files", "ada"),
				wantOpcode: transport.OpGetResponse, wantField: map[string]any{"found": true},
				// The synthesised document names the blob's minted id.
				varyPaths: []string{"data.blob_id"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "find on a blob bucket is refused", group: "query", storeType: "blob",
				notes: "SCAN of a blob bucket is an ERROR frame INVALID_ARGUMENT like every wrong-type refusal (BUG-0469; " +
					"it was in band, found:false, and before that found:true with no documents).",
				setup:      bucketSetup(db, "files", server.BucketTypeBlob),
				request:    call{op: transport.OpScan, payload: server.ScanRequest{Database: db, Bucket: "files"}},
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument, wantMessage: []string{"does not scan blob buckets", "A blob bucket takes:"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "search on a btree bucket answers a full scan", group: "query", storeType: "btree",
				notes: "Indexes exist on hash buckets only; SEARCH on a type with none, or on a hash with no index that " +
					"serves the filter, walks the bucket and says so in index_used.",
				setup: append(bucketSetup(db, "b", server.BucketTypeBTree), put(db, "b", "ada", ada())),
				request: call{op: transport.OpSearch, payload: server.SearchRequest{
					Database: db, Bucket: "b", Filter: map[string]any{"Name": "Ada"}, Limit: 10,
				}},
				wantOpcode: transport.OpSearch, wantField: map[string]any{"total": 1, "index_used": "none — full scan"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "range on a hash bucket is refused", group: "query", storeType: "hash",
				notes: "A verb the bucket's store type does not answer is an ERROR frame INVALID_ARGUMENT, never " +
					"INTERNAL: the message names the bucket, its type and the type the verb needs. The same for BTREE_FIRST " +
					"and BTREE_LAST.",
				setup:      bucketSetup(db, "users", server.BucketTypeHash),
				request:    call{op: transport.OpBTreeRange, payload: server.BTreeRangeRequest{Database: db, Bucket: "users", Low: "a", High: "z"}},
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument, wantMessage: []string{"is type hash", "requires type btree"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "blob read on a hash bucket is refused", group: "read", storeType: "hash",
				notes:      "BLOB_READ and BLOB_WRITE on any type but blob are ERROR frames INVALID_ARGUMENT.",
				setup:      append(bucketSetup(db, "users", server.BucketTypeHash), put(db, "users", "ada", ada())),
				request:    call{op: transport.OpBlobRead, payload: server.BlobReadRequest{Database: db, Bucket: "users", Key: "ada"}},
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument, wantMessage: []string{"is type hash", "requires type blob"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "update on a fifo bucket is refused", group: "write", storeType: "fifo",
				notes: "UPDATE addresses one document by key, which only a hash or btree bucket has: an ERROR frame " +
					"INVALID_ARGUMENT, and as a refused data write it says write_outcome not_applied.",
				setup: append(bucketSetup(db, "q", server.BucketTypeFIFO), push(transport.OpFIFOPush, db, "q", ada(), 0)),
				request: call{op: transport.OpUpdate, payload: server.UpdateRequest{
					Database: db, Bucket: "q", Key: "ada", Set: map[string]any{"Born": int32(1816)},
				}},
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument, wantField: notApplied(),
				wantMessage: []string{"is type fifo", "requires type hash or btree"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "search on a blob bucket is refused", group: "query", storeType: "blob",
				notes: "SEARCH walks documents, which a blob bucket has none of: an ERROR frame INVALID_ARGUMENT. " +
					"The same for DISTINCT and a filtered COUNT.",
				setup: bucketSetup(db, "files", server.BucketTypeBlob),
				request: call{op: transport.OpSearch, payload: server.SearchRequest{
					Database: db, Bucket: "files", Filter: map[string]any{"Name": "Ada"}, Limit: 10,
				}},
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument, wantMessage: []string{"not supported for bucket type blob"},
			}
		},
	}
}

// wrongTypeRefusalCases are the wrong-type rows BUG-0469 added: a verb a store
// type does not answer is one ERROR frame INVALID_ARGUMENT. Appended after every
// other group, the same rule as the rest, so no earlier exchange is renumbered.
func wrongTypeRefusalCases() []func(db string) exchangeCase {
	return []func(db string) exchangeCase{
		func(db string) exchangeCase {
			return exchangeCase{
				name: "peek on a btree bucket is refused", group: "read", storeType: "btree",
				notes:      "PEEK on a type with no order to take from is an ERROR frame INVALID_ARGUMENT (BUG-0469; it was found false in band).",
				setup:      bucketSetup(db, "b", server.BucketTypeBTree),
				request:    call{op: transport.OpFIFOPeek, payload: server.PeekRequest{Database: db, Bucket: "b"}},
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument,
				wantMessage: []string{"peek is not defined for btree buckets", "A btree bucket takes:"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "push on a blob bucket is refused", group: "write", storeType: "blob",
				notes: "FIFO_PUSH on a blob is an ERROR frame INVALID_ARGUMENT with write_outcome not_applied (BUG-0469; it was " +
					"success false in band). Nothing was written.",
				setup:      bucketSetup(db, "files", server.BucketTypeBlob),
				request:    push(transport.OpFIFOPush, db, "files", ada(), 0),
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument, wantField: notApplied(),
				wantMessage: []string{"push is not supported for bucket type blob", "A blob bucket takes:"},
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "consistent get on a fifo bucket is refused", group: "read", storeType: "fifo",
				notes:      "CONSISTENT_GET answers hash and btree buckets only; on any other type it is an ERROR frame INVALID_ARGUMENT (BUG-0469; it was found false in band).",
				setup:      bucketSetup(db, "q", server.BucketTypeFIFO),
				request:    call{op: transport.OpConsistentGet, payload: server.ConsistentGetRequest{Database: db, Bucket: "q", Key: "ada"}},
				wantOpcode: transport.OpError, wantCode: transport.ErrInvalidArgument,
				wantMessage: []string{"consistent get is not supported for bucket type fifo", "A fifo bucket takes:"},
			}
		},
	}
}

// consistentGetMissingBucketCases is the row BUG-0469 item 4 added: a consistent
// get of a bucket that does not exist is the refusal every other verb answers.
func consistentGetMissingBucketCases() []func(db string) exchangeCase {
	return []func(db string) exchangeCase{
		func(db string) exchangeCase {
			return exchangeCase{
				name: "consistent get of a bucket that does not exist", group: "read",
				setup: []call{createDB(db)},
				notes: "An ERROR NOT_FOUND like get, not a found false a driver reads as a missing key (BUG-0469; it was " +
					"found false in band).",
				request:    call{op: transport.OpConsistentGet, payload: server.ConsistentGetRequest{Database: db, Bucket: "nobody", Key: "ada"}},
				wantOpcode: transport.OpError, wantCode: transport.ErrNotFound,
				wantMessage: []string{`bucket "nobody" not found in database`},
			}
		},
	}
}
