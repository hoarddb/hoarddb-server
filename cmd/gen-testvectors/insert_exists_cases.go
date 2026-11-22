package main

import (
	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// insertExistsCases pin the insert refusal of document-placement step 5 on the
// wire: PUT, BATCH_PUT and BTREE_INSERT of an _id_ the bucket holds answer
// DOCUMENT_EXISTS (0x12). They are the newest cases, so every earlier exchange
// keeps its number, database name and request ID.
func insertExistsCases() []func(db string) exchangeCase {
	return []func(db string) exchangeCase{
		func(db string) exchangeCase {
			return exchangeCase{
				name: "insert of an existing id is refused", group: "write", storeType: "hash",
				notes: "PUT is insert: an _id_ the bucket holds is an ERROR frame DOCUMENT_EXISTS (0x12), write_outcome " +
					"not_applied, and the stored document is unchanged. The message names upsert and update, and says to retry " +
					"an insert whose answer was lost with UPSERT. It never echoes the document.",
				setup:      append(bucketSetup(db, "users", server.BucketTypeHash), put(db, "users", "ada", map[string]any{"Name": "Ada"})),
				request:    put(db, "users", "ada", map[string]any{"Name": "Ada Lovelace"}),
				wantOpcode: transport.OpError, wantCode: transport.ErrDocumentExists,
				wantMessage: []string{`Document "ada" already exists`, `db.users.upsert({_id_: "ada", ...})`, "retry with upsert"},
				wantField:   notApplied(),
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "insert many naming an existing id is refused whole", group: "write", storeType: "hash",
				notes: "A BATCH_PUT naming an _id_ the bucket holds is atomic: nothing is stored, and the ERROR frame " +
					"DOCUMENT_EXISTS carries results[] with the held position's code and every other position not_applied.",
				setup: append(bucketSetup(db, "users", server.BucketTypeHash), put(db, "users", "ada", map[string]any{"Name": "Ada"})),
				request: call{op: transport.OpBatchPut, payload: server.BatchInsertRequest{
					Database: db, Bucket: "users",
					Data: []map[string]any{
						{"_id_": "grace", "Name": "Grace"},
						{"_id_": "ada", "Name": "Ada Lovelace"},
					},
				}},
				wantOpcode: transport.OpError, wantCode: transport.ErrDocumentExists,
				wantMessage: []string{`"ada" at position 2`, "Nothing was stored"},
				wantField:   notApplied(),
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "btree insert of an existing key is refused", group: "write", storeType: "btree",
				notes:      "BTREE_INSERT is insert, as PUT is: a key the bucket holds is DOCUMENT_EXISTS, not_applied.",
				setup:      append(bucketSetup(db, "b", server.BucketTypeBTree), put(db, "b", "ada", map[string]any{"Name": "Ada"})),
				request:    call{op: transport.OpBTreeInsert, payload: server.InsertRequest{Database: db, Bucket: "b", Key: "ada", Data: map[string]any{"Name": "Ada Lovelace"}}},
				wantOpcode: transport.OpError, wantCode: transport.ErrDocumentExists,
				wantMessage: []string{`Document "ada" already exists`},
				wantField:   notApplied(),
			}
		},
	}
}
