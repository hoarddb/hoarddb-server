package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The batch-request group (batch-request spec §8.2): BATCH (0x22) answered by
// BATCH_OK (0x23), one answer per item, each the lone verb's reply. A batch is
// not a transaction. The group is appended after every other so no earlier
// exchange is renumbered.

// batchGroup is the group's name: not "batch", whose InsertMany vector is in
// the write group.
const batchGroup = "batch-request"

// batchNotes opens every batch vector's notes.
const batchNotes = "A batch is not a transaction: each item is applied, refused or left unknown on its own, and answers[i] answers items[i]. "

// batchVary are the minted values inside a batch's answers: a pushed entry's
// id and an element's minted _id_.
var batchVary = []string{"answers[].payload.entry_id", "answers[].payload.seq", "answers[].payload.element._id_", "answers[].payload.items[].element._id_"}

// batchUsers creates bucket User_<name> for each name, declaring Feed (fifo)
// and Posts (hash), with the user's document <name>.
func batchUsers(db string, names ...string) []call {
	calls := []call{createDB(db)}
	for _, name := range names {
		calls = append(calls, call{op: transport.OpCreateBucket, payload: server.CreateBucketRequest{
			Database: db, Name: "User_" + name, Type: server.BucketTypeHash,
			Options: map[string]any{"stores": []any{
				map[string]any{"name": "Feed", "engine": "fifo"},
				map[string]any{"name": "Posts", "engine": "hash"},
			}},
		}}, put(db, "User_"+name, name, map[string]any{"name": name, "posts_n": int32(0)}))
	}
	return calls
}

func batchField(db, user, field string) server.FieldRequest {
	return server.FieldRequest{Database: db, Bucket: "User_" + user, Key: user, Field: field}
}

// batchItem is one item: the request's opcode and the request.
func batchItem(op transport.Opcode, request any) bson.D {
	return bson.D{{Key: "opcode", Value: int32(op)}, {Key: "payload", Value: request}}
}

func batchPush(db, user string, doc map[string]any) bson.D {
	request := batchField(db, user, "Feed")
	request.Data = doc
	return batchItem(transport.OpFieldPush, request)
}

func batchPut(db, user, element string, doc map[string]any) bson.D {
	request := batchField(db, user, "Posts")
	request.Element, request.Data = element, doc
	return batchItem(transport.OpFieldPut, request)
}

func batchGet(db, user, element string) bson.D {
	request := batchField(db, user, "Posts")
	request.Element = element
	return batchItem(transport.OpFieldGet, request)
}

// batchInc is a keyed $inc, with only the members a driver sends.
func batchInc(db, user, key string) bson.D {
	return batchItem(transport.OpUpdate, bson.D{{Key: "database", Value: db}, {Key: "bucket", Value: "User_" + user}, {Key: "key", Value: key},
		{Key: "inc", Value: bson.D{{Key: "posts_n", Value: int64(1)}}}})
}

// batchCall is a BATCH request of items.
func batchCall(items ...bson.D) call {
	list := make(bson.A, len(items))
	for i, item := range items {
		list[i] = item
	}
	return call{op: transport.OpBatch, payload: bson.D{{Key: "items", Value: list}}}
}

// batchCases are the group's exchanges, on topology single unless named.
func batchCases() []func(db string) exchangeCase {
	cases := []func(db string) exchangeCase{
		func(db string) exchangeCase {
			peek := batchField(db, "b", "Feed")
			return exchangeCase{
				name: "batch of all seven verbs", group: batchGroup, storeType: "hash",
				notes: batchNotes + "Push, put, delete, get, len, peek and an update of one key. Items naming the same bucket apply in batch order, " +
					"so the get sees the put before it and the delete comes after both.",
				setup: batchUsers(db, "a", "b"),
				request: batchCall(
					batchPut(db, "a", "p-1", map[string]any{"text": "hello"}),
					batchPush(db, "b", map[string]any{"post": "p-1"}),
					batchGet(db, "a", "p-1"),
					batchItem(transport.OpFieldLen, batchField(db, "b", "Feed")),
					batchItem(transport.OpFieldPeek, server.FieldPeekRequest{FieldRequest: peek, Limit: 2}),
					batchInc(db, "a", "a"),
					batchItem(transport.OpFieldDelete, func() server.FieldRequest {
						request := batchField(db, "a", "Posts")
						request.Element = "p-1"
						return request
					}()),
				),
				wantOpcode: transport.OpBatchOK,
				varyPaths:  batchVary,
			}
		},
		func(db string) exchangeCase {
			missing := server.FieldRequest{Database: db, Bucket: "User_missing", Key: "missing", Field: "Feed", Data: map[string]any{"post": "p-1"}}
			return exchangeCase{
				name: "batch with a missing bucket in the middle", group: batchGroup, storeType: "hash",
				notes: batchNotes + "Item 2's bucket does not exist: it is answered ERROR NOT_FOUND with write_outcome not_applied and the lone refusal's text, " +
					"while items 1 and 3 apply.",
				setup:      batchUsers(db, "a", "b"),
				request:    batchCall(batchPush(db, "a", map[string]any{"n": int32(1)}), batchItem(transport.OpFieldPush, missing), batchPush(db, "b", map[string]any{"n": int32(3)})),
				wantOpcode: transport.OpBatchOK,
				varyPaths:  batchVary,
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "batch update of a missing key", group: batchGroup, storeType: "hash",
				notes: batchNotes + "An update of a key with no document answers the verb's own reply with success false, as the lone update does; " +
					"it is not applied, and resending it fails the same way.",
				setup:      batchUsers(db, "a"),
				request:    batchCall(batchInc(db, "a", "ghost")),
				wantOpcode: transport.OpBatchOK,
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "batch chain stopped by its second item", group: batchGroup, storeType: "hash",
				notes: batchNotes + "Items 1 to 3 name one bucket. Item 2 is not applied, so item 3 is not run: it answers ERROR UNAVAILABLE, " +
					"write_outcome not_applied, naming item 2, and the client resends from item 2.",
				setup:      batchUsers(db, "a"),
				request:    batchCall(batchPush(db, "a", map[string]any{"n": int32(1)}), batchInc(db, "a", "ghost"), batchPush(db, "a", map[string]any{"n": int32(3)})),
				wantOpcode: transport.OpBatchOK,
				varyPaths:  batchVary,
			}
		},
		func(db string) exchangeCase {
			peek := batchField(db, "a", "Feed")
			first, second := batchField(db, "a", "Feed"), batchField(db, "a", "Feed")
			first.Data, second.Data = map[string]any{"n": int32(1)}, map[string]any{"n": int32(2)}
			return exchangeCase{
				name: "batch get miss and peek of two", group: batchGroup, storeType: "hash",
				notes: batchNotes + "A get of an element that is not there answers found false, which is a success; a peek with limit 2 answers the next two in pop order.",
				setup: append(batchUsers(db, "a"), field(transport.OpFieldPush, first), field(transport.OpFieldPush, second)),
				request: batchCall(batchGet(db, "a", "nope"),
					batchItem(transport.OpFieldPeek, server.FieldPeekRequest{FieldRequest: peek, Limit: 2})),
				wantOpcode: transport.OpBatchOK,
				varyPaths:  batchVary,
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "batch read past the reply budget", group: batchGroup, storeType: "hash", server: "results", noReplay: true,
				notes: batchNotes + "Captured with HOARDDB_QUERY_MAX_RESULT_BYTES=1048576: the reply budget is min(that, 16 MiB). User_a's Posts hold an " +
					"element of 1.2 MB, written before the request and not recorded as setup (its size). Its get's answer would pass the budget, so it is " +
					"answered RESULT_TOO_LARGE with max_result_bytes (the budget) and retained_bytes (the bytes charged before it, here none), and nothing changed. " +
					"The push applies.",
				prepare:    bigBatchElements,
				request:    batchCall(batchGet(db, "a", "one"), batchPush(db, "b", map[string]any{"n": int32(1)})),
				wantOpcode: transport.OpBatchOK,
				varyPaths:  batchVary,
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "batch update answered without its document", group: batchGroup, storeType: "hash", server: "results", noReplay: true,
				notes: batchNotes + "Captured with HOARDDB_QUERY_MAX_RESULT_BYTES=1048576. The update of the 1.2 MB document \"big\", written before the request " +
					"and not recorded as setup, would answer more than the budget, so it is applied and answered without its document: success true, " +
					"doc_omitted true. Read the document if it is needed.",
				prepare:    bigBatchElements,
				request:    batchCall(batchItem(transport.OpUpdate, server.UpdateRequest{Database: db, Bucket: "User_a", Key: "big", Inc: map[string]int64{"n": 1}})),
				wantOpcode: transport.OpBatchOK,
				varyPaths:  batchVary,
			}
		},
		func(db string) exchangeCase {
			return exchangeCase{
				name: "batch from a reader that carries a write", group: batchGroup, storeType: "hash", noReplay: true,
				notes: batchNotes + "Sent on a connection authenticated as batch-reader, which holds only {role: reader, database: <this vector's database>}. " +
					"Every item is authorized on its own before any runs: the push refuses the whole batch with PERMISSION_DENIED, the lone refusal's text " +
					"prefixed with the item's position, and nothing in the batch was run.",
				prepare:  createBatchReader,
				username: "batch-reader", password: readerPassword,
				request:    batchCall(batchGet(db, "a", "p-1"), batchPush(db, "a", map[string]any{"n": int32(1)})),
				wantOpcode: transport.OpError, wantCode: transport.ErrPermissionDenied, wantField: notApplied(),
				wantMessage: []string{"Item 2 of 2", "permission denied", "Nothing in the batch was run."},
			}
		},
	}
	cases = append(cases, batchRefusalCases()...)
	// "batch spanning two owners on a cluster" was captured on the retired three-node cluster; its slot stays.
	return append(cases, retiredCase)
}

// batchRefusalCases are the whole-batch refusals of §3.4 a vector can hold:
// the batches over 16 MiB are left to the server's tests (their size).
func batchRefusalCases() []func(db string) exchangeCase {
	refused := func(name, notes string, code transport.ErrorCode, written bool, wantMessage []string, items func(db string) bson.A) func(db string) exchangeCase {
		return func(db string) exchangeCase {
			c := exchangeCase{
				name: name, group: batchGroup, storeType: "hash",
				notes:       batchNotes + notes + " The refusal is the whole batch's: nothing in it was run.",
				setup:       batchUsers(db, "a"),
				request:     call{op: transport.OpBatch, payload: bson.D{{Key: "items", Value: items(db)}}},
				wantOpcode:  transport.OpError,
				wantCode:    code,
				wantMessage: append(wantMessage, "Nothing in the batch was run."),
			}
			if written {
				c.wantField = notApplied()
			}
			return c
		}
	}
	one := func(item bson.D) func(db string) bson.A {
		return func(db string) bson.A { return bson.A{batchPush(db, "a", map[string]any{"n": int32(1)}), item} }
	}
	payloadWith := func(db string, extra ...bson.E) bson.D {
		return append(bson.D{{Key: "database", Value: db}, {Key: "bucket", Value: "User_a"}, {Key: "key", Value: "a"}, {Key: "field", Value: "Feed"}}, extra...)
	}
	return []func(db string) exchangeCase{
		refused("batch with no items", "A batch carries 1 to 1,000 items.", transport.ErrInvalidArgument, false,
			[]string{"A batch needs at least one item"}, func(string) bson.A { return bson.A{} }),
		refused("batch over 1,000 items", "Over the count is BATCH_TOO_LARGE, not INVALID_ARGUMENT: the fix is to split it, and the text computes the split. "+
			"The count is refused before any item is read, so the vector's 1,001 items are empty documents, which keeps the file small.",
			transport.ErrBatchTooLarge, false, []string{"This batch has 1,001 items; a batch carries at most 1,000", "items 1 to 1,000, then 1,001 to 1,001"},
			func(string) bson.A {
				items := make(bson.A, 1001)
				for i := range items {
					items[i] = bson.D{}
				}
				return items
			}),
		refused("batch carrying a batch", "A batch cannot carry another.", transport.ErrInvalidArgument, true,
			[]string{"Item 2 of 2 is itself a batch"}, func(db string) bson.A {
				return one(bson.D{{Key: "opcode", Value: int32(transport.OpBatch)}, {Key: "payload", Value: payloadWith(db)}})(db)
			}),
		refused("batch carrying an opcode that is not a request", "", transport.ErrInvalidArgument, true,
			[]string{"names opcode 0xEE, which is not a hoardDB request"}, func(db string) bson.A {
				return one(bson.D{{Key: "opcode", Value: int32(0xEE)}, {Key: "payload", Value: payloadWith(db)}})(db)
			}),
		refused("batch carrying a node-to-node opcode", "", transport.ErrInvalidArgument, true,
			[]string{"Item 2 of 2 is REPL_PUSH, which only cluster members send"}, func(db string) bson.A {
				return one(bson.D{{Key: "opcode", Value: int32(transport.OpReplPush)}, {Key: "payload", Value: payloadWith(db)}})(db)
			}),
		refused("batch carrying a bucket verb", "Bucket-level verbs are not carried in this version.", transport.ErrInvalidArgument, true,
			[]string{"Item 2 of 2 is PUT, which a batch does not carry"}, func(db string) bson.A {
				return one(bson.D{{Key: "opcode", Value: int32(transport.OpPut)}, {Key: "payload", Value: payloadWith(db)}})(db)
			}),
		refused("batch carrying a field pop", "A lost batch reply would lose the popped entries, so a pop is sent on its own.", transport.ErrInvalidArgument, true,
			[]string{"is a pop, which a batch does not carry"}, func(db string) bson.A {
				return one(batchItem(transport.OpFieldPop, batchField(db, "a", "Feed")))(db)
			}),
		refused("batch carrying a field list", "A list is paged and its answer can fill a reply by itself.", transport.ErrInvalidArgument, true,
			[]string{"is paged and its answer can fill a reply by itself"}, func(db string) bson.A {
				return one(batchItem(transport.OpFieldList, batchField(db, "a", "Feed")))(db)
			}),
		refused("batch carrying an update with a filter", "A batch carries an update of one key.", transport.ErrInvalidArgument, true,
			[]string{"with a filter, is a scan of the bucket"}, func(db string) bson.A {
				return one(batchItem(transport.OpUpdate, bson.D{{Key: "database", Value: db}, {Key: "bucket", Value: "User_a"},
					{Key: "filter", Value: bson.D{{Key: "name", Value: "a"}}}, {Key: "inc", Value: bson.D{{Key: "posts_n", Value: int64(1)}}}}))(db)
			}),
		refused("batch carrying an update of every match", "", transport.ErrInvalidArgument, true,
			[]string{"an update of every matching document"}, func(db string) bson.A {
				return one(batchItem(transport.OpUpdate, bson.D{{Key: "database", Value: db}, {Key: "bucket", Value: "User_a"},
					{Key: "multi", Value: true}, {Key: "inc", Value: bson.D{{Key: "posts_n", Value: int64(1)}}}}))(db)
			}),
		refused("batch item naming a write concern", "One write concern, on the batch, covers every write item.", transport.ErrInvalidArgument, true,
			[]string{"names a write concern (writeconcern)"}, func(db string) bson.A {
				return one(batchItem(transport.OpFieldPush, payloadWith(db, bson.E{Key: "writeconcern", Value: "majority"})))(db)
			}),
		refused("batch item naming a write_concern", "Both spellings are refused on an item.", transport.ErrInvalidArgument, true,
			[]string{"names a write concern (write_concern)"}, func(db string) bson.A {
				return one(batchItem(transport.OpFieldPush, payloadWith(db, bson.E{Key: "write_concern", Value: "majority"})))(db)
			}),
		refused("batch item with no payload", "Each item is {opcode, payload}.", transport.ErrInvalidArgument, true,
			[]string{"Item 2 of 2 has no payload document"}, func(db string) bson.A {
				return one(bson.D{{Key: "opcode", Value: int32(transport.OpFieldPush)}})(db)
			}),
		refused("batch item naming no bucket", "Every carried verb is addressed to one bucket.", transport.ErrInvalidArgument, true,
			[]string{"Item 2 of 2 names no bucket"}, func(db string) bson.A {
				return one(batchItem(transport.OpFieldPush, bson.D{{Key: "database", Value: db}, {Key: "key", Value: "a"}, {Key: "field", Value: "Feed"}}))(db)
			}),
		refused("batch item whose opcode is not an int32", "The opcode is a BSON int32 from 0 to 255.", transport.ErrInvalidArgument, false,
			[]string{"Item 1 of 1: opcode must be a 32-bit integer from 0 to 255; it is a double"}, func(db string) bson.A {
				return bson.A{bson.D{{Key: "opcode", Value: 208.0}, {Key: "payload", Value: payloadWith(db)}}}
			}),
	}
}

// bigBatchElements writes, through the results server's driver port and not
// recorded as setup, users a and b, a Posts element "one" of 1.2 MB on a, and
// a 1.2 MB document "big" in User_a: each answer alone passes the 1 MiB budget,
// so the vector's answers stay small.
func bigBatchElements(ctx context.Context, g *generation, db string) error {
	one := batchField(db, "a", "Posts")
	one.Element, one.Data = "one", map[string]any{"text": strings.Repeat("a", 1_200_000)}
	return g.requestsOn(ctx, "results", append(batchUsers(db, "a", "b"),
		field(transport.OpFieldPut, one),
		put(db, "User_a", "big", map[string]any{"text": strings.Repeat("c", 1_200_000), "n": int32(0)})))
}

// createBatchReader creates users a's bucket and the user batch-reader, a
// reader of the vector's database.
func createBatchReader(ctx context.Context, g *generation, db string) error {
	if err := g.requestsOn(ctx, "single", batchUsers(db, "a")); err != nil {
		return err
	}
	service, err := g.service("single")
	if err != nil {
		return err
	}
	material, err := server.NewCredential(readerPassword, server.Argon2Params{})
	if err != nil {
		return fmt.Errorf("deriving the batch reader's credential: %w", err)
	}
	if err := service.Auth().CreateUser("batch-reader", material, []server.Grant{{Role: "reader", DB: db}}, true); err != nil {
		return fmt.Errorf("creating the batch reader: %w", err)
	}
	return nil
}
