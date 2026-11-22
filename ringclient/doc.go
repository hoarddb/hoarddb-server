// Package ringclient is a Go client for the hoardDB entry-ring wire protocol: the TLS
// connection, AUTH, and the RING_ADMIN, RING_KEYED, RING_LIST and RING_COUNT opcodes of
// the entry ring client wire spec and the entry ring admin wire spec.
//
// The shape follows rule 11 of the project: a bucket holds documents (entities), and a
// document carries its own stores. Everything keyed is a method of a [Bucket]:
//
//	client, err := ringclient.Dial(ctx, ringclient.Options{
//		Addr: "127.0.0.1:7433", User: "root", Password: pw, Insecure: true, // local development only
//	})
//	if err != nil { ... }
//	defer client.Close()
//
//	_, err = client.CreateDatabase(ctx, "app", 0)
//	_, err = client.CreateBucket(ctx, "app", "User", []ringclient.FieldDecl{
//		{Name: "Name", Type: "string"}, {Name: "Purchases", Type: "fifoStore"},
//	})
//	users := client.Bucket("app", "User")
//	_, err = users.Insert(ctx, "ada", ringclient.Doc{"Name": "Ada"})
//	_, err = users.Push(ctx, "ada", "Purchases", bson.D{{Key: "item", Value: "coffee"}})
//	doc, err := users.Get(ctx, "ada")
//
// A Client is safe for concurrent use. It keeps one connection and pipelines requests on
// it: every request carries a 16-byte request id and its reply is matched by that id, so
// replies may arrive in any order. A retry of a write passes the same id with
// [WithRequestID], which makes the owner answer from its dedupe window instead of applying
// the write twice.
//
// The header of a keyed request (bucket id, declaration version, schema version seen) is
// learned from the RING_ADMIN reply and cached per bucket. A SCHEMA_NOT_CURRENT refusal
// refreshes the cached header and sends the request once more under the same request id.
//
// The package imports hoardDB/cluster, hoardDB/cluster/wire, hoardDB/store, hoardDB/storage
// and hoardDB/transport, all BSL 1.1. See the build report's SPEC-GAP RC-1 for what that
// means for the Apache-2.0 rule on clients/.
package ringclient
