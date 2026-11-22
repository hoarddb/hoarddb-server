package server

import (
	"context"
	"strings"
	"testing"
	"unicode"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// badName holds an escape sequence between letters. It is the name a client
// must not be able to create (owner ruling 2026-10-20).
const badName = "a\x1b[2Jb"

// refusalText is the refusal in a reply to a create or a declare, or "" when it
// was accepted: an error frame's message, or an unsuccessful response's.
func refusalText(t *testing.T, frame *transport.Frame) string {
	t.Helper()
	if frame.Opcode == transport.OpError {
		var refused ErrorResponse
		if err := bson.Unmarshal(frame.Payload, &refused); err != nil {
			t.Fatalf("decoding the error frame: %v", err)
		}
		return refused.Message
	}
	var answer struct {
		Success bool   `bson:"success"`
		Message string `bson:"message"`
	}
	if err := bson.Unmarshal(frame.Payload, &answer); err != nil {
		t.Fatalf("decoding the answer to %s: %v", frame.Opcode, err)
	}
	if answer.Success {
		return ""
	}
	return answer.Message
}

// requireControlRefusal checks the refusal names the thing, repeats the name
// without the raw control character, and says what to run.
func requireControlRefusal(t *testing.T, what, refusal string, wantInFix ...string) {
	t.Helper()
	if refusal == "" {
		t.Fatalf("%s holding a control character was accepted", what)
	}
	for _, character := range refusal {
		if unicode.IsControl(character) && character != '\n' {
			t.Errorf("the refusal repeats the control character %U raw: %q", character, refusal)
		}
	}
	for _, want := range append([]string{"control character", "U+001B"}, wantInFix...) {
		if !strings.Contains(refusal, want) {
			t.Errorf("the refusal lacks %q: %s", want, refusal)
		}
	}
}

// TestStandaloneCreatePathsRefuseControlCharacterNames covers a node that does
// not route, where the door judges the frame: a database, a bucket, a typed
// field at depth, a store field with a nested one, and a declare on an
// existing bucket.
func TestStandaloneCreatePathsRefuseControlCharacterNames(t *testing.T) {
	s := newTestServer(t)
	handler := NewHandler(s)
	handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "users", Type: BucketTypeHash})

	send := func(op transport.Opcode, req any) *transport.Frame {
		t.Helper()
		frame, err := handler(context.Background(), transport.NewFrame(op, mustMarshalBSON(t, req)))
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return frame
	}

	t.Run("create database", func(t *testing.T) {
		requireControlRefusal(t, "a database name", refusalText(t, send(transport.OpCreateDB, CreateDatabaseRequest{Name: badName})),
			"create database <name>")
	})
	t.Run("create bucket", func(t *testing.T) {
		requireControlRefusal(t, "a bucket name", refusalText(t, send(transport.OpCreateBucket,
			CreateBucketRequest{Database: "app", Name: badName, Type: BucketTypeHash})), "create bucket <name> in")
	})
	t.Run("create bucket in a database named with one", func(t *testing.T) {
		requireControlRefusal(t, "a database name on a bucket create", refusalText(t, send(transport.OpCreateBucket,
			CreateBucketRequest{Database: badName, Name: "ok", Type: BucketTypeHash})))
	})
	t.Run("typed field at depth", func(t *testing.T) {
		options := map[string]interface{}{"fields": []interface{}{
			map[string]interface{}{"name": "Address", "type": "object", "members": []interface{}{
				map[string]interface{}{"name": badName, "type": "string"},
			}},
		}}
		requireControlRefusal(t, "a typed member name", refusalText(t, send(transport.OpCreateBucket,
			CreateBucketRequest{Database: "app", Name: "typed", Type: BucketTypeHash, Options: options})), "Field name", "alter bucket")
	})
	t.Run("member under an array's element type", func(t *testing.T) {
		options := map[string]interface{}{"fields": []interface{}{
			map[string]interface{}{"name": "Tags", "type": "array", "elem": map[string]interface{}{
				"type": "object", "members": []interface{}{map[string]interface{}{"name": badName, "type": "string"}},
			}},
		}}
		requireControlRefusal(t, "a member name under an element type", refusalText(t, send(transport.OpCreateBucket,
			CreateBucketRequest{Database: "app", Name: "elems", Type: BucketTypeHash, Options: options})), "Field name")
	})
	t.Run("store field nested in a store field", func(t *testing.T) {
		options := map[string]interface{}{"stores": []interface{}{
			map[string]interface{}{"name": "Photos", "engine": "fifoStore", "fields": []interface{}{
				map[string]interface{}{"name": badName, "engine": "hashStore"},
			}},
		}}
		requireControlRefusal(t, "a nested store field name", refusalText(t, send(transport.OpCreateBucket,
			CreateBucketRequest{Database: "app", Name: "stored", Type: BucketTypeHash, Options: options})), "Field name")
	})
	t.Run("declare store fields on an existing bucket", func(t *testing.T) {
		requireControlRefusal(t, "a declared store field name", refusalText(t, send(transport.OpDeclareStoreFields,
			DeclareStoreFieldsRequest{Database: "app", Name: "users", Stores: []map[string]interface{}{
				{"name": badName, "engine": "hashStore"},
			}})), "Field name")
	})
	t.Run("a printable name still creates", func(t *testing.T) {
		if refusal := refusalText(t, send(transport.OpCreateBucket,
			CreateBucketRequest{Database: "app", Name: "café-ünï", Type: BucketTypeHash})); refusal != "" {
			t.Errorf("a printable name was refused: %s", refusal)
		}
	})
}

// TestClusteredCreatePathsRefuseControlCharacterNamesBeforeLogging: on a
// cluster the coordinator judges a database create and the owner judges a
// bucket create or a declare, before the append, so a refused name is neither
// logged nor shipped.
func TestClusteredCreatePathsRefuseControlCharacterNamesBeforeLogging(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	waitForFeatureConvergence(t, tc)
	owned := bucketOwnedBy(t, tc, 0, "plain-")
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: owned, Type: BucketTypeHash})
	wal := tc.nodes[0].server.core.wal

	loggedBefore := wal.LastSequence()
	t.Run("create database", func(t *testing.T) {
		requireControlRefusal(t, "a database name", refusalText(t, tc.frame(0, transport.OpCreateDB, CreateDatabaseRequest{Name: badName})),
			"create database <name>")
	})
	t.Run("create bucket", func(t *testing.T) {
		name := bucketOwnedBy(t, tc, 0, "a\x1b[2J-")
		requireControlRefusal(t, "a bucket name", refusalText(t, tc.frame(0, transport.OpCreateBucket,
			CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})), "create bucket <name> in")
		requireNoBucketAnywhere(t, tc, testDatabase, name)
	})
	t.Run("typed field", func(t *testing.T) {
		name := bucketOwnedBy(t, tc, 0, "typed-")
		options := map[string]interface{}{"fields": []interface{}{map[string]interface{}{"name": badName, "type": "string"}}}
		requireControlRefusal(t, "a typed field name", refusalText(t, tc.frame(0, transport.OpCreateBucket,
			CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash, Options: options})), "Field name")
	})
	t.Run("declare store fields", func(t *testing.T) {
		requireControlRefusal(t, "a declared store field name", refusalText(t, tc.frame(0, transport.OpDeclareStoreFields,
			DeclareStoreFieldsRequest{Database: testDatabase, Name: owned, Stores: []map[string]interface{}{
				{"name": badName, "engine": "hashStore"},
			}})), "Field name")
	})
	if logged := wal.LastSequence(); logged != loggedBefore {
		t.Errorf("the owner's log moved from %d to %d for refused names", loggedBefore, logged)
	}
}

// TestAnExistingControlCharacterNameStaysUsable is the other side of the rule:
// a database and a bucket made before it (here by the in-process create, which
// the rule does not touch, as a replay and a catalog load do not) are still
// listed, read from, written to and dropped through the client door.
func TestAnExistingControlCharacterNameStaysUsable(t *testing.T) {
	s := newTestServer(t)
	if resp, err := s.CreateDatabase(badName); err != nil || !resp.Success {
		t.Fatalf("seeding the database: %+v, %v", resp, err)
	}
	if resp, err := s.CreateBucket(CreateBucketRequest{Database: badName, Name: badName, Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("seeding the bucket: %+v, %v", resp, err)
	}
	// A follower's apply and a replay reach the create handlers directly, not
	// the client door: a rule added there would refuse these.
	seed := func(op transport.Opcode, handle func(context.Context, *transport.Frame, *Server) (*transport.Frame, error), req any) {
		t.Helper()
		frame, err := handle(context.Background(), transport.NewFrame(op, mustMarshalBSON(t, req)), s)
		if err != nil {
			t.Fatalf("%s through its handler: %v", op, err)
		}
		if refusal := refusalText(t, frame); refusal != "" {
			t.Fatalf("%s through its handler was refused: %s", op, refusal)
		}
	}
	seed(transport.OpCreateDB, handleCreateDB, CreateDatabaseRequest{Name: "applied" + badName})
	seed(transport.OpCreateBucket, handleCreateBucket, CreateBucketRequest{Database: "applied" + badName, Name: "applied" + badName, Type: BucketTypeHash,
		Options: map[string]interface{}{"stores": []interface{}{map[string]interface{}{"name": badName, "engine": "hashStore"}}}})
	handler := NewHandler(s)

	var databases ListDatabasesResponse
	if err := bson.Unmarshal(handlerSend(t, handler, transport.OpListDBs, ListDatabasesRequest{}).Payload, &databases); err != nil {
		t.Fatal(err)
	}
	if len(databases.Databases) != 2 {
		t.Fatalf("databases = %+v, want the two seeded", databases.Databases)
	}
	var buckets ListBucketsResponse
	if err := bson.Unmarshal(handlerSend(t, handler, transport.OpListBuckets, ListBucketsRequest{Database: badName}).Payload, &buckets); err != nil {
		t.Fatal(err)
	}
	if len(buckets.Buckets) != 1 || buckets.Buckets[0].Name != badName {
		t.Fatalf("buckets = %+v, want the one named %q", buckets.Buckets, badName)
	}
	handlerSend(t, handler, transport.OpPut, InsertRequest{Database: badName, Bucket: badName, Key: "k", Data: map[string]interface{}{"n": 1}})
	handlerSend(t, handler, transport.OpGet, FindRequest{Database: badName, Bucket: badName, Key: "k"})
	handlerSend(t, handler, transport.OpDeleteBucket, DeleteBucketRequest{Database: badName, Name: badName})
	handlerSend(t, handler, transport.OpDeleteDB, DeleteDatabaseRequest{Name: badName})
}

// TestRestoreRefusesAControlCharacterNameAndSaysHowToKeepTheDocuments: a
// restore creates its database and bucket through the same client door, so a
// dump holding such a name is refused for that bucket, before anything is
// changed, and the refusal names the ingest that keeps its documents.
func TestRestoreRefusesAControlCharacterNameAndSaysHowToKeepTheDocuments(t *testing.T) {
	s := newTestServer(t)
	ctx := rootContext()
	for _, request := range []RestoreStartRequest{
		{Database: "app", Bucket: badName, FormatVersion: dumpManifestFormatVersion, StoreType: BucketTypeHash},
		{Database: badName, Bucket: "users", FormatVersion: dumpManifestFormatVersion, StoreType: BucketTypeHash},
	} {
		_, refusal := dumpDispatch(t, s, ctx, transport.OpRestoreStart, request)
		requireControlRefusal(t, "a restored name", refusal, "hoarddb ingest")
	}
	if databases, err := s.ListDatabases(ListDatabasesRequest{}); err != nil || len(databases.Databases) != 0 {
		t.Errorf("a refused restore created %+v (%v)", databases, err)
	}
}
