package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// requireUnreachableName fails unless err is E-18 naming field, answered
// FIELD_TYPE.
func requireUnreachableName(t *testing.T, what string, err error, field string, parts ...string) {
	t.Helper()
	if err == nil {
		t.Errorf("%s was stored; a top-level name containing \".\", \"[\" or \"]\" must be refused", what)
		return
	}
	if !errors.Is(err, ErrFieldType) || codeFor(err) != transport.ErrFieldType {
		t.Errorf("%s was refused with %v (code %s); want E-18 answered FIELD_TYPE", what, err, codeFor(err))
	}
	for _, part := range append([]string{`Field name "` + field + `" in this document cannot be stored`, "Nothing was written."}, parts...) {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("%s was refused with %q; want it to say %q", what, err, part)
		}
	}
}

// TestTopLevelNameWithADotOrBracketIsRefused is OQ-25 / E-18 on an undeclared
// bucket: every write that stores a whole document refuses a top-level name no
// path can reach, and stores nothing.
func TestTopLevelNameWithADotOrBracketIsRefused(t *testing.T) {
	s := newPostServer(t)
	createTestBucket(t, s, "app", "people", BucketTypeHash)
	createTestBucket(t, s, "app", "queue", BucketTypeFIFO)

	_, err := s.Insert(InsertRequest{Database: "app", Bucket: "people", Key: "a", Data: map[string]interface{}{"Address.City": "Tirana"}})
	requireUnreachableName(t, "an insert", err, "Address.City",
		`bucket "people" (database "app")`,
		"(Address.City is the member City of Address)",
		"Nest it instead, for example {Address: {City: …}}, or rename it without the character, for example Address_City.")
	if documentOf(t, s, "app", "people", "a") != nil {
		t.Error("the refused insert stored the document")
	}
	_, err = s.Insert(InsertRequest{Database: "app", Bucket: "people", Key: "b", Data: map[string]interface{}{"Lines[3]": 1, "ok": 1}})
	requireUnreachableName(t, "a put", err, "Lines[3]", "(Lines[3] is position 3 of Lines)")
	_, err = s.Insert(InsertRequest{Database: "app", Bucket: "people", Key: "c", Data: map[string]interface{}{"odd]": 1}})
	requireUnreachableName(t, "a name with a lone bracket", err, "odd]", "Rename it without the character, for example odd.")

	_, err = s.Push(PushRequest{Database: "app", Bucket: "queue", Data: map[string]interface{}{"a.b": 1}})
	requireUnreachableName(t, "a fifo push", err, "a.b")
	_, err = s.Push(PushRequest{Database: "app", Bucket: "people", Data: map[string]interface{}{"a.b": 1}})
	requireUnreachableName(t, "a hash push", err, "a.b")

	_, err = s.BatchInsert(BatchInsertRequest{Database: "app", Bucket: "people", Data: []map[string]interface{}{
		{"_id_": "first", "ok": 1}, {"_id_": "second", "x.y": 1},
	}})
	requireUnreachableName(t, "a batch", err, "x.y", `Document 2 of the batch (_id_ "second"): `)
	if documentOf(t, s, "app", "people", "first") != nil {
		t.Error("the refused batch stored its first document")
	}

	_, err = s.FieldPushCtx(context.Background(), FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Comments",
		Data: map[string]interface{}{"by.whom": "x"}})
	requireUnreachableName(t, "a field-store push", err, "by.whom")
	_, err = s.FieldPutCtx(context.Background(), FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Tags", Element: "t1",
		Data: map[string]interface{}{"x[0]": "x"}})
	requireUnreachableName(t, "a field-store put", err, "x[0]")
}

// TestNestedNameWithADotIsStored: only top-level names are scanned (spec
// §4.9.8): a member name with a dot inside an object is stored, unreachable by
// a path, as the user documentation says.
func TestNestedNameWithADotIsStored(t *testing.T) {
	s := newTestServer(t)
	createTestBucket(t, s, "app", "people", BucketTypeHash)
	mustInsert(t, s, "app", "people", "a", map[string]interface{}{"Address": map[string]interface{}{"a.b": int32(1)}})
	if doc := documentOf(t, s, "app", "people", "a"); doc == nil {
		t.Fatal("a nested name with a dot was refused")
	}
}

// TestTypeRefusalCarriesFieldTypeCode, the E-18 row: over the wire the
// refusal is an error frame carrying FIELD_TYPE (0x10), and the session
// survives it.
func TestTypeRefusalCarriesFieldTypeCode(t *testing.T) {
	if transport.ErrFieldType != 0x10 || transport.ErrFieldType.String() != "FIELD_TYPE" {
		t.Fatalf("transport.ErrFieldType = %#x %q, want 0x10 FIELD_TYPE", uint8(transport.ErrFieldType), transport.ErrFieldType)
	}
	tc := newTestCluster(t, 1, 1)
	defer tc.stop()
	createTestHashBucket(t, tc)
	refused := tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "a",
		Data: map[string]interface{}{"Address.City": "Tirana"}})
	answer := refusalOf(t, refused)
	if transport.ErrorCode(answer.Code) != transport.ErrFieldType || !strings.Contains(answer.Message, `Field name "Address.City"`) {
		t.Fatalf("the refusal = code %#x %q, want FIELD_TYPE and E-18", answer.Code, answer.Message)
	}
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "b", Data: map[string]interface{}{"City": "Tirana"}})

	// The type row (TF-6): a value of the wrong declared type is answered the
	// same way, and the session survives it.
	createTypedOn(tc, 0, "typed", bson.A{bson.M{"name": "Age", "type": "int"}}, "")
	typed := refusalOf(t, tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: "typed", Key: "a",
		Data: map[string]interface{}{"Age": "thirty"}}))
	if transport.ErrorCode(typed.Code) != transport.ErrFieldType || !strings.Contains(typed.Message, `Field "Age" in bucket "typed"`) {
		t.Fatalf("the type refusal = code %#x %q; want FIELD_TYPE and E-1", typed.Code, typed.Message)
	}
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: "typed", Key: "b", Data: map[string]interface{}{"Age": int32(36)}})
}

// TestRestoreRefusesAnUnreachableName: restore sends each document down the
// client path, so a dumped document holding a dotted top-level name is
// refused with E-18 before it is written (spec §4.9.8, the CHANGELOG note).
func TestRestoreRefusesAnUnreachableName(t *testing.T) {
	s := newTestServer(t)
	if _, err := s.CreateDatabase("app"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	raw, err := bson.Marshal(bson.D{{Key: "_id_", Value: "legacy"}, {Key: "Address.City", Value: "Tirana"}})
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	_, done, message := restoreBucketRaw(t, s, rootContext(), RestoreStartRequest{
		Database: "app", Bucket: "people", FormatVersion: 2, StoreType: BucketTypeHash, Bytes: int64(len(raw)),
	}, raw, 1)
	if done != nil {
		message += done.Message
	}
	if !strings.Contains(message, `Field name "Address.City" in this document cannot be stored`) {
		t.Errorf("restore answered %q, want E-18", message)
	}
	if documentOf(t, s, "app", "people", "legacy") != nil {
		t.Error("restore stored the document holding an unreachable name")
	}
}

// TestClientCannotSetAdmitted: the admitted flag is read repair's own; the
// client door refuses it, with E-12, on every client opcode whose payload
// decodes into InsertRequest (spec §5.6): OpPut and OpBTreeInsert, each on a
// hash and a btree bucket. The flag is refused under every spelling the
// handler's struct decode accepts: the codec falls back to a case-insensitive
// match, so "Admitted" and "ADMITTED" set the field too.
func TestClientCannotSetAdmitted(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	defer tc.stop()
	createTestHashBucket(t, tc)
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "ordered", Type: BucketTypeBTree})
	want := `The field "admitted" is internal to hoardDB's own repair path and cannot be set by a client. Remove it from the request.`
	for _, spelling := range []string{"admitted", "Admitted", "ADMITTED"} {
		for _, row := range []struct {
			op     transport.Opcode
			bucket string
		}{
			{transport.OpPut, testBucket}, {transport.OpPut, "ordered"},
			{transport.OpBTreeInsert, testBucket}, {transport.OpBTreeInsert, "ordered"},
		} {
			key := "via-" + row.op.String() + "-" + row.bucket + "-" + spelling
			refused := tc.frame(0, row.op, bson.D{{Key: "database", Value: testDatabase}, {Key: "bucket", Value: row.bucket},
				{Key: "key", Value: key}, {Key: "data", Value: bson.D{{Key: "x.y", Value: 1}}}, {Key: spelling, Value: true}})
			if refused.Opcode != transport.OpError {
				t.Errorf("%s on %s setting %q was answered %s; want E-12", row.op, row.bucket, spelling, refused.Opcode)
			} else if answer := refusalOf(t, refused); transport.ErrorCode(answer.Code) != transport.ErrInvalidArgument || answer.Message != want {
				t.Errorf("%s on %s setting %q = code %#x %q; want INVALID_ARGUMENT and E-12", row.op, row.bucket, spelling, answer.Code, answer.Message)
			}
			if documentOf(t, tc.nodes[0].server.core, testDatabase, row.bucket, key) != nil {
				t.Errorf("%s on %s setting %q stored its document", row.op, row.bucket, spelling)
			}
		}
	}
}

// TestReadRepairOfALegacyDottedNameIsNotRefused: read repair re-puts a value
// an owner already stores (spec §5.6). A legacy document holding a dotted
// top-level name, written before E-18, must still converge: the repair's put
// carries Admitted, and the name scan skips it on every applier.
func TestReadRepairOfALegacyDottedNameIsNotRefused(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	createTestHashBucket(t, tc)
	requireEveryCopy(t, tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: "k1",
		Data: map[string]interface{}{"n": int32(1)}, WriteConcern: "all"}), "the write of k1")

	owner := ownerIndexOf(t, tc, testBucket)
	stale := (owner + 1) % len(tc.nodes)
	legacy := map[string]interface{}{"_id_": "k1", "n": int32(1), "Legacy.Name": "kept"}
	for index := range tc.nodes {
		if index == stale {
			continue
		}
		seedLegacyDocument(t, tc.nodes[index].server.core, "k1", legacy)
	}
	forceStale(t, tc.nodes[stale].server.core, testDatabase, testBucket, "k1", map[string]interface{}{"n": int32(0)})

	resp := consistentGet(t, tc.nodes[owner], testDatabase, testBucket, "k1")
	if !resp.Found || !resp.Repaired {
		t.Fatalf("the quorum read did not repair the stale copy (found=%v repaired=%v)", resp.Found, resp.Repaired)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		found, data, _, err := tc.nodes[stale].server.core.versionedFind(testDatabase, testBucket, "k1")
		if err == nil && found && docField(t, data, "Legacy.Name") == "kept" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the stale copy did not converge to the legacy document (found=%v err=%v data=%s)", found, err, bson.Raw(data))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// seedLegacyDocument overwrites key on one node with a raw store write at the
// version the node already holds: a document written before E-18 existed.
func seedLegacyDocument(t *testing.T, s *Server, key string, document map[string]interface{}) {
	t.Helper()
	_, _, version, err := s.versionedFind(testDatabase, testBucket, key)
	if err != nil {
		t.Fatalf("reading the version of %s: %v", key, err)
	}
	b, err := s.resolveBucket(testDatabase, testBucket)
	if err != nil {
		t.Fatalf("resolve bucket: %v", err)
	}
	b.rec.mu.Lock()
	defer b.rec.mu.Unlock()
	if err := b.hashStore.Put(store.ApplyInfo{}, key, document, version); err != nil {
		t.Fatalf("seeding the legacy document: %v", err)
	}
}

// BenchmarkTopLevelNameScan is the E-18 scan's cost per write (spec §9.4):
// the budget is 30 ns a key, with no allocation.
func BenchmarkTopLevelNameScan(b *testing.B) {
	for _, keys := range []int{5, 20, 100} {
		document := make(map[string]interface{}, keys)
		for index := range keys {
			document["FieldNumber"+string(rune('A'+index%26))+string(rune('a'+index/26))] = int32(index)
		}
		b.Run(fmt.Sprintf("%d_keys", keys), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if err := refuseUnreachableNames(document, "app", "bench"); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(keys), "ns/key")
		})
	}
}
