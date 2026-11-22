package server

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The unique constraint (GAP-14 build doc Steps 5 and 6). unique is not a kind
// of index: its entries are laid out like any other's, and the write path
// refuses a second document with the same key inside the apply, before the
// batch commits (RD-1, RD-2).

// wantDuplicateKeyCode is the wire code a duplicate refusal carries: the next
// free code after ErrResultTooLarge (0x0E), written here rather than read from
// transport so this test states the contract a driver would branch on.
const wantDuplicateKeyCode = 0x0F

// createUniqueBucket creates database/bucket as a hash bucket declaring the
// given indexes.
func createUniqueBucket(t *testing.T, s *Server, database, bucket string, indices ...interface{}) {
	t.Helper()
	if resp, err := s.CreateDatabase(database); err != nil || (!resp.Success && !strings.Contains(resp.Message, "already exists")) {
		t.Fatalf("CreateDatabase(%s) = %+v, %v", database, resp, err)
	}
	resp, err := s.CreateBucket(CreateBucketRequest{Database: database, Name: bucket, Type: BucketTypeHash,
		Options: map[string]interface{}{"placement": "whole", "indices": indices}})
	if err != nil || !resp.Success {
		t.Fatalf("CreateBucket(%s) with %v = %+v, %v", bucket, indices, resp, err)
	}
}

func uniqueOn(field string) map[string]interface{} {
	return map[string]interface{}{"field": field, "unique": true}
}

// insertOutcome is an insert's answer reduced to what these tests read: the
// refusal's text, empty when the write succeeded.
func insertOutcome(t *testing.T, s *Server, database, bucket, key string, doc map[string]interface{}) string {
	t.Helper()
	resp, err := s.Insert(InsertRequest{Database: database, Bucket: bucket, Key: key, Data: doc})
	switch {
	case err != nil:
		return err.Error()
	case !resp.Success:
		return resp.Message
	}
	return ""
}

func mustInsert(t *testing.T, s *Server, database, bucket, key string, doc map[string]interface{}) {
	t.Helper()
	if refusal := insertOutcome(t, s, database, bucket, key, doc); refusal != "" {
		t.Fatalf("Insert(%s/%s %s) was refused: %s", database, bucket, key, refusal)
	}
}

func documentOf(t *testing.T, s *Server, database, bucket, key string) map[string]interface{} {
	t.Helper()
	found, err := s.Find(FindRequest{Database: database, Bucket: bucket, Key: key})
	if err != nil {
		t.Fatalf("Find(%s): %v", key, err)
	}
	if !found.Found {
		return nil
	}
	var doc map[string]interface{}
	if err := bson.Unmarshal(found.Data, &doc); err != nil {
		t.Fatalf("decoding %s: %v", key, err)
	}
	return doc
}

// TestUniqueIndexRefusesSecondDocument (spec §10; guard 1): the second insert
// of one Email is refused and the first document is unchanged.
func TestUniqueIndexRefusesSecondDocument(t *testing.T) {
	s := newTestServer(t)
	createUniqueBucket(t, s, "app", "Users", uniqueOn("Email"))
	mustInsert(t, s, "app", "Users", "a1", map[string]interface{}{"Email": "dalek@example.com", "Name": "first"})
	if refusal := insertOutcome(t, s, "app", "Users", "a2", map[string]interface{}{"Email": "dalek@example.com", "Name": "second"}); refusal == "" {
		t.Fatal("a second document with the same Email was accepted by a unique index")
	}
	if doc := documentOf(t, s, "app", "Users", "a1"); doc == nil || doc["Name"] != "first" {
		t.Fatalf("the holder is %v after the refusal, want it unchanged", doc)
	}
}

// TestUniqueViolationNamesValueIndexAndHolder (spec §10, §4.6): the refusal
// names the value, the index, the field, the bucket and the document that
// holds the value, and offers three ways out.
func TestUniqueViolationNamesValueIndexAndHolder(t *testing.T) {
	s := newTestServer(t)
	createUniqueBucket(t, s, "app", "Users", uniqueOn("Email"))
	mustInsert(t, s, "app", "Users", "usr_01J", map[string]interface{}{"Email": "dalek@example.com"})
	refusal := insertOutcome(t, s, "app", "Users", "usr_02K", map[string]interface{}{"Email": "dalek@example.com"})
	for _, want := range []string{`unique index "Email_1"`, `bucket "Users"`, `Email = "dalek@example.com"`,
		`document "usr_01J"`, "change the value", "update that document", "delete it"} {
		if !strings.Contains(strings.ToLower(refusal), strings.ToLower(want)) {
			t.Errorf("the refusal does not say %q: %s", want, refusal)
		}
	}
}

// TestUniqueRefusalWritesNothing (spec §10): after the refusal the document is
// absent, no entry names it, and the bucket's count and logical size are
// unchanged — the check ran inside the apply, before the batch committed.
func TestUniqueRefusalWritesNothing(t *testing.T) {
	s := newTestServer(t)
	createUniqueBucket(t, s, "app", "Users", uniqueOn("Email"))
	mustInsert(t, s, "app", "Users", "a1", map[string]interface{}{"Email": "dalek@example.com"})
	users := servedBucket(t, s, "app", "Users")
	state := func() store.BucketState {
		snap := s.kv.NewSnapshot()
		defer snap.Discard()
		st, err := store.LoadBucketState(snap, users.slot, users.databaseID, users.id.Load())
		if err != nil {
			t.Fatalf("LoadBucketState: %v", err)
		}
		return st
	}
	before := state()
	if refusal := insertOutcome(t, s, "app", "Users", "a2", map[string]interface{}{"Email": "dalek@example.com"}); refusal == "" {
		t.Fatal("precondition: the duplicate was accepted")
	}
	if doc := documentOf(t, s, "app", "Users", "a2"); doc != nil {
		t.Errorf("the refused document is present: %v", doc)
	}
	for _, entry := range indexEntries(t, s, users, 0) {
		if bytes.HasSuffix(entry, []byte("a2")) {
			t.Errorf("an index entry names the refused document: %x", entry)
		}
	}
	if after := state(); after.Entries != before.Entries || after.LogicalBytes != before.LogicalBytes {
		t.Errorf("the refusal moved the state: entries %d → %d, logical bytes %d → %d",
			before.Entries, after.Entries, before.LogicalBytes, after.LogicalBytes)
	}
}

// TestUniqueAllowsUpdateOfTheSameDocument (spec §10): the holder rewritten
// with its own value, and updated in another field, is not its own duplicate.
func TestUniqueAllowsUpdateOfTheSameDocument(t *testing.T) {
	s := newTestServer(t)
	createUniqueBucket(t, s, "app", "Users", uniqueOn("Email"))
	mustInsert(t, s, "app", "Users", "a1", map[string]interface{}{"Email": "dalek@example.com", "Visits": 1})
	mustInsert(t, s, "app", "Users", "a1", map[string]interface{}{"Email": "dalek@example.com", "Visits": 2})
	updated, err := s.Update(UpdateRequest{Database: "app", Bucket: "Users", Key: "a1", Set: map[string]interface{}{"Visits": 3}})
	if err != nil || !updated.Success {
		t.Fatalf("updating another field of the holder = %+v, %v", updated, err)
	}
}

// TestUniqueOnIntegersBeyondFloat53 (spec §10): the index key is the
// mathematical value, so 2^53 and 2^53+1 are two values, and find agrees: it
// compares integers as integers (it once compared every number as
// a float64 and saw one value, the spec §12 residual this test pinned).
func TestUniqueOnIntegersBeyondFloat53(t *testing.T) {
	s := newTestServer(t)
	createUniqueBucket(t, s, "app", "Ledger", uniqueOn("N"))
	const big = int64(1) << 53
	mustInsert(t, s, "app", "Ledger", "low", map[string]interface{}{"N": big})
	mustInsert(t, s, "app", "Ledger", "high", map[string]interface{}{"N": big + 1})
	for _, value := range []int64{big, big + 1} {
		scanned, err := s.Scan(ScanRequest{Database: "app", Bucket: "Ledger", Filter: map[string]interface{}{"N": value}})
		if err != nil || scanned.Total != 1 {
			t.Errorf("find({N: %d}) = %+v, %v; want exactly the one document holding it", value, scanned, err)
		}
		searched, err := s.Search(SearchRequest{Database: "app", Bucket: "Ledger", Filter: map[string]interface{}{"N": value}})
		if err != nil || searched.Total != 1 {
			t.Errorf("search({N: %d}) = %+v, %v; want exactly the one document holding it", value, searched, err)
		}
	}
}

// TestNumericSpellingsAreOneUniqueValue (the unique half of spec §10's
// TestNumericKeyIsTheMathematicalValue and TestNegativeZeroAndZeroAreOneKey):
// 30 is one value however it is spelled, and so is zero.
func TestNumericSpellingsAreOneUniqueValue(t *testing.T) {
	s := newTestServer(t)
	createUniqueBucket(t, s, "app", "Ages", uniqueOn("Age"))
	mustInsert(t, s, "app", "Ages", "thirty", map[string]interface{}{"Age": int32(30)})
	for name, spelling := range map[string]interface{}{"int64": int64(30), "uint64": uint64(30), "float32": float32(30), "float64": 30.0} {
		if refusal := insertOutcome(t, s, "app", "Ages", "as-"+name, map[string]interface{}{"Age": spelling}); refusal == "" {
			t.Errorf("30 as %s was accepted beside int32(30)", name)
		}
	}
	mustInsert(t, s, "app", "Ages", "zero", map[string]interface{}{"Age": 0.0})
	for name, spelling := range map[string]interface{}{"-0.0": math.Copysign(0, -1), "int64(0)": int64(0)} {
		if refusal := insertOutcome(t, s, "app", "Ages", "as-"+name, map[string]interface{}{"Age": spelling}); refusal == "" {
			t.Errorf("%s was accepted beside 0.0", name)
		}
	}
}

// TestUniqueAllowsOneDocumentMissingTheField (spec §5.5): a missing field is
// null, and null is a value like any other — one document may lack it, the
// second is refused naming Email = null.
func TestUniqueAllowsOneDocumentMissingTheField(t *testing.T) {
	s := newTestServer(t)
	createUniqueBucket(t, s, "app", "Users", uniqueOn("Email"))
	mustInsert(t, s, "app", "Users", "anonymous", map[string]interface{}{"Name": "no email"})
	refusal := insertOutcome(t, s, "app", "Users", "also-anonymous", map[string]interface{}{"Name": "none either"})
	if refusal == "" || !strings.Contains(refusal, "Email = null") {
		t.Fatalf("a second document without Email answered %q, want a refusal naming Email = null", refusal)
	}
}

// TestUniqueIsScopedToOneBucket (spec §10's TestUniqueIsScopedToOneBucketNotASubtree,
// recast: no subtree exists): two buckets, and two buckets of one name in two
// databases, each take one document with the same value.
func TestUniqueIsScopedToOneBucket(t *testing.T) {
	s := newTestServer(t)
	for _, place := range [][2]string{{"app", "Users"}, {"app", "Admins"}, {"other", "Users"}} {
		createUniqueBucket(t, s, place[0], place[1], uniqueOn("Email"))
	}
	// Each document has its own key: a check that saw another bucket's entry
	// would find a holder that is not this document, and refuse.
	for _, place := range [][2]string{{"app", "Users"}, {"app", "Admins"}, {"other", "Users"}} {
		mustInsert(t, s, place[0], place[1], place[0]+"-"+place[1], map[string]interface{}{"Email": "shared@example.com"})
	}
}

// TestUniqueEntriesLiveInTheBucketRange (spec §10's
// TestUniqueEntriesLiveInTheBucketInstance, recast; a pin): after a restart
// the constraint still holds, every entry lies inside the bucket's own key
// range, and the bucket opened no store of its own.
func TestUniqueEntriesLiveInTheBucketRange(t *testing.T) {
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	createUniqueBucket(t, first.core, "app", "Users", uniqueOn("Email"))
	mustInsert(t, first.core, "app", "Users", "a1", map[string]interface{}{"Email": "dalek@example.com"})
	second := restartCatalog(t, first, dataDir)
	s := second.core
	if refusal := insertOutcome(t, s, "app", "Users", "a2", map[string]interface{}{"Email": "dalek@example.com"}); refusal == "" {
		t.Fatal("after a restart the unique index accepted a duplicate")
	}
	users := servedBucket(t, s, "app", "Users")
	lower := storage.Scope(users.slot, users.databaseID, users.id.Load())
	upper := storage.Scope(users.slot, users.databaseID, users.id.Load()+1)
	entries := indexEntries(t, s, users, 0)
	if len(entries) != 1 {
		t.Fatalf("%d entries, want the holder's one", len(entries))
	}
	for _, entry := range entries {
		if bytes.Compare(entry, lower) < 0 || bytes.Compare(entry, upper) >= 0 {
			t.Errorf("entry %x lies outside the bucket's range [%x, %x)", entry, lower, upper)
		}
	}
	for _, dir := range []string{filepath.Join(dataDir, "app"), filepath.Join(dataDir, "app", "Users")} {
		if _, err := os.Stat(dir); err == nil {
			t.Errorf("the bucket has a directory of its own at %s", dir)
		}
	}
}

// TestDuplicateKeyHasItsOwnWireCode (§4 F-2(a)): without its own code the
// client gets the right message under INTERNAL and cannot branch on it — a
// driver's "on duplicate, update instead" is impossible.
func TestDuplicateKeyHasItsOwnWireCode(t *testing.T) {
	s := newTestServer(t)
	createUniqueBucket(t, s, "app", "Users", uniqueOn("Email"))
	mustInsert(t, s, "app", "Users", "a1", map[string]interface{}{"Email": "dalek@example.com"})
	payload, err := bson.Marshal(InsertRequest{Database: "app", Bucket: "Users", Key: "a2", Data: map[string]interface{}{"Email": "dalek@example.com"}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	frame, err := s.dispatch(context.Background(), transport.NewFrameWithID(transport.OpPut, [16]byte{7}, payload))
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if frame.Opcode != transport.OpError {
		t.Fatalf("the duplicate answered opcode %s (%s), want an error frame", frame.Opcode, string(frame.Payload))
	}
	var answer ErrorResponse
	if err := bson.Unmarshal(frame.Payload, &answer); err != nil {
		t.Fatalf("decoding the error frame: %v", err)
	}
	if answer.Code != wantDuplicateKeyCode {
		t.Errorf("the duplicate's wire code is %d (%s), want %d", answer.Code, transport.ErrorCode(answer.Code), wantDuplicateKeyCode)
	}
	if !strings.HasPrefix(answer.Message, "Duplicate value for unique index") {
		t.Errorf("the error frame's message is %q, want the refusal's own text", answer.Message)
	}
}

// TestUniqueRefusalIsNotOwed (§4 F-2(c)): a refused duplicate is the
// operation's answer, not the store failing. Wrapped as a store failure it
// would be owed, pin the whole node's WAL retention, and replay on every start.
func TestUniqueRefusalIsNotOwed(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "Users", Type: BucketTypeHash,
		Options: map[string]interface{}{"placement": "whole", "indices": []interface{}{uniqueOn("Email")}}})
	tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: "Users", Key: "a1",
		Data: map[string]interface{}{"Email": "dalek@example.com"}})
	refused := tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: "Users", Key: "a2",
		Data: map[string]interface{}{"Email": "dalek@example.com"}})
	if refused.Opcode != transport.OpError {
		var answer InsertResponse
		_ = bson.Unmarshal(refused.Payload, &answer)
		if answer.Success {
			t.Fatal("precondition: the duplicate was accepted")
		}
	}
	for i, node := range tc.nodes {
		s := node.server.core
		if owed := s.owed.count(); owed != 0 {
			t.Errorf("node %d owes %d sequences after a refused duplicate; the refusal was recorded as a store failure", i+1, owed)
		}
		db := s.serverDataAccessor().getDB(testDatabase)
		if db == nil || db.getBucket("Users") == nil {
			continue
		}
		users := db.getBucket("Users")
		snap := s.kv.NewSnapshot()
		st, err := store.LoadBucketState(snap, users.slot, storage.LegacyDatabaseID(testDatabase), users.id.Load())
		snap.Discard()
		if err != nil {
			t.Fatalf("node %d: LoadBucketState: %v", i+1, err)
		}
		if len(st.FailedSeqs) != 0 {
			t.Errorf("node %d records failed sequences %v for a refused duplicate", i+1, st.FailedSeqs)
		}
	}
}

// batchOutcome is a batch insert's refusal text, empty when it succeeded.
func batchOutcome(t *testing.T, s *Server, database, bucket string, docs ...map[string]interface{}) string {
	t.Helper()
	resp, err := s.BatchInsert(BatchInsertRequest{Database: database, Bucket: bucket, Data: docs})
	switch {
	case err != nil:
		return err.Error()
	case !resp.Success:
		return resp.Message
	}
	return ""
}

// TestBatchInsertWithDuplicateWritesNothing (spec §10, §4.2.1): a batch whose
// fourth document duplicates a stored value writes none of its documents, and
// the refusal says which document it was and that nothing from the batch was
// written — a user told less re-sends the same frame forever (§4 F-2(d)).
func TestBatchInsertWithDuplicateWritesNothing(t *testing.T) {
	s := newTestServer(t)
	createUniqueBucket(t, s, "app", "Users", uniqueOn("Email"))
	mustInsert(t, s, "app", "Users", "held", map[string]interface{}{"Email": "taken@example.com"})
	users := servedBucket(t, s, "app", "Users")
	before := bucketStateOf(t, s, "app", "Users")
	refusal := batchOutcome(t, s, "app", "Users",
		map[string]interface{}{"_id_": "b1", "Email": "b1@example.com"},
		map[string]interface{}{"_id_": "b2", "Email": "b2@example.com"},
		map[string]interface{}{"_id_": "b3", "Email": "b3@example.com"},
		map[string]interface{}{"_id_": "b4", "Email": "taken@example.com"},
		map[string]interface{}{"_id_": "b5", "Email": "b5@example.com"},
	)
	if refusal == "" {
		t.Fatal("a batch carrying a duplicate was accepted")
	}
	for _, want := range []string{`"b4"`, "document 4 of the batch", `Email = "taken@example.com"`, `"held"`, "Email_1", "nothing from the batch was written"} {
		if !strings.Contains(refusal, want) {
			t.Errorf("the batch refusal does not say %q: %s", want, refusal)
		}
	}
	for _, key := range []string{"b1", "b2", "b3", "b4", "b5"} {
		if doc := documentOf(t, s, "app", "Users", key); doc != nil {
			t.Errorf("document %s of the refused batch is present", key)
		}
	}
	if entries := indexEntries(t, s, users, 0); len(entries) != 1 {
		t.Errorf("%d index entries after the refused batch, want the holder's one", len(entries))
	}
	if after := bucketStateOf(t, s, "app", "Users"); after.Entries != before.Entries || after.LogicalBytes != before.LogicalBytes {
		t.Errorf("the refused batch moved the state: %+v → %+v", before, after)
	}
}

// TestBatchInsertDuplicateWithinBatchIsRefused (spec §10; §4 F-5): Apply's
// snapshot does not see the batch's own writes, so two documents of one batch
// carrying one new value both pass a snapshot check. The batch is refused,
// naming both positions.
func TestBatchInsertDuplicateWithinBatchIsRefused(t *testing.T) {
	s := newTestServer(t)
	createUniqueBucket(t, s, "app", "Users", uniqueOn("Email"))
	refusal := batchOutcome(t, s, "app", "Users",
		map[string]interface{}{"_id_": "first", "Email": "new@example.com"},
		map[string]interface{}{"_id_": "other", "Email": "other@example.com"},
		map[string]interface{}{"_id_": "second", "Email": "new@example.com"},
	)
	if refusal == "" {
		t.Fatal("two documents of one batch took one unique value")
	}
	for _, want := range []string{"document 3 of the batch", "document 1 of the same batch", `"first"`, `"second"`} {
		if !strings.Contains(refusal, want) {
			t.Errorf("the refusal does not say %q: %s", want, refusal)
		}
	}
	for _, key := range []string{"first", "other", "second"} {
		if doc := documentOf(t, s, "app", "Users", key); doc != nil {
			t.Errorf("document %s of the refused batch is present", key)
		}
	}
}

// TestBatchReleasesAValueMovedOffEarlierInTheFrame: in one multi-update,
// document "a" moves off N=1 and then document "b" takes it. The snapshot
// still shows "a" holding 1; the batch released it, so "b" may take it.
func TestBatchReleasesAValueMovedOffEarlierInTheFrame(t *testing.T) {
	s := newTestServer(t)
	createUniqueBucket(t, s, "app", "Ranks", uniqueOn("N"))
	mustInsert(t, s, "app", "Ranks", "a", map[string]interface{}{"N": int64(1), "Team": "x"})
	mustInsert(t, s, "app", "Ranks", "b", map[string]interface{}{"N": int64(0), "Team": "x"})
	updated, err := s.Update(UpdateRequest{Database: "app", Bucket: "Ranks", Multi: true,
		Filter: map[string]interface{}{"Team": "x"}, Inc: map[string]int64{"N": 1}})
	if err != nil || !updated.Success {
		t.Fatalf("a multi-update that moves a off 1 and b onto it = %+v, %v; want it accepted", updated, err)
	}
	for key, want := range map[string]int64{"a": 2, "b": 1} {
		if doc := documentOf(t, s, "app", "Ranks", key); doc == nil || fmt.Sprint(doc["N"]) != fmt.Sprint(want) {
			t.Errorf("document %s is %v after the update, want N = %d", key, doc, want)
		}
	}
}

// TestUpdateOperatorCannotCreateADuplicate: an update operator reaches the
// same check as an insert — $set onto a held value is refused.
func TestUpdateOperatorCannotCreateADuplicate(t *testing.T) {
	s := newTestServer(t)
	createUniqueBucket(t, s, "app", "Users", uniqueOn("Email"))
	mustInsert(t, s, "app", "Users", "a1", map[string]interface{}{"Email": "taken@example.com"})
	mustInsert(t, s, "app", "Users", "a2", map[string]interface{}{"Email": "free@example.com"})
	updated, err := s.Update(UpdateRequest{Database: "app", Bucket: "Users", Key: "a2", Set: map[string]interface{}{"Email": "taken@example.com"}})
	if err == nil && updated.Success {
		t.Fatal("$set onto a value a unique index already holds was accepted")
	}
	if doc := documentOf(t, s, "app", "Users", "a2"); doc["Email"] != "free@example.com" {
		t.Errorf("a2 is %v after the refused update, want it unchanged", doc)
	}
}

// TestConcurrentUniqueInsertsProduceExactlyOneWinner (spec §10; guard 1 under
// concurrency): 32 writers across insert, batch insert, update and push race
// for one value; exactly one succeeds.
func TestConcurrentUniqueInsertsProduceExactlyOneWinner(t *testing.T) {
	s := newTestServer(t)
	createUniqueBucket(t, s, "app", "Users", uniqueOn("Email"))
	for i := 0; i < 8; i++ {
		mustInsert(t, s, "app", "Users", fmt.Sprintf("existing-%d", i), map[string]interface{}{"Email": fmt.Sprintf("existing-%d@example.com", i)})
	}
	const contested = "contested@example.com"
	writers := []func(i int) bool{
		func(i int) bool {
			resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "Users", Key: fmt.Sprintf("ins-%d", i), Data: map[string]interface{}{"Email": contested}})
			return err == nil && resp.Success
		},
		func(i int) bool {
			resp, err := s.BatchInsert(BatchInsertRequest{Database: "app", Bucket: "Users", Data: []map[string]interface{}{
				{"_id_": fmt.Sprintf("batch-%d", i), "Email": contested}}})
			return err == nil && resp.Success
		},
		func(i int) bool {
			resp, err := s.Update(UpdateRequest{Database: "app", Bucket: "Users", Key: fmt.Sprintf("existing-%d", i/4), Set: map[string]interface{}{"Email": contested}})
			return err == nil && resp.Success
		},
		func(i int) bool {
			resp, err := s.Push(PushRequest{Database: "app", Bucket: "Users", Data: map[string]interface{}{"_id_": fmt.Sprintf("push-%d", i), "Email": contested}})
			return err == nil && resp.Success
		},
	}
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		winners int
		start   = make(chan struct{})
	)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			if writers[i%len(writers)](i) {
				mu.Lock()
				winners++
				mu.Unlock()
			}
		}(i)
	}
	close(start)
	wg.Wait()
	if winners != 1 {
		t.Fatalf("%d of 32 writers took one unique value, want exactly 1", winners)
	}
	hits, err := s.Search(SearchRequest{Database: "app", Bucket: "Users", Filter: map[string]interface{}{"Email": contested}})
	if err != nil || hits.Total != 1 {
		t.Fatalf("search for the contested value = %+v, %v; want its one holder", hits, err)
	}
}
