package cli

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Step 0 of the ingest-build record: characterisation tests. Each pins a
// fact the ingest design rests on, so a server change that breaks one fails a
// test with a name instead of silently changing what ingest does. They pass on
// today's tree; each comment names the spec section it defends.

// TestCharNonStringIDIsReplacedByRandomKey defends ingest-spec.md §2.2 and
// §5.3: a non-string or absent `_id_` is invisible to the server, which mints
// a random key and overwrites the stored `_id_` with it. This is why the
// ingest renderer must produce a string key before anything is sent. An empty
// string is no longer in this class: the server refuses it (BUG-0153), which
// TestCharEmptyStringIDIsRefused pins.
func TestCharNonStringIDIsReplacedByRandomKey(t *testing.T) {
	srv := newIngestTestServer(t, nil)
	srv.makeBucket(t, "app", "users", server.BucketTypeHash)

	oid := primitive.ObjectID{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9}
	batch := []map[string]interface{}{
		{"_id_": int32(7), "n": "int"},
		{"_id_": oid, "n": "oid"},
	}
	resp, frame := srv.batchPut(t, "app", "users", batch)
	if frame.Opcode == transport.OpError || !resp.Success {
		t.Fatalf("first batch: %v %+v", frame.Opcode, resp)
	}
	if got := srv.entries(t, "app", "users"); got != 2 {
		t.Fatalf("entries after first batch = %d, want 2", got)
	}

	for _, stored := range srv.scanAll(t, "app", "users") {
		idValue, err := stored.LookupErr("_id_")
		if err != nil {
			t.Fatalf("stored document has no _id_: %v", err)
		}
		key, ok := idValue.StringValueOK()
		if !ok || key == "" || key == "7" {
			t.Errorf("stored _id_ = %v, want a minted random string key", idValue)
		}
	}

	// A re-send adds fresh random keys, so the count grows: this is the
	// non-idempotence a dropped renderer would reintroduce.
	srv.batchPut(t, "app", "users", batch)
	if got := srv.entries(t, "app", "users"); got != 4 {
		t.Fatalf("entries after re-send = %d, want 4 (two fresh random keys)", got)
	}
}

// TestCharEmptyStringIDIsRefused is BUG-0153: an _id_ present as an empty
// string is refused with INVALID_ARGUMENT rather than replaced by a generated
// key, and nothing is stored.
func TestCharEmptyStringIDIsRefused(t *testing.T) {
	srv := newIngestTestServer(t, nil)
	srv.makeBucket(t, "app", "users", server.BucketTypeHash)

	_, refused := srv.batchPut(t, "app", "users", []map[string]interface{}{{"_id_": "", "n": "empty"}})
	var refusal server.ErrorResponse
	if refused.Opcode != transport.OpError || bson.Unmarshal(refused.Payload, &refusal) != nil ||
		transport.ErrorCode(refusal.Code) != transport.ErrInvalidArgument {
		t.Fatalf("an empty _id_ was answered %v %+v, want INVALID_ARGUMENT", refused.Opcode, refusal)
	}
	if !strings.Contains(refusal.Message, "non-empty") {
		t.Errorf("refusal %q does not say an _id_ must be a non-empty string", refusal.Message)
	}
	if got := srv.entries(t, "app", "users"); got != 0 {
		t.Errorf("the refused batch stored %d documents, want 0", got)
	}
}

// TestCharLeafValuesSurviveAndOrderDoesNot defends ingest-spec.md §2.4: every
// leaf comes back equal in value and BSON type; field order does not survive.
func TestCharLeafValuesSurviveAndOrderDoesNot(t *testing.T) {
	srv := newIngestTestServer(t, nil)
	for _, engine := range []server.BucketType{server.BucketTypeHash, server.BucketTypeBTree} {
		t.Run(string(engine), func(t *testing.T) {
			srv.makeBucket(t, "app", "wide_"+string(engine), engine)
			want, sent := nineteenFieldDocument("wide")
			resp, frame := srv.batchPut(t, "app", "wide_"+string(engine), []map[string]interface{}{sent})
			if frame.Opcode == transport.OpError || !resp.Success {
				t.Fatalf("batch: %v %+v", frame.Opcode, resp)
			}
			stored, found := srv.getDocument(t, "app", "wide_"+string(engine), "wide")
			if !found {
				t.Fatal("document not found by its key")
			}
			wantRaw := bsonDocumentBytes(t, want)
			requireSameValue(t, "doc", bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: wantRaw}, bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: stored})

			wantKeys := documentKeys(t, wantRaw)
			gotKeys := documentKeys(t, stored)
			if strings.Join(wantKeys, ",") == strings.Join(gotKeys, ",") {
				t.Errorf("field order survived (%v); ingest-spec.md §2.4 says it does not — if the write path now preserves order, the spec and user page must change", gotKeys)
			}
		})
	}
}

func documentKeys(t *testing.T, doc bson.Raw) []string {
	t.Helper()
	elements, err := doc.Elements()
	if err != nil {
		t.Fatalf("reading document: %v", err)
	}
	keys := make([]string, 0, len(elements))
	for _, element := range elements {
		keys = append(keys, element.Key())
	}
	return keys
}

// TestCharDollarAndDottedNamesStoredIntact defends ingest-spec.md §2.8: field
// names starting with `$` are stored intact, a dotted name inside a
// sub-document is stored intact, and a sub-document that is exactly
// {"$$now": true} is stored as written (BUG-0151). A dotted top-level name is refused
// with FIELD_TYPE since typed-fields spec §4.9.8 (E-18): no path can reach it.
func TestCharDollarAndDottedNamesStoredIntact(t *testing.T) {
	srv := newIngestTestServer(t, nil)
	for _, engine := range []server.BucketType{server.BucketTypeHash, server.BucketTypeBTree} {
		name := "names_" + string(engine)
		srv.makeBucket(t, "app", name, engine)
		_, refused := srv.batchPut(t, "app", name, []map[string]interface{}{{"_id_": "dotted", "a.b": int32(2)}})
		var refusal server.ErrorResponse
		if refused.Opcode != transport.OpError || bson.Unmarshal(refused.Payload, &refusal) != nil ||
			transport.ErrorCode(refusal.Code) != transport.ErrFieldType {
			t.Errorf("%s: a top-level dotted name was answered %v %+v, want FIELD_TYPE", engine, refused.Opcode, refusal)
		}
		batch := []map[string]interface{}{{
			"_id_": "k",
			"$set": int32(1),
			"c":    map[string]interface{}{"$d": int32(3), "e.f": int32(4)},
			"n":    map[string]interface{}{"$$now": true},
		}}
		resp, frame := srv.batchPut(t, "app", name, batch)
		if frame.Opcode == transport.OpError || !resp.Success {
			t.Fatalf("%s batch: %v %+v", engine, frame.Opcode, resp)
		}
		stored, found := srv.getDocument(t, "app", name, "k")
		if !found {
			t.Fatalf("%s: document missing", engine)
		}
		for _, path := range [][]string{{"$set"}} {
			if _, err := stored.LookupErr(path...); err != nil {
				t.Errorf("%s: field %v not stored intact: %v", engine, path, err)
			}
		}
		for _, path := range [][]string{{"c", "$d"}, {"c", "e.f"}} {
			if _, err := stored.LookupErr(path...); err != nil {
				t.Errorf("%s: nested field %v not stored intact: %v", engine, path, err)
			}
		}
		// BUG-0151: the old now() sentinel's shape is a user's value, stored
		// as written on every write path.
		nowValue, err := stored.LookupErr("n")
		if err != nil {
			t.Fatalf("%s: n missing: %v", engine, err)
		}
		if nowValue.Type != bson.TypeEmbeddedDocument {
			t.Errorf("%s: {\"$$now\":true} stored as %s; want the sub-document as written", engine, nowValue.Type)
		}
	}
}

// TestCharOversizedBatchRefusedAtomicallyInBandOnAllFiveEngines defends
// ingest-spec.md §2.3 and §7.1: a batch above the store's atomic bound is
// refused whole, in-band (Success false, no transport error), with the
// sentinel's text, and a smaller batch commits.
func TestCharOversizedBatchRefusedAtomicallyInBandOnAllFiveEngines(t *testing.T) {
	// 60 x 100 KiB is over the 5 MB bound of the 32 MiB memtable floor, and
	// under the 20 MB bound of the 128 MiB default.
	srv := newIngestTestServer(t, func(cfg *config.Config) { cfg.Storage.MemtableMB = config.MinMemtableMB })
	payload := strings.Repeat("x", 100*1024)
	makeBatch := func(count int) []map[string]interface{} {
		docs := make([]map[string]interface{}, count)
		for i := range docs {
			docs[i] = map[string]interface{}{"_id_": fmt.Sprintf("k%03d", i), "body": payload}
		}
		return docs
	}
	engines := []server.BucketType{
		server.BucketTypeHash, server.BucketTypeBTree, server.BucketTypeFIFO, server.BucketTypeLIFO, server.BucketTypeHeap,
	}
	for _, engine := range engines {
		t.Run(string(engine), func(t *testing.T) {
			bucket := "big_" + string(engine)
			srv.makeBucket(t, "app", bucket, engine)

			resp, frame := srv.batchPut(t, "app", bucket, makeBatch(60))
			if frame.Opcode != transport.OpError {
				t.Fatalf("60 x 100 KiB was answered %s (%+v); the store's atomic bound is about 5 MB", frame.Opcode, resp)
			}
			if code := errorFrameCode(t, frame); code != transport.ErrBatchTooLarge {
				t.Fatalf("an oversized batch was answered code %s (%s), want BATCH_TOO_LARGE", code, errorFrameText(t, frame))
			}
			if got := srv.entries(t, "app", bucket); got != 0 {
				t.Fatalf("entries after the refused batch = %d, want 0 (atomic)", got)
			}

			resp, frame = srv.batchPut(t, "app", bucket, makeBatch(38))
			if frame.Opcode == transport.OpError || !resp.Success {
				t.Fatalf("38 x 100 KiB was refused: %v %+v", frame.Opcode, resp)
			}
			if got := srv.entries(t, "app", bucket); got != 38 {
				t.Fatalf("entries after the 38-document batch = %d, want 38", got)
			}
		})
	}
}

// TestCharQueueBatchReplyCarriesCountAndIDs defends ingest-spec.md §2.3 and §8.5
// (BUG-0516): a queue batch reply says Count N and the N ids in batch order, and
// a keyed reply reports the sent count even when a key repeats inside the batch.
// Ingest still reads only Success and Message and counts queue applications from
// the batch length; a queue reply that regressed to Count 0 would make the
// reported count untruthful for every other client.
func TestCharQueueBatchReplyCarriesCountAndIDs(t *testing.T) {
	srv := newIngestTestServer(t, nil)
	batch := func() []map[string]interface{} {
		return []map[string]interface{}{
			{"_id_": "k", "n": int32(1)}, {"_id_": "k", "n": int32(2)}, {"_id_": "j", "n": int32(3)},
		}
	}
	for _, engine := range []server.BucketType{server.BucketTypeFIFO, server.BucketTypeLIFO} {
		bucket := "q_" + string(engine)
		srv.makeBucket(t, "app", bucket, engine)
		resp, frame := srv.batchPut(t, "app", bucket, batch())
		if frame.Opcode == transport.OpError || !resp.Success {
			t.Fatalf("%s: %v %+v", engine, frame.Opcode, resp)
		}
		if want := []string{"k", "k", "j"}; resp.Count != 3 || !slices.Equal(resp.IDs, want) {
			t.Errorf("%s reply = Count %d, ids %v; want Count 3 and ids %v in batch order", engine, resp.Count, resp.IDs, want)
		}
		if got := srv.entries(t, "app", bucket); got != 3 {
			t.Errorf("%s stored %d, want 3", engine, got)
		}
	}
	for _, engine := range []server.BucketType{server.BucketTypeHash, server.BucketTypeBTree, server.BucketTypeHeap} {
		bucket := "k_" + string(engine)
		srv.makeBucket(t, "app", bucket, engine)
		// A repeated _id_ in a BATCH_PUT into a hash or btree bucket is refused;
		// the last-wins this test defends is BATCH_UPSERT's. A heap is keyed
		// but takes BATCH_PUT, and keeps the last.
		send := srv.batchPut
		if engine != server.BucketTypeHeap {
			send = srv.batchUpsert
		}
		resp, frame := send(t, "app", bucket, batch())
		if frame.Opcode == transport.OpError || !resp.Success {
			t.Fatalf("%s: %v %+v", engine, frame.Opcode, resp)
		}
		if int(resp.Count) != 3 {
			t.Errorf("%s reply Count = %d, want the sent 3 even though a key repeats", engine, resp.Count)
		}
		if got := srv.entries(t, "app", bucket); got != 2 {
			t.Errorf("%s stored %d, want 2 (k overwritten within the batch)", engine, got)
		}
	}
}

// TestCharHeapWithIDIsKeyedAndFifoAppends defends ingest-spec.md §5.2: a heap
// document with `_id_` is keyed (a re-run overwrites), a fifo push appends.
func TestCharHeapWithIDIsKeyedAndFifoAppends(t *testing.T) {
	srv := newIngestTestServer(t, nil)
	srv.makeBucket(t, "app", "jobs", server.BucketTypeHeap)
	srv.makeBucket(t, "app", "feed", server.BucketTypeFIFO)
	batch := func() []map[string]interface{} {
		return []map[string]interface{}{{"_id_": "a"}, {"_id_": "a"}, {"_id_": "b"}}
	}
	for round := 0; round < 2; round++ {
		for _, bucket := range []string{"jobs", "feed"} {
			if resp, frame := srv.batchPut(t, "app", bucket, batch()); frame.Opcode == transport.OpError || !resp.Success {
				t.Fatalf("%s round %d: %v %+v", bucket, round, frame.Opcode, resp)
			}
		}
	}
	if got := srv.entries(t, "app", "jobs"); got != 2 {
		t.Errorf("heap holds %d, want 2 (keyed by _id_)", got)
	}
	if got := srv.entries(t, "app", "feed"); got != 6 {
		t.Errorf("fifo holds %d, want 6 (append)", got)
	}
}

// TestCharTTLDeletesHistoricalDocumentButKeepsStringDate defends
// ingest-spec.md §5.5: a document whose date is older than the expiry is swept
// even though the load reported success, and an ISO string date never expires.
func TestCharTTLDeletesHistoricalDocumentButKeepsStringDate(t *testing.T) {
	srv := newIngestTestServer(t, func(cfg *config.Config) { cfg.Server.TTLSweepSeconds = 1 })
	srv.createDatabase(t, "app")
	srv.createBucket(t, "app", "events", server.BucketTypeHash, map[string]interface{}{
		"indices": []interface{}{map[string]interface{}{"fields": []interface{}{"at"}, "expire_after_seconds": int64(3600)}},
	})
	started := time.Now()
	resp, frame := srv.batchPut(t, "app", "events", []map[string]interface{}{
		{"_id_": "old", "at": primitive.NewDateTimeFromTime(time.Unix(0, 0))},
		{"_id_": "text", "at": "1970-01-01T00:00:00Z"},
	})
	if frame.Opcode == transport.OpError || !resp.Success || resp.Count != 2 {
		t.Fatalf("load: %v %+v", frame.Opcode, resp)
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, found := srv.getDocument(t, "app", "events", "old"); !found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the 1970 document was not swept within 15 s at a 1 s sweep")
		}
		time.Sleep(200 * time.Millisecond)
	}
	if wait := 12*time.Second - time.Since(started); wait > 0 {
		time.Sleep(wait)
	}
	if _, found := srv.getDocument(t, "app", "events", "text"); !found {
		t.Error("the document with an ISO string date was deleted; a string never expires (ingest-spec.md §5.5)")
	}
	stats, statsFrame := srv.statsOf(t, "app", "events")
	if statsFrame.Opcode == transport.OpError {
		t.Fatalf("stats: %s", errorFrameText(t, statsFrame))
	}
	// Q17: TTL detection parses this display string, so this test fails if the
	// format changes.
	if len(stats.Indexes) != 1 || !strings.Contains(stats.Indexes[0], "(ttl 3600s)") {
		t.Errorf("Indexes = %q, want one entry containing \"(ttl 3600s)\"", stats.Indexes)
	}
}

// TestCharExtJSONDropsTrailingContent defends ingest-spec.md §4.3: the
// Extended JSON decoder reports success after the first value, so the reader
// must validate one value per line itself.
func TestCharExtJSONDropsTrailingContent(t *testing.T) {
	for _, line := range []string{`{"a":1}{"b":2}`, `{"a":1} trailing`, `{"a":1,"a":2}`} {
		var decoded bson.M
		if err := bson.UnmarshalExtJSON([]byte(line), false, &decoded); err != nil {
			t.Errorf("UnmarshalExtJSON(%q) = %v; the pinned behaviour is silent success — if the decoder now refuses, the strict pre-validation may be simplified", line, err)
		}
	}
}

// TestCharCreateBucketOnExistingReportsSuccess defends ingest-spec.md §2.9 and
// §5.1: creating over an existing bucket succeeds and says so in a message, a
// second create database fails, and OpBucketStats answers a missing bucket and
// a missing database with an OpError, not Found:false.
//
// Recorded observations (S-3, ingest-build.md): OpBucketStats for a missing
// bucket in an existing database is an OpError with code NOT_FOUND (1); so is
// one for a missing database now, where it was INTERNAL (5), so only
// the message told the two apart. Ingest never matches that text: on any
// refusal it asks OpListDatabases whether the database exists.
func TestCharCreateBucketOnExistingReportsSuccess(t *testing.T) {
	srv := newIngestTestServer(t, nil)
	srv.makeBucket(t, "app", "users", server.BucketTypeHash)

	var created server.CreateBucketResponse
	frame := srv.call(t, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "users", Type: server.BucketTypeHash}, &created)
	if frame.Opcode == transport.OpError || !created.Success || !strings.Contains(created.Message, "already exists (type: hash)") {
		t.Fatalf("create over existing = %v %+v, want Success with \"already exists (type: hash)\"", frame.Opcode, created)
	}
	// A create naming another type is not that redelivery: it is
	// refused, and ingest's createBucket leaves the mismatch to its read-back.
	frame = srv.call(t, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "users", Type: server.BucketTypeBTree}, &created)
	if frame.Opcode != transport.OpError {
		t.Fatalf("create over an existing bucket of another type = %v %+v, want an error", frame.Opcode, created)
	}

	var again server.CreateDatabaseResponse
	frame = srv.call(t, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, &again)
	if frame.Opcode != transport.OpError && again.Success {
		t.Errorf("create database twice succeeded (%+v); ingest-spec.md §2.9 says it fails", again)
	}

	_, missingBucket := srv.statsOf(t, "app", "ghost")
	if missingBucket.Opcode != transport.OpError {
		t.Fatalf("stats of a missing bucket answered %s, want OpError", missingBucket.Opcode)
	}
	var bucketRefusal server.ErrorResponse
	if err := bson.Unmarshal(missingBucket.Payload, &bucketRefusal); err != nil {
		t.Fatal(err)
	}
	if transport.ErrorCode(bucketRefusal.Code) != transport.ErrNotFound {
		t.Errorf("missing bucket code = %s, want NOT_FOUND", transport.ErrorCode(bucketRefusal.Code))
	}

	_, missingDatabase := srv.statsOf(t, "nowhere", "ghost")
	if missingDatabase.Opcode != transport.OpError {
		t.Fatalf("stats of a missing database answered %s, want OpError", missingDatabase.Opcode)
	}
	var databaseRefusal server.ErrorResponse
	if err := bson.Unmarshal(missingDatabase.Payload, &databaseRefusal); err != nil {
		t.Fatal(err)
	}
	t.Logf("OpBucketStats, missing bucket: code %s; missing database: code %s (%q)",
		transport.ErrorCode(bucketRefusal.Code), transport.ErrorCode(databaseRefusal.Code), databaseRefusal.Message)
	if transport.ErrorCode(databaseRefusal.Code) != transport.ErrNotFound {
		t.Errorf("missing database code = %s, want NOT_FOUND", transport.ErrorCode(databaseRefusal.Code))
	}
}

// TestCharBucketStatsCostPerEngine measures OpBucketStats on a 100k-entry
// bucket of each engine (spec Q20, build doc S-7) and logs the durations. It
// asserts only that the call answers; the numbers are the deliverable.
func TestCharBucketStatsCostPerEngine(t *testing.T) {
	if testing.Short() {
		t.Skip("loads 100k documents into five engines")
	}
	srv := newIngestTestServer(t, nil)
	engines := []server.BucketType{
		server.BucketTypeHash, server.BucketTypeBTree, server.BucketTypeFIFO, server.BucketTypeLIFO, server.BucketTypeHeap,
	}
	for _, engine := range engines {
		bucket := "stats_" + string(engine)
		srv.makeBucket(t, "app", bucket, engine)
		for start := 0; start < 100000; start += 1000 {
			docs := make([]map[string]interface{}, 1000)
			for i := range docs {
				docs[i] = map[string]interface{}{"_id_": fmt.Sprintf("k%07d", start+i), "n": int64(start + i)}
			}
			if resp, frame := srv.batchPut(t, "app", bucket, docs); frame.Opcode == transport.OpError || !resp.Success {
				t.Fatalf("%s load at %d: %v %+v", engine, start, frame.Opcode, resp)
			}
		}
		began := time.Now()
		stats, frame := srv.statsOf(t, "app", bucket)
		elapsed := time.Since(began)
		if frame.Opcode == transport.OpError || stats.Entries != 100000 {
			t.Fatalf("%s stats: %v entries=%d", engine, frame.Opcode, stats.Entries)
		}
		t.Logf("OpBucketStats on 100,000 %s entries: %s", engine, elapsed)
	}
}

// TestCharReadWriteUserMayFillButNotCreate defends ingest-spec.md §2.6 and
// §5.1: a writer@app user can fill an existing bucket but cannot create a
// bucket or a database, so ingest's create path needs db-manager.
func TestCharReadWriteUserMayFillButNotCreate(t *testing.T) {
	srv := newIngestTestServer(t, nil)
	srv.makeBucket(t, "app", "users", server.BucketTypeHash)
	srv.createReadWriteUser(t, "bob", "bob-password-1", "app")

	bob := NewCLI(WithAddress(srv.addr), WithInsecureTLS(true), WithCredentials("bob", "bob-password-1"), WithTerminal(discardWriter{}))
	if err := bob.Connect(); err != nil {
		t.Fatalf("bob connect: %v", err)
	}
	t.Cleanup(bob.Close)

	var filled server.BatchInsertResponse
	frame := callOn(t, bob, transport.OpBatchPut, &server.BatchInsertRequest{
		Database: "app", Bucket: "users", Data: []map[string]interface{}{{"_id_": "a"}},
	}, &filled)
	if frame.Opcode == transport.OpError || !filled.Success {
		t.Fatalf("writer user could not fill an existing bucket: %v %+v", frame.Opcode, filled)
	}
	for _, op := range []transport.Opcode{transport.OpBucketStats, transport.OpCount} {
		var ignored map[string]interface{}
		var request interface{} = &server.BucketStatsRequest{Database: "app", Bucket: "users"}
		if op == transport.OpCount {
			request = &server.CountRequest{Database: "app", Bucket: "users"}
		}
		if reply := callOn(t, bob, op, request, &ignored); reply.Opcode == transport.OpError {
			t.Errorf("writer user refused %s: %s", op, errorFrameText(t, reply))
		}
	}

	var createdBucket server.CreateBucketResponse
	refused := callOn(t, bob, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "fresh", Type: server.BucketTypeHash}, &createdBucket)
	if refused.Opcode != transport.OpError || !strings.Contains(errorFrameText(t, refused), "db-manager") {
		t.Errorf("create bucket by writer user = %v %q; want a refusal naming db-manager", refused.Opcode, refused.Payload)
	}
	var createdDatabase server.CreateDatabaseResponse
	refused = callOn(t, bob, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "other"}, &createdDatabase)
	if refused.Opcode != transport.OpError || !strings.Contains(errorFrameText(t, refused), "db-manager") {
		t.Errorf("create database by writer user = %v %q; want a refusal naming db-manager", refused.Opcode, refused.Payload)
	}
}

// TestCharQueuePathIgnoresIDField defends ingest-spec.md Q10, and corrects it.
// The spec recommended that fifo/lifo targets push every document verbatim,
// "confirmed by a step 0 test" it had not run. Run: a fifo push does not read
// the foreign `_id` (a double is stored verbatim), but it DOES read `_id_`
// exactly as the keyed path does — a non-string `_id_` is replaced by a minted
// id and the stored value overwritten. So the renderer refuses a non-string or
// empty `_id_` on queue targets too, and skips only `_id` (ingest-build.md
// report, S-2 finding).
func TestCharQueuePathIgnoresIDField(t *testing.T) {
	srv := newIngestTestServer(t, nil)
	srv.makeBucket(t, "app", "feed", server.BucketTypeFIFO)
	resp, frame := srv.batchPut(t, "app", "feed", []map[string]interface{}{
		{"_id": 3.5, "n": "double"},
		{"_id_": int32(7), "n": "int"},
	})
	if frame.Opcode == transport.OpError || !resp.Success {
		t.Fatalf("batch: %v %+v", frame.Opcode, resp)
	}
	first := popQueue(t, srv, "feed")
	if idValue, err := first.LookupErr("_id"); err != nil || idValue.Type != bson.TypeDouble {
		t.Errorf("first document _id = %v (%v), want the double stored verbatim", idValue, err)
	}
	second := popQueue(t, srv, "feed")
	idValue, err := second.LookupErr("_id_")
	if err != nil {
		t.Fatalf("second document has no _id_: %v", err)
	}
	if idValue.Type != bson.TypeString {
		t.Errorf("a non-string _id_ pushed to fifo is stored as %s; the pinned behaviour is a minted string that overwrites the user's value", idValue.Type)
	}
}

func popQueue(t *testing.T, srv *ingestTestServer, bucket string) bson.Raw {
	t.Helper()
	var popped server.PopResponse
	frame := srv.call(t, transport.OpFIFOPop, &server.PopRequest{Database: "app", Bucket: bucket}, &popped)
	if frame.Opcode == transport.OpError || !popped.Found {
		t.Fatalf("pop: %v %+v", frame.Opcode, popped)
	}
	return popped.Items[0]
}

// discardWriter is io.Discard under a name usable as a value in a WithTerminal
// call without importing io in every test file.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// createReadWriteUser makes a user holding writer on one database.
func (s *ingestTestServer) createReadWriteUser(t *testing.T, username, password, database string) {
	t.Helper()
	material, err := server.NewCredential(password, server.DefaultArgon2Params())
	if err != nil {
		t.Fatalf("NewCredential: %v", err)
	}
	var resp server.UserMutationResponse
	frame := s.call(t, transport.OpUserCreate, &server.UserCreateRequest{
		RequestID: "create-" + username,
		Username:  username,
		Credential: server.UserCredential{
			Algo: material.Algo, Salt: material.Salt, Hash: material.Hash, Params: material.Params,
		},
		Roles: []server.UserRole{{Role: "writer", DB: database}},
	}, &resp)
	if frame.Opcode == transport.OpError || !resp.Success {
		t.Fatalf("create user %q: %v %+v", username, frame.Opcode, resp)
	}
}
