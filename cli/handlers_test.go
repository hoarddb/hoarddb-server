package cli

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// pushThenPop creates bucket in database "app" as bucketType, pushes each
// document through the REPL's own push handler, and pops them back over the
// wire in the bucket's order.
func pushThenPop(t *testing.T, bucketType server.BucketType, documents ...string) []bson.M {
	t.Helper()
	c := newDumpTestServer(t)
	if err := c.handler.send(transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil); err != nil {
		t.Fatalf("create database: %v", err)
	}
	var created server.CreateBucketResponse
	if err := c.handler.send(transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "q", Type: bucketType}, &created); err != nil || !created.Success {
		t.Fatalf("create %s bucket: %+v, %v", bucketType, created, err)
	}
	for _, document := range documents {
		if err := c.handler.handlePush("app", "q", []string{document}); err != nil {
			t.Fatalf("push %s: %v", document, err)
		}
	}
	popped := make([]bson.M, 0, len(documents))
	for range documents {
		var resp server.PopResponse
		if err := c.handler.send(transport.OpFIFOPop, &server.PopRequest{Database: "app", Bucket: "q"}, &resp); err != nil || !resp.Found {
			t.Fatalf("pop from the %s bucket: %+v, %v", bucketType, resp, err)
		}
		var doc bson.M
		if err := bson.Unmarshal(resp.Items[0], &doc); err != nil {
			t.Fatalf("decoding a popped element: %v", err)
		}
		popped = append(popped, doc)
	}
	return popped
}

// TestHandlePushKeepsAUserPriorityFieldOnNonHeapBuckets: the push
// handler deleted a numeric "priority" field from every document it sent,
// whatever the bucket's type, so a FIFO or LIFO element lost a field its
// author chose. Only a heap consumes priority; everywhere else it is data.
func TestHandlePushKeepsAUserPriorityFieldOnNonHeapBuckets(t *testing.T) {
	for _, bucketType := range []server.BucketType{server.BucketTypeFIFO, server.BucketTypeLIFO} {
		t.Run(string(bucketType), func(t *testing.T) {
			popped := pushThenPop(t, bucketType, `{priority: 3, x: 1}`)[0]
			if _, kept := popped["priority"]; !kept {
				t.Errorf("a %s push of {priority: 3, x: 1} stored %v: the user's priority field was stripped", bucketType, popped)
			}
			if popped["x"] == nil {
				t.Errorf("a %s push of {priority: 3, x: 1} stored %v: the rest of the document is missing", bucketType, popped)
			}
		})
	}
}

// TestHandlePushStripsPriorityOnlyForHeap is the negative half, named on its
// own so its absence is visible: on a heap the priority orders the element
// and is not stored in the document, as before.
func TestHandlePushStripsPriorityOnlyForHeap(t *testing.T) {
	popped := pushThenPop(t, server.BucketTypeHeap, `{priority: 3, x: 1}`, `{priority: 7, x: 2}`)
	for _, doc := range popped {
		if _, stored := doc["priority"]; stored {
			t.Errorf("a heap element was stored with its priority in the document: %v", doc)
		}
	}
	if popped[0]["x"] != int32(2) && popped[0]["x"] != float64(2) && popped[0]["x"] != int64(2) {
		t.Errorf("the heap popped %v first; want the priority-7 element (x: 2), so the priority did not order it", popped[0])
	}
}

// TestDescribeMissingBucketIsAnError is the Found:false path of `describe
// bucket`: the handler used to print the server's message and return nil, so a
// script exited 0 on a bucket that does not exist. It is a refused statement
// (a container the statement named is absent, spec 6.1 source 5). A real server
// never sends this reply for a missing bucket (it answers an OpError), so the
// reply is stubbed; the handler is what is under test.
func TestDescribeMissingBucketIsAnError(t *testing.T) {
	var out bytes.Buffer
	_, c := newStubServer(t, map[transport.Opcode]interface{}{
		transport.OpBucketStats: server.BucketStatsResponse{Found: false, Message: `bucket "Ghost" not found in database "app"`},
	}, &out)
	c.currentDB = "app"
	out.Reset()

	err := c.handle("describe bucket Ghost")
	var notFound *NotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("describe bucket <missing> returned %v (%T), want a *NotFoundError", err, err)
	}
	if notFound.Kind != "bucket" || notFound.Message != `bucket "Ghost" not found in database "app"` {
		t.Errorf("NotFoundError = %+v, want Kind bucket and the server's message", notFound)
	}
	if info := classify(err); info.Code != "NOT_FOUND" || info.Exit != ExitRefused {
		t.Errorf("classify = %s exit %d, want NOT_FOUND exit %d", info.Code, info.Exit, ExitRefused)
	}
	if out.Len() != 0 {
		t.Errorf("describe bucket <missing> printed %q before failing; the error line is the whole output", out.String())
	}
}

// TestRefusedPushIsAnError: handlePush once printed "ERR <message>" for a push
// the server refused and returned nil, so a piped script carried on and exited
// 0. A push to a blob is now an ERROR frame (BUG-0469) and the handler returns
// it as an error, as every other write does.
func TestRefusedPushIsAnError(t *testing.T) {
	session := newFixtureSession(t)
	err := session.cli.handle(`db.Files.push({"a": 1})`)
	requireWrongTypeRefused(t, err, "push is not supported for bucket type blob")
	if got := session.out.String(); strings.Contains(got, "ERR") {
		t.Errorf("a refused push still printed %q; the error line is the whole output", got)
	}
}

// TestFindPopPeekSurfaceAServerMessage: The server answers a find,
// pop or peek that produced no document with Found:false and a message, and
// the message says whether the thing was absent or the call failed. The
// handlers printed NOT FOUND or "EMPTY  <message>" for all of it and returned
// nil, so a failing store read as a drained queue. A message that is not one
// of the server's absence literals is now an error and nothing is printed.
// The real server cannot be made to fail a read, so a stub answers.
func TestFindPopPeekSurfaceAServerMessage(t *testing.T) {
	var out bytes.Buffer
	_, c := newStubServer(t, map[transport.Opcode]interface{}{
		transport.OpGet:      server.FindResponse{Found: false, Message: "hash get failed: disk I/O error"},
		transport.OpFIFOPop:  server.PopResponse{Found: false, Message: "queue pop failed: disk I/O error"},
		transport.OpFIFOPeek: server.PeekResponse{Found: false, Message: "peek is not defined for hash buckets: a hash has no order to take from"},
		transport.OpBlobRead: server.BlobReadResponse{Found: false, Message: "blob read failed: disk I/O error"},
		// get asks for the bucket's type first to choose between a point read
		// and a blob read; BUCKET_INFO answers it without walking the stores.
		transport.OpBucketInfo: server.BucketInfoResponse{Found: true, Bucket: server.BucketInfo{Type: server.BucketTypeBlob}},
	}, &out)
	c.currentDB = "app"

	cases := []struct {
		statement string
		message   string
	}{
		{`db.Users.find("k")`, "hash get failed: disk I/O error"},
		{`db.Jobs.pop()`, "queue pop failed: disk I/O error"},
		{`db.Users.peek()`, "peek is not defined for hash buckets: a hash has no order to take from"},
		{`db.Files.get("k")`, "blob read failed: disk I/O error"},
		{`db.Files.info("k")`, "blob read failed: disk I/O error"},
	}
	for _, tc := range cases {
		out.Reset()
		requireRefused(t, c.handle(tc.statement), tc.message)
		if out.Len() != 0 {
			t.Errorf("%s printed %q before failing; the error line is the whole output", tc.statement, out.String())
		}
	}
}

// TestExtJSONAppliesToEveryDocumentPrinter: --extjson promises
// that dates keep their type, but only scan and search passed the flag to
// jsonLine; twelve other print sites called it with one argument and printed a
// date as a plain string. Each row below drives one of those sites with a
// {"$date": ...} value and the flag on. The "off" half proves the row is not
// vacuous: without the flag the same statement prints no $date. A fixture
// writes {"$date": ...}, never ISODate(...): the latter does not parse.
func TestExtJSONAppliesToEveryDocumentPrinter(t *testing.T) {
	const date = `{"$date": "2026-02-01T00:00:00Z"}`
	session := newFixtureSession(t)
	runQuiet(t, session,
		`db.Users.insert({"_id_": "d1", "at": `+date+`})`,
		`create bucket Dated { type: fifo }`,
		`db.Dated.push({"at": `+date+`})`,
		`db.Dated.push({"at": `+date+`})`,
		`db.Events.insert({"_id_": "e1", "at": `+date+`})`,
		`db.Files.put({"_id_": "f3", "data": "x", "at": `+date+`})`,
		`db.Post.insert({"_id_": "p2"})`,
		`db.Post["p2"].Comments.push({"at": `+date+`})`,
		`db.Post["p2"].Comments.push({"at": `+date+`})`,
		`db.Post["p1"].Tags.put("t9", {"at": `+date+`})`,
	)

	sites := []struct {
		site      string
		statement string
		// destructive statements change state, so they run once, flag on.
		destructive bool
	}{
		{"handleFind", `db.Users.find("d1")`, false},
		{"handlePeek", `db.Dated.peek()`, false},
		{"handleRange", `db.Events.range({$gte: "a", $lte: "z"})`, false},
		{"handleEdge", `db.Events.first()`, false},
		{"handleBlobContent", `db.Files.get("f3")`, false},
		{"handleBlobInfo", `db.Files.info("f3")`, false},
		{"fieldRead", `db.Post["p1"].Tags.get("t9")`, false},
		{"fieldList with keys", `db.Post["p1"].Tags.list()`, false},
		{"fieldList without keys", `db.Post["p2"].Comments.list()`, false},
		{"handleUpdate", `db.Users.update("d1", {$set: {"n": 2}})`, true},
		{"handlePop", `db.Dated.pop()`, true},
		{"fieldWrite pop", `db.Post["p2"].Comments.pop()`, true},
	}
	if len(sites) != 12 {
		t.Fatalf("the bug names twelve print sites, this table has %d", len(sites))
	}

	for _, tc := range sites {
		t.Run(tc.site, func(t *testing.T) {
			if !tc.destructive {
				session.cli.handler.SetExtendedJSON(false)
				plain := runStatements(t, session.cli, session.out, tc.statement)
				if strings.Contains(plain, "$date") {
					t.Fatalf("%s without --extjson printed a $date; the row proves nothing:\n%s", tc.site, plain)
				}
			}
			session.cli.handler.SetExtendedJSON(true)
			extended := runStatements(t, session.cli, session.out, tc.statement)
			if !strings.Contains(extended, `{"$date":`) {
				t.Errorf("%s with --extjson printed no Extended JSON date:\n%s", tc.site, extended)
			}
		})
	}
}

// TestConsistentGetPrintsTheDocument: consistent-get decoded the
// reply into an interface{}, which the driver returns as an ordered list of
// key/value pairs, and printed that indented across several lines; every other
// read prints the document as one JSON line. It now prints exactly what find
// prints for the same key, honours --extjson, and reports a repaired read on
// the chatter stream instead of appending to the document.
func TestConsistentGetPrintsTheDocument(t *testing.T) {
	const date = `{"$date": "2026-02-01T00:00:00Z"}`
	t.Run("a hit prints what find prints", func(t *testing.T) {
		session := newFixtureSession(t)
		runQuiet(t, session, `db.Users.insert({"_id_": "two", "name": "ada", "n": 7})`)
		found := strings.TrimPrefix(runStatements(t, session.cli, session.out, `db.Users.find("two")`), "> db.Users.find(\"two\")\n")
		consistent := strings.TrimPrefix(runStatements(t, session.cli, session.out, "consistent-get app Users two"), "> consistent-get app Users two\n")
		if consistent != found || !strings.HasPrefix(consistent, "{") {
			t.Errorf("consistent-get printed %q, want find's %q", consistent, found)
		}
	})
	t.Run("a miss keeps its lowercase wording", func(t *testing.T) {
		session := newFixtureSession(t)
		got := runStatements(t, session.cli, session.out, "consistent-get app Users zz")
		if got != "> consistent-get app Users zz\nnot found\n" {
			t.Errorf("a missing key printed %q", got)
		}
	})
	t.Run("a miss the holders could not confirm is a refusal, not not found", func(t *testing.T) {
		var results bytes.Buffer
		_, c := newStubServer(t, map[transport.Opcode]interface{}{
			transport.OpConsistentGet: server.ConsistentGetResponse{
				Message: `consistent get of "k": 2 of 3 holders could not answer: dial tcp: refused; the key may exist`,
			},
		}, &results)
		results.Reset()
		err := c.handle("consistent-get app Users k")
		var refused *RefusedError
		if !errors.As(err, &refused) {
			t.Fatalf("consistent-get returned %v, want a *RefusedError", err)
		}
		if info := classify(err); info.Exit != ExitRefused {
			t.Errorf("exit code = %d, want ExitRefused (%d)", info.Exit, ExitRefused)
		}
		if !strings.Contains(err.Error(), "2 of 3 holders could not answer") {
			t.Errorf("refusal %q does not carry the server's message", err)
		}
		if strings.Contains(results.String(), "not found") {
			t.Errorf("results stream %q says not found", results.String())
		}
	})
	t.Run("the extjson flag applies", func(t *testing.T) {
		session := newFixtureSession(t)
		session.cli.handler.SetExtendedJSON(true)
		runQuiet(t, session, `db.Users.insert({"_id_": "d1", "at": `+date+`})`)
		got := runStatements(t, session.cli, session.out, "consistent-get app Users d1")
		if !strings.Contains(got, `"at": {"$date":"2026-02-01T00:00:00Z"}`) {
			t.Errorf("consistent-get under --extjson printed %q, want an Extended JSON date", got)
		}
	})
	t.Run("a repaired read is reported on the chatter stream", func(t *testing.T) {
		document, err := bson.Marshal(bson.D{{Key: "_id_", Value: "k"}, {Key: "n", Value: 1}})
		if err != nil {
			t.Fatalf("marshalling the stub document: %v", err)
		}
		var results, chatter bytes.Buffer
		_, c := newStubServer(t, map[transport.Opcode]interface{}{
			transport.OpConsistentGet: server.ConsistentGetResponse{Found: true, Data: document, Repaired: true},
		}, &results)
		c.handler.SetChatter(&chatter)
		results.Reset()
		if err := c.handle("consistent-get app Users k"); err != nil {
			t.Fatalf("consistent-get: %v", err)
		}
		if got := results.String(); got != `{"_id_": "k", "n": 1}`+"\n" {
			t.Errorf("the results stream carries %q, want only the document", got)
		}
		if got := chatter.String(); got != "note: this read repaired a replica\n" {
			t.Errorf("the chatter stream carries %q, want the repair note", got)
		}
	})
}

// storedDocument reads one document of bucket Q in database app, decoded.
func storedDocument(t *testing.T, c *CLI, key string) bson.M {
	t.Helper()
	var found server.FindResponse
	mustSend(t, c, transport.OpGet, &server.FindRequest{Database: "app", Bucket: "Q", Key: key}, &found)
	if !found.Found {
		t.Fatalf("document %q is not stored: %s", key, found.Message)
	}
	var document bson.M
	if err := bson.Unmarshal(found.Data, &document); err != nil {
		t.Fatalf("decoding %q: %v", key, err)
	}
	return document
}

// newBucketQ starts a server with database app and a hash bucket Q.
func newBucketQ(t *testing.T) *CLI {
	t.Helper()
	c := newDumpTestServer(t)
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	mustSend(t, c, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "Q", Type: server.BucketTypeHash}, nil)
	return c
}

// TestCLIUnsetListParses: the BSON reader hands a list back as primitive.A, a
// type stringList did not know, so {$unset: ["a"]} was refused as "not a list".
func TestCLIUnsetListParses(t *testing.T) {
	ops, err := parseDocument(`{$unset: ["a", "b"]}`)
	if err != nil {
		t.Fatalf("parseDocument: %v", err)
	}
	request := &server.UpdateRequest{}
	if err := applyUpdateOperators(request, ops); err != nil {
		t.Fatalf("applyUpdateOperators refused $unset with a list: %v", err)
	}
	if !reflect.DeepEqual(request.Unset, []string{"a", "b"}) {
		t.Errorf("Unset = %v, want [a b]", request.Unset)
	}

	c := newBucketQ(t)
	mustSend(t, c, transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Q", Key: "d", Data: map[string]interface{}{"a": 1, "b": 2, "c": 3}}, nil)
	if _, err := runStatement(t, c, `db.Q.update("d", {$unset: ["a", "b"]})`); err != nil {
		t.Fatalf("update with $unset: %v", err)
	}
	if stored := storedDocument(t, c, "d"); stored["a"] != nil || stored["b"] != nil || stored["c"] == nil {
		t.Errorf("after $unset [a, b] the document is %v, want only c left", stored)
	}
}

// TestCLIIncSmallIntegerParses: {$inc: {n: 1}} is an int32 now, which
// numberToInt64 did not know. A large integer and a whole double still work.
func TestCLIIncSmallIntegerParses(t *testing.T) {
	ops, err := parseDocument(`{$inc: {n: 1, big: 3000000000, whole: 2.0}}`)
	if err != nil {
		t.Fatalf("parseDocument: %v", err)
	}
	request := &server.UpdateRequest{}
	if err := applyUpdateOperators(request, ops); err != nil {
		t.Fatalf("applyUpdateOperators refused $inc with an int32: %v", err)
	}
	if want := (map[string]int64{"n": 1, "big": 3000000000, "whole": 2}); !reflect.DeepEqual(request.Inc, want) {
		t.Errorf("Inc = %v, want %v", request.Inc, want)
	}

	c := newBucketQ(t)
	mustSend(t, c, transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Q", Key: "d", Data: map[string]interface{}{"n": int32(1)}}, nil)
	if _, err := runStatement(t, c, `db.Q.update("d", {$inc: {n: 4}})`); err != nil {
		t.Fatalf("update with $inc: %v", err)
	}
	if got := wholeNumber(t, storedDocument(t, c, "d")["n"]); got != 5 {
		t.Errorf("n = %d after $inc 4 on 1, want 5", got)
	}
}

// TestDateLiteralsRunAtThePrompt checks end to end that ISODate(...) and
// new Date(...) reach the server from a typed statement and are stored as dates.
func TestDateLiteralsRunAtThePrompt(t *testing.T) {
	c := newBucketQ(t)
	statements := []string{
		`db.Q.insert({_id_: "a", ts: ISODate("2026-09-20T10:30:00Z")})`,
		`db.Q.insert({_id_: "b", ts: new Date("2026-09-20T10:30:00Z")})`,
	}
	for _, statement := range statements {
		if _, err := runStatement(t, c, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	want := time.Date(2026, 9, 20, 10, 30, 0, 0, time.UTC)
	for _, key := range []string{"a", "b"} {
		stored := storedDocument(t, c, key)["ts"]
		storedDate, isDate := stored.(primitive.DateTime)
		if !isDate || !storedDate.Time().Equal(want) {
			t.Errorf("document %q: ts = %#v, want the date %s", key, stored, want)
		}
	}
	if _, err := runStatement(t, c, `db.Q.find({ts: {$gt: ISODate("2026-01-01T00:00:00Z")}})`); err != nil {
		t.Errorf("find with an ISODate filter: %v", err)
	}
}

// TestNowIsSentFromTheDocumentTypedAtThePrompt: the server fixes
// now() once per write, before the log, so the prompt sends it in an
// insert and an update instead of refusing it, and the stored value is a date
// at the server's clock, not the text "now()".
func TestNowIsSentFromTheDocumentTypedAtThePrompt(t *testing.T) {
	c := newBucketQ(t)
	before := time.Now().Add(-time.Second)
	for _, statement := range []string{
		`db.Q.insert({_id_: "n", at: now()})`,
		`db.Q.insert({_id_: "m", Events: [{at: now()}], k: 1})`,
		`db.Q.update("n", {$set: {later: now()}})`,
	} {
		if _, err := runStatement(t, c, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	after := time.Now().Add(time.Second)

	nested := storedDocument(t, c, "m")["Events"].(primitive.A)[0].(bson.M)["at"]
	stored := storedDocument(t, c, "n")
	for _, value := range []interface{}{stored["at"], stored["later"], nested} {
		storedDate, isDate := value.(primitive.DateTime)
		if !isDate {
			t.Fatalf("now() was stored as %#v, want a date", value)
		}
		if instant := storedDate.Time(); instant.Before(before) || instant.After(after) {
			t.Errorf("now() was stored as %s, want a time between %s and %s", instant, before, after)
		}
	}
}

// TestAnUnknownFunctionIsStillRefusedInADocument: a bare word followed by '('
// would otherwise be stored as text.
func TestAnUnknownFunctionIsStillRefusedInADocument(t *testing.T) {
	c := newBucketQ(t)
	_, err := runStatement(t, c, `db.Q.insert({_id_: "n", at: tomorrow()})`)
	if err == nil || !strings.Contains(err.Error(), "tomorrow()") {
		t.Fatalf("tomorrow() in a document: error %v, want one that names tomorrow()", err)
	}
}

// TestNowWithAMethodIsRefusedByName: the server resolves the bare now() only,
// so now().add(...) is refused with the fixed-date alternative named, rather
// than stored as text or as a half-read document.
func TestNowWithAMethodIsRefusedByName(t *testing.T) {
	c := newBucketQ(t)
	for _, statement := range []string{
		`db.Q.insert({_id_: "n", at: now().add(7, "d")})`,
		`db.Q.insert({_id_: "n", at: now() .add(7, "d")})`,
	} {
		_, err := runStatement(t, c, statement)
		if err == nil {
			t.Fatalf("%s was accepted", statement)
		}
		for _, want := range []string{"now()", "add", "ISODate"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%s: error %q does not mention %q", statement, err, want)
			}
		}
	}
	var found server.FindResponse
	mustSend(t, c, transport.OpGet, &server.FindRequest{Database: "app", Bucket: "Q", Key: "n"}, &found)
	if found.Found {
		t.Errorf("a refused insert stored a document: %s", found.Message)
	}
}

// TestIncRefusesAFractionalOrHugeDelta: numberToInt64 truncated a
// double, so $inc 1.5 added 1, $inc 0.5 added 0 and $inc 1e30 added whatever
// the conversion yielded, all reported as success. The wire field is a whole
// number, so the CLI refuses what it cannot send and the document is unchanged.
func TestIncRefusesAFractionalOrHugeDelta(t *testing.T) {
	c := newBucketQ(t)
	mustSend(t, c, transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Q", Key: "d", Data: map[string]interface{}{"n": int32(10)}}, nil)

	for _, delta := range []string{"1.5", "0.5", "-0.5", "1e30", "-1e30"} {
		t.Run(delta, func(t *testing.T) {
			_, err := runStatement(t, c, `db.Q.update("d", {$inc: {n: `+delta+`}})`)
			if err == nil {
				t.Fatalf("$inc %s was accepted", delta)
			}
			for _, want := range []string{`"n"`, "whole number", "$set"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q lacks %q", err, want)
				}
			}
			if got := wholeNumber(t, storedDocument(t, c, "d")["n"]); got != 10 {
				t.Errorf("n = %d after the refused $inc %s, want 10", got, delta)
			}
		})
	}

	if _, err := runStatement(t, c, `db.Q.update("d", {$inc: {n: 2.0}})`); err != nil {
		t.Fatalf("$inc of the whole double 2.0 was refused: %v", err)
	}
	if got := wholeNumber(t, storedDocument(t, c, "d")["n"]); got != 12 {
		t.Errorf("n = %d after $inc 2.0 on 10, want 12", got)
	}
}

// TestCLIFractionalPriorityIsRefusedOnAHeapOnly: a heap orders by a whole
// number, so 2.5 is refused there with the fix; on any other engine the
// "priority" key is the user's data and a fraction in it is stored as written.
func TestCLIFractionalPriorityIsRefusedOnAHeapOnly(t *testing.T) {
	c := newDumpTestServer(t)
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	mustSend(t, c, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "Heap", Type: server.BucketTypeHeap}, nil)
	mustSend(t, c, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "Fifo", Type: server.BucketTypeFIFO}, nil)
	mustSend(t, c, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "Post", Type: server.BucketTypeHash, Options: map[string]interface{}{
		"stores": []interface{}{
			map[string]interface{}{"name": "Ranked", "engine": "heapStore"},
			map[string]interface{}{"name": "Queue", "engine": "fifoStore"},
		},
	}}, nil)
	mustSend(t, c, transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"title": "p1"}}, nil)

	for _, statement := range []string{
		`db.Heap.push({priority: 2.5, x: 1})`,
		`db.Post["p1"].Ranked.push({priority: 2.5, x: 1})`,
	} {
		_, err := runStatement(t, c, statement)
		if err == nil || !strings.Contains(err.Error(), "priority") || !strings.Contains(err.Error(), "whole number") {
			t.Errorf("%s = %v, want a refusal that says the priority must be a whole number", statement, err)
		}
	}
	for _, statement := range []string{
		`db.Fifo.push({priority: 2.5, x: 1})`,
		`db.Post["p1"].Queue.push({priority: 2.5, x: 1})`,
	} {
		if _, err := runStatement(t, c, statement); err != nil {
			t.Errorf("%s was refused on a non-heap engine: %v", statement, err)
		}
	}
}

// TestCLIFilterOnAUUIDFieldMatches: a filter is parsed like a document, so a
// {"$uuid": ...} in it is a UUID and finds the document that holds one. The
// document is written straight to the server because that is how a driver or
// ingest stores it; the CLI's own insert is not under test here.
func TestCLIFilterOnAUUIDFieldMatches(t *testing.T) {
	c := newDumpTestServer(t)
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	mustSend(t, c, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "Q", Type: server.BucketTypeHash}, nil)
	identifier := primitive.Binary{Subtype: 4, Data: []byte{0xd9, 0xc3, 0xb3, 0xa0, 0x1c, 0x3a, 0x4f, 0x5e, 0x8a, 0x3a, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab}}
	mustSend(t, c, transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Q", Key: "hit", Data: map[string]interface{}{"Id": identifier, "title": "the-match"}}, nil)
	mustSend(t, c, transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Q", Key: "miss", Data: map[string]interface{}{"Id": "plain", "title": "other"}}, nil)

	const filter = `{Id: {"$uuid": "d9c3b3a0-1c3a-4f5e-8a3a-0123456789ab"}}`
	counted, err := runStatement(t, c, `db.Q.count(`+filter+`)`)
	if err != nil || strings.TrimSpace(counted) != "1" {
		t.Errorf("count(%s) = %q, %v; want 1", filter, counted, err)
	}
	found, err := runStatement(t, c, `db.Q.find(`+filter+`)`)
	if err != nil || !strings.Contains(found, "the-match") || strings.Contains(found, "other") {
		t.Errorf("find(%s) = %q, %v; want only the UUID document", filter, found, err)
	}
}

// wholeNumber reads a decoded BSON number of any width as an int64, so a test
// about order does not care whether the CLI sent an int32, an int64 or a double.
func wholeNumber(t *testing.T, value interface{}) int64 {
	t.Helper()
	switch number := value.(type) {
	case int32:
		return int64(number)
	case int64:
		return number
	case float64:
		return int64(number)
	}
	t.Fatalf("%v (%T) is not a number", value, value)
	return 0
}

// TestCLIHeapPushKeepsItsPriority is TF-1 step 1.2's hazard: handlePush read
// the priority with .(float64), and once the CLI parses integers as int32 and
// int64 that assertion fails without a sound, so every heap element lands at
// priority 0 and the heap pops in insertion order. It covers all three ways a
// developer writes a priority: a small integer, a large one, a whole double.
func TestCLIHeapPushKeepsItsPriority(t *testing.T) {
	popped := pushThenPop(t, server.BucketTypeHeap,
		`{priority: 3, x: 1}`,
		`{priority: 7, x: 2}`,
		`{priority: 5, x: 3}`,
		`{priority: 3000000000, x: 4}`,
		`{priority: 6.0, x: 5}`,
	)
	wantOrder := []int64{4, 2, 5, 3, 1}
	for position, want := range wantOrder {
		if got := wholeNumber(t, popped[position]["x"]); got != want {
			t.Errorf("heap pop %d returned x=%d, want x=%d (highest priority first); the priority was lost", position, got, want)
		}
		if _, stored := popped[position]["priority"]; stored {
			t.Errorf("heap pop %d still carries its priority in the document: %v", position, popped[position])
		}
	}
}

// TestCLIFieldPushKeepsItsPriority is the same hazard for a heapStore field:
// handleField read data["priority"].(float64). No test guarded it before TF-1.
func TestCLIFieldPushKeepsItsPriority(t *testing.T) {
	c := newDumpTestServer(t)
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	mustSend(t, c, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "Post", Type: server.BucketTypeHash, Options: map[string]interface{}{
		"stores": []interface{}{map[string]interface{}{"name": "Ranked", "engine": "heapStore"}},
	}}, nil)
	mustSend(t, c, transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"title": "p1"}}, nil)
	for _, statement := range []string{
		`db.Post["p1"].Ranked.push({priority: 3, n: 1})`,
		`db.Post["p1"].Ranked.push({priority: 7, n: 2})`,
		`db.Post["p1"].Ranked.push({priority: 3000000000, n: 3})`,
		`db.Post["p1"].Ranked.push({priority: 5.0, n: 4})`,
	} {
		if _, err := runStatement(t, c, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	wantPriorities := []int64{3000000000, 7, 5, 3}
	for position, want := range wantPriorities {
		var popped server.FieldResponse
		mustSend(t, c, transport.OpFieldPop, &server.FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Ranked"}, &popped)
		if !popped.Found || len(popped.Items) != 1 || popped.Items[0].Priority == nil || *popped.Items[0].Priority != want {
			t.Errorf("heap field pop %d = %+v, want one item at priority %d; the push dropped its priority", position, popped, want)
		}
	}
}

// TestApplyUpdateOperatorsFillsPushAddToSetMulMinMaxRename: the
// help listed six update operators the parser refused. Each now fills the
// UpdateRequest field the server reads, with the value the user typed (an
// int32 stays an int32, the TF-1 number rule) and $mul's amount as a float64.
func TestApplyUpdateOperatorsFillsPushAddToSetMulMinMaxRename(t *testing.T) {
	ops, err := parseDocument(`{
		$push: {Tags: "y", "Lines[0].Opts": 3},
		$addToSet: {Tags: "z"},
		$mul: {Price: 2, Rate: 1.5, Big: 3000000000},
		$min: {Low: 4},
		$max: {High: 9.5},
		$rename: {old: "new"}
	}`)
	if err != nil {
		t.Fatalf("parseDocument: %v", err)
	}
	request := &server.UpdateRequest{}
	if err := applyUpdateOperators(request, ops); err != nil {
		t.Fatalf("applyUpdateOperators refused the six operators: %v", err)
	}
	if want := (map[string]interface{}{"Tags": "y", "Lines[0].Opts": int32(3)}); !reflect.DeepEqual(request.Push, want) {
		t.Errorf("Push = %#v, want %#v", request.Push, want)
	}
	if want := (map[string]interface{}{"Tags": "z"}); !reflect.DeepEqual(request.AddToSet, want) {
		t.Errorf("AddToSet = %#v, want %#v", request.AddToSet, want)
	}
	if want := (map[string]float64{"Price": 2, "Rate": 1.5, "Big": 3000000000}); !reflect.DeepEqual(request.Mul, want) {
		t.Errorf("Mul = %#v, want %#v", request.Mul, want)
	}
	if want := (map[string]interface{}{"Low": int32(4)}); !reflect.DeepEqual(request.Min, want) {
		t.Errorf("Min = %#v, want %#v", request.Min, want)
	}
	if want := (map[string]interface{}{"High": 9.5}); !reflect.DeepEqual(request.Max, want) {
		t.Errorf("Max = %#v, want %#v", request.Max, want)
	}
	if want := (map[string]string{"old": "new"}); !reflect.DeepEqual(request.Rename, want) {
		t.Errorf("Rename = %#v, want %#v", request.Rename, want)
	}
}

// TestApplyUpdateOperatorsExplainsABadOperand: each new arm names the operator
// and shows the form that works, as the $set and $inc arms do (rule 3).
func TestApplyUpdateOperatorsExplainsABadOperand(t *testing.T) {
	tests := []struct {
		statement string
		wantInErr []string
	}{
		{`{$push: ["a"]}`, []string{"$push needs a document", "{$push: {field: value}}"}},
		{`{$addToSet: 3}`, []string{"$addToSet needs a document"}},
		{`{$mul: {Price: "two"}}`, []string{`$mul on "Price"`, "number", "$set"}},
		{`{$mul: 2}`, []string{"$mul needs a document"}},
		{`{$min: "x"}`, []string{"$min needs a document"}},
		{`{$max: "x"}`, []string{"$max needs a document"}},
		{`{$rename: {old: 5}}`, []string{"$rename", `"old"`, "string", `{$rename: {oldName: "newName"}}`}},
		{`{$rename: ["a"]}`, []string{"$rename needs a document"}},
	}
	for _, test := range tests {
		t.Run(test.statement, func(t *testing.T) {
			ops, err := parseDocument(test.statement)
			if err != nil {
				t.Fatalf("parseDocument: %v", err)
			}
			err = applyUpdateOperators(&server.UpdateRequest{}, ops)
			if err == nil {
				t.Fatalf("%s was accepted", test.statement)
			}
			if strings.Contains(err.Error(), "is not supported") {
				t.Fatalf("%s was refused as an unsupported operator: %v", test.statement, err)
			}
			for _, want := range test.wantInErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not contain %q", err, want)
				}
			}
		})
	}
}

// TestCLIAcceptsEveryServerUpdateOperator checks end to end that each of the
// six operators the server applies (server/update_operators.go) reaches it from
// a typed statement and does what it does for a driver.
func TestCLIAcceptsEveryServerUpdateOperator(t *testing.T) {
	tests := []struct {
		statement string
		field     string
		want      string
	}{
		{`db.Q.update("d", {$push: {Tags: "y"}})`, "Tags", "[x y]"},
		{`db.Q.update("d", {$addToSet: {Tags: "x"}})`, "Tags", "[x]"},
		{`db.Q.update("d", {$addToSet: {Tags: "z"}})`, "Tags", "[x z]"},
		{`db.Q.update("d", {$mul: {n: 3}})`, "n", "6"},
		{`db.Q.update("d", {$mul: {n: 1.5}})`, "n", "3"},
		{`db.Q.update("d", {$min: {low: 4}})`, "low", "4"},
		{`db.Q.update("d", {$max: {high: 9}})`, "high", "9"},
		{`db.Q.update("d", {$rename: {old: "new"}})`, "new", "v"},
		{`db.Q.update("d", {$push: {"Lines[0].Opts": 1}})`, "Lines", "[map[Opts:[1] Sku:A]]"},
	}
	for _, test := range tests {
		t.Run(test.statement, func(t *testing.T) {
			c := newBucketQ(t)
			mustSend(t, c, transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Q", Key: "d", Data: map[string]interface{}{
				"n": int32(2), "Tags": []interface{}{"x"}, "low": int32(10), "high": int32(1), "old": "v",
				"Lines": []interface{}{map[string]interface{}{"Sku": "A"}},
			}}, nil)
			if _, err := runStatement(t, c, test.statement); err != nil {
				t.Fatalf("%s: %v", test.statement, err)
			}
			stored := storedDocument(t, c, "d")
			if got := fmt.Sprint(stored[test.field]); got != test.want {
				t.Errorf("%s: %s = %s, want %s (document %v)", test.statement, test.field, got, test.want, stored)
			}
		})
	}
}

// TestGetSendsNoBucketStatsFrames: BUG-0520. get asked the server for the
// bucket's type with BUCKET_STATS, which walks every element of every store
// field the bucket declares, once per statement. A hundred reads of one bucket
// must send none, and ask the declaration (which walks nothing) once.
func TestGetSendsNoBucketStatsFrames(t *testing.T) {
	stub, c, _ := newDeclarationStub(t, declarationScript(declaredBucket{
		bucketType: server.BucketTypeHash,
		stores:     []server.StoreStats{{Name: "Purchases", Engine: "fifo", Elements: 60000}},
	}))
	for range 100 {
		if err := c.handle(`db.Users.get("u1")`); err != nil {
			t.Fatalf("get: %v", err)
		}
	}
	if got := stub.sent(transport.OpGet); got != 100 {
		t.Fatalf("100 gets sent %d GET frames: the fixture is not reading", got)
	}
	if got := stub.sent(transport.OpBucketStats); got != 0 {
		t.Errorf("100 gets sent %d BUCKET_STATS frames, want 0", got)
	}
	// Owner decision 2026-10-12 (option Z): every lookup that routes a statement
	// is one fresh BUCKET_INFO, never a cached type.
	if got := stub.sent(transport.OpBucketInfo); got != 100 {
		t.Errorf("100 gets sent %d BUCKET_INFO frames, want 100: one fresh answer per statement", got)
	}
}

// TestNoteFewerCopiesPointsToStatus is BUG-0601. A write confirmed by fewer
// copies than the bucket's factor made the CLI say only "stored on 1 of 3
// copies", which names no cause and no next step: a developer at 3am could not
// tell a replica still connecting just after 'create bucket' from a node that
// refused the bucket, and had nowhere to look. The note now names the two
// places the cause is - 'status' and the node logs - and names the common
// causes briefly. A write on every copy still says nothing.
func TestNoteFewerCopiesPointsToStatus(t *testing.T) {
	cases := []struct {
		name     string
		answer   server.InsertResponse
		wantNote bool
	}{
		{"a copy is missing", server.InsertResponse{ID: "k1", Success: true, CopiesConfirmed: 1, ReplicationFactor: 3}, true},
		{"every copy holds it", server.InsertResponse{ID: "k1", Success: true, CopiesConfirmed: 3, ReplicationFactor: 3}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var results, chatter bytes.Buffer
			_, c := newStubServer(t, map[transport.Opcode]interface{}{transport.OpPut: tc.answer}, &results)
			c.handler.SetChatter(&chatter)
			c.currentDB = "app"
			results.Reset()
			if err := c.handle(`db.Users.insert({"_id_": "k1"})`); err != nil {
				t.Fatalf("insert: %v", err)
			}
			note := chatter.String()
			if !tc.wantNote {
				if note != "" {
					t.Errorf("a write on every copy printed %q, want silence", note)
				}
				return
			}
			for _, want := range []string{"stored on 1 of 3 copies", "status", "node logs", "create bucket"} {
				if !strings.Contains(note, want) {
					t.Errorf("the fewer-copies note is %q, and does not name %q", note, want)
				}
			}
			if lines := strings.Count(strings.TrimRight(note, "\n"), "\n") + 1; lines > 2 {
				t.Errorf("the fewer-copies note is %d lines, want at most two: %q", lines, note)
			}
		})
	}
}
