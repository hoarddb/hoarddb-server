package cli

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0520: the CLI asked BUCKET_STATS, which walks every store element, before
// every get. These tests count the frames a statement sends, because the
// answer is right either way: only the cost and the error texts are at stake.

// declaredBucket is what a stub server answers for one bucket's declaration.
type declaredBucket struct {
	bucketType server.BucketType
	stores     []server.StoreStats
}

// declarationScript answers the bucket's declaration the way the server does:
// BUCKET_INFO with the top-level stores only, BUCKET_STATS with all of them,
// plus one reply for each verb the tests send afterwards.
func declarationScript(bucket declaredBucket) map[transport.Opcode]interface{} {
	var topLevel []server.StoreFieldInfo
	for _, store := range bucket.stores {
		if store.Parent == "" {
			topLevel = append(topLevel, server.StoreFieldInfo{Name: store.Name, Engine: store.Engine})
		}
	}
	document, _ := bson.Marshal(bson.M{"_id_": "u1", "n": 1})
	return map[transport.Opcode]interface{}{
		transport.OpBucketInfo: server.BucketInfoResponse{Found: true, Database: "app",
			Bucket: server.BucketInfo{Name: "Users", Type: bucket.bucketType}, StoreFields: topLevel},
		transport.OpBucketStats: server.BucketStatsResponse{Found: true, Database: "app", Bucket: "Users",
			Type: bucket.bucketType, Stores: bucket.stores},
		transport.OpGet:       server.FindResponse{Found: true, Data: document},
		transport.OpFIFOPush:  server.PushResponse{Success: true, ID: "e1"},
		transport.OpFieldPut:  server.FieldResponse{Success: true},
		transport.OpFieldPush: server.FieldResponse{Success: true},
	}
}

// newDeclarationStub is a connected CLI on a stub that answers the script, with
// the connection's own start-up frames already forgotten.
func newDeclarationStub(t *testing.T, script map[transport.Opcode]interface{}) (*stubServer, *CLI, *bytes.Buffer) {
	t.Helper()
	var out bytes.Buffer
	stub, c := newStubServer(t, script, &out)
	c.currentDB = "app"
	stub.sequence()
	out.Reset()
	return stub, c, &out
}

// errorFrame is a scripted reply that is an ERROR frame.
func errorFrame(t *testing.T, code transport.ErrorCode, message string) stubRawReply {
	t.Helper()
	payload, err := bson.Marshal(server.ErrorResponse{Code: int(code), Message: message})
	if err != nil {
		t.Fatalf("marshalling the scripted error: %v", err)
	}
	return stubRawReply{Opcode: transport.OpError, Payload: payload}
}

// TestPushWithANumericPrioritySendsOneFreshBucketInfoEach: a bucket push whose
// document has a numeric priority asks the type, to know whether it is a heap.
func TestPushWithANumericPrioritySendsOneFreshBucketInfoEach(t *testing.T) {
	for _, bucketType := range []server.BucketType{server.BucketTypeHeap, server.BucketTypeFIFO} {
		t.Run(string(bucketType), func(t *testing.T) {
			stub, c, _ := newDeclarationStub(t, declarationScript(declaredBucket{bucketType: bucketType}))
			for range 20 {
				if err := c.handle(`db.Users.push({priority: 3, n: 1})`); err != nil {
					t.Fatalf("push: %v", err)
				}
			}
			if got := stub.sent(transport.OpBucketStats); got != 0 {
				t.Errorf("20 pushes with a priority sent %d BUCKET_STATS frames, want 0", got)
			}
			if got := stub.sent(transport.OpBucketInfo); got != 20 {
				t.Errorf("20 pushes with a priority sent %d BUCKET_INFO frames, want 20: one fresh answer per statement, never the cached type", got)
			}
		})
	}
}

// TestANestedFieldKeepsTheBucketStatsLookup: BUCKET_INFO names the top-level
// stores only, so a nested store field's engine cannot be read from it. That
// case alone still asks BUCKET_STATS, which carries the parent of each store.
func TestANestedFieldKeepsTheBucketStatsLookup(t *testing.T) {
	stub, c, out := newDeclarationStub(t, declarationScript(declaredBucket{
		bucketType: server.BucketTypeHash,
		stores: []server.StoreStats{
			{Name: "Comments", Engine: "fifo"},
			{Name: "Flags", Engine: "blob", Parent: "Comments"},
		},
	}))
	cmd, err := ParseCommand(`db.Users["u1"].Comments["c1"].Flags.put("k", {data: "x"})`)
	if err != nil || len(cmd.Path) == 0 {
		t.Fatalf("the fixture is not a nested address: %+v, %v", cmd, err)
	}
	if err := c.handle(`db.Users["u1"].Comments["c1"].Flags.put("k", {data: "x"})`); err != nil {
		t.Fatalf("nested put: %v\n%s", err, out)
	}
	if got := stub.sent(transport.OpBucketStats); got != 1 {
		t.Errorf("a nested field put sent %d BUCKET_STATS frames, want 1", got)
	}
}

// TestGetKeepsTheErrorsAUserSees pins what each failing get prints, captured
// from main before the change. Every row must read the same after it.
func TestGetKeepsTheErrorsAUserSees(t *testing.T) {
	notFoundMessage := `bucket "Ghost" not found in database "app". Run 'show buckets in app' to list what exists, or 'create bucket Ghost in app { type: hash }' to create it`
	rows := []struct {
		name      string
		statement string
		reply     interface{}
		wantError string
	}{
		{
			name:      "a missing bucket answered as an ERROR frame",
			statement: `db.Ghost.get("k")`,
			reply:     errorFrame(t, transport.ErrNotFound, "bucket not found: "+notFoundMessage),
			wantError: `inspecting bucket "Ghost": server error (1): bucket not found: ` + notFoundMessage,
		},
		{
			name:      "a bucket the user cannot read",
			statement: `db.Users.get("k")`,
			reply:     errorFrame(t, transport.ErrPermissionDenied, `permission denied: user "bob" has no read grant on app`),
			wantError: `inspecting bucket "Users": server error (3): permission denied: user "bob" has no read grant on app`,
		},
		{
			name:      "a reply that says found:false",
			statement: `db.Ghost.get("k")`,
			reply:     server.BucketStatsResponse{Found: false},
			wantError: `bucket "Ghost" not found in database "app" — run 'show buckets' to list what exists, or 'create bucket <name> in app {type: hash}' to create it`,
		},
		{
			name:      "a fifo bucket",
			statement: `db.Users.get("k")`,
			reply:     server.BucketStatsResponse{Found: true, Type: server.BucketTypeFIFO},
			wantError: "db.Users.get() is not defined for fifo buckets — they are read by position:\n       db.Users.peek()  shows the next entry without removing it\n       db.Users.pop()   removes and returns the next entry",
		},
		{
			name:      "a heap bucket",
			statement: `db.Users.get("k")`,
			reply:     server.BucketStatsResponse{Found: true, Type: server.BucketTypeHeap},
			wantError: "db.Users.get() is not defined for heap buckets — use db.Users.peek() for the highest-priority entry, or db.Users.pop() to remove it",
		},
	}
	// The same refusal is scripted on both declaration calls: before the change
	// the CLI asked BUCKET_STATS, after it asks BUCKET_INFO, and the user must
	// read the same line either way.
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			script := map[transport.Opcode]interface{}{
				transport.OpBucketStats: row.reply,
				transport.OpBucketInfo:  infoReplyLike(row.reply),
			}
			_, c, out := newDeclarationStub(t, script)
			err := c.handle(row.statement)
			if err == nil {
				t.Fatalf("%s succeeded and printed %q", row.statement, out)
			}
			if err.Error() != row.wantError {
				t.Errorf("%s returned\n%q\nwant\n%q", row.statement, err.Error(), row.wantError)
			}
			if out.Len() != 0 {
				t.Errorf("%s printed %q before failing; the error line is the whole output", row.statement, out)
			}
		})
	}
}

// infoReplyLike is the BUCKET_INFO reply that says what a BUCKET_STATS reply
// said, so one row can be run against either call.
func infoReplyLike(statsReply interface{}) interface{} {
	stats, isStats := statsReply.(server.BucketStatsResponse)
	if !isStats {
		return statsReply
	}
	return server.BucketInfoResponse{Found: stats.Found, Database: stats.Database,
		Bucket: server.BucketInfo{Name: stats.Bucket, Type: stats.Type}}
}

// TestARefusalAfterAFreshTypeIsNotRetried: the retry is for a stale type. A
// refusal that arrives after the type was just asked is the server's answer.
func TestARefusalAfterAFreshTypeIsNotRetried(t *testing.T) {
	stub, c, _ := newDeclarationStub(t, declarationScript(declaredBucket{bucketType: server.BucketTypeHash}))
	stub.answerWith(func(op transport.Opcode) (interface{}, bool) {
		if op == transport.OpGet {
			return server.FindResponse{Found: false, Message: "find not supported for bucket type hash"}, true
		}
		return nil, false
	})
	for range 2 {
		err := c.handle(`db.Users.get("u1")`)
		var refused *RefusedError
		if !errors.As(err, &refused) || refused.Message != "find not supported for bucket type hash" {
			t.Fatalf("get returned %v, want the server's refusal", err)
		}
		want := []transport.Opcode{transport.OpBucketInfo, transport.OpGet}
		if got := stub.sequence(); !slices.Equal(got, want) {
			t.Errorf("a refusal after a fresh declaration sent %v, want %v", got, want)
		}
	}
}

// TestAnOlderServerFallsBackToBucketStats: a server that does not know
// BUCKET_INFO answers "unsupported opcode"; the CLI asks the call it always
// asked, once per bucket, and the read works.
func TestAnOlderServerFallsBackToBucketStats(t *testing.T) {
	script := declarationScript(declaredBucket{bucketType: server.BucketTypeHash})
	script[transport.OpBucketInfo] = errorFrame(t, transport.ErrInternal,
		"unsupported opcode 0xD9 (request id 1): this server does not implement it")
	stub, c, out := newDeclarationStub(t, script)

	for round := range 3 {
		out.Reset()
		if err := c.handle(`db.Users.get("u1")`); err != nil {
			t.Fatalf("get %d on an older server: %v", round, err)
		}
		if got := strings.TrimSpace(out.String()); got != `{"_id_": "u1", "n": 1}` {
			t.Fatalf("get %d printed %q", round, got)
		}
	}
	if got := stub.sent(transport.OpBucketStats); got != 3 {
		t.Errorf("three gets on an older server sent %d BUCKET_STATS frames, want 3: asked every time, as before BUCKET_INFO", got)
	}
	if got := stub.sent(transport.OpBucketInfo); got != 1 {
		t.Errorf("three gets on an older server sent %d BUCKET_INFO frames, want 1: the refusal is remembered for the bucket", got)
	}
}

// TestAnOlderServerDecoratesNothing: the fallback gives the type and the
// engines; it must not turn the terminal display on where it was off.
func TestAnOlderServerDecoratesNothing(t *testing.T) {
	script := declarationScript(declaredBucket{bucketType: server.BucketTypeHash,
		stores: []server.StoreStats{{Name: "Photos", Engine: "lifo"}}})
	script[transport.OpBucketInfo] = errorFrame(t, transport.ErrInternal,
		"unsupported opcode 0xD9 (request id 1): this server does not implement it")
	_, c, out := newDeclarationStub(t, script)
	c.handler.SetDocumentDecoration(true)
	if err := c.handle(`db.Users.get("u1")`); err != nil {
		t.Fatalf("get: %v", err)
	}
	if strings.Contains(out.String(), "lifoStore") {
		t.Errorf("an older server's document was decorated: %q", out)
	}
}

// TestAPipedGetAsksAFreshTypeAndPrintsAsItAlwaysHas: the display is gated to a
// terminal, the type lookup is not. A piped session asks one fresh BUCKET_INFO
// per get and still prints every document as it always has.
func TestAPipedGetAsksAFreshTypeAndPrintsAsItAlwaysHas(t *testing.T) {
	c := newDumpTestServer(t)
	seedPostWithStores(t, c)

	for range 3 {
		printed, err := runStatement(t, c, `db.Post.get("p1")`)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if strings.TrimSpace(printed) != plainPostLine {
			t.Fatalf("a piped get printed\n%s\nwant\n%s", printed, plainPostLine)
		}
	}
	if c.handler.declarationFetches != 3 {
		t.Errorf("three piped gets made %d declaration calls, want 3", c.handler.declarationFetches)
	}
}

// Owner decision 2026-10-12: the lookups that decide how a document or element
// is written (a push with a numeric priority, a field put with a data key, a
// field push with a fractional priority) never trust the session's cached
// declaration. A wrong cached answer there changes what is stored, with no
// refusal to notice, so each statement asks one fresh BUCKET_INFO.

// declareStoreField creates app.<bucket> as a hash bucket declaring one store
// field of the given engine word (heapStore, fifoStore, blobStore, ...).
func declareStoreField(t *testing.T, c *CLI, bucket, field, engine string) {
	t.Helper()
	var created server.CreateBucketResponse
	mustSend(t, c, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: bucket, Type: server.BucketTypeHash, Options: map[string]interface{}{
		"stores": []interface{}{map[string]interface{}{"name": field, "engine": engine}},
	}}, &created)
	if !created.Success {
		t.Fatalf("creating %s: %s", bucket, created.Message)
	}
}

// TestAPushWithAPriorityIsNotRoutedByAStaleType: a heap recreated as a fifo
// behind the session's back. The next push must keep its priority key.
func TestAPushWithAPriorityIsNotRoutedByAStaleType(t *testing.T) {
	c := newDumpTestServer(t)
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	mustSend(t, c, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "Moody", Type: server.BucketTypeHeap}, nil)
	if _, err := runStatement(t, c, `db.Moody.push({priority: 3, x: 1})`); err != nil {
		t.Fatalf("push on the heap: %v", err)
	}
	// Direct sends, as another client: the session's own statements would forget.
	mustSend(t, c, transport.OpDeleteBucket, &server.DeleteBucketRequest{Database: "app", Name: "Moody"}, nil)
	mustSend(t, c, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "Moody", Type: server.BucketTypeFIFO}, nil)

	if _, err := runStatement(t, c, `db.Moody.push({priority: 5, x: 2})`); err != nil {
		t.Fatalf("push on the fifo: %v", err)
	}
	popped, err := runStatement(t, c, `db.Moody.pop()`)
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if !strings.Contains(popped, `"priority"`) {
		t.Errorf("a fifo element pushed after the bucket changed type lost its priority key: %s", popped)
	}
}

// TestAFieldPushIsNotRefusedByAStaleEngine: Tags was a heap field, and is a
// fifo field now. A fractional priority is an ordinary key on a fifo.
func TestAFieldPushIsNotRefusedByAStaleEngine(t *testing.T) {
	c := newDumpTestServer(t)
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	declareStoreField(t, c, "Post", "Tags", "heapStore")
	mustSend(t, c, transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"n": 1}}, nil)
	if _, err := runStatement(t, c, `db.Post["p1"].Tags.push({priority: 1.5, n: 1})`); err == nil {
		t.Fatal("a fractional priority on a heap field was accepted")
	}
	mustSend(t, c, transport.OpDeleteBucket, &server.DeleteBucketRequest{Database: "app", Name: "Post"}, nil)
	declareStoreField(t, c, "Post", "Tags", "fifoStore")
	mustSend(t, c, transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"n": 1}}, nil)

	if _, err := runStatement(t, c, `db.Post["p1"].Tags.push({priority: 1.5, n: 1})`); err != nil {
		t.Errorf("a fractional priority on a fifo field was refused by the stale heap engine: %v", err)
	}
}

// otherSession is a second connection to the same server, as another client
// or another terminal: its statements do not touch the first session's state.
func otherSession(t *testing.T, c *CLI) *CLI {
	t.Helper()
	other := NewCLI(WithAddress(c.addr), WithInsecureTLS(true), WithCredentials("admin", "dump-e2e-password"), WithTerminal(&bytes.Buffer{}))
	if err := other.Connect(); err != nil {
		t.Fatalf("connecting the other session: %v", err)
	}
	t.Cleanup(other.Close)
	return other
}

// recreateAs drops app.Moody and creates it again as bucketType holding one
// element under "k", from the other session.
func recreateAs(t *testing.T, other *CLI, bucketType server.BucketType) {
	t.Helper()
	// The bucket may not exist yet on the first call.
	_ = other.handler.send(transport.OpDeleteBucket, &server.DeleteBucketRequest{Database: "app", Name: "Moody"}, nil)
	mustSend(t, other, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "Moody", Type: bucketType}, nil)
	switch bucketType {
	case server.BucketTypeBlob:
		mustSend(t, other, transport.OpBlobWrite, &server.BlobWriteRequest{Database: "app", Bucket: "Moody", Key: "k", Data: []byte("hello")}, nil)
	case server.BucketTypeFIFO, server.BucketTypeLIFO, server.BucketTypeHeap:
		mustSend(t, other, transport.OpFIFOPush, &server.PushRequest{Database: "app", Bucket: "Moody", Data: map[string]interface{}{"n": 1}}, nil)
	default:
		mustSend(t, other, transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Moody", Key: "k", Data: map[string]interface{}{"n": 1}}, nil)
	}
}

// TestAGetAfterAnotherSessionRecreatesTheBucketReadsTheNewType: a cached type
// recovers only when the server refuses the verb, and two cases never do: a
// hash recreated as a blob (the server answers the get with the blob's
// metadata) and a fifo or heap recreated as a hash (the CLI refuses the get
// itself and never asks). The statement after the change must act on the new
// type with no statement in between that forgets anything.
func TestAGetAfterAnotherSessionRecreatesTheBucketReadsTheNewType(t *testing.T) {
	rows := []struct {
		from, to server.BucketType
		wantOut  string // a substring of what a successful get prints
		wantErr  string // a substring of the usage line a refused get returns
	}{
		{server.BucketTypeHash, server.BucketTypeBlob, "Data:\nhello", ""},
		{server.BucketTypeBTree, server.BucketTypeBlob, "Data:\nhello", ""},
		{server.BucketTypeBlob, server.BucketTypeHash, `"n": 1`, ""},
		{server.BucketTypeFIFO, server.BucketTypeHash, `"n": 1`, ""},
		{server.BucketTypeHeap, server.BucketTypeHash, `"n": 1`, ""},
		{server.BucketTypeHash, server.BucketTypeFIFO, "", "db.Moody.get() is not defined for fifo buckets"},
	}
	for _, row := range rows {
		t.Run(string(row.from)+" to "+string(row.to), func(t *testing.T) {
			c := newDumpTestServer(t)
			mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
			other := otherSession(t, c)
			recreateAs(t, other, row.from)
			// The first session reads it once, as it would at the prompt.
			_, _ = runStatement(t, c, `db.Moody.get("k")`)

			recreateAs(t, other, row.to)
			for round := 1; round <= 2; round++ {
				printed, err := runStatement(t, c, `db.Moody.get("k")`)
				switch {
				case row.wantErr != "":
					if err == nil || !strings.Contains(err.Error(), row.wantErr) {
						t.Fatalf("get %d after the change returned %v, want %q", round, err, row.wantErr)
					}
				case err != nil:
					t.Fatalf("get %d after the change: %v", round, err)
				case !strings.Contains(printed, row.wantOut):
					t.Fatalf("get %d after the change printed\n%s\nwant it to contain %q", round, printed, row.wantOut)
				}
			}
		})
	}
}

// TestTheDisplayCacheStillFetchesOncePerBucket: option Z makes the routing
// lookups fresh, and leaves the display's one-call-per-bucket cache alone.
func TestTheDisplayCacheStillFetchesOncePerBucket(t *testing.T) {
	c := terminalSession(t)
	for range 5 {
		for _, statement := range []string{`db.Post.find()`, `db.Post.find("p1")`, `show databases`} {
			if _, err := runStatement(t, c, statement); err != nil {
				t.Fatalf("%s: %v", statement, err)
			}
		}
	}
	if c.handler.declarationFetches != 1 {
		t.Fatalf("fifteen display statements made %d declaration calls, want 1", c.handler.declarationFetches)
	}
}
