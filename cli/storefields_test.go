package cli

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// seedPostWithStores makes app.Post, declaring two store fields, and one
// document. A store field belongs to each document and its elements are read
// through the field's own address, so the document itself never held them.
func seedPostWithStores(t *testing.T, c *CLI) {
	t.Helper()
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	var created server.CreateBucketResponse
	mustSend(t, c, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "Post", Type: server.BucketTypeHash, Options: map[string]interface{}{
		"stores": []interface{}{
			map[string]interface{}{"name": "Photos", "engine": "lifoStore"},
			map[string]interface{}{"name": "Friends", "engine": "hashStore"},
		},
	}}, &created)
	if !created.Success {
		t.Fatalf("creating Post: %s", created.Message)
	}
	mustSend(t, c, transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Post", Key: "p1",
		Data: map[string]interface{}{"Email": "susan@smith.com", "FirstName": "Susan"}}, nil)
}

// terminalSession is a session whose display gate is open, as on a terminal.
func terminalSession(t *testing.T) *CLI {
	t.Helper()
	c := newDumpTestServer(t)
	seedPostWithStores(t, c)
	c.handler.SetDocumentDecoration(true)
	return c
}

const (
	decoratedPostLine = `{"_id_": "p1", "Email": "susan@smith.com", "FirstName": "Susan", "Photos": "lifoStore", "Friends": "hashStore"}`
	plainPostLine     = `{"Email": "susan@smith.com", "FirstName": "Susan", "_id_": "p1"}`
)

// TestATerminalReadNamesTheStoresTheBucketDeclares is the point of the change:
// a document read out of a bucket on a terminal names the stores the bucket
// declares, in declaration order, after its own fields, with _id_ first. The
// value is the declaration, quoted so the line is still JSON.
func TestATerminalReadNamesTheStoresTheBucketDeclares(t *testing.T) {
	c := terminalSession(t)
	for _, statement := range []string{
		`db.Post.find()`, `db.Post.find("p1")`, `db.Post.get("p1")`,
		`db.Post.search({"FirstName": "Susan"})`,
		`db.Post.update("p1", {$set: {"FirstName": "Susan"}})`,
		`db.Post.find({"FirstName": "Susan"})`,
	} {
		printed, err := runStatement(t, c, statement)
		if err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
		if !strings.Contains(printed, decoratedPostLine) {
			t.Errorf("%s printed\n%s\nwant a line\n%s", statement, printed, decoratedPostLine)
		}
	}
}

// TestAPrettyReadPutsTheIDFirstAndTheStoresLast is the shape a person reads.
func TestAPrettyReadPutsTheIDFirstAndTheStoresLast(t *testing.T) {
	c := terminalSession(t)
	c.handler.pretty = true

	printed, err := runStatement(t, c, `db.Post.find()`)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	want := "{\n" +
		"  \"_id_\": \"p1\",\n" +
		"  \"Email\": \"susan@smith.com\",\n" +
		"  \"FirstName\": \"Susan\",\n" +
		"  \"Photos\": \"lifoStore\",\n" +
		"  \"Friends\": \"hashStore\"\n" +
		"}"
	if got := strings.TrimSpace(printed); got != want {
		t.Fatalf("a pretty find rendered\n%s\nwant\n%s", got, want)
	}
}

// TestWithTheGateClosedADocumentPrintsAsItAlwaysHas pins the piped bytes: no
// extra key, the old sorted order, and no call to the server for the
// declaration. A script parses this line.
func TestWithTheGateClosedADocumentPrintsAsItAlwaysHas(t *testing.T) {
	c := newDumpTestServer(t)
	seedPostWithStores(t, c)

	printed, err := runStatement(t, c, `db.Post.find()`)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if strings.TrimSpace(printed) != plainPostLine {
		t.Fatalf("a read with the gate closed printed\n%s\nwant\n%s", printed, plainPostLine)
	}
	if c.handler.declarationFetches != 0 {
		t.Fatalf("a read with the gate closed asked the server for a declaration %d times", c.handler.declarationFetches)
	}
}

// TestTheGateNeedsATerminalAtBothEnds: stdout, stdin and a TERM that can take
// the display, all three.
func TestTheGateNeedsATerminalAtBothEnds(t *testing.T) {
	for _, row := range []struct {
		name                          string
		stdout, stdin, termCanDisplay bool
		want                          bool
	}{
		{"a terminal at both ends", true, true, true, true},
		{"stdout redirected", false, true, true, false},
		{"stdin piped", true, false, true, false},
		{"TERM dumb or unset", true, true, false, false},
		{"nothing is a terminal", false, false, false, false},
	} {
		t.Run(row.name, func(t *testing.T) {
			if got := documentDecorationAllowed(row.stdout, row.stdin, row.termCanDisplay); got != row.want {
				t.Fatalf("documentDecorationAllowed(%v, %v, %v) = %v, want %v", row.stdout, row.stdin, row.termCanDisplay, got, row.want)
			}
		})
	}
}

// TestAStoreFieldsElementIsNotDecorated: an element of a store field is not a
// document of the bucket.
func TestAStoreFieldsElementIsNotDecorated(t *testing.T) {
	c := terminalSession(t)

	if _, err := runStatement(t, c, `db.Post["p1"].Photos.push({file: "a.jpg"})`); err != nil {
		t.Fatalf("push: %v", err)
	}
	printed, err := runStatement(t, c, `db.Post["p1"].Photos.pop()`)
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	for _, declared := range []string{"lifoStore", "hashStore"} {
		if strings.Contains(printed, declared) {
			t.Fatalf("an element of a store field carried the bucket's declaration %q: %s", declared, printed)
		}
	}
	if c.handler.declarationFetches != 0 {
		t.Fatalf("an element read asked for the bucket's declaration %d times", c.handler.declarationFetches)
	}
}

// TestAStoreFieldNamedLikeADocumentKeyPrintsTheDocumentsOwnValue: the display
// never overwrites what the document holds.
func TestAStoreFieldNamedLikeADocumentKeyPrintsTheDocumentsOwnValue(t *testing.T) {
	c := terminalSession(t)
	mustSend(t, c, transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Post", Key: "p2",
		Data: map[string]interface{}{"Photos": "my own value"}}, nil)

	printed, err := runStatement(t, c, `db.Post.get("p2")`)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	want := `{"_id_": "p2", "Photos": "my own value", "Friends": "hashStore"}`
	if strings.TrimSpace(printed) != want {
		t.Fatalf("get printed\n%s\nwant\n%s", printed, want)
	}
}

// TestABucketWithoutStoresCostsOneCallAndPutsTheIDFirst: a bucket that declares
// nothing prints its own fields, _id_ first, and the answer is cached too.
func TestABucketWithoutStoresCostsOneCallAndPutsTheIDFirst(t *testing.T) {
	c := terminalSession(t)
	mustSend(t, c, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "Users", Type: server.BucketTypeHash}, nil)
	mustSend(t, c, transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Users", Key: "ada",
		Data: map[string]interface{}{"Name": "Ada", "Age": 36}}, nil)

	for range 3 {
		printed, err := runStatement(t, c, `db.Users.find("ada")`)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if want := `{"_id_": "ada", "Age": 36, "Name": "Ada"}`; strings.TrimSpace(printed) != want {
			t.Fatalf("get printed\n%s\nwant\n%s", printed, want)
		}
	}
	if c.handler.declarationFetches != 1 {
		t.Fatalf("three reads of one bucket made %d declaration calls, want 1", c.handler.declarationFetches)
	}
}

// TestTheDeclarationIsOneFetchPerBucketPerSession is the cost the display is
// allowed to add: many reads, many commands, one call per bucket.
func TestTheDeclarationIsOneFetchPerBucketPerSession(t *testing.T) {
	c := terminalSession(t)
	for range 5 {
		for _, statement := range []string{`db.Post.find()`, `db.Post.find("p1")`, `show databases`} {
			if _, err := runStatement(t, c, statement); err != nil {
				t.Fatalf("%s: %v", statement, err)
			}
		}
	}
	if c.handler.declarationFetches != 1 {
		t.Fatalf("fifteen statements on one bucket made %d declaration calls, want 1", c.handler.declarationFetches)
	}
}

// TestAStatementThatCanChangeADeclarationForgetsIt: create, alter and drop of a
// bucket, a restore and a reconnect each end what the session knew, so the next
// read asks again and shows the new declaration.
func TestAStatementThatCanChangeADeclarationForgetsIt(t *testing.T) {
	c := terminalSession(t)
	if _, err := runStatement(t, c, `db.Post.find()`); err != nil {
		t.Fatalf("find: %v", err)
	}
	if _, err := runStatement(t, c, `alter bucket Post { Notes lifoStore }`); err != nil {
		t.Fatalf("alter: %v", err)
	}
	printed, err := runStatement(t, c, `db.Post.find()`)
	if err != nil {
		t.Fatalf("find after alter: %v", err)
	}
	if !strings.Contains(printed, `"Notes": "lifoStore"`) {
		t.Fatalf("a find after an alter showed a stale declaration:\n%s", printed)
	}
	if c.handler.declarationFetches != 2 {
		t.Fatalf("find, alter, find made %d declaration calls, want 2", c.handler.declarationFetches)
	}

	for _, statement := range []string{`create bucket Other { type: hash }`, `drop bucket Other`} {
		before := c.handler.declarationFetches
		if _, err := runStatement(t, c, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
		if _, err := runStatement(t, c, `db.Post.find()`); err != nil {
			t.Fatalf("find after %s: %v", statement, err)
		}
		if c.handler.declarationFetches != before+1 {
			t.Fatalf("a find after %q made %d calls, want 1", statement, c.handler.declarationFetches-before)
		}
	}
}

// TestEveryStatementKindIsSortedIntoKeepOrForget keeps the invalidation list
// honest: a statement that is not known to leave declarations alone forgets.
func TestEveryStatementKindIsSortedIntoKeepOrForget(t *testing.T) {
	for _, row := range []struct {
		cmd    Cmd
		forget bool
	}{
		{Cmd{Type: "bucket", Method: "find"}, false},
		{Cmd{Type: "bucket", Method: "insert"}, false},
		{Cmd{Type: "global", Method: "show"}, false},
		{Cmd{Type: "global", Method: "use"}, false},
		{Cmd{Type: "admin", Method: "create_bucket"}, true},
		{Cmd{Type: "admin", Method: "alter_bucket"}, true},
		{Cmd{Type: "admin", Method: "drop_bucket"}, true},
		{Cmd{Type: "admin", Method: "drop_database"}, true},
		{Cmd{Type: "restore"}, true},
		{Cmd{Type: "global", Method: "connect"}, true},
		{Cmd{Type: "global", Method: "migrate"}, true},
		{Cmd{Type: "bucket", Method: "a-verb-from-a-newer-cli"}, true},
	} {
		if got := forgetsDeclarations(&row.cmd); got != row.forget {
			t.Errorf("forgetsDeclarations(%s %s) = %v, want %v", row.cmd.Type, row.cmd.Method, got, row.forget)
		}
	}
}

// TestADeclarationThatCannotBeReadPrintsTheDocumentAsItAlwaysHas covers an
// older server that does not know the op, a user without the read grant, a
// missing bucket and a timeout: the document prints undecorated, with no error
// line and no error status, and the failure is remembered for the session.
func TestADeclarationThatCannotBeReadPrintsTheDocumentAsItAlwaysHas(t *testing.T) {
	for name, failure := range map[string]error{
		"an older server":  errors.New("server error (1): unknown opcode 0xD9"),
		"no read grant":    errors.New("server error (3): permission denied"),
		"a missing bucket": errors.New("bucket not found"),
		"a timeout":        context.DeadlineExceeded,
	} {
		t.Run(name, func(t *testing.T) {
			c := terminalSession(t)
			c.handler.declarationSource = func(string, string) (*server.BucketInfoResponse, error) { return nil, failure }
			var chatter strings.Builder
			c.handler.SetChatter(&chatter)

			for range 3 {
				printed, err := runStatement(t, c, `db.Post.find()`)
				if err != nil {
					t.Fatalf("a failed declaration failed the read: %v", err)
				}
				if strings.TrimSpace(printed) != plainPostLine {
					t.Fatalf("printed\n%s\nwant the undecorated\n%s", printed, plainPostLine)
				}
			}
			if chatter.Len() != 0 {
				t.Fatalf("a failed declaration said something: %q", chatter.String())
			}
			if c.handler.declarationFetches != 1 {
				t.Fatalf("three reads made %d declaration calls, want the failure remembered after 1", c.handler.declarationFetches)
			}
		})
	}
}

// TestAnEngineWordTheCLIDoesNotKnowPrintsAsTheServerSaidIt: a newer server may
// name an engine this CLI has no table row for. It prints the bare word the
// server sent: no panic, no error line, and the document is still decorated.
func TestAnEngineWordTheCLIDoesNotKnowPrintsAsTheServerSaidIt(t *testing.T) {
	c := terminalSession(t)
	c.handler.declarationSource = func(string, string) (*server.BucketInfoResponse, error) {
		return &server.BucketInfoResponse{Found: true, Bucket: server.BucketInfo{Type: server.BucketTypeHash},
			StoreFields: []server.StoreFieldInfo{{Name: "Photos", Engine: "lifo"}, {Name: "Orbit", Engine: "quasar"}}}, nil
	}
	printed, err := runStatement(t, c, `db.Post.get("p1")`)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	want := `{"_id_": "p1", "Email": "susan@smith.com", "FirstName": "Susan", "Photos": "lifoStore", "Orbit": "quasar"}`
	if strings.TrimSpace(printed) != want {
		t.Fatalf("get printed\n%s\nwant\n%s", printed, want)
	}
}

// TestTheEngineWordGainsTheStoreSuffixFromTheTable: the wire carries hash,
// btree, fifo, lifo, heap, blob; the display spells them as a declaration does.
func TestTheEngineWordGainsTheStoreSuffixFromTheTable(t *testing.T) {
	for _, row := range []struct{ wire, shown string }{
		{"hash", "hashStore"}, {"btree", "btreeStore"}, {"fifo", "fifoStore"},
		{"lifo", "lifoStore"}, {"heap", "heapStore"}, {"blob", "blobStore"}, {"quasar", "quasar"},
	} {
		if got := engineDisplayWord(row.wire); got != row.shown {
			t.Errorf("engineDisplayWord(%q) = %q, want %q", row.wire, got, row.shown)
		}
	}
}

// TestASessionWhoseOutputIsNotATerminalNeverDecorates wires the gate to the
// session: a buffer is not a terminal, so the display stays off whatever the
// test process's own stdin is.
func TestASessionWhoseOutputIsNotATerminalNeverDecorates(t *testing.T) {
	var output strings.Builder
	session := NewCLI(WithTerminal(&output))
	if session.handler.decorate {
		t.Fatal("a session writing to a buffer turned the document display on")
	}
}
