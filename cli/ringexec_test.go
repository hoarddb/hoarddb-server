package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

func u52Session() (*RingSession, *u52Fake) {
	fake := newU52Fake()
	session := NewRingSession(fake)
	session.mintID = func() (string, error) { return "minted-id", nil }
	return session, fake
}

func u52Want(t *testing.T, got string, want ...string) {
	t.Helper()
	if expected := strings.Join(want, "\n") + "\n"; got != expected {
		t.Fatalf("printed:\n%s\nwant:\n%s", got, expected)
	}
}

// TestCreateOutputLines pins the lines of create database and create bucket to queries 5 and 8.
func TestCreateOutputLines(t *testing.T) {
	session, _ := u52Session()
	got, err := u52Run(session, "create database app", "create bucket User in app { Name string, Purchases fifoStore }")
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got,
		"OK  created database app (replication factor 1, this node's default)",
		"Next: use app",
		"Then: create bucket <name> { Name string, Purchases fifoStore }   (declare every field)",
		"OK  created bucket app.User (2 fields)",
		"Fields: Name string, Purchases fifoStore")
	got, err = u52Run(session, "create database shop { replication_factor: 2 }")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, "OK  created database shop (replication factor 2)\n") {
		t.Fatalf("explicit factor: %q", got)
	}
}

// TestFirstFiveMinutesSingleNode runs the single-node part of queries 8 and compares it with the spec text.
func TestFirstFiveMinutesSingleNode(t *testing.T) {
	session, _ := u52Session()
	got, err := u52Run(session,
		"create database app",
		"use app",
		"create bucket User { Name string, Purchases fifoStore }",
		`db.User.insert({_id_: "ada", Name: "Ada"})`,
		`db.User["ada"].Purchases.push({Item: "coffee"})`,
		"db.User.list()",
		"db.User.count()")
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got,
		"OK  created database app (replication factor 1, this node's default)",
		"Next: use app",
		"Then: create bucket <name> { Name string, Purchases fifoStore }   (declare every field)",
		"Switched to database 'app'",
		"OK  created bucket app.User (2 fields)",
		"Fields: Name string, Purchases fifoStore",
		"OK  id=ada",
		"OK  pushed: Purchases now holds 1 element",
		`{"Name": "Ada", "Purchases": "fifoStore (1 element)", "_id_": "ada"}`,
		"1")
}

func TestDescribeAndShowBucketsPage(t *testing.T) {
	session, _ := u52Session()
	if _, err := u52Run(session, "create database app", "use app",
		"create bucket User { Name string }", "create bucket Post { Title string }"); err != nil {
		t.Fatal(err)
	}
	got, err := u52Run(session, "describe bucket User")
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got, "BUCKET app.User", "---", "Id:                  1f4a2c9b7de30015", "State:               active",
		"Declaration version: 1", "Fields:              (in declaration order)", "  Name         string")

	got, err = u52Run(session, "show buckets limit 1")
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got, "BUCKETS in app", "---", "NAME  ID                STATE", "User  1f4a2c9b7de30015  active", "more: show buckets in app limit 1 cursor 01")
	got, err = u52Run(session, "show buckets in app limit 1 cursor 01")
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got, "BUCKETS in app", "---", "NAME  ID                STATE", "Post  1f4a2c9b7de30015  active")
}

func TestShowBucketsEmpty(t *testing.T) {
	session, _ := u52Session()
	if _, err := u52Run(session, "create database app"); err != nil {
		t.Fatal(err)
	}
	got, err := u52Run(session, "show buckets in app")
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got, "BUCKETS in app", "---", "(no buckets yet — create one with 'create bucket <name> in app { Name string }')")
}

func TestUseChecksTheDatabaseAndFillsIn(t *testing.T) {
	session, _ := u52Session()
	_, err := u52Run(session, "use nowhere")
	var failed *RingError
	if !errors.As(err, &failed) || failed.Code != "NOT_FOUND" || session.Selected() != "" {
		t.Fatalf("use of a missing database: %v selected=%q", err, session.Selected())
	}
	_, err = u52Run(session, "db.User.count()")
	if !errors.As(err, &failed) || failed.Code != "PARSE_ERROR" || !strings.Contains(failed.Message, "use <db>") {
		t.Fatalf("no database: %v", err)
	}
	if _, err := u52Run(session, "create database app", "use app"); err != nil || session.Selected() != "app" {
		t.Fatalf("use: %v selected=%q", err, session.Selected())
	}
}

func TestDocumentStatements(t *testing.T) {
	session, _ := u52Session()
	if _, err := u52Run(session, "create database app", "use app", "create bucket User { Name string, Age int }"); err != nil {
		t.Fatal(err)
	}
	got, err := u52Run(session,
		`db.User.insert({Name: "Grace", Age: 40})`,
		`db.User["minted-id"].get()`,
		`db.User.upsert({_id_: "minted-id", Name: "Grace H", Age: 41})`,
		`db.User.update({_id_: "minted-id", $set: {Age: 42}})`,
		`db.User["minted-id"].get()`,
		`db.User.delete({_id_: "minted-id"})`,
		`db.User.count()`)
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got,
		"OK  id=minted-id (no _id_ was given, so one was made)",
		`{"Age": 40, "Name": "Grace", "_id_": "minted-id"}`,
		"OK  id=minted-id",
		"OK  updated id=minted-id",
		`{"Age": 42, "Name": "Grace H", "_id_": "minted-id"}`,
		"OK  deleted id=minted-id",
		"0")
}

func TestDocumentErrorsExplainTheFix(t *testing.T) {
	session, _ := u52Session()
	if _, err := u52Run(session, "create database app", "use app", "create bucket User { Name string }",
		`db.User.insert({_id_: "ada", Name: "Ada"})`); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		statement, code, text string
		exit                  int
	}{
		{`db.User["nobody"].get()`, "NOT_FOUND", `document "nobody" is not in app.User. Run 'db.User.list({limit: 10})' to see what exists, or 'db.User.insert({_id_: "nobody", <field>: <value>})' to create it`, 2},
		{`db.User.insert({_id_: "ada", Name: "Again"})`, "DOCUMENT_EXISTS", "upsert", 2},
		{`db.Nope.count()`, "NOT_FOUND", "show buckets in app", 2},
		{`db.User.update({_id_: "ada"})`, "PARSE_ERROR", "$set", 2},
		{`db.User.insert({_id_: 5, Name: "x"})`, "PARSE_ERROR", `db.User.insert({_id_: "5"`, 2},
	}
	for _, c := range cases {
		_, err := u52Run(session, c.statement)
		var failed *RingError
		if !errors.As(err, &failed) {
			t.Fatalf("%s: %v", c.statement, err)
		}
		if failed.Code != c.code || failed.Exit != c.exit || !strings.Contains(failed.Message, c.text) {
			t.Fatalf("%s: code=%q exit=%d message=%q", c.statement, failed.Code, failed.Exit, failed.Message)
		}
	}
}

func TestStoreVerbs(t *testing.T) {
	session, _ := u52Session()
	if _, err := u52Run(session, "create database app", "use app",
		"create bucket User { Name string, Purchases fifoStore, Friends hashStore }",
		`db.User.insert({_id_: "ada", Name: "Ada"})`); err != nil {
		t.Fatal(err)
	}
	got, err := u52Run(session,
		`db.User["ada"].Purchases.push({item: "a"})`,
		`db.User["ada"].Purchases.push({item: "b"})`,
		`db.User["ada"].Purchases.peek()`,
		`db.User["ada"].Purchases.list({limit: 1})`,
		`db.User["ada"].Purchases.count()`,
		`db.User["ada"].Purchases.pop()`,
		`db.User["ada"].Friends.put("grace", {since: 2024})`,
		`db.User["ada"].Friends.take("grace")`)
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got,
		"OK  pushed: Purchases now holds 1 element",
		"OK  pushed: Purchases now holds 2 elements",
		`{"item": "a"}`,
		`{"item": "a"}`,
		`more: db.User["ada"].Purchases.list({limit: 1, cursor: "01"})`,
		"2",
		`{"item": "a"}`,
		`OK  put "grace": Friends now holds 1 element`,
		`{"since": 2024}`)
	_, err = u52Run(session, `db.User["ada"].Purchases.pop()`, `db.User["ada"].Purchases.pop()`)
	var failed *RingError
	if !errors.As(err, &failed) || failed.Exit != 2 || !strings.Contains(failed.Message, `Purchases of "ada" in app.User is empty`) {
		t.Fatalf("pop of an empty store: %v", err)
	}
	got, err = u52Run(session, `db.User["ada"].Purchases.peek()`)
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got, "(empty)")
}

func TestLeaseVerbs(t *testing.T) {
	session, _ := u52Session()
	if _, err := u52Run(session, "create database app", "use app", "create bucket Job { Name string, Queue fifoStore }",
		`db.Job.insert({_id_: "j", Name: "n"})`, `db.Job["j"].Queue.push({task: 1})`); err != nil {
		t.Fatal(err)
	}
	got, err := u52Run(session, `db.Job["j"].Queue.claim(1)`)
	if err != nil {
		t.Fatal(err)
	}
	lease := "aabb0000000000000000000000000001"
	u52Want(t, got, "lease "+lease, `{"task": 1}`)
	got, err = u52Run(session,
		fmt.Sprintf(`db.Job["j"].Queue.get("%s")`, lease),
		fmt.Sprintf(`db.Job["j"].Queue.extend("%s", 5000)`, lease),
		fmt.Sprintf(`db.Job["j"].Queue.ack("%s")`, lease),
		fmt.Sprintf(`db.Job["j"].Queue.release("%s")`, lease))
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got, `{"task": 1}`, "OK  extended lease "+lease+" to 5000 ms", "OK  acked lease "+lease,
		"lease "+lease+" is not live (it was acked, released or has expired), so nothing was released")
	got, err = u52Run(session, `db.Job["j"].Queue.claim(1)`)
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got, "(nothing to claim: Queue is empty)")
	_, err = u52Run(session, `db.Job["j"].Queue.ack("zz")`)
	var failed *RingError
	if !errors.As(err, &failed) || failed.Code != "PARSE_ERROR" || !strings.Contains(failed.Message, "32 hex") {
		t.Fatalf("bad lease id: %v", err)
	}
}

func TestHeapAndBlobAreRefusedNotPanics(t *testing.T) {
	session, _ := u52Session()
	if _, err := u52Run(session, "create database app", "use app", "create bucket User { Name string, Books heapStore, Data blobStore, Recipes btreeStore }",
		`db.User.insert({_id_: "ada", Name: "Ada"})`); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{`db.User["ada"].Books.push({t: 1})`, `db.User["ada"].Books.take("k")`, `db.User["ada"].Data.put("k", {a: 1})`} {
		_, err := u52Run(session, statement)
		var failed *RingError
		if !errors.As(err, &failed) || failed.Exit != 2 || !strings.Contains(failed.Message, "not available") {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	_, err := u52Run(session, `db.User["ada"].Nope.push({a: 1})`)
	var failed *RingError
	if !errors.As(err, &failed) || !strings.Contains(failed.Message, "describe bucket User in app") {
		t.Fatalf("undeclared store field: %v", err)
	}
}

func TestListFilterLimitAndCursorHint(t *testing.T) {
	session, _ := u52Session()
	if _, err := u52Run(session, "create database app", "use app", "create bucket User { Name string }",
		`db.User.insert({_id_: "a", Name: "Ada"})`, `db.User.insert({_id_: "b", Name: "Ada"})`, `db.User.insert({_id_: "c", Name: "Bob"})`); err != nil {
		t.Fatal(err)
	}
	got, err := u52Run(session, `db.User.list({filter: {Name: "Ada"}, limit: 1})`, `db.User.count({filter: {Name: "Ada"}})`)
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got, `{"Name": "Ada", "_id_": "a"}`, `more: db.User.list({filter: {Name: "Ada"}, limit: 1, cursor: "01"})`, "2")
	got, err = u52Run(session, `db.User.list({filter: {Name: "Zed"}})`)
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got, "(no documents)")
}

// TestExitStatusPerCode: every ring code exits with cluster.Code.Exit, a parse error with 2,
// and a lost connection with 8.
func TestExitStatusPerCode(t *testing.T) {
	for _, code := range cluster.Codes() {
		session, fake := u52Session()
		fake.failures["CreateDatabaseWithStorage"] = &ringclient.Error{Code: code, Message: "server text names the fix"}
		_, err := u52Run(session, "create database app")
		var failed *RingError
		if !errors.As(err, &failed) {
			t.Fatalf("%s: %v", code, err)
		}
		if failed.Exit != code.Exit() || failed.Code != code.String() || failed.Message != "server text names the fix" {
			t.Fatalf("%s: exit %d (want %d) code %q message %q", code, failed.Exit, code.Exit(), failed.Code, failed.Message)
		}
	}
	session, fake := u52Session()
	fake.failures["CreateDatabaseWithStorage"] = fmt.Errorf("ringclient: the connection to x ended: %w", io.ErrUnexpectedEOF)
	_, err := u52Run(session, "create database app")
	var failed *RingError
	if !errors.As(err, &failed) || failed.Exit != ExitUnavailable {
		t.Fatalf("lost connection: %v", err)
	}
	_, err = u52Run(session, "create databse app")
	if !errors.As(err, &failed) || failed.Exit != 2 || failed.Code != "PARSE_ERROR" {
		t.Fatalf("parse error: %v", err)
	}
}

// nodes and status ran nothing in ring mode until 0e699a1c5, so they left this list; the seeding
// statements left it when S4 installed the seeding view.
func TestUnsupportedStatementsSayWhy(t *testing.T) {
	session, _ := u52Session()
	for _, statement := range []string{"drop database app", "alter database app { replication_factor: 2 }"} {
		_, err := u52Run(session, statement)
		var failed *RingError
		if !errors.As(err, &failed) || failed.Exit != 2 || !strings.Contains(failed.Message, "not available yet") {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

func TestLegacyStatementInRingModeIsExplained(t *testing.T) {
	session, _ := u52Session()
	_, err := u52Run(session, "show databases in app")
	var failed *RingError
	if !errors.As(err, &failed) {
		t.Fatal(err)
	}
	_, err = u52Run(session, `db.users.find({})`)
	if !errors.As(err, &failed) || !strings.Contains(failed.Message, "the CLI does not run this; 'help' lists the statements it runs") {
		t.Fatalf("legacy statement: %v", err)
	}
	_, err = u52Run(session, "create databse app")
	if !errors.As(err, &failed) || strings.Contains(failed.Message, "the CLI does not run this") {
		t.Fatalf("a typo is not a legacy statement: %v", err)
	}
}

func TestHelpIsTheRingGrammar(t *testing.T) {
	session, _ := u52Session()
	got, err := u52Run(session, "help")
	if err != nil || !strings.Contains(got, "create bucket") || strings.Contains(got, "ring mode") {
		t.Fatalf("help: %v\n%s", err, got)
	}
	got, err = u52Run(session, "help all")
	if err != nil || !strings.Contains(got, "create bucket") || !strings.Contains(got, "Not available yet") {
		t.Fatalf("help all: %v\n%s", err, got)
	}
	got, err = u52Run(session, "help push")
	if err != nil || !strings.Contains(got, "Purchases.push") {
		t.Fatalf("help push: %v\n%s", err, got)
	}
}

// TestSessionCancelsLongCalls: Execute passes its context to the client.
func TestExecuteHonoursContext(t *testing.T) {
	session, fake := u52Session()
	fake.failures["CreateDatabaseWithStorage"] = context.Canceled
	_, err := u52Run(session, "create database app")
	var failed *RingError
	if !errors.As(err, &failed) || failed.Exit != ExitUnavailable {
		t.Fatalf("canceled: %v", err)
	}
}

// TestU80DescribeBucketAlignsTypesToTheLongestFieldName: a name longer than the old fixed column pushed its type out of line.
func TestU80DescribeBucketAlignsTypesToTheLongestFieldName(t *testing.T) {
	session, _ := u52Session()
	got, err := u52Run(session, "create database app", "create bucket Profile in app { Name string, FollowingCount int }", "describe bucket Profile in app")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(got, "  Name           string\n  FollowingCount int\n") {
		t.Fatalf("columns:\n%s", got)
	}
}

// TestAlterBucketOutputLines pins the lines of alter bucket to queries 5: the count, what was
// added with its type, what was dropped, and that a repeat says nothing changed.
func TestAlterBucketOutputLines(t *testing.T) {
	session, fake := u52Session()
	if _, err := u52Run(session, "create database app", "create bucket User in app { Name string, Age int, Purchases fifoStore }"); err != nil {
		t.Fatal(err)
	}
	got, err := u52Run(session, "alter bucket User in app { add Email string, drop Age }")
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got,
		"OK  altered bucket app.User (3 fields)",
		"Added: Email string",
		"Dropped: Age (its values are deleted in the background)")
	got, err = u52Run(session, "alter bucket User in app { add Email string }")
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got, "OK  bucket app.User already has these fields (3 fields), so nothing changed")
	if version := fake.buckets["app.User"].info.DeclVersion; version != 2 {
		t.Fatalf("declaration version = %d, want 2: a repeat does not move it", version)
	}
	got, err = u52Run(session, "describe bucket User in app")
	if err != nil || !strings.Contains(got, "Email") || strings.Contains(got, "Age") {
		t.Fatalf("describe after the alter: %v\n%s", err, got)
	}
}

// TestAlterBucketRefusalsNameTheFix: a type change is the server's refusal with its exit status,
// a drop of an undeclared name never reaches the server, and a missing bucket is the server's NOT_FOUND.
func TestAlterBucketRefusalsNameTheFix(t *testing.T) {
	session, fake := u52Session()
	if _, err := u52Run(session, "create database app", "use app", "create bucket User { Name string }"); err != nil {
		t.Fatal(err)
	}
	var failed *RingError
	_, err := u52Run(session, "alter bucket User { add Name int }")
	if !errors.As(err, &failed) || failed.Code != "INVALID_ARGUMENT" || failed.Exit != ExitRefused || !strings.Contains(failed.Message, "a declared type never changes") {
		t.Fatalf("type change: %v", err)
	}
	calls := len(fake.calls)
	_, err = u52Run(session, "alter bucket User { drop Nmae }")
	if !errors.As(err, &failed) || failed.Exit != ExitRefused ||
		!strings.Contains(failed.Message, `field "Nmae" is not declared in app.User`) || !strings.Contains(failed.Message, "describe bucket User in app") {
		t.Fatalf("drop of a typo: %v", err)
	}
	for _, call := range fake.calls[calls:] {
		if call == "AlterBucket" {
			t.Fatal("a drop of an undeclared name was sent to the server")
		}
	}
	_, err = u52Run(session, "alter bucket Nope { add A string }")
	if !errors.As(err, &failed) || failed.Code != "NOT_FOUND" {
		t.Fatalf("missing bucket: %v", err)
	}
}

// TestAMixedAlterRunAgainIsANoOp: "run the same alter again to finish it" holds for an alter that
// both adds and drops: after it completed, the repeat is answered, not refused for the field it dropped.
func TestAMixedAlterRunAgainIsANoOp(t *testing.T) {
	session, fake := u52Session()
	if _, err := u52Run(session, "create database app", "create bucket User in app { Name string, Age int }"); err != nil {
		t.Fatal(err)
	}
	if _, err := u52Run(session, "alter bucket User in app { add Email string, drop Age }"); err != nil {
		t.Fatal(err)
	}
	got, err := u52Run(session, "alter bucket User in app { add Email string, drop Age }")
	if err != nil {
		t.Fatalf("the repeat of a finished mixed alter: %v", err)
	}
	u52Want(t, got, "OK  bucket app.User already has these fields (2 fields), so nothing changed")
	if version := fake.buckets["app.User"].info.DeclVersion; version != 2 {
		t.Fatalf("declaration version = %d, want 2", version)
	}
}

// TestCreateDatabaseWithStorageMemoryIsRefusedByTheCLI: the CLI refuses storage memory with the
// rule 3 sentence before it calls the API, and storage disk and the default still create.
func TestCreateDatabaseWithStorageMemoryIsRefusedByTheCLI(t *testing.T) {
	const want = "storage: memory is not available yet: this build keeps every database on disk, so a memory database would not be lost on restart as its name says. Create it with storage: disk (the default): create database cache"
	for _, statement := range []string{"create database cache { storage: memory }", "create database cache { replication_factor: 2, storage: memory }"} {
		session, fake := u52Session()
		_, err := u52Run(session, statement)
		var failed *RingError
		if !errors.As(err, &failed) || failed.Exit != ExitRefused || failed.Message != want {
			t.Fatalf("%s: %v, want a refusal with %q", statement, err, want)
		}
		if len(fake.calls) != 0 {
			t.Fatalf("%s: the API was called: %v", statement, fake.calls)
		}
	}
	session, fake := u52Session()
	got, err := u52Run(session, "create database plain", "create database onDisk { storage: disk }")
	if err != nil {
		t.Fatal(err)
	}
	if fake.storages["plain"] != "disk" || fake.storages["onDisk"] != "disk" {
		t.Fatalf("storage sent: %v, want disk for both", fake.storages)
	}
	if !strings.Contains(got, "OK  created database plain (replication factor 1, this node's default)") {
		t.Fatalf("disk create reply: %q", got)
	}
}

// TestAlterDatabaseStorageIsRefusedWithTheDumpRestoreSentence: a storage member of alter database
// is refused with the sentence the spec words, for memory and for disk alike, and sends nothing.
func TestAlterDatabaseStorageIsRefusedWithTheDumpRestoreSentence(t *testing.T) {
	want := `storage of database "app" cannot be changed: its data lives in a different store. ` +
		`Create a new database with 'create database <name> { storage: disk }', then move the data with dump and restore`
	for _, statement := range []string{"alter database app { storage: memory }", "alter database app { storage: disk }"} {
		session, fake := u52Session()
		_, err := u52Run(session, statement)
		var failed *RingError
		if !errors.As(err, &failed) || failed.Exit != ExitRefused {
			t.Fatalf("%s: %v, want a refusal", statement, err)
		}
		if failed.Message != want {
			t.Fatalf("%s: message %q, want %q", statement, failed.Message, want)
		}
		if len(fake.calls) != 0 {
			t.Fatalf("%s: the API was called: %v", statement, fake.calls)
		}
	}
}

// ringFactValue returns the value of the fact line that starts with label, or fails the test.
func ringFactValue(t *testing.T, lines []string, label string) string {
	t.Helper()
	for _, line := range lines {
		if rest, ok := strings.CutPrefix(line, label+":"); ok {
			return strings.TrimSpace(rest)
		}
	}
	t.Fatalf("no %q fact in %q", label, lines)
	return ""
}

// TestDescribeDatabasePrintsStorage: a disk database says disk and prints no memory figures; a memory
// database says memory and prints the node's used bytes and cap, labelled as shared by the node.
func TestDescribeDatabasePrintsStorage(t *testing.T) {
	session, fake := u52Session()
	fake.memoryUsed, fake.memoryCap = 1048576, 268435456
	if _, err := u52Run(session, "create database plain"); err != nil {
		t.Fatal(err)
	}
	// An existing memory database (a test cluster, an older build) is seeded behind the CLI: the CLI no longer creates one.
	if _, err := fake.createDatabase("cache", 1, "memory"); err != nil {
		t.Fatal(err)
	}
	got, err := u52Run(session, "describe database plain")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(got, "\n"), "\n")
	if value := ringFactValue(t, lines, "Storage"); value != "disk" {
		t.Fatalf("plain storage: %q, want disk", value)
	}
	for _, line := range lines {
		if strings.HasPrefix(line, "Memory store") {
			t.Fatalf("a disk database prints a memory figure: %q", line)
		}
	}
	got, err = u52Run(session, "describe database cache")
	if err != nil {
		t.Fatal(err)
	}
	lines = strings.Split(strings.TrimRight(got, "\n"), "\n")
	if value := ringFactValue(t, lines, "Storage"); value != "memory" {
		t.Fatalf("cache storage: %q, want memory", value)
	}
	want := "1048576 bytes of 268435456, shared by every memory database on this node"
	if value := ringFactValue(t, lines, "Memory store (this node)"); value != want {
		t.Fatalf("cache memory store: %q, want %q", value, want)
	}
}

// TestCreateDatabaseRefusesAReplicationFactorThatDoesNotFitInt32 pins BUG-0784. A factor the wire's
// int32 cannot hold was narrowed silently (4294967297 became 1), and an explicit 0 was sent as "omit"
// and took the server default. Each refused value names the range and creates nothing.
func TestCreateDatabaseRefusesAReplicationFactorThatDoesNotFitInt32(t *testing.T) {
	for _, value := range []string{"0", "-1", "2147483648", "4294967296", "4294967297"} {
		session, fake := u52Session()
		_, err := u52Run(session, "create database app { replication_factor: "+value+" }")
		want := "replication_factor " + value + " is out of range: write a whole number from 1 to 2147483647"
		if value == "-1" {
			// The options parser refuses a negative before the range check runs; both name the fix.
			want = `"-1" is not a number: write a whole number such as 10`
		}
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("replication_factor %s: err = %v, want it to say %q", value, err, want)
		}
		if len(fake.databases) != 0 {
			t.Errorf("replication_factor %s: a database was created anyway: %v", value, fake.databases)
		}
	}
	for _, value := range []string{"1", "2147483647"} {
		session, _ := u52Session()
		got, err := u52Run(session, "create database app { replication_factor: "+value+" }")
		if err != nil {
			t.Fatalf("replication_factor %s refused: %v", value, err)
		}
		if !strings.Contains(got, "(replication factor "+value+")") {
			t.Fatalf("replication_factor %s printed %q", value, got)
		}
	}
}

// TestCreateAndAlterBucketRefuseIndexesBeforeAnyServerCall: the grammar parses
// 'index ...', but the ring wire does not build it, so the statement must refuse instead of
// printing OK and dropping it.
func TestCreateAndAlterBucketRefuseIndexesBeforeAnyServerCall(t *testing.T) {
	for _, statement := range []string{
		"create bucket Ledger in app { Total double, index ByTotal (Total desc) }",
		"alter bucket Ledger in app { add index ByTotal2 (Total) }",
		"alter bucket Ledger in app { drop index ByTotal }",
	} {
		session, fake := u52Session()
		fake.calls = nil
		got, err := u52Run(session, statement)
		if err == nil {
			t.Errorf("%q printed %q and no error; it must be refused", statement, got)
			continue
		}
		for _, want := range []string{"Indexes are parsed but not built in this build; nothing was", "without the index entry"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%q: refusal %q lacks %q", statement, err.Error(), want)
			}
		}
		if len(fake.calls) != 0 {
			t.Errorf("%q reached the server: %v", statement, fake.calls)
		}
	}
}

// TestOfDeclarationRoundTripsAndDescribes: [GUARD the of word reaches the wire and describe prints it back].
func TestOfDeclarationRoundTripsAndDescribes(t *testing.T) {
	session, _ := u52Session()
	got, err := u52Run(session, "create database app", "create bucket User in app { Name string }",
		"create bucket Room in app { Name string, Members hashStore of User, Owner string of ref.User }")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "Fields: Name string, Members hashStore of User, Owner string of ref.User") {
		t.Fatalf("create printed:\n%s", got)
	}
}

// TestOfNeedsABucketName: [GUARD a dangling of is a parse error that says what to write].
func TestOfNeedsABucketName(t *testing.T) {
	session, _ := u52Session()
	_, err := u52Run(session, "create database app", "create bucket Room in app { Members hashStore of }")
	if err == nil || !strings.Contains(err.Error(), "of User") {
		t.Fatalf("error = %v", err)
	}
}

// expandAPI is the fake API whose buckets can expand: List answers from a fixed page and keeps the
// request it was sent.
type expandAPI struct {
	*u52Fake
	sent *[]ringclient.PageRequest
}

type expandHandle struct {
	RingBucket
	sent *[]ringclient.PageRequest
}

func (a expandAPI) Bucket(database, name string) RingBucket {
	return expandHandle{RingBucket: a.u52Fake.Bucket(database, name), sent: a.sent}
}

func expandedAnswer(label string, found bool, name string) bson.Raw {
	inner := bson.D{{Key: "expanded", Value: found}}
	if found {
		inner = append(inner, bson.E{Key: "doc", Value: bson.D{{Key: "_id_", Value: "ada"}, {Key: "Name", Value: name}}})
	} else {
		inner = append(inner, bson.E{Key: "reason", Value: "missing"})
	}
	raw, err := bson.Marshal(bson.D{{Key: label, Value: inner}})
	if err != nil {
		panic(err) // a test fixture of two fixed documents
	}
	return raw
}

func (h expandHandle) List(_ context.Context, _ any, page ringclient.PageRequest) (ringclient.EntryPage, error) {
	*h.sent = append(*h.sent, page)
	first, _ := bson.Marshal(bson.D{{Key: "_id_", Value: "m1"}, {Key: "Author", Value: "ada"}})
	second, _ := bson.Marshal(bson.D{{Key: "_id_", Value: "m2"}, {Key: "Author", Value: "ghost"}})
	return ringclient.EntryPage{Entries: []ringclient.Entry{{ID: "m1", Body: first}, {ID: "m2", Body: second}},
		Expanded: []bson.Raw{expandedAnswer("Author", true, "Ada"), expandedAnswer("Author", false, "")}, Served: "owner"}, nil
}

// TestListExpandPrintsAnExpandedAndAMissingRow: [GUARD a gone user reads (not found), never an empty object].
func TestListExpandPrintsAnExpandedAndAMissingRow(t *testing.T) {
	var sent []ringclient.PageRequest
	session := NewRingSession(expandAPI{u52Fake: newU52Fake(), sent: &sent})
	got, err := u52Run(session, "create database app", "use app", "create bucket Message { Author string, Text string }",
		`db.Message.list({expand: {Author: "User"}, local: true})`)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`  Author -> {"Name": "Ada", "_id_": "ada"}`, "  Author -> (not found)", "served: owner"} {
		if !strings.Contains(got, want) {
			t.Errorf("the output has no %q:\n%s", want, got)
		}
	}
	if len(sent) != 1 || !sent[0].Local || len(sent[0].Expand) != 1 || sent[0].Expand[0].Field != "Author" || sent[0].Expand[0].Bucket != "User" {
		t.Fatalf("the list request was %+v", sent)
	}
}

// TestExpandRefusalsSayWhatToWrite: [GUARD a bad expand is a parse error with a statement that works].
func TestExpandRefusalsSayWhatToWrite(t *testing.T) {
	var sent []ringclient.PageRequest
	session := NewRingSession(expandAPI{u52Fake: newU52Fake(), sent: &sent})
	for _, statement := range []string{`db.Message.list({expand: 5})`, `db.Message.list({expand: {Author: false}})`, `db.Message.list({where: {Name: "Ada"}})`} {
		_, err := u52Run(session, "create database app", "use app", statement)
		if err == nil || !strings.Contains(err.Error(), "Run '") {
			t.Errorf("%s: error = %v", statement, err)
		}
	}
	if len(sent) != 0 {
		t.Fatal("a refused statement reached the server")
	}
}

// TestExpandTextWritesTheOptionsBackForAMoreLine: [GUARD the more: hint keeps fields and bucket,
// so following it reads the same expansion].
func TestExpandTextWritesTheOptionsBackForAMoreLine(t *testing.T) {
	for want, request := range map[string]ringclient.ExpandRequest{
		"true":               {},
		`"User"`:             {Bucket: "User"},
		`"ref.User"`:         {Database: "ref", Bucket: "User"},
		`{fields: ["Name"]}`: {Fields: []string{"Name"}},
		`{bucket: "ref.User", fields: ["Name", "Age"]}`: {Database: "ref", Bucket: "User", Fields: []string{"Name", "Age"}},
	} {
		if got := ringExpandOneText(request); got != want {
			t.Errorf("ringExpandOneText(%+v) = %s, want %s", request, got, want)
		}
	}
	if got := ringExpandText([]ringclient.ExpandRequest{{Field: "Author", Fields: []string{"Name"}}}); got != `{Author: {fields: ["Name"]}}` {
		t.Errorf("ringExpandText = %s", got)
	}
}

// TestDescribePrintsTheWithClause: a store field that keeps aggregates prints them in the
// create grammar, after its type, so the line parses back.
func TestDescribePrintsTheWithClause(t *testing.T) {
	session, _ := u52Session()
	got, err := u52Run(session, "create database app", "use app",
		"create bucket Property in app { Name string, Expenses btreeStore with (sum Amount, sum Amount by Currency, count by Category), Tickets fifoStore }",
		"describe bucket Property in app")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"  Expenses     btreeStore with (sum Amount, sum Amount by Currency, count by Category)",
		"  Tickets      fifoStore\n",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("describe printed:\n%s\nwant a line %q", got, want)
		}
	}
}

// TestSumVerbsParseAndRunOnTheFakeSession: sum, sum with by, count with by and describe print what
// the store keeps, and an aggregate the bucket did not declare is refused with its name.
func TestSumVerbsParseAndRunOnTheFakeSession(t *testing.T) {
	session, _ := u52Session()
	got, err := u52Run(session, "create database app", "use app",
		"create bucket Property in app { Name string, Expenses hashStore with (sum Amount, sum Amount by Currency, count by Currency) }",
		`db.Property.insert({_id_: "p1", Name: "Maple"})`,
		`db.Property["p1"].Expenses.put("e1", {Amount: 100, Currency: "USD"})`,
		`db.Property["p1"].Expenses.put("e2", {Amount: 50, Currency: "EUR"})`,
		`db.Property["p1"].Expenses.put("e3", {Amount: 25, Currency: "USD"})`,
		`db.Property["p1"].Expenses.sum("Amount")`,
		`db.Property["p1"].Expenses.sum("Amount", {by: "Currency"})`,
		`db.Property["p1"].Expenses.count({by: "Currency"})`,
		`db.Property["p1"].Expenses.describe()`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Execute(context.Background(), `db.Property["p1"].Expenses.sum("Amout")`); err == nil || !strings.Contains(err.Error(), "AGGREGATE_NOT_DECLARED") {
		t.Fatalf("a typo'd sum was refused with %v, want AGGREGATE_NOT_DECLARED", err)
	}
	for _, want := range []string{"175\n", "EUR  50  1 element(s)\nUSD  125  2 element(s)\n", "EUR  1 element(s)\nUSD  2 element(s)\n", "sum Amount by Currency  ready"} {
		if !strings.Contains(got, want) {
			t.Fatalf("output:\n%s\nwant it to contain %q", got, want)
		}
	}
	if _, err := session.Execute(context.Background(), `db.Property["p1"].Expenses.sum("Amount", {by: "Currency"}, 1)`); err == nil {
		t.Fatal("a third argument was accepted")
	}
	if _, err := session.Execute(context.Background(), `db.Property["p1"].Expenses.count({})`); err == nil {
		t.Fatal("count({}) with no by was accepted")
	}
}

// TestAggregatesWorkEndToEndOnARealServer: create with aggregates, write, and read the sums
// back through the CLI, the client and the server's own apply.
func TestAggregatesWorkEndToEndOnARealServer(t *testing.T) {
	session, ctx := u52LiveSession(t)
	for _, statement := range []string{
		"create database app", "use app",
		"create bucket Property { Name string, Expenses btreeStore with (sum Amount, sum Amount by Currency, count by Currency), Tickets fifoStore with (count by Status) }",
		`db.Property.insert({_id_: "p1", Name: "Maple"})`,
		`db.Property["p1"].Expenses.put("e1", {Amount: 100, Currency: "USD"})`,
		`db.Property["p1"].Expenses.put("e2", {Amount: 50, Currency: "EUR"})`,
		`db.Property["p1"].Expenses.put("e1", {Amount: 70, Currency: "USD"})`,
		`db.Property["p1"].Tickets.push({Status: "open"})`,
		`db.Property["p1"].Tickets.push({Status: "open"})`,
		`db.Property["p1"].Tickets.push({Status: "done"})`,
	} {
		if _, err := session.Execute(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	want := map[string]string{
		`db.Property["p1"].Expenses.sum("Amount")`:                   "120",
		`db.Property["p1"].Expenses.sum("Amount", {by: "Currency"})`: "EUR  50  1 element(s)\nUSD  70  1 element(s)",
		`db.Property["p1"].Tickets.count({by: "Status"})`:            "done  1 element(s)\nopen  2 element(s)",
		`db.Property["p1"].Expenses.count({by: "Currency"})`:         "EUR  1 element(s)\nUSD  1 element(s)",
	}
	for statement, expected := range want {
		out, err := session.Execute(ctx, statement)
		if err != nil || strings.Join(out.Lines, "\n") != expected {
			t.Errorf("%s printed %q (%v), want %q", statement, out.Lines, err, expected)
		}
	}
	if _, err := session.Execute(ctx, `db.Property["p1"].Expenses.put("e3", {Name: "no amount"})`); err == nil || !strings.Contains(err.Error(), "AGGREGATE_FIELD") {
		t.Errorf("a put with no Amount was refused with %v, want AGGREGATE_FIELD", err)
	}
	if _, err := session.Execute(ctx, `db.Property["p1"].Expenses.sum("Amout")`); err == nil || !strings.Contains(err.Error(), "AGGREGATE_NOT_DECLARED") {
		t.Errorf("a typo'd sum was refused with %v, want AGGREGATE_NOT_DECLARED", err)
	}
}
