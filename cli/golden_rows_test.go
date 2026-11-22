package cli

import (
	"io"
	"strings"
	"testing"
)

// goldenRow is one route of the build doc's Appendix A: the statements that
// exercise it, run on a fresh server after goldenFixture (and, if given,
// setup, whose output is not recorded).
type goldenRow struct {
	slug       string
	setup      []string
	statements []string
	// extendedJSON runs the row with the --extjson flag on.
	extendedJSON bool
	// prepare, when set, runs after the fixture and before setup, for state a
	// statement cannot reach (a second connection dropping the database).
	prepare func(t *testing.T, session *goldenSession)
}

// goldenRows lists every route of Appendix A that one node can answer. Each
// row records a hit and a miss where the route has both, and the error a
// failing statement prints today. {OUT} in a statement is the row's scratch
// directory.
var goldenRows = []goldenRow{
	// Session and inspection.
	{slug: "global-use", statements: []string{"use app", "use nope", "use"}},
	{slug: "global-show-databases", statements: []string{"show databases"}},
	{slug: "global-show-buckets", statements: []string{"show buckets in app", "show buckets", "show buckets in nope", "show"}},
	{slug: "global-show-users", statements: []string{"show users"}},
	{slug: "global-describe-bucket", statements: []string{
		"describe bucket Users", "describe bucket Post", "describe bucket Emails", "describe bucket Nope", "describe bucket",
	}},
	{slug: "global-describe-database", statements: []string{"describe database app", "describe database nope"}},
	{slug: "global-status", statements: []string{"status"}},
	{slug: "global-version", statements: []string{"version"}},
	{slug: "global-exit", statements: []string{"exit"}},
	{slug: "global-quit", statements: []string{"quit"}},
	{slug: "global-nodes", statements: []string{"nodes"}},
	{slug: "global-master", statements: []string{"master"}},
	{slug: "global-theme", statements: []string{"theme"}},
	{slug: "global-help", statements: []string{"help", "help find"}},
	// The topic texts that name peek: the ones that said there was no peek(N).
	{slug: "help-topics", statements: []string{"help fifo", "help lifo", "help heap", "help fields", "help claim"}},
	{slug: "global-write-concern", statements: []string{"write-concern majority", "write-concern", "write-concern bogus"}},

	// Cluster verbs: one node can only say what it says to a node that is not
	// a cluster member. connect is last because a failed connect closes the
	// session, which the trailing statement records.
	{slug: "global-join", statements: []string{"join 127.0.0.1:2", "join"}},
	{slug: "global-remove-node", statements: []string{"remove-node n2", "remove-node"}},
	{slug: "global-migrate", statements: []string{"migrate app Users", "migrate app"}},
	{slug: "global-consistent-get", setup: []string{`db.Users.insert({"_id_": "solo"})`}, statements: []string{
		"consistent-get app Users solo", "consistent-get app Users zz", "consistent-get app Nope u1", "consistent-get app Users",
	}},
	{slug: "global-connect", statements: []string{"connect", "connect 127.0.0.1:1", "show databases"}},

	// User management.
	{slug: "user-create-user", statements: []string{
		"create user bob roles [reader@app]", "create user bob roles [reader@app]", "user describe bob",
	}},
	{slug: "user-alter-user", setup: []string{"create user bob roles [reader@app]"}, statements: []string{
		"alter user bob roles [writer@app]", "alter user bob disable", "alter user bob enable", "alter user bob password", "alter user nobody disable",
	}},
	{slug: "user-disable-enable-user", setup: []string{"create user bob roles [reader@app]"}, statements: []string{
		"disable user bob", "enable user bob", "disable user nobody",
	}},
	{slug: "user-drop-user", setup: []string{"create user bob roles [reader@app]"}, statements: []string{"drop user bob", "drop user bob"}},
	{slug: "user-grant-revoke", setup: []string{"create user bob roles [reader@app]"}, statements: []string{
		"grant role writer@app to bob", "user describe bob", "revoke role writer@app from bob", "grant access to app for bob",
	}},
	{slug: "user-list-users", setup: []string{"create user bob roles [reader@app]"}, statements: []string{"user list"}},
	{slug: "user-describe-user", setup: []string{"create user bob roles [reader@app]"}, statements: []string{"user describe bob", "user describe nobody"}},

	// Schema statements.
	{slug: "admin-create-database", statements: []string{"create database app2", "create database app"}},
	{slug: "admin-drop-database", setup: []string{"create database app2"}, statements: []string{"drop database app2", "drop database app2"}},
	{slug: "admin-create-bucket", statements: []string{
		"create bucket Scratch { type: hash }", "create bucket Scratch { type: hash }", "create bucket Queue { type: lifo }", "create bucket Bad { type: nope }",
	}},
	{slug: "admin-create-bucket-stores", statements: []string{
		"create bucket Photos { type: hash, Comments fifoStore, Shots blobStore }", "describe bucket Photos",
		"create bucket Plain { type: hash, Caption string }",
	}},
	{slug: "admin-create-bucket-unique", statements: []string{
		"create bucket Emails2 { type: hash, placement: whole, Email string unique }",
		"create bucket Both { type: hash, placement: whole, Email string unique, Name string unique }",
		`db.Emails.insert({"_id_": "e2", "Email": "a@example.com"})`,
		`db.Emails.insert({"_id_": "e3", "Email": "b@example.com"})`,
		"describe bucket Emails",
	}},
	{slug: "admin-alter-bucket", statements: []string{
		"alter bucket Post { Notes lifoStore }", "describe bucket Post",
		"alter bucket Post { Email string unique }", "alter bucket Post { Comments drop }", "alter bucket Nope { Notes lifoStore }",
	}},
	{slug: "admin-drop-bucket", setup: []string{"create bucket Scratch { type: hash }"}, statements: []string{"drop bucket Scratch", "drop bucket Scratch"}},

	// Documents: hash and btree buckets.
	{slug: "bucket-insert", statements: []string{
		`db.Users.insert({"_id_": "u3", "n": 3})`, `db.Users.insert({"n": 4})`, `db.Users.upsert({"_id_": "u3", "n": 30})`,
		`db.Users.insert({"_id_": "u3", "n": 31})`, `db.Users.find("u3")`, `db.Jobs.insert({"a": 1})`, `db.Users.insert(`,
	}},
	{slug: "bucket-find-point", statements: []string{
		`db.Users.find("u1")`, `db.Users.find("zz")`, `db.Users.find({_id_: "u2"})`, `db.Jobs.find("k")`, `db.Nope.find("k")`, `db.Files.find("f1")`,
	}},
	{slug: "bucket-get", statements: []string{
		`db.Users.get("u1")`, `db.Users.get("zz")`, `db.Events.get("e5")`, `db.Events.get("zz")`, `db.Jobs.get("k")`, `db.Tasks.get("k")`, `db.Nope.get("k")`,
	}},
	{slug: "bucket-find-scan", statements: []string{
		`db.Users.find()`, `db.Users.find({"n": {"$gt": 5}}, {limit: 2})`, `db.Users.find({}, {skip: 1, limit: 1})`,
		`db.Users.find({"zzz": 1})`, `db.Users.find({"n": {"$bogus": 1}})`, `db.Nope.find()`,
	}},
	{slug: "bucket-search", statements: []string{
		`db.Users.search({"n": 1})`, `db.Users.search({"n": {"$gt": 0}})`, `db.Users.search({"zzz": 1})`,
		`db.Emails.search({"Email": "a@example.com"})`, `db.Nope.search({"n": 1})`,
	}},
	// A bucket that declares store fields, read every way a document is read
	// out of a bucket. This is the piped transcript: it was recorded on the
	// build before the terminal display existed and must never change, because a
	// script parses it (TestPipedDocumentsAreByteIdenticalToBeforeTheDisplay).
	{slug: "bucket-stores-piped", statements: []string{
		`db.Post.find()`, `db.Post.find("p1")`, `db.Post.get("p1")`, `db.Post.search({"title": "hello"})`,
		`db.Post.update("p1", {$set: {"Name": "Ada"}})`, `db.Post.find({"Name": "Ada"})`,
	}},
	{slug: "bucket-count", statements: []string{`db.Users.count()`, `db.Users.length()`, `db.Jobs.count()`, `db.Nope.count()`}},
	{slug: "bucket-update", statements: []string{
		`db.Users.update("u1", {$set: {"n": 2}})`, `db.Users.update("u1", {$inc: {"n": 5}})`, `db.Users.update("u1", {$unset: ["n"]})`,
		`db.Users.update("zz", {$set: {"n": 2}})`, `db.Users.update("u1")`, `db.Users.find("u1")`,
	}},
	{slug: "bucket-delete", statements: []string{`db.Users.delete("u1")`, `db.Users.delete("u1")`, `db.Users.count()`, `db.Nope.delete("k")`}},
	{slug: "bucket-distinct", statements: []string{`db.Users.distinct("n")`, `db.Users.distinct("zz")`, `db.Nope.distinct("n")`}},
	{slug: "bucket-stats", statements: []string{`db.Users.stats()`, `db.Post.stats()`, `db.Emails.stats()`, `db.Nope.stats()`}},
	{slug: "bucket-range-first-last", statements: []string{
		`db.Events.range({$gte: "a", $lte: "z"})`, `db.Events.range({$gte: "x", $lte: "z"})`, `db.Events.range({"from": 1, "to": 9})`,
		`db.Events.first()`, `db.Events.last()`, `db.Users.first()`, `db.Events.delete("e5")`, `db.Events.first()`,
	}},

	// Queues and heaps.
	{slug: "bucket-push", statements: []string{
		`db.Jobs.push({"task": "y"})`, `db.Tasks.push({"priority": 1, "task": "u"})`, `db.Files.push({"a": 1})`, `db.Users.push({"a": 1})`, `db.Events.push({"a": 1})`,
	}},
	{slug: "bucket-pop", statements: []string{
		`db.Jobs.pop()`, `db.Jobs.pop()`, `db.Tasks.pop()`, `db.Tasks.pop()`, `db.Events.pop()`, `db.Users.pop()`, `db.Files.pop()`, `db.Nope.pop()`,
	}},
	// pop({limit: N}) needs more than the fixture's one entry per queue, and
	// the fixture has no lifo bucket, so this row creates one and pushes more
	// first (setup output is not recorded). The heap's priorities make its pop
	// order differ from its push order.
	{slug: "bucket-pop-limit", setup: []string{
		`create bucket Stack { type: lifo }`,
		`db.Jobs.push({"task": "y"})`, `db.Jobs.push({"task": "z"})`, `db.Jobs.push({"task": "w"})`, `db.Jobs.push({"task": "v"})`,
		`db.Stack.push({"n": 1})`, `db.Stack.push({"n": 2})`, `db.Stack.push({"n": 3})`, `db.Stack.push({"n": 4})`, `db.Stack.push({"n": 5})`,
		`db.Tasks.push({"priority": 5, "task": "u"})`, `db.Tasks.push({"priority": 3, "task": "v"})`,
		`db.Tasks.push({"priority": 4, "task": "w"})`, `db.Tasks.push({"priority": 1, "task": "x"})`,
	}, statements: []string{
		`db.Jobs.pop({limit: 2})`, `db.Jobs.peek({limit: 9})`, `db.Jobs.pop({limit: 9})`, `db.Jobs.pop({limit: 2})`,
		`db.Stack.pop({limit: 2})`, `db.Tasks.pop({limit: 2})`,
		`db.Jobs.pop(3)`, `db.Jobs.pop({limit: 0})`, `db.Jobs.pop({lmit: 3})`, `db.Jobs.pop({limit: 101})`, `db.Users.pop({limit: 2})`,
	}},
	{slug: "bucket-peek", statements: []string{
		`db.Jobs.peek()`, `db.Tasks.peek()`, `db.Jobs.peek(2)`, `db.Jobs.pop()`, `db.Jobs.peek()`, `db.Tasks.pop()`, `db.Tasks.peek()`, `db.Users.peek()`, `db.Nope.peek()`,
	}},
	// peek({limit: N}) needs more than the fixture's one entry per queue to show
	// a pop order, so this row pushes two more Jobs and two more Tasks first
	// (setup output is not recorded). The heap's priorities make its pop order
	// differ from its push order.
	{slug: "bucket-peek-limit", setup: []string{
		`db.Jobs.push({"task": "y"})`, `db.Jobs.push({"task": "z"})`,
		`db.Tasks.push({"priority": 5, "task": "u"})`, `db.Tasks.push({"priority": 3, "task": "v"})`,
	}, statements: []string{
		`db.Jobs.peek({limit: 2})`, `db.Jobs.peek({limit: 9})`, `db.Tasks.peek({limit: 2})`, `db.Jobs.peek({limit: 0})`,
		`db.Jobs.peek({limit: "3"})`, `db.Jobs.peek({lmit: 3})`, `db.Users.peek({limit: 2})`,
	}},

	// The lease verbs. Work is a fresh fifo with five entries; {LEASE} and
	// {ITEM1} are the handles the last claim printed (heldHandles), so the row
	// acknowledges what it really claimed. The refusals come last.
	{slug: "bucket-claim", setup: []string{
		`create bucket Work { type: fifo }`,
		`db.Work.push({"n": 1})`, `db.Work.push({"n": 2})`, `db.Work.push({"n": 3})`, `db.Work.push({"n": 4})`, `db.Work.push({"n": 5})`,
	}, statements: []string{
		`db.Work.claim({limit: 2, lease: "30s"})`, `db.Work.len()`,
		`db.Work.ack({lease_id: "{LEASE}", item_ids: ["{ITEM1}"]})`, `db.Work.ack({lease_id: "{LEASE}"})`,
		`db.Work.claim({limit: 100})`, `db.Work.peek()`, `db.Work.pop()`, `db.Work.claim()`,
		`db.Work.extend({lease_id: "{LEASE}", lease: "5m"})`, `db.Work.release({lease_id: "{LEASE}"})`,
		`db.Work.release({lease_id: "{LEASE}"})`, `db.Work.claim({limit: 100})`, `db.Work.claim()`,
		`db.Work.claim(3)`, `db.Work.claim({bogus: true})`, `db.Work.claim({lease: 30})`, `db.Work.claim({lease: "2h"})`,
		`db.Work.claim({lease: "500ms"})`, `db.Work.claim({limit: 101})`, `db.Work.ack()`, `db.Work.ack({lease_id: "nope"})`,
		`db.Work.ack({lease_id: "0000000000000000000000000000000000000000"})`, `db.Work.extend({lease_id: "{LEASE}"})`,
		`db.Users.claim()`, `db.Users.ack({lease_id: "0000000000000000000000000000000000000000"})`,
		`db.Post["p1"].Comments.claim()`, `db.Nope.claim()`,
	}},

	// The dead verbs. Work holds three entries that the row claims and releases
	// until each has been returned five times, which dead-letters it; {LEASE}
	// and {DEAD1} are the handles the last claim and the last dead() printed
	// (heldHandles). Slots is a heap whose dead element's id is then taken by a
	// live one, so requeue has something to refuse by name.
	{slug: "bucket-dead", setup: []string{
		`create bucket Work { type: fifo }`, `create bucket Slots { type: heap }`,
		`db.Work.push({"n": 1})`, `db.Work.push({"n": 2})`, `db.Work.push({"n": 3})`,
		`db.Slots.push({"_id_": "job-7", "priority": 3, "n": 1})`,
	}, statements: []string{
		`db.Work.dead()`,
		`db.Work.claim({limit: 3})`, `db.Work.release({lease_id: "{LEASE}"})`, `db.Work.claim({limit: 3})`, `db.Work.release({lease_id: "{LEASE}"})`,
		`db.Work.claim({limit: 3})`, `db.Work.release({lease_id: "{LEASE}"})`, `db.Work.claim({limit: 3})`, `db.Work.release({lease_id: "{LEASE}"})`,
		`db.Work.claim({limit: 3})`, `db.Work.release({lease_id: "{LEASE}"})`, `db.Work.claim()`, `db.Work.len()`,
		`db.Work.dead()`, `db.Work.dead({limit: 2})`, `db.Work.dead({limit: 2, after: "{DEAD2}"})`, `db.Work.dead({after: "{DEAD1}"})`,
		`describe bucket Work`, `show buckets`, `db.Work.dead()`,
		`db.Work.requeue({dead_id: "{DEAD1}"})`, `db.Work.requeue({dead_id: "{DEAD1}"})`, `db.Work.claim()`,
		`db.Work.discard({dead_id: "{DEAD2}"})`, `db.Work.dead()`, `db.Work.discard({all: true})`, `db.Work.dead()`, `db.Work.requeue({all: true})`,
		`db.Slots.claim()`, `db.Slots.release({lease_id: "{LEASE}"})`, `db.Slots.claim()`, `db.Slots.release({lease_id: "{LEASE}"})`,
		`db.Slots.claim()`, `db.Slots.release({lease_id: "{LEASE}"})`, `db.Slots.claim()`, `db.Slots.release({lease_id: "{LEASE}"})`,
		`db.Slots.claim()`, `db.Slots.release({lease_id: "{LEASE}"})`, `db.Slots.push({"_id_": "job-7", "priority": 3, "n": 2})`,
		`db.Slots.dead()`, `db.Slots.requeue({all: true})`, `db.Slots.requeue({dead_id: "{DEAD1}"})`, `db.Slots.discard({dead_id: "{DEAD1}"})`,
		`db.Slots.dead()`,
		`db.Work.requeue()`, `db.Work.requeue({dead_id: "ab", all: true})`, `db.Work.discard({all: false})`, `db.Work.dead({limit: 0})`,
		`db.Work.dead({after: "zz"})`, `db.Work.requeue({dead_id: "zz"})`, `db.Users.dead()`, `db.Users.requeue({all: true})`,
		`db.Post["p1"].Comments.dead()`, `db.Nope.dead()`,
	}},

	// Blobs.
	{slug: "bucket-blob-put", statements: []string{
		`db.Files.put({"_id_": "f2", "data": "yo", "name": "yo.txt"})`, `db.Files.count()`, `db.Files.put({"data": "no key"})`, `db.Users.put({"_id_": "k", "data": "x"})`,
	}},
	{slug: "bucket-blob-get-info", statements: []string{
		`db.Files.get("f1")`, `db.Files.get("nope")`, `db.Files.info("f1")`, `db.Files.info("nope")`, `db.Files.stream()`,
	}},

	// The nested field-store verbs.
	{slug: "field-fifo", statements: []string{
		`db.Post["p1"].Comments.push({"by": "bob"})`, `db.Post["p1"].Comments.peek()`, `db.Post["p1"].Comments.peek({limit: 2})`, `db.Post["p1"].Comments.len()`, `db.Post["p1"].Comments.list({limit: 10})`,
		`db.Post["p1"].Comments.pop()`, `db.Post["p1"].Comments.pop()`, `db.Post["p1"].Comments.pop()`, `db.Post["p1"].Comments.peek()`, `db.Post["p1"].Comments.len()`,
		`db.Post["p1"].Comments.list()`,
	}},
	// The fixture's Comments holds one element, so the setup pushes three more.
	{slug: "field-fifo-pop-limit", setup: []string{
		`db.Post["p1"].Comments.push({"by": "bob"})`, `db.Post["p1"].Comments.push({"by": "cy"})`, `db.Post["p1"].Comments.push({"by": "di"})`,
	}, statements: []string{
		`db.Post["p1"].Comments.pop({limit: 2})`, `db.Post["p1"].Comments.len()`, `db.Post["p1"].Comments.pop({limit: 5})`,
		`db.Post["p1"].Comments.pop({limit: 2})`, `db.Post["p1"].Comments.pop(3)`, `db.Post["p1"].Comments.pop({lmit: 1})`,
	}},
	{slug: "field-hash", statements: []string{
		`db.Post["p1"].Tags.put("t2", {"n": 2})`, `db.Post["p1"].Tags.get("t2")`, `db.Post["p1"].Tags.get("zz")`, `db.Post["p1"].Tags.len()`, `db.Post["p1"].Tags.list()`,
		`db.Post["p1"].Tags.delete("t2")`, `db.Post["p1"].Tags.delete("t2")`, `db.Post["p1"].Tags.len()`,
	}},
	{slug: "field-errors", statements: []string{
		`db.Post["nope"].Comments.get("c1")`, `db.Post["nope"].Comments.peek()`, `db.Post["nope"].Tags.get("t1")`,
		`db.Post["p1"].Comments.put("k", {})`, `db.Post["p1"].Tags.push({"a": 1})`, `db.Post["p1"].Nothing.len()`, `db.Users["u1"].Comments.len()`,
	}},

	// --extjson applies to every document printer, point reads and the
	// field-store reads included.
	{slug: "bucket-extjson", extendedJSON: true, setup: []string{
		`db.Users.insert({"_id_": "d1", "at": {"$date": "2026-02-01T00:00:00Z"}})`,
		`db.Post["p1"].Tags.put("t9", {"at": {"$date": "2026-02-01T00:00:00Z"}})`,
	}, statements: []string{
		`db.Users.find("d1")`, `db.Users.get("d1")`, `db.Users.find({"_id_": "d1"})`, `db.Users.search({"_id_": "d1"})`, `db.Users.find()`,
		`db.Users.update("d1", {$set: {"n": 2}})`, `db.Post["p1"].Tags.get("t9")`, `db.Post["p1"].Tags.list()`,
	}},

	// A database that vanished under an open session: the second connection
	// drops it, and the first still believes it is there.
	{slug: "errors-missing-database-read", prepare: func(t *testing.T, session *goldenSession) {
		t.Helper()
		other := NewCLI(WithAddress(session.addr), WithInsecureTLS(true), WithCredentials("admin", goldenPassword), WithTerminal(io.Discard))
		if err := other.Connect(); err != nil {
			t.Fatalf("second connection: %v", err)
		}
		defer other.Close()
		if err := other.handle("drop database app"); err != nil {
			t.Fatalf("dropping the database from the second connection: %v", err)
		}
	}, statements: []string{`db.Users.count()`, `db.Users.find("u1")`, `db.Users.find()`, `db.Users.insert({"n": 1})`, `show buckets`, `describe bucket Users`}},

	// Statements that do not parse.
	{slug: "parse-errors", statements: []string{
		"db.Users.", "count Users", "db.Users.foo()", "FROM x", `db.Users.find(`, "use app extra", "create bucket", "show nothing",
	}},

	// dump and restore as REPL statements (the verbs' own summaries and exit
	// codes are pinned in TestVerbSummariesAndExitCodesUnchanged). Flags must
	// precede the positional scope, which the third statement records.
	{slug: "dump-statement", statements: []string{
		"dump --out {OUT}/d1 app", "dump --out {OUT}/d1 nope", "dump --out {OUT}/d2 app Users", "dump --out {OUT}/d3 app Nope", "dump app --out {OUT}/d4",
	}},
	{slug: "restore-statement", setup: []string{"dump --out {OUT}/d1 app"}, statements: []string{
		"restore --dry-run --dir {OUT}/d1", "restore --drop --dir {OUT}/d1", "restore --dir {OUT}/d1", "restore --dir {OUT}/none", "restore extra",
	}},
}

// TestREPLTranscriptUnchanged (T20) fails when the text the REPL prints for
// any route changes. One sub-test per route of the build doc's Appendix A
// that a single node can answer; each starts from a fresh server so rows
// cannot depend on each other's order.
func TestREPLTranscriptUnchanged(t *testing.T) {
	for _, row := range goldenRows {
		t.Run(row.slug, func(t *testing.T) {
			session := newFixtureSession(t)
			expand := func(lines []string) []string {
				expanded := make([]string, len(lines))
				for i, line := range lines {
					expanded[i] = strings.ReplaceAll(line, "{OUT}", session.outDir)
				}
				return expanded
			}
			session.cli.handler.SetExtendedJSON(row.extendedJSON)
			if row.prepare != nil {
				row.prepare(t, session)
			}
			runQuiet(t, session, expand(row.setup)...)
			transcript := runStatements(t, session.cli, session.out, expand(row.statements)...)
			assertGolden(t, row.slug, session.normaliser().apply(transcript))
		})
	}
}
