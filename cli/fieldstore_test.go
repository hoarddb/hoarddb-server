package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestCLIDeclarationUsesServerValidator is spec T-11, the CLI half: the CLI
// refuses what the server refuses, with the server's words, before sending,
// because it calls the server's validator.
func TestCLIDeclarationUsesServerValidator(t *testing.T) {
	names := []string{"badger", "segments", "blobs", "indexes", "type", "indices", "acknowledge", "replication_factor", "_id_", "$where", "Comments"}
	for _, name := range names {
		cmd, err := ParseCommand("create bucket Post { " + name + " fifoStore }")
		if err != nil {
			t.Fatalf("parsing the create for %q: %v", name, err)
		}
		_, cliErr := parseBucketSchema(cmd, "app")
		serverErr := server.ValidateStoreDeclarations(server.StoreValidation{Database: "app", Bucket: "Post", BucketType: server.BucketTypeHash,
			Added: []server.StoreDeclaration{{Name: name, Engine: "fifoStore"}}})
		switch {
		case serverErr == nil && cliErr != nil:
			t.Errorf("the CLI refuses %q, which the server accepts: %v", name, cliErr)
		case serverErr != nil && (cliErr == nil || cliErr.Error() != serverErr.Error()):
			t.Errorf("the server refuses %q with %q; the CLI answers %v", name, serverErr, cliErr)
		}
	}
}

func TestDeclarationGrammar(t *testing.T) {
	cases := []struct {
		body     string
		options  map[string]interface{}
		stores   []server.StoreDeclaration
		scalars  []string
		uniques  []string
		removals []string
	}{
		{body: `{ type: hash, indices: ["Email"] }`, options: map[string]interface{}{"type": "hash", "indices": []interface{}{"Email"}}},
		{body: `{ type fifo }`, options: map[string]interface{}{"type": "fifo"}},
		{body: `{ Comments fifoStore, Photos blobStore }`, options: map[string]interface{}{},
			stores: []server.StoreDeclaration{{Name: "Comments", Engine: "fifoStore"}, {Name: "Photos", Engine: "blobStore"}}},
		{body: `{ Comments fifoStore { Author string, Flags fifoStore } }`, options: map[string]interface{}{},
			stores: []server.StoreDeclaration{{Name: "Comments", Engine: "fifoStore", Fields: []server.StoreDeclaration{{Name: "Flags", Engine: "fifoStore"}}}}},
		{body: `{ _id_ uuid auto, Caption string, Email string unique, Comments fifoStore, type: btree }`, options: map[string]interface{}{"type": "btree"},
			stores: []server.StoreDeclaration{{Name: "Comments", Engine: "fifoStore"}}, scalars: []string{"_id_ uuid auto", "Caption string"}, uniques: []string{"Email"}},
		{body: `{ Comments drop, Tags: null }`, options: map[string]interface{}{}, removals: []string{"Comments drop", "Tags: null"}},
	}
	for _, c := range cases {
		got, err := parseDeclarationBody(c.body)
		if err != nil {
			t.Errorf("%s: %v", c.body, err)
			continue
		}
		if !reflect.DeepEqual(got.options, c.options) || !reflect.DeepEqual(got.stores, c.stores) ||
			!reflect.DeepEqual(got.scalars, c.scalars) || !reflect.DeepEqual(got.uniques, c.uniques) || !reflect.DeepEqual(got.removals, c.removals) {
			t.Errorf("%s parsed as options %v, stores %+v, scalars %q, uniques %q, removals %q; want %v, %+v, %q, %q, %q",
				c.body, got.options, got.stores, got.scalars, got.uniques, got.removals, c.options, c.stores, c.scalars, c.uniques, c.removals)
		}
	}
	for _, garbage := range []string{`{ Comments fifostuff }`, `{ Comments }`, `{ Caption string { x fifoStore } }`} {
		if _, err := parseDeclarationBody(garbage); err == nil || !strings.Contains(err.Error(), `"`) {
			t.Errorf("%s parsed without an error quoting the entry: %v", garbage, err)
		}
	}
	// `type` is an option keyword, never a field name: a store named type
	// is refused by the validator the create path calls.
	cmd, _ := ParseCommand("create bucket Post { type fifoStore }")
	if _, err := parseBucketSchema(cmd, "app"); err == nil || !strings.Contains(err.Error(), `"type" is reserved`) {
		t.Fatalf("a store field named type = %v; want the reserved-name refusal", err)
	}
}

func TestFieldAddressGrammar(t *testing.T) {
	cmd, err := ParseCommand(`db.Post["p1"].Comments.push({Author: "ada"})`)
	if err != nil || cmd.Bucket != "Post" || cmd.DocKey != "p1" || cmd.Field != "Comments" || cmd.Method != "push" || cmd.Path != nil || len(cmd.Args) != 1 {
		t.Fatalf("the L2 address parsed as %+v, %v", cmd, err)
	}
	cmd, err = ParseCommand(`db.Post["p1"].Comments[3].Flags.pop()`)
	if err != nil || cmd.DocKey != "p1" || cmd.Field != "Flags" || !reflect.DeepEqual(cmd.Path, []interface{}{"Comments", int64(3)}) || cmd.Method != "pop" {
		t.Fatalf("the L3 address parsed as %+v, %v", cmd, err)
	}
	cmd, err = ParseCommand(`db.Post['p 1'].Tags.put("go", {w: 1})`)
	if err != nil || cmd.DocKey != "p 1" || cmd.Field != "Tags" || cmd.Method != "put" || len(cmd.Args) != 2 {
		t.Fatalf("a single-quoted key parsed as %+v, %v", cmd, err)
	}
	// A dotted name with no bracket keeps its meaning: one flat bucket.
	cmd, err = ParseCommand(`db.Post.Comments.push({Author: "ada"})`)
	if err != nil || cmd.Bucket != "Post.Comments" || cmd.DocKey != "" || cmd.Method != "push" {
		t.Fatalf("the flat dotted bucket parsed as %+v, %v", cmd, err)
	}
	for _, bad := range []string{`db.Post[p1].Comments.push({})`, `db.Post["p1"]Comments.push({})`, `db.Post["p1"].Comments`} {
		if _, err := ParseCommand(bad); err == nil {
			t.Errorf("%s parsed without an error", bad)
		}
	}
}

func TestAlterRouting(t *testing.T) {
	c := newDumpTestServer(t)
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	mustSend(t, c, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "Post", Type: server.BucketTypeHash}, nil)
	alter := func(body string) error {
		cmd, err := ParseCommand("alter bucket Post " + body)
		if err != nil {
			t.Fatalf("parsing alter %s: %v", body, err)
		}
		cmd.Database = "app"
		return c.handler.Handle(cmd)
	}
	if err := alter(`{ Tags hashStore }`); err != nil {
		t.Fatalf("declaring Tags: %v", err)
	}
	var stats server.BucketStatsResponse
	mustSend(t, c, transport.OpBucketStats, &server.BucketStatsRequest{Database: "app", Bucket: "Post"}, &stats)
	if len(stats.Stores) != 1 || stats.Stores[0].Name != "Tags" {
		t.Fatalf("after the declare Post reports stores %+v", stats.Stores)
	}
	if err := alter(`{ indices: ["Email"] }`); err == nil || err.Error() != errAlterBucket("Post").Error() {
		t.Fatalf("an options-only alter = %v; want today's errAlterBucket", err)
	}
	if err := alter(`{ Tags drop }`); err == nil || !strings.Contains(err.Error(), "not supported in 1.0") {
		t.Fatalf("a removal = %v; want Q3's refusal", err)
	}
	err := alter(`{ Notes hashStore, indices: ["Email"] }`)
	if err == nil || !strings.Contains(err.Error(), "Notes") || !strings.Contains(err.Error(), "indices") {
		t.Fatalf("a mixed alter = %v; want a refusal naming both halves", err)
	}
}

// runStatement runs one HQL statement against database app and returns what
// the handler printed.
func runStatement(t *testing.T, c *CLI, statement string) (string, error) {
	t.Helper()
	cmd, err := ParseCommand(statement)
	if err != nil {
		t.Fatalf("parsing %s: %v", statement, err)
	}
	cmd.Database = "app"
	var printed bytes.Buffer
	c.handler.terminal = &printed
	err = c.handler.Handle(cmd)
	return printed.String(), err
}

// TestFieldBlobPutCarriesTheDataPayload: on a blobStore field the
// CLI's put took its document as metadata only, so an element put with
// {data: "..."} held zero bytes and get could not show them. A blob field now
// reads its payload from "data" as a blob bucket does, get prints it, and
// {out: "file"} writes it; a hash field's "data" key stays an ordinary key.
// TestFieldCountSpellingIsUniform is the fourth seam in the same family as the
// store-type spelling: `help fifo`, `help btree` and the main help all teach
// `length()` on a bucket, but a store field took only `len()`, so following the
// help — the reasonable thing to do — was refused on the store field, which is
// where the data actually lives.
//
// A count is a count at either level, so all three spellings answer on both.
// The wire call is the same OpFieldLen in every case; only the verb word the
// developer typed differs.
func TestFieldCountSpellingIsUniform(t *testing.T) {
	c := newDumpTestServer(t)
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	mustSend(t, c, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "Post", Type: server.BucketTypeHash, Options: map[string]interface{}{
		"stores": []interface{}{
			map[string]interface{}{"name": "Comments", "engine": "fifoStore"},
			map[string]interface{}{"name": "Tags", "engine": "hashStore"},
		},
	}}, nil)
	mustSend(t, c, transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"title": "p1"}}, nil)

	count := func(statement string) string {
		t.Helper()
		printed, err := runStatement(t, c, statement)
		if err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
		return printed
	}
	count(`db.Post["p1"].Comments.push({by: "bob"})`)
	count(`db.Post["p1"].Comments.push({by: "ann"})`)
	count(`db.Post["p1"].Tags.put("t1", {n: 1})`)

	// Every spelling answers, on every engine, and answers the same number.
	for _, tc := range []struct {
		field string
		want  string
	}{{"Comments", "2"}, {"Tags", "1"}} { // two comments pushed, one tag
		first := count(`db.Post["p1"].` + tc.field + `.len()`)
		if strings.TrimSpace(first) != tc.want {
			t.Fatalf("%s.len() = %q, want %q", tc.field, first, tc.want)
		}
		for _, verb := range []string{"length", "count"} {
			got := count(`db.Post["p1"].` + tc.field + `.` + verb + `()`)
			if got != first {
				t.Errorf("%s.%s() = %q, want the same as .len() = %q", tc.field, verb, got, first)
			}
		}
	}
}

// TestBucketCountSpellingsStillWork guards the other end: the bucket-level
// count() and length() the help documents must keep answering after the field
// verbs accept them too, so no spelling stopped working anywhere.
func TestBucketCountSpellingsStillWork(t *testing.T) {
	c := newDumpTestServer(t)
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	mustSend(t, c, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "Jobs", Type: server.BucketTypeFIFO}, nil)
	mustSend(t, c, transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Jobs", Key: "j1", Data: map[string]interface{}{"t": "one"}}, nil)

	for _, verb := range []string{"count", "length", "len"} {
		printed, err := runStatement(t, c, `db.Jobs.`+verb+`()`)
		if err != nil {
			t.Errorf("db.Jobs.%s(): %v", verb, err)
			continue
		}
		if strings.TrimSpace(printed) != "1" {
			t.Errorf("db.Jobs.%s() = %q, want 1", verb, printed)
		}
	}
}

// TestFieldPeekWithNoCountIsUnchanged: the fix must not touch peek() itself,
// which returns the head in pop order and is what every existing transcript
// relies on.
func TestFieldPeekWithNoCountIsUnchanged(t *testing.T) {
	c := newDumpTestServer(t)
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	mustSend(t, c, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "Post", Type: server.BucketTypeHash, Options: map[string]interface{}{
		"stores": []interface{}{map[string]interface{}{"name": "Photos", "engine": "lifoStore"}},
	}}, nil)
	mustSend(t, c, transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"title": "p1"}}, nil)
	for _, name := range []string{"photo_1", "photo_2"} {
		if _, err := runStatement(t, c, `db.Post["p1"].Photos.push({photo: "`+name+`"})`); err != nil {
			t.Fatalf("push: %v", err)
		}
	}

	// A lifo pops newest-first, so peek must answer the newest of the two.
	printed, err := runStatement(t, c, `db.Post["p1"].Photos.peek()`)
	if err != nil {
		t.Fatalf("peek(): %v", err)
	}
	if !strings.Contains(printed, "photo_2") {
		t.Errorf("peek() = %q, want the newest element (photo_2) on a lifo", printed)
	}
	if strings.Contains(printed, "photo_1") {
		t.Errorf("peek() returned more than the head:\n%s", printed)
	}
}

// TestHelpNeverAdvertisesARefusedSpelling is the fifth instance of one class:
// the help printed `db.<bucket>.peek(N)` in three places while the bucket verb
// refused that exact argument out loud. A developer copying the help got an
// error; a developer who had been refused once had no reason to doubt the
// help. The two must agree.
//
// Each verb the help names is checked against the refusal it produces, so a
// future verb cannot be documented and unimplemented in the same breath.
func TestHelpNeverAdvertisesARefusedSpelling(t *testing.T) {
	for _, topic := range []string{"", "fifo", "lifo", "fields"} {
		var out strings.Builder
		h := &CommandHandler{terminal: &out}
		if topic == "" {
			h.handleHelp()
		} else {
			requireHelpTopic(t, h, topic)
		}
		text := out.String()
		for _, line := range strings.Split(text, "\n") {
			// Only a line that offers peek(N) as a command counts. A line
			// that says there is no peek(N) is the help telling the truth.
			if !strings.Contains(line, "db.<") || !strings.Contains(line, ".peek(N)") {
				continue
			}
			t.Errorf("help %q advertises %q, which the CLI refuses: %s",
				topic, "peek(N)", strings.TrimSpace(line))
		}
	}
}

// TestHelpDocumentsTheBucketWayToTakeN is the seam behind this one: list() is a
// store-field verb, so a developer who read `db.Post["p1"].Comments.list({limit:
// N})` reasonably tried `db.<bucket>.list({limit: N})` and was refused with
// "unknown bucket method". The bucket's equivalent is find({}, {limit: N}),
// which works on every bucket type except blob — and which the help did not
// print anywhere: only the {skip} page was documented.
func TestHelpDocumentsTheBucketWayToTakeN(t *testing.T) {
	var out strings.Builder
	NewCommandHandler(&out, false).handleHelp()
	help := out.String()

	if !strings.Contains(help, "find({}, {limit: N})") {
		t.Errorf("the help never shows find({}, {limit: N}), the bucket way to take N:\n%s", help)
	}
	// And the two must be distinguished, or the next reader repeats the trip.
	if !strings.Contains(help, "list({limit: N})") {
		t.Error("the help does not name list({limit: N}) to contrast it with")
	}

	cmd, err := ParseCommand("db.Users.find({}, {limit: 10})")
	if err != nil {
		t.Fatalf("find({}, {limit: 10}) does not parse: %v", err)
	}
	if cmd.Method != "find" || len(cmd.Args) != 2 {
		t.Fatalf("find({}, {limit: 10}) parsed as method=%q args=%v, want find with the filter and the options", cmd.Method, cmd.Args)
	}
}

// TestHelpDocumentsTheNestedStoreForm is the sixth instance of one class: the
// main help documented only db.<bucket>.<verb>, so the store form — the whole
// idea of the database — appeared only under `help fields`, which a developer
// has to already know about. A bucket can carry a store per document, and the
// main help did not say so.
//
// The examples in the new section are parsed here rather than trusted. Every
// `db.<bucket>["<key>"].<field>.<verb>` line the help prints must parse to a
// real store-field verb: that is the check that would have caught this help
// lying, and the next one.
func TestHelpDocumentsTheNestedStoreForm(t *testing.T) {
	var out strings.Builder
	NewCommandHandler(&out, false).handleHelp()
	help := out.String()

	// The shape, and one example of it.
	if !strings.Contains(help, `db.<bucket>["<key>"].<field>`) {
		t.Errorf("the main help never shows the store-field shape db.<bucket>[\"<key>\"].<field>.<verb>():\n%s", help)
	}

	// Every nested example the help prints must parse, and must address a
	// store field — a plain bucket verb in this shape would parse and mean
	// something else, which is exactly how help text misleads.
	checked := 0
	for _, line := range strings.Split(help, "\n") {
		line = strings.TrimSpace(line)
		// A transcript line carries the prompt; the example is what follows it.
		if _, after, hadPrompt := strings.Cut(line, "> "); hadPrompt {
			line = strings.TrimSpace(after)
		}
		if !strings.HasPrefix(line, "db.Post[") {
			continue
		}
		example, _, _ := strings.Cut(line, "  ") // cut the trailing comment
		example = strings.TrimSpace(example)
		if example == "" || strings.ContainsAny(example, "<>") {
			continue // the shape placeholder itself, not a runnable example
		}
		cmd, err := ParseCommand(example)
		if err != nil {
			t.Errorf("the help prints %q, which does not parse: %v", example, err)
			continue
		}
		if cmd.DocKey == "" || cmd.Field == "" {
			t.Errorf("the help prints %q, which does not address a store field (DocKey=%q Field=%q)",
				example, cmd.DocKey, cmd.Field)
			continue
		}
		checked++
	}
	if checked == 0 {
		t.Error("the help's store-field section has no runnable example to check")
	}
}

func TestFieldBlobPutCarriesTheDataPayload(t *testing.T) {
	c := newDumpTestServer(t)
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	mustSend(t, c, transport.OpCreateBucket, &server.CreateBucketRequest{Database: "app", Name: "Post", Type: server.BucketTypeHash, Options: map[string]interface{}{
		"stores": []interface{}{
			map[string]interface{}{"name": "Photos", "engine": "blobStore"},
			map[string]interface{}{"name": "Tags", "engine": "hashStore"},
		},
	}}, nil)
	mustSend(t, c, transport.OpPut, &server.InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"title": "p1"}}, nil)
	run := func(statement string) string {
		t.Helper()
		printed, err := runStatement(t, c, statement)
		if err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
		return printed
	}

	run(`db.Post["p1"].Photos.put("ph2", {data: "hello photo", name: "b.txt"})`)
	var stored server.FieldResponse
	mustSend(t, c, transport.OpFieldGet, &server.FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Photos", Element: "ph2"}, &stored)
	if string(stored.Content) != "hello photo" {
		t.Fatalf("Photos[ph2] holds %q, want the data payload %q", stored.Content, "hello photo")
	}
	if _, leaked := stored.Element.Lookup("data").StringValueOK(); leaked {
		t.Fatalf("the payload was also stored as metadata: %v", stored.Element)
	}
	if name, _ := stored.Element.Lookup("name").StringValueOK(); name != "b.txt" {
		t.Fatalf("the metadata lost name: %v", stored.Element)
	}

	got := run(`db.Post["p1"].Photos.get("ph2")`)
	for _, want := range []string{"hello photo", "b.txt"} {
		if !strings.Contains(got, want) {
			t.Errorf("get printed %q, want it to show %q", got, want)
		}
	}

	outPath := filepath.Join(t.TempDir(), "photo.txt")
	run(`db.Post["p1"].Photos.get("ph2", {out: "` + outPath + `"})`)
	if written, err := os.ReadFile(outPath); err != nil || string(written) != "hello photo" {
		t.Fatalf("get with out wrote %q, %v; want the payload", written, err)
	}

	// An element with no data stays metadata only, as before.
	run(`db.Post["p1"].Photos.put("ph1", {name: "a.txt"})`)
	stored = server.FieldResponse{}
	mustSend(t, c, transport.OpFieldGet, &server.FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "Photos", Element: "ph1"}, &stored)
	if !stored.Found || len(stored.Content) != 0 {
		t.Fatalf("Photos[ph1] = found %v, %d bytes; want a found, empty element", stored.Found, len(stored.Content))
	}

	// "data" on a hash field is the user's key, not a payload.
	run(`db.Post["p1"].Tags.put("t1", {data: "keep me", n: 1})`)
	if got := run(`db.Post["p1"].Tags.get("t1")`); !strings.Contains(got, "keep me") {
		t.Errorf("a hash field lost its data key: get printed %q", got)
	}
}

// TestFieldVerbsSendNoBucketStatsFrames: BUG-0520. A field put whose element
// has a data key, and a field push with a fractional priority, look up the
// field's engine. They asked BUCKET_STATS, which counts every element of every
// store; for a top-level field the engine is in BUCKET_INFO, which counts none.
func TestFieldVerbsSendNoBucketStatsFrames(t *testing.T) {
	stub, c, _ := newDeclarationStub(t, declarationScript(declaredBucket{
		bucketType: server.BucketTypeHash,
		stores: []server.StoreStats{
			{Name: "Photos", Engine: "blob", Elements: 60000},
			{Name: "Jobs", Engine: "heap", Elements: 60000},
			{Name: "Notes", Engine: "fifo", Elements: 60000},
		},
	}))
	statements := []struct {
		statement string
		wantError bool
	}{
		{`db.Users["u1"].Photos.put("a", {data: "x"})`, false},
		{`db.Users["u1"].Notes.put("a", {data: "x"})`, false},
		{`db.Users["u1"].Jobs.push({priority: 1.5, n: 1})`, true},
		{`db.Users["u1"].Notes.push({priority: 1.5, n: 1})`, false},
	}
	for range 5 {
		for _, row := range statements {
			err := c.handle(row.statement)
			if (err != nil) != row.wantError {
				t.Fatalf("%s returned %v, want an error: %v", row.statement, err, row.wantError)
			}
		}
	}
	if got := stub.sent(transport.OpBucketStats); got != 0 {
		t.Errorf("20 field statements sent %d BUCKET_STATS frames, want 0", got)
	}
	// Each of the four statements looks the engine up once per round, fresh:
	// 5 rounds x 4 lookups.
	if got := stub.sent(transport.OpBucketInfo); got != 20 {
		t.Errorf("20 field statements sent %d BUCKET_INFO frames, want 20: one fresh answer per lookup", got)
	}
}
