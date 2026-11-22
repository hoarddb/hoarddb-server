package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// u82Mode says how much of the example world a help example needs before it runs.
type u82Mode int

const (
	u82Nothing   u82Mode = iota // no database: the example makes one
	u82Databases                // databases app and shop, no bucket: the example makes one
	u82Bucket                   // the User bucket, no document: the example inserts one
	u82Full                     // the User bucket with the document ada and one element in each store the examples read
)

// u82UserBucket declares every store type, so one bucket serves every example.
const u82UserBucket = "create bucket User { Name string, Age int, Purchases fifoStore, Photos lifoStore, " +
	"Friends hashStore, Recipes btreeStore with (sum Minutes), Books heapStore, Data blobStore }"

// u82ModeOf is the world a form's examples are written for.
func u82ModeOf(verb string) u82Mode {
	switch verb {
	case "create database":
		return u82Nothing
	case "create bucket":
		return u82Databases
	case "insert":
		return u82Bucket
	}
	return u82Full
}

// u82Prepare builds the world the example runs in, through the statements a person would type.
func u82Prepare(ctx context.Context, t *testing.T, session *RingSession, mode u82Mode) {
	t.Helper()
	var statements []string
	if mode >= u82Databases {
		statements = append(statements, "create database app", "create database shop", "use app")
	}
	if mode >= u82Bucket {
		statements = append(statements, u82UserBucket)
	}
	if mode >= u82Full {
		statements = append(statements, `db.User.insert({_id_: "ada", Name: "Ada", Age: 36})`,
			`db.User["ada"].Purchases.push({Item: "coffee"})`, `db.User["ada"].Photos.push({File: "beach.jpg"})`,
			`db.User["ada"].Friends.put("grace", {since: 2024})`, `db.User["ada"].Recipes.put("soup", {Minutes: 30})`)
	}
	for _, statement := range statements {
		if _, err := session.Execute(ctx, statement); err != nil {
			t.Fatalf("preparing the example world: %q: %v", statement, err)
		}
	}
}

// u82ExampleTexts lists every example of every form ring mode runs.
func u82ExampleTexts(form ringForm) []string {
	texts := make([]string, len(form.Examples))
	for i, example := range form.Examples {
		texts[i] = example.Text
	}
	return texts
}

// TestEveryHelpExampleRuns: every example help prints is a statement the server runs, so
// help never advertises a refusal (the F3 finding: Friends.get("grace") was refused).
func TestEveryHelpExampleRuns(t *testing.T) {
	for _, form := range ringForms {
		if form.Unavailable {
			continue
		}
		for _, example := range u82ExampleTexts(form) {
			t.Run(example, func(t *testing.T) {
				session, fake := u52Session()
				if form.Group == "Users" {
					session.promptPassword = func() (string, error) { return u80NewPassword, nil }
					if !strings.HasPrefix(example, "create user") {
						u80WithAda(fake)
					}
				}
				u82Prepare(context.Background(), t, session, u82ModeOf(form.Verb))
				if _, err := session.Execute(context.Background(), example); err != nil {
					t.Errorf("help prints %q for %q and running it is refused: %v", example, form.Verb, err)
				}
			})
		}
	}
}

// TestLiveHelpExamplesRun runs the examples against the real in-process server, where the
// client refusals and the server's own checks both apply (a cursor that was never printed,
// a verb the store type does not take). The user statements change the server's users and ask
// for passwords; the fake run above and the live tests of ringusers_live_test.go cover them.
func TestLiveHelpExamplesRun(t *testing.T) {
	for _, form := range ringForms {
		// join, promote and replace-node change a cluster's membership, and this server is one node
		// with no cluster, which refuses them by design; TestRingJoinTranscript runs join and promote
		// against real cluster members and cluster/harness runs replace-node (BUG-0870). A single
		// node holds no seeding record, so seeding retry and seeding forget are SEED_NOT_HELD by
		// design; the cluster harness runs them.
		if form.Unavailable || form.Group == "Users" || form.Verb == "join" || form.Verb == "promote" || form.Verb == "replace-node" ||
			form.Verb == "seeding retry" || form.Verb == "seeding forget" {
			continue
		}
		for _, example := range u82ExampleTexts(form) {
			t.Run(example, func(t *testing.T) {
				session, ctx := u52LiveSession(t)
				u82Prepare(ctx, t, session, u82ModeOf(form.Verb))
				if _, err := session.Execute(ctx, example); err != nil {
					t.Errorf("help prints %q for %q and the server refuses it: %v", example, form.Verb, err)
				}
			})
		}
	}
}

// u82Cell is what one store verb does on one store type against the real server.
type u82Cell string

const (
	u82Runs    u82Cell = "runs"    // the server takes it, even if the answer is "empty"
	u82NotYet  u82Cell = "not yet" // the client says it is not available yet
	u82Refused u82Cell = "refused" // the server refuses the pairing and names the verbs the store takes
)

// u82ConfirmedCells is the verb by store-type matrix as measured live on a single node
// (SPEC-GAP U82-1). A verb absent from a store type's lists is refused.
var u82ConfirmedCells = map[string]map[u82Cell][]string{
	"push":     {u82Runs: {"fifoStore", "lifoStore"}, u82NotYet: {"heapStore"}},
	"pop":      {u82Runs: {"fifoStore", "lifoStore", "heapStore"}},
	"peek":     {u82Runs: {"fifoStore", "lifoStore", "heapStore"}},
	"list":     {u82Runs: {"fifoStore", "lifoStore", "heapStore", "hashStore", "btreeStore"}},
	"count":    {u82Runs: {"fifoStore", "lifoStore", "heapStore", "hashStore", "btreeStore", "blobStore"}},
	"nearest":  {u82Runs: {"fifoStore", "lifoStore", "heapStore", "hashStore", "btreeStore"}},
	"sum":      {u82Runs: {"fifoStore", "lifoStore", "heapStore", "hashStore", "btreeStore"}},
	"describe": {u82Runs: {"fifoStore", "lifoStore", "heapStore", "hashStore", "btreeStore"}},
	"put":      {u82Runs: {"hashStore", "btreeStore"}, u82NotYet: {"blobStore"}},
	"inc":      {u82Runs: {"hashStore", "btreeStore"}},
	"take":     {u82Runs: {"hashStore", "btreeStore"}, u82NotYet: {"heapStore"}},
	"claim":    {u82Runs: {"fifoStore", "lifoStore"}},
	"ack":      {u82Runs: {"fifoStore", "lifoStore"}},
	"release":  {u82Runs: {"fifoStore", "lifoStore"}},
	"extend":   {u82Runs: {"fifoStore", "lifoStore"}},
	"get":      {u82Runs: {"fifoStore", "lifoStore", "hashStore", "btreeStore", "heapStore"}},
}

// u82StoreFields is one field per store type, named so a failure says which.
var u82StoreFields = map[string]string{
	"fifoStore": "Fifo", "lifoStore": "Lifo", "hashStore": "Hash", "btreeStore": "Btree", "heapStore": "Heap", "blobStore": "Blob",
}

var u82LeasePattern = regexp.MustCompile(`^lease ([0-9a-f]{32})$`)

// TestLiveStoreVerbMatrix runs every store verb on every store type against the real server and
// compares what happened to u82ConfirmedCells: P1-1's check that help only names pairings that work.
func TestLiveStoreVerbMatrix(t *testing.T) {
	session, ctx := u52LiveSession(t)
	if _, err := u82Statements(ctx, session, "create database app", "use app",
		"create bucket User { Name string, Fifo fifoStore with (sum a), Lifo lifoStore with (sum a), Hash hashStore with (sum a), Btree btreeStore with (sum a), Heap heapStore with (sum a), Blob blobStore }",
		`db.User.insert({_id_: "ada", Name: "Ada"})`); err != nil {
		t.Fatal(err)
	}
	for storeType, field := range u82StoreFields {
		lease := "11111111111111111111111111111111"
		if storeType == "fifoStore" || storeType == "lifoStore" {
			target := `db.User["ada"].` + field
			if _, err := u82Statements(ctx, session, target+`.push({a: 1})`); err != nil {
				t.Fatal(err)
			}
			claimed, err := u82Statements(ctx, session, target+".claim(1)")
			if err != nil || len(claimed) == 0 || !u82LeasePattern.MatchString(claimed[0]) {
				t.Fatalf("claim on %s: %v %v", storeType, claimed, err)
			}
			lease = u82LeasePattern.FindStringSubmatch(claimed[0])[1]
		}
		calls := map[string]string{
			"push": `push({a: 1})`, "pop": `pop()`, "peek": `peek()`, "list": `list()`, "count": `count()`,
			"put": `put("k", {a: 1})`, "inc": `inc("k", {a: 1})`, "take": `take("k")`, "claim": `claim(1)`, "ack": `ack("` + lease + `")`,
			"release": `release("` + lease + `")`, "extend": `extend("` + lease + `", 5000)`, "get": `get("` + lease + `")`,
			"sum": `sum("a")`, "describe": `describe()`,
			"nearest": "nearest({vector: {\"$binary\": {\"base64\": \"AACAPwAAAAA=\", \"subType\": \"00\"}}, k: 1})",
		}
		for verb, call := range calls {
			statement := `db.User["ada"].` + field + "." + call
			_, err := session.Execute(ctx, statement)
			got := u82Runs
			var failed *RingError
			switch {
			case err == nil:
			case errors.As(err, &failed) && failed.Code == "NOT_FOUND":
			case strings.Contains(err.Error(), "not available yet"):
				got = u82NotYet
			default:
				got = u82Refused
			}
			want := u82Refused
			for cell, types := range u82ConfirmedCells[verb] {
				if slices.Contains(types, storeType) {
					want = cell
				}
			}
			if got != want {
				t.Errorf("%s: %s, want %s (%v)", statement, got, want, err)
			}
		}
	}
}

// u82Statements runs statements and returns the lines of the last one.
func u82Statements(ctx context.Context, session *RingSession, statements ...string) ([]string, error) {
	var last []string
	for _, statement := range statements {
		output, err := session.Execute(ctx, statement)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", statement, err)
		}
		last = output.Lines
	}
	return last, nil
}

// TestStoreVerbTableMatchesRefusals: for every verb and every store word, a word in NotYet gets the
// client's "not available yet" refusal, a word in Types is not refused by the client, and the table
// is exactly the matrix measured live (u82ConfirmedCells), so help and the refusals cannot disagree.
func TestStoreVerbTableMatchesRefusals(t *testing.T) {
	if len(ringVerbs) != len(u82ConfirmedCells) {
		t.Fatalf("the table has %d verbs, the live matrix %d", len(ringVerbs), len(u82ConfirmedCells))
	}
	for _, verb := range ringVerbs {
		cells, ok := u82ConfirmedCells[verb.Name]
		if !ok {
			t.Errorf("verb %q is not in the live matrix", verb.Name)
			continue
		}
		if !slices.Equal(verb.Types, cells[u82Runs]) || !slices.Equal(verb.NotYet, cells[u82NotYet]) {
			t.Errorf("verb %q: table types %v not-yet %v, live matrix runs on %v not yet on %v",
				verb.Name, verb.Types, verb.NotYet, cells[u82Runs], cells[u82NotYet])
		}
		for _, store := range ringStoreWords {
			refusal := ringUnwiredStoreVerb(ringStoreCall{verb: verb.Name, field: ringclient.FieldInfo{Name: "F", Type: store}, name: "User", id: "ada"})
			switch {
			case slices.Contains(verb.NotYet, store) && (refusal == nil || !strings.Contains(refusal.Message, "not available yet")):
				t.Errorf("%s on a %s is in NotYet but the client does not say so: %v", verb.Name, store, refusal)
			case !slices.Contains(verb.NotYet, store) && refusal != nil:
				t.Errorf("%s on a %s is not in NotYet but the client refuses it: %v", verb.Name, store, refusal)
			}
		}
	}
	for _, name := range ringVerbNames() {
		if _, ok := ringVerbByName(name); !ok {
			t.Errorf("verb %q is not found by name", name)
		}
	}
}

// TestFieldFormExamplesAreTheVerbExamples: the one 'field' grammar form takes its examples and its
// verb words from the verb table, so a verb cannot be parsed without a page or an example.
func TestFieldFormExamplesAreTheVerbExamples(t *testing.T) {
	form, ok := ringFormByVerb("field")
	if !ok || len(form.Examples) != len(ringVerbs) {
		t.Fatalf("field form: %v, %d examples for %d verbs", ok, len(form.Examples), len(ringVerbs))
	}
	for i, verb := range ringVerbs {
		if form.Examples[i].Text != verb.Example {
			t.Errorf("verb %s: field example %q, verb example %q", verb.Name, form.Examples[i].Text, verb.Example)
		}
		st, err := Parse(verb.Example, "app")
		if err != nil || st.Verb != "field" {
			t.Errorf("verb %s: example %q: %v %+v", verb.Name, verb.Example, err, st)
		}
		if verbArg, _ := st.Arg("verb"); verbArg.Text != verb.Name {
			t.Errorf("verb %s: example %q runs verb %q", verb.Name, verb.Example, verbArg.Text)
		}
	}
}

// u82Rows returns the lines of a help page.
func u82Rows(page string) []string { return strings.Split(strings.TrimRight(page, "\n"), "\n") }

// TestHelpAllIsTheReference: help all lists the syntax and one short description of every statement
// ring mode runs, no examples, no empty heading, and one block naming what it cannot run yet.
func TestHelpAllIsTheReference(t *testing.T) {
	all := HelpAll()
	if strings.Contains(all, "e.g.") {
		t.Error("help all carries an example")
	}
	for _, form := range ringForms {
		if form.Unavailable {
			if ringHelpListsSyntax(all, form.Syntax()) {
				t.Errorf("help all lists the unavailable form %q as a statement", form.Verb)
			}
			if !strings.Contains(all, form.Verb) {
				t.Errorf("help all does not name the unavailable statement %q", form.Verb)
			}
			continue
		}
		if !ringHelpListsSyntax(all, form.Syntax()) {
			t.Errorf("help all lacks the syntax of %q", form.Verb)
		}
		if form.Verb != "field" && !strings.Contains(all, form.Short) {
			t.Errorf("help all lacks the short description of %q: %q", form.Verb, form.Short)
		}
	}
	for _, verb := range ringVerbNames() {
		if !strings.Contains(all, verb) {
			t.Errorf("help all does not name the store verb %q", verb)
		}
	}
	rows := u82Rows(all)
	for i, row := range rows {
		if strings.HasSuffix(row, ":") && !strings.HasPrefix(row, " ") && (i+1 == len(rows) || rows[i+1] == "" || !strings.HasPrefix(rows[i+1], "  ")) {
			t.Errorf("heading %q has no rows", row)
		}
	}
	if strings.Contains(all, "-ring") || strings.Count(all, "Not available yet:") != 1 {
		t.Errorf("help all must say once which statements ring mode cannot run yet:\n%s", all)
	}
	if !strings.Contains(all, "  exit  ") {
		t.Error("help all does not list exit")
	}
}

// TestHelpFitsItsSizes: a page is the size the owner asked for and fits an 80-column terminal.
func TestHelpFitsItsSizes(t *testing.T) {
	pages := []struct {
		name     string
		text     string
		maxLines int
	}{{"help", Help(), 23}, {"help all", HelpAll(), 64}} // help all held 50 lines before show banner (U84), 51 before alter bucket became a statement (U40), 52 before drop bucket did (U42) and 53 before the three seeding statements did (T6), 62 after the three seeding statements became runnable (S4), and 64 after replace-node did, its description on a row of its own (BUG-0870)
	for _, topic := range u82Topics() {
		if topic == "field" {
			continue
		}
		page, err := HelpFor(topic)
		if err != nil {
			t.Errorf("help %s: %v", topic, err)
			continue
		}
		limit := 30
		switch topic {
		case "types":
			limit = 24
		case "show":
			// The page held 30 lines before 'show banner' joined the family (U84, 4 lines).
			limit = 34
		case "create":
			// The page held 30 lines before the aggregate example and the index entry joined it (2 lines).
			limit = 32
		case "all":
			continue
		}
		pages = append(pages, struct {
			name     string
			text     string
			maxLines int
		}{"help " + topic, page, limit})
	}
	for _, page := range pages {
		rows := u82Rows(page.text)
		if len(rows) > page.maxLines {
			t.Errorf("%s is %d lines, the limit is %d", page.name, len(rows), page.maxLines)
		}
		for _, row := range rows {
			// A runnable statement (Example, Before, Result) is one line that cannot wrap, and a $binary
			// literal alone is about 56 columns, so those lines are exempt; prose and Syntax must fit.
			if width := len([]rune(row)); width > 80 && !u82IsStatementLine(row) {
				t.Errorf("%s has a line of %d columns: %q", page.name, width, row)
			}
		}
	}
}

// u82IsStatementLine reports whether a help row is a runnable statement or its printed result: a
// two-space indent, then a db. statement or a scored nearest line.
func u82IsStatementLine(row string) bool {
	return strings.HasPrefix(row, "  db.") && !strings.Contains(row, "<") || strings.HasPrefix(row, "  1.000000  ")
}

// u82KnownExamples is every example the grammar table and the verb table print.
func u82KnownExamples() map[string]bool {
	known := map[string]bool{}
	for _, form := range ringForms {
		for _, example := range form.Examples {
			known[example.Text] = true
		}
	}
	for _, verb := range ringVerbs {
		known[verb.Example] = true
	}
	return known
}

// TestQuickStartRunsInOrder: the top page is a working session. Each line is an example the
// table already prints, and typed top to bottom on an empty server, every line runs.
func TestQuickStartRunsInOrder(t *testing.T) {
	known := u82KnownExamples()
	session, _ := u52Session()
	for _, line := range ringQuickStart {
		if !known[line.Example] {
			t.Errorf("quick start line %q is not an example of any form or verb", line.Example)
		}
		if _, err := session.Execute(context.Background(), line.Example); err != nil {
			t.Errorf("quick start line %q is refused after the lines above it: %v", line.Example, err)
		}
	}
	page := Help()
	for _, line := range ringQuickStart {
		if !strings.Contains(page, "  "+line.Example) || !strings.Contains(page, line.Note) {
			t.Errorf("help does not print the quick start line %q / %q", line.Example, line.Note)
		}
		if line.Heading != "" && !strings.Contains(page, line.Heading+":\n") {
			t.Errorf("help does not print the heading %q", line.Heading)
		}
	}
	if strings.Contains(page, "e.g.") {
		t.Error("the quick start carries an e.g. label")
	}
}

// TestLiveQuickStartRunsInOrder is the same session against the real server.
func TestLiveQuickStartRunsInOrder(t *testing.T) {
	session, ctx := u52LiveSession(t)
	for _, line := range ringQuickStart {
		if _, err := session.Execute(ctx, line.Example); err != nil {
			t.Errorf("quick start line %q is refused by the server: %v", line.Example, err)
		}
	}
}

// u82OnAVerbPage reports whether a store verb's page, or a two-level page, prints the statement.
func u82OnAVerbPage(statement string) bool {
	for _, verb := range ringVerbs {
		if verb.Example != statement {
			continue
		}
		page, err := HelpFor(verb.Name)
		return err == nil && u82HasLine(page, "  "+statement) || err == nil && u82HasLine(page, "  "+ringSamplePrompt+statement)
	}
	return false
}

// u82HasLine reports whether a page has a line that is exactly line, or starts with it and two spaces.
func u82HasLine(page, line string) bool {
	for _, row := range u82Rows(page) {
		if row == line || strings.HasPrefix(row, line+"  ") {
			return true
		}
	}
	return false
}

// u82PageHasLine reports whether 'help <topic>' prints the example as a line of its own.
func u82PageHasLine(topic, example string) bool {
	page, err := HelpFor(topic)
	return err == nil && u82HasLine(page, "  "+example)
}

// u82Topics is every topic a person can complete after 'help ', plus the two-word statements.
func u82Topics() []string {
	topics := ringTopics("")
	for _, form := range ringForms {
		topics = append(topics, form.Verb)
	}
	return topics
}

// TestEveryTopicHasAPage: every word 'help' completes has a page, and a page never points at a topic
// that has none.
func TestEveryTopicHasAPage(t *testing.T) {
	pointer := regexp.MustCompile(`help ([a-z]+)`)
	pages := map[string]string{"help": Help(), "help all": HelpAll()}
	for _, topic := range u82Topics() {
		if topic == "field" {
			continue
		}
		page, err := HelpFor(topic)
		if err != nil {
			t.Errorf("help %s: %v", topic, err)
			continue
		}
		pages["help "+topic] = page
	}
	for name, page := range pages {
		for _, match := range pointer.FindAllStringSubmatch(page, -1) {
			if _, err := HelpFor(match[1]); err != nil && match[1] != "word" && match[1] != "me" {
				t.Errorf("%s points at 'help %s', which has no page: %v", name, match[1], err)
			}
		}
	}
	topics := ringTopics("")
	for _, want := range []string{"create", "show", "describe", "db", "insert", "list", "push", "claim", "types", "all", "use", "set"} {
		if !slices.Contains(topics, want) {
			t.Errorf("the completed topics lack %q: %v", want, topics)
		}
	}
	// join and promote run in ring mode since b8e4c72d7, replace-node since BUG-0870, and are topics
	// now; leave and remove-node are still refused.
	for _, unavailable := range []string{"leave", "remove-node"} {
		if slices.Contains(ringTopics(""), unavailable) {
			t.Errorf("the completed topics list the statement ring mode cannot run: %q", unavailable)
		}
	}
}

// TestUnknownTopicListsTopics: the refusal lists the available topics and points a store word to help types.
func TestUnknownTopicListsTopics(t *testing.T) {
	_, err := HelpFor("fifo")
	if err == nil || !strings.Contains(err.Error(), `no help for "fifo". Topics: create, alter, drop, show, describe, db,`) ||
		!strings.HasSuffix(err.Error(), "Try 'help types' for the stores.") {
		t.Errorf("help fifo: %v", err)
	}
	for _, word := range []string{"fifoStore", "hash", "btreeStore", "heap", "blob"} {
		if _, err := HelpFor(word); err == nil || !strings.Contains(err.Error(), "help types") {
			t.Errorf("help %s must point to help types: %v", word, err)
		}
	}
	_, err = HelpFor("frobnicate")
	if err == nil || !strings.HasSuffix(err.Error(), "Run 'help' for the first five minutes, or 'help all' for every statement.") {
		t.Errorf("help frobnicate: %v", err)
	}
	for _, alias := range []string{"stores", "fields"} {
		page, aliasErr := HelpFor(alias)
		types, _ := HelpFor("types")
		if aliasErr != nil || page != types {
			t.Errorf("help %s is not the types page: %v", alias, aliasErr)
		}
	}
}

// TestVerbPageResultsAreTrue: the line a verb page prints under its example is what the example
// prints, so a page cannot show an output the CLI no longer gives.
func TestVerbPageResultsAreTrue(t *testing.T) {
	for _, verb := range ringVerbs {
		if verb.Result == "" {
			continue
		}
		session, _ := u52Session()
		u82Prepare(context.Background(), t, session, u82Bucket)
		if _, err := session.Execute(context.Background(), `db.User.insert({_id_: "ada", Name: "Ada"})`); err != nil {
			t.Fatal(err)
		}
		if verb.Before != "" {
			if _, err := session.Execute(context.Background(), verb.Before); err != nil {
				t.Fatalf("%s: before %q: %v", verb.Name, verb.Before, err)
			}
		}
		output, err := session.Execute(context.Background(), verb.Example)
		if err != nil || len(output.Lines) == 0 || output.Lines[0] != verb.Result {
			t.Errorf("%s: %q prints %v (%v), the page says %q", verb.Name, verb.Example, output.Lines, err, verb.Result)
		}
	}
}

// TestSixStoreExampleParsesAndRuns: the example 'help types' shows split over two lines is a real
// statement, and the page prints it whole.
func TestSixStoreExampleParsesAndRuns(t *testing.T) {
	session, _ := u52Session()
	if _, err := session.Execute(context.Background(), "create database app"); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Execute(context.Background(), ringSixStores); err != nil {
		t.Errorf("the six-store example is refused: %v", err)
	}
	page, err := HelpFor("types")
	if err != nil {
		t.Fatal(err)
	}
	if flat := strings.Join(strings.Fields(page), " "); !strings.Contains(flat, ringSixStores) {
		t.Errorf("help types does not show the six-store example whole:\n%s", page)
	}
}

// TestHelpTopicGoldens pins every help page: one golden holds each topic a person can ask for.
func TestHelpTopicGoldens(t *testing.T) {
	var out strings.Builder
	for _, topic := range append(u82Topics(), "stores", "frobnicate", "fifo") {
		if topic == "field" {
			continue
		}
		page, err := HelpFor(topic)
		if err != nil {
			page = "Error: " + err.Error() + "\n"
		}
		out.WriteString("##### help " + topic + "\n" + page + "\n")
	}
	assertRingGolden(t, "help-topics", out.String())
}

// TestIndexPageExamplesAreRealExamples: 'help db' prints examples and rows that exist in the table.
func TestIndexPageExamplesAreRealExamples(t *testing.T) {
	known := u82KnownExamples()
	for _, example := range ringDBPage.Examples {
		if !known[example] {
			t.Errorf("help db prints %q, which is no example of any form", example)
		}
	}
	for _, section := range ringDBPage.Sections {
		for _, verb := range section.Verbs {
			if _, ok := ringFormByVerb(verb); !ok {
				t.Errorf("help db lists %q, which is no form", verb)
			}
		}
	}
	for _, verb := range ringVerbs {
		if verb.Before != "" && !known[verb.Before] && !strings.HasPrefix(verb.Before, `db.User["ada"].`) {
			t.Errorf("verb %s: the statement before the example, %q, is not a statement", verb.Name, verb.Before)
		}
	}
}

// TestLiveSixStoreExampleRuns runs the example of 'help types' against the real server.
func TestLiveSixStoreExampleRuns(t *testing.T) {
	session, ctx := u52LiveSession(t)
	u82Prepare(ctx, t, session, u82Databases)
	if _, err := session.Execute(ctx, ringSixStores); err != nil {
		t.Errorf("the server refuses the example help types shows: %v", err)
	}
}

// u82Corpus is every example the tables print, once.
func u82Corpus() []string {
	var corpus []string
	for example := range u82KnownExamples() {
		corpus = append(corpus, example)
	}
	slices.Sort(corpus)
	return corpus
}

// TestParseErrorQuotesHeadline: a refused statement quotes the headline example of its form, never the
// longest, so the one line a developer reads at 3am is the shortest statement that works.
func TestParseErrorQuotesHeadline(t *testing.T) {
	checked := 0
	for _, example := range u82Corpus() {
		words := strings.Fields(example)
		for cut := 1; cut < len(words); cut++ {
			_, err := Parse(strings.Join(words[:cut], " "), "app")
			var refused *ParseError
			if !errors.As(err, &refused) || refused.Form == "" {
				continue
			}
			var form *ringForm
			for i := range ringForms {
				if ringForms[i].Syntax() == refused.Form {
					form = &ringForms[i]
				}
			}
			if form == nil {
				t.Fatalf("%q: the form %q is not in the table", example, refused.Form)
			}
			checked++
			if refused.Example != form.Examples[0].Text || !strings.Contains(refused.Message, "For example '"+form.Examples[0].Text+"'.") {
				t.Errorf("%q cut to %d words quotes %q, the headline of %q is %q", example, cut, refused.Example, form.Verb, form.Examples[0].Text)
			}
		}
	}
	if checked < 100 {
		t.Fatalf("only %d refusals were checked; the cuts are not reaching the forms", checked)
	}
	_, err := Parse("create bucket", "app")
	if err == nil || strings.Contains(err.Error(), "Photos") || !strings.Contains(err.Error(), "'create bucket User { Name string, Purchases fifoStore }'") {
		t.Errorf("create bucket quotes the six-store line instead of the headline: %v", err)
	}
}

// u82PipedStderr runs a piped session and returns its standard error.
func u82PipedStderr(t *testing.T, statements ...string) (string, int) {
	t.Helper()
	session, _ := u52Session()
	var out, errOut bytes.Buffer
	status := session.RunPiped(context.Background(), strings.NewReader(strings.Join(statements, "\n")+"\n"), &out, &errOut)
	return errOut.String(), status
}

// TestPipedErrorsStayOneUnchangedLine: a script's output is grep-friendly and does not change with
// the prompt's two-line errors: one line, the code, the message, in the words of v1.1 before the redesign.
func TestPipedErrorsStayOneUnchangedLine(t *testing.T) {
	cases := []struct{ statement, want string }{
		{"frobnicate", `CLI error: PARSE_ERROR: "frobnicate" is not a statement. Run 'help' to list them, for example 'show databases'`},
		{"creat database app", `CLI error: PARSE_ERROR: "creat" is not a statement. Run 'help' to list them, for example 'show databases'`},
		{"create database app\nuse app\ndrop database app", "CLI error: drop database is not available yet: the ring wire carries create database, create bucket, the show and describe " +
			"statements for databases and buckets, the document and store statements, the user and role statements and the seeding statements, and has no request for it. Run 'help' to list what runs"},
		{"use nope", `CLI error: NOT_FOUND: database "nope" not found. Run 'create database nope'`},
		{"db.User.find()", `CLI error: PARSE_ERROR: find is not a statement: reading one document has one spelling. Run 'db.User["<id>"].get()' to read a document by id, ` +
			`or 'db.User.list({limit: 10})' to see what exists (the CLI does not run this; 'help' lists the statements it runs)`},
		{"show buckets in", `CLI error: PARSE_ERROR: cannot read the statement at character 16: found the end of the statement, expected a database name. ` +
			`The form is 'show buckets [in <db>] [limit <n>] [cursor <hex>]'. For example 'show buckets in app'. Run 'help show' for every form`},
	}
	for _, c := range cases {
		got, status := u82PipedStderr(t, strings.Split(c.statement, "\n")...)
		if got != c.want+"\n" || status == 0 {
			t.Errorf("%s:\n got  %q (status %d)\n want %q", c.statement, got, status, c.want)
		}
		if strings.Count(got, "\n") != 1 {
			t.Errorf("%s: a piped error is not one line: %q", c.statement, got)
		}
	}
}

// TestInteractiveErrorsPrintTheFixOnItsOwnLine: at the prompt a refusal that carries a fix prints the
// problem, then the fix on a line of its own; a parse error labels its parts.
func TestInteractiveErrorsPrintTheFixOnItsOwnLine(t *testing.T) {
	session, _ := u52Session()
	cases := []struct {
		statement string
		want      []string
	}{
		{"create bucket", []string{
			"Error: PARSE_ERROR: cannot read the statement at character 14: found the end of",
			"       the statement, expected a name.",
			"       The form:    create bucket <name> [in <db>] { <Field> <type>, … }",
			"       For example: create bucket User { Name string, Purchases fifoStore }",
			"       Every form:  help create"}},
		{"creat database app", []string{
			`Error: PARSE_ERROR: "creat" is not a statement. Did you mean create?`,
			"       For example: create database app"}},
		{"frobnicate", []string{
			`Error: PARSE_ERROR: "frobnicate" is not a statement.`,
			"       Fix:   Run 'help' for the first five minutes, or 'help all' for every",
			"              statement."}},
		{"use shop", []string{
			`Error: NOT_FOUND: database "shop" not found.`,
			"       Fix:   Run 'create database shop'"}},
		{"drop database app", []string{
			"Error: drop database is not available yet: the ring wire carries create",
			"       database, create bucket, the show and describe statements for databases",
			"       and buckets, the document and store statements, the user and role",
			"       statements and the seeding statements, and has no request for it.",
			"       Fix:   Run 'help' to list what runs"}},
	}
	if _, err := u52Run(session, "create database app", "use app"); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		_, err := session.Execute(context.Background(), c.statement)
		if err == nil {
			t.Fatalf("%s was accepted", c.statement)
		}
		got := ringErrorLines(err)
		if !slices.Equal(got, c.want) {
			t.Errorf("%s:\n got\n%s\n want\n%s", c.statement, strings.Join(got, "\n"), strings.Join(c.want, "\n"))
		}
		for _, line := range got {
			if ringWidth(line) > 80 {
				t.Errorf("%s: a line of %d columns: %q", c.statement, ringWidth(line), line)
			}
		}
	}
	if got := ringErrorLines(errors.New("no database is open")); len(got) != 1 || got[0] != "Error: no database is open" {
		t.Errorf("an error without a fix stays one line: %v", got)
	}
}

// TestDidYouMeanNeedsACloseWord: only a word within two edits of a statement is suggested.
func TestDidYouMeanNeedsACloseWord(t *testing.T) {
	for typed, want := range map[string]string{"creat": "create", "shwo": "show", "desribe": "describe", "uze": "use", "frobnicate": "", "ab": "", "satus": "status", "nodes": ""} {
		got, _ := ringNearestStatement(typed)
		if got != want {
			t.Errorf("nearest statement to %q = %q, want %q", typed, got, want)
		}
	}
}

// TestQuotedFixScanReadsTheClientTexts: the quoted-fix guard reads the client's own refusals in cli/ring*.go
// as well as the server's in cluster/ (the F8 finding: a client text quoted a statement that does not run).
func TestQuotedFixScanReadsTheClientTexts(t *testing.T) {
	quotes := u51QuotedFixes(t)
	for _, want := range []string{"describe bucket %s in %s", "use <db>", "show buckets in app"} {
		if !slices.Contains(quotes, want) {
			t.Errorf("the scan found no %q in the refusal texts of cli/ring*.go and cluster/", want)
		}
	}
	for _, quote := range quotes {
		if strings.Contains(quote, "fifoStore }") && strings.HasPrefix(quote, "alter bucket %s") {
			t.Errorf("the field-type refusal quotes %q, a statement ring mode cannot run", quote)
		}
	}
}

// TestNearestIsInTheVerbTable: nearest is a row of ringVerbs with the five stores that take it, so
// help nearest, completion and 'help types' know it, and blobStore is in no cell.
func TestNearestIsInTheVerbTable(t *testing.T) {
	verb, ok := ringVerbByName("nearest")
	if !ok {
		t.Fatal("nearest is not a row of ringVerbs")
	}
	want := []string{"fifoStore", "lifoStore", "heapStore", "hashStore", "btreeStore"}
	if !slices.Equal(verb.Types, want) {
		t.Errorf("nearest runs on %v, want %v", verb.Types, want)
	}
	if !strings.Contains(HelpAll(), "nearest") {
		t.Error("help all does not list nearest")
	}
}

// TestNearestVerbRunsThroughTheSession: a nearest statement reaches RingBucket.Nearest, not the
// lease path, so the answer is scored lines and never a lease error.
func TestNearestVerbRunsThroughTheSession(t *testing.T) {
	session, _ := u52Session()
	ctx := context.Background()
	u82Prepare(ctx, t, session, u82Bucket)
	if _, err := session.Execute(ctx, `db.User.insert({_id_: "ada", Name: "Ada"})`); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`db.User["ada"].Friends.put("grace", {since: 2024, Vector: {"$binary": {"base64": "AACAPwAAAAA=", "subType": "00"}}})`,
		`db.User["ada"].Friends.put("linus", {since: 2025, Vector: {"$binary": {"base64": "AACAPwAAAAA=", "subType": "00"}}})`,
		`db.User["ada"].Friends.nearest({vector: {"$binary": {"base64": "AACAPwAAAAA=", "subType": "00"}}, k: 3})`,
	} {
		output, err := session.Execute(ctx, statement)
		if err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
		if strings.HasSuffix(statement, "k: 3})") && (len(output.Lines) < 2 || !strings.HasPrefix(output.Lines[0], "1.000000  grace  ")) {
			t.Errorf("nearest prints %v, want two scored lines starting with grace", output.Lines)
		}
	}
}
