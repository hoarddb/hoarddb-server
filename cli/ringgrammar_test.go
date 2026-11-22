package cli

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

// u51Source is a fixed NameSource for the completion tests.
type u51Source struct{}

func (u51Source) Databases() []string { return []string{"app", "shop"} }
func (u51Source) Buckets(database string) []string {
	if database == "app" {
		return []string{"User", "Order"}
	}
	return []string{"Cart"}
}
func (u51Source) Fields(database, bucket string) []string {
	return []string{"Name", "Age", "Purchases"}
}
func (u51Source) StoreFields(database, bucket string) []string { return []string{"Purchases"} }
func (u51Source) Nodes() []string                              { return []string{"node-a", "node-b"} }

// u51Placeholders maps a <placeholder> in a quoted fix to text the grammar accepts. An
// unmapped placeholder fails the test, so a new refusal text must be taught to the table.
var u51Placeholders = map[string]string{
	"db": "app", "database": "app", "bucket": "User", "name": "app", "id": "ada",
	"n": "5", "slot": "5", "rf": "2", "max": "100", "default": "10", "hex": "0a1b",
	"node": "node-b", "node-id": "node-b", "owner": "node-a", "addr": "127.0.0.1:4443",
	"host:port": "127.0.0.1:4443", "addr of a node in this cluster": "127.0.0.1:4443", "seed_addr": "127.0.0.1:4443", "field": "Email", "Field": "Email", "NewField": "Email2", "type": "string",
	"user": "ada", "reader or writer": "writer", "db-manager or reader": "reader", "role": "writer",
	"value": `"x"`, "e": "1", "d": "1", "s": "1",
	"voter": "node-a", "dead voter": "node-b", "IndexName": "ByAuthor", "target": "Account", "tdb": "ref",
}

// u51Instantiate turns a quoted fix into a statement text: formatting verbs and
// <placeholders> become sample names, and the elisions the texts use become
// the content the statement's own form needs.
func u51Instantiate(t *testing.T, quoted string) string {
	t.Helper()
	text, bad := u51InstantiateLenient(quoted)
	if len(bad) > 0 {
		t.Fatalf("quoted fix %q has placeholders %v that u51Placeholders does not map; add them", quoted, bad)
	}
	return text
}

// u51InstantiateLenient is u51Instantiate that reports unmapped placeholders instead of failing.
func u51InstantiateLenient(quoted string) (string, []string) {
	// A fix the parser builds from what the user typed keeps its Go spelling in the source:
	// '" + ringOr(m.argText("bucket"), "User") + "' stands for the typed name or the default.
	quoted = regexp.MustCompile(`" \+ ringOr\(m\.argText\("\w+"\), "(\w+)"\) \+ "`).ReplaceAllString(quoted, "$1")
	text := strings.NewReplacer(`\"`, `"`, `\\`, `\`).Replace(quoted)
	// A join quotes the node and its address as two verbs (F8 of the cluster-version spec):
	// the address sample must be host:port, which the generic "%s" sample is not.
	text = strings.ReplaceAll(text, "join %s@%s", "join node-b@127.0.0.1:4443")
	text = strings.NewReplacer("_id_: %q", `_id_: "ada"`, "[%q]", `["ada"]`, "[%s]", `["ada"]`, "%s", "app", "%q", "app", "%d", "5").Replace(text)
	switch {
	case strings.HasPrefix(text, "create bucket"):
		text = strings.NewReplacer("{ … }", "{ Name string }").Replace(text)
	case strings.HasPrefix(text, "alter bucket"):
		text = strings.NewReplacer("{ … }", "{ add Email string }").Replace(text)
	default:
		text = strings.NewReplacer(`, …}`, `}`, `{…}`, `{Name: "x"}`, `(…)`, `()`).Replace(text)
	}
	text = strings.NewReplacer(`<field>: <value>`, `Name: "x"`).Replace(text)
	var bad []string
	text = regexp.MustCompile(`<([^<>]+)>`).ReplaceAllStringFunc(text, func(match string) string {
		name := match[1 : len(match)-1]
		if sample, ok := u51Placeholders[name]; ok {
			return sample
		}
		bad = append(bad, name)
		return match
	})
	return text, bad
}

var u51VerbWords = map[string]bool{
	"create": true, "alter": true, "drop": true, "show": true, "describe": true, "use": true,
	"join": true, "promote": true, "leave": true, "remove-node": true, "replace-node": true,
	"nodes": true, "status": true, "seeding": true, "grant": true, "user": true, "revoke": true,
}

// u51Quoted returns every command quoted in a Go string of the refusal texts.
// A quote opens after a space, '(' or the double quote that starts a Go string (a refusal text
// continued on a new line), and closes before a space or punctuation,
// so an apostrophe inside a word ("release's") never opens one.
func u51Quoted(line string) []string {
	var out []string
	for i := 0; i < len(line); i++ {
		if line[i] != '\'' || i+1 >= len(line) || line[i+1] == ' ' || (i > 0 && line[i-1] != ' ' && line[i-1] != '(' && line[i-1] != '"') {
			continue
		}
		for j := i + 1; j < len(line); j++ {
			if line[j] == '\'' && line[j-1] != ' ' && (j+1 == len(line) || strings.IndexByte(" .,;:)\"`\\", line[j+1]) >= 0) {
				text := line[i+1 : j]
				first := strings.Fields(text + " x")[0]
				if text == "db." { // readstatement.go: "the bucket's name after 'db.'" names a prefix, not a statement
					i = j
					break
				}
				if u51VerbWords[first] || strings.HasPrefix(first, "db.") {
					out = append(out, text)
				}
				i = j
				break
			}
		}
	}
	return out
}

// u82NotRefusalText are the cli files that print the table, not refusals.
var u82NotRefusalText = map[string]bool{"ringgrammar.go": true, "ringhelp.go": true, "ringhelppages.go": true}

// u51QuotedFixes collects the quoted commands of the refusal texts in cluster,
// cluster/wire and cluster/control, and of the client's own refusals in cli/ring*.go
// (non-test Go files, comments skipped).
func u51QuotedFixes(t *testing.T) []string {
	t.Helper()
	seen := map[string]bool{}
	// The server's ring user texts (both bindings and the cluster-mode store) quote statements
	// too: a user change refused on a cluster member names the read to run.
	for _, pattern := range []string{"../cluster/*.go", "../cluster/wire/*.go", "../cluster/control/*.go", "ring*.go", "../server/ring_users*.go", "../server/auth_cluster.go"} {
		files, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range files {
			// The grammar table and the help pages quote words in prose ('use' selected); the
			// statements they print are the table's examples, which TestEveryHelpExampleRuns runs.
			if strings.HasSuffix(file, "_test.go") || u82NotRefusalText[filepath.Base(file)] {
				continue
			}
			body, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			for _, line := range strings.Split(string(body), "\n") {
				if strings.HasPrefix(strings.TrimSpace(line), "//") {
					continue
				}
				for _, quoted := range u51Quoted(line) {
					if strings.Contains(quoted, "%ss") { // ringlist.go: 'show %ss' with what = database or bucket
						seen[strings.ReplaceAll(quoted, "%ss", "databases")] = true
						seen[strings.ReplaceAll(quoted, "%ss", "buckets")] = true
						continue
					}
					seen[quoted] = true
				}
			}
		}
	}
	var out []string
	for quoted := range seen {
		out = append(out, quoted)
	}
	sort.Strings(out)
	return out
}

// u51KnownBadFixes are quoted commands the refusal texts carry that the
// grammar rightly refuses. Each is a defect of the text, recorded in the build
// doc; the test fails when one stops being quoted or starts parsing, so the
// list cannot rot.
var u51KnownBadFixes = map[string]string{}

// u80UnavailableQuotedFixes are the commands refusal texts quote whose statements parse but
// are refused in ring mode (Unavailable forms). It is the known gap of the quoted-fix guard.
const u80UnavailableQuotedFixes = "alter database <db> { replication_factor: <n> } | " +
	"leave <voter> | remove-node <dead voter> | " +
	"show placement for <id> in <db>.<bucket> | " +
	"show replication for <db>.<bucket> slot <slot> | show replication for <db>._buckets slot <slot>"

// u51Walk asserts that the statement parses, that every literal token of it is
// offered by completion at the point it is typed, and that help lists its form.
func u51Walk(t *testing.T, text string) Statement {
	t.Helper()
	st, err := Parse(text, "app")
	if err != nil {
		t.Fatalf("%q does not parse: %v", text, err)
	}
	if formOf(t, st).Unavailable {
		// An unavailable form parses (for a clear refusal) but is not offered: the default
		// help leaves it out, and 'help <word>' says it is not available yet.
		if ringHelpListsSyntax(HelpAll(), formOf(t, st).Syntax()) {
			t.Errorf("help lists the unavailable form %q", formOf(t, st).Syntax())
		}
		if one, err := HelpFor(st.Verb); err != nil || !strings.Contains(one, "Not available yet") {
			t.Errorf("help %s does not say %q is not available yet: %v %q", st.Verb, st.Verb, err, one)
		}
		return st
	}
	if !ringHelpListsSyntax(HelpAll(), formOf(t, st).Syntax()) {
		t.Errorf("help does not list the form %q of %q", formOf(t, st).Syntax(), text)
	}
	u51CheckCompletion(t, text, st)
	return st
}

func formOf(t *testing.T, st Statement) ringForm {
	t.Helper()
	for _, form := range ringForms {
		if form.Verb == st.Verb {
			return form
		}
	}
	t.Fatalf("statement verb %q has no row in the table", st.Verb)
	return ringForm{}
}

// u51CheckCompletion walks every prefix of text and asserts that the next
// literal token is among the candidates. Tokens inside a fields, changes,
// document or values region are free-form and skipped.
func u51CheckCompletion(t *testing.T, text string, st Statement) {
	t.Helper()
	known := map[string]bool{}
	for _, word := range ringLiterals() {
		known[word] = true
	}
	toks, err := ringLex(text)
	if err != nil {
		t.Fatalf("lex %q: %v", text, err)
	}
	inRegion := func(pos int) bool {
		for _, arg := range st.Args {
			if arg.Span != [2]int{} && pos >= arg.Span[0] && pos < arg.Span[1] {
				return true
			}
		}
		return false
	}
	for cut := 0; cut <= len(text); cut++ {
		var next *ringTok
		for i := range toks {
			if toks[i].end > cut {
				next = &toks[i]
				break
			}
		}
		if next == nil || inRegion(next.start) || !known[next.text] {
			continue
		}
		if next.start > cut && cut > 0 && text[cut-1] != ' ' {
			continue // between a token and the space after it: the next word is not due yet
		}
		prefix := text[:cut]
		candidates := CompleteWith(u51Source{}, prefix, "app")
		found := false
		for _, candidate := range candidates {
			if strings.EqualFold(candidate, next.text) {
				found = true
			}
		}
		if !found {
			t.Errorf("completion of %q (statement %q) lacks the next token %q; got %v", prefix, text, next.text, candidates)
		}
	}
}

func TestOneGrammarForEveryQuotedFix(t *testing.T) {
	quotes := u51QuotedFixes(t)
	if len(quotes) < 20 {
		t.Fatalf("found only %d quoted commands in the refusal texts; the scan is broken: %v", len(quotes), quotes)
	}
	var unavailable []string
	stale := map[string]bool{}
	for quoted := range u51KnownBadFixes {
		stale[quoted] = true
	}
	for _, quoted := range quotes {
		if reason, bad := u51KnownBadFixes[quoted]; bad {
			delete(stale, quoted)
			text, _ := u51InstantiateLenient(quoted)
			if _, err := Parse(text, "app"); err == nil {
				t.Errorf("%q is listed as a known bad fix (%s) but parses; remove it from the list", quoted, reason)
			}
			continue
		}
		st := u51Walk(t, u51Instantiate(t, quoted))
		if len(formOf(t, st).Fixes) == 0 {
			t.Errorf("%q is quoted by a refusal but form %q declares no Fixes", quoted, st.Verb)
		}
		if formOf(t, st).Unavailable {
			unavailable = append(unavailable, quoted)
		}
	}
	// A quoted fix of an unavailable statement parses, but running it is refused: the list is
	// the known gap, pinned so it only shrinks when the ring wire gains the request.
	if got := strings.Join(unavailable, " | "); got != u80UnavailableQuotedFixes {
		t.Errorf("quoted fixes of statements ring mode cannot run yet changed:\n got  %s\n want %s", got, u80UnavailableQuotedFixes)
	}
	for quoted := range stale {
		t.Errorf("known bad fix %q is no longer quoted anywhere; remove it from the list", quoted)
	}
}

func TestEveryRefusalFixParses(t *testing.T) {
	for _, form := range ringForms {
		for _, fix := range form.Fixes {
			text := u51Instantiate(t, fix)
			st := u51Walk(t, text)
			if st.Verb != form.Verb {
				t.Errorf("fix %q of form %q parses as %q", fix, form.Verb, st.Verb)
			}
		}
	}
	required := []string{
		"nodes", "show buckets in <db>", "show databases", "describe bucket <bucket> in <db>",
		"create bucket <bucket> in <db> { <Field> string }", "alter bucket <bucket> { add <field> <type> }",
		"alter database <db> { replication_factor: <n> }", "create database <name>", "join <node>@<addr>",
		"replace-node <node> --with <node>@<addr>", "replace-node <node> --with <node>@<addr> --accept-loss",
		"status seeding", "show placement for <id> in <db>.<bucket>", "show replication for <db>.<bucket> slot <slot>",
		"show replication for <db>._buckets slot <slot>", `db.<bucket>["<id>"].get()`,
		"grant role <role>@<database> to <user>", "grant role <reader or writer>@<database> to <user>",
		"grant role <db-manager or reader>@<database> to <user>",
		"db.<bucket>.list({limit: 10})", `db.<bucket>.upsert({_id_: "<id>", …})`,
		`db.<bucket>.insert({_id_: "<id>", <field>: <value>})`, `db.<bucket>.delete({_id_: "<id>"})`,
		"create bucket <bucket> { <Field> string, index <IndexName> (<Field>, <Field> desc) }",
		"alter bucket <bucket> { add index <IndexName> (<Field>) }",
	}
	declared := map[string]bool{}
	for _, form := range ringForms {
		for _, fix := range form.Fixes {
			declared[fix] = true
		}
	}
	for _, fix := range required {
		if !declared[fix] {
			t.Errorf("no form declares the fix %q that the specs quote", fix)
		}
	}
}

func TestHelpAndCompletionMatchTheParser(t *testing.T) {
	if len(ringForms) < 25 {
		t.Fatalf("the table has %d forms; the specs name at least 25 statements", len(ringForms))
	}
	all := HelpAll()
	for _, form := range ringForms {
		if len(form.Examples) == 0 {
			t.Errorf("form %q has no example", form.Verb)
		}
		if form.Spec == "" || form.Long == "" || form.Short == "" || form.Group == "" {
			t.Errorf("form %q lacks a group, short, long or spec section", form.Verb)
		}
		if form.Unavailable {
			if ringHelpListsSyntax(Help(), form.Syntax()) || ringHelpListsSyntax(all, form.Syntax()) {
				t.Errorf("help lists the unavailable form %q", form.Verb)
			}
		} else if !ringHelpListsSyntax(all, form.Syntax()) {
			t.Errorf("help all lacks the syntax line of %q: %q", form.Verb, form.Syntax())
		}
		for _, sample := range form.Examples {
			example := sample.Text
			if form.Verb == "field" {
				if !u82OnAVerbPage(example) {
					t.Errorf("example %q of the store verbs is on no verb page", example)
				}
			} else if !form.Unavailable && !u82PageHasLine(form.Verb, example) {
				t.Errorf("help %s lacks the example %q as a line of its own", form.Verb, example)
			}
			st := u51Walk(t, example)
			if st.Verb != form.Verb {
				t.Errorf("example %q of form %q parses as %q", example, form.Verb, st.Verb)
			}
			if again, err := Parse(st.String(), "app"); err != nil || again.String() != st.String() {
				t.Errorf("example %q renders as %q which parses to %q, %v", example, st.String(), again.String(), err)
			}
		}
		if form.Verb == "field" {
			continue
		}
		one, err := HelpFor(form.Verb)
		if err != nil || !strings.Contains(one, form.Syntax()) {
			t.Errorf("help %s does not show %q: %v", form.Verb, form.Syntax(), err)
		}
	}
	// Every top-level word completion offers starts a form, and every form's first word is offered.
	first := map[string]bool{}
	for _, form := range ringForms {
		if !form.Unavailable {
			first[regexp.MustCompile(`^[a-z-]+`).FindString(form.Syntax())] = true
		}
	}
	offered := Completions("", "app")
	for _, word := range offered {
		if !first[word] {
			t.Errorf("completion offers %q which starts no form", word)
		}
	}
	for word := range first {
		if !u51Contains(offered, word) {
			t.Errorf("completion of an empty line lacks %q", word)
		}
	}
	if _, err := HelpFor("frobnicate"); err == nil || !strings.Contains(err.Error(), "Run 'help'") {
		t.Errorf("an unknown topic must say how to list the topics: %v", err)
	}
	for _, page := range []string{Help(), HelpAll()} {
		if strings.Contains(page, "e.g.") {
			t.Errorf("a help page carries an e.g. label:\n%s", page)
		}
	}
	types, err := HelpFor("types")
	if err != nil {
		t.Fatal(err)
	}
	for _, word := range ringTypeWords() {
		if !strings.Contains(types, word) {
			t.Errorf("help types does not name the type word %q", word)
		}
	}
}

func TestUseMakesInOptional(t *testing.T) {
	for _, text := range []string{"show buckets", "describe bucket User", "drop bucket User", "create bucket User { Name string }", "alter bucket User { drop Age }", `db.User["ada"].get()`, "db.User.count()"} {
		st, err := Parse(text, "app")
		if err != nil {
			t.Fatalf("%q with a selected database: %v", text, err)
		}
		database, ok := st.Arg("database")
		if !ok || database.Text != "app" || !database.Implied {
			t.Errorf("%q: database arg = %+v, want implied app", text, database)
		}
		_, err = Parse(text, "")
		var parseErr *ParseError
		if err == nil || !errorsAs(err, &parseErr) || !strings.Contains(err.Error(), "use <db>") {
			t.Errorf("%q with no database must say to run 'use <db>': %v", text, err)
		}
		if strings.Contains(text, "bucket") && !strings.Contains(err.Error(), " in ") {
			t.Errorf("%q: the refusal must also offer 'in <db>': %v", text, err)
		}
	}
	st, err := Parse("show buckets in shop limit 5 cursor 0a1b", "app")
	if err != nil {
		t.Fatal(err)
	}
	if database, _ := st.Arg("database"); database.Text != "shop" || database.Implied {
		t.Errorf("an explicit 'in shop' must win over the selected database: %+v", database)
	}
}

func errorsAs(err error, target **ParseError) bool {
	parseErr, ok := err.(*ParseError)
	if ok {
		*target = parseErr
	}
	return ok
}

func TestParseTypedStatements(t *testing.T) {
	st, err := Parse(`create bucket User in app { Name string, Age int, Purchases fifoStore, Photos lifoStore, Friends hashStore, Recipes btreeStore, Books heapStore, Data blobStore }`, "")
	if err != nil {
		t.Fatal(err)
	}
	fields, _ := st.Arg("fields")
	if st.Verb != "create bucket" || len(fields.Fields) != 8 || (fields.Fields[2].Name != "Purchases" || fields.Fields[2].Type != "fifoStore") {
		t.Errorf("create bucket parsed to %+v", st)
	}
	st, err = Parse(`alter bucket User { add Email string, drop Age }`, "app")
	if err != nil {
		t.Fatal(err)
	}
	changes, _ := st.Arg("changes")
	if len(changes.Changes) != 2 || !changes.Changes[0].Add || changes.Changes[0].Type != "string" || changes.Changes[1].Add || changes.Changes[1].Name != "Age" {
		t.Errorf("alter bucket parsed to %+v", changes)
	}
	st, err = Parse(`db.User.list({filter: {Name: "Ada", Age: 36, Score: -1.5, Live: true, Gone: null}, limit: 10, cursor: "0a1b"})`, "app")
	if err != nil {
		t.Fatal(err)
	}
	doc, _ := st.Arg("doc")
	if st.Verb != "list" || len(doc.Doc.Entries) != 3 || doc.Doc.Entries[0].Key != "filter" || len(doc.Doc.Entries[0].Value.Entries) != 5 ||
		doc.Doc.Entries[1].Value.Int != 10 || doc.Doc.Entries[0].Value.Entries[2].Value.Float != -1.5 {
		t.Errorf("list parsed to %+v", doc)
	}
	st, err = Parse(`db.User["ada"].Purchases.push({Item: "coffee", Tags: ["a", "b"]})`, "app")
	if err != nil {
		t.Fatal(err)
	}
	field, _ := st.Arg("field")
	verb, _ := st.Arg("verb")
	id, _ := st.Arg("id")
	values, _ := st.Arg("values")
	if st.Verb != "field" || field.Text != "Purchases" || verb.Text != "push" || id.Text != "ada" || len(values.Values) != 1 {
		t.Errorf("field push parsed to %+v", st)
	}
	st, err = Parse(`replace-node node-b --with node-d@127.0.0.1:4443 --accept-loss`, "")
	if err != nil {
		t.Fatal(err)
	}
	addr, _ := st.Arg("addr")
	acceptLoss, _ := st.Arg("accept_loss")
	if addr.Text != "127.0.0.1:4443" || acceptLoss.Kind != ArgFlag {
		t.Errorf("replace-node parsed to %+v", st)
	}
	st, err = Parse(`create database app { replication_factor: 2 };`, "")
	if err != nil {
		t.Fatal(err)
	}
	if rf, _ := st.Arg("rf"); rf.Int != 2 {
		t.Errorf("create database parsed to %+v", st)
	}
	st, err = Parse("SHOW DATABASES", "")
	if err != nil || st.Verb != "show databases" {
		t.Errorf("keywords are case-insensitive: %v %+v", err, st)
	}
}

// TestParseOrderedIndexDeclarations reads the index entries of a create or alter body, and the
// field named index that must keep parsing as a field.
func TestParseOrderedIndexDeclarations(t *testing.T) {
	cases := []struct {
		line        string
		wantFields  []FieldDecl
		wantIndexes []IndexDecl
		wantDrops   []string
		wantErr     string
	}{
		{line: "create bucket Post { Author string, Ts datetime, index ByAuthor (Author, Ts desc) }",
			wantFields:  []FieldDecl{{Name: "Author", Type: "string"}, {Name: "Ts", Type: "datetime"}},
			wantIndexes: []IndexDecl{{Name: "ByAuthor", Components: []IndexComponentDecl{{Field: "Author"}, {Field: "Ts", Desc: true}}}}},
		{line: "create bucket Z { index string }", wantFields: []FieldDecl{{Name: "index", Type: "string"}}},
		{line: "alter bucket Post { add index ByAuthor (Author) }",
			wantIndexes: []IndexDecl{{Name: "ByAuthor", Components: []IndexComponentDecl{{Field: "Author"}}}}},
		{line: "alter bucket Post { drop index ByAuthor }", wantDrops: []string{"ByAuthor"}},
		{line: "create bucket Z { index I (A, B, C, D, E) }", wantErr: "at most 4"},
	}
	for _, c := range cases {
		st, err := Parse(c.line, "app")
		if c.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("%q: want a refusal containing %q, got %v", c.line, c.wantErr, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: %v", c.line, err)
			continue
		}
		body, ok := st.Arg("fields")
		if !ok {
			body, _ = st.Arg("changes")
		}
		if !reflect.DeepEqual(body.Fields, c.wantFields) || !reflect.DeepEqual(body.Indexes, c.wantIndexes) || !reflect.DeepEqual(body.DropIndexes, c.wantDrops) {
			t.Errorf("%q parsed to fields %+v, indexes %+v, drops %v", c.line, body.Fields, body.Indexes, body.DropIndexes)
		}
	}
}

// TestOrderedIndexRefusalsQuoteTheGrammar: every refusal of an index entry names the problem and
// quotes a statement that parses back, so the fix in the text is one the grammar accepts.
func TestOrderedIndexRefusalsQuoteTheGrammar(t *testing.T) {
	cases := []struct {
		line, want string
	}{
		{"create bucket Post { Author string, index ByAuthor Author }", "in parentheses"},
		{"create bucket Post { index ByAuthor () }", "one to four fields"},
		{"create bucket Post { index ByAuthor (A, B, C, D, E) }", "at most 4 fields"},
		{"create bucket Post { index ByAuthor (A asc) }", "write desc or nothing"},
		{"create bucket Post { index ByAuthor (A, B }", "close the index's field list with ')'"},
		{"create bucket Post { index ByAuthor (A,) }", "a comma must be followed by a field"},
		{"alter bucket Post { add index ByAuthor }", "in parentheses"},
	}
	quoted := regexp.MustCompile(`'((?:create|alter) bucket [^']*)'`)
	for _, c := range cases {
		_, err := Parse(c.line, "app")
		var parseErr *ParseError
		if err == nil || !errorsAs(err, &parseErr) {
			t.Errorf("%q: want a ParseError, got %v", c.line, err)
			continue
		}
		if !strings.Contains(parseErr.Message, c.want) {
			t.Errorf("%q: refusal %q lacks %q", c.line, parseErr.Message, c.want)
		}
		// Every refusal also prints its form, whose '…' is a placeholder, not a fix; the fix is the other quote.
		fixes := 0
		for _, m := range quoted.FindAllStringSubmatch(parseErr.Message, -1) {
			if strings.Contains(m[1], "…") {
				continue
			}
			fixes++
			if _, err := Parse(m[1], "app"); err != nil {
				t.Errorf("%q: the fix %q in the refusal does not parse: %v", c.line, m[1], err)
			}
		}
		if fixes == 0 {
			t.Errorf("%q: refusal %q quotes no create or alter statement to fix it with", c.line, parseErr.Message)
		}
	}
}

// TestOrderedIndexGrammarOnTheHelpPage: help and Tab show the index entries the parser reads.
func TestOrderedIndexGrammarOnTheHelpPage(t *testing.T) {
	for _, verb := range []string{"create bucket", "alter bucket"} {
		page, err := HelpFor(verb)
		if err != nil {
			t.Fatal(err)
		}
		// The paragraph wraps the grammar across lines, so compare the words, not the line breaks.
		if !strings.Contains(strings.Join(strings.Fields(page), " "), "index <IndexName> ( <Field> [desc]") {
			t.Errorf("help %s does not show the index entry:\n%s", verb, page)
		}
	}
	if offered := CompleteWith(nil, "create bucket Post { Author string, ", "app"); !u51Contains(offered, "index") {
		t.Errorf("completion after a field lacks the keyword index: %v", offered)
	}
}

func TestParseRefusalsNameThePositionAndTheFix(t *testing.T) {
	cases := []struct {
		line     string
		selected string
		wantPos  int
		contains []string
	}{
		{"create bucket User { Name }", "app", 27, []string{"Name", "type", "fifoStore"}},
		{"create bucket User { Name fifostore }", "app", 27, []string{`"fifostore"`, "fifoStore", "create bucket User"}},
		{"create bucket User { Name integer }", "app", 27, []string{`"integer"`, `"int"`}},
		{"create bucket User { Name object { a int } }", "app", 34, []string{"object", "any"}},
		{"create bucket User { Name array<int> }", "app", 32, []string{"array"}},
		{"create bucket User { type: hash }", "app", 0, []string{"a bucket has no type", "create bucket User { Name string }"}},
		{"create database app rf 2", "", 21, []string{"replication_factor", "storage: disk", "create database app { replication_factor: 2 }", "create database app"}},
		{"alter database app rf 2", "", 20, []string{"replication_factor", "storage: disk", "alter database app { replication_factor: 2 }"}},
		{"alter database app", "", 0, []string{"replication_factor", "alter database app { replication_factor: 2 }"}},
		{"show buckets limit", "app", 19, []string{"number", "show buckets in app"}},
		{"frobnicate", "", 1, []string{`"frobnicate"`, "Run 'help'", "show databases"}},
		{"", "", 0, []string{"Run 'help'"}},
		{"db.User.find({})", "app", 9, []string{"find", "db.User.list({limit: 10})"}},
		{`db.User.get("ada")`, "app", 0, []string{"db.User[\"ada\"].get()"}},
		{`db.User.insert({Name: ada})`, "app", 23, []string{"string", "number", "true", "{"}},
		{`db.User.insert({Name: "unterminated})`, "app", 23, []string{"unterminated", "closing"}},
		{"show buckets in app extra", "app", 21, []string{"extra", "end of the statement"}},
		{"use app; use shop", "", 10, []string{"one statement"}},
		{"join node-b", "", 12, []string{"@", "join node-b@127.0.0.1:4443"}},
	}
	for _, c := range cases {
		_, err := Parse(c.line, c.selected)
		parseErr, ok := err.(*ParseError)
		if !ok {
			t.Errorf("%q: want a *ParseError, got %v", c.line, err)
			continue
		}
		if c.wantPos != 0 && parseErr.Pos != c.wantPos {
			t.Errorf("%q: position = %d, want %d (%s)", c.line, parseErr.Pos, c.wantPos, parseErr.Message)
		}
		for _, want := range c.contains {
			if !strings.Contains(parseErr.Message, want) {
				t.Errorf("%q: message lacks %q: %s", c.line, want, parseErr.Message)
			}
		}
	}
}

func TestCompletionOfNames(t *testing.T) {
	src := u51Source{}
	want := map[string][]string{
		"use ":                           {"app", "shop"},
		"use s":                          {"shop"},
		"db.":                            {"Order", "User"},
		"db.U":                           {"User"},
		`db.User["ada"].`:                {"Purchases"},
		"describe bucket ":               {"Order", "User"},
		"describe bucket User in ":       {"app", "shop"},
		"show buckets in s":              {"shop"},
		"create bucket User { Name ":     {"string", "fifoStore", "int"},
		"db.User.list({":                 {"cursor", "filter", "limit"},
		"db.User.list({limit: 5, ":       {"cursor", "filter", "limit"},
		"show buckets in app ":           {"cursor", "limit"},
		"help ":                          {"create", "db", "show"},
		"alter bucket User { ":           {"add", "drop"},
		"alter bucket User { add Email ": {"string", "fifoStore", "int"},
		"drop bucket ":                   {"Order", "User"},
		"drop bucket User in ":           {"app", "shop"},
	}
	// join and promote run in ring mode since b8e4c72d7, so promote completes the member names; the
	// statements below still have no wire request.
	if got := CompleteWith(src, "promote ", "app"); !u51Contains(got, "node-a") || !u51Contains(got, "node-b") {
		t.Errorf("completion of \"promote \" lacks the members; got %v", got)
	}
	for _, prefix := range []string{"alter database ", "drop database "} {
		if got := CompleteWith(src, prefix, "app"); len(got) != 0 {
			t.Errorf("completion of %q offers %v, but the statement is not available in ring mode", prefix, got)
		}
	}
	for prefix, wants := range want {
		got := CompleteWith(src, prefix, "app")
		for _, w := range wants {
			if !u51Contains(got, w) {
				t.Errorf("completion of %q lacks %q; got %v", prefix, w, got)
			}
		}
	}
	for _, unwanted := range []struct{ prefix, word string }{
		{"alter bucket User { drop ", "add"},
		{"db.User.list({", "Name"},
		{"use ", "create"},
		{"create bucket User { Name ", "add"},
	} {
		if u51Contains(CompleteWith(src, unwanted.prefix, "app"), unwanted.word) {
			t.Errorf("completion of %q must not offer %q", unwanted.prefix, unwanted.word)
		}
	}
	if got := Completions("db.", "app"); len(got) != 0 {
		t.Errorf("with no name source, db. offers no bucket names: %v", got)
	}
}

func u51Contains(list []string, word string) bool {
	for _, entry := range list {
		if entry == word {
			return true
		}
	}
	return false
}

func TestTypeWordsAreCanonical(t *testing.T) {
	for _, word := range []string{"string", "int32", "int", "double", "decimal", "number", "bool", "datetime", "bsontimestamp", "binary", "uuid", "objectid", "object", "array", "regex", "null", "any", "fifoStore", "lifoStore", "hashStore", "btreeStore", "heapStore", "blobStore"} {
		if _, err := Parse("create bucket T { F "+word+" }", "app"); err != nil {
			t.Errorf("type word %q must parse: %v", word, err)
		}
	}
	for word, canonical := range map[string]string{"integer": "int", "int64": "int", "float": "double", "boolean": "bool", "date": "datetime", "timestamp": "datetime", "fifostore": "fifoStore", "BLOBSTORE": "blobStore", "FifoStore": "fifoStore"} {
		_, err := Parse("create bucket T { F "+word+" }", "app")
		if err == nil || !strings.Contains(err.Error(), `"`+canonical+`"`) {
			t.Errorf("type word %q must be refused naming %q: %v", word, canonical, err)
		}
	}
}

// u51Corpus is every valid statement the table knows: examples and instantiated fixes.
func u51Corpus(t *testing.T) []string {
	t.Helper()
	var corpus []string
	for _, form := range ringForms {
		for _, example := range form.Examples {
			corpus = append(corpus, example.Text)
		}
		for _, fix := range form.Fixes {
			corpus = append(corpus, u51Instantiate(t, fix))
		}
	}
	return corpus
}

func TestParseMutationsNeverPanicOrHang(t *testing.T) {
	corpus := u51Corpus(t)
	if len(corpus) < 40 {
		t.Fatalf("corpus of %d statements is too small to mutate", len(corpus))
	}
	rng := rand.New(rand.NewSource(51))
	alphabet := []byte(`{}[]().,:;@"' -_$0123456789abcxyzINUdb` + "\n\t\\")
	started := time.Now()
	accepted := 0
	for i := 0; i < 100000; i++ {
		line := []byte(corpus[rng.Intn(len(corpus))])
		for round := rng.Intn(4) + 1; round > 0 && len(line) > 0; round-- {
			at := rng.Intn(len(line))
			switch rng.Intn(5) {
			case 0:
				line = append(line[:at], line[at+1:]...)
			case 1:
				line = append(line[:at], append([]byte{alphabet[rng.Intn(len(alphabet))]}, line[at:]...)...)
			case 2:
				line[at] = alphabet[rng.Intn(len(alphabet))]
			case 3:
				line = line[:at]
			default:
				line = append(line, line[at:]...)
			}
		}
		call := time.Now()
		st, err := Parse(string(line), "app")
		Completions(string(line), "app")
		if took := time.Since(call); took > time.Second {
			t.Fatalf("%q took %v", line, took)
		}
		if err != nil {
			if _, ok := err.(*ParseError); !ok {
				t.Fatalf("%q: error is %T, want *ParseError", line, err)
			}
			continue
		}
		accepted++
		text := st.String()
		again, err := Parse(text, "app")
		if err != nil || again.String() != text {
			t.Fatalf("accepted %q renders as %q which re-parses to %q, %v", line, text, again.String(), err)
		}
	}
	t.Logf("%d of 100000 mutations were still valid statements", accepted)
	if accepted == 0 {
		t.Fatal("no mutation was accepted; the mutator is too destructive to prove the re-encode property")
	}
	if time.Since(started) > 90*time.Second {
		t.Fatalf("100k mutations took %v", time.Since(started))
	}
}

func TestDeeplyNestedLiteralsAreRefused(t *testing.T) {
	line := "db.User.insert(" + strings.Repeat("{a: ", 5000) + "1" + strings.Repeat("}", 5000) + ")"
	if _, err := Parse(line, "app"); err == nil || !strings.Contains(err.Error(), "nested") {
		t.Errorf("a 5000-deep document must be refused as nested too deep: %v", err)
	}
}

func TestLexerRejectsWhatItCannotSplit(t *testing.T) {
	for _, line := range []string{`db.User.insert({a: "x)`, "show # databases", "show \x00 databases"} {
		if _, err := ringLex(line); err == nil {
			t.Errorf("lexing %q must fail", line)
		}
	}
	toks, err := ringLex(`db.User["ada"].get()`)
	if err != nil || len(toks) != 10 {
		t.Fatalf("lex = %v, %v", toks, err)
	}
	var texts []string
	for _, tok := range toks {
		texts = append(texts, tok.text)
	}
	if strings.Join(texts, "|") != `db|.|User|[|ada|]|.|get|(|)` {
		t.Errorf("tokens = %v", texts)
	}
}

// TestParseStoreAggregateClause reads 'with (…)' after a store word: the three aggregate forms, dotted
// paths, the keyword in any case, and a field named with, which still parses as a field.
func TestParseStoreAggregateClause(t *testing.T) {
	cases := []struct {
		text string
		want []AggregateDecl
	}{
		{`create bucket Ledger in app { Expenses btreeStore with (sum Amount) }`, []AggregateDecl{{Kind: "sum", Field: "Amount"}}},
		{`create bucket Ledger in app { Lines hashStore with (sum Total, sum Total by Category, count by Category) }`,
			[]AggregateDecl{{Kind: "sum", Field: "Total"}, {Kind: "sum by", Field: "Total", By: "Category"}, {Kind: "count by", By: "Category"}}},
		{`create bucket Ledger in app { Lines fifoStore with (sum Amount.Cents by Shop.Region.Code) }`,
			[]AggregateDecl{{Kind: "sum by", Field: "Amount.Cents", By: "Shop.Region.Code"}}},
		{`create bucket Ledger in app { Lines fifoStore WITH (SUM Amount) }`, []AggregateDecl{{Kind: "sum", Field: "Amount"}}},
		{`create bucket Ledger in app { Lines fifoStore, Total double }`, nil},
	}
	for _, c := range cases {
		st, err := Parse(c.text, "")
		if err != nil {
			t.Errorf("%q: %v", c.text, err)
			continue
		}
		fields, _ := st.Arg("fields")
		if len(fields.Fields) == 0 || !reflect.DeepEqual(fields.Fields[0].Aggregates, c.want) {
			t.Errorf("%q parsed to %+v, want aggregates %+v", c.text, fields.Fields, c.want)
		}
	}

	st, err := Parse(`create bucket Ledger in app { with string, Lines fifoStore }`, "")
	if err != nil {
		t.Fatalf("a field named with must still parse: %v", err)
	}
	fields, _ := st.Arg("fields")
	if len(fields.Fields) != 2 || fields.Fields[0].Name != "with" || fields.Fields[0].Type != "string" || fields.Fields[1].Name != "Lines" {
		t.Errorf("a field named with parsed to %+v", fields.Fields)
	}

	if _, err := Parse(`alter bucket Ledger in app { add Lines hashStore with (sum Amount) }`, ""); err == nil {
		t.Error("alter bucket must not parse an aggregate clause: adding an aggregate to a live store is a later step")
	}

	completions := CompleteWith(nil, "create bucket Ledger in app { Lines btreeStore ", "")
	if !u51Contains(completions, "with") {
		t.Errorf("after a store word completion must offer with, got %v", completions)
	}
	completions = CompleteWith(nil, "create bucket Ledger in app { Total double ", "")
	if u51Contains(completions, "with") {
		t.Errorf("after a scalar word completion must not offer with, got %v", completions)
	}
}

// TestAggregateRefusalsQuoteTheGrammar refuses each malformed clause with a message that names the
// thing and quotes a fix. Every quoted command must parse back, and the gate that walks every quoted
// fix in the ring sources runs them too.
func TestAggregateRefusalsQuoteTheGrammar(t *testing.T) {
	nine := "sum A1"
	for i := 2; i <= 9; i++ {
		nine += fmt.Sprintf(", sum A%d", i)
	}
	cases := []struct {
		line string
		want string
	}{
		{`create bucket Ledger { Total double with (sum Amount) }`, `'with' goes after a store`},
		{`create bucket Ledger { Data blobStore with (sum Amount) }`, "a blob element has no fields"},
		{`create bucket Ledger { Lines hashStore with sum Amount }`, "in parentheses"},
		{`create bucket Ledger { Lines hashStore with () }`, "the aggregate list is empty"},
		{`create bucket Ledger { Lines hashStore with (sum Amount }`, "not closed"},
		{`create bucket Ledger { Lines hashStore with (count) }`, "every store already counts its elements"},
		{`create bucket Ledger { Lines hashStore with (` + nine + `) }`, "at most 8 aggregates"},
		{`create bucket Ledger { Lines hashStore with (sum Total, sum Total) }`, "AGGREGATE_DUPLICATE"},
		{`create bucket Ledger { Lines hashStore with (sum) }`, "sum needs a field"},
		{`create bucket Ledger { Lines hashStore with (sum Net-Total) }`, `"Net-Total" is not a field path`},
		{`create bucket Ledger { Lines hashStore with (sum A.B.C.D) }`, "at most 3 names deep"},
		{`create bucket Ledger { Lines hashStore with (sum Total by) }`, "by needs a field path"},
	}
	for _, c := range cases {
		_, err := Parse(c.line, "app")
		parseErr, ok := err.(*ParseError)
		if !ok {
			t.Errorf("%q: want a *ParseError, got %v", c.line, err)
			continue
		}
		if !strings.Contains(parseErr.Message, c.want) {
			t.Errorf("%q: refusal lacks %q: %s", c.line, c.want, parseErr.Message)
		}
		if !strings.Contains(parseErr.Message, "create bucket") {
			t.Errorf("%q: refusal does not quote the create form: %s", c.line, parseErr.Message)
		}
		fixes := regexp.MustCompile(`'((?:create bucket|db\.)[^']*)'`).FindAllStringSubmatch(parseErr.Problem, -1)
		if len(fixes) == 0 {
			t.Errorf("%q: refusal quotes no fix in its problem line: %s", c.line, parseErr.Problem)
		}
		for _, fix := range fixes {
			if _, err := Parse(fix[1], "app"); err != nil {
				t.Errorf("%q: the fix the refusal quotes, %q, does not parse: %v", c.line, fix[1], err)
			}
		}
	}
}

// ringHelpListsSyntax reports whether help has a line that starts with the
// syntax, indented two spaces and followed by the end of the line or the
// description column.
func ringHelpListsSyntax(help, syntax string) bool {
	for _, line := range strings.Split(help, "\n") {
		if rest, ok := strings.CutPrefix(line, "  "+syntax); ok && (rest == "" || strings.HasPrefix(rest, "  ")) {
			return true
		}
	}
	return false
}

func TestParseDatabaseOptions(t *testing.T) {
	st, err := Parse("create database Cache { replication_factor: 1, storage: memory }", "")
	if err != nil {
		t.Fatal(err)
	}
	if rf, ok := st.Arg("rf"); !ok || rf.Kind != ArgInt || rf.Int != 1 {
		t.Errorf("rf parsed to %+v", rf)
	}
	if storage, ok := st.Arg("storage"); !ok || storage.Kind != ArgText || storage.Text != "memory" {
		t.Errorf("storage parsed to %+v", storage)
	}
	if _, ok := st.Arg("options"); ok {
		t.Errorf("the composite argument must yield rf and storage, not one argument named options: %+v", st.Args)
	}
	refused := map[string]string{
		"create database Cache { storage: memory, storage: disk }": `database option "storage" is given twice. Give each once`,
		"create database Cache { size: 3 }":                        `database option "size" is not one of replication_factor, storage. Write { replication_factor: <n>, storage: disk }`,
		"create database Cache { storage: ram }":                   `storage "ram" is not a kind. Use storage: disk`,
		"create database Cache { replication_factor: x }":          `"x" is not a number: write a whole number such as 10`,
	}
	for line, want := range refused {
		_, err := Parse(line, "")
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Parse(%q) error = %v, want text %q", line, err, want)
		}
	}
}

func TestCreateDatabaseOptionsEitherOrder(t *testing.T) {
	for _, line := range []string{
		"create database Cache { storage: memory, replication_factor: 1 }",
		"create database Cache { replication_factor: 1, storage: memory }",
		"create database Cache { storage: disk }",
		"create database Cache { replication_factor: 2 }",
		"create database Cache",
	} {
		if _, err := Parse(line, ""); err != nil {
			t.Errorf("Parse(%q): %v", line, err)
		}
	}
}

func TestAlterDatabaseParsesTheSameOptions(t *testing.T) {
	st, err := Parse("alter database app { storage: memory }", "")
	if err != nil {
		t.Fatal(err)
	}
	if storage, _ := st.Arg("storage"); storage.Text != "memory" {
		t.Errorf("alter database parsed to %+v", st.Args)
	}
	if _, err := Parse("alter database app { replication_factor: 2 }", ""); err != nil {
		t.Errorf("the old alter form no longer parses: %v", err)
	}
}

// TestCreateDatabaseHelpDoesNotOfferStorageMemory: the memory store is not built, so the grammar,
// its long text, its examples and its suggested fixes offer disk only. The word still parses, so a
// developer who types it reads the refusal, not a parse error.
func TestCreateDatabaseHelpDoesNotOfferStorageMemory(t *testing.T) {
	for _, form := range ringForms {
		if form.Verb != "create database" {
			continue
		}
		offered := []string{form.Long}
		for _, example := range form.Examples {
			offered = append(offered, example.Text, example.Note)
		}
		offered = append(offered, form.Fixes...)
		for _, text := range offered {
			if strings.Contains(strings.ToLower(text), "memory") {
				t.Errorf("create database help still offers memory: %q", text)
			}
		}
		return
	}
	t.Fatal("no create database form")
}

// TestNoQuotedFixDeclaresABucketType: a bucket has no type in the ring ("a bucket has no type:
// declare its fields"), so a refusal that quotes `create bucket ... { type: ... }` as the fix
// sends the user to a statement the ring refuses. The grammar test above only checks that a
// quoted fix parses, and this one does parse.
func TestNoQuotedFixDeclaresABucketType(t *testing.T) {
	for _, quoted := range u51QuotedFixes(t) {
		if strings.HasPrefix(quoted, "create bucket") && strings.Contains(quoted, "type:") {
			t.Errorf("a refusal quotes %q as a fix, but the ring refuses a bucket type", quoted)
		}
	}
}
