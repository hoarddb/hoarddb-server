package cli

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/internal/repofile"
	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// This file is the gate for the defect class that produced seven defects in one
// session: a document — the help, or a page of the user docs — showing a verb
// the CLI refuses. A developer copies what the documentation shows, gets a
// refusal, and has no reason to doubt the documentation.
//
// The existing claims gate (website/scripts/claims-check.sh, Leg 2) covers the
// plain bucket form only. Its pattern is
// `db\.[A-Za-z_][A-Za-z0-9_.]*\.[a-z_]+\(`, which matches `db.Users.find(` and
// none of the 112 store-field forms in the user docs, because those carry a
// bracketed document key; and its verb list is grepped from cli/handlers.go,
// while the store-field verbs live in cli/declare.go. Every defect this file
// guards was a store-field form. So the store-field half is checked here, in
// the package that owns the parser and the dispatch.
//
// The verdict is deliberately narrow: a documented form fails only when the CLI
// refuses it as a *verb* — "unknown bucket method", "is not a field-store
// verb", or a parse error. "No such bucket", "no such document" and
// "not supported by the engine" are data and feature answers, not
// documentation defects, and treating them as failures would make the gate
// unusable. A form the documentation shows being refused is checked too, but
// only that the refusal is the documented kind.

// The strings this gate once matched on are gone: every
// them: every "the CLI has no such verb here" refusal is a *UsageError —
// usagef — and so is a refusal of a verb's argument, which is the same defect
// seen from the other side. The typed check catches both, and catches a new one
// without editing this file. The strings are kept only in
// TestTheGateWouldCatchAKnownRefusal's error report.

// isRoutingRefusal reports whether an error means the CLI will not run the
// statement at all, as distinct from an error about the data it names.
// A *UsageError is the CLI refusing the statement's shape — an unknown verb at
// this level, or a verb given an argument it does not take. A *RefusedError or
// a server error is about the data ("no such bucket"), which is a different
// question and not this gate's business.
func isRoutingRefusal(err error) bool {
	var usage *UsageError
	return errors.As(err, &usage)
}

// callShape matches a real bucket command at any depth: db.Users.find(, the
// store-field form db.User["ada"].Friends.list(, and the store-on-a-store form
// db.Post["p1"].Comments[0].Flags.push(. After the bucket name it is a chain of
// bracketed keys and dotted names ending in the verb.
//
// It is also what keeps prose containing the two letters "db." out of the
// results: capacity.md has "db.service" in a sentence about memory.high, and
// that has no call behind it.
var callShape = regexp.MustCompile(
	`^db\.[A-Za-z_][A-Za-z0-9_]*(\[[^\]]*\]|\.[A-Za-z_][A-Za-z0-9_]*)*\(.*\)$`)

// documentedCall finds the first `db.` call expression in line, from the token
// to its closing parenthesis. Braces and brackets inside it (a filter, a
// projection) do not end it; a parenthesis inside it does, so
// `find({a: {$in: [x]}})` closes on the last one.
func documentedCall(line string) (string, bool) {
	start := strings.Index(line, "db.")
	if start < 0 {
		return "", false
	}
	rest := line[start:]
	open := strings.Index(rest, "(")
	if open < 0 {
		return "", false
	}
	depth := 0
	for i := open; i < len(rest); i++ {
		switch rest[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return rest[:i+1], true
			}
		}
	}
	return "", false
}

// placeholders maps the documentation's angle-bracket placeholders to values
// the parser accepts. Without this every `db.<bucket>.` line is unparseable and
// the check would pass on vacuous data — it must be able to run the real form.
var placeholders = []struct{ from, to string }{
	{"<bucket>", "Sample"},
	{"<database>", "app"},
	{"<field>", "Photos"},
	{"<key>", "k1"},
	{"<uuid>", "k1"},
	{"<name>", "field"},
	{"<N>", "1"},
	{"<n>", "1"},
	{"<path>", "a.b"},
	{"<value>", "1"},
	{"<type>", "hash"},
	{"<id@host:port>", "node-1@127.0.0.1:7433"},
}

// concrete replaces the placeholders, so a documented shape becomes a statement
// the parser can be asked about.
func concrete(call string) string {
	out := call
	for _, p := range placeholders {
		out = strings.ReplaceAll(out, p.from, p.to)
	}
	return out
}

// showsARefusal reports whether the line after a documented call is an error.
// The pages document refusals on purpose — hql-spec.md shows `db.Jobs.peek(2)`
// failing — and those are correct documentation. What must not happen is a form
// shown as working that the CLI refuses as a verb.
func showsARefusal(lines []string, index int) bool {
	for _, next := range lines[index+1:] {
		next = strings.TrimSpace(next)
		if next == "" {
			continue
		}
		lower := strings.ToLower(next)
		return strings.HasPrefix(lower, "error:") ||
			strings.HasPrefix(lower, "cli error:") ||
			strings.HasPrefix(lower, "error ")
	}
	return false
}

// TestDocumentedStoreFieldVerbsAreImplemented is the check. It walks the help
// the CLI prints and every page of the user docs, runs each `db.` form through
// the real parser and the real dispatch, and fails on a verb the CLI refuses.
func TestDocumentedStoreFieldVerbsAreImplemented(t *testing.T) {
	// One server for the whole walk: the verdict is about routing, so what the
	// data contains is irrelevant and re-creating it per form would only cost
	// time.
	c := newDumpTestServer(t)
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	mustSend(t, c, transport.OpCreateBucket, &server.CreateBucketRequest{
		Database: "app", Name: "Post", Type: server.BucketTypeHash,
	}, nil)
	mustSend(t, c, transport.OpPut, &server.InsertRequest{
		Database: "app", Bucket: "Post", Key: "k1", Data: map[string]interface{}{"n": 1},
	}, nil)

	type finding struct {
		where string
		call  string
		why   string
	}
	var findings []finding
	// A gate that quietly stops matching is worse than no gate, so the walk
	// reports what it covered. The store-field floor matters most: the
	// existing claims gate's pattern matched none of those.
	var checked, checkedHelp, checkedDocs, checkedStoreField int

	check := func(where string, lines []string) {
		for i, line := range lines {
			// An error line quotes the call back in the CLI's own notation —
			// `db.Post["p1"].Comments.put()` — which is not an example of
			// anything and would be read as one.
			trimmed := strings.ToLower(strings.TrimSpace(line))
			if strings.HasPrefix(trimmed, "error:") || strings.HasPrefix(trimmed, "cli error:") {
				continue
			}
			call, ok := documentedCall(line)
			if !ok {
				continue
			}
			shown := concrete(call)
			// The shape is checked after substitution, not before: the help
			// prints db.<bucket>.find(...), which is not a bucket name until
			// <bucket> is replaced. Checking first rejected every help line.
			if !callShape.MatchString(shown) {
				continue
			}
			// Anything still carrying an angle bracket is a synopsis, not an
			// example: db.<bucket>.<verb>(<arguments>), db.<Field>.len().
			if strings.ContainsAny(shown, "<>") {
				continue
			}
			// Nor is an ellipsis one: db.<bucket>.insert({field: value, ...})
			// is the shape of a statement, and running it fails on the `...`
			// rather than on anything the gate is here to see. The server
			// writes the same shape with one character, …, when a message or a
			// batch's table names a statement: db.User_7.update("7", …).
			if strings.Contains(call, "...") || strings.Contains(call, "…") {
				continue
			}
			checked++
			if strings.Contains(call, "[") {
				checkedStoreField++
			}
			if where == "help" {
				checkedHelp++
			} else {
				checkedDocs++
			}
			cmd, err := ParseCommand(shown)
			if err != nil {
				// A documented form that does not parse is a documentation
				// defect whatever the page says next about it.
				findings = append(findings, finding{where, call, "does not parse: " + err.Error()})
				continue
			}
			if showsARefusal(lines, i) {
				continue // documented refusal, and not one this gate owns
			}
			cmd.Database = "app"
			c.handler.terminal = io.Discard
			if handleErr := c.handler.Handle(cmd); handleErr != nil && isRoutingRefusal(handleErr) {
				findings = append(findings, finding{where, call, handleErr.Error()})
			}
		}
	}

	// The help the CLI prints: the quick reference and every topic.
	var helpLines []string
	var out strings.Builder
	h := &CommandHandler{terminal: &out}
	h.handleHelp()
	helpLines = strings.Split(out.String(), "\n")
	for _, topic := range helpTopicList() {
		out.Reset()
		requireHelpTopic(t, h, topic)
		helpLines = append(helpLines, out.String())
	}
	check("help", helpLines)

	// Every publishable page. A page that cannot be read is skipped rather than
	// failed: this gate is about verbs, and a missing file is the link gate's
	// business.
	// repofile maps the private-tree path to the published layout, where the
	// user pages sit in docs/ beside go.mod rather than in the user-docs directory above the module.
	anchor, err := repofile.Find("docs/user/configuration.md")
	if err != nil {
		t.Fatalf("locating the user pages: %v", err)
	}
	userDir := filepath.Dir(anchor)
	pages, err := filepath.Glob(filepath.Join(userDir, "*.md"))
	if err != nil {
		t.Fatalf("glob %s: %v", userDir, err)
	}
	if len(pages) == 0 {
		t.Fatalf("no pages under %s: the gate would pass on nothing", userDir)
	}
	for _, page := range pages {
		// Ring-mode pages use the ring grammar, which this walk's earlier-engine
		// parser rejects by design; TestRingDocumentedStatementsParse reads them.
		if ringModePages[filepath.Base(page)] {
			continue
		}
		body, readErr := os.ReadFile(page)
		if readErr != nil {
			continue
		}
		check(filepath.Join("docs/user", filepath.Base(page)), strings.Split(string(body), "\n"))
	}

	t.Logf("checked %d documented forms (%d in help, %d under docs/user, %d store-field)",
		checked, checkedHelp, checkedDocs, checkedStoreField)
	// The pages this walk reads were shrunk to what the ring runs, so only a few forms are left under the user docs.
	if checkedDocs < 3 || checkedStoreField < 10 {
		t.Errorf("the walk covered only %d doc forms and %d store-field forms, too few to mean "+
			"anything: the extractor has probably stopped matching", checkedDocs, checkedStoreField)
	}

	if len(findings) == 0 {
		return
	}
	var report strings.Builder
	report.WriteString("\nthe documentation shows forms the CLI refuses as a verb:\n\n")
	for _, f := range findings {
		report.WriteString("  " + f.where + ": " + f.call + "\n      " + f.why + "\n")
	}
	report.WriteString("\nEither the form is wrong, or the verb is not implemented. ")
	report.WriteString("A form the page shows being refused is fine — the line after it must be that refusal.\n")
	t.Error(report.String())
}

// TestTheGateWouldCatchAKnownRefusal proves the gate is not vacuous: the same
// walk, over a page carrying a form the CLI refuses, must fail. A gate that
// passes on everything catches nothing, and this session produced seven
// defects precisely because nothing checked.
func TestTheGateWouldCatchAKnownRefusal(t *testing.T) {
	if _, err := ParseCommand("db.Sample.list({limit: 2})"); err != nil {
		t.Fatalf("the planted form does not parse at all, so this proves nothing: %v", err)
	}
	// `list` is a store-field verb; on a bucket the CLI refuses it. That refusal
	// is what the gate is looking for, and it is the shape a developer hits
	// after reading a store-field example.
	c := newDumpTestServer(t)
	mustSend(t, c, transport.OpCreateDB, &server.CreateDatabaseRequest{Name: "app"}, nil)
	mustSend(t, c, transport.OpCreateBucket, &server.CreateBucketRequest{
		Database: "app", Name: "Sample", Type: server.BucketTypeHash,
	}, nil)
	cmd, err := ParseCommand("db.Sample.list({limit: 2})")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	cmd.Database = "app"
	c.handler.terminal = io.Discard
	handleErr := c.handler.Handle(cmd)
	if handleErr == nil {
		t.Fatal("db.Sample.list() was accepted, so the verb surface changed and the gate needs revisiting")
	}
	if !isRoutingRefusal(handleErr) {
		t.Fatalf("db.Sample.list() failed for the wrong reason, so the gate would miss it: %v", handleErr)
	}
	// And the other half: a verb refused for an argument it does not take. This
	// is the defect the gate was built for — `Photos.peek(3)` silently answered
	// with one element, and `peek(3)` is now refused.
	peekCmd, err := ParseCommand(`db.Post["p1"].Photos.peek(3)`)
	if err != nil {
		t.Fatalf("the planted peek does not parse: %v", err)
	}
	peekCmd.Database = "app"
	c.handler.terminal = io.Discard
	if peekErr := c.handler.Handle(peekCmd); !isRoutingRefusal(peekErr) {
		t.Fatalf("peek(3) was accepted, so the gate cannot catch the defect it exists for: %v", peekErr)
	}
}

// ringModePages are the user pages written for the ring, which is what a plain hoarddb session runs. The earlier
// engine's documented-verb walk skips them and the ring walk below reads them.
var ringModePages = map[string]bool{
	"getting-started.md": true, "data-types.md": true,
	"howto.md": true, "cluster-add-a-node.md": true, "howto-agent-memory.md": true, "howto-chat.md": true, "howto-ledger.md": true, "howto-social.md": true, "howto-video.md": true,
	// Rewritten for ring mode on 2026-11-19; their runnable examples are executed by
	// TestStoreTypesPageExamplesRun and TestRingPageExamplesRun.
	"store-types.md": true, "indexes.md": true, "large-files.md": true, "replication.md": true,
	"limitations.md": true, "user-management.md": true, "plan.md": true, "limits.md": true,
	// Ring statement `db.users["ada"].get()` on the ingest page, flagged by the verb walk
	// as the old-engine grammar: the page is ring mode, so it is listed here.
	"ingest.md": true,
	// The five-minute path: ring statements at the prompt, run end to end against a fresh server.
	"quickstart.md": true,
	// Shrunk to the statement families the ring runs, one runnable example each.
	"hql-spec.md": true,
}

// TestRingDocumentedStatementsParse is the ring-mode half of the documented-verb gate: every
// statement a ring-mode page shows at a prompt must parse with the ring grammar, so a page
// cannot teach a statement that help and Tab completion do not know.
func TestRingDocumentedStatementsParse(t *testing.T) {
	anchor, err := repofile.Find("docs/user/configuration.md")
	if err != nil {
		t.Fatalf("locating the user pages: %v", err)
	}
	prompt := regexp.MustCompile(`^\[h\]oardDB(?: \[[^\]]+\])?> (.+)$`)
	var parsed int
	for page := range ringModePages {
		body, readErr := os.ReadFile(filepath.Join(filepath.Dir(anchor), page))
		if readErr != nil {
			t.Fatalf("reading %s: %v", page, readErr)
		}
		lines := strings.Split(string(body), "\n")
		for i, line := range lines {
			m := prompt.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			// A statement the page shows being refused as a parse error is a documented
			// refusal, and the next line is that refusal.
			if i+1 < len(lines) && strings.HasPrefix(lines[i+1], "CLI error: PARSE_ERROR") {
				continue
			}
			parsed++
			if _, parseErr := Parse(m[1], "app"); parseErr != nil {
				t.Errorf("%s shows %q, which the ring grammar does not parse: %v", page, m[1], parseErr)
			}
		}
	}
	if parsed < 20 {
		t.Fatalf("parsed only %d prompt lines: the scan stopped matching", parsed)
	}
}
