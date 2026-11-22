package cli

import (
	"reflect"
	"strings"
	"testing"
	"unicode"
)

// TestFilterPrefix covers the vocabulary filter Tab completion runs on every
// press: prefix matching, de-duplication and a stable order.
func TestFilterPrefix(t *testing.T) {
	words := []string{"find", "first", "fifo", "find", "get"}

	cases := []struct {
		prefix string
		want   []string
	}{
		{"fi", []string{"fifo", "find", "first"}},
		{"find", []string{"find"}},
		{"g", []string{"get"}},
		{"nope", nil},
		{"", []string{"fifo", "find", "first", "get"}},
	}
	for _, tc := range cases {
		t.Run(tc.prefix, func(t *testing.T) {
			got := filterPrefix(words, tc.prefix)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("filterPrefix(%q) = %v, want %v", tc.prefix, got, tc.want)
			}
		})
	}
}

// TestWordStart is the boundary the completer replaces at. Getting it wrong
// inserts the candidate into the middle of the previous word.
func TestWordStart(t *testing.T) {
	cases := []struct {
		line string
		pos  int
		want int
	}{
		{"fin", 3, 0},
		{"show fin", 8, 5},
		{"db.users.find({", 14, 14},
		{"insert({na", 10, 8},
		{"", 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.line, func(t *testing.T) {
			if got := wordStart(tc.line, tc.pos); got != tc.want {
				t.Fatalf("wordStart(%q, %d) = %d, want %d", tc.line, tc.pos, got, tc.want)
			}
		})
	}
}

// TestCompletionCandidates drives the completer the editor calls, without a
// terminal: the same function decides what a Tab press offers.
func TestCompletionCandidates(t *testing.T) {
	c := NewCLI()

	cases := []struct {
		name     string
		line     string
		pos      int
		want     []string
		wantWord int
	}{
		{"a verb prefix", "fir", 3, []string{"first"}, 0},
		{"a bare tab lists the vocabulary", "z", 1, nil, 0},
		{"help completes topics, not verbs", "help ha", 7, []string{"hash"}, 5},
		{"help with a full topic", "help btre", 9, []string{"btree"}, 5},
		{"after a space", "show buck", 9, []string{"bucket", "buckets"}, 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			start, got := c.completer()(tc.line, tc.pos)
			if start != tc.wantWord {
				t.Fatalf("completer(%q) start = %d, want %d", tc.line, start, tc.wantWord)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("completer(%q) = %v, want %v", tc.line, got, tc.want)
			}
		})
	}
}

// TestCompletionDoesNotOfferTheRefusedDatabaseForm is the completion half of
// refusing the replication clause. The vocabulary is flat and context-free, so a word in it is offered
// everywhere — and `with` is accepted by no parser path at all: its only use
// was to finish `create database <name> with replication_factor N`, the clause
// that was parsed and discarded and is now refused. Completing a user into a
// command the parser rejects is the same defect one keystroke earlier.
//
// `replication_factor` itself stays in the vocabulary: `create bucket X {
// replication_factor: N }` is a form the parser does accept.
func TestCompletionDoesNotOfferTheRefusedDatabaseForm(t *testing.T) {
	c := NewCLI()

	line := "create database MyApp wi"
	start, got := c.completer()(line, len(line))
	if got != nil {
		t.Fatalf("completer(%q) = %v at %d, want no candidates: no parser path accepts a 'with' clause", line, got, start)
	}

	for _, word := range completionWords {
		if word == "with" {
			t.Fatal("completionWords still offers \"with\"; the parser accepts it nowhere")
		}
	}

	// The scope fence: the bucket-level keyword is a form the parser accepts,
	// so the audit must not take it out with the database form.
	bucketLine := "create bucket Users { replication_f"
	if _, candidates := c.completer()(bucketLine, len(bucketLine)); len(candidates) != 1 || candidates[0] != "replication_factor" {
		t.Fatalf("completer(%q) = %v, want [replication_factor]", bucketLine, candidates)
	}
}

// TestBucketCandidatesWithoutASession asserts completion degrades quietly: Tab
// on a session with no connection offers nothing rather than reporting an error
// the user did not ask for.
func TestBucketCandidatesWithoutASession(t *testing.T) {
	c := NewCLI()
	if got := c.bucketCandidates("db."); got != nil {
		t.Fatalf("bucketCandidates with no connection = %v, want nil", got)
	}
}

// acceptedCommandForms is one line per shape of command the parser accepts,
// written out so that a word the completer offers has to be found in a form a
// user can really run. The corpus is part of the test: a new completion word
// needs a form here, and every form must parse, so the list cannot go stale
// without failing.
var acceptedCommandForms = []string{
	"use app",
	"show databases", "show buckets in app", "show users", "show banner",
	"describe bucket Users", "describe database app",
	"status", "encryption", "nodes", "help", "help hash", "exit", "quit",
	"connect localhost:7433", "master",
	"migrate app Users", "write-concern majority", "write-concern one", "write-concern all",
	"consistent-get app Users k1", "join node-2:7433", "remove-node node-2",
	"set pretty on", "set extjson off", "set pager off", "set banner off",
	"create database app", "drop database app", "drop bucket Users",
	"create bucket Users { type: hash, indices: [\"Email\"], replication_factor: 3 }",
	"create bucket Post { Comments fifoStore, Photos lifoStore, Friends hashStore, Recipes btreeStore, Books heapStore, Data blobStore }",
	"alter bucket Post { Tags hashStore }",
	"create user alice roles [reader@app]", "user list", "user describe alice",
	"alter user alice roles [reader@app]", "alter user alice password \"pw2\"",
	"disable user alice", "enable user alice",
	"grant role reader@app to alice", "revoke role reader@app from alice",
	"create bucket A { type: btree }", "create bucket B { type: fifo }", "create bucket C { type: lifo }",
	"create bucket D { type: heap }", "create bucket E { type: blob }",
	"db.Users.insert({Name: \"a\"})", "db.Users.upsert({_id_: \"a\", Name: \"a\"})", "db.Users.find({})", "db.Users.get({_id_: \"k\"})",
	"db.Users.put(\"k\", {Name: \"a\"})", "db.Users.delete({_id_: \"k\"})",
	"db.Users.update({_id_: \"k\"}, {$set: {Name: \"b\"}})", "db.Users.search({Email: \"a\"})",
	"db.Users.count()", "db.Users.length()", "db.Users.stats()", "db.Users.info()",
	"db.Events.push({n: 1})", "db.Events.pop()", "db.Events.peek()",
	"db.Events.claim({limit: 10, lease: \"30s\"})", "db.Events.ack({lease_id: \"x\"})",
	"db.Events.release({lease_id: \"x\"})", "db.Events.extend({lease_id: \"x\", lease: \"60s\"})",
	"db.Events.dead({limit: 50})", "db.Events.requeue({all: true})", "db.Events.discard({dead_id: \"x\"})",
	"db.Ledger.range({$gte: \"k2\"})", "db.Ledger.first()", "db.Ledger.last()",
	"db.Post[\"p1\"].Comments.push({Author: \"ada\"})", "db.Post[\"p1\"].Comments.len()",
	"db.Post[\"p1\"].Friends.list()",
	"batch { db.Post[\"p1\"].Comments.push({Author: \"ada\"}); db.Users.update(\"k\", {$inc: {n: 1}}) }",
}

// TestEveryCompletionKeywordAppearsInAnAcceptedCommandForm is step 4.2b. The
// completion vocabulary is a flat list, so a word in it is offered anywhere; a
// word that no accepted command form contains completes the user into a
// refusal one keystroke before the parser would say so (AGENTS.md rule 10).
// `with` and the database form of `replication_factor` were that defect;
// this test is what stops the next one.
func TestEveryCompletionKeywordAppearsInAnAcceptedCommandForm(t *testing.T) {
	inAForm := map[string]bool{}
	for _, form := range acceptedCommandForms {
		if _, err := ParseCommand(form); err != nil {
			t.Errorf("the form %q is in the corpus as accepted, but the parser refuses it: %v", form, err)
			continue
		}
		for _, token := range strings.FieldsFunc(form, func(r rune) bool {
			return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '-' && r != '_'
		}) {
			inAForm[token] = true
		}
	}
	for _, word := range completionWords {
		if !inAForm[word] {
			t.Errorf("completionWords offers %q, which appears in no command form the parser accepts", word)
		}
	}
}

// TestCompletionOffersReplicationFactorAfterAlterBucket: the vocabulary is
// flat, so this pins that `alter bucket X { repl` still completes to the key
// the command now sends, and that the key stays in the list.
func TestCompletionOffersReplicationFactorAfterAlterBucket(t *testing.T) {
	line := "alter bucket Sessions { repl"
	start, got := NewCLI().completer()(line, len(line))
	if want := len("alter bucket Sessions { "); start != want {
		t.Fatalf("completer(%q) replaces from %d, want %d", line, start, want)
	}
	if !reflect.DeepEqual(got, []string{"replication_factor"}) {
		t.Fatalf("completer(%q) = %v, want [replication_factor]", line, got)
	}
}
