package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
	"github.com/hoarddb/hoarddb-server/version"
)

func TestPipedRunStopsAtTheFirstFailureWithItsExitStatus(t *testing.T) {
	session, _ := u52Session()
	script := "# a comment\n\ncreate database app\nuse app\ncreate bucket User { Name string }\ndb.Nope.count()\ndb.User.count()\n"
	var out, errOut bytes.Buffer
	status := session.RunPiped(context.Background(), strings.NewReader(script), &out, &errOut)
	if status != cluster.CodeNotFound.Exit() {
		t.Fatalf("exit status %d, stderr %q", status, errOut.String())
	}
	if !strings.Contains(errOut.String(), "CLI error: NOT_FOUND: bucket \"Nope\" not found in database \"app\"") {
		t.Fatalf("stderr: %q", errOut.String())
	}
	if !strings.Contains(out.String(), "OK  created bucket app.User (1 fields)") || strings.Contains(out.String(), "\n0\n") {
		t.Fatalf("stdout: %q", out.String())
	}
	if !strings.HasPrefix(out.String(), "hoardDB CLI ") {
		t.Fatalf("greeting: %q", out.String())
	}
}

func TestPipedRunSucceedsAndExitEndsIt(t *testing.T) {
	session, _ := u52Session()
	var out, errOut bytes.Buffer
	status := session.RunPiped(context.Background(), strings.NewReader("create database app\nexit\ncreate database never\n"), &out, &errOut)
	if status != 0 || errOut.Len() != 0 || strings.Contains(out.String(), "never") || !strings.Contains(out.String(), "Goodbye.") {
		t.Fatalf("status %d out %q err %q", status, out.String(), errOut.String())
	}
}

func TestPromptIsTheV1PromptWithTheDatabase(t *testing.T) {
	session, _ := u52Session()
	if got := session.Prompt(); got != "[h]oardDB> " {
		t.Fatalf("prompt %q", got)
	}
	if _, err := u52Run(session, "create database app", "use app"); err != nil {
		t.Fatal(err)
	}
	if got := session.Prompt(); got != "[h]oardDB [app]> " {
		t.Fatalf("prompt %q", got)
	}
}

func TestCompletionOffersServerNamesFromACache(t *testing.T) {
	session, fake := u52Session()
	if _, err := u52Run(session, "create database app", "use app",
		"create bucket User { Name string, Purchases fifoStore }", "create bucket Post { Title string }"); err != nil {
		t.Fatal(err)
	}
	names := newRingServerNames(session)
	clock := time.Unix(1000, 0)
	names.now = func() time.Time { return clock }
	complete := func(line string) []string { return CompleteWith(names, line, session.selected) }

	got := complete("describe bucket ")
	if strings.Join(got, ",") != "Post,User" {
		t.Fatalf("buckets: %v", got)
	}
	_ = complete("describe bucket ")
	before := len(fake.calls)
	_ = complete("describe bucket ")
	if len(fake.calls) != before {
		t.Fatalf("a second lookup inside the TTL asked the server again: %v", fake.calls)
	}
	clock = clock.Add(ringNameTTL + time.Second)
	_ = complete("describe bucket ")
	if len(fake.calls) == before {
		t.Fatal("an old entry was not refreshed")
	}
	if got := complete(`db.User["ada"].`); strings.Join(got, ",") != "Purchases,get" {
		t.Fatalf("store fields: %v", got)
	}
	if got := complete("use "); strings.Join(got, ",") != "app" {
		t.Fatalf("databases: %v", got)
	}
	fake.failures["ShowBuckets"] = &ringclient.Error{Code: cluster.CodeUnavailable, Message: "down"}
	clock = clock.Add(time.Hour)
	if got := complete("describe bucket "); len(got) != 0 {
		t.Fatalf("a failed lookup must offer nothing, got %v", got)
	}
}

func TestCompleterReturnsTheWordStart(t *testing.T) {
	session, _ := u52Session()
	start, candidates := session.Completer()("cre", 3)
	if start != 0 || len(candidates) == 0 || candidates[0] != "create" {
		t.Fatalf("start %d candidates %v", start, candidates)
	}
}

// u82Greeting runs the start of a terminal session on a fake with the given databases: stdin is not a
// terminal, so the prompt ends at once and the output is the greeting.
func u82Greeting(t *testing.T, databases []string, failure error) string {
	t.Helper()
	session, fake := u52Session()
	for _, name := range databases {
		if _, err := session.Execute(context.Background(), "create database "+name); err != nil {
			t.Fatal(err)
		}
	}
	if failure != nil {
		fake.failures["ShowDatabases"] = failure
	}
	var out bytes.Buffer
	session.RunInteractive(context.Background(), &out, true)
	return out.String()
}

// TestFirstRunHintSaysWhatToTypeFirst: a terminal session tells the developer the next statement; the
// hint is one line between the version and the help line, and costs one page of at most four databases.
func TestFirstRunHintSaysWhatToTypeFirst(t *testing.T) {
	head := "hoardDB CLI " + version.Version + "\n"
	tail := "Type 'help' to get started, 'exit' to quit.\n\n"
	cases := []struct {
		name      string
		databases []string
		failure   error
		hint      string
	}{
		{"none", nil, nil, "No databases yet. Start with: create database app\n"},
		{"two", []string{"social", "app"}, nil, "Databases: app, social. Pick one with: use app\n"},
		{"three", []string{"c", "b", "a"}, nil, "Databases: a, b, c. Pick one with: use a\n"},
		{"four", []string{"d", "c", "b", "a"}, nil, "Databases: a, b, c, … ('show databases' for all). Pick one with: use a\n"},
		{"five", []string{"e", "d", "c", "b", "a"}, nil, "Databases: a, b, c, … ('show databases' for all). Pick one with: use a\n"},
		{"call failed", []string{"app"}, &ringclient.Error{Code: cluster.CodeUnavailable, Message: "down"}, ""},
	}
	for _, c := range cases {
		if got := u82Greeting(t, c.databases, c.failure); got != head+c.hint+tail+"[h]oardDB> \n" {
			t.Errorf("%s: greeting\n%q\nwant\n%q", c.name, got, head+c.hint+tail+"[h]oardDB> \n")
		}
	}
}

// TestPipedGreetingHasNoHint: a script's output keeps the two plain lines, and asks the server nothing.
func TestPipedGreetingHasNoHint(t *testing.T) {
	session, fake := u52Session()
	var out, errOut bytes.Buffer
	if status := session.RunPiped(context.Background(), strings.NewReader("create database app\n"), &out, &errOut); status != 0 {
		t.Fatalf("status %d: %s", status, errOut.String())
	}
	want := "hoardDB CLI " + version.Version + "\nType 'help' for help, 'exit' to quit.\n\nOK  created database app (replication factor 1, this node's default)\n" +
		"Next: use app\nThen: create bucket <name> { Name string, Purchases fifoStore }   (declare every field)\n"
	if out.String() != want {
		t.Errorf("piped output:\n%q\nwant\n%q", out.String(), want)
	}
	for _, call := range fake.calls {
		if call == "ShowDatabases" {
			t.Error("a piped run asked the server for its databases")
		}
	}
}
