package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// u52LiveServer runs the real server in process on a loopback port.
func u52LiveServer(t *testing.T) string {
	t.Helper()
	addr, _ := newRingTestHost(t)
	return addr
}

func u52LiveSession(t *testing.T) (*RingSession, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	_, client := newRingTestHost(t)
	return NewRingSession(NewRingAPI(client)), ctx
}

// u52Transcript runs statements and returns "<statement>\n<output>" for each, as a person sees them.
func u52Transcript(t *testing.T, ctx context.Context, session *RingSession, statements ...string) string {
	t.Helper()
	var out strings.Builder
	for _, statement := range statements {
		output, err := session.Execute(ctx, statement)
		out.WriteString(session.Prompt() + statement + "\n")
		for _, line := range output.Lines {
			out.WriteString(line + "\n")
		}
		if err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	return out.String()
}

// TestLiveFirstFiveMinutes is the single-node part of queries 8 against the real server.
func TestLiveFirstFiveMinutes(t *testing.T) {
	session, ctx := u52LiveSession(t)
	got := u52Transcript(t, ctx, session,
		"create database app",
		"use app",
		"create bucket User { Name string, Age int, Purchases fifoStore, Friends hashStore }",
		`db.User.insert({_id_: "ada", Name: "Ada", Age: 36})`,
		`db.User["ada"].Purchases.push({Item: "coffee"})`,
		`db.User["ada"].Purchases.push({Item: "tea"})`,
		`db.User["ada"].Friends.put("grace", {since: 2024})`,
		"db.User.list()",
		"db.User.count()",
		`db.User["ada"].get()`,
		`db.User["ada"].Purchases.peek()`,
		`db.User["ada"].Purchases.pop()`,
		`db.User["ada"].Purchases.count()`,
		`db.User.update({_id_: "ada", $set: {Age: 37}})`,
		`db.User["ada"].get()`,
		"describe bucket User",
		"show buckets",
		`db.User.insert({Name: "Minted"})`,
		`db.User.list({filter: {Name: "Minted"}})`,
		`db.User.delete({_id_: "ada"})`,
		"db.User.count()")
	t.Logf("\n%s", got)
	for _, want := range []string{
		"OK  created database app (replication factor 1, this node's default)",
		"Switched to database 'app'",
		"OK  created bucket app.User (4 fields)",
		"Fields: Name string, Age int, Purchases fifoStore, Friends hashStore",
		"OK  id=ada",
		"OK  pushed: Purchases now holds 1 element",
		"OK  pushed: Purchases now holds 2 elements",
		`OK  put "grace": Friends now holds 1 element`,
		`{"Age": 36, "Friends": "hashStore (1 element)", "Name": "Ada", "Purchases": "fifoStore (2 elements)", "_id_": "ada"}`,
		`{"Item": "coffee"}`,
		"OK  updated id=ada",
		`"Age": 37`,
		"BUCKET app.User",
		"State:               active",
		"OK  deleted id=ada",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("transcript lacks %q", want)
		}
	}
}

func TestLiveErrorsCarryCodeAndExitStatus(t *testing.T) {
	session, ctx := u52LiveSession(t)
	u52Transcript(t, ctx, session, "create database app", "use app", "create bucket User { Name string, Purchases fifoStore, Books heapStore }",
		`db.User.insert({_id_: "ada", Name: "Ada"})`)
	cases := []struct {
		statement, code, text string
		exit                  int
	}{
		{`db.User["nobody"].get()`, "NOT_FOUND", `document "nobody" is not in app.User. Run 'db.User.list({limit: 10})'`, 2},
		{`db.User.insert({_id_: "ada", Name: "Again"})`, "DOCUMENT_EXISTS", `document "ada" is already in app.User, and insert never replaces. Run 'db.User.upsert({_id_: "ada", …})'`, 2},
		{"db.Nope.count()", "NOT_FOUND", "show buckets in app", 2},
		{"create database app", "CONFLICT", "already exists", 8},
		{"create bucket User { Name string }", "BUCKET_EXISTS", "describe bucket User in app", 2},
		{`db.User["ada"].Purchases.pop()`, "NOT_FOUND", `Purchases of "ada" in app.User is empty`, 2},
		{`db.User["ada"].Books.push({a: 1})`, "", "not available", 2},
		{"use missing", "", "", 2},
		{"describe database nope", "NOT_FOUND", "show databases", 2},
	}
	for _, c := range cases {
		_, err := session.Execute(ctx, c.statement)
		var failed *RingError
		if !errors.As(err, &failed) {
			t.Errorf("%s: %v", c.statement, err)
			continue
		}
		t.Logf("%s\n   -> exit %d  %s", c.statement, failed.Exit, failed.Error())
		if failed.Exit != c.exit || (c.code != "" && failed.Code != c.code) || !strings.Contains(failed.Message, c.text) {
			t.Errorf("%s: code %q exit %d message %q", c.statement, failed.Code, failed.Exit, failed.Message)
		}
	}
}

// TestLiveRunMain drives the whole binary path: flags, credentials, dial, piped statements, exit status.
func TestLiveRunMain(t *testing.T) {
	addr := u52LiveServer(t)
	run := func(script string, extra ...string) (int, string, string) {
		var out, errOut bytes.Buffer
		args := append([]string{"-insecure", "-address", addr, "-user", "root", "-password=live-test-password"}, extra...)
		status := RunMain(Invocation{Args: args, Stdin: strings.NewReader(script), Stdout: &out, Stderr: &errOut,
			Getenv: func(string) string { return "" }})
		return status, out.String(), errOut.String()
	}
	status, out, errOut := run("create database app\nuse app\ncreate bucket User { Name string }\ndb.User.insert({_id_: \"ada\", Name: \"Ada\"})\ndb.User.count()\n")
	if status != 0 || !strings.Contains(out, "OK  created bucket app.User (1 fields)") || !strings.HasSuffix(out, "OK  id=ada\n1\n") {
		t.Fatalf("status %d\nstdout %q\nstderr %q", status, out, errOut)
	}
	status, out, errOut = run("use app\ndb.User[\"nobody\"].get()\ndb.User.count()\n")
	if status != 2 || !strings.Contains(errOut, "CLI error: NOT_FOUND: document \"nobody\" is not in app.User") || strings.Contains(out, "\n1\n") {
		t.Fatalf("status %d\nstdout %q\nstderr %q", status, out, errOut)
	}
	status, _, errOut = run("show databases in app\n")
	if status != 2 || !strings.Contains(errOut, "not a statement of the ring session") && !strings.Contains(errOut, "PARSE_ERROR") {
		t.Fatalf("status %d stderr %q", status, errOut)
	}
	// A wrong password is a refused login: exit 7, nothing sent.
	var wrongOut, wrongErr bytes.Buffer
	status = RunMain(Invocation{Args: []string{"-insecure", "-address", addr, "-user", "root", "-password=wrong"},
		Stdin: strings.NewReader("create database x\n"), Stdout: &wrongOut, Stderr: &wrongErr, Getenv: func(string) string { return "" }})
	if status != ExitAuth {
		t.Fatalf("wrong password: status %d stderr %q", status, wrongErr.String())
	}
	// Without -insecure or -fingerprint the ring session trusts nothing.
	var bareOut, bareErr bytes.Buffer
	status = RunMain(Invocation{Args: []string{"-address", addr, "-user", "root", "-password=live-test-password"},
		Stdin: strings.NewReader(""), Stdout: &bareOut, Stderr: &bareErr, Getenv: func(string) string { return "" }})
	if status != ExitConnect || !strings.Contains(bareErr.String(), "-fingerprint") {
		t.Fatalf("no trust: status %d stderr %q", status, bareErr.String())
	}
}
