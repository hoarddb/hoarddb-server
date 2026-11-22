package cli

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// u80UserStatements are the user and role statements, spelled as the help spells them.
var u80UserStatements = []string{
	"create user ada",
	"create user ada roles [reader@app]",
	"create user ada roles [writer@app, reader@logs]",
	"create user ada roles [reader@*]",
	"create user ada roles [reader]",
	`create user "ada.l@example"`,
	"alter user ada password",
	"alter user ada disable",
	"alter user ada enable",
	"alter user ada roles [writer@app, reader@logs]",
	"alter user ada roles []",
	"drop user ada",
	"grant role db-manager@app to ada",
	"grant role reader@* to ada",
	"grant role reader to ada",
	"revoke role writer@app from ada",
	"show users",
	"user list",
	"user describe ada",
}

const u80NewPassword = "ada-s3cret-password"

// u80Session is a session on the fake with every new password answered by the terminal prompt.
func u80Session() (*RingSession, *u52Fake) {
	session, fake := u52Session()
	session.promptPassword = func() (string, error) { return u80NewPassword, nil }
	return session, fake
}

// u80WithAda gives the fake an account ada, as an earlier create would.
func u80WithAda(fake *u52Fake) {
	users := fake.userStore()
	users.revision++
	users.accounts["ada"] = ringclient.UserInfo{Name: "ada", Roles: []ringclient.UserGrant{{Role: "reader", DB: "app"}}, Enabled: true, Revision: users.revision}
}

func u80Changes(fake *u52Fake) []string {
	var methods []string
	for _, call := range fake.userStore().changes {
		methods = append(methods, call.method)
	}
	return methods
}

func TestU80UserStatementsParseAndRoundTrip(t *testing.T) {
	for _, text := range u80UserStatements {
		st, err := Parse(text, "")
		if err != nil {
			t.Errorf("%s: %v", text, err)
			continue
		}
		again, err := Parse(st.String(), "")
		if err != nil || again.String() != st.String() {
			t.Errorf("%s renders as %q which parses to %q, %v", text, st.String(), again.String(), err)
		}
	}
}

func TestU80UserStatementsRefuseWhatV1Refuses(t *testing.T) {
	for _, text := range []string{"create user", "alter user ada", "alter user ada roles writer@app", "grant role writer@ to ada",
		"grant role writer@app ada", "revoke role from ada", "drop user", "user describe", "user frobnicate"} {
		if _, err := Parse(text, ""); err == nil {
			t.Errorf("%q parses", text)
		}
	}
}

// TestRingUserStatementsRunOnTheRingConnection: every user statement is one or two ring user
// calls on the session's own connection, with the arguments the statement names.
func TestRingUserStatementsRunOnTheRingConnection(t *testing.T) {
	grants := func(pairs ...string) *[]ringclient.UserGrant {
		out := []ringclient.UserGrant{}
		for i := 0; i < len(pairs); i += 2 {
			out = append(out, ringclient.UserGrant{Role: pairs[i], DB: pairs[i+1]})
		}
		return &out
	}
	no, yes := false, true
	cases := []struct {
		statement string
		reads     []string
		want      u52UserCall
	}{
		{"create user bob", []string{"ShowUsers"}, u52UserCall{method: "CreateUser", name: "bob", roles: grants()}},
		{"create user bob roles [writer@app, reader]", []string{"ShowUsers"}, u52UserCall{method: "CreateUser", name: "bob", roles: grants("writer", "app", "reader", "*")}},
		{`create user "bob.l@example" roles [reader@*]`, []string{"ShowUsers"}, u52UserCall{method: "CreateUser", name: "bob.l@example", roles: grants("reader", "*")}},
		{"alter user ada disable", []string{"DescribeUser"}, u52UserCall{method: "AlterUser", name: "ada", enabled: &no}},
		{"alter user ada enable", []string{"DescribeUser"}, u52UserCall{method: "AlterUser", name: "ada", enabled: &yes}},
		{"alter user ada roles [writer@app, reader@logs]", []string{"DescribeUser"}, u52UserCall{method: "AlterUser", name: "ada", roles: grants("writer", "app", "reader", "logs")}},
		{"alter user ada roles []", []string{"DescribeUser"}, u52UserCall{method: "AlterUser", name: "ada", roles: grants()}},
		{"drop user ada", nil, u52UserCall{method: "DropUser", name: "ada"}},
		{"grant role db-manager@app to ada", nil, u52UserCall{method: "GrantRole", name: "ada", grant: ringclient.UserGrant{Role: "db-manager", DB: "app"}}},
		{"grant role reader to ada", nil, u52UserCall{method: "GrantRole", name: "ada", grant: ringclient.UserGrant{Role: "reader", DB: "*"}}},
		{"revoke role writer@app from ada", nil, u52UserCall{method: "RevokeRole", name: "ada", grant: ringclient.UserGrant{Role: "writer", DB: "app"}}},
	}
	for _, c := range cases {
		session, fake := u80Session()
		u80WithAda(fake)
		if _, err := u52Run(session, c.statement); err != nil {
			t.Errorf("%s: %v", c.statement, err)
			continue
		}
		if !reflect.DeepEqual(fake.calls, append(append([]string(nil), c.reads...), c.want.method)) {
			t.Errorf("%s: calls %v, want %v then %s", c.statement, fake.calls, c.reads, c.want.method)
		}
		changes := fake.userStore().changes
		if len(changes) != 1 {
			t.Errorf("%s: %d changes", c.statement, len(changes))
			continue
		}
		got := changes[0]
		got.password, got.expectSeq, got.dropFence = nil, nil, 0
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: sent %+v, want %+v", c.statement, got, c.want)
		}
	}
	for _, read := range []string{"show users", "user list", "user describe ada"} {
		session, fake := u80Session()
		u80WithAda(fake)
		if _, err := u52Run(session, read); err != nil || len(fake.userStore().changes) != 0 {
			t.Errorf("%s: %v, changes %v", read, err, u80Changes(fake))
		}
	}
}

// TestRingUserStatementsNeverOpenALegacyConnection: the user statements have no second
// connection: no ring session file names the legacy user opcodes or the legacy handler.
func TestRingUserStatementsNeverOpenALegacyConnection(t *testing.T) {
	files, err := filepath.Glob("ring*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			ident, ok := node.(*ast.Ident)
			if !ok {
				return true
			}
			switch {
			case strings.HasPrefix(ident.Name, "OpUser"), ident.Name == "CommandHandler", ident.Name == "dialAndAuthenticate",
				ident.Name == "WithNewPasswordSource", strings.Contains(ident.Name, "RingLegacy"), strings.Contains(ident.Name, "ringLegacy"):
				t.Errorf("%s names %s: the user statements run on the ring connection alone", file, ident.Name)
			}
			return true
		})
	}
}

// TestRingCreateUserReadsTheDropFenceAndSendsIt: the create carries the drop fence of the user
// list read just before it, never a value of its own (ring-user-store.md 4.3, R3).
func TestRingCreateUserReadsTheDropFenceAndSendsIt(t *testing.T) {
	session, fake := u80Session()
	fake.userStore().dropSeq = 41
	if _, err := u52Run(session, "create user bob roles [reader@app]"); err != nil {
		t.Fatal(err)
	}
	changes := fake.userStore().changes
	if len(changes) != 1 || changes[0].dropFence != 41 {
		t.Fatalf("create sent %+v, want the drop fence 41 the user list read reported", changes)
	}
}

// TestRingCreateUserAfterADropIsRefusedWithTheServersText: a drop between the read and the
// create is the server's CONFLICT, passed through with its exit status, and nothing is created.
func TestRingCreateUserAfterADropIsRefusedWithTheServersText(t *testing.T) {
	session, fake := u80Session()
	fake.failures["CreateUser"] = &ringclient.Error{Code: cluster.CodeConflict, Message: `create user "bob" was not applied: a user was dropped since the users were read`}
	_, err := session.Execute(context.Background(), "create user bob")
	var failed *RingError
	if !errors.As(err, &failed) || failed.Code != "CONFLICT" || failed.Exit != ExitUnavailable || !strings.Contains(failed.Message, "dropped since") {
		t.Fatalf("got %v (%+v), want the server's CONFLICT text and exit %d", err, failed, ExitUnavailable)
	}
}

// TestRingAlterUserReadsTheRevisionFirst: an alter carries the revision of the read before it,
// so a change made in between is refused, not overwritten.
func TestRingAlterUserReadsTheRevisionFirst(t *testing.T) {
	for _, statement := range []string{"alter user ada disable", "alter user ada roles [writer@app]", "alter user ada password"} {
		session, fake := u80Session()
		u80WithAda(fake)
		read := fake.userStore().revision
		if _, err := u52Run(session, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
		changes := fake.userStore().changes
		if len(changes) != 1 || changes[0].expectSeq == nil || *changes[0].expectSeq != read {
			t.Fatalf("%s: sent %+v, want expect_seq %d", statement, changes, read)
		}
	}
}

// TestRingAlterUserPasswordDerivesLocally: the new password goes to the client's derivation
// (ringclient.AlterUser), which sends only the credential; the statement prints no part of it.
func TestRingAlterUserPasswordDerivesLocally(t *testing.T) {
	session, fake := u80Session()
	u80WithAda(fake)
	got, err := u52Run(session, "alter user ada password")
	if err != nil {
		t.Fatal(err)
	}
	changes := fake.userStore().changes
	if len(changes) != 1 || changes[0].password == nil || *changes[0].password != u80NewPassword || changes[0].roles != nil || changes[0].enabled != nil {
		t.Fatalf("alter sent %+v, want the new password alone", changes)
	}
	if strings.Contains(got, u80NewPassword) {
		t.Fatalf("the output shows the password: %q", got)
	}
}

// TestRingCreateUserPromptsTwiceOnATerminalAndReadsTheNextLineWhenPiped: a terminal session asks
// through the no-echo prompt (which asks twice); a piped one reads the two lines after the
// statement and runs the line after them as the next statement.
func TestRingCreateUserPromptsTwiceOnATerminalAndReadsTheNextLineWhenPiped(t *testing.T) {
	session, fake := u80Session()
	prompts := 0
	session.promptPassword = func() (string, error) { prompts++; return u80NewPassword, nil }
	if _, err := u52Run(session, "create user bob"); err != nil || prompts != 1 {
		t.Fatalf("terminal: %v, prompted %d times", err, prompts)
	}
	if got := fake.userStore().changes; len(got) != 1 || *got[0].password != u80NewPassword {
		t.Fatalf("terminal: sent %+v", got)
	}

	piped, pipedFake := u52Session()
	piped.promptPassword = func() (string, error) { t.Error("a piped session prompted"); return "", errors.New("prompted") }
	var out, errOut strings.Builder
	status := piped.RunPiped(context.Background(), strings.NewReader("create user bob\nbob-s3cret-password\nbob-s3cret-password\nshow users\n"), &out, &errOut)
	changes := pipedFake.userStore().changes
	if status != ExitOK || len(changes) != 1 || *changes[0].password != "bob-s3cret-password" {
		t.Fatalf("piped: status %d, sent %+v, stderr %q", status, changes, errOut.String())
	}
	if !reflect.DeepEqual(pipedFake.calls, []string{"ShowUsers", "CreateUser", "ShowUsers"}) {
		t.Fatalf("piped: calls %v, want the fence read, the create and the show users after the password lines", pipedFake.calls)
	}
}

// TestRingPasswordSourcesAreTheFlagsOfV1: --password-stdin reads one line, --password-file reads
// the file (and warns when others can read it), and each refusal names the fix.
func TestRingPasswordSourcesAreTheFlagsOfV1(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "new.pw")
	if err := os.WriteFile(file, []byte("file-s3cret-password\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var warned strings.Builder
	session, fake := u52Session()
	WithRingPasswordSource(file, false, &warned)(session)
	if _, err := u52Run(session, "create user bob"); err != nil {
		t.Fatal(err)
	}
	if got := fake.userStore().changes; len(got) != 1 || *got[0].password != "file-s3cret-password" {
		t.Fatalf("file: sent %+v", got)
	}
	if !strings.Contains(warned.String(), "readable beyond its owner") || strings.Contains(warned.String(), "file-s3cret") {
		t.Fatalf("file: warning %q", warned.String())
	}

	stdin, stdinFake := u52Session()
	WithRingPasswordSource("", true, &warned)(stdin)
	var out, errOut strings.Builder
	status := stdin.RunPiped(context.Background(), strings.NewReader("create user bob\nstdin-s3cret-password\nshow users\n"), &out, &errOut)
	if got := stdinFake.userStore().changes; status != ExitOK || len(got) != 1 || *got[0].password != "stdin-s3cret-password" {
		t.Fatalf("stdin: status %d sent %+v stderr %q", status, got, errOut.String())
	}

	refusals := []struct {
		name   string
		source func(*RingSession)
		script string
		want   string
	}{
		{"stdin on a terminal", WithRingPasswordSource("", true, &warned), "", "this session is a terminal"},
		{"script ends", func(*RingSession) {}, "create user bob\nonly-one-line\n", "the script ends where the new password should be"},
		{"lines disagree", func(*RingSession) {}, "create user bob\nfirst-password-1\nother-password-2\n", "do not agree"},
		{"empty line", WithRingPasswordSource("", true, &warned), "create user bob\n\n", "is empty"},
		{"missing file", WithRingPasswordSource(filepath.Join(dir, "absent"), false, &warned), "create user bob\n", "read password file"},
	}
	for _, c := range refusals {
		session, fake := u52Session()
		c.source(session)
		var err error
		if c.script == "" {
			_, err = session.Execute(context.Background(), "create user bob")
		} else {
			var out, errOut strings.Builder
			if status := session.RunPiped(context.Background(), strings.NewReader(c.script), &out, &errOut); status != ExitUsage {
				t.Errorf("%s: status %d, want %d", c.name, status, ExitUsage)
			}
			err = errors.New(errOut.String())
		}
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want text containing %q", c.name, err, c.want)
		}
		if len(fake.userStore().changes) != 0 || len(fake.calls) != 0 {
			t.Errorf("%s: sent %v", c.name, fake.calls)
		}
	}
}

// TestRingAShortPasswordIsRefusedNamingTheMinimum: the server's minimum, as it reported at login,
// is applied before anything is sent; a server that reported none is refused too.
func TestRingAShortPasswordIsRefusedNamingTheMinimum(t *testing.T) {
	for _, statement := range []string{"create user bob", "alter user ada password"} {
		session, fake := u52Session()
		u80WithAda(fake)
		session.promptPassword = func() (string, error) { return "short-pw", nil }
		_, err := session.Execute(context.Background(), statement)
		var failed *RingError
		if !errors.As(err, &failed) || failed.Exit != ExitRefused ||
			!strings.Contains(failed.Message, "is 8 characters; this server requires at least 12") || !strings.Contains(failed.Message, "HOARDDB_PASSWORD_MIN_LENGTH") {
			t.Errorf("%s: %v, want the minimum named and exit %d", statement, err, ExitRefused)
		}
		if len(fake.userStore().changes) != 0 {
			t.Errorf("%s: a short password was sent", statement)
		}
	}
	session, fake := u80Session()
	fake.userStore().minLength = 0
	_, err := session.Execute(context.Background(), "create user bob")
	if err == nil || !strings.Contains(err.Error(), "did not report its minimum password length") || len(fake.userStore().changes) != 0 {
		t.Fatalf("no minimum reported: %v", err)
	}
}

// TestRingPasswordIsNeverPrinted runs every statement that takes a password, on the success and
// the refusal paths, piped and at the terminal, and scans everything printed for it.
func TestRingPasswordIsNeverPrinted(t *testing.T) {
	const secret = "never-print-this-pw"
	for _, statement := range []string{"create user bob roles [reader@app]", "alter user ada password"} {
		for _, failure := range []error{nil, &ringclient.Error{Code: cluster.CodeInvalidArgument, Message: "refused"}} {
			session, fake := u52Session()
			u80WithAda(fake)
			if failure != nil {
				fake.failures["CreateUser"], fake.failures["AlterUser"] = failure, failure
			}
			var out, errOut strings.Builder
			session.RunPiped(context.Background(), strings.NewReader(statement+"\n"+secret+"\n"+secret+"\nshow users\n"), &out, &errOut)
			terminal, terminalFake := u52Session()
			u80WithAda(terminalFake)
			terminal.promptPassword = func() (string, error) { return secret, nil }
			if failure != nil {
				terminalFake.failures["CreateUser"], terminalFake.failures["AlterUser"] = failure, failure
			}
			printed, err := terminal.Execute(context.Background(), statement)
			everything := out.String() + errOut.String() + strings.Join(printed.Lines, "\n") + u80ErrText(err)
			if strings.Contains(everything, secret) {
				t.Errorf("%s (failure %v) printed the password: %q", statement, failure, everything)
			}
		}
	}
	session, _ := u52Session()
	session.promptPassword = func() (string, error) { return "tiny", nil }
	_, err := session.Execute(context.Background(), "create user bob")
	if err == nil || strings.Contains(err.Error(), "tiny") {
		t.Errorf("the short-password refusal: %v", err)
	}
}

func u80ErrText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// TestRingUserOutputKeepsTheShapeOfV1: the lines of v1.0, with the revision a later change
// passes in place of the config version, and no auth master.
func TestRingUserOutputKeepsTheShapeOfV1(t *testing.T) {
	session, _ := u80Session()
	got, err := u52Run(session, "show users", "create user ada roles [reader@app]", "grant role writer@app to ada", "revoke role reader@app from ada",
		"alter user ada disable", "user list", "user describe root", "drop user ada")
	if err != nil {
		t.Fatal(err)
	}
	u52Want(t, got,
		"USERS (revision 1)",
		"---",
		"root: enabled, roles [root@*], epoch 0",
		`user "ada" created (revision 2)`,
		`granted writer@app to "ada" (revision 3)`,
		`revoked reader@app from "ada" (revision 4)`,
		`user "ada" altered (revision 5)`,
		"USERS (revision 5)",
		"---",
		"ada: disabled, roles [writer@app], epoch 1",
		"root: enabled, roles [root@*], epoch 0",
		"user:  root",
		"status: enabled",
		"roles:  [root@*]",
		"epoch:  0",
		"created: 2026-09-19T06:45:01Z",
		"password changed: 2026-09-19T06:45:01Z",
		`user "ada" dropped`)
}

// TestRingUserRefusalsPassTheServerTextAndExit: the server's text is printed as it came, with its
// code, and the exit status is the exit table's for that code.
func TestRingUserRefusalsPassTheServerTextAndExit(t *testing.T) {
	cases := []struct {
		statement string
		code      cluster.Code
		exit      int
	}{
		{"grant role writer@app to ada", cluster.CodePermissionDenied, ExitRefused},
		{"drop user ada", cluster.CodeNotFound, ExitRefused},
		{"revoke role writer@app from ada", cluster.CodeConflict, ExitUnavailable},
		{"user describe ada", cluster.CodeNotFound, ExitRefused},
		{"show users", cluster.CodePermissionDenied, ExitRefused},
	}
	method := map[string]string{"grant": "GrantRole", "drop": "DropUser", "revoke": "RevokeRole", "user": "DescribeUser", "show": "ShowUsers"}
	for _, c := range cases {
		session, fake := u80Session()
		u80WithAda(fake)
		fake.failures[method[strings.Fields(c.statement)[0]]] = &ringclient.Error{Code: c.code, Message: "the server's own text. Run 'show users'"}
		_, err := session.Execute(context.Background(), c.statement)
		var failed *RingError
		if !errors.As(err, &failed) || failed.Exit != c.exit || failed.Message != "the server's own text. Run 'show users'" || failed.Code != c.code.String() {
			t.Errorf("%s: %v (%+v), want the server's text and exit %d", c.statement, err, failed, c.exit)
		}
	}
}

// TestRingAlterOfAnAbsentUserNamesTheFixBeforeAskingForAPassword: the read comes first, so a
// typo is refused with the server's text and no password is asked.
func TestRingAlterOfAnAbsentUserNamesTheFixBeforeAskingForAPassword(t *testing.T) {
	session, fake := u52Session()
	session.promptPassword = func() (string, error) { t.Error("prompted for an absent user"); return "", errors.New("prompted") }
	_, err := session.Execute(context.Background(), "alter user nobody password")
	if err == nil || !strings.Contains(err.Error(), "Run 'show users'") || len(fake.userStore().changes) != 0 {
		t.Fatalf("got %v", err)
	}
}

// TestU80TheScanSeesBothQuotedGrantFixes: ringDenied quotes its fix at the start of a continued
// Go string, which the scan used to skip, so the second grant fix was never checked.
func TestU80TheScanSeesBothQuotedGrantFixes(t *testing.T) {
	found := map[string]bool{}
	for _, quoted := range u51QuotedFixes(t) {
		found[quoted] = true
	}
	for _, want := range []string{"grant role <reader or writer>@<database> to <user>", "grant role <db-manager or reader>@<database> to <user>"} {
		if !found[want] {
			t.Errorf("the scan of refusal texts lacks %q", want)
		}
	}
}

// TestMasterStatementIsAParseErrorNamingWhatToRun: there is no auth master on the ring, so
// 'master' is not a statement; the refusal says the CLI does not run it and where the list is.
func TestMasterStatementIsAParseErrorNamingWhatToRun(t *testing.T) {
	session, fake := u52Session()
	_, err := session.Execute(context.Background(), "master")
	var failed *RingError
	if !errors.As(err, &failed) || failed.Code != "PARSE_ERROR" || failed.Exit != ExitRefused ||
		!strings.Contains(failed.Message, "the CLI does not run this; 'help' lists the statements it runs") {
		t.Fatalf("master: %v (%+v)", err, failed)
	}
	if len(fake.calls) != 0 {
		t.Fatalf("master sent %v", fake.calls)
	}
	if _, err := HelpFor("master"); err == nil {
		t.Fatal("help still has a topic for master")
	}
}

// TestRingAPasswordLineTheStatementDidNotTakeIsNeverPrinted: a script that supplies more password
// lines than its source takes (a second line with --password-stdin, any line with
// --password-file, a third line otherwise) leaves a password where the next statement should be.
// The run stops on it without quoting it, and names the mistake.
func TestRingAPasswordLineTheStatementDidNotTakeIsNeverPrinted(t *testing.T) {
	const secret = "SecretEcho-Line-xyz1"
	file := filepath.Join(t.TempDir(), "new.pw")
	if err := os.WriteFile(file, []byte("file-s3cret-password\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		source func(*RingSession)
		lines  int
		want   string
	}{
		{"--password-stdin with two lines", WithRingPasswordSource("", true, io.Discard), 2, "--password-stdin"},
		{"--password-file with two lines", WithRingPasswordSource(file, false, io.Discard), 2, "--password-file"},
		{"--password-file with one line", WithRingPasswordSource(file, false, io.Discard), 1, "--password-file"},
		{"script with three lines", func(*RingSession) {}, 3, "two lines"},
	}
	for _, c := range cases {
		for _, statement := range []string{"create user bob", "alter user ada password"} {
			session, fake := u52Session()
			u80WithAda(fake)
			c.source(session)
			script := statement + "\n" + strings.Repeat(secret+"\n", c.lines) + "show users\n"
			var out, errOut strings.Builder
			status := session.RunPiped(context.Background(), strings.NewReader(script), &out, &errOut)
			printed := out.String() + errOut.String()
			if strings.Contains(printed, secret) {
				t.Errorf("%s, %s: printed the password line:\n%s", c.name, statement, printed)
			}
			if status != ExitUsage || !strings.Contains(errOut.String(), c.want) || !strings.Contains(errOut.String(), "not shown") {
				t.Errorf("%s, %s: status %d, stderr %q; want exit %d naming %s", c.name, statement, status, errOut.String(), ExitUsage, c.want)
			}
			if strings.Count(strings.Join(fake.calls, ","), "ShowUsers") > 1 {
				t.Errorf("%s, %s: the run went on past the stray line: %v", c.name, statement, fake.calls)
			}
		}
	}
	// A statement after the password lines still runs, and a parse error elsewhere still quotes.
	session, _ := u80Session()
	_, err := session.Execute(context.Background(), "frobnicate quoted-word")
	if err == nil || !strings.Contains(err.Error(), "frobnicate") {
		t.Fatalf("an ordinary parse error: %v", err)
	}
}
