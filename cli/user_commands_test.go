package cli

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

func TestParseUserManagementCommands(t *testing.T) {
	cases := []struct {
		line   string
		typ    string
		method string
		name   string
		option string
		value  string
	}{
		{"create user alice roles [writer@app, reader@logs]", "user", "create_user", "alice", "roles", "[writer@app, reader@logs]"},
		{"create user alice", "user", "create_user", "alice", "", ""},
		{`create user "bob"`, "user", "create_user", "bob", "", ""},
		{"alter user alice disable", "user", "alter_user", "alice", "detail", "disable"},
		{"alter user alice enable", "user", "alter_user", "alice", "detail", "enable"},
		{"alter user alice password", "user", "alter_user", "alice", "detail", "password"},
		{"drop user alice", "user", "drop_user", "alice", "", ""},
		{"grant role db-manager@app to alice", "user", "grant_role", "alice", "role", "db-manager@app"},
		{"revoke role reader@logs from alice", "user", "revoke_role", "alice", "role", "reader@logs"},
		{"user list", "user", "list_users", "", "", ""},
		{"user describe alice", "user", "describe_user", "alice", "", ""},
		{"master", "global", "master", "", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.line, func(t *testing.T) {
			cmd, err := ParseCommand(tc.line)
			if err != nil {
				t.Fatalf("ParseCommand(%q): %v", tc.line, err)
			}
			if cmd.Type != tc.typ || cmd.Method != tc.method {
				t.Fatalf("got %s/%s, want %s/%s", cmd.Type, cmd.Method, tc.typ, tc.method)
			}
			if cmd.Name != tc.name {
				t.Fatalf("name = %q, want %q", cmd.Name, tc.name)
			}
			if tc.option != "" {
				if got := cmd.Options[tc.option]; got != tc.value {
					t.Fatalf("option %s = %v, want %q", tc.option, got, tc.value)
				}
			}
		})
	}
}

func TestParseCreateUserRefusesCommandLinePassword(t *testing.T) {
	if _, err := ParseCommand(`create user alice with password "secret"`); err == nil {
		t.Fatal("a password on the command line was accepted; it must be refused so it cannot reach ps or shell history")
	}
}

func TestParseRoleSpec(t *testing.T) {
	spec, err := parseRoleSpec("writer@app")
	if err != nil || spec.Role != "writer" || spec.DB != "app" {
		t.Fatalf("writer@app = %+v, %v", spec, err)
	}
	spec, err = parseRoleSpec("read")
	if err != nil || spec.Role != "read" || spec.DB != "*" {
		t.Fatalf("bare read = %+v, %v", spec, err)
	}
	if _, err := parseRoleSpec("reader@"); err == nil {
		t.Fatal("reader@ parsed, want an error")
	}
}

func TestRolesFromCommand(t *testing.T) {
	cmd, err := ParseCommand("create user alice roles [writer@app, reader@logs]")
	if err != nil {
		t.Fatalf("ParseCommand: %v", err)
	}
	roles, err := rolesFromCommand(cmd)
	if err != nil {
		t.Fatalf("rolesFromCommand: %v", err)
	}
	if len(roles) != 2 || roles[0].Role != "writer" || roles[0].DB != "app" || roles[1].Role != "reader" || roles[1].DB != "logs" {
		t.Fatalf("roles = %+v", roles)
	}
}

// TestGrantAccessPointsAtRoleForm keeps the old access verb from pretending
// to work while still telling the operator the supported spelling.
func TestGrantAccessPointsAtRoleForm(t *testing.T) {
	cmd, err := ParseCommand("grant access to app for alice")
	if err != nil {
		t.Fatalf("ParseCommand: %v", err)
	}
	h := &CommandHandler{terminal: &strings.Builder{}}
	if err := h.Handle(cmd); err == nil || !strings.Contains(err.Error(), "grant role") {
		t.Fatalf("grant access error = %v, want a pointer at 'grant role'", err)
	}
}

// TestMissingFields is the rolling-upgrade guard: a server that does not know
// a field cannot name it in Changed, so a missing entry means the change was
// not applied (spec 6.1 item 3).
func TestMissingFields(t *testing.T) {
	if got := missingFields([]string{"credentials"}, []string{"credentials"}); len(got) != 0 {
		t.Fatalf("missingFields = %v, want none", got)
	}
	if got := missingFields(nil, []string{"credentials"}); len(got) != 1 || got[0] != "credentials" {
		t.Fatalf("missingFields on an echo-less response = %v, want [credentials]", got)
	}
	if got := missingFields([]string{"roles"}, []string{"credentials"}); len(got) != 1 || got[0] != "credentials" {
		t.Fatalf("missingFields = %v, want [credentials]", got)
	}
	if got := missingFields([]string{"roles", "enabled"}, []string{"roles", "enabled"}); len(got) != 0 {
		t.Fatalf("missingFields = %v, want none", got)
	}
}

// TestAlterUserRolesKeepsTheCaseOfRolesAndDatabases: the roles
// expression was cut from a lowercased copy of the statement, so a database
// named Shop was stored as shop and the grant matched nothing.
func TestAlterUserRolesKeepsTheCaseOfRolesAndDatabases(t *testing.T) {
	session := newGoldenSession(t)
	runQuiet(t, session, "create database Shop", "create user alice")
	transcript := runStatements(t, session.cli, session.out,
		"alter user alice roles [reader@Shop, writer@Shop]",
		"user describe alice",
	)
	if !strings.Contains(transcript, `user "alice" altered`) {
		t.Fatalf("alter did not succeed:\n%s", transcript)
	}
	if !strings.Contains(transcript, "reader@Shop") || !strings.Contains(transcript, "writer@Shop") {
		t.Errorf("the roles were not stored as typed:\n%s", transcript)
	}
	if strings.Contains(transcript, "@shop") {
		t.Errorf("the database name was lowercased:\n%s", transcript)
	}
}

// TestAlterUserKeywordsAreCaseInsensitive keeps the keyword matching that the
// lowercased copy existed for: only the keywords fold, not the names.
func TestAlterUserKeywordsAreCaseInsensitive(t *testing.T) {
	session := newGoldenSession(t)
	runQuiet(t, session, "create database Shop", "create user alice")
	transcript := runStatements(t, session.cli, session.out,
		"alter user alice ROLES [reader@Shop]",
		"alter user alice DISABLE",
		"user describe alice",
	)
	if strings.Count(transcript, `user "alice" altered`) != 2 || !strings.Contains(transcript, "reader@Shop") ||
		!strings.Contains(transcript, "disabled") {
		t.Errorf("upper-case keywords were not accepted, or the names were changed:\n%s", transcript)
	}
}

// pipedSession runs script as a piped session against the server of session,
// the way `printf ... | hoarddb` does, and returns what it printed and the
// error Run reported.
func pipedSession(t *testing.T, session *goldenSession, script string, extra ...CLIOption) (string, error) {
	t.Helper()
	var out bytes.Buffer
	options := append([]CLIOption{
		WithAddress(session.addr), WithInsecureTLS(true), WithCredentials("admin", goldenPassword),
		WithTerminal(&out), WithTheme("plain"), WithInput(strings.NewReader(script)),
	}, extra...)
	c := NewCLI(options...)
	if err := c.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(c.Close)
	runErr := c.Run()
	return out.String(), runErr
}

// requireCanLogIn fails unless username can authenticate with password.
func requireCanLogIn(t *testing.T, session *goldenSession, username, password string) {
	t.Helper()
	var out bytes.Buffer
	c := NewCLI(WithAddress(session.addr), WithInsecureTLS(true), WithCredentials(username, password),
		WithTerminal(&out), WithTheme("plain"))
	if err := c.Connect(); err != nil {
		t.Fatalf("user %q cannot log in with the password the script supplied: %v", username, err)
	}
	c.Close()
}

// TestPipedCreateUserReadsItsPasswordFromTheScript: a piped
// `create user` took its two password lines from a second buffered reader of
// stdin and failed with a bare EOF, abandoning the rest of the script.
func TestPipedCreateUserReadsItsPasswordFromTheScript(t *testing.T) {
	session := newGoldenSession(t)
	script := "create database app\ncreate user bob roles [reader@app]\nsecretpw-long\nsecretpw-long\nshow databases\nexit\n"
	out, err := pipedSession(t, session, script)
	if err != nil {
		t.Fatalf("piped session failed: %v\noutput:\n%s", err, out)
	}
	if !strings.Contains(out, `user "bob" created`) {
		t.Errorf("the user was not created:\n%s", out)
	}
	if !strings.Contains(out, "app") || !strings.Contains(out, "Goodbye.") {
		t.Errorf("the statements after the password lines did not run:\n%s", out)
	}
	requireCanLogIn(t, session, "bob", "secretpw-long")
}

// TestPipedCreateUserWithoutPasswordLinesNamesTheFile checks that a script that ends where the
// password should be fails with a message that names the way out.
func TestPipedCreateUserWithoutPasswordLinesNamesTheFile(t *testing.T) {
	session := newGoldenSession(t)
	_, err := pipedSession(t, session, "create user bob roles [reader@app]\n")
	if err == nil {
		t.Fatal("a missing password in a piped session was accepted")
	}
	if !strings.Contains(err.Error(), "--password-file") {
		t.Errorf("error %q does not name --password-file", err)
	}
	if strings.Contains(err.Error(), "EOF") {
		t.Errorf("error %q is the bare EOF", err)
	}
}

// TestPipedCreateUserWhoseStatementsAreReadAsThePasswordNamesTheFile: a script
// that leaves out its password lines has its next statements read as the
// password. The refusal must say so, name --password-file, and never print the
// consumed lines, which could be a real password.
func TestPipedCreateUserWhoseStatementsAreReadAsThePasswordNamesTheFile(t *testing.T) {
	scripts := map[string]string{
		"two different statements": "create user dave roles [reader@app]\ncreate database leaked-marker-one\nshow databases\n",
		"an empty line":            "create user dave roles [reader@app]\n\n\nshow databases\n",
	}
	for name, script := range scripts {
		t.Run(name, func(t *testing.T) {
			session := newGoldenSession(t)
			_, err := pipedSession(t, session, script)
			if err == nil {
				t.Fatal("a create user with no password lines was accepted")
			}
			for _, want := range []string{"--password-file", "read as the password"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q lacks %q", err, want)
				}
			}
			if strings.Contains(err.Error(), "did not match") || strings.Contains(err.Error(), "leaked-marker-one") {
				t.Errorf("error %q blames a mismatch or echoes a script line", err)
			}
		})
	}
}

// TestPasswordStdinIsRefusedOnATerminal: without a piped session there is no
// stream to read the line from, and a line typed in the clear defeats the flag.
func TestPasswordStdinIsRefusedOnATerminal(t *testing.T) {
	handler := &CommandHandler{passwordStdin: true}
	_, err := handler.resolveNewPassword()
	if err == nil || !strings.Contains(err.Error(), "--password-file") {
		t.Errorf("--password-stdin on a terminal: error %v, want a refusal naming --password-file", err)
	}
}

// TestPasswordStdinDoesNotCompeteWithTheStatementStream checks that
// --password-stdin is the next line of the same stream the statements come
// from, read by the one reader that owns it.
func TestPasswordStdinDoesNotCompeteWithTheStatementStream(t *testing.T) {
	session := newGoldenSession(t)
	script := "create user carol roles [reader@app]\nsecretpw-long\nshow databases\nexit\n"
	out, err := pipedSession(t, session, script, WithNewPasswordSource("", true))
	if err != nil {
		t.Fatalf("piped session failed: %v\noutput:\n%s", err, out)
	}
	if !strings.Contains(out, `user "carol" created`) || !strings.Contains(out, "Goodbye.") {
		t.Errorf("the user was not created, or the statements after the password line did not run:\n%s", out)
	}
	requireCanLogIn(t, session, "carol", "secretpw-long")
}

// passwordOfLength returns a distinctive password of exactly n characters, so
// a test can tell whether an error message echoed it.
func passwordOfLength(t *testing.T, n int) string {
	t.Helper()
	const source = "Qx7-correct-horse-battery-staple-Zq9"
	if n > len(source) {
		t.Fatalf("passwordOfLength(%d): the source has only %d characters", n, len(source))
	}
	return source[:n]
}

// minimumSession starts a real server whose password_min_length is minimum
// (0 keeps the default) and returns a session a piped CLI can reach.
func minimumSession(t *testing.T, minimum int) *goldenSession {
	t.Helper()
	_, cfg := startLoopbackService(t, goldenPassword, func(cfg *config.Config) {
		if minimum > 0 {
			cfg.Auth.PasswordMinLength = minimum
		}
	})
	return &goldenSession{addr: cfg.TCP.Listen}
}

// userFound reports whether the server of session has a user called username.
func userFound(t *testing.T, session *goldenSession, username string) bool {
	t.Helper()
	var out bytes.Buffer
	c := NewCLI(WithAddress(session.addr), WithInsecureTLS(true), WithCredentials("admin", goldenPassword),
		WithTerminal(&out), WithTheme("plain"))
	if err := c.Connect(); err != nil {
		t.Fatalf("connect to read user %q: %v", username, err)
	}
	defer c.Close()
	// USER_GET answers a missing user with an error frame, so the list is
	// the unambiguous way to ask.
	var resp server.UserListResponse
	if err := c.handler.send(transport.OpUserList, &server.UserListRequest{}, &resp); err != nil {
		t.Fatalf("listing users to find %q: %v", username, err)
	}
	for _, user := range resp.Users {
		if user.Username == username {
			return true
		}
	}
	return false
}

// requireShortPasswordRefusal fails unless err is the refusal of a password
// of length characters against a server minimum of minimum: it names both
// numbers and the setting, and never echoes the password.
func requireShortPasswordRefusal(t *testing.T, err error, password string, minimum int) {
	t.Helper()
	if err == nil {
		t.Fatalf("a password of %d characters was accepted by a server whose minimum is %d", len(password), minimum)
	}
	for _, want := range []string{
		fmt.Sprintf("%d characters", len(password)),
		fmt.Sprintf("at least %d", minimum),
		"HOARDDB_PASSWORD_MIN_LENGTH",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not say %q", err, want)
		}
	}
	if strings.Contains(err.Error(), password) {
		t.Errorf("refusal %q echoes the password", err)
	}
}

// createUserScript is a piped `create user` with its --password-stdin line.
func createUserScript(username, password string) string {
	return "create user " + username + " roles [reader@app]\n" + password + "\n"
}

// alterPasswordScript is a piped `alter user ... password` with its
// --password-stdin line.
func alterPasswordScript(username, password string) string {
	return "alter user " + username + " password\n" + password + "\n"
}

// TestUserCreateRefusesAPasswordShorterThanTheServersMinimum (BUG-0493): the
// server cannot measure a password it never receives, so the CLI refuses one
// below the minimum the server reports, before deriving anything, and the user
// is not created. A password at the minimum is accepted.
func TestUserCreateRefusesAPasswordShorterThanTheServersMinimum(t *testing.T) {
	session := minimumSession(t, 0)
	short := passwordOfLength(t, 11)
	_, err := pipedSession(t, session, createUserScript("shortpw", short), WithNewPasswordSource("", true))
	requireShortPasswordRefusal(t, err, short, 12)
	if userFound(t, session, "shortpw") {
		t.Error("the user was created although its password was refused")
	}

	twelve := passwordOfLength(t, 12)
	out, err := pipedSession(t, session, createUserScript("longpw", twelve), WithNewPasswordSource("", true))
	if err != nil {
		t.Fatalf("a 12-character password was refused: %v\noutput:\n%s", err, out)
	}
	if !userFound(t, session, "longpw") {
		t.Error("user list does not show the user created with a 12-character password")
	}
	requireCanLogIn(t, session, "longpw", twelve)
}

// TestAlterUserPasswordRefusesAPasswordShorterThanTheServersMinimum
// (BUG-0493): a password change below the minimum is refused and the old
// password still works; one at the minimum replaces it.
func TestAlterUserPasswordRefusesAPasswordShorterThanTheServersMinimum(t *testing.T) {
	session := minimumSession(t, 0)
	original := passwordOfLength(t, 14)
	if out, err := pipedSession(t, session, createUserScript("bob", original), WithNewPasswordSource("", true)); err != nil {
		t.Fatalf("creating the user: %v\noutput:\n%s", err, out)
	}

	short := passwordOfLength(t, 11)
	_, err := pipedSession(t, session, alterPasswordScript("bob", short), WithNewPasswordSource("", true))
	requireShortPasswordRefusal(t, err, short, 12)
	requireCanLogIn(t, session, "bob", original)

	twelve := passwordOfLength(t, 12)
	if out, err := pipedSession(t, session, alterPasswordScript("bob", twelve), WithNewPasswordSource("", true)); err != nil {
		t.Fatalf("a 12-character password change was refused: %v\noutput:\n%s", err, out)
	}
	requireCanLogIn(t, session, "bob", twelve)
}

// TestCLIUsesTheServersConfiguredPasswordMinimum (BUG-0493): the number comes
// from the server, not from a constant in the CLI, so a server configured for
// 16 refuses 12 and 15 through the CLI and accepts 16.
func TestCLIUsesTheServersConfiguredPasswordMinimum(t *testing.T) {
	session := minimumSession(t, 16)
	for _, length := range []int{12, 15} {
		password := passwordOfLength(t, length)
		username := fmt.Sprintf("user%d", length)
		_, err := pipedSession(t, session, createUserScript(username, password), WithNewPasswordSource("", true))
		requireShortPasswordRefusal(t, err, password, 16)
		if userFound(t, session, username) {
			t.Errorf("user %q was created although its %d-character password was refused", username, length)
		}
	}

	sixteen := passwordOfLength(t, 16)
	if out, err := pipedSession(t, session, createUserScript("user16", sixteen), WithNewPasswordSource("", true)); err != nil {
		t.Fatalf("a 16-character password was refused by a server whose minimum is 16: %v\noutput:\n%s", err, out)
	}
	requireCanLogIn(t, session, "user16", sixteen)

	fifteen := passwordOfLength(t, 15)
	_, err := pipedSession(t, session, alterPasswordScript("user16", fifteen), WithNewPasswordSource("", true))
	requireShortPasswordRefusal(t, err, fifteen, 16)
	requireCanLogIn(t, session, "user16", sixteen)
}

// TestPasswordMinimumCountsCharactersNotBytes: the documented minimum is in
// characters, so twelve two-byte characters meet a minimum of 12 and eleven
// do not, although both are more than 12 bytes.
func TestPasswordMinimumCountsCharactersNotBytes(t *testing.T) {
	handler := &CommandHandler{passwordMinLength: 12}
	if err := handler.checkPasswordMinimum("ana", strings.Repeat("é", 12)); err != nil {
		t.Errorf("twelve two-byte characters were refused: %v", err)
	}
	err := handler.checkPasswordMinimum("ana", strings.Repeat("é", 11))
	if err == nil || !strings.Contains(err.Error(), "11 characters") {
		t.Errorf("eleven two-byte characters (22 bytes): error %v, want a refusal counting 11 characters", err)
	}
}

// TestNewPasswordIsRefusedWhenTheServerReportsNoMinimum: a server that does
// not report password_min_length gives the CLI nothing to check against, and
// the CLI never substitutes a constant of its own, so it refuses and names the
// fix.
func TestNewPasswordIsRefusedWhenTheServerReportsNoMinimum(t *testing.T) {
	handler := &CommandHandler{}
	err := handler.checkPasswordMinimum("ana", passwordOfLength(t, 30))
	if err == nil {
		t.Fatal("a new password was accepted with no minimum reported by the server")
	}
	for _, want := range []string{"did not report", "upgrade the server", "nothing was changed"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not say %q", err, want)
		}
	}
}

// TestUserAlterReportsUnknownUser is BUG-0622: the server answers USER_GET for
// an unknown user with found false, so alter and describe print the CLI's own
// sentence instead of the wire error under a "reading user" prefix.
func TestUserAlterReportsUnknownUser(t *testing.T) {
	session := newGoldenSession(t)
	want := `user "ghost" not found; run 'user list' to see the users`
	for _, script := range []string{"alter user ghost disable\nexit\n", "user describe ghost\nexit\n"} {
		_, err := pipedSession(t, session, script)
		if err == nil {
			t.Fatalf("%q: no error for an unknown user", script)
		}
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%q: error = %q, want it to contain %q", script, err, want)
		}
		if strings.Contains(err.Error(), "reading user") || strings.Contains(err.Error(), "describing user") {
			t.Errorf("%q: error = %q carries the wire-error prefix, so the found:false branch did not run", script, err)
		}
	}
}

// TestMasterAndUserListPrintTheMastersClientAddress is BUG-0709: the address
// the CLI prints for the master is the one the server gave, and an address that
// has not arrived is said so, never filled in.
func TestMasterAndUserListPrintTheMastersClientAddress(t *testing.T) {
	cases := []struct {
		name       string
		masterAddr string
		masterNote string
		wantMaster string
		wantList   string
	}{
		{
			name: "address known", masterAddr: "192.0.2.4:7433",
			wantMaster: "cluster master for user changes: node-a at 192.0.2.4:7433\n",
			wantList:   "USERS (master: node-a at 192.0.2.4:7433, config version 2)\n",
		},
		{
			name: "address withheld with a reason", masterAddr: "", masterNote: "it accepts clients on loopback only (127.0.0.1:7433); on that node set tcp.listen to 0.0.0.0:7433",
			wantMaster: "cluster master for user changes: node-a (no address to give: it accepts clients on loopback only (127.0.0.1:7433); on that node set tcp.listen to 0.0.0.0:7433)\n",
			wantList:   "USERS (master: node-a (no address to give: it accepts clients on loopback only (127.0.0.1:7433); on that node set tcp.listen to 0.0.0.0:7433), config version 2)\n",
		},
		{
			name: "address not known yet", masterAddr: "",
			wantMaster: "cluster master for user changes: node-a (its client address has not reached this node yet; run 'master' again in a few seconds)\n",
			wantList:   "USERS (master: node-a (its client address has not reached this node yet; run 'master' again in a few seconds), config version 2)\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var terminal bytes.Buffer
			script := bannerScript()
			script[transport.OpUserList] = server.UserListResponse{Master: "node-a", MasterAddr: tc.masterAddr, MasterAddrNote: tc.masterNote, Version: 2}
			_, c := newStubServer(t, script, &terminal)

			terminal.Reset()
			if err := c.handle("master"); err != nil {
				t.Fatalf("master: %v", err)
			}
			if got := terminal.String(); got != tc.wantMaster {
				t.Errorf("master printed %q, want %q", got, tc.wantMaster)
			}

			terminal.Reset()
			if err := c.handle("user list"); err != nil {
				t.Fatalf("user list: %v", err)
			}
			if got := strings.SplitAfter(terminal.String(), "\n")[0]; got != tc.wantList {
				t.Errorf("user list header = %q, want %q", got, tc.wantList)
			}
		})
	}
}
