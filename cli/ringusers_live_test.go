package cli

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

const u80RootPassword = "live-test-password"

// u80Passwords writes a 0600 password file the user statements read a new password from.
func u80Passwords(t *testing.T, password string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "new.pw")
	if err := os.WriteFile(path, []byte(password+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// u80Live opens a ring session for user on a live server whose new passwords come from passwordFile.
func u80Live(t *testing.T, addr, user, password, passwordFile string) (*RingSession, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	client, err := ringclient.Dial(ctx, ringclient.Options{Addr: addr, User: user, Password: password, Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return NewRingSession(NewRingAPI(client), WithRingPasswordSource(passwordFile, false, io.Discard)), ctx
}

// TestU80LiveUsersAreCreatedGrantedListedAndRevoked runs the lifecycle against the real server's
// user store over the ring; every line keeps the shape the v1.0 handlers printed.
func TestU80LiveUsersAreCreatedGrantedListedAndRevoked(t *testing.T) {
	addr := u52LiveServer(t)
	session, ctx := u80Live(t, addr, "root", u80RootPassword, u80Passwords(t, "alice-s3cret-pw"))
	got := u52Transcript(t, ctx, session,
		"create database app",
		"create user alice roles [reader@app]",
		"grant role writer@app to alice",
		"user describe alice",
		"show users",
		"revoke role reader@app from alice",
		"alter user alice disable",
		"alter user alice enable",
		"alter user alice roles [db-manager@app, reader@*]",
		"user list",
		"alter user alice password",
		"drop user alice",
		"user list")
	t.Logf("\n%s", got)
	if strings.Contains(got, "alice-s3cret-pw") || strings.Contains(got, "master") {
		t.Errorf("the transcript shows the password or an auth master:\n%s", got)
	}
	for _, want := range []string{
		`user "alice" created (revision `,
		`granted writer@app to "alice" (revision `,
		"status: enabled",
		"roles:  [reader@app, writer@app]",
		"USERS (revision ",
		"alice: enabled, roles [reader@app, writer@app], epoch ",
		`revoked reader@app from "alice" (revision `,
		`user "alice" altered (revision `,
		"alice: enabled, roles [db-manager@app, reader@*]",
		`user "alice" dropped`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("transcript lacks %q", want)
		}
	}
}

func TestU80LiveUserFailuresCarryTheServerText(t *testing.T) {
	addr := u52LiveServer(t)
	session, ctx := u80Live(t, addr, "root", u80RootPassword, u80Passwords(t, "alice-s3cret-pw"))
	u52Transcript(t, ctx, session, "create user alice roles [reader@app]")
	cases := []struct{ statement, text string }{
		{"create user alice", "alice"},
		{"grant role writer@app to nobody", "nobody"},
		{"grant role boss@app to alice", "boss"},
		{"drop user nobody", `NOT_FOUND: drop user "nobody" was not applied: there is no user "nobody". Run 'show users' to list the users.`},
		{"user describe nobody", `NOT_FOUND: there is no user "nobody". Run 'show users' to list the users.`},
	}
	for _, c := range cases {
		_, err := session.Execute(ctx, c.statement)
		if err == nil || !strings.Contains(err.Error(), c.text) {
			t.Errorf("%s: %v, want text containing %q", c.statement, err, c.text)
		}
	}
	short := u80Passwords(t, "short")
	shortSession, ctx2 := u80Live(t, addr, "root", u80RootPassword, short)
	_, err := shortSession.Execute(ctx2, "create user bob")
	if err == nil || !strings.Contains(err.Error(), "this server requires at least") {
		t.Fatalf("a short password: %v", err)
	}
}

// TestU80LiveNonAdminIsRefused: the same user statements from a non-admin user are refused with
// a message and a non-zero status, and nothing changes.
func TestU80LiveNonAdminIsRefused(t *testing.T) {
	addr := u52LiveServer(t)
	passwords := u80Passwords(t, "alice-s3cret-pw")
	session, ctx := u80Live(t, addr, "root", u80RootPassword, passwords)
	u52Transcript(t, ctx, session, "create database app", "create user alice roles [reader@app]")
	for _, statement := range []string{"create user bob", "grant role writer@app to alice", "drop user alice", "user list", "alter user alice disable"} {
		var out, errOut bytes.Buffer
		args := []string{"-insecure", "-address", addr, "-user", "alice", "-password=alice-s3cret-pw", "-password-file", passwords}
		status := RunMain(Invocation{Args: args, Stdin: strings.NewReader(statement + "\n"), Stdout: &out, Stderr: &errOut, Getenv: func(string) string { return "" }})
		if errOut.Len() == 0 || status == 0 {
			t.Errorf("%s: a non-admin user got status %d and %q", statement, status, errOut.String())
		}
	}
}

// TestU80LivePipedSessionReadsThePasswordFromTheScript drives the binary path: the lines after
// create user are the password, as in v1.0.
func TestU80LivePipedSessionReadsThePasswordFromTheScript(t *testing.T) {
	addr := u52LiveServer(t)
	var out, errOut bytes.Buffer
	script := "create database app\ncreate user alice roles [writer@app]\nalice-s3cret-pw\nalice-s3cret-pw\nshow users\n"
	status := RunMain(Invocation{Args: []string{"-insecure", "-address", addr, "-user", "root", "-password=" + u80RootPassword},
		Stdin: strings.NewReader(script), Stdout: &out, Stderr: &errOut, Getenv: func(string) string { return "" }})
	if status != 0 || !strings.Contains(out.String(), `user "alice" created`) || !strings.Contains(out.String(), "alice: enabled, roles [writer@app]") {
		t.Fatalf("status %d\nstdout %q\nstderr %q", status, out.String(), errOut.String())
	}
	// The new user can log in with that password.
	client, err := ringclient.Dial(context.Background(), ringclient.Options{Addr: addr, User: "alice", Password: "alice-s3cret-pw", Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
}

// TestRingLivePasswordFlagsReachTheRingSession drives the binary with --password-stdin and
// --password-file against a real server: the new users log in with those passwords, and neither
// password appears in anything the run printed.
func TestRingLivePasswordFlagsReachTheRingSession(t *testing.T) {
	addr := u52LiveServer(t)
	run := func(script string, flags ...string) (string, string) {
		var out, errOut bytes.Buffer
		args := append([]string{"-insecure", "-address", addr, "-user", "root", "-password=" + u80RootPassword}, flags...)
		status := RunMain(Invocation{Args: args, Stdin: strings.NewReader(script), Stdout: &out, Stderr: &errOut, Getenv: func(string) string { return "" }})
		if status != 0 {
			t.Fatalf("%v: status %d\nstdout %q\nstderr %q", flags, status, out.String(), errOut.String())
		}
		return out.String(), errOut.String()
	}
	stdout, stderr := run("create user bob roles [reader@*]\nbob-stdin-password\nshow users\n", "--password-stdin")
	if strings.Contains(stdout+stderr, "bob-stdin-password") || !strings.Contains(stdout, "bob: enabled, roles [reader@*]") {
		t.Fatalf("stdin run printed\n%s\n%s", stdout, stderr)
	}
	stdout, stderr = run("create user carol\nalter user carol password\n", "--password-file", u80Passwords(t, "carol-file-password"))
	if strings.Contains(stdout+stderr, "carol-file-password") || !strings.Contains(stdout, `user "carol" altered (revision `) {
		t.Fatalf("file run printed\n%s\n%s", stdout, stderr)
	}
	for user, password := range map[string]string{"bob": "bob-stdin-password", "carol": "carol-file-password"} {
		client, err := ringclient.Dial(context.Background(), ringclient.Options{Addr: addr, User: user, Password: password, Insecure: true})
		if err != nil {
			t.Fatalf("%s cannot log in with the password the CLI read: %v", user, err)
		}
		_ = client.Close()
	}
}

// TestRingLivePermanentUserRefusalsExit2: a refusal a retry never clears exits 2, so a wrapper that
// retries on 8 does not loop: a name that exists, and the last administrator.
func TestRingLivePermanentUserRefusalsExit2(t *testing.T) {
	addr := u52LiveServer(t)
	passwords := u80Passwords(t, "alice-s3cret-pw")
	for _, c := range []struct{ statement, code string }{
		{"create user root", "USER_EXISTS"},
		{"drop user root", "INVALID_ARGUMENT"},
	} {
		var out, errOut bytes.Buffer
		args := []string{"-insecure", "-address", addr, "-user", "root", "-password=" + u80RootPassword, "-password-file", passwords}
		status := RunMain(Invocation{Args: args, Stdin: strings.NewReader(c.statement + "\n"), Stdout: &out, Stderr: &errOut, Getenv: func(string) string { return "" }})
		if status != ExitRefused || !strings.Contains(errOut.String(), c.code) {
			t.Errorf("%s: status %d, stderr %q; want exit %d and %s", c.statement, status, errOut.String(), ExitRefused, c.code)
		}
	}
}

// TestRingLiveAlterPasswordReplacesTheCredential: create with one password, change it to another
// from a second file, then log in: the new password works and the old one is refused. Two files,
// so an alter that silently kept the old credential cannot pass.
func TestRingLiveAlterPasswordReplacesTheCredential(t *testing.T) {
	addr := u52LiveServer(t)
	create, ctx := u80Live(t, addr, "root", u80RootPassword, u80Passwords(t, "first-password-1"))
	u52Transcript(t, ctx, create, "create user dora")
	alter, ctx2 := u80Live(t, addr, "root", u80RootPassword, u80Passwords(t, "second-password-2"))
	u52Transcript(t, ctx2, alter, "alter user dora password")
	client, err := ringclient.Dial(ctx2, ringclient.Options{Addr: addr, User: "dora", Password: "second-password-2", Insecure: true})
	if err != nil {
		t.Fatalf("the new password does not log in: %v", err)
	}
	_ = client.Close()
	if old, err := ringclient.Dial(ctx2, ringclient.Options{Addr: addr, User: "dora", Password: "first-password-1", Insecure: true}); err == nil {
		_ = old.Close()
		t.Fatal("the old password still logs in after alter user password")
	}
}
