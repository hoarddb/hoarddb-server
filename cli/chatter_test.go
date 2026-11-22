package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// splitSessionOptions are the options of a CLI that talks to the golden
// server, with results and chatter going to separate buffers.
func splitSessionOptions(session *goldenSession, results, chatter *bytes.Buffer) []CLIOption {
	return []CLIOption{
		WithAddress(session.addr), WithInsecureTLS(true), WithCredentials("admin", goldenPassword),
		WithTerminal(results), WithChatter(chatter), WithTheme("plain"),
	}
}

// TestChatterGoesToTheChatterSink (Part 5): the CLI's commentary about itself
// goes to the chatter writer and never to the results writer, so a one-shot
// run can put results on stdout and commentary on stderr.
func TestChatterGoesToTheChatterSink(t *testing.T) {
	t.Run("connect and authenticate", func(t *testing.T) {
		session := newGoldenSession(t)
		var results, chatter bytes.Buffer
		c := NewCLI(splitSessionOptions(session, &results, &chatter)...)
		if err := c.Connect(); err != nil {
			t.Fatalf("connect: %v", err)
		}
		t.Cleanup(c.Close)
		if err := c.handle("show databases"); err != nil {
			t.Fatalf("show databases: %v", err)
		}

		for _, want := range []string{"WARNING: TLS certificate verification is disabled", "Authenticated as admin", "Connected to " + session.addr} {
			if !strings.Contains(chatter.String(), want) {
				t.Errorf("chatter lacks %q:\n%s", want, chatter.String())
			}
			if strings.Contains(results.String(), want) {
				t.Errorf("results contain %q, which is commentary:\n%s", want, results.String())
			}
		}
		if !strings.Contains(results.String(), "DATABASES") {
			t.Errorf("results lack the statement's own output:\n%s", results.String())
		}
	})

	t.Run("token resume and the password fallback", func(t *testing.T) {
		session := newGoldenSession(t)
		var resumedResults, resumedChatter bytes.Buffer
		resumed := NewCLI(append(splitSessionOptions(session, &resumedResults, &resumedChatter), WithToken(session.cli.token))...)
		if err := resumed.Connect(); err != nil {
			t.Fatalf("connect with a valid token: %v", err)
		}
		t.Cleanup(resumed.Close)
		if !strings.Contains(resumedChatter.String(), "Authenticated as admin (resumed by token)") || resumedResults.Len() != 0 {
			t.Errorf("resume: chatter %q, results %q; want the line on chatter only", resumedChatter.String(), resumedResults.String())
		}

		var fallbackResults, fallbackChatter bytes.Buffer
		fallback := NewCLI(append(splitSessionOptions(session, &fallbackResults, &fallbackChatter), WithToken("not-a-token"))...)
		if err := fallback.Connect(); err != nil {
			t.Fatalf("connect with a bad token and a password: %v", err)
		}
		t.Cleanup(fallback.Close)
		if !strings.Contains(fallbackChatter.String(), "Token resume failed; falling back to password") || fallbackResults.Len() != 0 {
			t.Errorf("fallback: chatter %q, results %q; want the line on chatter only", fallbackChatter.String(), fallbackResults.String())
		}
	})

	t.Run("the trust prompt", func(t *testing.T) {
		session := newGoldenSession(t)
		var results, chatter bytes.Buffer
		c := NewCLI(
			WithAddress(session.addr), WithCredentials("admin", goldenPassword), WithTerminal(&results), WithChatter(&chatter),
			WithKnownServersPath(cliKnownServersPath(t)), WithInput(strings.NewReader("")), WithTheme("plain"),
		)
		if err := c.Connect(); err == nil {
			t.Fatal("connected to a server nobody trusts")
		}
		for _, want := range []string{"The authenticity of server", "ED25519 key fingerprint is", "Refusing to trust it non-interactively"} {
			if !strings.Contains(chatter.String(), want) {
				t.Errorf("chatter lacks %q:\n%s", want, chatter.String())
			}
		}
		if results.Len() != 0 {
			t.Errorf("results should be empty, got %q", results.String())
		}
	})

	t.Run("debug lines", func(t *testing.T) {
		session := newGoldenSession(t)
		var results, chatter bytes.Buffer
		c := NewCLI(append(splitSessionOptions(session, &results, &chatter), WithDebug(true))...)
		if err := c.Connect(); err != nil {
			t.Fatalf("connect: %v", err)
		}
		t.Cleanup(c.Close)
		runQuietOn(t, c, "create database app", "use app", "create bucket Users { type: hash }")
		results.Reset()
		chatter.Reset()
		if err := c.handle("db.Users.count()"); err != nil {
			t.Fatalf("count: %v", err)
		}
		if !strings.Contains(chatter.String(), "[DEBUG] Command:") || !strings.Contains(chatter.String(), "[COUNT] db.Users.count(") {
			t.Errorf("chatter lacks the debug lines:\n%s", chatter.String())
		}
		if strings.Contains(results.String(), "[DEBUG]") || strings.Contains(results.String(), "[COUNT]") {
			t.Errorf("results contain debug lines:\n%s", results.String())
		}
	})

	t.Run("the password file warning", func(t *testing.T) {
		session := newGoldenSession(t)
		var results, chatter bytes.Buffer
		c := NewCLI(splitSessionOptions(session, &results, &chatter)...)
		if err := c.Connect(); err != nil {
			t.Fatalf("connect: %v", err)
		}
		t.Cleanup(c.Close)
		readable := filepath.Join(t.TempDir(), "password")
		if err := os.WriteFile(readable, []byte(goldenUserPassword+"\n"), 0o644); err != nil {
			t.Fatalf("writing the password file: %v", err)
		}
		c.handler.SetPasswordSource(readable, false)
		results.Reset()
		chatter.Reset()
		if err := c.handle("create user bob roles [reader@app]"); err != nil {
			t.Fatalf("create user: %v", err)
		}
		if !strings.Contains(chatter.String(), "warning: password file") {
			t.Errorf("chatter lacks the warning:\n%s", chatter.String())
		}
		if strings.Contains(results.String(), "warning: password file") || !strings.Contains(results.String(), `user "bob" created`) {
			t.Errorf("results %q: want the created line only", results.String())
		}
	})
}

// TestOneWriterByDefault: without WithChatter, commentary and results share
// the terminal in the order they always did, which is what keeps every REPL
// transcript unchanged.
func TestOneWriterByDefault(t *testing.T) {
	session := newGoldenSession(t)
	var terminal bytes.Buffer
	c := NewCLI(WithAddress(session.addr), WithInsecureTLS(true), WithCredentials("admin", goldenPassword), WithTerminal(&terminal), WithTheme("plain"))
	if err := c.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(c.Close)
	if err := c.handle("show databases"); err != nil {
		t.Fatalf("show databases: %v", err)
	}
	output := terminal.String()
	order := []string{"WARNING: TLS certificate verification is disabled", "Authenticated as admin", "Connected to " + session.addr, "DATABASES"}
	position := 0
	for _, want := range order {
		index := strings.Index(output[position:], want)
		if index < 0 {
			t.Fatalf("terminal lacks %q after offset %d, in this order %v:\n%s", want, position, order, output)
		}
		position += index + len(want)
	}
}

// runQuietOn runs statements on c and fails the test if one errors.
func runQuietOn(t *testing.T, c *CLI, lines ...string) {
	t.Helper()
	for _, line := range lines {
		if err := c.handle(line); err != nil {
			t.Fatalf("%q failed: %v", line, err)
		}
	}
}
