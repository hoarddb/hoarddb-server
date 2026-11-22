package cli

import (
	"bytes"
	"flag"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
)

// runMainCapturing runs RunMain with the given arguments and an empty stdin and
// returns the exit code and both streams. Getenv answers nothing, so the
// developer's HOARDDB_* variables cannot change the outcome.
func runMainCapturing(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	code = RunMain(Invocation{
		Args:   args,
		Stdin:  strings.NewReader(""),
		Stdout: &outBuf,
		Stderr: &errBuf,
		Getenv: func(string) string { return "" },
	})
	return code, outBuf.String(), errBuf.String()
}

// TestAddressFlagHelpDescribesTheConfigOverride: without -address the session,
// dump, restore and ingest dial the tcp.listen address of the loaded
// configuration, so the printed default alone ("localhost:7433") named an
// address the CLI never used (BUG-0274). Every command's help must say so.
func TestAddressFlagHelpDescribesTheConfigOverride(t *testing.T) {
	sessionFlags := flag.NewFlagSet("session", flag.ContinueOnError)
	registerGlobalFlags(sessionFlags)
	connectionFlagSet := flag.NewFlagSet("connection", flag.ContinueOnError)
	registerConnectionFlags(connectionFlagSet)

	// dump, restore and ingest share registerConnectionFlags; naming each
	// keeps this test honest if one of them stops using it.
	tests := []struct {
		command string
		flags   *flag.FlagSet
	}{
		{"session", sessionFlags},
		{"dump", connectionFlagSet},
		{"restore", connectionFlagSet},
		{"ingest", connectionFlagSet},
	}
	for _, tc := range tests {
		t.Run(tc.command, func(t *testing.T) {
			addressFlag := tc.flags.Lookup("address")
			if addressFlag == nil {
				t.Fatalf("%s has no -address flag", tc.command)
			}
			usage := addressFlag.Usage
			if !strings.Contains(usage, "tcp.listen") && !strings.Contains(usage, "HOARDDB_TCP_LISTEN") {
				t.Errorf("%s -address help %q does not say the config's tcp.listen is the default", tc.command, usage)
			}
			// Read from the config, not a literal, so the help cannot drift
			// from the default again.
			if defaultListen := config.DefaultConfig().TCP.Listen; !strings.Contains(usage, defaultListen) {
				t.Errorf("%s -address help %q does not state the config's real default %q", tc.command, usage, defaultListen)
			}
		})
	}
}

// TestPasswordFlagWithoutAValueNeedsATerminal: a bare -password asks for the
// password on a terminal, as mysql and mongo do. Where stdin is not a terminal
// the statements are the input, so the CLI refuses and names the forms that work
// in a script, rather than taking a password from the statements.
func TestPasswordFlagWithoutAValueNeedsATerminal(t *testing.T) {
	code, _, stderr := runMainCapturing(t, "-password")
	if code != ExitUsage {
		t.Errorf("exit = %d, want %d", code, ExitUsage)
	}
	if strings.Contains(stderr, "flag needs an argument") {
		t.Errorf("a bare -password is refused as a missing value, not read as a prompt:\n%s", stderr)
	}
	for _, want := range []string{"needs a terminal", "HOARDDB_ROOT_PASSWORD", "-password=VALUE"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr does not name %s:\n%s", want, stderr)
		}
	}
}

// TestFlagErrorsExit1 is T18. The default flag set exits 2 on a bad
// flag, the number a refused dump returns, and the CLI ignored a positional
// argument and opened a session. Both are usage errors, exit 1, and neither
// dials: each case here has no server to dial, so a dial would show as a
// "Failed to connect" line.
func TestFlagErrorsExit1(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantStderr []string
	}{
		{"an unknown flag", []string{"-bogus"}, []string{"flag provided but not defined: -bogus"}},
		{"an unknown long flag", []string{"--bogus"}, []string{"flag provided but not defined: -bogus"}},
		{"a flag that is only a verb flag", []string{"--json"}, []string{"flag provided but not defined: -json"}},
		{"a flag with no value", []string{"-address"}, []string{"flag needs an argument: -address"}},
		{"a statement as a positional argument", []string{"show databases"}, []string{"takes no positional arguments", "to run a verb, put it first"}},
		{"a statement after flags", []string{"-insecure", "show databases"}, []string{"takes no positional arguments"}},
		{"a flag before a verb", []string{"--address", "127.0.0.1:1", "dump"}, []string{"a verb must come first"}},
		{"a flag before the restore verb", []string{"--insecure", "restore", "--dir", "x"}, []string{"a verb must come first"}},
		{"both password sources", []string{"-password-file", "x", "-password-stdin"}, []string{"mutually exclusive"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := runMainCapturing(t, tc.args...)
			if code != ExitUsage {
				t.Errorf("RunMain(%q) = %d, want %d (ExitUsage)\nstderr: %s", tc.args, code, ExitUsage, stderr)
			}
			for _, want := range tc.wantStderr {
				if !strings.Contains(stderr, want) {
					t.Errorf("stderr lacks %q:\n%s", want, stderr)
				}
			}
			if strings.Contains(stderr, "Failed to connect") || strings.Contains(stdout, "hoardDB CLI") {
				t.Errorf("RunMain opened a session for a usage error:\nstdout: %s\nstderr: %s", stdout, stderr)
			}
		})
	}
}

// TestRunMainDoesNotTouchTheDefaultFlagSet: the default flag.CommandLine exits
// the process with 2 on a bad flag. RunMain must use a private set, so neither
// parsing nor registration reaches it. (go test itself parses CommandLine, so
// "unparsed" is not assertable; what RunMain must not change is what the test
// binary left there.)
func TestRunMainDoesNotTouchTheDefaultFlagSet(t *testing.T) {
	parsedBefore, flagsSetBefore, argsBefore := flag.CommandLine.Parsed(), flag.CommandLine.NFlag(), flag.CommandLine.NArg()
	for _, args := range [][]string{{"-bogus"}, {"-v"}, {"-h"}, {"show databases"}, {"-address", "127.0.0.1:1", "-extjson", "-insecure"}} {
		runMainCapturing(t, args...)
	}
	if got := flag.CommandLine.Parsed(); got != parsedBefore {
		t.Errorf("flag.CommandLine.Parsed() = %v after RunMain, was %v", got, parsedBefore)
	}
	if got := flag.CommandLine.NFlag(); got != flagsSetBefore {
		t.Errorf("flag.CommandLine.NFlag() = %d, was %d: RunMain set a flag on the default set", got, flagsSetBefore)
	}
	if got := flag.CommandLine.NArg(); got != argsBefore {
		t.Errorf("flag.CommandLine.NArg() = %d, was %d: RunMain parsed arguments into the default set", got, argsBefore)
	}
	for _, name := range []string{"address", "extjson", "v", "insecure"} {
		if flag.CommandLine.Lookup(name) != nil {
			t.Errorf("RunMain registered -%s on flag.CommandLine", name)
		}
	}
}

// TestVersionAndHelpExitZero: -v prints one line on stdout and -h prints the
// usage, with runnable examples, on stderr; both return 0 without dialling.
func TestVersionAndHelpExitZero(t *testing.T) {
	for _, spelling := range []string{"-v", "-version", "--version"} {
		code, stdout, stderr := runMainCapturing(t, spelling)
		if code != ExitOK || !strings.HasPrefix(stdout, "hoardDB ") || !strings.Contains(stdout, "protocol=") || stderr != "" {
			t.Errorf("%s: code %d, stdout %q, stderr %q; want 0, a version line and no diagnostics", spelling, code, stdout, stderr)
		}
	}
	for _, spelling := range []string{"-h", "--help"} {
		code, stdout, stderr := runMainCapturing(t, spelling)
		if code != ExitOK || stdout != "" {
			t.Errorf("%s: code %d, stdout %q; want 0 and nothing on stdout", spelling, code, stdout)
		}
		for _, want := range []string{"Examples:", "hoarddb dump ", "echo 'show databases' | hoarddb", "-address string"} {
			if !strings.Contains(stderr, want) {
				t.Errorf("%s usage lacks %q:\n%s", spelling, want, stderr)
			}
		}
	}
}

// TestRunMainDispatchesVerbsFirst: dump, restore and ingest are reached only as
// the first argument, with the rest of the arguments. Each verb's own flag set
// answers an unknown flag, so its message proves the verb ran and not the
// session's.
func TestRunMainDispatchesVerbsFirst(t *testing.T) {
	for verb, usageLine := range map[string]string{
		"dump":    "flag provided but not defined: -bogus. Run 'hoarddb dump --help'",
		"restore": "flag provided but not defined: -bogus. Run 'hoarddb restore --help'",
		"ingest":  "hoarddb ingest",
	} {
		t.Run(verb, func(t *testing.T) {
			var code int
			var stdout, stderr string
			code, stdout, stderr = captureProcessStreams(t, func() int {
				return RunMain(Invocation{Args: []string{verb, "--bogus"}, Stdin: strings.NewReader(""), Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}, Getenv: func(string) string { return "" }})
			})
			if code != ExitUsage {
				t.Errorf("RunMain(%s --bogus) = %d, want %d", verb, code, ExitUsage)
			}
			if !strings.Contains(stdout+stderr, usageLine) {
				t.Errorf("RunMain(%s --bogus) printed no %q:\nstdout: %s\nstderr: %s", verb, usageLine, stdout, stderr)
			}
		})
	}
}

// runScript pipes script into a fresh session and returns Run's error with
// everything the session printed.
func runScript(t *testing.T, script string) (transcript string, runErr error) {
	t.Helper()
	session := newGoldenSession(t, WithInput(strings.NewReader(script)))
	session.out.Reset()
	runErr = session.cli.Run()
	return session.out.String(), runErr
}

// TestRunAcceptsAStatementSpanningLines: The statement is cut at
// the first newline today and refused with "unclosed brace"; with the fix the
// lines are joined until the braces balance, and the statement runs once.
func TestRunAcceptsAStatementSpanningLines(t *testing.T) {
	script := "create database app\nuse app\ncreate bucket ML {\n  Caption string,\n  Photos blobStore\n};\ndescribe bucket ML\n"
	transcript, runErr := runScript(t, script)
	if runErr != nil {
		t.Fatalf("Run returned %v, want nil\n%s", runErr, transcript)
	}
	if !strings.Contains(transcript, "Photos") {
		t.Errorf("describe bucket ML does not show the Photos store field:\n%s", transcript)
	}
}

// TestRunAcceptsTheCoreRuleSchemaAsWritten pipes the schema example from root
// AGENTS.md rule 11, indentation and trailing semicolon included.
func TestRunAcceptsTheCoreRuleSchemaAsWritten(t *testing.T) {
	script := `create database app
use app
create bucket User {
    Name        string,
    Age         int,
    CreatedDate datetime,
    Purchases   fifoStore,
    Photos      lifoStore,
    Friends     hashStore,
    Recipes     btreeStore,
    Books       heapStore,
    Data        blobStore
};
describe bucket User
`
	transcript, runErr := runScript(t, script)
	if runErr != nil {
		t.Fatalf("Run returned %v, want nil\n%s", runErr, transcript)
	}
	for _, store := range []string{"Purchases", "Photos", "Friends", "Recipes", "Books", "Data"} {
		if !strings.Contains(transcript, store) {
			t.Errorf("describe bucket User does not show the %s store field:\n%s", store, transcript)
		}
	}
}

// TestRunStatementOverEndOfInputNamesTheOpenBrace pins the EOF report: the
// unclosed character and the line the statement began on, not the parser's
// opaque body text.
func TestRunStatementOverEndOfInputNamesTheOpenBrace(t *testing.T) {
	script := "create database app\nuse app\ncreate bucket ML {\n  Caption string,\n"
	transcript, runErr := runScript(t, script)
	if runErr == nil {
		t.Fatalf("Run returned nil for a statement left open at end of input\n%s", transcript)
	}
	for _, want := range []string{`"{"`, "line 3"} {
		if !strings.Contains(runErr.Error(), want) {
			t.Errorf("Run error %q lacks %q", runErr, want)
		}
	}
	if strings.Contains(runErr.Error(), "the body") {
		t.Errorf("Run error %q is the parser's body text, not a report of the open statement", runErr)
	}
}

// TestRunBraceInsideStringDoesNotContinue: a brace inside a quoted string is
// text, so the statement is complete at its newline and the next line is its
// own statement.
func TestRunBraceInsideStringDoesNotContinue(t *testing.T) {
	script := "create database app\nuse app\ncreate bucket Notes { type: hash }\n" +
		`db.Notes.insert({"_id_": "n1", "text": "an open { brace and a [ bracket"})` + "\n" +
		"db.Notes.count()\n"
	transcript, runErr := runScript(t, script)
	if runErr != nil {
		t.Fatalf("Run returned %v, want nil\n%s", runErr, transcript)
	}
	if strings.Contains(transcript, "...>") {
		t.Errorf("a continuation prompt appeared for a brace inside a string:\n%s", transcript)
	}
}

// TestRunCommentBraceDoesNotContinue: a `--` comment with an unbalanced brace
// or bracket must not swallow the lines after it. The statement ends at the
// comment's line and the next statement runs.
func TestRunCommentBraceDoesNotContinue(t *testing.T) {
	for _, comment := range []string{"show buckets -- {", "show buckets -- ["} {
		script := "create database app\nuse app\ncreate bucket ML { type: hash }\n" +
			comment + "\n" +
			"create bucket After { type: hash }\n"
		transcript, runErr := runScript(t, script)
		if runErr != nil {
			t.Fatalf("Run returned %v for %q\n%s", runErr, comment, transcript)
		}
		if !strings.Contains(transcript, "created bucket app.After") {
			t.Errorf("the statement after %q did not run:\n%s", comment, transcript)
		}
		if strings.Contains(transcript, "...>") {
			t.Errorf("a continuation prompt appeared for %q:\n%s", comment, transcript)
		}
	}
}

// TestRunUnterminatedQuoteInsideABraceFailsAtThatLine: the statement ends at
// the line whose quote never closes, so the parser reports the unterminated
// string there instead of the CLI reading to end of input.
func TestRunUnterminatedQuoteInsideABraceFailsAtThatLine(t *testing.T) {
	script := "create database app\nuse app\ncreate bucket N { type: hash }\n" +
		`db.N.insert({"a": "x})` + "\n" +
		"create bucket Swallowed { type: hash }\n"
	transcript, runErr := runScript(t, script)
	if runErr == nil {
		t.Fatalf("Run returned nil for an unterminated string\n%s", transcript)
	}
	if !strings.Contains(runErr.Error(), "unterminated") {
		t.Errorf("Run error %q is not the parser's unterminated-string report", runErr)
	}
	if strings.Contains(runErr.Error(), "unfinished") || strings.Contains(runErr.Error(), "never closed") {
		t.Errorf("Run error %q reports an unfinished statement instead of the unterminated string", runErr)
	}
	if strings.Contains(transcript, "Swallowed") {
		t.Errorf("a line after the unterminated quote ran:\n%s", transcript)
	}
}

// TestEveryVerbRunsOnTheRing: no verb in verbRunners is held back as "not ported to the ring
// yet". Each verb is run with an unknown flag, which its own flag set refuses before any
// dial, and the answer must be that refusal and never a guard's.
func TestEveryVerbRunsOnTheRing(t *testing.T) {
	for verb := range verbRunners {
		t.Run(verb, func(t *testing.T) {
			t.Setenv("HOARDDB_CLI_RING", "1")
			code, stdout, stderr := captureProcessStreams(t, func() int {
				return RunMain(Invocation{Args: []string{verb, "--bogus"}, Stdin: strings.NewReader(""), Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}, Getenv: func(string) string { return "" }})
			})
			if strings.Contains(stdout+stderr, "not ported to the ring yet") {
				t.Errorf("hoarddb %s is still guarded as an unported verb:\nstdout: %s\nstderr: %s", verb, stdout, stderr)
			}
			if code != ExitUsage || !strings.Contains(stdout+stderr, "flag provided but not defined: -bogus") {
				t.Errorf("hoarddb %s --bogus: exit %d, want %d from the verb's own flag set:\nstdout: %s\nstderr: %s", verb, code, ExitUsage, stdout, stderr)
			}
		})
	}
}

// TestDefaultSessionIsRing: a plain session, with no flag and no environment, is the ring
// session. Without -insecure or -fingerprint it refuses to trust the server in the ring's own
// words, and it does that before any dial.
func TestDefaultSessionIsRing(t *testing.T) {
	code, stdout, stderr := runMainCapturing(t, "-address", "127.0.0.1:1", "-user", "root", "-password=unused")
	if code != ExitConnect || !strings.Contains(stderr, ringTrustRefusal) {
		t.Fatalf("a plain session: exit %d, want %d with the ring's trust refusal\nstdout: %s\nstderr: %s", code, ExitConnect, stdout, stderr)
	}
}

// TestHOARDDBCLIRINGHasNoEffect: the environment switch is retired with the flag. Set to 1
// or to 0 it changes nothing: the session is the ring either way and a verb is never refused
// because of it.
func TestHOARDDBCLIRINGHasNoEffect(t *testing.T) {
	for _, value := range []string{"1", "0", ""} {
		t.Run("value "+value, func(t *testing.T) {
			var outBuf, errBuf bytes.Buffer
			code := RunMain(Invocation{
				Args: []string{"-address", "127.0.0.1:1", "-user", "root", "-password=unused"}, Stdin: strings.NewReader(""),
				Stdout: &outBuf, Stderr: &errBuf,
				Getenv: func(name string) string {
					if name == "HOARDDB_CLI_RING" {
						return value
					}
					return ""
				}})
			if code != ExitConnect || !strings.Contains(errBuf.String(), ringTrustRefusal) {
				t.Fatalf("HOARDDB_CLI_RING=%q: exit %d, want %d with the ring's trust refusal\nstderr: %s", value, code, ExitConnect, errBuf.String())
			}
		})
	}
}

// TestFingerprintHelpDoesNotSayNotYetSupported: the ring session and the verbs pin a server's
// certificate with -fingerprint (a wrong pin exits 6), so no help text may say it is not
// supported.
func TestFingerprintHelpDoesNotSayNotYetSupported(t *testing.T) {
	_, stdout, stderr := runMainCapturing(t, "-h")
	if !strings.Contains(stdout+stderr, "-fingerprint") || strings.Contains(stdout+stderr, "not yet supported") {
		t.Errorf("the -h text must describe -fingerprint and not call it unsupported:\n%s%s", stdout, stderr)
	}
	for _, verb := range []string{"dump", "restore", "ingest"} {
		code, out, errOut := captureProcessStreams(t, func() int { return verbRunners[verb]([]string{"-h"}) })
		_ = code
		if strings.Contains(out+errOut, "not yet supported") {
			t.Errorf("hoarddb %s -h still says -fingerprint is not yet supported:\n%s%s", verb, out, errOut)
		}
	}
}
