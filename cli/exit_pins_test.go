package cli

import (
	"os"
	"strings"
	"testing"
)

// exitPin is one (verb, outcome) pair and the exit number the process returns
// for it. The table exists so GAP-24 Part 2 (one exit table in one file) can
// move every constant without moving a number by accident: it passes before
// the refactor and after, and the only row Part 2 is allowed to change is the
// one its build doc assigns (ingest completed with skips, 2 -> 3). Parts 3 to
// 8b then change further rows deliberately, one commit each, by editing this
// table.
type exitPin struct {
	verb    string
	outcome string
	want    int
}

// exitPins lists every pair the verbs and the REPL binary can produce today.
// A pair marked (unit) comes from the summary's exitCode mapping because a
// full run would need a signal or a lost reply and would be a flaky test.
var exitPins = []exitPin{
	{"dump", "success", 0},
	{"dump", "usage: unknown flag", 1},
	{"dump", "usage: too many positionals", 1},
	{"dump", "scope error: database missing (runDumpBuckets error)", 2},
	{"dump", "refused: no bucket succeeded", 2},
	{"dump", "partial: some buckets failed (unit)", 3},
	{"dump", "interrupted (unit)", 4},
	{"dump", "connect failure: nothing listening", 2},
	{"dump", "auth failure: wrong password", 2},

	{"restore", "success", 0},
	{"restore", "partial: blob data skipped (fixture has a blob bucket)", 3},
	{"restore", "usage: unknown flag", 1},
	{"restore", "usage: positional argument", 1},
	{"restore", "partial: one bucket refused (--drop over a blob bucket)", 3},
	{"restore", "source directory unreadable (runRestoreBuckets error)", 1},
	{"restore", "refused: no bucket succeeded (unit)", 2},
	{"restore", "interrupted (unit)", 4},
	{"restore", "connect failure: nothing listening", 2},
	{"restore", "auth failure: wrong password", 2},

	{"repl", "version flag", 0},
	// GAP-24 Part 7: a bad flag and a leftover word were 2 and 0.
	// 2 means "refused" everywhere else, and the word was silently ignored.
	{"repl", "unknown flag", 1},
	{"repl", "positional argument", 1},
	{"repl", "flag before a verb", 1},
	// GAP-24 Part 8a: the session path classifies a failed connect instead of
	// returning a literal 1.
	{"repl", "connect failure: nothing listening", 6},
	{"repl", "auth failure: wrong password", 7},
	{"repl", "piped script, every statement succeeds", 0},
	// The session is the ring: a refused statement is "the server answered and did not
	// apply it" (ExitRefused), and the ring session has no batch statement, so the three
	// batch-exit pins of the earlier engine's session went with it.
	{"repl", "piped script, a statement fails", 2},
}

// exitPinRunner collects the exit number each case produced. The test then
// checks that every row of exitPins was exercised exactly once, so a row
// cannot be added to the table and never run.
type exitPinRunner struct {
	t        *testing.T
	observed map[string]int
}

func (r *exitPinRunner) record(verb, outcome string, got int) {
	r.t.Helper()
	key := verb + "|" + outcome
	if _, dup := r.observed[key]; dup {
		r.t.Fatalf("%s / %s was run twice", verb, outcome)
	}
	r.observed[key] = got
}

// TestVerbExitCodesPinned runs each (verb, outcome) pair and compares the exit
// number with exitPins. It must pass on the tree as it stands (Part 1's rule
// for a characterisation) and after Part 2's refactor.
func TestVerbExitCodesPinned(t *testing.T) {
	runner := &exitPinRunner{t: t, observed: map[string]int{}}
	runDumpAndRestorePins(t, runner)
	runREPLBinaryPins(t, runner)

	for _, pin := range exitPins {
		got, ran := runner.observed[pin.verb+"|"+pin.outcome]
		if !ran {
			t.Errorf("%s / %s is in exitPins but no case ran it", pin.verb, pin.outcome)
			continue
		}
		if got != pin.want {
			t.Errorf("%s / %s exited %d, want %d", pin.verb, pin.outcome, got, pin.want)
		}
	}
	if len(runner.observed) != len(exitPins) {
		t.Errorf("%d cases ran but exitPins has %d rows: every case needs a row", len(runner.observed), len(exitPins))
	}
}

func runDumpAndRestorePins(t *testing.T, runner *exitPinRunner) {
	t.Helper()
	session := newFixtureSession(t)
	out := session.outDir
	connection := verbConnectionFlags(session)
	deadAddr := closedTCPAddr(t)
	dead := closedAddressFlags(deadAddr)
	wrongPassword := wrongPasswordFlags(session)

	dump := func(outcome string, connection, flags, positionals []string) {
		t.Helper()
		runner.record("dump", outcome, runProcessVerb(t, "dump", RunDumpCommand, connection, flags, positionals).code)
	}
	dump("success", connection, []string{"--out", out + "/ok"}, []string{"app"})
	dump("usage: unknown flag", connection, []string{"--bogus"}, nil)
	dump("usage: too many positionals", connection, []string{"--out", out + "/x"}, []string{"a", "b", "c"})
	dump("scope error: database missing (runDumpBuckets error)", connection, []string{"--out", out + "/nodb"}, []string{"nope"})
	dump("refused: no bucket succeeded", connection, []string{"--out", out + "/none"}, []string{"app", "Nope"})
	dump("connect failure: nothing listening", dead, []string{"--out", out + "/dead"}, []string{"app"})
	dump("auth failure: wrong password", wrongPassword, []string{"--out", out + "/bad"}, []string{"app"})
	runner.record("dump", "partial: some buckets failed (unit)", (&dumpSummary{buckets: 1, failures: []dumpFailure{{Bucket: "b"}}}).exitCode())
	runner.record("dump", "interrupted (unit)", (&dumpSummary{buckets: 1, interrupted: true}).exitCode())

	restore := func(outcome string, connection, flags, positionals []string) {
		t.Helper()
		runner.record("restore", outcome, runProcessVerb(t, "restore", RunRestoreCommand, connection, flags, positionals).code)
	}
	source := out + "/ok"
	restore("partial: blob data skipped (fixture has a blob bucket)", connection, []string{"--dir", source}, nil)
	// A dump with no blob bucket restores cleanly, exit 0. The shared fixture
	// holds a blob bucket, so dump only Users: a blobStore field's payload is
	// in no dump, and its presence would make any restore partial (exit 3).
	noBlob := out + "/no-blob"
	if code, _, stderr := captureProcessStreams(t, func() int {
		return RunDumpCommand(append(verbConnectionFlags(session), "--out", noBlob, "app", "Users"))
	}); code != 0 {
		t.Fatalf("preparing the no-blob dump: exit %d\n%s", code, stderr)
	}
	restore("success", connection, []string{"--dir", noBlob}, nil)
	restore("usage: unknown flag", connection, []string{"--bogus"}, nil)
	restore("usage: positional argument", connection, []string{"--dir", source}, []string{"extra"})
	restore("partial: one bucket refused (--drop over a blob bucket)", connection, []string{"--drop", "--dir", source}, nil)
	restore("source directory unreadable (runRestoreBuckets error)", connection, []string{"--dir", out + "/none"}, nil)
	restore("connect failure: nothing listening", dead, []string{"--dir", source}, nil)
	restore("auth failure: wrong password", wrongPassword, []string{"--dir", source}, nil)
	runner.record("restore", "refused: no bucket succeeded (unit)",
		(&restoreSummary{failures: []dumpFailure{{Bucket: "b"}}}).exitCode())
	runner.record("restore", "interrupted (unit)", (&restoreSummary{buckets: 1, interrupted: true}).exitCode())
}

func runREPLBinaryPins(t *testing.T, runner *exitPinRunner) {
	t.Helper()
	binary := builtCLI(t)
	session := newGoldenSession(t)
	connection := []string{"--address", session.addr, "--insecure", "--user", "admin", "--password", goldenPassword}
	deadConnection := []string{"--address", closedTCPAddr(t), "--insecure", "--user", "admin", "--password", goldenPassword}
	wrongConnection := []string{"--address", session.addr, "--insecure", "--user", "admin", "--password", "wrong-password-golden"}

	code, _, _ := runBinary(t, binary, "", "-v")
	runner.record("repl", "version flag", code)
	code, _, _ = runBinary(t, binary, "", "-bogus")
	runner.record("repl", "unknown flag", code)
	code, _, _ = runBinary(t, binary, "", "show databases")
	runner.record("repl", "positional argument", code)
	code, _, _ = runBinary(t, binary, "", "--address", "127.0.0.1:1", "dump")
	runner.record("repl", "flag before a verb", code)
	code, _, _ = runBinary(t, binary, "", deadConnection...)
	runner.record("repl", "connect failure: nothing listening", code)
	code, _, _ = runBinary(t, binary, "", wrongConnection...)
	runner.record("repl", "auth failure: wrong password", code)
	code, _, _ = runBinary(t, binary, "show databases\n", connection...)
	runner.record("repl", "piped script, every statement succeeds", code)
	code, _, stderr := runBinary(t, binary, "show databases\nuse nope\nshow databases\n", connection...)
	if !strings.Contains(stderr, "CLI error:") {
		t.Errorf("a failing piped statement printed no CLI error line:\n%s", stderr)
	}
	runner.record("repl", "piped script, a statement fails", code)
}

// TestRestoreOfAnUnreadableDirectoryIsNotPartial: a restore that
// failed before it applied anything exited 3, which a script reads as "some
// buckets were restored, check the errors". An unreadable source is client-side
// input: exit 1.
func TestRestoreOfAnUnreadableDirectoryIsNotPartial(t *testing.T) {
	session := newFixtureSession(t)
	missing := session.outDir + "/no-such-dump"
	result := runProcessVerb(t, "restore", RunRestoreCommand, verbConnectionFlags(session), []string{"--dir", missing}, nil)
	if result.code == ExitPartial {
		t.Fatalf("restore of a missing --dir exited %d (partial); nothing was applied. stderr:\n%s", result.code, result.stderr)
	}
	if result.code != ExitUsage {
		t.Errorf("restore of a missing --dir exited %d, want %d (unreadable input)", result.code, ExitUsage)
	}
}

// TestDumpOfAMissingDatabaseIsRefused: the server answering that a
// database does not exist is a refusal (2), not a usage error (1).
func TestDumpOfAMissingDatabaseIsRefused(t *testing.T) {
	session := newFixtureSession(t)
	flags := []string{"--out", session.outDir + "/missing-db"}
	result := runProcessVerb(t, "dump", RunDumpCommand, verbConnectionFlags(session), flags, []string{"nosuchdb"})
	if result.code != ExitRefused {
		t.Errorf("dump of a missing database exited %d, want %d (refused). stderr:\n%s", result.code, ExitRefused, result.stderr)
	}
}

// TestDumpToAnUnusableOutDirectoryIsUsage keeps the other half of runDumpBuckets
// where it was: an --out the CLI cannot use is client-side, exit 1.
func TestDumpToAnUnusableOutDirectoryIsUsage(t *testing.T) {
	session := newFixtureSession(t)
	occupied := session.outDir + "/occupied"
	if err := os.MkdirAll(occupied, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(occupied+"/stray.txt", []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	result := runProcessVerb(t, "dump", RunDumpCommand, verbConnectionFlags(session), []string{"--out", occupied}, []string{"app"})
	if result.code != ExitUsage {
		t.Errorf("dump into a directory that is not a dump exited %d, want %d. stderr:\n%s", result.code, ExitUsage, result.stderr)
	}
}
