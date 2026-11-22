package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestDumpAcceptsFlagsAfterTheDatabase: the verbs stopped parsing flags at the
// first positional, so `dump app --out ./d` left "--out" and "./d" in the
// positional list and was refused as "too many positional arguments", although
// the usage line is `dump [database [bucket]] [flags]`.
func TestDumpAcceptsFlagsAfterTheDatabase(t *testing.T) {
	session := newFixtureSession(t)

	t.Run("standalone verb", func(t *testing.T) {
		out := filepath.Join(session.outDir, "verb")
		args := append(verbConnectionFlags(session), "app", "Users", "--out", out, "--json")
		code, stdout, stderr := captureProcessStreams(t, func() int { return RunDumpCommand(args) })
		if code != ExitOK {
			t.Fatalf("dump app Users --out %s exited %d\nstdout: %s\nstderr: %s", out, code, stdout, stderr)
		}
		if !strings.Contains(stdout, `"databases":["app"]`) || !strings.Contains(stdout, `"buckets":1`) {
			t.Errorf("the dump did not take the database and bucket before the flags: %s", stdout)
		}
		if _, err := os.Stat(filepath.Join(out, "manifest.json")); err != nil {
			t.Errorf("--out after the database was ignored: %v", err)
		}
	})

	t.Run("REPL statement", func(t *testing.T) {
		out := filepath.Join(session.outDir, "repl")
		transcript := runStatements(t, session.cli, session.out, "dump app --out "+out)
		if strings.Contains(transcript, "Error:") {
			t.Errorf("the statement was refused:\n%s", transcript)
		}
		if _, err := os.Stat(filepath.Join(out, "manifest.json")); err != nil {
			t.Errorf("--out after the database was ignored: %v", err)
		}
	})

	t.Run("a flag after a positional still counts positionals", func(t *testing.T) {
		args := append(verbConnectionFlags(session), "app", "Users", "extra", "--out", filepath.Join(session.outDir, "three"))
		code, _, stderr := captureProcessStreams(t, func() int { return RunDumpCommand(args) })
		if code != ExitUsage || !strings.Contains(stderr, "too many positional arguments") {
			t.Errorf("three positionals exited %d with stderr %q, want a usage refusal", code, stderr)
		}
	})
}

// TestRestoreRefusesAPositionalEvenBeforeFlags: restore takes no positional, so
// a stray word is still refused after the flags change.
func TestRestoreRefusesAPositionalEvenBeforeFlags(t *testing.T) {
	session := newFixtureSession(t)
	args := append(verbConnectionFlags(session), "somewhere", "--dir", session.outDir)
	code, _, stderr := captureProcessStreams(t, func() int { return RunRestoreCommand(args) })
	if code != ExitUsage || !strings.Contains(stderr, "no positional arguments") {
		t.Errorf("restore with a positional exited %d with stderr %q, want a usage refusal", code, stderr)
	}
}
