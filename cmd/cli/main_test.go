package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestVersionFlagSpellingsAndNoSideEffects runs the real CLI binary. -v,
// -version and --version must each print one line naming the CLI and its
// version, and exit without dialling a server or leaving anything behind in the
// working directory — the CLI is the binary a user runs first, so it has to
// answer on a machine that has no server at all.
//
// cmd/server has the same test for the server binary; this one exists because
// the CLI's flag path was the only part of GAP-4 with no test of its own, and
// because the name in that line is easy to get wrong (the server prints
// "hoarddb-server", the CLI prints "hoardDB").
func TestVersionFlagSpellingsAndNoSideEffects(t *testing.T) {
	dir := t.TempDir()

	// Build from the module root (two levels up: cmd/cli -> cmd -> module).
	out, err := exec.Command("go", "build", "-C", "../..", "-o", dir+"/hcli", "./cmd/cli").CombinedOutput()
	if err != nil {
		t.Fatalf("build cli: %v\n%s", err, out)
	}

	for _, spelling := range []string{"-v", "-version", "--version"} {
		cmd := exec.Command(dir+"/hcli", spelling)
		cmd.Dir = dir
		cmd.Stdin = strings.NewReader("")
		cmdOut, runErr := cmd.CombinedOutput()
		if runErr != nil {
			t.Fatalf("%s: %v\n%s", spelling, runErr, cmdOut)
		}
		got := string(cmdOut)
		if !strings.HasPrefix(got, "hoardDB ") {
			t.Errorf("%s printed %q, want a line starting with the CLI's own name", spelling, got)
		}
		if strings.Contains(got, "hoarddb-server") {
			t.Errorf("%s printed %q — that is the server's name, not the CLI's", spelling, got)
		}
		for _, field := range []string{"version=", "commit=", "build_date="} {
			if !strings.Contains(got, field) {
				t.Errorf("%s printed %q, missing %s", spelling, got, field)
			}
		}
		if !strings.Contains(got, "protocol=") {
			t.Errorf("%s printed %q, missing the protocol version", spelling, got)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, entry := range entries {
		if entry.Name() != "hcli" {
			t.Errorf("-v left %q in the working directory; the flag must have no side effects", entry.Name())
		}
	}
}

// TestIngestVerbIsDispatchedBeforeTheREPL runs the real CLI binary: `ingest`
// must be answered by the verb, not fall through into the REPL, which reads
// standard input to EOF and would hang a pipeline instead of failing
// (dump-restore-build.md trap #9, ingest-build.md §3). With no file the verb
// exits 1 with its usage; the REPL would print its banner and exit 0.
func TestIngestVerbIsDispatchedBeforeTheREPL(t *testing.T) {
	dir := t.TempDir()
	out, err := exec.Command("go", "build", "-C", "../..", "-o", dir+"/hcli", "./cmd/cli").CombinedOutput()
	if err != nil {
		t.Fatalf("build cli: %v\n%s", err, out)
	}

	cmd := exec.Command(dir+"/hcli", "ingest")
	cmd.Dir = dir
	cmd.Stdin = strings.NewReader("")
	cmdOut, runErr := cmd.CombinedOutput()
	exitErr, isExit := runErr.(*exec.ExitError)
	if !isExit || exitErr.ExitCode() != 1 {
		t.Fatalf("`ingest` with no file: %v, want exit status 1\n%s", runErr, cmdOut)
	}
	if strings.Contains(string(cmdOut), "Type 'help' for help") || !strings.Contains(string(cmdOut), "exactly one file") {
		t.Errorf("`ingest` fell through to the REPL or printed no usage:\n%s", cmdOut)
	}

	help := exec.Command(dir+"/hcli", "ingest", "--help")
	help.Dir = dir
	helpOut, helpErr := help.CombinedOutput()
	if helpErr != nil || !strings.Contains(string(helpOut), "hoarddb ingest users.bson --into app.users") {
		t.Errorf("`ingest --help` = %v\n%s, want exit 0 and a runnable example", helpErr, helpOut)
	}
}
