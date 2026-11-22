package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// runRestoreVerifyForTest runs runRestoreVerify over buffers and returns its exit code
// with what it wrote to each stream.
func runRestoreVerifyForTest(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := runRestoreVerify(args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

// splitRestoreVerifyLines returns the lines of a whole stdout, without the final newline.
func splitRestoreVerifyLines(stdout string) []string {
	return strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
}

// TestRestoreVerifyPrintsTheSummary pins the summary line of section 3.4: the file, the
// four counts with separators, exit 0, and nothing on stderr.
func TestRestoreVerifyPrintsTheSummary(t *testing.T) {
	path := writeVerifyTestDump(t, dumpVerifyTestRecords(t, "hashStore"), dumpVerifyTestCounts)
	code, stdout, stderr := runRestoreVerifyForTest(t, "-verify", path)
	if code != ExitOK {
		t.Fatalf("exit %d, want %d\nstderr: %s", code, ExitOK, stderr)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want nothing", stderr)
	}
	lines := splitRestoreVerifyLines(stdout)
	want := "Verified " + path + ": databases 1, buckets 1, entities 2, elements 3"
	if len(lines) != 2 || lines[1] != want {
		t.Fatalf("stdout lines = %q, want the consistency statement and then %q", lines, want)
	}
}

// TestRestoreVerifyOnACutOffFileExits1AndNamesTheFix pins case 1 of section 3.1 through
// the command: a file cut before its footer exits 1, prints the incomplete text with its
// fix to stderr, and prints nothing to stdout.
func TestRestoreVerifyOnACutOffFileExits1AndNamesTheFix(t *testing.T) {
	path := writeVerifyTestDump(t, dumpVerifyTestRecords(t, "hashStore"), dumpVerifyTestCounts)
	fileBytes := readVerifyTestFile(t, path)
	cut := filepath.Join(t.TempDir(), "cut.hoarddump")
	if err := os.WriteFile(cut, fileBytes[:len(fileBytes)-5], 0o600); err != nil {
		t.Fatalf("writing the cut file: %v", err)
	}
	code, stdout, stderr := runRestoreVerifyForTest(t, "-verify", cut)
	if code != ExitUsage {
		t.Fatalf("exit %d, want %d (usage)", code, ExitUsage)
	}
	if stdout != "" {
		t.Fatalf("stdout = %q, want nothing for a damaged file", stdout)
	}
	if !strings.HasPrefix(stderr, "This dump is incomplete:") || !strings.Contains(stderr, "Take a new dump") {
		t.Fatalf("stderr = %q, want the incomplete text naming the fix", stderr)
	}
	if strings.Count(stderr, "\n") != 1 || !strings.HasSuffix(stderr, "\n") {
		t.Fatalf("stderr = %q, want one line and a newline", stderr)
	}
}

// TestRestoreVerifyPrintsTheConsistencyStatementFirst pins the first line of section 3.1
// "What the tool prints": restore --verify states what the dump promises before anything
// else, so an operator sees it even when the rest of the output is scrolled away.
func TestRestoreVerifyPrintsTheConsistencyStatementFirst(t *testing.T) {
	path := writeVerifyTestDump(t, dumpVerifyTestRecords(t, "hashStore"), dumpVerifyTestCounts)
	code, stdout, stderr := runRestoreVerifyForTest(t, "-verify", path)
	if code != ExitOK {
		t.Fatalf("exit %d, want %d\nstderr: %s", code, ExitOK, stderr)
	}
	lines := splitRestoreVerifyLines(stdout)
	if lines[0] != restoreConsistencyStatement {
		t.Fatalf("first line = %q, want %q", lines[0], restoreConsistencyStatement)
	}
}

// TestRestoreVerifySaysQueueMessagesAreNotInTheFile pins the queue-messages line of
// section 3.1 "What the tool prints": a file that declares a fifoStore or lifoStore field
// prints the line as the second line, before the summary; a file without one does not.
func TestRestoreVerifySaysQueueMessagesAreNotInTheFile(t *testing.T) {
	queue := writeVerifyTestDump(t, dumpVerifyTestRecords(t, "fifoStore"), dumpVerifyTestCounts)
	code, stdout, stderr := runRestoreVerifyForTest(t, "-verify", queue)
	if code != ExitOK {
		t.Fatalf("exit %d, want %d\nstderr: %s", code, ExitOK, stderr)
	}
	lines := splitRestoreVerifyLines(stdout)
	if len(lines) != 3 || lines[1] != restoreQueueLeftOutLine || !strings.HasPrefix(lines[2], "Verified ") {
		t.Fatalf("stdout lines = %q, want the statement, the queue line and the summary", lines)
	}

	plain := writeVerifyTestDump(t, dumpVerifyTestRecords(t, "hashStore"), dumpVerifyTestCounts)
	_, stdout, _ = runRestoreVerifyForTest(t, "-verify", plain)
	if strings.Contains(stdout, restoreQueueLeftOutLine) {
		t.Fatalf("a dump without a queue field printed the queue line:\n%s", stdout)
	}
}

// TestRestoreVerifyJSONIsOneObject pins the --json form: stdout holds exactly one JSON
// object (json.Unmarshal refuses trailing data), its members are the ones of section 3.4,
// and the queue text appears only when messages are left out.
func TestRestoreVerifyJSONIsOneObject(t *testing.T) {
	for _, test := range []struct {
		storeWord string
		queue     bool
	}{
		{"fifoStore", true},
		{"hashStore", false},
	} {
		path := writeVerifyTestDump(t, dumpVerifyTestRecords(t, test.storeWord), dumpVerifyTestCounts)
		code, stdout, stderr := runRestoreVerifyForTest(t, "-json", "-verify", path)
		if code != ExitOK {
			t.Fatalf("%s: exit %d, want %d\nstderr: %s", test.storeWord, code, ExitOK, stderr)
		}
		if stderr != "" {
			t.Fatalf("%s: stderr = %q, want nothing", test.storeWord, stderr)
		}
		var object map[string]any
		if err := json.Unmarshal([]byte(stdout), &object); err != nil {
			t.Fatalf("%s: stdout is not one JSON object: %v\n%s", test.storeWord, err, stdout)
		}
		if object["file"] != path || object["consistency"] != dumpConsistencyWord ||
			object["consistency_statement"] != restoreConsistencyStatement {
			t.Fatalf("%s: members file, consistency and consistency_statement = %v, %v, %v", test.storeWord,
				object["file"], object["consistency"], object["consistency_statement"])
		}
		if object["queue_messages_left_out"] != test.queue || object["entities"] != float64(2) ||
			object["elements"] != float64(3) || object["databases"] != float64(1) ||
			object["buckets"] != float64(1) || object["records"] != float64(6) {
			t.Fatalf("%s: counts and queue flag = %v", test.storeWord, object)
		}
		text, hasText := object["queue_messages_left_out_text"]
		if hasText != test.queue || (test.queue && text != restoreQueueLeftOutLine) {
			t.Fatalf("%s: queue_messages_left_out_text present = %t, text = %v", test.storeWord, hasText, text)
		}
	}
}

// TestRestoreVerifyNamesTheFixForABadPathOrArgument pins the two refusals that send no
// dump read: a path that cannot be opened and a command line without exactly one file.
// Both exit 1 and name the command that works.
func TestRestoreVerifyNamesTheFixForABadPathOrArgument(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "gone.hoarddump")
	code, stdout, stderr := runRestoreVerifyForTest(t, "-verify", missing)
	if code != ExitUsage || stdout != "" {
		t.Fatalf("a missing file: exit %d, stdout %q, want exit 1 and no stdout", code, stdout)
	}
	if !strings.HasPrefix(stderr, "Cannot read "+missing+": ") || !strings.Contains(stderr, "take a new dump") {
		t.Fatalf("a missing file: stderr = %q, want the cannot-read text naming the fix", stderr)
	}

	code, _, stderr = runRestoreVerifyForTest(t, "-verify")
	if code != ExitUsage || !strings.Contains(stderr, "usage: hoarddb restore <file> --verify") {
		t.Fatalf("no file: exit %d, stderr %q, want the usage line", code, stderr)
	}
}
