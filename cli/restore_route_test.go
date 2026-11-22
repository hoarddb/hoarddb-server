package cli

import (
	"strings"
	"testing"
)

// TestRestoreVerbWithVerifyNeverDials runs the verb the way main does, through verbRunners,
// with no connection flags: the file check must work without a server.
func TestRestoreVerbWithVerifyNeverDials(t *testing.T) {
	path := writeVerifyTestDump(t, dumpVerifyTestRecords(t, "fifoStore"), dumpVerifyTestCounts)
	code, stdout, stderr := captureProcessStreams(t, func() int {
		return verbRunners["restore"]([]string{path, "--verify"})
	})
	if code != ExitOK {
		t.Fatalf("restore --verify exited %d, want %d; stderr %q", code, ExitOK, stderr)
	}
	if !strings.HasPrefix(stdout, restoreConsistencyStatement+"\n") || !strings.Contains(stdout, "Verified "+path+": databases 1, buckets 1, entities 2, elements 3") {
		t.Fatalf("restore --verify printed %q", stdout)
	}
	if stderr != "" {
		t.Fatalf("restore --verify wrote %q to stderr", stderr)
	}
}

// TestRestoreVerbWithoutVerifyIsTheRingRestore pins the fall-through: without --verify the
// verb is the ring restore, so a missing file is refused by its own text (exit 1, before any
// dial).
func TestRestoreVerbWithoutVerifyIsTheRingRestore(t *testing.T) {
	code, _, stderr := captureProcessStreams(t, func() int {
		return verbRunners["restore"]([]string{"/nonexistent/app.hoarddump"})
	})
	if code != ExitUsage {
		t.Fatalf("restore of a missing file exited %d, want %d", code, ExitUsage)
	}
	if !strings.Contains(stderr, "Cannot read /nonexistent/app.hoarddump") {
		t.Fatalf("restore printed %q, want the missing-file refusal", stderr)
	}
}
