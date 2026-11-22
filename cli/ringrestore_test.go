package cli

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// runRingRestoreForTest runs the verb the way main does, with its own streams.
func runRingRestoreForTest(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errOut bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	code = runRingRestore(ctx, args, ringRestoreEnv{stdout: &out, stderr: &errOut})
	return code, out.String(), errOut.String()
}

// restoreOnHost runs the verb against the test host at addr with the root login the host
// seeded.
func restoreOnHost(t *testing.T, addr string, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	return runRingRestoreForTest(t, append(append([]string{}, args...), ringIngestLogin(addr)...)...)
}

// countingListener accepts nothing and counts the connection attempts it sees: a restore
// that contacts the server shows up as a count above zero.
func countingListener(t *testing.T) (addr string, accepts *atomic.Int64) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepts = &atomic.Int64{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepts.Add(1)
			_ = conn.Close()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
	})
	return listener.Addr().String(), accepts
}

// TestRestoreOfACorruptFileNeverContactsTheServer: pass 1 stops a damaged file before a
// connection is opened (section 3.4), for a flipped byte and for a cut-off file.
func TestRestoreOfACorruptFileNeverContactsTheServer(t *testing.T) {
	addr, accepts := countingListener(t)
	good := writeVerifyTestDump(t, dumpVerifyTestRecords(t, "fifoStore"), dumpVerifyTestCounts)
	fileBytes := readVerifyTestFile(t, good)

	flipped := append([]byte{}, fileBytes...)
	flipped[len(flipped)/2] ^= 0x40
	cut := fileBytes[:len(fileBytes)-7]
	for name, content := range map[string][]byte{"flipped": flipped, "cut": cut} {
		path := filepath.Join(t.TempDir(), name+".hoarddump")
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatal(err)
		}
		code, stdout, stderr := runRingRestoreForTest(t, path, "-address", addr, "-user", "root", "-password=x", "-insecure")
		if code != ExitUsage {
			t.Errorf("%s: exit %d, want %d (usage); stderr %q", name, code, ExitUsage, stderr)
		}
		if stdout != "" {
			t.Errorf("%s: stdout %q, want nothing", name, stdout)
		}
		if !strings.Contains(stderr, "This dump is") {
			t.Errorf("%s: stderr %q does not carry the dump's own refusal", name, stderr)
		}
	}
	if got := accepts.Load(); got != 0 {
		t.Fatalf("the server saw %d connection attempts for a damaged dump, want 0", got)
	}
}

// TestRestorePrintsTheDumpsConsistencyStatementFirst: the first line a restore prints is the
// statement the header's word maps to, before any server contact.
func TestRestorePrintsTheDumpsConsistencyStatementFirst(t *testing.T) {
	addr, _ := newRingTestHost(t)
	path := writeVerifyTestDump(t, dumpVerifyTestRecords(t, "hashStore"), dumpVerifyTestCounts)
	_, _, stderr := restoreOnHost(t, addr, path)
	lines := strings.Split(stderr, "\n")
	if lines[0] != restoreConsistencyStatement {
		t.Fatalf("first line = %q, want %q\nfull stderr:\n%s", lines[0], restoreConsistencyStatement, stderr)
	}
}

// TestRestoreSaysQueueMessagesAreNotRestored: a dump that declares a queue field says,
// right after the statement, that claimed and dead-lettered messages are not in it; a dump
// without one does not.
func TestRestoreSaysQueueMessagesAreNotRestored(t *testing.T) {
	addr, _ := newRingTestHost(t)
	queue := writeVerifyTestDump(t, dumpVerifyTestRecords(t, "fifoStore"), dumpVerifyTestCounts)
	_, _, stderr := restoreOnHost(t, addr, queue)
	lines := strings.Split(stderr, "\n")
	if len(lines) < 2 || lines[1] != restoreQueueLeftOutLine {
		t.Fatalf("second line of %q is not the queue-messages line", stderr)
	}
	plain := writeVerifyTestDump(t, dumpVerifyTestRecords(t, "hashStore"), dumpVerifyTestCounts)
	_, _, stderr = restoreOnHost(t, addr, plain)
	if strings.Contains(stderr, restoreQueueLeftOutLine) {
		t.Fatalf("a dump without a queue field printed the queue line:\n%s", stderr)
	}
}

// TestRestoreRefusesAPipe: restore reads its file three times, so a pipe or a terminal
// cannot be restored; the refusal names the fix and nothing is dialled.
func TestRestoreRefusesAPipe(t *testing.T) {
	addr, accepts := countingListener(t)
	connection := []string{"-address", addr, "-user", "root", "-password=x", "-insecure"}
	fifo := filepath.Join(t.TempDir(), "pipe")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skipf("no named pipes here: %v", err)
	}
	for _, source := range []string{"-", fifo} {
		code, _, stderr := runRingRestoreForTest(t, append([]string{source}, connection...)...)
		if code != ExitUsage {
			t.Errorf("%q: exit %d, want %d", source, code, ExitUsage)
		}
		if !strings.Contains(stderr, "reads the file more than once") || !strings.Contains(stderr, "hoarddb restore <file>") {
			t.Errorf("%q: stderr %q does not say why and name the fix", source, stderr)
		}
	}
	if accepts.Load() != 0 {
		t.Fatalf("a refused pipe dialled the server %d times", accepts.Load())
	}
}
