package cli

import (
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestASessionRecallsItsOwnLinesAndKeepsNoHistoryOnDisk is the guard for the
// owner's decision of 2026-10-11: the REPL recalls the lines typed in the
// running session — Up and Down walk them — and nothing is read from or written
// to disk.
//
// Both halves are asserted on one real session: a stub server, a terminal at
// both ends, and a statement run twice, the second time by pressing Up. The
// home directory is a fresh one, so the history file the feature used to keep
// under ~/.hoarddb is what this test fails on if it comes back. A session that
// only exits would not be enough: the old code never wrote an empty history, so
// the completed statement is what would have created the file.
func TestASessionRecallsItsOwnLinesAndKeepsNoHistoryOnDisk(t *testing.T) {
	t.Setenv("TERM", "xterm-256color")
	home := t.TempDir()
	t.Setenv("HOME", home)

	inMaster, inSlave := openTestPty(t)
	setPtySize(t, inSlave, 24, 80)

	script := bannerScript()
	script[transport.OpListDBs] = server.ListDatabasesResponse{}

	// A terminal at both ends: with a redirected output the editor reads plain
	// lines, which have no Up key, and the recall under test would not exist.
	_, outSlave := openTestPty(t)
	setPtySize(t, outSlave, 24, 80)
	stub, c := newStubServer(t, script, outSlave)
	c.reader = inSlave
	swapStdin(t, inSlave)
	// One statement, then Up + Enter to run it again from the session's own
	// history, then leave.
	feedPty(t, inMaster, "show databases\n\x1b[A\nexit\n")

	if err := c.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := countRequests(stub, transport.OpListDBs); got != 2 {
		t.Fatalf("the server was asked for the databases %d times, want 2: Up did not recall the statement", got)
	}

	var written []string
	if err := filepath.WalkDir(home, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path != home {
			written = append(written, path)
		}
		return nil
	}); err != nil {
		t.Fatalf("walking the home directory: %v", err)
	}
	if len(written) > 0 {
		t.Fatalf("the session wrote %v under the home directory; its history lives in memory only", written)
	}
}

// countRequests is how many times a session sent one opcode.
func countRequests(stub *stubServer, op transport.Opcode) int {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	count := 0
	for _, sent := range stub.requests {
		if sent == op {
			count++
		}
	}
	return count
}
