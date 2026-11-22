package cli

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/server"
)

// TestStatusAndNodesShowTwoProcessesUnderOneNodeID: status prints one row per
// duplicated node id with the server's sentence, and nodes marks the row
// (BUG-0600). Nothing is printed when the server sends none.
func TestStatusAndNodesShowTwoProcessesUnderOneNodeID(t *testing.T) {
	sentence := "two processes are running as node n2: ... give it its own HOARDDB_NODE_ID ..."
	var terminal bytes.Buffer
	h := NewCommandHandler(&terminal, false)
	h.printDuplicates(nil)
	if terminal.Len() != 0 {
		t.Fatalf("status printed %q with no duplicates", terminal.String())
	}
	h.printDuplicates([]string{sentence})
	if got, want := terminal.String(), "Duplicate:    "+sentence+"\n"; got != want {
		t.Fatalf("status duplicate row:\n got %q\nwant %q", got, want)
	}

	terminal.Reset()
	joined := time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
	h.printNodeRow(&server.NodeInfo{NodeID: "n1", Addr: "192.0.2.1:4433", Status: "active", JoinedAt: joined})
	h.printNodeRow(&server.NodeInfo{NodeID: "n2", Addr: "192.0.2.2:4433", Status: "active", JoinedAt: joined, Duplicate: "DUPLICATE IDENTITY - see status"})
	lines := strings.Split(strings.TrimSuffix(terminal.String(), "\n"), "\n")
	if len(lines) != 3 || !strings.Contains(lines[1], "n2") || !strings.Contains(lines[2], "DUPLICATE IDENTITY") {
		t.Fatalf("nodes rows:\n%s\nwant n1, n2, and the duplicate mark under n2", terminal.String())
	}
}
