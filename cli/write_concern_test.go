package cli

import (
	"bytes"
	"testing"
)

// TestParseWriteConcernAndSet covers #3: the CLI can set a per-request write
// concern that is carried on subsequent inserts.
func TestParseWriteConcernAndSet(t *testing.T) {
	cmd, err := ParseCommand("write-concern all")
	if err != nil {
		t.Fatalf("parse write-concern: %v", err)
	}
	if cmd.Method != "write-concern" || len(cmd.Args) != 1 || cmd.Args[0] != "all" {
		t.Fatalf("parsed command = %+v, want method=write-concern args=[all]", cmd)
	}

	var out bytes.Buffer
	h := NewCommandHandler(&out, false)
	if err := h.handleWriteConcern(cmd); err != nil {
		t.Fatalf("handleWriteConcern: %v", err)
	}
	if h.wc != "all" {
		t.Fatalf("session write concern = %q, want all", h.wc)
	}

	// An unknown level is refused, not silently ignored.
	if err := h.handleWriteConcern(&Cmd{Args: []string{"sometimes"}}); err == nil {
		t.Fatal("an invalid write concern was accepted")
	}
	if h.wc != "all" {
		t.Fatalf("a rejected level changed the session concern to %q", h.wc)
	}

	// Missing argument is a usage error.
	if err := h.handleWriteConcern(&Cmd{}); err == nil {
		t.Fatal("a missing write concern argument was accepted")
	}
}
