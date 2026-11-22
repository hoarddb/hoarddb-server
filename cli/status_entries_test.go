package cli

import (
	"bytes"
	"strings"
	"testing"
)

// TestStatusPrintsNoEntriesRow proves status reports node totals only: a
// node-wide entry count cost a read of every bucket (catalog-gc spec Open
// Question 1, owner ruling 41). One bucket's count is 'describe bucket'.
func TestStatusPrintsNoEntriesRow(t *testing.T) {
	c := newDumpTestServer(t)
	var terminal bytes.Buffer
	c.handler.terminal = &terminal
	if err := c.handler.handleStatus(); err != nil {
		t.Fatalf("handleStatus: %v", err)
	}
	out := terminal.String()
	if !strings.Contains(out, "Buckets:") {
		t.Fatalf("status printed no Buckets row:\n%s", out)
	}
	if strings.Contains(out, "Entries:") {
		t.Errorf("status still prints an Entries row:\n%s", out)
	}
}
