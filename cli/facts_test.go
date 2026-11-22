package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
)

// valueColumn is where a line's value starts: the first non-space character
// after the label's colon. -1 when the line has no colon or no value.
func valueColumn(line string) int {
	colon := strings.Index(line, ":")
	if colon < 0 {
		return -1
	}
	for i := colon + 1; i < len(line); i++ {
		if line[i] != ' ' {
			return i
		}
	}
	return -1
}

// TestDescribeFactsAlignToTheWidestLabel pins the alignment: every value starts in
// the same column, and that column is set by the longest label rather than a fixed
// width, so a block of short labels stays tight and a long label still lines up.
func TestDescribeFactsAlignToTheWidestLabel(t *testing.T) {
	var out bytes.Buffer
	h := NewCommandHandler(&out, false)
	h.printFacts(bucketFacts(server.BucketStatsResponse{
		Type:         server.BucketTypeHash,
		Entries:      121,
		LogicalBytes: 4096,
		Encrypted:    "off",
		Indexes:      []string{"title_1"},
	}, "(none)"))

	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if len(lines) != 5 {
		t.Fatalf("printed %d rows, want 5 (type, entries, size, encryption, indexes):\n%s", len(lines), out.String())
	}
	want := len("Logical size") + 2 // the label, its colon, one space
	for _, line := range lines {
		if got := valueColumn(line); got != want {
			t.Errorf("value starts in column %d, want %d:\n%s\nall rows:\n%s", got, want, line, out.String())
		}
	}

	// A longer label moves the column for the whole block: the report re-aligns
	// rather than letting one value drift.
	out.Reset()
	h.printFacts([]fact{{"Type", "hash"}, {"Replication factor provenance", "inherited"}})
	block := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	want = len("Replication factor provenance") + 2
	for _, line := range block {
		if got := valueColumn(line); got != want {
			t.Errorf("after a longer label, value starts in column %d, want %d:\n%s", got, want, line)
		}
	}
}

// TestDescribeFactsOmitWhatTheServerDidNotSay: an older server sends no encryption
// mode, and the block then says nothing about encryption rather than claiming the
// store is plaintext.
func TestDescribeFactsOmitWhatTheServerDidNotSay(t *testing.T) {
	rows := bucketFacts(server.BucketStatsResponse{Type: server.BucketTypeHash, Entries: 1}, "(none)")
	for _, row := range rows {
		if row.label == "Encrypted" {
			t.Fatalf("an unreported encryption mode produced a row saying %q", row.value)
		}
	}
	if len(rows) != 4 {
		t.Fatalf("rows = %d, want 4 (type, entries, size, indexes) when encryption is unreported", len(rows))
	}
}

// TestDescribeBucketFactsShowTheReplicationFactorAndItsOrigin is step 4.2a on
// the CLI side: a declared factor reads "3 (declared)", the node's own reads
// "1 (inherited)", and a server that predates the field sends neither, so the
// block says nothing about copies rather than something wrong.
func TestDescribeBucketFactsShowTheReplicationFactorAndItsOrigin(t *testing.T) {
	valueOf := func(resp server.BucketStatsResponse) (string, bool) {
		for _, row := range bucketFacts(resp, "(none)") {
			if row.label == "Replication factor" {
				return row.value, true
			}
		}
		return "", false
	}
	cases := []struct {
		name      string
		resp      server.BucketStatsResponse
		wantValue string
		wantRow   bool
	}{
		{"declared", server.BucketStatsResponse{Type: server.BucketTypeHash, ReplicationFactor: 3}, "3 (declared)", true},
		{"inherited", server.BucketStatsResponse{Type: server.BucketTypeHash, ReplicationFactor: 1, Inherited: true}, "1 (inherited)", true},
		{"older server", server.BucketStatsResponse{Type: server.BucketTypeHash}, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, present := valueOf(tc.resp)
			if present != tc.wantRow || got != tc.wantValue {
				t.Fatalf("Replication factor row = %q (present %v), want %q (present %v)", got, present, tc.wantValue, tc.wantRow)
			}
		})
	}
}
