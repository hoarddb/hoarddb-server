package cli

import (
	"strings"
	"testing"
)

// TestCreateBucketPrintsUniqueIndexesReadably: a unique field's index entry is
// a map, and the confirmation printed it with %v as Go map syntax.
func TestCreateBucketPrintsUniqueIndexesReadably(t *testing.T) {
	session := newFixtureSession(t)
	cases := []struct {
		statement string
		want      string
	}{
		{"create bucket OneUnique { type: hash, placement: whole, Email string unique }", "Indexes: [Email (unique)]"},
		{"create bucket TwoUnique { type: hash, placement: whole, Email string unique, Name string unique }", "Indexes: [Email (unique), Name (unique)]"},
		{`create bucket PlainIndex { type: hash, indices: ["Email"] }`, "Indexes: [Email]"},
	}
	for _, tc := range cases {
		t.Run(tc.statement, func(t *testing.T) {
			transcript := runStatements(t, session.cli, session.out, tc.statement)
			if strings.Contains(transcript, "map[") {
				t.Errorf("the confirmation prints a Go map:\n%s", transcript)
			}
			if !strings.Contains(transcript, tc.want) {
				t.Errorf("the confirmation lacks %q:\n%s", tc.want, transcript)
			}
		})
	}
}
