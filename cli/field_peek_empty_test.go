package cli

import (
	"strings"
	"testing"
)

// TestFieldPeekOnAnEmptyFieldPrintsEmpty: pop on an empty queue field printed
// EMPTY while peek on the same field printed NOT FOUND, the word a keyed read
// of a hash field uses for a missing key.
func TestFieldPeekOnAnEmptyFieldPrintsEmpty(t *testing.T) {
	session := newFixtureSession(t)
	runQuiet(t, session, `db.Post["p1"].Comments.pop()`)

	cases := []struct {
		statement string
		want      string
	}{
		{`db.Post["p1"].Comments.pop()`, "EMPTY\n"},
		{`db.Post["p1"].Comments.peek()`, "EMPTY\n"},
		{`db.Post["p1"].Comments.peek({limit: 3})`, "EMPTY\n"},
		{`db.Post["p1"].Tags.get("missing")`, "NOT FOUND\n"},
	}
	for _, tc := range cases {
		t.Run(tc.statement, func(t *testing.T) {
			transcript := runStatements(t, session.cli, session.out, tc.statement)
			got := strings.TrimPrefix(transcript, "> "+tc.statement+"\n")
			if got != tc.want {
				t.Errorf("%s printed %q, want %q", tc.statement, got, tc.want)
			}
		})
	}
}
