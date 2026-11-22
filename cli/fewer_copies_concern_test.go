package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestNoteFewerCopiesIsSilentWhenTheWriteConcernWasMet is BUG-0691. At the
// default concern (majority) the owner answers as soon as a majority holds the
// write, so a healthy three-node cluster answers "2 of 3" on nearly every
// insert while the third copy is a few milliseconds behind. That is the
// concern working, not a warning; the note is for a write that is below a
// majority of its factor, which is what a lost or refused copy looks like at
// every factor the concern still lets succeed.
func TestNoteFewerCopiesIsSilentWhenTheWriteConcernWasMet(t *testing.T) {
	cases := []struct {
		name      string
		confirmed int
		factor    int
		wantNote  bool
	}{
		{"three copies of three", 3, 3, false},
		{"a majority of three, the third follows", 2, 3, false},
		{"a majority of five", 3, 5, false},
		{"four of five", 4, 5, false},
		{"one of three, below a majority", 1, 3, true},
		{"two of five, below a majority", 2, 5, true},
		{"one of two, below a majority", 1, 2, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			answer := server.InsertResponse{ID: "k1", Success: true, CopiesConfirmed: tc.confirmed, ReplicationFactor: tc.factor}
			var results, chatter bytes.Buffer
			_, c := newStubServer(t, map[transport.Opcode]interface{}{transport.OpPut: answer}, &results)
			c.handler.SetChatter(&chatter)
			c.currentDB = "app"
			if err := c.handle(`db.Users.insert({"_id_": "k1"})`); err != nil {
				t.Fatalf("insert: %v", err)
			}
			note := chatter.String()
			if !tc.wantNote {
				if note != "" {
					t.Errorf("%d of %d copies printed %q, want silence: a majority holds the write", tc.confirmed, tc.factor, note)
				}
				return
			}
			if !strings.Contains(note, "stored on ") || !strings.Contains(note, "status") {
				t.Errorf("%d of %d copies printed %q, want the fewer-copies note with its cause text", tc.confirmed, tc.factor, note)
			}
		})
	}
}
