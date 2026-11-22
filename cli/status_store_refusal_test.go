package cli

import (
	"bytes"
	"testing"
)

// TestStatusSaysWhenTheStoreRefusesWrites: a node whose store cannot persist
// reports why in status, and a node that accepts writes prints no row.
func TestStatusSaysWhenTheStoreRefusesWrites(t *testing.T) {
	for _, tt := range []struct {
		name    string
		refusal string
		want    string
	}{
		{"refusing", "the node store at /data/store refuses writes: ...", "Store:        REFUSING WRITES - the node store at /data/store refuses writes: ...\n"},
		{"accepting", "", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var terminal bytes.Buffer
			NewCommandHandler(&terminal, false).printStoreRefusal(tt.refusal)
			if got := terminal.String(); got != tt.want {
				t.Errorf("status store row:\n got %q\nwant %q", got, tt.want)
			}
		})
	}
}
