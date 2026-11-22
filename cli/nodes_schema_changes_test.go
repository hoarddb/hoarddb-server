package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
)

// TestNodesShowsWhetherSchemaChangesArePaused: 'nodes' prints, below the
// member table, the pause and the refusal's paragraph while a join is
// unsettled, and that schema changes are allowed otherwise; a standalone
// server sends nothing and nothing is printed (join-freeze spec §7.2).
func TestNodesShowsWhetherSchemaChangesArePaused(t *testing.T) {
	paused := "Schema changes are paused while n4 joins the cluster: 3 of 10 buckets of this membership change have been copied."
	for _, tt := range []struct {
		name   string
		report *server.SchemaChangesReport
		want   []string
	}{
		{"paused", &server.SchemaChangesReport{Paused: true, Epoch: 5, Joiners: []string{"n4"}, Message: paused},
			[]string{"Schema changes: PAUSED\n", paused}},
		{"allowed after a join", &server.SchemaChangesReport{Epoch: 7, SettledAt: 7},
			[]string{"Schema changes: allowed (every member settled at epoch 7)\n"}},
		{"allowed, never joined", &server.SchemaChangesReport{Epoch: 1},
			[]string{"Schema changes: allowed (no member has joined since the cluster formed)\n"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var terminal bytes.Buffer
			h := NewCommandHandler(&terminal, false)
			h.printSchemaChanges(tt.report)
			for _, want := range tt.want {
				if !strings.Contains(terminal.String(), want) {
					t.Errorf("nodes printed:\n%s\nwhich lacks %q", terminal.String(), want)
				}
			}
		})
	}
	var terminal bytes.Buffer
	NewCommandHandler(&terminal, false).printSchemaChanges(nil)
	if terminal.Len() != 0 {
		t.Errorf("nodes printed %q for a standalone server, want nothing", terminal.String())
	}
}
