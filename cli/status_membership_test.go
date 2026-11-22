package cli

import (
	"bytes"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
)

// TestStatusShowsWhetherTheNodeHasBeenAdded: status prints the membership row
// a cluster node sends, with the way out when it has not been added, and
// nothing for a standalone server (join-freeze spec §5.4).
func TestStatusShowsWhetherTheNodeHasBeenAdded(t *testing.T) {
	for _, tt := range []struct {
		name       string
		membership string
		detail     string
		want       string
	}{
		{"confirmed", server.MembershipConfirmed, "", "Membership:   confirmed\n"},
		{"not added yet", server.MembershipNotAddedYet, "", "Membership:   NOT ADDED YET - run 'join <address>' on a member\n"},
		{"refused", server.MembershipRefused, "refusing to exchange cluster membership with n2: ...",
			"Membership:   NOT CONFIRMED - refusing to exchange cluster membership with n2: ...\n"},
		{"waiting for founders", server.MembershipWaitingForFounders, "n3",
			"Membership:   WAITING FOR FOUNDERS - n3 not answered yet; start them, or run 'remove-node <node-id>' for one that is not coming back\n"},
		{"standalone", "", "", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var terminal bytes.Buffer
			h := NewCommandHandler(&terminal, false)
			h.printMembership(tt.membership, tt.detail)
			if got := terminal.String(); got != tt.want {
				t.Errorf("status membership row:\n got %q\nwant %q", got, tt.want)
			}
		})
	}
}
