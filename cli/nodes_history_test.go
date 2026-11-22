package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
)

// TestNodesHistoryPrintsOneLinePerEpoch: 'nodes history' prints the
// membership history oldest first, one line per epoch with its sorted members
// and an 8-digit digest of them, so two members' histories compare line by
// line.
func TestNodesHistoryPrintsOneLinePerEpoch(t *testing.T) {
	var terminal bytes.Buffer
	h := NewCommandHandler(&terminal, false)
	err := h.printMembershipHistory("127.0.0.1:4433", server.NodeListResponse{HistoryReported: true,
		MembershipHistory: []server.MembershipRecord{
			{Epoch: 1, Members: []server.Peer{{ID: "n3"}, {ID: "n1"}, {ID: "n2"}}},
			{Epoch: 2, Members: []server.Peer{{ID: "n1"}, {ID: "n3"}}},
		}})
	if err != nil {
		t.Fatalf("printing a reported history: %v", err)
	}
	want := "MEMBERSHIP HISTORY of 127.0.0.1:4433\n---\n" +
		"epoch 1: n1 n2 n3  (digest 6401716e)\n" +
		"epoch 2: n1 n3  (digest 684f09d1)\n"
	if terminal.String() != want {
		t.Errorf("nodes history printed:\n%s\nwant:\n%s", terminal.String(), want)
	}
}

// TestNodesHistoryRefusesAMemberThatDoesNotReportIt: an answer without
// history_reported comes from a build without 'nodes history', and an empty
// history from it would agree with every member: it is refused, with the
// upgrade to make, and nothing is printed. Guard: the history_reported check.
func TestNodesHistoryRefusesAMemberThatDoesNotReportIt(t *testing.T) {
	var terminal bytes.Buffer
	h := NewCommandHandler(&terminal, false)
	err := h.printMembershipHistory("127.0.0.1:4435", server.NodeListResponse{})
	if err == nil || !strings.Contains(err.Error(), "member 127.0.0.1:4435 does not report its membership history") ||
		!strings.Contains(err.Error(), "Upgrade it, then run 'nodes history' again") {
		t.Errorf("an answer without history_reported gave %v; want the refusal naming the member and the upgrade", err)
	}
	if terminal.Len() != 0 {
		t.Errorf("the refusal printed %q; it must print nothing", terminal.String())
	}
}
