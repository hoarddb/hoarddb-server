package cli

import (
	"context"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/ringclient"
)

// TestReplaceNodeStatementSendsTheNodeItsAddressAndAcceptLoss: [GUARD BUG-0870 replace, the CLI:
// 'replace-node <node> --with <node>@<addr>' runs in ring mode, sends the node, its address and
// --accept-loss as the server's accept_loss, and prints the server's line; a --with that names
// another id is refused before anything is sent, with the statement that works]. Red while the form
// is unavailable in ring mode.
func TestReplaceNodeStatementSendsTheNodeItsAddressAndAcceptLoss(t *testing.T) {
	session, fake := u52Session()
	fake.membership = ringclient.MembershipResult{Epoch: 4, StateSeq: 30, Result: "Replacing node-b@127.0.0.1:4443: joining, then active when it holds the 5461 pairs it owns (epoch 4). Run 'status seeding' to follow it"}
	ctx := context.Background()
	for _, statement := range []string{"replace-node node-b --with node-b@127.0.0.1:4443", "replace-node node-b --with node-b@127.0.0.1:4443 --accept-loss"} {
		output, err := session.Execute(ctx, statement)
		if err != nil || strings.Join(output.Lines, "\n") != fake.membership.Result {
			t.Errorf("%s printed %q, %v; want the server's line", statement, output.Lines, err)
		}
	}
	if got, want := strings.Join(fake.calls, " | "), "replace-node node-b@127.0.0.1:4443 accept_loss=false | ReplaceNode | replace-node node-b@127.0.0.1:4443 accept_loss=true | ReplaceNode"; got != want {
		t.Errorf("the session sent %s, want %s", got, want)
	}
	fake.calls = nil
	_, err := session.Execute(ctx, "replace-node node-b --with node-c@127.0.0.1:4443")
	if err == nil || !strings.Contains(err.Error(), "'replace-node node-b --with node-b@127.0.0.1:4443'") || ringFailure(err).Exit != ExitRefused {
		t.Errorf("a --with naming another id came back as %v, want a refusal naming the statement that works", err)
	}
	if len(fake.calls) != 0 {
		t.Errorf("a refused statement was sent: %v", fake.calls)
	}
}
