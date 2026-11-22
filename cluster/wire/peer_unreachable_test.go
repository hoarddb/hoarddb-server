package wire

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/peer"
)

// TestAnUnreachableOwnerKeepsItsTransportCauseInTheChain (BUG-0753): the coded text is for the
// client; code that matches the transport failure (a backoff, a closed connection) must still
// find it, and a refusal that is already coded or a caller whose own context ended must pass
// through without the "owner is unreachable" text. On the client wire the reply carries the
// coded text, not the dial error.
func TestAnUnreachableOwnerKeepsItsTransportCauseInTheChain(t *testing.T) {
	cause := fmt.Errorf("calling n1 on the data channel: %w for another 50ms after 1 failures", peer.ErrBackoff)
	err := unreachableOwner(context.Background(), cluster.CondForwardWriteUnreachable, "n1", cause)
	if !errors.Is(err, peer.ErrBackoff) {
		t.Errorf("errors.Is(err, peer.ErrBackoff) = false for %v", err)
	}
	var coded *cluster.Error
	if !errors.As(err, &coded) || coded.Code != cluster.CodeUnavailable {
		t.Fatalf("err %v is not a coded UNAVAILABLE", err)
	}
	refusal := ringRefusalOf(err)
	if refusal.Code != cluster.CodeUnavailable || refusal.Message != coded.Message || strings.Contains(refusal.Message, "data channel") {
		t.Errorf("the client wire replied %d %q, want the coded text %q", refusal.Code, refusal.Message, coded.Message)
	}

	refused := &cluster.Error{Code: cluster.CodeNotFound, Message: "document \"k\" is not in app.users"}
	if got := unreachableOwner(context.Background(), cluster.CondForwardReadUnreachable, "n1", refused); got != error(refused) {
		t.Errorf("a coded refusal was rewritten to %v", got)
	}

	gone, cancel := context.WithCancel(context.Background())
	cancel()
	if got := unreachableOwner(gone, cluster.CondForwardReadUnreachable, "n1", context.Canceled); !errors.Is(got, context.Canceled) || errors.As(got, &coded) && coded.Code == cluster.CodeUnavailable {
		t.Errorf("a canceled caller got %v, want its own cancellation", got)
	}
}
