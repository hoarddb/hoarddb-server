package wire

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// TestTheRestOfAReplaceIsRetriedWhileTheStateMoves: [GUARD BUG-0870 review B-6: the rest of a
// replace's join commits against the state the leader read, and the seed reporters commit
// concurrently, so a STATE_MOVED answer is retried a bounded number of times before the operator is
// told to run the statement again; any other error is returned at once]. Red while the step runs once.
func TestTheRestOfAReplaceIsRetriedWhileTheStateMoves(t *testing.T) {
	moved := fmt.Errorf("joining n2: %w", &cluster.Error{Code: cluster.CodeStateMoved, Message: "cluster state moved"})
	calls := 0
	if err := retryStateMoved(func() error {
		calls++
		if calls < 3 {
			return moved
		}
		return nil
	}); err != nil || calls != 3 {
		t.Fatalf("two STATE_MOVED answers then success: err %v after %d calls, want nil after 3", err, calls)
	}
	calls = 0
	if err := retryStateMoved(func() error { calls++; return moved }); !errors.Is(err, moved) || calls != stateMovedRetries {
		t.Fatalf("STATE_MOVED every time: err %v after %d calls, want it after %d", err, calls, stateMovedRetries)
	}
	other := errors.New("disk full")
	calls = 0
	if err := retryStateMoved(func() error { calls++; return other }); !errors.Is(err, other) || calls != 1 {
		t.Fatalf("another error: err %v after %d calls, want it after 1", err, calls)
	}
}

// TestAnUnfinishedAcceptedLossNamesTheFlagToRunAgain: [GUARD BUG-0870 narrow review B-3: a replace
// that accepted a loss and was cut after its commit leaves a joining row whose bits are cleared, and
// the gate counts its pairs again on the repeat; the advice must name --accept-loss, or the advised
// statement is refused with R4 (the held-reports harness test runs that repeat with the flag and it
// finishes)]. BREAK: drop the flag from the advice.
func TestAnUnfinishedAcceptedLossNamesTheFlagToRunAgain(t *testing.T) {
	cause := errors.New("the ask timed out")
	accepted := replaceUnfinished("n2", "n2:1", "the ask that admits it", true, cause).Error()
	if !strings.Contains(accepted, "'replace-node n2 --with n2@n2:1 --accept-loss'") {
		t.Fatalf("an unfinished accepted-loss replace advises %q; want the statement with --accept-loss", accepted)
	}
	plain := replaceUnfinished("n2", "n2:1", "the ask that admits it", false, cause).Error()
	if !strings.Contains(plain, "'replace-node n2 --with n2@n2:1'") || strings.Contains(plain, "--accept-loss") {
		t.Fatalf("an unfinished plain replace advises %q; want the statement without --accept-loss", plain)
	}
}
