package server

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// TestAHolderOutsideTheSetWarnsOnceItsReleaseHasWaitedFiveMinutes is rev 7's
// A-10.8 (spec §20.8.7): copies outside their sets that a member does not
// cover yet are kept, and the node says so once they have waited more than
// five minutes, then at most once an hour, naming how many and the members
// waited on, with the two remedies. A pass that leaves none ends the wait, and
// a new wait starts its five minutes again. Guard: warn on the first pass.
func TestAHolderOutsideTheSetWarnsOnceItsReleaseHasWaitedFiveMinutes(t *testing.T) {
	var logged bytes.Buffer
	c := &Cluster{logger: slog.New(slog.NewTextHandler(&logged, nil))}
	warnings := func() int { return strings.Count(logged.String(), "outside their replica sets") }
	began := time.Date(2026, 10, 7, 9, 0, 0, 0, time.UTC)
	waitedOn := []string{"node-b:4433"}

	steps := []struct {
		at       time.Duration
		waiting  int
		warnings int
	}{
		{at: 0, waiting: 3, warnings: 0},
		{at: 4 * time.Minute, waiting: 3, warnings: 0},
		{at: 6 * time.Minute, waiting: 2, warnings: 1},
		{at: 30 * time.Minute, waiting: 2, warnings: 1},
		{at: 66 * time.Minute, waiting: 2, warnings: 2},
		{at: 70 * time.Minute, waiting: 0, warnings: 2},
		{at: 72 * time.Minute, waiting: 1, warnings: 2},
		{at: 76 * time.Minute, waiting: 1, warnings: 2},
		{at: 78 * time.Minute, waiting: 1, warnings: 3},
	}
	for _, step := range steps {
		c.noteReleaseWait(step.waiting, waitedOn, began.Add(step.at))
		if got := warnings(); got != step.warnings {
			t.Fatalf("after a pass at +%s leaving %d copies waiting, %d warnings were logged, want %d:\n%s",
				step.at, step.waiting, got, step.warnings, logged.String())
		}
	}
	first := strings.SplitN(logged.String(), "\n", 2)[0]
	for _, want := range []string{"2 bucket copies", "node-b:4433", "remove-node", "kept for 6m0s"} {
		if !strings.Contains(first, want) {
			t.Errorf("the first warning does not say %q: %s", want, first)
		}
	}
}
