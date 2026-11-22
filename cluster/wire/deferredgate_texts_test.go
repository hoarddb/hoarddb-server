package wire

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// What an operator reads while a pair waits on the deferred-pair gate (the review's F6 of the
// gate's build): the texts must name the wait as it is and not send the operator to a fix that
// changes nothing.

// TestASeedPullFromAReplayingSourceDoesNotSayItCannotReachIt: a source whose pair is deferred
// refuses SEED_REGISTER with the gate's error, which reaches the puller as text only, as the
// peer channel carries it. The source is up; the pull's status must say that it is replaying,
// not that it cannot be reached.
// [BREAK: report every failed seed call as "cannot reach": the status says so]
func TestASeedPullFromAReplayingSourceDoesNotSayItCannotReachIt(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{})
	owner.src.cfg.Deferred = func(uint16, uint64) bool { return true }
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	rig.wrap = func(_ string, msg transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
		if err != nil && msg.Env.Kind == transport.KindSeedRegister {
			return nil, errors.New(err.Error()) // the peer channel carries the text, not the code
		}
		return reply, err
	}
	rig.pullUntil("a status naming the source", func() bool {
		status, ok := rig.puller.Status(rig.slot, reconDB)
		return ok && strings.Contains(status.Detail, "x")
	})
	status, _ := rig.puller.Status(rig.slot, reconDB)
	if strings.Contains(status.Detail, "cannot reach") || !strings.Contains(status.Detail, "replaying its log") {
		t.Errorf("the pull's status while its source replays its log says %q; want it to say the source is up and replaying, not that it cannot be reached", status.Detail)
	}
}

// TestTheReplayReportDoesNotAdviseARestart: a pair whose own store lacks the declaration its
// log needs, while the routed read answers not found, waits for good (the local read proves
// nothing). The node's report of the wait must not tell the operator that restarting the node
// retries the replay: the replay retries by itself, and a restart reads the same store.
// [BREAK: keep "restarting this node retries the replay" in the report]
func TestTheReplayReportDoesNotAdviseARestart(t *testing.T) {
	bug0760Errors = &s0Errors{}
	f := newG7Fixture(t, true)
	f.unhold()
	f.staysDeferred(f.box, 3*time.Second, "its own store lacks Box's declaration and the routed read answers not found")
	deadline := time.Now().Add(20 * time.Second)
	for {
		for _, text := range bug0760Errors.all() {
			if !strings.Contains(text, "finishing the replay of the log tail") {
				continue
			}
			if strings.Contains(text, "restarting this node") || !strings.Contains(text, "retries by itself") {
				t.Fatalf("the report of a replay that waits on this node's own store says %q; want it to say the replay retries by itself, with no advice to restart the node", text)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no report of the waiting replay 20 s after the release; reports: %q", bug0760Errors.all())
		}
		time.Sleep(50 * time.Millisecond)
	}
}
