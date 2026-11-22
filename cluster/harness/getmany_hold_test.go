package harness_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// heldWorld is a three-node cluster in which one node, held, was killed and restarted with a wide
// step_down_ms, so every pair it owns is in its restart hold for several seconds: a read of
// such a pair parks, whether it reaches the node locally or forwarded.
type heldWorld struct {
	c           *harness.Cluster
	held        string
	other       [2]string // other[0] is the leader, the node the tests send frames to
	heldIDs     []string
	otherIDs    [2][]string
	restartedAt time.Time
}

// heldStepDown is the step_down_ms newHeldWorld commits on every node before it kills the held one,
// and heldHeartbeat is the heartbeat_ms the harness runs (timings.go, P0). A restarted owner
// holds its pairs until step_down_ms + heartbeat_ms have passed since its boot, 12.1 s here
// (BUG-0813: before its fix the hold ignored a step_down_ms committed after the last epoch and
// was 2.2 s).
const (
	heldStepDown  = 12 * time.Second
	heldHeartbeat = 100 * time.Millisecond
)

// heldFor is how long after the restart a test may count on the hold lasting: step_down_ms,
// which is the hold's length less the heartbeat_ms and less the few tens of milliseconds
// between the instant restartedAt is taken and the process's own boot, the origin the hold is
// counted from. The margin (heartbeat_ms and the boot gap) keeps a test from sending a frame
// that could outlive the hold. A frame sent early in the window finds its keys parked.
const heldFor = heldStepDown

// An owner that has not decided its hold yet (its log not applied, or the handoff not yet fed
// the applied state) refuses a read at once, with a retryable UNAVAILABLE that does not park; a
// frame sent before then says nothing about the hold, so newHeldWorld waits it out.
const undecidedText = "this node restarted and does not know yet which pairs it owns: "

// newHeldWorld seeds 150 documents, then restarts a non-leader owner into its hold. The hold
// lasts about step_down_ms plus its tolerance from the restart, so a test has to be quick: it
// skips, as the existing hold tests do, when the restart itself used up the room.
func newHeldWorld(t *testing.T) *heldWorld {
	t.Helper()
	c, ids := getManyCluster(t, 150)
	leader := c.AwaitLeader(10 * time.Second)
	// The restart hold follows the committed step_down_ms, so widen it before the kill. A raise
	// restarts every node's lease under the new value: wait for them as the BUG-0813 test does.
	if err := c.Node(leader).SetSetting(proofCtx(t), "step_down_ms", heldStepDown.Milliseconds()); err != nil {
		t.Fatalf("setting step_down_ms: %v", err)
	}
	time.Sleep(time.Second)
	c.AwaitLeases(heldStepDown + 10*time.Second)
	w := &heldWorld{c: c}
	for _, id := range c.IDs() {
		switch {
		case id == leader:
			w.other[0] = id
		case w.held == "":
			w.held = id
		default:
			w.other[1] = id
		}
	}
	owners, _ := ownersOf(t, c, proofDB, ids)
	grouped := idsByOwner(ids, owners)
	w.heldIDs, w.otherIDs[0], w.otherIDs[1] = grouped[w.held], grouped[w.other[0]], grouped[w.other[1]]
	if len(w.heldIDs) < 40 || len(w.otherIDs[0]) < 6 || len(w.otherIDs[1]) < 6 {
		t.Fatalf("owners hold %d, %d and %d of the documents; the test needs 40, 6 and 6", len(w.heldIDs), len(w.otherIDs[0]), len(w.otherIDs[1]))
	}
	c.Kill(w.held)
	booted := time.Now()
	c.Restart(w.held)
	w.restartedAt = booted
	c.AwaitLeases(10 * time.Second)
	if took := time.Since(booted); took > heldFor/4 {
		t.Skipf("the restart took %s, too close to the %s hold to see it", took, heldFor)
	}
	// A read can reach the owner before it decided its hold: it is refused at once, so retry it.
	// The nodes that talked to the held node when it was killed keep it in backoff for a while:
	// a read forwarded now is refused "could not reach", which says nothing about the hold. Probe
	// with a short lone read until the refusal is the caller's own timeout, the sign that the read
	// reached the owner and parked, then let the probe's own timeout leave its backoff.
	for {
		probe, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_, err := c.Node(w.other[0]).Get(probe, proofDB, proofBucket, w.heldIDs[0])
		cancel()
		if err == nil {
			t.Fatalf("a read forwarded to the restarted owner %s was answered %s after boot, inside its hold", w.held, time.Since(booted))
		}
		if !strings.Contains(err.Error(), "could not reach") && !strings.Contains(err.Error(), undecidedText) {
			break
		}
		if time.Since(booted) > heldFor/2 {
			t.Skipf("%s was still in backoff or undecided %s after the restart, too close to the hold to see it", w.held, time.Since(booted))
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(400 * time.Millisecond)
	return w
}

// requireHeld skips the test when the frame, sent at began with a deadline of deadline, could not
// have ended inside the hold: the assertions below describe keys that were parked, and a hold
// that is over parks nothing. It judges the frame's start, not its end, so a handler that ignored
// the deadline and waited out the hold is not excused by taking too long.
func (w *heldWorld) requireHeld(t *testing.T, began time.Time, deadline time.Duration) {
	t.Helper()
	if until := began.Add(deadline).Sub(w.restartedAt); until > heldFor {
		t.Skipf("the frame was sent %s after the restart with a deadline of %s, past the %s hold; the VM was too slow to see it", began.Sub(w.restartedAt), deadline, heldFor)
	}
}

// sawBackoff reports whether a key was refused "could not reach" the held owner: the nodes that
// talked to it when it was killed keep it in backoff for up to two seconds (the probe in
// newHeldWorld waits most of that out, not always), and a read refused there never reached the
// hold. Such a frame still has to answer every key and keep the others' keys, but the texts of
// the deadline are only asserted when no key took that way.
func sawBackoff(t *testing.T, reply wire.RingReply) bool {
	t.Helper()
	for _, answer := range reply.Answers {
		if answer.Error != nil && strings.Contains(answer.Error.Message, "could not reach") {
			t.Logf("a key was refused %q: the peer was in backoff after the kill, so the strict texts are not asserted", answer.Error.Message)
			return true
		}
	}
	return false
}

// deadlineText is the check every deadline answer shares: UNAVAILABLE, the frame's own text, and
// never Go's "context deadline exceeded".
func deadlineText(t *testing.T, label string, answer wire.RingAnswer, ms uint32, strict bool) (cutOff bool) {
	t.Helper()
	if answer.Error == nil {
		t.Errorf("%s answered a reply, want the deadline's refusal", label)
		return false
	}
	message := answer.Error.Message
	if answer.Error.Code != cluster.CodeUnavailable {
		t.Errorf("%s answered %s (%s), want UNAVAILABLE", label, answer.Error.Code, message)
	}
	if strings.Contains(message, "context deadline exceeded") {
		t.Errorf("%s says Go's own words: %q", label, message)
	}
	cut := strings.Contains(message, "was cut off while it was being read")
	if !strict {
		return cut
	}
	if !cut && !strings.Contains(message, "was not read") {
		t.Errorf("%s: %q says neither 'was not read' nor 'was cut off while it was being read'", label, message)
	}
	if !strings.Contains(message, "the frame's deadline of ") || !strings.Contains(message, "Reads change nothing, so resend it") {
		t.Errorf("%s: %q does not name the deadline and the fix", label, message)
	}
	return cut
}

// TestGetManyWithMoreThanSixteenHeldKeysOnOneOwner: the frame goes to the restarted owner itself
// (so its keys park in the node's own hold, with no peer in between): 40 keys on it, then six keys
// on each of the other two owners, in that request order. The held owner's keys take its slots and
// wait; the other owners' keys are answered at once and the frame ends at its deadline. A second frame with no deadline waits the hold out and answers
// every key, the held ones last.
func TestGetManyWithMoreThanSixteenHeldKeysOnOneOwner(t *testing.T) {
	w := newHeldWorld(t)
	client := newGetManyClient(t, w.c, w.held)
	var asked []string
	asked = append(asked, w.heldIDs[:40]...)
	asked = append(asked, w.otherIDs[0][:6]...)
	asked = append(asked, w.otherIDs[1][:6]...)
	const deadline = 400
	began := time.Now()
	reply := client.send(wire.RingPrologue{DeadlineMs: deadline}, client.reads(asked...))
	took := time.Since(began)
	t.Logf("the first frame was sent %s after the restart and took %s", began.Sub(w.restartedAt), took)
	w.requireHeld(t, began, deadline*time.Millisecond)
	strict := !sawBackoff(t, reply)
	if took > deadline*time.Millisecond+2500*time.Millisecond {
		t.Errorf("the frame took %s with a deadline of %d ms", took, deadline)
	}
	if reply.Outcome != "partial" {
		t.Errorf("outcome %q, want partial", reply.Outcome)
	}
	cutOff, notRead := 0, 0
	for i, id := range asked {
		if i >= 40 {
			if docID(reply.Answers[i]) != id {
				t.Errorf("key %d (%s) on a live owner answered %+v while another owner's pairs were held", i, id, reply.Answers[i])
			}
			continue
		}
		if deadlineText(t, "held key "+id, reply.Answers[i], deadline, strict) {
			cutOff++
		} else {
			notRead++
		}
	}
	// Three owners have keys, so the held owner has about a third of the 16 slots until the
	// others finish and then all of them: more than 16 keys cannot all be in flight.
	if strict && (cutOff == 0 || cutOff > 16 || notRead < 40-16) {
		t.Errorf("%d held keys were cut off in flight and %d not read; want 1 to 16 in flight and at least 24 not launched", cutOff, notRead)
	}

	// No deadline: the frame parks until the hold ends and then answers everything. The failed
	// reads of the first frame can leave the owner in backoff for a moment, which refuses a key
	// "could not reach" without reading it, so the frame is sent again until nothing is refused so.
	began = time.Now()
	for {
		reply = client.sendSettled(wire.RingPrologue{}, client.reads(asked...))
		backoff := false
		for _, answer := range reply.Answers {
			backoff = backoff || (answer.Error != nil && strings.Contains(answer.Error.Message, "could not reach"))
		}
		if !backoff || time.Since(began) > 20*time.Second {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if reply.Outcome != "ok" {
		t.Fatalf("the frame with no deadline answered %q; every key should be answered once the hold ends", reply.Outcome)
	}
	for i, id := range asked {
		if docID(reply.Answers[i]) != id {
			t.Errorf("key %d wants %s and was answered %+v", i, id, reply.Answers[i])
		}
	}
	t.Logf("the frame with no deadline was answered %s after the first frame began, %s after the restart", time.Since(began), time.Since(w.restartedAt))
}

// TestGetManyDeadlineSpentAnswersTheTailNotRead: the frame goes to the held owner itself, every key
// is on it and the deadline is short. The reads in flight are cut off, the keys that were never launched say they were not
// read, and no key is left unanswered.
func TestGetManyDeadlineSpentAnswersTheTailNotRead(t *testing.T) {
	w := newHeldWorld(t)
	client := newGetManyClient(t, w.c, w.held)
	asked := w.heldIDs[:40]
	began := time.Now()
	reply := client.send(wire.RingPrologue{DeadlineMs: 400}, client.reads(asked...))
	w.requireHeld(t, began, 400*time.Millisecond)
	strict := !sawBackoff(t, reply)
	cutOff, notRead := 0, 0
	for i, id := range asked {
		if deadlineText(t, "key "+id, reply.Answers[i], 400, strict) {
			cutOff++
		} else {
			notRead++
		}
	}
	if strict && (cutOff == 0 || cutOff > 16 || notRead < 40-16) {
		t.Errorf("%d keys cut off in flight and %d not read; want 1 to 16 in flight and at least 24 not launched", cutOff, notRead)
	}
}

// TestGetManyDeadlineCutsAKeyParkedInAHold: the frame goes to the restarted owner itself, so its
// keys park in the node's own restart hold. They answer the in-flight text when the deadline
// passes, while a key of another owner is answered.
func TestGetManyDeadlineCutsAKeyParkedInAHold(t *testing.T) {
	w := newHeldWorld(t)
	client := newGetManyClient(t, w.c, w.held)
	asked := append(append([]string{}, w.heldIDs[:3]...), w.otherIDs[0][0])
	began := time.Now()
	reply := client.send(wire.RingPrologue{DeadlineMs: 400}, client.reads(asked...))
	w.requireHeld(t, began, 400*time.Millisecond)
	strict := !sawBackoff(t, reply)
	for i, id := range asked[:3] {
		if !deadlineText(t, "parked key "+id, reply.Answers[i], 400, strict) && strict {
			t.Errorf("key %s was parked in the hold when the deadline passed, and says it was not read instead of cut off", id)
		}
	}
	if got := docID(reply.Answers[3]); got != asked[3] {
		t.Errorf("the key of another owner answered %q, want %s", got, asked[3])
	}
}

// TestGetManyDeadlineCutsAForwardedKey: the frame goes to another node, which forwards the keys
// to the held owner; the owner parks them and the receiving node cuts them off at the deadline.
func TestGetManyDeadlineCutsAForwardedKey(t *testing.T) {
	w := newHeldWorld(t)
	client := newGetManyClient(t, w.c, w.other[0])
	asked := append(append([]string{}, w.heldIDs[:3]...), w.otherIDs[1][0])
	began := time.Now()
	reply := client.send(wire.RingPrologue{DeadlineMs: 400}, client.reads(asked...))
	w.requireHeld(t, began, 400*time.Millisecond)
	strict := !sawBackoff(t, reply)
	for i, id := range asked[:3] {
		if !deadlineText(t, "forwarded key "+id, reply.Answers[i], 400, strict) && strict {
			t.Errorf("key %s was forwarded and in flight when the deadline passed, and says it was not read instead of cut off", id)
		}
	}
	if got := docID(reply.Answers[3]); got != asked[3] {
		t.Errorf("the key of another owner answered %q, want %s", got, asked[3])
	}
}
