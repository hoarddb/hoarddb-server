package server

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/ring"
)

// BUG-0731: a first handshake nobody answered at cluster formation was logged
// and counted as the stream leaving the in-sync set, for a peer that had never
// been in it. The peer stays out of the set (write concern and the replication
// view must not wait for a copy that cannot answer); only the departure and
// the return events are false.

var errFormationRefused = fmt.Errorf("opening the stream to node-b before replaying the log to it: %w",
	&peerSilentError{err: errors.New("dial: connection refused")})

// ghostPeer is a peer record of a started one-node cluster whose node has no
// sender, so the test is the only caller of markFailed. Its cluster logs warnings
// and above into the returned buffer.
func ghostPeer(t *testing.T) (*peer, *syncBuffer) {
	t.Helper()
	logged := &syncBuffer{}
	tc := newTestClusterTuned(t, 1, 1, nil, func(_ int, c *Cluster) {
		c.logger = slog.New(slog.NewTextHandler(logged, &slog.HandlerOptions{Level: slog.LevelInfo}))
	})
	return clusterOf(tc, 0).peerFor(ring.Node{ID: "ghost-" + t.Name(), Addr: "127.0.0.1:1"}), logged
}

// TestFirstFailedDialIsNotAnInSyncDeparture: the first unanswered handshake of
// a never-streamed stream logs no departure and fires no hook, and the peer is
// still out of the in-sync set for write concern.
func TestFirstFailedDialIsNotAnInSyncDeparture(t *testing.T) {
	departures := countDepartures(t)
	p, logged := ghostPeer(t)

	p.markFailed(errFormationRefused)
	p.markFailed(errFormationRefused)

	departures.requireNone(t, "a never-opened stream's refused dial")
	if strings.Contains(logged.String(), "left the in-sync set") {
		t.Fatalf("a refused first dial logged a departure: %s", logged.String())
	}
	if p.inSync() {
		t.Fatal("a peer that never answered reads as in sync: write concern would wait for a copy that cannot answer")
	}
	c := p.c
	if n := c.inSyncCount([]*peer{p}); n != 1 {
		t.Fatalf("inSyncCount with a never-answering peer = %d, want 1 (this node alone)", n)
	}
	if need := neededAcks(WcAll, c.inSyncCount([]*peer{p})); need != 1 {
		t.Fatalf("WcAll needs %d acks with a never-answering peer, want 1", need)
	}
	if lag := p.lagAt(time.Now(), 0); !lag.notInSync {
		t.Fatal("the not-in-sync reading hides a peer that is genuinely unreachable")
	}
}

// TestFirstAnswerAfterSilentDialIsNotAReturn: when the peer finally answers,
// the stream returns to sync without a "back in sync" line for a departure
// that was never logged.
func TestFirstAnswerAfterSilentDialIsNotAReturn(t *testing.T) {
	departures := countDepartures(t)
	p, logged := ghostPeer(t)

	p.markFailed(errFormationRefused)
	p.streamMu.Lock()
	p.returnToSyncLocked()
	p.streamMu.Unlock()

	departures.requireNone(t, "a silent first dial followed by an answer")
	if strings.Contains(logged.String(), "in sync") {
		t.Fatalf("a never-logged departure was followed by a return line: %s", logged.String())
	}
	if !p.inSync() {
		t.Fatal("the stream did not return to sync once its replay reached the boundary")
	}
}

// TestFailureAfterStreamingStillRecordsOneDeparture guards the real departure:
// a stream that has streamed and then fails is recorded once, and its return
// is logged.
func TestFailureAfterStreamingStillRecordsOneDeparture(t *testing.T) {
	departures := countDepartures(t)
	p, logged := ghostPeer(t)

	if _, _, entered := p.enterStreaming(p.currentGeneration()); !entered {
		t.Fatal("the stream did not enter streaming")
	}
	p.markFailed(errFormationRefused)
	p.markFailed(errFormationRefused)

	if n := departures.count.Load(); n != 1 {
		t.Fatalf("a stream that had streamed recorded %d departures after failing, want 1", n)
	}
	if p.inSync() {
		t.Fatal("a stream that failed after streaming still reads as in sync")
	}
	p.streamMu.Lock()
	p.returnToSyncLocked()
	p.streamMu.Unlock()
	if !strings.Contains(logged.String(), "back in sync") {
		t.Fatalf("a real departure's return was not logged: %s", logged.String())
	}
}

// TestRefusedFirstHandshakeStillRecordsDeparture guards the other side of the
// line: a peer that answered and refused is a departure, streamed or not.
func TestRefusedFirstHandshakeStillRecordsDeparture(t *testing.T) {
	departures := countDepartures(t)
	p, _ := ghostPeer(t)

	p.markFailed(errors.New("node-b refused to open this node's stream: test refusal"))

	if n := departures.count.Load(); n != 1 {
		t.Fatalf("a refused handshake recorded %d departures, want 1", n)
	}
	if p.inSync() {
		t.Fatal("a peer that refused the stream reads as in sync")
	}
}

// TestRefusalAfterSilentDialIsStillAnnounced: a peer that first did not answer
// and then answered with a refusal is announced once, at the refusal.
func TestRefusalAfterSilentDialIsStillAnnounced(t *testing.T) {
	departures := countDepartures(t)
	p, _ := ghostPeer(t)

	p.markFailed(errFormationRefused)
	departures.requireNone(t, "the silent dial")
	p.markFailed(errors.New("node-b refused to open this node's stream: test refusal"))
	p.markFailed(errors.New("node-b refused to open this node's stream: test refusal"))

	if n := departures.count.Load(); n != 1 {
		t.Fatalf("a refusal after a silent dial recorded %d departures, want 1", n)
	}
}
