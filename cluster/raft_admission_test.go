package cluster

import (
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// admissionHello is a peer's handshake on channel, at this build's own range.
func admissionHello(channel byte, id string) transport.Handshake {
	return BuildStateRange().Stamp(transport.Handshake{Channel: channel, NodeID: id, Incarnation: 1})
}

// TestARaftOpenedToJoinWithNoStateRefusesEveryChannelUntilAdmitted: [GUARD BUG-0870 guard: a node
// that opened with no Raft state in join mode takes no peer connection, the Raft channel included,
// until Admit; then it takes them. A bootstrapped node and a node restarted on its own Raft state are
// never held]. Red while AdmitPeer admits a node with no state on the Raft channel at once, which is
// how a member whose ring directory was moved aside was caught up as the voter it had been.
func TestARaftOpenedToJoinWithNoStateRefusesEveryChannelUntilAdmitted(t *testing.T) {
	channels := []byte{transport.HandshakeChannelRaft, transport.HandshakeChannelData, transport.HandshakeChannelControl, transport.HandshakeChannelSeed}
	dir := t.TempDir()
	cfg := quickRaft(t, dir, "n3")
	cfg.Bootstrap = false
	joiner := openHeld(t, cfg)
	for _, channel := range channels {
		err := joiner.AdmitPeer(admissionHello(channel, "n1"), false)
		if err == nil {
			t.Fatalf("the %s channel admitted n1 before any member admitted this node", transport.Channel(channel))
		}
		for _, want := range []string{"refused n1", "holds no cluster state", "'join n3@n3:1'", "keep the ring directory you moved aside", "'replace-node n3 --with n3@n3:1'"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the %s refusal lacks %q: %v", transport.Channel(channel), want, err)
			}
		}
	}
	if !joiner.AwaitingAdmission() {
		t.Fatal("a node that refuses its peers does not report that it waits for admission")
	}
	joiner.Admit()
	if joiner.AwaitingAdmission() {
		t.Fatal("Admit left the node waiting")
	}
	if err := joiner.AdmitPeer(admissionHello(transport.HandshakeChannelRaft, "n1"), false); err != nil {
		t.Fatalf("the Raft channel refused n1 after Admit: %v", err)
	}

	boot := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	if boot.AwaitingAdmission() {
		t.Fatal("a bootstrapping node waits for admission")
	}
	if err := boot.AdmitPeer(admissionHello(transport.HandshakeChannelRaft, "n2"), false); err != nil {
		t.Fatalf("a bootstrapped node refused the Raft channel: %v", err)
	}

	restartDir := t.TempDir()
	first := openOrFail(t, quickRaft(t, restartDir, "n4")) // the open commits BOOTSTRAP before it returns
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	again := quickRaft(t, restartDir, "n4")
	again.Bootstrap = false
	restarted := openHeld(t, again)
	if restarted.AwaitingAdmission() {
		t.Fatal("a node restarted on its own Raft state waits for admission")
	}
}

// TestAConnectionAcceptedBeforeTheOpenReadsItsStateIsRefused: [GUARD BUG-0870 guard, review F1: the
// listener is bound before OpenRaft runs and the leader redials a restarted voter all the time, so a
// connection can sit in the accept backlog and be admitted the moment the stream layer starts, before
// the open has read the Raft directory. That connection is long-lived: if it passes, the leader catches
// the wiped voter up. It must be refused]. Red while the latch is decided only after the stream starts
// (its zero value admits). The seam holds the open until the backlogged connection has its answer.
func TestAConnectionAcceptedBeforeTheOpenReadsItsStateIsRefused(t *testing.T) {
	cfg := quickRaft(t, t.TempDir(), "n3")
	cfg.Bootstrap = false
	backlogged := dialWithHandshake(t, cfg.Listener.Addr().String(), admissionHello(transport.HandshakeChannelRaft, "n1"))
	refused := make(chan bool, 1)
	afterRaftStreamStart = func() {
		// A refused connection is closed at once; an admitted one stays open for the library.
		if err := backlogged.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Error(err)
		}
		_, err := backlogged.Read(make([]byte, 1))
		var timeout net.Error
		refused <- !errors.As(err, &timeout) || !timeout.Timeout()
	}
	t.Cleanup(func() { afterRaftStreamStart = nil })
	joiner := openHeld(t, cfg)
	if !<-refused {
		t.Fatal("a Raft connection waiting in the accept backlog was admitted before the open decided whether this node is held")
	}
	if !joiner.AwaitingAdmission() {
		t.Fatal("the node opened to join with no state is not held")
	}
}
