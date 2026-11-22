package server

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestShipOrderTurnRule pins the rule that makes a peer's queue sequence
// ordered. A write waits for a lower sequence that could still reach
// the same peer, and for no other write.
func TestShipOrderTurnRule(t *testing.T) {
	const peerX, peerY = "x", "y"
	cases := []struct {
		name string
		// The other open ticket: its floor (unassigned, so a lower bound) and
		// the peers it still has to be queued for (nil = not yet routed).
		earlierFloor uint64
		earlierPeers []string
		// laterSeq is the waiting write's sequence; it waits for peerX.
		laterSeq    uint64
		wantBlocked bool
	}{
		{name: "lower sequence bound for the same peer blocks", earlierFloor: 10, earlierPeers: []string{peerX}, laterSeq: 11, wantBlocked: true},
		{name: "lower sequence not yet routed blocks every peer", earlierFloor: 10, earlierPeers: nil, laterSeq: 11, wantBlocked: true},
		{name: "lower sequence for another peer does not block", earlierFloor: 10, earlierPeers: []string{peerY}, laterSeq: 11, wantBlocked: false},
		{name: "lower sequence already queued for this peer does not block", earlierFloor: 10, earlierPeers: []string{}, laterSeq: 11, wantBlocked: false},
		{name: "higher floor never blocks", earlierFloor: 12, earlierPeers: nil, laterSeq: 11, wantBlocked: false},
		{name: "unassigned ticket whose floor is this sequence will land above it", earlierFloor: 11, earlierPeers: nil, laterSeq: 11, wantBlocked: false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			order := newShipOrder()
			earlier := order.open(func() uint64 { return tt.earlierFloor - 1 })
			if tt.earlierPeers != nil {
				order.route(earlier, tt.earlierPeers)
			}
			later := order.open(func() uint64 { return 0 })
			order.assign(later, tt.laterSeq)
			order.route(later, []string{peerX})

			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			err := order.waitTurn(ctx, later, peerX)
			if gotBlocked := errors.Is(err, context.DeadlineExceeded); gotBlocked != tt.wantBlocked {
				t.Fatalf("waitTurn blocked=%v (err %v), want blocked=%v", gotBlocked, err, tt.wantBlocked)
			}
		})
	}
}

// TestShipOrderReleasesWaiterWhenEarlierWriteQueues checks the wake-up: a write
// held behind a lower sequence proceeds as soon as that sequence is queued for
// the peer, or retired without being queued.
func TestShipOrderReleasesWaiterWhenEarlierWriteQueues(t *testing.T) {
	releases := []struct {
		name    string
		release func(order *shipOrder, earlier *shipTicket)
	}{
		{name: "queued", release: func(order *shipOrder, earlier *shipTicket) { order.queued(earlier, "x") }},
		{name: "retired", release: func(order *shipOrder, earlier *shipTicket) { order.close(earlier) }},
	}
	for _, tt := range releases {
		t.Run(tt.name, func(t *testing.T) {
			order := newShipOrder()
			earlier := order.open(func() uint64 { return 9 })
			order.assign(earlier, 10)
			order.route(earlier, []string{"x"})
			later := order.open(func() uint64 { return 10 })
			order.assign(later, 11)
			order.route(later, []string{"x"})

			turn := make(chan error, 1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				turn <- order.waitTurn(ctx, later, "x")
			}()

			select {
			case err := <-turn:
				t.Fatalf("seq 11 got its turn while seq 10 was still pending for the peer (err %v)", err)
			case <-time.After(50 * time.Millisecond):
			}
			tt.release(order, earlier)
			if err := <-turn; err != nil {
				t.Fatalf("seq 11 was not released after seq 10 was %s: %v", tt.name, err)
			}
		})
	}
}
