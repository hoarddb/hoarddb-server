package wire

import (
	"context"
	"sync"
	"testing"
	"time"
)

// stepTwoBLink blocks every Write to one peer until the test releases it or the call's
// context ends, like a copy that accepted the connection and then stopped answering.
type stepTwoBLink struct {
	*u32Link
	stuck   string
	release chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (l *stepTwoBLink) Write(ctx context.Context, peer string, frame WriteFrame) error {
	if peer == l.stuck {
		l.once.Do(func() { close(l.entered) })
		select {
		case <-l.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return l.u32Link.Write(ctx, peer, frame)
}

// TestAStuckCopyDoesNotHoldUpTheShipmentToAnotherCopy: the stuck copy is listed first, so a
// flusher that ships one copy at a time never reaches the healthy one. The cluster
// harness showed the same thing as a healthy write that waited out its whole write
// concern while a different copy was stalled. [GUARD: ship every copy from one loop]
func TestAStuckCopyDoesNotHoldUpTheShipmentToAnotherCopy(t *testing.T) {
	var link *stepTwoBLink
	owner := newU32Node(t, "owner", func(cfg *ReplicatorConfig) {
		link = &stepTwoBLink{u32Link: newU32Link(), stuck: "stuck", release: make(chan struct{}), entered: make(chan struct{})}
		cfg.Link = link
	})
	owner.copies = []string{"stuck", "fast"}
	owner.ownRun(u32Epoch, 1, 3)
	ctx, stop := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); owner.rep.Run(ctx) }()
	t.Cleanup(func() {
		close(link.release)
		stop()
		<-runDone
		owner.rep.Close()
	})

	owner.rep.Push(u32Slot, u32DB, u32Pos(u32Epoch, 3))
	select {
	case <-link.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the send to the stuck copy never started")
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for len(link.sent("fast")) < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("the healthy copy was sent %d of 3 frames in 500 ms while another copy was stuck", len(link.sent("fast")))
		}
		time.Sleep(10 * time.Millisecond)
	}
}
