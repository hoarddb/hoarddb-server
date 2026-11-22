package wire

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/transport"
)

// stallClock is real time plus a shift the test moves: the puller's retries keep firing in
// real milliseconds while the idle clock jumps.
type stallClock struct{ shift atomic.Int64 }

func (c *stallClock) Now() time.Time { return time.Now().Add(time.Duration(c.shift.Load())) }

func (c *stallClock) advance(by time.Duration) { c.shift.Add(int64(by)) }

// stallWait polls until done, or fails the test after five seconds.
func stallWait(t *testing.T, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// startPull runs the rig's puller on its pair and returns the function that stops it.
func (r *t3Rig) startPull() (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		r.puller.Run(ctx)
	}()
	r.puller.Queue(r.slot, reconDB)
	return func() {
		cancel()
		<-finished
	}
}

// stalledPull is a pull that applied one chunk of three entries and then lost the source:
// every later ack fails, while registrations still succeed, so each retry reaches the point
// where a pull announces that it has a source.
type stalledPull struct {
	rig   *t3Rig
	clock *stallClock
	acks  atomic.Int64
	// chunkBytes is the entry bytes of the one chunk that was applied.
	chunkBytes int64
}

const stallIdle = 10 * time.Second

func newStalledPull(t *testing.T) *stalledPull {
	t.Helper()
	pull := &stalledPull{clock: &stallClock{}}
	pull.rig = newT3Rig(t, t3Options{seed: SeedOptions{Idle: stallIdle, Now: pull.clock.Now}})
	owner := t3NewSource(t, "x", "x", uint64(pull.rig.state.Epoch), SeedOptions{ChunkEntries: 3})
	pull.rig.owner, pull.rig.sources = "x", map[string]*t3Source{"x": owner}
	owner.fill(t, pull.rig.slot, reconDB, 10, t3Cut(t, pull.rig), 0)
	for _, entry := range owner.dump(t, pull.rig.slot, reconDB)[:3] {
		pull.chunkBytes += int64(entryBytes(entry.Key, entry.Value))
	}
	pull.rig.wrap = func(_ string, msg transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
		if msg.Env.Kind == transport.KindSeedAck && pull.acks.Add(1) >= 2 {
			return nil, errors.New("the link to the source is down")
		}
		return reply, err
	}
	return pull
}

// stallAfterTwoRetries runs the pull until it has failed, moves the clock past seed_idle_seconds
// and lets two more retries run, so a retry that re-anchored the idle clock would show.
func (s *stalledPull) stallAfterTwoRetries(t *testing.T) (PullStatus, func()) {
	t.Helper()
	stop := s.rig.startPull()
	t.Cleanup(stop)
	stallWait(t, "a failed ack with its reason", func() bool {
		status, _ := s.rig.puller.Status(s.rig.slot, reconDB)
		return s.acks.Load() >= 3 && status.Detail != ""
	})
	s.clock.advance(stallIdle + time.Second)
	seen := s.acks.Load()
	stallWait(t, "two retries after the clock moved", func() bool { return s.acks.Load() >= seen+2 })
	// A retry clears the reason when it registers and sets it again when its ack fails.
	var status PullStatus
	stallWait(t, "the reason of the failed ack", func() bool {
		status, _ = s.rig.puller.Status(s.rig.slot, reconDB)
		return status.Detail != ""
	})
	return status, stop
}

func TestAStallIsMeasuredFromTheLastAppliedChunk(t *testing.T) {
	pull := newStalledPull(t)
	status, _ := pull.stallAfterTwoRetries(t)
	if status.State != PullStalled {
		t.Errorf("the pull's state is %q, want %q: eleven seconds have passed since the last applied chunk and seed_idle_seconds is ten", status.State, PullStalled)
	}
	if status.Idle < stallIdle+time.Second || status.Idle > stallIdle+6*time.Second {
		t.Errorf("Idle is %s, want about 11s", status.Idle)
	}
	if status.Detail == "" {
		t.Error("a stalled row has no Detail")
	}
}

func TestAStatusRowCarriesBytesEntriesAndIdle(t *testing.T) {
	pull := newStalledPull(t)
	status, _ := pull.stallAfterTwoRetries(t)
	if status.Bytes != pull.chunkBytes || status.Entries != 3 {
		t.Errorf("the row has %d bytes and %d entries, want %d and 3, the one chunk that was applied", status.Bytes, status.Entries, pull.chunkBytes)
	}
	if status.Idle <= 0 {
		t.Errorf("Idle is %s, want the time since the applied chunk", status.Idle)
	}
}

// failoverPull is a pull whose owner x answers every message with nothing, a wait that is not a
// failed call, and whose other copy y serves.
type failoverPull struct {
	rig     *t3Rig
	clock   *stallClock
	xCalls  atomic.Int64
	yCalls  atomic.Int64
	shiftAt atomic.Int64 // the clock's shift at the first message to y
}

func newFailoverPull(t *testing.T) *failoverPull {
	t.Helper()
	pull := &failoverPull{clock: &stallClock{}}
	pull.rig = newT3Rig(t, t3Options{seed: SeedOptions{Idle: stallIdle, Now: pull.clock.Now}})
	epoch := uint64(pull.rig.state.Epoch)
	owner := t3NewSource(t, "x", "x", epoch, SeedOptions{})
	copyY := t3NewSource(t, "y", "x", epoch, SeedOptions{})
	copyY.owner = "y"
	copyY.fill(t, pull.rig.slot, reconDB, 5, t3Cut(t, pull.rig), 0)
	pull.rig.owner, pull.rig.sources = "x", map[string]*t3Source{"x": owner, "y": copyY}
	pull.rig.wrap = func(source string, _ transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
		if source == "x" {
			pull.xCalls.Add(1)
			return nil, nil
		}
		if pull.yCalls.Add(1) == 1 {
			pull.shiftAt.Store(pull.clock.shift.Load())
		}
		return reply, err
	}
	return pull
}

// run lets the pull fail against x, moves the clock past seed_idle_seconds and waits for the
// pair to become ready from y.
func (f *failoverPull) run(t *testing.T) {
	t.Helper()
	stop := f.rig.startPull()
	t.Cleanup(stop)
	stallWait(t, "a call to the owner that is answered with nothing", func() bool {
		status, _ := f.rig.puller.Status(f.rig.slot, reconDB)
		return f.xCalls.Load() >= 1 && status.Detail != ""
	})
	f.clock.advance(stallIdle + time.Second)
	stallWait(t, "the pair to be ready from the other copy", f.rig.isReady)
}

func TestAStalledPullRetriesAgainstTheNextLegalSource(t *testing.T) {
	pull := newFailoverPull(t)
	pull.run(t)
	if pull.xCalls.Load() == 0 {
		t.Fatal("the owner was never asked")
	}
	if shift := time.Duration(pull.shiftAt.Load()); shift < stallIdle {
		t.Errorf("the first message to y came with the clock %s ahead, want it after the %s stall", shift, stallIdle)
	}
	if record, _ := pull.rig.rec.Record(pull.rig.slot, reconDB); record.State != machine.SeedReady {
		t.Errorf("the record is in state %d, want ready from the second copy", record.State)
	}
}

func TestAFreshAttemptRestartsTheProgressCounts(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 3})
	rig.owner, rig.sources = "x", map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 9, t3Cut(t, rig), 0)

	var chunks atomic.Int64
	var restarted atomic.Bool
	var before atomic.Int64
	var afterRestart atomic.Int64
	rig.wrap = func(_ string, msg transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
		if restarted.Load() {
			afterRestart.Add(1)
			return nil, errors.New("the link to the source is down")
		}
		if reply != nil && reply.Env.Kind == transport.KindSeedChunk && chunks.Add(1) == 2 {
			// The source loses its pin: the next ack is answered SEED_RESTART.
			ack, ackErr := transport.DecodeSeedAck(payloadOf(msg))
			if ackErr != nil {
				t.Error(ackErr)
			}
			owner.src.dropPin(ack.SeedID)
		}
		if reply != nil && reply.Env.Kind == transport.KindSeedRestart {
			status, _ := rig.puller.Status(rig.slot, reconDB)
			before.Store(status.Bytes)
			restarted.Store(true)
		}
		return reply, err
	}
	stop := rig.startPull()
	t.Cleanup(stop)
	stallWait(t, "the restart and two failed calls after it", func() bool { return restarted.Load() && afterRestart.Load() >= 2 })
	if before.Load() == 0 {
		t.Fatal("the pull had counted no bytes when the source restarted it, so the test proves nothing")
	}
	status, _ := rig.puller.Status(rig.slot, reconDB)
	if status.Bytes != 0 || status.Entries != 0 {
		t.Errorf("after a restart on a fresh cut the row has %d bytes and %d entries, want 0 and 0 (it had %d bytes before)", status.Bytes, status.Entries, before.Load())
	}
}
