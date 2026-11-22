package wire

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestFeedSeedRefusesAnEventForAttemptWhenThePairHoldsNoSeedingRecord (BUG-0830): an event that
// names its attempt answers a call made outside the pair lock, so a pair that holds no
// seeding record when it lands is the replaced case too, not the stale one. It is refused
// with ErrSeedAttemptReplaced and nothing is written; an event with no Attempt keeps the
// stale answer.
// [guard: feedSeedLocked answers "stale, nil" before it looks at FeedSeedRequest.Attempt]
func TestFeedSeedRefusesAnEventForAttemptWhenThePairHoldsNoSeedingRecord(t *testing.T) {
	st := joiningNodes(t)
	kv := openKV(t, t.TempDir())
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Errorf("closing the store: %v", err)
		}
	})
	g := newRig(t, kv, st, testSelf, true)
	g.boot(t)
	slot := findSlots(t, st).outside
	cfg := machine.SeedConfig{SyncBytes: 1 << 20, Epoch: uint64(st.Epoch)}
	accepted := machine.RegisterAccepted{Cut: cluster.Position{Epoch: uint64(st.Epoch), Seq: 40}, WindowBytes: 1 << 20}
	if _, held := g.rec.Record(slot, reconDB); held {
		t.Fatalf("slot %d holds a record, want a pair this node holds nothing of", slot)
	}

	sentFor := [16]byte{1, 2, 3}
	_, err := g.rec.FeedSeed(slot, reconDB, FeedSeedRequest{Event: accepted, Cfg: cfg, Attempt: &sentFor})
	if !errors.Is(err, ErrSeedAttemptReplaced) {
		t.Fatalf("an accept for an attempt, on a pair with no seeding record, answered %v, want ErrSeedAttemptReplaced", err)
	}
	if _, held := g.rec.Record(slot, reconDB); held {
		t.Errorf("the refused accept wrote a record")
	}
	if _, ok := diskRecord(t, kv, slot); ok {
		t.Errorf("the refused accept wrote a record to disk")
	}

	out, err := g.rec.FeedSeed(slot, reconDB, FeedSeedRequest{Event: accepted, Cfg: cfg})
	if err != nil || out.Restart != 0 || out.MarkReady {
		t.Errorf("an event with no Attempt on a pair with no record: outcome %+v, error %v, want the stale answer (no restart, not ready, nil)", out, err)
	}
}

// TestAnAcceptThatLandsOnAPairWithNoSeedingRecordResetsAndStreamsNothing (BUG-0830): a second
// actor removes the pair's record, and then writes a key into its range, while SEED_REGISTER
// is in flight. The accept that lands must not delete that key (the range reset ran from a
// record read before the call) and must not open the stream: no SEED_ACK for a pair this
// node holds no record of. The hook plays a second remover of the record, which nothing in
// the running system is; the test pins the invariant that stands in for the guard.
// [guard: Puller.register resets from the record it read before the call; feedSeedLocked
// answers nil for an absent record]
func TestAnAcceptThatLandsOnAPairWithNoSeedingRecordResetsAndStreamsNothing(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	bug0818Source(t, rig)
	key := reconcileKey{rig.slot, reconDB}
	later := t3Key(rig.slot, reconDB, 777)
	var accepts atomic.Int64
	// The hook runs on the puller's worker and pullUntil's condition runs on the test's goroutine.
	var removed, looked atomic.Bool
	var requestAfter atomic.Uint32
	rig.wrap = func(_ string, msg transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
		if removed.Load() && !looked.Load() {
			looked.Store(true)
			requestAfter.Store(uint32(msg.Env.Kind))
		}
		if err != nil || reply == nil {
			return reply, err
		}
		if reply.Env.Kind == transport.KindSeedAccept && accepts.Add(1) == 1 {
			t3Leave(t, rig, t3Out(t, rig))
			if err := dropLeftSeed(context.Background(), rig.rec, key, rig.puller.resetRange); err != nil {
				t.Errorf("dropLeftSeed: %v", err)
			}
			if _, held := rig.rec.Record(rig.slot, reconDB); held {
				t.Error("the script did not remove the record")
			}
			batch := rig.kv.NewBatch()
			if err := batch.Set(later, []byte("later")); err != nil {
				t.Error(err)
			}
			if err := batch.Commit(true); err != nil {
				t.Error(err)
			}
			batch.Discard()
			removed.Store(true)
		}
		return reply, err
	}
	rig.pullUntil("the puller to be done with the pair", func() bool { return removed.Load() && rig.puller.Pending() == 0 })

	laterHeld := false
	for _, entry := range t3Dump(t, rig.kv, rig.slot, reconDB) {
		if bytes.Equal(entry.Key, later) {
			laterHeld = true
		}
	}
	if !laterHeld {
		t.Errorf("the accept's range reset ran on a pair with no seeding record and deleted a key written after the record went")
	}
	if looked.Load() && byte(requestAfter.Load()) == transport.KindSeedAck {
		t.Errorf("the pull sent SEED_ACK for a pair with no seeding record: it treated the accept as registered")
	}
}
