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

// BUG-0818: a SEED_ACCEPT is the answer to a SEED_REGISTER that was sent for one attempt
// (one seed id). While the call is in flight the pair's record can be replaced by a new
// attempt, which owes a range reset. The accept must not register that new attempt: its
// reset has not run, and the cut is another attempt's.

// bug0818Replacer replaces the attempt of the rig's pair the way something beside the pull
// does while a registration is in flight. It runs on the puller's worker, inside the call.
type bug0818Replacer func(t *testing.T, rig *t3Rig)

// bug0818Restart writes a fresh attempt in one record change, as a pass that answers
// SlotRestartSeed does (both end in Reconciler.freshAttempt).
func bug0818Restart(t *testing.T, rig *t3Rig) {
	if err := rig.rec.RestartSeed(rig.slot, reconDB); err != nil {
		t.Errorf("RestartSeed: %v", err)
	}
	rig.puller.Queue(rig.slot, reconDB)
}

// bug0818LeaveAndReturn takes the pair out of the node's copy set and back in two passes: the
// first voids the attempt (a left seed), the second finds the pair in the copy set again.
func bug0818LeaveAndReturn(t *testing.T, rig *t3Rig) {
	back := rig.state
	t3Leave(t, rig, t3Out(t, rig))
	rig.state = back
	if err := rig.rec.OnStateSeq(context.Background(), back); err != nil {
		t.Errorf("the pass that sees the pair come back: %v", err)
	}
	rig.puller.Queue(rig.slot, reconDB)
}

// bug0818Source gives the rig one source, the owner, holding ten keys and one dedupe entry.
func bug0818Source(t *testing.T, rig *t3Rig) *t3Source {
	t.Helper()
	owner := t3NewSource(t, "x", "x", uint64(rig.state.Epoch), SeedOptions{ChunkEntries: 3})
	rig.owner = "x"
	rig.sources = map[string]*t3Source{"x": owner}
	owner.fill(t, rig.slot, reconDB, 10, t3Cut(t, rig), 42)
	return owner
}

// TestAnAcceptSentForAReplacedAttemptDoesNotRegisterTheNewOne (BUG-0818): the record is
// replaced while SEED_REGISTER is in flight. The accept that comes back was sent for the old
// attempt, so the new one stays in the reset phase, no chunk is asked for under it, and the
// pull registers the new attempt itself, which resets the range before the first chunk.
// [guard: FeedSeed applies an accept to whatever attempt the record holds]
func TestAnAcceptSentForAReplacedAttemptDoesNotRegisterTheNewOne(t *testing.T) {
	cases := []struct {
		name string
		// staleAccept is which accept is in flight when the attempt is replaced: 1 is the
		// first registration (the old attempt owes its reset), 2 is the resume after a broken
		// stream (the range holds the old attempt's prefix).
		staleAccept int64
		replace     bug0818Replacer
	}{
		{"a first registration and a restart of the seed", 1, bug0818Restart},
		{"a resume registration and a restart of the seed", 2, bug0818Restart},
		{"a resume registration and the pair leaving and returning", 2, bug0818LeaveAndReturn},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newT3Rig(t, t3Options{})
			bug0818Source(t, rig)

			var accepts, chunks atomic.Int64
			var sent, renewed, landed SeedRecord
			var replaced, looked, firstChunkSeen bool
			var requestAfter byte
			heldAtFirstChunk := -1
			rig.wrap = func(_ string, msg transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
				if err != nil || reply == nil {
					return reply, err
				}
				if replaced && !looked {
					// The first exchange after the accept landed: what the record is now, and
					// what the pull asked for under it.
					looked = true
					landed, _ = rig.rec.Record(rig.slot, reconDB)
					requestAfter = msg.Env.Kind
				}
				switch reply.Env.Kind {
				case transport.KindSeedChunk:
					if replaced && !firstChunkSeen {
						firstChunkSeen = true
						heldAtFirstChunk = len(t3Dump(t, rig.kv, rig.slot, reconDB))
					}
					if chunks.Add(1) == 2 && tc.staleAccept == 2 {
						return nil, errors.New("the stream breaks after one applied chunk, so the next registration is a resume")
					}
				case transport.KindSeedAccept:
					if accepts.Add(1) == tc.staleAccept {
						sent, _ = rig.rec.Record(rig.slot, reconDB)
						tc.replace(t, rig)
						renewed, _ = rig.rec.Record(rig.slot, reconDB)
						replaced = true
					}
				}
				return reply, err
			}

			rig.pullUntil("the pair to be ready", rig.isReady)

			if !looked {
				t.Fatal("the pull made no call after the accept of the replaced attempt")
			}
			if renewed.SeedID == sent.SeedID || renewed.Phase != machine.PhaseReset {
				t.Fatalf("the script did not replace the attempt: sent for %x (phase %d), then %x (phase %d)", sent.SeedID, sent.Phase, renewed.SeedID, renewed.Phase)
			}
			if landed.SeedID != renewed.SeedID {
				t.Fatalf("after the accept the record is attempt %x, want the new attempt %x", landed.SeedID, renewed.SeedID)
			}
			if landed.Phase != machine.PhaseReset {
				t.Errorf("an accept sent for attempt %x registered attempt %x, which owed a range reset: phase %d, cut %v", sent.SeedID, landed.SeedID, landed.Phase, landed.Cut)
			}
			if requestAfter != transport.KindSeedRegister {
				t.Errorf("after the accept of the replaced attempt the pull sent kind 0x%02x, want SEED_REGISTER (0x%02x) for the new attempt", requestAfter, transport.KindSeedRegister)
			}
			if heldAtFirstChunk != 0 {
				t.Errorf("the range held %d keys when the new attempt's first chunk arrived, want 0: its reset comes before its registration", heldAtFirstChunk)
			}
		})
	}
}

// TestAReadyCopyHoldsNoKeyItsSourceDeletedAfterAReplacedAttemptsAccept (BUG-0818, the
// consequence): the resume registration of an attempt that applied one chunk is in flight
// when the attempt is replaced and the source deletes a key that chunk carried. The stream
// then breaks before a chunk applies, so the new attempt registers fresh on a cut past the
// delete. The copy that turns ready must hold what the source holds: the range of the old
// attempt, deleted key included, is reset before the new attempt registers.
// [guard: FeedSeed applies an accept to whatever attempt the record holds]
func TestAReadyCopyHoldsNoKeyItsSourceDeletedAfterAReplacedAttemptsAccept(t *testing.T) {
	rig := newT3Rig(t, t3Options{})
	owner := bug0818Source(t, rig)
	deleted := t3Key(rig.slot, reconDB, 0)

	var accepts, chunks atomic.Int64
	var replaced, brokeAfter bool
	rig.wrap = func(_ string, _ transport.Message, reply *transport.Message, err error) (*transport.Message, error) {
		if err != nil || reply == nil {
			return reply, err
		}
		switch reply.Env.Kind {
		case transport.KindSeedChunk:
			if chunks.Add(1) == 2 {
				return nil, errors.New("the stream breaks after one applied chunk, so the next registration is a resume")
			}
			if replaced && !brokeAfter {
				brokeAfter = true
				return nil, errors.New("the stream breaks again before a chunk applies under the new attempt")
			}
		case transport.KindSeedAccept:
			if accepts.Add(1) == 2 {
				bug0818Restart(t, rig)
				// The source moves on between the two cuts: the first key is deleted there, at
				// a later position.
				batch := owner.kv.NewBatch()
				defer batch.Discard()
				if err := batch.Delete(deleted); err != nil {
					t.Error(err)
				}
				if err := batch.Commit(true); err != nil {
					t.Error(err)
				}
				moved := t3Cut(t, rig)
				moved.Seq++
				owner.pairs.With(rig.slot, reconDB, func(pair *cluster.Pair) { pair.AppliedPos = moved })
				replaced = true
			}
		}
		return reply, err
	}

	rig.pullUntil("the pair to be ready", rig.isReady)

	if !replaced || !brokeAfter {
		t.Fatalf("the script did not run: replaced %v, second break %v", replaced, brokeAfter)
	}
	want := owner.dump(t, rig.slot, reconDB)
	for _, entry := range want {
		if bytes.Equal(entry.Key, deleted) {
			t.Fatal("the source still holds the key the script deleted")
		}
	}
	got := t3Dump(t, rig.kv, rig.slot, reconDB)
	for _, entry := range got {
		if bytes.Equal(entry.Key, deleted) {
			t.Errorf("the ready copy holds key %q, which its source deleted", entry.Key[len(entry.Key)-7:])
		}
	}
	t3SameEntries(t, got, want)
}
