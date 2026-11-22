package wire

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// bug0766Verb is one store read of the handoff fixture's document, answered or refused.
type bug0766Verb struct {
	name string
	read func(h *u36Handoff, ctx context.Context) error
}

func (h *u36Handoff) fieldRequest() ReadRequest {
	return ReadRequest{Slot: h.slot, DBID: u33DB, BucketID: testBucket, EncID: h.encID, ID: "p1", RequestID: [16]byte{9, 9}}
}

// bug0766Verbs is every read that is not a document get and goes through the read gate: the
// three readField verbs, peek and lease_get.
var bug0766Verbs = []bug0766Verb{
	{"list", func(h *u36Handoff, ctx context.Context) error {
		_, err := h.reader.ListField(ctx, FieldRead{Request: h.fieldRequest(), FieldID: 5, Engine: store.FieldFIFO, Limit: 10})
		return err
	}},
	{"count", func(h *u36Handoff, ctx context.Context) error {
		_, err := h.reader.CountField(ctx, FieldRead{Request: h.fieldRequest(), FieldID: 5, Engine: store.FieldFIFO})
		return err
	}},
	{"get", func(h *u36Handoff, ctx context.Context) error {
		_, _, err := h.reader.GetField(ctx, FieldRead{Request: h.fieldRequest(), FieldID: 6, Engine: store.FieldHash}, "k")
		return err
	}},
	{"peek", func(h *u36Handoff, ctx context.Context) error {
		_, err := h.reader.Peek(ctx, PeekField{Request: h.fieldRequest(), FieldID: 5, Engine: store.FieldFIFO, Count: 1})
		return err
	}},
	{"lease_get", func(h *u36Handoff, ctx context.Context) error {
		_, err := h.reader.LeaseElements(ctx, LeaseField{Request: h.fieldRequest(), FieldID: 5, Engine: store.FieldFIFO})
		return err
	}},
	{"nearest", func(h *u36Handoff, ctx context.Context) error {
		_, err := h.reader.Nearest(ctx, FieldRead{Request: h.fieldRequest(), FieldID: 5, Engine: store.FieldFIFO}, store.NearestRequest{Metric: store.NearestCosine, K: 3, Field: "Vector", Vector: u90Vector(1, 0)})
		return err
	}},
}

// bug0766Untrue fails when a read refusal carries the write path's words or an unfilled placeholder.
func bug0766Untrue(t *testing.T, what string, err error) {
	t.Helper()
	text := err.Error()
	for _, bad := range []string{"<", "paused", "retained log", "Nothing was written", "Nothing was acknowledged"} {
		if strings.Contains(text, bad) {
			t.Errorf("%s: the refusal of a read contains %q: %s", what, bad, text)
		}
	}
}

// TestAStoreReadOnAHeldOwnerWaitsForTheHold (BUG-0766): a store read at the node whose pair is
// held parks as a document read does, and answers once the hold ends.
func TestAStoreReadOnAHeldOwnerWaitsForTheHold(t *testing.T) {
	for _, verb := range bug0766Verbs {
		t.Run(verb.name, func(t *testing.T) {
			node, slots := u33Awaiting(t, cluster.Position{})
			h := newU36Handoff(t, node, slots[0], true)
			errs := make(chan error, 1)
			go func() { errs <- verb.read(h, context.Background()) }()
			select {
			case err := <-errs:
				t.Fatalf("%s returned instead of waiting in the hold: %v", verb.name, err)
			case <-h.holder.parkedSignal(t, node, slots[0]):
			}
			u33Stopped(t, node, "A", 2, slots[0])
			if err := node.h.MaybeSendReady(context.Background(), slots[0], u33DB); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-errs:
				if err != nil {
					t.Fatalf("%s after the hold ended: %v", verb.name, err)
				}
			case <-time.After(10 * time.Second):
				t.Fatalf("%s never answered after the hold ended", verb.name)
			}
			if h.fwd.count() != 0 || h.holder.calls.Load() != 1 {
				t.Fatalf("%s answered locally once, but %d forwards and %d holds", verb.name, h.fwd.count(), h.holder.calls.Load())
			}
			if held, waiters := h.heldLists(); held != 0 || waiters != 0 {
				t.Fatalf("%s is still listed: %d held, %d waiting", verb.name, held, waiters)
			}
		})
	}
}

// parkedSignal returns a channel closed once a read is parked on the pair.
func (h *u36CountingHolder) parkedSignal(t *testing.T, node *u33Node, slot uint16) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			node.h.mu.Lock()
			hp := node.h.pairs[pairKey{slot, u33DB}]
			parked := hp != nil && len(hp.waiters) > 0
			node.h.mu.Unlock()
			if parked {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	return done
}

// TestAStoreReadOnAHeldOwnerRefusesWithTheTrueTextWhenTheHoldDoesNotLift (BUG-0766, BUG-0759): the
// wait is bounded by the caller's deadline and the refusal is a retryable UNAVAILABLE that
// says nothing was returned, with no unfilled placeholder and no write-path wording.
func TestAStoreReadOnAHeldOwnerRefusesWithTheTrueTextWhenTheHoldDoesNotLift(t *testing.T) {
	for _, verb := range bug0766Verbs {
		t.Run(verb.name, func(t *testing.T) {
			node, slots := u33Awaiting(t, cluster.Position{})
			h := newU36Handoff(t, node, slots[0], true)
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			err := verb.read(h, ctx)
			var coded *cluster.Error
			if !errors.As(err, &coded) || coded.Code != cluster.CodeUnavailable {
				t.Fatalf("%s: want a coded UNAVAILABLE after the bound, got %v", verb.name, err)
			}
			if !strings.Contains(err.Error(), "Nothing was returned") || !strings.Contains(err.Error(), "Retry") {
				t.Fatalf("%s: the refusal must say nothing was returned and to retry: %v", verb.name, err)
			}
			bug0766Untrue(t, verb.name, err)
		})
	}
}

// TestAStoreReadOnAPairTheGateHoldsSaysWhatIsHeld (BUG-0759): a hold the Handoff does not know
// (a replay, a seeding copy) is refused with its cause and the retry advice, not with the
// paused-writes text of a poisoned log.
func TestAStoreReadOnAPairTheGateHoldsSaysWhatIsHeld(t *testing.T) {
	cases := []struct {
		name  string
		setup func(h *u36Handoff)
		cause string
	}{
		{"log tail not replayed", func(h *u36Handoff) {
			h.node.pairs.With(h.slot, u33DB, func(p *cluster.Pair) { p.Flags = flagStopProved })
		}, "replayed"},
		{"seeding", func(h *u36Handoff) { h.states.set(h.slot, machine.SlotSeeding) }, "seeding"},
	}
	for _, tc := range cases {
		for _, verb := range bug0766Verbs {
			t.Run(tc.name+"/"+verb.name, func(t *testing.T) {
				node, slot := u36Settled(t)
				h := newU36Handoff(t, node, slot, true)
				h.node.pairs.With(slot, u33DB, func(p *cluster.Pair) { p.Flags = flagReplayed | flagStopProved })
				tc.setup(h)
				err := verb.read(h, context.Background())
				var coded *cluster.Error
				if !errors.As(err, &coded) || coded.Code != cluster.CodeUnavailable {
					t.Fatalf("want a coded UNAVAILABLE, got %v", err)
				}
				if !strings.Contains(err.Error(), tc.cause) || !strings.Contains(err.Error(), "Retry") || !strings.Contains(err.Error(), "Nothing was returned") {
					t.Fatalf("the refusal must name %q, say nothing was returned and to retry: %v", tc.cause, err)
				}
				bug0766Untrue(t, tc.name+"/"+verb.name, err)
				if h.fwd.count() != 0 {
					t.Fatalf("a held pair forwarded %d reads", h.fwd.count())
				}
			})
		}
	}
}

// TestAStoreReadThatFailsItsConfirmIsRedoneFromANewSnapshot (BUG-0766): the declaration moves
// between the admission and the confirmation of a list and of a peek, so the first answer is
// dropped and the read runs again on a new snapshot. The answer is one clean read: no page is
// built on the first pass's elements, so none is doubled and none is missing.
func TestAStoreReadThatFailsItsConfirmIsRedoneFromANewSnapshot(t *testing.T) {
	verbs := []struct {
		name string
		read func(reader *Reader, req ReadRequest) (FieldElements, error)
	}{
		{"list with a cursor", func(reader *Reader, req ReadRequest) (FieldElements, error) {
			return reader.ListField(context.Background(), FieldRead{Request: req, FieldID: 4, Engine: store.FieldFIFO, After: store.PositionRemainder(1), Limit: 3})
		}},
		{"peek with a count", func(reader *Reader, req ReadRequest) (FieldElements, error) {
			return reader.Peek(context.Background(), PeekField{Request: req, FieldID: 4, Engine: store.FieldFIFO, Count: 3})
		}},
	}
	for _, verb := range verbs {
		t.Run(verb.name, func(t *testing.T) {
			world := newU36World(t)
			world.withStoreSchema()
			world.insertWithStore("p1", 0, 0)
			world.pushPosts(6)
			kv := &u36SnapshotCounter{KV: world.fx.kv}
			gate := NewReadGate(ReadGateConfig{Topology: world.fx.topo, Pairs: world.fx.pairs, Clock: fixedClock{}, Lease: world.fx.lease, Schema: world.fx.schema, Slots: world.states})
			reader := NewReader(ReaderConfig{Gate: gate, KV: kv, Holder: world.barrier, PeerReachable: func(uint16, uint64) bool { return true }, Forward: world.fwd, Self: "n1"})
			req := ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("p1"), ID: "p1", RequestID: [16]byte{3}}

			clean, err := verb.read(reader, req)
			if err != nil || len(clean.Elements) != 3 {
				t.Fatalf("the clean read: %d elements, %v, want 3", len(clean.Elements), err)
			}
			loadsBefore := kv.loads.Load()

			// The bump lands inside the confirmation's declaration read, once.
			base := world.fx.schema.viewCount()
			world.fx.schema.beforeView = func(n int, s *fakeSchema) {
				if n == base+2 {
					s.decl.Version++
					s.seen, s.published = s.decl.Version, s.decl.Version
				}
			}
			got, err := u36BoundedElements(t, func() (FieldElements, error) { return verb.read(reader, req) })
			if err != nil {
				t.Fatalf("the read after the declaration moved: %v", err)
			}
			if loads := kv.loads.Load() - loadsBefore; loads != 2 {
				t.Fatalf("%d snapshots, want 2: the confirmation did not fail, so nothing was tested", loads)
			}
			if !reflect.DeepEqual(got, clean) {
				t.Fatalf("the redone read answered %+v, want the clean read's %+v", got, clean)
			}
		})
	}
}

// pushPosts appends count elements to the Posts fifoStore (field 4) of document p1 through the
// field-store write, so the state and the cursors are the ones a real push leaves.
func (w *u36World) pushPosts(count int) {
	w.t.Helper()
	owner := storage.Key(w.slot, testDB, testBucket, storage.NSFieldStore, w.encID("p1"))
	ref := store.FieldRef{Owner: owner, FieldID: 4, Engine: store.FieldFIFO}
	for element := 0; element < count; element++ {
		snap := w.fx.kv.NewSnapshot()
		batch := w.fx.kv.NewBatch()
		var parent store.BucketState
		if _, err := store.FieldPush(store.FieldWrite{Snap: snap, State: &parent, Batch: batch}, ref, []byte(fmt.Sprintf("element-%d", element))); err != nil {
			w.t.Fatalf("FieldPush: %v", err)
		}
		if err := batch.Commit(true); err != nil {
			w.t.Fatalf("Commit: %v", err)
		}
		snap.Discard()
	}
}

// u36BoundedElements runs a store read and fails the test instead of hanging when it does not return.
func u36BoundedElements(t *testing.T, fn func() (FieldElements, error)) (FieldElements, error) {
	t.Helper()
	type outcome struct {
		page FieldElements
		err  error
	}
	done := make(chan outcome, 1)
	go func() {
		page, err := fn()
		done <- outcome{page, err}
	}()
	select {
	case got := <-done:
		return got.page, got.err
	case <-time.After(5 * time.Second):
		t.Fatal("the read did not return: it is looping")
		return FieldElements{}, nil
	}
}
