package wire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// u36States is the slot record state per pair; absent pairs read as ready.
type u36States struct {
	mu    sync.Mutex
	state map[uint16]machine.SlotState
	// rebuilding are the slots whose record is ready on disk but asked for a re-seed
	// (machine.SlotRecord posture: serves nothing).
	rebuilding map[uint16]bool
}

// Serves implements SlotStates: ready and not rebuilding.
func (s *u36States) Serves(slot uint16, db uint64) bool {
	s.mu.Lock()
	rebuilding := s.rebuilding[slot]
	s.mu.Unlock()
	return !rebuilding && s.State(slot, db) == machine.SlotReady
}

func (s *u36States) setRebuilding(slot uint16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rebuilding == nil {
		s.rebuilding = map[uint16]bool{}
	}
	s.rebuilding[slot] = true
}

func (s *u36States) State(slot uint16, _ uint64) machine.SlotState {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state, ok := s.state[slot]; ok {
		return state
	}
	return machine.SlotReady
}

func (s *u36States) set(slot uint16, state machine.SlotState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state[slot] = state
}

// u36Holder is a Holder that parks until released and then returns rel. The real
// Handoff is used in TestAHeldReadIsDeliveredExactlyOnce.
type u36Holder struct {
	rel     HoldRelease
	parked  chan struct{}
	release chan struct{}
	calls   atomic.Int64
}

func newU36Holder() *u36Holder {
	return &u36Holder{rel: HoldRelease{Owner: "n1", Local: true}, parked: make(chan struct{}, 16), release: make(chan struct{})}
}

func (h *u36Holder) Hold(ctx context.Context, _ HeldRequest) (HoldRelease, error) {
	h.calls.Add(1)
	h.parked <- struct{}{}
	select {
	case <-h.release:
		return h.rel, nil
	case <-ctx.Done():
		return HoldRelease{}, fmt.Errorf("waiting for the hold: %w", ctx.Err())
	}
}

// u36OwnerNearest is the answer the fake owner gives a forwarded nearest.
var u36OwnerNearest = NearestAnswer{
	FieldElements: FieldElements{Elements: [][]byte{[]byte("owner-a"), []byte("owner-b")}, Keys: []string{"k1", "k2"}},
	Scores:        []float64{0.75, 0.5}, Scanned: 7, Skipped: 2,
}

// u36Forwarder answers as the owner would and records what it was asked.
type u36Forwarder struct {
	mu     sync.Mutex
	reqs   []ReadRequest
	fields []FieldForward
}

func (f *u36Forwarder) ForwardRead(_ context.Context, _ string, req ReadRequest) (Document, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs = append(f.reqs, req)
	return Document{Slot: req.Slot, EncID: req.EncID, ServedBy: "n1"}, nil
}

func (f *u36Forwarder) ForwardFieldRead(_ context.Context, _ string, forwarded FieldForward) (FieldAnswer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs = append(f.reqs, forwarded.Req)
	f.fields = append(f.fields, forwarded)
	if forwarded.Op == FieldOpNearest {
		return FieldAnswer{Nearest: u36OwnerNearest}, nil
	}
	if forwarded.Op == FieldOpAggregate {
		return FieldAnswer{Aggregate: AggregateAnswer{Document: []byte("owner-aggregate"), More: true, Cursor: []byte("c")}}, nil
	}
	return FieldAnswer{Total: 42, Found: true, Element: []byte("owner"), Page: FieldElements{More: true, Next: []byte("n")}, Lease: LeaseRead{Found: true, Until: 7}}, nil
}

func (f *u36Forwarder) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reqs)
}

// flakyLease is valid for its first validFor evaluations and lapsed after.
type flakyLease struct {
	validFor int64
	calls    atomic.Int64
}

func (l *flakyLease) Serve(machine.ServeCheck) machine.LeaseDecision {
	return machine.LeaseDecision{Valid: l.calls.Add(1) <= l.validFor}
}

type u36World struct {
	t       *testing.T
	fx      *fixture
	states  *u36States
	barrier *u36Holder
	fwd     *u36Forwarder
	slot    uint16
	gate    *ReadGate
	reader  *Reader
}

// newU36World is a three-member cluster at RF 2 where this node is n1 and owns slot.
func newU36World(t *testing.T) *u36World {
	t.Helper()
	fx := newFixture(t)
	slot := fx.withCluster(2, machine.ConcernOne)
	world := &u36World{t: t, fx: fx, states: &u36States{state: map[uint16]machine.SlotState{}}, barrier: newU36Holder(), fwd: &u36Forwarder{}, slot: slot}
	world.gate, world.reader = world.build(fx.topo, fx.pairs, fx.lease)
	return world
}

func (w *u36World) build(topo Topology, pairs *cluster.PairSet, lease ServeChecker) (*ReadGate, *Reader) {
	gate := NewReadGate(ReadGateConfig{Topology: topo, Pairs: pairs, Clock: fixedClock{}, Lease: lease, Schema: w.fx.schema, Slots: w.states})
	return gate, NewReader(ReaderConfig{Gate: gate, KV: w.fx.kv, Holder: w.barrier, PeerReachable: func(uint16, uint64) bool { return true }, Forward: w.fwd, Self: topo.Self()})
}

func (w *u36World) encID(id string) []byte {
	w.t.Helper()
	encoded, err := store.EncodeComponent(id)
	if err != nil {
		w.t.Fatalf("EncodeComponent: %v", err)
	}
	return encoded
}

func (w *u36World) get(id string) (Document, error) {
	return w.reader.Get(context.Background(), w.slot, testDB, testBucket, w.encID(id))
}

func u36Refusal(t *testing.T, err error, want cluster.Code) {
	t.Helper()
	var coded *cluster.Error
	if !errors.As(err, &coded) {
		t.Fatalf("want a %s refusal, got %v", want, err)
	}
	if coded.Code != want {
		t.Fatalf("want %s, got %s: %s", want, coded.Code, coded.Message)
	}
}

// u36Parked runs fn in a goroutine and fails the test unless it is waiting in the hold.
func u36Parked(t *testing.T, barrier *u36Holder, fn func() (Document, error)) (<-chan struct{}, *Document, *error) {
	t.Helper()
	done := make(chan struct{})
	var doc Document
	var err error
	go func() {
		defer close(done)
		doc, err = fn()
	}()
	select {
	case <-barrier.parked:
	case <-done:
		t.Fatalf("the read returned instead of waiting in the hold: doc %q, err %v", doc.Body, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the read neither waited nor returned")
	}
	select {
	case <-done:
		t.Fatalf("the read returned while the hold was open: doc %q, err %v", doc.Body, err)
	default:
	}
	return done, &doc, &err
}

func TestKeyedReadIsAnsweredByTheOwner(t *testing.T) {
	world := newU36World(t)
	world.fx.mustWrite(world.slot, "p1")

	doc, err := world.get("p1")
	if err != nil {
		t.Fatalf("Get at the owner: %v", err)
	}
	if doc.ServedBy != "n1" || !strings.Contains(doc.Render(), `_id_: "p1"`) || !strings.Contains(doc.Render(), `Name: "Ada"`) {
		t.Fatalf("the owner's answer is %q served by %q", doc.Render(), doc.ServedBy)
	}
	if world.fwd.count() != 0 {
		t.Fatalf("the owner forwarded %d reads", world.fwd.count())
	}

	// n2 holds a copy of the same Badger contents, but it is not the owner: it must
	// send the read to n1 once, and its own copy must not answer.
	copyTopo := &fakeTopology{committed: testEpoch, self: "n2", members: 3, table: world.fx.topo.table, dbRow: world.fx.topo.dbRow}
	_, copyReader := world.build(copyTopo, &cluster.PairSet{}, world.fx.lease)
	answer, err := copyReader.Get(context.Background(), world.slot, testDB, testBucket, world.encID("p1"))
	if err != nil {
		t.Fatalf("Get at a copy: %v", err)
	}
	if answer.ServedBy != "n1" || world.fwd.count() != 1 {
		t.Fatalf("a copy answered itself: served by %q after %d forwards", answer.ServedBy, world.fwd.count())
	}
	if hop := world.fwd.reqs[0].Hop; hop != 1 {
		t.Fatalf("the forwarded read carries hop %d, want 1: a second reroute would circle", hop)
	}
}

func TestAReadAtTheSecondHopIsRefusedNotOwner(t *testing.T) {
	world := newU36World(t)
	copyTopo := &fakeTopology{committed: testEpoch, self: "n2", members: 3, table: world.fx.topo.table, dbRow: world.fx.topo.dbRow}
	_, copyReader := world.build(copyTopo, &cluster.PairSet{}, world.fx.lease)
	_, err := copyReader.Read(context.Background(), ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("p1"), Hop: 1})
	u36Refusal(t, err, cluster.CodeNotOwner)
	if world.fwd.count() != 0 {
		t.Fatalf("a read already forwarded once was forwarded again")
	}
}

func TestReadDuringABarrierIsNeverServedLocally(t *testing.T) {
	t.Run("sending side", func(t *testing.T) {
		world := newU36World(t)
		world.fx.mustWrite(world.slot, "p1")
		world.fx.setFlags(world.slot, flagSending|flagReplayed|flagStopProved)
		world.barrier.rel = HoldRelease{Owner: "n2"}

		done, doc, err := u36Parked(t, world.barrier, func() (Document, error) { return world.get("p1") })
		// The new owner is n2 from now on: the hold ends and this node is no longer rank 1.
		world.fx.topo.table = cluster.BuildTable(cluster.ClusterState{Epoch: int64(testEpoch), Members: testMembers("n2", "n3")})
		world.fx.setFlags(world.slot, 0)
		close(world.barrier.release)
		<-done
		if *err != nil {
			t.Fatalf("Get after the hold: %v", *err)
		}
		if doc.ServedBy != "n1" || world.fwd.count() != 1 {
			t.Fatalf("after the hold the old owner must forward, not answer: served by %q, %d forwards", doc.ServedBy, world.fwd.count())
		}
		if len(doc.Body) != 0 {
			t.Fatalf("the old owner returned its own bytes %x", doc.Body)
		}
	})

	t.Run("awaiting side", func(t *testing.T) {
		world := newU36World(t)
		world.fx.mustWrite(world.slot, "p1")
		world.fx.setFlags(world.slot, flagAwaiting|flagReplayed)
		done, doc, err := u36Parked(t, world.barrier, func() (Document, error) { return world.get("p1") })
		world.fx.setFlags(world.slot, flagReplayed|flagStopProved)
		close(world.barrier.release)
		<-done
		if *err != nil || doc.ServedBy != "n1" || len(doc.Body) == 0 {
			t.Fatalf("after READY the new owner answers: doc %x by %q, err %v", doc.Body, doc.ServedBy, *err)
		}
		if world.fwd.count() != 0 {
			t.Fatalf("the awaiting side forwarded a read it was holding")
		}
	})
}

// u36Insert writes a document straight into Badger the way the applier does, with a
// store field whose state key says count elements and whose elements are the first held.
func (w *u36World) insertWithStore(id string, count uint64, held int) {
	w.t.Helper()
	encID := w.encID(id)
	batch := w.fx.kv.NewBatch()
	defer batch.Discard()
	ref := storage.BucketRef{Slot: w.slot, DatabaseID: testDB, BucketID: testBucket}
	idValue := append([]byte{0x02, byte(len(id) + 1), 0, 0, 0}, append([]byte(id), 0)...)
	insert := store.DocumentInsert{Ref: ref, EncID: encID, Header: idValue,
		Fields: []store.DocumentField{{FieldID: 3, Stored: []byte{0x10, 7, 0, 0, 0}}}}
	if err := store.WriteInsert(batch, insert); err != nil {
		w.t.Fatalf("WriteInsert: %v", err)
	}
	if count > 0 || held > 0 {
		prefix := store.StorePrefix(w.slot, testDB, testBucket, encID, 4)
		if err := batch.Set(store.FieldStateKey(prefix), store.EncodeFieldInstanceState(store.FieldInstanceState{Count: count})); err != nil {
			w.t.Fatalf("Set state: %v", err)
		}
		for element := 0; element < held; element++ {
			if err := batch.Set(store.FieldElementKey(prefix, 0x01, store.PositionRemainder(uint64(element+1))), []byte("x")); err != nil {
				w.t.Fatalf("Set element: %v", err)
			}
		}
	}
	if err := batch.Commit(false); err != nil {
		w.t.Fatalf("Commit: %v", err)
	}
}

// withStoreSchema declares Name (2), Age (3) and Posts (4, fifoStore) plus Tags (5, hashStore).
func (w *u36World) withStoreSchema() {
	w.fx.schema.decl.Fields = machine.FieldTable{NextFieldID: 6, Entries: []machine.FieldEntry{
		{ID: 1, Type: "any", Name: "_id_"}, {ID: 2, Type: "string", Name: "Name"}, {ID: 3, Type: "int", Name: "Age"},
		{ID: 4, Type: "fifoStore", Name: "Posts"}, {ID: 5, Type: "hashStore", Name: "Tags"},
	}}
}

func TestDocumentRenderingShowsStoreMarkers(t *testing.T) {
	cases := []struct {
		name        string
		count       uint64
		held        int
		wantMarker  string
		wantOmitted bool
	}{
		// The state key says 3 although only one element key survives a partial sweep.
		{"count comes from the state key", 3, 1, "Posts: fifoStore (3 elements)", false},
		{"one element is singular", 1, 5, "Posts: fifoStore (1 element)", false},
		{"a store that emptied prints zero", 0, 2, "Posts: fifoStore (0 elements)", false},
		{"a store never written is omitted", 0, 0, "Posts", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world := newU36World(t)
			world.withStoreSchema()
			world.insertWithStore("p1", tc.count, tc.held)
			doc, err := world.get("p1")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			text := doc.Render()
			if tc.wantOmitted {
				if strings.Contains(text, tc.wantMarker) {
					t.Fatalf("an unwritten store is printed: %q", text)
				}
			} else if !strings.Contains(text, tc.wantMarker) {
				t.Fatalf("rendering %q lacks %q", text, tc.wantMarker)
			}
			if !strings.HasPrefix(text, `_id_: "p1"`) {
				t.Fatalf("_id_ is not first: %q", text)
			}
			if strings.Contains(text, "Name") {
				t.Fatalf("an unset scalar is printed: %q", text)
			}
			if !strings.Contains(text, "Age: 7") {
				t.Fatalf("a set scalar is missing: %q", text)
			}
		})
	}
}

func TestRenderingIsOneFunctionForGetAndList(t *testing.T) {
	world := newU36World(t)
	world.withStoreSchema()
	world.insertWithStore("p1", 2, 2)
	doc, err := world.get("p1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	gate := NewReadGate(ReadGateConfig{Topology: world.fx.topo, Pairs: world.fx.pairs, Clock: fixedClock{}, Lease: world.fx.lease, Schema: world.fx.schema, Slots: world.states})
	scanner := NewSlotScanner(SlotScannerConfig{KV: world.fx.kv, Topology: world.fx.topo, Schema: world.fx.schema, Gate: gate})
	var mask cluster.SlotMask
	mask.Set(world.slot)
	reply, err := scanner.List(context.Background(), cluster.SlotListRequest{Epoch: testEpoch, DBID: testDB, BucketID: testBucket, Mask: mask, Want: 10, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(reply.Entries) != 1 {
		t.Fatalf("list returned %d entries", len(reply.Entries))
	}
	// The list entry's body comes from the scanner's own path; it must be the very bytes
	// get returned, and the one renderer must print them alike under the declaration
	// the schema cache holds.
	listedBody := reply.Entries[0].Body
	if !bytes.Equal(listedBody, doc.Body) {
		t.Fatalf("list built %x and get built %x", listedBody, doc.Body)
	}
	decl := world.fx.schema.View(testDB, testBucket).Decl
	if got, want := RenderDocument(Document{Body: listedBody}, decl), doc.Render(); got != want || !strings.Contains(got, "Posts: fifoStore (2 elements)") {
		t.Fatalf("list renders %q and get renders %q", got, want)
	}
}

func TestRankOneHoldsBeforeTheBarrierOnTheRealPath(t *testing.T) {
	cases := []struct {
		name  string
		setup func(w *u36World)
	}{
		{"log tail not replayed", func(w *u36World) { w.fx.setFlags(w.slot, flagStopProved) }},
		{"replay needed", func(w *u36World) { w.fx.setFlags(w.slot, flagReplayed|flagStopProved|flagReplayNeeded) }},
		{"predecessor not proved stopped", func(w *u36World) { w.fx.setFlags(w.slot, flagReplayed) }},
		{"applied position short of the target", func(w *u36World) {
			w.fx.setFlags(w.slot, flagReplayed|flagStopProved)
			w.fx.topo.setTarget(cluster.Position{Epoch: testEpoch, Seq: 9})
		}},
		{"pair still seeding", func(w *u36World) { w.states.set(w.slot, machine.SlotSeeding) }},
		{"pair retired", func(w *u36World) { w.states.set(w.slot, machine.SlotRetired) }},
		{"pair retiring", func(w *u36World) { w.states.set(w.slot, machine.SlotRetiring) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world := newU36World(t)
			world.fx.mustWrite(world.slot, "p1")
			tc.setup(world)
			done, doc, err := u36Parked(t, world.barrier, func() (Document, error) { return world.get("p1") })
			if len(doc.Body) != 0 {
				t.Fatalf("a held pair answered %x", doc.Body)
			}
			// Nothing ends the hold; the caller's deadline would. Release it with the pair still held.
			world.fx.setFlags(world.slot, flagReplayed|flagStopProved)
			world.fx.topo.setTarget(cluster.Position{})
			world.states.set(world.slot, machine.SlotReady)
			close(world.barrier.release)
			<-done
			if *err != nil || len(doc.Body) == 0 {
				t.Fatalf("once the hold ended the owner must answer: %v", *err)
			}
		})
	}

	t.Run("a pair the real write path made current answers", func(t *testing.T) {
		world := newU36World(t)
		world.fx.mustWrite(world.slot, "p1")
		doc, err := world.get("p1")
		if err != nil || len(doc.Body) == 0 {
			t.Fatalf("Get: %v", err)
		}
	})
}

func TestReadRefusalsAndTheOneReAdmission(t *testing.T) {
	t.Run("a dropping bucket", func(t *testing.T) {
		world := newU36World(t)
		world.fx.mustWrite(world.slot, "p1")
		world.fx.schema.decl.State = machine.StateDropping
		_, err := world.get("p1")
		u36Refusal(t, err, cluster.CodeBucketDropping)
	})
	t.Run("a missing bucket", func(t *testing.T) {
		world := newU36World(t)
		gate := NewReadGate(ReadGateConfig{Topology: world.fx.topo, Pairs: world.fx.pairs, Clock: fixedClock{}, Lease: world.fx.lease, Schema: u36MissingSchema{}, Slots: world.states})
		reader := NewReader(ReaderConfig{Gate: gate, KV: world.fx.kv, Holder: world.barrier, PeerReachable: func(uint16, uint64) bool { return true }, Forward: world.fwd, Self: "n1"})
		_, err := reader.Get(context.Background(), world.slot, testDB, testBucket, world.encID("p1"))
		u36Refusal(t, err, cluster.CodeNotFound)
		if !strings.Contains(err.Error(), "bucket") {
			t.Fatalf("the refusal is not the bucket text: %s", err)
		}
	})
	t.Run("a stale client epoch", func(t *testing.T) {
		world := newU36World(t)
		_, err := world.reader.Read(context.Background(), ReadRequest{Slot: world.slot, DBID: testDB, BucketID: testBucket, EncID: world.encID("p1"), ClientEpoch: testEpoch - 1})
		u36Refusal(t, err, cluster.CodeStaleEpoch)
	})
	t.Run("a bump that left the bucket alone is re-read once", func(t *testing.T) {
		world := newU36World(t)
		world.fx.mustWrite(world.slot, "p1")
		world.fx.schema.published = testSeen + 1
		before := world.fx.schema.refetchCount()
		if _, err := world.get("p1"); err != nil {
			t.Fatalf("Get after a bump of another bucket: %v", err)
		}
		if got := world.fx.schema.refetchCount() - before; got != 1 {
			t.Fatalf("%d refetches, want exactly the one re-admission", got)
		}
	})
	t.Run("a second miss is the refusal", func(t *testing.T) {
		world := newU36World(t)
		world.fx.mustWrite(world.slot, "p1")
		world.fx.schema.published = testSeen + 1
		world.fx.schema.staleAfter = true
		_, err := world.get("p1")
		u36Refusal(t, err, cluster.CodeSchemaNotCurrent)
		if got := world.fx.schema.refetchCount(); got != 1 {
			t.Fatalf("%d refetches, want one", got)
		}
	})
	t.Run("an absent document", func(t *testing.T) {
		world := newU36World(t)
		_, err := world.get("nobody")
		u36Refusal(t, err, cluster.CodeNotFound)
		if !strings.Contains(err.Error(), `document "`) {
			t.Fatalf("the refusal is not the document text: %s", err)
		}
	})
	t.Run("a read takes no position", func(t *testing.T) {
		world := newU36World(t)
		world.fx.mustWrite(world.slot, "p1")
		before := world.fx.pairs.Snapshot(world.slot, testDB)
		logged := len(world.fx.logged())
		if _, err := world.get("p1"); err != nil {
			t.Fatalf("Get: %v", err)
		}
		if after := world.fx.pairs.Snapshot(world.slot, testDB); after != before {
			t.Fatalf("a read changed the pair: %+v to %+v", before, after)
		}
		if len(world.fx.logged()) != logged {
			t.Fatal("a read appended to the log")
		}
	})
}

func TestLeaseIsRecheckedImmediatelyBeforeTheAnswer(t *testing.T) {
	t.Run("lapsed at admission", func(t *testing.T) {
		world := newU36World(t)
		world.fx.mustWrite(world.slot, "p1")
		world.fx.lease.valid.Store(false)
		_, err := world.get("p1")
		u36Refusal(t, err, cluster.CodeLeaseLapsed)
	})
	t.Run("lapses while the document is being read", func(t *testing.T) {
		world := newU36World(t)
		world.fx.mustWrite(world.slot, "p1")
		lease := &flakyLease{validFor: 1}
		_, world.reader = world.build(world.fx.topo, world.fx.pairs, lease)
		doc, err := world.get("p1")
		u36Refusal(t, err, cluster.CodeLeaseLapsed)
		if len(doc.Body) != 0 {
			t.Fatalf("a lapsed node answered %x", doc.Body)
		}
	})
	t.Run("a lapsed forwarder names no owner", func(t *testing.T) {
		world := newU36World(t)
		copyTopo := &fakeTopology{committed: testEpoch, self: "n2", members: 3, table: world.fx.topo.table, dbRow: world.fx.topo.dbRow}
		_, copyReader := world.build(copyTopo, &cluster.PairSet{}, &flakyLease{})
		_, err := copyReader.Get(context.Background(), world.slot, testDB, testBucket, world.encID("p1"))
		u36Refusal(t, err, cluster.CodeLeaseLapsed)
		if world.fwd.count() != 0 {
			t.Fatal("a node without a lease forwarded")
		}
	})
	t.Run("a one-member cluster holds no lease", func(t *testing.T) {
		fx := newFixture(t)
		fx.lease.valid.Store(false)
		world := &u36World{t: t, fx: fx, states: &u36States{state: map[uint16]machine.SlotState{}}, barrier: newU36Holder(), fwd: &u36Forwarder{}, slot: testSlot}
		world.gate, world.reader = world.build(fx.topo, fx.pairs, fx.lease)
		fx.mustWrite(testSlot, "p1")
		if _, err := world.get("p1"); err != nil {
			t.Fatalf("a lone node refused a read: %v", err)
		}
	})
}

func TestScanAdmissionSharesTheReadGate(t *testing.T) {
	scan := func(world *u36World, slots ...uint16) error {
		var mask cluster.SlotMask
		for _, slot := range slots {
			mask.Set(slot)
		}
		return world.gate.AdmitMask(context.Background(), testDB, testBucket, &mask)
	}
	t.Run("an owned, ready slot is admitted", func(t *testing.T) {
		world := newU36World(t)
		if err := scan(world, world.slot); err != nil {
			t.Fatalf("AdmitMask: %v", err)
		}
	})
	t.Run("a slot not ready is refused", func(t *testing.T) {
		world := newU36World(t)
		world.states.set(world.slot, machine.SlotSeeding)
		u36Refusal(t, scan(world, world.slot), cluster.CodeUnavailable)
	})
	t.Run("a held pair is refused", func(t *testing.T) {
		world := newU36World(t)
		world.fx.setFlags(world.slot, flagSending|flagReplayed|flagStopProved)
		u36Refusal(t, scan(world, world.slot), cluster.CodeUnavailable)
	})
	t.Run("a lapsed lease is refused", func(t *testing.T) {
		world := newU36World(t)
		world.fx.lease.valid.Store(false)
		u36Refusal(t, scan(world, world.slot), cluster.CodeLeaseLapsed)
	})
	t.Run("a dropping bucket is refused", func(t *testing.T) {
		world := newU36World(t)
		world.fx.schema.decl.State = machine.StateDropping
		u36Refusal(t, scan(world, world.slot), cluster.CodeBucketDropping)
	})
	t.Run("a slot owned by another node is refused", func(t *testing.T) {
		world := newU36World(t)
		var foreign uint16
		for slot := uint16(0); slot < storage.SlotCount; slot++ {
			if owner, ok := world.fx.topo.table.Owner(slot, 2); ok && owner.ID != "n1" {
				foreign = slot
				break
			}
		}
		u36Refusal(t, scan(world, foreign), cluster.CodeNotOwner)
	})
}

func TestRenderDocumentSurvivesMutation(t *testing.T) {
	decl := machine.Declaration{Fields: machine.FieldTable{Entries: []machine.FieldEntry{
		{ID: 1, Type: "any", Name: "_id_"}, {ID: 2, Type: "string", Name: "Name"}, {ID: 4, Type: "fifoStore", Name: "Posts"},
	}}}
	seed := []byte{}
	elements := []byte{0x02, '_', 'i', 'd', '_', 0, 3, 0, 0, 0, 'p', '1', 0}
	elements = append(elements, 0x02, 'P', 'o', 's', 't', 's', 0, 23, 0, 0, 0)
	elements = append(elements, []byte("fifoStore (3 elements)")...)
	elements = append(elements, 0)
	total := uint32(len(elements) + 5)
	seed = append(seed, byte(total), byte(total>>8), byte(total>>16), byte(total>>24))
	seed = append(append(seed, elements...), 0)
	if text := RenderDocument(Document{Body: seed}, decl); !strings.Contains(text, `_id_: "p1"`) || !strings.Contains(text, "Posts: fifoStore (3 elements)") {
		t.Fatalf("the seed renders as %q", text)
	}

	random := rand.New(rand.NewSource(36))
	for round := 0; round < 100000; round++ {
		body := append([]byte(nil), seed...)
		for edits := 1 + random.Intn(4); edits > 0; edits-- {
			switch random.Intn(3) {
			case 0:
				body[random.Intn(len(body))] = byte(random.Intn(256))
			case 1:
				if len(body) > 1 {
					at := random.Intn(len(body))
					body = append(body[:at], body[at+1:]...)
				}
			default:
				at := random.Intn(len(body))
				body = append(body[:at], append([]byte{byte(random.Intn(256))}, body[at:]...)...)
			}
		}
		if text := RenderDocument(Document{Body: body}, decl); len(text) > 16*len(body)+256 {
			t.Fatalf("a %d-byte body rendered %d bytes", len(body), len(text))
		}
	}
}

// u36MissingSchema is a cache whose declaration fetch found no such bucket.
type u36MissingSchema struct{}

func (u36MissingSchema) View(uint64, uint64) SchemaView {
	return SchemaView{Present: true, Missing: true}
}
func (u36MissingSchema) Refetch(context.Context, uint64, uint64) error { return nil }

// u36Bounded runs a read and fails the test instead of hanging when it does not return.
func u36Bounded(t *testing.T, fn func() (Document, error)) (Document, error) {
	t.Helper()
	type outcome struct {
		doc Document
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		doc, err := fn()
		done <- outcome{doc, err}
	}()
	select {
	case got := <-done:
		return got.doc, got.err
	case <-time.After(5 * time.Second):
		t.Fatal("the read did not return: it is looping")
		return Document{}, nil
	}
}

func TestAReadNeverLoopsOnAMovingDeclaration(t *testing.T) {
	t.Run("a refetch that never advances", func(t *testing.T) {
		world := newU36World(t)
		world.fx.mustWrite(world.slot, "p1")
		world.fx.schema.published = testSeen + 1
		world.fx.schema.staleAfter = true
		base := world.fx.schema.viewCount()
		_, err := u36Bounded(t, func() (Document, error) { return world.get("p1") })
		u36Refusal(t, err, cluster.CodeSchemaNotCurrent)
		// Three for the admission and its confirmations, one more for the names in the refusal text (BUG-0748).
		if views := world.fx.schema.viewCount() - base; views > 4 {
			t.Fatalf("%d declaration reads for one refused read, want at most 4", views)
		}
	})
	t.Run("a bump lands between admission and the confirmation on every pass", func(t *testing.T) {
		world := newU36World(t)
		world.fx.mustWrite(world.slot, "p1")
		base := world.fx.schema.viewCount()
		world.fx.schema.beforeView = func(n int, s *fakeSchema) {
			if n >= base+2 {
				s.published++
			}
		}
		_, err := u36Bounded(t, func() (Document, error) { return world.get("p1") })
		u36Refusal(t, err, cluster.CodeSchemaNotCurrent)
		// Three for the admission and its confirmations, one more for the names in the refusal text (BUG-0748).
		if views := world.fx.schema.viewCount() - base; views > 4 {
			t.Fatalf("%d declaration reads for one refused read, want at most 4", views)
		}
		if refetches := world.fx.schema.refetchCount(); refetches > 1 {
			t.Fatalf("%d refetches, check 4 allows the one", refetches)
		}
	})
}

func TestConfirmReflectsTheNewestState(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(w *u36World)
	}{
		{"a barrier opens", func(w *u36World) { w.fx.setFlags(w.slot, flagSending|flagReplayed|flagStopProved) }},
		{"ownership moves", func(w *u36World) {
			w.fx.topo.table = cluster.BuildTable(cluster.ClusterState{Epoch: int64(testEpoch), Members: testMembers("n2", "n3")})
		}},
		{"the pair starts seeding", func(w *u36World) { w.states.set(w.slot, machine.SlotSeeding) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world := newU36World(t)
			world.fx.mustWrite(world.slot, "p1")
			world.barrier.rel = HoldRelease{Owner: "n2"}
			close(world.barrier.release)
			// The mutation lands inside the confirmation's declaration read.
			base := world.fx.schema.viewCount()
			world.fx.schema.beforeView = func(n int, _ *fakeSchema) {
				if n == base+2 {
					tc.mutate(world)
				}
			}
			doc, err := u36Bounded(t, func() (Document, error) { return world.get("p1") })
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if len(doc.Body) != 0 {
				t.Fatalf("a pair that changed under the read returned its body %x", doc.Body)
			}
			if world.fwd.count() != 1 {
				t.Fatalf("%d forwards, want the read handed to the owner once", world.fwd.count())
			}
		})
	}
}

// u36Handoff is a node of the real barrier wiring (U33) with a Reader over it.
type u36Handoff struct {
	node   *u33Node
	fx     *fixture
	fwd    *u36Forwarder
	holder *u36CountingHolder
	reader *Reader
	slot   uint16
	encID  []byte
	topo   *u36HandoffTopo
	states *u36States
}

type u36CountingHolder struct {
	inner *Handoff
	calls atomic.Int64
}

func (h *u36CountingHolder) Hold(ctx context.Context, req HeldRequest) (HoldRelease, error) {
	h.calls.Add(1)
	return h.inner.Hold(ctx, req)
}

type u36HandoffTopo struct {
	self   string
	table  *cluster.SlotTable
	target cluster.Position
	name   string
}

func (t *u36HandoffTopo) Committed() uint64                      { return 2 }
func (t *u36HandoffTopo) Self() string                           { return t.self }
func (t *u36HandoffTopo) Members() int                           { return 2 }
func (t *u36HandoffTopo) Table() *cluster.SlotTable              { return t.table }
func (t *u36HandoffTopo) Target(uint16, uint64) cluster.Position { return t.target }
func (t *u36HandoffTopo) Database(db uint64) (DatabaseInfo, bool) {
	return DatabaseInfo{Name: t.name, RF: 2}, db == u33DB
}

func newU36Handoff(t *testing.T, node *u33Node, slot uint16, peerReachable bool) *u36Handoff {
	t.Helper()
	fx := newFixture(t)
	encID, err := store.EncodeComponent("p1")
	if err != nil {
		t.Fatalf("EncodeComponent: %v", err)
	}
	batch := fx.kv.NewBatch()
	defer batch.Discard()
	insert := store.DocumentInsert{Ref: storage.BucketRef{Slot: slot, DatabaseID: u33DB, BucketID: testBucket}, EncID: encID,
		Header: []byte{0x02, 3, 0, 0, 0, 'p', '1', 0}}
	if err := store.WriteInsert(batch, insert); err != nil {
		t.Fatalf("WriteInsert: %v", err)
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// The write path would have replayed the pair; the Handoff owns only the hold bits.
	node.pairs.With(slot, u33DB, func(p *cluster.Pair) { p.Flags |= flagReplayed })
	table := cluster.BuildTable(u33State(2, 2, u33Active("A", "B")))
	topo := &u36HandoffTopo{self: node.id, table: table}
	states := &u36States{state: map[uint16]machine.SlotState{}}
	gate := NewReadGate(ReadGateConfig{Topology: topo, Pairs: node.pairs, Clock: fixedClock{}, Lease: fx.lease, Schema: fx.schema,
		Slots: states})
	holder := &u36CountingHolder{inner: node.h}
	fwd := &u36Forwarder{}
	reader := NewReader(ReaderConfig{Gate: gate, KV: fx.kv, Holder: holder, PeerReachable: func(uint16, uint64) bool { return peerReachable }, Forward: fwd, Self: node.id})
	return &u36Handoff{node: node, fx: fx, fwd: fwd, holder: holder, reader: reader, slot: slot, encID: encID, topo: topo, states: states}
}

func (h *u36Handoff) start(ctx context.Context) (<-chan Document, <-chan error) {
	docs, errs := make(chan Document, 1), make(chan error, 1)
	go func() {
		doc, err := h.reader.Read(ctx, ReadRequest{Slot: h.slot, DBID: u33DB, BucketID: testBucket, EncID: h.encID, RequestID: [16]byte{9, 9}})
		docs <- doc
		errs <- err
	}()
	return docs, errs
}

// heldLists reports what the machine and the waiter list still hold across the node.
func (h *u36Handoff) heldLists() (held, waiters int) {
	h.node.h.mu.Lock()
	defer h.node.h.mu.Unlock()
	for _, hp := range h.node.h.pairs {
		held += len(hp.barrier.Held)
		waiters += len(hp.waiters)
	}
	return held, waiters
}

func TestAHeldReadIsDeliveredExactlyOnce(t *testing.T) {
	t.Run("released to this node", func(t *testing.T) {
		node, slots := u33Awaiting(t, cluster.Position{})
		h := newU36Handoff(t, node, slots[0], true)
		docs, errs := h.start(context.Background())
		u33WaitParked(t, node, slots[0], 1)
		if held, waiters := h.heldLists(); held != 1 || waiters != 1 {
			t.Fatalf("a parked read is listed %d times and waited on %d times, want once each", held, waiters)
		}
		u33Stopped(t, node, "A", 2, slots[0])
		if err := node.h.MaybeSendReady(context.Background(), slots[0], u33DB); err != nil {
			t.Fatal(err)
		}
		doc, err := <-docs, <-errs
		if err != nil || len(doc.Body) == 0 || doc.ServedBy != "B" {
			t.Fatalf("the new owner's read returned %x by %q, %v", doc.Body, doc.ServedBy, err)
		}
		if h.fwd.count() != 0 || h.holder.calls.Load() != 1 {
			t.Fatalf("answered locally once, but %d forwards and %d holds", h.fwd.count(), h.holder.calls.Load())
		}
		if held, waiters := h.heldLists(); held != 0 || waiters != 0 {
			t.Fatalf("the released read is still listed: %d held, %d waiting", held, waiters)
		}
	})

	t.Run("released to another owner", func(t *testing.T) {
		node, slots := u33Sending(t)
		h := newU36Handoff(t, node, slots[0], true)
		docs, errs := h.start(context.Background())
		u33WaitParked(t, node, slots[0], 1)
		viewsBefore := h.fx.schema.viewCount()
		u33Ready(t, node, "B", 2, cluster.Position{}, slots[0])
		doc, err := <-docs, <-errs
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if len(doc.Body) != 0 || h.fwd.count() != 1 {
			t.Fatalf("the old owner answered itself (%x) or forwarded %d times, want one forward", doc.Body, h.fwd.count())
		}
		if sent := h.fwd.reqs[0]; sent.Hop != 1 || sent.RequestID != [16]byte{9, 9} {
			t.Fatalf("the forward carries hop %d and request id %x, want hop 1 and the client's id", sent.Hop, sent.RequestID)
		}
		if h.fx.schema.viewCount() != viewsBefore {
			t.Fatal("the read admitted itself again after its release: the release is the only decision")
		}
		if held, waiters := h.heldLists(); held != 0 || waiters != 0 {
			t.Fatalf("the released read is still listed: %d held, %d waiting", held, waiters)
		}
	})

	t.Run("the deadline passes first", func(t *testing.T) {
		node, slots := u33Awaiting(t, cluster.Position{})
		h := newU36Handoff(t, node, slots[0], true)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		docs, errs := h.start(ctx)
		doc, err := <-docs, <-errs
		var refusal *cluster.Error
		if !errors.As(err, &refusal) || (refusal.Code != cluster.CodeNotOwner && refusal.Code != cluster.CodeUnavailable) {
			t.Fatalf("a read past its deadline returned %x, %v; want a retryable refusal", doc.Body, err)
		}
		if len(doc.Body) != 0 || h.fwd.count() != 0 {
			t.Fatalf("a refused read still delivered: body %x, %d forwards", doc.Body, h.fwd.count())
		}
		if held, waiters := h.heldLists(); held != 0 || waiters != 0 {
			t.Fatalf("the refused read is still listed: %d held, %d waiting", held, waiters)
		}
	})

	t.Run("a caller that cannot wait for an unreachable old owner", func(t *testing.T) {
		node, slots := u33Awaiting(t, cluster.Position{})
		h := newU36Handoff(t, node, slots[0], false)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		docs, errs := h.start(ctx)
		doc, err := <-docs, <-errs
		u36Refusal(t, err, cluster.CodeNotOwner)
		if len(doc.Body) != 0 || h.fwd.count() != 0 {
			t.Fatalf("a refused read still delivered: body %x, %d forwards", doc.Body, h.fwd.count())
		}
	})
}

// u36Settled is the new owner B with no barrier open for any pair: the Handoff has
// nothing to wait for, and returns "settled" to any read it is asked to hold.
func u36Settled(t *testing.T) (*u33Node, uint16) {
	t.Helper()
	state := u33State(2, 2, u33Active("A", "B"))
	node := newU33Node("B")
	node.h.OnEpoch(state)
	table := cluster.BuildTable(state)
	for slot := uint16(0); slot < storage.SlotCount; slot++ {
		if owner, ok := table.Owner(slot, 2); ok && owner.ID == "B" {
			return node, slot
		}
	}
	t.Fatal("B owns no slot")
	return nil, 0
}

func TestAPairTheGateHoldsButTheHandoffDoesNotKnowIsRetryableNotOwnerless(t *testing.T) {
	cases := []struct {
		name  string
		setup func(h *u36Handoff)
		cause string
	}{
		{"log tail not replayed", func(h *u36Handoff) {
			h.node.pairs.With(h.slot, u33DB, func(p *cluster.Pair) { p.Flags = flagStopProved })
		}, "replayed"},
		{"replay needed", func(h *u36Handoff) {
			h.node.pairs.With(h.slot, u33DB, func(p *cluster.Pair) { p.Flags = flagReplayed | flagStopProved | flagReplayNeeded })
		}, "replay"},
		{"applied position short of the target", func(h *u36Handoff) {
			h.topo.target = cluster.Position{Epoch: 2, Seq: 9}
			h.node.pairs.With(h.slot, u33DB, func(p *cluster.Pair) { p.Flags = flagReplayed | flagStopProved })
		}, "catching up"},
		{"seeding", func(h *u36Handoff) { h.states.set(h.slot, machine.SlotSeeding) }, "seeding"},
		{"retired", func(h *u36Handoff) { h.states.set(h.slot, machine.SlotRetired) }, "retired"},
		{"retiring", func(h *u36Handoff) { h.states.set(h.slot, machine.SlotRetiring) }, "retiring"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node, slot := u36Settled(t)
			h := newU36Handoff(t, node, slot, true)
			h.node.pairs.With(slot, u33DB, func(p *cluster.Pair) { p.Flags = flagReplayed | flagStopProved })
			tc.setup(h)
			docs, errs := h.start(context.Background())
			doc, err := <-docs, <-errs
			u36Refusal(t, err, cluster.CodeUnavailable)
			if !strings.Contains(err.Error(), tc.cause) || !strings.Contains(err.Error(), "Retry") {
				t.Fatalf("the refusal must name %q and say to retry: %s", tc.cause, err)
			}
			if len(doc.Body) != 0 || h.fwd.count() != 0 {
				t.Fatalf("a held pair delivered: body %x, %d forwards", doc.Body, h.fwd.count())
			}
		})
	}
}

func TestAReleaseWithNoOwnerIsRefusedNotForwarded(t *testing.T) {
	node, slots := u33Awaiting(t, cluster.Position{})
	h := newU36Handoff(t, node, slots[0], true)
	docs, errs := h.start(context.Background())
	u33WaitParked(t, node, slots[0], 1)
	// Every member is down at the next epoch: the hold ends and nobody owns the slot.
	allDown := u33State(3, 2, []cluster.Member{u33Member("A", cluster.MemberDown), u33Member("B", cluster.MemberDown)})
	node.h.OnEpoch(allDown)
	doc, err := <-docs, <-errs
	u36Refusal(t, err, cluster.CodeUnavailable)
	if len(doc.Body) != 0 || h.fwd.count() != 0 {
		t.Fatalf("a read with no owner delivered: body %x, %d forwards", doc.Body, h.fwd.count())
	}
}

func TestOneReAdmissionPerReadNotPerAdmit(t *testing.T) {
	world := newU36World(t)
	world.fx.mustWrite(world.slot, "p1")
	base := world.fx.schema.viewCount()
	// A bump lands before the admission and again before the confirmation. The read has
	// the one refetch of check 4 and has spent it by then.
	world.fx.schema.beforeView = func(n int, s *fakeSchema) {
		if n == base+1 || n == base+3 {
			s.published++
		}
	}
	_, err := u36Bounded(t, func() (Document, error) { return world.get("p1") })
	u36Refusal(t, err, cluster.CodeSchemaNotCurrent)
	if refetches := world.fx.schema.refetchCount(); refetches > 1 {
		t.Fatalf("%d refetches for one read, check 4 allows the one", refetches)
	}
	// Three for the admission and its confirmations, one more for the names in the refusal text (BUG-0748).
	if views := world.fx.schema.viewCount() - base; views > 4 {
		t.Fatalf("%d declaration reads for one refused read, want at most 4", views)
	}
}

// u36ChurningSchema reports an effective declaration version that is different on every
// read, never ahead of the published one, so no fence refuses it and only the reader's
// own bound can stop a read that keeps seeing the declaration move.
type u36ChurningSchema struct {
	decl    machine.Declaration
	reads   atomic.Int64
	refetch atomic.Int64
}

func (c *u36ChurningSchema) View(uint64, uint64) SchemaView {
	decl := c.decl
	decl.Version = 100 + c.reads.Add(1)
	return SchemaView{Present: true, Decl: decl, Seen: 1_000_000, Published: 1_000_000}
}

func (c *u36ChurningSchema) Refetch(context.Context, uint64, uint64) error {
	c.refetch.Add(1)
	return nil
}

// u36SnapshotCounter counts the document loads, one snapshot each.
type u36SnapshotCounter struct {
	storage.KV
	loads atomic.Int64
}

func (k *u36SnapshotCounter) NewSnapshot() storage.KVSnapshot {
	k.loads.Add(1)
	return k.KV.NewSnapshot()
}

func TestAReadThatSeesTheDeclarationMoveEveryTimeIsRefusedAfterTwoLoads(t *testing.T) {
	world := newU36World(t)
	world.fx.mustWrite(world.slot, "p1")
	churn := &u36ChurningSchema{decl: world.fx.schema.decl}
	kv := &u36SnapshotCounter{KV: world.fx.kv}
	gate := NewReadGate(ReadGateConfig{Topology: world.fx.topo, Pairs: world.fx.pairs, Clock: fixedClock{}, Lease: world.fx.lease, Schema: churn, Slots: world.states})
	reader := NewReader(ReaderConfig{Gate: gate, KV: kv, Holder: world.barrier, PeerReachable: func(uint16, uint64) bool { return true }, Forward: world.fwd, Self: "n1"})
	_, err := u36Bounded(t, func() (Document, error) {
		return reader.Get(context.Background(), world.slot, testDB, testBucket, world.encID("p1"))
	})
	u36Refusal(t, err, cluster.CodeSchemaNotCurrent)
	if loads := kv.loads.Load(); loads > 2 {
		t.Fatalf("%d document loads for one refused read, want at most 2", loads)
	}
	if churn.refetch.Load() != 0 {
		t.Fatalf("%d refetches although no read was ever behind the published version", churn.refetch.Load())
	}
}

// TestNewReaderDefaultsNearestMaxElements guards the zero-value default: a zero
// ceiling would reach the store as MaxElements 0, which nearest refuses as an
// invalid request, so a node built without the setting must take 100000.
func TestNewReaderDefaultsNearestMaxElements(t *testing.T) {
	if got := NewReader(ReaderConfig{}).cfg.NearestMaxElements; got != 100_000 {
		t.Errorf("NewReader(zero).cfg.NearestMaxElements = %d, want 100000", got)
	}
}
