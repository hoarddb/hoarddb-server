package wire

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// reportNode is the node name every reporter test runs as.
const reportNode = "n1"

// reportSend is one proposal the reporter sent, decoded from its body.
type reportSend struct {
	kind byte
	node string
	db   int64
	bits []byte
}

// reportRecord is one seed record a test put on the fake node's disk.
type reportRecord struct {
	slot  uint16
	db    uint64
	state uint8
}

// reportWorld is the node a reporter runs against: committed state, the records
// on disk, a clock, and every proposal the reporter sent.
type reportWorld struct {
	mu        sync.Mutex
	now       time.Time
	committed cluster.ClusterState
	records   map[reconcileKey]uint8
	sends     []reportSend
	errs      []error
	calls     int
	// sendFn returns what the call-th proposal gets back; nil accepts every proposal.
	sendFn func(call int) error
}

// newReportWorld returns a node whose committed state holds the given databases and
// no records and no bits.
func newReportWorld(databases ...int64) *reportWorld {
	w := &reportWorld{now: time.Unix(1_700_000_000, 0), records: make(map[reconcileKey]uint8)}
	for _, id := range databases {
		w.committed.Databases = append(w.committed.Databases, cluster.Database{ID: id, RF: 2})
	}
	return w
}

// reporter returns a reporter wired to the fake node.
func (w *reportWorld) reporter() *seedReporter {
	return newSeedReporter(seedReportDeps{
		Self: reportNode,
		Committed: func() cluster.ClusterState {
			w.mu.Lock()
			defer w.mu.Unlock()
			return w.committed
		},
		Record: func(slot uint16, db uint64) (uint8, bool) {
			w.mu.Lock()
			defer w.mu.Unlock()
			state, ok := w.records[reconcileKey{slot, db}]
			return state, ok
		},
		Records: func(visit func(slot uint16, db uint64, state uint8)) {
			for _, rec := range w.recordList() {
				visit(rec.slot, rec.db, rec.state)
			}
		},
		Send: func(_ context.Context, kind byte, body []byte) error {
			return w.send(kind, body)
		},
		OnError: func(err error) {
			w.mu.Lock()
			defer w.mu.Unlock()
			w.errs = append(w.errs, err)
		},
		Now: func() time.Time {
			w.mu.Lock()
			defer w.mu.Unlock()
			return w.now
		},
	})
}

// recordList copies the records so a visit runs without the world's lock.
func (w *reportWorld) recordList() []reportRecord {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]reportRecord, 0, len(w.records))
	for key, state := range w.records {
		out = append(out, reportRecord{slot: key.slot, db: key.db, state: state})
	}
	return out
}

// send records the proposal, then asks sendFn what it returns. sendFn runs without the lock, so
// it may block.
func (w *reportWorld) send(kind byte, body []byte) error {
	doc := bsoncore.Document(body)
	_, bits := doc.Lookup("bits").Binary()
	w.mu.Lock()
	w.calls++
	call := w.calls
	w.sends = append(w.sends, reportSend{
		kind: kind,
		node: doc.Lookup("node").StringValue(),
		db:   doc.Lookup("db_id").Int64(),
		bits: slices.Clone(bits),
	})
	fn := w.sendFn
	w.mu.Unlock()
	if fn != nil {
		return fn(call)
	}
	return nil
}

// proposals returns the proposals of one kind, in the order they were sent.
func (w *reportWorld) proposals(kind byte) []reportSend {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []reportSend
	for _, s := range w.sends {
		if s.kind == kind {
			out = append(out, s)
		}
	}
	return out
}

// allSends returns every proposal sent so far.
func (w *reportWorld) allSends() []reportSend {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.sends)
}

// setRecord puts a seed record in state on the fake disk.
func (w *reportWorld) setRecord(db uint64, slot uint16, state uint8) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.records[reconcileKey{slot, db}] = state
}

// setCommittedBit sets one bit of this node's committed row for db.
func (w *reportWorld) setCommittedBit(t *testing.T, db uint64, slot uint16) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := range w.committed.SeedBitmaps {
		if w.committed.SeedBitmaps[i].Node == reportNode && w.committed.SeedBitmaps[i].DBID == int64(db) {
			if err := machine.Set(w.committed.SeedBitmaps[i].Bits, slot); err != nil {
				t.Fatalf("setting committed bit %d: %v", slot, err)
			}
			return
		}
	}
	bits := make([]byte, machine.BitmapBytes)
	if err := machine.Set(bits, slot); err != nil {
		t.Fatalf("setting committed bit %d: %v", slot, err)
	}
	w.committed.SeedBitmaps = append(w.committed.SeedBitmaps, cluster.SeedBitmap{Node: reportNode, DBID: int64(db), Bits: bits})
}

// advance moves the fake clock forward.
func (w *reportWorld) advance(d time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.now = w.now.Add(d)
}

// setSlots lists the set bits of a bitmap, ascending.
func setSlots(t *testing.T, bits []byte) []uint16 {
	t.Helper()
	var out []uint16
	for slot := range uint16(machine.BitmapBytes * 8) {
		on, err := machine.Bit(bits, slot)
		if err != nil {
			t.Fatalf("reading bit %d: %v", slot, err)
		}
		if on {
			out = append(out, slot)
		}
	}
	return out
}

func TestTheReporterRederivesBitsFromTheRecords(t *testing.T) {
	w := newReportWorld(1)
	r := w.reporter()
	for _, slot := range []uint16{5, 6, 7} {
		w.setRecord(1, slot, machine.SeedReady)
		r.Note(1, slot, false)
	}
	w.setRecord(1, 6, machine.SeedSeeding) // the record moved after the note
	w.advance(reportInterval)
	r.Flush(context.Background())
	seeded := w.proposals(cluster.CommandSeeded)
	if len(seeded) != 1 {
		t.Fatalf("%d SEEDED proposals, want 1", len(seeded))
	}
	if got := setSlots(t, seeded[0].bits); !slices.Equal(got, []uint16{5, 7}) {
		t.Fatalf("SEEDED carries bits %v, want [5 7]: slot 6 is seeding at send time", got)
	}
}

func TestTheReporterSendsNothingForAGoneDatabase(t *testing.T) {
	w := newReportWorld(1) // database 2 is not in the committed state
	w.setRecord(2, 4, machine.SeedReady)
	w.setRecord(2, 9, machine.SeedSeeding)
	w.setCommittedBit(t, 2, 9)
	r := w.reporter()
	r.Reconcile()
	r.Note(2, 4, false)
	w.advance(reportInterval)
	r.Flush(context.Background())
	if sends := w.allSends(); len(sends) != 0 {
		t.Fatalf("%d proposals for database 2, which is gone; want none", len(sends))
	}
}

func TestTheReporterDerivesBothDirectionsAtBoot(t *testing.T) {
	w := newReportWorld(1)
	w.setRecord(1, 1, machine.SeedReady)   // ready with committed bit 0: report set
	w.setRecord(1, 2, machine.SeedSeeding) // seeding with committed bit 1: report clear
	w.setCommittedBit(t, 1, 2)
	r := w.reporter()
	r.Reconcile()
	w.advance(reportInterval)
	r.Flush(context.Background())
	seeded := w.proposals(cluster.CommandSeeded)
	if len(seeded) != 1 || !slices.Equal(setSlots(t, seeded[0].bits), []uint16{1}) {
		t.Fatalf("SEEDED proposals %+v, want one carrying slot 1", seeded)
	}
	retired := w.proposals(cluster.CommandRetire)
	if len(retired) != 1 || !slices.Equal(setSlots(t, retired[0].bits), []uint16{2}) {
		t.Fatalf("RETIRE proposals %+v, want one carrying slot 2", retired)
	}
}

func TestClearSendsOnlyWhenTheBitIsSet(t *testing.T) {
	w := newReportWorld(1)
	r := w.reporter()
	if err := r.Clear(context.Background(), 1, 9); err != nil {
		t.Fatalf("Clear with the bit at 0: %v", err)
	}
	if sends := w.allSends(); len(sends) != 0 {
		t.Fatalf("Clear sent %d proposals with the committed bit at 0; want none", len(sends))
	}
	w.setCommittedBit(t, 1, 9)
	if err := r.Clear(context.Background(), 1, 9); err != nil {
		t.Fatalf("Clear with the bit at 1: %v", err)
	}
	retired := w.allSends()
	if len(retired) != 1 || retired[0].kind != cluster.CommandRetire || !slices.Equal(setSlots(t, retired[0].bits), []uint16{9}) {
		t.Fatalf("Clear with the bit at 1 sent %+v, want one RETIRE carrying slot 9", retired)
	}
}

func TestClearWaitsForABatchInFlight(t *testing.T) {
	w := newReportWorld(1)
	w.setRecord(1, 3, machine.SeedReady)
	w.setCommittedBit(t, 1, 9)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	w.sendFn = func(call int) error {
		if call == 1 {
			entered <- struct{}{}
			<-release
		}
		return nil
	}
	r := w.reporter()
	r.Note(1, 3, false)
	w.advance(reportInterval)
	flushDone := make(chan struct{})
	go func() {
		r.Flush(context.Background())
		close(flushDone)
	}()
	<-entered // the SEEDED batch for slot 3 is in flight
	clearDone := make(chan error, 1)
	go func() {
		clearDone <- r.Clear(context.Background(), 1, 9)
	}()
	time.Sleep(100 * time.Millisecond)
	if sends := w.allSends(); len(sends) != 1 {
		t.Fatalf("%d proposals while the batch is in flight, want 1: Clear must wait for it", len(sends))
	}
	close(release)
	<-flushDone
	if err := <-clearDone; err != nil {
		t.Fatalf("Clear after the batch: %v", err)
	}
	sends := w.allSends()
	if len(sends) != 2 || sends[1].kind != cluster.CommandRetire {
		t.Fatalf("proposals %+v, want the batch then one RETIRE", sends)
	}
}

func TestARefusalDropsTheCandidatesAndAnErrorKeepsThem(t *testing.T) {
	refusal := &cluster.Error{Code: cluster.CodePermissionDenied, Message: "denied"}
	refused := newReportWorld(1)
	refused.setRecord(1, 3, machine.SeedReady)
	refused.sendFn = func(int) error { return refusal }
	r := refused.reporter()
	r.Note(1, 3, false)
	refused.advance(reportInterval)
	r.Flush(context.Background())
	refused.sendFn = nil
	refused.advance(reportInterval)
	r.Flush(context.Background())
	if sends := refused.allSends(); len(sends) != 1 {
		t.Fatalf("%d proposals after a refusal, want 1: a refused bit is dropped, not retried", len(sends))
	}

	boom := errors.New("no leader within the proposal budget")
	failed := newReportWorld(1)
	failed.setRecord(1, 3, machine.SeedReady)
	failed.sendFn = func(int) error { return boom }
	r = failed.reporter()
	r.Note(1, 3, false)
	failed.advance(reportInterval)
	r.Flush(context.Background())
	if len(failed.errs) != 1 || !errors.Is(failed.errs[0], boom) {
		t.Fatalf("errors %v, want the send error once", failed.errs)
	}
	failed.sendFn = nil
	failed.advance(reportInterval)
	r.Flush(context.Background())
	sends := failed.allSends()
	if len(sends) != 2 || !slices.Equal(setSlots(t, sends[1].bits), []uint16{3}) {
		t.Fatalf("proposals %+v, want the failed bit sent again on the next flush", sends)
	}
}

// TestReconcileSaysWhetherARecordIsStillSeeding: a seed finishes between two commits, so the
// reporter must scan again until no record of a known database is seeding, or the bit of
// the last pairs to finish is never noted (an RF raise on an idle database stalled at the
// bits it had before the raise).
func TestReconcileSaysWhetherARecordIsStillSeeding(t *testing.T) {
	w := newReportWorld(1)
	w.setRecord(1, 3, machine.SeedSeeding)
	r := w.reporter()
	if !r.Reconcile() {
		t.Fatal("Reconcile said nothing is seeding while slot 3 is")
	}
	w.setRecord(1, 3, machine.SeedReady)
	if r.Reconcile() {
		t.Fatal("Reconcile said a record is still seeding after slot 3 turned ready")
	}
	w.setRecord(2, 4, machine.SeedSeeding) // database 2 is not in the committed state
	if r.Reconcile() {
		t.Fatal("Reconcile counted a seeding record of a database that is gone")
	}
}
