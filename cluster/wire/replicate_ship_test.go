package wire

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// shipKey is the pair every test of the read and send halves ships.
var shipKey = pairKey{u32Slot, u32DB}

// inflightLink records, at each Write, the positions the replicator holds in flight for the
// copy: what a send that marked a record only after its write would not yet show.
type inflightLink struct {
	*u32Link
	owner *Replicator
	mu    sync.Mutex
	seen  []string
}

func (l *inflightLink) Write(ctx context.Context, peer string, frame WriteFrame) error {
	l.owner.mu.Lock()
	held := l.owner.pairs[frame.keyOf()].peers[peer].inflight
	last := "none"
	if len(held) > 0 {
		last = held[len(held)-1].pos.String()
	}
	l.owner.mu.Unlock()
	l.mu.Lock()
	l.seen = append(l.seen, fmt.Sprintf("%s:%s", frame.Pos, last))
	l.mu.Unlock()
	return l.u32Link.Write(ctx, peer, frame)
}

func (f WriteFrame) keyOf() pairKey { return pairKey{f.Slot, f.DB} }

// TestReadShipmentSendsNothingAndAdvancesTheCursor: the read half takes the records out of the
// log and moves the pair's bookkeeping, and puts nothing on the link and nothing in flight, so
// a later half can decide what to do with them.
func TestReadShipmentSendsNothingAndAdvancesTheCursor(t *testing.T) {
	owner := newU32Node(t, "owner", nil)
	owner.copies = []string{"copy"}
	owner.ownRun(u32Epoch, 1, 3)

	sh, ok, err := owner.rep.readShipment(context.Background(), shipKey, "copy")
	if err != nil || !ok {
		t.Fatalf("readShipment = ok %v, err %v, want a shipment", ok, err)
	}
	if len(sh.records) != 3 || sh.records[2].Pos != u32Pos(u32Epoch, 3) {
		t.Fatalf("the shipment holds %d records, want the 3 the log has", len(sh.records))
	}
	if sh.more {
		t.Fatal("more is set, but the read did not fill the window's room")
	}
	if sent := owner.link.total(); sent != 0 {
		t.Fatalf("readShipment put %d frames on the link, want 0", sent)
	}
	owner.rep.mu.Lock()
	defer owner.rep.mu.Unlock()
	st := owner.rep.pairs[shipKey].peers["copy"]
	if len(st.inflight) != 0 {
		t.Fatalf("readShipment marked %d records in flight, want 0", len(st.inflight))
	}
	if st.shipped != u32Pos(u32Epoch, 3) || st.next <= 1 {
		t.Fatalf("the cursor is at next %d, shipped %s, want it past the 3 records read", st.next, st.shipped)
	}
	if !st.ackBy.IsZero() {
		t.Fatal("the read armed the ack deadline, but nothing is in flight to wait for (BUG-0866)")
	}
	if _, waiting := owner.rep.awaiting[retryKey{shipKey, "copy"}]; waiting {
		t.Fatal("the copy is in the awaiting set after the read, before any frame is in flight")
	}
}

// TestSendShipmentMarksEachRecordInFlightBeforeItsWrite: an ack that overtakes the receipt must
// find its record in flight, so the mark comes first. The link looks at the in-flight list from
// inside Write.
func TestSendShipmentMarksEachRecordInFlightBeforeItsWrite(t *testing.T) {
	var link *inflightLink
	owner := newU32Node(t, "owner", func(cfg *ReplicatorConfig) {
		link = &inflightLink{u32Link: newU32Link()}
		cfg.Link = link
	})
	link.owner = owner.rep
	owner.copies = []string{"copy"}
	owner.ownRun(u32Epoch, 1, 3)

	sh, ok, err := owner.rep.readShipment(context.Background(), shipKey, "copy")
	if err != nil || !ok {
		t.Fatalf("readShipment = ok %v, err %v, want a shipment", ok, err)
	}
	if err := owner.rep.sendShipment(context.Background(), sh); err != nil {
		t.Fatalf("sendShipment: %v", err)
	}
	var want []string
	for seq := uint64(1); seq <= 3; seq++ {
		pos := u32Pos(u32Epoch, seq)
		want = append(want, fmt.Sprintf("%s:%s", pos, pos))
	}
	link.mu.Lock()
	defer link.mu.Unlock()
	if fmt.Sprint(link.seen) != fmt.Sprint(want) {
		t.Fatalf("in flight at each write = %v, want each record already last in flight: %v", link.seen, want)
	}
}

// TestAShipmentReadBeforeARewindIsNotSent: an ack rewound the copy between the two halves, so
// the records were read from a cursor that is stale now; sending them would ship out of order.
// The rewind itself queued the pair to be read again.
func TestAShipmentReadBeforeARewindIsNotSent(t *testing.T) {
	owner := newU32Node(t, "owner", nil)
	owner.copies = []string{"copy"}
	owner.ownRun(u32Epoch, 1, 3)

	sh, ok, err := owner.rep.readShipment(context.Background(), shipKey, "copy")
	if err != nil || !ok {
		t.Fatalf("readShipment = ok %v, err %v, want a shipment", ok, err)
	}
	owner.rep.mu.Lock()
	sh.st.generation++
	owner.rep.mu.Unlock()

	if err := owner.rep.sendShipment(context.Background(), sh); err != nil {
		t.Fatalf("sendShipment: %v", err)
	}
	if sent := owner.link.total(); sent != 0 {
		t.Fatalf("a shipment read before a rewind put %d frames on the link, want 0", sent)
	}
	owner.rep.mu.Lock()
	defer owner.rep.mu.Unlock()
	if inflight := owner.rep.pairs[shipKey].peers["copy"].inflight; len(inflight) != 0 {
		t.Fatalf("%d stale records were marked in flight, want 0", len(inflight))
	}
}
