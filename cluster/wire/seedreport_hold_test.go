package wire

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// TestAHeldReporterSendsNothing: with HoldSeedReports the reporter's Run neither derives nor
// sends a SEEDED or RETIRE, however many pairs are ready and however old the candidates are,
// while Clear, the reseed's own clear, still sends its one-slot RETIRE.
// [BREAK: ignore deps.Hold in Run]
func TestAHeldReporterSendsNothing(t *testing.T) {
	const db = 1
	w := newReportWorld(db)
	for slot := range uint16(reportDueSlots) {
		w.setRecord(db, slot, machine.SeedReady) // enough to be due without waiting
	}
	w.setRecord(db, 100, machine.SeedSeeding)
	w.setCommittedBit(t, db, 100) // a RETIRE candidate
	w.setCommittedBit(t, db, 9)   // the bit Clear will retire
	w.advance(10 * reportInterval)
	held := w.reporter()
	held.deps.Hold = true

	ctx, cancel := context.WithCancel(context.Background())
	var running sync.WaitGroup
	running.Add(1)
	go func() {
		defer running.Done()
		held.Run(ctx)
	}()
	time.Sleep(3*reportTick + reportTick/2)
	cancel()
	running.Wait()

	if sends := w.allSends(); len(sends) != 0 {
		t.Fatalf("a held reporter sent %d proposals (first kind %d) in 3 ticks, want none", len(sends), sends[0].kind)
	}
	if err := held.Clear(context.Background(), db, 9); err != nil {
		t.Fatalf("Clear on a held reporter: %v", err)
	}
	sends := w.proposals(cluster.CommandRetire)
	if len(sends) != 1 || len(setSlots(t, sends[0].bits)) != 1 || setSlots(t, sends[0].bits)[0] != 9 {
		t.Fatalf("after Clear, proposals %+v, want one RETIRE for slot 9", w.allSends())
	}
}
