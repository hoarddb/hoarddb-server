package wire

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// lapsedDecision is a lease that ended at 400 ms on the fixed clock (BootNow is one second).
func lapsedDecision() *machine.LeaseDecision {
	return &machine.LeaseDecision{Refusal: cluster.CodeLeaseLapsed, Cause: machine.LapseTickGap, LapsedAt: 400 * time.Millisecond}
}

func lapseRefusalText(t *testing.T, err error) string {
	t.Helper()
	var coded *cluster.Error
	if !errors.As(err, &coded) || coded.Code != cluster.CodeLeaseLapsed {
		t.Fatalf("want a LEASE_LAPSED refusal, got %v", err)
	}
	return coded.Message
}

func requireFigures(t *testing.T, text string) {
	t.Helper()
	if strings.ContainsAny(text, "<>") {
		t.Errorf("the refusal still holds a placeholder: %s", text)
	}
	if !strings.Contains(text, "stopped serving reads and writes 600 ms ago") {
		t.Errorf("lapsed_ms is not now (1000 ms) minus the decision's LapsedAt (400 ms): %s", text)
	}
	if !strings.Contains(text, "(step_down_ms 2000)") {
		t.Errorf("step_down_ms is not the committed setting: %s", text)
	}
	if !strings.Contains(text, "in app.users") {
		t.Errorf("the refusal does not name the bucket: %s", text)
	}
}

// lapseOnce makes the first serve check answer d and every later one a renewed lease, so a
// refusal that asks the lease a second time prints a different lapse than the one it refused.
func (f *fakeLease) lapseOnce(d *machine.LeaseDecision) {
	f.afterFirst.Store(&machine.LeaseDecision{Valid: true})
	f.decision.Store(d)
}

// TestAWriteRefusedForALapsedLeaseFillsItsFigures is spec lease-renewer-cadence O2 (BUG-0751): the
// refusal carries the refused decision's own time, not a second serve check's.
func TestAWriteRefusedForALapsedLeaseFillsItsFigures(t *testing.T) {
	fx := newFixture(t)
	fx.topo.setMembers(3)
	fx.topo.stepDownMs = 2000
	fx.topo.dbRow.Name = "app"
	fx.schema.name = "users"
	fx.lease.lapseOnce(lapsedDecision())
	_, err := fx.write(testSlot, "p1")
	requireFigures(t, lapseRefusalText(t, err))
}

// TestAReadRefusedForALapsedLeaseFillsItsFigures is O3 (the lease half of BUG-0787).
func TestAReadRefusedForALapsedLeaseFillsItsFigures(t *testing.T) {
	world := newU36World(t)
	world.fx.topo.stepDownMs = 2000
	world.fx.topo.dbRow.Name = "app"
	world.fx.schema.name = "users"
	world.fx.mustWrite(world.slot, "p1")
	world.fx.lease.lapseOnce(lapsedDecision())
	_, err := world.get("p1")
	requireFigures(t, lapseRefusalText(t, err))
}
