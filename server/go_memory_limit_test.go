package server

import (
	"errors"
	"strings"
	"testing"
)

// TestGoMemoryLimitIsThreeQuartersOfTheDetectedLimit: with no
// GOMEMLIMIT in the environment, the server bounds the Go heap at 75% of the
// memory limit it detects, so the GC's default headroom (about 2.4 times the
// live heap under an ingest burst) turns into GC work rather than resident
// memory on a small host.
func TestGoMemoryLimitIsThreeQuartersOfTheDetectedLimit(t *testing.T) {
	decision := decideGoMemoryLimit("", fixedMemoryLimit(4*gib))
	if decision.bytes != 3*gib {
		t.Errorf("Go memory limit at a 4 GiB memory limit = %d, want %d (75%%)", decision.bytes, 3*gib)
	}
	want := "3.0 GiB, 75% of this node's memory limit of 4.0 GiB (the host's MemTotal; no cgroup memory limit is set); set GOMEMLIMIT to choose another"
	if decision.text != want {
		t.Errorf("start-up text:\n got %s\nwant %s", decision.text, want)
	}
}

// TestAnOperatorsGOMEMLIMITWins: an operator's GOMEMLIMIT is applied by the Go
// runtime itself and is never overridden.
func TestAnOperatorsGOMEMLIMITWins(t *testing.T) {
	decision := decideGoMemoryLimit("2GiB", fixedMemoryLimit(4*gib))
	if decision.bytes != 0 {
		t.Errorf("with GOMEMLIMIT=2GiB the server would set %d itself, want nothing", decision.bytes)
	}
	if decision.text != "GOMEMLIMIT=2GiB, from the environment" {
		t.Errorf("start-up text = %q", decision.text)
	}
}

func TestNoDetectedLimitLeavesGoAlone(t *testing.T) {
	decision := decideGoMemoryLimit("", func() (memoryLimit, error) { return memoryLimit{}, errors.New("no /proc here") })
	if decision.bytes != 0 {
		t.Errorf("with no memory limit read the server would set %d, want nothing", decision.bytes)
	}
	if !strings.Contains(decision.text, "not set: this node's memory limit could not be read (no /proc here)") ||
		!strings.Contains(decision.text, "set GOMEMLIMIT") {
		t.Errorf("start-up text = %q, want the reason and the fix", decision.text)
	}
}
