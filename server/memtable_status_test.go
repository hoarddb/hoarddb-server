package server

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
)

// TestHealthReportsTheMemtable: status carries the applied size and why, so
// "hoardDB status" can print it (memtable-host-scaled-design.md §5.3).
func TestHealthReportsTheMemtable(t *testing.T) {
	qs := newCapacityService(t, nil, nil)
	defer func() {
		if err := qs.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()
	health, err := qs.core.HealthCheck()
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	memtable := health.Memtable
	if memtable == nil {
		t.Fatal("HealthResponse.Memtable is nil")
	}
	if memtable.MB != 64 || memtable.Source != memtableSourceHost || memtable.ChoiceMB != 64 ||
		memtable.ChosenFromLimitBytes != testMemoryLimitBytes || memtable.LimitBytes != testMemoryLimitBytes ||
		memtable.BatchBytes != config.MemtableBatchBytes(64) || memtable.Large || memtable.ChosenAt == 0 {
		t.Errorf("HealthResponse.Memtable = %+v, want 64 MiB chosen from the test limit just under 7 GiB, not large", memtable)
	}
}

func TestHealthNamesAnExplicitMemtable(t *testing.T) {
	t.Setenv(memtableEnvVar, "")
	qs := newCapacityService(t, func(cfg *config.Config) { cfg.Storage.MemtableMB = 256 }, nil)
	defer func() {
		if err := qs.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()
	health, err := qs.core.HealthCheck()
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if health.Memtable == nil || health.Memtable.MB != 256 || health.Memtable.Source != memtableSourceConfig || health.Memtable.ChoiceMB != 64 {
		t.Errorf("HealthResponse.Memtable = %+v, want 256 MiB set in the config, choice 64", health.Memtable)
	}
}

// TestMemtableGaugeIsTheAppliedSize: one node-wide series, no labels, nothing
// per bucket (§5.6).
func TestMemtableGaugeIsTheAppliedSize(t *testing.T) {
	qs := newCapacityService(t, nil, nil)
	defer func() {
		if err := qs.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()
	series := gatherSampler(t, qs.core.capacity)["hoarddb_node_memtable_bytes"]
	if len(series) != 1 || len(series[0].GetLabel()) != 0 || series[0].GetGauge().GetValue() != float64(64<<20) {
		t.Errorf("hoarddb_node_memtable_bytes = %v, want one unlabelled series at %d", series, 64<<20)
	}
}
