package server

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// fakeSeedCounts is a seedCountSource whose counts a test sets between scrapes.
type fakeSeedCounts struct{ counts wire.SeedCounts }

func (f *fakeSeedCounts) SeedCounts() wire.SeedCounts { return f.counts }

// seedingSeriesNames are the five node-wide series; no other name is allowed.
var seedingSeriesNames = map[string]bool{
	"hoarddb_node_slots_ready":        true,
	"hoarddb_node_slots_seeding":      true,
	"hoarddb_node_slots_retired":      true,
	"hoarddb_node_seed_bytes_total":   true,
	"hoarddb_node_seed_retries_total": true,
}

// TestStatusPrintsNoPerSlotSeries gathers the seeding registry while three
// slots are seeding. It must hold the five series named in the design, each
// with one metric and no label, so no slot, database or bucket can appear in it.
func TestStatusPrintsNoPerSlotSeries(t *testing.T) {
	source := &fakeSeedCounts{counts: wire.SeedCounts{Ready: 6324, Seeding: 3, Retired: 1, BytesTotal: 4096, RetriesTotal: 2}}
	registry, err := newSeedingMetrics(source)
	if err != nil {
		t.Fatalf("newSeedingMetrics: %v", err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	if len(families) != len(seedingSeriesNames) {
		t.Fatalf("Gather returned %d families, want %d", len(families), len(seedingSeriesNames))
	}
	for _, family := range families {
		if !seedingSeriesNames[family.GetName()] {
			t.Errorf("family %q is not one of the five seeding series", family.GetName())
		}
		if n := len(family.GetMetric()); n != 1 {
			t.Errorf("family %q has %d metrics, want one series per node", family.GetName(), n)
		}
		for _, metric := range family.GetMetric() {
			if n := len(metric.GetLabel()); n != 0 {
				t.Errorf("family %q carries %d labels, want none", family.GetName(), n)
			}
		}
	}
}

// TestTheSeedingSeriesAreReadAtScrapeTime changes the counts between two
// scrapes of one registry. The second scrape must show the new counts, so the
// collector reads the source when it is scraped and not when it is built.
func TestTheSeedingSeriesAreReadAtScrapeTime(t *testing.T) {
	source := &fakeSeedCounts{counts: wire.SeedCounts{Seeding: 0, RetriesTotal: 0}}
	registry, err := newSeedingMetrics(source)
	if err != nil {
		t.Fatalf("newSeedingMetrics: %v", err)
	}
	if got := seedingSeriesValue(t, registry, "hoarddb_node_slots_seeding"); got != 0 {
		t.Fatalf("first scrape: slots_seeding = %v, want 0", got)
	}

	source.counts = wire.SeedCounts{Seeding: 2, RetriesTotal: 5}
	if got := seedingSeriesValue(t, registry, "hoarddb_node_slots_seeding"); got != 2 {
		t.Errorf("second scrape: slots_seeding = %v, want 2", got)
	}
	if got := seedingSeriesValue(t, registry, "hoarddb_node_seed_retries_total"); got != 5 {
		t.Errorf("second scrape: seed_retries_total = %v, want 5", got)
	}
}

// seedingSeriesValue returns the one value of the named series in a scrape.
func seedingSeriesValue(t *testing.T, registry *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		metric := family.GetMetric()[0]
		if family.GetType().String() == "COUNTER" {
			return metric.GetCounter().GetValue()
		}
		return metric.GetGauge().GetValue()
	}
	t.Fatalf("series %q is not in the scrape", name)
	return 0
}
