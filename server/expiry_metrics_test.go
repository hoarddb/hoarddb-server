package server

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

type fakeExpirySource struct {
	fakeSeedCounts
	expiry wire.ExpiryCounts
}

func (f *fakeExpirySource) ExpiryCounts() wire.ExpiryCounts { return f.expiry }

// TestTheExpirySeriesCarryNoPerBucketLabel: the expiry counters are node-wide; the only label is
// the kind of item removed, two values, so no database, bucket or slot can appear.
func TestTheExpirySeriesCarryNoPerBucketLabel(t *testing.T) {
	source := &fakeExpirySource{expiry: wire.ExpiryCounts{RemovedDocuments: 7, RemovedElements: 3, Orphans: 2, Backlog: 11}}
	registry, err := newSeedingMetrics(source)
	if err != nil {
		t.Fatal(err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, family := range families {
		switch family.GetName() {
		case "hoarddb_expiry_removed_total":
			seen[family.GetName()] = true
			if len(family.GetMetric()) != 2 {
				t.Errorf("removed_total has %d series, want one per kind", len(family.GetMetric()))
			}
			for _, metric := range family.GetMetric() {
				if len(metric.GetLabel()) != 1 || metric.GetLabel()[0].GetName() != "kind" {
					t.Errorf("removed_total labels = %v, want only kind", metric.GetLabel())
				}
			}
		case "hoarddb_expiry_orphans_total", "hoarddb_expiry_backlog":
			seen[family.GetName()] = true
			if len(family.GetMetric()) != 1 || len(family.GetMetric()[0].GetLabel()) != 0 {
				t.Errorf("%s must be one series with no label", family.GetName())
			}
		}
	}
	for _, name := range []string{"hoarddb_expiry_removed_total", "hoarddb_expiry_orphans_total", "hoarddb_expiry_backlog"} {
		if !seen[name] {
			t.Errorf("series %s is missing", name)
		}
	}
	if got := seedingSeriesValue(t, registry, "hoarddb_expiry_backlog"); got != 11 {
		t.Errorf("backlog = %v, want 11", got)
	}
}

// TestAnUnknownExpiryBacklogIsLeftOutNotReportedAsZero: a queue that could not be read must not
// read as a sweep that is caught up.
func TestAnUnknownExpiryBacklogIsLeftOutNotReportedAsZero(t *testing.T) {
	source := &fakeExpirySource{expiry: wire.ExpiryCounts{RemovedDocuments: 1, BacklogUnknown: true}}
	registry, err := newSeedingMetrics(source)
	if err != nil {
		t.Fatal(err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "hoarddb_expiry_backlog" {
			t.Fatalf("an unreadable queue was reported as a backlog of %v", family.GetMetric()[0].GetGauge().GetValue())
		}
	}
}
