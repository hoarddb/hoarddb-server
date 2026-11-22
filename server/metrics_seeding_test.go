package server

import "testing"

// TestARingNodeServesTheFiveSeedingSeries: a started server with an entry-ring node serves the
// five node-wide seeding series from /metrics, one series each and no label, so no slot, database
// or bucket can become a label (AGENTS.md rule 11).
func TestARingNodeServesTheFiveSeedingSeries(t *testing.T) {
	want := []string{
		"hoarddb_node_slots_ready",
		"hoarddb_node_slots_seeding",
		"hoarddb_node_slots_retired",
		"hoarddb_node_seed_bytes_total",
		"hoarddb_node_seed_retries_total",
	}
	qs, _ := newTCPEnabledServer(t, nil)
	if qs.ringNode == nil {
		t.Fatal("the started server holds no entry-ring node, so there is no seeding to serve")
	}
	served := servedMetrics(t, qs.core)
	for _, name := range want {
		series := served[name]
		if len(series) != 1 {
			t.Errorf("%s: /metrics serves %d series, want exactly one (node-wide)", name, len(series))
			continue
		}
		if labels := series[0].GetLabel(); len(labels) != 0 {
			t.Errorf("%s carries labels %v, want none", name, labels)
		}
	}
}
