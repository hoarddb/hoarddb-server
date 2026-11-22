package server

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// seedCountSource is what the seeding collector reads at each scrape; a
// *wire.Node satisfies it.
type seedCountSource interface {
	SeedCounts() wire.SeedCounts
}

// expiryCountSource is the optional second face of a seedCountSource: a node that sweeps expired
// items reports three counters and one gauge, all node-wide.
type expiryCountSource interface {
	ExpiryCounts() wire.ExpiryCounts
}

// seedingCollector exports the seeding counts at scrape time from one
// SeedCounts snapshot. Every series is node-wide with no labels: a slot, a
// database or a bucket must never become a label.
type seedingCollector struct {
	source seedCountSource

	slotsReady   *prometheus.Desc
	slotsSeeding *prometheus.Desc
	slotsRetired *prometheus.Desc
	bytesTotal   *prometheus.Desc
	retriesTotal *prometheus.Desc

	// The expiry series exist only when the source reports expiry (expiry spec 7): the one label
	// is the kind of item removed, never a database, a bucket or a slot.
	expiry        expiryCountSource
	expiryRemoved *prometheus.Desc
	expiryOrphans *prometheus.Desc
	expiryBacklog *prometheus.Desc
}

// newSeedingMetrics builds the seeding registry, one per server so in-process
// nodes keep separate series. It is only built for a node that runs a ring
// cluster; a node without one exports no hoarddb_node_slots_ series.
func newSeedingMetrics(source seedCountSource) (*prometheus.Registry, error) {
	desc := func(name, help string) *prometheus.Desc { return prometheus.NewDesc(name, help, nil, nil) }
	registry := prometheus.NewRegistry()
	collector := &seedingCollector{
		source:       source,
		slotsReady:   desc("hoarddb_node_slots_ready", "Slots this node holds ready: each is a copy it can serve. One series per node."),
		slotsSeeding: desc("hoarddb_node_slots_seeding", "Slots this node is copying from another node, queued or pulling. One series per node."),
		slotsRetired: desc("hoarddb_node_slots_retired", "Slots this node has given up, retired or retiring, whose range is kept until the deletion gate passes. One series per node."),
		bytesTotal:   desc("hoarddb_node_seed_bytes_total", "Bytes of seed chunks this node has applied as a receiver, since the process started. One series per node."),
		retriesTotal: desc("hoarddb_node_seed_retries_total", "Seed pulls this node has moved to another source or restarted on a fresh cut, since the process started. One series per node."),
	}
	if expiry, ok := source.(expiryCountSource); ok {
		collector.expiry = expiry
		collector.expiryRemoved = prometheus.NewDesc("hoarddb_expiry_removed_total", "Expired items this node's sweep has removed since the process started, by kind (document or element). Node-wide.", []string{"kind"}, nil)
		collector.expiryOrphans = desc("hoarddb_expiry_orphans_total", "Stale expiry entries this node's sweep dropped without a removal since the process started. One series per node.")
		collector.expiryBacklog = desc("hoarddb_expiry_backlog", "Expiry entries due now and not yet removed, counted up to 10000. One series per node.")
	}
	if err := registry.Register(collector); err != nil {
		return nil, fmt.Errorf("registering the seeding collector: %w", err)
	}
	return registry, nil
}

// Describe sends the collector's descriptors.
func (c *seedingCollector) Describe(descs chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{c.slotsReady, c.slotsSeeding, c.slotsRetired, c.bytesTotal, c.retriesTotal} {
		descs <- desc
	}
	if c.expiry != nil {
		descs <- c.expiryRemoved
		descs <- c.expiryOrphans
		descs <- c.expiryBacklog
	}
}

// Collect reads one snapshot of the seeding counts and sends the five series.
func (c *seedingCollector) Collect(metrics chan<- prometheus.Metric) {
	counts := c.source.SeedCounts()
	metrics <- prometheus.MustNewConstMetric(c.slotsReady, prometheus.GaugeValue, float64(counts.Ready))
	metrics <- prometheus.MustNewConstMetric(c.slotsSeeding, prometheus.GaugeValue, float64(counts.Seeding))
	metrics <- prometheus.MustNewConstMetric(c.slotsRetired, prometheus.GaugeValue, float64(counts.Retired))
	metrics <- prometheus.MustNewConstMetric(c.bytesTotal, prometheus.CounterValue, float64(counts.BytesTotal))
	metrics <- prometheus.MustNewConstMetric(c.retriesTotal, prometheus.CounterValue, float64(counts.RetriesTotal))
	if c.expiry == nil {
		return
	}
	expired := c.expiry.ExpiryCounts()
	metrics <- prometheus.MustNewConstMetric(c.expiryRemoved, prometheus.CounterValue, float64(expired.RemovedDocuments), "document")
	metrics <- prometheus.MustNewConstMetric(c.expiryRemoved, prometheus.CounterValue, float64(expired.RemovedElements), "element")
	metrics <- prometheus.MustNewConstMetric(c.expiryOrphans, prometheus.CounterValue, float64(expired.Orphans))
	if !expired.BacklogUnknown {
		metrics <- prometheus.MustNewConstMetric(c.expiryBacklog, prometheus.GaugeValue, float64(expired.Backlog))
	}
}
