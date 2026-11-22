package server

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/hoarddb/hoarddb-server/storage"
)

// walCollector exports the log's counters at scrape time from one
// storage.WAL.Stats snapshot. Every series is node-wide with no labels: the log
// is one per node, and nothing here is per bucket or per store.
type walCollector struct {
	wal *storage.WAL

	preallocated      *prometheus.Desc
	flush             *prometheus.Desc
	rotations         *prometheus.Desc
	rotationWaits     *prometheus.Desc
	rotationWaitTime  *prometheus.Desc
	zeroFillBytes     *prometheus.Desc
	zeroFillTime      *prometheus.Desc
	spareFillFailures *prometheus.Desc
	spareReady        *prometheus.Desc
	bytesAppended     *prometheus.Desc
}

// newWALMetrics builds the log's registry, one per server so in-process nodes
// keep separate series. It is only built for a node that has a log: a node with
// none exports no hoarddb_wal_ series.
func newWALMetrics(wal *storage.WAL) (*prometheus.Registry, error) {
	desc := func(name, help string) *prometheus.Desc { return prometheus.NewDesc(name, help, nil, nil) }
	registry := prometheus.NewRegistry()
	collector := &walCollector{
		wal:          wal,
		preallocated: desc("hoarddb_wal_preallocated", "1 when the write-ahead log preallocates its segments, 0 when it does not. The reason is in the start-up log line and in status. One series per node."),
		flush:        desc("hoarddb_wal_flush_seconds", "How long each group-commit flush of the write-ahead log took. Buckets double from 0.5 ms to 1 s."),
		rotations:    desc("hoarddb_wal_rotations_total", "Write-ahead log segments retired by a rotation."),
		rotationWaits: desc("hoarddb_wal_rotation_waits_total",
			"Rotations that waited for a fill in flight or filled the next segment themselves, holding every write meanwhile."),
		rotationWaitTime:  desc("hoarddb_wal_rotation_wait_seconds_total", "Seconds the rotations counted by hoarddb_wal_rotation_waits_total spent waiting."),
		zeroFillBytes:     desc("hoarddb_wal_zero_fill_bytes_total", "Bytes of zeros written into next segments before use."),
		zeroFillTime:      desc("hoarddb_wal_zero_fill_seconds_total", "Seconds spent writing those zeros, failed fills included."),
		spareFillFailures: desc("hoarddb_wal_spare_fill_failures_total", "Fills of the next segment that failed."),
		spareReady:        desc("hoarddb_wal_spare_ready", "1 when a filled next segment is waiting for the next rotation."),
		bytesAppended:     desc("hoarddb_wal_bytes_appended_total", "Bytes of log entries appended to the write-ahead log, framed as written, since the node started. The baseline a storage-format change is measured against."),
	}
	if err := registry.Register(collector); err != nil {
		return nil, fmt.Errorf("registering the write-ahead log collector: %w", err)
	}
	return registry, nil
}

// Describe sends the collector's descriptors.
func (c *walCollector) Describe(descs chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{
		c.preallocated, c.flush, c.rotations, c.rotationWaits, c.rotationWaitTime,
		c.zeroFillBytes, c.zeroFillTime, c.spareFillFailures, c.spareReady, c.bytesAppended,
	} {
		descs <- desc
	}
}

// Collect reads one snapshot of the log's counters and sends the ten series.
func (c *walCollector) Collect(metrics chan<- prometheus.Metric) {
	stats := c.wal.Stats()
	buckets := make(map[float64]uint64, storage.WALFlushBuckets)
	for i, bound := range storage.WALFlushBucketBounds() {
		buckets[bound.Seconds()] = stats.FlushesAtMost[i]
	}
	metrics <- prometheus.MustNewConstMetric(c.preallocated, prometheus.GaugeValue, boolGauge(stats.Preallocated))
	metrics <- prometheus.MustNewConstHistogram(c.flush, stats.Flushes, stats.FlushTime.Seconds(), buckets)
	metrics <- prometheus.MustNewConstMetric(c.rotations, prometheus.CounterValue, float64(stats.Rotations))
	metrics <- prometheus.MustNewConstMetric(c.rotationWaits, prometheus.CounterValue, float64(stats.RotationWaits))
	metrics <- prometheus.MustNewConstMetric(c.rotationWaitTime, prometheus.CounterValue, stats.RotationWaitTime.Seconds())
	metrics <- prometheus.MustNewConstMetric(c.zeroFillBytes, prometheus.CounterValue, float64(stats.ZeroFillBytes))
	metrics <- prometheus.MustNewConstMetric(c.zeroFillTime, prometheus.CounterValue, stats.ZeroFillTime.Seconds())
	metrics <- prometheus.MustNewConstMetric(c.spareFillFailures, prometheus.CounterValue, float64(stats.SpareFillFailures))
	metrics <- prometheus.MustNewConstMetric(c.spareReady, prometheus.GaugeValue, boolGauge(stats.SpareReady))
	metrics <- prometheus.MustNewConstMetric(c.bytesAppended, prometheus.CounterValue, float64(stats.BytesAppended))
}

// boolGauge is 1 for true and 0 for false.
func boolGauge(on bool) float64 {
	if on {
		return 1
	}
	return 0
}
