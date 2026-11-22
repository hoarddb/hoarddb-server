package server

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// capacityMetrics is one server's capacity series, on a registry that server
// owns. A process-global registry written by several in-process samplers
// would make a series' presence a property of whichever sampler wrote last,
// and every capacity test asserts presence or absence (build doc §0.1, R-8).
// Production runs one Server per process, and /metrics serves this registry
// beside the default one.
//
// Every probe-backed series is a GaugeVec, including the ones with no labels,
// so it is absent until a sample sets it and can be deleted when its probe
// fails. A plain Gauge would be exported as 0 before the first read and hold
// its last value after a failure: a healthy-looking reading nobody took.
type capacityMetrics struct {
	registry *prometheus.Registry

	ratio         *prometheus.GaugeVec
	threshold     *prometheus.GaugeVec
	mappings      *prometheus.GaugeVec
	mappingsLimit *prometheus.GaugeVec
	anonBytes     *prometheus.GaugeVec
	memLimitBytes *prometheus.GaugeVec
	diskUsedBytes *prometheus.GaugeVec
	diskSizeBytes *prometheus.GaugeVec
	// blobDiskUsedBytes and blobDiskSizeBytes are the blob directory's
	// filesystem, set only on a node where it is not the data directory's.
	blobDiskUsedBytes *prometheus.GaugeVec
	blobDiskSizeBytes *prometheus.GaugeVec
	dataBytes         *prometheus.GaugeVec
	buckets           *prometheus.GaugeVec
	sampleErrors      *prometheus.CounterVec
	lastSample        prometheus.Gauge
	// memtableBytes is the node store's memtable, known at start and fixed
	// for the run, so a plain gauge set once is the truth from the first
	// scrape.
	memtableBytes prometheus.Gauge
	// encryptionSealed and encryptionInfo are the node's at-rest state, fixed
	// at start like the memtable: one 0/1 gauge from the structural check, and
	// one info series whose only labels are the configured mode and keystore.
	// Both are node-wide; nothing is labelled by database or bucket.
	encryptionSealed prometheus.Gauge
	encryptionInfo   *prometheus.GaugeVec
}

// Label values of hoarddb_node_data_bytes{part}.
const (
	dataPartStoreLSM  = "store_lsm"
	dataPartStoreVlog = "store_vlog"
	dataPartWAL       = "wal"
)

// capacityDataParts is the closed set of part label values.
var capacityDataParts = []string{dataPartStoreLSM, dataPartStoreVlog, dataPartWAL}

// newCapacityMetrics builds the collectors on a fresh registry. Only the
// threshold children and the error counters are created here: a reading
// series created at construction would be exported at 0 before the first
// sample, which is the defect the zero-label GaugeVecs exist to prevent.
func newCapacityMetrics(thresholds map[string]float64, resources []string) (*capacityMetrics, error) {
	unlabelled := func(name, help string) *prometheus.GaugeVec {
		return prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, nil)
	}
	m := &capacityMetrics{
		registry: prometheus.NewRegistry(),
		ratio: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "hoarddb_node_capacity_ratio",
			Help: "Each resource's reading divided by its live kernel limit. 0-1 for fds, mappings, disk and blob_disk (the blob directory's filesystem, present only when it is not the data directory's); memory can exceed 1 (swapped memory counts as used). Absent when the resource cannot be read.",
		}, []string{"resource"}),
		threshold: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "hoarddb_node_capacity_threshold_ratio",
			Help: "The ratio at which each resource is red: disk_high_water_pct/100 for disk when the join guard is on, otherwise the policy constant 0.80. A policy line, not a measured cliff.",
		}, []string{"resource"}),
		mappings: unlabelled("hoarddb_node_memory_mappings",
			"Memory mappings this process holds (lines of /proc/self/maps)."),
		mappingsLimit: unlabelled("hoarddb_node_memory_mappings_limit",
			"vm.max_map_count, where mmap starts failing."),
		anonBytes: unlabelled("hoarddb_node_memory_anon_bytes",
			"Anonymous memory, resident plus swapped (RssAnon + VmSwap)."),
		memLimitBytes: unlabelled("hoarddb_node_memory_limit_bytes",
			"The smaller of the cgroup memory.max and memory.high over this process's cgroup and every ancestor it can see, or the host's MemTotal when none is set; memory.high is a throttle line, not a hard limit."),
		diskUsedBytes: unlabelled("hoarddb_node_disk_used_bytes",
			"Bytes in use on the data directory's filesystem, counting the root reserve as used ((Blocks - Bavail) x Frsize)."),
		diskSizeBytes: unlabelled("hoarddb_node_disk_size_bytes",
			"Size of the data directory's filesystem (Blocks x Frsize)."),
		blobDiskUsedBytes: unlabelled("hoarddb_node_blob_disk_used_bytes",
			"Bytes in use on the blob directory's filesystem, counting the root reserve as used. Present only when storage.blob_dir is on a different filesystem from the data directory."),
		blobDiskSizeBytes: unlabelled("hoarddb_node_blob_disk_size_bytes",
			"Size of the blob directory's filesystem. Present only when storage.blob_dir is on a different filesystem from the data directory."),
		dataBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "hoarddb_node_data_bytes",
			Help: "Bytes hoardDB itself holds on disk, by part: the store's LSM and value log (as Badger last measured them, up to a minute stale) and the WAL's segments.",
		}, []string{"part"}),
		buckets: unlabelled("hoarddb_node_buckets",
			"Buckets this node serves: attached, and not dropped. A count kept on attach and drop, not a walk. Node-wide; there is no per-bucket series."),
		sampleErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hoarddb_node_capacity_sample_errors_total",
			Help: "Capacity samples in which a resource could not be read. The resource's ratio, reading and limit series are absent while it cannot be read.",
		}, []string{"resource"}),
		lastSample: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hoarddb_node_capacity_last_sample_timestamp_seconds",
			Help: "Unix time of the last completed capacity sample. 0 before the first; a stale value means the sampler has stopped.",
		}),
		memtableBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hoarddb_node_memtable_bytes",
			Help: "The node store's memtable size for this run: the size its store chose at its first start, or the explicit storage.memtable_mb. One series per node; nothing per bucket.",
		}),
		encryptionSealed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hoarddb_node_encryption_sealed",
			Help: "1 when the structural check of the node's store, taken once after it opened, found it sealed at rest; 0 otherwise. One series per node; nothing per bucket.",
		}),
		encryptionInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "hoarddb_node_encryption_info",
			Help: "Always 1. The labels are the configured encryption.mode (off, on, require) and encryption.keystore (off, keyfile, command, systemd-creds). One series per node.",
		}, []string{"mode", "keystore"}),
	}
	for _, collector := range []prometheus.Collector{
		m.ratio, m.threshold, m.mappings, m.mappingsLimit, m.anonBytes, m.memLimitBytes,
		m.diskUsedBytes, m.diskSizeBytes, m.blobDiskUsedBytes, m.blobDiskSizeBytes, m.dataBytes, m.buckets, m.sampleErrors, m.lastSample, m.memtableBytes,
		m.encryptionSealed, m.encryptionInfo,
	} {
		if err := m.registry.Register(collector); err != nil {
			return nil, fmt.Errorf("registering a capacity collector: %w", err)
		}
	}
	for _, resource := range resources {
		m.threshold.WithLabelValues(resource).Set(thresholds[resource])
		m.sampleErrors.WithLabelValues(resource)
	}
	return m, nil
}

// deleteResource removes a resource's ratio and its reading and limit series.
// It only deletes: the probe-failure path must never write a value.
func (m *capacityMetrics) deleteResource(resource string) {
	m.ratio.DeleteLabelValues(resource)
	for _, series := range m.resourceSeries(resource) {
		series.DeleteLabelValues()
	}
}

// resourceSeries is the zero-label reading and limit series behind a
// resource's ratio. fds has none of its own: process_open_fds and
// process_max_fds already publish them.
func (m *capacityMetrics) resourceSeries(resource string) []*prometheus.GaugeVec {
	switch resource {
	case resourceMappings:
		return []*prometheus.GaugeVec{m.mappings, m.mappingsLimit}
	case resourceMemory:
		return []*prometheus.GaugeVec{m.anonBytes, m.memLimitBytes}
	case resourceDisk:
		return []*prometheus.GaugeVec{m.diskUsedBytes, m.diskSizeBytes}
	case resourceBlobDisk:
		return []*prometheus.GaugeVec{m.blobDiskUsedBytes, m.blobDiskSizeBytes}
	}
	return nil
}
