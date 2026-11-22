// Package server — Prometheus metrics for hoardDB.
//
// Provides request-level and bucket-level metrics exposed via a /metrics
// HTTP endpoint. All metrics are registered once at init time and updated
// by the InstrumentHandler wrapper on the request path.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// requestFaultsRecoveredHelp is the help text both series of
// hoarddb_request_faults_recovered_total share, as the registry requires.
const requestFaultsRecoveredHelp = "Request handler faults the server recovered from, by port (driver or internode): the request failed, its connection was closed and the process kept serving. Any non-zero value is a server bug; the error-level log line names the fault id and carries the stack."

// Prometheus metric descriptors, registered once at init.
var (
	requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoarddb_requests_total",
			Help: "Total requests by opcode and status.",
		},
		[]string{"method", "status"},
	)

	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "hoarddb_request_duration_seconds",
			Help:    "Request duration in seconds by opcode.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)

	requestInFlight = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "hoarddb_request_in_flight",
			Help: "Number of requests currently being processed.",
		},
	)

	bucketOpsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoarddb_bucket_operations_total",
			Help: "Total operations by operation type.",
		},
		[]string{"op"},
	)

	// guardTotal counts guarded updates and deletes by outcome: applied when the guard held,
	// refused when it did not. A deduped retry is not counted again. One series each, never
	// one per bucket.
	guardTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoarddb_update_guard_total",
			Help: "Guarded updates and deletes by outcome: applied when the if guard held, refused when it did not.",
		},
		[]string{"outcome"},
	)

	// pipelineDrainsTotal and pipelineDrainWaitSeconds count the owner writes that waited for a
	// pair's pending list to drain because they could read what a pending write changes (a
	// same-document or same-request write, a catalog write or an epoch opener), and the seconds
	// they waited. Two node-wide series: no label, nothing per bucket, store or document, and
	// O(1) work per wait at any bucket count.
	pipelineDrainsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hoarddb_write_pipeline_drains_total",
		Help: "Owner writes that waited for a pair's pending writes to drain before they could be appended.",
	})
	pipelineDrainWaitSeconds = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hoarddb_write_pipeline_drain_wait_seconds_total",
		Help: "Seconds the writes counted by hoarddb_write_pipeline_drains_total spent waiting.",
	})

	// Auth-config replication (spec 5.3.3). The per-node version alone cannot
	// show divergence or how long a member has been behind, which is why the
	// master also publishes the count and the oldest unacked age.
	authConfigVersion = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "hoarddb_auth_config_version",
		Help: "This node's auth config version.",
	})
	authConfigLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "hoarddb_auth_config_lag",
		Help: "Versions the furthest-behind reachable member is behind the master.",
	})
	authConfigDivergedMembers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "hoarddb_auth_config_diverged_members",
		Help: "Members whose auth config version differs from the master's.",
	})
	authConfigOldestUnackedSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "hoarddb_auth_config_oldest_unacked_seconds",
		Help: "Age of the oldest auth-config push the master has not acknowledged.",
	})

	// Failed applies (shared-storage spec §4.4 rule 5, §11 Q23 and Q24). Both
	// are node-wide: a series per bucket is cardinality that 10^6 buckets
	// cannot afford, and the log line names the bucket.
	applyFailuresOwed = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "hoarddb_apply_failures_owed",
		Help: "Writes whose store apply failed on this node and which replay still owes; each holds WAL retention until the next restart.",
	})
	// replicaDivergences counts entries this node refused although their
	// owner applied them (replication dispatch 10, R8-E). One node-wide
	// series; the bucket and the sender are in the ERROR and the 'v' record.
	replicaDivergences = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hoarddb_replica_divergences_total",
		Help: "Entries this replica refused although their owner applied them; each stops replication from that owner to this node until 'migrate' repairs the bucket.",
	})
	indexUniqueRefusals = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hoarddb_index_unique_refusals_total",
		Help: "Writes refused because a unique index already held the value, whether the owner's pre-check or the apply caught them.",
	})
	// indexesByState counts this node's declared indexes by state, a closed
	// set of four values: ready, building, conflicted, unreadable. Per-index
	// detail is describe's, on demand (GAP-14 build doc S-10).
	indexesByState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "hoarddb_indexes",
		Help: "Declared secondary indexes on this node, by state. A conflicted or unreadable index refuses search; describe names it.",
	}, []string{"state"})
	ttlSweepDocuments = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "hoarddb_ttl_sweep_documents_total",
		Help: "Documents the TTL sweeper expired (deleted), found refreshed since its scan (spared), failed to expire (failed), or left whole because their store fields are too large to delete in one step (skipped; counted again each pass that retries them).",
	}, []string{"outcome"})
	ttlSweepDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "hoarddb_ttl_sweep_duration_seconds",
		Help:    "How long each TTL sweep pass took.",
		Buckets: prometheus.DefBuckets,
	})
	ttlSweepBudgetExhausted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hoarddb_ttl_sweep_budget_exhausted_total",
		Help: "TTL sweep passes that ended on their budget with expired documents left; each starts the next pass at once.",
	})
	ttlBacklogEstimate = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "hoarddb_ttl_backlog_estimate",
		Help: "Expired documents the last TTL pass found and did not reach, summed over every TTL bucket: a lower bound, 0 when it drained.",
	})
	indexOrphanSkips = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hoarddb_index_orphan_skips_total",
		Help: "Index entries search skipped because the document they name does not exist; never repaired on the read path.",
	})
	// The two typed-field counters (typed-fields spec §9.5) are node-wide and
	// unlabelled: each counts once per write, on the apply that answers the
	// client, never on a replica or in replay (answersClient). The hourly warn
	// line names the buckets.
	fieldTypeRefusals = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hoarddb_field_type_refusals_total",
		Help: "Writes refused because a value did not match its bucket's declared field types (field_types: strict).",
	})
	fieldTypeMismatchesAccepted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hoarddb_field_type_mismatches_accepted_total",
		Help: "Writes stored although a value did not match its bucket's declared field types (field_types: warn); the hourly log line names the buckets.",
	})
	replicaConstraintDisagreements = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hoarddb_replica_constraint_disagreements_total",
		Help: "Writes a replica and its owner answered differently under a unique index, the bucket's declared field types or a path rule; each leaves the replica's copy of its bucket different from the owner's.",
	})
	applyFailuresNotReplayed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hoarddb_apply_failures_not_replayed_total",
		Help: "Failed writes replay could not apply without diverging from the replicas; each leaves this node's copy of its bucket different from theirs.",
	})

	// The reclaimer (shared-storage spec §5.2.8 answer 2): aggregates only,
	// with no label. A series per dropped bucket is cardinality a million
	// buckets cannot afford; one bucket's progress is its reclaim record's
	// keys_deleted and files_unlinked. The two gauges are set by each lap from
	// what it walked, never by a scan at start.
	reclaimPendingRecords = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "hoarddb_reclaim_pending_records",
		Help: "Reclaim records of dropped buckets whose space is still being reclaimed, as of the reclaimer's last lap.",
	})
	reclaimKeysDeleted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hoarddb_reclaim_keys_deleted_total",
		Help: "Keys of dropped buckets the reclaimer has deleted.",
	})
	reclaimKeysVisited = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hoarddb_reclaim_keys_visited_total",
		Help: "Stored versions the reclaimer has walked, delete markers included: the unit its budget is charged in.",
	})
	reclaimFilesUnlinked = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hoarddb_reclaim_files_unlinked_total",
		Help: "Blob payload files of dropped buckets the reclaimer has unlinked.",
	})
	reclaimOldestPendingSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "hoarddb_reclaim_oldest_pending_seconds",
		Help: "Age of the oldest pending reclaim record, as of the reclaimer's last lap; 0 when none is pending. Past a day the server warns.",
	})

	// The frame-length guard's counter (query-pipelining spec §3.3). The count
	// lives in transport, which cannot import this package, so the metric
	// reads it rather than being incremented.
	responseFrameTooLarge = prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: "hoarddb_response_frame_too_large_total",
		Help: "Responses replaced by RESULT_TOO_LARGE because they exceeded the 32 MiB frame limit; any non-zero value is a server bug.",
	}, func() float64 { return float64(transport.OversizedResponses()) })

	// Request handler faults recovered at the request boundary, one series
	// per port and none per bucket or opcode: the log line carries the
	// opcode, the fault id and the stack. The counts live in transport.
	requestFaultsRecoveredDriver = prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name:        "hoarddb_request_faults_recovered_total",
		Help:        requestFaultsRecoveredHelp,
		ConstLabels: prometheus.Labels{"port": "driver"},
	}, func() float64 {
		driver, _ := transport.RecoveredFaults()
		return float64(driver)
	})
	requestFaultsRecoveredInternode = prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name:        "hoarddb_request_faults_recovered_total",
		Help:        requestFaultsRecoveredHelp,
		ConstLabels: prometheus.Labels{"port": "internode"},
	}, func() float64 {
		_, internode := transport.RecoveredFaults()
		return float64(internode)
	})

	// Reads that reached a damaged block of the node store. The
	// count lives in storage, which logs the first; one node-wide series,
	// because which bucket a read was for is in the read's error, not a label.
	storeDamagedReads = prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: "hoarddb_store_damaged_reads_total",
		Help: "Reads, by key or by walk, that reached a block of the node store that failed its checksum or could not be read; any non-zero value means a store file is damaged and the node needs restoring from a replica or a backup.",
	}, func() float64 { return float64(storage.DamagedReads()) })

	// Queries refused by their result budget (query-pipelining spec §3.2). The
	// method label takes the five query names, passed as constants rather
	// than Opcode.String(), which names one of them UNKNOWN.
	queryResultTooLarge = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "hoarddb_query_result_too_large_total",
		Help: "Queries refused with RESULT_TOO_LARGE because what they had to hold outgrew HOARDDB_QUERY_MAX_RESULT_BYTES.",
	}, []string{"method"})

	// The delivery counters of claim, ack, release, extend and the return step
	// (claim-ack-spec 11.1). Node aggregates, never labelled by database or
	// bucket (TestNoDeliveryMetricHasABucketLabel): the paced log line and the
	// bucket's own answers name the bucket. Counted once, on the apply that
	// answers the client (answeringApply).
	queueClaims = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hoarddb_queue_claims_total",
		Help: "Claims this node answered, empty ones included.",
	})
	queueClaimedEntries = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hoarddb_queue_claimed_entries_total",
		Help: "Entries claims moved under a lease.",
	})
	queueLeaseSettlements = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "hoarddb_queue_lease_settlements_total",
		Help: "Ack, release and extend answers, by verb and by the state of the lease they named.",
	}, []string{"verb", "lease_state"})
	queueEntriesReturned = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hoarddb_queue_entries_returned_total",
		Help: "Entries written back to their queue because their lease ended or was released.",
	})
	queueEntriesDeadLettered = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "hoarddb_queue_entries_dead_lettered_total",
		Help: "Entries moved to a dead list, by reason: max_deliveries (claimed too many times) or collided (a heap id was pushed again while it was claimed).",
	}, []string{"reason"})
	queueLeaseClockSteps = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hoarddb_queue_lease_clock_steps_total",
		Help: "Applies whose stamped time fell behind a bucket's lease clock by more than the tolerance, so the stamp won; a node clock that steps back this far is worth finding.",
	})
	queueDeadRequeued = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hoarddb_queue_dead_requeued_total",
		Help: "Dead entries written back to their queue by a requeue.",
	})
	queueDeadDiscarded = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hoarddb_queue_dead_discarded_total",
		Help: "Dead entries dropped by a discard.",
	})
	queueClaimsEntryTooLarge = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hoarddb_queue_claims_entry_too_large_total",
		Help: "Claims refused because the entry at the front of the queue is too large to claim; pop takes such an entry.",
	})
	queueEmptyClaimsUnlogged = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hoarddb_queue_empty_claims_unlogged_total",
		Help: "Claims the bucket's owner answered empty without logging them: nothing to take and no lease to return.",
	})

	// Query admission (query-pipelining spec §3.1, §3.1.1). Node aggregates:
	// the two pools' held slots, and one wait histogram labelled by pool
	// (build doc SA-11).
	queryExecuteInFlight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "hoarddb_query_in_flight",
		Help: "Queries holding an execution slot: executing, or waiting for their answer's write.",
	})
	queryForwardInFlight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "hoarddb_query_forwarded_in_flight",
		Help: "Forwarded query results this node holds for its clients, each under a forwarded-result slot.",
	})
	queryAdmissionWait = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "hoarddb_query_admission_wait_seconds",
		Help:    "How long a query waited for a slot, by pool: execute (running it) or forward (holding its forwarded result).",
		Buckets: prometheus.DefBuckets,
	}, []string{"pool"})
	// Queries abandoned because their client went away (query-pipelining
	// spec §3.4): one node-wide series.
	queryCancelled = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hoarddb_query_cancelled_total",
		Help: "Queries stopped, while waiting for a slot or while walking, because the connection that asked for them was gone.",
	})

	// Internode requests in flight (query-pipelining spec §3.6, build doc
	// step 8), labelled by peer node ID and lane: O(peers) series, never
	// per bucket. A peer's series are removed when the peer is closed.
	internodeInFlight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "hoarddb_internode_in_flight",
		Help: "Requests this node has in flight to a peer, by lane: control (replication, membership) or request (forwarded client traffic, migration).",
	}, []string{"peer", "lane"})
)

// internodeInFlightObserver feeds one peer's pool into the internode
// in-flight gauge. The two series are looked up once, here, and each call
// adds to its own: after forgetInternodeInFlight removes them, a pool still
// draining adds only to series nothing exports, so a closed peer's series
// never reappear.
func internodeInFlightObserver(peerID string) func(transport.Lane, int) {
	control := internodeInFlight.WithLabelValues(peerID, transport.LaneControl.String())
	request := internodeInFlight.WithLabelValues(peerID, transport.LaneRequest.String())
	return func(lane transport.Lane, delta int) {
		if lane == transport.LaneControl {
			control.Add(float64(delta))
			return
		}
		request.Add(float64(delta))
	}
}

// forgetInternodeInFlight removes a peer's internode in-flight series.
func forgetInternodeInFlight(peerID string) {
	for _, lane := range []transport.Lane{transport.LaneControl, transport.LaneRequest} {
		internodeInFlight.DeleteLabelValues(peerID, lane.String())
	}
}

func init() {
	prometheus.MustRegister(
		requestsTotal,
		requestDuration,
		requestInFlight,
		bucketOpsTotal,
		guardTotal,
		pipelineDrainsTotal,
		pipelineDrainWaitSeconds,
		authConfigVersion,
		authConfigLag,
		authConfigDivergedMembers,
		authConfigOldestUnackedSeconds,
		applyFailuresOwed,
		applyFailuresNotReplayed,
		replicaConstraintDisagreements,
		fieldTypeRefusals,
		fieldTypeMismatchesAccepted,
		indexOrphanSkips,
		indexUniqueRefusals,
		replicaDivergences,
		indexesByState,
		ttlSweepDocuments,
		ttlSweepDuration,
		ttlSweepBudgetExhausted,
		ttlBacklogEstimate,
		reclaimPendingRecords,
		reclaimKeysDeleted,
		reclaimKeysVisited,
		reclaimFilesUnlinked,
		reclaimOldestPendingSeconds,
		responseFrameTooLarge,
		requestFaultsRecoveredDriver,
		requestFaultsRecoveredInternode,
		storeDamagedReads,
		queryResultTooLarge,
		queueClaims,
		queueClaimedEntries,
		queueLeaseSettlements,
		queueEntriesReturned,
		queueEntriesDeadLettered,
		queueLeaseClockSteps,
		queueClaimsEntryTooLarge,
		queueEmptyClaimsUnlogged,
		queueDeadRequeued,
		queueDeadDiscarded,
		queryExecuteInFlight,
		queryForwardInFlight,
		queryAdmissionWait,
		queryCancelled,
		internodeInFlight,
		batchRequestsTotal,
		batchItemsTotal,
		batchItemsPerRequest,
		batchForwardsTotal,
		batchItemsInFlight,
		batchAnswersTrimmed,
	)
}

// InstrumentHandler wraps a transport.RequestHandler so every request frame
// is recorded in Prometheus.
//
// This replaces the previous gRPC unary interceptor: the TCP transport has no
// middleware chain, so instrumentation lives on the request-handler wrapper
// that listen.go installs around NewHandler.
func InstrumentHandler(next transport.RequestHandler) transport.RequestHandler {
	return func(ctx context.Context, frame *transport.Frame) (*transport.Frame, error) {
		op := frame.Opcode.String()

		requestInFlight.Inc()
		start := time.Now()

		resp, err := next(ctx, frame)

		duration := time.Since(start).Seconds()
		requestInFlight.Dec()

		statusStr := "ok"
		if err != nil || (resp != nil && resp.Opcode == transport.OpError) {
			statusStr = "error"
		}

		requestsTotal.WithLabelValues(op, statusStr).Inc()
		requestDuration.WithLabelValues(op).Observe(duration)
		bucketOpsTotal.WithLabelValues(op).Inc()

		return resp, err
	}
}

// writeProbeResponse sets the status and body for a /health or /ready probe.
// A failed Write means the client already dropped the connection; there is
// no retry that helps, so this only logs rather than propagating an error a
// http.HandlerFunc has no return path for.
func writeProbeResponse(w http.ResponseWriter, status int, body string) {
	w.WriteHeader(status)
	if _, err := w.Write([]byte(body)); err != nil {
		slog.Default().Warn("writing probe response", "error", err)
	}
}

// probeBody is the JSON /health answers with: the verdict, who gave it, and
// the reasons when the verdict is no.
type probeBody struct {
	Healthy bool     `json:"healthy"`
	NodeID  string   `json:"node_id,omitempty"`
	Uptime  string   `json:"uptime,omitempty"`
	Reasons []string `json:"reasons,omitempty"`
}

// writeHealthProbe answers /health: 200 when the body says healthy, 503
// otherwise.
func writeHealthProbe(w http.ResponseWriter, body probeBody) {
	encoded, err := json.Marshal(body)
	if err != nil {
		slog.Default().Warn("encoding the health probe answer", "error", err)
		writeProbeResponse(w, http.StatusInternalServerError, "could not encode the health answer")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	status := http.StatusOK
	if !body.Healthy {
		status = http.StatusServiceUnavailable
	}
	writeProbeResponse(w, status, string(encoded))
}

// newMetricsMux builds the routes served by the metrics HTTP server:
// Prometheus scraping at /metrics plus the /live, /health and /ready probes. It is
// factored out of StartMetricsServer so tests can exercise the real mux
// instead of reimplementing the handlers in closures.
func newMetricsMux(server *Server) *http.ServeMux {
	mux := http.NewServeMux()
	// promhttp.Handler() with the gatherer swapped: plain HandlerFor would drop
	// promhttp_metric_handler_requests_total and _in_flight.
	// InstrumentMetricHandler reuses those counters when they are already
	// registered, so building several muxes in one process is safe.
	mux.Handle("/metrics", promhttp.InstrumentMetricHandler(prometheus.DefaultRegisterer,
		promhttp.HandlerFor(metricsGatherer(server), promhttp.HandlerOpts{})))
	mux.HandleFunc("/live", func(w http.ResponseWriter, r *http.Request) {
		// Liveness is "the process answers", nothing more: a node short of
		// disk or with a damaged store is not mended by a restart.
		writeProbeResponse(w, http.StatusOK, "alive")
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if server == nil {
			writeHealthProbe(w, probeBody{Reasons: []string{"server not initialized"}})
			return
		}
		check, err := server.HealthCheck()
		if err != nil {
			writeHealthProbe(w, probeBody{Reasons: []string{"health check failed: " + err.Error()}})
			return
		}
		writeHealthProbe(w, probeBody{Healthy: check.Healthy, NodeID: check.NodeID, Uptime: check.Uptime, Reasons: check.Reasons})
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if server == nil {
			writeProbeResponse(w, http.StatusServiceUnavailable, "server not initialized")
			return
		}
		if server.auth == nil {
			writeProbeResponse(w, http.StatusServiceUnavailable, "auth not initialized")
			return
		}
		// Ready follows the same verdict as /health, so a probe that watches
		// only readiness stops sending traffic to a node that cannot serve it.
		check, err := server.HealthCheck()
		if err != nil {
			writeProbeResponse(w, http.StatusServiceUnavailable, "health check failed: "+err.Error())
			return
		}
		if !check.Healthy {
			writeProbeResponse(w, http.StatusServiceUnavailable, "not ready: "+strings.Join(check.Reasons, "; "))
			return
		}
		writeProbeResponse(w, http.StatusOK, "ready")
	})
	return mux
}

// metricsGatherer is what /metrics serves: the default registry and, beside
// it, the server's own capacity registry (capacity_metrics.go) and its
// cluster's replication gauges (replication_metrics.go), which are per Server
// so that in-process servers keep separate series. A nil server gets the
// default registry alone; a bare one whose sampler never started has no
// capacity registry, and a standalone node has no cluster.
func metricsGatherer(server *Server) prometheus.Gatherer {
	gatherers := prometheus.Gatherers{prometheus.DefaultGatherer}
	if server == nil {
		return gatherers
	}
	if server.capacity != nil {
		gatherers = append(gatherers, server.capacity.metrics.registry)
	}
	if server.walMetrics != nil {
		gatherers = append(gatherers, server.walMetrics)
	}
	if server.seedMetrics != nil {
		gatherers = append(gatherers, server.seedMetrics)
	}
	if server.cluster != nil {
		gatherers = append(gatherers, server.cluster.metrics)
	}
	return gatherers
}

// The runtime setters, variables so a test can see what applyPProfSampling asks for.
var (
	setBlockProfileRate     = runtime.SetBlockProfileRate
	setMutexProfileFraction = runtime.SetMutexProfileFraction
)

// applyPProfSampling turns on block and mutex sampling when pprof is enabled. Without it the
// block and mutex profiles the endpoint serves are empty, which hides lock and wait costs.
// A node that did not ask for pprof keeps the runtime's default of no sampling.
func applyPProfSampling(cfg *config.Config) {
	if !cfg.Debug.PProfEnabled {
		return
	}
	setBlockProfileRate(cfg.Debug.PProfBlockRateNS)
	setMutexProfileFraction(cfg.Debug.PProfMutexFraction)
}

// pprofListen is where the pprof debug server binds when debug.pprof_enabled
// is set. A variable so a test can bind an ephemeral loopback port.
var pprofListen = "127.0.0.1:6060"

// StartMetricsServer starts an HTTP server that exposes Prometheus metrics
// on the /metrics endpoint. It is non-blocking and returns immediately.
// The server is only started if cfg.Metrics.Enabled is true; the pprof debug
// server is started when cfg.Debug.PProfEnabled is, independently.
func StartMetricsServer(cfg *config.Config, logger *slog.Logger, server *Server) error {
	// pprof has its own switch and must not depend on metrics being on: with
	// the metrics default (off) it used to be skipped by the early return
	// below, with no log line and no error. Nor may metrics depend
	// on pprof: a pprof bind failure is reported, and metrics still start.
	var pprofErr error
	if cfg.Debug.PProfEnabled {
		applyPProfSampling(cfg)
		pprofErr = startPProfServer(logger)
	}
	if !cfg.Metrics.Enabled {
		logger.Info("metrics server disabled")
		return pprofErr
	}

	listen := cfg.Metrics.Listen
	if listen == "" {
		listen = "127.0.0.1:9090"
	}

	mux := newMetricsMux(server)

	srv := &http.Server{
		Addr:              listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("metrics HTTP server listening", "address", listen)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("metrics HTTP server error", "error", err)
		}
	}()

	return pprofErr
}

// startPProfServer binds the pprof debug server and serves it in the
// background. The listener is bound before the log line, so the address
// logged is the one serving and a bind failure is an error, not a line.
func startPProfServer(logger *slog.Logger) error {
	listener, err := net.Listen("tcp", pprofListen)
	if err != nil {
		return fmt.Errorf("starting the pprof debug server on %s: %w", pprofListen, err)
	}
	pprofServer := &http.Server{ReadHeaderTimeout: 5 * time.Second}
	logger.Info("pprof debug server listening", "address", listener.Addr().String())
	go func() {
		if err := pprofServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("pprof debug server error", "error", err)
		}
	}()
	return nil
}

// observeDrain counts one write's wait for a pair's pending list to drain.
func observeDrain(waited time.Duration) {
	pipelineDrainsTotal.Inc()
	pipelineDrainWaitSeconds.Add(waited.Seconds())
}

// observeGuard counts one guarded write the ring node ordered.
func observeGuard(applied bool) {
	outcome := "refused"
	if applied {
		outcome = "applied"
	}
	guardTotal.WithLabelValues(outcome).Inc()
}
