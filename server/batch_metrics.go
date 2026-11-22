package server

import "github.com/prometheus/client_golang/prometheus"

// The BATCH metrics (batch-request spec §6). Aggregates only: no bucket or
// database label, so 10^6 buckets add no series.
var (
	batchRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "hoarddb_batch_requests_total",
		Help: "BATCH requests by outcome: ok (every item a success), partial (some item not), refused (refused whole; nothing ran).",
	}, []string{"outcome"})
	batchItemsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "hoarddb_batch_items_total",
		Help: "BATCH items by opcode and outcome: ok, not_applied, applied (stored, write concern not met), unknown, verb_failed (the verb answered success false), read_failed.",
	}, []string{"opcode", "outcome"})
	batchItemsPerRequest = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "hoarddb_batch_items",
		Help:    "Items per BATCH request that ran.",
		Buckets: []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000},
	})
	batchForwardsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hoarddb_batch_forwards_total",
		Help: "Sub-batches this node sent to bucket owners, one per remote owner per BATCH.",
	})
	batchItemsInFlight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "hoarddb_batch_items_in_flight",
		Help: "BATCH items running on this node, local and forwarded, against the node-wide bound of 256.",
	})
	batchAnswersTrimmed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "hoarddb_batch_answers_trimmed_total",
		Help: "BATCH answers replaced under the reply budget: a read answered RESULT_TOO_LARGE, or an update answered without its document (doc_omitted).",
	}, []string{"kind"})
)
