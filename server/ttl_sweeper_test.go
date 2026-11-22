package server

import (
	"context"
	"log/slog"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

// failedExpiries reads hoarddb_ttl_sweep_documents_total{outcome="failed"}.
func failedExpiries(t *testing.T) float64 {
	t.Helper()
	var metric dto.Metric
	if err := ttlSweepDocuments.WithLabelValues("failed").Write(&metric); err != nil {
		t.Fatalf("reading the failed counter: %v", err)
	}
	return metric.GetCounter().GetValue()
}

// TestTTLSweepCountsAFailedExpiryAsFailed: when the store fails
// the apply of an expiry the answer is an ordinary DeleteResponse with
// Success false. The sweep must count it failed and say why, and leave the
// document for the next pass, which then deletes it.
func TestTTLSweepCountsAFailedExpiryAsFailed(t *testing.T) {
	s := newTestServer(t)
	logger, captured := newLogCapture()
	s.logger = logger
	now := time.Now()
	createExpiringPost(t, s)
	insertPost(t, s, "stale", now.Add(-2*time.Hour))
	failedBefore := failedExpiries(t)
	// The store fails the commit of the expiry's apply, once, inside store.Apply.
	failNextApplyOn(t, s.kv, anySeq)
	pass := s.sweepTTL(context.Background(), now)
	if documentOf(t, s, "app", "Post", "stale") == nil {
		t.Fatalf("the injected commit failure did not keep the document (pass %+v): the test does not exercise a failed expiry", pass)
	}
	if pass.deleted != 0 || pass.failed != 1 {
		t.Errorf("the pass reports %+v for an expiry whose commit failed, want 0 deleted and 1 failed", pass)
	}
	if got := failedExpiries(t) - failedBefore; got != 1 {
		t.Errorf("the failed counter moved by %v, want 1", got)
	}
	if lines := captured.messages(slog.LevelWarn, "TTL sweep could not expire a document"); len(lines) != 1 {
		t.Errorf("the failed expiry wrote %d warning lines, want 1", len(lines))
	}

	retry := s.sweepTTL(context.Background(), now)
	if retry.deleted != 1 || retry.failed != 0 || documentOf(t, s, "app", "Post", "stale") != nil {
		t.Errorf("the next pass reports %+v, want the document deleted", retry)
	}
}
