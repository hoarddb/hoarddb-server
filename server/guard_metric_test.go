package server

import (
	"testing"

	dto "github.com/prometheus/client_model/go"
)

func guardCount(t *testing.T, outcome string) float64 {
	t.Helper()
	var metric dto.Metric
	if err := guardTotal.WithLabelValues(outcome).Write(&metric); err != nil {
		t.Fatalf("reading the %s counter: %v", outcome, err)
	}
	return metric.GetCounter().GetValue()
}

func TestGuardMetricCountsAppliedAndRefused(t *testing.T) {
	applied, refused := guardCount(t, "applied"), guardCount(t, "refused")
	observeGuard(true)
	observeGuard(true)
	observeGuard(false)
	if got := guardCount(t, "applied") - applied; got != 2 {
		t.Errorf("applied rose by %v, want 2", got)
	}
	if got := guardCount(t, "refused") - refused; got != 1 {
		t.Errorf("refused rose by %v, want 1", got)
	}
}
