package main

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

// The witness rule (coordinator ruling 2026-10-24): the reference is the
// cheapest data-only flush on the disk (pwrite + fdatasync to a preallocated
// block), and a depth-1 write passes when its median is at least
// witnessMinRatio of it. Measured on a quiet VM: durable hoardDB L-A 1.8 to
// 2.1 ms against 2.8 to 3.4 ms (ratio 0.61 to 0.82); the unsafe path 0.13 to
// 0.27 ms (ratio 0.04 to 0.09).

// durableFigures are a quiet VM's three sync figures: the data-only flush that is
// the reference, the O_DSYNC overwrite and the O_DSYNC append.
var durableFigures = syncFigures{DatasyncMs: 2.9, OverwriteMs: 3.2, AppendMs: 4.1}

func TestWitnessPassesDurableAgainstDatasyncReference(t *testing.T) {
	result := judgeWitness(engineHoardDB, "L-A", repeated(1800*time.Microsecond, 200), durableFigures)
	if !result.Pass || len(result.IntegrityFailures) != 0 {
		t.Fatalf("a durable p50 of 1.8 ms failed against a datasync reference of 2.9 ms: %+v", result)
	}
	if result.SingleSyncMsP50 != 2.9 || result.SyncOverwriteMsP50 != 3.2 || result.SingleSyncAppendMs != 4.1 {
		t.Errorf("figures recorded %v, %v, %v; want the datasync 2.9 as the reference with the overwrite 3.2 and append 4.1 beside it",
			result.SingleSyncMsP50, result.SyncOverwriteMsP50, result.SingleSyncAppendMs)
	}
	if result.P50OverSingleSync < 0.61 || result.P50OverSingleSync > 0.63 {
		t.Errorf("ratio %v; want 1.8 / 2.9 = 0.62", result.P50OverSingleSync)
	}
	if result.MinRatio != witnessMinRatio {
		t.Errorf("min ratio %v printed; want %v", result.MinRatio, witnessMinRatio)
	}
	line, err := json.Marshal(result)
	for _, want := range []string{`"single_sync_ms_p50":2.9`, `"sync_overwrite_ms_p50":3.2`, `"single_sync_append_ms_p50":4.1`, `"p50_over_single_sync"`, `"witness_min_ratio":0.3`} {
		if err != nil || !strings.Contains(string(line), want) {
			t.Errorf("printed line %s (%v) lacks %s", line, err, want)
		}
	}
}

func TestWitnessFailsTheUnsafePath(t *testing.T) {
	for _, level := range []string{"L-A", "L-R"} {
		result := judgeWitness(engineHoardDB, level, repeated(130*time.Microsecond, 200), durableFigures)
		if result.Pass || len(result.IntegrityFailures) != 1 {
			t.Errorf("an unsafe p50 of 0.13 ms passed at %s: %+v", level, result)
		}
	}
}

// TestWitnessThresholdIsThreeTenths pins the constant and both sides of the line.
// A threshold of 1.0 fails the durable 1.8 ms case above; one of 0.1 passes the
// unsafe 0.13 ms case; the boundary itself passes.
func TestWitnessThresholdIsThreeTenths(t *testing.T) {
	if witnessMinRatio != 0.3 {
		t.Fatalf("witnessMinRatio = %v; the measured gap puts it at 0.3", witnessMinRatio)
	}
	figures := syncFigures{DatasyncMs: 2.0, OverwriteMs: 2.5, AppendMs: 3.0}
	cases := []struct {
		median   time.Duration
		wantPass bool
	}{
		{590 * time.Microsecond, false},
		{600 * time.Microsecond, true},
		{610 * time.Microsecond, true},
		{260 * time.Microsecond, false},
	}
	for _, tc := range cases {
		if got := judgeWitness(engineMongo, "L-A", repeated(tc.median, 200), figures).Pass; got != tc.wantPass {
			t.Errorf("p50 %v against 2.0 ms: pass = %v; want %v", tc.median, got, tc.wantPass)
		}
	}
}

// TestWitnessReferenceIsNotTheDsyncFigures: with a disk whose O_DSYNC overwrite
// and append are slow, a durable 1.8 ms write still passes, because the
// reference is the data-only flush. Taking the reference from either O_DSYNC
// figure would fail it (1.8 < 0.3 x 8.0).
func TestWitnessReferenceIsNotTheDsyncFigures(t *testing.T) {
	figures := syncFigures{DatasyncMs: 2.9, OverwriteMs: 8.0, AppendMs: 9.0}
	if result := judgeWitness(engineHoardDB, "L-A", repeated(1800*time.Microsecond, 200), figures); !result.Pass {
		t.Errorf("the reference came from an O_DSYNC figure, not the datasync one: %+v", result)
	}
	// And the other direction: an unsafe write cannot hide behind a tiny O_DSYNC figure.
	figures = syncFigures{DatasyncMs: 2.9, OverwriteMs: 0.1, AppendMs: 0.1}
	if result := judgeWitness(engineHoardDB, "L-A", repeated(130*time.Microsecond, 200), figures); result.Pass {
		t.Errorf("an unsafe p50 passed because the O_DSYNC figures were small: %+v", result)
	}
}

func TestWitnessRelaxedLevelPassesFastWithANote(t *testing.T) {
	result := judgeWitness(engineHoardDB, "L-B", repeated(130*time.Microsecond, 200), durableFigures)
	if !result.Pass || !result.ExpectedRelaxed {
		t.Errorf("L-B and fast must pass as expected-relaxed: %+v", result)
	}
}

func TestWitnessWithoutOptionalFigures(t *testing.T) {
	result := judgeWitness(engineHoardDB, "L-A", repeated(1800*time.Microsecond, 200), syncFigures{DatasyncMs: 2.9})
	if result.SyncOverwriteMsP50 != 0 || result.SingleSyncAppendMs != 0 {
		t.Errorf("figures appeared that nobody passed: %+v", result)
	}
}

// TestSyncProbeMeasuresThreeFigures: the probe writes in a directory it cleans
// up and reports three positive medians, the data-only flush among them.
func TestSyncProbeMeasuresThreeFigures(t *testing.T) {
	dir := t.TempDir()
	var out bytes.Buffer
	if err := runSyncProbe(&config{SyncDir: dir}, &out); err != nil {
		t.Fatalf("syncprobe: %v", err)
	}
	var probed syncProbeResult
	if err := json.Unmarshal(out.Bytes(), &probed); err != nil || probed.Mode != modeSyncProbe ||
		probed.OverwriteMsP50 <= 0 || probed.AppendMsP50 <= 0 || probed.DatasyncMsP50 <= 0 {
		t.Errorf("printed %q (%v); want the datasync, overwrite and append medians positive", out.String(), err)
	}
	if !strings.Contains(out.String(), `"sync_datasync_ms_p50"`) {
		t.Errorf("printed %q; want sync_datasync_ms_p50", out.String())
	}
	if left, _ := os.ReadDir(dir); len(left) != 0 {
		t.Errorf("the probe left %d files behind", len(left))
	}
	if err := runSyncProbe(&config{}, &out); err == nil {
		t.Error("syncprobe without -sync-dir succeeded")
	}
}

// TestLegRecordCarriesAllSyncFiguresAndTheRatio: the leg's witness object has the
// datasync reference, the two O_DSYNC figures and the ratio.
func TestLegRecordCarriesAllSyncFiguresAndTheRatio(t *testing.T) {
	cfg := mixTestConfig("rh", "social_sync")
	cfg.SyncMS, cfg.SyncOverwriteMS, cfg.SyncAppendMS, cfg.WitnessP50MS = 2.9, 3.2, 4.1, 1.8
	record := completedMixRecord(t, fakeHoardEngine(cfg), cfg)
	if record.Witness.SingleSyncMsP50 != 2.9 || record.Witness.SyncOverwriteMsP50 != 3.2 || record.Witness.SingleSyncAppendMsP50 != 4.1 {
		t.Errorf("record witness %+v; want 2.9, 3.2 and 4.1", record.Witness)
	}
	if record.Witness.Depth1WriteP50Ms != 1.8 || record.Witness.P50OverSingleSync < 0.61 || record.Witness.P50OverSingleSync > 0.63 {
		t.Errorf("record witness %+v must carry the witness median 1.8 and its ratio 0.62 to the reference", record.Witness)
	}
}
