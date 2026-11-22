package main

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

func batchOps(api ringAPI, size int) *ringOps {
	return &ringOps{api: api, batchSize: size, batch: newBatchTotals(size)}
}

var batchElement = bson.D{{Key: "k", Value: "post"}}

// TestRingBatchSendsFramesOfTheChosenSize: 150 followers at size 64 are three frames of 64, 64
// and 22, not 150 requests, and every follower's feed holds the copy once. [Send one push per
// follower: the log holds 150 pushes and no frame.]
func TestRingBatchSendsFramesOfTheChosenSize(t *testing.T) {
	model, followers := fanoutModel(t, 100, 150)
	ops := batchOps(model.connect(), 64)
	if err := ops.pushFeeds(t.Context(), followers, batchElement, &lockedLedger{}); err != nil {
		t.Fatal(err)
	}
	var sizes []int
	for _, send := range model.sends() {
		sizes = append(sizes, send.ops)
	}
	if !slices.Equal(sizes, []int{64, 64, 22}) {
		t.Errorf("frames of %v operations, want [64 64 22]", sizes)
	}
	for _, follower := range followers {
		if got := feedLength(t, model, follower); got != 1 {
			t.Fatalf("user %d feed holds %d copies, want 1", follower, got)
		}
	}
	totals := ops.batch.snapshot()
	if totals.frames != 3 || totals.ops != 150 || totals.retriedFrames != 0 || totals.refused != 0 || totals.notAttempted != 0 {
		t.Errorf("totals %+v, want 3 frames, 150 operations and nothing retried or refused", totals)
	}
}

// TestRingBatchResendsTheWholeFrameUnderTheSameIDWhenOperationsWereNotAttempted: the tail of
// the first send answers UNAVAILABLE; the frame goes again, whole, under the first id, and the
// applied operations are not applied twice. [Retry only the failed operations, or under a new
// id: the ids differ or the frame is shorter, and the head is applied twice.]
func TestRingBatchResendsTheWholeFrameUnderTheSameIDWhenOperationsWereNotAttempted(t *testing.T) {
	model, followers := fanoutModel(t, 100, 10)
	model.batchFault = func(send, op int) error {
		if send == 0 && op >= 6 {
			return &ringclient.Error{Code: cluster.CodeUnavailable, Message: "not attempted, not applied. Resend the whole frame with the same request id"}
		}
		return nil
	}
	ops := batchOps(model.connect(), 64)
	if err := ops.pushFeeds(t.Context(), followers, batchElement, &lockedLedger{}); err != nil {
		t.Fatal(err)
	}
	sends := model.sends()
	if len(sends) != 2 || sends[0].id != sends[1].id || sends[0].ops != 10 || sends[1].ops != 10 {
		t.Fatalf("frames %+v, want two of 10 operations under one id", sends)
	}
	for _, follower := range followers {
		if got := feedLength(t, model, follower); got != 1 {
			t.Errorf("user %d feed holds %d copies, want 1", follower, got)
		}
	}
	totals := ops.batch.snapshot()
	if totals.notAttempted != 4 || totals.retriedFrames != 1 || totals.frames != 2 || totals.refused != 0 {
		t.Errorf("totals %+v, want 4 not attempted, 1 retried frame, 2 sends", totals)
	}
}

// TestRingBatchCountsADefinitiveRefusalAndDoesNotResend: a refusal other than UNAVAILABLE
// cannot be cured by sending again, so the publish fails naming the follower, the refusal is
// counted, and the other operations of the frame stay applied.
func TestRingBatchCountsADefinitiveRefusalAndDoesNotResend(t *testing.T) {
	model, followers := fanoutModel(t, 100, 6)
	model.batchFault = func(_, op int) error {
		if op == 3 {
			return &ringclient.Error{Code: cluster.CodeInvalidArgument, Message: "the Feed store is full"}
		}
		return nil
	}
	ops := batchOps(model.connect(), 64)
	err := ops.pushFeeds(t.Context(), followers, batchElement, &lockedLedger{})
	var refusal *ringclient.Error
	if err == nil || !errors.As(err, &refusal) || !strings.Contains(err.Error(), "user 103") {
		t.Fatalf("error %v, want the refusal naming user 103", err)
	}
	if len(model.sends()) != 1 {
		t.Errorf("%d frames sent, want 1: a definitive refusal is not resent", len(model.sends()))
	}
	if totals := ops.batch.snapshot(); totals.refused != 1 {
		t.Errorf("refused %d, want 1", totals.refused)
	}
	if got := feedLength(t, model, 104); got != 1 {
		t.Errorf("user 104 feed holds %d copies, want 1: the others are applied", got)
	}
}

// TestRingBatchResendsAFrameWhoseAnswerWasLost: the connection failed after the server applied the
// frame; the whole frame goes again under its id and nothing is applied twice.
func TestRingBatchResendsAFrameWhoseAnswerWasLost(t *testing.T) {
	model, followers := fanoutModel(t, 100, 5)
	model.batchFailWhole = func(send int) error {
		if send == 0 {
			return errors.New("connection reset")
		}
		return nil
	}
	ops := batchOps(model.connect(), 64)
	if err := ops.pushFeeds(t.Context(), followers, batchElement, &lockedLedger{}); err != nil {
		t.Fatal(err)
	}
	for _, follower := range followers {
		if got := feedLength(t, model, follower); got != 1 {
			t.Errorf("user %d feed holds %d copies, want 1", follower, got)
		}
	}
	if totals := ops.batch.snapshot(); totals.retriedFrames != 1 {
		t.Errorf("retried frames %d, want 1", totals.retriedFrames)
	}
}

// TestRingBatchGivesUpAfterTheRetriesAndSaysSo: a frame that stays unavailable fails the publish
// after the first send and feedPushRetries resends.
func TestRingBatchGivesUpAfterTheRetriesAndSaysSo(t *testing.T) {
	model, followers := fanoutModel(t, 100, 3)
	model.batchFault = func(_, op int) error {
		if op == 2 {
			return &ringclient.Error{Code: cluster.CodeUnavailable, Message: "not attempted"}
		}
		return nil
	}
	ops := batchOps(model.connect(), 64)
	err := ops.pushFeeds(t.Context(), followers, batchElement, &lockedLedger{})
	if err == nil || !strings.Contains(err.Error(), "user 102") {
		t.Fatalf("error %v, want a failure naming user 102", err)
	}
	if got := len(model.sends()); got != 1+feedPushRetries {
		t.Errorf("%d frames sent, want %d", got, 1+feedPushRetries)
	}
}

// TestRingBatchKeepsOrderAndTellsTheLedger: a follower named twice gets its copies in list order
// (queue order inside a frame), and the ledger sees every follower sent, then delivered.
func TestRingBatchKeepsOrderAndTellsTheLedger(t *testing.T) {
	model, followers := fanoutModel(t, 100, 4)
	list := append(slices.Clone(followers), 101, 101)
	ledger := &lockedLedger{}
	if err := batchOps(model.connect(), 3).pushFeeds(t.Context(), list, batchElement, ledger); err != nil {
		t.Fatal(err)
	}
	if got := feedLength(t, model, 101); got != 3 {
		t.Errorf("user 101 feed holds %d copies, want 3", got)
	}
	if !slices.Equal(ledger.sent, list) || !slices.Equal(ledger.done, list) {
		t.Errorf("ledger sent %v delivered %v, want %v for both", ledger.sent, ledger.done, list)
	}
}

func TestRingBatchFlags(t *testing.T) {
	cfg, err := parseFlags([]string{"-engine", "ring", "-ring-batch"})
	if err != nil || !cfg.RingBatch || cfg.RingBatchSize != 64 {
		t.Fatalf("-ring-batch parsed to %+v %v, want on with the default frame of 64", cfg, err)
	}
	record := &legRecord{EngineSettings: map[string]string{}}
	if err := labelRecord(record, cfg, tinySpec); err != nil {
		t.Fatal(err)
	}
	if record.EngineSettings["ring_batch"] != "true" || record.EngineSettings["ring_batch_size"] != "64" {
		t.Errorf("settings %v, want ring_batch true and ring_batch_size 64", record.EngineSettings)
	}
	if cfg, err = parseFlags([]string{"-engine", "ring", "-ring-batch", "-ring-batch-size", "255"}); err != nil || cfg.RingBatchSize != 255 {
		t.Errorf("size 255 parsed to %+v %v", cfg, err)
	}
	for name, test := range map[string]struct {
		args []string
		want string
	}{
		"over the cap":  {[]string{"-engine", "ring", "-ring-batch", "-ring-batch-size", "256"}, "at most 255"},
		"zero":          {[]string{"-engine", "ring", "-ring-batch", "-ring-batch-size", "0"}, "at least 1"},
		"with parallel": {[]string{"-engine", "ring", "-ring-batch", "-ring-fanout-parallel"}, "-ring-fanout-parallel"},
	} {
		if _, err := parseFlags(test.args); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("%s: parseFlags(%v) = %v, want a refusal containing %q", name, test.args, err, test.want)
		}
	}
	if cfg, err = parseFlags([]string{"-engine", "mysql", "-ring-batch"}); err != nil || cfg.RingBatch || cfg.RingBatchSize != 0 {
		t.Errorf("-ring-batch leaked into a %s run: %+v %v", cfg.Engine, cfg, err)
	}
}

// TestRingBatchSummaryNamesTheFrameSize: the block printed when the engine closes has the frame
// size in its header.
func TestRingBatchSummaryNamesTheFrameSize(t *testing.T) {
	totals := newBatchTotals(64)
	totals.record(batchFrame{ops: 64, took: 2 * time.Millisecond})
	summary := totals.summary()
	for _, want := range []string{"frame size 64", "64 ops", "ops/s", "p50", "p99"} {
		if !strings.Contains(summary, want) {
			t.Errorf("summary %q lacks %q", summary, want)
		}
	}
}
