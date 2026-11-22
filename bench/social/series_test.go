package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// parsedSeriesLine is one line of a -series-out file.
type parsedSeriesLine struct {
	Second int      `json:"s"`
	Reads  int64    `json:"reads"`
	Writes int64    `json:"writes"`
	LenS   *float64 `json:"len_s"`
}

// readSeries parses a series file, one JSON line per second.
func readSeries(t *testing.T, path string) []parsedSeriesLine {
	t.Helper()
	text, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("the leg wrote no series file: %v", err)
	}
	var lines []parsedSeriesLine
	for _, raw := range strings.Split(strings.TrimSpace(string(text)), "\n") {
		if strings.HasPrefix(raw, `{"per_client"`) {
			continue // the closing line, read by readPerClient
		}
		var line parsedSeriesLine
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			t.Fatalf("series line %q is not JSON: %v", raw, err)
		}
		lines = append(lines, line)
	}
	return lines
}

// TestSeriesSumsToTheLegRecord (T-H1): one line per second, seconds counted from
// the measured start, warm-up seconds negative, and the measured lines add up to
// the very counts the record's reads_per_s and writes_per_s were derived from.
func TestSeriesSumsToTheLegRecord(t *testing.T) {
	cfg := mixTestConfig("sk", "social_series_sum")
	cfg.Warmup, cfg.Duration = 2*time.Second, 3*time.Second
	cfg.SeriesOut = filepath.Join(t.TempDir(), "series.jsonl")
	record, err := runMixLeg(context.Background(), cfg, tinySpec, fakeHoardEngine(cfg))
	if err != nil {
		t.Fatalf("running the leg: %v", err)
	}
	lines := readSeries(t, cfg.SeriesOut)
	var seconds []int
	var reads, writes, warmOps int64
	for _, line := range lines {
		seconds = append(seconds, line.Second)
		if line.Second >= 0 {
			reads, writes = reads+line.Reads, writes+line.Writes
		} else {
			warmOps += line.Reads + line.Writes
		}
		if line.LenS != nil {
			t.Errorf("second %d carries len_s %v although the measured time is a whole number of seconds", line.Second, *line.LenS)
		}
	}
	if want := []int{-2, -1, 0, 1, 2}; !reflect.DeepEqual(seconds, want) {
		t.Errorf("series seconds = %v; want %v", seconds, want)
	}
	wantReads := int64(math.Round(record.ReadsPerSec * record.MeasuredS))
	wantWrites := int64(math.Round(record.WritesPerSec * record.MeasuredS))
	if reads != wantReads || writes != wantWrites || reads == 0 || writes == 0 {
		t.Errorf("measured lines hold %d reads and %d writes; the record's rates give %d and %d", reads, writes, wantReads, wantWrites)
	}
	if warmOps == 0 {
		t.Error("the warm-up seconds hold no operation, so a warm-up operation was dropped or counted into second 0")
	}
}

// TestSeriesBucketsByCompletion: every call takes 1.2 s, so nothing that starts in
// the measured window can finish in its first second; an operation counted where
// it started would put its count there.
func TestSeriesBucketsByCompletion(t *testing.T) {
	fake := newFakeServer()
	cfg := mixTestConfig("op:R2", "social_series_completion")
	engine := fakeHoardEngineOn(cfg, fake)
	if _, err := engine.setupSocial(context.Background(), tinySpec); err != nil {
		t.Fatal(err)
	}
	if err := engine.seedSocial(context.Background(), newSocialGraph(tinySpec, datasetSeed), 1); err != nil {
		t.Fatal(err)
	}
	// The fake sleeps callDelay only on a connection that counts calls in flight.
	var inFlight, maxInFlight atomic.Int64
	engine.dial = func(string) (wireCaller, func(), error) { return fake.dial(&inFlight, &maxInFlight), func() {}, nil }
	cfg.Attach, cfg.Warmup, cfg.Duration = true, 0, 3*time.Second
	cfg.SeriesOut = filepath.Join(t.TempDir(), "series.jsonl")
	fake.callDelay.Store(int64(1200 * time.Millisecond))
	if _, err := runMixLeg(context.Background(), cfg, tinySpec, engine); err != nil {
		t.Fatalf("running the leg: %v", err)
	}
	var total int64
	for _, line := range readSeries(t, cfg.SeriesOut) {
		if line.Second == 0 && line.Reads+line.Writes != 0 {
			t.Errorf("second 0 holds %d operations although no call lasts under 1.2 s", line.Reads+line.Writes)
		}
		total += line.Reads + line.Writes
	}
	if total == 0 {
		t.Error("the series holds no operation at all")
	}
}

// TestSeriesMarksAPartialLastSecond: a measured time that is not whole puts its
// remainder on the last line as len_s, and on no other.
func TestSeriesMarksAPartialLastSecond(t *testing.T) {
	cfg := mixTestConfig("sk", "social_series_partial")
	cfg.Duration = 1500 * time.Millisecond
	cfg.SeriesOut = filepath.Join(t.TempDir(), "series.jsonl")
	if _, err := runMixLeg(context.Background(), cfg, tinySpec, fakeHoardEngine(cfg)); err != nil {
		t.Fatalf("running the leg: %v", err)
	}
	lines := readSeries(t, cfg.SeriesOut)
	last := lines[len(lines)-1]
	if last.Second != 1 || last.LenS == nil || math.Abs(*last.LenS-0.5) > 1e-9 {
		t.Fatalf("last line = %+v; want second 1 with len_s 0.5", last)
	}
	for _, line := range lines[:len(lines)-1] {
		if line.LenS != nil {
			t.Errorf("second %d carries len_s; only the last line may", line.Second)
		}
	}
}

// TestSeriesWrittenOnAFailedLeg: a leg that ends in an error, on either path the
// kill cycle meets, still leaves a complete series file (BD-2).
func TestSeriesWrittenOnAFailedLeg(t *testing.T) {
	t.Run("the ledger cannot be written", func(t *testing.T) {
		cfg := mixTestConfig("sk", "social_series_ledger")
		cfg.LedgerOut = filepath.Join(t.TempDir(), "missing-directory", "ledger.json")
		cfg.SeriesOut = filepath.Join(t.TempDir(), "series.jsonl")
		if _, err := runMixLeg(context.Background(), cfg, tinySpec, fakeHoardEngine(cfg)); err == nil {
			t.Fatal("the leg succeeded although its ledger path cannot be written, so this pins nothing")
		}
		if lines := readSeries(t, cfg.SeriesOut); len(lines) == 0 {
			t.Error("the series file is empty")
		}
	})
	t.Run("a read turns wrong", func(t *testing.T) {
		fake := newFakeServer()
		cfg := mixTestConfig("rh", "social_series_wrong")
		cfg.Duration = 1500 * time.Millisecond
		cfg.SeriesOut = filepath.Join(t.TempDir(), "series.jsonl")
		engine := fakeHoardEngineOn(cfg, fake)
		timer := time.AfterFunc(300*time.Millisecond, func() {
			fake.mutex.Lock()
			fake.reverseFeedPeeks = true
			fake.mutex.Unlock()
		})
		defer timer.Stop()
		if _, err := runMixLeg(context.Background(), cfg, tinySpec, engine); err == nil {
			t.Fatal("the leg against a server that turns wrong succeeded, so this pins nothing")
		}
		lines := readSeries(t, cfg.SeriesOut)
		var operations int64
		for _, line := range lines {
			operations += line.Reads + line.Writes
		}
		if len(lines) == 0 || operations == 0 {
			t.Errorf("the series of a failed leg holds %d lines and %d operations", len(lines), operations)
		}
	})
}

// TestSeriesNeedsAMix: -series-out without a -mix leg is refused, naming both.
func TestSeriesNeedsAMix(t *testing.T) {
	if _, err := parseFlags([]string{"-series-out", "series.jsonl"}); err == nil || !strings.Contains(err.Error(), "-series-out") || !strings.Contains(err.Error(), "-mix") {
		t.Errorf("-series-out alone: %v; want a refusal naming -series-out and -mix", err)
	}
	if _, err := parseFlags([]string{"-mix", "rh", "-series-out", "series.jsonl"}); err != nil {
		t.Errorf("-series-out with -mix: %v; want it accepted", err)
	}
}

// flattenRecord reduces a leg record to its key paths and the kind of value at
// each, so timing, date and rate fields cannot make two records differ.
func flattenRecord(prefix string, value any, into map[string]string) {
	switch typed := value.(type) {
	case map[string]any:
		for key, inner := range typed {
			flattenRecord(prefix+"."+key, inner, into)
		}
	case []any:
		into[prefix] = "array"
	case string:
		into[prefix] = "string"
	case float64:
		into[prefix] = "number"
	case bool:
		into[prefix] = "bool"
	default:
		into[prefix] = "null"
	}
}

// TestSeriesOffLeavesTheRecordUnchanged is a regression guard, green on the
// parent: without -series-out no file appears and the record has the key paths
// and value kinds it had before the flag existed (testdata/leg-record-shape.txt).
func TestSeriesOffLeavesTheRecordUnchanged(t *testing.T) {
	golden, err := os.ReadFile(filepath.Join("testdata", "leg-record-shape.txt"))
	if err != nil {
		t.Fatalf("reading the golden record shape: %v", err)
	}
	directory := t.TempDir()
	t.Chdir(directory)
	cfg := mixTestConfig("op:R3", "social_series_off")
	record := completedMixRecord(t, fakeHoardEngine(cfg), cfg)
	var encoded bytes.Buffer
	if err := writeLegRecord(&encoded, record); err != nil {
		t.Fatalf("writing the leg record: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded.Bytes(), &decoded); err != nil {
		t.Fatalf("the record is not JSON: %v", err)
	}
	shape := map[string]string{}
	flattenRecord("", decoded, shape)
	var got []string
	for path, kind := range shape {
		got = append(got, path+" "+kind)
	}
	sort.Strings(got)
	if want := strings.Split(strings.TrimSpace(string(golden)), "\n"); !reflect.DeepEqual(got, want) {
		t.Errorf("record shape changed:\n got %s\nwant %s", strings.Join(got, "\n    "), strings.Join(want, "\n    "))
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 0 {
		t.Errorf("a leg without -series-out left %d files in its working directory (%v)", len(entries), err)
	}
}

// parsedPerClientLine is the closing line of a -series-out file (BD-3).
type parsedPerClientLine struct {
	PerClient struct {
		Reads  []int64 `json:"reads"`
		Writes []int64 `json:"writes"`
	} `json:"per_client"`
}

// readPerClient parses the series file's last line.
func readPerClient(t *testing.T, path string) parsedPerClientLine {
	t.Helper()
	text, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("the leg wrote no series file: %v", err)
	}
	rows := strings.Split(strings.TrimSpace(string(text)), "\n")
	var line parsedPerClientLine
	if err := json.Unmarshal([]byte(rows[len(rows)-1]), &line); err != nil || line.PerClient.Reads == nil {
		t.Fatalf("the series file does not end in a per_client line: last line %q (%v)", rows[len(rows)-1], err)
	}
	return line
}

// stalledCaller holds every call until a deadline and then fails it, as a load
// client stuck behind a dead node does until the leg is over.
type stalledCaller struct {
	wireCaller
	until time.Time
}

func (c stalledCaller) call(transport.Opcode, any, any) error {
	time.Sleep(time.Until(c.until))
	return errors.New("connection reset by peer")
}

// TestSeriesRecordsEachClientsCount (T-H2): one load client is stuck for the
// whole window, so its counts are 0 and the others' are not; the entries add up
// to the measured lines, which a merged count written once could not show.
func TestSeriesRecordsEachClientsCount(t *testing.T) {
	fake := newFakeServer()
	cfg := mixTestConfig("sk", "social_series_clients")
	cfg.Warmup, cfg.Duration = 0, time.Second
	engine := fakeHoardEngineOn(cfg, fake)
	if _, err := engine.setupSocial(context.Background(), tinySpec); err != nil {
		t.Fatal(err)
	}
	if err := engine.seedSocial(context.Background(), newSocialGraph(tinySpec, datasetSeed), 1); err != nil {
		t.Fatal(err)
	}
	cfg.Attach = true
	cfg.SeriesOut = filepath.Join(t.TempDir(), "series.jsonl")
	// Past the leg's end, so the stuck client's failed call cannot be retried inside the window.
	until := time.Now().Add(cfg.Duration + 300*time.Millisecond)
	var dials atomic.Int64
	engine.dial = func(string) (wireCaller, func(), error) {
		wire := fake.dial(nil, nil)
		if dials.Add(1) == 3 {
			return stalledCaller{wireCaller: wire, until: until}, func() {}, nil
		}
		return wire, func() {}, nil
	}
	if _, err := runMixLeg(context.Background(), cfg, tinySpec, engine); err != nil {
		t.Fatalf("running the leg: %v", err)
	}
	perClient := readPerClient(t, cfg.SeriesOut).PerClient
	if len(perClient.Reads) != cfg.Conc || len(perClient.Writes) != cfg.Conc {
		t.Fatalf("per_client has %d read and %d write entries; want one per load client (%d)", len(perClient.Reads), len(perClient.Writes), cfg.Conc)
	}
	var reads, writes, idle int64
	for client := range perClient.Reads {
		reads, writes = reads+perClient.Reads[client], writes+perClient.Writes[client]
		if perClient.Reads[client]+perClient.Writes[client] == 0 {
			idle++
		}
	}
	var wantReads, wantWrites int64
	for _, line := range readSeries(t, cfg.SeriesOut) {
		if line.Second >= 0 {
			wantReads, wantWrites = wantReads+line.Reads, wantWrites+line.Writes
		}
	}
	if idle != 1 {
		t.Errorf("%d clients counted nothing; want exactly the stuck one (reads %v, writes %v)", idle, perClient.Reads, perClient.Writes)
	}
	if reads != wantReads || writes != wantWrites || reads+writes == 0 {
		t.Errorf("per_client sums to %d reads and %d writes; the measured lines hold %d and %d", reads, writes, wantReads, wantWrites)
	}
}

// TestSeriesPerClientLeavesOutWarmUp: with a warm-up, every client's count is
// its measured operations only, so the entries add up to the lines from second 0
// and fall short of the total that includes the negative seconds.
func TestSeriesPerClientLeavesOutWarmUp(t *testing.T) {
	cfg := mixTestConfig("sk", "social_series_clients_warm")
	cfg.Warmup, cfg.Duration = time.Second, 2*time.Second
	cfg.SeriesOut = filepath.Join(t.TempDir(), "series.jsonl")
	if _, err := runMixLeg(context.Background(), cfg, tinySpec, fakeHoardEngine(cfg)); err != nil {
		t.Fatalf("running the leg: %v", err)
	}
	var measured, everything int64
	for _, line := range readSeries(t, cfg.SeriesOut) {
		everything += line.Reads + line.Writes
		if line.Second >= 0 {
			measured += line.Reads + line.Writes
		}
	}
	perClient := readPerClient(t, cfg.SeriesOut).PerClient
	var counted int64
	for client := range perClient.Reads {
		counted += perClient.Reads[client] + perClient.Writes[client]
	}
	if counted != measured || measured == 0 {
		t.Errorf("per_client sums to %d operations; the lines from second 0 hold %d", counted, measured)
	}
	if counted >= everything {
		t.Errorf("per_client sums to %d, not less than the %d of all lines, so warm-up operations leaked in (or none ran)", counted, everything)
	}
}
