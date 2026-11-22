package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeStore is an in-memory store: no database, no network.
type fakeStore struct {
	label     string
	mu        sync.Mutex
	keys      map[int64]bool
	calls     [opKinds]int
	prepared  bool
	failGets  bool
	loadCalls [][2]int64
}

func newFakeStore(label string, records int64) *fakeStore {
	keys := map[int64]bool{}
	for id := range records {
		keys[id] = true
	}
	return &fakeStore{label: label, keys: keys}
}

func (f *fakeStore) name() string       { return f.label }
func (f *fakeStore) durability() string { return "fake" }
func (f *fakeStore) close()             {}
func (f *fakeStore) prepare(context.Context) error {
	f.prepared = true
	return nil
}
func (f *fakeStore) nextKey(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return int64(len(f.keys)), nil
}
func (f *fakeStore) loadRange(_ context.Context, from, to int64, _ int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadCalls = append(f.loadCalls, [2]int64{from, to})
	for id := from; id < to; id++ {
		f.keys[id] = true
	}
	return nil
}
func (f *fakeStore) count(kind opKind) {
	f.mu.Lock()
	f.calls[kind]++
	f.mu.Unlock()
}
func (f *fakeStore) get(context.Context, int64) error {
	f.count(opPointRead)
	if f.failGets {
		return errors.New("boom")
	}
	return nil
}
func (f *fakeStore) update(context.Context, int64, int64, int) error {
	f.count(opUpdate)
	return nil
}
func (f *fakeStore) insert(_ context.Context, id int64, _ int) error {
	f.count(opInsert)
	f.mu.Lock()
	f.keys[id] = true
	f.mu.Unlock()
	return nil
}
func (f *fakeStore) byOwner(context.Context, int64) (int, error) {
	f.count(opOwnerRead)
	return ownerFanout, nil
}

func TestParsePointsStrictlyIncreasing(t *testing.T) {
	points, err := parsePoints("10000, 30000,100000")
	if err != nil || len(points) != 3 || points[2] != 100000 {
		t.Fatalf("parsePoints = %v, %v", points, err)
	}
	for _, bad := range []string{"10,10", "30,10", "0,5", "5,x", ""} {
		if _, err := parsePoints(bad); err == nil {
			t.Errorf("parsePoints(%q) accepted a list that is not strictly increasing positive numbers", bad)
		}
	}
}

func TestLoadChunksCoverTheRangeOnce(t *testing.T) {
	ranges := loadChunks(100, 1000, 250)
	want := [][2]int64{{100, 350}, {350, 600}, {600, 850}, {850, 1000}}
	if len(ranges) != len(want) {
		t.Fatalf("loadChunks = %v, want %v", ranges, want)
	}
	for i := range want {
		if ranges[i] != want[i] {
			t.Fatalf("chunk %d = %v, want %v", i, ranges[i], want[i])
		}
	}
	if len(loadChunks(500, 500, 100)) != 0 {
		t.Error("an empty growth must produce no chunk")
	}
}

func TestMixIsHalfReadsHalfWrites(t *testing.T) {
	var counts [opKinds]int
	for draw := range mixTotal {
		counts[pickOp(draw)]++
	}
	if reads := counts[opPointRead] + counts[opOwnerRead]; reads != mixTotal/2 {
		t.Errorf("reads = %d per mille, want %d", reads, mixTotal/2)
	}
	if writes := counts[opUpdate] + counts[opInsert]; writes != mixTotal/2 {
		t.Errorf("writes = %d per mille, want %d", writes, mixTotal/2)
	}
	if counts[opOwnerRead] == 0 || counts[opInsert] == 0 {
		t.Error("the secondary-index read and the insert must both be in the mix")
	}
}

func TestBodyIsExactSizeAndDeterministic(t *testing.T) {
	for _, size := range []int{1, 7, 1024, 4096} {
		if got := len(bodyFor(42, 3, size)); got != size {
			t.Errorf("body for size %d has %d bytes", size, got)
		}
	}
	first, again := bodyFor(1, 0, 64), bodyFor(1, 0, 64)
	if first != again || first == bodyFor(2, 0, 64) {
		t.Error("a body must repeat for one key and version and differ between keys")
	}
	if ownerOf(0) != ownerOf(ownerFanout-1) || ownerOf(ownerFanout) == ownerOf(0) {
		t.Error("ownerFanout consecutive keys must share an owner")
	}
}

func TestLegCapsInsertsAndRecordsTheEngineLabel(t *testing.T) {
	db := newFakeStore("mongo", 1000)
	cfg := legConfig{Records: 1000, Workers: 4, Warmup: 20 * time.Millisecond, Measured: 150 * time.Millisecond, BodyBytes: 16, Seed: 3}
	counters, inserted, measured := runLeg(context.Background(), db, cfg)
	if limit := 2 * (cfg.Records / insertCapDiv); inserted > limit {
		t.Errorf("inserted %d records, cap is %d (warm-up and measured window)", inserted, limit)
	}
	record := buildRecord(db, cfg, counters, inserted, measured)
	if record.Engine != "mongo" {
		t.Errorf("record engine = %q, want the store's own label", record.Engine)
	}
	if record.ByKind["insert"].Count == 0 {
		t.Error("the measured window must contain inserts, not only the warm-up")
	}
	if record.Ops == 0 || record.OpsPerS <= 0 {
		t.Errorf("leg recorded no operations: %+v", record)
	}
	if len(record.Errors) != 0 {
		t.Errorf("unexpected errors %v", record.Errors)
	}
}

func TestLegCountsErrorsAndKeepsRunning(t *testing.T) {
	db := newFakeStore("mysql", 100)
	db.failGets = true
	cfg := legConfig{Records: 100, Workers: 2, Warmup: 5 * time.Millisecond, Measured: 60 * time.Millisecond, BodyBytes: 8, Seed: 1}
	counters, _, _ := runLeg(context.Background(), db, cfg)
	if counters.errors["boom"] == 0 {
		t.Error("failed reads must be counted, not dropped")
	}
}

func TestLegModeWritesOneRecordWithNoAddress(t *testing.T) {
	db := newFakeStore("mysql", 500)
	var out bytes.Buffer
	opt := sweepFlags{mode: "leg", workers: 2, warmup: 5 * time.Millisecond, measured: 50 * time.Millisecond, bodyBytes: 8, seed: 1}
	if err := runWith(context.Background(), db, opt, &out); err != nil {
		t.Fatal(err)
	}
	if !db.prepared {
		t.Error("the leg must create its schema when missing")
	}
	var record map[string]any
	if err := json.Unmarshal(out.Bytes(), &record); err != nil {
		t.Fatalf("leg output is not one JSON object: %v: %s", err, out.String())
	}
	if record["engine"] != "mysql" || record["schema"] != "scale-sweep-leg/1" {
		t.Errorf("record = %v", record)
	}
	if regexp.MustCompile(`\d+\.\d+\.\d+\.\d+`).MatchString(out.String()) {
		t.Error("the leg record carries an address")
	}
}

func TestLoadModeGrowsFromWhereTheStoreEnds(t *testing.T) {
	db := newFakeStore("mongo", 0)
	var out bytes.Buffer
	if err := runWith(context.Background(), db, sweepFlags{mode: "load", from: 0, to: 2500, bodyBytes: 8}, &out); err != nil {
		t.Fatal(err)
	}
	next := newFakeStore("mongo", 2500)
	if err := runWith(context.Background(), next, sweepFlags{mode: "next"}, &out); err != nil || strings.TrimSpace(out.String()) != "2500" {
		t.Fatalf("next = %q, %v", out.String(), err)
	}
	if err := runWith(context.Background(), db, sweepFlags{mode: "load", from: 10, to: 10}, &out); err == nil {
		t.Error("an empty range must be refused")
	}
	if err := runWith(context.Background(), db, sweepFlags{mode: "leg"}, &out); err != nil && !strings.Contains(err.Error(), "holds no records") {
		t.Errorf("leg on loaded store: %v", err)
	}
	if err := runWith(context.Background(), newFakeStore("mongo", 0), sweepFlags{mode: "leg"}, &out); err == nil || !strings.Contains(err.Error(), "load some first") {
		t.Errorf("a leg on an empty store must say how to fix it, got %v", err)
	}
}

func TestOpenStoreRefusesUnknownEngine(t *testing.T) {
	_, err := openStore(context.Background(), sweepFlags{engine: "oracle", fanout: 10})
	if err == nil || !strings.Contains(err.Error(), "use mongo, mysql or hoarddb") {
		t.Errorf("error = %v", err)
	}
}

func TestParallelBatchesCoverEveryKeyOnce(t *testing.T) {
	var mu sync.Mutex
	seen := map[int64]int{}
	err := parallelBatches(context.Background(), 5, 3500, func(from, to int64) error {
		mu.Lock()
		defer mu.Unlock()
		for id := from; id < to; id++ {
			seen[id]++
		}
		return nil
	})
	if err != nil || len(seen) != 3495 {
		t.Fatalf("err %v, keys %d", err, len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("key %d loaded %d times", id, n)
		}
	}
}
