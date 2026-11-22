package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSplitOpsSumsExactlyAndEvenly(t *testing.T) {
	share := splitOps(20003, 16)
	total, low, high := 0, 1<<30, 0
	for _, n := range share {
		total += n
		low, high = min(low, n), max(high, n)
	}
	if total != 20003 || high-low > 1 {
		t.Fatalf("shares %v total %d", share, total)
	}
}

// fakeWorker stores ids in a shared map, so gets find what inserts wrote.
type fakeWorker struct {
	store   *fakeStore
	failOn  int64
	counter *atomic.Int64
	closed  *atomic.Int64
}

type fakeStore struct {
	mu   sync.Mutex
	docs map[string]string
}

func (w *fakeWorker) insert(_ context.Context, id, payload string) error {
	if w.failOn > 0 && w.counter.Add(1) == w.failOn {
		return errors.New("disk on fire")
	}
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	w.store.docs[id] = payload
	return nil
}

func (w *fakeWorker) get(_ context.Context, id string) error {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	if _, ok := w.store.docs[id]; !ok {
		return errNotFound
	}
	return nil
}

func (w *fakeWorker) close() { w.closed.Add(1) }

type fakeEngine struct {
	store   *fakeStore
	vars    map[string]string
	failOn  int64
	counter atomic.Int64
	closed  atomic.Int64
	setups  atomic.Int64
}

func newFakeEngine(vars map[string]string) *fakeEngine {
	return &fakeEngine{store: &fakeStore{docs: map[string]string{}}, vars: vars}
}

func (e *fakeEngine) readVariables(context.Context) (map[string]string, error) { return e.vars, nil }
func (e *fakeEngine) setup(context.Context, int) error                         { e.setups.Add(1); return nil }
func (e *fakeEngine) target() string                                           { return "fake:1" }
func (e *fakeEngine) close()                                                   {}
func (e *fakeEngine) newWorker(context.Context) (worker, error) {
	return &fakeWorker{store: e.store, failOn: e.failOn, counter: &e.counter, closed: &e.closed}, nil
}

func smokeConfig(t *testing.T, engineName string) *config {
	t.Helper()
	return &config{Engine: engineName, Ops: 64, Workers: 4, Payload: 16, Preload: 8, Smoke: true, Timeout: time.Second,
		Out: filepath.Join(t.TempDir(), "out.json"), DatadirFSNote: "tmpfs"}
}

func TestExecuteRunsFourPhasesAndWritesTableAndJSON(t *testing.T) {
	cfg := smokeConfig(t, engineMySQL)
	eng := newFakeEngine(map[string]string{"innodb_flush_log_at_trx_commit": "1", "sync_binlog": "1"})
	var table, progress bytes.Buffer
	if err := execute(context.Background(), cfg, eng, &table, &progress); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(cfg.Out)
	if err != nil {
		t.Fatal(err)
	}
	var got runResult
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	names := []string{"insert_serial", "insert_parallel", "get_serial", "get_parallel"}
	wantWorkers := []int{1, 4, 1, 4}
	if len(got.Phases) != 4 {
		t.Fatalf("phases %+v", got.Phases)
	}
	for i, phase := range got.Phases {
		if phase.Name != names[i] || phase.Workers != wantWorkers[i] || phase.Ops != 64 {
			t.Errorf("phase %d is %+v, want %s with %d workers and 64 ops", i, phase, names[i], wantWorkers[i])
		}
	}
	if got.Durability["sync_binlog"] != "1" || got.DatadirFSNote != "tmpfs" || !got.InternalOnly {
		t.Fatalf("result %+v", got)
	}
	if want := 64 + 64 + 8; len(eng.store.docs) != want {
		t.Fatalf("store holds %d documents, want %d (two insert phases and the preload)", len(eng.store.docs), want)
	}
	if eng.closed.Load() != 4 {
		t.Fatalf("%d of 4 workers were closed", eng.closed.Load())
	}
	if !strings.Contains(table.String(), "get_parallel") || !strings.Contains(progress.String(), "durability observed") {
		t.Fatalf("table %q progress %q", table.String(), progress.String())
	}
}

func TestExecuteRefusesRelaxedDurabilityBeforeTouchingTheServer(t *testing.T) {
	cfg := smokeConfig(t, engineMySQL)
	eng := newFakeEngine(map[string]string{"innodb_flush_log_at_trx_commit": "2", "sync_binlog": "1"})
	err := execute(context.Background(), cfg, eng, &bytes.Buffer{}, &bytes.Buffer{})
	if !errors.Is(err, errRelaxedDurability) {
		t.Fatalf("err %v, want errRelaxedDurability", err)
	}
	if eng.setups.Load() != 0 || len(eng.store.docs) != 0 {
		t.Fatal("the run set up or wrote before it refused")
	}
	if _, statErr := os.Stat(cfg.Out); statErr == nil {
		t.Fatal("a refused run wrote a result file")
	}
}

func TestAWorkerErrorStopsThePhaseAndEveryGoroutineExits(t *testing.T) {
	cfg := smokeConfig(t, engineMongo)
	eng := newFakeEngine(map[string]string{"write_concern_w": "1", "write_concern_j": "true", "storage_persistent": "true"})
	eng.failOn = 10
	err := execute(context.Background(), cfg, eng, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "disk on fire") || !strings.Contains(err.Error(), "insert_serial") {
		t.Fatalf("err %v", err)
	}
	if eng.closed.Load() != 4 {
		t.Fatal("workers were not closed after a failed phase")
	}
}

func TestACancelledContextStopsARun(t *testing.T) {
	cfg := smokeConfig(t, engineMongo)
	eng := newFakeEngine(map[string]string{"write_concern_w": "1", "write_concern_j": "true", "storage_persistent": "true"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	workers := []worker{&fakeWorker{store: eng.store, counter: &eng.counter, closed: &eng.closed}}
	b := &benchmark{cfg: cfg, workers: workers, ids: newIDGenerator(time.Now, bytes.NewReader(bytes.Repeat([]byte{3}, 1<<16))), payload: "x"}
	var mu sync.Mutex
	if _, _, err := runPhase(ctx, workers, 10, func(ctx context.Context, w worker, i int) (time.Duration, error) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		return b.insertOp(false, &mu)(ctx, w, i)
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v", err)
	}
}

func TestRandomPayloadHasTheRequestedLength(t *testing.T) {
	for _, n := range []int{1, 255, 256, 4097} {
		got, err := randomPayload(n)
		if err != nil || len(got) != n {
			t.Errorf("payload(%d) = %d bytes, %v", n, len(got), err)
		}
	}
}
