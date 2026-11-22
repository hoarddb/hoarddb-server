package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The write-latency measurement behind the figures in the CHANGELOG and
// ackGatherLimit). It runs only when asked, because it needs a real disk:
//
//	cd hoardDB && HOARDDB_MEASURE_ACK_DIR=$HOME/ack-measure \
//	    go test -count=1 -v -run TestMeasureAcknowledgedWrites ./server/ | grep -E 'RESULT|SYNCS'
//
// Add HOARDDB_MEASURE_ACK_UNSAFE=1 to measure with
// storage.unsafe_ack_before_sync on, which is the figure before the fix.
// Writes go through the client handler in process, so the figures leave out
// the network and TLS. The directory must not be tmpfs: a sync there costs
// nothing, and t.TempDir is under /tmp, which is tmpfs on many hosts.

const measureDirEnv = "HOARDDB_MEASURE_ACK_DIR"

// syncTimingKV counts the store's Syncs and their time. Only the node's own
// s.kv is replaced, which is what acknowledgeDurably syncs.
type syncTimingKV struct {
	storage.KV
	syncs atomic.Int64
	nanos atomic.Int64
}

func (kv *syncTimingKV) Sync() error {
	start := time.Now()
	err := kv.KV.Sync()
	kv.syncs.Add(1)
	kv.nanos.Add(int64(time.Since(start)))
	return err
}

// measureServer is a standalone node on a fresh directory under root, with
// databases bench and buckets B0..B{buckets-1}, and its Syncs timed from here on.
func measureServer(t *testing.T, root string, buckets int) (transport.RequestHandler, *syncTimingKV) {
	t.Helper()
	dataDir, err := os.MkdirTemp(root, "ack-measure-")
	if err != nil {
		t.Fatalf("making a directory under %s: %v", root, err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dataDir); err != nil {
			t.Errorf("removing %s: %v", dataDir, err)
		}
	})
	s := newTestServerWith(t, func(cfg *config.Config) {
		cfg.Storage.DataDir = dataDir
		cfg.Storage.UnsafeAckBeforeSync = os.Getenv("HOARDDB_MEASURE_ACK_UNSAFE") != ""
	})
	handler := NewHandler(s)
	handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "bench"})
	for b := 0; b < buckets; b++ {
		handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "bench", Name: fmt.Sprintf("B%d", b), Type: BucketTypeHash})
	}
	timed := &syncTimingKV{KV: s.kv}
	s.kv = timed
	return handler, timed
}

// measuredPut sends one PUT and returns how long its answer took.
func measuredPut(t *testing.T, handler transport.RequestHandler, bucket, key string) (time.Duration, error) {
	payload := mustMarshalBSON(t, InsertRequest{Database: "bench", Bucket: bucket, Key: key,
		Data: map[string]interface{}{"name": key, "age": 42, "email": key + "@example.com"}})
	start := time.Now()
	frame, err := handler(context.Background(), transport.NewFrame(transport.OpPut, payload))
	took := time.Since(start)
	if err != nil {
		return took, fmt.Errorf("put %s/%s: %w", bucket, key, err)
	}
	if frame.Opcode == transport.OpError {
		return took, fmt.Errorf("put %s/%s refused: %s", bucket, key, frame.Payload)
	}
	return took, nil
}

func reportLatencies(label string, latencies []time.Duration, wall time.Duration) {
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	millis := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	at := func(q float64) time.Duration { return latencies[int(float64(len(latencies)-1)*q)] }
	fmt.Printf("RESULT %-32s n=%6d  %8.0f /s  p50 %7.3f ms  p99 %7.3f ms  max %7.3f ms\n",
		label, len(latencies), float64(len(latencies))/wall.Seconds(), millis(at(0.5)), millis(at(0.99)), millis(latencies[len(latencies)-1]))
}

func reportSyncs(label string, kv *syncTimingKV, answered int) {
	syncs := kv.syncs.Load()
	if syncs == 0 {
		fmt.Printf("SYNCS  %-32s none\n", label)
		return
	}
	fmt.Printf("SYNCS  %-32s %d, %.1f answers each, %.3f ms each\n",
		label, syncs, float64(answered)/float64(syncs), float64(kv.nanos.Load())/float64(syncs)/1e6)
}

// requireRealDisk skips unless root is set, and fails on tmpfs.
func requireRealDisk(t *testing.T) string {
	t.Helper()
	root := os.Getenv(measureDirEnv)
	if root == "" {
		t.Skipf("set %s to a directory on the disk to measure", measureDirEnv)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("making %s: %v", root, err)
	}
	var fs unix.Statfs_t
	if err := unix.Statfs(root, &fs); err != nil {
		t.Fatalf("statfs %s: %v", root, err)
	}
	if fs.Type == unix.TMPFS_MAGIC {
		t.Fatalf("%s is on tmpfs, where a sync costs nothing; point %s at a directory on a real disk", root, measureDirEnv)
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		t.Fatalf("resolving %s: %v", root, err)
	}
	return abs
}

func TestMeasureAcknowledgedWrites(t *testing.T) {
	root := requireRealDisk(t)
	const sequential, writers, perWriter, batches = 2000, 16, 400, 40

	t.Run("sequential", func(t *testing.T) {
		handler, timed := measureServer(t, root, 1)
		latencies := make([]time.Duration, 0, sequential)
		start := time.Now()
		for i := 0; i < sequential; i++ {
			took, err := measuredPut(t, handler, "B0", fmt.Sprintf("u%07d", i))
			if err != nil {
				t.Fatal(err)
			}
			latencies = append(latencies, took)
		}
		reportLatencies("insert, one writer", latencies, time.Since(start))
		reportSyncs("insert, one writer", timed, sequential)
	})

	for _, oneBucket := range []bool{true, false} {
		label := fmt.Sprintf("insert, %d writers, %d buckets", writers, writers)
		if oneBucket {
			label = fmt.Sprintf("insert, %d writers, one bucket", writers)
		}
		t.Run(label, func(t *testing.T) {
			handler, timed := measureServer(t, root, writers)
			var mu sync.Mutex
			var latencies []time.Duration
			var wg sync.WaitGroup
			start := time.Now()
			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func(writer int) {
					defer wg.Done()
					bucket := fmt.Sprintf("B%d", writer)
					if oneBucket {
						bucket = "B0"
					}
					mine := make([]time.Duration, 0, perWriter)
					for i := 0; i < perWriter; i++ {
						took, err := measuredPut(t, handler, bucket, fmt.Sprintf("w%02d-%07d", writer, i))
						if err != nil {
							t.Error(err)
							return
						}
						mine = append(mine, took)
					}
					mu.Lock()
					latencies = append(latencies, mine...)
					mu.Unlock()
				}(w)
			}
			wg.Wait()
			if t.Failed() {
				return
			}
			reportLatencies(label, latencies, time.Since(start))
			reportSyncs(label, timed, writers*perWriter)
		})
	}

	t.Run("batch", func(t *testing.T) {
		handler, timed := measureServer(t, root, 1)
		latencies := make([]time.Duration, 0, batches)
		start := time.Now()
		for b := 0; b < batches; b++ {
			documents := make([]map[string]interface{}, 1000)
			for i := range documents {
				key := fmt.Sprintf("b%03d-%04d", b, i)
				documents[i] = map[string]interface{}{"_id_": key, "name": key, "age": 42}
			}
			sent := time.Now()
			handlerSend(t, handler, transport.OpBatchPut, BatchInsertRequest{Database: "bench", Bucket: "B0", Data: documents})
			latencies = append(latencies, time.Since(sent))
		}
		reportLatencies("batch of 1,000", latencies, time.Since(start))
		reportSyncs("batch of 1,000", timed, batches)
	})
}
