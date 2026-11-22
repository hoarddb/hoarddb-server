package storage

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestWALConcurrentAppendGroupCommit exercises many goroutines calling
// Append at once, including WAL rotation mid-stream (a small maxFileSize
// forces several), to verify group-commit fsync batching (see waitForSync)
// does not lose, duplicate, or misattribute entries under concurrency.
func TestWALConcurrentAppendGroupCommit(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	// Small enough that this workload rotates several times, so the test
	// exercises rotate()'s syncWG handoff, not just the common case.
	w.maxFileSize = 4096

	const goroutines = 32
	const perGoroutine = 25
	total := goroutines * perGoroutine

	type result struct {
		seq  uint64
		key  string
		data string
		err  error
	}
	results := make(chan result, total)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				key := fmt.Sprintf("g%d-i%d", g, i)
				data := fmt.Sprintf("data-%d-%d", g, i)
				seq, err := w.Append("bucket", OpInsert, []byte(key), []byte(data))
				results <- result{seq: seq, key: key, data: data, err: err}
			}
		}(g)
	}
	wg.Wait()
	close(results)

	expected := make(map[uint64]result, total)
	seen := make(map[uint64]bool, total)
	for r := range results {
		if r.err != nil {
			t.Fatalf("Append(%s): unexpected error: %v", r.key, r.err)
		}
		if seen[r.seq] {
			t.Fatalf("sequence %d returned to two different Append calls", r.seq)
		}
		seen[r.seq] = true
		expected[r.seq] = r
	}
	if len(seen) != total {
		t.Fatalf("got %d distinct sequences, want %d", len(seen), total)
	}
	for seq := uint64(1); seq <= uint64(total); seq++ {
		if !seen[seq] {
			t.Fatalf("sequence %d was never assigned — gap in a supposedly contiguous run", seq)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	entries, err := reopened.ReadFrom(1)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(entries) != total {
		t.Fatalf("read back %d entries after reopen, want %d (group commit lost or duplicated entries)", len(entries), total)
	}
	for i, e := range entries {
		wantSeq := uint64(i + 1)
		if e.Seq != wantSeq {
			t.Fatalf("entry %d has seq %d, want %d — entries out of order on disk", i, e.Seq, wantSeq)
		}
		want, ok := expected[e.Seq]
		if !ok {
			t.Fatalf("entry with seq %d does not correspond to any Append call", e.Seq)
		}
		if string(e.Key) != want.key || string(e.Data) != want.data {
			t.Fatalf("entry seq %d = (key=%q data=%q), want (key=%q data=%q)",
				e.Seq, e.Key, e.Data, want.key, want.data)
		}
	}
}

// TestWALPoisonedAfterSyncFailure verifies that once an fsync failure has
// poisoned the WAL (syncErr set — see waitForSync's doc for why it cannot
// safely roll back a batched write instead), every subsequent Append fails
// fast with a clear, wrapped error instead of silently writing past an
// uncertain tail.
func TestWALPoisonedAfterSyncFailure(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer func() { _ = w.Close() }()

	if _, err := w.Append("bucket", OpInsert, []byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("first Append: unexpected error: %v", err)
	}

	simulated := errors.New("simulated fsync failure")
	w.syncMu.Lock()
	w.syncErr = simulated
	w.syncMu.Unlock()

	_, err = w.Append("bucket", OpInsert, []byte("k2"), []byte("v2"))
	if err == nil {
		t.Fatal("Append after a poisoning fsync failure succeeded; want a fast failure")
	}
	if !errors.Is(err, simulated) {
		t.Fatalf("Append error = %v, want it to wrap the poisoning error %v", err, simulated)
	}
}

// TestWALCloseDuringConcurrentAppends exercises Close racing against
// in-flight Appends (run with -race): rotate/Close wait on syncWG (see
// their doc comments) specifically so this cannot race a concurrent
// waitForSync fsync on the same *os.File. The assertion here is simply that
// nothing panics or data-races — Appends losing the race against Close are
// expected to error, not succeed.
func TestWALCloseDuringConcurrentAppends(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir, nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("g%d-i%d", g, i)
				_, _ = w.Append("bucket", OpInsert, []byte(key), []byte("v"))
			}
		}(g)
	}

	// Give the appenders a moment to actually get in flight before closing,
	// so this exercises Close racing a live Append rather than an idle WAL.
	time.Sleep(2 * time.Millisecond)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	close(stop)
	wg.Wait()
}
