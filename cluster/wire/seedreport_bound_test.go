package wire

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// boundSlack is how long past its budget a bounded call may take before a test calls it unbounded.
const boundSlack = time.Second

// TestAReseedClearIsBoundedWhileABatchIsStuck: a Clear that queues behind a batch whose
// proposal is stuck (no leader) gives up one relayCallTimeout after it began, counting the
// wait for the send lock, and sends nothing. The deadline is real time: relayCallTimeout is
// a constant and the reporter's clock only dates candidates.
// [BREAK: in Clear take the send lock before starting the timeout]
func TestAReseedClearIsBoundedWhileABatchIsStuck(t *testing.T) {
	w := newReportWorld(1)
	w.setRecord(1, 3, machine.SeedReady)
	w.setCommittedBit(t, 1, 9)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	w.sendFn = func(call int) error {
		if call == 1 {
			entered <- struct{}{}
			<-release
		}
		return nil
	}
	r := w.reporter()
	r.Note(1, 3, false)
	w.advance(reportInterval)
	flushDone := make(chan struct{})
	go func() {
		r.Flush(context.Background())
		close(flushDone)
	}()
	t.Cleanup(func() {
		close(release)
		<-flushDone
	})
	<-entered // the SEEDED batch is in flight and will not answer

	started := time.Now()
	clearDone := make(chan error, 1)
	go func() { clearDone <- r.Clear(context.Background(), 1, 9) }()
	select {
	case err := <-clearDone:
		if err == nil {
			t.Fatal("a Clear that never got the send lock returned nil")
		}
		for _, want := range []string{"seed bit 9", "database 1", "in-flight batch", "next catch-up"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the error %q does not say %q", err, want)
			}
		}
		t.Logf("the Clear gave up after %v: %v", time.Since(started).Round(time.Millisecond), err)
	case <-time.After(relayCallTimeout + boundSlack):
		t.Fatalf("the Clear is still waiting %v behind a stuck batch, want an error within %v", time.Since(started).Round(time.Millisecond), relayCallTimeout+boundSlack)
	}
	if sends := w.allSends(); len(sends) != 1 {
		t.Fatalf("%d proposals, want only the stuck batch: a Clear that timed out must send nothing", len(sends))
	}
}

// TestAStuckFlushIsBoundedByItsOwnTimeout: a proposal that never answers is given up after
// one relayCallTimeout, the candidate stays for the next Flush and the error is reported,
// so the send lock is never held longer than one proposal budget.
// [BREAK: in flushBatch pass the unbounded ctx to Send]
func TestAStuckFlushIsBoundedByItsOwnTimeout(t *testing.T) {
	w := newReportWorld(1)
	w.setRecord(1, 3, machine.SeedReady)
	r := w.reporter()
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	r.deps.Send = func(ctx context.Context, _ byte, _ []byte) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			return nil
		}
	}
	r.Note(1, 3, false)
	w.advance(reportInterval)

	started := time.Now()
	flushDone := make(chan struct{})
	go func() {
		r.Flush(context.Background())
		close(flushDone)
	}()
	select {
	case <-flushDone:
		t.Logf("the Flush gave up after %v", time.Since(started).Round(time.Millisecond))
	case <-time.After(relayCallTimeout + boundSlack):
		t.Fatalf("the Flush is still waiting %v on a proposal that never answers, want it back within %v", time.Since(started).Round(time.Millisecond), relayCallTimeout+boundSlack)
	}
	r.mu.Lock()
	kept := 0
	if p := r.dbs[1]; p != nil {
		kept = p.pending
	}
	r.mu.Unlock()
	if kept != 1 {
		t.Fatalf("%d candidates kept after the stuck proposal, want 1", kept)
	}
	w.mu.Lock()
	reported := len(w.errs)
	w.mu.Unlock()
	if reported != 1 {
		t.Fatalf("OnError called %d times, want 1", reported)
	}
}
