package storage

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// TestAFailedFastForwardFsyncPoisonsTheLog: [FAIL-FIRST, BUG-0807]. FastForward fsyncs the active file
// before it moves the log past a sequence. When that fsync fails the log must be poisoned: Linux reports
// an fsync error once, so a later fsync that succeeds proves nothing about the entries written before it,
// and a SyncThrough that reports them durable would acknowledge writes that may not be on disk.
func TestAFailedFastForwardFsyncPoisonsTheLog(t *testing.T) {
	w, err := OpenWAL(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeWAL(t, w)
	var seq uint64
	for i := range 2 {
		origin := WALOrigin{Kind: WALOriginReplica, Source: "n1", Seq: uint64(i + 1)}
		seq, err = w.AppendUnsyncedFrom("bucket", OpInsert, []byte(fmt.Sprintf("k%d", i+1)), []byte("v"), origin)
		if err != nil {
			t.Fatal(err)
		}
	}

	injected := fmt.Errorf("simulated EIO on fsync: %w", syscall.EIO)
	realFsync := w.sys.fsync
	var fsyncs atomic.Int64
	w.sys.fsync = func(f *os.File) error {
		if fsyncs.Add(1) == 1 {
			return injected
		}
		return realFsync(f)
	}
	if moved, err := w.FastForward(seq + 10); !errors.Is(err, injected) || moved {
		t.Fatalf("FastForward with a failed fsync = %v, %v, want the fsync failure and no move", moved, err)
	}

	// The fsync succeeds from here on, as it does after a transient EIO. The entries written before the
	// failed fsync are not known durable, so the log must refuse to say so.
	if err := w.SyncThrough(seq); !errors.Is(err, injected) {
		t.Fatalf("SyncThrough(%d) after a failed FastForward fsync = %v, want the poison (the fsync failure)", seq, err)
	}
	if refusal := w.WriteRefusal(); refusal == nil {
		t.Fatal("WriteRefusal is nil after a failed FastForward fsync: the log is not poisoned")
	}
	if synced := w.SyncedSequence(); synced >= seq {
		t.Fatalf("SyncedSequence = %d after a failed FastForward fsync, want below %d", synced, seq)
	}
}

// TestAFastForwardFsyncErrorDuringAGroupFlushPoisonsTheLeaderToo: [FAIL-FIRST, BUG-0807 follow-up].
// Linux hands one fsync error to one fsync of a file. If FastForward fsyncs the active file while a
// group-commit leader is mid-flush on it, FastForward gets the error and the leader's flush returns nil
// for pages the kernel dropped; the leader must not then report its entry durable. The fake kernel here
// fails FastForward's fsync only when it overlaps the leader's flush, which is the interleaving the bug
// needs: a log that makes FastForward wait for the flush never overlaps, so FastForward succeeds and the
// test has nothing to refuse.
func TestAFastForwardFsyncErrorDuringAGroupFlushPoisonsTheLeaderToo(t *testing.T) {
	w, err := OpenWAL(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeWAL(t, w)
	origin := WALOrigin{Kind: WALOriginReplica, Source: "n1", Seq: 1}
	seq, err := w.AppendUnsyncedFrom("bucket", OpInsert, []byte("k1"), []byte("v"), origin)
	if err != nil {
		t.Fatal(err)
	}

	var inFlush atomic.Bool
	flushHeld := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFlush := func() { releaseOnce.Do(func() { close(release) }) }
	w.SetFsyncHook(func() {
		inFlush.Store(true)
		close(flushHeld)
		<-release
		inFlush.Store(false)
	})
	defer w.SetFsyncHook(nil)

	injected := fmt.Errorf("simulated EIO on fsync: %w", syscall.EIO)
	realFsync := w.sys.fsync
	w.sys.fsync = func(f *os.File) error {
		if !inFlush.Load() {
			return realFsync(f)
		}
		// Overlaps the leader's flush: this fsync receives the one error. The leader is let go with
		// nil and given time to take syncMu back before FastForward poisons the log.
		releaseFlush()
		time.Sleep(100 * time.Millisecond)
		return injected
	}

	leaderErr := make(chan error, 1)
	go func() { leaderErr <- w.SyncThrough(seq) }()
	<-flushHeld

	fastForwardErr := make(chan error, 1)
	go func() {
		_, err := w.FastForward(seq + 10)
		fastForwardErr <- err
	}()
	// A FastForward that waits for the flush never reaches its fsync, so nothing else frees the leader.
	time.AfterFunc(300*time.Millisecond, releaseFlush)

	syncErr := <-leaderErr
	ffErr := <-fastForwardErr
	if ffErr == nil {
		return // FastForward waited for the flush: no fsync error was handed out, nothing to refuse.
	}
	if !errors.Is(ffErr, injected) {
		t.Fatalf("FastForward = %v, want nil or the injected fsync failure", ffErr)
	}
	if !errors.Is(syncErr, injected) {
		t.Fatalf("SyncThrough(%d) = %v while FastForward's fsync took the file's error, want the poison (the fsync failure)", seq, syncErr)
	}
	if synced := w.SyncedSequence(); synced >= seq {
		t.Fatalf("SyncedSequence = %d, want below %d: the entry's pages were dropped", synced, seq)
	}
}
