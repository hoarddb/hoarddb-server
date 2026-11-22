package storage

import (
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
)

// TestWALStatErrorConsumesNoSequence: an append whose entry write fails gives no
// sequence away. writeLocked no longer stats the active file (the write offset
// is kept in memory), and it commits the sequence only after the write, so a
// failed write, whole or partial, leaves LastSequence where it was and the next
// append takes the next sequence with nothing left behind to read (BUG-0716,
// in part). The failure is injected through the syscall table's writeAt, which
// fails to compile on the parent: the table has no writeAt there, and the Stat
// returns it replaces cannot be failed on demand.
func TestWALStatErrorConsumesNoSequence(t *testing.T) {
	for _, mode := range []struct {
		name string
		keys func(t *testing.T) WALKeySource
	}{
		{"plain", func(*testing.T) WALKeySource { return nil }},
		{"sealed", func(t *testing.T) WALKeySource { return newSealedTestKeySource(t) }},
	} {
		for _, partial := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/partial=%v", mode.name, partial), func(t *testing.T) {
				dir, keys := t.TempDir(), mode.keys(t)
				w, err := OpenWAL(dir, keys)
				if err != nil {
					t.Fatalf("OpenWAL: %v", err)
				}
				appendN(t, w, 3)

				// A partial write is what a full disk does: some of the entry
				// lands, then the call fails.
				simulated := errors.New("simulated ENOSPC from the entry write")
				var failed atomic.Bool
				real := w.sys.writeAt
				w.sys.writeAt = func(file *os.File, entry []byte, offset int64) (int, error) {
					if !failed.CompareAndSwap(false, true) {
						return real(file, entry, offset)
					}
					if partial {
						written, err := real(file, entry[:len(entry)/2], offset)
						if err != nil {
							return written, err
						}
						return written, simulated
					}
					return 0, simulated
				}

				// The refused entry is much longer than the one after it, so half
				// of it reaches past the next entry's end unless the rollback cuts it.
				if _, err := w.Append("bucket", OpInsert, []byte("refused"), make([]byte, 4<<10)); !errors.Is(err, simulated) {
					t.Fatalf("Append with a failing write = %v, want the injected error", err)
				}
				if got := w.LastSequence(); got != 3 {
					t.Fatalf("the failed write moved LastSequence to %d, want 3", got)
				}
				seq, err := w.Append("bucket", OpInsert, []byte("after"), []byte("payload"))
				if err != nil {
					t.Fatalf("the append after the failed write: %v", err)
				}
				if seq != 4 {
					t.Fatalf("the append after the failed write got sequence %d, want 4", seq)
				}
				readBack, err := w.ReadFrom(1)
				if err != nil {
					t.Fatalf("ReadFrom(1): %v", err)
				}
				requireSeqs(t, "ReadFrom(1)", readBack, 4)
				if string(readBack[3].Key) != "after" {
					t.Fatalf("sequence 4 holds key %q, want the append after the failure", readBack[3].Key)
				}
				requireSeqs(t, "OpenCursor(1)+ReadBatch", readAllThroughCursor(t, w), 4)

				// The rollback cut the failed bytes: the next open finds a log
				// that ends at its last entry, not a torn tail to repair.
				if err := w.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
				reopened, err := OpenWAL(dir, keys)
				if err != nil {
					t.Fatalf("reopening: %v", err)
				}
				defer closeWAL(t, reopened)
				if recovery := reopened.Recovery(); recovery != nil {
					t.Fatalf("the reopen repaired a torn tail the rollback should have cut: %+v", *recovery)
				}
				readBack, err = reopened.ReadFrom(1)
				if err != nil {
					t.Fatalf("ReadFrom(1) after the reopen: %v", err)
				}
				requireSeqs(t, "ReadFrom(1) after the reopen", readBack, 4)
			})
		}
	}
}
