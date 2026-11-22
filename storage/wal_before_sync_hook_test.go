package storage

import "testing"

// TestTheBeforeSyncHookFiresInSyncThrough: a split append neither waits nor reports the
// fsync; SyncThrough fires the before-sync hook once, with its own sequence argument.
func TestTheBeforeSyncHookFiresInSyncThrough(t *testing.T) {
	w, err := OpenWAL(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Errorf("closing the WAL: %v", err)
		}
	})

	var fired []uint64
	w.SetBeforeSyncHook(func(seq uint64) { fired = append(fired, seq) })

	seq, err := w.AppendUnsyncedFrom("bucket", OpInsert, []byte("k"), []byte("v"),
		WALOrigin{Kind: WALOriginOwn, Epoch: 1})
	if err != nil {
		t.Fatalf("AppendUnsyncedFrom: %v", err)
	}
	if len(fired) != 0 {
		t.Fatalf("AppendUnsyncedFrom fired the before-sync hook %d times, want 0", len(fired))
	}

	if err := w.SyncThrough(seq); err != nil {
		t.Fatalf("SyncThrough(%d): %v", seq, err)
	}
	if len(fired) != 1 {
		t.Fatalf("SyncThrough(%d) fired the before-sync hook %d times, want 1", seq, len(fired))
	}
	if fired[0] != seq {
		t.Fatalf("the before-sync hook fired with seq %d, want the SyncThrough argument %d", fired[0], seq)
	}
}
