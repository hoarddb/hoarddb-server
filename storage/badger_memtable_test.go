package storage

import "testing"

func TestMemtableSizeSetsTheAtomicBatchBound(t *testing.T) {
	tests := []struct {
		name          string
		memtableMB    int
		wantBatchByte int64
	}{
		{"the floor", 32, 15 * (32 << 20) / 100},
		{"an operator choice", 256, 15 * (256 << 20) / 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kv, err := OpenBadgerKV(t.TempDir(), BadgerTuning{MemTableMB: tt.memtableMB}, nil)
			if err != nil {
				t.Fatalf("OpenBadgerKV: %v", err)
			}
			defer func() {
				if closeErr := kv.Close(); closeErr != nil {
					t.Errorf("Close: %v", closeErr)
				}
			}()
			if got := kv.MaxBatchBytes(); got != tt.wantBatchByte {
				t.Errorf("MaxBatchBytes() = %d, want %d (15%% of the memtable)", got, tt.wantBatchByte)
			}
			if got, want := BatchEntryBound(tt.wantBatchByte), kv.db.MaxBatchCount(); got != want {
				t.Errorf("BatchEntryBound(%d) = %d, but the open store's entry bound is %d", tt.wantBatchByte, got, want)
			}
		})
	}
}
