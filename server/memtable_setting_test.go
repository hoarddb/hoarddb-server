package server

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
)

// TestNodeStoreHonoursTheMemtableSetting: the one store every bucket shares is
// opened at storage.memtable_mb, so the atomic batch bound a client meets is 15%
// of what the operator configured. Before the setting existed the bound was
// 5,033,164 bytes whatever the configuration said.
func TestNodeStoreHonoursTheMemtableSetting(t *testing.T) {
	tests := []struct {
		name       string
		memtableMB int
		wantBound  int64
	}{
		{"the floor", config.MinMemtableMB, 5_033_164},
		{"an operator choice", 64, 15 * (64 << 20) / 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qs := newCapacityService(t, func(cfg *config.Config) { cfg.Storage.MemtableMB = tt.memtableMB }, nil)
			defer func() {
				if err := qs.Stop(); err != nil {
					t.Errorf("Stop: %v", err)
				}
			}()
			if got := qs.core.kv.MaxBatchBytes(); got != tt.wantBound {
				t.Errorf("store atomic bound = %d at memtable_mb=%d, want %d", got, tt.memtableMB, tt.wantBound)
			}
		})
	}
}
