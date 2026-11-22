package config

import (
	"strings"
	"testing"
)

// TestUnsetMemtableIsZeroNotDefault: an unset memtable is 0, "not set", so the
// server can tell it from an explicit size and use the store's own choice
// (memtable-host-scaled-design.md R-4). A filled-in default would be
// indistinguishable from an operator's explicit 128.
func TestUnsetMemtableIsZeroNotDefault(t *testing.T) {
	if got := DefaultConfig().Storage.MemtableMB; got != 0 {
		t.Errorf("DefaultConfig().Storage.MemtableMB = %d, want 0 (not set)", got)
	}
	t.Setenv("HOARDDB_MEMTABLE_MB", "")
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Storage.MemtableMB != 0 {
		t.Errorf("memtable_mb with nothing set = %d, want 0 (not set)", cfg.Storage.MemtableMB)
	}
}

// TestHostMemtableSteps: the memory limit / 56 MiB, rounded down to a power of
// two, from the 32 MiB floor to the 128 MiB the automatic choice is capped at
// (owner ruling, release burn-down §4d item 23, which replaced item 22's 64). The steps sit between nominal
// machine sizes, so a "4 GB" VM (about 3.8 GiB) and a "3 GB" one land on
// different sides of 3.5 GiB and identical hosts choose identically.
func TestHostMemtableSteps(t *testing.T) {
	const gib = int64(1) << 30
	for _, tt := range []struct {
		limit  int64
		wantMB int
	}{
		{0, 32},
		{1 * gib, 32},
		{29 * gib / 10, 32},
		{7*gib/2 - 1, 32},
		{7 * gib / 2, 64},
		{699 * gib / 100, 64},
		{7*gib - 1, 64},
		{7 * gib, 128},
		{14 * gib, 128},
		{16 * gib, 128},
		{100 * gib, 128},
		{1024 * gib, 128},
	} {
		if gotMB := HostMemtableMB(tt.limit); gotMB != tt.wantMB {
			t.Errorf("HostMemtableMB(%.2f GiB) = %d, want %d", float64(tt.limit)/float64(gib), gotMB, tt.wantMB)
		}
	}
}

// TestPredictedPeakBytes is the measured slope (memtable-host-scaled-design.md
// §1): about 200 MB plus 8.2 MB per MiB of memtable, from 463 MB at 32 MiB and
// 1,251 MB at 128 MiB on a 3 GB VM.
func TestPredictedPeakBytes(t *testing.T) {
	for _, tt := range []struct {
		memtableMB int
		wantBytes  int64
	}{
		{32, 462_400_000},
		{64, 724_800_000},
		{128, 1_249_600_000},
		{256, 2_299_200_000},
	} {
		if got := PredictedPeakBytes(tt.memtableMB); got != tt.wantBytes {
			t.Errorf("PredictedPeakBytes(%d) = %d, want %d", tt.memtableMB, got, tt.wantBytes)
		}
	}
}

// TestMemtableBatchBytes is Badger's atomic batch bound, 15% of the memtable.
func TestMemtableBatchBytes(t *testing.T) {
	for _, tt := range []struct {
		memtableMB int
		wantBytes  int64
	}{
		{32, 5_033_164},
		{64, 10_066_329},
		{128, 20_132_659},
	} {
		if got := MemtableBatchBytes(tt.memtableMB); got != tt.wantBytes {
			t.Errorf("MemtableBatchBytes(%d) = %d, want %d", tt.memtableMB, got, tt.wantBytes)
		}
	}
}

func TestMemtableIsReadFromYAMLAndEnvironment(t *testing.T) {
	t.Setenv("HOARDDB_MEMTABLE_MB", "")
	cfg, err := Load(writeYAML(t, "storage:\n  memtable_mb: 64\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Storage.MemtableMB != 64 {
		t.Errorf("memtable_mb from YAML = %d, want 64", cfg.Storage.MemtableMB)
	}

	t.Setenv("HOARDDB_MEMTABLE_MB", "256")
	cfg, err = Load(writeYAML(t, "storage:\n  memtable_mb: 64\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Storage.MemtableMB != 256 {
		t.Errorf("memtable_mb with HOARDDB_MEMTABLE_MB=256 = %d, want 256 (the environment wins)", cfg.Storage.MemtableMB)
	}
}

func TestMemtableBoundsAreRefusedWithTheFix(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr []string
	}{
		{"floor is accepted", "32", nil},
		{"ceiling is accepted", "1024", nil},
		{"below the floor", "31", []string{"memtable_mb", "31", "32", "HOARDDB_MEMTABLE_MB", "5 MB"}},
		{"above the ceiling", "1025", []string{"memtable_mb", "1025", "1024", "HOARDDB_MEMTABLE_MB"}},
		{"negative", "-8", []string{"memtable_mb", "-8", "32"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HOARDDB_MEMTABLE_MB", tt.value)
			_, err := Load("")
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("Load with memtable_mb=%s: %v", tt.value, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Load with memtable_mb=%s succeeded, want a refusal", tt.value)
			}
			for _, want := range tt.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal %q does not contain %q", err, want)
				}
			}
		})
	}
}
