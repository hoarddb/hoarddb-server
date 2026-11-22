package storage

import "testing"

// TestBadgerDefaultLevelZeroTables pins the Level-0 table limits hoardDB
// builds when no experiment override is set. Badger's own default is 5 and 15;
// the measured read-path gain (2026-11-12) came from capping L0 at 2.
func TestBadgerDefaultLevelZeroTables(t *testing.T) {
	t.Setenv("HOARDDB_EXPERIMENT_BADGER_NUM_LEVEL_ZERO_TABLES", "")
	opts, err := badgerOptions(t.TempDir(), BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("badgerOptions: unexpected error: %v", err)
	}
	if opts.NumLevelZeroTables != 2 {
		t.Errorf("NumLevelZeroTables = %d, want 2", opts.NumLevelZeroTables)
	}
	if opts.NumLevelZeroTablesStall != 7 {
		t.Errorf("NumLevelZeroTablesStall = %d, want 7", opts.NumLevelZeroTablesStall)
	}
}
