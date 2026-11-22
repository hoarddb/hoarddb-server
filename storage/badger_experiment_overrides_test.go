package storage

import (
	"reflect"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
)

func TestApplyExperimentOverridesNoEnv(t *testing.T) {
	// When no environment variables are set, options should be identical to defaults.
	baseOpts := badger.DefaultOptions("/tmp/test")
	resultOpts, err := applyExperimentOverrides(baseOpts)
	if err != nil {
		t.Fatalf("applyExperimentOverrides: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(resultOpts, baseOpts) {
		t.Errorf("options should be unchanged when no overrides are set")
	}
}

func TestApplyExperimentOverridesCompression(t *testing.T) {
	tests := []struct {
		name       string
		envValue   string
		wantErr    bool
		wantErrMsg string
		wantComp   options.CompressionType
	}{
		{"none", "none", false, "", options.None},
		{"snappy", "snappy", false, "", options.Snappy},
		{"zstd", "zstd", false, "", options.ZSTD},
		{"none uppercase", "NONE", false, "", options.None},
		{"snappy mixed case", "SnApPy", false, "", options.Snappy},
		{"invalid", "invalid", true, "HOARDDB_EXPERIMENT_BADGER_COMPRESSION invalid value", options.Snappy},
		{"empty string not allowed", "", false, "", options.Snappy}, // empty means no override
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envValue != "" {
				t.Setenv("HOARDDB_EXPERIMENT_BADGER_COMPRESSION", tt.envValue)
			}
			baseOpts := badger.DefaultOptions("/tmp/test")
			resultOpts, err := applyExperimentOverrides(baseOpts)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				if tt.wantErrMsg != "" && !reflect.DeepEqual(err.Error(), tt.wantErrMsg) &&
					!containsSubstring(err.Error(), tt.wantErrMsg) {
					t.Errorf("error message %q does not contain %q", err.Error(), tt.wantErrMsg)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if tt.envValue != "" && resultOpts.Compression != tt.wantComp {
					t.Errorf("Compression = %v, want %v", resultOpts.Compression, tt.wantComp)
				}
			}
		})
	}
}

func TestApplyExperimentOverridesDetectConflicts(t *testing.T) {
	tests := []struct {
		name       string
		envValue   string
		wantErr    bool
		wantErrMsg string
		wantVal    bool
	}{
		{"true", "true", false, "", true},
		{"false", "false", false, "", false},
		{"true uppercase", "TRUE", false, "", true},
		{"false mixed case", "FaLsE", false, "", false},
		{"invalid", "maybe", true, "HOARDDB_EXPERIMENT_BADGER_DETECT_CONFLICTS invalid value", false},
		{"empty string not allowed", "", false, "", false}, // empty means no override
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envValue != "" {
				t.Setenv("HOARDDB_EXPERIMENT_BADGER_DETECT_CONFLICTS", tt.envValue)
			}
			baseOpts := badger.DefaultOptions("/tmp/test")
			baseDetectConflicts := baseOpts.DetectConflicts
			resultOpts, err := applyExperimentOverrides(baseOpts)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				if tt.wantErrMsg != "" && !containsSubstring(err.Error(), tt.wantErrMsg) {
					t.Errorf("error message %q does not contain %q", err.Error(), tt.wantErrMsg)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if tt.envValue != "" && resultOpts.DetectConflicts != tt.wantVal {
					t.Errorf("DetectConflicts = %v, want %v", resultOpts.DetectConflicts, tt.wantVal)
				} else if tt.envValue == "" && resultOpts.DetectConflicts != baseDetectConflicts {
					t.Errorf("DetectConflicts changed when no override set")
				}
			}
		})
	}
}

func TestApplyExperimentOverridesNumCompactors(t *testing.T) {
	tests := []struct {
		name       string
		envValue   string
		wantErr    bool
		wantErrMsg string
		wantVal    int
	}{
		{"min valid", "2", false, "", 2},
		{"max valid", "16", false, "", 16},
		{"mid range", "8", false, "", 8},
		{"below min", "1", true, "HOARDDB_EXPERIMENT_BADGER_NUM_COMPACTORS invalid value", 0},
		{"above max", "17", true, "HOARDDB_EXPERIMENT_BADGER_NUM_COMPACTORS invalid value", 0},
		{"non-integer", "abc", true, "HOARDDB_EXPERIMENT_BADGER_NUM_COMPACTORS invalid value", 0},
		{"empty string not allowed", "", false, "", 0}, // empty means no override
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envValue != "" {
				t.Setenv("HOARDDB_EXPERIMENT_BADGER_NUM_COMPACTORS", tt.envValue)
			}
			baseOpts := badger.DefaultOptions("/tmp/test")
			baseNumCompactors := baseOpts.NumCompactors
			resultOpts, err := applyExperimentOverrides(baseOpts)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				if tt.wantErrMsg != "" && !containsSubstring(err.Error(), tt.wantErrMsg) {
					t.Errorf("error message %q does not contain %q", err.Error(), tt.wantErrMsg)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if tt.envValue != "" && resultOpts.NumCompactors != tt.wantVal {
					t.Errorf("NumCompactors = %d, want %d", resultOpts.NumCompactors, tt.wantVal)
				} else if tt.envValue == "" && resultOpts.NumCompactors != baseNumCompactors {
					t.Errorf("NumCompactors changed when no override set")
				}
			}
		})
	}
}

func TestApplyExperimentOverridesNumLevelZeroTables(t *testing.T) {
	tests := []struct {
		name       string
		envValue   string
		wantErr    bool
		wantErrMsg string
		wantNum    int
		wantStall  int
	}{
		{"min valid", "2", false, "", 2, 7},
		{"max valid", "64", false, "", 64, 69},
		{"mid range", "10", false, "", 10, 15},
		{"below min", "1", true, "HOARDDB_EXPERIMENT_BADGER_NUM_LEVEL_ZERO_TABLES invalid value", 0, 0},
		{"above max", "65", true, "HOARDDB_EXPERIMENT_BADGER_NUM_LEVEL_ZERO_TABLES invalid value", 0, 0},
		{"non-integer", "abc", true, "HOARDDB_EXPERIMENT_BADGER_NUM_LEVEL_ZERO_TABLES invalid value", 0, 0},
		{"empty string not allowed", "", false, "", 0, 0}, // empty means no override
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envValue != "" {
				t.Setenv("HOARDDB_EXPERIMENT_BADGER_NUM_LEVEL_ZERO_TABLES", tt.envValue)
			}
			baseOpts := badger.DefaultOptions("/tmp/test")
			baseNum := baseOpts.NumLevelZeroTables
			baseStall := baseOpts.NumLevelZeroTablesStall
			resultOpts, err := applyExperimentOverrides(baseOpts)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				if tt.wantErrMsg != "" && !containsSubstring(err.Error(), tt.wantErrMsg) {
					t.Errorf("error message %q does not contain %q", err.Error(), tt.wantErrMsg)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if tt.envValue != "" {
					if resultOpts.NumLevelZeroTables != tt.wantNum {
						t.Errorf("NumLevelZeroTables = %d, want %d", resultOpts.NumLevelZeroTables, tt.wantNum)
					}
					if resultOpts.NumLevelZeroTablesStall != tt.wantStall {
						t.Errorf("NumLevelZeroTablesStall = %d, want %d", resultOpts.NumLevelZeroTablesStall, tt.wantStall)
					}
				} else {
					if resultOpts.NumLevelZeroTables != baseNum {
						t.Errorf("NumLevelZeroTables changed when no override set")
					}
					if resultOpts.NumLevelZeroTablesStall != baseStall {
						t.Errorf("NumLevelZeroTablesStall changed when no override set")
					}
				}
			}
		})
	}
}

func TestApplyExperimentOverridesBaseTableSize(t *testing.T) {
	tests := []struct {
		name       string
		envValue   string
		wantErr    bool
		wantErrMsg string
		wantBytes  int64
	}{
		{"min valid", "1", false, "", 1 << 20},
		{"max valid", "64", false, "", 64 << 20},
		{"mid range", "4", false, "", 4 << 20},
		{"below min", "0", true, "HOARDDB_EXPERIMENT_BADGER_BASE_TABLE_SIZE_MB invalid value", 0},
		{"above max", "65", true, "HOARDDB_EXPERIMENT_BADGER_BASE_TABLE_SIZE_MB invalid value", 0},
		{"non-integer", "xyz", true, "HOARDDB_EXPERIMENT_BADGER_BASE_TABLE_SIZE_MB invalid value", 0},
		{"empty string not allowed", "", false, "", 0}, // empty means no override
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envValue != "" {
				t.Setenv("HOARDDB_EXPERIMENT_BADGER_BASE_TABLE_SIZE_MB", tt.envValue)
			}
			baseOpts := badger.DefaultOptions("/tmp/test")
			baseSize := baseOpts.BaseTableSize
			resultOpts, err := applyExperimentOverrides(baseOpts)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				if tt.wantErrMsg != "" && !containsSubstring(err.Error(), tt.wantErrMsg) {
					t.Errorf("error message %q does not contain %q", err.Error(), tt.wantErrMsg)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if tt.envValue != "" && resultOpts.BaseTableSize != tt.wantBytes {
					t.Errorf("BaseTableSize = %d, want %d", resultOpts.BaseTableSize, tt.wantBytes)
				} else if tt.envValue == "" && resultOpts.BaseTableSize != baseSize {
					t.Errorf("BaseTableSize changed when no override set")
				}
			}
		})
	}
}

// containsSubstring checks if substr is in str, used for loose error message matching.
func containsSubstring(str, substr string) bool {
	for i := 0; i <= len(str)-len(substr); i++ {
		if str[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
