package config

import (
	"strings"
	"testing"
)

// TestBadgerTableSyncDefaultsToMsync checks storage.badger_table_sync: a node
// syncs each new table with Badger's single msync unless the file or the
// variable opts in to the stepwise writeback, and the variable wins over the
// file. Stepwise is safe only on a filesystem that commits its journal in
// order (ext4, XFS), which the server cannot detect, so it is never the
// default.
func TestBadgerTableSyncDefaultsToMsync(t *testing.T) {
	if got := DefaultConfig().Storage.BadgerTableSync; got != TableSyncMsync {
		t.Fatalf("DefaultConfig badger_table_sync = %q, want %q", got, TableSyncMsync)
	}
	cases := []struct {
		name string
		yaml string
		env  string
		want string
	}{
		{name: "no file, no variable", want: "msync"},
		{name: "file without the key", yaml: "storage:\n  data_dir: ./data\n", want: "msync"},
		{name: "file", yaml: "storage:\n  badger_table_sync: stepwise\n", want: "stepwise"},
		{name: "variable", env: "stepwise", want: "stepwise"},
		{name: "variable over file", yaml: "storage:\n  badger_table_sync: stepwise\n", env: "msync", want: "msync"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOARDDB_BADGER_TABLE_SYNC", tc.env)
			path := ""
			if tc.yaml != "" {
				path = writeYAML(t, tc.yaml)
			}
			cfg, err := Load(path)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.Storage.BadgerTableSync != tc.want {
				t.Fatalf("badger_table_sync = %q, want %q", cfg.Storage.BadgerTableSync, tc.want)
			}
		})
	}
}

// TestBadgerTableSyncUnknownValueIsRefusedWithTheFix checks that a mistyped
// value stops the start naming the key, the variable and both values, instead
// of reaching the store.
func TestBadgerTableSyncUnknownValueIsRefusedWithTheFix(t *testing.T) {
	for _, yamlValue := range []string{"fsync", `""`, "Stepwise"} {
		t.Run(yamlValue, func(t *testing.T) {
			t.Setenv("HOARDDB_BADGER_TABLE_SYNC", "")
			_, err := Load(writeYAML(t, "storage:\n  badger_table_sync: "+yamlValue+"\n"))
			if err == nil {
				t.Fatalf("Load with badger_table_sync: %s succeeded, want it refused", yamlValue)
			}
			for _, want := range []string{"storage.badger_table_sync", "HOARDDB_BADGER_TABLE_SYNC", "stepwise", "msync", `the default, "msync"`} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal %q does not name %q", err, want)
				}
			}
		})
	}
}
