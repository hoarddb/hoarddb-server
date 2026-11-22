package storage

import (
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4/options"
)

// TestBadgerTableSyncReachesTheStoreOptions checks that BadgerTuning.TableSync
// picks Badger's table sync mode: empty (a caller that never set it, such as
// a bench or a test) and "msync" are upstream's single msync, "stepwise" is
// the opt-in stepwise writeback, and anything else is refused before Badger
// opens.
func TestBadgerTableSyncReachesTheStoreOptions(t *testing.T) {
	cases := []struct {
		tableSync string
		want      options.TableSyncMode
	}{
		{"", options.TableSyncMsync},
		{TableSyncStepwise, options.TableSyncStepwise},
		{TableSyncMsync, options.TableSyncMsync},
	}
	for _, tc := range cases {
		opts, err := badgerOptions(t.TempDir(), BadgerTuning{MemTableMB: 32, TableSync: tc.tableSync}, nil)
		if err != nil {
			t.Fatalf("badgerOptions with TableSync %q: %v", tc.tableSync, err)
		}
		if opts.TableSyncMode != tc.want {
			t.Errorf("TableSync %q gave mode %d, want %d", tc.tableSync, opts.TableSyncMode, tc.want)
		}
	}

	_, err := badgerOptions(t.TempDir(), BadgerTuning{MemTableMB: 32, TableSync: "fsync"}, nil)
	if err == nil || !strings.Contains(err.Error(), `"fsync"`) {
		t.Fatalf("badgerOptions with TableSync \"fsync\": %v, want a refusal naming the value", err)
	}
}
