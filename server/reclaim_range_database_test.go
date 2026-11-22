package server

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// TestReclaimRecordRangeDatabase pins which database each reclaim-record
// writer's range sits under: a bucket drop and a database-drop expansion name
// Database; a failed materialise leaves Database empty (it must not read as a
// drop) and names RangeDatabase. Integration cover per writer:
// TestDropPendingPrefixesTakesTheSlot (bucket drop), TestDropDatabase* (the
// expansion) and TestFailedMaterializeRangeIsReclaimed (failed materialise).
func TestReclaimRecordRangeDatabase(t *testing.T) {
	want := storage.LegacyDatabaseID("app")
	for name, record := range map[string]reclaimRecord{
		"a bucket drop":             {Database: "app", Name: "users"},
		"a database-drop expansion": {Database: "app", Name: "users", DroppedSeq: 3},
		"a failed materialise":      {RangeDatabase: "app"},
	} {
		if got := record.rangeDatabaseID(); got != want {
			t.Errorf("%s: range database id %d, want %d", name, got, want)
		}
	}
}
