package cluster

import (
	"path/filepath"
	"strings"
	"testing"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

func testLogStore(t *testing.T, entries ...*hraft.Log) *raftboltdb.BoltStore {
	t.Helper()
	store, err := raftboltdb.NewBoltStore(filepath.Join(t.TempDir(), "raft.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for _, entry := range entries {
		if err := store.StoreLog(entry); err != nil {
			t.Fatal(err)
		}
	}
	return store
}

// TestLastCommandIndexSkipsConfigurationEntries: the gate the seed driver waits on compares
// the FSM's applied index with the last command entry, not with a configuration or no-op
// entry the FSM never sees.
func TestLastCommandIndexSkipsConfigurationEntries(t *testing.T) {
	store := testLogStore(t,
		&hraft.Log{Index: 1, Term: 1, Type: hraft.LogCommand, Data: []byte{1}},
		&hraft.Log{Index: 2, Term: 1, Type: hraft.LogCommand, Data: []byte{1}},
		&hraft.Log{Index: 3, Term: 1, Type: hraft.LogConfiguration},
		&hraft.Log{Index: 4, Term: 2, Type: hraft.LogNoop},
	)
	last, err := lastCommandIndex(store)
	if err != nil || last != 2 {
		t.Fatalf("lastCommandIndex = %d, %v; want 2", last, err)
	}
	empty := testLogStore(t)
	if last, err := lastCommandIndex(empty); err != nil || last != 0 {
		t.Fatalf("lastCommandIndex of an empty log = %d, %v; want 0", last, err)
	}
}

// TestLastCommandIndexRefusesAHoleInTheLog (pi S-3): a missing entry between the first and
// the last index is a damaged or partially compacted log; answering 0 would open the seed
// driver's gate at once, which is the window it exists to close, so the open fails and the
// message names the log and the fix. [BREAK: return (0, nil) on ErrLogNotFound]
func TestLastCommandIndexRefusesAHoleInTheLog(t *testing.T) {
	store := testLogStore(t,
		&hraft.Log{Index: 1, Term: 1, Type: hraft.LogCommand, Data: []byte{1}},
		&hraft.Log{Index: 2, Term: 1, Type: hraft.LogCommand, Data: []byte{1}},
		&hraft.Log{Index: 4, Term: 2, Type: hraft.LogConfiguration}, // 3 is missing
	)
	last, err := lastCommandIndex(store)
	if err == nil {
		t.Fatalf("lastCommandIndex over a hole = %d, nil; want an error that names the log", last)
	}
	for _, want := range []string{"raft log", "index 3", "compacted", "Retry"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the error %q does not say %q", err, want)
		}
	}
	// At replication factor 1 this directory may hold the only copy of the data.
	if strings.Contains(err.Error(), "fresh data directory") {
		t.Fatalf("the error %q tells an operator to discard a directory that may hold the only copy", err)
	}
}
