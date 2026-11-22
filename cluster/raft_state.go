package cluster

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	bolt "go.etcd.io/bbolt"
)

// RaftStateOnDisk reports whether the Raft directory under dir holds state from an earlier run, as
// the library judges it at OpenRaft (hraft.HasExistingState: a current term, a log entry or a
// snapshot), before the Raft is opened. The ring node asks it before its store's first start
// (review R-1), when the Raft is not open yet. A directory with no raft.db and no snapshot
// directory has none, and nothing is created; otherwise it opens the stores OpenRaft opens a moment
// later, reads, and closes them.
func RaftStateOnDisk(dir string) (bool, error) {
	dbPath := filepath.Join(dir, "raft", "raft.db")
	dbExists, err := pathExists(dbPath)
	if err != nil {
		return false, err
	}
	snapsExist, err := pathExists(snapshotDir(dir))
	if err != nil {
		return false, err
	}
	if !dbExists && !snapsExist {
		return false, nil
	}
	if snapsExist {
		snaps, err := hraft.NewFileSnapshotStoreWithLogger(snapshotStoreDir(dir), raftSnapshotsRetained, hclog.NewNullLogger())
		if err != nil {
			return false, fmt.Errorf("opening the raft snapshot store in %s: %w", snapshotStoreDir(dir), err)
		}
		listed, err := snaps.List()
		if err != nil {
			return false, fmt.Errorf("listing the raft snapshots in %s: %w", snapshotStoreDir(dir), err)
		}
		if len(listed) > 0 {
			return true, nil
		}
	}
	if !dbExists {
		return false, nil
	}
	store, err := raftboltdb.New(raftboltdb.Options{Path: dbPath, BoltOptions: &bolt.Options{Timeout: raftStateOpenTimeout}})
	if err != nil {
		return false, fmt.Errorf("opening the raft log %s (is another process running on this ring directory?): %w", dbPath, err)
	}
	hasState, err := hraft.HasExistingState(store, store, hraft.NewDiscardSnapshotStore())
	if closeErr := store.Close(); closeErr != nil {
		err = errors.Join(err, fmt.Errorf("closing the raft log %s: %w", dbPath, closeErr))
	}
	if err != nil {
		return false, fmt.Errorf("reading the raft state in %s: %w", dbPath, err)
	}
	return hasState, nil
}

// raftStateOpenTimeout bounds the wait for the Raft log's file lock in RaftStateOnDisk: a lock held
// means another process runs on this ring directory, and the probe fails naming the file instead
// of waiting for ever (review R2 F-10).
const raftStateOpenTimeout = 5 * time.Second

// pathExists reports whether path exists.
func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("looking at %s: %w", path, err)
	}
	return true, nil
}

// RaftHoldsDatabase reports whether the Raft directory under dir holds a database declaration: a
// snapshot whose state lists a database, or a log entry that is a CREATE_DATABASE command (a
// database dropped since still counts: it was declared). A node whose store and log are gone and
// whose Raft holds one has lost data the Raft cannot give back; one that never declared a database
// has lost nothing. It reads before the Raft is opened, as RaftStateOnDisk does, and creates
// nothing.
func RaftHoldsDatabase(dir string) (bool, error) {
	if exists, err := pathExists(snapshotDir(dir)); err != nil {
		return false, err
	} else if exists {
		snaps, err := hraft.NewFileSnapshotStoreWithLogger(snapshotStoreDir(dir), raftSnapshotsRetained, hclog.NewNullLogger())
		if err != nil {
			return false, fmt.Errorf("opening the raft snapshot store in %s: %w", snapshotStoreDir(dir), err)
		}
		listed, err := snaps.List()
		if err != nil {
			return false, fmt.Errorf("listing the raft snapshots in %s: %w", snapshotStoreDir(dir), err)
		}
		if len(listed) > 0 {
			_, src, err := snaps.Open(listed[0].ID)
			if err != nil {
				return false, fmt.Errorf("opening the raft snapshot %s: %w", listed[0].ID, err)
			}
			raw, err := io.ReadAll(io.LimitReader(src, maxSnapshotBytes))
			if closeErr := src.Close(); err == nil {
				err = closeErr
			}
			if err != nil {
				return false, fmt.Errorf("reading the raft snapshot %s: %w", listed[0].ID, err)
			}
			st, err := DecodeSnapshot(raw)
			if err != nil {
				return false, fmt.Errorf("decoding the raft snapshot %s: %w", listed[0].ID, err)
			}
			if len(st.Databases) > 0 {
				return true, nil
			}
		}
	}
	dbPath := filepath.Join(dir, "raft", "raft.db")
	if exists, err := pathExists(dbPath); err != nil || !exists {
		return false, err
	}
	store, err := raftboltdb.New(raftboltdb.Options{Path: dbPath, BoltOptions: &bolt.Options{Timeout: raftStateOpenTimeout}})
	if err != nil {
		return false, fmt.Errorf("opening the raft log %s (is another process running on this ring directory?): %w", dbPath, err)
	}
	found, err := scanForCreateDatabase(store)
	if closeErr := store.Close(); closeErr != nil {
		err = errors.Join(err, fmt.Errorf("closing the raft log %s: %w", dbPath, closeErr))
	}
	if err != nil {
		return false, fmt.Errorf("reading the raft log in %s: %w", dbPath, err)
	}
	return found, nil
}

// scanForCreateDatabase reports whether any command entry of the log is a CREATE_DATABASE.
func scanForCreateDatabase(store hraft.LogStore) (bool, error) {
	first, err := store.FirstIndex()
	if err != nil {
		return false, err
	}
	last, err := store.LastIndex()
	if err != nil {
		return false, err
	}
	for index := first; index <= last && index > 0; index++ {
		var entry hraft.Log
		if err := store.GetLog(index, &entry); err != nil {
			return false, fmt.Errorf("reading raft log index %d: %w", index, err)
		}
		if entry.Type == hraft.LogCommand && len(entry.Data) >= 2 && entry.Data[1] == CommandCreateDatabase {
			return true, nil
		}
	}
	return false, nil
}
