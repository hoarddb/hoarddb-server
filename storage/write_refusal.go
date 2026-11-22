package storage

import (
	"errors"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

// StoreRefusesWritesError is the error every write gets while the node store
// cannot persist what it is given: a memtable flush keeps failing, a full
// memtable cannot be replaced (the directory is gone, the disk is full or
// read-only, the device returns errors), or the store's directory was removed
// or replaced under it (DirectoryGoneError). Acknowledging the write would promise
// what the store cannot keep. Reads are still served.
type StoreRefusesWritesError struct {
	Dir   string // the store directory
	Cause error  // Badger's refusal, naming the failure
}

func (e *StoreRefusesWritesError) Error() string {
	var gone *DirectoryGoneError
	if errors.As(e.Cause, &gone) {
		return "the node store refuses writes: " + gone.Error()
	}
	var unsynced *StoreSyncFailedError
	if errors.As(e.Cause, &unsynced) {
		return fmt.Sprintf("the node store at %s refuses writes: %v. Reads are still served. Check the disk (the kernel log, "+
			"free space), then restart the node: the restart reads back what is on disk and replays the write-ahead log over it",
			e.Dir, unsynced)
	}
	if errors.Is(e.Cause, badger.ErrL0StalledByDamagedTable) {
		// Waiting would not help and neither would free space: the file stays
		// damaged until the node is restored (BUG-0381).
		return fmt.Sprintf("the node store at %s refuses writes: a file of it is damaged, and the merges that would make room "+
			"for new writes fail on it (%v). Reads of undamaged data keep working%s", e.Dir, e.Cause, withRepairAdvice(e.Dir))
	}
	if errors.Is(e.Cause, badger.ErrFlushNotNamed) {
		// The flush is not retried, so waiting or freeing space does not lift this (BUG-0868).
		return fmt.Sprintf("the node store at %s refuses writes: %v. Writes stay refused until the node is restarted, "+
			"and reads are still served. Check the disk (the kernel log), then restart the node: the restart finds the "+
			"table the flush wrote, or replays the memtable log it kept", e.Dir, e.Cause)
	}
	return fmt.Sprintf("the node store at %s refuses writes: it cannot persist them (%v). "+
		"Check that the directory exists, is writable and its disk has free space: the store accepts writes "+
		"again once it can persist them, and a restart replays the memtable logs that are still on disk",
		e.Dir, e.Cause)
}

func (e *StoreRefusesWritesError) Unwrap() error { return e.Cause }

// ErrCommittedNotSynced is wrapped by the error of a Commit(true) whose batch
// committed and whose sync then failed: the batch is in the store and readable,
// and whether it is on disk is not known. It is not a failed commit, so a caller
// must not apply the batch again. The store refuses every later write
// (StoreSyncFailedError) until a restart.
var ErrCommittedNotSynced = errors.New("the batch is committed, but syncing it to disk failed")

// StoreSyncFailedError is the cause every write is refused with after a sync of
// the node store failed. A failed fsync can drop dirty pages and clear the
// error, so a later sync that succeeds proves nothing about what was written
// before it: the store stops acknowledging writes, as the write-ahead log does
// after its own failed fsync (BUG-0807, BUG-0809).
type StoreSyncFailedError struct {
	Cause error // the failed sync
}

func (e *StoreSyncFailedError) Error() string {
	return fmt.Sprintf("a sync of it failed (%v), so writes it holds in memory may not be on disk, and no later sync can show that they are", e.Cause)
}

func (e *StoreSyncFailedError) Unwrap() error { return e.Cause }

// refusedWrite wraps err in a StoreRefusesWritesError when it is Badger's
// write refusal, and returns it unchanged otherwise.
func refusedWrite(dir string, err error) error {
	if errors.Is(err, badger.ErrWritesRefused) {
		return &StoreRefusesWritesError{Dir: dir, Cause: err}
	}
	return err
}

// WriteRefusal returns the error writes are refused with, or nil while the
// store accepts them. The server reports it in HEALTH_CHECK and /ready.
func (kv *BadgerKV) WriteRefusal() error {
	if poison := kv.syncPoison.Load(); poison != nil {
		return &StoreRefusesWritesError{Dir: kv.dir, Cause: poison}
	}
	if err := kv.dirGuard.check(); err != nil {
		return &StoreRefusesWritesError{Dir: kv.dir, Cause: err}
	}
	if err := kv.db.WriteRefusal(); err != nil {
		return &StoreRefusesWritesError{Dir: kv.dir, Cause: err}
	}
	return nil
}
