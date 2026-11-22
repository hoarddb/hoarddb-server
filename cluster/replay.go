package cluster

import (
	"errors"
	"fmt"
	"sync"

	"github.com/hoarddb/hoarddb-server/storage"
)

// replayBatchBytes bounds the log read per batch, so a replay over a long log
// holds a window of it and not all of it.
const replayBatchBytes = 1 << 20

// Cursor is a node-local WAL sequence: the number AppendFrom returned for a
// record, which the pair record stores so a replay can seek (conv §9).
type Cursor uint64

// LogReader is the part of storage.WAL a replay reads through. *storage.WAL
// implements it.
type LogReader interface {
	OpenCursor(from uint64) (storage.WALCursor, error)
	ReadBatch(cur storage.WALCursor, upTo uint64, maxBytes int) ([]storage.WALEntry, storage.WALCursor, error)
	SyncedSequence() uint64
}

// WALRecord is one retained-log record with the pair, position and cursor of
// its log entry: what a replay hands the apply. The apply is deterministic
// and evaluates no schema fence, because the record carries the declaration
// version the owner used (conv §9).
type WALRecord struct {
	storage.WALRecord
	Slot   uint16
	DB     uint64
	Pos    Position
	Cursor Cursor
}

// Replayer replays the node's one retained log into the pairs' stores after a
// restart and says when a pair may serve. Badger may be behind the log, so a
// pair serves nothing until its replay reaches the log tail (write §3.3).
type Replayer struct {
	log     LogReader
	applied func(slot uint16, db uint64) Position

	mu        sync.Mutex
	completed bool
}

// NewReplayer returns a Replayer over log. applied reports a pair's recorded
// applied position (zero when it has none); the replay resumes each pair after
// it, so a record at or before it is not delivered again.
func NewReplayer(log LogReader, applied func(slot uint16, db uint64) Position) *Replayer {
	return &Replayer{log: log, applied: applied}
}

// Replay reads the log from the cursor to the tail it has when Replay starts
// and calls fn for each record its pair has not applied yet, in log order. A
// record after until is not delivered; the zero Position means no bound.
// Every entry must be keyed by a position: an entry of any other shape is an
// error naming its sequence, because skipping it would hide a record from the
// replay. Replay returns fn's error wrapped, and a pair is current only after
// a Replay without an until bound has returned nil.
func (r *Replayer) Replay(from Cursor, until Position, fn func(WALRecord) error) error {
	r.mu.Lock()
	r.completed = false
	r.mu.Unlock()

	tail := r.log.SyncedSequence()
	if uint64(from) <= tail {
		if err := r.scan(from, tail, until, fn); err != nil {
			return err
		}
	}
	if until.Zero() {
		r.mu.Lock()
		r.completed = true
		r.mu.Unlock()
	}
	return nil
}

func (r *Replayer) scan(from Cursor, tail uint64, until Position, fn func(WALRecord) error) error {
	cursor, err := r.log.OpenCursor(uint64(from))
	if err != nil {
		return fmt.Errorf("replay: opening the log at sequence %d: %w", from, err)
	}
	for {
		batch, next, err := r.log.ReadBatch(cursor, tail, replayBatchBytes)
		if err != nil {
			return fmt.Errorf("replay: reading the log: %w", err)
		}
		for _, entry := range batch {
			if err := r.deliver(entry, until, fn); err != nil {
				return err
			}
		}
		cursor = next
		if len(batch) == 0 {
			return nil
		}
	}
}

func (r *Replayer) deliver(entry storage.WALEntry, until Position, fn func(WALRecord) error) error {
	slot, db, logged, err := storage.DecodePositionKey(entry.Key)
	if err != nil {
		return fmt.Errorf("replay: log sequence %d is not a position-keyed record: %w", entry.Seq, err)
	}
	pos := Position(logged)
	// Every record the replay reads is decoded, applied or not: a log of another format
	// must stop the open at the first record read (R1), and a fully applied log would
	// otherwise be skipped record by record without one being looked at.
	record, err := storage.DecodeWALRecord(entry.Op, entry.Data)
	if err != nil {
		return replayRecordRefusal(entry.Seq, slot, pos, err)
	}
	if !r.applied(slot, db).Less(pos) || (!until.Zero() && until.Less(pos)) {
		return nil
	}
	if err := fn(WALRecord{WALRecord: record, Slot: slot, DB: db, Pos: pos, Cursor: Cursor(entry.Seq)}); err != nil {
		return replayRecordRefusal(entry.Seq, slot, pos, err)
	}
	return nil
}

// ReplayRecordError is the replay's refusal of one record (storage-format spec §1.6): R1, a
// record of another version, or R2, a record that cannot be trusted. Its text names the sequence
// and what the record holds, names no other build, and gives no advice on the directory:
// whether moving it aside or starting empty is safe depends on whether the node is a cluster
// member, which the replayer does not know, so the ring node adds the advice (store-vouched-cut
// review R-1).
type ReplayRecordError struct {
	Seq  uint64 // the log sequence of the record
	Text string // what was refused
	Err  error  // the decoder's refusal
}

// Error is the refusal's text, then its cause.
func (e *ReplayRecordError) Error() string { return e.Text + ": " + e.Err.Error() }

// Unwrap returns the decoder's refusal.
func (e *ReplayRecordError) Unwrap() error { return e.Err }

// replayRecordRefusal wraps what stopped the replay at one record. A record of another
// version is R1 and a record that cannot be trusted (its body's class differs from the
// entry's, or its flags carry a bit this build does not know) is R2 (storage-format spec
// §1.6): each names the sequence, because the open turns it into the node's refusal and
// nothing else explains it to the operator. Neither names another build: none was released
// (owner ruling 2026-11-21), and the fix is the advice the node adds. R2 does not call the log
// damaged: the record was decoded after the log's checksum passed, so it was written that way.
func replayRecordRefusal(seq uint64, slot uint16, pos Position, err error) error {
	var version *storage.WALRecordVersionError
	if errors.As(err, &version) {
		return &ReplayRecordError{Seq: seq, Err: err, Text: fmt.Sprintf("retained-log record at sequence %d: version %#02x is not "+
			"the 0x02 this release reads. Nothing was applied", seq, version.Version)}
	}
	var class *storage.WALRecordClassError
	var flags *FlagsError
	var why string
	switch {
	case errors.As(err, &class):
		why = class.Error()
	case errors.As(err, &flags):
		why = flags.Error()
	default:
		return fmt.Errorf("replay: log sequence %d (slot %d, position %s): %w", seq, slot, pos, err)
	}
	return &ReplayRecordError{Seq: seq, Err: err, Text: fmt.Sprintf("retained-log record at sequence %d (position %s): %s, so "+
		"the record cannot be trusted. Nothing was applied. This release does not know how that record was written", seq, pos, why)}
}

// AtTail reports whether the pair's replay has reached the log tail. The
// answer is a property of the one node-wide log, so every pair is current at
// the same moment; the pair is named so a caller states which pair it asks
// about, and the first per-pair reason to hold one back changes only this
// method.
func (r *Replayer) AtTail(_ uint16, _ uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.completed
}
