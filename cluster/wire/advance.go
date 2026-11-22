package wire

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// errNothingStaged is what a Stage callback returns when the step turned out to be a
// refusal, a hold, a forward or a dedupe answer: no batch is committed and no pair record
// moves. The caller keeps the reason in its own closure; the advancer only has to stop.
var errNothingStaged = errors.New("nothing was staged")

// errOwedNotHeld marks the refusal of the owed replay (BUG-0809) of a pair this node no longer
// holds: the pair stays fenced, but the refusal belongs to that pair's own recovery, not to a scan
// that merely passes its record.
var errOwedNotHeld = errors.New("the owed record's pair is not held here")

// AdvanceTo is the pair record a step moves to: the position applied, the WAL cursor of
// the record that holds it and the pair's ordinal once it counts (conv §12, §18).
type AdvanceTo struct {
	Applied   cluster.Position
	WALCursor uint64
	Ordinal   uint64
}

// AdvanceStep is one write's, or one replayed record's, part of the pair record advance.
// Both callbacks run under the pair's lock, so neither may call back into the PairSet for
// the same pair or into the advancer. For an RF 1 owner write the append is not in Stage:
// it happened in the first section, and Stage (the fill) and Commit run in the second,
// after the fsync, under the pair's lock.
type AdvanceStep struct {
	// Stage orders and appends the write when it is a write, writes its keys into batch
	// and returns the record the batch moves the pair to. It may read the pair and may set
	// its AppendedSeq and Flags, which the append and a failed apply change; it must not
	// set AppliedPos or Ordinal, which the advancer installs after Commit. Returning an
	// error commits nothing.
	Stage func(batch storage.KVBatch, pair *cluster.Pair) (AdvanceTo, error)
	// Commit commits batch, synced when sync is set. The advancer has already added the pair
	// record to it. A write commits without sync, because the WAL record is durable and a
	// replay re-applies it, except when the batch creates the pair's record: a replay skips a
	// pair with no record, so that record must not be lost to a power loss (BUG-0809).
	Commit func(batch storage.KVBatch, sync bool) error
	// Replay says the step is the boot replay of a log record (Writer.Replay sets it, nothing
	// else does), not a live write or apply. A record-less pair is skipped on a replay and
	// judged against the target set on a live step (seeding-completion plan §0.2).
	Replay bool
	// Pos is the replayed record's position (Replay only): the advancer compares it with the pair's last
	// appended position to find the record a row 9 left a record-less pair owing (BUG-0809).
	Pos cluster.Position
	// Admitted says the step applies a record that an earlier critical section admitted and appended:
	// the second section of a pipelined owner write. The advancer does not judge the pair's copy set
	// again, because the committed cluster state moves without the pair's lock and a refusal after the
	// append would leave a durable position no replay applies (pair-lock spec I11, F8).
	Admitted bool
	// Ctx bounds any wait the advancer makes before the step runs (a pair busy in a
	// reconcile pass's batch); nil waits on the background context.
	Ctx context.Context
	// Appended says, once Stage has returned, whether the step appended a record to the log. The
	// advancer reads it on every exit to release the log-floor hold of a step that appended
	// nothing (store-vouched-cut-build.md §2). A live step must set it; a nil Appended keeps the
	// hold (the safe direction). A replay step registers no hold and may leave it nil.
	Appended func() bool
}

// PairAdvancer moves a pair's record in the same batch as the data it describes, as one
// step under the pair's lock. Reconciler.ApplyAdvance of U44 is one; LocalAdvancer is the
// one a node without a seed record per pair uses.
type PairAdvancer interface {
	// Advance runs step under the pair's lock and, when Stage and Commit both succeed,
	// installs the position and ordinal it staged. The caller must not hold the pair's lock.
	Advance(slot uint16, db uint64, step AdvanceStep) error
}

// LockedAdvancer is the entry point of a caller that already holds the pair's lock and
// has the pair in hand: a copy's apply decides on the pair's sequence under the lock and
// must stage the record in the same critical section (BUG-0752). It does what Advance does
// and does not take the lock, so it must not be called without it, and it never waits: a
// pair a reconcile pass holds busy answers ErrPairBusy at once.
type LockedAdvancer interface {
	AdvanceLocked(pair *cluster.Pair, slot uint16, db uint64, step AdvanceStep) error
}

// PipelineAdvancer is what the owner's RF 1 write path needs of its advancer to split a write in two
// sections of the pair's lock around its WAL fsync (pair-lock-across-fsync spec 3.2): the checks that
// precede the stage in Advance run in the first section as PrepareLocked, and the stage and commit run in
// the second as AdvanceLocked. Both must be called with the pair's lock held, and neither waits.
type PipelineAdvancer interface {
	LockedAdvancer
	// PrepareLocked is the part of Advance before Stage: it returns ErrPairBusy for a pair a reconcile
	// pass holds busy (the caller releases the lock, waits and starts again), and any other error when
	// the pair may take no write now. recordless says the pair has no record yet, so the write it admits
	// is the one whose apply creates it: nothing may be appended behind that write until it has applied,
	// because a failure of that apply cannot be replayed (a replay stages nothing for a pair with no
	// record). For a pair with no log cursor it registers the log-floor hold before the caller appends
	// (store-vouched-cut-spec.md §3.6); held says this call added it. It changes nothing else.
	PrepareLocked(pair *cluster.Pair, slot uint16, db uint64) (recordless, held bool, err error)
	// AbandonLocked releases the hold a PrepareLocked of this write added, on an exit of the first
	// section that appended nothing: the advancer's Stage never runs there, so Appended cannot.
	AbandonLocked(slot uint16, db uint64)
}

// LocalAdvancer is a PairAdvancer over the node's PairSet and Badger. It writes the pair
// record as 32 bytes under the seed-state namespace: [epoch:8][seq:8][cursor:8][ordinal:8],
// all big endian.
type LocalAdvancer struct {
	pairs *cluster.PairSet
	kv    BatchSource
	// floor is the node's log-floor tracker; nil registers no hold (a rig without a node).
	floor *logFloor
}

// NewLocalAdvancer returns an advancer over pairs that batches through kv.
func NewLocalAdvancer(pairs *cluster.PairSet, kv BatchSource) *LocalAdvancer {
	return &LocalAdvancer{pairs: pairs, kv: kv}
}

// localPairRecordLen is the size of the record LocalAdvancer writes.
const localPairRecordLen = 32

// LocalPairRecordKey is where LocalAdvancer keeps the record of one pair.
func LocalPairRecordKey(slot uint16, db uint64) []byte {
	var pair [10]byte
	binary.BigEndian.PutUint16(pair[:2], slot)
	binary.BigEndian.PutUint64(pair[2:], db)
	return storage.NodeKey(storage.NSSeedState, pair[:])
}

// Advance implements PairAdvancer.
func (a *LocalAdvancer) Advance(slot uint16, db uint64, step AdvanceStep) error {
	var failure error
	a.pairs.With(slot, db, func(pair *cluster.Pair) {
		failure = a.advanceLocked(pair, slot, db, step)
	})
	return failure
}

// AdvanceLocked implements LockedAdvancer.
func (a *LocalAdvancer) AdvanceLocked(pair *cluster.Pair, slot uint16, db uint64, step AdvanceStep) error {
	return a.advanceLocked(pair, slot, db, step)
}

// PrepareLocked implements PipelineAdvancer: a node without a seed record per pair has nothing to check,
// and registers the floor hold of a pair with no cursor.
func (a *LocalAdvancer) PrepareLocked(pair *cluster.Pair, slot uint16, db uint64) (bool, bool, error) {
	held := false
	if pair.TailCursor == 0 {
		held = a.floor.hold(pairKey{slot, db})
	}
	return false, held, nil
}

// AbandonLocked implements PipelineAdvancer.
func (a *LocalAdvancer) AbandonLocked(slot uint16, db uint64) {
	a.floor.release(pairKey{slot, db})
}

func (a *LocalAdvancer) advanceLocked(pair *cluster.Pair, slot uint16, db uint64, step AdvanceStep) error {
	key := pairKey{slot, db}
	// Registered before Stage appends: the record a pair with no cursor appends must sit at or
	// above the floor until its commit returns (§3.6). A replay registers nothing; the boot hold
	// covers it.
	added := false
	if !step.Replay && pair.TailCursor == 0 {
		added = a.floor.hold(key)
	}
	batch := a.kv.NewBatch()
	defer batch.Discard()
	to, err := step.Stage(batch, pair)
	if err != nil {
		a.floor.releaseUnappended(key, added, step.Appended)
		return err
	}
	var record [localPairRecordLen]byte
	binary.BigEndian.PutUint64(record[0:], to.Applied.Epoch)
	binary.BigEndian.PutUint64(record[8:], to.Applied.Seq)
	binary.BigEndian.PutUint64(record[16:], to.WALCursor)
	binary.BigEndian.PutUint64(record[24:], to.Ordinal)
	if err := batch.Set(LocalPairRecordKey(slot, db), record[:]); err != nil {
		return fmt.Errorf("staging the pair record of slot %d database %d: %w", slot, db, err)
	}
	if beforeAdvanceCommit != nil {
		beforeAdvanceCommit(slot, db)
	}
	// A replay of a pair with no 32-byte record still applies it, so this record never needs
	// the sync the Reconciler's created record does.
	if err := step.Commit(batch, false); err != nil {
		return err
	}
	pair.AppliedPos, pair.Ordinal, pair.TailCursor = to.Applied, to.Ordinal, to.WALCursor
	// The commit returned and the pair has a cursor: its record no longer needs the floor, and
	// a replay releases a failed first write's hold here too.
	if pair.TailCursor != 0 {
		a.floor.release(key)
	}
	return nil
}

// LoadLocalPairRecords reads every 32-byte pair record into pairs at boot: the applied
// position and, through CutOrdinal, the ordinal, before the pair's first lookup. It returns
// the place a log-tail scan starts: the lowest non-zero WAL cursor of the 32-byte records
// and of the ready seed records, or 1 when none holds one.
//
// On a cluster node (onClusterNode) every 32-byte record is first rewritten as a ready seed
// record at the same position, cursor and ordinal, in one synced batch, so that after this
// call every pair of a cluster node has one record shape and advances through the
// Reconciler (seeding-completion plan §0.2; SPEC-GAP T3-1 closed). A single node has no
// Reconciler to read a seed record, so it keeps the 32-byte shape, which this loader is the
// only reader of. A seeding record's cursor is the pull's, not the log's, and a retired
// record's is stale, so neither is folded into the scan start.
func LoadLocalPairRecords(kv storage.KV, pairs *cluster.PairSet, onClusterNode bool) (uint64, error) {
	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	lower := storage.NodeKey(storage.NSSeedState)
	iter := snapshot.NewIterator(storage.IterOptions{Lower: lower, Upper: keyRangeEnd(lower)})
	defer iter.Close()
	rewrite := kv.NewBatch()
	defer rewrite.Discard()
	rewrites := 0
	lowest := uint64(0)
	fold := func(cursor uint64) {
		if cursor != 0 && (lowest == 0 || cursor < lowest) {
			lowest = cursor
		}
	}
	for iter.Seek(lower); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) != len(lower)+10 {
			return 0, fmt.Errorf("the seed-state namespace holds a key of %d bytes, want %d: the store is damaged", len(key), len(lower)+10)
		}
		raw, err := iter.Value()
		if err != nil {
			return 0, fmt.Errorf("reading the pair record under key %x: %w", key, err)
		}
		if len(raw) >= seedRecordFixedBytes {
			// A seed record: the Reconciler loads it. Only its scan start is read here.
			record, err := DecodeSeedRecord(raw)
			if err != nil {
				return 0, fmt.Errorf("reading the seed record under key %x: %w", key, err)
			}
			if record.State == machine.SeedReady {
				fold(record.WALCursor)
			}
			continue
		}
		if len(raw) != localPairRecordLen {
			return 0, fmt.Errorf("the pair record under key %x has %d bytes, want %d", key, len(raw), localPairRecordLen)
		}
		slot := binary.BigEndian.Uint16(key[len(lower):])
		db := binary.BigEndian.Uint64(key[len(lower)+2:])
		applied := cluster.Position{Epoch: binary.BigEndian.Uint64(raw[0:]), Seq: binary.BigEndian.Uint64(raw[8:])}
		cursor := binary.BigEndian.Uint64(raw[16:])
		ordinal := binary.BigEndian.Uint64(raw[24:])
		pairs.With(slot, db, func(pair *cluster.Pair) {
			pair.AppliedPos, pair.TailCursor = applied, cursor
			CutOrdinal(pair, ordinal)
		})
		fold(cursor)
		if onClusterNode {
			// The rewritten record is "ready" with a zero SeedID and no source: the signature of a
			// pair that was written here and never seeded, which S3b's new-database rule reads.
			seed := SeedRecord{WALCursor: cursor, Ordinal: ordinal}
			seed.State, seed.Applied = machine.SeedReady, applied
			encoded, err := EncodeSeedRecord(seed)
			if err != nil {
				return 0, fmt.Errorf("rewriting the pair record of slot %d database %d: %w", slot, db, err)
			}
			if err := rewrite.Set(append([]byte(nil), key...), encoded); err != nil {
				return 0, fmt.Errorf("rewriting the pair record of slot %d database %d: %w", slot, db, err)
			}
			rewrites++
		}
	}
	if err := iter.Err(); err != nil {
		return 0, fmt.Errorf("reading the pair records: %w", err)
	}
	if rewrites > 0 {
		// Synced: the tail scan that follows trusts the record it reads, and a crash between
		// the rewrite and the scan must find either shape, never a torn one.
		if err := rewrite.Commit(true); err != nil {
			return 0, fmt.Errorf("rewriting %d pair records as seed records: %w", rewrites, err)
		}
	}
	return max(lowest, 1), nil
}
