package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
)

// A node's replication positions live in its store, under namespace
// storage.NSReplication (spec §19.3.8). Three record kinds, told apart by the
// first byte after the namespace:
//
//	"p" ‖ uvarint(len(source)) ‖ source                          the stream position of one source
//	"f" ‖ uvarint(len(source)) ‖ source ‖ PlacementKey(db, bkt)  a snapshot position, 8 bytes big-endian
//	"l" ‖ uvarint(len(source)) ‖ source ‖ bucket                 a snapshot position by bare name (before placement by database)
//
// A fourth source-keyed kind, "s", records an entry of the source this node
// refused as too large for its store (batchSizeRefusal, cluster_memtable.go); it is
// read at start after the positions, to hold one below it.
//
// The namespace holds a further kind that is not a position: "g", a dropped
// database generation (generation.go), keyed by database and not by source.
// The start scan reads the three position kinds only, each by its own range,
// so a "g" record is never taken for a position and never read at start.
//
// Nothing here is written per replicated frame. The log is the record of a
// stream position (rebuildPositionsFromLog); a "p" record is a checkpoint for
// the moves the log cannot carry, and for a log that has been pruned.
const (
	positionRecordKind byte = 'p'
	seededRecordKind   byte = 'f'
	legacyRecordKind   byte = 'l'
)

// positionRecord is the value of a "p" record. Incarnation and Log are the
// source's incarnation and log id the position was learned under, recorded by
// the handshake's rebase (stream_handshake.go); a record written by a build
// before the handshake has neither, and the source's chain judges it as a
// position learned before chains existed. Log is new with the handshake: a
// record from an earlier build decodes without it.
type positionRecord struct {
	Incarnation    []byte `bson:"incarnation"`
	AppliedThrough uint64 `bson:"applied_through"`
	RebasedAt      uint64 `bson:"rebased_at"`
	Log            []byte `bson:"log,omitempty"`
	// Clamped says a rebase once lowered the position: the source's log went
	// backwards (recordedStream.clamped). New with replication dispatch 10b;
	// absent from a record of an earlier build, which reads as false.
	Clamped bool `bson:"clamped,omitempty"`
	// VoidedAbove and VoidedThrough are the source's orphan void
	// (replica_orphans.go): its entries in this node's log at or below local
	// sequence VoidedThrough whose own sequence is above VoidedAbove were
	// logged by a run past an entry that stopped it, never applied, and are
	// neither counted nor replayed. New with BUG-0521; absent, nothing is void.
	VoidedAbove   uint64 `bson:"voided_above,omitempty"`
	VoidedThrough uint64 `bson:"voided_through,omitempty"`
}

// replicationPrefix is the key prefix every record of the namespace shares.
func replicationPrefix() []byte {
	return storage.NodeKey(storage.NSReplication)
}

// replicationRange is the half-open range holding every replication record.
func replicationRange() (lower, upper []byte) {
	return replicationPrefix(), storage.NodeKey(storage.NSReplication + 1)
}

// sourceKeyPrefix is the key prefix of one record kind of one source.
func sourceKeyPrefix(kind byte, source string) []byte {
	return storage.NodeKey(storage.NSReplication,
		[]byte{kind}, binary.AppendUvarint(nil, uint64(len(source))), []byte(source))
}

func positionKey(source string) []byte {
	return sourceKeyPrefix(positionRecordKind, source)
}

func seededKey(source, database, bucket string) []byte {
	return append(sourceKeyPrefix(seededRecordKind, source), storage.PlacementKey(database, bucket, nil)...)
}

func legacySeededKey(source, bucket string) []byte {
	return append(sourceKeyPrefix(legacyRecordKind, source), bucket...)
}

// keyPrefixEnd is the smallest key greater than every key that starts with
// prefix.
func keyPrefixEnd(prefix []byte) []byte {
	end := bytes.Clone(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			return end[:i+1]
		}
	}
	return nil
}

// decodedReplicationKey is one record's key taken apart.
type decodedReplicationKey struct {
	kind     byte
	source   string
	database string // "f" only
	bucket   string // "f" and "l"
}

// decodeReplicationKey reverses positionKey, seededKey and legacySeededKey.
func decodeReplicationKey(key []byte) (decodedReplicationKey, error) {
	prefix := replicationPrefix()
	if !bytes.HasPrefix(key, prefix) || len(key) == len(prefix) {
		return decodedReplicationKey{}, fmt.Errorf("key %x is not a replication record", key)
	}
	rest := key[len(prefix):]
	decoded := decodedReplicationKey{kind: rest[0]}
	rest = rest[1:]
	// A dropped generation is keyed by its database, and what follows its
	// kind byte is no source: it is told apart before a source is read.
	if decoded.kind == tombstoneRecordKind {
		return decodedReplicationKey{}, fmt.Errorf("key %x is a dropped database generation, not a replication position", key)
	}
	source, rest, err := splitLengthPrefixed(rest)
	if err != nil {
		return decodedReplicationKey{}, fmt.Errorf("reading the source of key %x: %w", key, err)
	}
	decoded.source = source
	switch decoded.kind {
	case positionRecordKind, sizeRefusalRecordKind, divergenceRecordKind:
		return decoded, nil
	case legacyRecordKind:
		decoded.bucket = string(rest)
		return decoded, nil
	case seededRecordKind:
		decoded.database, rest, err = splitLengthPrefixed(rest)
		if err != nil {
			return decodedReplicationKey{}, fmt.Errorf("reading the database of key %x: %w", key, err)
		}
		decoded.bucket, _, err = splitLengthPrefixed(rest)
		if err != nil {
			return decodedReplicationKey{}, fmt.Errorf("reading the bucket of key %x: %w", key, err)
		}
		return decoded, nil
	default:
		return decodedReplicationKey{}, fmt.Errorf("key %x has the unknown replication record kind %q", key, decoded.kind)
	}
}

// splitLengthPrefixed reads one uvarint(len) ‖ bytes field off the front of
// encoded and returns it with what follows.
func splitLengthPrefixed(encoded []byte) (string, []byte, error) {
	length, read := binary.Uvarint(encoded)
	if read <= 0 || uint64(len(encoded)-read) < length {
		return "", nil, errors.New("a length-prefixed field runs past the end of the key")
	}
	end := read + int(length)
	return string(encoded[read:end]), encoded[end:], nil
}

func encodeFloor(sequence uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, sequence)
}

func decodeFloor(encoded []byte) (uint64, error) {
	if len(encoded) != 8 {
		return 0, fmt.Errorf("a snapshot position is 8 bytes, this one is %d", len(encoded))
	}
	return binary.BigEndian.Uint64(encoded), nil
}

// loadPositions restores what this node knows about each source's stream: one
// prefix scan of the namespace fills the in-memory positions, then the log and
// the refusals and divergences it holds lower them. A missing record is the
// normal first start and is not an error.
//
// A legacy replication_state.bson is not read here: its fate is a property of
// the data directory, not of the cluster, so NewService imports it for every
// start, standalone included (importLegacyReplicationState, BUG-0216).
func (c *Cluster) loadPositions() error {
	if err := c.loadStoredPositions(); err != nil {
		return err
	}
	if err := c.rebuildPositionsFromLog(); err != nil {
		return err
	}
	if err := c.lowerPastSizeRefusals(); err != nil {
		return err
	}
	return c.lowerPastDivergences()
}

// positionRecordKinds are the record kinds the start scan loads.
var positionRecordKinds = []byte{seededRecordKind, legacyRecordKind, positionRecordKind}

// loadStoredPositions reads the namespace's position records, one prefix scan
// per kind: the namespace also holds dropped generations, which are not
// positions and are never read at start.
func (c *Cluster) loadStoredPositions() error {
	snapshot := c.s.kv.NewSnapshot()
	defer snapshot.Discard()
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	legacyPositions := 0
	for _, kind := range positionRecordKinds {
		loaded, err := c.loadPositionKindLocked(snapshot, kind)
		if err != nil {
			return err
		}
		legacyPositions += loaded
	}
	if len(c.appliedFrom) > 0 || len(c.seededThrough) > 0 {
		c.logger.Info("replication positions loaded", "sources", len(c.appliedFrom), "seeded_sources", len(c.seededThrough))
	}
	if legacyPositions > 0 {
		// Dropping these would let a source's catch-up replay a seeded
		// bucket's history over its snapshot, and which database
		// the snapshot was of was never recorded. So each keeps doing what it
		// did: cover every database's bucket of its name. A new snapshot of a
		// bucket records a database-qualified position beside it.
		c.logger.Warn("replication positions hold snapshot positions recorded by bucket name only, not "+
			"qualified by database; each covers every database's bucket of that name",
			"legacy_positions", legacyPositions)
	}
	return nil
}

// loadPositionKindLocked loads every record of one position kind and returns
// how many legacy snapshot positions it held. The caller holds appliedMu.
func (c *Cluster) loadPositionKindLocked(snapshot storage.KVSnapshot, kind byte) (int, error) {
	lower := storage.NodeKey(storage.NSReplication, []byte{kind})
	iterator := snapshot.NewIterator(storage.IterOptions{Lower: lower, Upper: keyPrefixEnd(lower)})
	defer iterator.Close()
	legacyPositions := 0
	for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		decoded, err := decodeReplicationKey(iterator.Key())
		if err != nil {
			return 0, fmt.Errorf("reading the replication positions: %w", err)
		}
		stored, err := iterator.Value()
		if err != nil {
			return 0, fmt.Errorf("reading the replication record of source %q: %w", decoded.source, err)
		}
		switch decoded.kind {
		case positionRecordKind:
			var record positionRecord
			if err := bson.Unmarshal(stored, &record); err != nil {
				return 0, fmt.Errorf("decoding the position record of source %q: %w", decoded.source, err)
			}
			c.appliedFrom[decoded.source] = record.AppliedThrough
			c.rebasedAt[decoded.source] = record.RebasedAt
			c.recordedStream[decoded.source] = recordedStream{incarnation: record.Incarnation, log: record.Log, clamped: record.Clamped}
			if record.VoidedThrough != 0 {
				c.orphanVoids[decoded.source] = orphanVoid{above: record.VoidedAbove, through: record.VoidedThrough}
			}
		case seededRecordKind:
			floor, err := decodeFloor(stored)
			if err != nil {
				return 0, fmt.Errorf("decoding the snapshot position of %s/%s from %q: %w", decoded.database, decoded.bucket, decoded.source, err)
			}
			c.raiseSeededLocked(decoded.source, decoded.database, decoded.bucket, floor)
		case legacyRecordKind:
			floor, err := decodeFloor(stored)
			if err != nil {
				return 0, fmt.Errorf("decoding the legacy snapshot position of bucket %q from %q: %w", decoded.bucket, decoded.source, err)
			}
			if c.legacySeeded[decoded.source] == nil {
				c.legacySeeded[decoded.source] = make(map[string]uint64)
			}
			c.legacySeeded[decoded.source][decoded.bucket] = floor
			legacyPositions++
		}
	}
	if err := iterator.Err(); err != nil {
		return 0, fmt.Errorf("walking the replication positions: %w", err)
	}
	return legacyPositions, nil
}

// rebuildPositionsFromLog raises each source's position to the highest origin
// sequence among the log's entries that were applied from it. R-5 logs and
// syncs a replica's entry before it applies and acknowledges it, so the log
// holds every position the node ever acknowledged that a checkpoint does not:
// nothing is written per replicated frame for this to read back.
//
// It runs in NewCluster, its own pass with the cursor reader, because the
// replay that applies the log runs later and pushes must not be accepted
// before positions are known. An entry whose apply failed is counted too: it is
// in this node's log and the replay applies it, so a re-send of it is a
// duplicate. Entries at or below a source's rebased_at are not counted, and
// neither are the entries its orphan void names, which a run logged past the
// entry that stopped it and never applied (replica_orphans.go).
func (c *Cluster) rebuildPositionsFromLog() error {
	wal := c.s.wal
	if wal == nil {
		return nil
	}
	first, err := wal.FirstSequence()
	if err != nil {
		return fmt.Errorf("reading the first retained log sequence to rebuild positions: %w", err)
	}
	cursor, err := wal.OpenCursor(first)
	if err != nil {
		return fmt.Errorf("opening the log at sequence %d to rebuild positions: %w", first, err)
	}
	syncedThrough := wal.SyncedSequence()
	for {
		entries, next, err := wal.ReadBatch(cursor, syncedThrough, catchUpReadBytes)
		if err != nil {
			return fmt.Errorf("reading the log to rebuild positions: %w", err)
		}
		if len(entries) == 0 {
			return nil
		}
		cursor = next
		c.raisePositionsFrom(entries)
	}
}

// raisePositionsFrom applies one read of the log to the positions.
func (c *Cluster) raisePositionsFrom(entries []storage.WALEntry) {
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	for _, entry := range entries {
		if entry.Origin.Kind != storage.WALOriginReplica || entry.Seq <= c.rebasedAt[entry.Origin.Source] ||
			c.orphanVoids[entry.Origin.Source].covers(entry) {
			continue
		}
		c.appliedFrom[entry.Origin.Source] = max(c.appliedFrom[entry.Origin.Source], entry.Origin.Seq)
	}
}

// raiseSeededLocked records a snapshot position in memory, with appliedMu held.
func (c *Cluster) raiseSeededLocked(source, database, bucket string, floor uint64) {
	floors, ok := c.seededThrough[source]
	if !ok {
		floors = make(seedFloors)
		c.seededThrough[source] = floors
	}
	floors.raise(database, bucket, floor)
	c.seedCeiling[source] = max(c.seedCeiling[source], floor)
}

// replicationStatePath returns the file the previous build kept its positions
// in. Nothing writes it any more; it is read once, by
// importLegacyReplicationState.
func replicationStatePath(dataDir string) string {
	if dataDir == "" {
		return ""
	}
	return filepath.Join(dataDir, "replication_state.bson")
}

// importLegacyReplicationState moves the positions of replication_state.bson
// into the store and deletes the file. It runs for every start, clustered or
// standalone: the file's fate is a property of the data directory, not of the
// cluster (BUG-0216), so a node that ran clustered and now starts standalone
// leaves no plaintext peer ids and database/bucket names behind, and the
// positions are in the store where a later clustered start reads them.
//
// The positions are committed in one batch and synced before the delete, so a
// file that is gone never had a position that was not durable. It is deleted
// and not kept under another name because it holds peer ids and
// database/bucket names in plaintext, which a sealed node must not leave on its
// disk, and because a file that is left behind is a file a later start can
// import over a position that has since moved down.
//
// A crash between the sync and the delete leaves the file in place, and the
// next start imports it again. That is safe because the import only raises: a
// position already in the store is replaced by the file's only when the file's
// is higher. So an import that runs twice neither counts anything twice nor
// lowers a position the store has moved past since.
//
// remove is the delete, nil for os.Remove; a test passes one that fails to
// build a crash between the sync and the delete.
func importLegacyReplicationState(dataDir string, kv storage.KV, logger *slog.Logger, remove func(string) error) error {
	path := replicationStatePath(dataDir)
	if path == "" {
		return nil
	}
	stateBytes, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("reading the replication state file %s: %w", path, err)
	}
	var legacy replicationState
	if err := bson.Unmarshal(stateBytes, &legacy); err != nil {
		// Starting without these positions can apply a re-sent entry twice — a
		// duplicate queue element, a second pop — and that is a trade only the
		// operator can accept. The server does not move the file itself: a file
		// it set aside would be a silent decision, and one it deleted would take
		// the evidence with it.
		return fmt.Errorf("the replication state file %s cannot be read: %w. "+
			"It holds this node's replication positions and snapshot positions, "+
			"and without them entries that peers re-send may be applied twice (a queue can gain a duplicate element). "+
			"To start without them, move the file aside and start the node again: mv %s %s.unreadable. "+
			"On a node whose store is encrypted, the moved file still holds peer ids and bucket names in plaintext "+
			"and the encryption audit flags it under that name too: delete it once you no longer need it",
			path, err, path, path)
	}
	importer := newPositionImporter(kv)
	defer importer.discard()
	if err := importer.addAll(legacy); err != nil {
		return fmt.Errorf("importing the replication state file %s: %w", path, err)
	}
	if err := importer.commitSynced(); err != nil {
		return fmt.Errorf("committing the replication state file %s: %w", path, err)
	}
	if remove == nil {
		remove = os.Remove
	}
	if err := remove(path); err != nil {
		return fmt.Errorf("the positions in %s are imported, but the file could not be deleted: %w; "+
			"remove it by hand (rm %s) and start the node again", path, err, path)
	}
	logger.Info("imported the replication state file into the node's store",
		"path", path, "sources", len(legacy.AppliedFrom), "seeded_sources", len(legacy.SeededThrough))
	return nil
}

// positionImporter writes a legacy state file's positions as raise-only sets.
// A state file can hold one position per bucket ever seeded, so it commits in
// pieces when one batch would be too big to commit atomically; every piece is
// idempotent, which is what makes the pieces safe.
type positionImporter struct {
	kv        storage.KV
	snapshot  storage.KVSnapshot
	batch     storage.KVBatch
	batchSize int64
}

func newPositionImporter(kv storage.KV) *positionImporter {
	return &positionImporter{kv: kv, snapshot: kv.NewSnapshot(), batch: kv.NewBatch()}
}

func (importer *positionImporter) discard() {
	importer.batch.Discard()
	importer.snapshot.Discard()
}

func (importer *positionImporter) addAll(legacy replicationState) error {
	for source, sequence := range legacy.AppliedFrom {
		if err := importer.addPosition(source, sequence); err != nil {
			return err
		}
	}
	for source, perKey := range legacy.SeededThrough {
		for key, floor := range perKey {
			storedKey := legacySeededKey(source, key)
			if database, bucket, qualified := strings.Cut(key, seededStateKeySeparator); qualified {
				storedKey = seededKey(source, database, bucket)
			}
			if err := importer.addFloor(storedKey, floor); err != nil {
				return err
			}
		}
	}
	return nil
}

func (importer *positionImporter) addPosition(source string, sequence uint64) error {
	key := positionKey(source)
	record := positionRecord{AppliedThrough: sequence}
	stored, err := importer.snapshot.Get(key)
	switch {
	case err == nil:
		if err := bson.Unmarshal(stored, &record); err != nil {
			return fmt.Errorf("decoding the position record of source %q: %w", source, err)
		}
		record.AppliedThrough = max(record.AppliedThrough, sequence)
	case !errors.Is(err, storage.ErrKeyNotFound):
		return fmt.Errorf("reading the position record of source %q: %w", source, err)
	}
	encoded, err := bson.Marshal(record)
	if err != nil {
		return fmt.Errorf("encoding the position record of source %q: %w", source, err)
	}
	return importer.set(key, encoded)
}

func (importer *positionImporter) addFloor(key []byte, floor uint64) error {
	stored, err := importer.snapshot.Get(key)
	switch {
	case err == nil:
		existing, err := decodeFloor(stored)
		if err != nil {
			return fmt.Errorf("reading the stored snapshot position %x: %w", key, err)
		}
		floor = max(floor, existing)
	case !errors.Is(err, storage.ErrKeyNotFound):
		return fmt.Errorf("reading the stored snapshot position %x: %w", key, err)
	}
	return importer.set(key, encodeFloor(floor))
}

// set adds one record, committing the piece so far first if this one would not
// fit beside it.
func (importer *positionImporter) set(key, encoded []byte) error {
	size := int64(len(key) + len(encoded))
	if importer.batchSize > 0 && importer.batchSize+size > importer.kv.MaxBatchBytes()/2 {
		if err := importer.batch.Commit(false); err != nil {
			return fmt.Errorf("committing a piece of the import: %w", err)
		}
		importer.batch = importer.kv.NewBatch()
		importer.batchSize = 0
	}
	if err := importer.batch.Set(key, encoded); err != nil {
		return fmt.Errorf("adding %x to the import: %w", key, err)
	}
	importer.batchSize += size
	return nil
}

// commitSynced commits the last piece and makes everything durable.
func (importer *positionImporter) commitSynced() error {
	return importer.batch.Commit(true)
}

// checkpointPosition writes source's "p" record from what the node holds in
// memory, and syncs the store when asked. Called where the log cannot carry the
// position: before an acknowledgement that moved it without a log entry, before
// the store sync that precedes a log truncation, and at a clean stop.
func (c *Cluster) checkpointPosition(source string, sync bool) error {
	_, err := c.checkpointPositions([]string{source}, sync)
	return err
}

// checkpointAllPositions checkpoints every source this node holds a position
// for, in one batch, and returns the positions it committed. Unsynced, it pays
// no checkpoint debt: the caller settles with the returned positions once its
// own sync has made them durable (pruneWAL).
func (c *Cluster) checkpointAllPositions(sync bool) (map[string]uint64, error) {
	c.appliedMu.Lock()
	sources := make([]string, 0, len(c.appliedFrom))
	for source := range c.appliedFrom {
		sources = append(sources, source)
	}
	c.appliedMu.Unlock()
	return c.checkpointPositions(sources, sync)
}

// checkpointPositions is the one writer of "p" records, and returns the
// position it committed for each source. positionMu is held from the read of
// the positions to the commit, so two checkpoints cannot commit out of the
// order they read in and leave an older position on top of a newer one.
//
// Only a synced commit settles a checkpoint debt. An unsynced one can still be
// lost to a power cut, and a debt cleared on its strength lets the next retry
// of a skipped frame be acknowledged with nothing durable behind it.
func (c *Cluster) checkpointPositions(sources []string, sync bool) (map[string]uint64, error) {
	if len(sources) == 0 {
		return nil, nil
	}
	c.positionMu.Lock()
	defer c.positionMu.Unlock()

	batch := c.s.kv.NewBatch()
	defer batch.Discard()
	passed := make(map[string]uint64)
	written := make(map[string]uint64, len(sources))
	for _, source := range sources {
		record, floorsPassed := c.positionRecordOf(source)
		written[source] = record.AppliedThrough
		encoded, err := bson.Marshal(record)
		if err != nil {
			return nil, fmt.Errorf("encoding the position record of source %q: %w", source, err)
		}
		if err := batch.Set(positionKey(source), encoded); err != nil {
			return nil, fmt.Errorf("adding the position record of source %q to a checkpoint: %w", source, err)
		}
		if !floorsPassed {
			continue
		}
		if err := c.deletePassedFloors(batch, source, record.AppliedThrough); err != nil {
			return nil, err
		}
		passed[source] = record.AppliedThrough
	}
	if c.beforeCheckpointCommit != nil {
		c.beforeCheckpointCommit()
	}
	if err := batch.Commit(sync); err != nil {
		return nil, fmt.Errorf("checkpointing %d replication positions: %w", len(sources), err)
	}
	for source, through := range passed {
		c.dropPassedFloors(source, through)
	}
	if sync {
		c.settleOwedCheckpoints(written)
	}
	return written, nil
}

// raisePositionOwingCheckpoint moves source's position past an entry whose
// move no log entry of source's origin carries — one a snapshot position
// covers, which is skipped, or one that landed on this node as the bucket's
// owner, which applyAsOwner logs as this node's own — and records that the
// move is owed a synced checkpoint. The debt is paid by a synced checkpoint
// that commits a position at or past it, so a frame whose checkpoint failed
// and was refused cannot have its retry acknowledged on the strength of a
// memory that was never made durable.
func (c *Cluster) raisePositionOwingCheckpoint(source string, seq uint64) {
	c.markApplied(source, seq)
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	c.checkpointOwed[source] = max(c.checkpointOwed[source], seq)
}

// checkpointIsOwed reports whether a position moved for source has not yet been
// checkpointed.
func (c *Cluster) checkpointIsOwed(source string) bool {
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	return c.checkpointOwed[source] != 0
}

// settleOwedCheckpoints clears each source's debt that a durable checkpoint at
// the given positions covers. It is called only once the checkpoint is synced.
func (c *Cluster) settleOwedCheckpoints(committed map[string]uint64) {
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	for source, through := range committed {
		if owed, isOwed := c.checkpointOwed[source]; isOwed && owed <= through {
			delete(c.checkpointOwed, source)
		}
	}
}

// positionRecordOf is source's position as it is in memory, and whether every
// snapshot position recorded for that source is at or below it. A snapshot
// position at or below the stream position covers nothing shouldApply does not
// already skip, so it is dead weight.
func (c *Cluster) positionRecordOf(source string) (positionRecord, bool) {
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	recorded := c.recordedStream[source]
	void := c.orphanVoids[source]
	record := positionRecord{AppliedThrough: c.appliedFrom[source], RebasedAt: c.rebasedAt[source],
		Incarnation: recorded.incarnation, Log: recorded.log, Clamped: recorded.clamped,
		VoidedAbove: void.above, VoidedThrough: void.through}
	ceiling := c.seedCeiling[source]
	return record, ceiling > 0 && ceiling <= record.AppliedThrough
}

// deletePassedFloors adds to batch the deletion of every "f" record of source
// whose position is at or below through. A record above it (one written since
// the positions were read) is left alone, whatever the ceiling said.
func (c *Cluster) deletePassedFloors(batch storage.KVBatch, source string, through uint64) error {
	snapshot := c.s.kv.NewSnapshot()
	defer snapshot.Discard()
	prefix := sourceKeyPrefix(seededRecordKind, source)
	iterator := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: keyPrefixEnd(prefix)})
	defer iterator.Close()
	for iterator.Seek(nil); iterator.Valid(); iterator.Next() {
		stored, err := iterator.Value()
		if err != nil {
			return fmt.Errorf("reading a snapshot position of source %q: %w", source, err)
		}
		floor, err := decodeFloor(stored)
		if err != nil {
			return fmt.Errorf("reading a snapshot position of source %q: %w", source, err)
		}
		if floor > through {
			continue
		}
		if err := batch.Delete(iterator.Key()); err != nil {
			return fmt.Errorf("deleting a passed snapshot position of source %q: %w", source, err)
		}
	}
	if err := iterator.Err(); err != nil {
		return fmt.Errorf("walking the snapshot positions of source %q: %w", source, err)
	}
	return nil
}

// dropPassedFloors forgets, in memory, source's snapshot positions at or below
// through, after the checkpoint that deleted them is committed.
func (c *Cluster) dropPassedFloors(source string, through uint64) {
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	floors := c.seededThrough[source]
	var highest uint64
	for bucket, perDatabase := range floors {
		for database, floor := range perDatabase {
			if floor <= through {
				floors.drop(database, bucket)
				continue
			}
			highest = max(highest, floor)
		}
	}
	if len(floors) == 0 {
		delete(c.seededThrough, source)
	}
	c.seedCeiling[source] = highest
}

// recordSeeded remembers that a snapshot of database/bucket pulled from
// `from` was taken at seq of that source's stream, as one key of the store:
// from here on every entry of that bucket from that source at or below seq is
// contents this node holds, not work to apply (handleReplPush), and that must
// survive a restart.
//
// The write is synced, one sync per pull, and before the copy materializes. A
// flush of a rotated memtable makes the copy's writes durable on its own,
// without any sync, so a position that was not yet durable could be lost while
// the copy it describes stays, and the bucket's history would replay over the
// copy. The sync covers every write committed before it, including
// writes in a memtable rotated out and not yet flushed: that is the patched
// Badger in third_party/badger. Upstream v4.9.6 synced only the
// active memtable.
//
// positionMu is held so a checkpoint that has read the store cannot commit a
// deletion of this key after it is written: a checkpoint decides which
// snapshot positions to delete from a read, and this write must not land in
// between.
func (c *Cluster) recordSeeded(from, database, bucket string, seq uint64) error {
	return c.recordSeededWithin(from, database, bucket, seq, nil)
}

// recordSeededWithin is recordSeeded for a pull that holds a snapshotCap: seq
// is lowered to what the rebases that ran since the pull read the source's
// incarnation say the source's log keeps, and a position lowered to zero
// covers nothing and is not recorded. positionMu is held, and a rebase holds
// it too, so none can lower the cap between the read below and the write.
func (c *Cluster) recordSeededWithin(from, database, bucket string, seq uint64, held *snapshotCap) error {
	c.positionMu.Lock()
	defer c.positionMu.Unlock()
	lowered := false
	if held != nil {
		c.appliedMu.Lock()
		capped := min(seq, held.validThrough)
		c.appliedMu.Unlock()
		if capped < seq {
			c.logger.Info("recording a pulled snapshot position lower than the snapshot was taken at: the source's incarnation changed "+
				"during the pull, and its new log keeps only the entries up to the lower one",
				"database", database, "bucket", bucket, "source", from, "taken_at", seq, "recorded", capped)
			lowered = true
		}
		seq = capped
	}
	if seq == 0 && !lowered {
		return nil
	}
	batch := c.s.kv.NewBatch()
	defer batch.Discard()
	if seq != 0 {
		if err := batch.Set(seededKey(from, database, bucket), encodeFloor(seq)); err != nil {
			return fmt.Errorf("persisting the snapshot position of %s/%s from %s: %w", database, bucket, from, err)
		}
	}
	// The copy's lineage position is the sequence the snapshot was taken at,
	// which the source's new log reuses; the source is clamped in the batch
	// that records the lowered position, as a rebase that lowers one does
	// (BUG-0621).
	if lowered {
		record, _ := c.positionRecordOf(from)
		record.Clamped = true
		encoded, err := bson.Marshal(record)
		if err != nil {
			return fmt.Errorf("encoding the clamped position record of source %q: %w", from, err)
		}
		if err := batch.Set(positionKey(from), encoded); err != nil {
			return fmt.Errorf("persisting the clamp of source %q with the snapshot position of %s/%s: %w", from, database, bucket, err)
		}
	}
	if err := batch.Commit(true); err != nil {
		return fmt.Errorf("persisting the snapshot position of %s/%s from %s: %w", database, bucket, from, err)
	}
	c.appliedMu.Lock()
	if seq != 0 {
		c.raiseSeededLocked(from, database, bucket, seq)
	}
	if lowered {
		recorded := c.recordedStream[from]
		recorded.clamped = true
		c.recordedStream[from] = recorded
	}
	c.appliedMu.Unlock()
	return nil
}

// forgetSeeded drops the snapshot position of database/bucket from `from`,
// for a pull that did not land: keeping it would make this node skip the
// bucket's history as already held when it holds nothing. A legacy
// bare-name position is not this pull's and is left alone.
func (c *Cluster) forgetSeeded(from, database, bucket string) error {
	c.positionMu.Lock()
	defer c.positionMu.Unlock()
	c.appliedMu.Lock()
	if floors, ok := c.seededThrough[from]; ok {
		floors.drop(database, bucket)
		if len(floors) == 0 {
			delete(c.seededThrough, from)
		}
	}
	c.appliedMu.Unlock()
	batch := c.s.kv.NewBatch()
	defer batch.Discard()
	if err := batch.Delete(seededKey(from, database, bucket)); err != nil {
		return fmt.Errorf("persisting the dropped snapshot position of %s/%s from %s: %w", database, bucket, from, err)
	}
	if err := batch.Commit(true); err != nil {
		return fmt.Errorf("persisting the dropped snapshot position of %s/%s from %s: %w", database, bucket, from, err)
	}
	return nil
}

// forgetSeededEverywhere drops the snapshot positions of database/bucket from
// every source: a copy this node replaced (G-6 (d)) leaves nothing of itself
// behind, or the new copy's first entries would be passed over as covered.
func (c *Cluster) forgetSeededEverywhere(database, bucket string) error {
	c.appliedMu.Lock()
	var sources []string
	for source, floors := range c.seededThrough {
		if _, held := floors[bucket][database]; held {
			sources = append(sources, source)
		}
	}
	c.appliedMu.Unlock()
	for _, source := range sources {
		if err := c.forgetSeeded(source, database, bucket); err != nil {
			return err
		}
	}
	return nil
}
