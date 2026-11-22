package server

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Database generations (the replication-database-ddl-design record §3,
// option 1). A database name is a sequence of incarnations, each named by a
// generation: a non-zero 63-bit value, minted where the incarnation is created
// and carried in every payload that concerns it. A node keeps, per database
// name, the generations live on it (its marker, L(D)) and the generations it
// knows to be dropped (tombstones, T(D)); every bucket records the generation
// it was created in (its catalog entry's db_gen). A drop names the generations
// it removes, so it removes the same buckets wherever and whenever it lands.
//
// A generation is not the node-local log position a marker's CreatedSeq holds
// (Server.generationSeq): that one is this node's and feeds the replay gate of
// a drop logged before this build; a generation is the cluster's.

// tombstoneRecordKind is the record kind of a dropped generation under
// storage.NSReplication, beside the stream's 'p', 'f' and 'l':
//
//	'g' ‖ uvarint(len(database)) ‖ database ‖ generation (8 bytes, big-endian) → tombstoneRecord
//
// Keyed by (database, generation), so equal values under two names never
// interact, and T(D) is one prefix scan. Nothing reads these records at start.
const tombstoneRecordKind byte = 'g'

// tombstoneRecord is the value of a dropped generation's record.
type tombstoneRecord struct {
	// DroppedSeq is this node's apply sequence of the drop that taught it the
	// generation, or 0 for a drop applied with none (a node with no log).
	DroppedSeq int64 `bson:"dropped_seq"`
}

// legacyGenerationHook, when set and returning true, supplies the generation
// legacyGeneration derives for a name. A test forces two names to one value
// with it, to show that tombstones are keyed by name as well as generation.
// Nil in production.
var legacyGenerationHook atomic.Pointer[func(database string) (int64, bool)]

// legacyGeneration is the generation of a database that existed before this
// build: derived from its name, so every node computes the same value with no
// coordination. An absent generation on a marker, and an absent db_gen on a
// catalog entry, read as it (rev 2, review F-2).
func legacyGeneration(database string) int64 {
	if hook := legacyGenerationHook.Load(); hook != nil {
		if generation, ok := (*hook)(database); ok {
			return generation
		}
	}
	derived := int64(xxhash.Sum64String("hoardDB legacy incarnation\x00"+database) & math.MaxInt64)
	if derived == 0 {
		return 1
	}
	return derived
}

// mintGeneration draws a new generation: 63 random bits, never zero and never
// one of taken — the generations live or dropped on the minting node.
func mintGeneration(taken ...[]int64) (int64, error) {
	var drawn [8]byte
	for {
		if _, err := rand.Read(drawn[:]); err != nil {
			return 0, fmt.Errorf("drawing a database generation: %w", err)
		}
		generation := int64(binary.BigEndian.Uint64(drawn[:]) & math.MaxInt64)
		if generation == 0 || slices.ContainsFunc(taken, func(set []int64) bool { return slices.Contains(set, generation) }) {
			continue
		}
		return generation, nil
	}
}

// tombstonePrefix is the key prefix of every dropped generation of database.
func tombstonePrefix(database string) []byte {
	return storage.NodeKey(storage.NSReplication,
		[]byte{tombstoneRecordKind}, binary.AppendUvarint(nil, uint64(len(database))), []byte(database))
}

// tombstoneKey is the key of one dropped generation of database.
func tombstoneKey(database string, generation int64) []byte {
	return binary.BigEndian.AppendUint64(tombstonePrefix(database), uint64(generation))
}

// droppedGenerations is T(database): every generation of the name this node
// knows to be dropped, in key order. One prefix scan.
func (s *Server) droppedGenerations(database string) ([]int64, error) {
	prefix := tombstonePrefix(database)
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: keyPrefixEnd(prefix)})
	defer it.Close()
	var dropped []int64
	for it.Seek(nil); it.Valid(); it.Next() {
		generation := it.Key()[len(prefix):]
		if len(generation) != 8 {
			return nil, fmt.Errorf("the dropped-generation record %x of database %q has a %d-byte generation; the node store is damaged — restore it from a backup",
				it.Key(), database, len(generation))
		}
		dropped = append(dropped, int64(binary.BigEndian.Uint64(generation)))
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("walking the dropped generations of database %q: %w", database, err)
	}
	return dropped, nil
}

// markerGenerations reads database's marker: its live generations, and
// whether it has a marker at all. A marker written before generations existed
// holds the legacy generation.
func markerGenerations(snap storage.KVSnapshot, database string) ([]int64, databaseMarker, bool, error) {
	encoded, err := snap.Get(catalogKey(database, ""))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return nil, databaseMarker{}, false, nil
	}
	if err != nil {
		return nil, databaseMarker{}, false, fmt.Errorf("reading the catalog entry of database %q: %w", database, err)
	}
	var marker databaseMarker
	if err := bson.Unmarshal(encoded, &marker); err != nil {
		return nil, databaseMarker{}, false, fmt.Errorf("decoding the catalog entry of database %q: %w", database, err)
	}
	if len(marker.Generations) == 0 {
		return []int64{legacyGeneration(database)}, marker, true, nil
	}
	return slices.Clone(marker.Generations), marker, true, nil
}

// liveGenerations is L(database) on this node, in the order the generations
// were added: its marker's, or — for a database known only from buckets it
// holds, which has no marker — the generations of those buckets. A record no
// create has committed (id 0) holds nothing: its generation is only the one a
// create or pull is claiming it for.
func (s *Server) liveGenerations(database string) ([]int64, error) {
	snap := s.kv.NewSnapshot()
	live, _, present, err := markerGenerations(snap, database)
	snap.Discard()
	if err != nil || present {
		return live, err
	}
	db := s.serverDataAccessor().getDB(database)
	if db == nil {
		return nil, nil
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	db.table.each(func(_ string, held heldRecord) {
		if generation := held.rec.generation.Load(); held.rec.id.Load() != 0 && !slices.Contains(live, generation) {
			live = append(live, generation)
		}
	})
	return live, nil
}

// entryGeneration is the generation a catalog entry was created in.
func entryGeneration(entry catalogEntry) int64 {
	if entry.DatabaseGeneration == 0 {
		return legacyGeneration(entry.database)
	}
	return entry.DatabaseGeneration
}

// generationStamp is what the node a client reached writes into the database
// DDL and bucket creates it logs, before the append, overwriting anything the
// client sent under these names (rules G-1, G-3, G-5 and G-9, with SA-DG-14:
// the coordinator, not the bucket's owner, stamps a create's generation).
type generationStamp struct {
	// refused is set when nothing is to be logged — a create of a database
	// live here, or a drop of one this node does not know — and message is
	// the answer, worded as the handler words it.
	refused bool
	message string
	// cause is the sentinel under the refusal, for the code the client reads.
	cause   error
	payload []byte
}

// stampGenerations returns payload with the generations op logs, or the
// refusal that logs nothing. Only a create or drop of a database and a create
// of a bucket carry generations; every other payload is returned as it is. It
// runs on the coordinator of a multi-node cluster (Cluster.handleClientFrame),
// so s.cluster is set.
func (s *Server) stampGenerations(ctx context.Context, op transport.Opcode, payload []byte) (generationStamp, error) {
	switch op {
	case transport.OpCreateDB:
		return s.stampDatabaseCreate(ctx, payload)
	case transport.OpDeleteDB:
		return s.stampDatabaseDrop(payload)
	case transport.OpCreateBucket:
		return s.stampBucketCreate(ctx, payload)
	}
	return generationStamp{payload: payload}, nil
}

// stampDatabaseCreate is G-1: a database live here, or on another member, is
// answered "exists" and nothing is logged; otherwise a new generation is
// minted and the create carries it with every generation of the name this
// node or another member knows to be dropped.
func (s *Server) stampDatabaseCreate(ctx context.Context, payload []byte) (generationStamp, error) {
	database := decodeFrameMeta(payload).Name
	live, dropped, err := s.generationsOfName(ctx, database, mintWhenSilent)
	if err != nil {
		return generationStamp{}, err
	}
	if len(live) > 0 {
		return generationStamp{refused: true, message: databaseExistsMessage(database), cause: ErrDatabaseAlreadyExists}, nil
	}
	generation, err := mintGeneration(dropped)
	if err != nil {
		return generationStamp{}, err
	}
	fields := bson.D{{Key: "generation", Value: generation}, {Key: "dropped_generations", Value: nonNil(dropped)}}
	stamped, err := stampPayload(payload, fields)
	return generationStamp{payload: stamped}, err
}

// stampDatabaseDrop is G-3: the drop names the generations live here; a
// database with none is answered "no database" and nothing is logged.
func (s *Server) stampDatabaseDrop(payload []byte) (generationStamp, error) {
	database := decodeFrameMeta(payload).Name
	live, err := s.liveGenerations(database)
	if err != nil {
		return generationStamp{}, err
	}
	if len(live) == 0 {
		return generationStamp{refused: true, message: noDatabaseErr(database), cause: ErrDatabaseNotFound}, nil
	}
	stamped, err := stampPayload(payload, bson.D{{Key: "generations", Value: live}})
	return generationStamp{payload: stamped}, err
}

// stampBucketCreate is G-5 as SA-DG-14 rules it: the node the client reached
// stamps the generation most recently added to the database here, or on the
// member it asked when the database is not live here, minting one only when
// the database is live nowhere, and carries every generation of the name it
// knows to be dropped, so the owner applies those first.
func (s *Server) stampBucketCreate(ctx context.Context, payload []byte) (generationStamp, error) {
	database := decodeFrameMeta(payload).Database
	live, dropped, err := s.generationsOfName(ctx, database, refuseWhenSilent)
	if err != nil {
		return generationStamp{}, err
	}
	var generation int64
	if len(live) > 0 {
		generation = live[len(live)-1]
	} else if generation, err = mintGeneration(dropped); err != nil {
		return generationStamp{}, err
	}
	fields := bson.D{{Key: "db_gen", Value: generation}, {Key: "dropped_generations", Value: nonNil(dropped)}}
	stamped, err := stampPayload(payload, fields)
	return generationStamp{payload: stamped}, err
}

// generationsOfName returns the live generations of database a create is
// stamped against, and the generations of the name known to be dropped. When
// the database is live here, both are this node's. When it is not, this node
// may only not have learned it: a joiner learns a database with its first
// copied bucket. The other members are asked, and the live generations are the
// first answering member's that holds any, so the create joins the incarnation
// the members hold instead of starting one beside it. An
// incarnation this node or any member knows dropped is not live, whoever still
// holds it.
//
// When no member that answered holds the database live and some did not
// answer, silent decides. A bucket create (refuseWhenSilent) is refused with
// retry, errMembersUnheard, naming the members: a member that did not answer
// may hold the bucket, and a create in an incarnation minted beside its own
// leaves the new node serving an empty copy (verifier D-2). A database create
// (mintWhenSilent) mints, as it did before: it drops nothing by itself, and a
// database created while a member is down is delivered to it when it returns
// (the coordinator's ruling).
func (s *Server) generationsOfName(ctx context.Context, database string, silent silentMembers) (live, dropped []int64, err error) {
	if live, err = s.liveGenerations(database); err != nil {
		return nil, nil, err
	}
	if dropped, err = s.droppedGenerations(database); err != nil {
		return nil, nil, err
	}
	if len(live) > 0 {
		return live, dropped, nil
	}
	members := s.cluster.surveyCatalog(ctx, ReplCatalogAsk{Database: database})
	dropped = mergeGenerations(dropped, members.dropped)
	// A member that missed a drop still holds the dropped incarnation live; a
	// create stamped with it would be refused as dropped, and a database create
	// answered "exists" for a database this node has just dropped.
	for _, answer := range members.live {
		if live = slices.DeleteFunc(slices.Clone(answer), func(generation int64) bool { return slices.Contains(dropped, generation) }); len(live) > 0 {
			break
		}
	}
	if len(live) == 0 && len(members.unheard) > 0 && silent == refuseWhenSilent {
		return nil, nil, &sentencedError{cause: errMembersUnheard, message: fmt.Sprintf(
			"this node has not learned database %q, and %s did not answer whether they hold it (%v), so it cannot tell a new database "+
				"from one it has not copied yet; nothing was created. Retry when they are reachable ('nodes' shows the members), "+
				"or send the request to a member that already holds the database",
			database, strings.Join(members.unheard, ", "), errors.Join(members.unanswered...))}
	}
	return live, dropped, nil
}

// silentMembers says what generationsOfName does when it finds no live
// incarnation and some member did not answer.
type silentMembers int

const (
	// mintWhenSilent reports none live, so the caller mints.
	mintWhenSilent silentMembers = iota
	// refuseWhenSilent refuses with errMembersUnheard.
	refuseWhenSilent
)

// errMembersUnheard is a bucket create refused because the node coordinating
// it has not learned the database and could not ask every member whether one
// holds it. It is answered UNAVAILABLE: the same create succeeds once they answer.
var errMembersUnheard = errors.New("the coordinating node could not hear every member about a database it has not learned")

// matchesServerField reports whether a client's key would reach the struct
// field a server-owned key names. The BSON struct decoder falls back to a
// lower-cased name while EqualFold uses simple folding, and they disagree on
// a few code points (U+0130 lower-cases to "i" but does not fold to it), so
// both rules must be checked or such a key survives the strip (BUG-0240).
func matchesServerField(serverKey, clientKey string) bool {
	return strings.EqualFold(serverKey, clientKey) || strings.ToLower(serverKey) == strings.ToLower(clientKey) //nolint:staticcheck // SA6005: lower-casing is exactly the decoder's rule, which EqualFold is not
}

// stampPayload returns payload with the server's fields written over
// anything the client sent under their names (G-9): every element whose key
// is one of theirs in any letter case is removed — the module's struct
// decoder matches an unknown key to a field by its lower-cased name, so
// "Dropped_Generations" reaches DroppedGenerations as surely as the exact name
// does — and fields are appended. Every other element is kept byte for byte
// in its order: the payload is not re-encoded from a request struct, because
// the struct does not declare every key a client may send (a bucket create's
// write_concern, which the router reads from the payload, is one).
func stampPayload(payload []byte, fields bson.D) ([]byte, error) {
	var document bson.D
	if err := bson.Unmarshal(payload, &document); err != nil {
		return nil, fmt.Errorf("decoding a request to stamp its database generation: %w", err)
	}
	stamped := make(bson.D, 0, len(document)+len(fields))
	for _, element := range document {
		if !slices.ContainsFunc(fields, func(field bson.E) bool { return matchesServerField(field.Key, element.Key) }) {
			stamped = append(stamped, element)
		}
	}
	stamped = append(stamped, fields...)
	encoded, err := bson.Marshal(stamped)
	if err != nil {
		return nil, fmt.Errorf("encoding a request stamped with its database generation: %w", err)
	}
	return encoded, nil
}

// nonNil returns generations, or an empty list for none: the stamp writes the
// field even when it is empty, so no client value can stand in for it.
func nonNil(generations []int64) []int64 {
	if generations == nil {
		return []int64{}
	}
	return generations
}

// clusterApplyCtxKey marks a dispatch whose payload came through the cluster's
// write path — stamped by the node a client reached, or logged and shipped by
// another node — so its generations are the cluster's word, not a client's.
type clusterApplyCtxKey struct{}

// withClusterApply marks a dispatch as an apply of a stamped or shipped entry.
func withClusterApply(ctx context.Context) context.Context {
	return context.WithValue(ctx, clusterApplyCtxKey{}, true)
}

// generationsTrusted reports whether the generations in a dispatch's payload
// are to be applied as they are: the payload was stamped before it was logged,
// or is being replayed from this node's own log. Anything else is a client's
// request to a node that coordinates for itself, whose values are ignored and
// stamped here (G-9).
func generationsTrusted(ctx context.Context) bool {
	stamped, _ := ctx.Value(clusterApplyCtxKey{}).(bool)
	return stamped || isReplay(ctx)
}

// mergeGenerations is the union of two sets of generations, first's order
// kept and second's new members after it.
func mergeGenerations(first, second []int64) []int64 {
	merged := slices.Clone(first)
	for _, generation := range second {
		if !slices.Contains(merged, generation) {
			merged = append(merged, generation)
		}
	}
	return merged
}

// generationState is a node's bookkeeping of database incarnations in memory.
// What it guards is in the store; nothing here survives a restart or needs to.
type generationState struct {
	// learnMu is held from reading which of a set of generations this node
	// lacks to writing them, so each generation is logged again here at most
	// once (G-12). It is taken before any database's lock.
	learnMu sync.Mutex

	// mu guards locks and signal.
	mu sync.Mutex
	// locks holds one mutex per database whose incarnations are changing now,
	// reference-counted like the fences, so it is bounded by changes in
	// flight and not by names ever used.
	locks map[string]*databaseLock
	// signal is closed, and replaced, whenever this node learns a dropped
	// generation.
	signal chan struct{}
}

// databaseLock is one database's mutex in generationState.locks.
type databaseLock struct {
	mu   sync.Mutex
	refs int
}

// lockDatabase serialises the changes to one database's incarnations on this
// node: an add (G-2) against a drop (G-4), so the marker the store holds and
// the database the registry holds change together. It returns the unlock.
// Lock order: a database lock is taken before data.mu and never inside a
// fence's callers' bucket locks.
func (s *Server) lockDatabase(database string) func() {
	g := &s.generations
	g.mu.Lock()
	if g.locks == nil {
		g.locks = make(map[string]*databaseLock)
	}
	lock, ok := g.locks[database]
	if !ok {
		lock = &databaseLock{}
		g.locks[database] = lock
	}
	lock.refs++
	g.mu.Unlock()
	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		g.mu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(g.locks, database)
		}
		g.mu.Unlock()
	}
}

// tombstonesChanged returns a channel closed the next time this node learns a
// dropped generation.
func (s *Server) tombstonesChanged() <-chan struct{} {
	g := &s.generations
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.signal == nil {
		g.signal = make(chan struct{})
	}
	return g.signal
}

// noteTombstonesChanged wakes every tombstonesChanged waiter.
func (s *Server) noteTombstonesChanged() {
	g := &s.generations
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.signal != nil {
		close(g.signal)
	}
	g.signal = make(chan struct{})
}

// generationDropped reports whether this node knows generation of database to
// be dropped. One read.
func (s *Server) generationDropped(database string, generation int64) (bool, error) {
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	_, err := snap.Get(tombstoneKey(database, generation))
	if errors.Is(err, storage.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("reading whether generation %d of database %q was dropped: %w", generation, database, err)
	}
	return true, nil
}

// generationDropCommitHook, when set, runs before a drop of generations
// commits; an error fails the commit there, as a store failure would. Nil in
// production.
var generationDropCommitHook atomic.Pointer[func(database string) error]

// generationAddCommitHook, when set, runs before a create's catalog entry
// commits; an error fails the commit there, as a store failure would. Nil in
// production.
var generationAddCommitHook atomic.Pointer[func(database string) error]

// generationSeams switches rules off for a test that must isolate another
// rule: each rule below is a second path to the same outcome, and a test of
// the first could not fail while the second is on.
type generationSeams struct {
	// noRelog applies a learned dropped generation without logging it again
	// (G-12 off).
	noRelog bool
	// noCarried ignores the dropped generations a create carries (G-2's and
	// SA-DG-14's first step off).
	noCarried bool
	// noPullExchange sends and merges no dropped generations on a pull
	// (G-11 (b) off).
	noPullExchange bool
}

// generationSeamsHook, when set, switches off the rules its generationSeams
// names. Nil in production.
var generationSeamsHook atomic.Pointer[generationSeams]

func seamsOff() generationSeams {
	if seams := generationSeamsHook.Load(); seams != nil {
		return *seams
	}
	return generationSeams{}
}

// peerEntryCtxKey marks the apply of an entry another node logged — shipped
// to this node, or replayed from this node's log where it was recorded with
// that node as its origin. A drop such an entry names teaches this node
// generations it may not hold, which it logs again (G-12).
type peerEntryCtxKey struct{}

// withPeerEntry marks a dispatch as the apply of another node's entry, or,
// with peer false, clears the mark for a nested apply of this node's own.
func withPeerEntry(ctx context.Context, peer bool) context.Context {
	return context.WithValue(ctx, peerEntryCtxKey{}, peer)
}

// fromPeer reports whether a dispatch applies another node's entry.
func fromPeer(ctx context.Context) bool {
	peer, _ := ctx.Value(peerEntryCtxKey{}).(bool)
	return peer
}

// unknownGenerations returns the members of generations this node does not
// hold dropped, without repeats.
func (s *Server) unknownGenerations(database string, generations []int64) ([]int64, error) {
	dropped, err := s.droppedGenerations(database)
	if err != nil {
		return nil, err
	}
	var unknown []int64
	for _, generation := range generations {
		if !slices.Contains(dropped, generation) && !slices.Contains(unknown, generation) {
			unknown = append(unknown, generation)
		}
	}
	return unknown, nil
}

// learnTombstones is G-11 (c) and G-12: of generations, the ones this node
// does not yet hold dropped are logged as this node's own drop of the
// database, naming exactly them, and applied (G-4); the drop is shipped to
// every other member like any database DDL, so a drop that reached one member
// reaches them all. The append comes before the tombstones are written: a
// restart between the two replays the logged drop, while one before the
// append leaves nothing written, and the sender re-sends what taught it.
//
// Start-up replay is the exception (BUG-0610, owner ruling 2026-10-18): it
// runs before the start hold, and a sequence taken then can be one a peer
// holds another entry at. Replay applies the drop here and the hold appends
// it once open (startHold.deferRelog). A stop in between loses the re-log:
// the next replay finds the generations dropped and logs nothing. The peers
// then learn the drop as a peer that never received a re-log does, from this
// node's answer to an entry of the database it refuses (G-11 (a),
// Cluster.reportTombstones).
func (s *Server) learnTombstones(ctx context.Context, database string, generations []int64) error {
	if len(generations) == 0 {
		return nil
	}
	s.generations.learnMu.Lock()
	defer s.generations.learnMu.Unlock()
	unknown, err := s.unknownGenerations(database, generations)
	if err != nil || len(unknown) == 0 {
		return err
	}
	own := withPeerEntry(ctx, false)
	relog := s.cluster != nil && s.wal != nil && !seamsOff().noRelog
	if relog && !isReplay(ctx) {
		return s.cluster.relogDrop(own, database, unknown)
	}
	resp, err := s.applyGenerationDrop(own, database, unknown)
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("applying the dropped generations %v of database %q: %s", unknown, database, resp.Message)
	}
	if relog {
		s.cluster.startHold.deferRelog(database, unknown)
	}
	return nil
}

// afterDatabaseDropCommittedHook, when set, runs once a database drop's batch
// has committed and before its buckets are detached. Tests cut the drop there,
// as a crash would. Nil in production.
var afterDatabaseDropCommittedHook atomic.Pointer[func(database string)]

// applyGenerationDrop is G-4: every generation in dropped is recorded dropped,
// the database's live generations lose them, and the buckets of the database
// created in one of them are dropped — on this node, whatever else it holds,
// and also when it holds nothing of the database, where only the tombstones are
// written. Buckets of other generations are not touched. The database goes
// when no live generation is left.
func (s *Server) applyGenerationDrop(ctx context.Context, database string, dropped []int64) (*DeleteDatabaseResponse, error) {
	unlock := s.lockDatabase(database)
	defer unlock()
	known := s.serverDataAccessor().getDB(database) != nil
	// The drop record is in the store from the commit on, and the expander
	// turns it into bucket reclaim records the same lap may step: the
	// reclaimer leaves the database's records until the detach below has
	// unlinked its buckets (BUG-0527).
	window := s.unlinkWindow.holdDatabase(database)
	defer window.release()
	outcome, err := s.commitGenerationDrop(database, dropped, applySeqFrom(ctx), known)
	if err != nil {
		return &DeleteDatabaseResponse{
			Success: false,
			Message: failureMessage(fmt.Sprintf("database %q was not dropped: recording the drop failed", database), err),
		}, nil
	}
	window.recordCommitted()
	if hook := afterDatabaseDropCommittedHook.Load(); hook != nil {
		(*hook)(database)
	}
	if outcome.learned {
		s.noteTombstonesChanged()
	}
	if !known && !outcome.markerPresent {
		// Nothing of the database is linked on this node.
		window.namesGone()
		return &DeleteDatabaseResponse{Success: true, Message: fmt.Sprintf(
			"database %q is not on this node; the dropped incarnation is recorded, so a create of it that arrives later is ignored", database)}, nil
	}
	removed := s.detachDroppedGenerations(database, dropped, len(outcome.remaining) == 0)
	window.namesGone()
	s.wakeReclaimer()
	if len(outcome.remaining) > 0 {
		return &DeleteDatabaseResponse{Success: true, Message: fmt.Sprintf(
			"database %q: the dropped incarnation's %d bucket(s) are removed; the database stays, because it was also created through another node "+
				"that this drop had not heard from. Run 'drop database %s' again to drop the rest. Space is reclaimed in the background; "+
				"watch hoarddb_reclaim_pending_records to follow it.", database, removed, database)}, nil
	}
	return &DeleteDatabaseResponse{
		Success: true,
		Message: fmt.Sprintf("database %q dropped (%d bucket(s)). Its space is reclaimed in the background; watch hoarddb_reclaim_pending_records to follow it.", database, removed),
	}, nil
}

// generationDropOutcome is what commitGenerationDrop changed.
type generationDropOutcome struct {
	// remaining is the database's live generations after the drop.
	remaining []int64
	// markerPresent is whether the database had a marker.
	markerPresent bool
	// learned is whether a generation was recorded dropped that was not.
	learned bool
}

// commitGenerationDrop is a drop's one synced batch, under the allocator's
// mutex as every marker change and every bucket create's commit is, so a
// create in a dropped generation either committed before it (and is dropped
// with the rest) or finds the tombstone (G-6 (a)): the tombstones; the
// database's drop record naming the generations, merged into a pending one,
// when the database is known here; and its marker without them, deleted once
// none is left. The awaiting and needs-a-copy records of the database's
// buckets of those generations are deleted in the same batch, found by one
// prefix scan of each kind, so a drop replayed at start that learns its
// generations deletes them before the start-up load reads them (spec §3.3); a
// drop already durable returns earlier and deletes nothing, and alterOfNoCopy
// writes no record of a dropped generation (BUG-0593). The pulls of the
// database's buckets of those generations are cancelled, and the records dropped from the awaiting map, under the same
// mutex.
func (s *Server) commitGenerationDrop(database string, dropped []int64, droppedSeq uint64, known bool) (generationDropOutcome, error) {
	s.bucketIDs.mu.Lock()
	defer s.bucketIDs.mu.Unlock()
	snap := s.kv.NewSnapshot()
	live, marker, present, err := markerGenerations(snap, database)
	snap.Discard()
	if err != nil {
		return generationDropOutcome{}, err
	}
	unknown, err := s.unknownGenerations(database, dropped)
	if err != nil {
		return generationDropOutcome{}, err
	}
	outcome := generationDropOutcome{markerPresent: present, learned: len(unknown) > 0}
	for _, generation := range live {
		if !slices.Contains(dropped, generation) {
			outcome.remaining = append(outcome.remaining, generation)
		}
	}
	if !outcome.learned && len(outcome.remaining) == len(live) {
		return outcome, nil
	}
	batch := s.kv.NewBatch()
	defer batch.Discard()
	if err := writeTombstones(batch, database, unknown, droppedSeq); err != nil {
		return generationDropOutcome{}, err
	}
	if known || present {
		if err := s.mergeDropRecord(batch, database, dropped, droppedSeq); err != nil {
			return generationDropOutcome{}, err
		}
	}
	if present {
		if err := s.writeRemainingMarker(batch, database, marker, outcome.remaining); err != nil {
			return generationDropOutcome{}, err
		}
	}
	scan := s.kv.NewSnapshot()
	defer scan.Discard()
	if err := deleteNodeRecords(scan, batch, storage.AwaitingRecordKind, databaseRecordScope(database), dropped); err != nil {
		return generationDropOutcome{}, fmt.Errorf("deleting the awaiting records of generations %v of database %q: %w", dropped, database, err)
	}
	if err := deleteNodeRecords(scan, batch, storage.NeedsCopyRecordKind, databaseRecordScope(database), dropped); err != nil {
		return generationDropOutcome{}, fmt.Errorf("deleting the needs-a-copy records of generations %v of database %q: %w", dropped, database, err)
	}
	if hook := generationDropCommitHook.Load(); hook != nil {
		if err := (*hook)(database); err != nil {
			return generationDropOutcome{}, err
		}
	}
	if err := batch.Commit(true); err != nil {
		return generationDropOutcome{}, fmt.Errorf("committing the drop of generations %v of database %q: %w", dropped, database, err)
	}
	if s.cluster != nil {
		s.cluster.dropDatabaseClearsPulls(database, dropped)
		s.cluster.forgetAwaitingDatabase(database, dropped)
		s.cluster.forgetPendingCopiesOf(database, dropped)
	}
	return outcome, nil
}

// writeTombstones adds to batch a record of each of generations dropped.
func writeTombstones(batch storage.KVBatch, database string, generations []int64, droppedSeq uint64) error {
	encoded, err := bson.Marshal(tombstoneRecord{DroppedSeq: int64(droppedSeq)})
	if err != nil {
		return fmt.Errorf("encoding a dropped generation of database %q: %w", database, err)
	}
	for _, generation := range generations {
		if err := batch.Set(tombstoneKey(database, generation), encoded); err != nil {
			return fmt.Errorf("recording generation %d of database %q dropped: %w", generation, database, err)
		}
	}
	return nil
}

// mergeDropRecord adds to batch the database's drop record naming dropped,
// merged into the record of a drop the expander has not finished: the
// expander then removes the buckets of every generation either drop named. A
// pending record a drop logged before generations existed keeps its max_id,
// which still decides the entries it covered.
func (s *Server) mergeDropRecord(batch storage.KVBatch, database string, dropped []int64, droppedSeq uint64) error {
	earlier, pending, err := s.pendingDatabaseDrop(database)
	if err != nil {
		return err
	}
	record := databaseDropRecord{Database: database, DroppedSeq: int64(droppedSeq), DroppedAt: time.Now().UTC(), Generations: dropped}
	if pending {
		record.DroppedAt = earlier.DroppedAt
		record.MaxID = earlier.MaxID
		record.Cursor = nil
		record.Generations = mergeGenerations(earlier.Generations, dropped)
	}
	encoded, err := bson.Marshal(record)
	if err != nil {
		return fmt.Errorf("encoding the drop record of database %q: %w", database, err)
	}
	if err := batch.Set(databaseDropKey(database), encoded); err != nil {
		return fmt.Errorf("writing the drop record of database %q: %w", database, err)
	}
	return nil
}

// writeRemainingMarker adds to batch the database's marker with remaining as
// its live generations, or its deletion when none remains.
func (s *Server) writeRemainingMarker(batch storage.KVBatch, database string, marker databaseMarker, remaining []int64) error {
	if len(remaining) == 0 {
		if err := batch.Delete(catalogKey(database, "")); err != nil {
			return fmt.Errorf("deleting the catalog entry of database %q: %w", database, err)
		}
		return nil
	}
	marker.Generations = remaining
	return s.putDatabaseMarker(batch, database, marker)
}

// afterDetachBeforeForgetHook, when set, runs in detachDroppedGenerations
// after the dropped buckets are closed and before what they owed is
// forgotten: the window in which a bucket of the same name can be created and
// owe a write. Nil in production.
var afterDetachBeforeForgetHook atomic.Pointer[func(database string)]

// detachDroppedGenerations removes from the registry the buckets of database
// created in one of dropped — and the database itself, when whole and no
// bucket of another generation is left — and closes them, so a writer that
// resolved one before the drop finds it closed. A bucket's generation is read
// here under db.mu, which is where a create or a pull publishes the
// generation it claims a record for (inMemoryDB.claimBucket): a create
// that committed before the drop is found by it even before it has attached.
//
// whole means no live generation is left in the database's marker, but a
// create or pull of a generation the drop does not name may be between its
// registry lookup and its catalog commit, which will put that generation back
// in the marker. Its bucket stays, and so does the database it is reached
// through; detaching them would leave the bucket catalogued and not served
// until a restart. A nil dropped is a drop that names no
// generation (deleteLegacyDatabase): it removes every bucket, as every drop
// did before generations, except the uncommitted record of a create, which
// that drop never saw and which is acknowledged only at its commit,
// and cancels every pull of the database, so an
// in-flight pull is refused at its commit. It returns how many buckets it
// removed.
func (s *Server) detachDroppedGenerations(database string, dropped []int64, whole bool) int {
	data := s.serverDataAccessor()
	data.mu.Lock()
	db := data.dbs[database]
	var gone []heldRecord
	var names []string
	// The ids are what the debts are forgotten by, read here under the locks:
	// by the forget below, a bucket of the same name may have been created and
	// owe a write of its own.
	var ids []uint64
	// unlinkErrs holds the records the index could not give up; they are logged
	// after the locks are released, since this function has no error to return.
	var unlinkErrs []error
	removed := false
	if db != nil {
		db.mu.Lock()
		db.table.each(func(name string, held heldRecord) {
			if dropped == nil && held.rec.isCreating() && held.rec.id.Load() == 0 {
				return
			}
			if dropped == nil || slices.Contains(dropped, held.rec.generation.Load()) {
				gone = append(gone, held)
				names = append(names, name)
				ids = append(ids, held.rec.id.Load())
			}
		})
		// Phase one of the drop for all of them, in this one hold. A record its
		// chain cannot reach stays linked, served and out of the lists below:
		// closing and freeing it would give its slot back while the name still
		// found it.
		keptGone := make([]heldRecord, 0, len(gone))
		keptNames := make([]string, 0, len(names))
		keptIDs := make([]uint64, 0, len(ids))
		for i, name := range names {
			if _, unlinkErr := db.table.unlink(name, gone[i]); unlinkErr != nil {
				unlinkErrs = append(unlinkErrs, fmt.Errorf("database %q: %w", database, unlinkErr))
				continue
			}
			keptGone = append(keptGone, gone[i])
			keptNames = append(keptNames, name)
			keptIDs = append(keptIDs, ids[i])
		}
		gone, names, ids = keptGone, keptNames, keptIDs
		if whole && db.table.count() == 0 {
			db.dropped = true
			delete(data.dbs, database)
			removed = true
		}
		db.mu.Unlock()
	}
	data.mu.Unlock()

	for _, unlinkErr := range unlinkErrs {
		s.logger.Error("a dropped database's bucket could not be unlinked from the registry; it stays served and its record stays allocated",
			"error", unlinkErr)
	}

	if len(gone) > 0 {
		unlinked := make([]*bucketRec, len(gone))
		for i, held := range gone {
			unlinked[i] = held.rec
		}
		s.noteBucketDetached(s.retireRecords(db, unlinked))
	}
	if hook := afterDetachBeforeForgetHook.Load(); hook != nil {
		(*hook)(database)
	}
	for _, id := range ids {
		s.owed.dropBucketID(id)
	}
	for _, name := range names {
		// A bucket of this name served again since the locks were released
		// (created in the database's other live generation) registered its own
		// factor and TTL declaration, and both registries are keyed by name:
		// forgetting would erase the new bucket's, not the dropped one's
		// (BUG-0455). peekBucket takes no bucket lock, so a create in flight
		// counts as present and registers for itself when it attaches. The
		// registries are keyed by an encoded (database, bucket) key, so they
		// cannot be filtered by database without scanning every entry; the
		// names this drop already walked are the way to O(its buckets).
		if _, served := s.peekBucket(database, name); served {
			continue
		}
		s.unregisterBucketFactor(database, name)
		s.ttl.forget(database, name)
	}
	if removed && s.serverDataAccessor().getDB(database) == nil {
		s.ttl.forgetDatabase(database)
	}
	return len(gone)
}

// relogDrop is G-12's append: this node logs, as its own, a drop of database
// naming exactly the generations it has just learned, applies it, and ships it
// to every other member as it ships any database DDL. A drop learned by
// start-up replay comes here once the start hold has opened
// (startHold.deferRelog), already applied; its apply here changes nothing.
//
// It is not asked memberFeaturePrecheck (BUG-0576). Refusing this copy would
// turn learning the dropped generations into a failure and block convergence:
// on the replicated-apply path the peer's DELETE_DB would be answered "was
// not dropped". The exemption is safe only while DELETE_DB's writeOpcodeFeatures
// row is everyBuildHasIt (TestDeleteDatabaseStaysEveryBuildHasItWhileRelogDropIsExempt).
func (c *Cluster) relogDrop(ctx context.Context, database string, generations []int64) error {
	payload, err := bson.Marshal(DeleteDatabaseRequest{Name: database, Generations: generations})
	if err != nil {
		return fmt.Errorf("encoding the drop of generations %v of database %q: %w", generations, database, err)
	}
	entry := ReplEntry{Op: int32(transport.OpDeleteDB), Payload: payload}
	resp, err := c.broadcastMutation(ctx, entry, database)
	if err != nil {
		return err
	}
	if transport.Opcode(resp.Opcode) == transport.OpError {
		return fmt.Errorf("applying this node's drop of generations %v of database %q was refused: %s", generations, database, resp.Payload)
	}
	var dropped DeleteDatabaseResponse
	if err := bson.Unmarshal(resp.Payload, &dropped); err != nil {
		return fmt.Errorf("decoding this node's drop of generations %v of database %q: %w", generations, database, err)
	}
	if !dropped.Success {
		return fmt.Errorf("this node's drop of generations %v of database %q did not apply: %s", generations, database, dropped.Message)
	}
	c.logger.Info("learned a dropped incarnation of a database and logged its drop, so every member hears of it",
		"database", database, "generations", generations)
	return nil
}

// commitGenerationAdd adds generation to database's live generations, under
// the allocator's mutex as every marker change is, unless this node knows it
// to be dropped: then it reports late and writes nothing (G-2). The check and
// the write are under the one mutex a drop's tombstones are written under.
func (s *Server) commitGenerationAdd(database string, generation int64, applySeq uint64) (late bool, err error) {
	s.bucketIDs.mu.Lock()
	defer s.bucketIDs.mu.Unlock()
	if late, err = s.generationDropped(database, generation); err != nil || late {
		return late, err
	}
	batch := s.kv.NewBatch()
	defer batch.Discard()
	if err := s.addGenerationToMarker(batch, database, generation, applySeq); err != nil {
		return false, err
	}
	if hook := generationAddCommitHook.Load(); hook != nil {
		if err := (*hook)(database); err != nil {
			return false, err
		}
	}
	if err := batch.Commit(false); err != nil {
		return false, fmt.Errorf("committing the catalog entry of database %q: %w", database, err)
	}
	return false, nil
}

// errGenerationDropped is a bucket create refused because the incarnation of
// its database it was stamped with is dropped on this node (G-6 (a)).
var errGenerationDropped = errors.New("the database incarnation the bucket was to be created in has been dropped")

// droppedWhileCreatingMessage is G-6 (a)'s answer to the client: nothing was
// stored, and the command that makes the create possible again.
func droppedWhileCreatingMessage(database, bucket string) string {
	bucket = displayBucket(bucket)
	return fmt.Sprintf("Database %q was dropped while bucket %q was being created; nothing was created. "+
		"Run 'create database %s', then create the bucket again.", database, bucket, database)
}

// decideGenerationOfCreate is G-6 before a bucket create takes its bucket's
// lock: the dropped generations the create carries are applied first
// (SA-DG-14); a create in a generation dropped here is refused (a); and a copy
// of the name held in another generation that the create carries as dropped,
// or that this node holds dropped, is dropped as a bucket drop drops it, so the
// create makes the name afresh (d) — never answered "already exists", which
// would merge the new incarnation's writes into the dropped one's copy (review
// F-1). A copy of a generation nobody has dropped is kept and the create
// refused: its coordinator had not learned the generation. The caller holds the bucket's fence exclusively. A
// non-nil response is the refusal.
func (s *Server) decideGenerationOfCreate(ctx context.Context, req CreateBucketRequest, generation int64) *CreateBucketResponse {
	if generationsTrusted(ctx) && !seamsOff().noCarried {
		if err := s.learnTombstones(ctx, req.Database, req.DroppedGenerations); err != nil {
			return &CreateBucketResponse{Success: false, Message: failureMessage(fmt.Sprintf("bucket %q was not created: applying the drops its create carries failed", req.Name), err)}
		}
	}
	dropped, err := s.generationDropped(req.Database, generation)
	if err != nil {
		return &CreateBucketResponse{Success: false, Message: failureMessage(fmt.Sprintf("bucket %q was not created", req.Name), err)}
	}
	if dropped {
		return &CreateBucketResponse{Success: false, Message: droppedWhileCreatingMessage(req.Database, req.Name)}
	}
	// A create a peer delivered of an incarnation this node recorded dropped is
	// a late one, from a stream that had not yet seen the drop: it is refused
	// whether or not a copy is held, and a drop leaves none behind to remind
	// the node (BUG-0466). The owner's own create is stamped with an
	// incarnation minted a moment ago, which no record can name, and 0 is a
	// create logged before incarnations existed.
	if fromPeer(ctx) && req.Incarnation != 0 {
		droppedHere, err := s.dropRecorded(req.Database, req.Name, req.Incarnation)
		if err != nil {
			return &CreateBucketResponse{Success: false, Message: failureMessage(fmt.Sprintf("bucket %q was not created: reading whether incarnation %d of it was dropped here failed", req.Name, req.Incarnation), err)}
		}
		if droppedHere {
			s.logger.Warn("refused a create of a bucket incarnation this node recorded dropped",
				"database", req.Database, "bucket", req.Name, "incarnation", req.Incarnation)
			return &CreateBucketResponse{Success: false, Message: fmt.Sprintf(
				"bucket %q in database %q: incarnation %d of it was dropped on this node; nothing was created", req.Name, req.Database, req.Incarnation)}
		}
	}
	// Only a create the bucket's owner logged replaces a held copy: the owner
	// decided the name was free under the bucket's exclusive fence before it
	// logged the create, and its stream delivers that create after anything
	// earlier of the name (I-ORD). On the owner itself, and on a node that
	// coordinates for itself, a held name is answered "already exists".
	if !fromPeer(ctx) {
		return nil
	}
	held, isHeld := s.peekBucket(req.Database, req.Name)
	if !isHeld || held.id == 0 {
		return nil
	}
	if held.generation == generation {
		return s.decideIncarnationOfCreate(ctx, req, held)
	}
	// The held copy is replaced only when its incarnation is known dropped: the
	// create carries it, or this node holds its tombstone. A create from a
	// coordinator that had not learned the database carries neither, and the
	// copy it would replace is live (review R-1). That copy is kept and
	// the create answered "already exists" below, not refused: a refused entry
	// is re-sent for ever and holds every later entry of the stream behind it
	// (verifier D-2).
	heldGeneration := held.generation
	replace, err := s.heldIncarnationDropped(req.Database, heldGeneration, req.DroppedGenerations)
	if err != nil {
		return &CreateBucketResponse{Success: false, Message: failureMessage(fmt.Sprintf("bucket %q was not created: reading whether the copy held here was dropped failed", req.Name), err)}
	}
	if !replace {
		s.logger.Warn("kept a copy of a bucket against a create in another incarnation of its database that nobody has dropped",
			"database", req.Database, "bucket", req.Name, "held_generation", heldGeneration, "create_generation", generation)
		return s.decideIncarnationOfCreate(ctx, req, held)
	}
	if resp, err := s.DeleteBucketCtx(ctx, DeleteBucketRequest{Database: req.Database, Name: req.Name}); err != nil || !resp.Success {
		message := ""
		if resp != nil {
			message = resp.Message
		}
		return &CreateBucketResponse{Success: false, Message: failureMessage(fmt.Sprintf(
			"bucket %q was not created: the copy held here from an earlier incarnation of database %q could not be dropped first (%s)", req.Name, req.Database, message), err)}
	}
	if s.cluster != nil {
		if err := s.cluster.forgetSeededEverywhere(req.Database, req.Name); err != nil {
			return &CreateBucketResponse{Success: false, Message: failureMessage(fmt.Sprintf("bucket %q was not created: forgetting the replaced copy's snapshot positions failed", req.Name), err)}
		}
	}
	s.logger.Info("replaced a copy of a bucket held from an earlier incarnation of its database",
		"database", req.Database, "bucket", req.Name, "generation", generation, "replaced_generation", heldGeneration)
	return nil
}

// heldIncarnationDropped reports whether held, the incarnation of a copy this
// node holds, is known dropped: carried by the create, or in this node's
// tombstones.
func (s *Server) heldIncarnationDropped(database string, held int64, carried []int64) (bool, error) {
	if slices.Contains(carried, held) {
		return true, nil
	}
	return s.generationDropped(database, held)
}

// dropNamesGenerations reports whether a logged drop of a database names the
// generations it removes; one logged before generations existed does not.
func dropNamesGenerations(payload []byte) bool {
	var drop DeleteDatabaseRequest
	return bson.Unmarshal(payload, &drop) == nil && len(drop.Generations) > 0
}

// createCarried is the dropped generations a logged bucket create carries.
func createCarried(payload []byte) []int64 {
	var create CreateBucketRequest
	if bson.Unmarshal(payload, &create) != nil {
		return nil
	}
	return create.DroppedGenerations
}

// createInDroppedGeneration returns G-6 (a)'s refusal when entry is a bucket
// create stamped with a generation this node knows to be dropped, or nil
// twice. The second error is a failure to find out.
func (c *Cluster) createInDroppedGeneration(entry ReplEntry) (refusal, err error) {
	var create CreateBucketRequest
	if err := bson.Unmarshal(entry.Payload, &create); err != nil {
		return nil, fmt.Errorf("decoding the create of bucket %q: %w", entry.Bucket, err)
	}
	generation := create.DatabaseGeneration
	if generation == 0 {
		generation = legacyGeneration(create.Database)
	}
	dropped, err := c.s.generationDropped(create.Database, generation)
	if err != nil || !dropped {
		return nil, err
	}
	return fmt.Errorf("%w: %s", errGenerationDropped, droppedWhileCreatingMessage(create.Database, create.Name)), nil
}

// createOfDroppedGeneration reports whether a logged entry is a create — of a
// database or of a bucket — of a generation this node holds dropped. Such an
// entry is addressed to nobody (G-10): it would only bring back, on a member
// that missed the drop, an incarnation the cluster dropped. A drop is always
// shipped. One read, and only for a create.
func (s *Server) createOfDroppedGeneration(op transport.Opcode, payload []byte) (bool, error) {
	var database string
	var generation int64
	switch op {
	case transport.OpCreateDB:
		var create CreateDatabaseRequest
		if err := bson.Unmarshal(payload, &create); err != nil {
			return false, fmt.Errorf("decoding a logged create of a database: %w", err)
		}
		database, generation = create.Name, create.Generation
	case transport.OpCreateBucket:
		var create CreateBucketRequest
		if err := bson.Unmarshal(payload, &create); err != nil {
			return false, fmt.Errorf("decoding a logged create of a bucket: %w", err)
		}
		database, generation = create.Database, create.DatabaseGeneration
		if generation == 0 {
			generation = legacyGeneration(database)
		}
	default:
		return false, nil
	}
	if generation == 0 {
		return false, nil
	}
	return s.generationDropped(database, generation)
}

// legacyDatabaseDDL reports whether an entry is a create or drop of a database
// logged before generations existed: a create with no generation, a drop that
// names none.
func legacyDatabaseDDL(e ReplEntry) bool {
	switch transport.Opcode(e.Op) {
	case transport.OpCreateDB:
		var create CreateDatabaseRequest
		return bson.Unmarshal(e.Payload, &create) == nil && create.Generation == 0
	case transport.OpDeleteDB:
		return !dropNamesGenerations(e.Payload)
	}
	return false
}

// reportTombstones adds to resp the dropped generations of the database of
// entry e when they are why this node passed it over or refused it: a create
// of a generation dropped here, or any entry of the database this node
// refused while it holds a dropped generation of it (G-11 (a)). The sender
// learns the drop it missed from them. A refusal for any other reason sends
// them too: the sender merges what it lacks and ignores the rest.
func (c *Cluster) reportTombstones(resp *ReplPushResponse, e ReplEntry, applyErr error) {
	op := transport.Opcode(e.Op)
	if applyErr == nil {
		dropped, err := c.s.createOfDroppedGeneration(op, e.Payload)
		if err != nil || !dropped {
			return
		}
	}
	bucket, database := decodeFrameMeta(e.Payload).scope(op)
	if database == "" || slices.ContainsFunc(resp.DroppedGenerations, func(sent DatabaseTombstones) bool { return sent.Database == database }) {
		return
	}
	dropped, err := c.s.droppedGenerations(database)
	if err != nil {
		c.logger.Warn("could not read the dropped generations to send back with a refusal; the sender learns of the drop later",
			"database", database, "bucket", bucket, "error", err)
		return
	}
	if len(dropped) > 0 {
		resp.DroppedGenerations = append(resp.DroppedGenerations, DatabaseTombstones{Database: database, Generations: dropped})
	}
}

// learnFromPeer applies the dropped generations a peer sent back with its
// answer (G-11 (a), (c)). A failure is logged, not returned: the answer it
// rode on is still the answer, and the peer sends the generations again with
// its next refusal.
func (c *Cluster) learnFromPeer(peerID string, sent []DatabaseTombstones) {
	for _, tombstones := range sent {
		if err := c.s.learnTombstones(c.ctx, tombstones.Database, tombstones.Generations); err != nil {
			c.logger.Warn("could not apply the dropped generations a peer sent; they are applied when it sends them again",
				"peer", peerID, "database", tombstones.Database, "generations", tombstones.Generations, "error", err)
		}
	}
}

// untilCreateRefused returns a context that ends when ctx does or when this
// node learns that the generation the bucket create entry was stamped with is
// dropped, and the function that stops watching, which waits for the watcher
// to return.
func (c *Cluster) untilCreateRefused(ctx context.Context, entry ReplEntry) (context.Context, func()) {
	watched, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			changed := c.s.tombstonesChanged()
			if refused, err := c.createInDroppedGeneration(entry); refused != nil || err != nil {
				cancel()
				return
			}
			select {
			case <-changed:
			case <-watched.Done():
				return
			}
		}
	}()
	return watched, func() {
		cancel()
		<-done
	}
}

// decideIncarnationOfCreate is the holder's rule for a create a peer logged of
// a bucket it holds live (replication dispatch 10a.6; R8-A, review M-7): a
// create of the held copy's own incarnation — a redelivery, a replay — is
// answered "already exists"; one of another incarnation (an exact compare, 0
// a value like any other) is refused, never answered "already exists" with
// success, and the replica records it and stalls its sender on it
// (applyAsReplica). A held copy of an incarnation this node recorded dropped
// is the dropped bucket's, and the create replaces it, as G-6 (d) replaces a
// dropped generation's: without a drop entry, so it writes no drop record.
func (s *Server) decideIncarnationOfCreate(ctx context.Context, req CreateBucketRequest, held bucketFacts) *CreateBucketResponse {
	heldIncarnation, err := s.heldIncarnation(req.Database, held.slot, held.id)
	if err != nil {
		return &CreateBucketResponse{Success: false, Message: failureMessage(fmt.Sprintf("bucket %q was not created: reading the incarnation held here failed", req.Name), err)}
	}
	if heldIncarnation == req.Incarnation {
		return nil
	}
	droppedHere, err := s.dropRecorded(req.Database, req.Name, heldIncarnation)
	if err != nil {
		return &CreateBucketResponse{Success: false, Message: failureMessage(fmt.Sprintf("bucket %q was not created: reading whether the copy held here was dropped failed", req.Name), err)}
	}
	if droppedHere {
		return s.replaceDroppedCopy(ctx, req, heldIncarnation)
	}
	s.logger.Warn("refused a create of another incarnation of a bucket this node holds; the copy held here is kept",
		"database", req.Database, "bucket", req.Name, "held_incarnation", heldIncarnation, "create_incarnation", req.Incarnation)
	if outcome := applyOutcomeFrom(ctx); outcome != nil {
		outcome.anotherIncarnation = &incarnationMismatch{held: heldIncarnation, created: req.Incarnation}
	}
	return &CreateBucketResponse{Success: false, Message: fmt.Sprintf(
		"bucket %q in database %q: this node holds incarnation %d of it, and the create names incarnation %d; nothing was created",
		req.Name, req.Database, heldIncarnation, req.Incarnation)}
}

// replaceDroppedCopy drops a held copy of an incarnation this node recorded
// dropped, so that the create replaces it.
func (s *Server) replaceDroppedCopy(ctx context.Context, req CreateBucketRequest, heldIncarnation uint64) *CreateBucketResponse {
	if resp, err := s.DeleteBucketCtx(ctx, DeleteBucketRequest{Database: req.Database, Name: req.Name}); err != nil || !resp.Success {
		message := ""
		if resp != nil {
			message = resp.Message
		}
		return &CreateBucketResponse{Success: false, Message: failureMessage(fmt.Sprintf(
			"bucket %q was not created: the copy held here of dropped incarnation %d could not be dropped first (%s)", req.Name, heldIncarnation, message), err)}
	}
	if s.cluster != nil {
		if err := s.cluster.forgetSeededEverywhere(req.Database, req.Name); err != nil {
			return &CreateBucketResponse{Success: false, Message: failureMessage(fmt.Sprintf("bucket %q was not created: forgetting the replaced copy's snapshot positions failed", req.Name), err)}
		}
	}
	s.logger.Info("replaced a copy of a dropped bucket incarnation", "database", req.Database, "bucket", req.Name, "replaced_incarnation", heldIncarnation)
	return nil
}
