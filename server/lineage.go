package server

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The lineage of a copy (replication spec §20.8.1, build doc step 9.1).
//
// Every copy of a bucket records, in the state document each apply already
// rewrites, which owner's term it follows and how far into it: the term, and
// the origin's sequence of the last entry of that term it applied. Two copies
// with the same pair hold the same entries; of two in one term, the higher
// position holds everything the lower does. The position is the origin's
// sequence, never this node's own log's: on the owner they are the same
// number, on a replica they are unrelated (build doc §1 trap 6), and a replica
// that recorded its own would hold a position no other copy shares.
//
// Dispatch 9 records the lineage; nothing reads it to decide anything until
// the hand-off of dispatch 10.

type lineageCtxKey struct{}

// appliedLineage is what the write path knows of the entry being applied that
// the copy's lineage records: the origin's sequence for it and whether this
// node is that origin.
type appliedLineage struct {
	originSeq  uint64
	originated bool
}

// withLineage attaches the origin's sequence for the entry being applied, and
// whether this node originated it: the owner's apply passes its own sequence
// and true, a replica's the owner's sequence (entry.Seq, not the sequence its
// own log gave the entry), and replay the one its log's origin block records.
func withLineage(ctx context.Context, originSeq uint64, originated bool) context.Context {
	return context.WithValue(ctx, lineageCtxKey{}, appliedLineage{originSeq: originSeq, originated: originated})
}

// lineageFrom returns what withLineage attached; the zero value for an apply
// that records no lineage (a standalone node, an entry logged before origins).
func lineageFrom(ctx context.Context) appliedLineage {
	applied, _ := ctx.Value(lineageCtxKey{}).(appliedLineage)
	return applied
}

// stampCreateLineage writes the bucket's first term and its incarnation into a
// create's payload: the membership epoch the owner logs the create under
// (A-9.1), and a number minted here (R8-A). Both are stamped before the
// append, so the owner's apply, every replica's and every replay read the one
// pair, and a copy whose apply a crash interrupted learns them from its own
// log. A client's values are overwritten.
func stampCreateLineage(payload []byte, epoch uint64) ([]byte, error) {
	incarnation, err := mintIncarnation()
	if err != nil {
		return nil, err
	}
	stamped, err := stampPayload(payload, bson.D{
		{Key: "term", Value: int64(epoch)},
		{Key: "incarnation", Value: int64(incarnation)},
	})
	if err != nil {
		return nil, fmt.Errorf("stamping a create with its first term %d and incarnation %d: %w", epoch, incarnation, err)
	}
	return stamped, nil
}

// mintIncarnation draws a bucket incarnation: random, never 0. It is drawn
// below 2^63 because BSON carries an integer as a signed 64-bit one, which is
// how the stamp and the state document both store it.
func mintIncarnation() (uint64, error) {
	var drawn [8]byte
	for {
		if _, err := rand.Read(drawn[:]); err != nil {
			return 0, fmt.Errorf("drawing a bucket incarnation: %w", err)
		}
		if incarnation := binary.BigEndian.Uint64(drawn[:]) & math.MaxInt64; incarnation != 0 {
			return incarnation, nil
		}
	}
}

// createLineage is the lineage a create's state document starts with: the
// term and incarnation the owner stamped and the create's own position, opened
// on the node that originated it. A create a client sent straight to a node
// that logs nothing for it carries no trusted term, and its incarnation is
// minted here, because no other node will stamp it and the node keeps no log
// to replay it from (review B-2). That is a standalone node, and also a
// one-member cluster, whose client frames take the standalone door while the
// cluster will grow (BUG-0624): there the first term is the membership epoch
// and the copy is opened, as on the owner that stamped it. A create that
// arrived stamped (a coordinator's, a replica's, a replay's) never mints
// here: a number drawn here would differ on every node (trap 10).
func (s *Server) createLineage(ctx context.Context, req CreateBucketRequest) (store.BucketState, error) {
	applied := lineageFrom(ctx)
	state := store.BucketState{Through: applied.originSeq, Opened: applied.originated}
	if generationsTrusted(ctx) {
		state.Term, state.Incarnation = req.Term, req.Incarnation
		return state, nil
	}
	incarnation, err := mintIncarnation()
	if err != nil {
		return store.BucketState{}, err
	}
	state.Incarnation = incarnation
	if s.cluster != nil {
		state.Term, state.Opened = s.cluster.Epoch(), true
	}
	return state, nil
}

// shippedTerm is the term an entry this node logs as an owner ships in (step
// 9.2): a create's is the epoch it is logged under, which is the bucket's
// first term; any other entry's is its bucket copy's term. An entry of no
// bucket held here — database DDL, a write the apply refuses — ships in 0.
func (c *Cluster) shippedTerm(entry ReplEntry, epoch uint64) (uint64, error) {
	op := transport.Opcode(entry.Op)
	if op == transport.OpCreateBucket {
		return epoch, nil
	}
	if entry.Bucket == "" {
		return 0, nil
	}
	_, database := decodeFrameMeta(entry.Payload).scope(op)
	term, _, err := c.s.bucketLineage(database, entry.Bucket)
	if err != nil {
		return 0, fmt.Errorf("reading the term to ship %s of bucket %q in database %q: %w", op, entry.Bucket, database, err)
	}
	return term, nil
}

// The in-memory copy of a bucket's term and opened flag, kept beside its id so
// that the write and receive paths need not read the store (spec §20.8.1: 16
// bytes per attached bucket). It is read from the state document the first
// time it is wanted after the bucket is attached, rather than at the attach:
// a start would otherwise read a million state documents to load a million
// buckets. Every writer of either field sets it under the bucket's mutex.
const (
	lineageKnown uint32 = 1 << iota
	lineageOpened
	// lineagePositionZero says the copy's position (through) is zero. The
	// cache keeps 16 bytes per bucket and has no field for the position, and a
	// copy in term 0 is judged opened by copyIsOpened's pre-lineage rule only
	// when its position is zero.
	lineagePositionZero
)

// bucketLineage returns the term of database/bucket's copy and whether this
// node opened it: a peek of the cache (class P), and on a miss the fill
// (class PW, fillLineage). A bucket this node does not hold has term 0. The
// caller must not hold the bucket's lock.
func (s *Server) bucketLineage(database, bucket string) (term uint64, opened bool, err error) {
	facts, held := s.peekBucket(database, bucket)
	if !held {
		return 0, false, nil
	}
	if facts.lineageFlags&lineageKnown != 0 {
		return facts.term, facts.lineageFlags&lineageOpened != 0, nil
	}
	return s.fillLineage(database, bucket)
}

// fillLineage loads the lineage cache of database/bucket from its state
// document under the bucket's lock, unless a writer filled it first. A
// bucket whose create has not committed waits for it on the lock; one that
// failed, or that a drop closed, has term 0.
func (s *Server) fillLineage(database, bucket string) (term uint64, opened bool, err error) {
	rec := s.lockServedRecord(database, bucket)
	if rec == nil {
		return 0, false, nil
	}
	defer rec.mu.Unlock()
	if flags := rec.lineageFlags.Load(); flags&lineageKnown != 0 {
		return rec.term.Load(), flags&lineageOpened != 0, nil
	}
	snapshot := s.kv.NewSnapshot()
	defer snapshot.Discard()
	id := rec.id.Load()
	st, err := store.LoadBucketState(snapshot, rec.slot, storage.LegacyDatabaseID(database), id)
	if err != nil {
		return 0, false, fmt.Errorf("reading the lineage of bucket %d: %w", id, err)
	}
	rec.setLineageStateLocked(st)
	return st.Term, st.Opened, nil
}
