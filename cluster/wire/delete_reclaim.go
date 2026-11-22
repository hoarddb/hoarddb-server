package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

const (
	// reclaimDeleteKind is the kind byte of a delete's reclaim record.
	reclaimDeleteKind byte = 'd'
	// reclaimTail is the epoch and seq that end every 'd' key.
	reclaimTail = 16
	// reclaimHead is the node prefix and kind: 11 + 1 bytes.
	reclaimHead = 12
	// reclaimScope is the slot, database and bucket id: 2 + 8 + 8 bytes.
	reclaimScope = 18
	// maxEncodedID is conv §2's largest encID.
	maxEncodedID = 1024
)

// ReclaimRecord is a delete's node-local 'd' record (conv §11, §12):
// [0xFFFF][0x00×8][0x08]['d'][slot:2][db:8][bucket:8][encID][epoch:8][seq:8],
// with an empty value, stamped with the position that created it.
type ReclaimRecord struct {
	// Doc is the deleted document.
	Doc DocumentKey
	// Pos is the delete's position, which is also its fence position.
	Pos cluster.Position
}

// reclaimPrefix is the key up to and including the encID.
func reclaimPrefix(doc DocumentKey) []byte {
	scope := storage.Scope(doc.Ref.Slot, doc.Ref.DatabaseID, doc.Ref.BucketID)
	return storage.NodeKey(storage.NSReclaim, []byte{reclaimDeleteKind}, scope, doc.EncID)
}

// Key encodes the record's key.
func (r ReclaimRecord) Key() []byte {
	return cluster.AppendPosition(reclaimPrefix(r.Doc), r.Pos)
}

// DecodeReclaimKey parses a 'd' key from its end: the encID is whatever lies
// between the 18-byte scope and the 16 fixed bytes, and it must be exactly one
// encoded id component of at most 1024 bytes.
func DecodeReclaimKey(key []byte) (ReclaimRecord, error) {
	head := storage.NodeKey(storage.NSReclaim, []byte{reclaimDeleteKind})
	if len(key) < reclaimHead+reclaimScope+1+reclaimTail || !bytes.HasPrefix(key, head) {
		return ReclaimRecord{}, errors.New("the key is not a delete reclaim record")
	}
	scope := key[reclaimHead : reclaimHead+reclaimScope]
	encID := key[reclaimHead+reclaimScope : len(key)-reclaimTail]
	if len(encID) > maxEncodedID {
		return ReclaimRecord{}, fmt.Errorf("the record's id is %d bytes and the maximum is %d", len(encID), maxEncodedID)
	}
	n, err := store.EncodedComponentLen(encID)
	if err != nil {
		return ReclaimRecord{}, fmt.Errorf("measuring the record's id component: %w", err)
	}
	if n != len(encID) {
		return ReclaimRecord{}, fmt.Errorf("the record's id is %d bytes but its first component is %d, so it is not exactly one component", len(encID), n)
	}
	pos, err := cluster.DecodePosition(key[len(key)-reclaimTail:])
	if err != nil {
		return ReclaimRecord{}, fmt.Errorf("decoding the record's position: %w", err)
	}
	return ReclaimRecord{
		Doc: DocumentKey{
			Ref: storage.BucketRef{
				Slot:       binary.BigEndian.Uint16(scope[0:2]),
				DatabaseID: binary.BigEndian.Uint64(scope[2:10]),
				BucketID:   binary.BigEndian.Uint64(scope[10:18]),
			},
			EncID: append([]byte(nil), encID...),
		},
		Pos: pos,
	}, nil
}

// reclaimRecordKeys finds doc's 'd' records. The id is prefix-free, so a key
// with the 16 fixed bytes after the prefix is this document's and no other's.
func reclaimRecordKeys(snap storage.KVSnapshot, doc DocumentKey) ([][]byte, error) {
	prefix := reclaimPrefix(doc)
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: storage.PrefixEnd(prefix)})
	defer it.Close()
	var keys [][]byte
	for it.Seek(prefix); it.Valid(); it.Next() {
		if key := it.Key(); len(key) == len(prefix)+reclaimTail {
			keys = append(keys, append([]byte(nil), key...))
		}
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("listing the 'd' records of document %x: %w", doc.EncID, err)
	}
	return keys, nil
}

// PairLocker runs fn inside the (slot, database) critical section;
// cluster.PairSet is the production one.
type PairLocker interface {
	// With holds the pair's lock for the duration of fn.
	With(slot uint16, db uint64, fn func(*cluster.Pair))
}

// Sweeper removes the keys a delete left behind (conv §11 rule 3). It holds no
// per-document state and starts no goroutine: the reclaimer calls SweepBatch at
// the rate config:Config.Storage.ReclaimKeysPerSecond allows.
type Sweeper struct {
	kv    storage.KV
	pairs PairLocker
}

// NewSweeper returns a sweeper over kv that takes each batch's critical section
// from pairs.
func NewSweeper(kv storage.KV, pairs PairLocker) *Sweeper {
	return &Sweeper{kv: kv, pairs: pairs}
}

// SweepProgress reports what one SweepBatch did.
type SweepProgress struct {
	// Deleted is how many document keys the batch removed, the header included.
	Deleted int
	// Done is true when the record is gone and nothing remains to sweep.
	Done bool
	// Reinserted is true when the header was no longer a tombstone, so the
	// record was dropped and no document key was touched.
	Reinserted bool
}

// SweepBatch removes up to maxKeys keys of the deleted document rec names, in
// one batch under the pair's critical section. It re-reads the header inside
// that section: a header that is not 0x00 means the document was re-inserted,
// and the record alone is dropped. Otherwise the order is st, then doc's field
// keys, then the header and the record together, so a crash leaves an
// invisible document and a later insert finishes the job. Call again until
// Done.
func (s *Sweeper) SweepBatch(rec ReclaimRecord, maxKeys int) (SweepProgress, error) {
	if maxKeys <= 0 {
		return SweepProgress{}, fmt.Errorf("sweeping document %x with a batch of %d keys", rec.Doc.EncID, maxKeys)
	}
	var progress SweepProgress
	var sweepErr error
	s.pairs.With(rec.Doc.Ref.Slot, rec.Doc.Ref.DatabaseID, func(*cluster.Pair) {
		progress, sweepErr = s.sweepLocked(rec, maxKeys)
	})
	return progress, sweepErr
}

func (s *Sweeper) sweepLocked(rec ReclaimRecord, maxKeys int) (SweepProgress, error) {
	doc := rec.Doc
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	state, err := ReadHeaderState(snap, doc)
	if err != nil {
		return SweepProgress{}, fmt.Errorf("sweeping document %x: %w", doc.EncID, err)
	}
	batch := s.kv.NewBatch()
	defer batch.Discard()
	var progress SweepProgress
	switch state {
	case HeaderLive:
		progress = SweepProgress{Done: true, Reinserted: true}
	case HeaderAbsent:
		progress = SweepProgress{Done: true}
	case HeaderTombstone:
		// The last batch also carries the header and the record, so their
		// size is held back from every batch's budget.
		reserve := int64(len(doc.header()) + len(rec.Key()) + 2*batchEntryOverhead)
		keys, exhausted, err := nextSweepKeys(snap, doc, sweepLimits{keys: maxKeys, bytes: s.kv.MaxBatchBytes() - reserve})

		if err != nil {
			return SweepProgress{}, err
		}
		for _, key := range keys {
			if err := batch.Delete(key); err != nil {
				return SweepProgress{}, fmt.Errorf("sweeping key %x of document %x: %w", key, doc.EncID, err)
			}
		}
		progress.Deleted = len(keys)
		if exhausted {
			if err := batch.Delete(doc.header()); err != nil {
				return SweepProgress{}, fmt.Errorf("sweeping the header of document %x: %w", doc.EncID, err)
			}
			progress.Deleted++
			progress.Done = true
		}
	}
	if progress.Done {
		if err := batch.Delete(rec.Key()); err != nil {
			return SweepProgress{}, fmt.Errorf("dropping the 'd' record of document %x: %w", doc.EncID, err)
		}
	}
	if err := batch.Commit(false); err != nil {
		return SweepProgress{}, fmt.Errorf("committing the sweep of document %x: %w", doc.EncID, err)
	}
	return progress, nil
}

// sweepLimits bound one sweep batch by key count and by estimated bytes.
type sweepLimits struct {
	keys  int
	bytes int64
}

// nextSweepKeys returns the document's next keys within limits, st first, then
// doc's field keys, never the header. It always takes at least one key, so a
// key larger than the budget still goes, alone. exhausted is true when every
// key fit, so the header may go in the same batch.
func nextSweepKeys(snap storage.KVSnapshot, doc DocumentKey, limits sweepLimits) ([][]byte, bool, error) {
	header := doc.header()
	var keys [][]byte
	var size int64
	for _, prefix := range [][]byte{doc.storePrefix(), header} {
		it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: storage.PrefixEnd(prefix)})
		for it.Seek(prefix); it.Valid(); it.Next() {
			key := it.Key()
			if bytes.Equal(key, header) {
				continue
			}
			cost := int64(len(key) + batchEntryOverhead)
			if len(keys) == limits.keys || (len(keys) > 0 && size+cost > limits.bytes) {
				it.Close()
				return keys, false, nil
			}
			size += cost
			keys = append(keys, append([]byte(nil), key...))
		}
		err := it.Err()
		it.Close()
		if err != nil {
			return nil, false, fmt.Errorf("listing the keys of deleted document %x: %w", doc.EncID, err)
		}
	}
	return keys, true, nil
}
