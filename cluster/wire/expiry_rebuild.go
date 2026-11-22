package wire

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// expiryRebuildBatch is how many entries one batch of the rebuild writes.
const expiryRebuildBatch = 512

// RebuildExpiry derives the expiry entries of every document and queue element a pair holds from
// the stored data and each bucket's declaration (expiry spec 3.4). A seed copies a pair's raw keys
// and the node-local expiry queue is not among them, so without this a copy seeded after expiring
// data was stored would never remove it once it became the owner. It is idempotent: an entry that
// the drain already wrote is written again with the same key and an empty value, and an entry for
// something a concurrent write has since changed is verified and dropped by the sweep, which
// re-reads the item before it removes anything.
//
// Cost: one pass over the documents and field stores of the buckets that declare expiry, which is
// the order of the seed that just copied them; nothing resident is kept per bucket, and a bucket
// without expiry costs one declaration read.
func (s *RetireSweep) RebuildExpiry(ctx context.Context, slot uint16, dbID uint64) error {
	if s.deps.KV == nil {
		return errors.New("the node's retention sweep has not started, so the expiry queue of the seeded pair cannot be built yet. The pull retries")
	}
	var after uint64
	first := true
	for {
		page, err := s.bucketsOfPair(slot, dbID, after, first)
		if err != nil {
			return err
		}
		first = false
		for _, bucket := range page {
			table, err := s.expireOf(ctx, dbID, bucket)
			if err != nil {
				return err
			}
			if table.BucketField == 0 && len(table.Stores) == 0 {
				continue
			}
			if err := rebuildBucketExpiry(ctx, s.deps.KV, s.budget, storage.BucketRef{Slot: slot, DatabaseID: dbID, BucketID: bucket}, table); err != nil {
				return err
			}
		}
		if len(page) < recordPage {
			return nil
		}
		after = page[len(page)-1]
	}
}

// expireOf is the expiry declaration in force for an active bucket, the zero table for a bucket
// that is gone, dropping or does not expire.
func (s *RetireSweep) expireOf(ctx context.Context, dbID, bucket uint64) (machine.ExpireTable, error) {
	name, found, err := s.deps.BucketName(ctx, dbID, bucket)
	if err != nil {
		return machine.ExpireTable{}, fmt.Errorf("reading the name of bucket %016x: %w", bucket, err)
	}
	if !found {
		return machine.ExpireTable{}, nil
	}
	decl, _, found, err := s.deps.Declarations.Read(ctx, dbID, name)
	if err != nil {
		return machine.ExpireTable{}, fmt.Errorf("reading the declaration of bucket %q: %w", name, err)
	}
	if !found || decl.BucketID != bucket || decl.State != machine.StateActive {
		return machine.ExpireTable{}, nil
	}
	return machine.EffectiveExpire(decl, s.deps.Published(dbID)), nil
}

// expiryEntrySink collects the entries of a scan and writes them in batches.
type expiryEntrySink struct {
	kv      storage.KV
	budget  *reclaimBudget
	pending []storage.ExpiryEntry
}

func (k *expiryEntrySink) add(ctx context.Context, entry storage.ExpiryEntry) error {
	k.pending = append(k.pending, entry)
	if len(k.pending) < expiryRebuildBatch {
		return nil
	}
	return k.flush(ctx)
}

func (k *expiryEntrySink) flush(ctx context.Context) error {
	if len(k.pending) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("rebuilding the expiry queue: %w", err)
	}
	batch := k.kv.NewBatch()
	for _, entry := range k.pending {
		if err := setExpiryEntry(batch, entry); err != nil {
			batch.Discard()
			return err
		}
	}
	if err := batch.Commit(false); err != nil {
		return fmt.Errorf("committing %d rebuilt expiry entries: %w", len(k.pending), err)
	}
	cost := len(k.pending)
	k.pending = k.pending[:0]
	if k.budget == nil {
		return nil
	}
	return k.budget.charge(ctx, cost)
}

// rebuildBucketExpiry scans one bucket's documents and expiring store fields and sets the entry
// of every live document whose expire field is a datetime and of every queue element that holds
// one at its declared path.
func rebuildBucketExpiry(ctx context.Context, kv storage.KV, budget *reclaimBudget, ref storage.BucketRef, table machine.ExpireTable) error {
	snapshot := kv.NewSnapshot()
	defer snapshot.Discard()
	sink := &expiryEntrySink{kv: kv, budget: budget}
	if table.BucketField != 0 {
		if err := scanDocumentExpiry(ctx, snapshot, sink, ref, table.BucketField); err != nil {
			return err
		}
	}
	if len(table.Stores) != 0 {
		if err := scanElementExpiry(ctx, snapshot, sink, ref, table.Stores); err != nil {
			return err
		}
	}
	return sink.flush(ctx)
}

// scanDocumentExpiry walks a bucket's document keys: a header then its scalars, in key order.
func scanDocumentExpiry(ctx context.Context, snapshot storage.KVSnapshot, sink *expiryEntrySink, ref storage.BucketRef, fieldID uint16) error {
	prefix := storage.Key(ref.Slot, ref.DatabaseID, ref.BucketID, storage.NSDocuments)
	iter := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: storage.PrefixEnd(prefix)})
	defer iter.Close()
	var liveID []byte
	for iter.Seek(prefix); iter.Valid(); iter.Next() {
		key := iter.Key()
		rest := key[len(prefix):]
		width, err := storage.EncodedComponentLen(rest)
		if err != nil {
			return fmt.Errorf("a document key %x of bucket %d does not parse: %w", key, ref.BucketID, err)
		}
		encID, tail := rest[:width], rest[width:]
		switch len(tail) {
		case 0:
			header, err := iter.Value()
			if err != nil {
				return fmt.Errorf("reading the header of document %x: %w", encID, err)
			}
			liveID = nil
			if len(header) != 1 || header[0] != 0x00 {
				liveID = append([]byte(nil), encID...)
			}
		case 2:
			if binary.BigEndian.Uint16(tail) != fieldID || string(encID) != string(liveID) {
				continue
			}
			stored, err := iter.Value()
			if err != nil {
				return fmt.Errorf("reading the expire field of document %x: %w", encID, err)
			}
			if atMillis, ok := expiryDatetime(stored); ok {
				entry := storage.ExpiryEntry{AtMillis: atMillis, Slot: ref.Slot, DBID: ref.DatabaseID, BucketID: ref.BucketID, Kind: storage.ExpiryKindDocument, EncID: append([]byte(nil), encID...)}
				if err := sink.add(ctx, entry); err != nil {
					return err
				}
			}
		}
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("walking the documents of bucket %d: %w", ref.BucketID, err)
	}
	return nil
}

// scanElementExpiry walks a bucket's field stores for the queue elements of the expiring fields.
// An element key is encID ‖ fieldID:2 ‖ NSQueue ‖ position:8; anything longer is a nested store.
func scanElementExpiry(ctx context.Context, snapshot storage.KVSnapshot, sink *expiryEntrySink, ref storage.BucketRef, stores []machine.ExpireStore) error {
	prefix := storage.Key(ref.Slot, ref.DatabaseID, ref.BucketID, storage.NSFieldStore)
	iter := snapshot.NewIterator(storage.IterOptions{Lower: prefix, Upper: storage.PrefixEnd(prefix)})
	defer iter.Close()
	var checkedID []byte
	var live bool
	for iter.Seek(prefix); iter.Valid(); iter.Next() {
		key := iter.Key()
		rest := key[len(prefix):]
		width, err := storage.EncodedComponentLen(rest)
		if err != nil {
			return fmt.Errorf("a field store key %x of bucket %d does not parse: %w", key, ref.BucketID, err)
		}
		encID, tail := rest[:width], rest[width:]
		if len(tail) != 2+1+8 || tail[2] != storage.NSQueue {
			continue
		}
		path, expires := "", false
		fieldID := binary.BigEndian.Uint16(tail)
		for _, expiring := range stores {
			if expiring.FieldID == fieldID {
				path, expires = expiring.Path, true
			}
		}
		if !expires {
			continue
		}
		if checkedID == nil || string(checkedID) != string(encID) {
			state, err := ReadHeaderState(snapshot, DocumentKey{Ref: ref, EncID: encID})
			if err != nil {
				return err
			}
			checkedID, live = append([]byte(nil), encID...), state == HeaderLive
		}
		if !live {
			continue
		}
		element, err := iter.Value()
		if err != nil {
			return fmt.Errorf("reading an element of field %d of document %x: %w", fieldID, encID, err)
		}
		atMillis, _, ok := elementExpiry(element, []byte(path))
		if !ok {
			continue
		}
		position := binary.BigEndian.Uint64(tail[3:])
		entry := storage.ExpiryEntry{
			AtMillis: atMillis, Slot: ref.Slot, DBID: ref.DatabaseID, BucketID: ref.BucketID,
			Kind: storage.ExpiryKindElement, EncID: append([]byte(nil), encID...), FieldID: fieldID, Position: position,
		}
		if err := sink.add(ctx, entry); err != nil {
			return err
		}
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("walking the field stores of bucket %d: %w", ref.BucketID, err)
	}
	return nil
}
