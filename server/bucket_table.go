package server

import (
	"fmt"
	"sync"
)

// bucketTable is a database's registry of buckets by name, and the only door
// to it. Inside is a catalogTable: a bucket is a pointer-free record in a
// chunk that never moves (catalog-gc spec §5.2), so an idle bucket is nothing
// the collector traces. No method hands out the inside, and no caller ranges
// over it under its own lock except through each.
//
// Every method except scan and scanRecords expects the caller to hold the
// lock named by guard (the owning inMemoryDB's mu): read for get, count and
// each, write for claim, unlink and free. The two scans take the lock
// themselves, one record chunk per hold, because they must release it before
// they call the visitor.
type bucketTable struct {
	guard   *sync.RWMutex
	records *catalogTable
}

// heldRecord is a record and the epoch it was found under. A holder that
// locks the record later compares the epoch under the record's mutex: a slot
// is reused once its bucket is dropped, and the epoch is what tells a stale
// holder that the record now names another bucket (spec §5.3).
type heldRecord struct {
	rec   *bucketRec
	epoch uint32
}

// current reports whether held still names the bucket it was found as, and
// that bucket is not closed. The caller holds the record's mutex.
func (held heldRecord) current() bool {
	return held.rec.epoch.Load() == held.epoch && !held.rec.closed
}

// newBucketTable returns an empty table guarded by guard.
func newBucketTable(guard *sync.RWMutex) bucketTable {
	return bucketTable{guard: guard, records: newCatalogTable()}
}

// get returns the record named name, and false when there is none. The caller
// holds the guard.
func (t *bucketTable) get(name string) (heldRecord, bool) {
	rec := t.records.find(name)
	if rec == nil {
		return heldRecord{}, false
	}
	return heldRecord{rec: rec, epoch: rec.epoch.Load()}, true
}

// count returns the number of names in the table, including a bucket whose
// create has not committed. The caller holds the guard.
func (t *bucketTable) count() int {
	return t.records.linkedCount()
}

// clientCount is count without the parts of spread collections: the number of
// buckets a listing shows. The caller holds the guard.
func (t *bucketTable) clientCount() int {
	return t.records.linkedNonPartCount()
}

// claim returns the record named name, giving the name a slot if it has none,
// of type btype. A record no create has committed (id 0) is claimed for
// generation and creating, under the guard detachDroppedGenerations reads
// them under. The caller holds the guard for writing.
func (t *bucketTable) claim(name string, btype BucketType, generation int64, creating bool) (heldRecord, error) {
	rec, err := t.records.claim(name, bucketTypeCode(btype), generation, creating)
	if err != nil {
		return heldRecord{}, fmt.Errorf("claiming bucket %q: %w", name, err)
	}
	return heldRecord{rec: rec, epoch: rec.epoch.Load()}, nil
}

// unlink is phase one of a drop: the name stops finding held's record, which
// stays allocated until free. It reports false, and changes nothing, when the
// name no longer finds that record in that epoch: another drop unlinked it
// first and owns the rest of the drop. An error is the index contradicting
// itself: the name finds the record but the record's chain does not reach it,
// so the record is left linked and the caller must not finish the drop. The
// caller holds the guard for writing.
func (t *bucketTable) unlink(name string, held heldRecord) (bool, error) {
	if t.records.find(name) != held.rec || held.rec.epoch.Load() != held.epoch {
		return false, nil
	}
	if err := t.records.unlink(held.rec); err != nil {
		return false, fmt.Errorf("unlinking bucket %q from its registry: %w", name, err)
	}
	return true, nil
}

// free is phase three of a drop: the slot of a record unlink unlinked, and
// the drop closed, is given back. The caller holds the guard for writing.
func (t *bucketTable) free(rec *bucketRec) error {
	if err := t.records.free(rec); err != nil {
		return fmt.Errorf("freeing a dropped bucket's record: %w", err)
	}
	return nil
}

// each calls visit with every name in the table and its record. The caller
// holds the guard, for reading or for writing, across the whole walk, so it is
// for the few callers that must decide over every bucket under one hold (a
// database drop); visit must not take the guard or unlink.
func (t *bucketTable) each(visit func(name string, held heldRecord)) {
	t.records.forEachLinked(func(name string, rec *bucketRec) {
		visit(name, heldRecord{rec: rec, epoch: rec.epoch.Load()})
	})
}

// scan calls visit with the table's names, one record chunk at a time and
// outside the guard, so a visitor may take any lock. A name present for the
// whole walk is visited exactly once; one created or dropped during it may be
// visited or not. A visitor that needs more than a name asks for the bucket
// by name afterwards. A step is one record chunk, at most maxRecordChunk
// names (spec §5.4's scan contract).
func (t *bucketTable) scan(visit func(names []string)) {
	t.records.scan(t.guard, visit)
}

// scanRecords is scan for a caller that locks each record afterwards: it
// hands each chunk's records with their epochs, outside the guard.
func (t *bucketTable) scanRecords(visit func(held []heldRecord)) {
	for cursor := 0; ; {
		t.guard.RLock()
		held, next, done := t.records.recordsStep(cursor)
		t.guard.RUnlock()
		if done {
			return
		}
		cursor = next
		if len(held) > 0 {
			visit(held)
		}
	}
}

// The record stores a bucket's type in one byte. The codes are in-memory
// only; the catalog entry records the type by name.
var bucketTypeCodes = [...]BucketType{BucketTypeHash, BucketTypeBTree, BucketTypeFIFO, BucketTypeLIFO, BucketTypeHeap, BucketTypeBlob}

// bucketTypeCode is btype's code, 0 for a type no bucket can have (a create
// refuses it before its record is written, and an attach before its id is).
func bucketTypeCode(btype BucketType) uint8 {
	for index, known := range bucketTypeCodes {
		if known == btype {
			return uint8(index + 1)
		}
	}
	return 0
}

// bucketTypeOfCode is the type code names, "" for 0.
func bucketTypeOfCode(code uint8) BucketType {
	if code == 0 || int(code) > len(bucketTypeCodes) {
		return ""
	}
	return bucketTypeCodes[code-1]
}
