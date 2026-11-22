package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/hoarddb/hoarddb-server/storage"
)

// This file is the key arithmetic of the node's two background sweeps (conv 12): which keys of
// a bucket belong to one retired field id, and how a step walks a range in bounded pieces. It
// reads keys and decides; it deletes nothing itself.

// keyRange is a half-open range of keys.
type keyRange struct{ lower, upper []byte }

// sweepWindow bounds one step: the keys it visits, and the estimated bytes of the deletes it
// batches. A step visits at most keys keys however few of them match, so a bucket of millions
// of live keys cannot pin a pair lock for one long scan.
type sweepWindow struct {
	keys  int
	bytes int64
}

// stepScan is what one step found.
type stepScan struct {
	// doomed are the keys to delete, copied.
	doomed [][]byte
	// last is the last key the step visited; the next step resumes strictly after it.
	last []byte
	// visited counts the keys the step looked at.
	visited int
	// done is true when every range was read to its end.
	done bool
}

// retiredFieldRanges are the only two spaces a field id appears in: a scalar field's key lives
// in the document space (0x01) and a store field's keys in the field-store space (0x07).
// Nothing else under a bucket's scope carries a field id after a document id.
func retiredFieldRanges(slot uint16, db, bucket uint64) []keyRange {
	ranges := make([]keyRange, 0, 2)
	for _, space := range []byte{storage.NSDocuments, storage.NSFieldStore} {
		lower := storage.Key(slot, db, bucket, space)
		ranges = append(ranges, keyRange{lower: lower, upper: storage.PrefixEnd(lower)})
	}
	return ranges
}

// bucketRange is every key under a bucket's scope: documents, field stores, delivery state and
// dedupe entries alike.
func bucketRange(slot uint16, db, bucket uint64) []keyRange {
	scope := storage.Scope(slot, db, bucket)
	return []keyRange{{lower: scope, upper: storage.PrefixEnd(scope)}}
}

// errKeyShape marks a key in a bucket's document or field-store space that does not have the
// shape the layout gives it. The sweep never deletes such a key by guesswork.
var errKeyShape = errors.New("the key does not have the layout of a document or field-store key")

// keyCarriesField reports whether key, a data key of the document space (0x01) or the
// field-store space (0x07) of one bucket, is a key of field id fieldID. The layout is
//
//	scalar   [slot:2][db:8][bucket:8][0x01] ‖ encID ‖ fieldID:2
//	header   [slot:2][db:8][bucket:8][0x01] ‖ encID
//	store    [slot:2][db:8][bucket:8][0x07] ‖ encID ‖ fieldID:2 ‖ inner:1 ‖ …
//
// and encID is one prefix-free encoded component, so its length is read from its own tag and
// the field id is the two bytes right after it. Matching the id bytes anywhere else, or by
// suffix, would hit a document whose id contains them; this reads exactly one position.
func keyCarriesField(key []byte, fieldID uint16) (bool, error) {
	if len(key) < 19 {
		return false, fmt.Errorf("%w: %d bytes", errKeyShape, len(key))
	}
	space := key[18]
	n, err := storage.EncodedComponentLen(key[19:])
	if err != nil {
		return false, fmt.Errorf("%w: measuring the document id of key %x: %w", errKeyShape, key, err)
	}
	rest := key[19+n:]
	switch space {
	case storage.NSDocuments:
		switch len(rest) {
		case 0:
			return false, nil
		case 2:
			return binary.BigEndian.Uint16(rest) == fieldID, nil
		}
	case storage.NSFieldStore:
		if len(rest) >= 3 {
			return binary.BigEndian.Uint16(rest) == fieldID, nil
		}
	default:
		return false, fmt.Errorf("%w: key %x is in space 0x%02x", errKeyShape, key, space)
	}
	return false, fmt.Errorf("%w: key %x has %d bytes after its document id in space 0x%02x", errKeyShape, key, len(rest), space)
}

// scanRanges walks ranges in order, starting strictly after cursor, and collects the keys match
// accepts. It stops before the key that would pass the window, so every key it reports as
// visited is one it has judged. A key whose delete alone is over the byte bound is still taken
// when it is the first, or no step could ever make progress.
func scanRanges(snap storage.KVSnapshot, ranges []keyRange, cursor []byte, window sweepWindow, match func(key []byte) (bool, error)) (stepScan, error) {
	var scan stepScan
	var size int64
	for _, span := range ranges {
		if cursor != nil && bytes.Compare(cursor, span.upper) >= 0 {
			continue
		}
		lower := span.lower
		if cursor != nil && bytes.Compare(cursor, lower) >= 0 {
			lower = append(append(make([]byte, 0, len(cursor)+1), cursor...), 0x00)
		}
		stopped, err := scanOne(snap, span, lower, window, &scan, &size, match)
		if err != nil {
			return stepScan{}, err
		}
		if stopped {
			return scan, nil
		}
	}
	scan.done = true
	return scan, nil
}

// scanOne walks one range from lower and reports whether the window ended the step.
func scanOne(snap storage.KVSnapshot, span keyRange, lower []byte, window sweepWindow, scan *stepScan, size *int64, match func(key []byte) (bool, error)) (bool, error) {
	it := snap.NewIterator(storage.IterOptions{Lower: lower, Upper: span.upper})
	defer it.Close()
	for it.Seek(lower); it.Valid(); it.Next() {
		key := it.Key()
		cost := int64(len(key) + batchEntryOverhead)
		if scan.visited >= window.keys || (len(scan.doomed) > 0 && *size+cost > window.bytes) {
			return true, nil
		}
		scan.visited++
		scan.last = append(scan.last[:0], key...)
		doomed, err := match(key)
		if err != nil {
			return false, err
		}
		if doomed {
			*size += cost
			scan.doomed = append(scan.doomed, append([]byte(nil), key...))
		}
	}
	if err := it.Err(); err != nil {
		return false, fmt.Errorf("walking keys from %x: %w", lower, err)
	}
	return false, nil
}
