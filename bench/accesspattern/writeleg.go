package main

import (
	"fmt"
	"time"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// writeDocs writes n documents into a hash bucket, optionally maintaining a
// secondary index on the field the data is ordered by, and returns how many
// document keys and index keys the bucket now holds.
//
// This is the write leg's whole content, and the reason it exists separately
// from the read legs: an index is paid on EVERY write, while a key encoding is
// maintained by the write that had to happen anyway. No read-side optimisation
// closes that, which is why the access-pattern claim is only half-proven by the
// read legs.
//
// Both sides write the same number of the same documents through the same
// store. The only difference is whether a range index on the ordering field is
// maintained, so the difference measured IS the index's write cost — not a
// simulation of one.
func writeDocs(kv storage.KV, n int, indexed bool) (keys int, err error) {
	var idxMgr *store.IndexManager
	if indexed {
		fields := []store.IndexKeyPart{{Field: "priority"}}
		specs := []store.IndexSpec{{Name: store.DefaultIndexName(fields), Fields: fields, Version: store.IndexSpecVersion}}
		var mgrErr error
		idxMgr, mgrErr = store.NewIndexManager(kv, slot, databaseID, id, specs)
		if mgrErr != nil {
			return 0, fmt.Errorf("opening the index manager: %w", mgrErr)
		}
	}
	h := store.NewHashStore(kv, slot, databaseID, id, new(store.OwedApplies))
	if idxMgr != nil {
		h.SetIndexManager(idxMgr)
	}

	info := store.ApplyInfo{}
	batch := make([]map[string]interface{}, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if _, putErr := h.PutBatch(info, store.PutBatchRequest{Items: batch}); putErr != nil {
			return fmt.Errorf("putting a batch of %d: %w", len(batch), putErr)
		}
		batch = batch[:0]
		return nil
	}
	for i := 0; i < n; i++ {
		batch = append(batch, doc{id: fmt.Sprintf("w%09d", i), priority: int64(i)}.bson())
		if len(batch) == batchSize {
			if flushErr := flush(); flushErr != nil {
				return 0, flushErr
			}
		}
	}
	if flushErr := flush(); flushErr != nil {
		return 0, flushErr
	}

	// Count the keys each side actually stored, by walking the bucket's two
	// ranges. The index entries live in the index namespace and the documents
	// in the document one, so this is the index's real cost in keys, read back
	// from the store rather than assumed.
	return countBucketKeys(kv, indexed)
}

// countBucketKeys reports the document keys and, when the bucket carries an
// index, the index keys in it.
func countBucketKeys(kv storage.KV, indexed bool) (int, error) {
	snap := kv.NewSnapshot()
	defer snap.Discard()
	total := 0
	prefixes := [][]byte{storage.Key(slot, databaseID, id, storage.NSDocuments)}
	if indexed {
		prefixes = append(prefixes, storage.Key(slot, databaseID, id, storage.NSIndex))
	}
	for _, prefix := range prefixes {
		it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: prefixEnd(prefix)})
		for it.Seek(nil); it.Valid(); it.Next() {
			total++
		}
		walkErr := it.Err()
		it.Close()
		if walkErr != nil {
			return 0, fmt.Errorf("counting the keys under %x: %w", prefix, walkErr)
		}
	}
	return total, nil
}

// writeLeg is the one leg where both sides write instead of answering a
// question, so it is measured as a pair of write costs rather than a pair of
// answers.
//
// The layout side writes the same documents with no index. The indexed side
// writes them with a range index on the ordering field — which is precisely
// what a document engine maintains so that its own ORDER BY becomes a seek. The
// ratio is therefore the cost of hoardDB NOT having to maintain a second
// structure to answer the question its key order already answers.
func writeLeg() leg {
	return leg{
		name:     "write",
		writeLeg: true,
		fill: func(kv storage.KV, n int) error {
			// Both halves start from the same populated bucket, so the write
			// cost compared is the cost of a steady stream of writes into an
			// already-populated bucket, not a cold start.
			_, err := writeDocs(kv, n, false)
			return err
		},
		left: func(q *stores) (string, error) {
			return "", nil
		},
		right: func(q *stores) (string, error) {
			return "", nil
		},
		leftSecond: func(kv storage.KV, n int) error {
			_, err := writeDocs(kv, n, false)
			return err
		},
		rightSecond: func(kv storage.KV, n int) error {
			_, err := writeDocs(kv, n, true)
			return err
		},
	}
}

func (l leg) runWrite(root string, n int) (LegResult, error) {
	var out LegResult
	out.Leg = l.name

	for _, half := range []struct {
		label   string
		indexed bool
		second  func(kv storage.KV, n int) error
	}{
		{label: "layout", indexed: false, second: l.leftSecond},
		{label: "indexed", indexed: true, second: l.rightSecond},
	} {
		dir := root + "-" + half.label
		kv, err := storage.OpenBadgerKV(dir, benchTuning(), nil)
		if err != nil {
			return LegResult{}, fmt.Errorf("open %s: %w", dir, err)
		}
		if err := l.fill(kv, n); err != nil {
			_ = kv.Close()
			return LegResult{}, fmt.Errorf("fill for the %s half: %w", half.label, err)
		}
		if err := kv.Sync(); err != nil {
			_ = kv.Close()
			return LegResult{}, fmt.Errorf("sync after the %s fill: %w", half.label, err)
		}
		start := time.Now()
		runErr := half.second(kv, n)
		dur := time.Since(start)
		// The keys each side's bucket holds afterwards, read back from the
		// store: the layout side stores the document, the indexed side stores
		// the document AND the index entry that makes its own ORDER BY a seek.
		keys, countErr := countBucketKeys(kv, half.indexed)
		closeErr := kv.Close()
		if runErr != nil {
			return LegResult{}, fmt.Errorf("the %s half: %w", half.label, runErr)
		}
		if countErr != nil {
			return LegResult{}, fmt.Errorf("counting the %s half's keys: %w", half.label, countErr)
		}
		if closeErr != nil {
			return LegResult{}, fmt.Errorf("closing the %s half: %w", half.label, closeErr)
		}
		if half.label == "layout" {
			out.Native = Sample{Duration: dur, Touched: keys}
		} else {
			out.ScanSort = Sample{Duration: dur, Touched: keys}
		}
	}
	if out.Native.Duration > 0 {
		out.Ratio = float64(out.ScanSort.Duration) / float64(out.Native.Duration)
	}
	return out, nil
}

// prefixEnd is the exclusive upper bound of a prefix, mirroring
// store/index.go:prefixEnd: the last byte that can be incremented, truncated at
// it. A prefix of all 0xFF has no upper bound, which is why it returns nil.
func prefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}
