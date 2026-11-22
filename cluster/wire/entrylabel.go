package wire

import (
	"fmt"
	"strconv"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
)

// EntryBucketLabel is the operator-facing "<dbID>.<bucketID>" of one ring log entry, derived
// at read time from the entry's position key (the database id) and its record body (the
// bucket id). A ring entry carries no label of its own (storage-format spec §1.1: the label
// was never parsed, and the two ids are already in the bytes), so an inspector that prints
// an entry calls this rather than reading entry.Bucket, which is empty.
func EntryBucketLabel(entry storage.WALEntry) (string, error) {
	_, db, _, err := storage.DecodePositionKey(entry.Key)
	if err != nil {
		return "", fmt.Errorf("labelling log sequence %d: %w", entry.Seq, err)
	}
	record, err := storage.DecodeWALRecord(entry.Op, entry.Data)
	if err != nil {
		return "", fmt.Errorf("labelling log sequence %d: %w", entry.Seq, err)
	}
	op, err := cluster.DecodeOp(record.Body)
	if err != nil {
		return "", fmt.Errorf("labelling log sequence %d: %w", entry.Seq, err)
	}
	return strconv.FormatUint(db, 10) + "." + strconv.FormatUint(op.BucketID, 10), nil
}
