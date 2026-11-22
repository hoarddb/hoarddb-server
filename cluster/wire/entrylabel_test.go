package wire

import (
	"fmt"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// TestARingEntryCarriesNoLabelAndTheReaderStillNamesTheBucket (storage-format slice A, F6):
// an owner's entry and a copy's entry carry an empty bucket label, and EntryBucketLabel
// derives "<dbID>.<bucketID>" from the key's database id and the body's bucket id.
func TestARingEntryCarriesNoLabelAndTheReaderStillNamesTheBucket(t *testing.T) {
	fx := newFixture(t)
	fx.mustWrite(testSlot, "p1")
	owned := fx.logged()[0]
	if owned.Bucket != "" {
		t.Fatalf("the owner's entry carries the label %q; a ring entry carries none", owned.Bucket)
	}
	if label, err := EntryBucketLabel(owned); err != nil || label != fmt.Sprintf("%d.%d", testDB, testBucket) {
		t.Fatalf("EntryBucketLabel(owner entry) = %q, %v; want %d.%d", label, err, testDB, testBucket)
	}

	copyNode := newU32Node(t, "copy", nil)
	if res := copyNode.write(u32Frame(t, u32Epoch, u32Pos(7, 1), cluster.Position{})); res.Ack.Outcome != AckApplied {
		t.Fatalf("the copy's apply: ack %+v", res.Ack)
	}
	entries, err := copyNode.wal.ReadFrom(1)
	if err != nil || len(entries) != 1 {
		t.Fatalf("the copy's log holds %d entries, %v", len(entries), err)
	}
	if entries[0].Bucket != "" {
		t.Fatalf("the copy's entry carries the label %q; a ring entry carries none", entries[0].Bucket)
	}
	if label, err := EntryBucketLabel(entries[0]); err != nil || label != fmt.Sprintf("%d.%d", u32DB, u32Bucket) {
		t.Fatalf("EntryBucketLabel(copy entry) = %q, %v; want %d.%d", label, err, u32DB, u32Bucket)
	}
}
