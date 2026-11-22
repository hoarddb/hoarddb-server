package store

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

func TestQueueTakePositionRemovesOneElementAndLeavesTheCursorsAlone(t *testing.T) {
	kv := fdOpen(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	ref := fdRef(t, FieldFIFO)
	fdPush(t, kv, ref, "a", "b", "c")
	var taken []byte
	var found bool
	fdCommit(t, kv, func(snap storage.KVSnapshot, batch storage.KVBatch) error {
		var parent BucketState
		var err error
		taken, found, err = QueueTakePosition(FieldWrite{Snap: snap, State: &parent, Batch: batch}, ref, 1)
		return err
	})
	if !found || fdText(t, taken) != "b" {
		t.Fatalf("take of position 1 = %q found %v, want b", taken, found)
	}
	fdCommit(t, kv, func(snap storage.KVSnapshot, batch storage.KVBatch) error {
		var parent BucketState
		_, found, err := QueueTakePosition(FieldWrite{Snap: snap, State: &parent, Batch: batch}, ref, 1)
		if found {
			t.Fatal("a second take of the same position found an element")
		}
		return err
	})
	var popped []string
	fdCommit(t, kv, func(snap storage.KVSnapshot, batch storage.KVBatch) error {
		var parent BucketState
		_, err := FieldPopN(FieldWrite{Snap: snap, State: &parent, Batch: batch}, ref, 5, func(entry FieldEntry) error {
			popped = append(popped, fdText(t, entry.Value))
			return nil
		})
		return err
	})
	if len(popped) != 2 || popped[0] != "a" || popped[1] != "c" {
		t.Fatalf("pop after the take returned %v, want [a c]", popped)
	}
}

func TestFieldHasLeasesSeesAClaimAndNotAnAck(t *testing.T) {
	kv := fdOpen(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	ref := fdRef(t, FieldFIFO)
	fdPush(t, kv, ref, "a")
	has := func() bool {
		snap := kv.NewSnapshot()
		defer snap.Discard()
		got, err := FieldHasLeases(snap, ref)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	if has() {
		t.Fatal("a field never claimed has leases")
	}
	claim, _ := fdClaim(t, kv, ref, fdArgs(1, 1_000, 1))
	if !has() {
		t.Fatal("a claimed field has no leases")
	}
	fdAck(t, kv, ref, claim.LeaseID, 2_000)
	if has() {
		t.Fatal("an acked field still has leases")
	}
}

func TestAnAckByItemIdReportsTheElementsItRemoved(t *testing.T) {
	kv := fdOpen(t, t.TempDir())
	t.Cleanup(func() { _ = kv.Close() })
	ref := fdRef(t, FieldFIFO)
	fdPush(t, kv, ref, "a", "b")
	claim, _ := fdClaim(t, kv, ref, fdArgs(2, 1_000, 1))
	var positions []uint64
	var texts []string
	fdCommit(t, kv, func(snap storage.KVSnapshot, batch storage.KVBatch) error {
		queue, err := newFieldQueue(snap, batch, ref)
		if err != nil {
			return err
		}
		args := LeaseArgs{LeaseID: claim.LeaseID, Now: 2_000, Acked: func(position uint64, element []byte) {
			positions = append(positions, position)
			texts = append(texts, fdText(t, element))
		}}
		_, err = ack(queue, ApplyInfo{}, args, [][]byte{PositionRemainder(1)})
		return err
	})
	if len(positions) != 1 || positions[0] != 1 || texts[0] != "b" {
		t.Fatalf("ack by item id reported positions %v texts %v, want position 1 text b", positions, texts)
	}
}
