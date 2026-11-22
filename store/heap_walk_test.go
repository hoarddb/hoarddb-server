package store

import (
	"fmt"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// TestHeapStoreWalkIsPriorityOrderedAndReadOnly covers the walk a scan needs:
// highest priority first, and neither Len() nor PopItem() is disturbed by
// walking — a scan must not consume the heap it reads.
func TestHeapStoreWalkIsPriorityOrderedAndReadOnly(t *testing.T) {
	heap, _ := openTestHeap(t)

	for _, priority := range []int64{1, 5, 3} {
		element := map[string]interface{}{"_id_": fmt.Sprintf("p%d", priority), "priority": priority}
		if _, err := heap.PushItem(ApplyInfo{}, priority, element); err != nil {
			t.Fatalf("PushItem(%d): %v", priority, err)
		}
	}

	var ids []string
	if err := heap.Walk(func(value []byte) error {
		var doc map[string]interface{}
		if err := bson.Unmarshal(value, &doc); err != nil {
			return fmt.Errorf("decoding heap entry: %w", err)
		}
		id, _ := doc["_id_"].(string)
		ids = append(ids, id)
		return nil
	}); err != nil {
		t.Fatalf("Walk: %v", err)
	}

	want := []string{"p5", "p3", "p1"}
	if len(ids) != len(want) {
		t.Fatalf("Walk returned %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("Walk returned %v, want %v", ids, want)
		}
	}

	if heapLen(t, heap) != 3 {
		t.Fatalf("Len after walk = %d, want 3: the walk consumed entries", heapLen(t, heap))
	}
	value, err := heap.PopItem(ApplyInfo{})
	if err != nil {
		t.Fatalf("PopItem after walk: %v", err)
	}
	if value == nil {
		t.Fatal("PopItem after walk returned nothing")
	}
	var popped map[string]interface{}
	if err := bson.Unmarshal(value, &popped); err != nil {
		t.Fatalf("decoding popped entry: %v", err)
	}
	if id, _ := popped["_id_"].(string); id != "p5" {
		t.Fatalf("PopItem after walk = %q, want p5", id)
	}
}
