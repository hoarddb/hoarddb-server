package server

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentCreatesGetDistinctIDs is spec T-27(d). Creates of different
// names run concurrently; every third create batch is held before it commits,
// so an allocator that released its mutex before the commit would let the
// batches land out of id order and the persisted high-water mark go backwards.
// After a restart the next id must still be above every id handed out.
func TestConcurrentCreatesGetDistinctIDs(t *testing.T) {
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)

	var commits atomic.Int64
	hook := func(uint64) {
		if commits.Add(1)%3 == 0 {
			time.Sleep(20 * time.Millisecond)
		}
	}
	allocatorCommitHook.Store(&hook)
	t.Cleanup(func() { allocatorCommitHook.Store(nil) })

	const creators = 64
	ids := make([]uint64, creators)
	var wg sync.WaitGroup
	for i := 0; i < creators; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("bucket-%02d", i)
			resp, err := first.core.CreateBucket(CreateBucketRequest{Database: "app", Name: name, Type: BucketTypeHash})
			if err != nil || !resp.Success {
				t.Errorf("CreateBucket(%s) = %+v, %v", name, resp, err)
				return
			}
			ids[i] = bucketIDOf(t, first.core, "app", name)
		}(i)
	}
	wg.Wait()
	allocatorCommitHook.Store(nil)

	highest := uint64(0)
	owner := make(map[uint64]int, creators)
	for i, id := range ids {
		if id == 0 {
			t.Fatalf("bucket-%02d holds no id", i)
		}
		if other, taken := owner[id]; taken {
			t.Fatalf("bucket-%02d and bucket-%02d were both given id %d", other, i, id)
		}
		owner[id] = i
		if id > highest {
			highest = id
		}
	}

	second := restartCatalog(t, first, dataDir)
	next := mustCreateBucket(t, second.core, "app", "after-restart")
	if next <= highest {
		t.Fatalf("after a restart the next create got id %d, not above the highest id handed out, %d: the allocator's persisted mark went backwards", next, highest)
	}
}
