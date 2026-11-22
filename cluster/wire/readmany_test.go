package wire

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
)

// readManyRefs returns n distinct refs of near, with at set to their index.
func readManyRefs(t *testing.T, near bucketRef, n int) []expandRef {
	t.Helper()
	refs := make([]expandRef, n)
	for i := range refs {
		encID, err := store.EncodeComponent(fmt.Sprintf("r%d", i))
		if err != nil {
			t.Fatal(err)
		}
		refs[i] = expandRef{ref: near, encID: encID, slot: near.slot(encID), at: i}
	}
	return refs
}

// TestReadManyReturnsAResultPerKeyAndNeverCancelsSiblings: [GUARD one key's error is that key's
// result; the other keys, on its owner and on others, are still read and answered].
func TestReadManyReturnsAResultPerKeyAndNeverCancelsSiblings(t *testing.T) {
	world := newExpandWorld(t, u70AllowAll{})
	refs := readManyRefs(t, world.near, 40)
	failing := string(refs[3].encID)
	down := &cluster.Error{Code: cluster.CodeOwnerDown, Message: "the owner is down"}
	var cancelled atomic.Int32
	results := readMany(context.Background(), refs, readManyPlan{
		ownerOf: func(_ bucketRef, slot uint16) string { return fmt.Sprintf("node-%d", slot%3) },
		fetch: func(ctx context.Context, _ bucketRef, encID []byte, _ []uint16, _ bool) (Document, error) {
			if string(encID) == failing {
				return Document{}, down
			}
			select {
			case <-time.After(10 * time.Millisecond):
				return Document{Body: []byte{5, 0, 0, 0, 0}, EncID: encID, Served: servedOwner}, nil
			case <-ctx.Done():
				cancelled.Add(1)
				return Document{}, ctx.Err()
			}
		},
	})
	if len(results) != len(refs) {
		t.Fatalf("%d results for %d reads", len(results), len(refs))
	}
	for i, result := range results {
		if !result.read {
			t.Errorf("read %d was never launched", i)
			continue
		}
		if i == 3 {
			if !errors.Is(result.err, down) {
				t.Errorf("read 3 answered %v, want its own owner-down error", result.err)
			}
			continue
		}
		if result.err != nil || string(result.doc.EncID) != string(refs[i].encID) {
			t.Errorf("read %d answered %q, %v: a sibling's error reached it", i, result.doc.EncID, result.err)
		}
	}
	if cancelled.Load() != 0 {
		t.Fatalf("%d reads saw their context cancelled because a sibling failed", cancelled.Load())
	}
}

// TestReadManyBoundsReadsInFlightPerOwnerAndPerFrame: [GUARD at most 16 reads in flight per owner,
// and with a frame bound at most that many in the whole call].
func TestReadManyBoundsReadsInFlightPerOwnerAndPerFrame(t *testing.T) {
	world := newExpandWorld(t, u70AllowAll{})
	refs := readManyRefs(t, world.near, 200)
	for _, frame := range []int{0, ringBatchInFlight} {
		var mu sync.Mutex
		perOwner, peakOwner, total, peakTotal := map[string]int{}, 0, 0, 0
		ownerOf := func(_ bucketRef, slot uint16) string { return fmt.Sprintf("node-%d", slot%4) }
		readMany(context.Background(), refs, readManyPlan{
			ownerOf:       ownerOf,
			frameInFlight: frame,
			fetch: func(_ context.Context, ref bucketRef, encID []byte, _ []uint16, _ bool) (Document, error) {
				owner := ownerOf(ref, ref.slot(encID))
				mu.Lock()
				perOwner[owner]++
				total++
				peakOwner, peakTotal = max(peakOwner, perOwner[owner]), max(peakTotal, total)
				mu.Unlock()
				time.Sleep(2 * time.Millisecond)
				mu.Lock()
				perOwner[owner]--
				total--
				mu.Unlock()
				return Document{}, nil
			},
		})
		if peakOwner > ringBatchInFlight {
			t.Errorf("frame bound %d: %d reads in flight on one owner, want at most %d", frame, peakOwner, ringBatchInFlight)
		}
		if frame > 0 && peakTotal > frame {
			t.Errorf("frame bound %d: %d reads in flight in the call", frame, peakTotal)
		}
		if frame == 0 && peakTotal <= ringBatchInFlight {
			t.Errorf("no frame bound: only %d reads in flight over 4 owners; the owners were not read in parallel", peakTotal)
		}
	}
}

// TestReadManyStopsLaunchingWhenToldTo: [GUARD a read proceed refuses is not launched, nor any
// after it; its result says it was not read].
func TestReadManyStopsLaunchingWhenToldTo(t *testing.T) {
	world := newExpandWorld(t, u70AllowAll{})
	refs := readManyRefs(t, world.near, 50)
	var launched, settled atomic.Int32
	results := readMany(context.Background(), refs, readManyPlan{
		ownerOf:       func(bucketRef, uint16) string { return "only" },
		frameInFlight: 1,
		proceed:       func() bool { return settled.Load() < 10 },
		fetch: func(context.Context, bucketRef, []byte, []uint16, bool) (Document, error) {
			launched.Add(1)
			return Document{}, nil
		},
		settle: func(_ int, result manyResult) manyResult {
			settled.Add(1)
			return result
		},
	})
	if launched.Load() != 10 {
		t.Fatalf("%d reads launched, want 10: proceed said stop after 10 settled", launched.Load())
	}
	for i, result := range results {
		if result.read != (i < 10) {
			t.Errorf("read %d: read=%v", i, result.read)
		}
	}
}
