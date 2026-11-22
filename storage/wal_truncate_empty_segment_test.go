package storage

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

// TestTruncateKeepsEntriesAboveTheBoundWhenTheLastSegmentIsEmpty holds BUG-0180:
// an empty last segment (a crash between rotation and the first write, or a
// failed write rolled back to the new segment's start) must not make the
// segment before it look entirely at or below the bound. On a sealed log the
// empty segment is a header and nothing else.
func TestTruncateKeepsEntriesAboveTheBoundWhenTheLastSegmentIsEmpty(t *testing.T) {
	for _, tc := range []struct {
		name string
		keys WALKeySource
	}{
		{name: "plain", keys: nil},
		{name: "sealed", keys: newSealedTestKeySource(t)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Segments 1-3, 4-6, 7-9, then an empty one.
			w := openSegmentedWAL(t, tc.keys, []int{3, 3, 3, 0})
			defer func() { _ = w.Close() }()

			if err := w.Truncate(7); err != nil {
				t.Fatalf("Truncate(7): %v", err)
			}
			assertRetained(t, w, 7, 9)
		})
	}
}

// TestTruncateWithoutAnEmptySegmentIsUnchanged pins the rule BUG-0180's fix must
// not move: a segment goes only when its successor starts at or below the
// bound, so the segment holding the bound is always kept.
func TestTruncateWithoutAnEmptySegmentIsUnchanged(t *testing.T) {
	for _, tc := range []struct {
		bound       uint64
		first       uint64
		segmentsOut int
	}{
		{bound: 3, first: 1, segmentsOut: 3},
		{bound: 5, first: 4, segmentsOut: 2},
		{bound: 7, first: 7, segmentsOut: 1},
		{bound: 9, first: 7, segmentsOut: 1},
	} {
		t.Run(fmt.Sprintf("bound %d", tc.bound), func(t *testing.T) {
			w := openSegmentedWAL(t, nil, []int{3, 3, 3})
			defer func() { _ = w.Close() }()

			if err := w.Truncate(tc.bound); err != nil {
				t.Fatalf("Truncate(%d): %v", tc.bound, err)
			}
			if got := len(w.listSegments()); got != tc.segmentsOut {
				t.Errorf("after Truncate(%d): %d segments remain, want %d", tc.bound, got, tc.segmentsOut)
			}
			assertRetained(t, w, tc.first, 9)
		})
	}
}

// TestTruncateTreatsAMiddleEmptySegmentAsStartingWhereItsSuccessorDoes covers
// an empty segment with a non-empty one after it (an entry larger than the
// segment limit, written right after a crash or a rolled-back write, rotates
// away from the empty segment). It must go with everything behind it once the
// bound reaches its successor, and before that it must not make the segment in
// front of it look prunable.
func TestTruncateTreatsAMiddleEmptySegmentAsStartingWhereItsSuccessorDoes(t *testing.T) {
	for _, tc := range []struct {
		name string
		keys WALKeySource
	}{
		{name: "plain", keys: nil},
		{name: "sealed", keys: newSealedTestKeySource(t)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("bound inside the segment before it", func(t *testing.T) {
				// Segments 1-3, 4-6, empty, 7-9.
				w := openSegmentedWAL(t, tc.keys, []int{3, 3, 0, 3})
				defer func() { _ = w.Close() }()

				if err := w.Truncate(5); err != nil {
					t.Fatalf("Truncate(5): %v", err)
				}
				assertRetained(t, w, 4, 9)
			})
			t.Run("bound at its successor", func(t *testing.T) {
				w := openSegmentedWAL(t, tc.keys, []int{3, 3, 0, 3})
				defer func() { _ = w.Close() }()

				if err := w.Truncate(7); err != nil {
					t.Fatalf("Truncate(7): %v", err)
				}
				if got := len(w.listSegments()); got != 1 {
					t.Errorf("after Truncate(7): %d segments remain, want 1", got)
				}
				assertRetained(t, w, 7, 9)
			})
		})
	}
}

// TestTruncateCountsAHeaderOnlyOrZeroFilledV2SegmentAsEmpty: a v2 segment that
// holds its header only, or its header and then 64 MiB of zeros (a preallocated
// segment nothing was written to), is an empty segment to Truncate, in the
// middle of the log and at its end, in both key modes. Break by: reading the
// zero first word as an entry, or bounding the active segment by its file size.
func TestTruncateCountsAHeaderOnlyOrZeroFilledV2SegmentAsEmpty(t *testing.T) {
	for _, mode := range walKeyModes {
		for _, zeroFilled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/zero filled=%v", mode.name, zeroFilled), func(t *testing.T) {
				keys := mode.keys(t)
				emptyLike := func(t *testing.T, w *WAL, index int) {
					t.Helper()
					if !zeroFilled {
						return
					}
					if err := os.Truncate(segmentFilePath(w.dir, index), walFileHeaderLen+defaultMaxFileSize); err != nil {
						t.Fatalf("filling segment %d with zeros: %v", index, err)
					}
				}

				t.Run("in the middle", func(t *testing.T) {
					w := openSegmentedWAL(t, keys, []int{3, 3, 0, 3})
					defer func() { _ = w.Close() }()
					emptyLike(t, w, w.listSegments()[2].index)
					if err := w.Truncate(5); err != nil {
						t.Fatalf("Truncate(5): %v", err)
					}
					assertRetained(t, w, 4, 9)
					if err := w.Truncate(7); err != nil {
						t.Fatalf("Truncate(7): %v", err)
					}
					if got := len(w.listSegments()); got != 1 {
						t.Errorf("after Truncate(7): %d segments remain, want 1", got)
					}
					assertRetained(t, w, 7, 9)
				})
				t.Run("at the end", func(t *testing.T) {
					w := openSegmentedWAL(t, keys, []int{3, 3, 3, 0})
					defer func() { _ = w.Close() }()
					emptyLike(t, w, w.activeIndex)
					if err := w.Truncate(7); err != nil {
						t.Fatalf("Truncate(7): %v", err)
					}
					assertRetained(t, w, 7, 9)
				})
			})
		}
	}
}

// openSegmentedWAL builds a log whose segments hold the given numbers of
// entries, rotating by hand between them so a zero leaves an empty segment
// (header-only when sealed) exactly where a crash or a rolled-back write would.
func openSegmentedWAL(t *testing.T, keys WALKeySource, entriesPerSegment []int) *WAL {
	t.Helper()

	w, err := OpenWAL(t.TempDir(), keys)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	value := bytes.Repeat([]byte("x"), 64)
	appended := 0
	for segment, count := range entriesPerSegment {
		if segment > 0 {
			w.mu.Lock()
			err = w.rotate()
			w.mu.Unlock()
			if err != nil {
				t.Fatalf("rotate into segment %d: %v", segment+1, err)
			}
		}
		for i := 0; i < count; i++ {
			appended++
			if _, err := w.Append("b", OpInsert, []byte(fmt.Sprintf("k%d", appended)), value); err != nil {
				t.Fatalf("append %d: %v", appended, err)
			}
		}
	}
	if got := len(w.listSegments()); got != len(entriesPerSegment) {
		t.Fatalf("built %d segments, want %d", got, len(entriesPerSegment))
	}
	return w
}

// assertRetained checks that the log's horizon is first and that every entry
// from first through last is still readable, in order.
func assertRetained(t *testing.T, w *WAL, first, last uint64) {
	t.Helper()

	horizon, err := w.FirstSequence()
	if err != nil {
		t.Fatalf("FirstSequence: %v", err)
	}
	if horizon != first {
		t.Errorf("FirstSequence = %d, want %d", horizon, first)
	}
	entries, err := w.ReadFrom(first)
	if err != nil {
		t.Fatalf("ReadFrom(%d): %v", first, err)
	}
	if want := int(last - first + 1); len(entries) != want {
		t.Fatalf("ReadFrom(%d) returned %d entries, want %d (sequences %d..%d)", first, len(entries), want, first, last)
	}
	for i, entry := range entries {
		if want := first + uint64(i); entry.Seq != want {
			t.Errorf("entry %d has sequence %d, want %d", i, entry.Seq, want)
		}
	}
}
