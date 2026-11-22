package server

import (
	"encoding/binary"
	"errors"
	"runtime"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"
)

// allocNoise receives the background goroutine's garbage so the compiler cannot
// drop the allocations it makes.
var allocNoise []byte

// noiseBudget bounds the background allocations: enough to land inside one
// AllocsPerRun window, few enough that later windows run clean, as a fixed
// check must allow.
const noiseBudget = 1_000_000

// TestAllocAssertionSurvivesBackgroundNoise runs the allocation checks while
// another goroutine allocates. testing.AllocsPerRun counts every allocation in
// the process, so the noise must not make the checks fail.
//
// AllocsPerRun pins GOMAXPROCS to 1, so a goroutine that never blocks gets the
// processor only when the measured function yields it: the check yields first,
// which is how a real background goroutine lands in a window.
func TestAllocAssertionSurvivesBackgroundNoise(t *testing.T) {
	for range 5 {
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range noiseBudget {
				select {
				case <-stop:
					return
				default:
					allocNoise = make([]byte, 64)
				}
			}
			<-stop
		}()
		yielding := func(raw []byte, limit int) error {
			runtime.Gosched()
			return validateAtDepth(raw, limit)
		}
		holdIterative(t, yielding)
		close(stop)
		wg.Wait()
	}
}

// The depth check (BUG-0526) is the one reading of a client's bytes made before
// anything decodes them, so it must not be recursive itself, must stop at the
// bound however deep the input goes, and must refuse bytes it cannot follow
// without trusting a length it has not checked. Every boundary validates
// through validateAt (build step 7b), so these tests hold it.

// validateAtDepth is validateAt for a test that needs only its refusal.
func validateAtDepth(raw []byte, limit int) error {
	_, err := validateAt(raw, limit)
	return err
}

// TestDepthCheckIsIterative holds validateAt to the corpus's depth vectors
// (100 accepted, 101 and 1000 refused, documents and arrays), shows it stops at
// level 101 by corrupting the 1000-level input below that level, and shows its
// memory does not grow with the input's depth.
func TestDepthCheckIsIterative(t *testing.T) {
	holdIterative(t, validateAtDepth)
}

func holdIterative(t *testing.T, checkDepth func(raw []byte, limit int) error) {
	t.Helper()
	for _, arrays := range []bool{false, true} {
		if err := checkDepth(nestedDocumentBytes(100, arrays), maxDocumentDepth); err != nil {
			t.Errorf("arrays=%t: depth 100 refused: %v", arrays, err)
		}
		for _, depth := range []int{101, 1000} {
			if err := checkDepth(nestedDocumentBytes(depth, arrays), maxDocumentDepth); !errors.Is(err, ErrDocumentTooDeep) {
				t.Errorf("arrays=%t: depth %d: %v, want ErrDocumentTooDeep", arrays, depth, err)
			}
		}

		// Every type byte from level 200 down is made unknown, with the lengths
		// left consistent: a scan that read past level 101 would call the input
		// malformed instead of too deep.
		corrupted := nestedDocumentBytes(1000, arrays)
		elementHeader := 1 + 2 + 4 // type byte, a one-letter key and its NUL, the next level's length
		for level := 200; level < 1000; level++ {
			corrupted[4+(level-1)*elementHeader] = 0x20
		}
		if err := checkDepth(corrupted, maxDocumentDepth); !errors.Is(err, ErrDocumentTooDeep) {
			t.Errorf("arrays=%t: the 1000-level input corrupted below level 200: %v, want ErrDocumentTooDeep (the scan reads no deeper than 101)", arrays, err)
		}
	}

	// An accepted document is the common case, scanned on every request: it
	// allocates nothing. A refusal allocates its error, the same for any depth.
	atBound := nestedDocumentBytes(100, false)
	if accepted := minAllocs(7, 20, func() { _ = checkDepth(atBound, maxDocumentDepth) }); accepted != 0 {
		t.Errorf("scanning an accepted 100-level document allocates %v times, want 0", accepted)
	}
	deepInput, shallowInput := nestedDocumentBytes(1000, false), nestedDocumentBytes(101, false)
	shallowScan := minAllocs(7, 20, func() { _ = checkDepth(shallowInput, maxDocumentDepth) })
	deepScan := minAllocs(7, 20, func() { _ = checkDepth(deepInput, maxDocumentDepth) })
	if deepScan > shallowScan {
		t.Errorf("refusing 1000 levels allocates %v times and 101 levels %v: the scan's memory grows with the input's depth", deepScan, shallowScan)
	}
}

// minAllocs returns the smallest testing.AllocsPerRun(runs, f) over up to
// windows measurements. AllocsPerRun counts every allocation in the process, so
// another goroutine can only add to a measurement, and the least measurement is
// the function's own cost. It stops at the first zero, which no later
// measurement can beat.
func minAllocs(windows, runs int, f func()) float64 {
	least := testing.AllocsPerRun(runs, f)
	for range windows - 1 {
		if least == 0 {
			break
		}
		least = min(least, testing.AllocsPerRun(runs, f))
	}
	return least
}

// TestDepthCheckRefusesBytesItCannotFollow: a length that does not fit, a
// truncated document, a type with no defined size and an old binary whose two
// lengths disagree are refused as malformed, never trusted, never a panic, and
// never reported as a depth refusal.
func TestDepthCheckRefusesBytesItCannotFollow(t *testing.T) {
	holdMalformedRefused(t, validateAtDepth)
}

func holdMalformedRefused(t *testing.T, checkDepth func(raw []byte, limit int) error) {
	t.Helper()
	nested := nestedDocumentBytes(50, false)
	withLength := func(raw []byte, offset int, length int32) []byte {
		changed := append([]byte(nil), raw...)
		binary.LittleEndian.PutUint32(changed[offset:], uint32(length))
		return changed
	}
	// The length of the embedded document at level 11: the root's length, nine
	// whole element headers, then the tenth's type byte and key.
	innerLength := 4 + 9*7 + 3
	truncated := append([]byte(nil), nested[:len(nested)-3]...)
	binary.LittleEndian.PutUint32(truncated, uint32(len(truncated)))
	oldBinary := func(outer, inner int32) []byte {
		value := binary.LittleEndian.AppendUint32(nil, uint32(outer))
		value = append(value, 0x02)
		value = binary.LittleEndian.AppendUint32(value, uint32(inner))
		value = append(value, make([]byte, outer-4)...)
		return bsoncore.BuildDocument(nil, append(append([]byte{0x05}, "b\x00"...), value...))
	}

	for _, malformed := range []struct {
		name string
		raw  []byte
	}{
		{"empty", nil},
		{"shorter than a header", []byte{5, 0, 0}},
		{"root length negative", withLength(nested, 0, -1)},
		{"root length beyond the bytes", withLength(nested, 0, int32(len(nested)+10))},
		{"root length short of the bytes", withLength(nested, 0, int32(len(nested)-1))},
		{"inner length negative", withLength(nested, innerLength, -7)},
		{"inner length beyond the bytes", withLength(nested, innerLength, 1<<30)},
		{"inner length short of its elements", withLength(nested, innerLength, 5)},
		{"truncated, length adjusted", truncated},
		{"unknown type", bsoncore.BuildDocument(nil, []byte{0x20, 'a', 0, 1})},
		{"key unterminated", []byte{8, 0, 0, 0, 0x10, 'a', 'b', 0}},
		{"string length negative", bsoncore.BuildDocument(nil, append([]byte{0x02, 'a', 0}, 0xff, 0xff, 0xff, 0xff))},
		{"old binary whose lengths disagree", oldBinary(8, 0)},
	} {
		err := checkDepth(malformed.raw, maxDocumentDepth)
		if err == nil {
			t.Errorf("%s: accepted", malformed.name)
			continue
		}
		if errors.Is(err, ErrDocumentTooDeep) {
			t.Errorf("%s: refused as too deep: %v", malformed.name, err)
		}
	}

	if err := checkDepth(oldBinary(8, 4), maxDocumentDepth); err != nil {
		t.Errorf("an old binary whose lengths agree was refused: %v", err)
	}
}
