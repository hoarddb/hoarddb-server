package server

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

// TestInterningManyDistinctDeclarationsIsLinear: interning is amortised O(1)
// per declaration, so interning ten times as many distinct declarations takes
// about ten times as long. A sweep of the whole table on every miss made it
// quadratic, a hundred times as long: 40,000 took 16.7 s, a million about
// three hours, all of it inside the start. The test compares two sizes of one
// run instead of holding it to a wall-clock budget, so a loaded host or the
// race detector slows both alike and cannot fail it.
func TestInterningManyDistinctDeclarationsIsLinear(t *testing.T) {
	const (
		small    = 10_000
		large    = 100_000
		maxRatio = 35.0 // linear is 10, quadratic is 100; 15 failed once at 15.1 on a loaded host
		// earlyMax checks the run at four times the small size, where linear is
		// 4 and quadratic 16.
		earlyMax = 10.0
	)
	s := newTestServer(t)
	held := make([]*bucketFieldTypes, 0, large)
	started := time.Now()
	var smallElapsed time.Duration
	for i := range large {
		holder, err := s.fieldTypesHolder([]catalogField{{Name: fmt.Sprintf("F%d", i), catalogType: catalogType{Type: "int"}}}, "")
		if err != nil {
			t.Fatalf("interning declaration %d: %v", i, err)
		}
		held = append(held, holder)
		if i+1 == small {
			smallElapsed = time.Since(started)
		}
		// A quadratic break is stopped here rather than waited out for
		// minutes: at four times the small size a linear run is at ratio 4
		// and a quadratic one at 16.
		if i+1 == 4*small {
			if ratio := float64(time.Since(started)) / float64(smallElapsed); ratio > earlyMax {
				t.Fatalf("interning %d distinct declarations took %.1fx as long as %d: interning is not amortised O(1)",
					4*small, ratio, small)
			}
		}
	}
	largeElapsed := time.Since(started)
	t.Logf("%d distinct declarations interned in %v, %d in %v", small, smallElapsed, large, largeElapsed)
	if ratio := float64(largeElapsed) / float64(smallElapsed); ratio > maxRatio {
		t.Errorf("interning %d distinct declarations took %.1fx as long as %d, over %.0fx: interning is not amortised O(1)",
			large, ratio, small, maxRatio)
	}
	if got := s.schemas.size(); got != large {
		t.Errorf("the table holds %d schemas for %d live distinct declarations", got, large)
	}
	runtime.KeepAlive(held)
}
