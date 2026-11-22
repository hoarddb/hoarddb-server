package store

import "testing"

// minAllocs returns the smallest testing.AllocsPerRun(runs, f) over up to
// windows measurements. AllocsPerRun counts every allocation in the process, so
// a goroutine from another test can only add to a measurement, and the least
// measurement is the function's own cost. It stops at the first zero, which no
// later measurement can beat. The server package keeps its copy beside the depth
// check; the two packages cannot share a test helper without a new package.
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
