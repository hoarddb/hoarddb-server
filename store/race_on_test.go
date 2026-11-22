//go:build race

package store

// raceEnabled lets a long seeded model step aside under the race detector,
// whose instrumentation makes millions of steps cost minutes. The model is
// single-goroutine, so the detector has nothing to find in it.
const raceEnabled = true
