//go:build !race

package store

// raceEnabled is false without the race detector; see race_on_test.go.
const raceEnabled = false
