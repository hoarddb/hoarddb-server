//go:build race

package server

// raceEnabled lets a timing assertion step aside under the race detector,
// whose instrumentation costs more than the budgets it would be measuring.
const raceEnabled = true
