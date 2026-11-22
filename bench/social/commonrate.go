package main

import (
	"errors"
	"fmt"
	"sort"
)

// commonRateResult is one offered rate for every engine, with how hard it
// pushes each of them.
type commonRateResult struct {
	// OfferedRate is operations per second, the same for every engine.
	OfferedRate float64 `json:"offered_rate"`
	// Utilisation is OfferedRate over the engine's own closed-loop median.
	Utilisation map[string]float64 `json:"utilisation"`
	// SlowestEngine is the engine whose median fixed the rate.
	SlowestEngine string `json:"slowest_engine"`
}

// commonRate fixes the one absolute rate all engines are offered in open-loop
// legs: fraction of the slowest engine's closed-loop median. Offering each
// engine a fraction of its own capacity would make their queueing
// incomparable (spec 6.6, F-02), so the fast engines run lightly loaded on
// purpose and the utilisation column says by how much.
func commonRate(closedMedians map[string]float64, fraction float64) (commonRateResult, error) {
	if len(closedMedians) == 0 {
		return commonRateResult{}, errors.New("no closed-loop medians to fix a common rate from")
	}
	if fraction <= 0 || fraction > 1 {
		return commonRateResult{}, fmt.Errorf("fraction %v of the slowest engine must be above 0 and at most 1", fraction)
	}
	engines := make([]string, 0, len(closedMedians))
	for engineName := range closedMedians {
		engines = append(engines, engineName)
	}
	sort.Strings(engines) // map order is random; a tie must always name the same engine
	slowest := engines[0]
	for _, engineName := range engines {
		median := closedMedians[engineName]
		if median <= 0 {
			return commonRateResult{}, fmt.Errorf("engine %q has a closed-loop median of %v ops/s; a rate cannot be fixed from it", engineName, median)
		}
		if median < closedMedians[slowest] {
			slowest = engineName
		}
	}
	offered := fraction * closedMedians[slowest]
	utilisation := make(map[string]float64, len(closedMedians))
	for engineName, median := range closedMedians {
		utilisation[engineName] = offered / median
	}
	return commonRateResult{OfferedRate: offered, Utilisation: utilisation, SlowestEngine: slowest}, nil
}
