package main

import (
	"fmt"
	"strconv"
	"strings"
)

// parsePoints reads a comma-separated list of record counts. Counts must be
// positive and strictly increasing: a sweep that goes back or repeats a size
// measures nothing new and hides a typo.
func parsePoints(list string) ([]int64, error) {
	var points []int64
	for _, field := range strings.Split(list, ",") {
		field = strings.TrimSpace(field)
		size, err := strconv.ParseInt(field, 10, 64)
		if err != nil || size <= 0 {
			return nil, fmt.Errorf("point %q is not a positive whole number; write sizes like 10000,30000,100000", field)
		}
		if n := len(points); n > 0 && size <= points[n-1] {
			return nil, fmt.Errorf("point %d does not exceed the one before it (%d); list sizes in strictly increasing order", size, points[n-1])
		}
		points = append(points, size)
	}
	return points, nil
}

// loadChunks splits growing from have to target records into ranges of at most
// chunk keys, so the caller can check free disk between ranges.
func loadChunks(have, target, chunk int64) [][2]int64 {
	var ranges [][2]int64
	for from := have; from < target; from += chunk {
		ranges = append(ranges, [2]int64{from, min(from+chunk, target)})
	}
	return ranges
}
