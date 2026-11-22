package main

import (
	"fmt"
	"slices"
	"strings"
)

// mixShare is one operation's percentage of a mix.
type mixShare struct {
	operation string
	percent   int
}

// operationIDs lists every operation of spec 6.3, which -mix op:<ID> accepts.
// M1 and Q1 are legs of their own and appear in no mix table.
var operationIDs = []string{"R1", "R2", "R3", "R4", "R5", "R6", "R7", "N1", "W1", "W2", "W3", "W4", "W5", "W6", "M1", "Q1"}

// mixTables are the exact percentages of spec 6.5: RH is 90% reads, WH 50%,
// SK (soak) 70%. N1 consumes, so it is on the write side for durability.
var mixTables = map[string][]mixShare{
	"rh": {{"R1", 40}, {"R2", 10}, {"R3", 15}, {"R4", 15}, {"R5", 5}, {"R6", 3}, {"R7", 2}, {"N1", 2}, {"W1", 2}, {"W2", 4}, {"W3", 1}, {"W4", 0}, {"W5", 1}, {"W6", 0}},
	"wh": {{"R1", 25}, {"R2", 5}, {"R3", 8}, {"R4", 8}, {"R5", 2}, {"R6", 1}, {"R7", 1}, {"N1", 5}, {"W1", 15}, {"W2", 15}, {"W3", 4}, {"W4", 1}, {"W5", 8}, {"W6", 2}},
	"sk": {{"R1", 35}, {"R2", 8}, {"R3", 12}, {"R4", 12}, {"R5", 3}, {"R6", 2}, {"R7", 1}, {"N1", 9}, {"W1", 1}, {"W2", 12}, {"W3", 2}, {"W4", 2}, {"W5", 1}, {"W6", 0}},
}

// mixTable returns the shares of a named mix (rh, wh or sk).
func mixTable(name string) ([]mixShare, error) {
	table, ok := mixTables[name]
	if !ok {
		return nil, fmt.Errorf("mix %q is not one of rh, wh, sk or op:<ID>", name)
	}
	return table, nil
}

// mixScheduler hands out operations in the proportions of a mix. It is a
// smooth weighted round-robin, a low-discrepancy sequence: because the shares
// are whole percentages that sum to 100, every window of 100 draws holds each
// operation exactly its share, which is how the 0.1-point bound of spec 6.5
// holds for any run length, not only on average. It is not safe for concurrent
// use; each worker owns one.
type mixScheduler struct {
	shares []mixShare
	credit []int
	total  int
	pinned string // set for -mix op:<ID>
}

// newMixScheduler parses a -mix value: rh, wh, sk or op:<ID>. The seed only
// chooses where in the 100-draw cycle the scheduler starts, so workers seeded
// differently do not issue the same operation at the same instant.
func newMixScheduler(mix string, seed uint64) (*mixScheduler, error) {
	if operation, isOp := strings.CutPrefix(mix, "op:"); isOp {
		if !slices.Contains(operationIDs, operation) {
			return nil, fmt.Errorf("-mix op:%s names no operation; use one of %s", operation, strings.Join(operationIDs, ", "))
		}
		return &mixScheduler{pinned: operation}, nil
	}
	table, err := mixTable(mix)
	if err != nil {
		return nil, err
	}
	scheduler := &mixScheduler{credit: make([]int, 0, len(table))}
	for _, share := range table {
		if share.percent > 0 {
			scheduler.shares = append(scheduler.shares, share)
			scheduler.credit = append(scheduler.credit, 0)
			scheduler.total += share.percent
		}
	}
	for range seed % uint64(scheduler.total) {
		scheduler.next()
	}
	return scheduler, nil
}

// next returns the operation ID of the next draw.
func (s *mixScheduler) next() string {
	if s.pinned != "" {
		return s.pinned
	}
	best := 0
	for i, share := range s.shares {
		s.credit[i] += share.percent
		if s.credit[i] > s.credit[best] {
			best = i
		}
	}
	s.credit[best] -= s.total
	return s.shares[best].operation
}
