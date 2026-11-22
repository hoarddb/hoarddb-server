package main

import (
	"fmt"
	"math"
	"sort"
)

// Entity access modes of -access.
const (
	accessUniform = "uniform"
	accessZipf    = "zipf"
)

// accessConfig says how a request picks its target entity among a layout's seeded ones.
type accessConfig struct {
	// Mode is accessUniform or accessZipf.
	Mode string
	// ZipfS is the exponent of the Zipf law, used in accessZipf only: rank k (counting from 1)
	// is drawn with probability proportional to 1/k^ZipfS.
	ZipfS float64
	// PageDepth is the share of paged reads on an ordered store that start at a random offset
	// inside the seeded elements instead of at the first one.
	PageDepth float64
}

// zipfTable draws a rank in [0, n) with probability proportional to 1/(rank+1)^s. The standard
// library's Zipf needs an exponent above 1, and 1.0 is the usual choice, so the table is built
// directly: cum[i] is the probability of a rank at or below i, and a draw is a binary search.
// It is read-only after construction, so every virtual user shares one per bucket.
type zipfTable struct{ cum []float64 }

// newZipfTable builds the table over n ranks with exponent s.
func newZipfTable(n int, s float64) (*zipfTable, error) {
	if n < 1 {
		return nil, fmt.Errorf("a Zipf table needs at least one entity, got %d", n)
	}
	if !(s > 0) || math.IsInf(s, 0) {
		return nil, fmt.Errorf("-zipf-s %v is not an exponent. Use a number above 0, 1.0 is the usual choice", s)
	}
	cum := make([]float64, n)
	var total float64
	for k := range n {
		total += math.Pow(float64(k+1), -s)
		cum[k] = total
	}
	for k := range cum {
		cum[k] /= total
	}
	cum[n-1] = 1
	return &zipfTable{cum: cum}, nil
}

// draw maps u in [0, 1) to a rank.
func (z *zipfTable) draw(u float64) int {
	return min(sort.SearchFloat64s(z.cum, u), len(z.cum)-1)
}
