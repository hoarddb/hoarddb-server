//go:build !bsoncorpus

package storage

// corpusTapEnabled is a constant so that `if corpusTapEnabled` at a write site
// compiles to nothing: a normal build carries no state, goroutine or
// descriptor for the corpus tap (bson-codec-build.md, step 1d).
const corpusTapEnabled = false

// recordCorpusValue is the tap's no-op. Only the bsoncorpus build records.
func recordCorpusValue(key, value []byte) {}
