package main

import "time"

// The record types are what -out writes. The raw per-sample record is not
// committed (bench/RESULTS.md carries the published numbers and the
// parameters); this exists so a run's full detail survives for whoever re-runs
// it, and so a leg that failed carries its reason rather than vanishing.

// Record is one full run.
type Record struct {
	N          int         `json:"n"`
	Legs       string      `json:"legs"`
	Repeats    int         `json:"repeats"`
	Host       HostFacts   `json:"host"`
	Recorded   string      `json:"recorded"`
	MemTableMB int         `json:"memtable_mb"`
	Results    []LegResult `json:"results"`
}

// HostFacts is the machine the numbers came from. A benchmark without it is an
// anecdote: the same run on a laptop and on this host are not comparable.
type HostFacts struct {
	MemTotalMB int `json:"mem_total_mb"`
	Cores      int `json:"cores"`
}

// LegResult is one leg in one run. Native and ScanSort keep their field names
// because they are the record's original names, but a write leg's two samples
// are a layout cost and an indexed cost — the leg carries the labels, so a
// reader is never left guessing which is which.
type LegResult struct {
	Leg      string `json:"leg"`
	Repeat   int    `json:"repeat"`
	Native   Sample `json:"native"`
	ScanSort Sample `json:"scan_sort"`
	// Ratio is ScanSort over Native: above 1 means the query-time answer cost
	// more than the native one, which is the direction the claim points.
	Ratio float64 `json:"ratio"`
	Error string  `json:"error,omitempty"`
}

// Sample is one side of one leg in one run.
type Sample struct {
	Duration time.Duration `json:"duration_ns"`
	// Touched is how many elements this side had to read, or how many keys it
	// wrote on the write leg. It is the claim's own content and the only
	// counter that moves on a memory-mapped store: /proc/self/io reports
	// neither reads nor writes made through a mapped file.
	Touched int `json:"touched"`
	// Answer is the element both sides found, as a digest. It is recorded so a
	// later reader can confirm the two sides really were asked the same
	// question, and a leg that returned nothing has an empty one.
	Answer string `json:"answer,omitempty"`
}
