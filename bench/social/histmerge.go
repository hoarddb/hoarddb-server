package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// mergedHistRecord is the line -mode merge-hist prints: the percentiles of the
// union of the samples in Files, which is what a two-client leg's latency is.
// A percentile is n.a. under minSamplesForPercentile samples, as in a leg record.
type mergedHistRecord struct {
	Mode  string   `json:"mode"`
	Files []string `json:"files"`
	latencySummary
}

// mergeHistFiles decodes every file and adds their samples into one histogram.
// Each client VM of a leg writes its own histogram; the quantiles of the sum
// are not the quantiles of any one of them, nor an average of them.
func mergeHistFiles(files []string) (*histogram, error) {
	merged := newHistogram()
	for _, name := range files {
		encoded, err := os.ReadFile(name)
		if err != nil {
			return nil, fmt.Errorf("reading histogram %s: %w", name, err)
		}
		part, err := decodeHistogram(encoded)
		if err != nil {
			return nil, fmt.Errorf("histogram %s: %w", name, err)
		}
		if dropped := merged.recorded.Merge(part.recorded); dropped != 0 {
			return nil, fmt.Errorf("histogram %s: %d samples fall outside the %d us to %d us range of the merged histogram; it was not written by this tool", name, dropped, histMinMicros, histMaxMicros)
		}
	}
	return merged, nil
}

// runMergeHist prints the merged percentiles of files as one JSON line.
func runMergeHist(files []string, out io.Writer) error {
	merged, err := mergeHistFiles(files)
	if err != nil {
		return err
	}
	record := mergedHistRecord{Mode: modeMergeHist, Files: files, latencySummary: merged.summary()}
	line, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encoding the merged histogram record: %w", err)
	}
	if _, err := fmt.Fprintln(out, string(line)); err != nil {
		return fmt.Errorf("writing the merged histogram record: %w", err)
	}
	return nil
}
