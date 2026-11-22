package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"time"
)

// opSeries is one worker's per-second operation counts (BD-2). Index 0 is the
// first warm-up second, so second s lives at index s+warmSeconds. Each worker
// owns its own series, and the slices are merged after the workers return, so
// nothing here takes a lock.
type opSeries struct {
	warmSeconds, measuredSeconds int
	reads, writes                []int64
}

func newOpSeries(warmup, duration time.Duration) *opSeries {
	warm, measured := wholeSeconds(warmup), wholeSeconds(duration)
	return &opSeries{
		warmSeconds: warm, measuredSeconds: measured,
		reads: make([]int64, warm+measured), writes: make([]int64, warm+measured),
	}
}

// wholeSeconds rounds a duration up to whole seconds: the lines it needs.
func wholeSeconds(length time.Duration) int { return int(math.Ceil(length.Seconds())) }

// add counts one completed operation. A counted operation (one the leg record
// counts) goes to the second it completed in, capped at the last measured
// second; a warm-up operation goes to its completion second, capped at -1, so
// the measured seconds sum exactly to the record's counts.
func (s *opSeries) add(read, counted bool, completed, measureFrom time.Time) {
	if !counted && s.warmSeconds == 0 {
		return // no warm-up lines to hold it
	}
	second := int(math.Floor(completed.Sub(measureFrom).Seconds()))
	if counted {
		second = min(max(second, 0), s.measuredSeconds-1)
	} else {
		second = min(max(second, -s.warmSeconds), -1)
	}
	if read {
		s.reads[second+s.warmSeconds]++
	} else {
		s.writes[second+s.warmSeconds]++
	}
}

// seriesLine is one line of the series file.
type seriesLine struct {
	Second int   `json:"s"`
	Reads  int64 `json:"reads"`
	Writes int64 `json:"writes"`
	// LenS is the length of the last measured second when it is partial.
	LenS *float64 `json:"len_s,omitempty"`
}

// perClientLine closes a series file: each load client's measured operation
// counts, one entry per client, so a starved client shows up as a 0 that the
// merged lines hide (BD-3).
type perClientLine struct {
	PerClient struct {
		Reads  []int64 `json:"reads"`
		Writes []int64 `json:"writes"`
	} `json:"per_client"`
}

// measuredCounts is one worker's measured reads and writes: the seconds from 0
// on, which hold exactly the operations the leg record counts.
func (s *opSeries) measuredCounts() (reads, writes int64) {
	for index := s.warmSeconds; index < len(s.reads); index++ {
		reads += s.reads[index]
		writes += s.writes[index]
	}
	return reads, writes
}

// writeSeries merges the workers' series and writes one line per second from the
// first warm-up second to the last measured one. A leg that ended early has
// fewer measured seconds; operations that completed after the measured time
// fold into its last line.
func writeSeries(path string, results []*mixResult, measured time.Duration) error {
	var merged *opSeries
	var clients perClientLine
	clients.PerClient.Reads, clients.PerClient.Writes = []int64{}, []int64{}
	for _, result := range results {
		if result.series == nil {
			continue
		}
		reads, writes := result.series.measuredCounts()
		clients.PerClient.Reads = append(clients.PerClient.Reads, reads)
		clients.PerClient.Writes = append(clients.PerClient.Writes, writes)
		if merged == nil {
			merged = newOpSeries(time.Duration(result.series.warmSeconds)*time.Second, time.Duration(result.series.measuredSeconds)*time.Second)
		}
		for index := range merged.reads {
			merged.reads[index] += result.series.reads[index]
			merged.writes[index] += result.series.writes[index]
		}
	}
	if merged == nil {
		return nil
	}
	lastMeasured := min(max(wholeSeconds(measured), 1), merged.measuredSeconds) - 1
	var out bytes.Buffer
	for second := -merged.warmSeconds; second <= lastMeasured; second++ {
		line := seriesLine{Second: second}
		first, last := second+merged.warmSeconds, second+merged.warmSeconds
		if second == lastMeasured {
			last = len(merged.reads) - 1
		}
		for index := first; index <= last; index++ {
			line.Reads += merged.reads[index]
			line.Writes += merged.writes[index]
		}
		if whole := math.Floor(measured.Seconds()); second == lastMeasured && measured.Seconds() > whole {
			partial := measured.Seconds() - whole
			line.LenS = &partial
		}
		encoded, err := json.Marshal(line)
		if err != nil {
			return fmt.Errorf("encoding series second %d: %w", second, err)
		}
		out.Write(append(encoded, '\n'))
	}
	encoded, err := json.Marshal(clients)
	if err != nil {
		return fmt.Errorf("encoding the per-client counts: %w", err)
	}
	out.Write(append(encoded, '\n'))
	if err := os.WriteFile(path, out.Bytes(), 0o644); err != nil {
		return fmt.Errorf("writing the series to %s: %w", path, err)
	}
	return nil
}
