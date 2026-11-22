package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// rawRecord is one line the bsoncorpus tap wrote (storage/corpus_tap.go).
type rawRecord struct {
	KeyPrefix string `json:"keyPrefix"`
	Value     []byte `json:"value"`
}

// corpusRecord is one line of records.jsonl.gz. Value is base64 in JSON.
type corpusRecord struct {
	Class      string `json:"class"`
	DepthLimit int    `json:"depthLimit"`
	KeyPrefix  string `json:"keyPrefix"`
	Value      []byte `json:"value"`
}

type buildOptions struct {
	inputDirs     []string
	maxRecords    int
	maxGzipBytes  int
	maxValueBytes int
}

// buildStats is printed as JSON so SOURCE.md can quote it.
type buildStats struct {
	Read     int            `json:"read"`
	Foreign  int            `json:"excludedForeignKeys"`
	Oversize int            `json:"excludedOverMaxValueBytes"`
	Unique   int            `json:"uniqueBySHA256"`
	Kept     int            `json:"kept"`
	GzipSize int            `json:"gzipBytes"`
	PerClass map[string]int `json:"uniquePerClass"`
	KeptBy   map[string]int `json:"keptPerClass"`
}

type candidate struct {
	hash   [sha256.Size]byte
	record corpusRecord
}

// build reads every records-*.jsonl in the input directories (in name order,
// so the first class a value was seen under is the same on every run),
// removes foreign keys, oversize values and repeats by SHA-256, shares the
// record cap between classes, and returns the gzip of the kept records in
// hash order. The gzip header carries no time, so equal input gives equal
// bytes. If the gzip is over the byte cap, the record cap shrinks by a tenth
// until it fits.
func build(options buildOptions) ([]byte, buildStats, error) {
	stats := buildStats{PerClass: map[string]int{}, KeptBy: map[string]int{}}
	byClass := map[string][]candidate{}
	seen := map[[sha256.Size]byte]bool{}
	for _, inputDir := range options.inputDirs {
		recordFiles, err := filepath.Glob(filepath.Join(inputDir, "records-*.jsonl"))
		if err != nil {
			return nil, stats, fmt.Errorf("listing %s: %w", inputDir, err)
		}
		sort.Strings(recordFiles)
		for _, recordFile := range recordFiles {
			if err := readRecordFile(recordFile, options, &stats, seen, byClass); err != nil {
				return nil, stats, err
			}
		}
	}
	classSizes := map[string]int{}
	for class, candidates := range byClass {
		sort.Slice(candidates, func(left, right int) bool {
			return bytes.Compare(candidates[left].hash[:], candidates[right].hash[:]) < 0
		})
		classSizes[class] = len(candidates)
		stats.PerClass[class] = len(candidates)
		stats.Unique += len(candidates)
	}
	for recordCap := options.maxRecords; ; recordCap -= max(1, recordCap/10) {
		compressed, kept, err := encodeCorpus(byClass, shareOutCap(classSizes, recordCap))
		if err != nil {
			return nil, stats, err
		}
		if len(compressed) <= options.maxGzipBytes || recordCap <= 1 {
			stats.GzipSize = len(compressed)
			for class, count := range kept {
				stats.Kept += count
				stats.KeptBy[class] = count
			}
			return compressed, stats, nil
		}
	}
}

func readRecordFile(recordFile string, options buildOptions, stats *buildStats, seen map[[sha256.Size]byte]bool, byClass map[string][]candidate) (err error) {
	opened, err := os.Open(recordFile)
	if err != nil {
		return fmt.Errorf("opening %s: %w", recordFile, err)
	}
	defer func() {
		if closeErr := opened.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("closing %s: %w", recordFile, closeErr)
		}
	}()
	reader := bufio.NewReaderSize(opened, 1<<20)
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			var raw rawRecord
			if err := json.Unmarshal(line, &raw); err != nil {
				return fmt.Errorf("parsing a line of %s: %w", recordFile, err)
			}
			stats.Read++
			sorted := classifyRecord(raw, options, stats, seen)
			if sorted != nil {
				byClass[sorted.record.Class] = append(byClass[sorted.record.Class], *sorted)
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return fmt.Errorf("reading %s: %w", recordFile, readErr)
		}
	}
}

// classifyRecord returns the candidate for a raw record, or nil when the
// record is excluded or repeats one already seen.
func classifyRecord(raw rawRecord, options buildOptions, stats *buildStats, seen map[[sha256.Size]byte]bool) *candidate {
	recordSite, kept := classifyKeyPrefix(raw.KeyPrefix)
	if !kept {
		stats.Foreign++
		return nil
	}
	if len(raw.Value) > options.maxValueBytes {
		stats.Oversize++
		return nil
	}
	hash := sha256.Sum256(raw.Value)
	if seen[hash] {
		return nil
	}
	seen[hash] = true
	return &candidate{hash: hash, record: corpusRecord{Class: recordSite.class, DepthLimit: recordSite.depthLimit, KeyPrefix: raw.KeyPrefix, Value: raw.Value}}
}

// encodeCorpus writes the first shares[class] candidates of each class, in
// global hash order.
func encodeCorpus(byClass map[string][]candidate, shares map[string]int) ([]byte, map[string]int, error) {
	var selected []candidate
	kept := map[string]int{}
	for class, candidates := range byClass {
		selected = append(selected, candidates[:shares[class]]...)
		kept[class] = shares[class]
	}
	sort.Slice(selected, func(left, right int) bool {
		return bytes.Compare(selected[left].hash[:], selected[right].hash[:]) < 0
	})
	var compressed bytes.Buffer
	writer, err := gzip.NewWriterLevel(&compressed, gzip.BestCompression)
	if err != nil {
		return nil, nil, fmt.Errorf("starting the gzip writer: %w", err)
	}
	encoder := json.NewEncoder(writer)
	for _, entry := range selected {
		if err := encoder.Encode(entry.record); err != nil {
			return nil, nil, fmt.Errorf("encoding record %s: %w", hex.EncodeToString(entry.hash[:]), err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, nil, fmt.Errorf("finishing the gzip stream: %w", err)
	}
	return compressed.Bytes(), kept, nil
}
