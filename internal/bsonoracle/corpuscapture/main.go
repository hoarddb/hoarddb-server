// Command corpuscapture turns the raw records the bsoncorpus storage tap wrote
// (storage/corpus_tap.go) into the committed v1.0 corpus,
// internal/bsonoracle/testdata/v1corpus/records.jsonl.gz: it classifies each
// value by its key prefix, removes repeats by SHA-256 and keeps at most a
// fixed number of records and bytes. capture.sh, beside the corpus, runs it.
//
//	go run ./internal/bsonoracle/corpuscapture -in dir1,dir2 -out records.jsonl.gz
//
// The statistics go to standard output as JSON. A test tool, not product code.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "corpuscapture:", err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	flags := flag.NewFlagSet("corpuscapture", flag.ContinueOnError)
	inputDirs := flags.String("in", "", "comma-separated directories holding the tap's records-*.jsonl files")
	outputPath := flags.String("out", "records.jsonl.gz", "the corpus file to write")
	maxRecords := flags.Int("max-records", 50000, "most records kept after deduplication")
	maxGzipBytes := flags.Int("max-gzip-bytes", 8<<20, "most bytes of the gzip file")
	maxValueBytes := flags.Int("max-value-bytes", 256<<10, "largest value kept; larger ones are counted and left out")
	if err := flags.Parse(arguments); err != nil {
		return fmt.Errorf("reading flags: %w", err)
	}
	if *inputDirs == "" {
		return fmt.Errorf("-in is required: name the directories the tap wrote to ($BSONCORPUS_OUT)")
	}
	compressed, stats, err := build(buildOptions{
		inputDirs:     strings.Split(*inputDirs, ","),
		maxRecords:    *maxRecords,
		maxGzipBytes:  *maxGzipBytes,
		maxValueBytes: *maxValueBytes,
	})
	if err != nil {
		return err
	}
	if err := os.WriteFile(*outputPath, compressed, 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", *outputPath, err)
	}
	encoded, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding the statistics: %w", err)
	}
	fmt.Println(string(encoded))
	return nil
}
