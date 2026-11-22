package bsonoracle

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
)

// V1Record is one value the v1.0 server wrote to Badger, captured by the
// bsoncorpus tap (storage/corpus_tap.go) and classified by its key prefix.
// DepthLimit is the deepest nesting the server accepts at the record's site
// (server/nesting_depth.go:recordDepthLimit); for a dead entry it is the
// largest, the heap's, because the key does not name the store type.
type V1Record struct {
	Class      string `json:"class"`
	DepthLimit int    `json:"depthLimit"`
	KeyPrefix  string `json:"keyPrefix"`
	Value      []byte `json:"value"`
}

// v1CorpusDir is testdata/v1corpus next to this file's source. Tests run in
// their own package directory, so a relative path would break for a test of
// another package that loads the corpus.
func v1CorpusDir() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "testdata", "v1corpus")
}

// V1Corpus loads the committed first v1.0-written corpus, in the order it was
// written (by SHA-256 of the value).
func V1Corpus() (records []V1Record, err error) {
	corpusPath := filepath.Join(v1CorpusDir(), "records.jsonl.gz")
	opened, err := os.Open(corpusPath)
	if err != nil {
		return nil, fmt.Errorf("opening the v1.0 corpus %s: %w", corpusPath, err)
	}
	defer func() {
		if closeErr := opened.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("closing %s: %w", corpusPath, closeErr)
		}
	}()
	decompressed, err := gzip.NewReader(bufio.NewReader(opened))
	if err != nil {
		return nil, fmt.Errorf("reading the gzip header of %s: %w", corpusPath, err)
	}
	decoder := json.NewDecoder(decompressed)
	for {
		var record V1Record
		if err := decoder.Decode(&record); errors.Is(err, io.EOF) {
			return records, nil
		} else if err != nil {
			return nil, fmt.Errorf("decoding record %d of %s: %w", len(records), corpusPath, err)
		}
		records = append(records, record)
	}
}
