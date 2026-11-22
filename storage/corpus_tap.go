//go:build bsoncorpus

package storage

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// The BSON corpus tap (bson-codec-spec.md §9.1.6, bson-codec-build.md step
// 1d): under the bsoncorpus build tag, every value that frames as one BSON
// document and is written to Badger is appended to
// $BSONCORPUS_OUT/records-<pid>.jsonl, so the first corpus holds what v1.0's
// own codec wrote. The variable is deliberately not HOARDDB_-prefixed: it is a
// build-time test tool, not a product setting. The tap never decodes a value.

// corpusTapEnabled is a constant so that `if corpusTapEnabled` at a write site
// reads the same in both builds; here it is true.
const corpusTapEnabled = true

// corpusKeyPrefixBytes is how much of a key a record keeps: the slot, bucket
// id and namespace (11 bytes), plus the byte after the namespace, which names
// the record kind under NSDelivery and NSReplication. Later bytes are user
// keys and say nothing about the site.
const corpusKeyPrefixBytes = 12

// corpusRecord is one line of the tap's output. Value is base64 in JSON.
type corpusRecord struct {
	KeyPrefix string `json:"keyPrefix"`
	Value     []byte `json:"value"`
}

// corpusTap serialises writers to the one output file of this process. The
// file is opened again if BSONCORPUS_OUT changes, which only tests do.
var corpusTap struct {
	mutex sync.Mutex
	path  string
	file  *os.File
}

// framesAsDocument reports whether value is laid out as one BSON document:
// at least 5 bytes, a little-endian length prefix equal to len(value), and a
// zero last byte.
func framesAsDocument(value []byte) bool {
	return len(value) >= 5 &&
		int64(binary.LittleEndian.Uint32(value)) == int64(len(value)) &&
		value[len(value)-1] == 0
}

// recordCorpusValue appends value to the tap's file if it frames as a BSON
// document. A failure is reported on stderr and the write it observed goes
// on: the tap must not change what the database does.
func recordCorpusValue(key, value []byte) {
	if !framesAsDocument(value) {
		return
	}
	outputDir := os.Getenv("BSONCORPUS_OUT")
	if outputDir == "" {
		return
	}
	prefixLength := min(len(key), corpusKeyPrefixBytes)
	line, err := json.Marshal(corpusRecord{KeyPrefix: hex.EncodeToString(key[:prefixLength]), Value: value})
	if err != nil {
		fmt.Fprintf(os.Stderr, "bsoncorpus tap: encoding a record: %v\n", err)
		return
	}
	line = append(line, '\n')

	corpusTap.mutex.Lock()
	defer corpusTap.mutex.Unlock()
	outputPath := filepath.Join(outputDir, fmt.Sprintf("records-%d.jsonl", os.Getpid()))
	if corpusTap.file == nil || corpusTap.path != outputPath {
		if corpusTap.file != nil {
			if err := corpusTap.file.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "bsoncorpus tap: closing %s: %v\n", corpusTap.path, err)
			}
			corpusTap.file = nil
		}
		opened, err := os.OpenFile(outputPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "bsoncorpus tap: opening %s: %v\n", outputPath, err)
			return
		}
		corpusTap.file, corpusTap.path = opened, outputPath
	}
	// One write per record: O_APPEND keeps lines whole, and nothing is
	// buffered, so a killed test server loses no record.
	if _, err := corpusTap.file.Write(line); err != nil {
		fmt.Fprintf(os.Stderr, "bsoncorpus tap: writing %s: %v\n", outputPath, err)
	}
}
