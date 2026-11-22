package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// fileSummary describes one written file. It is printed as JSON: counts and
// sizes only.
type fileSummary struct {
	Shape          string `json:"shape"`
	File           string `json:"file"`
	Documents      int    `json:"documents"`
	Bytes          int    `json:"bytes"`
	TopLevelFields int    `json:"topLevelFields"`
	Depth          int    `json:"depth"`
	LargestBytes   int    `json:"largestDocumentBytes"`
}

// writeBSONFiles writes one <shape>.bson file per shape into outputDir: the
// documents back to back, which is the format `hoarddb ingest` reads.
func writeBSONFiles(outputDir string, scale int, seed uint64) ([]fileSummary, error) {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating %s: %w", outputDir, err)
	}
	summaries := make([]fileSummary, 0, len(shapes))
	for _, oneShape := range shapes {
		var fileBytes []byte
		largest := 0
		documents := oneShape.generate(scale, seed)
		for _, document := range documents {
			encoded, err := bson.Marshal(document)
			if err != nil {
				return nil, fmt.Errorf("encoding a %s document: %w", oneShape.name, err)
			}
			fileBytes = append(fileBytes, encoded...)
			largest = max(largest, len(encoded))
		}
		fileName := oneShape.name + ".bson"
		if err := os.WriteFile(filepath.Join(outputDir, fileName), fileBytes, 0o644); err != nil {
			return nil, fmt.Errorf("writing %s: %w", fileName, err)
		}
		summaries = append(summaries, fileSummary{
			Shape: oneShape.name, File: fileName, Documents: len(documents), Bytes: len(fileBytes),
			TopLevelFields: oneShape.topLevelFields(), Depth: oneShape.depth(), LargestBytes: largest,
		})
	}
	return summaries, nil
}

// nextDocument splits the first BSON document off the front of fileBytes.
func nextDocument(fileBytes []byte) (document bson.Raw, rest []byte, ok bool) {
	if len(fileBytes) < 5 {
		return nil, nil, false
	}
	length := int(binary.LittleEndian.Uint32(fileBytes))
	if length < 5 || length > len(fileBytes) {
		return nil, nil, false
	}
	return fileBytes[:length], fileBytes[length:], true
}

func main() {
	outputDir := flag.String("out", "", "directory to write the <shape>.bson files into")
	scale := flag.Int("scale", 1, "multiply every shape's document count")
	seed := flag.Uint64("seed", 1, "seed of the generator; one seed gives the same files")
	flag.Parse()
	if *outputDir == "" {
		fmt.Fprintln(os.Stderr, "propertyshape: -out is required, for example: go run ./bench/propertyshape -out /tmp/propshape")
		os.Exit(2)
	}
	summaries, err := writeBSONFiles(*outputDir, *scale, *seed)
	if err != nil {
		fmt.Fprintln(os.Stderr, "propertyshape:", err)
		os.Exit(1)
	}
	encoded, err := json.MarshalIndent(summaries, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, "propertyshape: encoding the summary:", err)
		os.Exit(1)
	}
	fmt.Println(string(encoded))
}
