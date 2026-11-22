package main

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// shapeDepth is the nesting depth of a document, counted the way the server
// counts it: the root is level 1, and each embedded document or array adds 1.
func shapeDepth(document bson.Raw) int {
	deepest := 1
	elements, err := document.Elements()
	if err != nil {
		return 0
	}
	for _, element := range elements {
		value := element.Value()
		switch value.Type {
		case bson.TypeEmbeddedDocument:
			deepest = max(deepest, 1+shapeDepth(value.Document()))
		case bson.TypeArray:
			deepest = max(deepest, 1+shapeDepth(value.Array()))
		}
	}
	return deepest
}

func marshalAll(t *testing.T, documents []bson.D) []bson.Raw {
	t.Helper()
	encoded := make([]bson.Raw, 0, len(documents))
	for _, document := range documents {
		documentBytes, err := bson.Marshal(document)
		if err != nil {
			t.Fatalf("encoding a generated document: %v", err)
		}
		encoded = append(encoded, documentBytes)
	}
	return encoded
}

func TestEveryShapeMatchesItsDeclaredFieldCountDepthAndSize(t *testing.T) {
	if len(shapes) != 12 {
		t.Fatalf("%d shapes, want 12", len(shapes))
	}
	for _, oneShape := range shapes {
		t.Run(oneShape.name, func(t *testing.T) {
			documents := oneShape.generate(1, 7)
			if len(documents) != oneShape.docs {
				t.Fatalf("generated %d documents, want %d", len(documents), oneShape.docs)
			}
			reached := 0
			for position, document := range marshalAll(t, documents) {
				fieldCount := len(documents[position])
				if fieldCount != oneShape.topLevelFields() {
					t.Fatalf("document %d has %d top-level fields, want %d", position, fieldCount, oneShape.topLevelFields())
				}
				depth := shapeDepth(document)
				if depth > oneShape.depth() {
					t.Fatalf("document %d nests %d deep, want at most %d", position, depth, oneShape.depth())
				}
				reached = max(reached, depth)
				if size := len(document); size < oneShape.minBytes || size > oneShape.maxBytes {
					t.Fatalf("document %d is %d bytes, want %d to %d", position, size, oneShape.minBytes, oneShape.maxBytes)
				}
			}
			if reached != oneShape.depth() {
				t.Errorf("no document nests deeper than %d, want one that reaches %d", reached, oneShape.depth())
			}
		})
	}
}

func TestGenerationIsDeterministicAndScales(t *testing.T) {
	for _, oneShape := range shapes {
		first := marshalAll(t, oneShape.generate(1, 7))
		second := marshalAll(t, oneShape.generate(1, 7))
		for position := range first {
			if string(first[position]) != string(second[position]) {
				t.Fatalf("%s: document %d differs between two runs with one seed", oneShape.name, position)
			}
		}
		if scaled := oneShape.generate(3, 7); len(scaled) != 3*oneShape.docs {
			t.Fatalf("%s: scale 3 gave %d documents, want %d", oneShape.name, len(scaled), 3*oneShape.docs)
		}
		if other := marshalAll(t, oneShape.generate(1, 8)); string(other[0]) == string(first[0]) {
			t.Fatalf("%s: seeds 7 and 8 gave the same first document", oneShape.name)
		}
	}
}

// syntheticKey is every key name the generator writes: a type word and a
// number, or one of the structural names. Nothing is copied from a real
// dataset, so no key can identify a person or a property.
var syntheticKey = regexp.MustCompile(`^(_id|(oid|str|dt|flag|nul|i32|i64|dbl|ids|names|rows|blob|sub|row|nest)_[0-9]{2}_*)$`)

func collectKeysAndStrings(t *testing.T, document bson.Raw, keys map[string]bool, texts *[]string) {
	t.Helper()
	elements, err := document.Elements()
	if err != nil {
		t.Fatalf("reading elements: %v", err)
	}
	for _, element := range elements {
		keys[element.Key()] = true
		value := element.Value()
		switch value.Type {
		case bson.TypeString:
			*texts = append(*texts, value.StringValue())
		case bson.TypeEmbeddedDocument:
			collectKeysAndStrings(t, value.Document(), keys, texts)
		case bson.TypeArray:
			collectKeysAndStrings(t, value.Array(), keys, texts)
		}
	}
}

func TestGeneratedKeysAndStringsAreSynthetic(t *testing.T) {
	wordOnly := regexp.MustCompile(`^[a-z ]*$`)
	for _, oneShape := range shapes {
		keys := map[string]bool{}
		var texts []string
		for _, document := range marshalAll(t, oneShape.generate(1, 7)) {
			collectKeysAndStrings(t, document, keys, &texts)
		}
		for key := range keys {
			// Array positions are "0", "1", ...: not names.
			if !syntheticKey.MatchString(key) && !regexp.MustCompile(`^[0-9]+$`).MatchString(key) {
				t.Errorf("%s: key %q is not a synthetic name", oneShape.name, key)
			}
		}
		for _, text := range texts {
			if !wordOnly.MatchString(text) || !utf8.ValidString(text) {
				t.Errorf("%s: string %q is not lower-case letters and spaces", oneShape.name, text)
			}
		}
	}
}

func TestWriteBSONFilesWritesOneValidFilePerShape(t *testing.T) {
	outputDir := t.TempDir()
	summaries, err := writeBSONFiles(outputDir, 1, 7)
	if err != nil {
		t.Fatalf("writeBSONFiles: %v", err)
	}
	if len(summaries) != len(shapes) {
		t.Fatalf("%d summaries, want %d", len(summaries), len(shapes))
	}
	for _, summary := range summaries {
		fileBytes, err := os.ReadFile(filepath.Join(outputDir, summary.File))
		if err != nil {
			t.Fatalf("reading %s: %v", summary.File, err)
		}
		if len(fileBytes) != summary.Bytes {
			t.Errorf("%s: %d bytes on disk, summary says %d", summary.File, len(fileBytes), summary.Bytes)
		}
		documents := 0
		for rest := fileBytes; len(rest) > 0; documents++ {
			document, remaining, ok := nextDocument(rest)
			if !ok {
				t.Fatalf("%s: document %d is cut off", summary.File, documents)
			}
			if err := document.Validate(); err != nil {
				t.Fatalf("%s: document %d: %v", summary.File, documents, err)
			}
			rest = remaining
		}
		if documents != summary.Documents {
			t.Errorf("%s: %d documents on disk, summary says %d", summary.File, documents, summary.Documents)
		}
	}
}
