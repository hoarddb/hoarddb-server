package server

import (
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// The rows the build doc's step 2a names (spec 9.2). Each compared the
// library's Validate (lib=hoarddb) with the server's depth scan (lib=vendor);
// build step 9 removed the scan, and its rows with it, so only the library's
// rows remain, under the names benchgate knows.

// benchmarkReference450 is a flat document of exactly 450 bytes with several
// value types, the size the library's own Validate benchmarks use.
func benchmarkReference450(tb testing.TB) []byte {
	tb.Helper()
	fixedElements := [][]byte{
		elemOf(0x10, "age", 41, 0, 0, 0),
		elemOf(0x08, "active", 1),
		elemOf(0x01, "score", make([]byte, 8)...),
		elemOf(0x07, "owner", make([]byte, 12)...),
		elemOf(0x03, "address", docOf(elemOf(0x02, "city", bsonString("Springfield")...))...),
	}
	// Notes pads the document to 450 bytes: type, "notes\0", length, text, NUL.
	paddingLength := 450 - len(docOf(fixedElements...)) - (1 + len("notes") + 1 + 4 + 1)
	reference := docOf(append(fixedElements, elemOf(0x02, "notes", bsonString(strings.Repeat("n", paddingLength))...))...)
	if len(reference) != 450 {
		tb.Fatalf("the reference document is %d bytes, want 450", len(reference))
	}
	return reference
}

func BenchmarkValidateAgainstDepthScan(b *testing.B) {
	inputs := []struct {
		name  string
		bytes []byte
	}{
		{"size=ref450", benchmarkReference450(b)},
		{"size=depth100", nestedChain(100, "doc")},
	}
	for _, input := range inputs {
		b.Run(input.name+"/lib=hoarddb", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := bson.Validate(input.bytes, bson.Limits{Depth: maxDocumentDepth}); err != nil {
					b.Fatalf("Validate refused the benchmark document: %v", err)
				}
			}
		})
	}
}

// BenchmarkTypedPathPair is Validate then Document.Decode into an ordered
// document (spec 9.2), the typed path a boundary takes. Its lib=vendor row,
// the depth scan then a vendor decode, went with the scan (build step 9).
func BenchmarkTypedPathPair(b *testing.B) {
	reference := benchmarkReference450(b)
	b.Run("lib=hoarddb", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			document, err := bson.Validate(reference, bson.Limits{Depth: maxDocumentDepth})
			if err != nil {
				b.Fatalf("Validate refused the reference document: %v", err)
			}
			var decoded bson.D
			if err := document.Decode(&decoded); err != nil {
				b.Fatalf("decoding the reference document: %v", err)
			}
		}
	})
}
