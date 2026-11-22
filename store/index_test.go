package store

import (
	"go/ast"
	"go/parser"
	"go/token"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// fixedWidthComponentsForTest is one encoded key component per fixed-width
// tag componentLen resolves without scanning, with the length it
// must report for each.
func fixedWidthComponentsForTest(tb testing.TB) []struct {
	name    string
	encoded []byte
	length  int
} {
	tb.Helper()
	encode := func(v interface{}) []byte {
		encoded, err := EncodeComponent(v)
		if err != nil {
			tb.Fatalf("encoding %v as a key component: %v", v, err)
		}
		return encoded
	}
	return []struct {
		name    string
		encoded []byte
		length  int
	}{
		{name: "null", encoded: encode(nil), length: 1},
		{name: "bool", encoded: encode(true), length: 2},
		{name: "number", encoded: encode(int64(42)), length: 18},
		{name: "datetime", encoded: encodeDateTime(1_700_000_000_000), length: 9},
	}
}

// TestComponentLenBuildsNoMapPerCall is the failing-first test. The
// per-call map literal never reached the heap (escape analysis keeps it on
// the stack, so an allocation count cannot see it), but building it still
// hashed and inserted four entries on every call: runtime.mapassign was 79%
// of componentLen's profiled time. The cost is invisible to AllocsPerRun and
// a timing assertion would be flaky, so this checks the cause directly: no
// map composite literal may appear anywhere in componentLen's body.
func TestComponentLenBuildsNoMapPerCall(t *testing.T) {
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, "index.go", nil, 0)
	if err != nil {
		t.Fatalf("parsing store/index.go: %v", err)
	}
	var componentLenDecl *ast.FuncDecl
	for _, decl := range parsed.Decls {
		if funcDecl, ok := decl.(*ast.FuncDecl); ok && funcDecl.Name.Name == "componentLen" {
			componentLenDecl = funcDecl
		}
	}
	if componentLenDecl == nil {
		t.Fatal("store/index.go no longer declares componentLen; move this test with it")
	}
	ast.Inspect(componentLenDecl.Body, func(node ast.Node) bool {
		literal, ok := node.(*ast.CompositeLit)
		if !ok {
			return true
		}
		if _, isMap := literal.Type.(*ast.MapType); isMap {
			t.Errorf("componentLen builds a map literal at %s on every call; resolve the fixed-width tags with a switch",
				fileSet.Position(literal.Pos()))
		}
		return true
	})
}

// TestComponentLenDoesNotAllocateForFixedWidthTag guards the fix
// against a later heap regression: componentLen resolves a fixed-width tag's
// length with zero allocations and still returns the same length it always
// did. It passed before the fix too — the old map literal was stack-allocated
// — so TestComponentLenBuildsNoMapPerCall is the test that proves the fix.
func TestComponentLenDoesNotAllocateForFixedWidthTag(t *testing.T) {
	for _, component := range fixedWidthComponentsForTest(t) {
		t.Run(component.name, func(t *testing.T) {
			var length int
			allocs := minAllocs(7, 1000, func() {
				n, err := componentLen(component.encoded)
				if err != nil {
					t.Fatalf("componentLen: %v", err)
				}
				length = n
			})
			if length != component.length {
				t.Fatalf("componentLen = %d, want %d", length, component.length)
			}
			if allocs != 0 {
				t.Errorf("componentLen on a %s component allocated %.1f times per call, want 0", component.name, allocs)
			}
		})
	}
}

// TestComponentLenErrorsUnchanged pins componentLen's error cases, which
// the fix must not alter: empty input, a truncated fixed-width
// component, an unknown tag and an unterminated string.
func TestComponentLenErrorsUnchanged(t *testing.T) {
	cases := []struct {
		name    string
		encoded []byte
		wantErr string
	}{
		{name: "empty", encoded: nil, wantErr: "an empty key component"},
		{name: "truncated number", encoded: []byte{tagNumber, 0x01}, wantErr: "a key component of tag 0x30 is 2 bytes, want 18"},
		{name: "unknown tag", encoded: []byte{0x7F}, wantErr: "unknown key component tag 0x7f"},
		{name: "unterminated string", encoded: []byte{tagString, 'a', 'b'}, wantErr: "a key component of tag 0x50 has no terminator"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := componentLen(tc.encoded)
			if err == nil || err.Error() != tc.wantErr {
				t.Fatalf("componentLen error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

// BenchmarkComponentLen times componentLen on a fixed-width (number) tag, the
// per-entry call that was measured.
func BenchmarkComponentLen(b *testing.B) {
	encoded, err := EncodeComponent(int64(42))
	if err != nil {
		b.Fatalf("encoding a number key component: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := componentLen(encoded); err != nil {
			b.Fatalf("componentLen: %v", err)
		}
	}
}

// TestRangeScanNeverLeavesItsTypeBracket (GAP-14 build doc Step 14): a range walk carries its type tag at both ends. A walk from
// prefix‖low to prefix‖high crosses from one type into the next whenever the
// bounds' tags differ — the shape of the TTL sweep's S1, where a scan up to a
// date bound would also collect every null, boolean and number.
func TestRangeScanNeverLeavesItsTypeBracket(t *testing.T) {
	kv := kvfault.New()
	im, err := NewIndexManager(kv, 1, 1, 1, oneFieldSpecs("V"))
	if err != nil {
		t.Fatalf("NewIndexManager: %v", err)
	}
	documents := map[string]interface{}{
		"null":      nil,
		"bool":      true,
		"low-num":   int64(-1 << 60),
		"mid-num":   3.5,
		"high-num":  uint64(1 << 63),
		"date":      primitive.DateTime(1_700_000_000_000),
		"string":    "a string",
		"binary":    primitive.Binary{Data: []byte{0x01}},
		"late-date": primitive.DateTime(1_900_000_000_000),
	}
	for key, value := range documents {
		doc := map[string]interface{}{"V": value}
		if err := inBatch(kv, func(b storage.KVBatch) error { return im.OnInsert(b, []byte(key), doc) }); err != nil {
			t.Fatalf("OnInsert(%s): %v", key, err)
		}
	}
	index := im.indexes[0]
	scan := func(tag byte, upper interface{}) []string {
		t.Helper()
		encodedUpper, err := EncodeComponent(upper)
		if err != nil {
			t.Fatalf("EncodeComponent(%v): %v", upper, err)
		}
		var keys []string
		if err := index.ScanBracket(tag, encodedUpper, func(docKey []byte) error {
			keys = append(keys, string(docKey))
			return nil
		}); err != nil {
			t.Fatalf("ScanBracket: %v", err)
		}
		sort.Strings(keys)
		return keys
	}
	// An upper bound above every number: the numbers, and nothing else.
	if got := scan(tagNumber, math.MaxFloat64); strings.Join(got, ",") != "high-num,low-num,mid-num" {
		t.Errorf("the number bracket up to MaxFloat64 returned %v, want the three numbers only", got)
	}
	// A date bound between the two dates: the earlier date only — no number
	// below the date tag, no string above it.
	if got := scan(tagDateTime, primitive.DateTime(1_800_000_000_000)); strings.Join(got, ",") != "date" {
		t.Errorf("the date bracket returned %v, want [date]", got)
	}
}
