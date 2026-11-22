package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// countingSource serves a generated stream and records how far ahead of the
// consumer the reader has pulled: the streaming guard for T18 (buffering a whole
// stream is the failure it catches).
type countingSource struct {
	t         *testing.T
	generate  func(index int) []byte
	total     int
	next      int
	pending   []byte
	served    int64
	maxServed int64
}

// Read fills p as a file or a pipe would, across generated documents, so a
// reader that asks for a large buffer really is served a large buffer and the
// look-ahead it causes is visible.
func (s *countingSource) Read(p []byte) (int, error) {
	filled := 0
	for filled < len(p) {
		if len(s.pending) == 0 {
			if s.next >= s.total {
				break
			}
			s.pending = s.generate(s.next)
			s.next++
		}
		copied := copy(p[filled:], s.pending)
		s.pending = s.pending[copied:]
		filled += copied
	}
	s.served += int64(filled)
	s.maxServed = s.served
	if filled == 0 {
		return 0, io.EOF
	}
	return filled, nil
}

// TestIngestStreamsRatherThanBuffering is T18: a 400 MB virtual input is never
// materialised, and after ten documents the reader has pulled no more than one
// batch (2 MiB) beyond what it handed back.
func TestIngestStreamsRatherThanBuffering(t *testing.T) {
	padding := strings.Repeat("p", 4*1024*1024)
	for _, tc := range []struct {
		name     string
		generate func(int) []byte
		open     func(io.Reader) documentReader
	}{
		{"bson", func(i int) []byte {
			return bsonDocumentBytes(t, bson.D{{Key: "_id", Value: int32(i)}, {Key: "pad", Value: padding}})
		}, func(src io.Reader) documentReader { return newBSONReader(src, readerStart{totalBytes: -1}) }},
		{"jsonl", func(i int) []byte {
			return []byte(fmt.Sprintf(`{"_id":%d,"pad":"%s"}`+"\n", i, padding))
		}, func(src io.Reader) documentReader { return newJSONLReader(src, readerStart{totalBytes: -1}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &countingSource{t: t, generate: tc.generate, total: 100}
			reader := tc.open(source)
			var handedBack int64
			for i := 0; i < 10; i++ {
				document, err := reader.Next()
				if err != nil {
					t.Fatalf("document %d: %v", i, err)
				}
				handedBack += document.End - document.Start
			}
			const oneBatch = 2 << 20
			if ahead := source.served - handedBack; ahead > oneBatch {
				t.Fatalf("the reader pulled %d bytes ahead of the consumer; the limit is one 2 MiB batch", ahead)
			}
		})
	}
}

func TestBSONReaderTruncatedNamesOffset(t *testing.T) {
	good := bsonStream(t, bson.D{{Key: "a", Value: int32(1)}}, bson.D{{Key: "b", Value: int32(2)}})
	truncated := append(append([]byte{}, good...), bsonDocumentBytes(t, bson.D{{Key: "c", Value: strings.Repeat("z", 200)}})[:50]...)

	reader := newBSONReader(bytes.NewReader(truncated), readerStart{name: "users.bson", totalBytes: int64(len(truncated))})
	for i := 0; i < 2; i++ {
		if _, err := reader.Next(); err != nil {
			t.Fatalf("good document %d: %v", i, err)
		}
	}
	_, err := reader.Next()
	var corrupt *inputCorruptError
	if !errors.As(err, &corrupt) {
		t.Fatalf("truncated tail = %v, want an *inputCorruptError", err)
	}
	if corrupt.Offset != int64(len(good)) {
		t.Errorf("offset = %d, want %d (the start of the damaged document)", corrupt.Offset, len(good))
	}
	for _, want := range []string{"users.bson", "byte offset", "only 50 byte(s) remain", "truncated"} {
		if !strings.Contains(corrupt.Error(), want) {
			t.Errorf("message %q does not contain %q", corrupt.Error(), want)
		}
	}

	garbage := []byte("this is not a bson file at all, it is text")
	_, err = newBSONReader(bytes.NewReader(garbage), readerStart{name: "notes.bson", totalBytes: int64(len(garbage))}).Next()
	if !errors.As(err, &corrupt) || !strings.Contains(corrupt.Error(), "is not a stream of BSON documents") {
		t.Errorf("text read as BSON = %v, want the invalid-length refusal", err)
	}
}

func TestBSONReaderValidLengthUndecodableContentsIsPerDocument(t *testing.T) {
	good := bsonDocumentBytes(t, bson.D{{Key: "a", Value: int32(1)}})
	// A well-formed length prefix and terminator around an element with an
	// impossible type byte.
	broken := []byte{12, 0, 0, 0, 0x7f, 'x', 0, 1, 2, 3, 4, 0}
	stream := append(append(append([]byte{}, good...), broken...), good...)

	reader := newBSONReader(bytes.NewReader(stream), readerStart{name: "x.bson", totalBytes: int64(len(stream))})
	if _, err := reader.Next(); err != nil {
		t.Fatalf("first good document: %v", err)
	}
	_, err := reader.Next()
	var failure *documentFailure
	if !errors.As(err, &failure) {
		t.Fatalf("undecodable document = %v, want a *documentFailure", err)
	}
	if failure.Ordinal != 2 || failure.Start != int64(len(good)) || !bytes.Equal(failure.Source, broken) {
		t.Errorf("failure = ordinal %d start %d source % x", failure.Ordinal, failure.Start, failure.Source)
	}
	if third, err := reader.Next(); err != nil || third.Ordinal != 3 {
		t.Errorf("the reader did not continue past a per-document failure: %v %+v", err, third)
	}
}

// TestBSONReaderOversizedDocumentIsSkippedWithoutBuffering: a document above
// the limit is a per-document failure and its bytes are discarded, not held.
func TestBSONReaderOversizedDocumentIsSkippedWithoutBuffering(t *testing.T) {
	huge := bsonDocumentBytes(t, bson.D{{Key: "pad", Value: strings.Repeat("h", ingestMaxDocumentBytes+10)}})
	small := bsonDocumentBytes(t, bson.D{{Key: "a", Value: int32(1)}})
	stream := append(append([]byte{}, huge...), small...)
	reader := newBSONReader(bytes.NewReader(stream), readerStart{name: "big.bson", totalBytes: int64(len(stream))})

	_, err := reader.Next()
	var failure *documentFailure
	if !errors.As(err, &failure) || !failure.Oversize || failure.Length != int64(len(huge)) {
		t.Fatalf("oversized document = %v, want an oversize *documentFailure of %d bytes", err, len(huge))
	}
	if failure.Source != nil {
		t.Errorf("the oversized document's %d bytes were kept in memory", len(failure.Source))
	}
	if next, err := reader.Next(); err != nil || next.Ordinal != 2 {
		t.Errorf("did not continue after the oversized document: %v %+v", err, next)
	}
}

// TestJSONLRefusesTrailingContentAndDuplicateKey is T23 at the reader: the
// Extended JSON decoder stops after the first value and reports success
// (characterisation C8), so a missing newline would drop documents.
func TestJSONLRefusesTrailingContentAndDuplicateKey(t *testing.T) {
	input := strings.Join([]string{
		`{"ok":1}`,
		`{"a":1}{"b":2}`,
		`{"a":1} trailing`,
		`{"a":1,"a":2}`,
		`{"nested":{"k":1,"k":2}}`,
		`{"fine":true}`,
	}, "\n") + "\n"
	reader := newJSONLReader(strings.NewReader(input), readerStart{name: "events.jsonl", totalBytes: int64(len(input))})

	if first, err := reader.Next(); err != nil || first.Ordinal != 1 {
		t.Fatalf("first line: %v %+v", err, first)
	}
	wantReasons := []string{"more than one", "more than one", "duplicate key", "duplicate key"}
	for index, wantReason := range wantReasons {
		_, err := reader.Next()
		var failure *documentFailure
		if !errors.As(err, &failure) {
			t.Fatalf("bad line %d = %v, want a *documentFailure", index+2, err)
		}
		if failure.Ordinal != int64(index+2) || !strings.Contains(failure.Reason, wantReason) {
			t.Errorf("line %d: ordinal %d reason %q, want it to contain %q", index+2, failure.Ordinal, failure.Reason, wantReason)
		}
		if strings.Contains(failure.Reason, "trailing") && strings.Contains(failure.Reason, `"a"`) {
			t.Errorf("reason %q echoes document content", failure.Reason)
		}
	}
	if last, err := reader.Next(); err != nil || last.Ordinal != 6 {
		t.Fatalf("the line after the bad ones: %v %+v", err, last)
	}
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		t.Errorf("end = %v, want io.EOF", err)
	}
}

func TestJSONLLongLineOver64KiB(t *testing.T) {
	body := strings.Repeat("x", 200*1024)
	input := fmt.Sprintf("{\"pad\":\"%s\"}\n\n{\"n\":1}\n", body)
	reader := newJSONLReader(strings.NewReader(input), readerStart{totalBytes: int64(len(input))})
	first, err := reader.Next()
	if err != nil {
		t.Fatalf("a 200 KiB line: %v (bufio.Scanner's 64 KiB token limit must not apply)", err)
	}
	if got, _ := first.Doc.Lookup("pad").StringValueOK(); len(got) != len(body) {
		t.Errorf("pad length = %d, want %d", len(got), len(body))
	}
	second, err := reader.Next()
	if err != nil || second.Ordinal != 3 {
		t.Errorf("after the blank line: %v ordinal %d, want the third line", err, second.Ordinal)
	}
}

func TestJSONLLineOverTheDocumentLimitIsPerDocument(t *testing.T) {
	huge := strings.Repeat("x", ingestMaxDocumentBytes+10)
	input := fmt.Sprintf("{\"pad\":\"%s\"}\n{\"n\":1}\n", huge)
	reader := newJSONLReader(strings.NewReader(input), readerStart{totalBytes: int64(len(input))})
	_, err := reader.Next()
	var failure *documentFailure
	if !errors.As(err, &failure) || !failure.Oversize {
		t.Fatalf("oversized line = %v, want an oversize *documentFailure", err)
	}
	if next, err := reader.Next(); err != nil || next.Ordinal != 2 {
		t.Errorf("did not continue after the oversized line: %v %+v", err, next)
	}
}

// TestJSONLKeepsExtendedJSONTypes: a plain ISO string stays a string, and the
// Extended JSON wrappers keep their types (spec §4.3, T3).
func TestJSONLKeepsExtendedJSONTypes(t *testing.T) {
	line := `{"when":{"$date":"2021-03-04T05:06:07Z"},"text":"2021-03-04T05:06:07Z","oid":{"$oid":"000000000000000000000001"},"n":5}` + "\n"
	document, err := newJSONLReader(strings.NewReader(line), readerStart{totalBytes: int64(len(line))}).Next()
	if err != nil {
		t.Fatal(err)
	}
	if got := document.Doc.Lookup("when").Type; got != bson.TypeDateTime {
		t.Errorf("$date became %s, want datetime", got)
	}
	if got := document.Doc.Lookup("text").Type; got != bson.TypeString {
		t.Errorf("an ISO string became %s, want it kept as a string", got)
	}
	if got := document.Doc.Lookup("oid").Type; got != bson.TypeObjectID {
		t.Errorf("$oid became %s, want object id", got)
	}
}

func TestDetectFormatRefusesArrayAndPrettyObject(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		first   string
		wantFix string
	}{
		{"array", "events.json", "[\n  {\"a\": 1}\n]", "jq -c '.[]' events.json > events.jsonl"},
		{"array after spaces", "events.jsonl", "   \n[{\"a\":1}]", "jq -c '.[]'"},
		{"pretty object", "one.json", "{\n  \"a\": 1\n}\n", "jq -c ."},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := detectFormat(tc.path, []byte(tc.first), "")
			if err == nil {
				t.Fatal("accepted; ingest reads one JSON document per line")
			}
			if !strings.Contains(err.Error(), tc.wantFix) {
				t.Errorf("message %q does not contain the fix %q", err.Error(), tc.wantFix)
			}
			if strings.Contains(err.Error(), "cannot decode array") {
				t.Errorf("message %q leaks the decoder's text", err.Error())
			}
		})
	}
	format, err := detectFormat("events.json", []byte("{\"a\":1}\n{\"a\":2}\n"), "")
	if err != nil || format != formatJSONL {
		t.Errorf("a JSON Lines .json file = (%q, %v), want jsonl", format, err)
	}
}

func TestDetectFormatByNameAndOverride(t *testing.T) {
	for path, want := range map[string]inputFormat{
		"users.bson": formatBSON, "events.jsonl": formatJSONL, "events.ndjson": formatJSONL, "events.json": formatJSONL,
		"USERS.BSON": formatBSON,
	} {
		first := []byte("{\"a\":1}\n")
		if want == formatBSON {
			first = bsonDocumentBytes(t, bson.D{{Key: "a", Value: int32(1)}})
		}
		if got, err := detectFormat(path, first, ""); err != nil || got != want {
			t.Errorf("detectFormat(%q) = (%q, %v), want %q", path, got, err, want)
		}
	}
	if _, err := detectFormat("dump.dat", nil, ""); err == nil || !strings.Contains(err.Error(), "--format") {
		t.Errorf("an unknown suffix = %v, want an error naming --format", err)
	}
	if got, err := detectFormat("dump.dat", []byte("{\"a\":1}\n"), "jsonl"); err != nil || got != formatJSONL {
		t.Errorf("--format jsonl = (%q, %v)", got, err)
	}
	if _, err := detectFormat("users.bson", nil, "csv"); err == nil || !strings.Contains(err.Error(), "bson") {
		t.Errorf("--format csv = %v, want an error naming the accepted formats", err)
	}
}

// TestDetectFormatRefusesGzipByMagicAndSuffix is T11.
func TestDetectFormatRefusesGzipByMagicAndSuffix(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		first []byte
	}{
		{"suffix", "users.bson.gz", []byte{0x00}},
		{"gz suffix alone", "events.gz", []byte("plain")},
		{"magic under a bson name", "users.bson", []byte{0x1f, 0x8b, 0x08, 0x00}},
		{"magic under a jsonl name", "events.jsonl", []byte{0x1f, 0x8b, 0x08, 0x00}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := detectFormat(tc.path, tc.first, "")
			if err == nil {
				t.Fatal("a gzip file was accepted; it would read as a truncated BSON document")
			}
			for _, want := range []string{"gunzip", tc.path} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("message %q does not contain %q", err.Error(), want)
				}
			}
		})
	}
	// --format does not override a gzip refusal.
	if _, err := detectFormat("users.bson", []byte{0x1f, 0x8b}, "bson"); err == nil {
		t.Error("--format bson let a gzip file through")
	}
}

// TestReaderOffsetsAreBytesForBothFormats: resume offsets are bytes for both
// formats, so a UTF-8 multi-byte line and CRLF endings must not skew them.
func TestReaderOffsetsAreBytesForBothFormats(t *testing.T) {
	first := bsonDocumentBytes(t, bson.D{{Key: "a", Value: "héllo"}})
	second := bsonDocumentBytes(t, bson.D{{Key: "b", Value: int32(2)}})
	bsonInput := append(append([]byte{}, first...), second...)
	bsonReader := newBSONReader(bytes.NewReader(bsonInput), readerStart{totalBytes: int64(len(bsonInput))})
	one, _ := bsonReader.Next()
	two, _ := bsonReader.Next()
	if one.Start != 0 || one.End != int64(len(first)) || two.Start != int64(len(first)) || two.End != int64(len(bsonInput)) {
		t.Errorf("bson offsets = [%d,%d) [%d,%d), want [0,%d) [%d,%d)", one.Start, one.End, two.Start, two.End, len(first), len(first), len(bsonInput))
	}

	jsonInput := "{\"a\":\"héllo\"}\r\n\n{\"b\":2}"
	jsonReader := newJSONLReader(strings.NewReader(jsonInput), readerStart{totalBytes: int64(len(jsonInput))})
	lineOne, _ := jsonReader.Next()
	lineThree, err := jsonReader.Next()
	if err != nil {
		t.Fatal(err)
	}
	if lineOne.Start != 0 || lineOne.End != int64(len("{\"a\":\"héllo\"}\r\n")) {
		t.Errorf("jsonl line 1 = [%d,%d), want [0,%d)", lineOne.Start, lineOne.End, len("{\"a\":\"héllo\"}\r\n"))
	}
	wantStart := int64(len("{\"a\":\"héllo\"}\r\n\n"))
	if lineThree.Start != wantStart || lineThree.End != int64(len(jsonInput)) || lineThree.Ordinal != 3 {
		t.Errorf("jsonl line 3 = [%d,%d) ordinal %d, want [%d,%d) ordinal 3", lineThree.Start, lineThree.End, lineThree.Ordinal, wantStart, len(jsonInput))
	}

	// A reader started at a resume offset numbers from there.
	resumed := newJSONLReader(strings.NewReader(jsonInput[wantStart:]), readerStart{offset: wantStart, ordinal: 2, totalBytes: int64(len(jsonInput))})
	got, err := resumed.Next()
	if err != nil || got.Start != wantStart || got.Ordinal != 3 {
		t.Errorf("resumed reader = %v [%d,%d) ordinal %d, want start %d ordinal 3", err, got.Start, got.End, got.Ordinal, wantStart)
	}
}
