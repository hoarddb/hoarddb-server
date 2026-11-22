package cli

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// inputFormat names the two file formats ingest reads.
type inputFormat string

const (
	formatBSON  inputFormat = "bson"
	formatJSONL inputFormat = "jsonl"
)

// ingestMaxDocumentBytes is the largest single document ingest sends. It is
// server/dump.go:maxDumpDocumentBytes (frame limit less 64 KiB of envelope),
// which is unexported; the value is the same by construction and is checked
// against the frame limit here so a change to either is visible in review.
const ingestMaxDocumentBytes = transport.MaxFrameLength - 64*1024

// readerStart says where in the input a reader begins, so a resumed run reports
// the same byte offsets and document numbers as the run it continues.
type readerStart struct {
	// name is the input's display name, used in messages.
	name string
	// offset is the byte offset the reader's first byte sits at.
	offset int64
	// ordinal counts the documents (BSON) or lines (JSON Lines) before offset.
	ordinal int64
	// totalBytes is the input's length, or -1 when unknown (standard input).
	totalBytes int64
}

// readDocument is one document read from the input.
type readDocument struct {
	// Doc is the validated document.
	Doc bson.Raw
	// Source is the input bytes the document came from (a JSON line without
	// its line ending), kept for the rejects file.
	Source []byte
	// Start and End are the byte offsets of the document's first byte and of
	// the byte after it (after the newline, for a JSON line).
	Start, End int64
	// Ordinal is the 1-based number of the document (BSON) or line (JSON
	// Lines) counted from the start of the input.
	Ordinal int64
}

// documentFailure is the failure of one document. The reader has already moved
// past it, so the caller may continue (--on-error=skip) or stop.
type documentFailure struct {
	Ordinal    int64
	Start, End int64
	// Length is the document's size in bytes, set for an oversized document.
	Length int64
	// Oversize marks a document above ingestMaxDocumentBytes. Its bytes were
	// discarded, not held.
	Oversize bool
	// Reason is a sentence about the failure that never quotes the document.
	Reason string
	// Source is the raw bytes for the rejects file; nil for an oversized one.
	Source []byte
}

// Error renders the failure without its location, which the caller adds.
func (f *documentFailure) Error() string { return terminalSafeText(f.Reason) }

// inputCorruptError is a failure of the whole input: the reader cannot find
// the next document, so nothing after Offset can be read.
type inputCorruptError struct {
	Offset  int64
	Message string
}

// Error returns the refusal text.
func (e *inputCorruptError) Error() string { return terminalSafeText(e.Message) }

// documentReader yields documents until io.EOF.
type documentReader interface {
	// Next returns the next document. It returns a *documentFailure for a bad
	// document (and can be called again), an *inputCorruptError when the input
	// cannot be read further, and io.EOF at the end.
	Next() (readDocument, error)
}

// bsonDocumentReader streams a file of concatenated BSON documents.
type bsonDocumentReader struct {
	source  io.Reader
	start   readerStart
	offset  int64
	ordinal int64
	failed  *inputCorruptError
}

// newBSONReader streams documents from src, which is positioned at
// start.offset. It reads a document's length prefix and then exactly that many
// bytes, so it holds one document and never the file.
func newBSONReader(src io.Reader, start readerStart) *bsonDocumentReader {
	return &bsonDocumentReader{source: src, start: start, offset: start.offset, ordinal: start.ordinal}
}

// Next implements documentReader.
func (r *bsonDocumentReader) Next() (readDocument, error) {
	if r.failed != nil {
		return readDocument{}, r.failed
	}
	documentStart := r.offset

	header := make([]byte, 4)
	headerRead, err := io.ReadFull(r.source, header)
	switch {
	case errors.Is(err, io.EOF):
		return readDocument{}, io.EOF
	case err != nil:
		return readDocument{}, r.corrupt(documentStart, fmt.Sprintf("only %d byte(s) remain, and a BSON document starts with a 4-byte length", headerRead))
	}
	length := int64(binary.LittleEndian.Uint32(header))
	remaining := r.remainingAt(documentStart)
	if length < 5 || (remaining >= 0 && length > remaining) {
		return readDocument{}, r.corrupt(documentStart, r.lengthDetail(length, remaining))
	}
	r.ordinal++

	if length > ingestMaxDocumentBytes {
		return r.discardOversized(documentStart, length)
	}
	document := make([]byte, length)
	copy(document, header)
	bodyRead, err := io.ReadFull(r.source, document[4:])
	if err != nil {
		return readDocument{}, r.corrupt(documentStart, r.lengthDetail(length, int64(4+bodyRead)))
	}
	r.offset += length

	if validateErr := bson.Raw(document).Validate(); validateErr != nil {
		return readDocument{}, &documentFailure{
			Ordinal: r.ordinal, Start: documentStart, End: r.offset, Source: document,
			Reason: "the document's length is right but its contents are not valid BSON (" + validateErr.Error() + ")",
		}
	}
	if reason := reservedNamespaceReason(document); reason != "" {
		return readDocument{}, &documentFailure{Ordinal: r.ordinal, Start: documentStart, End: r.offset, Source: document, Reason: reason}
	}
	return readDocument{Doc: bson.Raw(document), Source: document, Start: documentStart, End: r.offset, Ordinal: r.ordinal}, nil
}

// reservedNamespaceReason returns "" when document holds no DBPointer in the
// namespace the server reserves for now(), else a sentence naming the field.
// Such a pointer would be stored as the time of the write (BUG-0151), so ingest
// refuses it; every other deprecated value, DBPointers of other namespaces
// included, loads as it always did. The common document pays one scan of its
// bytes and no walk.
func reservedNamespaceReason(document bson.Raw) string {
	namespace := server.NowMarker().DB
	if !bytes.Contains(document, []byte(namespace)) {
		return ""
	}
	path, found := reservedNamespacePath(document, "", namespace)
	if !found {
		return ""
	}
	return fmt.Sprintf("field %q holds a BSON DBPointer in the namespace %q, which hoardDB reserves for now(); "+
		"change the namespace, or store the pointer as a string or a sub-document", path, namespace)
}

// reservedNamespacePath walks a document or array for a DBPointer in namespace
// and returns its dotted path.
func reservedNamespacePath(container bson.Raw, prefix, namespace string) (string, bool) {
	elements, err := container.Elements()
	if err != nil {
		return "", false
	}
	for _, element := range elements {
		path := prefix + element.Key()
		value := element.Value()
		switch value.Type {
		case bson.TypeDBPointer:
			if db, _, ok := value.DBPointerOK(); ok && db == namespace {
				return path, true
			}
		case bson.TypeEmbeddedDocument:
			if found, ok := reservedNamespacePath(value.Document(), path+".", namespace); ok {
				return found, true
			}
		case bson.TypeArray:
			if found, ok := reservedNamespacePath(value.Array(), path+".", namespace); ok {
				return found, true
			}
		}
	}
	return "", false
}

// discardOversized skips a document too large to send without holding it.
func (r *bsonDocumentReader) discardOversized(documentStart, length int64) (readDocument, error) {
	discarded, err := io.CopyN(io.Discard, r.source, length-4)
	if err != nil {
		return readDocument{}, r.corrupt(documentStart, r.lengthDetail(length, 4+discarded))
	}
	r.offset += length
	return readDocument{}, &documentFailure{
		Ordinal: r.ordinal, Start: documentStart, End: r.offset, Length: length, Oversize: true,
		Reason: fmt.Sprintf("the document is %s, and the largest document hoardDB accepts is %s", formatBytes(length), formatBytes(ingestMaxDocumentBytes)),
	}
}

// remainingAt returns the bytes left in the input from offset, or -1.
func (r *bsonDocumentReader) remainingAt(offset int64) int64 {
	if r.start.totalBytes < 0 {
		return -1
	}
	return r.start.totalBytes - offset
}

func (r *bsonDocumentReader) lengthDetail(length, remaining int64) string {
	if remaining < 0 {
		return fmt.Sprintf("the length %d is below 5 or runs past the end of the input", length)
	}
	return fmt.Sprintf("the length is %d but only %s byte(s) remain", length, formatCount(remaining))
}

// corrupt records the failure that ends the input.
func (r *bsonDocumentReader) corrupt(offset int64, detail string) *inputCorruptError {
	r.failed = &inputCorruptError{
		Offset: offset,
		Message: fmt.Sprintf("%s: invalid BSON document at byte offset %s (%s) — the file is truncated or is not a stream of BSON documents.",
			r.start.name, formatCount(offset), detail),
	}
	return r.failed
}

// jsonlReader streams a JSON Lines file, one document per line.
type jsonlReader struct {
	source  *bufio.Reader
	start   readerStart
	offset  int64
	ordinal int64
}

// newJSONLReader streams documents from src, which is positioned at
// start.offset. Lines are read with bufio.Reader.ReadSlice rather than
// bufio.Scanner, whose 64 KiB token limit rejects large lines, and a line
// above the document limit is discarded as it is read instead of held.
func newJSONLReader(src io.Reader, start readerStart) *jsonlReader {
	return &jsonlReader{source: bufio.NewReaderSize(src, 64*1024), start: start, offset: start.offset, ordinal: start.ordinal}
}

// Next implements documentReader.
func (r *jsonlReader) Next() (readDocument, error) {
	for {
		lineStart := r.offset
		line, length, oversize, err := r.readLine()
		if err != nil && !errors.Is(err, io.EOF) {
			return readDocument{}, fmt.Errorf("%s: reading line %d: %w", r.start.name, r.ordinal+1, err)
		}
		if length == 0 {
			return readDocument{}, io.EOF
		}
		r.ordinal++
		r.offset += length

		if oversize {
			return readDocument{}, &documentFailure{
				Ordinal: r.ordinal, Start: lineStart, End: r.offset, Length: length, Oversize: true,
				Reason: fmt.Sprintf("the line is %s, and the largest document hoardDB accepts is %s", formatBytes(length), formatBytes(ingestMaxDocumentBytes)),
			}
		}
		content := bytes.TrimSpace(line)
		if r.ordinal == 1 && lineStart == 0 {
			content = bytes.TrimPrefix(content, []byte("\xef\xbb\xbf"))
		}
		if len(content) == 0 {
			continue
		}
		source := bytes.TrimRight(line, "\r\n")
		document, reason := decodeJSONLine(content)
		if reason != "" {
			return readDocument{}, &documentFailure{Ordinal: r.ordinal, Start: lineStart, End: r.offset, Reason: reason, Source: source}
		}
		return readDocument{Doc: document, Source: source, Start: lineStart, End: r.offset, Ordinal: r.ordinal}, nil
	}
}

// readLine reads one line including its line ending. A line above the document
// limit is counted and discarded as it streams past, so a file with no
// newlines cannot grow memory without bound; oversize is then true and line nil.
func (r *jsonlReader) readLine() (line []byte, length int64, oversize bool, err error) {
	for {
		chunk, readErr := r.source.ReadSlice('\n')
		length += int64(len(chunk))
		switch {
		case oversize:
		case length > ingestMaxDocumentBytes:
			oversize, line = true, nil
		default:
			line = append(line, chunk...)
		}
		if !errors.Is(readErr, bufio.ErrBufferFull) {
			return line, length, oversize, readErr
		}
	}
}

// decodeJSONLine turns one line into a BSON document, or returns a sentence
// saying why it cannot. The sentence never quotes the line.
//
// bson.UnmarshalExtJSON stops after the first JSON value and reports success
// (characterisation C8), and it accepts a repeated key silently, so the line is
// first walked as a token stream: exactly one object, no text after it, no
// repeated key at any depth (ingest-spec.md §4.3, F-4).
func decodeJSONLine(line []byte) (bson.Raw, string) {
	if reason := checkSingleJSONObject(line); reason != "" {
		return nil, reason
	}
	var document bson.D
	if err := bson.UnmarshalExtJSON(line, false, &document); err != nil {
		return nil, "the line is not valid Extended JSON (" + truncateForMessage(err.Error(), 100) + ")"
	}
	encoded, err := bson.Marshal(document)
	if err != nil {
		return nil, "the line decoded but could not be encoded as BSON (" + truncateForMessage(err.Error(), 100) + ")"
	}
	return bson.Raw(encoded), reservedNamespaceReason(encoded)
}

// jsonFrame tracks one open object or array while walking tokens.
type jsonFrame struct {
	isObject  bool
	expectKey bool
	keys      map[string]struct{}
}

// checkSingleJSONObject returns "" when line is exactly one JSON object with no
// repeated key, else a sentence saying which rule it breaks.
func checkSingleJSONObject(line []byte) string {
	decoder := json.NewDecoder(bytes.NewReader(line))
	first, err := decoder.Token()
	if err != nil {
		return "the line is not valid JSON"
	}
	opening, isDelim := first.(json.Delim)
	switch {
	case isDelim && opening == '[':
		return "the line is a JSON array, and ingest reads one JSON object per line (convert with: jq -c '.[]' file.json > file.jsonl)"
	case !isDelim || opening != '{':
		return "the line is not a JSON object"
	}

	stack := []jsonFrame{{isObject: true, expectKey: true, keys: map[string]struct{}{}}}
	for len(stack) > 0 {
		token, err := decoder.Token()
		if err != nil {
			return "the line is not valid JSON"
		}
		top := &stack[len(stack)-1]
		if delim, ok := token.(json.Delim); ok {
			switch delim {
			case '{', '[':
				stack = append(stack, jsonFrame{isObject: delim == '{', expectKey: delim == '{', keys: map[string]struct{}{}})
			default:
				stack = stack[:len(stack)-1]
				if len(stack) > 0 && stack[len(stack)-1].isObject {
					stack[len(stack)-1].expectKey = true
				}
			}
			continue
		}
		if !top.isObject {
			continue
		}
		if top.expectKey {
			key, _ := token.(string)
			if _, repeated := top.keys[key]; repeated {
				return "the line contains a duplicate key, which would silently keep only one of the values"
			}
			top.keys[key] = struct{}{}
			top.expectKey = false
			continue
		}
		top.expectKey = true
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return "the line holds more than one JSON value, or text after the first one; ingest reads exactly one document per line " +
			"(a missing newline between two files is the usual cause)"
	}
	return ""
}

// truncateForMessage bounds text echoed from a library error.
func truncateForMessage(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	return text[:limit] + "…"
}

// detectFormat decides the input format from the file name and its first
// bytes, refusing what ingest cannot read with the command that fixes it.
// override is the --format flag ("" for detection).
func detectFormat(path string, first []byte, override string) (inputFormat, error) {
	base := filepath.Base(path)
	lower := strings.ToLower(base)
	if strings.HasSuffix(lower, ".gz") || bytes.HasPrefix(first, []byte{0x1f, 0x8b}) {
		stem := strings.TrimSuffix(base, filepath.Ext(base))
		if !strings.HasSuffix(lower, ".gz") {
			stem = base + ".unpacked"
		}
		return "", fmt.Errorf("%s: this version reads uncompressed .bson and JSON Lines only. Run 'gunzip -k %s' and ingest %s, or re-export without compression", path, path, stem)
	}

	format := inputFormat(override)
	switch override {
	case "":
		switch {
		case strings.HasSuffix(lower, ".bson"):
			format = formatBSON
		case strings.HasSuffix(lower, ".jsonl"), strings.HasSuffix(lower, ".ndjson"), strings.HasSuffix(lower, ".json"):
			format = formatJSONL
		default:
			return "", fmt.Errorf("%s: cannot tell whether this is a BSON file or JSON Lines from its name. Pass --format bson or --format jsonl", path)
		}
	case string(formatBSON), string(formatJSONL):
	default:
		return "", fmt.Errorf("--format %q is not a format ingest reads. Use --format bson or --format jsonl", override)
	}

	if format == formatJSONL {
		if err := checkJSONLShape(path, first); err != nil {
			return "", err
		}
	}
	return format, nil
}

// checkJSONLShape refuses a JSON array and a pretty-printed object, which are
// valid JSON but not one document per line.
func checkJSONLShape(path string, first []byte) error {
	trimmed := bytes.TrimLeft(first, " \t\r\n\xef\xbb\xbf")
	if bytes.HasPrefix(trimmed, []byte("[")) {
		base := filepath.Base(path)
		converted := strings.TrimSuffix(base, filepath.Ext(base)) + ".jsonl"
		return fmt.Errorf("%s starts with '[': ingest reads one JSON document per line, not a JSON array. Convert it with:  jq -c '.[]' %s > %s   and ingest %s", path, base, converted, converted)
	}
	firstLine, _, _ := bytes.Cut(trimmed, []byte("\n"))
	if string(bytes.TrimSpace(firstLine)) == "{" {
		base := filepath.Base(path)
		converted := strings.TrimSuffix(base, filepath.Ext(base)) + ".jsonl"
		return fmt.Errorf("%s looks like one pretty-printed JSON object: ingest reads one JSON document per line. Convert it with:  jq -c . %s > %s   and ingest %s", path, base, converted, converted)
	}
	return nil
}
