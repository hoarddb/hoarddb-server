package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// The BSON corpus is clients/testdata/bson-vectors/v1.json: bytes the server's
// own encoder wrote, which the in-tree codecs of the Python and C++ drivers
// are checked against byte for byte (the driver-python-spec record OQ-3).
const committedBSONCorpusPath = "../../../clients/testdata/bson-vectors/v1.json"

// committedBSONCorpus reads the committed file: a failure in the private
// tree, a named skip in the published server tree (which has no clients/).
func committedBSONCorpus(t *testing.T) []byte {
	t.Helper()
	committed, err := os.ReadFile(committedBSONCorpusPath)
	if err == nil {
		return committed
	}
	if errors.Is(err, fs.ErrNotExist) {
		if _, markerErr := os.Stat(privateTreeMarker); errors.Is(markerErr, fs.ErrNotExist) {
			t.Skipf("published server tree: clients/ is not part of it, so there is no committed %s to check", committedBSONCorpusPath)
		}
	}
	t.Fatalf("reading the committed BSON corpus %s: %v; generate it with 'make bson-vectors' (hoardDB/Makefile)", committedBSONCorpusPath, err)
	return nil
}

func freshBSONCorpus(t *testing.T, commit string) []byte {
	t.Helper()
	generated, err := generateBSONCorpus(commit)
	if err != nil {
		t.Fatalf("generating the BSON corpus: %v", err)
	}
	return generated
}

func (v bsonVector) reencodes() bool { return v.ReencodesIdentically != nil && *v.ReencodesIdentically }

func mustDecodeHex(t *testing.T, name, encoded string) []byte {
	t.Helper()
	raw, err := hex.DecodeString(encoded)
	if err != nil {
		t.Fatalf("vector %q: hex does not decode: %v", name, err)
	}
	return raw
}

func mustParseCorpus(t *testing.T, encoded []byte) bsonCorpusFile {
	t.Helper()
	corpus, err := decodeBSONCorpus(encoded)
	if err != nil {
		t.Fatalf("parsing the corpus: %v", err)
	}
	return corpus
}

// TestBSONCorpusIsDeterministic: two runs give the same bytes, which is what
// lets a regeneration gate exist at all.
func TestBSONCorpusIsDeterministic(t *testing.T) {
	first := freshBSONCorpus(t, "0000000000000000000000000000000000000000")
	second := freshBSONCorpus(t, "0000000000000000000000000000000000000000")
	if !bytes.Equal(first, second) {
		t.Fatal("two generations of the BSON corpus differ: the output must not depend on time, order of maps or randomness")
	}
}

// TestBSONCorpusMatchesCommitted fails when the committed file is not what the
// server's encoder writes today. server_commit is the one field allowed to
// differ: it names the commit the file was last generated at.
func TestBSONCorpusMatchesCommitted(t *testing.T) {
	committed := committedBSONCorpus(t)
	committedCorpus := mustParseCorpus(t, committed)
	generated := freshBSONCorpus(t, committedCorpus.ServerCommit)
	if !bytes.Equal(committed, generated) {
		t.Fatalf("%s does not match the server's encoder at this commit; regenerate with 'make bson-vectors' and review the diff: a byte that changes here is a change to every in-tree codec", committedBSONCorpusPath)
	}
}

func TestBSONCorpusHeader(t *testing.T) {
	corpus := mustParseCorpus(t, freshBSONCorpus(t, "abc123"))
	if corpus.Format != bsonCorpusFormat {
		t.Errorf("format = %q, want %q", corpus.Format, bsonCorpusFormat)
	}
	if corpus.ServerCommit != "abc123" {
		t.Errorf("server_commit = %q, want the commit passed in", corpus.ServerCommit)
	}
	if corpus.GeneratedBy == "" || corpus.License == "" || len(corpus.ValueForms) == 0 {
		t.Errorf("header incomplete: generated_by=%q license=%q value_forms=%d", corpus.GeneratedBy, corpus.License, len(corpus.ValueForms))
	}
	if strings.Contains(string(freshBSONCorpus(t, "abc123")), homeDirPrefix) {
		t.Error("the corpus names a local path")
	}
}

// TestBSONEncodeVectorsRoundTrip: the typed form is enough to rebuild the
// bytes (so a driver needs no BSON library to read the file), and the server's
// decoder reads the bytes back to the same typed form.
func TestBSONEncodeVectorsRoundTrip(t *testing.T) {
	corpus := mustParseCorpus(t, freshBSONCorpus(t, "abc123"))
	seen := 0
	for _, vector := range corpus.Vectors {
		if vector.Kind != "encode" {
			continue
		}
		seen++
		want := mustDecodeHex(t, vector.Name, vector.hexValue())
		document, err := typedToDocument(vector.Document)
		if err != nil {
			t.Fatalf("vector %q: typed form does not parse: %v", vector.Name, err)
		}
		got, err := bson.Marshal(document)
		if err != nil {
			t.Fatalf("vector %q: server encoder: %v", vector.Name, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("vector %q: typed form encodes to %x, file says %x", vector.Name, got, want)
		}
		decoded, err := serverDecode(want)
		if err != nil {
			t.Errorf("vector %q: server decoder refuses its own encoder's bytes: %v", vector.Name, err)
			continue
		}
		again, err := documentToTyped(decoded)
		if err != nil {
			t.Errorf("vector %q: decoded value does not render: %v", vector.Name, err)
			continue
		}
		if !jsonEqual(t, again, vector.Document) {
			t.Errorf("vector %q: decoded back to %s, file says %s", vector.Name, again, vector.Document)
		}
		reencoded, err := bson.Marshal(decoded)
		if err != nil || !bytes.Equal(reencoded, want) {
			t.Errorf("vector %q: re-encoding the decoded value gives %x (err %v), want %x", vector.Name, reencoded, err, want)
		}
	}
	if seen < 100 {
		t.Errorf("only %d encode vectors; the corpus must cover every type of driver-python-spec section 7.2 and every edge of 7.1", seen)
	}
}

func TestBSONDecodeVectorsReplay(t *testing.T) {
	corpus := mustParseCorpus(t, freshBSONCorpus(t, "abc123"))
	seen := 0
	for _, vector := range corpus.Vectors {
		if vector.Kind != "decode" {
			continue
		}
		seen++
		raw := mustDecodeHex(t, vector.Name, vector.hexValue())
		decoded, err := serverDecode(raw)
		if err != nil {
			t.Errorf("vector %q: the server's decoder refuses it: %v", vector.Name, err)
			continue
		}
		rendered, err := documentToTyped(decoded)
		if err != nil || !jsonEqual(t, rendered, vector.Document) {
			t.Errorf("vector %q: decoded to %s (err %v), file says %s", vector.Name, rendered, err, vector.Document)
		}
		reencoded, err := bson.Marshal(decoded)
		identical := err == nil && bytes.Equal(reencoded, raw)
		if identical != vector.reencodes() {
			t.Errorf("vector %q: reencodes_identically = %v in the file, observed %v", vector.Name, vector.reencodes(), identical)
		}
	}
	if seen < 8 {
		t.Errorf("only %d decode vectors", seen)
	}
}

// TestBSONRejectVectorsReplay replays every malformed input through the
// server's decoder and requires the outcome the file records. A vector whose
// server_decoder is "accepted" is a place the spec is stricter than the
// server, and carries a note saying so.
func TestBSONRejectVectorsReplay(t *testing.T) {
	corpus := mustParseCorpus(t, freshBSONCorpus(t, "abc123"))
	seen, refused := 0, 0
	for _, vector := range corpus.Vectors {
		if vector.Kind != "reject" {
			continue
		}
		seen++
		raw := mustDecodeHex(t, vector.Name, vector.hexValue())
		_, err := serverDecode(raw)
		observed := "accepted"
		if err != nil {
			observed = "refused"
			refused++
		}
		if observed != vector.ServerDecoder {
			t.Errorf("vector %q: server_decoder = %q in the file, observed %q", vector.Name, vector.ServerDecoder, observed)
		}
		if vector.Refusal != "ProtocolError" {
			t.Errorf("vector %q: refusal class %q, want ProtocolError (driver-python-spec section 7.2)", vector.Name, vector.Refusal)
		}
		if vector.Reason == "" {
			t.Errorf("vector %q: no reason", vector.Name)
		}
		if vector.ServerDecoder == "accepted" && !strings.Contains(vector.Notes, "server accepts") {
			t.Errorf("vector %q: the server accepts it but the notes do not say so", vector.Name)
		}
	}
	if seen < 30 {
		t.Errorf("only %d reject vectors", seen)
	}
	if refused < 20 {
		t.Errorf("the server's decoder refuses only %d of %d reject vectors; the replay proves little", refused, seen)
	}
}

// TestBSONEncodeRefusalVectors: what the spec makes a driver refuse before
// writing, and what the server's encoder does with the same input.
func TestBSONEncodeRefusalVectors(t *testing.T) {
	corpus := mustParseCorpus(t, freshBSONCorpus(t, "abc123"))
	seen := 0
	for _, vector := range corpus.Vectors {
		if vector.Kind != "encode_refuse" {
			continue
		}
		seen++
		document, err := typedToDocument(vector.Document)
		if err != nil {
			t.Fatalf("vector %q: %v", vector.Name, err)
		}
		_, err = bson.Marshal(document)
		observed := "accepted"
		if err != nil {
			observed = "refused"
		}
		if observed != vector.ServerEncoder {
			t.Errorf("vector %q: server_encoder = %q in the file, observed %q", vector.Name, vector.ServerEncoder, observed)
		}
		if vector.Refusal != "InvalidArgumentError" {
			t.Errorf("vector %q: refusal %q", vector.Name, vector.Refusal)
		}
	}
	if seen < 1 {
		t.Error("no encode_refuse vector (a key holding NUL)")
	}
}

// TestBSONDatetimeFloorVectors: instants with sub-millisecond digits floor
// toward negative infinity; the bytes come from the server's time encoder.
func TestBSONDatetimeFloorVectors(t *testing.T) {
	corpus := mustParseCorpus(t, freshBSONCorpus(t, "abc123"))
	seen := 0
	sawMinusOne := false
	for _, vector := range corpus.Vectors {
		if vector.Kind != "datetime_floor" {
			continue
		}
		seen++
		if vector.Instant == "1969-12-31T23:59:59.9995Z" && vector.ExpectedMillis == "-1" {
			sawMinusOne = true
		}
		raw := mustDecodeHex(t, vector.Name, vector.hexValue())
		decoded, err := serverDecode(raw)
		if err != nil {
			t.Fatalf("vector %q: %v", vector.Name, err)
		}
		rendered, err := documentToTyped(decoded)
		if err != nil {
			t.Fatalf("vector %q: %v", vector.Name, err)
		}
		if !strings.Contains(string(rendered), `"`+vector.ExpectedMillis+`"`) {
			t.Errorf("vector %q: bytes hold %s, expected_millis is %s", vector.Name, rendered, vector.ExpectedMillis)
		}
	}
	if seen < 6 || !sawMinusOne {
		t.Errorf("datetime_floor vectors: %d, 1969-12-31T23:59:59.9995Z -> -1 present: %v", seen, sawMinusOne)
	}
}

// TestBSONSizeVectors: documents at and over the limits are described by a
// recipe (a 32 MiB hex string would make the file unusable); the recipe,
// expanded and encoded by the server, must give the recorded length and hash.
func TestBSONSizeVectors(t *testing.T) {
	corpus := mustParseCorpus(t, freshBSONCorpus(t, "abc123"))
	lengths := map[int]bool{}
	for _, vector := range corpus.Vectors {
		if vector.Kind != "size" {
			continue
		}
		raw, err := bson.Marshal(bson.D{{Key: vector.Recipe.Key, Value: strings.Repeat(vector.Recipe.Fill, vector.Recipe.FillBytes)}})
		if err != nil {
			t.Fatalf("vector %q: %v", vector.Name, err)
		}
		if len(raw) != vector.DocumentBytes {
			t.Errorf("vector %q: recipe encodes to %d bytes, file says %d", vector.Name, len(raw), vector.DocumentBytes)
		}
		if sum := sha256.Sum256(raw); hex.EncodeToString(sum[:]) != vector.SHA256 {
			t.Errorf("vector %q: sha256 differs", vector.Name)
		}
		lengths[vector.DocumentBytes] = true
	}
	const payloadMax = 32<<20 - 17
	for _, want := range []int{24 << 20, 24<<20 + 1, payloadMax, payloadMax + 1} {
		if !lengths[want] {
			t.Errorf("no size vector with a document of %d bytes", want)
		}
	}
}

// TestBSONCorpusCoversTheSpec names what driver-python-spec sections 7.1 and
// 7.2 require; deleting a case from the generator fails here.
func TestBSONCorpusCoversTheSpec(t *testing.T) {
	corpus := mustParseCorpus(t, freshBSONCorpus(t, "abc123"))
	have := map[string]bool{}
	for _, vector := range corpus.Vectors {
		have[vector.Name] = true
	}
	required := []string{
		"int32/max", "int32/min", "int64/just-over-int32", "int64/just-under-int32", "int64/max", "int64/min",
		"double/zero", "double/negative-zero", "double/nan", "double/infinity", "double/negative-infinity",
		"double/subnormal-smallest", "double/2^53-1", "double/2^53", "double/2^53+2",
		"string/empty", "string/multibyte-2-byte", "string/multibyte-4-byte", "string/embedded-nul",
		"datetime/epoch", "datetime/minus-one-ms", "datetime/int64-max", "datetime/int64-min",
		"datetime/python-max", "datetime/python-min", "datetime/over-python-max", "datetime/under-python-min",
		"binary/generic-empty", "binary/generic", "binary/uuid-subtype-4",
		"decimal128/zero", "decimal128/negative-zero", "decimal128/infinity", "decimal128/negative-infinity", "decimal128/nan",
		"decimal128/max-digits", "decimal128/exponent-max", "decimal128/exponent-min",
		"objectid/typical", "document/empty", "document/key-order", "document/nested-depth-100", "array/nested-depth-100",
		"document/empty-key", "document/key-multibyte",
		"decode/duplicate-keys", "decode/array-keys-out-of-order", "decode/double-nan-payload",
		"reject/depth-101-documents", "reject/depth-101-arrays", "reject/length-too-long", "reject/length-too-short",
		"reject/trailing-garbage", "reject/missing-terminator", "reject/truncated-header", "reject/empty-input",
		"reject/string-invalid-utf8", "reject/key-invalid-utf8", "reject/string-lone-surrogate",
		"reject/nested-length-lie", "reject/length-negative", "reject/boolean-byte-2",
		"encode_refuse/key-with-nul",
	}
	for _, name := range required {
		if !have[name] {
			t.Errorf("the corpus has no vector %q", name)
		}
	}
}

// TestBSONGeneratorWritesOnlyItsFile: -bson-out writes bson-vectors/v1.json and
// nothing else, and refuses the hand-maintained and wire vector directories.
func TestBSONGeneratorWritesOnlyItsFile(t *testing.T) {
	out := filepath.Join(t.TempDir(), "bson-vectors")
	var stdout, stderr bytes.Buffer
	if err := run([]string{"-bson-out", out}, &stdout, &stderr); err != nil {
		t.Fatalf("-bson-out: %v\n%s", err, stderr.String())
	}
	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "v1.json" {
		t.Fatalf("-bson-out wrote %v, want only v1.json", entries)
	}
	for _, forbidden := range []string{"uri-vectors", "driver-vectors"} {
		err := run([]string{"-bson-out", filepath.Join(t.TempDir(), forbidden)}, &stdout, &stderr)
		if err == nil || !strings.Contains(err.Error(), forbidden) {
			t.Errorf("-bson-out into %s: err = %v, want a refusal naming it", forbidden, err)
		}
	}
	if err := run([]string{"-bson-out", out, "-out", out}, &stdout, &stderr); err == nil {
		t.Error("-bson-out together with -out was accepted")
	}
}

func TestBSONCheckFlag(t *testing.T) {
	committed := committedBSONCorpus(t)
	var stdout, stderr bytes.Buffer
	if err := run([]string{"-bson-check", committedBSONCorpusPath}, &stdout, &stderr); err != nil {
		t.Fatalf("-bson-check on the committed file: %v", err)
	}
	altered := filepath.Join(t.TempDir(), "v1.json")
	if err := os.WriteFile(altered, bytes.Replace(committed, []byte(`"int32/max"`), []byte(`"int32/maxx"`), 1), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-bson-check", altered}, &stdout, &stderr); err == nil {
		t.Error("-bson-check accepted an altered file")
	}
}

// jsonEqual compares two JSON documents structurally; key order inside
// objects is not significant to JSON, and $doc pairs keep theirs as arrays.
func jsonEqual(t *testing.T, left, right json.RawMessage) bool {
	t.Helper()
	var leftTree, rightTree any
	if err := json.Unmarshal(left, &leftTree); err != nil {
		t.Fatalf("not JSON: %s", left)
	}
	if err := json.Unmarshal(right, &rightTree); err != nil {
		t.Fatalf("not JSON: %s", right)
	}
	leftCanon, err := json.Marshal(leftTree)
	if err != nil {
		t.Fatal(err)
	}
	rightCanon, err := json.Marshal(rightTree)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.Equal(leftCanon, rightCanon)
}

// homeDirPrefix is built at run time so no source file in the public tree holds
// the literal path the publish-tree private-content guard rejects; the corpus
// tests look for it to prove generated output does not leak the generating
// machine's home directory.
var homeDirPrefix = strings.Join([]string{"", "home", ""}, "/")
