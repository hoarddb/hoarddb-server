package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"
)

// The BSON corpus: clients/testdata/bson-vectors/v1.json. Bytes the server's
// own BSON package wrote (the package every wire frame goes through), for the
// in-tree codecs of the Python and C++ drivers to be held to byte for byte
// (the driver-python-spec record section 7.1, OQ-3; the driver-spec record
// section 4.1). Nothing here is hand-written bytes except the reject inputs,
// which are malformed on purpose and whose outcome the server's decoder is
// asked about. The file is data, licensed Apache-2.0 under clients/; this
// generator imports server code and stays in the server's tree.
//
//	go run ./cmd/gen-testvectors -bson-out ../clients/testdata/bson-vectors
//	go run ./cmd/gen-testvectors -bson-check ../clients/testdata/bson-vectors/v1.json

// bsonCorpusFormat is the file's format word. A breaking change to the shape
// is a new word and a new file, never an edit of this one.
const bsonCorpusFormat = "hoarddb-bson-vectors/1"

// bsonCorpusFileName is the one file -bson-out writes.
const bsonCorpusFileName = "v1.json"

// bsonCorpusMaxDepth is the nesting limit of driver-python-spec section 7.2
// (the root document has depth 1; each embedded document or array adds 1).
const bsonCorpusMaxDepth = 100

// frameBodyMax and requestHeaderBytes give the largest payload a frame holds:
// opcode (1) + request id (16) + payload <= 32 MiB (transport.MaxFrameLength).
const (
	frameBodyMax       = 32 << 20
	frameHeaderBytes   = 17
	defaultDocumentMax = 24 << 20
	documentCeiling    = 30 << 20
)

// bsonCorpusFile is the whole file.
type bsonCorpusFile struct {
	Format       string            `json:"format"`
	License      string            `json:"license"`
	ServerCommit string            `json:"server_commit"`
	GeneratedBy  string            `json:"generated_by"`
	Codec        string            `json:"codec"`
	Limits       bsonCorpusLimits  `json:"limits"`
	ValueForms   map[string]string `json:"value_forms"`
	Kinds        map[string]string `json:"kinds"`
	Vectors      []bsonVector      `json:"vectors"`
}

// bsonCorpusLimits are the numbers the size and depth vectors are built on.
type bsonCorpusLimits struct {
	MaxDepth             int    `json:"max_depth"`
	DepthRule            string `json:"depth_rule"`
	FramePayloadMax      int    `json:"frame_payload_max_bytes"`
	DefaultDocumentLimit int    `json:"default_document_limit_bytes"`
	DocumentCeiling      int    `json:"document_ceiling_bytes"`
}

// bsonVector is one entry. Kind decides which members are set.
type bsonVector struct {
	Name  string `json:"name"`
	Group string `json:"group"`
	Kind  string `json:"kind"`
	Notes string `json:"notes,omitempty"`

	// encode, decode, encode_refuse: the typed document.
	Document json.RawMessage `json:"document,omitempty"`
	// encode, decode, reject, datetime_floor: the bytes, lowercase hex.
	Hex *string `json:"hex,omitempty"`

	// decode
	ReencodesIdentically *bool           `json:"reencodes_identically,omitempty"`
	LastWinsDocument     json.RawMessage `json:"last_wins_document,omitempty"`

	// reject
	Refusal       string `json:"refusal,omitempty"`
	Reason        string `json:"reason,omitempty"`
	ServerDecoder string `json:"server_decoder,omitempty"`

	// encode_refuse
	ServerEncoder string `json:"server_encoder,omitempty"`

	// datetime_floor
	Instant        string `json:"instant,omitempty"`
	ExpectedMillis string `json:"expected_millis,omitempty"`

	// size
	Recipe                     *sizeRecipe `json:"recipe,omitempty"`
	DocumentBytes              int         `json:"document_bytes,omitempty"`
	SHA256                     string      `json:"sha256,omitempty"`
	FitsOneFrame               *bool       `json:"fits_one_frame,omitempty"`
	WithinDefaultDocumentLimit *bool       `json:"within_default_document_limit,omitempty"`
}

// sizeRecipe builds a one-field document {key: fill repeated fill_bytes times}.
type sizeRecipe struct {
	Key       string `json:"key"`
	Fill      string `json:"fill"`
	FillBytes int    `json:"fill_bytes"`
}

// hexValue is the vector's bytes as hex, "" when it has none.
func (v bsonVector) hexValue() string {
	if v.Hex == nil {
		return ""
	}
	return *v.Hex
}

func bsonHexOf(raw []byte) *string {
	encoded := hex.EncodeToString(raw)
	return &encoded
}

// ---- the server's codec, used both ways -----------------------------------

// serverDecode is the server's decoder: what a document goes through on
// receipt. The server validates the raw bytes first, at the nesting bound
// (bson.Validate; server/nesting_depth.go:validateAt at maxDocumentDepth, the
// corpus's bsonCorpusMaxDepth), then decodes them (bson.Unmarshal;
// transport/driver_client.go, transport/serve.go).
func serverDecode(raw []byte) (bson.D, error) {
	if _, err := bson.Validate(raw, bson.Limits{Depth: bsonCorpusMaxDepth}); err != nil {
		return nil, fmt.Errorf("validating %d bytes: %w", len(raw), err)
	}
	var decoded bson.D
	if err := bson.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("decoding %d bytes: %w", len(raw), err)
	}
	return decoded, nil
}

// ---- the typed form --------------------------------------------------------

// bsonValueForms documents the typed JSON form in the file's own header, so a
// driver's test can build every value without a BSON library.
func bsonValueForms() map[string]string {
	return map[string]string{
		"$doc":               `a document: {"$doc": [[key, value], ...]}, an ordered list of [key, value] pairs. Order is the wire order and a key may repeat (decode/duplicate-keys); a plain JSON object is never a document in this file.`,
		"array":              "an array is a JSON array of typed values.",
		"string":             "a string is a JSON string (\\u0000 is a NUL character). Encode vectors hold valid Unicode only.",
		"bool":               "true or false.",
		"null":               "JSON null is BSON null.",
		"$numberInt":         `{"$numberInt": "-5"}: BSON int32, a decimal string.`,
		"$numberLong":        `{"$numberLong": "-5"}: BSON int64, a decimal string. An int64 stays int64 even when the number fits int32.`,
		"$numberDouble":      `{"$numberDouble": "1.5"}: BSON double, the shortest decimal that round-trips, or "NaN" (the quiet NaN 0x7ff8000000000000), "Infinity", "-Infinity", "-0" (negative zero). Parse with a correctly rounded parser.`,
		"$numberDecimal":     `{"$numberDecimal": "1.50"}: BSON decimal128 (BID), as the canonical decimal string; the exponent and trailing zeros are part of the value ("1.50" and "1.5" are different bytes). "NaN", "Infinity", "-Infinity" and "-0" are legal.`,
		"$date":              `{"$date": {"$numberLong": "-1"}}: BSON datetime, milliseconds since 1970-01-01T00:00:00Z, signed.`,
		"$oid":               `{"$oid": "507f1f77bcf86cd799439011"}: BSON ObjectId, 24 lowercase hex digits.`,
		"$binary":            `{"$binary": {"hex": "00ff", "subType": "04"}}: BSON binary; subType is two hex digits; hex is the payload ("" for empty).`,
		"$timestamp":         `{"$timestamp": {"t": 1, "i": 2}}: BSON timestamp (unsigned 32-bit t and i).`,
		"$regularExpression": `{"$regularExpression": {"pattern": "a", "options": "i"}}: BSON regex, options as written.`,
		"$code":              `{"$code": "x"}: BSON JavaScript code.`,
		"$codeWithScope":     `{"$codeWithScope": {"code": "x", "scope": {"$doc": []}}}: BSON JavaScript with scope.`,
		"$symbol":            `{"$symbol": "x"}: BSON symbol (deprecated).`,
		"$undefined":         `{"$undefined": true}: BSON undefined (deprecated).`,
		"$minKey":            `{"$minKey": 1}: BSON min key.`,
		"$maxKey":            `{"$maxKey": 1}: BSON max key.`,
		"$dbPointer":         `{"$dbPointer": {"namespace": "ns", "oid": "507f1f77bcf86cd799439011"}}: BSON DBPointer (deprecated).`,
	}
}

func bsonKinds() map[string]string {
	return map[string]string{
		"encode":         `document -> hex. The codec must write exactly these bytes for the typed document, and read them back to the same typed document.`,
		"decode":         `hex -> document. Bytes the server's decoder accepts, read to the typed document; reencodes_identically says whether writing that document gives the same bytes (false for a non-canonical input). last_wins_document, when present, is the document as a mapping that keeps the last of a repeated key (position of the first).`,
		"reject":         `hex -> refusal. A decoder must refuse the bytes with the refusal class named (ProtocolError, driver-python-spec section 7.2), never crash, hang or raise a language-level exception. server_decoder records what the server's own decoder does with the same bytes: "accepted" marks an input the spec is stricter about than the server.`,
		"encode_refuse":  `document -> refusal. An encoder must refuse to write the typed document (InvalidArgumentError, before any byte is written). server_encoder records what the server's encoder does.`,
		"datetime_floor": `instant -> expected_millis. A time with sub-millisecond digits becomes milliseconds by flooring toward negative infinity (never truncating toward zero); hex is the document {"t": <that datetime>} the server's time encoder wrote.`,
		"size":           `recipe -> document_bytes, sha256. A document too large to list as hex: build {key: fill repeated fill_bytes times}, encode it, and the bytes must have this length and SHA-256. fits_one_frame and within_default_document_limit say where it sits against the limits.`,
	}
}

type typedNode = any

// documentToTyped renders a document the server's codec produced (or will
// produce) in the typed form.
func documentToTyped(document bson.D) (json.RawMessage, error) {
	node, err := nodeOf(document)
	if err != nil {
		return nil, err
	}
	return marshalNode(node)
}

func marshalNode(node typedNode) (json.RawMessage, error) {
	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(node); err != nil {
		return nil, fmt.Errorf("encoding a typed value: %w", err)
	}
	return json.RawMessage(bytes.TrimRight(out.Bytes(), "\n")), nil
}

func bsonTag(name string, body any) map[string]any { return map[string]any{name: body} }

// nodeOf maps one Go value the server's codec uses to its typed form.
func nodeOf(field any) (typedNode, error) {
	switch typed := field.(type) {
	case nil:
		return nil, nil
	case string:
		return typed, nil
	case bool:
		return typed, nil
	case int32:
		return bsonTag("$numberInt", strconv.FormatInt(int64(typed), 10)), nil
	case int64:
		return bsonTag("$numberLong", strconv.FormatInt(typed, 10)), nil
	case float64:
		return bsonTag("$numberDouble", formatDouble(typed)), nil
	case primitive.Decimal128:
		return bsonTag("$numberDecimal", typed.String()), nil
	case primitive.DateTime:
		return bsonTag("$date", bsonTag("$numberLong", strconv.FormatInt(int64(typed), 10))), nil
	case primitive.ObjectID:
		return bsonTag("$oid", typed.Hex()), nil
	case primitive.Binary:
		return bsonTag("$binary", map[string]any{"hex": hex.EncodeToString(typed.Data), "subType": fmt.Sprintf("%02x", typed.Subtype)}), nil
	case primitive.Timestamp:
		return bsonTag("$timestamp", map[string]any{"t": typed.T, "i": typed.I}), nil
	case primitive.Regex:
		return bsonTag("$regularExpression", map[string]any{"pattern": typed.Pattern, "options": typed.Options}), nil
	case primitive.JavaScript:
		return bsonTag("$code", string(typed)), nil
	case primitive.CodeWithScope:
		scope, err := nodeOf(typed.Scope)
		if err != nil {
			return nil, err
		}
		return bsonTag("$codeWithScope", map[string]any{"code": string(typed.Code), "scope": scope}), nil
	case primitive.Symbol:
		return bsonTag("$symbol", string(typed)), nil
	case primitive.Undefined:
		return bsonTag("$undefined", true), nil
	case primitive.MinKey:
		return bsonTag("$minKey", 1), nil
	case primitive.MaxKey:
		return bsonTag("$maxKey", 1), nil
	case primitive.DBPointer:
		return bsonTag("$dbPointer", map[string]any{"namespace": typed.DB, "oid": typed.Pointer.Hex()}), nil
	case bson.D:
		pairs := make([]any, 0, len(typed))
		for _, entry := range typed {
			member, err := nodeOf(entry.Value)
			if err != nil {
				return nil, fmt.Errorf("key %q: %w", entry.Key, err)
			}
			pairs = append(pairs, []any{entry.Key, member})
		}
		return bsonTag("$doc", pairs), nil
	case bson.A:
		members := make([]any, 0, len(typed))
		for position, element := range typed {
			member, err := nodeOf(element)
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", position, err)
			}
			members = append(members, member)
		}
		return members, nil
	default:
		return nil, fmt.Errorf("no typed form for %T", field)
	}
}

// formatDouble is the shortest decimal that parses back to the same double.
func formatDouble(number float64) string {
	switch {
	case math.IsNaN(number):
		return "NaN"
	case math.IsInf(number, 1):
		return "Infinity"
	case math.IsInf(number, -1):
		return "-Infinity"
	case number == 0 && math.Signbit(number):
		return "-0"
	}
	return strconv.FormatFloat(number, 'g', -1, 64)
}

// typedToDocument is the inverse of documentToTyped: it proves the typed form
// carries every byte (a driver does the same without a BSON library).
func typedToDocument(typed json.RawMessage) (bson.D, error) {
	var tree any
	decoder := json.NewDecoder(bytes.NewReader(typed))
	if err := decoder.Decode(&tree); err != nil {
		return nil, fmt.Errorf("typed form is not JSON: %w", err)
	}
	built, err := valueOfNode(tree)
	if err != nil {
		return nil, err
	}
	document, ok := built.(bson.D)
	if !ok {
		return nil, fmt.Errorf("the typed form is a %T, not a document", built)
	}
	return document, nil
}

// valueOfNode maps a decoded typed tree back to the Go value the codec writes.
func valueOfNode(node any) (any, error) {
	switch typed := node.(type) {
	case nil, string, bool:
		return typed, nil
	case []any:
		members := make(bson.A, 0, len(typed))
		for position, element := range typed {
			member, err := valueOfNode(element)
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", position, err)
			}
			members = append(members, member)
		}
		return members, nil
	case map[string]any:
		if len(typed) != 1 {
			return nil, fmt.Errorf("a tagged value has exactly one key, got %d", len(typed))
		}
		for name, body := range typed {
			return valueOfTag(name, body)
		}
	}
	return nil, fmt.Errorf("unexpected %T in a typed form", node)
}

func textOf(body any, what string) (string, error) {
	text, ok := body.(string)
	if !ok {
		return "", fmt.Errorf("%s must be a string, got %T", what, body)
	}
	return text, nil
}

func membersOf(body any, what string) (map[string]any, error) {
	members, ok := body.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an object, got %T", what, body)
	}
	return members, nil
}

// valueOfTag builds the Go value for one {"$tag": body} form.
func valueOfTag(name string, body any) (any, error) {
	switch name {
	case "$doc":
		pairs, ok := body.([]any)
		if !ok {
			return nil, fmt.Errorf("$doc must be a list of pairs, got %T", body)
		}
		document := make(bson.D, 0, len(pairs))
		for position, pair := range pairs {
			couple, ok := pair.([]any)
			if !ok || len(couple) != 2 {
				return nil, fmt.Errorf("$doc pair %d is not [key, value]", position)
			}
			key, err := textOf(couple[0], "a key")
			if err != nil {
				return nil, err
			}
			member, err := valueOfNode(couple[1])
			if err != nil {
				return nil, fmt.Errorf("key %q: %w", key, err)
			}
			document = append(document, bson.E{Key: key, Value: member})
		}
		return document, nil
	case "$numberInt":
		text, err := textOf(body, name)
		if err != nil {
			return nil, err
		}
		parsed, err := strconv.ParseInt(text, 10, 32)
		return int32(parsed), err
	case "$numberLong":
		text, err := textOf(body, name)
		if err != nil {
			return nil, err
		}
		return strconv.ParseInt(text, 10, 64)
	case "$numberDouble":
		text, err := textOf(body, name)
		if err != nil {
			return nil, err
		}
		if text == "NaN" {
			return math.Float64frombits(0x7ff8000000000000), nil
		}
		return strconv.ParseFloat(text, 64)
	case "$numberDecimal":
		text, err := textOf(body, name)
		if err != nil {
			return nil, err
		}
		return primitive.ParseDecimal128(text)
	case "$date":
		inner, err := membersOf(body, name)
		if err != nil {
			return nil, err
		}
		text, err := textOf(inner["$numberLong"], "$date.$numberLong")
		if err != nil {
			return nil, err
		}
		millis, err := strconv.ParseInt(text, 10, 64)
		return primitive.DateTime(millis), err
	case "$oid":
		text, err := textOf(body, name)
		if err != nil {
			return nil, err
		}
		return primitive.ObjectIDFromHex(text)
	case "$binary":
		inner, err := membersOf(body, name)
		if err != nil {
			return nil, err
		}
		payloadHex, err := textOf(inner["hex"], "$binary.hex")
		if err != nil {
			return nil, err
		}
		subtypeHex, err := textOf(inner["subType"], "$binary.subType")
		if err != nil {
			return nil, err
		}
		payload, err := hex.DecodeString(payloadHex)
		if err != nil {
			return nil, fmt.Errorf("$binary.hex: %w", err)
		}
		subtype, err := strconv.ParseUint(subtypeHex, 16, 8)
		return primitive.Binary{Subtype: byte(subtype), Data: payload}, err
	case "$timestamp":
		inner, err := membersOf(body, name)
		if err != nil {
			return nil, err
		}
		seconds, okSeconds := inner["t"].(float64)
		counter, okCounter := inner["i"].(float64)
		if !okSeconds || !okCounter {
			return nil, fmt.Errorf("$timestamp needs numeric t and i")
		}
		return primitive.Timestamp{T: uint32(seconds), I: uint32(counter)}, nil
	case "$regularExpression":
		inner, err := membersOf(body, name)
		if err != nil {
			return nil, err
		}
		pattern, err := textOf(inner["pattern"], "pattern")
		if err != nil {
			return nil, err
		}
		options, err := textOf(inner["options"], "options")
		return primitive.Regex{Pattern: pattern, Options: options}, err
	case "$code":
		text, err := textOf(body, name)
		return primitive.JavaScript(text), err
	case "$codeWithScope":
		inner, err := membersOf(body, name)
		if err != nil {
			return nil, err
		}
		code, err := textOf(inner["code"], "code")
		if err != nil {
			return nil, err
		}
		scope, err := valueOfNode(inner["scope"])
		return primitive.CodeWithScope{Code: primitive.JavaScript(code), Scope: scope}, err
	case "$symbol":
		text, err := textOf(body, name)
		return primitive.Symbol(text), err
	case "$undefined":
		return primitive.Undefined{}, nil
	case "$minKey":
		return primitive.MinKey{}, nil
	case "$maxKey":
		return primitive.MaxKey{}, nil
	case "$dbPointer":
		inner, err := membersOf(body, name)
		if err != nil {
			return nil, err
		}
		namespace, err := textOf(inner["namespace"], "namespace")
		if err != nil {
			return nil, err
		}
		oidHex, err := textOf(inner["oid"], "oid")
		if err != nil {
			return nil, err
		}
		pointer, err := primitive.ObjectIDFromHex(oidHex)
		return primitive.DBPointer{DB: namespace, Pointer: pointer}, err
	}
	return nil, fmt.Errorf("unknown tag %q", name)
}

// ---- generation ------------------------------------------------------------

// generateBSONCorpus builds the whole file. It reads no clock, no random
// source and no map in iteration order, so equal inputs give equal bytes.
func generateBSONCorpus(commit string) ([]byte, error) {
	var vectors []bsonVector
	builders := []func() ([]bsonVector, error){
		bsonEncodeVectors, bsonDecodeVectors, bsonRejectVectors, bsonEncodeRefuseVectors, bsonDatetimeFloorVectors, bsonSizeVectors,
	}
	for _, build := range builders {
		built, err := build()
		if err != nil {
			return nil, err
		}
		vectors = append(vectors, built...)
	}
	seen := make(map[string]bool, len(vectors))
	for _, vector := range vectors {
		if seen[vector.Name] {
			return nil, fmt.Errorf("two BSON vectors are named %q", vector.Name)
		}
		seen[vector.Name] = true
	}
	header := bsonCorpusFile{
		Format:       bsonCorpusFormat,
		License:      "Apache-2.0",
		ServerCommit: commit,
		GeneratedBy:  "github.com/hoarddb/hoarddb-server/cmd/gen-testvectors -bson-out (make bson-vectors)",
		Codec:        "the BSON codec the server is built with (the encoder and decoder every wire frame passes through)",
		Limits: bsonCorpusLimits{
			MaxDepth:             bsonCorpusMaxDepth,
			DepthRule:            "the root document has depth 1; each embedded document or array adds 1; a decoder refuses depth 101 (driver-python-spec section 7.2)",
			FramePayloadMax:      frameBodyMax - frameHeaderBytes,
			DefaultDocumentLimit: defaultDocumentMax,
			DocumentCeiling:      documentCeiling,
		},
		ValueForms: bsonValueForms(),
		Kinds:      bsonKinds(),
	}
	return encodeBSONCorpus(header, vectors)
}

// encodeBSONCorpus writes the header indented and each vector on one line,
// so a diff of the file reads one vector per line.
func encodeBSONCorpus(header bsonCorpusFile, vectors []bsonVector) ([]byte, error) {
	var headerText bytes.Buffer
	encoder := json.NewEncoder(&headerText)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(header); err != nil {
		return nil, fmt.Errorf("encoding the corpus header: %w", err)
	}
	text := headerText.String()
	const emptyVectors = "\"vectors\": null\n}\n"
	cut := strings.LastIndex(text, emptyVectors)
	if cut < 0 {
		return nil, fmt.Errorf("the corpus header does not end with its empty vector list")
	}
	var out bytes.Buffer
	out.WriteString(text[:cut])
	out.WriteString("\"vectors\": [\n")
	for position, vector := range vectors {
		var line bytes.Buffer
		lineEncoder := json.NewEncoder(&line)
		lineEncoder.SetEscapeHTML(false)
		if err := lineEncoder.Encode(vector); err != nil {
			return nil, fmt.Errorf("encoding vector %q: %w", vector.Name, err)
		}
		out.WriteString("    ")
		out.Write(bytes.TrimRight(line.Bytes(), "\n"))
		if position < len(vectors)-1 {
			out.WriteString(",")
		}
		out.WriteString("\n")
	}
	out.WriteString("  ]\n}\n")
	return out.Bytes(), nil
}

// decodeBSONCorpus reads a file written by encodeBSONCorpus.
func decodeBSONCorpus(encoded []byte) (bsonCorpusFile, error) {
	var corpus bsonCorpusFile
	if err := json.Unmarshal(encoded, &corpus); err != nil {
		return bsonCorpusFile{}, fmt.Errorf("decoding the BSON corpus: %w", err)
	}
	return corpus, nil
}

// compareBSONCorpora lists how generated differs from committed, by vector
// name, ignoring server_commit (it names the commit the file was last
// generated at, which every later commit changes).
func compareBSONCorpora(committed, generated []byte) ([]string, error) {
	before, err := decodeBSONCorpus(committed)
	if err != nil {
		return nil, fmt.Errorf("the committed file: %w", err)
	}
	after, err := decodeBSONCorpus(generated)
	if err != nil {
		return nil, fmt.Errorf("the generated file: %w", err)
	}
	var differences []string
	oldByName := make(map[string]string, len(before.Vectors))
	for _, vector := range before.Vectors {
		line, err := json.Marshal(vector)
		if err != nil {
			return nil, err
		}
		oldByName[vector.Name] = string(line)
	}
	newNames := make(map[string]bool, len(after.Vectors))
	for _, vector := range after.Vectors {
		newNames[vector.Name] = true
		line, err := json.Marshal(vector)
		if err != nil {
			return nil, err
		}
		old, existed := oldByName[vector.Name]
		switch {
		case !existed:
			differences = append(differences, fmt.Sprintf("vector %q is new", vector.Name))
		case old != string(line):
			differences = append(differences, fmt.Sprintf("vector %q changed", vector.Name))
		}
	}
	for _, vector := range before.Vectors {
		if !newNames[vector.Name] {
			differences = append(differences, fmt.Sprintf("vector %q is gone", vector.Name))
		}
	}
	if len(differences) == 0 {
		adjusted := strings.Replace(string(generated), `"server_commit": "`+after.ServerCommit+`"`, `"server_commit": "`+before.ServerCommit+`"`, 1)
		if adjusted != string(committed) {
			differences = append(differences, "the header or the layout differs: regenerate")
		}
	}
	return differences, nil
}

// runBSONCorpus is the -bson-out and -bson-check mode of the command.
func runBSONCorpus(outDir, checkPath string, stdout io.Writer) error {
	if outDir != "" {
		if err := refuseOtherVectorDirs(outDir); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	generated, err := generateBSONCorpus(serverCommit(ctx))
	if err != nil {
		return err
	}
	if checkPath != "" {
		committed, err := os.ReadFile(checkPath)
		if err != nil {
			return fmt.Errorf("reading the committed BSON corpus: %w", err)
		}
		differences, err := compareBSONCorpora(committed, generated)
		if err != nil {
			return err
		}
		if len(differences) > 0 {
			return fmt.Errorf("%s does not match the server's encoder at this commit:\n  %s\nregenerate with 'make bson-vectors' and review the diff: a byte that changes is a change to every in-tree codec",
				checkPath, joinLines(differences))
		}
		_, err = fmt.Fprintf(stdout, "%s matches the server's encoder\n", checkPath)
		return err
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("creating %s: %w", outDir, err)
	}
	target := filepath.Join(outDir, bsonCorpusFileName)
	if err := os.WriteFile(target, generated, 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", target, err)
	}
	_, err = fmt.Fprintf(stdout, "wrote %s (%d bytes)\n", target, len(generated))
	return err
}

// refuseOtherVectorDirs keeps -bson-out from overwriting the hand-maintained
// URI vectors, the wire vectors or the trust vectors, one mistyped flag away.
func refuseOtherVectorDirs(outDir string) error {
	resolved, err := filepath.Abs(outDir)
	if err != nil {
		return fmt.Errorf("resolving -bson-out %q: %w", outDir, err)
	}
	for _, other := range []string{"uri-vectors", "driver-vectors", "trust-vectors"} {
		if strings.Contains(resolved, other) {
			return fmt.Errorf("refusing -bson-out %s: clients/testdata/%s is not this generator mode's file; use -bson-out clients/testdata/bson-vectors", outDir, other)
		}
	}
	return nil
}

// ---- the size vectors: built once, they are tens of megabytes --------------

var (
	sizeVectorsOnce  sync.Once
	sizeVectorsBuilt []bsonVector
	sizeVectorsErr   error
)

// recipeOverhead is the bytes around the fill in {k: "<fill>"}: length (4),
// type (1), "k\0" (2), string length (4), the string's NUL (1), terminator (1).
const recipeOverhead = 13

func bsonSizeVectors() ([]bsonVector, error) {
	sizeVectorsOnce.Do(func() {
		lengths := []struct {
			name   string
			length int
			notes  string
		}{
			{"size/document-24-mib", defaultDocumentMax, "exactly the default document limit (24 MiB)."},
			{"size/document-24-mib-plus-1", defaultDocumentMax + 1, "one byte over the default document limit: the codec writes it, the server refuses it."},
			{"size/document-30-mib", documentCeiling, "exactly the configurable document ceiling (30 MiB)."},
			{"size/document-30-mib-plus-1", documentCeiling + 1, "one byte over the ceiling."},
			{"size/document-frame-max", frameBodyMax - frameHeaderBytes, "the largest payload one frame holds (32 MiB minus opcode and request id): a driver sends it."},
			{"size/document-frame-max-plus-1", frameBodyMax - frameHeaderBytes + 1, "one byte over: a driver refuses it before sending (FrameTooLargeError, driver-python-spec section 7.3)."},
		}
		for _, entry := range lengths {
			recipe := sizeRecipe{Key: "k", Fill: "a", FillBytes: entry.length - recipeOverhead}
			raw, err := bson.Marshal(bson.D{{Key: recipe.Key, Value: strings.Repeat(recipe.Fill, recipe.FillBytes)}})
			if err != nil {
				sizeVectorsErr = fmt.Errorf("vector %s: %w", entry.name, err)
				return
			}
			if len(raw) != entry.length {
				sizeVectorsErr = fmt.Errorf("vector %s: recipe encodes to %d bytes, want %d", entry.name, len(raw), entry.length)
				return
			}
			sum := sha256.Sum256(raw)
			sizeVectorsBuilt = append(sizeVectorsBuilt, bsonVector{
				Name: entry.name, Group: "size", Kind: "size", Notes: entry.notes,
				Recipe: &recipe, DocumentBytes: len(raw), SHA256: hex.EncodeToString(sum[:]),
				FitsOneFrame:               boolPointer(len(raw) <= frameBodyMax-frameHeaderBytes),
				WithinDefaultDocumentLimit: boolPointer(len(raw) <= defaultDocumentMax),
			})
		}
	})
	return sizeVectorsBuilt, sizeVectorsErr
}

// ---- datetime floor --------------------------------------------------------

func bsonDatetimeFloorVectors() ([]bsonVector, error) {
	instants := []struct {
		instant string
		millis  int64
		notes   string
	}{
		{"1970-01-01T00:00:00Z", 0, "the epoch."},
		{"1970-01-01T00:00:00.0009999Z", 0, "99.99 microseconds short of 1 ms floors to 0."},
		{"1970-01-01T00:00:00.001Z", 1, ""},
		{"1969-12-31T23:59:59.999Z", -1, "one millisecond before the epoch is -1."},
		{"1969-12-31T23:59:59.9995Z", -1, "the pre-epoch case: floor gives -1, truncation toward zero gives 0 (a one-millisecond error for every pre-1970 instant)."},
		{"1969-12-31T23:59:59.9990001Z", -1, ""},
		{"1969-12-31T23:59:59.9989999Z", -2, "floors down, not toward the epoch."},
		{"1969-12-31T23:59:59.0005Z", -1000, "-999.5 ms floors to -1000."},
		{"1969-12-31T23:59:58.9999999Z", -1001, "-1000.0001 ms floors to -1001."},
		{"2026-10-13T12:34:56.7895Z", 1791894896789, "after the epoch the floor is the truncation."},
		{"1970-01-01T01:00:00+01:00", 0, "a zone offset is converted to UTC first."},
	}
	vectors := make([]bsonVector, 0, len(instants))
	for _, entry := range instants {
		moment, err := time.Parse(time.RFC3339Nano, entry.instant)
		if err != nil {
			return nil, fmt.Errorf("instant %q: %w", entry.instant, err)
		}
		raw, err := bson.Marshal(bson.D{{Key: "t", Value: moment}})
		if err != nil {
			return nil, fmt.Errorf("instant %q: %w", entry.instant, err)
		}
		decoded, err := serverDecode(raw)
		if err != nil {
			return nil, err
		}
		written, ok := decoded[0].Value.(primitive.DateTime)
		if !ok || int64(written) != entry.millis {
			return nil, fmt.Errorf("instant %q: the server's encoder wrote %v, the table says %d", entry.instant, decoded[0].Value, entry.millis)
		}
		vectors = append(vectors, bsonVector{
			Name: "datetime_floor/" + entry.instant, Group: "datetime_floor", Kind: "datetime_floor", Notes: entry.notes,
			Instant: entry.instant, ExpectedMillis: strconv.FormatInt(entry.millis, 10), Hex: bsonHexOf(raw),
		})
	}
	return vectors, nil
}
