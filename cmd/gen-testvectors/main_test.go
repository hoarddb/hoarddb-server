package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"
)

// The committed vectors live outside this module, in the repository's
// Apache-2.0 clients/ tree. The published server tree carries this module
// alone (scripts/publish-tree.sh), so there a missing file is expected; in the
// private repository, which has scripts/ci.sh beside the module, it is a
// failure.
const (
	committedVectorsPath = "../../../clients/testdata/driver-vectors/v1.json"
	privateTreeMarker    = "../../../scripts/ci.sh"
)

// committedVectors reads the committed file, failing when the private tree
// lacks it and skipping, by name, only in the published tree.
func committedVectors(t *testing.T) []byte {
	t.Helper()
	committed, err := os.ReadFile(committedVectorsPath)
	if err == nil {
		return committed
	}
	if errors.Is(err, fs.ErrNotExist) {
		if _, markerErr := os.Stat(privateTreeMarker); errors.Is(markerErr, fs.ErrNotExist) {
			t.Skipf("published server tree: clients/ is not part of it, so there is no committed %s to check", committedVectorsPath)
		}
	}
	t.Fatalf("reading the committed vectors %s: %v; generate them with 'make vectors' (hoardDB/Makefile)", committedVectorsPath, err)
	return nil
}

var (
	firstGeneration     []byte
	firstGenerationErr  error
	firstGenerationOnce sync.Once
)

// generatedOnce runs one full generation per test binary and shares it: a
// generation starts four servers and is the slow part of every test here.
func generatedOnce(t *testing.T) []byte {
	t.Helper()
	firstGenerationOnce.Do(func() {
		firstGeneration, firstGenerationErr = generateForTest(t)
	})
	if firstGenerationErr != nil {
		t.Fatalf("generating the vectors: %v", firstGenerationErr)
	}
	return firstGeneration
}

func generateForTest(t *testing.T) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	var progress bytes.Buffer
	generated, err := generateFile(ctx, generateOptions{log: &progress})
	if err != nil {
		t.Logf("generator log:\n%s", progress.String())
	}
	return generated, err
}

// TestDriverVectorsMatchServerProto is the drift guard: the
// committed vectors are what the server at this commit answers, so a request
// or response type that gains or loses a field without the vectors changing
// fails here, naming the vector and the field.
func TestDriverVectorsMatchServerProto(t *testing.T) {
	committed := committedVectors(t)
	differences, err := compareVectorFiles(committed, generatedOnce(t))
	if err != nil {
		t.Fatalf("comparing: %v", err)
	}
	if len(differences) > 0 {
		t.Fatalf("the committed vectors do not match the server at this commit; regenerate with 'make vectors' and review the diff, because a payload change is a change to every driver:\n  %s",
			joinLines(differences))
	}
}

// TestGenerationIsDeterministic: two generations are byte-identical, so the
// committed file can be compared byte for byte and a diff in review is a
// change in the server, never noise.
func TestGenerationIsDeterministic(t *testing.T) {
	first := generatedOnce(t)
	second, err := generateForTest(t)
	if err != nil {
		t.Fatalf("second generation: %v", err)
	}
	if !bytes.Equal(first, second) {
		differences, err := compareVectorFiles(first, second)
		if err != nil {
			t.Fatalf("comparing: %v", err)
		}
		t.Fatalf("two generations differ (%d and %d bytes); a value that varies run to run is not normalised:\n  %s",
			len(first), len(second), joinLines(differences))
	}
}

// TestEveryClientOpcodeHasAVector: every opcode the server names is either a
// client request with at least one exchange vector, or excluded by name with
// a reason (a response opcode, node-to-node traffic, or outside the v1
// driver API).
func TestEveryClientOpcodeHasAVector(t *testing.T) {
	file, err := decodeVectorFile(committedVectors(t))
	if err != nil {
		t.Fatal(err)
	}
	exchanged := make(map[uint8]bool)
	for _, v := range file.Vectors {
		if v.Kind == "exchange" && v.Request != nil {
			exchanged[v.Request.Opcode] = true
		}
	}
	var missing []string
	for code := 0; code < 256; code++ {
		op := transport.Opcode(code)
		if op.String() == "UNKNOWN" {
			continue
		}
		if reason, excluded := nonClientOpcodes[op]; excluded {
			if reason == "" {
				t.Errorf("opcode %s is excluded with no reason", op)
			}
			continue
		}
		if !exchanged[uint8(op)] {
			missing = append(missing, op.String())
		}
	}
	if len(missing) > 0 {
		t.Errorf("client opcodes with no exchange vector: %s; add a case to cases.go, or exclude the opcode in nonClientOpcodes with its reason",
			strings.Join(missing, ", "))
	}
	for op := range nonClientOpcodes {
		if op.String() == "UNKNOWN" {
			t.Errorf("nonClientOpcodes excludes 0x%02X, which the server does not name", uint8(op))
		}
	}
}

// TestAllowedFieldsAreTheRequestTypes: each request vector's allowed_fields
// is its request type's wire names minus the internal ones (handbook wire.md
// section 3), and the request it carries uses only those names, so a new
// request field cannot reach drivers unannounced.
func TestAllowedFieldsAreTheRequestTypes(t *testing.T) {
	file, err := decodeVectorFile(committedVectors(t))
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, v := range file.Vectors {
		// A request with payload_hex and no payload_json is raw bytes on
		// purpose: GET_FAST's compact form, or a payload that is not BSON.
		if v.Kind != "exchange" || v.Request == nil || len(v.Request.PayloadJSON) == 0 && v.Request.PayloadHex != "" {
			continue
		}
		want, known := allowedFieldsFor(transport.Opcode(v.Request.Opcode))
		if !known {
			t.Errorf("vector %q: no request type is mapped for %s", v.Name, v.Request.OpcodeName)
			continue
		}
		if !slices.Equal(v.Request.AllowedFields, want) {
			t.Errorf("vector %q: allowed_fields %v, the request type's wire names are %v", v.Name, v.Request.AllowedFields, want)
		}
		sent, err := topLevelNames(v.Request.PayloadHex)
		if err != nil {
			t.Errorf("vector %q: %v", v.Name, err)
			continue
		}
		for _, name := range sent {
			if !slices.Contains(want, name) {
				t.Errorf("vector %q sends %q, which is not an allowed field", v.Name, name)
			}
		}
		checked++
	}
	if checked == 0 {
		t.Fatal("no request vectors were checked")
	}
}

// TestGeneratorRefusesTheURIVectors: an -out naming the hand-maintained URI
// vectors is refused and nothing is written.
func TestGeneratorRefusesTheURIVectors(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "clients", "testdata", "uri-vectors")
	var stdout, stderr bytes.Buffer
	err := run([]string{"-out", target}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "uri-vectors") {
		t.Fatalf("run -out %s returned %v; want a refusal naming uri-vectors", target, err)
	}
	written := 0
	walkErr := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path != root {
			written++
		}
		return nil
	})
	if walkErr != nil {
		t.Fatal(walkErr)
	}
	if written != 0 {
		t.Fatalf("the refused run created %d paths under %s; it must write nothing", written, root)
	}
}

// TestNormaliseSortsAndFixesDocumentsInsideItems: a peek answers its documents
// in an items array; each is a stored document whose field order the server
// does not keep and whose server-minted _id_ differs per run.
func TestNormaliseSortsAndFixesDocumentsInsideItems(t *testing.T) {
	answerWith := func(documentID string, idFirst bool) []byte {
		idElement := bsoncore.AppendStringElement(nil, "_id_", documentID)
		nameElement := bsoncore.AppendStringElement(nil, "Name", "Ada")
		members := append(slices.Clone(nameElement), idElement...)
		if idFirst {
			members = append(slices.Clone(idElement), nameElement...)
		}
		itemDocument := bsoncore.BuildDocument(nil, members)
		itemsArray := bsoncore.BuildArray(nil, bsoncore.Value{Type: bson.TypeEmbeddedDocument, Data: itemDocument})
		return bsoncore.BuildDocument(nil, bsoncore.AppendArrayElement(nil, "items", itemsArray))
	}
	rules := normaliseRules{varyPaths: []string{"items[]._id_"}}
	left, err := normaliseAnswer(answerWith("0199-aaaa", true), rules)
	if err != nil {
		t.Fatalf("normalising: %v", err)
	}
	right, err := normaliseAnswer(answerWith("0199-bbbb", false), rules)
	if err != nil {
		t.Fatalf("normalising: %v", err)
	}
	if !bytes.Equal(left.payload, right.payload) {
		t.Fatalf("items[] documents were not normalised: %v != %v", bsoncore.Document(left.payload), bsoncore.Document(right.payload))
	}
}

// dataMutationOpcodes are the client opcodes that change stored data. The
// handbook (operations.md, write concern) says a write concern applies to
// each, and the server reads "writeconcern" from every non-read-only
// payload (server/write_path.go:frameMeta), so each one's vectors must allow it.
var dataMutationOpcodes = []transport.Opcode{
	transport.OpPut, transport.OpBTreeInsert, transport.OpBatchPut,
	transport.OpFIFOPush, transport.OpLIFOPush, transport.OpHeapPush,
	transport.OpFIFOPop, transport.OpLIFOPop, transport.OpHeapPop,
	transport.OpDelete, transport.OpUpdate, transport.OpBlobWrite,
	transport.OpFieldPush, transport.OpFieldPop, transport.OpFieldPut, transport.OpFieldDelete,
	transport.OpQueueClaim, transport.OpQueueAck, transport.OpQueueRelease, transport.OpQueueExtend,
	transport.OpDeadRequeue, transport.OpDeadDiscard,
}

// TestEveryDataMutationAllowsAWriteConcern checks that every data mutation vector allows a write concern.
func TestEveryDataMutationAllowsAWriteConcern(t *testing.T) {
	file, err := decodeVectorFile(committedVectors(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range dataMutationOpcodes {
		allowed, known := allowedFieldsFor(op)
		if !known || !slices.Contains(allowed, "writeconcern") {
			t.Errorf("%s: allowedFieldsFor = %v, want it to include writeconcern", op, allowed)
		}
	}
	mutations := map[uint8]bool{}
	for _, op := range dataMutationOpcodes {
		mutations[uint8(op)] = true
	}
	checked := 0
	for _, v := range file.Vectors {
		if v.Kind != "exchange" || v.Request == nil || !mutations[v.Request.Opcode] {
			continue
		}
		checked++
		if !slices.Contains(v.Request.AllowedFields, "writeconcern") {
			t.Errorf("vector %q (%s): allowed_fields %v lacks writeconcern", v.Name, v.Request.OpcodeName, v.Request.AllowedFields)
		}
	}
	if checked == 0 {
		t.Fatal("no data-mutation vectors were checked")
	}
}

// TestPopAllowsALimitAndNotACeiling: the bucket pops take a limit, and
// result_ceiling, which the bucket's owner stamps and a driver never sends,
// is in no pop vector's allowed_fields.
func TestPopAllowsALimitAndNotACeiling(t *testing.T) {
	file, err := decodeVectorFile(committedVectors(t))
	if err != nil {
		t.Fatal(err)
	}
	popOpcodes := []uint8{uint8(transport.OpFIFOPop), uint8(transport.OpLIFOPop), uint8(transport.OpHeapPop)}
	checked := 0
	for _, v := range file.Vectors {
		if v.Kind != "exchange" || v.Request == nil || !slices.Contains(popOpcodes, v.Request.Opcode) {
			continue
		}
		checked++
		if !slices.Contains(v.Request.AllowedFields, "limit") || slices.Contains(v.Request.AllowedFields, "result_ceiling") {
			t.Errorf("vector %q: allowed_fields %v; want limit and not result_ceiling", v.Name, v.Request.AllowedFields)
		}
	}
	if checked == 0 {
		t.Fatal("no bucket pop vector was checked")
	}
	for _, op := range popOpcodes {
		allowed, known := allowedFieldsFor(transport.Opcode(op))
		if !known || !slices.Contains(allowed, "limit") || slices.Contains(allowed, "result_ceiling") {
			t.Errorf("allowedFieldsFor(%s) = %v; want limit and not result_ceiling", transport.Opcode(op), allowed)
		}
	}
}

// TestFieldPopAllowsALimit: FIELD_POP's request type is server.FieldPopRequest,
// which carries limit; result_ceiling, which the owner stamps, is not allowed.
func TestFieldPopAllowsALimit(t *testing.T) {
	allowed, known := allowedFieldsFor(transport.OpFieldPop)
	if !known || !slices.Contains(allowed, "limit") || slices.Contains(allowed, "result_ceiling") {
		t.Errorf("allowedFieldsFor(FIELD_POP) = %v, want limit and not result_ceiling", allowed)
	}
	file, err := decodeVectorFile(committedVectors(t))
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, v := range file.Vectors {
		if v.Kind == "exchange" && v.Request != nil && v.Request.Opcode == uint8(transport.OpFieldPop) {
			checked++
			if !slices.Contains(v.Request.AllowedFields, "limit") || slices.Contains(v.Request.AllowedFields, "result_ceiling") {
				t.Errorf("vector %q: allowed_fields %v; want limit and not result_ceiling", v.Name, v.Request.AllowedFields)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no FIELD_POP vector was checked")
	}
}

// TestFieldPeekAllowsALimit checks that FIELD_PEEK's
// request type is server.FieldPeekRequest, which carries limit.
func TestFieldPeekAllowsALimit(t *testing.T) {
	allowed, known := allowedFieldsFor(transport.OpFieldPeek)
	if !known || !slices.Contains(allowed, "limit") {
		t.Errorf("allowedFieldsFor(FIELD_PEEK) = %v, want it to include limit", allowed)
	}
	file, err := decodeVectorFile(committedVectors(t))
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, v := range file.Vectors {
		if v.Kind == "exchange" && v.Request != nil && v.Request.Opcode == uint8(transport.OpFieldPeek) {
			checked++
			if !slices.Contains(v.Request.AllowedFields, "limit") {
				t.Errorf("vector %q: allowed_fields %v lacks limit", v.Name, v.Request.AllowedFields)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no FIELD_PEEK vector was checked")
	}
}

// TestPongIsWhatTheServerSends checks the PONG vector: the TCP serve
// path answers PING itself with an empty payload (transport/serve.go:ServeFrame),
// so the vector must say the PONG has no fields and must not claim {ok: true}.
func TestPongIsWhatTheServerSends(t *testing.T) {
	var written bytes.Buffer
	session := &transport.Session{RemoteAddr: "vectors-test"}
	ping := transport.NewFrameWithID(transport.OpPing, [16]byte{1}, nil)
	handler := func(context.Context, *transport.Frame) (*transport.Frame, error) {
		t.Error("PING reached the request handler")
		return nil, nil
	}
	if err := transport.ServeFrame(context.Background(), session, ping, &written, handler, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("serving PING: %v", err)
	}
	pong, err := transport.DecodeFrame(&written)
	if err != nil {
		t.Fatalf("reading the PONG: %v", err)
	}
	if len(pong.Payload) != 0 {
		t.Fatalf("the server's PONG carries %d payload bytes; this test's premise (an empty PONG) is gone", len(pong.Payload))
	}

	file, err := decodeVectorFile(committedVectors(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range file.Vectors {
		if v.Name != "ping before authentication" {
			continue
		}
		if v.Response.PayloadHex != "" {
			t.Errorf("PONG payload_hex = %q, the server sends none", v.Response.PayloadHex)
		}
		if len(v.Response.Fields) != 0 {
			t.Errorf("PONG fields = %v, the server's PONG has no payload", v.Response.Fields)
		}
		if strings.Contains(v.Notes, "{ok") {
			t.Errorf("notes claim a PONG payload: %q", v.Notes)
		}
		return
	}
	t.Fatal("no \"ping before authentication\" vector")
}

// TestNotesDoNotPromiseUnlandedWork fails when a vector note still says its
// subject is waiting on a build step. A note that said "pending ... step 11"
// outlived the step it named (query pipelining landed) and told drivers the
// capabilities list was about to change shape. Only notes are checked: server
// messages quoted in responses legitimately contain "pending" (a metric name).
func TestNotesDoNotPromiseUnlandedWork(t *testing.T) {
	file, err := decodeVectorFile(committedVectors(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range file.Vectors {
		loweredNotes := strings.ToLower(v.Notes)
		for _, stale := range []string{"pending", "step 11"} {
			if strings.Contains(loweredNotes, stale) {
				t.Errorf("vector %q: note mentions %q, a promise about unlanded work: %q", v.Name, stale, v.Notes)
			}
		}
	}
}

// TestTheOwnershipMovedVectorCarriesTheServersMessage: the decode vector's
// message is a hand copy in static.go (BUG-0575), and nothing compared it with
// server/write_path.go:ownershipMovedErr, so BUG-0528 changed the behaviour
// and left the vector describing the old one. The vector must carry the
// server's current text, built from server.OwnershipMovedMessage.
func TestTheOwnershipMovedVectorCarriesTheServersMessage(t *testing.T) {
	const vectorName = "unavailable with an unknown write outcome"
	const database, bucket = "v000", "users"
	// The shipped file is what a driver reads, so the test checks the frame
	// there (the generator's decode vectors need a live capture for G17).
	file, err := decodeVectorFile(committedVectors(t))
	if err != nil {
		t.Fatalf("reading the committed vectors: %v", err)
	}
	for _, v := range file.Vectors {
		if v.Name != vectorName {
			continue
		}
		raw, err := hex.DecodeString(v.FrameHex)
		if err != nil {
			t.Fatalf("vector %q: decoding frame_hex: %v", vectorName, err)
		}
		frame, err := transport.DecodeFrame(bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("vector %q: decoding the frame: %v", vectorName, err)
		}
		var answered server.ErrorResponse
		if err := bson.Unmarshal(frame.Payload, &answered); err != nil {
			t.Fatalf("vector %q: decoding its error payload: %v", vectorName, err)
		}
		if want := server.OwnershipMovedMessage(database, bucket); answered.Message != want {
			t.Errorf("vector %q carries a message the server no longer sends:\n  vector: %q\n  server: %q", vectorName, answered.Message, want)
		}
		return
	}
	t.Fatalf("no %q vector in the decode vectors", vectorName)
}

// TestNonClientOpcodesAreTheOpcodesNoDriverSends: ALTER_BUCKET (0x8E) is a
// request a driver sends, so it has exchange vectors and is not excused by
// name (bucket-replication-rev8-spec §11.4).
func TestNonClientOpcodesAreTheOpcodesNoDriverSends(t *testing.T) {
	if reason, listed := nonClientOpcodes[transport.OpAlterBucket]; listed {
		t.Errorf("ALTER_BUCKET is listed as an opcode no driver sends (%q), but a driver sends it and it needs an exchange vector", reason)
	}
}
