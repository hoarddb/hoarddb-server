package server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// callRaw sends one frame and returns the response frame untouched, so a test
// can look at the payload the server actually sent. tcpServerClient.call skips
// decoding an empty payload, which is exactly how the empty not-found frame
// went unnoticed.
func (c *tcpServerClient) callRaw(op transport.Opcode, req any) *transport.Frame {
	c.t.Helper()
	payload, err := bson.Marshal(req)
	if err != nil {
		c.t.Fatalf("marshal %s request: %v", op, err)
	}
	_ = c.conn.SetDeadline(time.Now().Add(30 * time.Second))
	if _, err := c.conn.Write(transport.NewFrame(op, payload).Encode()); err != nil {
		c.t.Fatalf("write %s: %v", op, err)
	}
	response, err := transport.DecodeFrame(c.br)
	if err != nil {
		c.t.Fatalf("read response to %s: %v", op, err)
	}
	return response
}

// TestNotFoundAnswersCarryAPayload: a miss is an ordinary answer
// (found=false plus a message saying why), and it must reach the client as a
// decodable document, not as a zero-length frame.
func TestNotFoundAnswersCarryAPayload(t *testing.T) {
	_, addr := newTCPEnabledServer(t, nil)
	client := dialTCPServer(t, addr)
	client.login()
	client.call(transport.OpCreateDB, &CreateDatabaseRequest{Name: "app"}, nil)
	for name, bucketType := range map[string]BucketType{
		"hashes": BucketTypeHash, "trees": BucketTypeBTree, "blobs": BucketTypeBlob,
		"fifo": BucketTypeFIFO, "lifo": BucketTypeLIFO, "heap": BucketTypeHeap,
	} {
		client.call(transport.OpCreateBucket, &CreateBucketRequest{Database: "app", Name: name, Type: bucketType}, nil)
	}

	find := func(bucket string) any { return &FindRequest{Database: "app", Bucket: bucket, Key: "absent"} }
	pop := func(bucket string) any { return &PopRequest{Database: "app", Bucket: bucket} }
	peek := func(bucket string) any { return &PeekRequest{Database: "app", Bucket: bucket} }

	cases := []struct {
		name        string
		opcode      transport.Opcode
		request     any
		wantOpcode  transport.Opcode
		wantMessage string
	}{
		{"hash get of a missing key", transport.OpGet, find("hashes"), transport.OpGetResponse, "document not found"},
		{"btree lookup of a missing key", transport.OpBTreeLookup, find("trees"), transport.OpBTreeResponse, "document not found"},
		{"blob get of a missing key", transport.OpGet, find("blobs"), transport.OpGetResponse, "not found"},
		{"fifo pop when empty", transport.OpFIFOPop, pop("fifo"), transport.OpFIFOPopResponse, "queue is empty"},
		{"fifo peek when empty", transport.OpFIFOPeek, peek("fifo"), transport.OpFIFOPeek, "queue is empty"},
		{"lifo pop when empty", transport.OpLIFOPop, pop("lifo"), transport.OpLIFOPopResponse, "queue is empty"},
		{"lifo peek when empty", transport.OpLIFOPeek, peek("lifo"), transport.OpLIFOPeek, "queue is empty"},
		{"heap pop when empty", transport.OpHeapPop, pop("heap"), transport.OpHeapPopResponse, "heap is empty"},
		// Wrong-type verbs are no longer misses: since BUG-0469 they are one ERROR
		// refusal, pinned by TestEveryVerbOnTheWrongStoreTypeIsRefusedTheSameWay.
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			response := client.callRaw(tc.opcode, tc.request)
			if response.Opcode != tc.wantOpcode {
				t.Fatalf("response opcode = %s, want %s", response.Opcode, tc.wantOpcode)
			}
			if len(response.Payload) == 0 {
				t.Fatalf("%s answered with a zero-length payload; a miss must say found=false and why", tc.opcode)
			}
			var answer bson.M
			if err := bson.Unmarshal(response.Payload, &answer); err != nil {
				t.Fatalf("decoding the %s answer: %v", tc.opcode, err)
			}
			if found, present := answer["found"]; !present || found != false {
				t.Errorf("found = %v (present=%v), want an explicit false", found, present)
			}
			if message, _ := answer["message"].(string); !strings.Contains(message, tc.wantMessage) {
				t.Errorf("message = %q, want it to contain %q", message, tc.wantMessage)
			}
			if _, present := answer["data"]; present {
				t.Errorf("a miss carried a data field: %v", answer["data"])
			}
		})
	}
}

// TestNoResponseMarshalErrorIsDiscarded guards the cause of a swallowed marshal failure: every
// `payload, _ := bson.Marshal(...)` turned a failed encode into an empty frame
// the client could not tell from a miss. A response path reports the failure
// (as an error frame naming the opcode); it never drops it.
func TestNoResponseMarshalErrorIsDiscarded(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("listing server sources: %v", err)
	}
	fileSet := token.NewFileSet()
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", path, err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			assign, ok := node.(*ast.AssignStmt)
			if !ok || len(assign.Lhs) != 2 || len(assign.Rhs) != 1 {
				return true
			}
			call, ok := assign.Rhs[0].(*ast.CallExpr)
			if !ok {
				return true
			}
			callee, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || callee.Sel.Name != "Marshal" {
				return true
			}
			if pkg, ok := callee.X.(*ast.Ident); !ok || pkg.Name != "bson" {
				return true
			}
			if blank, ok := assign.Lhs[1].(*ast.Ident); ok && blank.Name == "_" {
				t.Errorf("%s: bson.Marshal error discarded", fileSet.Position(assign.Pos()))
			}
			return true
		})
	}
}

// TestRespondReportsEncodeFailureWithOpcode pins the other half of the fix: a
// response that cannot be encoded is an OpError frame that names the request
// opcode and the cause, never an empty payload under the success opcode.
func TestRespondReportsEncodeFailureWithOpcode(t *testing.T) {
	request := transport.NewFrameWithID(transport.OpGet, [16]byte{41}, nil)
	unencodable := struct{ Sink chan int }{Sink: make(chan int)}

	response, err := respond(request, transport.OpGetResponse, unencodable)
	if err != nil {
		t.Fatalf("respond returned an error %v; the failure must travel in the frame", err)
	}
	if response.Opcode != transport.OpError {
		t.Fatalf("response opcode = %s, want ERROR", response.Opcode)
	}
	if response.RequestID != request.RequestID {
		t.Errorf("request id = %x, want %x", response.RequestID, request.RequestID)
	}
	var body ErrorResponse
	if err := bson.Unmarshal(response.Payload, &body); err != nil {
		t.Fatalf("decoding the error frame: %v", err)
	}
	if body.Code == int(transport.ErrOK) {
		t.Errorf("error code = %d, which reads as success", body.Code)
	}
	if !strings.Contains(body.Message, transport.OpGet.String()) {
		t.Errorf("message %q does not name the opcode %s", body.Message, transport.OpGet)
	}
}
