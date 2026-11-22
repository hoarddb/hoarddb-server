package server

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0549: a request payload that is not well-formed BSON is the client's
// mistake, so it is answered INVALID_ARGUMENT with the scan's wording, not
// INTERNAL; a document nested too deep keeps its own refusal.
func TestMalformedRequestPayloadIsInvalidArgument(t *testing.T) {
	handler := NewHandler(newTestServer(t))
	ctx := rootSession()
	for _, op := range []transport.Opcode{transport.OpGet, transport.OpPut} {
		t.Run(op.String(), func(t *testing.T) {
			answer := readScanAnswer(t, sendOrderFrame(t, ctx, handler, op, []byte{0x01, 0x02, 0x03}))
			if !answer.refused {
				t.Fatalf("a 3-byte payload was answered %s, want an error frame", answer.opcode)
			}
			if answer.code != int(transport.ErrInvalidArgument) {
				t.Errorf("answered code %d (%q), want INVALID_ARGUMENT (%d)", answer.code, answer.message, transport.ErrInvalidArgument)
			}
			if !strings.Contains(answer.message, "is not well-formed BSON") {
				t.Errorf("message %q does not carry the scan's wording", answer.message)
			}
		})
	}
}

// TestDepthRefusalCodeIsUnchangedByTheMalformedCode pins the other branch of
// refusalForScan.
func TestDepthRefusalCodeIsUnchangedByTheMalformedCode(t *testing.T) {
	handler := NewHandler(newTestServer(t))
	payload := wireRequest(wireString("database", "app"), wireString("bucket", "docs"), wireString("key", "k"),
		wireDocument("data", pastTheBound()))
	answer := readScanAnswer(t, sendOrderFrame(t, rootSession(), handler, transport.OpPut, payload))
	if !answer.isScanRefusal() {
		t.Errorf("a document nested past the bound was answered code %d %q, want INVALID_ARGUMENT with the depth wording", answer.code, answer.message)
	}
}

// TestServerMeasuredMalformedBytesStayInternal: the wrap refuseDeepUpdateResult
// builds for bytes the server encoded itself is a server fault, so codeFor must
// not map errMalformedBSON (only ErrMalformedPayload, which a client's bytes carry).
func TestServerMeasuredMalformedBytesStayInternal(t *testing.T) {
	measured := fmt.Errorf("measuring the depth of updated document %q: %w", "k", errMalformedBSON)
	if got := codeFor(measured); got != transport.ErrInternal {
		t.Errorf("codeFor of server-measured malformed bytes = %d, want INTERNAL (%d)", got, transport.ErrInternal)
	}
}
