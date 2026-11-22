package transport

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// TestPingWorkedExample verifies the exact wire bytes from the spec's
// worked example (updated for little-endian length):
//
//	Ver:    01
//	Length: 11 00 00 00  (17 = 1 opcode + 16 request ID + 0 payload, little-endian)
//	Opcode: A0
//	Req ID: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 01
//	Payload: (empty)
func TestPingWorkedExample(t *testing.T) {
	expected := []byte{
		0x01,                   // Version
		0x11, 0x00, 0x00, 0x00, // Length = 17 (little-endian)
		0xA0, // Opcode = PING
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // Request ID
		// No payload
	}

	var reqID [RequestIDLen]byte
	reqID[15] = 0x01

	f := NewFrameWithID(OpPing, reqID, nil)
	encoded := f.Encode()

	if !bytes.Equal(encoded, expected) {
		t.Errorf("PING wire encoding mismatch\n  got:  %x\n  want: %x", encoded, expected)
	}

	if f.SizeBytes() != 22 {
		t.Errorf("expected SizeBytes() = 22, got %d", f.SizeBytes())
	}
}

// TestPingRoundTrip encodes a PING frame and decodes it back, verifying
// perfect round-trip fidelity.
func TestPingRoundTrip(t *testing.T) {
	var reqID [RequestIDLen]byte
	reqID[15] = 0x01

	original := NewFrameWithID(OpPing, reqID, nil)
	encoded := original.Encode()

	decoded, err := DecodeFrame(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("DecodeFrame failed: %v", err)
	}

	if decoded.Version != ProtocolVersion {
		t.Errorf("version: got %d, want %d", decoded.Version, ProtocolVersion)
	}
	if decoded.Opcode != OpPing {
		t.Errorf("opcode: got %s, want PING", decoded.Opcode)
	}
	if decoded.RequestID != original.RequestID {
		t.Errorf("request ID mismatch: got %x, want %x", decoded.RequestID, original.RequestID)
	}
	if len(decoded.Payload) != 0 {
		t.Errorf("expected empty payload, got %d bytes", len(decoded.Payload))
	}
}

// TestPutRoundTrip tests encode/decode of a PUT frame with payload.
func TestPutRoundTrip(t *testing.T) {
	payload := []byte(`{"key":"user_001","value":{"name":"Alice"}}`)
	f := NewFrame(OpPut, payload)
	encoded := f.Encode()

	decoded, err := DecodeFrame(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("DecodeFrame failed: %v", err)
	}

	if decoded.Opcode != OpPut {
		t.Errorf("opcode: got %s, want PUT", decoded.Opcode)
	}
	if !bytes.Equal(decoded.Payload, payload) {
		t.Errorf("payload mismatch: got %x, want %x", decoded.Payload, payload)
	}
}

// TestMultipleFramesInSequence verifies reading multiple frames from
// a single reader (stream simulation).
func TestMultipleFramesInSequence(t *testing.T) {
	var buf bytes.Buffer

	frames := []*Frame{
		NewFrame(OpPut, []byte("data1")),
		NewFrame(OpGet, []byte("key1")),
		NewFrame(OpDelete, []byte("key1")),
	}
	for _, f := range frames {
		if _, err := buf.Write(f.Encode()); err != nil {
			t.Fatalf("write frame: %v", err)
		}
	}

	for i, expected := range frames {
		got, err := DecodeFrame(&buf)
		if err != nil {
			t.Fatalf("read frame %d: %v", i, err)
		}
		if got.Opcode != expected.Opcode {
			t.Errorf("frame %d opcode: got %s, want %s", i, got.Opcode, expected.Opcode)
		}
		if !bytes.Equal(got.Payload, expected.Payload) {
			t.Errorf("frame %d payload mismatch", i)
		}
	}
}

// TestLengthFieldLittleEndian verifies the Length field is little-endian
// and covers opcode + requestID + payload.
func TestLengthFieldLittleEndian(t *testing.T) {
	payload := []byte("hello")
	f := NewFrame(OpPut, payload)
	encoded := f.Encode()

	// Read the length field (bytes 1-4) — little-endian.
	length := binary.LittleEndian.Uint32(encoded[1:5])

	expectedLen := uint32(1 + RequestIDLen + len(payload)) // opcode + reqID + payload
	if length != expectedLen {
		t.Errorf("length field: got %d, want %d", length, expectedLen)
	}
}

// TestProtocolVersionRejection tests that an unknown version is rejected.
func TestProtocolVersionRejection(t *testing.T) {
	f := NewFrame(OpPing, nil)
	f.Version = 0xFF
	encoded := f.Encode()

	_, err := DecodeFrame(bytes.NewReader(encoded))
	if err == nil {
		t.Fatal("expected error for unsupported version")
	}

	var wireErr *WireError
	if !errors.As(err, &wireErr) {
		t.Fatalf("expected WireError, got %T: %v", err, err)
	}
	if wireErr.Code != ErrUnsupportedVersion {
		t.Errorf("error code: got %s, want UNSUPPORTED_VERSION", wireErr.Code)
	}
}

// TestShortFrameRejection tests that a frame with length too short is rejected.
func TestShortFrameRejection(t *testing.T) {
	// Build a frame with length = 5 (too short for opcode + request ID).
	buf := []byte{
		0x01,                   // Version
		0x05, 0x00, 0x00, 0x00, // Length = 5 (little-endian, too short)
		0xA0,                   // Opcode
		0x00, 0x01, 0x02, 0x03, // Only 4 bytes of "body" (need 17 minimum)
	}

	_, err := DecodeFrame(bytes.NewReader(buf))
	if err == nil {
		t.Fatal("expected error for short frame")
	}
}

// TestRequestIDUniqueness tests that NewFrame generates unique request IDs.
func TestRequestIDUniqueness(t *testing.T) {
	seen := make(map[[RequestIDLen]byte]bool)
	for i := 0; i < 10000; i++ {
		f := NewFrame(OpPing, nil)
		if seen[f.RequestID] {
			t.Fatalf("duplicate request ID at iteration %d: %x", i, f.RequestID)
		}
		seen[f.RequestID] = true
	}
}

// TestBATCH_PUT_RoundTrip tests the batch insert response with per-key status.
func TestBATCH_PUT_RoundTrip(t *testing.T) {
	type batchError struct {
		Key       string `bson:"key"`
		ErrorCode uint8  `bson:"error_code"`
		Message   string `bson:"message"`
	}
	type batchResponse struct {
		Inserted []string     `bson:"inserted"`
		Failed   []batchError `bson:"failed"`
	}

	resp := batchResponse{
		Inserted: []string{"key1", "key2"},
		Failed: []batchError{
			{Key: "key3", ErrorCode: 0x08, Message: "conflict"},
		},
	}

	payload, err := bson.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}

	f := NewFrameWithID(OpBatchPutOK, [RequestIDLen]byte{}, payload)
	encoded := f.Encode()
	decoded, err := DecodeFrame(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	var decodedResp batchResponse
	if err := bson.Unmarshal(decoded.Payload, &decodedResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(decodedResp.Inserted) != 2 {
		t.Errorf("expected 2 inserted, got %d", len(decodedResp.Inserted))
	}
	if len(decodedResp.Failed) != 1 {
		t.Errorf("expected 1 failed, got %d", len(decodedResp.Failed))
	}
	if decodedResp.Failed[0].Key != "key3" {
		t.Errorf("expected failed key 'key3', got %q", decodedResp.Failed[0].Key)
	}
}

// TestErrorCodeString tests ErrorCode string representation.
func TestErrorCodeString(t *testing.T) {
	tests := []struct {
		code ErrorCode
		want string
	}{
		{ErrOK, "OK"},
		{ErrNotFound, "NOT_FOUND"},
		{ErrPermissionDenied, "PERMISSION_DENIED"},
		{ErrUnsupportedVersion, "UNSUPPORTED_VERSION"},
		{ErrEarlyDataRejected, "EARLY_DATA_REJECTED"},
		{ErrNotAuthenticated, "NOT_AUTHENTICATED"},
		{ErrorCode(0xFF), "UNKNOWN(255)"},
	}
	for _, tt := range tests {
		if got := tt.code.String(); got != tt.want {
			t.Errorf("ErrorCode(%d).String() = %q, want %q", uint8(tt.code), got, tt.want)
		}
	}
}

// TestOpcodeString tests Opcode string representation.
func TestOpcodeString(t *testing.T) {
	if OpPing.String() != "PING" {
		t.Errorf("OpPing.String() = %q, want 'PING'", OpPing.String())
	}
	if Opcode(0xFF).String() != "UNKNOWN" {
		t.Errorf("Opcode(0xFF).String() = %q, want 'UNKNOWN'", Opcode(0xFF).String())
	}
	if OpStreamAck.String() != "STREAM_ACK" {
		t.Errorf("OpStreamAck.String() = %q, want 'STREAM_ACK'", OpStreamAck.String())
	}
}

// TestAuthorizedKeys tests the AuthorizedKeys set.
func TestAuthorizedKeys(t *testing.T) {
	ak := NewAuthorizedKeys()
	if ak.Count() != 0 {
		t.Errorf("empty set count: got %d, want 0", ak.Count())
	}

	pub := make([]byte, 32)
	pub[0] = 0x01
	ak.Add(pub)
	if ak.Count() != 1 {
		t.Errorf("count after add: got %d, want 1", ak.Count())
	}
	if !ak.IsAuthorized(pub) {
		t.Error("key should be authorized")
	}

	other := make([]byte, 32)
	other[0] = 0x02
	if ak.IsAuthorized(other) {
		t.Error("different key should not be authorized")
	}
}

// TestFingerprint tests the Fingerprint function.
func TestFingerprint(t *testing.T) {
	pub := make([]byte, 32)
	fp := Fingerprint(pub)
	if len(fp) == 0 {
		t.Error("fingerprint should not be empty")
	}
	// SHA256 hex = 64 hex chars + "SHA256:" prefix = 71 chars
	if len(fp) != 71 {
		t.Errorf("fingerprint length: got %d, want 71", len(fp))
	}
}

// TestStreamAckOpcode verifies the STREAM_ACK opcode exists.
func TestStreamAckOpcode(t *testing.T) {
	f := NewFrame(OpStreamAck, []byte(`{"window":100}`))
	encoded := f.Encode()
	decoded, err := DecodeFrame(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Opcode != OpStreamAck {
		t.Errorf("opcode: got %s, want STREAM_ACK", decoded.Opcode)
	}
	if string(decoded.Payload) != `{"window":100}` {
		t.Errorf("payload: got %q", string(decoded.Payload))
	}
}
