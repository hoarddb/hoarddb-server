package transport

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"runtime"
	"strings"
	"testing"
	"time"
)

// TestDecodeFrameBoundsAllocationByBytesRead is the regression test: the memory
// DecodeFrame takes for a frame body follows the bytes that arrived, not the
// length word, so a frame that claims the full cap and sends 1 KiB costs
// about 1 KiB.
func TestDecodeFrameBoundsAllocationByBytesRead(t *testing.T) {
	var stream bytes.Buffer
	stream.WriteByte(ProtocolVersion)
	_ = binary.Write(&stream, binary.LittleEndian, uint32(MaxFrameLength))
	stream.WriteByte(byte(OpPut))
	stream.Write(make([]byte, RequestIDLen))
	stream.Write(make([]byte, 1024))

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := DecodeFrame(bytes.NewReader(stream.Bytes()))
	runtime.ReadMemStats(&after)

	if err == nil {
		t.Fatal("DecodeFrame of a truncated frame succeeded, want a read error")
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 1<<20 {
		t.Fatalf("DecodeFrame allocated %d bytes for a frame that delivered %d; want under 1 MiB",
			allocated, stream.Len())
	}
}

// TestFrameBeforeLoginIsBoundedInSize is the regression test on the listener: before
// AUTH succeeds a frame is limited to PreAuthFrameLength. A larger one is
// answered by its request ID, naming the limit and what to do, and the
// connection is closed without the body being read.
func TestFrameBeforeLoginIsBoundedInSize(t *testing.T) {
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{FrameTimeout: 20 * time.Second}), authEchoHandler, nil)
	conn := dialTestTCP(t, ln.Addr().String())
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	br := bufio.NewReader(conn)

	requestID := writeOversizeRequestHead(t, conn, 1<<20)
	answer := mustDecode(t, br)
	if answer.Opcode != OpError || answer.RequestID != requestID {
		t.Fatalf("answer = %s for request %x, want ERROR for %x", answer.Opcode, answer.RequestID, requestID)
	}
	code, message := wireErrorOf(t, answer)
	if code != ErrInvalidArgument {
		t.Errorf("answer code = %s, want INVALID_ARGUMENT", code)
	}
	for _, want := range []string{"64 KiB", "AUTH"} {
		if !strings.Contains(message, want) {
			t.Errorf("answer %q does not name %q", message, want)
		}
	}
	if _, err := DecodeFrame(br); err == nil {
		t.Error("connection stayed open after a frame over the before-login limit")
	}
}

// TestFrameAfterLoginKeepsTheFullLimit: the before-login limit ends at
// AUTH_OK; an authenticated connection sends a frame far above it.
func TestFrameAfterLoginKeepsTheFullLimit(t *testing.T) {
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{}), authEchoHandler, nil)
	conn := dialTestTCP(t, ln.Addr().String())
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReader(conn)

	if _, err := conn.Write(NewFrame(OpAuth, []byte("admin")).Encode()); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	if answer := mustDecode(t, br); answer.Opcode != OpAuthOK {
		t.Fatalf("auth answered %s, want AUTH_OK", answer.Opcode)
	}
	large := NewFrame(OpPut, bytes.Repeat([]byte("x"), 512<<10))
	if _, err := conn.Write(large.Encode()); err != nil {
		t.Fatalf("write large request: %v", err)
	}
	answer := mustDecode(t, br)
	if answer.Opcode != OpPut || len(answer.Payload) != len(large.Payload) {
		t.Fatalf("large request answered %s with %d bytes, want its %d-byte echo", answer.Opcode, len(answer.Payload), len(large.Payload))
	}
}
