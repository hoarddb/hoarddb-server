package transport

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"
)

// dialAuthenticatedDriverClient connects a DriverClient to ln and sends the
// AUTH that authEchoHandler accepts.
func dialAuthenticatedDriverClient(t *testing.T, ln *TCPListener) *DriverClient {
	t.Helper()
	address := ln.Addr().String()
	tlsConfig, err := MakeClientTLSConfig("", "", TrustPolicy{Insecure: &InsecureTrust{Reason: "transport test client"}}, address)
	if err != nil {
		t.Fatalf("client TLS config: %v", err)
	}
	tlsConfig.NextProtos = []string{ALPNFor(ProtocolMajor)}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := NewDriverClient(ctx, address, tlsConfig)
	if err != nil {
		t.Fatalf("NewDriverClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if answer, err := client.SendRequest(ctx, OpAuth, []byte("admin")); err != nil || answer.Opcode != OpAuthOK {
		t.Fatalf("auth = %v, %v, want AUTH_OK", answer, err)
	}
	return client
}

// TestDriverClientRefusesAnOversizeRequestBeforeSending covers the client side: a
// request whose body would be over the frame cap is refused by the client with
// an error that names the cap and the remedy, nothing is written, and the same
// client's next request succeeds.
func TestDriverClientRefusesAnOversizeRequestBeforeSending(t *testing.T) {
	ln := startTestTCPListener(t, testTCPConfig(TCPConfig{FrameTimeout: 20 * time.Second}), authEchoHandler, nil)
	client := dialAuthenticatedDriverClient(t, ln)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := client.SendRequest(ctx, OpPut, make([]byte, MaxFrameLength+1024))
	if err == nil {
		t.Fatal("an oversize request was accepted")
	}
	for _, want := range []string{"32 MiB", "Split", "blob"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}

	answer, err := client.SendRequest(ctx, OpGet, []byte("still works"))
	if err != nil {
		t.Fatalf("the request after the refused one failed, so the connection was lost: %v", err)
	}
	if answer.Opcode != OpGet || string(answer.Payload) != "still works" {
		t.Errorf("next request answered %s %q, want the echo", answer.Opcode, answer.Payload)
	}
}

// writeOversizeRequestHead writes the header, opcode and request id of a frame
// whose length word is bodyLength, and returns the id it used.
func writeOversizeRequestHead(t *testing.T, conn interface{ Write([]byte) (int, error) }, bodyLength uint32) [RequestIDLen]byte {
	t.Helper()
	probe := NewFrame(OpPut, nil)
	head := make([]byte, 0, 5+1+RequestIDLen)
	head = append(head, ProtocolVersion)
	head = binary.LittleEndian.AppendUint32(head, bodyLength)
	head = append(head, byte(probe.Opcode))
	head = append(head, probe.RequestID[:]...)
	if _, err := conn.Write(head); err != nil {
		t.Fatalf("write oversize head: %v", err)
	}
	return probe.RequestID
}

// TestServerAnswersAnOversizeRequestInsteadOfResettingTheConnection covers
// the server side: a frame whose length word is over the cap but whose header is valid
// is answered with an error that carries its request id and names the cap. For
// an authenticated session the body is drained, within a bound, and the
// connection stays usable. Beyond the bound, and before authentication, the
// server answers and closes.
func TestServerAnswersAnOversizeRequestInsteadOfResettingTheConnection(t *testing.T) {
	authenticate := func(t *testing.T, br *bufio.Reader, conn interface{ Write([]byte) (int, error) }) {
		t.Helper()
		if _, err := conn.Write(NewFrame(OpAuth, []byte("admin")).Encode()); err != nil {
			t.Fatalf("write auth: %v", err)
		}
		if answer := mustDecode(t, br); answer.Opcode != OpAuthOK {
			t.Fatalf("auth answered %s, want AUTH_OK", answer.Opcode)
		}
	}
	assertNamesTheCap := func(t *testing.T, answer *Frame, wantID [RequestIDLen]byte) {
		t.Helper()
		if answer.Opcode != OpError {
			t.Fatalf("answer opcode = %s, want ERROR", answer.Opcode)
		}
		if answer.RequestID != wantID {
			t.Errorf("answer request id %x, want the oversize request's %x", answer.RequestID, wantID)
		}
		code, message := wireErrorOf(t, answer)
		if code != ErrInvalidArgument || !strings.Contains(message, "32 MiB") {
			t.Errorf("answer = %s %q, want INVALID_ARGUMENT naming 32 MiB", code, message)
		}
	}

	t.Run("an authenticated session keeps its connection", func(t *testing.T) {
		ln := startTestTCPListener(t, testTCPConfig(TCPConfig{FrameTimeout: 20 * time.Second}), authEchoHandler, nil)
		conn := dialTestTCP(t, ln.Addr().String())
		_ = conn.SetDeadline(time.Now().Add(60 * time.Second))
		br := bufio.NewReader(conn)
		authenticate(t, br, conn)

		bodyLength := uint32(MaxFrameLength + 4096)
		requestID := writeOversizeRequestHead(t, conn, bodyLength)
		assertNamesTheCap(t, mustDecode(t, br), requestID)

		// The rest of the body follows, as a client that did not know the cap
		// would send it; the server discards it and reads the next frame.
		remaining := make([]byte, int(bodyLength)-1-RequestIDLen)
		if _, err := conn.Write(remaining); err != nil {
			t.Fatalf("write the oversize body: %v", err)
		}
		next := NewFrame(OpGet, []byte("after"))
		if _, err := conn.Write(next.Encode()); err != nil {
			t.Fatalf("write the next request: %v", err)
		}
		if answer := mustDecode(t, br); answer.Opcode != OpGet || answer.RequestID != next.RequestID {
			t.Errorf("next request answered %s, want its echo: the connection did not survive", answer.Opcode)
		}
	})

	t.Run("a length beyond the drain bound is answered and closed", func(t *testing.T) {
		ln := startTestTCPListener(t, testTCPConfig(TCPConfig{FrameTimeout: 20 * time.Second}), authEchoHandler, nil)
		conn := dialTestTCP(t, ln.Addr().String())
		_ = conn.SetDeadline(time.Now().Add(60 * time.Second))
		br := bufio.NewReader(conn)
		authenticate(t, br, conn)

		requestID := writeOversizeRequestHead(t, conn, 1<<31)
		assertNamesTheCap(t, mustDecode(t, br), requestID)
		if _, err := DecodeFrame(br); err == nil || errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("connection stayed open after a length beyond the drain bound: %v", err)
		}
	})

	t.Run("a session that has not authenticated is answered and closed", func(t *testing.T) {
		ln := startTestTCPListener(t, testTCPConfig(TCPConfig{FrameTimeout: 20 * time.Second}), authEchoHandler, nil)
		conn := dialTestTCP(t, ln.Addr().String())
		_ = conn.SetDeadline(time.Now().Add(60 * time.Second))
		br := bufio.NewReader(conn)

		requestID := writeOversizeRequestHead(t, conn, uint32(MaxFrameLength+4096))
		assertNamesTheCap(t, mustDecode(t, br), requestID)
	})
}

// TestOversizeRequestMessageNamesTheNewCLI: the frame-cap refusal tells the
// user that `hoarddb ingest` sizes batches. BUG-0271 renamed the CLI to
// lowercase, so the old `hoardDB ingest` spelling names a command that does
// not exist on a case-sensitive system. The sentence is wire text that a
// driver vector pins byte for byte, so the change lands with the
// pre-certification vector regeneration (BUG-0597).
func TestOversizeRequestMessageNamesTheNewCLI(t *testing.T) {
	message := requestTooLargeMessage(OpPut, MaxFrameLength+1)
	if !strings.Contains(message, "'hoarddb ingest'") {
		t.Errorf("the frame-cap refusal %q does not name `hoarddb ingest`; a user copying the command gets command not found", message)
	}
	if strings.Contains(message, "'hoardDB ingest'") {
		t.Errorf("the frame-cap refusal %q still names `hoardDB ingest`, the old spelling", message)
	}
}

// TestOversizeRequestMessageNamesTheUserPageWithoutAPath: the frame-cap
// refusal used to print a private path to the limits page, which is wrong in
// the published tree. It names the page without a path instead (BUG-0300's
// carve-out). The guard in internal/publiccite checks every string literal
// again once this literal stops being allowed through.
func TestOversizeRequestMessageNamesTheUserPageWithoutAPath(t *testing.T) {
	message := requestTooLargeMessage(OpPut, MaxFrameLength+1)
	if strings.Contains(message, "docs/") {
		t.Errorf("the frame-cap refusal %q names a docs path that differs between the private and published layouts; name the page instead", message)
	}
	if !strings.Contains(message, "the limits page of the user docs") {
		t.Errorf("the frame-cap refusal %q does not name the limits page of the user docs", message)
	}
}
