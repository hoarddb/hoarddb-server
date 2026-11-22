package cli

import (
	"bufio"
	"crypto/tls"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// stubServer is a TCP+TLS listener that speaks just enough of the driver
// protocol for a CLI to connect and authenticate, and answers every other
// opcode from a script. The real server cannot be made to fail a read or to
// answer a shape it no longer sends, so the characterisation tests that pin how
// the CLI reads those replies stand one up.
type stubServer struct {
	addr string
	// script maps an opcode to the response body (marshalled as BSON) the stub
	// sends back with that opcode's reply frame; an opcode not in it gets an
	// OpError frame naming the omission, so a test never hangs on a request it
	// did not expect.
	script map[transport.Opcode]interface{}
	// replyOpcode overrides the reply frame's opcode (default: the request's
	// own opcode is answered with itself, which the CLI accepts).
	requests []transport.Opcode
	// payloads is each request's payload by opcode, in arrival order, so a
	// test can read the member a statement sent.
	payloads map[transport.Opcode][][]byte
	// dynamic, when set, is asked first for the reply to an opcode, so a test
	// can change what the server says between two statements (a bucket dropped
	// and recreated as another type). It answers false to fall through to the
	// script.
	dynamic func(op transport.Opcode) (interface{}, bool)
	mu      sync.Mutex
}

// answerWith installs the dynamic reply function.
func (s *stubServer) answerWith(dynamic func(op transport.Opcode) (interface{}, bool)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dynamic = dynamic
}

// sent is how many frames of the opcode the stub has received.
func (s *stubServer) sent(op transport.Opcode) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, seen := range s.requests {
		if seen == op {
			count++
		}
	}
	return count
}

// payloadsOf is the payload of every frame of the opcode the stub has received.
func (s *stubServer) payloadsOf(op transport.Opcode) [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]byte(nil), s.payloads[op]...)
}

// sequence is every opcode the stub received since the last call, in order.
func (s *stubServer) sequence() []transport.Opcode {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := s.requests
	s.requests = nil
	return seen
}

// stubRawReply is a script value that is sent as-is: the reply frame carries
// Opcode and Payload verbatim, so a test can answer an OpError with a payload
// the CLI cannot decode, or with a code the real server does not send.
type stubRawReply struct {
	Opcode  transport.Opcode
	Payload []byte
}

// newStubServer starts the stub and returns it with a connected, authenticated
// CLI whose output goes to out.
func newStubServer(t *testing.T, script map[transport.Opcode]interface{}, out io.Writer) (*stubServer, *CLI) {
	t.Helper()
	tlsConfig, _, err := transport.MakeServerTLSConfig(t.TempDir())
	if err != nil {
		t.Fatalf("stub server TLS config: %v", err)
	}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", tlsConfig)
	if err != nil {
		t.Fatalf("stub server listen: %v", err)
	}
	stub := &stubServer{addr: listener.Addr().String(), script: script}

	var connections sync.WaitGroup
	var open sync.Map
	connections.Add(1)
	go func() {
		defer connections.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			open.Store(conn, struct{}{})
			connections.Add(1)
			go func() {
				defer connections.Done()
				defer open.Delete(conn)
				stub.serve(conn)
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		open.Range(func(conn, _ interface{}) bool {
			_ = conn.(net.Conn).Close()
			return true
		})
		connections.Wait()
	})

	c := NewCLI(WithAddress(stub.addr), WithInsecureTLS(true), WithCredentials("admin", goldenPassword), WithTerminal(out), WithTheme("plain"))
	if err := c.Connect(); err != nil {
		t.Fatalf("connecting to the stub server: %v", err)
	}
	t.Cleanup(c.Close)
	return stub, c
}

// serve answers frames on one connection until it closes.
func (s *stubServer) serve(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		frame, err := transport.DecodeFrame(reader)
		if err != nil {
			return
		}
		if frame.Opcode == transport.OpAuth {
			payload, _ := bson.Marshal(server.AuthResponse{Success: true})
			if err := transport.WriteFrame(conn, transport.NewFrameWithID(transport.OpAuthOK, frame.RequestID, payload)); err != nil {
				return
			}
			continue
		}
		s.mu.Lock()
		s.requests = append(s.requests, frame.Opcode)
		if s.payloads == nil {
			s.payloads = make(map[transport.Opcode][][]byte)
		}
		s.payloads[frame.Opcode] = append(s.payloads[frame.Opcode], frame.Payload)
		s.mu.Unlock()
		body, scripted := s.script[frame.Opcode]
		s.mu.Lock()
		dynamic := s.dynamic
		s.mu.Unlock()
		if dynamic != nil {
			if replacement, answered := dynamic(frame.Opcode); answered {
				body, scripted = replacement, true
			}
		}
		if !scripted {
			_ = transport.WriteErrorResponse(conn, transport.ErrInternal, "the stub server has no scripted reply for "+frame.Opcode.String(), frame.RequestID)
			continue
		}
		replyOpcode := frame.Opcode
		var payload []byte
		if raw, isRaw := body.(stubRawReply); isRaw {
			replyOpcode, payload = raw.Opcode, raw.Payload
		} else if payload, err = bson.Marshal(body); err != nil {
			return
		}
		if err := transport.WriteFrame(conn, transport.NewFrameWithID(replyOpcode, frame.RequestID, payload)); err != nil {
			return
		}
	}
}
