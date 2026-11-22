package ringclient

import (
	"bufio"
	"crypto/tls"
	"net"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// fakeServer is a TLS listener that answers AUTH itself and hands every other frame to
// handle, which replies on the connection whenever it likes.
type fakeServer struct {
	t           *testing.T
	listener    net.Listener
	fingerprint string
	authFails   bool
	// authOK is the AUTH_OK body; nil sends none, as a server that reports nothing.
	authOK []byte
	handle func(conn *fakeConn, frame *transport.Frame)
	wait   sync.WaitGroup
}

type fakeConn struct {
	raw  net.Conn
	lock sync.Mutex
}

func (c *fakeConn) send(frame *transport.Frame) {
	c.lock.Lock()
	defer c.lock.Unlock()
	_, _ = c.raw.Write(frame.Encode())
}

func (c *fakeConn) reply(request *transport.Frame, payload []byte) {
	c.send(transport.NewFrameWithID(transport.OpRingReply, request.RequestID, payload))
}

func (c *fakeConn) refuse(request *transport.Frame, code cluster.Code, message string) {
	payload, err := wire.EncodeRingError(wire.RingError{Code: code, Message: message})
	if err != nil {
		panic(err) // test-only: a code this package picked
	}
	c.send(transport.NewFrameWithID(transport.OpError, request.RequestID, payload))
}

func startFakeServer(t *testing.T, handle func(conn *fakeConn, frame *transport.Frame)) *fakeServer {
	t.Helper()
	return startFakeServerAuthOK(t, nil, handle)
}

// startFakeServerAuthOK is startFakeServer answering every login with authOK as the AUTH_OK body.
func startFakeServerAuthOK(t *testing.T, authOK []byte, handle func(conn *fakeConn, frame *transport.Frame)) *fakeServer {
	t.Helper()
	tlsConfig, fingerprint, err := transport.MakeServerTLSConfig(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", tlsConfig)
	if err != nil {
		t.Fatal(err)
	}
	server := &fakeServer{t: t, listener: listener, fingerprint: fingerprint, handle: handle, authOK: authOK}
	server.wait.Add(1)
	go server.accept()
	t.Cleanup(func() {
		_ = listener.Close()
		server.wait.Wait()
	})
	return server
}

func (s *fakeServer) addr() string { return s.listener.Addr().String() }

func (s *fakeServer) accept() {
	defer s.wait.Done()
	for {
		raw, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.wait.Add(1)
		go func() {
			defer s.wait.Done()
			defer func() { _ = raw.Close() }()
			s.serve(&fakeConn{raw: raw})
		}()
	}
}

func (s *fakeServer) serve(conn *fakeConn) {
	reader := bufio.NewReader(conn.raw)
	for {
		frame, err := transport.DecodeFrame(reader)
		if err != nil {
			return
		}
		if frame.Opcode == transport.OpAuth {
			if s.authFails {
				conn.send(transport.NewFrameWithID(transport.OpAuthFail, frame.RequestID, nil))
			} else {
				conn.send(transport.NewFrameWithID(transport.OpAuthOK, frame.RequestID, s.authOK))
			}
			continue
		}
		s.handle(conn, frame)
	}
}

func (s *fakeServer) options() Options {
	return Options{Addr: s.addr(), User: "root", Password: "pw", Fingerprint: s.fingerprint}
}

func adminReplyFrame(t *testing.T, conn *fakeConn, request *transport.Frame, reply wire.RingAdminReply) {
	t.Helper()
	payload, err := wire.EncodeRingAdminReply(reply)
	if err != nil {
		t.Fatal(err)
	}
	conn.reply(request, payload)
}

func userFields() []wire.RingAdminReplyField {
	return []wire.RingAdminReplyField{{ID: 1, Name: "_id_", Type: "string"}, {ID: 2, Name: "Name", Type: "string"}, {ID: 3, Name: "Purchases", Type: "fifoStore"}}
}
