package cluster

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestTheStreamLayerCloseWaitsForTheLibrarysConnectionHandlers: the library's transport serves an
// accepted connection on a goroutine of its own and answers a heartbeat on it directly
// (NetworkTransport.handleCommand's fast path), which writes the term to the stable store. Raft
// shutdown does not wait for those goroutines, so Raft.Close closed the bolt store under a
// handler that was still answering a heartbeat, and the library panicked with "failed to save
// current term: database not open" (seen in TestJoinRefusesAnAddressAnotherMemberHolds at
// GOMAXPROCS 1). The layer's Close is what runs between the library's shutdown and the store's
// close, so it must not return while a handler still holds a connection it accepted.
// Break: drop the handlers wait from streamLayer.Close and this is red.
func TestTheStreamLayerCloseWaitsForTheLibrarysConnectionHandlers(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	layer := newStreamLayer(listener, streamHooks{
		local:                func() localIdentity { return localIdentity{NodeID: "n1", Range: BuildStateRange()} },
		committedIncarnation: func(string) (uint64, bool) { return 0, false },
	}, RaftLimits{}, hclog.NewNullLogger())
	layer.start()
	dialWithHandshake(t, listener.Addr().String(), rangeHello(transport.HandshakeChannelRaft, "n2", BuildStateRange()))
	conn, err := layer.Accept()
	if err != nil {
		t.Fatal(err)
	}
	// The library's handler: it reads until the connection fails, then finishes the request it
	// was answering (the store write) before it closes the connection.
	var handlerDone atomic.Bool
	go func() {
		buf := make([]byte, 64)
		for {
			if _, err := conn.Read(buf); err != nil {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
		handlerDone.Store(true)
		_ = conn.Close()
	}()
	if err := layer.Close(); err != nil {
		t.Fatal(err)
	}
	if !handlerDone.Load() {
		t.Fatal("the stream layer closed while the library still served a connection it accepted: the store would be closed under it")
	}
}
