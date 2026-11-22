package transport

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime/debug"
	"sync/atomic"
)

// recoveredDriverFaults and recoveredInternodeFaults count the request handler
// faults this process recovered at the request boundary, on the driver port
// and on the internode port. Process-wide, as oversizedResponses is: the
// server's metric reads them, since transport cannot import its registry.
var recoveredDriverFaults, recoveredInternodeFaults atomic.Uint64

// RecoveredFaults reports how many request handler faults this process has
// recovered at the request boundary, on the driver port and on the internode
// port. Any non-zero value is a server bug; each one is an error-level log
// line with its fault id and stack.
func RecoveredFaults() (driver, internode uint64) {
	return recoveredDriverFaults.Load(), recoveredInternodeFaults.Load()
}

// handlerFault is what a panic in a request handler becomes. It is logged with
// the stack where the panic happened, and the client gets an INTERNAL answer
// that names a fault id and asks for a report, without the panic's value: the
// value is the server's own state and not for a client.
//
// The recovery sits at the request boundary and nowhere deeper, so it does not
// hide a fault from the operator: every one is an error-level log line. The
// request that faulted fails and its connection closes; other requests on the
// connection that were already in flight answer first (the same drain a
// handler error gets), and no other connection is touched.
func (l *TCPListener) handlerFault(recovered any, addr string, opcode Opcode) error {
	if l.cfg.PeerMode {
		recoveredInternodeFaults.Add(1)
	} else {
		recoveredDriverFaults.Add(1)
	}
	faultID := fmt.Sprintf("%08x", rand.Uint32())
	l.logger.Error("request handler fault: the request failed and its connection is closed",
		"fault_id", faultID, "addr", addr, "opcode", opcode, "panic", fmt.Sprint(recovered), "stack", string(debug.Stack()))
	return errors.New("the server hit an internal fault while handling this request, so it failed and this connection is being closed; " +
		"other connections are not affected. This is a server bug: report it with fault id " + faultID +
		" and the server log line that carries the same id")
}
