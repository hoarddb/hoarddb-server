package server

import (
	"sync/atomic"

	"github.com/hoarddb/hoarddb-server/transport"
)

// requestDecodeHook, when set, is told each time our own code is about to
// decode request bytes at one of the places that read them for dispatch,
// authorization, scoping, the node-to-node handlers and restore records:
// the site's name, the opcode (0 where the site is not told it) and the bytes
// about to be decoded. Tests set it to prove that the validating scan
// (validateRequest, restoreChunk) answers before any of those reads
// (BUG-0539). Nil in production, where a site costs one atomic load and a nil
// check and allocates nothing. It is atomic because goroutines a test did not
// start may reach a site while the test sets or clears it.
var requestDecodeHook atomic.Pointer[func(site string, op transport.Opcode, payload []byte)]

// noteRequestDecode reports one read of request bytes to requestDecodeHook.
func noteRequestDecode(site string, op transport.Opcode, payload []byte) {
	if hook := requestDecodeHook.Load(); hook != nil {
		(*hook)(site, op, payload)
	}
}
