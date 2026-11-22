package server

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestRequestSubjectNamesTheControlChannelOpcodes keeps requestSubject's
// node-to-node list in step with transport's isNodeToNodeOpcode: the control
// channel's probe pair is internode traffic, not a document.
func TestRequestSubjectNamesTheControlChannelOpcodes(t *testing.T) {
	for _, op := range []transport.Opcode{transport.OpClusterProbe, transport.OpClusterProbeReply, transport.OpClusterPropose, transport.OpReplPush} {
		if got := requestSubject(op); got != "The node-to-node request" {
			t.Errorf("requestSubject(%s) = %q, want the node-to-node request", op, got)
		}
	}
}
