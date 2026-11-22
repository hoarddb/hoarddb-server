package server

import (
	"context"
	"slices"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestBatchOpcodeIsAClientRequest pins what the protocol says about BATCH
// before any of it runs (batch-request spec §3.1, §4.2, §5.6): its number and
// its answer's, that it may write, that it is authorized under an unscoped
// row and then item by item, that it is advertised to drivers and members,
// and that its envelope's depth is measured at maxDocumentDepth + 4.
func TestBatchOpcodeIsAClientRequest(t *testing.T) {
	if transport.OpBatch != 0x22 || transport.OpBatchOK != 0x23 {
		t.Fatalf("BATCH = 0x%02X and BATCH_OK = 0x%02X; the spec gives 0x22 and 0x23", uint8(transport.OpBatch), uint8(transport.OpBatchOK))
	}
	if transport.OpBatch.IsReadOnly() {
		t.Fatal("BATCH is classified read-only; it carries writes")
	}
	for _, op := range []transport.Opcode{transport.OpBatch, transport.OpBatchOK} {
		rule, ok := opRules[op]
		if !ok {
			t.Fatalf("%s has no opRules row", op)
		}
		if rule.Action != ActionNone || rule.Scope != ScopeNone {
			t.Fatalf("%s's row is %+v; the envelope names no database and each item is authorized on its own", op, rule)
		}
	}
	if !slices.Contains(serverCapabilities(), featureBatch) {
		t.Fatalf("serverCapabilities() = %v; a driver learns BATCH by the %q capability", serverCapabilities(), featureBatch)
	}
	if !slices.Contains(clusterFeatures(), featureBatch) {
		t.Fatalf("clusterFeatures() = %v; a coordinator sends a member one sub-batch only when it advertises %q", clusterFeatures(), featureBatch)
	}
	if got, want := requestDepthLimit(transport.OpBatch), maxDocumentDepth+4; got != want {
		t.Fatalf("requestDepthLimit(BATCH) = %d, want %d (envelope, items, item, then the item payload's own +1)", got, want)
	}
}

// TestBatchNotInLocalHandlers is T-13 (regression guard): BATCH is never a
// localHandlers entry, so replay and a replica never run one, and
// Server.dispatch refuses a BATCH frame.
func TestBatchNotInLocalHandlers(t *testing.T) {
	if implementsOpcode(transport.OpBatch) {
		t.Fatal("implementsOpcode(BATCH) is true; a batch must never be dispatchable where replicated entries funnel")
	}
	s := newTestServer(t)
	reply, err := s.dispatch(context.Background(), transport.NewFrame(transport.OpBatch, nil))
	if err != nil {
		t.Fatalf("dispatch of a BATCH frame: %v", err)
	}
	if reply.Opcode != transport.OpError {
		t.Fatalf("dispatch of a BATCH frame answered %s; it must refuse it", reply.Opcode)
	}
}
