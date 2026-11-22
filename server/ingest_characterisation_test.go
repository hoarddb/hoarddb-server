package server

import (
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestCharBatchThroughNonOwnerIsForwardedWhole pins the cluster fact
// ingest-spec.md §2.9 rests on (characterisation C10, build doc step 0, S-1):
// an OpBatchPut aimed at a node that does not own the bucket is forwarded whole
// to the owner and replicated to every node, and a batch the owner refuses
// leaves none of its documents on any node — not the ones that were valid.
// Ingest relies on that to treat one batch as one atomic unit whatever node it
// dials. This file is a test only; it changes no production code.
func TestCharBatchThroughNonOwnerIsForwardedWhole(t *testing.T) {
	tc, owner := uniqueCluster(t, 3, 3)
	defer tc.stop()
	other := (owner + 1) % len(tc.nodes)

	batch := func(docs ...map[string]interface{}) *transport.Frame {
		return tc.frame(other, transport.OpBatchPut, BatchInsertRequest{Database: testDatabase, Bucket: "Users", Data: docs})
	}

	accepted := batch(
		map[string]interface{}{"_id_": "b1", "Email": "one@example.com"},
		map[string]interface{}{"_id_": "b2", "Email": "two@example.com"},
		map[string]interface{}{"_id_": "b3", "Email": "three@example.com"},
	)
	var answer BatchInsertResponse
	if accepted.Opcode == transport.OpError || bson.Unmarshal(accepted.Payload, &answer) != nil || !answer.Success {
		t.Fatalf("a batch through a non-owner: %s / %+v", accepted.Opcode, answer)
	}
	for _, key := range []string{"b1", "b2", "b3"} {
		for index := range tc.nodes {
			pollUntil(t, "node holds "+key, func() bool { return holdsDocument(t, tc, index, key) })
		}
	}

	// b5 repeats one@example.com under a different key, so the owner refuses
	// the whole batch; b4 is valid and must not survive it.
	refused := batch(
		map[string]interface{}{"_id_": "b4", "Email": "four@example.com"},
		map[string]interface{}{"_id_": "b5", "Email": "one@example.com"},
	)
	if refused.Opcode != transport.OpError {
		t.Fatalf("a violating batch through a non-owner answered %s, want an error", refused.Opcode)
	}
	// A later write proves every node has caught up with the owner's log
	// before absence is asserted.
	if frame := putEmail(t, tc, owner, "settle", "settle@example.com"); frame.Opcode == transport.OpError {
		t.Fatalf("settling write: %s", string(frame.Payload))
	}
	for index := range tc.nodes {
		pollUntil(t, "node holds settle", func() bool { return holdsDocument(t, tc, index, "settle") })
		for _, key := range []string{"b4", "b5"} {
			if holdsDocument(t, tc, index, key) {
				t.Errorf("node %d holds %s from a refused batch; the batch is not atomic across the cluster", index+1, key)
			}
		}
	}
}
