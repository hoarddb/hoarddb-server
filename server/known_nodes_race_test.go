package server

import (
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// TestKnownNodesSnapshotIsRaceFreeAgainstGossip is BUG-0577: handleListNodes
// encodes the known-node records after releasing knownNodesMu, while a gossip
// round rewrites them in place under the lock. Encoding the shared records
// races those writes; a snapshot must hand out copies.
func TestKnownNodesSnapshotIsRaceFreeAgainstGossip(t *testing.T) {
	s := &Server{knownNodes: make(map[string]*NodeInfo)}
	s.knownNodes["peer"] = &NodeInfo{
		NodeID:      "peer",
		Addr:        "192.0.2.11:7000",
		Fingerprint: "fp-0",
		JoinedAt:    time.Now(),
		Status:      "active",
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			addr := "192.0.2.11:7000"
			fingerprint := "fp-even"
			if i%2 == 1 {
				addr = "192.0.2.12:7000"
				fingerprint = "fp-odd"
			}
			s.recordNode("peer", addr)
			s.recordObservedFingerprint("peer", fingerprint)
		}
	}()

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, err := bson.Marshal(s.KnownNodes()); err != nil {
			t.Fatalf("encode known nodes: %v", err)
		}
	}
	close(stop)
	<-done

	// The snapshot is a copy: a later gossip write must not reach it.
	snapshot := s.KnownNodes()
	before := snapshot["peer"]
	beforeAddr, beforeStatus, beforeFingerprint := before.Addr, before.Status, before.Fingerprint
	s.recordNode("peer", "192.0.2.19:7000")
	s.recordObservedFingerprint("peer", "fp-after")
	after := snapshot["peer"]
	if after.Addr != beforeAddr || after.Status != beforeStatus || after.Fingerprint != beforeFingerprint {
		t.Fatalf("KnownNodes snapshot changed after gossip: before %q/%q/%q, after %q/%q/%q",
			beforeAddr, beforeStatus, beforeFingerprint, after.Addr, after.Status, after.Fingerprint)
	}
}
