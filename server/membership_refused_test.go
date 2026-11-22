package server

import (
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestANodeThatCannotExchangeMembershipSaysWhy is build report F-D: an
// encryption.mode=require node takes no membership from a plaintext peer, so
// it never hears that peer agree and is never confirmed. "Not added yet … run
// 'join'" is the wrong advice: it must name the peer and the encryption-mode
// mismatch, in its refusals and in status. (The plaintext node is confirmed:
// the require node's own gossip request carries a view it agrees with, and
// every member of its view has then answered.)
func TestANodeThatCannotExchangeMembershipSaysWhy(t *testing.T) {
	const plaintext, requiring = 0, 1
	tc := startTestClusterTuned(t, 2, 1, func(index int, cfg *config.Config) {
		cfg.Server.AutoRebalance = false
		if index == requiring {
			sealNode(t, cfg, "require", 0x50)
		}
	}, nil)
	defer tc.stop()
	node, peer := tc.nodes[requiring], tc.addrs[plaintext]

	var message string
	deadline := time.Now().Add(15 * time.Second)
	for {
		frame := tc.frameOn(node, transport.OpCreateDB, CreateDatabaseRequest{Name: newcomerDatabase})
		if frame.Opcode != transport.OpError {
			t.Fatalf("the require node answered a create database while it cannot exchange membership: %s", string(frame.Payload))
		}
		var refused ErrorResponse
		if err := bson.Unmarshal(frame.Payload, &refused); err != nil {
			t.Fatalf("decoding the refusal: %v", err)
		}
		message = refused.Message
		if strings.Contains(message, "encryption.mode") || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if strings.Contains(message, "'join ") || !strings.Contains(message, "encryption.mode=require") || !strings.Contains(message, peer) {
		t.Errorf("the require node refuses with %q; want the encryption-mode mismatch and %s named, and no 'join' advice", message, peer)
	}

	health, err := node.server.core.HealthCheck()
	if err != nil {
		t.Fatalf("the require node's health check: %v", err)
	}
	if health.Membership != MembershipRefused || !strings.Contains(health.MembershipDetail, peer) {
		t.Errorf("the require node's status says membership %q (%q); want %q naming %s",
			health.Membership, health.MembershipDetail, MembershipRefused, peer)
	}
}
