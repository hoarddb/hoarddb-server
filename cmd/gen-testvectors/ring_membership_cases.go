package main

import (
	hc "github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// ringMembershipReplyFields are the members of a membership reply (admin-wire section 9.2). The
// request members node_id, data_addr and witness are in ringFieldTable["admin"], because a
// request vector's allowed fields are those of its opcode.
var ringMembershipReplyFields = []string{"cmd", "epoch", "state_seq", "result"}

// membershipVectors are the five membership commands: a request and its reply each, the refusals
// a driver must tell apart, and the requests and replies a strict decoder refuses.
func (b *ringBuilder) membershipVectors() {
	fields, replyFields := ringFieldTable["admin"], ringMembershipReplyFields
	reply := func(cmd, line string) []byte {
		return b.must(wire.EncodeRingAdminReply(wire.RingAdminReply{Cmd: cmd, Membership: &wire.RingAdminMembership{Epoch: 2, StateSeq: 9, Result: line}}))
	}
	join := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminJoin, NodeID: "node-b", DataAddr: "node-b.example:4433"}))
	b.exchange("ring admin: join", "join takes node_id and data_addr (host:port) and registers the node as a non-voting member. The reply is epoch and state_seq, the committed position, and one result line to print. witness is absent unless the node joins as a witness.",
		transport.OpRingAdmin, fields, join, replyFields, reply(wire.AdminJoin, "Added node-b as a member: joining, then active when it holds the slots it will own (epoch 2)"))
	witness := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminJoin, NodeID: "node-w", DataAddr: "node-w.example:4433", Witness: true}))
	b.exchange("ring admin: join as a witness", "witness is written only when true; a witness holds no data, is never ranked and uses a voter slot.",
		transport.OpRingAdmin, fields, witness, replyFields, reply(wire.AdminJoin, "Added node-w as a witness (epoch 2)"))
	promote := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminPromote, NodeID: "node-b"}))
	b.exchange("ring admin: promote", "promote takes node_id only and makes a joined non-voter a voter.",
		transport.OpRingAdmin, fields, promote, replyFields, reply(wire.AdminPromote, "node-b is a voter"))
	leave := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminLeave, NodeID: "node-b"}))
	b.exchange("ring admin: leave", "leave takes node_id only and starts the orderly departure of a member.",
		transport.OpRingAdmin, fields, leave, replyFields, reply(wire.AdminLeave, "node-b is leaving"))
	remove := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminRemoveNode, NodeID: "node-b"}))
	b.exchange("ring admin: remove_node", "remove_node takes node_id only; the member must be down or dead.",
		transport.OpRingAdmin, fields, remove, replyFields, reply(wire.AdminRemoveNode, "node-b was removed"))
	replace := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminReplaceNode, NodeID: "node-b", DataAddr: "node-b.example:4433"}))
	b.exchange("ring admin: replace_node", "replace_node takes node_id and data_addr, the member's own address, and gives a down or dead member's identity back to the node that lost its data there (BUG-0870). accept_loss is absent unless the operator accepts losing the pairs only that member held.",
		transport.OpRingAdmin, fields, replace, replyFields, reply(wire.AdminReplaceNode, "Replacing node-b@node-b.example:4433: joining, then active when it holds the 5461 pairs it owns (epoch 2). Run 'status seeding' on node-b to follow it"))
	acceptLoss := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminReplaceNode, NodeID: "node-b", DataAddr: "node-b.example:4433", AcceptLoss: true}))
	b.exchange("ring admin: replace_node accepting the loss", "accept_loss is written only when true: the CLI's --accept-loss. The leader then replaces a member that held the only copy of some pairs, and those documents are lost.",
		transport.OpRingAdmin, fields, acceptLoss, replyFields, reply(wire.AdminReplaceNode, "Replacing node-b@node-b.example:4433: joining, then active when it holds the 2731 pairs it owns (epoch 2). Run 'status seeding' on node-b to follow it"))
	b.refused("ring admin: membership on a node that has it off", "A node that does not change membership answers all five commands with INVALID_ARGUMENT (exit 2) saying the commands are not available yet and naming the setting that exists and the restart; the session was authorized first.",
		transport.OpRingAdmin, fields, join, wire.RingError{Code: hc.CodeInvalidArgument, Message: wire.ErrMembershipDisabled.Error()}, "cluster.enabled")
	b.refused("ring admin: membership without the cluster-manager role", "The five commands need the unscoped cluster-manager@* (or root@*) grant; a grant on one database is denied; PERMISSION_DENIED names no node, address or setting and is the same whether or not the node has membership on.",
		transport.OpRingAdmin, fields, join, wire.RingError{Code: hc.CodePermissionDenied, Message: "this session cannot change the cluster's membership: the user holds no cluster-manager grant on all databases. Nothing was done. Ask an administrator to run 'grant role cluster-manager@* to <user>' (a grant on one database is not enough) if the user should have access"}, "cluster-manager")
	b.refusal("ring admin refused: a join without its address", "join needs data_addr.", transport.OpRingAdmin, "request", fields, ringDoc("cmd", wire.AdminJoin, "node_id", "node-b"))
	b.refusal("ring admin refused: a join address with port 0", "A port is 1 to 65535.", transport.OpRingAdmin, "request", fields, ringDoc("cmd", wire.AdminJoin, "node_id", "node-b", "data_addr", "node-b.example:0"))
	b.refusal("ring admin refused: a promote with an address", "promote takes node_id only.", transport.OpRingAdmin, "request", fields, ringDoc("cmd", wire.AdminPromote, "node_id", "node-b", "data_addr", "node-b.example:4433"))
	b.refusal("ring admin refused: witness written as false", "witness is absent, not false.", transport.OpRingAdmin, "request", fields, ringDoc("cmd", wire.AdminJoin, "node_id", "node-b", "data_addr", "node-b.example:4433", "witness", false))
	b.refusal("ring admin refused: a replace_node that is a witness", "Only join takes witness.", transport.OpRingAdmin, "request", fields,
		ringDoc("cmd", wire.AdminReplaceNode, "node_id", "node-b", "data_addr", "node-b.example:4433", "witness", true))
	b.refusal("ring admin refused: accept_loss written as false", "accept_loss is absent, not false.", transport.OpRingAdmin, "request", fields,
		ringDoc("cmd", wire.AdminReplaceNode, "node_id", "node-b", "data_addr", "node-b.example:4433", "accept_loss", false))
	b.refusal("ring admin refused: a join that accepts a loss", "Only replace_node takes accept_loss.", transport.OpRingAdmin, "request", fields,
		ringDoc("cmd", wire.AdminJoin, "node_id", "node-b", "data_addr", "node-b.example:4433", "accept_loss", true))
	b.refusal("ring admin reply refused: a membership reply with a negative epoch", "epoch and state_seq are 0 or more.", transport.OpRingAdmin, "reply", replyFields,
		ringDoc("cmd", wire.AdminJoin, "epoch", int64(-1), "state_seq", int64(0), "result", "x"))
	b.refusal("ring admin reply refused: a membership reply with extra members", "A membership reply is epoch, state_seq and result, and nothing else.", transport.OpRingAdmin, "reply", replyFields,
		ringDoc("cmd", wire.AdminJoin, "epoch", int64(1), "state_seq", int64(1), "result", "x", "more", true))
}
