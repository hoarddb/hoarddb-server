package wire

import (
	"context"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

// This file serves RING_TOPOLOGY (entry-ring-client-wire.md section 4). Any authenticated
// session may ask: the members and the table name no database, bucket or document. Only
// the database rows are tenant information, so each is shown to the sessions the
// authorizer says may see its name. The work is O(8192 slots + members + databases) and
// does not depend on the number of buckets.

// ringTopologyDenied is the answer to a session the authorizer refuses outright (no
// identity, or an internode session): one text, no table.
func ringTopologyDenied() error {
	return &cluster.Error{Code: cluster.CodePermissionDenied, Message: "this session cannot read the topology: the connection is not " +
		"authenticated as a user. Nothing was done. Connect with a user name and password ('hoarddb -u <user>'), then ask again"}
}

func (s *ringServer) topology(ctx context.Context, frame *transport.Frame) (*transport.Frame, error) {
	req, err := DecodeRingTopology(frame.Payload)
	if err != nil {
		return ringFailure(frame, err)
	}
	reply, err := s.serveTopology(ctx, req)
	if err != nil {
		return ringFailure(frame, err)
	}
	payload, err := EncodeRingTopologyReply(reply)
	if err != nil {
		return ringFailure(frame, err)
	}
	return transport.NewFrameWithID(transport.OpRingReply, frame.RequestID, payload), nil
}

// serveTopology answers from the committed view in one atomic load, so the epoch, the
// state_seq, the table and the databases are one state. A requester whose view matches
// gets the numbers alone; epoch 0 never matches, because it means "I hold nothing".
func (s *ringServer) serveTopology(ctx context.Context, req RingTopologyRequest) (RingTopologyReply, error) {
	if err := s.authz.Authorize(ctx, RingAccess{Class: RingClassTopology}); err != nil {
		return RingTopologyReply{}, ringTopologyDenied()
	}
	view := s.node.topology.view.Load()
	reply := RingTopologyReply{Epoch: int64(view.epoch), StateSeq: view.seq}
	if req.Epoch != 0 && req.Epoch == reply.Epoch && req.StateSeq == reply.StateSeq {
		reply.Unchanged = true
		return reply, nil
	}
	reply.Table, reply.Witnesses, reply.Voters = view.table, TopologyWitnessRows(view.witnesses), view.committedVoters
	for _, row := range view.databases {
		if err := s.authz.Authorize(ctx, RingAccess{Database: row.Name, Class: RingClassTopology}); err != nil {
			continue
		}
		reply.Databases = append(reply.Databases, RingTopologyDatabase{Name: row.Name, ID: row.ID, RF: row.RF})
	}
	return reply, nil
}

// TopologyWitnessRows returns the witness rows a RING_TOPOLOGY reply carries: a copy of each
// row with its client address cleared. A witness holds no password key, so an address it
// published is not one a driver can log in at; an empty address is the contract's "skip this
// member". The committed state keeps the address (it is the node's own advertisement).
func TopologyWitnessRows(witnesses []cluster.Member) []cluster.Member {
	if len(witnesses) == 0 {
		return nil
	}
	rows := make([]cluster.Member, len(witnesses))
	for i, witness := range witnesses {
		witness.ClientAddr = ""
		rows[i] = witness
	}
	return rows
}
