package control

import (
	"context"
	"fmt"
	"net"

	"github.com/hoarddb/hoarddb-server/transport"
)

// RelayProposal sends one cluster command to member leader and returns the leader's answer.
// It is one request and one reply on the control channel; the leader never passes it on, so a
// command crosses at most one hop. A transport failure is an error, and the caller must treat
// that attempt as undecided: the command may have been applied although no reply came back.
func (c *Control) RelayProposal(ctx context.Context, leader string, req transport.RelayProposal) (transport.RelayReply, error) {
	reply, err := c.call(ctx, leader, req)
	if err != nil {
		return transport.RelayReply{}, err
	}
	answer, ok := reply.(transport.RelayReply)
	if !ok {
		return transport.RelayReply{}, fmt.Errorf("%s answered RELAY_PROPOSAL with %T, want RELAY_PROPOSAL_REPLY", leader, reply)
	}
	return answer, nil
}

// RelayMembership sends one membership command to member leader and returns the leader's answer.
// Like RelayProposal it is one request and one reply, the leader never passes it on, and a
// transport failure leaves the outcome unknown: the caller asks again with the same request id.
//
// A join runs several Raft operations on the leader and can wait on a dial, so it travels on a
// connection of its own: on the shared member client it would hold the lease renewals and probes
// of this node behind it, and a follower that forwards a slow join would lose its lease.
func (c *Control) RelayMembership(ctx context.Context, leader string, req transport.MembershipRelay) (transport.MembershipRelayReply, error) {
	addr, ok := c.cfg.Addr(leader)
	if !ok {
		return transport.MembershipRelayReply{}, fmt.Errorf("member %q has no control address; run 'nodes' to list the members", leader)
	}
	message, err := transport.NewControlMessage(c.envelope(), req)
	if err != nil {
		return transport.MembershipRelayReply{}, fmt.Errorf("encode MEMBERSHIP_RELAY for %s: %w", leader, err)
	}
	client := transport.NewChannelClient(addr, transport.ChannelControl, c.cfg.Hello)
	if c.cfg.Dial != nil {
		client.SetDialer(func(ctx context.Context, addr string) (net.Conn, error) { return c.cfg.Dial(ctx, leader, addr) })
	}
	raw, err := client.Call(ctx, message)
	if closeErr := client.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		return transport.MembershipRelayReply{}, fmt.Errorf("membership relay to %s: %w", leader, err)
	}
	decoded, err := raw.Control()
	if err != nil {
		return transport.MembershipRelayReply{}, fmt.Errorf("membership relay reply from %s: %w", leader, err)
	}
	answer, ok := decoded.(transport.MembershipRelayReply)
	if !ok {
		return transport.MembershipRelayReply{}, fmt.Errorf("%s answered MEMBERSHIP_RELAY with %T, want MEMBERSHIP_RELAY_REPLY", leader, decoded)
	}
	return answer, nil
}

// RequestPepper asks member for the cluster's password key (ring user store spec 5.3). Like a
// membership relay it travels on a connection of its own and closes it, so the asker's lease
// renewals and probes never queue behind it, and the key's bytes cross only that pinned session.
func (c *Control) RequestPepper(ctx context.Context, member string, req transport.PepperRequest) (transport.PepperReply, error) {
	addr, ok := c.cfg.Addr(member)
	if !ok {
		return transport.PepperReply{}, fmt.Errorf("member %q has no control address; run 'nodes' to list the members", member)
	}
	message, err := transport.NewControlMessage(c.envelope(), req)
	if err != nil {
		return transport.PepperReply{}, fmt.Errorf("encode PEPPER_REQUEST for %s: %w", member, err)
	}
	client := transport.NewChannelClient(addr, transport.ChannelControl, c.cfg.Hello)
	if c.cfg.Dial != nil {
		client.SetDialer(func(ctx context.Context, addr string) (net.Conn, error) { return c.cfg.Dial(ctx, member, addr) })
	}
	raw, err := client.Call(ctx, message)
	if closeErr := client.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		return transport.PepperReply{}, fmt.Errorf("asking %s for the cluster's password key: %w", member, err)
	}
	decoded, err := raw.Control()
	if err != nil {
		return transport.PepperReply{}, fmt.Errorf("password key reply from %s: %w", member, err)
	}
	answer, ok := decoded.(transport.PepperReply)
	if !ok {
		return transport.PepperReply{}, fmt.Errorf("%s answered PEPPER_REQUEST with %T, want PEPPER_REPLY", member, decoded)
	}
	return answer, nil
}
