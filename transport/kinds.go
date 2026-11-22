package transport

import (
	"errors"
	"fmt"
)

// Channel names one of the five node-to-node channels of conventions section
// 6. The values are the handshake's channel byte.
type Channel byte

// The five node-to-node channels.
const (
	ChannelData    Channel = Channel(HandshakeChannelData)
	ChannelControl Channel = Channel(HandshakeChannelControl)
	ChannelSeed    Channel = Channel(HandshakeChannelSeed)
	ChannelRaft    Channel = Channel(HandshakeChannelRaft)
	// ChannelIdentity answers IDENTITY_REQUEST on the pinned key alone.
	ChannelIdentity Channel = Channel(HandshakeChannelIdentity)
)

// String returns the channel's name as the spec's tables spell it.
func (c Channel) String() string { return channelWord(byte(c)) }

// ErrInvalidKind is the node-to-node protocol refusal for an unknown kind or a
// known kind on the wrong channel. Its text is the INVALID_ARGUMENT code of
// the cluster package, which this package cannot import. It is never relayed
// to a client.
var ErrInvalidKind = errors.New("INVALID_ARGUMENT")

// Message kinds of conventions section 6.1 that other units name by constant.
const (
	// KindHandoffStopped is HANDOFF_STOPPED: the old owner has stopped
	// ordering and serving reads for the pairs. It is a data kind, equal epoch
	// required.
	KindHandoffStopped byte = 0x14
	// KindForwardWrite is FORWARD_WRITE: a forwarder hands one client write to the
	// owner it names, once (write path section 1). The reply kind is KindForwardWriteReply.
	// Not in conventions 6.1 before U57; added there as SPEC-GAP U57-2.
	KindForwardWrite byte = 0x15
	// KindForwardWriteReply is the owner's answer to FORWARD_WRITE: an answer or a refusal.
	KindForwardWriteReply byte = 0x16
	// KindForwardRead is FORWARD_READ: a keyed read rerouted to the owner, and the
	// declaration read of conventions 20 (SPEC-GAP U57-2).
	KindForwardRead byte = 0x17
	// KindForwardReadReply is the owner's answer to FORWARD_READ.
	KindForwardReadReply byte = 0x18
	// KindIdentityRequest is IDENTITY_REQUEST: the leader asks a node it is about to add who it is.
	KindIdentityRequest byte = 0x1B
	// KindIdentityReply is the node's IDENTITY_REPLY.
	KindIdentityReply byte = 0x1C
	// KindMembershipRelay is MEMBERSHIP_RELAY: a member hands one membership command to the
	// Raft leader, once (membership node-side plan section 2).
	KindMembershipRelay byte = 0x1D
	// KindMembershipRelayReply is the leader's answer to MEMBERSHIP_RELAY.
	KindMembershipRelayReply byte = 0x1E
	// KindSeedDedupe is the withdrawn SEED_DEDUPE. It is declared only so the
	// decoder can refuse it by name: a dedupe entry is an ordinary key of the
	// pair's range.
	KindSeedDedupe byte = 0x28
	// KindWriteAckBatch is WRITE_ACK_BATCH: a copy acknowledges records of several pairs in one call.
	KindWriteAckBatch byte = 0x2B
	// KindWriteBatch is WRITE_BATCH: a lane sends the records of several pairs to a copy in one call.
	KindWriteBatch byte = 0x2C
)

// kindChannels is the closed registry of section 6.1, one channel per kind.
// A new kind is added to the spec table before it is added here.
var kindChannels = map[byte]Channel{
	0x01: ChannelData, 0x02: ChannelData, 0x03: ChannelData, 0x04: ChannelData,
	0x05: ChannelData, 0x06: ChannelData, 0x07: ChannelData,
	0x08: ChannelControl, 0x09: ChannelControl, 0x0A: ChannelControl,
	0x0B: ChannelControl, 0x0C: ChannelControl, 0x0D: ChannelControl,
	0x0E: ChannelData, 0x0F: ChannelData, 0x10: ChannelData, 0x11: ChannelData,
	0x12: ChannelData, 0x13: ChannelData, KindHandoffStopped: ChannelData,
	KindForwardWrite: ChannelData, KindForwardWriteReply: ChannelData,
	KindForwardRead: ChannelData, KindForwardReadReply: ChannelData,
	KindRelayProposal: ChannelControl, KindRelayProposalReply: ChannelControl,
	KindIdentityRequest: ChannelIdentity, KindIdentityReply: ChannelIdentity,
	KindMembershipRelay: ChannelControl, KindMembershipRelayReply: ChannelControl,
	0x20: ChannelSeed, 0x21: ChannelSeed, 0x22: ChannelSeed, 0x23: ChannelSeed,
	0x24: ChannelSeed, 0x25: ChannelSeed, 0x26: ChannelSeed, 0x27: ChannelSeed,
	KindPepperRequest: ChannelControl, KindPepperReply: ChannelControl,
	KindWriteAckBatch: ChannelData, KindWriteBatch: ChannelData,
}

// KindChannel returns the one channel a kind is valid on, and false for a kind
// that is not in the registry.
func KindChannel(kind byte) (Channel, bool) {
	channel, ok := kindChannels[kind]
	return channel, ok
}

// CheckKind refuses an unknown kind, naming the number, and a known kind on a
// channel other than its own, naming the kind and the channel it belongs to.
func CheckKind(kind byte, on Channel) error {
	home, ok := KindChannel(kind)
	if !ok {
		return unknownKindError(kind)
	}
	if home != on {
		return fmt.Errorf("%w: kind %d (0x%02x) belongs to the %s channel and arrived on the %s channel",
			ErrInvalidKind, kind, kind, home, on)
	}
	return nil
}

func unknownKindError(kind byte) error {
	if kind == KindSeedDedupe {
		return fmt.Errorf("%w: unknown kind %d (0x%02x): SEED_DEDUPE is withdrawn, a dedupe entry is an ordinary key of the range",
			ErrInvalidKind, kind, kind)
	}
	return fmt.Errorf("%w: unknown kind %d (0x%02x)", ErrInvalidKind, kind, kind)
}
