package transport

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"unicode"
	"unicode/utf8"
)

// The control kinds of the membership relay (membership node-side plan section 2): a member that
// is not the Raft leader hands one membership command to the leader, once. It is not the proposal
// relay (kinds 0x19 and 0x1A): join and promote change the Raft configuration, which only the
// leader's library handle can, so what travels is the admin command itself.

// The commands a MEMBERSHIP_RELAY carries.
const (
	MembershipJoin        byte = 1
	MembershipPromote     byte = 2
	MembershipLeave       byte = 3
	MembershipRemoveNode  byte = 4
	MembershipReplaceNode byte = 5
	// MembershipActivate is the joiner's own ask to be activated (plan section 3.6). It is
	// internal: no client statement produces it, and the leader takes it only from the member it
	// names.
	MembershipActivate byte = 6
)

// MembershipStatus is the first byte of a MEMBERSHIP_RELAY_REPLY.
type MembershipStatus byte

// The statuses of a membership relay reply.
const (
	// MembershipDone: the command ran; Epoch, StateSeq and Result are its outcome.
	MembershipDone MembershipStatus = 0
	// MembershipNotLeader: the receiver does not lead; Leader is its hint, empty when it knows none.
	MembershipNotLeader MembershipStatus = 1
	// MembershipNoQuorum: the leader could not tell whether the command committed in time. The
	// forwarder may ask again with the same request id.
	MembershipNoQuorum MembershipStatus = 2
	// MembershipRefused: the leader refused the command; Code and Message are the refusal.
	MembershipRefused MembershipStatus = 3
)

const (
	// maxMembershipText bounds a node id and an address, the length byte's range.
	maxMembershipText = 255
	// maxMembershipResult bounds a DONE result line, the bound RING_ADMIN's reply has.
	maxMembershipResult = 512
)

// MembershipRelay is MEMBERSHIP_RELAY: [requestID:16][cmd:1][flags:1][idLen:1][node_id][addrLen:1][data_addr].
// The flags byte's bit 0 is witness (join only) and bit 1 is accept_loss (replace_node only,
// BUG-0870); bits 2 to 7 are refused. The sender's identity is the handshake's, never a field here,
// and there is no hop count because the leader never forwards.
type MembershipRelay struct {
	RequestID [RelayRequestIDLen]byte
	Cmd       byte
	Witness   bool
	// AcceptLoss is replace-node's --accept-loss: the operator accepts that the pairs only the
	// replaced node held lose their documents.
	AcceptLoss bool
	NodeID     string
	DataAddr   string
}

// The bits of MEMBERSHIP_RELAY's flags byte.
const (
	membershipFlagWitness    = 1 << 0
	membershipFlagAcceptLoss = 1 << 1
)

// MembershipRelayReply is MEMBERSHIP_RELAY_REPLY. Which fields are meaningful follows Status.
type MembershipRelayReply struct {
	Status   MembershipStatus
	Epoch    uint64
	StateSeq uint64
	Result   string
	Leader   string
	Code     uint16
	Message  string
}

func (MembershipRelay) controlKind() byte      { return KindMembershipRelay }
func (MembershipRelayReply) controlKind() byte { return KindMembershipRelayReply }

func membershipText(what, text string, min int) error {
	if len(text) < min || len(text) > maxMembershipText {
		return fmt.Errorf("%w: MEMBERSHIP_RELAY %s is %d bytes, it must be %d to %d", ErrControlBody, what, len(text), min, maxMembershipText)
	}
	if !utf8.ValidString(text) || strings.IndexFunc(text, unicode.IsControl) >= 0 {
		return fmt.Errorf("%w: MEMBERSHIP_RELAY %s is not printable UTF-8", ErrControlBody, what)
	}
	return nil
}

// checkMembershipRelay is the one rule set of the request, applied to both directions.
func checkMembershipRelay(m MembershipRelay) error {
	if m.Cmd < MembershipJoin || m.Cmd > MembershipActivate {
		return fmt.Errorf("%w: MEMBERSHIP_RELAY command %d is not one of 1 to 6", ErrControlBody, m.Cmd)
	}
	if m.Witness && m.Cmd != MembershipJoin {
		return fmt.Errorf("%w: MEMBERSHIP_RELAY command %d takes no witness flag, which only join takes", ErrControlBody, m.Cmd)
	}
	if m.AcceptLoss && m.Cmd != MembershipReplaceNode {
		return fmt.Errorf("%w: MEMBERSHIP_RELAY command %d takes no accept_loss flag, which only replace_node takes", ErrControlBody, m.Cmd)
	}
	if err := membershipText("node id", m.NodeID, 1); err != nil {
		return err
	}
	takesAddr := m.Cmd == MembershipJoin || m.Cmd == MembershipReplaceNode
	if takesAddr {
		return membershipText("data address", m.DataAddr, 1)
	}
	if m.DataAddr != "" {
		return fmt.Errorf("%w: MEMBERSHIP_RELAY command %d takes no data address", ErrControlBody, m.Cmd)
	}
	return nil
}

func (m MembershipRelay) appendBody(dst []byte) ([]byte, error) {
	if err := checkMembershipRelay(m); err != nil {
		return nil, err
	}
	dst = append(dst, m.RequestID[:]...)
	var flags byte
	if m.Witness {
		flags |= membershipFlagWitness
	}
	if m.AcceptLoss {
		flags |= membershipFlagAcceptLoss
	}
	dst = append(dst, m.Cmd, flags, byte(len(m.NodeID)))
	dst = append(dst, m.NodeID...)
	dst = append(dst, byte(len(m.DataAddr)))
	return append(dst, m.DataAddr...), nil
}

func (m MembershipRelayReply) appendBody(dst []byte) ([]byte, error) {
	dst = append(dst, byte(m.Status))
	switch m.Status {
	case MembershipDone:
		if m.Epoch > math.MaxInt64 || m.StateSeq > math.MaxInt64 {
			return nil, fmt.Errorf("%w: MEMBERSHIP_RELAY_REPLY position is past the int64 range", ErrControlBody)
		}
		if len(m.Result) == 0 || len(m.Result) > maxMembershipResult || !utf8.ValidString(m.Result) || strings.IndexFunc(m.Result, unicode.IsControl) >= 0 {
			return nil, fmt.Errorf("%w: MEMBERSHIP_RELAY_REPLY result is %d bytes, it is one line of printable UTF-8 of 1 to %d", ErrControlBody, len(m.Result), maxMembershipResult)
		}
		dst = binary.BigEndian.AppendUint64(dst, m.Epoch)
		dst = binary.BigEndian.AppendUint64(dst, m.StateSeq)
		dst = binary.BigEndian.AppendUint16(dst, uint16(len(m.Result)))
		return append(dst, m.Result...), nil
	case MembershipNotLeader:
		if len(m.Leader) > maxMembershipText {
			return nil, fmt.Errorf("%w: MEMBERSHIP_RELAY_REPLY leader hint is %d bytes, it must be 0 to %d", ErrControlBody, len(m.Leader), maxMembershipText)
		}
		dst = append(dst, byte(len(m.Leader)))
		return append(dst, m.Leader...), nil
	case MembershipNoQuorum:
		return dst, nil
	case MembershipRefused:
		if len(m.Message) > MaxRelayMessage {
			return nil, fmt.Errorf("%w: MEMBERSHIP_RELAY_REPLY refusal text is %d bytes, it must be at most %d", ErrControlBody, len(m.Message), MaxRelayMessage)
		}
		dst = binary.BigEndian.AppendUint16(dst, m.Code)
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(m.Message)))
		return append(dst, m.Message...), nil
	}
	return nil, fmt.Errorf("%w: MEMBERSHIP_RELAY_REPLY status %d is not one of 0 to 3", ErrControlBody, m.Status)
}

// decodeMembershipRelay reads a MEMBERSHIP_RELAY body, checking every length against the bytes
// present before anything is copied, then the rules of checkMembershipRelay.
func decodeMembershipRelay(body []byte) (MembershipRelay, error) {
	const fixed = RelayRequestIDLen + 3
	if len(body) < fixed+1 {
		return MembershipRelay{}, fmt.Errorf("%w: MEMBERSHIP_RELAY body is %d bytes, shorter than its fixed part", ErrControlBody, len(body))
	}
	flags := body[RelayRequestIDLen+1]
	if flags&^(membershipFlagWitness|membershipFlagAcceptLoss) != 0 {
		return MembershipRelay{}, fmt.Errorf("%w: MEMBERSHIP_RELAY flags byte is 0x%02x, and only bit 0 (witness) and bit 1 (accept_loss) are defined", ErrControlBody, flags)
	}
	idLen := int(body[RelayRequestIDLen+2])
	if len(body) < fixed+idLen+1 {
		return MembershipRelay{}, fmt.Errorf("%w: MEMBERSHIP_RELAY announces a %d-byte node id the body does not hold", ErrControlBody, idLen)
	}
	addrLen := int(body[fixed+idLen])
	if len(body) != fixed+idLen+1+addrLen {
		return MembershipRelay{}, fmt.Errorf("%w: MEMBERSHIP_RELAY says %d address bytes and the body holds %d", ErrControlBody, addrLen, len(body)-fixed-idLen-1)
	}
	msg := MembershipRelay{
		Cmd: body[RelayRequestIDLen], Witness: flags&membershipFlagWitness != 0, AcceptLoss: flags&membershipFlagAcceptLoss != 0,
		NodeID: string(body[fixed : fixed+idLen]), DataAddr: string(body[fixed+idLen+1:]),
	}
	copy(msg.RequestID[:], body)
	if err := checkMembershipRelay(msg); err != nil {
		return MembershipRelay{}, err
	}
	return msg, nil
}

// decodeMembershipRelayReply reads a MEMBERSHIP_RELAY_REPLY body.
func decodeMembershipRelayReply(body []byte) (MembershipRelayReply, error) {
	if len(body) < 1 {
		return MembershipRelayReply{}, fmt.Errorf("%w: MEMBERSHIP_RELAY_REPLY is empty", ErrControlBody)
	}
	status, rest := MembershipStatus(body[0]), body[1:]
	var reply MembershipRelayReply
	switch status {
	case MembershipDone:
		if len(rest) < 18 {
			return reply, fmt.Errorf("%w: MEMBERSHIP_RELAY_REPLY DONE is %d bytes, shorter than its 18-byte fixed part", ErrControlBody, len(rest))
		}
		resultLen := int(binary.BigEndian.Uint16(rest[16:]))
		if len(rest) != 18+resultLen {
			return reply, fmt.Errorf("%w: MEMBERSHIP_RELAY_REPLY DONE says %d result bytes and carries %d", ErrControlBody, resultLen, len(rest)-18)
		}
		reply = MembershipRelayReply{Status: status, Epoch: binary.BigEndian.Uint64(rest), StateSeq: binary.BigEndian.Uint64(rest[8:]), Result: string(rest[18:])}
	case MembershipNotLeader:
		if len(rest) < 1 {
			return reply, fmt.Errorf("%w: MEMBERSHIP_RELAY_REPLY NOT_LEADER has no hint length", ErrControlBody)
		}
		if err := controlBodyLength(KindMembershipRelayReply, rest, 1+int(rest[0])); err != nil {
			return reply, err
		}
		reply = MembershipRelayReply{Status: status, Leader: string(rest[1:])}
	case MembershipNoQuorum:
		if err := controlBodyLength(KindMembershipRelayReply, rest, 0); err != nil {
			return reply, err
		}
		reply = MembershipRelayReply{Status: status}
	case MembershipRefused:
		if len(rest) < 6 {
			return reply, fmt.Errorf("%w: MEMBERSHIP_RELAY_REPLY REFUSED is %d bytes, shorter than its 6-byte header", ErrControlBody, len(rest))
		}
		textLen := binary.BigEndian.Uint32(rest[2:])
		if textLen > MaxRelayMessage {
			return reply, fmt.Errorf("%w: MEMBERSHIP_RELAY_REPLY refusal text length %d is over %d", ErrControlBody, textLen, MaxRelayMessage)
		}
		if err := controlBodyLength(KindMembershipRelayReply, rest, 6+int(textLen)); err != nil {
			return reply, err
		}
		reply = MembershipRelayReply{Status: status, Code: binary.BigEndian.Uint16(rest), Message: string(rest[6:])}
	default:
		return reply, fmt.Errorf("%w: MEMBERSHIP_RELAY_REPLY status %d is not one of 0 to 3", ErrControlBody, status)
	}
	// Whatever decodes must encode: this refuses the rules the layout alone does not carry.
	if _, err := reply.appendBody(nil); err != nil {
		return MembershipRelayReply{}, err
	}
	return reply, nil
}
