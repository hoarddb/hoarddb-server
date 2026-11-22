package transport

import (
	"encoding/binary"
	"fmt"
)

// The control kinds of the proposal relay (conventions section 6.1, SPEC-GAP R-1): a member
// that is not the Raft leader hands one cluster command to the leader, once.
const (
	// KindRelayProposal is RELAY_PROPOSAL: one cluster command for the leader to commit.
	KindRelayProposal byte = 0x19
	// KindRelayProposalReply is the leader's answer to RELAY_PROPOSAL.
	KindRelayProposalReply byte = 0x1A
)

const (
	// RelayRequestIDLen is the length of the id that makes a retried proposal recognisable.
	RelayRequestIDLen = 16
	// MaxRelayBody bounds a relayed command's BSON body. Cluster commands are small
	// documents (a database row, a setting, a schema bump), so a megabyte is generous.
	MaxRelayBody = 1 << 20
	// MaxRelayMessage bounds a refusal's text.
	MaxRelayMessage = 16 << 10

	relayProposalFixed = RelayRequestIDLen + 1 + 4
)

// RelayStatus is the first byte of a RELAY_PROPOSAL_REPLY.
type RelayStatus byte

// The statuses of a relay reply.
const (
	// RelayApplied: the command committed; Epoch and StateSeq are the state after it.
	RelayApplied RelayStatus = 0
	// RelayNotLeader: the receiver is not the Raft leader; Leader is its hint, empty when
	// it knows none. Nothing was applied.
	RelayNotLeader RelayStatus = 1
	// RelayNoQuorum: the leader could not commit within its bound. The command may still
	// commit later; the proposer must treat the attempt as undecided.
	RelayNoQuorum RelayStatus = 2
	// RelayRefused: the leader's FSM or the relay's own rules refused the command; Code and
	// Message are the refusal, and nothing changed.
	RelayRefused RelayStatus = 3
)

// RelayProposal is RELAY_PROPOSAL: [requestID:16][kind:1][bodyLen:4][body]. Kind is the
// cluster command kind and Body its BSON document. The sender's identity is the
// handshake's, never a field here, and there is no hop count because the leader never relays.
type RelayProposal struct {
	RequestID [RelayRequestIDLen]byte
	Kind      byte
	Body      []byte
}

// RelayReply is RELAY_PROPOSAL_REPLY. Which fields are meaningful follows Status.
type RelayReply struct {
	Status     RelayStatus
	Epoch      uint64
	StateSeq   uint64
	EpochMoved bool
	Leader     string
	Code       uint16
	Message    string
}

func (RelayProposal) controlKind() byte { return KindRelayProposal }
func (RelayReply) controlKind() byte    { return KindRelayProposalReply }

func (m RelayProposal) appendBody(dst []byte) ([]byte, error) {
	if m.Kind == 0 {
		return nil, fmt.Errorf("%w: RELAY_PROPOSAL names no command kind", ErrControlBody)
	}
	if len(m.Body) == 0 || len(m.Body) > MaxRelayBody {
		return nil, fmt.Errorf("%w: RELAY_PROPOSAL body is %d bytes, it must be 1 to %d", ErrControlBody, len(m.Body), MaxRelayBody)
	}
	dst = append(dst, m.RequestID[:]...)
	dst = append(dst, m.Kind)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(m.Body)))
	return append(dst, m.Body...), nil
}

func (m RelayReply) appendBody(dst []byte) ([]byte, error) {
	dst = append(dst, byte(m.Status))
	switch m.Status {
	case RelayApplied:
		dst = binary.BigEndian.AppendUint64(dst, m.Epoch)
		dst = binary.BigEndian.AppendUint64(dst, m.StateSeq)
		if m.EpochMoved {
			return append(dst, 1), nil
		}
		return append(dst, 0), nil
	case RelayNotLeader:
		if len(m.Leader) > 255 {
			return nil, fmt.Errorf("%w: RELAY_PROPOSAL_REPLY leader hint is %d bytes, it must be 0 to 255", ErrControlBody, len(m.Leader))
		}
		dst = append(dst, byte(len(m.Leader)))
		return append(dst, m.Leader...), nil
	case RelayNoQuorum:
		return dst, nil
	case RelayRefused:
		if len(m.Message) > MaxRelayMessage {
			return nil, fmt.Errorf("%w: RELAY_PROPOSAL_REPLY refusal text is %d bytes, it must be at most %d", ErrControlBody, len(m.Message), MaxRelayMessage)
		}
		dst = binary.BigEndian.AppendUint16(dst, m.Code)
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(m.Message)))
		return append(dst, m.Message...), nil
	}
	return nil, fmt.Errorf("%w: RELAY_PROPOSAL_REPLY status %d is not one of 0 to 3", ErrControlBody, m.Status)
}

// decodeRelayProposal reads a RELAY_PROPOSAL body. Every length is checked against the
// bytes present and against its cap before anything is copied.
func decodeRelayProposal(body []byte) (RelayProposal, error) {
	if len(body) < relayProposalFixed {
		return RelayProposal{}, fmt.Errorf("%w: RELAY_PROPOSAL body is %d bytes, shorter than its %d-byte header", ErrControlBody, len(body), relayProposalFixed)
	}
	if body[RelayRequestIDLen] == 0 {
		return RelayProposal{}, fmt.Errorf("%w: RELAY_PROPOSAL names no command kind", ErrControlBody)
	}
	bodyLen := binary.BigEndian.Uint32(body[RelayRequestIDLen+1:])
	if bodyLen == 0 || bodyLen > MaxRelayBody {
		return RelayProposal{}, fmt.Errorf("%w: RELAY_PROPOSAL body length %d is outside 1 to %d", ErrControlBody, bodyLen, MaxRelayBody)
	}
	if uint64(len(body)-relayProposalFixed) != uint64(bodyLen) {
		return RelayProposal{}, fmt.Errorf("%w: RELAY_PROPOSAL says %d body bytes and carries %d", ErrControlBody, bodyLen, len(body)-relayProposalFixed)
	}
	msg := RelayProposal{Kind: body[RelayRequestIDLen], Body: append([]byte(nil), body[relayProposalFixed:]...)}
	copy(msg.RequestID[:], body)
	return msg, nil
}

// decodeRelayReply reads a RELAY_PROPOSAL_REPLY body.
func decodeRelayReply(body []byte) (RelayReply, error) {
	if len(body) < 1 {
		return RelayReply{}, fmt.Errorf("%w: RELAY_PROPOSAL_REPLY is empty", ErrControlBody)
	}
	status, rest := RelayStatus(body[0]), body[1:]
	switch status {
	case RelayApplied:
		if err := controlBodyLength(KindRelayProposalReply, rest, 17); err != nil {
			return RelayReply{}, err
		}
		if rest[16] > 1 {
			return RelayReply{}, fmt.Errorf("%w: RELAY_PROPOSAL_REPLY epochMoved byte is %d, it must be 0 or 1", ErrControlBody, rest[16])
		}
		return RelayReply{Status: status, Epoch: binary.BigEndian.Uint64(rest), StateSeq: binary.BigEndian.Uint64(rest[8:]), EpochMoved: rest[16] == 1}, nil
	case RelayNotLeader:
		if len(rest) < 1 {
			return RelayReply{}, fmt.Errorf("%w: RELAY_PROPOSAL_REPLY NOT_LEADER has no hint length", ErrControlBody)
		}
		if err := controlBodyLength(KindRelayProposalReply, rest, 1+int(rest[0])); err != nil {
			return RelayReply{}, err
		}
		return RelayReply{Status: status, Leader: string(rest[1:])}, nil
	case RelayNoQuorum:
		if err := controlBodyLength(KindRelayProposalReply, rest, 0); err != nil {
			return RelayReply{}, err
		}
		return RelayReply{Status: status}, nil
	case RelayRefused:
		if len(rest) < 6 {
			return RelayReply{}, fmt.Errorf("%w: RELAY_PROPOSAL_REPLY REFUSED is %d bytes, shorter than its 6-byte header", ErrControlBody, len(rest))
		}
		textLen := binary.BigEndian.Uint32(rest[2:])
		if textLen > MaxRelayMessage {
			return RelayReply{}, fmt.Errorf("%w: RELAY_PROPOSAL_REPLY refusal text length %d is over %d", ErrControlBody, textLen, MaxRelayMessage)
		}
		if err := controlBodyLength(KindRelayProposalReply, rest, 6+int(textLen)); err != nil {
			return RelayReply{}, err
		}
		return RelayReply{Status: status, Code: binary.BigEndian.Uint16(rest), Message: string(rest[6:])}, nil
	}
	return RelayReply{}, fmt.Errorf("%w: RELAY_PROPOSAL_REPLY status %d is not one of 0 to 3", ErrControlBody, status)
}
