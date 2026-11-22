package wire

import (
	"encoding/binary"
	"math"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// This file is the codec of the identity channel (membership node-side plan section 1.2): the
// leader sends IDENTITY_REQUEST to a node it is about to add, and the node answers with the facts
// only it holds. The codec is canonical: fixed widths, lengths 1 to 255, UTF-8 without a control
// character, a body over identityMaxBytes refused before anything is allocated, no trailing byte,
// flag bits 4 to 7 of the reply zero. Flag bit 1 says a client address follows the node id; it is
// written only when the node has one, so a node with none encodes as before. The request is the
// nonce and one flags byte whose bit 0 admits the node (BUG-0870); bits 1 to 7 are refused, and so
// is the 8-byte request of the earlier build (no compatibility before the first release).

const (
	// identityRequestBytes is the body of IDENTITY_REQUEST: the nonce, then the flags byte.
	identityRequestBytes = 8 + 1
	// identityRequestAdmit is the request's flags bit 0: the asker admits the node to its Raft
	// channel. It is the request's only flag.
	identityRequestAdmit = 1 << 0
	// identityMaxBytes bounds an IDENTITY_REPLY body; the longest valid one is 802 bytes without the range and the build.
	identityMaxBytes = 802 + 3 + 1 + identityMaxBuild
	// identityFlagHoldsState is flags bit 0: the node holds Raft state (it has bootstrapped or
	// been joined before).
	identityFlagHoldsState = 1 << 0
	// identityFlagClientAddr is flags bit 1: a client address follows the node id.
	identityFlagClientAddr = 1 << 1
	// identityFlagRange is flags bit 2: three bytes follow the client address, the node's state
	// range as min, max, writes. identityFlagBuild is flags bit 3: a length-prefixed build string
	// follows them. Both are written only when set, so a node from before them encodes as before
	// and the leader reads its reply as level 0.
	identityFlagRange  = 1 << 2
	identityFlagBuild  = 1 << 3
	identityKnownFlags = identityFlagHoldsState | identityFlagClientAddr | identityFlagRange | identityFlagBuild
	// identityMaxBuild bounds the build string (version.Version).
	identityMaxBuild   = 64
	identityFixedBytes = 8 + 8 + 1 + 1 + 16
)

// IdentityRequest is the body of IDENTITY_REQUEST: the nonce, and whether the asker admits the node
// to the cluster's Raft channel (BUG-0870 guard; see Node.serveIdentity).
type IdentityRequest struct {
	// Nonce is echoed by the reply, so a reply cannot answer another question.
	Nonce uint64
	// Admit is flags bit 0: the asking leader is about to add the node, so a node that opened with
	// no Raft state may take the leader's Raft traffic from now on.
	Admit bool
}

// IdentityReply is the body of IDENTITY_REPLY: what a node says about itself to the leader that
// asked, once, before it is a member.
type IdentityReply struct {
	// Nonce echoes the request's, so a reply cannot answer another question.
	Nonce uint64
	// Incarnation is the node's incarnation, minted at its first start (at least 1).
	Incarnation int64
	// Witness is the node's own cluster.witness setting.
	Witness bool
	// HoldsRaftState is flags bit 0: the node bootstrapped or was joined before.
	HoldsRaftState bool
	// ClusterID is the cluster the node belongs to, zero for a node that belongs to none.
	ClusterID [16]byte
	// DataAddr is the internode address the node advertises, which its peers will dial.
	DataAddr string
	// NodeID is the node's own id, a cross-check of the key it authenticated with.
	NodeID string
	// ClientAddr is the address drivers connect to on this node, or empty when it publishes none.
	// The leader checks it against the data address the pinned key vouches for before it commits it.
	ClientAddr string
	// Range is the state-level range the node can act at; the zero value is level 0, which an
	// older build's reply (no flag bit 2) reads as.
	Range cluster.StateRange
	// Build is the node's build string, empty when it sent none, so the join text can name it.
	Build string
}

// EncodeIdentityRequest returns the body of IDENTITY_REQUEST.
func EncodeIdentityRequest(request IdentityRequest) []byte {
	out := binary.BigEndian.AppendUint64(make([]byte, 0, identityRequestBytes), request.Nonce)
	var flags byte
	if request.Admit {
		flags |= identityRequestAdmit
	}
	return append(out, flags)
}

// DecodeIdentityRequest reads an IDENTITY_REQUEST body: exactly the nonce and the flags byte, with no
// flag bit but bit 0 set. What it accepts encodes back to the same bytes.
func DecodeIdentityRequest(body []byte) (IdentityRequest, error) {
	if len(body) != identityRequestBytes {
		return IdentityRequest{}, ringRefusal("an IDENTITY_REQUEST body is %d bytes and it is exactly %d, the nonce and the flags byte", len(body), identityRequestBytes)
	}
	flags := body[8]
	if flags&^identityRequestAdmit != 0 {
		return IdentityRequest{}, ringRefusal("an IDENTITY_REQUEST sets flag bits 0x%02x of which this build defines only bit 0", flags)
	}
	return IdentityRequest{Nonce: binary.BigEndian.Uint64(body), Admit: flags&identityRequestAdmit != 0}, nil
}

// checkIdentityText bounds a node id or an address of the reply.
func checkIdentityText(what, text string) error {
	if len(text) == 0 || len(text) > math.MaxUint8 {
		return ringRefusal("an IDENTITY_REPLY %s is %d bytes and it is 1 to 255", what, len(text))
	}
	if !utf8.ValidString(text) || strings.IndexFunc(text, unicode.IsControl) >= 0 {
		return ringRefusal("an IDENTITY_REPLY %s is not printable UTF-8", what)
	}
	return nil
}

// EncodeIdentityReply returns the body of IDENTITY_REPLY, refusing a value the decoder would.
func EncodeIdentityReply(reply IdentityReply) ([]byte, error) {
	if reply.Incarnation < 1 {
		return nil, ringRefusal("an IDENTITY_REPLY incarnation is %d and the first one is 1", reply.Incarnation)
	}
	if err := checkIdentityText("node id", reply.NodeID); err != nil {
		return nil, err
	}
	if err := checkIdentityText("data address", reply.DataAddr); err != nil {
		return nil, err
	}
	if reply.ClientAddr != "" {
		if err := checkIdentityText("client address", reply.ClientAddr); err != nil {
			return nil, err
		}
	}
	out := make([]byte, 0, identityFixedBytes+3+len(reply.DataAddr)+len(reply.NodeID)+len(reply.ClientAddr))
	out = binary.BigEndian.AppendUint64(out, reply.Nonce)
	out = binary.BigEndian.AppendUint64(out, uint64(reply.Incarnation))
	out = append(out, boolByte(reply.Witness))
	var flags byte
	if reply.HoldsRaftState {
		flags |= identityFlagHoldsState
	}
	if reply.ClientAddr != "" {
		flags |= identityFlagClientAddr
	}
	if !reply.Range.Valid() {
		return nil, ringRefusal("an IDENTITY_REPLY state range min %d, max %d, writes %d is malformed: it is 1 <= min <= writes <= max", reply.Range.Min, reply.Range.Max, reply.Range.Writes)
	}
	if reply.Range != (cluster.StateRange{}) {
		flags |= identityFlagRange
	}
	if reply.Build != "" {
		if len(reply.Build) > identityMaxBuild || checkIdentityText("build", reply.Build) != nil {
			return nil, ringRefusal("an IDENTITY_REPLY build is 1 to %d bytes of printable UTF-8", identityMaxBuild)
		}
		flags |= identityFlagBuild
	}
	out = append(out, flags)
	out = append(out, reply.ClusterID[:]...)
	out = append(out, byte(len(reply.DataAddr)))
	out = append(out, reply.DataAddr...)
	out = append(out, byte(len(reply.NodeID)))
	out = append(out, reply.NodeID...)
	if reply.ClientAddr != "" {
		out = append(out, byte(len(reply.ClientAddr)))
		out = append(out, reply.ClientAddr...)
	}
	if flags&identityFlagRange != 0 {
		out = append(out, reply.Range.Min, reply.Range.Max, reply.Range.Writes)
	}
	if reply.Build != "" {
		out = append(out, byte(len(reply.Build)))
		out = append(out, reply.Build...)
	}
	return out, nil
}

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// DecodeIdentityReply reads an IDENTITY_REPLY body. What it accepts encodes back to the same bytes.
func DecodeIdentityReply(body []byte) (IdentityReply, error) {
	if len(body) > identityMaxBytes {
		return IdentityReply{}, ringRefusal("an IDENTITY_REPLY body is %d bytes and it is at most %d", len(body), identityMaxBytes)
	}
	if len(body) < identityFixedBytes+4 {
		return IdentityReply{}, ringRefusal("an IDENTITY_REPLY body is %d bytes, shorter than its fixed part and two names", len(body))
	}
	incarnation := binary.BigEndian.Uint64(body[8:])
	if incarnation < 1 || incarnation > math.MaxInt64 {
		return IdentityReply{}, ringRefusal("an IDENTITY_REPLY incarnation is %d and it is 1 or more", incarnation)
	}
	if body[16] > 1 {
		return IdentityReply{}, ringRefusal("an IDENTITY_REPLY witness byte is %d and it is 0 or 1", body[16])
	}
	if body[17]&^identityKnownFlags != 0 {
		return IdentityReply{}, ringRefusal("an IDENTITY_REPLY sets flag bits 0x%02x of which this build defines only bits 0 to 3", body[17])
	}
	reply := IdentityReply{
		Nonce: binary.BigEndian.Uint64(body), Incarnation: int64(incarnation),
		Witness: body[16] == 1, HoldsRaftState: body[17]&identityFlagHoldsState != 0,
	}
	copy(reply.ClusterID[:], body[18:34])
	rest := body[identityFixedBytes:]
	var err error
	if reply.DataAddr, rest, err = takeIdentityText("data address", rest); err != nil {
		return IdentityReply{}, err
	}
	if reply.NodeID, rest, err = takeIdentityText("node id", rest); err != nil {
		return IdentityReply{}, err
	}
	if body[17]&identityFlagClientAddr != 0 {
		if reply.ClientAddr, rest, err = takeIdentityText("client address", rest); err != nil {
			return IdentityReply{}, err
		}
	}
	if body[17]&identityFlagRange != 0 {
		if len(rest) < 3 {
			return IdentityReply{}, ringRefusal("an IDENTITY_REPLY sets the range flag but holds %d of its 3 range bytes", len(rest))
		}
		reply.Range = cluster.StateRange{Min: rest[0], Max: rest[1], Writes: rest[2]}
		if !reply.Range.Valid() {
			return IdentityReply{}, ringRefusal("an IDENTITY_REPLY state range min %d, max %d, writes %d is malformed: it is 1 <= min <= writes <= max", reply.Range.Min, reply.Range.Max, reply.Range.Writes)
		}
		if reply.Range == (cluster.StateRange{}) {
			return IdentityReply{}, ringRefusal("an IDENTITY_REPLY sets the range flag with a range of zeros; a node with no range leaves the flag clear")
		}
		rest = rest[3:]
	}
	if body[17]&identityFlagBuild != 0 {
		if reply.Build, rest, err = takeIdentityText("build", rest); err != nil {
			return IdentityReply{}, err
		}
		if len(reply.Build) > identityMaxBuild {
			return IdentityReply{}, ringRefusal("an IDENTITY_REPLY build is %d bytes and it is at most %d", len(reply.Build), identityMaxBuild)
		}
	}
	if len(rest) != 0 {
		return IdentityReply{}, ringRefusal("an IDENTITY_REPLY has %d bytes after its last field, which none may", len(rest))
	}
	return reply, nil
}

// takeIdentityText reads one length-prefixed string and returns what follows it.
func takeIdentityText(what string, rest []byte) (string, []byte, error) {
	if len(rest) < 1 || int(rest[0]) > len(rest)-1 {
		return "", nil, ringRefusal("an IDENTITY_REPLY %s announces more bytes than the body holds", what)
	}
	text := string(rest[1 : 1+int(rest[0])])
	if err := checkIdentityText(what, text); err != nil {
		return "", nil, err
	}
	return text, rest[1+int(rest[0]):], nil
}
