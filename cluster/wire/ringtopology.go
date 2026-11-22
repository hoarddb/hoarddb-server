package wire

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

// This file is the codec of RING_TOPOLOGY (entry-ring-queries-and-clients.md Formats): a
// two-number request and a reply that carries the ranked members, the full 8192-slot table
// and the database rows, or only the node's view when the requester's still matches. Both
// directions are strict like ringlist.go: sizes are checked before anything is allocated
// for them, and a body is accepted only if it re-encodes to the same bytes.

const (
	// ringTopologyRequestMax bounds a request: two int64 members in a BSON document.
	ringTopologyRequestMax = 64
	// ringTopologySlots is the slot count of the wire table (conventions section 3).
	ringTopologySlots = 8192
	// ringTopologyMinMember is the least a member row takes by length: state, flags, the id length, an
	// id of at least one byte (a zero-length one is refused after the count check), the data address
	// length and the client address length.
	ringTopologyMinMember = 6
	// topologyFlagVoter is bit 0 of a member's flags byte: it votes in the state group. The
	// other bits are reserved and a row that sets one is refused.
	topologyFlagVoter = 1 << 0
	// ringTopologyDepth is the document, the databases array and one database document.
	ringTopologyDepth = 4
)

// RingTopologyRequest is the RING_TOPOLOGY body: queries Formats' TopologyRequest.
type RingTopologyRequest struct {
	// Epoch and StateSeq are the view the requester holds; 0 and 0 ask for everything.
	Epoch, StateSeq int64
}

// RingTopologyDatabase is one database row of a topology reply.
type RingTopologyDatabase struct {
	// Name is the database name, ID its id and RF its replication factor.
	Name string
	ID   int64
	RF   int32
}

// RingTopologyReply is the RING_REPLY body answering a RING_TOPOLOGY.
type RingTopologyReply struct {
	// Epoch and StateSeq are the node's committed view.
	Epoch, StateSeq int64
	// Unchanged is true when the requester's view matched: Table and Databases are then absent.
	Unchanged bool
	// Table is the full ranking of all 8192 slots; its Members are the reply's members blob.
	Table *cluster.SlotTable
	// Witnesses are the witnesses (voting or not; members that hold no documents), by id: never ranked, so
	// never in Table. Absent when Unchanged.
	Witnesses []cluster.Member
	// Voters is how many members vote in the cluster state group, from the committed voter list. It
	// counts a voter that is neither ranked nor listed as a witness (one that is joining or dead), so
	// a driver or the CLI derives the quorum from it and not from the flags alone. Absent when 0.
	Voters int32
	// Databases are the databases the session may see, by id ascending.
	Databases []RingTopologyDatabase
}

type ringTopologyRequestDoc struct {
	Epoch    int64 `bson:"epoch"`
	StateSeq int64 `bson:"state_seq"`
}

type ringTopologyDatabaseDoc struct {
	Name string `bson:"name"`
	ID   int64  `bson:"id"`
	RF   int32  `bson:"rf"`
}

type ringTopologyReplyDoc struct {
	Epoch     int64                     `bson:"epoch"`
	StateSeq  int64                     `bson:"state_seq"`
	Unchanged bool                      `bson:"unchanged,omitempty"`
	Members   []byte                    `bson:"members,omitempty"`
	Witnesses []byte                    `bson:"witnesses,omitempty"`
	Voters    int32                     `bson:"voters,omitempty"`
	Table     []byte                    `bson:"table,omitempty"`
	Databases []ringTopologyDatabaseDoc `bson:"databases,omitempty"`
}

func checkRingTopologyRequest(req RingTopologyRequest) error {
	if req.Epoch < 0 || req.StateSeq < 0 {
		return ringRefusal("epoch is %d and state_seq is %d; both count from 0, and 0 asks for the whole topology", req.Epoch, req.StateSeq)
	}
	return nil
}

// EncodeRingTopology returns the RING_TOPOLOGY body for req. It refuses a request
// DecodeRingTopology would not accept, with INVALID_ARGUMENT.
func EncodeRingTopology(req RingTopologyRequest) ([]byte, error) {
	if err := checkRingTopologyRequest(req); err != nil {
		return nil, err
	}
	raw, err := bson.Marshal(ringTopologyRequestDoc(req))
	if err != nil {
		return nil, fmt.Errorf("encoding a topology request: %w", err)
	}
	return raw, nil
}

// DecodeRingTopology reads a RING_TOPOLOGY body written by EncodeRingTopology. It never
// panics: the size is checked before the document is unpacked, and it is accepted only if
// it re-encodes to the same bytes, so a reordered, duplicated or extra member and a widened
// number are all refused.
func DecodeRingTopology(raw []byte) (RingTopologyRequest, error) {
	if len(raw) > ringTopologyRequestMax {
		return RingTopologyRequest{}, ringRefusal("a topology request is %d bytes and at most %d are taken: send {epoch, state_seq} as two int64 values", len(raw), ringTopologyRequestMax)
	}
	if _, err := bson.Validate(raw, bson.Limits{Depth: 2, Size: ringTopologyRequestMax}); err != nil {
		return RingTopologyRequest{}, ringRefusal("a topology request is not a valid BSON document: %v", err)
	}
	var doc ringTopologyRequestDoc
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return RingTopologyRequest{}, ringRefusal("a topology request is not the document of queries Formats (epoch and state_seq, both int64): %v", err)
	}
	req := RingTopologyRequest(doc)
	if err := checkRingTopologyRequest(req); err != nil {
		return RingTopologyRequest{}, err
	}
	again, err := bson.Marshal(doc)
	if err != nil {
		return RingTopologyRequest{}, ringRefusal("a topology request holds a value that cannot be read back: %v", err)
	}
	if !bytes.Equal(again, raw) {
		return RingTopologyRequest{}, ringRefusal("a topology request does not have the layout of queries Formats (epoch then state_seq, both int64, nothing else)")
	}
	return req, nil
}

// AppendTopologyMembers appends a members blob of queries Formats: the count, then per
// member its state byte, a flags byte (bit 0: it votes), its id, its data address and its
// client address, each text behind a uvarint length. An empty client address means the
// data address. The same layout carries the witnesses blob. The driver vectors build their
// refusal cases from it.
func AppendTopologyMembers(dst []byte, members []cluster.Member) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(members)))
	for _, member := range members {
		var flags byte
		if member.Suffrage == "voter" {
			flags |= topologyFlagVoter
		}
		dst = append(dst, byte(member.State), flags)
		dst = binary.AppendUvarint(dst, uint64(len(member.ID)))
		dst = append(dst, member.ID...)
		dst = binary.AppendUvarint(dst, uint64(len(member.DataAddr)))
		dst = append(dst, member.DataAddr...)
		dst = binary.AppendUvarint(dst, uint64(len(member.ClientAddr)))
		dst = append(dst, member.ClientAddr...)
	}
	return dst
}

// readTopologyUvarint reads a uvarint that is the shortest form of its value.
func readTopologyUvarint(raw []byte) (uint64, int, bool) {
	value, used := binary.Uvarint(raw)
	if used <= 0 || used != len(binary.AppendUvarint(nil, value)) {
		return 0, 0, false
	}
	return value, used, true
}

// readTopologyText reads a uvarint-prefixed string of at most limit bytes.
func readTopologyText(raw []byte, what string, limit uint64) (string, int, error) {
	length, used, ok := readTopologyUvarint(raw)
	if !ok || length > limit || uint64(len(raw)-used) < length {
		return "", 0, ringRefusal("members blob: the %s is cut short, longer than %d bytes or its length is not the shortest varint", what, limit)
	}
	return string(raw[used : used+int(length)]), used + int(length), nil
}

// plainTopologyText reports whether an address is valid UTF-8 with no NUL, as the state keeps it.
func plainTopologyText(text string) bool {
	return utf8.ValidString(text) && !strings.ContainsRune(text, 0)
}

// parseTopologyMembers reads a members blob, or a witnesses blob when witnesses is true.
// The members are active, down or leaving, sorted by id bytes with no id twice, which is
// the order the table's rank indices refer to. The count is checked against the bytes
// present before the slice is made.
func parseTopologyMembers(blob []byte, witnesses bool) ([]cluster.Member, error) {
	count, pos, ok := readTopologyUvarint(blob)
	if !ok {
		return nil, ringRefusal("members blob: the member count is missing or not the shortest varint")
	}
	if count > math.MaxUint16 {
		return nil, ringRefusal("members blob: %d members are claimed and a table ranks at most %d", count, math.MaxUint16)
	}
	if count > uint64(len(blob)-pos)/ringTopologyMinMember {
		return nil, ringRefusal("members blob: %d members are claimed but %d bytes follow, and a member takes at least %d", count, len(blob)-pos, ringTopologyMinMember)
	}
	members := make([]cluster.Member, 0, count)
	for index := range count {
		if len(blob)-pos < 2 {
			return nil, ringRefusal("members blob: member %d is cut short", index)
		}
		state, err := cluster.DecodeMemberState(blob[pos])
		if err != nil {
			return nil, err
		}
		if state != cluster.MemberActive && state != cluster.MemberDown && state != cluster.MemberLeaving {
			return nil, ringRefusal("members blob: member %d is %s; only active, down and leaving members are listed", index, state)
		}
		flags := blob[pos+1]
		if flags&^topologyFlagVoter != 0 {
			return nil, ringRefusal("members blob: member %d sets flag bits %#x that this format reserves", index, flags&^topologyFlagVoter)
		}
		pos += 2
		id, used, err := readTopologyText(blob[pos:], "member id", ringMaxNodeText)
		if err != nil {
			return nil, err
		}
		pos += used
		addr, used, err := readTopologyText(blob[pos:], "data address", ringMaxNodeText)
		if err != nil {
			return nil, err
		}
		pos += used
		clientAddr, used, err := readTopologyText(blob[pos:], "client address", cluster.MaxClientAddrBytes)
		if err != nil {
			return nil, err
		}
		pos += used
		if id == "" || !utf8.ValidString(id) {
			return nil, ringRefusal("members blob: member %d has an empty or non-UTF-8 id", index)
		}
		if addr == "" || !plainTopologyText(addr) || !plainTopologyText(clientAddr) {
			return nil, ringRefusal("members blob: member %d has an empty data address, or an address that is not UTF-8 or holds a NUL byte", index)
		}
		if len(members) > 0 && members[len(members)-1].ID >= id {
			return nil, ringRefusal("members blob: member %q follows %q; members are listed once each, sorted by id bytes, because the table's rank indices count in that order", id, members[len(members)-1].ID)
		}
		suffrage := "nonvoter"
		if flags&topologyFlagVoter != 0 {
			suffrage = "voter"
		}
		members = append(members, cluster.Member{ID: id, DataAddr: addr, ClientAddr: clientAddr, State: state, Suffrage: suffrage, Witness: witnesses})
	}
	if pos != len(blob) {
		return nil, ringRefusal("members blob: %d bytes follow the last member", len(blob)-pos)
	}
	return members, nil
}

func checkRingTopologyDatabases(databases []RingTopologyDatabase) error {
	seen := make(map[string]bool, len(databases))
	var previous int64
	for _, database := range databases {
		switch {
		case len(database.Name) == 0 || len(database.Name) > ringMaxName || !utf8.ValidString(database.Name):
			return ringRefusal("a database name is %d bytes and must be 1 to %d bytes of UTF-8", len(database.Name), ringMaxName)
		case database.ID <= previous:
			return ringRefusal("database %q has id %d after id %d; databases are listed by id ascending, ids from 1", database.Name, database.ID, previous)
		case database.RF < 1:
			return ringRefusal("database %q has replication factor %d, and a factor is at least 1", database.Name, database.RF)
		case seen[database.Name]:
			return ringRefusal("database %q is listed twice", database.Name)
		}
		seen[database.Name] = true
		previous = database.ID
	}
	return nil
}

// checkRingTopologyWitnesses refuses a witness row that is not a witness or that is also a
// ranked member: the table lists data members only, and a witness is never ranked.
func checkRingTopologyWitnesses(reply RingTopologyReply) error {
	if len(reply.Witnesses) == 0 {
		return nil
	}
	rankedMembers := reply.Table.Members()
	ranked := make(map[string]bool, len(rankedMembers))
	for _, member := range rankedMembers {
		ranked[member.ID] = true
	}
	for _, witness := range reply.Witnesses {
		switch {
		case !witness.Witness:
			return ringRefusal("member %q is listed as a witness but is not one: only witnesses are listed apart from the table", witness.ID)
		case ranked[witness.ID]:
			return ringRefusal("member %q is both a ranked data member and a witness", witness.ID)
		}
	}
	return nil
}

func checkRingTopologyReply(reply RingTopologyReply) error {
	if reply.Epoch < 0 || reply.StateSeq < 0 {
		return ringRefusal("epoch is %d and state_seq is %d; both count from 0", reply.Epoch, reply.StateSeq)
	}
	if reply.Unchanged && (reply.Table != nil || len(reply.Databases) != 0 || len(reply.Witnesses) != 0) {
		return ringRefusal("an unchanged topology reply carries no members, table or databases: the requester keeps what it has")
	}
	if !reply.Unchanged && reply.Table == nil {
		return ringRefusal("a topology reply that is not unchanged carries the table")
	}
	if reply.Voters < 0 || (reply.Unchanged && reply.Voters != 0) {
		return ringRefusal("a topology reply's voter count is %d: it is absent when 0 or unchanged, and never negative", reply.Voters)
	}
	if err := checkRingTopologyWitnesses(reply); err != nil {
		return err
	}
	return checkRingTopologyDatabases(reply.Databases)
}

func ringTopologyReplyDocOf(reply RingTopologyReply) (ringTopologyReplyDoc, error) {
	doc := ringTopologyReplyDoc{Epoch: reply.Epoch, StateSeq: reply.StateSeq, Unchanged: reply.Unchanged, Voters: reply.Voters}
	if reply.Table != nil {
		members := reply.Table.Members()
		doc.Members = AppendTopologyMembers(nil, members)
		if _, err := parseTopologyMembers(doc.Members, false); err != nil {
			return ringTopologyReplyDoc{}, err
		}
		doc.Table = reply.Table.Encode()
	}
	if len(reply.Witnesses) != 0 {
		doc.Witnesses = AppendTopologyMembers(nil, reply.Witnesses)
		if _, err := parseTopologyMembers(doc.Witnesses, true); err != nil {
			return ringTopologyReplyDoc{}, err
		}
	}
	for _, database := range reply.Databases {
		doc.Databases = append(doc.Databases, ringTopologyDatabaseDoc(database))
	}
	return doc, nil
}

// EncodeRingTopologyReply returns the RING_REPLY body for reply. It refuses a reply
// DecodeRingTopologyReply would not accept, with INVALID_ARGUMENT.
func EncodeRingTopologyReply(reply RingTopologyReply) ([]byte, error) {
	if err := checkRingTopologyReply(reply); err != nil {
		return nil, err
	}
	doc, err := ringTopologyReplyDocOf(reply)
	if err != nil {
		return nil, err
	}
	raw, err := bson.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("encoding a topology reply: %w", err)
	}
	// A frame's length counts its opcode and request id as well as the payload.
	if room := maxSubScanBody - 1 - transport.RequestIDLen; len(raw) > room {
		return nil, ringRefusal("a topology reply would be %d bytes and a frame holds at most %d", len(raw), room)
	}
	return raw, nil
}

// DecodeRingTopologyReply reads a RING_REPLY body written by EncodeRingTopologyReply. It
// never panics: the document is validated against the frame ceiling before it is unpacked,
// the member count and the table length are checked against the bytes present before
// anything is allocated for them, and it is accepted only if it re-encodes to the same
// bytes (no reordered or extra member, no widened integer, no written default).
func DecodeRingTopologyReply(raw []byte) (RingTopologyReply, error) {
	if _, err := bson.Validate(raw, bson.Limits{Depth: ringTopologyDepth, Size: maxSubScanBody}); err != nil {
		return RingTopologyReply{}, ringRefusal("a topology reply is not a valid BSON document: %v", err)
	}
	var doc ringTopologyReplyDoc
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return RingTopologyReply{}, ringRefusal("a topology reply is not the document of queries Formats (epoch and state_seq as int64, members, witnesses and table as binary, databases of name, id as int64 and rf as int32): %v", err)
	}
	reply := RingTopologyReply{Epoch: doc.Epoch, StateSeq: doc.StateSeq, Unchanged: doc.Unchanged, Voters: doc.Voters}
	for _, database := range doc.Databases {
		reply.Databases = append(reply.Databases, RingTopologyDatabase(database))
	}
	if doc.Unchanged {
		if doc.Members != nil || doc.Witnesses != nil || doc.Table != nil || doc.Databases != nil {
			return RingTopologyReply{}, ringRefusal("an unchanged topology reply carries no members, witnesses, table or databases")
		}
	} else {
		members, err := parseTopologyMembers(doc.Members, false)
		if err != nil {
			return RingTopologyReply{}, err
		}
		if doc.Witnesses != nil {
			if reply.Witnesses, err = parseTopologyMembers(doc.Witnesses, true); err != nil {
				return RingTopologyReply{}, err
			}
		}
		if least := ringTopologySlots * (1 + len(members)); len(doc.Table) < least {
			return RingTopologyReply{}, ringRefusal("the table is %d bytes and %d members need at least %d: one count and one rank index per member for each of %d slots", len(doc.Table), len(members), least, ringTopologySlots)
		}
		table, err := cluster.DecodeTable(doc.Table, members)
		if err != nil {
			return RingTopologyReply{}, err
		}
		reply.Table = table
	}
	if err := checkRingTopologyReply(reply); err != nil {
		return RingTopologyReply{}, err
	}
	again, err := EncodeRingTopologyReply(reply)
	if err != nil {
		return RingTopologyReply{}, ringRefusal("a topology reply holds a value that cannot be read back: %v", err)
	}
	if !bytes.Equal(again, raw) {
		return RingTopologyReply{}, ringRefusal("a topology reply does not have the layout of queries Formats (members in order, none duplicated or extra, the stated integer types, absent rather than written as a default)")
	}
	return reply, nil
}
