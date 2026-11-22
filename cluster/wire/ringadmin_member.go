package wire

import (
	"context"
	"net"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// This file is the codec and the server end of the five membership commands of RING_ADMIN
// (entry-ring-admin-wire.md, "Membership"): join, promote, leave, remove_node and
// replace_node. The codec follows ringadmin.go: members in the stated order, none duplicated
// or extra, an optional member absent rather than written as its default, sizes checked
// before a document is unpacked. The server end authorizes the class once, with no database,
// and hands the request to a MembershipOps; what the node does is not decided here.

// The cmd values of the membership commands.
const (
	// AdminJoin registers a node as a non-voting member.
	AdminJoin = "join"
	// AdminPromote makes a joined non-voter a voter.
	AdminPromote = "promote"
	// AdminLeave asks a member to leave the cluster in an orderly way.
	AdminLeave = "leave"
	// AdminRemoveNode removes a member that is down or dead.
	AdminRemoveNode = "remove_node"
	// AdminReplaceNode puts a new machine in the place of a down or dead member.
	AdminReplaceNode = "replace_node"
)

// The classes of the membership commands. They are never on the wire, like the other admin
// classes, and they sit in 0xF0 to 0xF4, above the highest transport opcode (0xDF) and above
// 0xE0 to 0xEF, which stay free for ring wire opcodes: no class byte can read as an opcode in
// an audit line. A RING_KEYED body that carries one as
// its class byte is refused.
const (
	// RingClassJoin is a join command.
	RingClassJoin RingClass = 0xF0
	// RingClassPromote is a promote command.
	RingClassPromote RingClass = 0xF1
	// RingClassLeave is a leave command.
	RingClassLeave RingClass = 0xF2
	// RingClassRemoveNode is a remove_node command.
	RingClassRemoveNode RingClass = 0xF3
	// RingClassReplaceNode is a replace_node command.
	RingClassReplaceNode RingClass = 0xF4
)

// ringMaxMemberResult bounds the result line of a membership reply, in bytes.
const ringMaxMemberResult = 512

// membershipClasses lists the five classes, for RingClasses.
func membershipClasses() []RingClass {
	return []RingClass{RingClassJoin, RingClassPromote, RingClassLeave, RingClassRemoveNode, RingClassReplaceNode}
}

func isMembershipCmd(cmd string) bool {
	switch cmd {
	case AdminJoin, AdminPromote, AdminLeave, AdminRemoveNode, AdminReplaceNode:
		return true
	}
	return false
}

// membershipClassOf is the class a membership cmd is authorized as.
func membershipClassOf(cmd string) RingClass {
	switch cmd {
	case AdminJoin:
		return RingClassJoin
	case AdminPromote:
		return RingClassPromote
	case AdminLeave:
		return RingClassLeave
	case AdminRemoveNode:
		return RingClassRemoveNode
	}
	return RingClassReplaceNode
}

// RingAdminMembership is the reply of the five membership commands.
type RingAdminMembership struct {
	// Epoch and StateSeq are the committed position of the change, both 0 or more.
	Epoch, StateSeq int64
	// Result is one line, 1 to 512 bytes of UTF-8 with no control character, saying what was committed.
	Result string
}

type adminMemberJoinDoc struct {
	Cmd      string `bson:"cmd"`
	NodeID   string `bson:"node_id"`
	DataAddr string `bson:"data_addr"`
	Witness  bool   `bson:"witness,omitempty"`
}

type adminMemberNodeDoc struct {
	Cmd    string `bson:"cmd"`
	NodeID string `bson:"node_id"`
}

type adminMemberReplaceDoc struct {
	Cmd        string `bson:"cmd"`
	NodeID     string `bson:"node_id"`
	DataAddr   string `bson:"data_addr"`
	AcceptLoss bool   `bson:"accept_loss,omitempty"`
}

type adminMemberReplyDoc struct {
	Cmd      string `bson:"cmd"`
	Epoch    int64  `bson:"epoch"`
	StateSeq int64  `bson:"state_seq"`
	Result   string `bson:"result"`
}

// checkMemberText bounds a node id or a data address: 1 to 256 bytes of UTF-8 with no control character.
func checkMemberText(cmd, member, text string) error {
	if len(text) == 0 || len(text) > ringMaxNodeText {
		fix := "Send the node id that 'nodes' lists"
		if member == "data_addr" {
			fix = "Send host:port, the address the node serves the cluster on, for example node-b.example:4433"
		}
		return ringRefusal("%s: %s is %d bytes and it is 1 to %d. %s", cmd, member, len(text), ringMaxNodeText, fix)
	}
	if !utf8.ValidString(text) {
		return ringRefusal("%s: %s is not valid UTF-8. Send it as UTF-8 text", cmd, member)
	}
	if at := strings.IndexFunc(text, unicode.IsControl); at >= 0 {
		return ringRefusal("%s: %s has a control character at byte %d, which none may hold. Send printable characters", cmd, member, at)
	}
	return nil
}

// checkMemberAddr checks a data address: text, then host:port with a host and a port from 1 to 65535.
func checkMemberAddr(cmd, addr string) error {
	if err := checkMemberText(cmd, "data_addr", addr); err != nil {
		return err
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return ringRefusal("%s: data_addr %q is not host:port. Send the address the node serves the cluster on, for example node-b.example:4433", cmd, addr)
	}
	if host == "" {
		return ringRefusal("%s: data_addr %q has no host. Send host:port, for example node-b.example:4433", cmd, addr)
	}
	if number, err := strconv.ParseUint(port, 10, 16); err != nil || number == 0 {
		return ringRefusal("%s: data_addr %q has port %q and a port is 1 to 65535. Send host:port, for example node-b.example:4433", cmd, addr, port)
	}
	return nil
}

// checkMembershipRequest checks the members of the five membership requests.
func checkMembershipRequest(req RingAdminRequest) error {
	if err := adminStray(req, ""); err != nil {
		return err
	}
	if err := checkMemberText(req.Cmd, "node_id", req.NodeID); err != nil {
		return err
	}
	takesAddr := req.Cmd == AdminJoin || req.Cmd == AdminReplaceNode
	switch {
	case takesAddr:
		if err := checkMemberAddr(req.Cmd, req.DataAddr); err != nil {
			return err
		}
	case req.DataAddr != "":
		return ringRefusal("%s takes only node_id and cmd, and this request also sets data_addr. Remove data_addr", req.Cmd)
	}
	if req.Witness && req.Cmd != AdminJoin {
		return ringRefusal("%s takes no witness, which only join takes. Remove witness; a node is made a witness when it joins", req.Cmd)
	}
	if req.AcceptLoss && req.Cmd != AdminReplaceNode {
		return ringRefusal("%s takes no accept_loss, which only replace_node takes. Remove accept_loss", req.Cmd)
	}
	return nil
}

func membershipDocOf(req RingAdminRequest) any {
	switch req.Cmd {
	case AdminJoin:
		return adminMemberJoinDoc{Cmd: req.Cmd, NodeID: req.NodeID, DataAddr: req.DataAddr, Witness: req.Witness}
	case AdminReplaceNode:
		return adminMemberReplaceDoc{Cmd: req.Cmd, NodeID: req.NodeID, DataAddr: req.DataAddr, AcceptLoss: req.AcceptLoss}
	}
	return adminMemberNodeDoc{Cmd: req.Cmd, NodeID: req.NodeID}
}

// membershipMembers lists the members each membership command takes besides cmd.
var membershipMembers = map[string][]string{
	AdminJoin:        {"node_id", "data_addr", "witness"},
	AdminPromote:     {"node_id"},
	AdminLeave:       {"node_id"},
	AdminRemoveNode:  {"node_id"},
	AdminReplaceNode: {"node_id", "data_addr", "accept_loss"},
}

func unpackMembershipRequest(cmd string, raw []byte) (RingAdminRequest, error) {
	bad := func(err error) (RingAdminRequest, error) {
		return RingAdminRequest{}, ringRefusal("a %s request is not the document of entry-ring-admin-wire.md section 2 (the members and types of that command): %v", cmd, err)
	}
	// The typed unmarshal below drops a member it has no field for, so name one here, as the
	// describe_node arm does, instead of leaving the re-encode compare to refuse it unnamed.
	members, err := bson.Raw(raw).Elements()
	if err != nil {
		return bad(err)
	}
	for _, member := range members {
		if key := member.Key(); key != "cmd" && !slices.Contains(membershipMembers[cmd], key) {
			return RingAdminRequest{}, ringRefusal("%s takes only %s and cmd, and this request also sets %s. Remove %s",
				cmd, strings.Join(membershipMembers[cmd], ", "), key, key)
		}
	}
	switch cmd {
	case AdminJoin:
		var doc adminMemberJoinDoc
		if err := bson.Unmarshal(raw, &doc); err != nil {
			return bad(err)
		}
		return RingAdminRequest{Cmd: cmd, NodeID: doc.NodeID, DataAddr: doc.DataAddr, Witness: doc.Witness}, nil
	case AdminReplaceNode:
		var doc adminMemberReplaceDoc
		if err := bson.Unmarshal(raw, &doc); err != nil {
			return bad(err)
		}
		return RingAdminRequest{Cmd: cmd, NodeID: doc.NodeID, DataAddr: doc.DataAddr, AcceptLoss: doc.AcceptLoss}, nil
	}
	var doc adminMemberNodeDoc
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return bad(err)
	}
	return RingAdminRequest{Cmd: cmd, NodeID: doc.NodeID}, nil
}

// checkMembershipReply checks the reply of a membership command; zeroRest says no other member is set.
func checkMembershipReply(reply RingAdminReply, zeroRest bool) error {
	if reply.Membership == nil || !zeroRest {
		return ringRefusal("a %s reply is epoch, state_seq and result, and nothing else", reply.Cmd)
	}
	membership := reply.Membership
	if membership.Epoch < 0 || membership.StateSeq < 0 {
		return ringRefusal("epoch is %d and state_seq %d, each 0 or more", membership.Epoch, membership.StateSeq)
	}
	if len(membership.Result) == 0 || len(membership.Result) > ringMaxMemberResult {
		return ringRefusal("a result line is %d bytes and must be 1 to %d", len(membership.Result), ringMaxMemberResult)
	}
	if !utf8.ValidString(membership.Result) || strings.IndexFunc(membership.Result, unicode.IsControl) >= 0 {
		return ringRefusal("a result line is one line of printable UTF-8")
	}
	return nil
}

func unpackMembershipReply(cmd string, raw []byte) (RingAdminReply, error) {
	var doc adminMemberReplyDoc
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return RingAdminReply{}, ringRefusal("a %s reply is not the document of entry-ring-admin-wire.md section 3: %v", cmd, err)
	}
	return RingAdminReply{Cmd: cmd, Membership: &RingAdminMembership{Epoch: doc.Epoch, StateSeq: doc.StateSeq, Result: doc.Result}}, nil
}

// MembershipOutcome is what a MembershipOps method reports on success: the committed position
// and the line the reply carries.
type MembershipOutcome struct {
	// Epoch and StateSeq are the committed position of the change.
	Epoch, StateSeq int64
	// Result is one human line saying what was committed.
	Result string
}

// MembershipOps is what the server end of the membership commands calls once the session is
// authorized. A method returns a *cluster.Error, which the session sees unchanged, for a refusal
// it can explain; any other error becomes UNAVAILABLE carrying err.Error() verbatim, so wrap with
// the node id and never with another member's address. An implementation must:
//
//   - act only on the leader, committing or forwarding, never applying locally on a follower;
//   - check the setting that enables membership itself, inside the method (NoMembership is a
//     default, not a switch), once that setting exists;
//   - be idempotent by content: a join retried after a lost reply returns the same committed
//     epoch, state_seq and result, not ALREADY_EXISTS, because the interface carries no request id;
//   - refuse join while the cluster holds any database (owner ruling, 2026-11-01), with the text
//     that names the fix;
//   - treat ctx as commit-or-not: a cancelled ctx leaves no half-applied entry, and the outcome
//     is read after the commit;
//   - reuse the legacy codes (remove_node of an unknown node is NOT_FOUND naming 'nodes');
//   - decide the data address itself. The wire checks only host:port and does not refuse
//     loopback, wildcard or link-local addresses, because a one-machine cluster is legal. The
//     refusal belongs in Join and ReplaceNode, mirroring config.validateClusterReachability
//     (name HOARDDB_LISTEN and the fix); there is no address denylist, and the address must not
//     be dialled before the membership commit has succeeded.
type MembershipOps interface {
	// Join registers node id at the data address addr as a non-voting member; witness makes it a witness.
	Join(ctx context.Context, id, addr string, witness bool) (MembershipOutcome, error)
	// Promote makes the joined non-voter id a voter.
	Promote(ctx context.Context, id string) (MembershipOutcome, error)
	// Leave starts the orderly departure of member id.
	Leave(ctx context.Context, id string) (MembershipOutcome, error)
	// RemoveNode removes member id, which must be down or dead.
	RemoveNode(ctx context.Context, id string) (MembershipOutcome, error)
	// ReplaceNode gives member id, down or dead, to the process now at the data address addr,
	// which must be the member's own (BUG-0870); acceptLoss accepts that the pairs only id held
	// lose their documents.
	ReplaceNode(ctx context.Context, id, addr string, acceptLoss bool) (MembershipOutcome, error)
}

// ErrMembershipDisabled is what NoMembership refuses every call with. It is INVALID_ARGUMENT
// (exit 2) and names the one setting that exists today and the restart; it names no key that is not built.
var ErrMembershipDisabled error = &cluster.Error{Code: cluster.CodeInvalidArgument,
	Message: "membership changes are not available on this node yet: set cluster.enabled: true and restart; the join, promote, leave, remove-node and replace-node commands arrive with the cluster membership work"}

// NoMembership is the MembershipOps of a node that does not change membership: it refuses
// every call with ErrMembershipDisabled and does nothing else.
type NoMembership struct{}

// Join refuses with ErrMembershipDisabled.
func (NoMembership) Join(context.Context, string, string, bool) (MembershipOutcome, error) {
	return MembershipOutcome{}, ErrMembershipDisabled
}

// Promote refuses with ErrMembershipDisabled.
func (NoMembership) Promote(context.Context, string) (MembershipOutcome, error) {
	return MembershipOutcome{}, ErrMembershipDisabled
}

// Leave refuses with ErrMembershipDisabled.
func (NoMembership) Leave(context.Context, string) (MembershipOutcome, error) {
	return MembershipOutcome{}, ErrMembershipDisabled
}

// RemoveNode refuses with ErrMembershipDisabled.
func (NoMembership) RemoveNode(context.Context, string) (MembershipOutcome, error) {
	return MembershipOutcome{}, ErrMembershipDisabled
}

// ReplaceNode refuses with ErrMembershipDisabled.
func (NoMembership) ReplaceNode(context.Context, string, string, bool) (MembershipOutcome, error) {
	return MembershipOutcome{}, ErrMembershipDisabled
}

// WithMembership supplies the node-side behaviour of the membership commands. Without it a
// handler serves NoMembership.
func WithMembership(ops MembershipOps) RingOption {
	return func(s *ringServer) {
		if ops != nil {
			s.membership = ops
		}
	}
}

// membershipDenied is the PERMISSION_DENIED answer for a membership command: one text for
// every command and every cause, naming no node, address or setting.
func membershipDenied() error {
	return &cluster.Error{Code: cluster.CodePermissionDenied, Message: "this session cannot change the cluster's membership: the user holds no cluster-manager grant on all databases. " +
		"Nothing was done. Ask an administrator to run 'grant role cluster-manager@* to <user>' (a grant on one database is not enough) if the user should have access"}
}

// serveMembership authorizes the command's class once, with no database, before it looks at
// anything else, then calls the MembershipOps.
func (s *ringServer) serveMembership(ctx context.Context, req RingAdminRequest) (RingAdminReply, error) {
	if err := s.authz.Authorize(ctx, RingAccess{Class: membershipClassOf(req.Cmd)}); err != nil {
		return RingAdminReply{}, membershipDenied()
	}
	var outcome MembershipOutcome
	var err error
	switch req.Cmd {
	case AdminJoin:
		outcome, err = s.membership.Join(ctx, req.NodeID, req.DataAddr, req.Witness)
	case AdminPromote:
		outcome, err = s.membership.Promote(ctx, req.NodeID)
	case AdminLeave:
		outcome, err = s.membership.Leave(ctx, req.NodeID)
	case AdminRemoveNode:
		outcome, err = s.membership.RemoveNode(ctx, req.NodeID)
	default:
		outcome, err = s.membership.ReplaceNode(ctx, req.NodeID, req.DataAddr, req.AcceptLoss)
	}
	if err != nil {
		return RingAdminReply{}, err
	}
	return RingAdminReply{Cmd: req.Cmd, Membership: &RingAdminMembership{Epoch: outcome.Epoch, StateSeq: outcome.StateSeq, Result: outcome.Result}}, nil
}
