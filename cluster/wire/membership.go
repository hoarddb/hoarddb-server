package wire

import (
	"context"
	entropy "crypto/rand"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// This file is the node-side behaviour of the membership commands (membership node-side plan
// sections 2, 3 and 6): join and promote run on the Raft leader; a command entered on any other
// member is forwarded to the leader once; cluster.allow_membership is read first on the node the
// statement enters and again on the leader. replace_node is replace.go (BUG-0870); leave and
// remove_node are the next slice and refuse here by name.
//
// Cost: no goroutine of its own and no state per bucket or store. Per forwarded command one
// control-channel round trip and one ledger entry of about 100 bytes (membershipLedgerCap per
// node); per join one identity connection and two to four Raft operations.

const (
	// membershipLedgerCap bounds the leader's memory of answered forwarded commands.
	membershipLedgerCap = 256
	// membershipBudget bounds one membership statement from entry to answer.
	membershipBudget = 60 * time.Second
	// activationAskEvery is how often a joiner repeats its ask to be activated.
	activationAskEvery = 500 * time.Millisecond
	// activationRetries bounds how often the leader stamps an activation again after STATE_MOVED.
	activationRetries = 5
)

// membershipOps is the MembershipOps of a networked node.
type membershipOps struct{ n *Node }

// NewMembership returns the MembershipOps of node, for WithMembership. A node with no cluster
// has none to change and keeps NoMembership.
func NewMembership(node *Node) MembershipOps { return membershipOps{n: node} }

// Join registers node id at addr (plan section 3.1).
func (m membershipOps) Join(ctx context.Context, id, addr string, witness bool) (MembershipOutcome, error) {
	return m.n.runMembership(ctx, transport.MembershipRelay{Cmd: transport.MembershipJoin, NodeID: id, DataAddr: addr, Witness: witness})
}

// Promote makes member id a voter while a slot is free (plan section 3.1, step 10).
func (m membershipOps) Promote(ctx context.Context, id string) (MembershipOutcome, error) {
	return m.n.runMembership(ctx, transport.MembershipRelay{Cmd: transport.MembershipPromote, NodeID: id})
}

// Leave is the next membership slice.
func (m membershipOps) Leave(context.Context, string) (MembershipOutcome, error) {
	if err := m.n.membershipAllowed(); err != nil {
		return MembershipOutcome{}, err
	}
	return MembershipOutcome{}, notBuiltYet("leave")
}

// RemoveNode is the next membership slice.
func (m membershipOps) RemoveNode(context.Context, string) (MembershipOutcome, error) {
	if err := m.n.membershipAllowed(); err != nil {
		return MembershipOutcome{}, err
	}
	return MembershipOutcome{}, notBuiltYet("remove-node")
}

// ReplaceNode gives the identity of a down or dead member to the process at its own address that
// lost its data (BUG-0870, replace.go).
func (m membershipOps) ReplaceNode(ctx context.Context, id, addr string, acceptLoss bool) (MembershipOutcome, error) {
	return m.n.runMembership(ctx, transport.MembershipRelay{Cmd: transport.MembershipReplaceNode, NodeID: id, DataAddr: addr, AcceptLoss: acceptLoss})
}

func notBuiltYet(statement string) error {
	return membershipRefusal(cluster.CodeInvalidArgument,
		"%s is not available yet: this build does not run it. Run 'nodes' to see the members; join, promote and replace-node are available now", statement)
}

// membershipName is the statement a command byte comes from, for texts and counters.
func membershipName(cmd byte) string {
	switch cmd {
	case transport.MembershipJoin:
		return "join"
	case transport.MembershipPromote:
		return "promote"
	case transport.MembershipLeave:
		return "leave"
	case transport.MembershipRemoveNode:
		return "remove_node"
	case transport.MembershipReplaceNode:
		return "replace_node"
	}
	return "activate"
}

// membershipAllowed is M1: cluster.allow_membership is false on this node.
func (n *Node) membershipAllowed() error {
	if n.cl != nil && n.cl.opts.MembershipLocked {
		return membershipRefusal(cluster.CodeInvalidArgument,
			"membership changes are locked on this node: cluster.allow_membership is false. Nothing was changed. "+
				"Set 'cluster.allow_membership: true' (HOARDDB_CLUSTER_ALLOW_MEMBERSHIP=1) on %s and restart it, or run the statement on a member where it is true", n.cfg.NodeID)
	}
	return nil
}

// runMembership is the entry of a statement on this node: the lock first, then the leader's work
// here or the forward to the leader.
func (n *Node) runMembership(ctx context.Context, ask transport.MembershipRelay) (outcome MembershipOutcome, err error) {
	defer func() { n.cl.countMembership(ask.Cmd, err) }()
	if n.cl == nil {
		return MembershipOutcome{}, ErrMembershipDisabled
	}
	if err := n.membershipAllowed(); err != nil {
		return MembershipOutcome{}, err
	}
	if _, bounded := ctx.Deadline(); !bounded {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, membershipBudget)
		defer cancel()
	}
	if n.raft.IsLeader() {
		return n.membershipOnLeader(ctx, ask)
	}
	return n.forwardMembership(ctx, ask)
}

// membershipOnLeader runs a command on this node, which leads. It never forwards. The ask's
// request id is not read here.
func (n *Node) membershipOnLeader(ctx context.Context, ask transport.MembershipRelay) (MembershipOutcome, error) {
	cmd, id, addr := ask.Cmd, ask.NodeID, ask.DataAddr
	if cmd != transport.MembershipActivate {
		if err := n.membershipAllowed(); err != nil {
			return MembershipOutcome{}, err
		}
	}
	if err := n.cl.membershipTurn(ctx); err != nil {
		return MembershipOutcome{}, err
	}
	defer n.cl.membershipDone()
	switch cmd {
	case transport.MembershipJoin:
		return n.joinOnLeader(ctx, id, addr, ask.Witness)
	case transport.MembershipPromote:
		return n.promoteOnLeader(ctx, id)
	case transport.MembershipReplaceNode:
		return n.replaceOnLeader(ctx, id, addr, ask.AcceptLoss)
	case transport.MembershipActivate:
		return n.activateOnLeader(ctx, id)
	}
	return MembershipOutcome{}, notBuiltYet(membershipName(cmd))
}

// membershipTurn takes the leader's one membership slot: join and promote change the Raft
// configuration one at a time, and two statements at once would only fight over state_seq.
func (c *clusterParts) membershipTurn(ctx context.Context) error {
	select {
	case c.membershipSlot <- struct{}{}:
		return nil
	case <-ctx.Done():
		return &cluster.Error{Code: cluster.CodeUnavailable, Message: "another membership change is still running on the leader, so this one waited and gave up. Nothing was changed. Run 'nodes' to see the members, then retry"}
	}
}

func (c *clusterParts) membershipDone() { <-c.membershipSlot }

// joinOnLeader is plan section 3.1 steps 2 to 12. A known id is answered before J1 is asked: a
// join that completed stays answerable after a database exists, and only an unfinished one is held
// back by J1.
func (n *Node) joinOnLeader(ctx context.Context, id, addr string, witness bool) (MembershipOutcome, error) {
	return n.runJoin(ctx, id, addr, witness, false)
}

// JoinRegisterOnly registers the node at id and addr through the gates of join (the identity dial
// and its checks, J2, J3, the address rule) and stops there: the member is left joining and not a
// voter, for a caller that promotes and activates it itself. J1 is the one gate it does not apply,
// because the seeding tests join a node into a cluster that holds data, which is what J1 holds back
// from the statement until slice C. It is the leader's call and never forwards. It exists for the
// harness; the join statement is membershipOps.Join.
func (n *Node) JoinRegisterOnly(ctx context.Context, id, addr string) error {
	if n.cl == nil {
		return ErrMembershipDisabled
	}
	if err := n.cl.membershipTurn(ctx); err != nil {
		return err
	}
	defer n.cl.membershipDone()
	_, err := n.runJoin(ctx, id, addr, false, true)
	return err
}

// runJoin is joinOnLeader; registerOnly stops it after the registration.
func (n *Node) runJoin(ctx context.Context, id, addr string, witness, registerOnly bool) (MembershipOutcome, error) {
	state := n.raft.State()
	if row, known := reconcileMember(state, id); known {
		if row.DataAddr != addr {
			return MembershipOutcome{}, idTaken(id, row.DataAddr, addr)
		}
		if witness && !row.Witness {
			return MembershipOutcome{}, witnessRowDisagrees(id, addr)
		}
		if row.State != cluster.MemberJoining {
			return joinOutcome(state, row, addr, n.cfg.Voters), nil
		}
		if len(state.Databases) > 0 && !registerOnly {
			return MembershipOutcome{}, joinRefusedWhileData(id, addr, state.Databases[0].Name)
		}
		// A join retried after a lost reply, or one a leader change cut short: a node that holds
		// a joining row is not serving yet, so it is asked who it is and judged by its range
		// before anything more is committed for it, then the join is finished from the row.
		if err := n.checkJoinerRange(ctx, id, addr); err != nil {
			return MembershipOutcome{}, err
		}
		return n.finishJoin(ctx, id, addr, registerOnly)
	}
	if len(state.Databases) > 0 && !registerOnly {
		return MembershipOutcome{}, joinRefusedWhileData(id, addr, state.Databases[0].Name)
	}
	if err := checkJoinAddress(id, addr, state); err != nil {
		return MembershipOutcome{}, err
	}
	identity, err := n.askIdentity(ctx, id, addr, false, true)
	if err != nil {
		return MembershipOutcome{}, err
	}
	if witness && !identity.Witness {
		return MembershipOutcome{}, witnessFlagDisagrees(id, addr, witness)
	}
	member := cluster.Member{ID: id, Incarnation: identity.Incarnation, DataAddr: addr, ClientAddr: identity.ClientAddr, Witness: identity.Witness}
	if err := n.raft.AddJoiner(id, addr); err != nil {
		return MembershipOutcome{}, fmt.Errorf("joining %s: %w", id, err)
	}
	// The range the joiner declared travels in REGISTER_MEMBER, so a finalize that commits between
	// this ask and the registration refuses the registration instead of leaving a member that
	// cannot run the cluster's version (text F8).
	_, err = n.raft.RegisterJoiner(cluster.JoinRequest{Member: member, RaftAddr: addr, PlacingSet: fullPlacingSet(), Range: identity.Range})
	n.raft.EndJoin(id)
	if err != nil {
		return MembershipOutcome{}, fmt.Errorf("joining %s: %w", id, err)
	}
	n.topology.Refresh()
	return n.finishJoin(ctx, id, addr, registerOnly)
}

// checkJoinerRange asks the node at id and addr who it is and refuses it when its range does not
// overlap this leader's. It is for the path of a known joining row, which has no identity ask of
// its own. The ask admits the node (BUG-0870 guard) only when it is not a voter in the committed voters
// list nor in the library's configuration: a joining row can already vote (promoted, not yet
// activated), and a voter that holds no Raft state lost the log it voted with, so caught up it would
// have forgotten its term and vote. Such a voter is refused.
func (n *Node) checkJoinerRange(ctx context.Context, id, addr string) error {
	// Both books: PromoteWithin changes the library's configuration before it commits SET_VOTERS, so
	// in between only the library counts the node (review round 2, A-1).
	configured, err := n.raft.ConfiguredVoter(id)
	if err != nil {
		return fmt.Errorf("joining %s: reading the raft configuration: %w", id, err)
	}
	voter := configured || slices.Contains(n.raft.State().Voters, id)
	identity, err := n.askIdentity(ctx, id, addr, true, !voter)
	if err != nil {
		return err
	}
	if voter && !identity.HoldsRaftState {
		return voterLostItsState(id, addr)
	}
	return nil
}

// fullPlacingSet is the placing set of a join on a cluster with no database: every slot, which is
// the whole of nothing. The FSM validates its size and stores no copy.
func fullPlacingSet() []byte {
	set := make([]byte, 1024)
	for i := range set {
		set[i] = 0xFF
	}
	return set
}

// joinSteps is what is left to do for a registered member.
type joinSteps struct{ promote, activateWitness, activate bool }

// joinStepsFor is the work a join still owes a member row. Only a joining row owes any: the order
// is promote, then activate, so an active row means the promotion was already attempted, and a
// repeat of a completed join must not take a voter slot or commit anything.
func joinStepsFor(row cluster.Member) joinSteps {
	if row.State != cluster.MemberJoining {
		return joinSteps{}
	}
	return joinSteps{promote: true, activateWitness: row.Witness, activate: !row.Witness}
}

// finishJoin runs the steps after the registration: the voter slot, a witness's activation at once,
// and the activation of a data joiner whose placing set is empty. Each is idempotent, so a retry
// that finds a half-finished join completes it. It answers from the committed row.
func (n *Node) finishJoin(ctx context.Context, id, addr string, registerOnly bool) (MembershipOutcome, error) {
	state, row, err := n.completeJoin(ctx, id, registerOnly)
	if err != nil {
		return MembershipOutcome{}, err
	}
	return joinOutcome(state, row, addr, n.cfg.Voters), nil
}

// completeJoin is finishJoin's work; it returns the committed state and row it read last, for the
// statement's own line (join's, or replace-node's).
func (n *Node) completeJoin(ctx context.Context, id string, registerOnly bool) (cluster.ClusterState, cluster.Member, error) {
	gone := func() (cluster.ClusterState, cluster.Member, error) {
		return cluster.ClusterState{}, cluster.Member{}, fmt.Errorf("joining %s: its member row is gone, so the join did not complete; run 'nodes' and retry the join", id)
	}
	row, registered := reconcileMember(n.raft.State(), id)
	if !registered {
		// Unreachable after a committed REGISTER_MEMBER, but a row removed in between must not be
		// answered as a join that completed.
		return gone()
	}
	steps := joinStepsFor(row)
	if registerOnly {
		steps = joinSteps{}
	}
	if steps.promote {
		if _, err := n.raft.PromoteWithin(id, n.cfg.Voters); err != nil {
			var full *cluster.VoterCapError
			if !errors.As(err, &full) {
				return cluster.ClusterState{}, cluster.Member{}, fmt.Errorf("joining %s: %w", id, err)
			}
		}
	}
	if steps.activateWitness {
		if err := n.setMemberState(ctx, id, "active"); err != nil {
			return cluster.ClusterState{}, cluster.Member{}, fmt.Errorf("joining %s: %w", id, err)
		}
	}
	if steps.activate {
		if err := n.activateAfterRegistration(ctx, id); err != nil {
			return cluster.ClusterState{}, cluster.Member{}, err
		}
	}
	n.topology.Refresh()
	state := n.raft.State()
	row, registered = reconcileMember(state, id)
	if !registered {
		return gone()
	}
	return state, row, nil
}

// joinOutcome answers a join from the committed row: its registration position, never the current one.
func joinOutcome(state cluster.ClusterState, row cluster.Member, addr string, voters int) MembershipOutcome {
	return MembershipOutcome{Epoch: state.Epoch, StateSeq: row.JoinedStateSeq, Result: joinLine(state, row, addr, voters)}
}

// activateAfterRegistration activates a data joiner at once when nothing is left to seed. A joiner
// with pairs to pull is left joining: it asks to be activated when they are ready (plan section 3.6).
func (n *Node) activateAfterRegistration(ctx context.Context, id string) error {
	var err error
	for range activationRetries {
		if err = n.Activate(ctx, id); err == nil || errors.Is(err, ErrStillSeeding) {
			return nil
		}
		if !retryableActivation(err) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("activating %s: %w", id, err)
}

// retryableActivation is true for a refusal that says only that something else committed first.
func retryableActivation(err error) bool {
	var refusal *cluster.Error
	return errors.As(err, &refusal) && refusal.Code == cluster.CodeStateMoved
}

// joinLine is the one line a join answers with (plan section 5, queries section 8). A joiner
// that holds a vote says nothing of voting; one without says why.
func joinLine(state cluster.ClusterState, row cluster.Member, addr string, voters int) string {
	voter := slices.Contains(state.Voters, row.ID)
	subject := fmt.Sprintf("%s@%s", row.ID, addr)
	switch {
	case row.State == cluster.MemberDown || row.State == cluster.MemberDead:
		// BUG-0870: a join is what an operator tries first for a member that lost its data.
		return fmt.Sprintf("%s is a member at %s: %s (epoch %d). ", row.ID, addr, row.State, state.Epoch) +
			strings.NewReplacer("<node>", row.ID, "<addr>", addr).Replace("If it lost its data, start it again on an empty ring directory and run 'replace-node <node> --with <node>@<addr>'")
	case row.State != cluster.MemberJoining && row.State != cluster.MemberActive:
		return fmt.Sprintf("%s is a member at %s: %s (epoch %d)", row.ID, addr, row.State, state.Epoch)
	case row.Witness && voter:
		return fmt.Sprintf("Added %s as a witness: it votes and holds no documents (epoch %d)", subject, state.Epoch)
	case row.Witness:
		return fmt.Sprintf("Added %s as a witness: it holds no documents (not a voter: the cluster already has its %d voters) (epoch %d)", subject, voters, state.Epoch)
	case voter:
		return fmt.Sprintf("Added %s as a member: joining, then active when it holds the slots it will own (epoch %d)", subject, state.Epoch)
	}
	return fmt.Sprintf("Added %s as a member: joining (not a voter: the cluster already has its %d voters) (epoch %d)", subject, voters, state.Epoch)
}

// promoteOnLeader makes id a voter, or reports that it already is.
func (n *Node) promoteOnLeader(ctx context.Context, id string) (MembershipOutcome, error) {
	before := n.raft.State()
	already := slices.Contains(before.Voters, id)
	promoted, err := n.raft.PromoteWithin(id, n.cfg.Voters)
	if err != nil {
		return MembershipOutcome{}, err
	}
	n.topology.Refresh()
	state := n.raft.State()
	line := fmt.Sprintf("Promoted %s: it now votes (%d voters) (epoch %d)", id, len(state.Voters), state.Epoch)
	if already {
		line = fmt.Sprintf("%s already votes (%d voters) (epoch %d)", id, len(state.Voters), state.Epoch)
	}
	return MembershipOutcome{Epoch: state.Epoch, StateSeq: promoted.StateSeq, Result: line}, nil
}

// activateOnLeader is the ask to activate: a joining member (plan section 3.6), or a member
// committed down that asks to come back (BUG-0754); the gate is the joiner's for a joining member
// and the member's own selfPairsReady for a down one.
func (n *Node) activateOnLeader(ctx context.Context, id string) (MembershipOutcome, error) {
	before := n.raft.State()
	row, member := reconcileMember(before, id)
	if !member {
		return MembershipOutcome{}, filled(cluster.CodeNotFound,
			"node <node> has not joined this cluster, so there is nothing to activate. Run 'join <node>@<addr>' on any member first", "<node>", id, "<addr>", "<host:port>")
	}
	switch row.State {
	case cluster.MemberJoining, cluster.MemberDown:
	default:
		// Only a joining or a down member is activated; a repeat ask commits nothing and answers the facts.
		return MembershipOutcome{Epoch: before.Epoch, StateSeq: before.StateSeq, Result: fmt.Sprintf("%s is already %s (epoch %d)", id, row.State, before.Epoch)}, nil
	}
	if err := n.activateAfterRegistrationStrict(ctx, id); err != nil {
		return MembershipOutcome{}, err
	}
	n.topology.Refresh()
	state := n.raft.State()
	return MembershipOutcome{Epoch: state.Epoch, StateSeq: state.StateSeq, Result: fmt.Sprintf("Activated %s (epoch %d)", id, state.Epoch)}, nil
}

// activateAfterRegistrationStrict is activateAfterRegistration that reports a joiner still seeding.
func (n *Node) activateAfterRegistrationStrict(ctx context.Context, id string) error {
	var err error
	for range activationRetries {
		if err = n.Activate(ctx, id); err == nil {
			return nil
		}
		if !retryableActivation(err) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if errors.Is(err, ErrStillSeeding) {
		return &cluster.Error{Code: cluster.CodeUnavailable, Message: err.Error()}
	}
	return fmt.Errorf("activating %s: %w", id, err)
}

// checkJoinAddress is validateClusterReachability for a join: a loopback or link-local address
// is refused when the cluster has a member on another machine, because that member could not
// dial it.
func checkJoinAddress(id, addr string, state cluster.ClusterState) error {
	if zone := addressZone(addr); zone != "" {
		return filled(cluster.CodeInvalidArgument,
			"cannot join <node>@<addr>: the address \"<addr>\" names a network zone (<zone>), which no other machine can use, so its peers could not dial <node> there. "+
				"Advertise a routable address of <node> instead, for example 192.0.2.1:4433: set HOARDDB_CLUSTER_ADVERTISE on <node> to it, restart <node>, then run 'join <node>@<host:port>'",
			"<node>", id, "<addr>", addr, "<zone>", zone)
	}
	if !reachableOnlyLocally(addr) {
		return nil
	}
	for _, member := range state.Members {
		if !reachableOnlyLocally(member.DataAddr) {
			return filled(cluster.CodeInvalidArgument,
				"cannot join <node>@<addr>: <addr> is a loopback or link-local address, and <remote> (<remoteaddr>) is on another machine, so its peers could not dial <node> there. "+
					"Set HOARDDB_LISTEN (and HOARDDB_CLUSTER_ADVERTISE if it listens on every interface) on <node> to the address the other nodes dial, restart it, then run 'join <node>@<host:port>'",
				"<node>", id, "<addr>", addr, "<remote>", member.ID, "<remoteaddr>", member.DataAddr)
		}
	}
	return nil
}

// reachableOnlyLocally reports whether addr names loopback or a link-local address, which no
// other machine can dial. A name is trusted: it may resolve to any interface.
func reachableOnlyLocally(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && (ip.IsLoopback() || ip.IsLinkLocalUnicast())
}

// addressZone returns the zone of a host:port address ("%eth0" in "[fe80::1%eth0]:4433"), or ""
// when the host has none. net.ParseIP rejects a zoned host, so the reachability test cannot see it.
func addressZone(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return ""
	}
	if i := strings.IndexByte(host, '%'); i >= 0 {
		return host[i:]
	}
	return ""
}

func joinRefusedWhileData(id, addr, database string) error {
	return filled(cluster.CodeInvalidArgument,
		"cannot join <node>@<addr>: this cluster holds database \"<db>\" and this build does not seed a joining node's slots, so joining would make <node> an owner of documents it does not hold. "+
			"Form the cluster before creating data: start the members on fresh data directories, join them, then 'create database <db>'",
		"<node>", id, "<addr>", addr, "<db>", database)
}

func witnessFlagDisagrees(id, addr string, asked bool) error {
	as, is := "member", "witness"
	if asked {
		as, is = "witness", "member"
	}
	return filled(cluster.CodeInvalidArgument,
		"cannot join <node>@<addr> as a <as>: <node> is configured as a <is> (cluster.witness on <node>). "+
			"Run 'join <node>@<addr>' without the flag, or change cluster.witness on <node> and restart it",
		"<node>", id, "<addr>", addr, "<as>", as, "<is>", is)
}

// witnessRowDisagrees is J3 for the flag: the id is registered as a data member and the statement
// asks for a witness. (The reverse is not refused: a plain join is the CLI's only form, and it
// repeats a witness join.)
func witnessRowDisagrees(id, addr string) error {
	return filled(cluster.CodeConflict,
		"node <node> is already registered as a member, not a witness, so it cannot be joined as a witness at <addr>. "+
			"Run 'nodes' to see it; a node's role is fixed when it joins, so repeat the plain 'join <node>@<addr>'",
		"<node>", id, "<addr>", addr)
}

func idTaken(id, existing, addr string) error {
	return filled(cluster.CodeConflict,
		"node <node> is already a member at <existing>, so it cannot be joined again at <typed>. "+
			"Run 'nodes' to see it; a member keeps its address in this release, so start <node> at <existing>, and if it lost its data run 'replace-node <node> --with <node>@<addr>'",
		"<node>", id, "<existing>", existing, "<typed>", addr, "<addr>", existing)
}

// ---- forwarding (plan section 2) ---------------------------------------------------------

// membershipRelayCall bounds one forwarded attempt: a join waits on a dial and several commits.
const membershipRelayCall = 45 * time.Second

// forwardMembership hands the command to the leader, once, and retries once with the leader a
// NOT_LEADER answer names. The request id is reused on the retry, so a repeat after a lost reply
// is answered from the leader's ledger or, past it, by idempotence by content.
func (n *Node) forwardMembership(ctx context.Context, request transport.MembershipRelay) (MembershipOutcome, error) {
	if _, err := entropy.Read(request.RequestID[:]); err != nil {
		return MembershipOutcome{}, fmt.Errorf("drawing the request id of a membership command: %w", err)
	}
	cmd := request.Cmd
	hint := ""
	var last error
	for attempt := 0; attempt < 2; attempt++ {
		leader := hint
		if leader == "" {
			leader = n.raft.Leader()
		}
		if leader == "" {
			return MembershipOutcome{}, noLeaderWithinOneHop(cmd, "")
		}
		if leader == n.cfg.NodeID {
			return n.membershipOnLeader(ctx, request)
		}
		callCtx, cancel := context.WithTimeout(ctx, membershipRelayCall)
		reply, err := n.cl.ctrl.RelayMembership(callCtx, leader, request)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return MembershipOutcome{}, &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
					"%s did not finish before its time ran out, and it may have run on the leader (%s). Do not run it again yet: run 'nodes' to see the members and check first", membershipName(cmd), leader)}
			}
			last, hint = err, ""
			continue
		}
		switch reply.Status {
		case transport.MembershipDone:
			n.awaitLocalApply(ctx, int64(reply.StateSeq))
			return MembershipOutcome{Epoch: int64(reply.Epoch), StateSeq: int64(reply.StateSeq), Result: reply.Result}, nil
		case transport.MembershipRefused:
			return MembershipOutcome{}, &cluster.Error{Code: cluster.Code(reply.Code), Message: reply.Message}
		case transport.MembershipNotLeader:
			last, hint = fmt.Errorf("%s is not the leader", leader), reply.Leader
			if hint == "" {
				return MembershipOutcome{}, noLeaderWithinOneHop(cmd, "")
			}
		default:
			last, hint = fmt.Errorf("the leader %s could not tell whether the command committed in time", leader), ""
		}
	}
	return MembershipOutcome{}, noLeaderWithinOneHop(cmd, fmt.Sprint(last))
}

// noLeaderWithinOneHop is F1.
func noLeaderWithinOneHop(cmd byte, cause string) error {
	why := "no leader is elected"
	if cause != "" {
		why = "the leader it knows did not take the command (" + cause + ")"
	}
	return membershipRefusal(cluster.CodeUnavailable,
		"%s could not reach the Raft leader: this node is not the leader and %s. Nothing was changed. "+
			"Run 'nodes' to see who leads, then retry; the statement runs on any member and is forwarded to the leader", membershipName(cmd), why)
}

// awaitLocalApply waits, briefly, until this node has applied the position the leader committed,
// so a statement entered here reads its own change.
func (n *Node) awaitLocalApply(ctx context.Context, stateSeq int64) {
	deadline := time.After(2 * time.Second)
	for n.raft.StateSeq() < stateSeq {
		select {
		case <-ctx.Done():
			return
		case <-deadline:
			return
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// membershipLedger is the leader's memory of answered forwarded commands, keyed by sender and
// request id: the relay ledger's shape for the membership kinds.
type membershipLedger struct {
	mu       sync.Mutex
	applied  map[relayKey]ledgerAnswer
	order    []relayKey
	inflight map[relayKey]*membershipCall
	replays  atomic.Int64
}

// ledgerAsk is what a request id was used for: a repeat under the same id must be the same ask, or
// the sender would be handed another command's stored answer.
type ledgerAsk struct {
	cmd        byte
	witness    bool
	acceptLoss bool
	node       string
	addr       string
}

func askOf(req transport.MembershipRelay) ledgerAsk {
	return ledgerAsk{cmd: req.Cmd, witness: req.Witness, acceptLoss: req.AcceptLoss, node: req.NodeID, addr: req.DataAddr}
}

type ledgerAnswer struct {
	ask   ledgerAsk
	reply transport.MembershipRelayReply
}

type membershipCall struct {
	done  chan struct{}
	ask   ledgerAsk
	reply transport.MembershipRelayReply
}

// errLedgerAskDiffers marks a request id reused for a different ask.
var errLedgerAskDiffers = errors.New("request id reused for a different membership command")

func (l *membershipLedger) begin(key relayKey, ask ledgerAsk) (stored transport.MembershipRelayReply, pending *membershipCall, owner bool, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if answer, ok := l.applied[key]; ok {
		if answer.ask != ask {
			return transport.MembershipRelayReply{}, nil, false, errLedgerAskDiffers
		}
		l.replays.Add(1)
		return answer.reply, nil, false, nil
	}
	if call, ok := l.inflight[key]; ok {
		if call.ask != ask {
			return transport.MembershipRelayReply{}, nil, false, errLedgerAskDiffers
		}
		l.replays.Add(1)
		return transport.MembershipRelayReply{}, call, false, nil
	}
	if l.inflight == nil {
		l.inflight = map[relayKey]*membershipCall{}
	}
	call := &membershipCall{done: make(chan struct{}), ask: ask}
	l.inflight[key] = call
	return transport.MembershipRelayReply{}, call, true, nil
}

func (l *membershipLedger) finish(key relayKey, call *membershipCall, reply transport.MembershipRelayReply) {
	l.mu.Lock()
	call.reply = reply
	delete(l.inflight, key)
	if reply.Status == transport.MembershipDone {
		if l.applied == nil {
			l.applied = map[relayKey]ledgerAnswer{}
		}
		l.order = append(l.order, key)
		l.applied[key] = ledgerAnswer{ask: call.ask, reply: reply}
		if len(l.order) > membershipLedgerCap {
			delete(l.applied, l.order[0])
			l.order = l.order[1:]
		}
	}
	l.mu.Unlock()
	close(call.done)
}

// MembershipReplays is how many forwarded membership commands this node answered from its ledger
// instead of running them again.
func (n *Node) MembershipReplays() int64 { return n.cl.membership.replays.Load() }

// serveMembershipRelay is the leader's side of MEMBERSHIP_RELAY. It never forwards: a node that
// does not lead answers NOT_LEADER with the leader it knows. The sender is the pinned identity of
// the handshake; it must be a committed member that is not dead, and an activation is taken only
// for the member that asks.
func (n *Node) serveMembershipRelay(ctx context.Context, from transport.Handshake, req transport.MembershipRelay) transport.MembershipRelayReply {
	if !n.raft.IsLeader() {
		return transport.MembershipRelayReply{Status: transport.MembershipNotLeader, Leader: n.raft.Leader()}
	}
	sender, member := reconcileMember(n.raft.State(), from.NodeID)
	if !member || sender.State == cluster.MemberDead {
		return membershipRelayRefusal(cluster.CodePermissionDenied, "%s is not a live member of this cluster, so it may not hand the leader a membership command. Run the statement on a member", from.NodeID)
	}
	if req.Cmd == transport.MembershipActivate && req.NodeID != from.NodeID {
		return membershipRelayRefusal(cluster.CodePermissionDenied, "%s may ask to activate only itself, not %s. Run the statement on the member that should be activated", from.NodeID, req.NodeID)
	}
	key := relayKey{from: from.NodeID, id: req.RequestID}
	stored, call, owner, err := n.cl.membership.begin(key, askOf(req))
	if err != nil {
		return membershipRelayRefusal(cluster.CodeInvalidArgument,
			"request id %x from %s was already used for a different membership command. Nothing was changed. Run the statement again", req.RequestID, from.NodeID)
	}
	if call == nil {
		return stored
	}
	if !owner {
		select {
		case <-call.done:
			return call.reply
		case <-ctx.Done():
			return transport.MembershipRelayReply{Status: transport.MembershipNoQuorum}
		}
	}
	outcome, runErr := n.membershipOnLeader(ctx, req)
	reply := membershipReply(outcome, runErr)
	n.cl.membership.finish(key, call, reply)
	return reply
}

func membershipRelayRefusal(code cluster.Code, format string, args ...any) transport.MembershipRelayReply {
	return transport.MembershipRelayReply{Status: transport.MembershipRefused, Code: uint16(code), Message: fmt.Sprintf(format, args...)}
}

// membershipReply turns the outcome of a command run on the leader into the wire reply.
func membershipReply(outcome MembershipOutcome, err error) transport.MembershipRelayReply {
	var notLeader *cluster.NotLeaderError
	var refusal *cluster.Error
	switch {
	case err == nil:
		return transport.MembershipRelayReply{Status: transport.MembershipDone, Epoch: uint64(outcome.Epoch), StateSeq: uint64(outcome.StateSeq), Result: outcome.Result}
	case errors.As(err, &notLeader) && notLeader.Undecided:
		return transport.MembershipRelayReply{Status: transport.MembershipNoQuorum}
	case errors.As(err, &notLeader):
		return transport.MembershipRelayReply{Status: transport.MembershipNotLeader, Leader: notLeader.Leader}
	case errors.As(err, &refusal):
		return transport.MembershipRelayReply{Status: transport.MembershipRefused, Code: uint16(refusal.Code), Message: err.Error()}
	}
	return transport.MembershipRelayReply{Status: transport.MembershipRefused, Code: uint16(cluster.CodeUnavailable), Message: err.Error()}
}

// ---- the joiner's ask to be activated (plan section 3.6) ---------------------------------

// askActivation runs on the supervisor's step of a joiner or of a member committed down: when
// this node is joining or down and every pair of its placing set is ready, it asks the leader
// to activate it, and repeats until the committed state says active. A member that is itself
// the leader runs the same command on itself. The leader's gate is unchanged and decides; a
// member that is behind simply does not ask.
func (n *Node) askActivation(ctx context.Context) {
	c := n.cl
	if c.opts.HoldActivation || c.ctrl == nil {
		return
	}
	state := n.raft.State()
	self, member := reconcileMember(state, n.cfg.NodeID)
	if !member || !activationAskable(self) {
		return
	}
	now := time.Now().UnixNano()
	if last := c.lastActivationAsk.Load(); now-last < int64(activationAskEvery) || !c.lastActivationAsk.CompareAndSwap(last, now) {
		return
	}
	if !n.selfPairsReady(state) {
		return
	}
	leader := n.raft.Leader()
	if leader == "" {
		return
	}
	if leader == n.cfg.NodeID {
		n.activateSelfAsLeader(ctx)
		return
	}
	var requestID [transport.RelayRequestIDLen]byte
	if _, err := entropy.Read(requestID[:]); err != nil {
		c.report(fmt.Errorf("drawing the request id of an activation ask: %w", err))
		return
	}
	callCtx, cancel := context.WithTimeout(ctx, membershipRelayCall)
	defer cancel()
	c.activationAsks.Add(1) // counted as sent: the leader may activate this node before the reply is back
	reply, err := c.ctrl.RelayMembership(callCtx, leader, transport.MembershipRelay{RequestID: requestID, Cmd: transport.MembershipActivate, NodeID: n.cfg.NodeID})
	switch {
	case err != nil:
		c.report(fmt.Errorf("asking %s to activate %s: %w", leader, n.cfg.NodeID, err))
	case reply.Status == transport.MembershipRefused && reply.Code != uint16(cluster.CodeUnavailable):
		c.report(fmt.Errorf("%s refused to activate %s: %s", leader, n.cfg.NodeID, reply.Message))
	}
}

// activationAskable is true for the two states a member asks to leave: joining (a joiner that has pulled its pairs) and
// down (a member the cluster committed down that is back, BUG-0754). A witness never asks: it holds no pairs.
func activationAskable(member cluster.Member) bool {
	return !member.Witness && (member.State == cluster.MemberJoining || member.State == cluster.MemberDown)
}

// activateSelfAsLeader is the ask of a joining or down member that holds the leadership: there
// is no one to message, so it runs the leader's own handler. A leader that changed under the
// call is not an error; the next ask is activationAskEvery away.
func (n *Node) activateSelfAsLeader(ctx context.Context) {
	c := n.cl
	callCtx, cancel := context.WithTimeout(ctx, membershipRelayCall)
	defer cancel()
	c.activationAsks.Add(1)
	_, err := n.membershipOnLeader(callCtx, transport.MembershipRelay{Cmd: transport.MembershipActivate, NodeID: n.cfg.NodeID})
	var notLeader *cluster.NotLeaderError
	var refusal *cluster.Error
	switch {
	case err == nil, errors.As(err, &notLeader):
	case errors.As(err, &refusal) && refusal.Code == cluster.CodeUnavailable:
	default:
		c.report(fmt.Errorf("activating %s on itself as the leader: %w", n.cfg.NodeID, err))
	}
}

// ActivationAsks is how many times this node asked the leader to activate it.
func (n *Node) ActivationAsks() int64 { return n.cl.activationAsks.Load() }

// selfPairsReady is true when every pair of this joiner's placing set is ready on this node. With
// no database there is no pair, which is the whole of a join on an empty cluster.
func (n *Node) selfPairsReady(state cluster.ClusterState) bool {
	if len(state.Databases) == 0 {
		return true
	}
	table := reconcilePlacingTable(state, n.cfg.NodeID)
	slots := n.cl.seeding.Load().slotsView()
	for _, database := range state.Databases {
		for slot := range uint16(storage.SlotCount) {
			for _, copyOf := range table.Copies(slot, int(database.RF)) {
				if copyOf.ID == n.cfg.NodeID && slots.State(slot, uint64(database.ID)) != machine.SlotReady {
					return false
				}
			}
		}
	}
	return true
}

// ---- counters ---------------------------------------------------------------------------

// membershipCounter counts statements by command and outcome: the aggregate
// hoarddb_membership_commands_total{cmd,outcome}. There is no per-member or per-bucket series.
type membershipCounter struct {
	mu     sync.Mutex
	counts map[string]uint64
}

func (c *clusterParts) countMembership(cmd byte, err error) {
	if c == nil {
		return
	}
	outcome := "ok"
	if err != nil {
		outcome = "refused"
	}
	c.counter.mu.Lock()
	defer c.counter.mu.Unlock()
	if c.counter.counts == nil {
		c.counter.counts = map[string]uint64{}
	}
	c.counter.counts[membershipName(cmd)+"/"+outcome]++
}

// MembershipCommands is a copy of the statement counters, keyed "cmd/outcome".
func (n *Node) MembershipCommands() map[string]uint64 {
	if n.cl == nil {
		return nil
	}
	n.cl.counter.mu.Lock()
	defer n.cl.counter.mu.Unlock()
	out := make(map[string]uint64, len(n.cl.counter.counts))
	for key, count := range n.cl.counter.counts {
		out[key] = count
	}
	return out
}
