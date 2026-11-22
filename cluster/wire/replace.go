package wire

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
)

// replace-node on the leader (BUG-0870 design section 5.2, owner ruling 2026-11-19: same address
// only; refused when the node held the only copy of some pairs unless the operator accepts the
// loss). A member that lost its ring directory is started again with its key, id and address; the
// BUG-0870 guard holds it out of Raft and its peers commit it down. replace-node then gives it its
// identity back: one SET_MEMBER_STATE{joining, its new incarnation, clear_bits, expect_state} that
// the FSM already knows (a newer incarnation from down or dead to joining), the Raft re-entry
// (out of the configuration and back as a nonvoter, so the leader counts it from nothing), the
// admit bit on a second identity ask, and the rest of a join: the voter slot, and its activation
// once it holds the pairs it owns, which it pulls from the other copies like any joiner.
//
// Cost (rule 11): no state per bucket or store, no goroutine, no metric beyond the aggregate
// statement counter's replace_node outcomes. Per statement: one scan of the committed seed bitmaps
// (O(slots x databases x members)), up to two identity connections, one commit, two or three Raft
// configuration changes. The membership change itself is O(pairs the node owns) store syncs, not
// O(buckets): the node pulls its pairs, and while it is joining the survivors may take a temporary
// copy of them (at most about twice its pairs in all).

// replaceOnLeader is replace-node run on the leader, inside the membership turn.
func (n *Node) replaceOnLeader(ctx context.Context, id, addr string, acceptLoss bool) (MembershipOutcome, error) {
	state := n.raft.State()
	row, known := reconcileMember(state, id)
	if !known {
		return MembershipOutcome{}, filled(cluster.CodeNotFound,
			"node <node> is not a member of this cluster, so there is nothing to replace. Nothing was changed. "+
				"Run 'nodes' to see the members; a new node is added with 'join <node>@<addr>'",
			"<node>", id, "<addr>", addr)
	}
	switch row.State {
	case cluster.MemberDown, cluster.MemberDead, cluster.MemberJoining:
	default:
		return MembershipOutcome{}, replaceNotDown(id, row.DataAddr, row.State, state.Epoch)
	}
	if addr != row.DataAddr {
		return MembershipOutcome{}, replaceOtherAddress(id, addr, row.DataAddr)
	}
	if row.State == cluster.MemberJoining {
		return n.resumeReplace(ctx, id, addr, acceptLoss)
	}
	// Fail fast before the identity ask; gatedCommit counts again on the state it commits against.
	if _, err := replaceLossGate(state, id, addr, acceptLoss); err != nil {
		return MembershipOutcome{}, err
	}
	identity, err := n.askIdentity(ctx, id, addr, true, false)
	if err != nil {
		return MembershipOutcome{}, replaceAskFailed(id, addr, err)
	}
	switch {
	case identity.HoldsRaftState:
		return MembershipOutcome{}, replaceHoldsState(id, addr)
	case identity.Witness != row.Witness:
		return MembershipOutcome{}, replaceRoleDiffers(id, addr, row.Witness)
	case identity.Incarnation <= row.Incarnation:
		return MembershipOutcome{}, replaceStaleIncarnation(id, addr, identity.Incarnation, row.Incarnation)
	}
	sole, err := n.gatedCommit(ctx, id, addr, acceptLoss, replaceBody(id, row.State, identity.Incarnation))
	if err != nil {
		return MembershipOutcome{}, err
	}
	n.warnLoss(id, sole)
	n.topology.Refresh()
	outcome, err := n.reenterAndFinish(ctx, id, addr, acceptLoss)
	return n.noteLoss(id, sole, outcome, err)
}

// resumeReplace is a replace-node repeated for a row that is already joining (design section 5.2
// step 8): the node restarted while it seeded, or the statement before was cut after its commit.
// Nothing new is committed. A node that holds Raft state is in the cluster's Raft group and pulling,
// so the join is finished from the row; a node that holds none never took the leader's traffic (it
// is held by the guard), so it is re-entered and admitted first. The same path finishes a joining
// row a join left, which is the same work.
func (n *Node) resumeReplace(ctx context.Context, id, addr string, acceptLoss bool) (MembershipOutcome, error) {
	identity, err := n.askIdentity(ctx, id, addr, true, false)
	if err != nil {
		return MembershipOutcome{}, replaceAskFailed(id, addr, err)
	}
	if identity.HoldsRaftState {
		return n.finishReplace(ctx, id, addr, acceptLoss)
	}
	// A node held with no Raft state may have reported pairs before it lost its ring directory
	// again; those bits name copies it no longer has, so they are cleared before it is admitted
	// (review B-3). The row stays joining at its incarnation: the FSM moves a newer one only from
	// down or dead, and the handshake accepts the node's newer incarnation as it is.
	sole, err := n.gatedCommit(ctx, id, addr, acceptLoss, clearJoiningBody(id))
	if err != nil {
		return MembershipOutcome{}, err
	}
	n.warnLoss(id, sole)
	outcome, err := n.reenterAndFinish(ctx, id, addr, acceptLoss)
	return n.noteLoss(id, sole, outcome, err)
}

// clearJoiningBody is SET_MEMBER_STATE{joining, clear_bits} for a row that is joining now.
func clearJoiningBody(id string) []byte {
	elems := bsoncore.AppendStringElement(nil, "id", id)
	elems = bsoncore.AppendStringElement(elems, "state", cluster.MemberJoining.String())
	elems = bsoncore.AppendStringElement(elems, "expect_state", cluster.MemberJoining.String())
	elems = bsoncore.AppendBooleanElement(elems, "clear_bits", true)
	return bsoncore.BuildDocument(nil, elems)
}

// gatedCommit runs the R4 gate on the state as it is now and commits body (a clear of id's bits)
// stamped at that state's state_seq, so a member that went down or reported between the count and
// the commit makes the commit STATE_MOVED instead of letting a stale count stand (narrow review
// B-1); STATE_MOVED is counted and stamped again, a bounded number of times.
func (n *Node) gatedCommit(ctx context.Context, id, addr string, acceptLoss bool, body []byte) (soleCopies, error) {
	var sole soleCopies
	err := retryStateMoved(func() error {
		state := n.raft.State()
		var gateErr error
		if sole, gateErr = replaceLossGate(state, id, addr, acceptLoss); gateErr != nil {
			return gateErr
		}
		_, commitErr := n.proposeAtSeq(ctx, cluster.CommandSetMemberState, body, uint64(state.StateSeq))
		return commitErr
	})
	if err != nil {
		var refusal *cluster.Error
		if errors.As(err, &refusal) && refusal.Code == cluster.CodeInvalidArgument {
			return soleCopies{}, err // R4
		}
		return soleCopies{}, fmt.Errorf("replacing %s: %w", id, err)
	}
	return sole, nil
}

// finishReplace is the rest of a join for the replaced node, run again while the seed reporters'
// concurrent commits answer STATE_MOVED (review B-6), then the statement's line.
func (n *Node) finishReplace(ctx context.Context, id, addr string, acceptLoss bool) (MembershipOutcome, error) {
	var state cluster.ClusterState
	var row cluster.Member
	err := retryStateMoved(func() error {
		var stepErr error
		state, row, stepErr = n.completeJoin(ctx, id, false)
		return stepErr
	})
	if err != nil {
		return MembershipOutcome{}, replaceUnfinished(id, addr, "the rest of its join", acceptLoss, err)
	}
	return replaceOutcome(state, row, addr), nil
}

// reenterAndFinish is steps 6 (the Raft half) and 7 of the design: the Raft re-entry, the admit
// bit, then the rest of a join.
func (n *Node) reenterAndFinish(ctx context.Context, id, addr string, acceptLoss bool) (MembershipOutcome, error) {
	if err := n.raft.ReenterAsJoiner(id, addr); err != nil {
		return MembershipOutcome{}, replaceUnfinished(id, addr, "its re-entry into the Raft configuration", acceptLoss, err)
	}
	if _, err := n.askIdentity(ctx, id, addr, true, true); err != nil {
		return MembershipOutcome{}, replaceUnfinished(id, addr, "the ask that admits it", acceptLoss, err)
	}
	return n.finishReplace(ctx, id, addr, acceptLoss)
}

// replaceBody is SET_MEMBER_STATE{joining, the new incarnation, clear_bits}, only from the state the
// leader read (expect_state), so a repeat after the row moved on changes nothing.
func replaceBody(id string, from cluster.MemberState, incarnation int64) []byte {
	elems := bsoncore.AppendStringElement(nil, "id", id)
	elems = bsoncore.AppendStringElement(elems, "state", cluster.MemberJoining.String())
	elems = bsoncore.AppendStringElement(elems, "expect_state", from.String())
	elems = bsoncore.AppendInt64Element(elems, "incarnation", incarnation)
	elems = bsoncore.AppendBooleanElement(elems, "clear_bits", true)
	return bsoncore.BuildDocument(nil, elems)
}

// replaceOutcome is the statement's one line, from the committed row.
func replaceOutcome(state cluster.ClusterState, row cluster.Member, addr string) MembershipOutcome {
	line := fmt.Sprintf("Replaced %s@%s: %s (epoch %d)", row.ID, addr, row.State, state.Epoch)
	if row.State == cluster.MemberJoining {
		line = fmt.Sprintf("Replacing %s@%s: joining, then active when it holds the %d pairs it owns (epoch %d). Run 'status seeding' on %s to follow it",
			row.ID, addr, activationGap(state, row.ID).Total, state.Epoch, row.ID)
	}
	return MembershipOutcome{Epoch: state.Epoch, StateSeq: state.StateSeq, Result: line}
}

// soleCopies is what replace-node would lose: the pairs no other serving member has reported.
type soleCopies struct {
	pairs     int
	databases int    // how many databases have such a pair
	database  string // the first of them
	rf        int32  // its replication factor
}

// inDatabases words the databases of a loss for a line: "1 database" or "3 databases".
func (s soleCopies) inDatabases() string {
	if s.databases == 1 {
		return "1 database"
	}
	return fmt.Sprintf("%d databases", s.databases)
}

// soleCopiesOf counts the pairs (slot, database) id may hold the only copy of: every pair id owns
// by placement, ranked as if it were active so a dead id's pairs are found too (its id ranks as it
// always did), and every pair whose committed SEEDED bit id holds. A pair is not counted when an
// active or joining member other than id has its committed bit: that member reported a copy it
// serves or is seeding into. A bit nobody has committed yet (the reporters batch them, review B-1)
// is no copy, and neither is a down member's (it may never come back) or a dead one's (review B-3):
// replace-node then refuses, and the operator brings the other member back first, waits for the
// reports and retries, or accepts the loss. Clearing id's bits leaves such a pair with no holder,
// and its next owner makes it ready and empty (design section 4.2).
func soleCopiesOf(state cluster.ClusterState, id string) soleCopies {
	rows := newSeedBitRows(state)
	var holders []string
	ranked := state
	ranked.Members = slices.Clone(state.Members)
	for i, member := range ranked.Members {
		if member.ID == id {
			ranked.Members[i].State = cluster.MemberActive
			continue
		}
		if !member.Witness && (member.State == cluster.MemberActive || member.State == cluster.MemberJoining) {
			holders = append(holders, member.ID)
		}
	}
	table := cluster.BuildTable(ranked)
	var sole soleCopies
	for _, database := range state.Databases {
		found := 0
		for slot := range uint16(storage.SlotCount) {
			mine := rows.Holds(id, database.ID, slot) || slices.ContainsFunc(table.Copies(slot, int(database.RF)), func(copyOf cluster.Member) bool { return copyOf.ID == id })
			if !mine || slices.ContainsFunc(holders, func(other string) bool { return rows.Holds(other, database.ID, slot) }) {
				continue
			}
			found++
		}
		if found == 0 {
			continue
		}
		if sole.databases == 0 {
			sole.database, sole.rf = database.Name, database.RF
		}
		sole.pairs += found
		sole.databases++
	}
	return sole
}

// ---- texts (rule 3: the thing, then the statement that fixes it) ----------------------------

// replaceNotDown is R2.
func replaceNotDown(id, addr string, state cluster.MemberState, epoch int64) error {
	return filled(cluster.CodeConflict,
		"node <node> is <state> at epoch <e>, and replace-node gives the identity of a node that is down or dead to a new process. Nothing was changed. "+
			"Stop <node> and wait until 'nodes' shows it down, then run 'replace-node <node> --with <node>@<addr>' again",
		"<node>", id, "<state>", state.String(), "<e>", strconv.FormatInt(epoch, 10), "<addr>", addr)
}

// replaceOtherAddress is R3: this release replaces a node at its own committed address only.
func replaceOtherAddress(id, typed, committed string) error {
	return filled(cluster.CodeInvalidArgument,
		"cannot replace <node> at <typed>: this release replaces a node at its own address, <addr>. Nothing was changed. "+
			"Start the new process with cluster.listen (or cluster.advertise) set to <addr>, then run 'replace-node <node> --with <node>@<addr>'",
		"<node>", id, "<typed>", typed, "<addr>", committed)
}

// replaceWouldLoseCopies is R4, the owner's refusal by default (design section 9 Q1).
func replaceWouldLoseCopies(id, addr string, sole soleCopies) error {
	where := fmt.Sprintf(`database "%s" (replication factor %d)`, sole.database, sole.rf)
	if sole.databases > 1 {
		where += fmt.Sprintf(" and %d other databases", sole.databases-1)
	}
	return filled(cluster.CodeInvalidArgument,
		"replace-node <node> would leave <n> pairs of <where> with no copy: no other active member has reported one, so their documents would be lost. Nothing was changed. "+
			"Start <node> again on the ring directory you moved aside to keep them; if another member that holds a copy is down, start it first, or wait a few seconds for the members' reports and retry; "+
			"or, to accept the loss, run 'replace-node <node> --with <node>@<addr> --accept-loss'",
		"<node>", id, "<addr>", addr, "<n>", strconv.Itoa(sole.pairs), "<where>", where)
}

// replaceHoldsState is R5: the node answering at the address did not lose its data. Its code and
// text are the conventions catalog's REPLACE_TARGET_HAS_DATA, the one source (review B-4).
func replaceHoldsState(id, addr string) error {
	return &cluster.Error{Code: cluster.CodeReplaceTargetHasData, Message: cluster.CodeReplaceTargetHasData.Text(cluster.CondReplaceTargetHasData, map[string]string{"node": id, "addr": addr})}
}

// replaceRoleDiffers refuses a replacement whose cluster.witness setting is not the member's role.
func replaceRoleDiffers(id, addr string, witness bool) error {
	row, is := "member", "witness"
	if witness {
		row, is = "witness", "member"
	}
	return filled(cluster.CodeInvalidArgument,
		"cannot replace <node>: it is registered as a <row>, and the node at <addr> is configured as a <is> (cluster.witness). Nothing was changed. "+
			"Set cluster.witness on <node> to match its role, restart it, then run 'replace-node <node> --with <node>@<addr>' again",
		"<node>", id, "<addr>", addr, "<row>", row, "<is>", is)
}

// replaceStaleIncarnation refuses a node whose incarnation is not newer than the committed one: the
// FSM would refuse the commit, and a clock that went back is the cause to fix.
func replaceStaleIncarnation(id, addr string, got, committed int64) error {
	return filled(cluster.CodeConflict,
		"cannot replace <node>: the node at <addr> reports incarnation <got>, not newer than the <committed> the cluster holds for it. Nothing was changed. "+
			"Check the clock of <node>'s machine, restart <node>, then run 'replace-node <node> --with <node>@<addr>' again",
		"<node>", id, "<addr>", addr, "<got>", strconv.FormatInt(got, 10), "<committed>", strconv.FormatInt(committed, 10))
}

// replaceAskFailed is an identity ask refused before anything was committed. A refusal that says
// who the node is (another key, another cluster, another build) keeps its own text; one that never
// reached the node says how to start it for a replace; its cause is not repeated, because the
// join-worded text of the ask (I5) carries a join's fix.
func replaceAskFailed(id, addr string, err error) error {
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeUnavailable {
		return err
	}
	return filled(cluster.CodeUnavailable,
		"cannot replace <node>: <addr> did not answer the identity request, so <node> is not running there or does not trust this node's key. Nothing was changed. "+
			"Start <node> with its own node id, keys and address, an empty ring directory and hash_ring.seed_nodes (HOARDDB_SEED_NODES) naming this cluster, "+
			"then run 'replace-node <node> --with <node>@<addr>' again",
		"<node>", id, "<addr>", addr)
}

// replaceUnfinished is a failure after the commit: the row is joining and a repeat finishes it.
// When the statement accepted a loss its bits are cleared, and the repeat's gate counts those pairs
// again, so the advice repeats --accept-loss (narrow review B-3).
func replaceUnfinished(id, addr, step string, acceptLoss bool, err error) error {
	template := "replace-node <node>: <node> is committed joining, and <step> did not finish (<cause>). " +
		"Run 'replace-node <node> --with <node>@<addr>' again to finish it"
	if acceptLoss {
		template = "replace-node <node>: <node> is committed joining with the loss accepted, and <step> did not finish (<cause>). " +
			"Run 'replace-node <node> --with <node>@<addr> --accept-loss' again to finish it"
	}
	return filled(cluster.CodeUnavailable, template, "<node>", id, "<addr>", addr, "<step>", step, "<cause>", err.Error())
}

// stateMovedRetries bounds how often the rest of a replace runs again after STATE_MOVED.
const stateMovedRetries = 5

// retryStateMoved runs step until it succeeds, fails with anything but STATE_MOVED, or has run
// stateMovedRetries times: the rest of a join commits against the state it read while the seed
// reporters commit concurrently (review B-6).
func retryStateMoved(step func() error) error {
	var err error
	for attempt := range stateMovedRetries {
		if err = step(); err == nil || !retryableActivation(err) {
			return err
		}
		if attempt < stateMovedRetries-1 {
			time.Sleep(20 * time.Millisecond)
		}
	}
	return err
}

// replaceLossGate is R4, asked before any commit that clears id's bits: the first replace and a
// repeat that clears a held joining node's bits (re-review F-1). Without accept_loss a sole copy is
// refused; with it the loss is returned so the statement names and logs it.
func replaceLossGate(state cluster.ClusterState, id, addr string, acceptLoss bool) (soleCopies, error) {
	sole := soleCopiesOf(state, id)
	if sole.pairs > 0 && !acceptLoss {
		return soleCopies{}, replaceWouldLoseCopies(id, addr, sole)
	}
	return sole, nil
}

// noteLoss logs an accepted loss on the leader and appends it to the statement's line (review B-2).
func (n *Node) noteLoss(id string, sole soleCopies, outcome MembershipOutcome, err error) (MembershipOutcome, error) {
	if sole.pairs == 0 || err != nil {
		return outcome, err
	}
	outcome.Result += fmt.Sprintf(" It accepted the loss of %d pairs in %s, which only %s held.", sole.pairs, sole.inDatabases(), id)
	return outcome, nil
}

// warnLoss writes the accepted loss to the leader's log as soon as the clearing commit is made.
func (n *Node) warnLoss(id string, sole soleCopies) {
	if sole.pairs > 0 {
		n.cl.warn(fmt.Sprintf("replace-node %s accepted the loss of %d pairs in %s, which only %s held", id, sole.pairs, sole.inDatabases(), id))
	}
}
