package wire

import (
	"context"
	entropy "crypto/rand"
	"errors"
	"fmt"
	jitter "math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The relay of cluster commands to the Raft leader (conventions 6.1 kinds 0x19 and 0x1A, build
// doc SPEC-GAP R-1).
//
// A command (create database, schema bump, member state, setting) can be committed only by the
// Raft leader. A node that is not the leader hands the command to the leader on the control
// channel, once: the leader applies it through the same applyAsLeader a command it proposed
// itself goes through, and never relays it again, so there is no hop count and no loop. The
// sender's identity is the pinned TLS identity the handshake carries, never a field of the
// message.
//
// At most once. A relayed attempt has one of three fates: it did not reach the leader's log
// (NOT_LEADER, refusals), it is in the log (APPLIED), or nothing is known (a lost reply, a
// leadership change after the append, any Raft error that is not a refusal). The proposer
// repeats only the first and the third, and before repeating an unknown one it looks at the
// committed state:
//   - the leader keeps, for the last relayLedgerCap applied proposals, their (sender, request
//     id) and result, and answers a repeat with the stored result without applying it again;
//     a repeat that arrives while the first is still committing waits for it;
//   - past that ledger (a leader change, or 256 later relays) each relayable command is
//     guarded by the state it changes, enforced by the FSM: CREATE_DATABASE names the id the
//     first application took, BUMP_SCHEMA the version it left, SET_SETTING carries
//     expect_value and SET_MEMBER_STATE expect_state, so a second application is refused;
//   - the proposer reads the effect in the committed state (committedEffect) before it
//     repeats and again before it reports a refusal, and finding it, returns success.
//
// Cost: relayLedgerCap entries per node (about 100 bytes each), nothing per bucket or store,
// no goroutine and no timer. Each relayed command also makes the leader copy the committed
// state once (the dry run in relayAuthority hands ApplyCommand a shallow copy, which clones), and
// once more for the kinds that read the voters or the sender's row; about 1 KiB per member and
// database (the seed bitmaps), so O(members x databases) of allocation per relayed create,
// setting or activation, never O(buckets).

const (
	// relayCallTimeout bounds one relayed attempt; the leader's own commit can take a
	// replication round, never five seconds on a healthy group.
	relayCallTimeout = 5 * time.Second
	// relayLedgerCap bounds the leader's memory of applied relayed proposals.
	relayLedgerCap = 256
	// staleBaseTries bounds how often the leader re-stamps a command refused STATE_MOVED
	// because something else committed between reading state_seq and appending.
	staleBaseTries = 3
	// effectSettle is how long a proposer keeps reading the committed state for the effect
	// of an unknown attempt after the leader refused the repeat.
	effectSettle = 2 * time.Second
)

// relayable lists the command kinds a member may relay. Everything else is the leader's own:
// bootstrap, join, promote and removal change the Raft configuration. The seed commands are
// relayable for the sender's own bits only, which relayAuthority enforces: a node reports the
// bits it holds, and a witness holds none. The six user commands are relayable from any member
// that is not a witness (ring user store spec 6.1 step 4): a user change is accepted on whichever
// data node the client is logged in to.
func relayable(kind byte) bool {
	return cluster.UserCommandKind(kind) || slices.Contains([]byte{cluster.CommandSetMemberState, cluster.CommandCreateDatabase, cluster.CommandSetSetting, cluster.CommandBumpSchema,
		cluster.CommandSeeded, cluster.CommandRetire}, kind)
}

// relayKey names one proposal of one member.
type relayKey struct {
	from string
	id   [transport.RelayRequestIDLen]byte
}

// relayCall is a proposal being committed; later copies of it wait on done.
type relayCall struct {
	done  chan struct{}
	reply transport.RelayReply
}

// relayLedger is the leader's bounded memory of relayed proposals.
type relayLedger struct {
	mu       sync.Mutex
	applied  map[relayKey]transport.RelayReply
	order    []relayKey
	inflight map[relayKey]*relayCall
	// replays counts repeats answered from memory (stored or waited for), for tests and
	// diagnosis.
	replays atomic.Int64
}

// begin returns the stored reply of an applied proposal, or the call to wait for when the
// same proposal is already committing, or registers the caller as the one to commit it.
func (l *relayLedger) begin(key relayKey) (stored transport.RelayReply, pending *relayCall, owner bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if reply, ok := l.applied[key]; ok {
		l.replays.Add(1)
		return reply, nil, false
	}
	if call, ok := l.inflight[key]; ok {
		l.replays.Add(1)
		return transport.RelayReply{}, call, false
	}
	if l.inflight == nil {
		l.inflight = map[relayKey]*relayCall{}
	}
	call := &relayCall{done: make(chan struct{})}
	l.inflight[key] = call
	return transport.RelayReply{}, call, true
}

// finish records the owner's reply, remembers it when it is an applied one, and wakes the
// copies that waited.
func (l *relayLedger) finish(key relayKey, call *relayCall, reply transport.RelayReply) {
	l.mu.Lock()
	call.reply = reply
	delete(l.inflight, key)
	if reply.Status == transport.RelayApplied {
		if l.applied == nil {
			l.applied = map[relayKey]transport.RelayReply{}
		}
		l.order = append(l.order, key)
		l.applied[key] = reply
		if len(l.order) > relayLedgerCap {
			delete(l.applied, l.order[0])
			l.order = l.order[1:]
		}
	}
	l.mu.Unlock()
	close(call.done)
}

// RelayReplays is how many relayed proposals this node answered from its ledger instead of
// committing them again (a repeat after a lost reply).
func (n *Node) RelayReplays() int64 { return n.cl.relays.replays.Load() }

// serveRelay is the leader's side of RELAY_PROPOSAL. It never relays: a node that is not the
// leader answers NOT_LEADER with the leader it knows. It checks no membership itself: the
// channel server admitted the sender at the handshake as a committed member of this cluster
// whose pinned key is its own (AdmitPeer), and from.NodeID is that identity.
func (n *Node) serveRelay(ctx context.Context, from transport.Handshake, req transport.RelayProposal) transport.RelayReply {
	if !n.raft.IsLeader() {
		return transport.RelayReply{Status: transport.RelayNotLeader, Leader: n.raft.Leader()}
	}
	if !relayable(req.Kind) {
		return relayRefusal(cluster.CodeInvalidArgument, "command kind 0x%02x cannot be relayed. Run it on the Raft leader (%s)", req.Kind, n.cfg.NodeID)
	}
	key := relayKey{from: from.NodeID, id: req.RequestID}
	stored, call, owner := n.cl.relays.begin(key)
	if call == nil {
		return stored
	}
	if !owner {
		select {
		case <-call.done:
			return call.reply
		case <-ctx.Done():
			return transport.RelayReply{Status: transport.RelayNoQuorum}
		}
	}
	// The ledger is asked first: a repeat of an applied proposal must get its stored answer,
	// where the dry run would refuse it as stale.
	var reply transport.RelayReply
	if refusal := n.relayAuthority(ctx, from.NodeID, req); refusal != nil {
		reply = n.relayReply(cluster.Result{}, refusal)
	} else {
		reply = n.relayReply(n.applyAsLeader(ctx, req.Kind, req.Body))
	}
	n.cl.relays.finish(key, call, reply)
	return reply
}

// relayAuthority is everything the leader checks about a relayed command before it appends
// anything: the command is dry-run against the committed state (the FSM's own rules, so the
// name policy, the replication bound and every compare-and-set are the same ones a local
// proposal meets, and a refusal costs no log entry), then the sender's standing is checked
// per kind. nil means go on.
//   - SET_MEMBER_STATE: only the sender's own activation or re-join (id == the handshake's
//     NodeID, state active or joining, expect_state given). down and dead stay on the
//     failure detector's evidence path on the leader.
//   - SET_SETTING and CREATE_DATABASE: only from a voter. CREATE_DATABASE also meets the
//     storage-directory rule, which needs this node's layout.
//   - BUMP_SCHEMA: only the next version (every alter and drop bumps by one); a note proves
//     nothing, so a far jump is refused whatever it carries.
//   - SEEDED and RETIRE: only the sender's own seed bits (node == the handshake's NodeID), and
//     never a witness's, which holds no data. A joining member may report: it is a committed
//     member, and the bits it holds are the point of the report.
//   - The user commands: from a committed member that is not a witness and is joining, active
//     or down (never dead or leaving). A witness holds no
//     password key and serves no logins (owner ruling, 2026-11-19), so no user change can come
//     from a session on one. Who asked (by, by_seq) is the sender's account of it, judged by the
//     FSM against the committed table, as CREATE_DATABASE trusts the relaying voter.
func (n *Node) relayAuthority(ctx context.Context, sender string, req transport.RelayProposal) error {
	// The same dry run ProposeContext makes, so a refusal is the cluster's and a diverged leader
	// refuses with text 4; a state that moved since the sequence was read comes back STATE_MOVED.
	// Only the sequence is read here: the dry run takes its own copy of the state.
	if err := n.raft.DryRun(ctx, cluster.Command{Version: cluster.CommandVersion, Kind: req.Kind, BaseSeq: uint64(n.raft.StateSeq()), Body: req.Body}); err != nil {
		return err
	}
	doc := bsoncore.Document(req.Body)
	denied := func(format string, args ...any) error {
		return &cluster.Error{Code: cluster.CodePermissionDenied, Message: fmt.Sprintf(format, args...)}
	}
	switch req.Kind {
	case cluster.CommandSetMemberState:
		id, _ := doc.Lookup("id").StringValueOK()
		word, _ := doc.Lookup("state").StringValueOK()
		_, expects := doc.Lookup("expect_state").StringValueOK()
		if id != sender {
			return denied("%s may relay only its own member state, not %s's. Run the command on the Raft leader (%s) instead", sender, id, n.raft.Leader())
		}
		if word != "active" && word != "joining" {
			return denied("a relayed member state is active or joining; %q is set by the leader's failure detector from probe evidence", word)
		}
		if !expects {
			return denied("a relayed member state needs expect_state, the state the sender saw, so a repeat cannot undo a newer change")
		}
	case cluster.CommandSetSetting, cluster.CommandCreateDatabase:
		if !slices.Contains(n.raft.State().Voters, sender) {
			return denied("%s is not a voter, so it may not change cluster settings or create databases through a relay. Run the command on a voter", sender)
		}
		if req.Kind == cluster.CommandSetSetting {
			if doc.Lookup("expect_value").Type == 0 {
				return denied("a relayed setting needs expect_value, the value the sender saw, so a repeat cannot overwrite a newer change")
			}
			break
		}
		name, _ := doc.Lookup("name").StringValueOK()
		if err := databaseNameRefusal(name, n.cfg.StorageNames); err != nil {
			return err
		}
	case cluster.CommandBumpSchema:
		from, _ := doc.Lookup("from").Int64OK()
		to, _ := doc.Lookup("to").Int64OK()
		if to != from+1 {
			return denied("a relayed schema bump moves a database one version, not from %d to %d: every alter and drop bumps by one", from, to)
		}
	case cluster.CommandSeeded, cluster.CommandRetire:
		node, _ := doc.Lookup("node").StringValueOK()
		if node != sender {
			return denied("%s may relay only its own seed bits, not %s's. Run the command on the Raft leader (%s) instead", sender, node, n.raft.Leader())
		}
		member, known := reconcileMember(n.raft.State(), sender)
		if !known || member.Witness {
			return denied("%s holds no data (a witness has no seed bits), so it may not report any", sender)
		}
	default:
		if !cluster.UserCommandKind(req.Kind) {
			break
		}
		member, known := reconcileMember(n.raft.State(), sender)
		if !known || member.Witness {
			return denied("%s is a witness or not a member: it serves no client logins, so it may not change users. Run the statement on a data node", sender)
		}
		// Who asked is the sender's claim, so only a member the cluster still counts may make it:
		// the states the pepper hand-out serves (spec 5.3), never a dead or leaving one, which the
		// handshake still admits (Raft.AdmitPeer checks the row, not its state).
		if member.State != cluster.MemberJoining && member.State != cluster.MemberActive && member.State != cluster.MemberDown {
			return denied("%s is %s, not a live member of this cluster, so it may not change users. Run the statement on a data node that is joining, active or down", sender, member.State)
		}
	}
	return nil
}

func relayRefusal(code cluster.Code, format string, args ...any) transport.RelayReply {
	return transport.RelayReply{Status: transport.RelayRefused, Code: uint16(code), Message: fmt.Sprintf(format, args...)}
}

// relayReply turns the outcome of applyAsLeader into the wire reply.
func (n *Node) relayReply(result cluster.Result, err error) transport.RelayReply {
	var notLeader *cluster.NotLeaderError
	var refusal *cluster.Error
	switch {
	case err == nil:
		return transport.RelayReply{Status: transport.RelayApplied, Epoch: uint64(result.Epoch), StateSeq: uint64(result.StateSeq), EpochMoved: result.EpochMoved}
	case errors.As(err, &notLeader) && notLeader.Undecided:
		return transport.RelayReply{Status: transport.RelayNoQuorum}
	case errors.As(err, &notLeader):
		return transport.RelayReply{Status: transport.RelayNotLeader, Leader: notLeader.Leader}
	case errors.As(err, &refusal):
		return transport.RelayReply{Status: transport.RelayRefused, Code: uint16(refusal.Code), Message: refusal.Message}
	}
	// Any other error from the Raft library is an unknown outcome, not a refusal: the entry
	// may be in the log.
	return transport.RelayReply{Status: transport.RelayNoQuorum}
}

// applyAsLeader commits one command through this node's Raft seat against the state it holds
// now: the one path a local proposal on the leader and a relayed proposal both take. A command
// the FSM refuses STATE_MOVED because something committed between reading state_seq and the
// append changed nothing, so it is stamped again, a bounded number of times.
func (n *Node) applyAsLeader(ctx context.Context, kind byte, body []byte) (cluster.Result, error) {
	var result cluster.Result
	// Returned as is when time has run out before anything was sent: nothing was appended.
	err := error(&cluster.NotLeaderError{Refusal: &cluster.Error{Code: cluster.CodeUnavailable, Message: "time ran out before the command was sent, so nothing was changed"}})
	for range staleBaseTries {
		if ctx.Err() != nil {
			break
		}
		result, err = n.raft.ProposeContext(ctx, cluster.Command{Version: cluster.CommandVersion, Kind: kind, BaseSeq: uint64(n.topology.Refresh().StateSeq), Body: body})
		var refusal *cluster.Error
		if !errors.As(err, &refusal) || refusal.Code != cluster.CodeStateMoved {
			break
		}
	}
	n.topology.Refresh()
	return result, err
}

// attempt is one try of a proposal.
type attempt struct {
	result cluster.Result
	err    error
	// retry is true when the proposal may be tried again; false means result and err are final.
	retry bool
	// undecided is true when the command may have been committed although this attempt
	// reports no result.
	undecided bool
	// leader is the hint a NOT_LEADER reply gave.
	leader string
}

// propose commits one cluster command wherever the Raft leader is. On the leader it goes
// through applyAsLeader; elsewhere it is relayed to the leader and the call returns once this
// node has applied the command too (and, for a schema bump, published it). A not-leader
// refusal, STATE_MOVED, a leader change and a lost reply are retried with jittered backoff.
// The whole proposal, every attempt and the wait for the local apply included, ends at the
// budget (ClusterOptions.ProposalBudget, default bootTimeout) or when the caller's context
// ends. Any other refusal by the FSM is final.
func (n *Node) propose(parent context.Context, kind byte, body []byte) (cluster.Result, error) {
	result, _, err := n.proposeTracked(parent, kind, body)
	return result, err
}

// proposeTracked is propose that also reports whether any attempt was undecided: sent, with its
// outcome never learned. A caller that must tell "nothing changed" from "may have changed" reads
// it (ring user store spec 6.1 step 6, 6.2 B3).
func (n *Node) proposeTracked(parent context.Context, kind byte, body []byte) (cluster.Result, bool, error) {
	var requestID [transport.RelayRequestIDLen]byte
	if _, err := entropy.Read(requestID[:]); err != nil {
		return cluster.Result{}, false, fmt.Errorf("drawing the request id of a cluster command: %w", err)
	}
	seed := jitter.Uint64()
	rng := jitter.New(jitter.NewPCG(seed, seed))
	budget := bootTimeout
	if n.cl != nil && n.cl.opts.ProposalBudget > 0 {
		budget = n.cl.opts.ProposalBudget
	}
	ctx, cancel := context.WithTimeout(parent, budget)
	defer cancel()
	undecided := false
	hint := ""
	var last error
	for tries := 0; ; tries++ {
		if tries > 0 {
			select {
			case <-ctx.Done():
				result, err := n.proposalEnded(parent, kind, body, last, undecided)
				return result, undecided, err
			case <-time.After(alterBackoff(tries-1, rng)):
			}
			if undecided {
				if result, done := n.committedEffect(kind, body); done {
					return result, undecided, nil
				}
			}
		}
		got := n.proposeOnce(ctx, requestID, kind, body, hint)
		undecided = undecided || got.undecided
		if got.retry {
			hint, last = got.leader, got.err
			if ctx.Err() != nil {
				result, err := n.proposalEnded(parent, kind, body, last, undecided)
				return result, undecided, err
			}
			continue
		}
		if got.err != nil && undecided {
			if result, done := n.settledEffect(ctx, kind, body); done {
				return result, undecided, nil
			}
		}
		n.topology.Refresh()
		return got.result, undecided, got.err
	}
}

// proposalEnded is the result of a proposal whose budget or caller's context ran out. It says
// whether the command might nevertheless be in the log, and one last read of the committed
// state may find that it is.
func (n *Node) proposalEnded(parent context.Context, kind byte, body []byte, last error, undecided bool) (cluster.Result, error) {
	if undecided {
		if result, done := n.committedEffect(kind, body); done {
			return result, nil
		}
	}
	if err := parent.Err(); err != nil {
		if undecided {
			return cluster.Result{}, fmt.Errorf("the command may have been committed; do not run it again yet, check the cluster state first: %w", err)
		}
		return cluster.Result{}, fmt.Errorf("waiting for the cluster to take the command: %w", err)
	}
	return cluster.Result{}, proposalGaveUp(last, undecided)
}

// proposalGaveUp is the error of a proposal that found no leader in time.
func proposalGaveUp(last error, undecided bool) error {
	if undecided {
		return &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
			"no Raft leader answered in time and the command may have been committed (%v). Do not run it again yet: run 'nodes' to see who leads and check the cluster state first", last)}
	}
	return &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
		"no Raft leader answered in time, so nothing was changed (%v). Run 'nodes' to see which members are up, then retry", last)}
}

// retryable reports whether a refusal from the FSM means "nothing changed, try again": only
// STATE_MOVED, a command built on a state_seq something else has since moved.
func retryable(err error) bool {
	var refusal *cluster.Error
	return errors.As(err, &refusal) && refusal.Code == cluster.CodeStateMoved
}

// decided reports whether err is a refusal by the FSM or the relay, which proves the command
// is not in the log. Any other error from the Raft library leaves the outcome unknown.
func decided(err error) bool {
	var refusal *cluster.Error
	return errors.As(err, &refusal)
}

func (n *Node) proposeOnce(ctx context.Context, requestID [transport.RelayRequestIDLen]byte, kind byte, body []byte, hint string) attempt {
	leader := hint
	if leader == "" {
		leader = n.raft.Leader() // a NOT_LEADER hint is newer than this node's own view
	}
	if n.cl == nil || n.raft.IsLeader() {
		result, err := n.applyAsLeader(ctx, kind, body)
		var notLeader *cluster.NotLeaderError
		switch {
		case err == nil:
			return attempt{result: result}
		case errors.As(err, &notLeader):
			return attempt{err: err, retry: true, undecided: notLeader.Undecided, leader: notLeader.Leader}
		case retryable(err):
			return attempt{err: err, retry: true}
		case !decided(err):
			return attempt{err: err, retry: true, undecided: true}
		}
		return attempt{err: err}
	}
	if leader == "" || leader == n.cfg.NodeID {
		return attempt{err: errors.New("no leader is elected yet"), retry: true}
	}
	callCtx, cancel := context.WithTimeout(ctx, relayCallTimeout)
	defer cancel()
	reply, err := n.cl.ctrl.RelayProposal(callCtx, leader, transport.RelayProposal{RequestID: requestID, Kind: kind, Body: body})
	if err != nil {
		if errors.Is(err, transport.ErrControlBody) {
			// The codec refused to carry the command: it never left this node.
			return attempt{err: &cluster.Error{Code: cluster.CodeInvalidArgument, Message: fmt.Sprintf(
				"the command cannot be sent to the leader: %v. Nothing was changed. Use a shorter name or value (a command body is at most %d bytes)", err, transport.MaxRelayBody)}}
		}
		// The command may have reached the leader's log before the connection failed.
		return attempt{err: fmt.Errorf("relaying the command to leader %s: %w", leader, err), retry: true, undecided: true}
	}
	switch reply.Status {
	case transport.RelayApplied:
		result := cluster.Result{Epoch: int64(reply.Epoch), StateSeq: int64(reply.StateSeq), EpochMoved: reply.EpochMoved}
		if err := n.awaitApplied(ctx, kind, body, result); err != nil {
			// Committed, but not yet applied here: final, and its outcome is not one this node
			// can show yet, so a caller must not say "nothing changed".
			return attempt{err: err, undecided: true}
		}
		return attempt{result: result}
	case transport.RelayNotLeader:
		return attempt{err: fmt.Errorf("%s is not the Raft leader", leader), retry: true, leader: reply.Leader}
	case transport.RelayNoQuorum:
		return attempt{err: fmt.Errorf("leader %s could not commit the command in time", leader), retry: true, undecided: true}
	case transport.RelayRefused:
		refusal := &cluster.Error{Code: cluster.Code(reply.Code), Message: reply.Message}
		return attempt{err: refusal, retry: retryable(refusal)}
	}
	return attempt{err: fmt.Errorf("leader %s answered a relayed command with status %d", leader, reply.Status), retry: true, undecided: true}
}

// awaitApplied waits until this node's own FSM has applied the command the leader committed,
// and for a schema bump until this node publishes the new version, so the caller reads its own
// write exactly as it does after a proposal on the leader. It ends with the proposal's context.
func (n *Node) awaitApplied(ctx context.Context, kind byte, body []byte, result cluster.Result) error {
	for {
		if n.raft.StateSeq() >= result.StateSeq && n.publishedBump(kind, body) {
			return nil
		}
		select {
		case <-ctx.Done():
			return &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
				"the leader committed the command at state_seq %d, but this node has not applied it yet (it is at %d). The change is not lost: do not run the command again; run 'nodes' and check in a moment",
				result.StateSeq, n.raft.StateSeq())}
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// publishedBump is true for any command but a schema bump, and for a bump once this node
// publishes the version it moves to.
func (n *Node) publishedBump(kind byte, body []byte) bool {
	if kind != cluster.CommandBumpSchema {
		return true
	}
	dbID, to, ok := bumpTarget(body)
	if !ok {
		return true
	}
	published, found := n.bumps.Published(uint64(dbID))
	return found && published >= to
}

func bumpTarget(body []byte) (dbID, to int64, ok bool) {
	doc := bsoncore.Document(body)
	dbID, okDB := doc.Lookup("db_id").Int64OK()
	to, okTo := doc.Lookup("to").Int64OK()
	return dbID, to, okDB && okTo
}

// committedEffect reports whether the committed state already shows the effect of command
// kind with body, which is how a proposer recognises that an attempt it lost track of was
// applied. Every relayable kind names what it changes: CREATE_DATABASE the name, id, copies and storage,
// BUMP_SCHEMA the version, SET_SETTING the value, SET_MEMBER_STATE the state, and SEEDED and
// RETIRE the seed bitmap row (every named bit set, respectively clear). A bump read as done by
// another bump that went further is harmless: the alter re-reads the table in force. Their
// expect fields make a second application a refusal, so "the effect is there" and "applied
// once" agree. A user command shows its effect when its request id is in the cluster's list of
// applied user commands (ring user store spec 6.1 step 6): not the row, which a later change may
// have moved or a drop removed (B1, B5), and the same for USER_BOOTSTRAP, so a node whose own
// bootstrap reply was lost reports it done (B14). Its Result carries the state_seq that applied
// it; a user command never moves the epoch, so Epoch is left zero.
func (n *Node) committedEffect(kind byte, body []byte) (cluster.Result, bool) {
	if cluster.UserCommandKind(kind) {
		users := n.raft.Users()
		request, ok := cluster.UserCommandRequest(body)
		if !ok {
			return cluster.Result{}, false
		}
		seq, done := users.RequestApplied(request)
		return cluster.Result{StateSeq: seq}, done
	}
	state := n.raft.State()
	doc := bsoncore.Document(body)
	done := false
	switch kind {
	case cluster.CommandCreateDatabase:
		name, okName := doc.Lookup("name").StringValueOK()
		id, okID := doc.Lookup("id").Int64OK()
		rf, okRF := doc.Lookup("rf").Int32OK()
		word := "disk"
		if v, has := doc.Lookup("storage").StringValueOK(); has {
			word = v
		}
		// Ids come from NextDatabaseID, so two creators on two nodes can propose one name and id
		// with other options: only a row with this command's own copies and storage is its effect.
		done = okName && okID && okRF && slices.ContainsFunc(state.Databases, func(row cluster.Database) bool {
			return row.Name == name && row.ID == id && row.RF == rf && row.StorageWord() == word
		})
	case cluster.CommandBumpSchema:
		dbID, to, ok := bumpTarget(body)
		done = ok && slices.ContainsFunc(state.Databases, func(row cluster.Database) bool { return row.ID == dbID && row.SchemaVersion >= to }) && n.publishedBump(kind, body)
	case cluster.CommandSetSetting:
		done = settingHolds(state.Settings, doc)
	case cluster.CommandSetMemberState:
		id, okID := doc.Lookup("id").StringValueOK()
		word, okWord := doc.Lookup("state").StringValueOK()
		done = okID && okWord && slices.ContainsFunc(state.Members, func(m cluster.Member) bool { return m.ID == id && m.State.String() == word })
	case cluster.CommandSeeded, cluster.CommandRetire:
		done = seedBitsHold(state, doc, kind == cluster.CommandSeeded)
	}
	return cluster.Result{Epoch: state.Epoch, StateSeq: state.StateSeq}, done
}

// seedBitsHold reports whether the committed seed bitmap row a SEEDED or RETIRE body names
// already holds every bit the body sets (wantSet) or every bit it clears (!wantSet). An absent
// row counts every bit as clear, so a RETIRE of bits it does not hold is already done. A body
// whose node, db_id or bits cannot be read shows no effect: claiming one would report success
// for a command the FSM would refuse.
func seedBitsHold(state cluster.ClusterState, doc bsoncore.Document, wantSet bool) bool {
	node, okNode := doc.Lookup("node").StringValueOK()
	dbID, okDB := doc.Lookup("db_id").Int64OK()
	_, bits, okBits := doc.Lookup("bits").BinaryOK()
	if !okNode || !okDB || !okBits || machine.Validate(bits) != nil {
		return false
	}
	var row []byte
	for _, b := range state.SeedBitmaps {
		if b.Node == node && b.DBID == dbID {
			row = b.Bits
			break
		}
	}
	for slot := range machine.BitmapBytes * 8 {
		if bits[slot/8]>>(slot%8)&1 == 0 {
			continue
		}
		held := row != nil && row[slot/8]>>(slot%8)&1 == 1
		if held != wantSet {
			return false
		}
	}
	return true
}

// settingHolds reports whether the setting a SET_SETTING body names already holds its value.
func settingHolds(settings cluster.Settings, doc bsoncore.Document) bool {
	key, okKey := doc.Lookup("key").StringValueOK()
	now, known := settings.Value(key)
	value := doc.Lookup("value")
	if !okKey || !known {
		return false
	}
	switch want := now.(type) {
	case bool:
		got, ok := value.BooleanOK()
		return ok && got == want
	case int32:
		got, ok := value.Int32OK()
		return ok && got == want
	case int64:
		got, ok := value.Int64OK()
		return ok && got == want
	}
	return false
}

// settledEffect is committedEffect read for a short while: a refusal of a repeat can arrive
// before this node has applied the entry of the attempt it lost track of.
func (n *Node) settledEffect(ctx context.Context, kind byte, body []byte) (cluster.Result, bool) {
	ctx, cancel := context.WithTimeout(ctx, effectSettle)
	defer cancel()
	for {
		if result, done := n.committedEffect(kind, body); done {
			return result, true
		}
		select {
		case <-ctx.Done():
			return cluster.Result{}, false
		case <-time.After(5 * time.Millisecond):
		}
	}
}
