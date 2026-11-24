package wire

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// SlotStates reports the persisted state of a (slot, database) pair record (conv §12).
type SlotStates interface {
	// State is the pair's record state; machine.SlotAbsent when this node holds none.
	State(slot uint16, db uint64) machine.SlotState
	// Serves says a copy on this node may answer a read: the record is ready, is not asked to
	// rebuild (LOG_BELOW_HORIZON), has been reconciled in this process and is in the target
	// set. The on-disk State alone says none of the last three.
	Serves(slot uint16, db uint64) bool
}

// ReadGateConfig is the collaborators of a ReadGate.
type ReadGateConfig struct {
	// Topology is the committed cluster state.
	Topology Topology
	// Pairs is the owner's pair state, the same set the Writer holds.
	Pairs *cluster.PairSet
	// Clock is the boot clock the lease re-check reads.
	Clock machine.Clock
	// Lease is the node's lease (conv §15).
	Lease ServeChecker
	// Schema is the declaration cache (conv §20).
	Schema Schema
	// Slots is the pair records' state, so a pair that is seeding or retired serves nothing.
	Slots SlotStates
	// Settled reports whether the Handoff has been fed the given committed epoch (BUG-0758).
	// A rank-1 pair is held while it is false, whatever flags it carries. Nil is always settled.
	Settled func(committed uint64) bool
	// Undecided says, after a restart and before the node knows which pairs it owns, what it is
	// waiting for ("" when decided or never restarted). Nil is always decided.
	Undecided func() string
}

// ReadGate is the admission of a read: conv §8 checks 1, 2, 3, 3a and 4 through
// machine.Check, the one re-admission of check 4, the pair-record state, and the
// lease re-check of §15. It takes no position and no pair lock, so a read never
// delays a write. The keyed Reader and the SLOT_LIST/SLOT_COUNT scanner share it,
// so the two read paths cannot disagree about when a pair may be read.
type ReadGate struct{ cfg ReadGateConfig }

// NewReadGate returns a gate over cfg.
func NewReadGate(cfg ReadGateConfig) *ReadGate { return &ReadGate{cfg: cfg} }

// ReadRequest is one keyed read reaching a node.
type ReadRequest struct {
	// Slot is the slot of the document's placement key.
	Slot uint16
	// DBID and BucketID name the bucket.
	DBID, BucketID uint64
	// EncID is the document's encoded _id_ (placement §3).
	EncID []byte
	// ID is the id as the caller spelled it, for the refusal texts; empty prints the encoding.
	ID string
	// RequestID is the client's request id, kept when the read is held or forwarded.
	RequestID [16]byte
	// ClientEpoch is the epoch a client prologue carried, 0 for "route it for me".
	ClientEpoch uint64
	// FromNode marks a node frame, whose FrameEpoch check 1 compares instead.
	FromNode bool
	// FrameEpoch is the epoch a node frame carried.
	FrameEpoch uint64
	// Hop is the forward count of check 3: 1 means the read was already rerouted once.
	Hop uint8
	// Local lets a copy answer: the caller accepts the replication lag of this node's copy
	// (the per-call form of cluster.stale_reads).
	Local bool
	// Fields projects the body: nil returns every declared field; a non-nil slice (empty
	// allowed) is the scalar field ids to return besides _id_, ascending, and no store
	// field is read.
	Fields []uint16
}

// ReadVerdict is what the gate decided for a read it did not refuse.
type ReadVerdict struct {
	// Held says the pair is in a hold: nothing may be answered, and the caller parks the read.
	Held bool
	// HoldCause says why, for the refusal when nothing is left to wait for.
	HoldCause string
	// Forward says this node is not the owner: the caller reroutes the read once, to Owner.
	Forward bool
	// Owner is the node that owns the slot at the committed epoch, empty when none does.
	Owner string
	// Decl is the declaration the read was admitted with.
	Decl machine.Declaration
	// Table is its effective field table at the published version.
	Table machine.FieldTable
	// Expire is its effective expiry declaration at the published version.
	Expire machine.ExpireTable
	// Version is the effective declaration version.
	Version int64
}

// Answerable reports a verdict that lets this node answer from its own store.
func (v ReadVerdict) Answerable() bool { return !v.Held && !v.Forward }

// leaseDecision is one serve check; a refusal that names the lapse is given this decision.
func (g *ReadGate) leaseDecision() machine.LeaseDecision {
	return g.cfg.Lease.Serve(machine.ServeCheck{At: g.cfg.Clock.BootNow(), CommittedEpoch: g.cfg.Topology.Committed()})
}

func (g *ReadGate) owner(slot uint16, rf int) (cluster.Member, bool) {
	return g.cfg.Topology.Table().Owner(slot, rf)
}

// readBudget is the one re-admission conv §8 check 4 allows a read, spent by whichever
// admission or confirmation needs it first. It lives as long as the read does.
type readBudget struct{ refetched bool }

// evaluation is one run of machine.Check with what the verdict needs of its inputs.
type evaluation struct {
	decision machine.AdmissionResult
	view     SchemaView
	table    machine.FieldTable
	owner    string
	cause    string
	// lease is the serve check the admission was judged on.
	lease machine.LeaseDecision
}

// holdCause says why a pair this node would read is held, in words a developer can act
// on. It is empty when no hold applies.
func (g *ReadGate) holdCause(pair cluster.Pair, rank1, settled bool, req ReadRequest) string {
	switch {
	case pair.Flags&flagAwaiting != 0:
		return awaitingCause(pair)
	case pair.Flags&flagSending != 0:
		return handoffHoldCause
	case !rank1:
		return ""
	case !settled:
		// The committed epoch may have just made this node the owner and the Handoff has not
		// opened its barrier yet; flags set earlier, as a copy, prove nothing (BUG-0758).
		return "this node has not yet applied the latest membership change"
	case pair.Flags&flagBootReplay != 0:
		return bootReplayHoldCause
	case pair.Flags&flagReplayNeeded != 0:
		return "its log has to be replayed after a failed apply"
	case pair.Flags&flagReplayed == 0:
		if cause := g.undecidedCause(); cause != "" {
			return cause
		}
		return "its log tail has not been replayed since the restart"
	case pair.Flags&flagStopProved == 0:
		return "the previous owner has not yet been proved stopped"
	case pair.AppliedPos.Less(g.cfg.Topology.Target(req.Slot, req.DBID)):
		return "this node is catching up to the highest position the old copies report"
	}
	// A pair this node holds no record of (SlotAbsent) serves: it has only ever been
	// written here by its own owner path, and the flags above, not the record, say
	// whether it is current.
	switch g.cfg.Slots.State(req.Slot, req.DBID) {
	case machine.SlotSeeding:
		return "this node's copy is still seeding"
	case machine.SlotRetired:
		return "this node's copy is retired"
	case machine.SlotRetiring:
		return "this node's copy is retiring"
	}
	return ""
}

// undecidedCause is the restart's undecided window while the node is in it, "" otherwise.
func (g *ReadGate) undecidedCause() string {
	if g.cfg.Undecided == nil {
		return ""
	}
	return g.cfg.Undecided()
}

// evaluate fills machine.Admission for one read and runs machine.Check on it. The
// declaration is read first and ownership, the pair, the hold and the lease last, so
// the decision reflects the newest state the node has at the moment it is made.
func (g *ReadGate) evaluate(req ReadRequest, dbRow DatabaseInfo, readmitted bool) evaluation {
	return g.evaluateAs(req, dbRow, readmitted, false)
}

// evaluateAs is evaluate for an owner (asCopy false) or for a ready copy answering as one.
func (g *ReadGate) evaluateAs(req ReadRequest, dbRow DatabaseInfo, readmitted, asCopy bool) evaluation {
	view := g.cfg.Schema.View(req.DBID, req.BucketID)
	table, version := machine.EffectiveTable(view.Decl, view.Published)
	owner, held := g.owner(req.Slot, dbRow.RF)
	rank1 := held && owner.ID == g.cfg.Topology.Self()
	if asCopy {
		// A copy answers as if it held the slot: CopyVerdict has already shown this node is in
		// the copy set and its pair record is ready, and the hold flags below are read as a
		// copy's (rank1 false), so a seeding or sending pair still refuses.
		rank1 = true
	}
	// Order matters (BUG-0758). Committed is read after the table, so an epoch in between
	// only holds longer. Settled is read before the pair: OnEpoch stores the applied epoch
	// only after it fed every pair, so a true answer means every feed that epoch makes was
	// published before the snapshot below, which therefore holds its barrier flags. Reading
	// the pair first would let a feed land between the two and admit a stale pair.
	settled := g.cfg.Settled == nil || g.cfg.Settled(g.cfg.Topology.Committed())
	pair := g.cfg.Pairs.ReadView(req.Slot, req.DBID)
	cause := g.holdCause(pair, rank1 && !asCopy, settled, req)
	if cause == "" && asCopy && pair.Flags&(flagReplayNeeded|flagBootReplay) != 0 {
		// A copy whose apply failed, or that has not replayed its log since a restart, is
		// behind by an unbounded amount: "no read, no ordered write, no copy apply".
		cause = "its log has to be replayed before this copy answers"
	}
	lease := g.leaseDecision()
	admission := machine.Admission{
		FromNode:        req.FromNode,
		FrameEpoch:      req.FrameEpoch,
		Committed:       g.cfg.Topology.Committed(),
		ClientEpoch:     req.ClientEpoch,
		Hold:            cause != "",
		Rank1:           rank1,
		LeaseValid:      lease.Valid,
		Lease:           lease,
		QuorumLost:      g.quorumLost(),
		Members:         g.cfg.Topology.Members(),
		Present:         view.Present,
		Missing:         view.Missing,
		Decl:            view.Decl,
		Seen:            view.Seen,
		Published:       view.Published,
		ReAdmitted:      readmitted,
		PayloadBucketID: req.BucketID,
		AdmittedVersion: version,
		Hop:             req.Hop,
	}
	decision := machine.Check(admission)
	if decision.MarkBehind {
		markBehind(g.cfg.Lease, seenEpoch(req.FromNode, req.FrameEpoch, req.ClientEpoch), !req.FromNode)
	}
	return evaluation{decision: decision, view: view, table: table, owner: owner.ID, cause: cause, lease: lease}
}

// Admit is checks 1 to 4 for a read, with the one transparent re-admission of check 4:
// a bump that left this bucket alone is refetched once and the read goes on, and a
// second miss is the refusal. A refusal is a *cluster.Error; a read the node may not
// answer yet is a verdict, not an error.
func (g *ReadGate) Admit(ctx context.Context, req ReadRequest) (ReadVerdict, error) {
	return g.admit(ctx, req, &readBudget{})
}

// admit is Admit drawing on a budget the caller keeps for the whole read.
func (g *ReadGate) admit(ctx context.Context, req ReadRequest, budget *readBudget) (ReadVerdict, error) {
	dbRow, ok := g.cfg.Topology.Database(req.DBID)
	if !ok {
		return ReadVerdict{}, g.refusal(cluster.CodeNotFound, req)
	}
	run := g.evaluate(req, dbRow, budget.refetched)
	if run.decision.ReAdmit {
		budget.refetched = true
		if err := g.cfg.Schema.Refetch(ctx, req.DBID, req.BucketID); err != nil {
			return ReadVerdict{}, g.refusal(refetchCode(err), req)
		}
		run = g.evaluate(req, dbRow, true)
	}
	return g.verdict(req, run)
}

// CopyVerdict decides whether this node may answer req from its own copy: it is in the
// slot's copy set but is not the owner, the machine's own serve rule holds for its pair (ready,
// not asked to rebuild, reconciled in this process, in the target set: SlotStates.Serves), the
// pair's hold flags are clear as a copy's, it has no failed apply or boot replay to finish, and the
// lease and declaration checks of Admit pass. It returns
// the verdict Admit would have given an owner, and ok true; ok false means the read must go
// to the owner. It never answers a pair that is seeding, retired, retiring, held or on a
// declaration it has to refetch, and it takes no pair lock and no position. A forwarded read
// (req.FromNode) is never answered by a copy: the sender asked the owner by name.
func (g *ReadGate) CopyVerdict(req ReadRequest) (ReadVerdict, bool) {
	if req.FromNode {
		return ReadVerdict{}, false
	}
	dbRow, ok := g.cfg.Topology.Database(req.DBID)
	if !ok || !g.holdsCopy(req.Slot, dbRow.RF) {
		return ReadVerdict{}, false
	}
	if !g.cfg.Slots.Serves(req.Slot, req.DBID) {
		return ReadVerdict{}, false
	}
	run := g.evaluateAs(req, dbRow, false, true)
	if run.decision.ReAdmit {
		return ReadVerdict{}, false
	}
	verdict, err := g.verdict(req, run)
	if err != nil || !verdict.Answerable() {
		return ReadVerdict{}, false
	}
	return verdict, true
}

// holdsCopy reports that this node is in the copy set of slot and is not its owner.
func (g *ReadGate) holdsCopy(slot uint16, rf int) bool {
	self := g.cfg.Topology.Self()
	if owner, held := g.owner(slot, rf); held && owner.ID == self {
		return false
	}
	for _, member := range g.cfg.Topology.Table().Copies(slot, rf) {
		if member.ID == self {
			return true
		}
	}
	return false
}

func (g *ReadGate) verdict(req ReadRequest, run evaluation) (ReadVerdict, error) {
	switch {
	case run.decision.Code != 0:
		return ReadVerdict{}, g.refusalFor(run.decision.Code, req, run.lease)
	case run.decision.Hold:
		return ReadVerdict{Held: true, HoldCause: run.cause}, nil
	case run.decision.Forward:
		return ReadVerdict{Forward: true, Owner: run.owner}, nil
	}
	return ReadVerdict{Decl: run.view.Decl, Table: run.table, Expire: machine.EffectiveExpire(run.view.Decl, run.view.Published), Version: run.decision.EffectiveVersion}, nil
}

// holdRefusal is the answer for a read whose pair is still held after the Holder had
// nothing more to wait for: retryable, naming the cause, and not NOT_OWNER, because
// this node is the owner and no barrier is left to end.
func (g *ReadGate) holdRefusal(req ReadRequest, cause string) error {
	if cause == restartHoldCause {
		return &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
			"slot %d of %s cannot be read on this node yet: %s. Nothing was returned. Retry in a few seconds",
			req.Slot, g.where(req), cause)}
	}
	if strings.HasPrefix(cause, restartUndecidedPrefix) {
		return &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
			"slot %d of %s cannot be read on this node yet: %s. Nothing was returned. Retry in a few seconds; if it persists, 'nodes' shows whether this node is up and has joined the cluster",
			req.Slot, g.where(req), cause)}
	}
	if cause == bootReplayHoldCause {
		return &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
			"slot %d of %s cannot be read on this node yet: %s. Nothing was returned. Retry in a few seconds; 'nodes' shows whether the other nodes are up, which the replay needs",
			req.Slot, g.where(req), cause)}
	}
	return &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
		"slot %d of %s cannot be read on this node yet: %s. Nothing was returned. Retry in a few seconds; 'nodes' shows whether the previous owner is up and 'status seeding' shows this node's pulls",
		req.Slot, g.where(req), cause)}
}

// AdmitMask admits a scan of every slot in mask as a read of each: the owner of the
// slot, no hold, the pair readable, the bucket not dropping, a valid lease. A scan
// never reroutes and never waits. A slot another node owns is NOT_OWNER, the code the
// coordinator already follows to the current owner; a slot this node owns but holds is
// a retryable UNAVAILABLE with the cause, because the owner is this node and sending
// the caller to it again would loop (BUG-0764).
func (g *ReadGate) AdmitMask(ctx context.Context, db, bucketID uint64, mask *cluster.SlotMask) error {
	for slot := uint16(0); slot < storage.SlotCount; slot++ {
		if !mask.Has(slot) {
			continue
		}
		req := ReadRequest{Slot: slot, DBID: db, BucketID: bucketID, ID: "(any document of the slot)", Hop: 1}
		verdict, err := g.Admit(ctx, req)
		if err != nil {
			return err
		}
		switch {
		case verdict.Held:
			return g.holdRefusal(req, verdict.HoldCause)
		case verdict.Forward:
			return g.refusal(cluster.CodeNotOwner, req)
		}
	}
	return nil
}

// idText is the id as the refusal texts spell it.
func (r ReadRequest) idText() string {
	if r.ID != "" {
		return r.ID
	}
	return "0x" + hex.EncodeToString(r.EncID)
}

func (g *ReadGate) refusalArgs(req ReadRequest) map[string]string {
	args := g.idArgs(req)
	g.nameArgs(req, args)
	return args
}

// idArgs is the refusal arguments the gate holds as ids, with the owner when there is one.
func (g *ReadGate) idArgs(req ReadRequest) map[string]string {
	committed := strconv.FormatUint(g.cfg.Topology.Committed(), 10)
	args := map[string]string{
		"node": g.cfg.Topology.Self(), "slot": strconv.FormatUint(uint64(req.Slot), 10),
		"db": strconv.FormatUint(req.DBID, 10), "bucket": strconv.FormatUint(req.BucketID, 10),
		"e": committed, "epoch": committed, "id": req.idText(),
		"e_seen": strconv.FormatUint(req.ClientEpoch, 10),
	}
	if dbRow, ok := g.cfg.Topology.Database(req.DBID); ok {
		if owner, held := g.owner(req.Slot, dbRow.RF); held {
			args["owner"] = owner.ID
		}
	}
	return args
}

// nameArgs replaces the database and bucket ids in args with their names (BUG-0759): the gate
// holds ids, a developer reads names. An id stays where the row or the declaration cache no
// longer has the name, as the owner-fence text does.
func (g *ReadGate) nameArgs(req ReadRequest, args map[string]string) {
	if dbRow, ok := g.cfg.Topology.Database(req.DBID); ok && dbRow.Name != "" {
		args["db"] = dbRow.Name
	}
	if name := g.cfg.Schema.View(req.DBID, req.BucketID).Name; name != "" {
		args["bucket"] = name
	}
}

// where is the bucket a read refusal speaks of: "app.users" when the gate knows both names,
// otherwise the ids it holds.
func (g *ReadGate) where(req ReadRequest) string {
	args := g.refusalArgs(req)
	if args["db"] == strconv.FormatUint(req.DBID, 10) || args["bucket"] == strconv.FormatUint(req.BucketID, 10) {
		return "database " + strconv.FormatUint(req.DBID, 10)
	}
	return args["db"] + "." + args["bucket"]
}

// refusal is the coded error of a read that was not answered. A placeholder this path
// cannot fill stays visible in the text rather than hiding.
func (g *ReadGate) refusal(code cluster.Code, req ReadRequest) error {
	return g.refusalFor(code, req, machine.LeaseDecision{})
}

// refusalFor is refusal for a read refused on lease: LEASE_LAPSED prints the figures of the lease
// decision the read was refused on, never those of a second serve check, which could race a renewal.
func (g *ReadGate) refusalFor(code cluster.Code, req ReadRequest, lease machine.LeaseDecision) error {
	code, seen := behindRefusal(g.cfg.Lease, code, g.cfg.Topology.Committed(), req.FromNode)
	// The schema fence names the bucket from its own read of the declaration, so it takes ids.
	args := g.idArgs(req)
	if code != cluster.CodeSchemaNotCurrent {
		g.nameArgs(req, args)
	}
	if _, named := args["owner"]; code == cluster.CodeNotOwner && !named {
		// Nobody owns the slot, so there is no node to send the caller to.
		return &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
			"every copy of slot %d of %s is down, so no node owns it. Nothing was returned. Run 'nodes' to see which members are down", req.Slot, g.where(req))}
	}
	if seen != 0 {
		args["e_seen"], args["kind"] = strconv.FormatUint(seen, 10), "request"
	}
	condition := refusalConditions[code]
	if code == cluster.CodeSchemaNotCurrent {
		condition = fillSchemaNotCurrent(args, g.cfg.Schema, g.cfg.Topology, req.DBID, req.BucketID)
	}
	args["v"] = strconv.Itoa(g.cfg.Topology.Members())
	if code == cluster.CodeLeaseLapsed {
		fillLeaseLapsed(args, lease, g.cfg.Clock.BootNow(), g.cfg.Topology)
	}
	return &cluster.Error{Code: code, Message: code.Text(condition, args)}
}

func (g *ReadGate) quorumLost() bool {
	reporter, ok := g.cfg.Lease.(QuorumReporter)
	return ok && reporter.QuorumLost()
}
