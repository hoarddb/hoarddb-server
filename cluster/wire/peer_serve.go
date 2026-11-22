package wire

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// serveControl answers the control channel: lease requests, probes, failure reports and
// cluster notices.
func (n *Node) serveControl(ctx context.Context, from transport.Handshake, req transport.Message) (*transport.Message, error) {
	if n.cl == nil || !n.cl.serving.Load() {
		return nil, errStarting
	}
	return n.cl.ctrl.Handle(ctx, from, req)
}

// serveData answers the data channel: the replication stream, catch-up, the handoff
// messages, the position query and the two forwards. The sender's id is the handshake's,
// never a field of the message.
func (n *Node) serveData(ctx context.Context, from transport.Handshake, req transport.Message) (*transport.Message, error) {
	if n.cl == nil || !n.cl.serving.Load() {
		return nil, errStarting
	}
	env := req.Env
	switch env.Kind {
	case 0x01:
		return n.serveWrite(from, env, req.Body)
	case 0x02:
		ack, err := DecodeWriteAckBody(req.Body)
		if err != nil {
			return nil, err
		}
		ack.Slot, ack.DB, ack.From = env.Slot, env.DBID, from.NodeID
		if !slices.Contains(n.copiesOf(env.Slot, env.DBID), from.NodeID) {
			return nil, &cluster.Error{Code: cluster.CodeNotOwner, Message: fmt.Sprintf(
				"refused a WRITE_ACK from %s for slot %d: %s is not a copy of that pair at the committed epoch. Nothing was counted", from.NodeID, env.Slot, from.NodeID)}
		}
		n.cl.repl.OnWriteAck(ack)
		return nil, nil
	case transport.KindWriteAckBatch:
		acks, err := DecodeWriteAckBatchBody(req.Body)
		if err != nil {
			return nil, err
		}
		isCopy := func(slot uint16, db uint64, node string) bool { return slices.Contains(n.copiesOf(slot, db), node) }
		return nil, n.cl.repl.OnWriteAckBatch(from.NodeID, acks, isCopy)
	case transport.KindWriteBatch:
		entries, err := DecodeWriteBatchBody(req.Body)
		if err != nil {
			return nil, err
		}
		owns := func(slot uint16, db uint64) bool {
			owner, known := n.ownerOf(slot, db)
			return known && owner == from.NodeID
		}
		n.cl.repl.OnWriteBatch(from.NodeID, env.Epoch, entries, owns)
		return nil, nil
	case 0x03:
		return n.serveCatchUp(ctx, from, env, req.Body)
	case kindHandoffReq:
		msg, err := DecodeHandoffReq(req.Body)
		if err != nil {
			return nil, err
		}
		return nil, n.handoff.OnHandoffReq(from.NodeID, env, msg)
	case transport.KindHandoffStopped:
		msg, err := DecodeHandoffStopped(req.Body)
		if err != nil {
			return nil, err
		}
		return nil, n.handoff.OnHandoffStopped(from.NodeID, env, msg)
	case kindHandoffReady:
		msg, err := DecodeHandoffReady(req.Body)
		if err != nil {
			return nil, err
		}
		return nil, n.handoff.OnHandoffReady(from.NodeID, env, msg)
	case kindPositionQuery:
		return n.servePosition(env, req.Body)
	case transport.KindForwardWrite:
		return n.serveForwardWrite(ctx, env, req.Body)
	case transport.KindForwardRead:
		return n.serveForwardRead(ctx, env, req.Body)
	}
	return nil, fmt.Errorf("%w: the data channel of this node does not serve kind 0x%02x", transport.ErrInvalidKind, env.Kind)
}

func (n *Node) reply(kind byte, env transport.Envelope, body []byte) *transport.Message {
	return &transport.Message{Env: transport.Envelope{
		Version: transport.EnvelopeVersion, Kind: kind, Epoch: uint64(n.topology.Committed()), StateSeq: uint64(n.raft.StateSeq()), Slot: env.Slot, DBID: env.DBID,
	}, Body: body}
}

func (n *Node) serveWrite(from transport.Handshake, env transport.Envelope, body []byte) (*transport.Message, error) {
	rec, err := DecodeWriteBody(body)
	if err != nil {
		return nil, err
	}
	if owner, known := n.ownerOf(env.Slot, env.DBID); !known || owner != from.NodeID {
		return nil, &cluster.Error{Code: cluster.CodeNotOwner, Message: fmt.Sprintf(
			"refused a WRITE from %s for slot %d: %s is not the owner of that pair at the committed epoch (the owner is %q). Nothing was applied", from.NodeID, env.Slot, from.NodeID, owner)}
	}
	adm, err := n.cl.repl.AdmitWrite(WriteFrame{Slot: env.Slot, DB: env.DBID, From: from.NodeID, Epoch: env.Epoch, PairRecord: rec})
	if err != nil {
		return nil, err
	}
	ack := adm.ack
	if adm.Admitted() {
		var queued bool
		if ack, queued = n.cl.repl.EnqueueWrite(adm); queued {
			// An empty reply is the receipt. The acknowledgement follows from the apply worker,
			// after the record is fsynced and applied, on the reverse path.
			return nil, nil
		}
	}
	return n.reply(0x02, env, EncodeWriteAckBody(ack)), nil
}

func (n *Node) serveCatchUp(ctx context.Context, from transport.Handshake, env transport.Envelope, body []byte) (*transport.Message, error) {
	req, err := DecodeCatchUpReqBody(body)
	if err != nil {
		return nil, err
	}
	req.Slot, req.DB, req.Node, req.Epoch = env.Slot, env.DBID, from.NodeID, env.Epoch
	if !n.mayCatchUp(env.Slot, env.DBID, from.NodeID) {
		// One text for every caller that is not a copy, so the refusal says nothing about who
		// holds what; a copy racing an epoch change retries on it.
		return nil, &cluster.Error{Code: cluster.CodeEpochFenced, Message: cluster.CodeEpochFenced.Text(cluster.CondEpochFenced, map[string]string{
			"kind": "CATCHUP_REQ", "node": from.NodeID, "e": strconv.FormatUint(env.Epoch, 10), "e_seen": strconv.FormatUint(uint64(n.topology.Committed()), 10),
		})}
	}
	chunk, err := n.answerCatchUp(ctx, req)
	if err != nil {
		return nil, err
	}
	raw, err := EncodeCatchUpChunkBody(chunk)
	if err != nil {
		return nil, err
	}
	return n.reply(0x04, env, raw), nil
}

// answerCatchUp serves the requester of a live seed pin as the pin's closing round and every
// other copy as a plain catch-up (see Replicator.ServePinnedCatchUp).
func (n *Node) answerCatchUp(ctx context.Context, req CatchUpRequest) (CatchUpChunk, error) {
	if n.cl.seed != nil && n.cl.seed.Holds(req.Slot, req.DB, req.Node) {
		n.cl.seed.touch(req.Slot, req.DB, req.Node)
		return n.cl.repl.ServePinnedCatchUp(ctx, req)
	}
	return n.cl.repl.ServeCatchUp(ctx, req)
}

func (n *Node) servePosition(env transport.Envelope, body []byte) (*transport.Message, error) {
	query, err := DecodePositionQuery(body)
	if err != nil {
		return nil, err
	}
	if n.pairDeferred(query.Slot, query.DB) {
		// Before the position is read, and an error, never a state: the pair's log holds
		// acknowledged records above its applied position, and a "not ready" reply would let
		// Handoff.Target account for this copy and open the pair without it.
		return nil, pairReplayingError(n.cfg.NodeID, query.Slot, query.DB)
	}
	pair := n.pairs.Snapshot(query.Slot, query.DB)
	raw, err := EncodePositionReply(PositionReply{Slot: query.Slot, DB: query.DB, State: positionState(n.cl.seeding.Load().slotsView().State(query.Slot, query.DB)), AppliedPos: pair.AppliedPos})
	if err != nil {
		return nil, err
	}
	return n.reply(kindPositionReply, env, raw), nil
}

// forwardCatchUpWait caps how long a forwarded frame about an unknown database waits for this
// node to apply the state_seq its sender had applied. A create database followed at once by a create bucket reaches an owner
// that is a Raft commit behind (tens of milliseconds); two seconds covers that with room, and a
// node that stays behind longer is not worth parking the sender's request for.
const forwardCatchUpWait = 2 * time.Second

// awaitSenderState waits until this node has applied the state_seq the frame's sender had applied,
// and reports true when it is still behind at the end: the cap, the request's context or this
// node's shutdown came first. The common case, no gap, is one read of the applied state_seq.
func (n *Node) awaitSenderState(ctx context.Context, env transport.Envelope) (behind bool) {
	if uint64(n.raft.StateSeq()) >= env.StateSeq {
		return false
	}
	wait := forwardCatchUpWait
	if n.cl.forwardCatchUpCap > 0 {
		wait = n.cl.forwardCatchUpCap
	}
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	poll := time.NewTicker(2 * time.Millisecond)
	defer poll.Stop()
	for uint64(n.raft.StateSeq()) < env.StateSeq {
		select {
		case <-ctx.Done():
			return true
		case <-n.cl.ctx.Done():
			return true
		case <-deadline.C:
			return true
		case <-poll.C:
		}
	}
	return false
}

// notAppliedYet is the refusal for a forwarded frame about a database this node does not know while
// it is still behind its sender: the database is most likely one created a moment ago. The frame
// carries the database's id, not its name (the name is in the row this node has not applied), so the
// text names the id. A ring client sees this text alone: ringRefusalOf keeps the first
// *cluster.Error's message and drops the "creating bucket ... in database ..." wrapper the forwarding
// node adds, so the words must stand without names (the createRun wrapping of BUG-0763's proposed
// fix 3 is still open). The advice is to run the statement again, the only thing a create bucket
// user can do. "Nothing was written" is claimed for this frame only: a create bucket writes its
// reverse index first, so the statement may have partly run, and running it again finishes it.
func (n *Node) notAppliedYet(env transport.Envelope, what string) *cluster.Error {
	return &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
		"database %d is not known on %s yet: this node has applied state_seq %d and the node that forwarded the %s had applied %d, so the %s to slot %d was not served, and this frame wrote nothing. The database was most likely created a moment ago: run the statement again in a moment. A statement that may have partly run, such as create bucket, finishes when it is run again",
		env.DBID, n.cfg.NodeID, n.raft.StateSeq(), what, env.StateSeq, what, env.Slot)}
}

// refusedUntilApplied reports the refusal to send when the database the frame is about is unknown
// on this node while the frame's sender is ahead of it and still is after the wait, and nil
// otherwise. Only an unknown database waits: a known one is judged by the owner path at once, so an
// owner that stays behind (a slow apply, a partition that left it a lease) does not park every
// forwarded request of every sender for the cap. A frame for a known database that an epoch move
// has outrun is already fenced by its epoch.
func (n *Node) refusedUntilApplied(ctx context.Context, env transport.Envelope, what string) *cluster.Error {
	if _, known := n.topology.Database(env.DBID); known {
		return nil
	}
	if !n.awaitSenderState(ctx, env) {
		return nil
	}
	if _, known := n.topology.Database(env.DBID); known {
		return nil
	}
	return n.notAppliedYet(env, what)
}

func (n *Node) serveForwardWrite(ctx context.Context, env transport.Envelope, body []byte) (*transport.Message, error) {
	req, err := DecodeForwardWriteBody(body)
	if err != nil {
		return nil, err
	}
	if refusal := n.refusedUntilApplied(ctx, env, "write"); refusal != nil {
		return n.reply(transport.KindForwardWriteReply, env, EncodeForwardRefusal(refusal)), nil
	}
	req.Slot, req.DBID, req.FromNode, req.FrameEpoch = env.Slot, env.DBID, true, env.Epoch
	answer, err := nodeSender{node: n}.Write(ctx, req)
	if err == nil && (answer.Held || answer.Forward) {
		err = &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf("the forwarded write to slot %d was not ordered on %s because %s. Nothing was written. Retry it in a few seconds: a retry with the same request id is safe", env.Slot, n.cfg.NodeID, answer.notOrderedBecause())}
	}
	if err == nil && answer.ApplyFailure != nil {
		// The reply codec has no field for the failure, and the ordered result is nil after one: sent as
		// an answer, the forwarding node would report applied, with an empty document for a Return. The
		// refusal form carries the code and words, and a retry with the same request id is deduped.
		database, _ := n.topology.DatabaseName(env.DBID)
		err = &cluster.Error{Code: cluster.CodeUnavailable, Message: fmt.Sprintf(
			"the forwarded write to slot %d of database %q is durable at %s on %s but applying it failed, so it has no result. Retry it with the same request id to read its answer once the pair has recovered",
			env.Slot, database, answer.Pos, n.cfg.NodeID)}
	}
	var refusal *cluster.Error
	if errors.As(err, &refusal) {
		return n.reply(transport.KindForwardWriteReply, env, EncodeForwardRefusal(refusal)), nil
	}
	if err != nil {
		return nil, err
	}
	return n.reply(transport.KindForwardWriteReply, env, EncodeForwardWriteAnswer(answer)), nil
}

func (n *Node) serveForwardRead(ctx context.Context, env transport.Envelope, body []byte) (*transport.Message, error) {
	msg, err := DecodeForwardReadBody(body)
	if err != nil {
		return nil, err
	}
	msg.Req.Slot, msg.Req.DBID, msg.Req.FromNode, msg.Req.FrameEpoch = env.Slot, env.DBID, true, env.Epoch
	if refusal := n.refusedUntilApplied(ctx, env, "read"); refusal != nil {
		return n.reply(transport.KindForwardReadReply, env, EncodeForwardRefusal(refusal)), nil
	}
	if msg.Stored {
		return n.serveStoredRead(env, msg.Req)
	}
	if msg.Field != nil {
		return n.serveForwardField(ctx, env, *msg.Field)
	}
	// A peer's read may be the first touch of the pair since a restart: it opens the restart
	// hold like a local one (BUG-0743), or the pair would stay unmarked until a local read.
	n.own.Acquire(env.Slot, env.DBID)
	doc, err := n.reader.Read(ctx, msg.Req)
	var refusal *cluster.Error
	if errors.As(err, &refusal) {
		return n.reply(transport.KindForwardReadReply, env, EncodeForwardRefusal(refusal)), nil
	}
	if err != nil {
		return nil, err
	}
	raw, err := EncodeForwardReadDocument(doc)
	if err != nil {
		return nil, err
	}
	return n.reply(transport.KindForwardReadReply, env, raw), nil
}

// serveStoredRead answers the declaration read of conventions 20: one seek, no position,
// and only of the _buckets bucket.
func (n *Node) serveStoredRead(env transport.Envelope, req ReadRequest) (*transport.Message, error) {
	if req.BucketID != store.BucketsBucketID {
		return nil, &cluster.Error{Code: cluster.CodeInvalidArgument, Message: "a stored read serves the _buckets bucket only. Read other documents with a keyed get"}
	}
	snapshot := n.kv.NewSnapshot()
	defer snapshot.Discard()
	doc, found, err := readStoredDocument(snapshot, req, n.ringAdvice())
	if err != nil {
		return nil, err
	}
	stored := make(map[uint16][]byte, len(doc.fields))
	for _, field := range doc.fields {
		stored[field.id] = field.stored
	}
	raw, err := EncodeForwardReadStored(stored, found)
	if err != nil {
		return nil, err
	}
	return n.reply(transport.KindForwardReadReply, env, raw), nil
}

// mayCatchUp is the CATCHUP_REQ role check: id must be a copy of the pair at the committed
// epoch or the one before, or the registered receiver of a live seed pin for the pair. The
// replication factor is the database's current one for both.
func (n *Node) mayCatchUp(slot uint16, db uint64, id string) bool {
	info, ok := n.topology.Database(db)
	if !ok {
		return false
	}
	if catchUpEligible(n.topology.Table(), n.handoff.TableBefore(), info.RF, info.RF, slot, id) {
		return true
	}
	// A joining gainer is in no copy set, and the closing round of its seed asks this source
	// for catch-up on the data channel: the registered receiver of a live pin for the pair is
	// served, and nobody else.
	return n.cl != nil && n.cl.seed != nil && n.cl.seed.Holds(slot, db, id)
}
