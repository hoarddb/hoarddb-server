package wire

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/peer"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// peerLinks is the real Link, handoff link, read forwarder and write forwarder of a
// networked node: each method is one request and one reply on a peer's data channel.
// Replication and handoff use the main lane; a forwarded client request uses the forward
// lane, so a long catch-up never queues a client write behind it.
type peerLinks struct {
	self  string
	net   *peer.Network
	hello func() transport.Handshake
	decl  func(db, bucket uint64) (machine.Declaration, bool)
	// onAck hands a WRITE_ACK that arrived as a reply to the replicator.
	onAck func(WriteAck)
}

var (
	_ Link           = (*peerLinks)(nil)
	_ AckBatcher     = (*peerLinks)(nil)
	_ WriteBatcher   = (*peerLinks)(nil)
	_ HandoffLink    = (*peerLinks)(nil)
	_ ReadForwarder  = (*peerLinks)(nil)
	_ WriteForwarder = (*peerLinks)(nil)
)

// WriteForwarder sends a write this node may not order to the owner it names.
type WriteForwarder interface {
	// ForwardWrite sends req, whose Hop is already counted, to owner and returns its answer
	// or its refusal.
	ForwardWrite(ctx context.Context, owner string, req WriteRequest) (WriteAnswer, error)
}

func (l *peerLinks) envelope(kind byte, epoch uint64, slot uint16, db uint64) transport.Envelope {
	return transport.Envelope{Version: transport.EnvelopeVersion, Kind: kind, Epoch: epoch, StateSeq: l.hello().StateSeq, Slot: slot, DBID: db}
}

// linkCallTimeout bounds one replication, catch-up, handoff or declaration-read call, so a
// peer that is stopped with its sockets open costs one timeout and not the replicator's
// whole pass, and a declaration read that waits in vain reports itself in 2 s. A forwarded
// client request keeps the client's own deadline.
const linkCallTimeout = 2 * time.Second

func (l *peerLinks) call(ctx context.Context, target string, lane peer.Lane, env transport.Envelope, body []byte, wantKind byte) (*transport.Message, error) {
	return l.callOn(ctx, transport.ChannelData, target, lane, env, body, wantKind)
}

// callOn is call on channel ch: the data channel for replication and handoff, the seed
// channel for a pull.
func (l *peerLinks) callOn(ctx context.Context, ch transport.Channel, target string, lane peer.Lane, env transport.Envelope, body []byte, wantKind byte) (*transport.Message, error) {
	// Only the data channel's replication, handoff and declaration-read calls are bounded
	// here. A seed call has its own bound (dialSeed): the register's 2 s, the stream's
	// seed_idle_seconds.
	if (lane == peer.LaneMain || lane == peer.LaneDecl) && ch == transport.ChannelData {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, linkCallTimeout)
		defer cancel()
	}
	reply, err := l.net.Call(ctx, target, ch, lane, transport.Message{Env: env, Body: body})
	if err != nil {
		return nil, err
	}
	if wantKind == 0 {
		return reply, nil
	}
	if reply == nil || reply.Env.Kind != wantKind {
		got := byte(0)
		if reply != nil {
			got = reply.Env.Kind
		}
		return nil, fmt.Errorf("%s answered kind 0x%02x with kind 0x%02x, want 0x%02x", target, env.Kind, got, wantKind)
	}
	return reply, nil
}

// Write implements Link: the WRITE goes out and returns once the copy has answered. The
// answer is a receipt (the copy queued the frame) or, when the copy did not queue it, a
// WRITE_ACK that goes to the replicator. A receipt is not an acknowledgement: the durable
// WRITE_ACK arrives later on the copy's own call (Ack on the copy, case 0x02 here).
func (l *peerLinks) Write(ctx context.Context, target string, frame WriteFrame) error {
	reply, err := l.call(ctx, target, peer.LaneMain, l.envelope(0x01, frame.Epoch, frame.Slot, frame.DB), EncodeWriteBody(frame.PairRecord), 0)
	if err != nil {
		return fmt.Errorf("sending WRITE for slot %d to %s: %w", frame.Slot, target, err)
	}
	return l.readWriteReply(target, frame, reply)
}

// Ack implements Link.
func (l *peerLinks) Ack(ctx context.Context, target string, ack WriteAck) error {
	if _, err := l.call(ctx, target, peer.LaneMain, l.envelope(0x02, l.hello().Epoch, ack.Slot, ack.DB), EncodeWriteAckBody(ack), 0); err != nil {
		return fmt.Errorf("sending WRITE_ACK for slot %d to %s: %w", ack.Slot, target, err)
	}
	return nil
}

// AckBatch implements AckBatcher: one WRITE_ACK_BATCH call carries the acks of every pair in acks. The envelope carries this
// node's epoch and slot and database 0, because each entry names its own pair; the owner reads the sender from the handshake.
func (l *peerLinks) AckBatch(ctx context.Context, target string, acks []WriteAck) error {
	body, err := EncodeWriteAckBatchBody(acks)
	if err != nil {
		return fmt.Errorf("encoding a WRITE_ACK_BATCH of %d acks for %s: %w", len(acks), target, err)
	}
	if _, err := l.call(ctx, target, peer.LaneMain, l.envelope(transport.KindWriteAckBatch, l.hello().Epoch, 0, 0), body, 0); err != nil {
		return fmt.Errorf("sending WRITE_ACK_BATCH of %d acks to %s: %w", len(acks), target, err)
	}
	return nil
}

// WriteBatch implements WriteBatcher: one WRITE_BATCH call carries the records of every frame, which all share frames[0]'s epoch. The
// envelope names no slot or database because each entry names its own pair; the owner is the handshake's sender. The reply must be
// the empty receipt: a copy answers an entry it did not queue on the ack path, never in this reply.
func (l *peerLinks) WriteBatch(ctx context.Context, target string, frames []WriteFrame) error {
	entries := make([]WriteEntry, 0, len(frames))
	for _, frame := range frames {
		entries = append(entries, WriteEntry{Slot: frame.Slot, DB: frame.DB, PairRecord: frame.PairRecord})
	}
	body, err := EncodeWriteBatchBody(entries)
	if err != nil {
		return fmt.Errorf("encoding a WRITE_BATCH of %d WRITEs for %s: %w", len(frames), target, err)
	}
	reply, err := l.call(ctx, target, peer.LaneMain, l.envelope(transport.KindWriteBatch, frames[0].Epoch, 0, 0), body, 0)
	if err != nil {
		return fmt.Errorf("sending %d WRITEs to %s: %w", len(frames), target, err)
	}
	if reply != nil {
		return fmt.Errorf("%s answered a WRITE_BATCH with kind 0x%02x, want an empty receipt", target, reply.Env.Kind)
	}
	return nil
}

// CatchUp implements Link.
func (l *peerLinks) CatchUp(ctx context.Context, source string, req CatchUpRequest) (CatchUpChunk, error) {
	reply, err := l.call(ctx, source, peer.LaneMain, l.envelope(0x03, req.Epoch, req.Slot, req.DB), EncodeCatchUpReqBody(req), 0x04)
	if err != nil {
		return CatchUpChunk{}, fmt.Errorf("asking %s for a catch-up of slot %d: %w", source, req.Slot, err)
	}
	return DecodeCatchUpChunkBody(reply.Body)
}

// SendHandoffReq implements HandoffLink.
func (l *peerLinks) SendHandoffReq(ctx context.Context, target string, db uint64, msg HandoffReq) error {
	body, err := EncodeHandoffReq(msg)
	if err != nil {
		return err
	}
	_, err = l.call(ctx, target, peer.LaneMain, l.envelope(kindHandoffReq, msg.ToEpoch, 0, db), body, 0)
	return err
}

// SendHandoffStopped implements HandoffLink.
func (l *peerLinks) SendHandoffStopped(ctx context.Context, target string, db uint64, msg HandoffStopped) error {
	body, err := EncodeHandoffStopped(msg)
	if err != nil {
		return err
	}
	_, err = l.call(ctx, target, peer.LaneMain, l.envelope(transport.KindHandoffStopped, msg.ToEpoch, 0, db), body, 0)
	return err
}

// SendHandoffReady implements HandoffLink.
func (l *peerLinks) SendHandoffReady(ctx context.Context, target string, db uint64, msg HandoffReady) error {
	body, err := EncodeHandoffReady(msg)
	if err != nil {
		return err
	}
	_, err = l.call(ctx, target, peer.LaneMain, l.envelope(kindHandoffReady, msg.ToEpoch, 0, db), body, 0)
	return err
}

// QueryPosition implements HandoffLink.
func (l *peerLinks) QueryPosition(ctx context.Context, target string, query PositionQuery) (PositionReply, error) {
	body, err := EncodePositionQuery(query)
	if err != nil {
		return PositionReply{}, err
	}
	reply, err := l.call(ctx, target, peer.LaneMain, l.envelope(kindPositionQuery, l.hello().Epoch, query.Slot, query.DB), body, kindPositionReply)
	if err != nil {
		return PositionReply{}, fmt.Errorf("asking %s for the position of slot %d: %w", target, query.Slot, err)
	}
	return DecodePositionReply(reply.Body)
}

// unreachableForward is the error a forwarded request returns when the owner could not be
// reached: the coded UNAVAILABLE a client reads, with the transport failure kept in the chain
// so errors.Is still sees peer.ErrBackoff and the dial error.
type unreachableForward struct {
	coded *cluster.Error
	cause error
}

func (e *unreachableForward) Error() string { return e.coded.Message }

// Unwrap lets errors.As find the coded error and errors.Is find the transport cause.
func (e *unreachableForward) Unwrap() []error { return []error{e.coded, e.cause} }

// unreachableOwner gives a transport failure of a forward the coded UNAVAILABLE text of cond.
// A failure the caller caused (its own context ended) and a refusal that already carries a
// code pass through unchanged: the first is not the owner's fault and the second already
// says what to do.
func unreachableOwner(ctx context.Context, cond cluster.Condition, owner string, cause error) error {
	var coded *cluster.Error
	if errors.As(cause, &coded) || ctx.Err() != nil {
		return cause
	}
	return &unreachableForward{
		coded: &cluster.Error{Code: cluster.CodeUnavailable, Message: cluster.CodeUnavailable.Text(cond, map[string]string{"owner": owner})},
		cause: cause,
	}
}

// ForwardRead implements ReadForwarder.
func (l *peerLinks) ForwardRead(ctx context.Context, owner string, req ReadRequest) (Document, error) {
	body, err := EncodeForwardReadBody(ForwardReadBody{Req: req})
	if err != nil {
		return Document{}, err
	}
	reply, err := l.call(ctx, owner, peer.LaneForward, l.envelope(transport.KindForwardRead, l.hello().Epoch, req.Slot, req.DBID), body, transport.KindForwardReadReply)
	if err != nil {
		return Document{}, unreachableOwner(ctx, cluster.CondForwardReadUnreachable, owner, fmt.Errorf("forwarding the read of %s to %s: %w", req.idText(), owner, err))
	}
	doc, err := DecodeForwardReadDocument(reply.Body)
	if err != nil {
		return Document{}, clientFacing(err, owner)
	}
	if decl, ok := l.decl(req.DBID, req.BucketID); ok {
		doc.Decl = decl
	}
	return doc, nil
}

// ForwardWrite implements WriteForwarder.
func (l *peerLinks) ForwardWrite(ctx context.Context, owner string, req WriteRequest) (WriteAnswer, error) {
	body, err := EncodeForwardWriteBody(req)
	if err != nil {
		return WriteAnswer{}, err
	}
	reply, err := l.call(ctx, owner, peer.LaneForward, l.envelope(transport.KindForwardWrite, l.hello().Epoch, req.Slot, req.DBID), body, transport.KindForwardWriteReply)
	if err != nil {
		return WriteAnswer{}, unreachableOwner(ctx, cluster.CondForwardWriteUnreachable, owner, fmt.Errorf("forwarding the write to %s: %w", owner, err))
	}
	answer, err := DecodeForwardWriteReply(reply.Body)
	return answer, clientFacing(err, owner)
}

// readStored asks owner for the stored fields of one document of the _buckets bucket.
func (l *peerLinks) readStored(ctx context.Context, owner string, read DeclarationRead) (map[uint16][]byte, bool, error) {
	body, err := EncodeForwardReadBody(ForwardReadBody{Stored: true, Req: ReadRequest{BucketID: store.BucketsBucketID, EncID: read.EncodedID}})
	if err != nil {
		return nil, false, err
	}
	reply, err := l.call(ctx, owner, peer.LaneDecl, l.envelope(transport.KindForwardRead, l.hello().Epoch, read.Slot, read.DBID), body, transport.KindForwardReadReply)
	if err != nil {
		return nil, false, fmt.Errorf("reading a declaration at %s: %w", owner, err)
	}
	return DecodeForwardReadStored(reply.Body)
}

// networkDeclarations reads a declaration from the node that owns its slot: this node's
// own Badger when that is itself, the owner's data channel otherwise.
type networkDeclarations struct {
	self  string
	local *LocalDeclarationReader
	links *peerLinks
}

var _ DeclarationReader = networkDeclarations{}

// ReadDeclaration implements DeclarationReader.
func (r networkDeclarations) ReadDeclaration(ctx context.Context, owner cluster.Member, read DeclarationRead) (map[uint16][]byte, bool, error) {
	if owner.ID == r.self {
		return r.local.ReadDeclaration(ctx, owner, read)
	}
	return r.links.readStored(ctx, owner.ID, read)
}

// ReadBucketName implements DeclarationReader.
func (r networkDeclarations) ReadBucketName(ctx context.Context, owner cluster.Member, read DeclarationRead) (string, bool, error) {
	if owner.ID == r.self {
		return r.local.ReadBucketName(ctx, owner, read)
	}
	stored, found, err := r.links.readStored(ctx, owner.ID, read)
	if err != nil || !found {
		return "", false, err
	}
	name, err := storedString(stored[createFieldName])
	if err != nil {
		return "", false, fmt.Errorf("reading the name of the reverse-index document %x: %w", read.EncodedID, err)
	}
	return name, true, nil
}
