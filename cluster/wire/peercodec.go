package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
)

// This file is the byte layout of the data-channel messages the replication stream and
// the forwards ride on. The registry (conventions 6.1) fixes WRITE's first byte and the
// WRITE_ACK, CATCHUP_REQ and CATCHUP_CHUNK headers but gives a WRITE no position and no
// log cursor, and names no forward message; the layouts below are this build's
// (SPEC-GAP U57-1, U57-2). Every integer is big-endian. Every length is checked against
// the bytes that remain before anything is allocated for it, and a body is accepted only
// if it re-encodes to the same bytes.

const (
	// recordFixedBytes is a PairRecord without its body: pos 16, cursor 8, class 1,
	// opIndex 1, request id 16, body length 4.
	recordFixedBytes = 16 + 8 + 1 + 1 + 16 + 4
	// maxChunkRecords is the 2-byte recordCount of CATCHUP_CHUNK.
	maxChunkRecords = 1<<16 - 1
	// forwardWriteFixed is a FORWARD_WRITE without its operation.
	forwardWriteFixed = 1 + 4 + 1 + 2 + 1 + 8 + 16 + 4
	// catchUpFlagCaughtUp and catchUpFlagDivergent are the bits of CATCHUP_CHUNK's
	// caughtUp byte; the registry names only the first.
	catchUpFlagCaughtUp  byte = 1 << 0
	catchUpFlagDivergent byte = 1 << 1
)

var errPeerBody = errors.New("INVALID_ARGUMENT")

func peerRefusal(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errPeerBody, fmt.Sprintf(format, args...))
}

// peerReader walks a body, refusing any read past its end.
type peerReader struct {
	rest []byte
	what string
}

func (r *peerReader) take(n int) ([]byte, error) {
	if n < 0 || n > len(r.rest) {
		return nil, peerRefusal("%s needs %d more bytes and %d remain", r.what, n, len(r.rest))
	}
	out := r.rest[:n:n]
	r.rest = r.rest[n:]
	return out, nil
}

func (r *peerReader) u8() (byte, error) {
	b, err := r.take(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func (r *peerReader) u16() (uint16, error) {
	b, err := r.take(2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(b), nil
}

func (r *peerReader) u32() (uint32, error) {
	b, err := r.take(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

func (r *peerReader) u64() (uint64, error) {
	b, err := r.take(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b), nil
}

func (r *peerReader) position() (cluster.Position, error) {
	b, err := r.take(16)
	if err != nil {
		return cluster.Position{}, err
	}
	return cluster.DecodePosition(b)
}

func (r *peerReader) done() error {
	if len(r.rest) != 0 {
		return peerRefusal("%s has %d bytes after its last field", r.what, len(r.rest))
	}
	return nil
}

func appendRecord(dst []byte, rec PairRecord) []byte {
	dst = cluster.AppendPosition(dst, rec.Pos)
	dst = binary.BigEndian.AppendUint64(dst, rec.Cursor)
	dst = append(dst, rec.Record.Class, rec.Record.OpIndex)
	dst = append(dst, rec.Record.RequestID[:]...)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(rec.Record.Body)))
	return append(dst, rec.Record.Body...)
}

func (r *peerReader) record() (PairRecord, error) {
	var rec PairRecord
	var err error
	if rec.Pos, err = r.position(); err != nil {
		return PairRecord{}, err
	}
	if rec.Cursor, err = r.u64(); err != nil {
		return PairRecord{}, err
	}
	head, err := r.take(2)
	if err != nil {
		return PairRecord{}, err
	}
	rec.Record = storage.WALRecord{Class: head[0], OpIndex: head[1]}
	id, err := r.take(16)
	if err != nil {
		return PairRecord{}, err
	}
	copy(rec.Record.RequestID[:], id)
	length, err := r.u32()
	if err != nil {
		return PairRecord{}, err
	}
	if int64(length) > int64(len(r.rest)) {
		return PairRecord{}, peerRefusal("a record says %d body bytes and %d remain", length, len(r.rest))
	}
	body, err := r.take(int(length))
	if err != nil {
		return PairRecord{}, err
	}
	rec.Record.Body = append([]byte(nil), body...)
	return rec, nil
}

// EncodeWriteBody is the body of WRITE: one pair record. The slot, database and the
// sender's epoch ride in the envelope, the sender's id in the handshake.
func EncodeWriteBody(rec PairRecord) []byte { return appendRecord(nil, rec) }

// DecodeWriteBody reads a WRITE body.
func DecodeWriteBody(body []byte) (PairRecord, error) {
	r := &peerReader{rest: body, what: "WRITE"}
	rec, err := r.record()
	if err != nil {
		return PairRecord{}, err
	}
	return rec, r.done()
}

// EncodeWriteAckBody is the body of WRITE_ACK: [position:16][outcome:1][copiesConfirmed:1].
// A copy's ack carries copiesConfirmed 0, which the owner ignores.
func EncodeWriteAckBody(ack WriteAck) []byte {
	return append(cluster.AppendPosition(nil, ack.Pos), byte(ack.Outcome), 0)
}

// DecodeWriteAckBody reads a WRITE_ACK body into an ack whose Slot, DB and From the
// caller fills from the envelope and the handshake.
func DecodeWriteAckBody(body []byte) (WriteAck, error) {
	r := &peerReader{rest: body, what: "WRITE_ACK"}
	pos, err := r.position()
	if err != nil {
		return WriteAck{}, err
	}
	outcome, err := r.u8()
	if err != nil {
		return WriteAck{}, err
	}
	if AckOutcome(outcome) > AckRefused {
		return WriteAck{}, peerRefusal("WRITE_ACK outcome %d is not 0 not applied, 1 applied or 2 refused", outcome)
	}
	copies, err := r.u8()
	if err != nil {
		return WriteAck{}, err
	}
	if copies != 0 {
		return WriteAck{}, peerRefusal("a copy's WRITE_ACK carries copiesConfirmed %d; it is meaningful only in the owner's own answer, so a copy sends 0", copies)
	}
	return WriteAck{Pos: pos, Outcome: AckOutcome(outcome)}, r.done()
}

// maxBatchEntries bounds one WRITE_BATCH or WRITE_ACK_BATCH. The count field could say 65,535; a frame that large holds a
// copy's connection for seconds, so a sender cuts frames here and a receiver refuses more.
const maxBatchEntries = 256

// ackBatchEntryBytes is one WRITE_ACK_BATCH entry: [slot:2][db:8][position:16][outcome:1].
const ackBatchEntryBytes = 2 + 8 + 16 + 1

// EncodeWriteAckBatchBody is the body of WRITE_ACK_BATCH: [count:2] then count × [slot:2][db:8][position:16][outcome:1].
// It refuses zero entries and more than maxBatchEntries.
func EncodeWriteAckBatchBody(acks []WriteAck) ([]byte, error) {
	if len(acks) == 0 || len(acks) > maxBatchEntries {
		return nil, peerRefusal("a WRITE_ACK_BATCH of %d entries must hold 1 to %d", len(acks), maxBatchEntries)
	}
	body := binary.BigEndian.AppendUint16(nil, uint16(len(acks)))
	for _, ack := range acks {
		if ack.Outcome > AckRefused {
			return nil, peerRefusal("WRITE_ACK_BATCH outcome %d is not 0 not applied, 1 applied or 2 refused", ack.Outcome)
		}
		body = binary.BigEndian.AppendUint16(body, ack.Slot)
		body = binary.BigEndian.AppendUint64(body, ack.DB)
		body = cluster.AppendPosition(body, ack.Pos)
		body = append(body, byte(ack.Outcome))
	}
	return body, nil
}

// DecodeWriteAckBatchBody reads a WRITE_ACK_BATCH body; From is left empty, the caller fills it from the handshake.
// It checks the count against the bytes that remain before it allocates, and refuses an outcome above AckRefused.
func DecodeWriteAckBatchBody(body []byte) ([]WriteAck, error) {
	r := &peerReader{rest: body, what: "WRITE_ACK_BATCH"}
	count, err := r.u16()
	if err != nil {
		return nil, err
	}
	if count == 0 || int(count) > maxBatchEntries {
		return nil, peerRefusal("WRITE_ACK_BATCH says %d entries; it holds 1 to %d", count, maxBatchEntries)
	}
	if int(count)*ackBatchEntryBytes > len(r.rest) {
		return nil, peerRefusal("WRITE_ACK_BATCH says %d entries and %d bytes remain, fewer than the %d each needs", count, len(r.rest), ackBatchEntryBytes)
	}
	acks := make([]WriteAck, 0, count)
	for range count {
		slot, err := r.u16()
		if err != nil {
			return nil, err
		}
		db, err := r.u64()
		if err != nil {
			return nil, err
		}
		pos, err := r.position()
		if err != nil {
			return nil, err
		}
		outcome, err := r.u8()
		if err != nil {
			return nil, err
		}
		if AckOutcome(outcome) > AckRefused {
			return nil, peerRefusal("WRITE_ACK_BATCH entry %d outcome %d is not 0 not applied, 1 applied or 2 refused", len(acks), outcome)
		}
		acks = append(acks, WriteAck{Slot: slot, DB: db, Pos: pos, Outcome: AckOutcome(outcome)})
	}
	return acks, r.done()
}

// WriteEntry is one record of a WRITE_BATCH with the pair it belongs to.
type WriteEntry struct {
	Slot uint16
	DB   uint64
	PairRecord
}

// writeBatchEntryFixed is one WRITE_BATCH entry without its record body: [slot:2][db:8] then a record's fixed bytes.
const writeBatchEntryFixed = 2 + 8 + recordFixedBytes

// EncodeWriteBatchBody is the body of WRITE_BATCH: [count:2] then count × [slot:2][db:8][record], each record in WRITE's
// layout (appendRecord), so a copy reads a batched record the way it reads a WRITE. It refuses zero entries and more than
// maxBatchEntries.
func EncodeWriteBatchBody(entries []WriteEntry) ([]byte, error) {
	if len(entries) == 0 || len(entries) > maxBatchEntries {
		return nil, peerRefusal("a WRITE_BATCH of %d entries must hold 1 to %d", len(entries), maxBatchEntries)
	}
	body := binary.BigEndian.AppendUint16(nil, uint16(len(entries)))
	for _, entry := range entries {
		body = binary.BigEndian.AppendUint16(body, entry.Slot)
		body = binary.BigEndian.AppendUint64(body, entry.DB)
		body = appendRecord(body, entry.PairRecord)
	}
	return body, nil
}

// DecodeWriteBatchBody reads a WRITE_BATCH body. It checks the count against the bytes that remain before it allocates,
// as the record fields of each entry are read by the same reader WRITE uses.
func DecodeWriteBatchBody(body []byte) ([]WriteEntry, error) {
	r := &peerReader{rest: body, what: "WRITE_BATCH"}
	count, err := r.u16()
	if err != nil {
		return nil, err
	}
	if count == 0 || int(count) > maxBatchEntries {
		return nil, peerRefusal("WRITE_BATCH says %d entries; it holds 1 to %d", count, maxBatchEntries)
	}
	if int(count)*writeBatchEntryFixed > len(r.rest) {
		return nil, peerRefusal("WRITE_BATCH says %d entries and %d bytes remain, fewer than the %d each needs", count, len(r.rest), writeBatchEntryFixed)
	}
	entries := make([]WriteEntry, 0, count)
	for range count {
		slot, err := r.u16()
		if err != nil {
			return nil, err
		}
		db, err := r.u64()
		if err != nil {
			return nil, err
		}
		rec, err := r.record()
		if err != nil {
			return nil, err
		}
		entries = append(entries, WriteEntry{Slot: slot, DB: db, PairRecord: rec})
	}
	return entries, r.done()
}

// EncodeCatchUpReqBody is the body of CATCHUP_REQ: [fromPosition:16][fromCursor:8].
func EncodeCatchUpReqBody(req CatchUpRequest) []byte {
	return binary.BigEndian.AppendUint64(cluster.AppendPosition(nil, req.From), req.FromCursor)
}

// DecodeCatchUpReqBody reads a CATCHUP_REQ body; the caller fills Slot, DB, Node and Epoch.
func DecodeCatchUpReqBody(body []byte) (CatchUpRequest, error) {
	r := &peerReader{rest: body, what: "CATCHUP_REQ"}
	from, err := r.position()
	if err != nil {
		return CatchUpRequest{}, err
	}
	cursor, err := r.u64()
	if err != nil {
		return CatchUpRequest{}, err
	}
	return CatchUpRequest{From: from, FromCursor: cursor}, r.done()
}

// EncodeCatchUpChunkBody is the body of CATCHUP_CHUNK:
// [sourceAppendedPos:16][flags:1][recordCount:2][lastPosition:16][records]. Bit 0 of the
// flags is the registry's caughtUp; bit 1 is divergent (SPEC-GAP U57-1).
func EncodeCatchUpChunkBody(chunk CatchUpChunk) ([]byte, error) {
	if len(chunk.Records) > maxChunkRecords {
		return nil, peerRefusal("a catch-up chunk of %d records exceeds the 2-byte count (%d)", len(chunk.Records), maxChunkRecords)
	}
	var flags byte
	if chunk.CaughtUp {
		flags |= catchUpFlagCaughtUp
	}
	if chunk.Divergent {
		flags |= catchUpFlagDivergent
	}
	body := cluster.AppendPosition(nil, chunk.SourceAppended)
	body = append(body, flags)
	body = binary.BigEndian.AppendUint16(body, uint16(len(chunk.Records)))
	body = cluster.AppendPosition(body, chunk.LastPosition)
	for _, rec := range chunk.Records {
		body = appendRecord(body, rec)
	}
	return body, nil
}

// DecodeCatchUpChunkBody reads a CATCHUP_CHUNK body.
func DecodeCatchUpChunkBody(body []byte) (CatchUpChunk, error) {
	r := &peerReader{rest: body, what: "CATCHUP_CHUNK"}
	var chunk CatchUpChunk
	var err error
	if chunk.SourceAppended, err = r.position(); err != nil {
		return CatchUpChunk{}, err
	}
	flags, err := r.u8()
	if err != nil {
		return CatchUpChunk{}, err
	}
	if flags&^(catchUpFlagCaughtUp|catchUpFlagDivergent) != 0 {
		return CatchUpChunk{}, peerRefusal("CATCHUP_CHUNK flags 0x%02x set reserved bits", flags)
	}
	chunk.CaughtUp, chunk.Divergent = flags&catchUpFlagCaughtUp != 0, flags&catchUpFlagDivergent != 0
	count, err := r.u16()
	if err != nil {
		return CatchUpChunk{}, err
	}
	if chunk.LastPosition, err = r.position(); err != nil {
		return CatchUpChunk{}, err
	}
	if int(count)*recordFixedBytes > len(r.rest) {
		return CatchUpChunk{}, peerRefusal("CATCHUP_CHUNK says %d records and %d bytes remain, fewer than the %d each needs", count, len(r.rest), recordFixedBytes)
	}
	chunk.Records = make([]PairRecord, 0, count)
	for range count {
		rec, err := r.record()
		if err != nil {
			return CatchUpChunk{}, err
		}
		chunk.Records = append(chunk.Records, rec)
	}
	return chunk, r.done()
}

// EncodeForwardWriteBody is the body of FORWARD_WRITE: the hop count, the client's
// remaining budget in milliseconds, the batch shape, the concern, the client's epoch, the
// request id, and the operation in cluster.EncodeClientOp's form: the client's bytes
// unchanged, no flags byte and no stamp, because the ordering owner alone builds the record
// body (storage-format spec §1.2). The slot and database ride in the envelope. A request
// with a Rewrite or a ClaimSink cannot cross: both are closures over the forwarder's memory.
func EncodeForwardWriteBody(req WriteRequest) ([]byte, error) {
	if req.Rewrite != nil || req.ClaimSink != nil {
		return nil, peerRefusal("a write with a read-modify-write or a claim sink cannot be forwarded in this build: send it to the owner of its slot")
	}
	op, err := cluster.EncodeClientOp(req.Op)
	if err != nil {
		return nil, fmt.Errorf("encoding the operation to forward: %w", err)
	}
	if req.BatchSize < 0 || req.BatchSize > 1<<16-1 {
		return nil, peerRefusal("a batch of %d operations does not fit the forward", req.BatchSize)
	}
	body := make([]byte, 0, forwardWriteFixed+len(op))
	body = append(body, req.Hop)
	body = binary.BigEndian.AppendUint32(body, uint32(min(req.Deadline.Milliseconds(), 1<<32-1)))
	body = append(body, req.OpIndex)
	body = binary.BigEndian.AppendUint16(body, uint16(req.BatchSize))
	body = append(body, byte(req.Concern))
	body = binary.BigEndian.AppendUint64(body, req.ClientEpoch)
	body = append(body, req.RequestID[:]...)
	body = binary.BigEndian.AppendUint32(body, uint32(len(op)))
	return append(body, op...), nil
}

// DecodeForwardWriteBody reads a FORWARD_WRITE body; the caller fills Slot, DBID,
// FromNode and FrameEpoch from the envelope.
func DecodeForwardWriteBody(body []byte) (WriteRequest, error) {
	r := &peerReader{rest: body, what: "FORWARD_WRITE"}
	var req WriteRequest
	var err error
	if req.Hop, err = r.u8(); err != nil {
		return WriteRequest{}, err
	}
	millis, err := r.u32()
	if err != nil {
		return WriteRequest{}, err
	}
	req.Deadline = time.Duration(millis) * time.Millisecond
	if req.OpIndex, err = r.u8(); err != nil {
		return WriteRequest{}, err
	}
	batch, err := r.u16()
	if err != nil {
		return WriteRequest{}, err
	}
	req.BatchSize = int(batch)
	concern, err := r.u8()
	if err != nil {
		return WriteRequest{}, err
	}
	req.Concern = machine.Concern(concern)
	if req.ClientEpoch, err = r.u64(); err != nil {
		return WriteRequest{}, err
	}
	id, err := r.take(16)
	if err != nil {
		return WriteRequest{}, err
	}
	copy(req.RequestID[:], id)
	length, err := r.u32()
	if err != nil {
		return WriteRequest{}, err
	}
	raw, err := r.take(int(min(int64(length), int64(len(r.rest))+1)))
	if err != nil {
		return WriteRequest{}, err
	}
	if req.Op, err = cluster.DecodeClientOp(raw); err != nil {
		return WriteRequest{}, fmt.Errorf("decoding the forwarded operation: %w", err)
	}
	return req, r.done()
}

// Reply statuses of FORWARD_WRITE_REPLY and FORWARD_READ_REPLY.
const (
	forwardAnswered byte = 0
	forwardRefused  byte = 1
)

// EncodeForwardRefusal is a reply body that carries the owner's refusal, so a forwarded
// write fails with the owner's own code and words: [1][code:2][len:4][message].
func EncodeForwardRefusal(refusal *cluster.Error) []byte {
	body := []byte{forwardRefused}
	body = binary.BigEndian.AppendUint16(body, uint16(refusal.Code))
	body = binary.BigEndian.AppendUint32(body, uint32(len(refusal.Message)))
	return append(body, refusal.Message...)
}

// The flags byte of a FORWARD_WRITE_REPLY answer. Bit 0 is the byte's old 0 or 1 deduped flag, so
// a reply without the new bits reads as before; every other bit is refused.
const (
	forwardFlagDeduped    byte = 1 << iota // the answer came from the dedupe window
	forwardFlagNotApplied                  // the guard did not hold; the result is [field id:2][scalar set]
	forwardFlagTruncated                   // a deduped retry's document was over the dedupe cap
	forwardFlagsKnown     = forwardFlagDeduped | forwardFlagNotApplied | forwardFlagTruncated
)

// EncodeForwardWriteAnswer is a FORWARD_WRITE_REPLY body that carries an answer:
// [0][outcome:1][pos:16][copies:1][flags:1][resultLen:4][result]. A not_applied answer's result
// is the guard field's id (2 bytes) then the current document's scalar set.
func EncodeForwardWriteAnswer(answer WriteAnswer) []byte {
	body := []byte{forwardAnswered, byte(answer.Outcome)}
	body = cluster.AppendPosition(body, answer.Pos)
	var flags byte
	answerResult := answer.Result
	if answer.Deduped {
		flags |= forwardFlagDeduped
	}
	if answer.DocumentTruncated {
		flags |= forwardFlagTruncated
	}
	if answer.NotApplied {
		flags |= forwardFlagNotApplied
		answerResult = binary.BigEndian.AppendUint16(nil, answer.GuardField)
		answerResult = append(answerResult, answer.Result...)
	}
	body = append(body, answer.CopiesConfirmed, flags)
	body = binary.BigEndian.AppendUint32(body, uint32(len(answerResult)))
	return append(body, answerResult...)
}

// DecodeForwardWriteReply reads a FORWARD_WRITE_REPLY: the owner's answer, or its
// refusal as a *cluster.Error.
func DecodeForwardWriteReply(body []byte) (WriteAnswer, error) {
	r := &peerReader{rest: body, what: "FORWARD_WRITE_REPLY"}
	status, err := r.u8()
	if err != nil {
		return WriteAnswer{}, err
	}
	if status == forwardRefused {
		return WriteAnswer{}, decodeForwardRefusal(r)
	}
	if status != forwardAnswered {
		return WriteAnswer{}, peerRefusal("FORWARD_WRITE_REPLY status %d is not 0 answered or 1 refused", status)
	}
	var answer WriteAnswer
	outcome, err := r.u8()
	if err != nil {
		return WriteAnswer{}, err
	}
	answer.Outcome = machine.Outcome(outcome)
	if answer.Pos, err = r.position(); err != nil {
		return WriteAnswer{}, err
	}
	if answer.CopiesConfirmed, err = r.u8(); err != nil {
		return WriteAnswer{}, err
	}
	flags, err := r.u8()
	if err != nil {
		return WriteAnswer{}, err
	}
	if flags&^forwardFlagsKnown != 0 {
		return WriteAnswer{}, peerRefusal("FORWARD_WRITE_REPLY flags 0x%02x set a bit this build does not know (known: 0x%02x)", flags, forwardFlagsKnown)
	}
	answer.Deduped = flags&forwardFlagDeduped != 0
	answer.NotApplied = flags&forwardFlagNotApplied != 0
	answer.DocumentTruncated = flags&forwardFlagTruncated != 0
	length, err := r.u32()
	if err != nil {
		return WriteAnswer{}, err
	}
	answerResult, err := r.take(int(min(int64(length), int64(len(r.rest))+1)))
	if err != nil {
		return WriteAnswer{}, err
	}
	if answer.NotApplied {
		if len(answerResult) < 2 {
			return WriteAnswer{}, peerRefusal("FORWARD_WRITE_REPLY says not applied and its answerResult has %d bytes, fewer than the 2 of the guard field id", len(answerResult))
		}
		answer.GuardField, answerResult = binary.BigEndian.Uint16(answerResult), answerResult[2:]
	}
	if len(answerResult) > 0 {
		answer.Result = append([]byte(nil), answerResult...)
	}
	return answer, r.done()
}

func decodeForwardRefusal(r *peerReader) error {
	code, err := r.u16()
	if err != nil {
		return err
	}
	length, err := r.u32()
	if err != nil {
		return err
	}
	text, err := r.take(int(min(int64(length), int64(len(r.rest))+1)))
	if err != nil {
		return err
	}
	if err := r.done(); err != nil {
		return err
	}
	return &cluster.Error{Code: cluster.Code(code), Message: string(text)}
}

// The sub-kinds of a FORWARD_READ: a keyed document read (the reroute of check 3) and
// the raw read of one stored document that conventions 20 uses for declarations.
const (
	forwardReadDocument byte = 0
	forwardReadStored   byte = 1
	// forwardReadFields is a document read that carries a projection: the document form
	// followed by [count:1][field id:2]..., ascending, at most 64. An unprojected read is
	// still sub-kind 0, byte for byte as before.
	forwardReadFields byte = 2
	// forwardReadField is a store read (list, count, get, peek, lease_get) of one field of
	// the document: the document form followed by the field read's own tail (fieldforward.go).
	forwardReadField byte = 3
)

// ForwardReadBody is a FORWARD_READ: the keyed read to answer. The slot and database ride
// in the envelope.
type ForwardReadBody struct {
	// Stored asks for the stored field values of the document, not a rendered one.
	Stored bool
	Req    ReadRequest
	// Field, when set, is a store read of one field of the document; Req is then its
	// request and a projection is not carried.
	Field *FieldForward
}

// EncodeForwardReadBody is [sub:1][hop:1][bucketID:8][clientEpoch:8][requestID:16]
// [idLen:2][id][encLen:2][encID], and for a projected read (sub-kind 2) the scalar field
// ids to return, [count:1][id:2]..., after the encID.
func EncodeForwardReadBody(msg ForwardReadBody) ([]byte, error) {
	if len(msg.Req.ID) > 1<<16-1 || len(msg.Req.EncID) > 1<<16-1 {
		return nil, peerRefusal("a read id of %d/%d bytes does not fit the forward", len(msg.Req.ID), len(msg.Req.EncID))
	}
	sub := forwardReadDocument
	switch {
	case msg.Field != nil && (msg.Stored || msg.Req.Fields != nil):
		return nil, peerRefusal("a store read carries neither the stored form nor a projection")
	case msg.Field != nil:
		sub = forwardReadField
	case msg.Stored && msg.Req.Fields != nil:
		return nil, peerRefusal("a stored read of a declaration carries no projection")
	case msg.Stored:
		sub = forwardReadStored
	case msg.Req.Fields != nil:
		if err := checkForwardFields(msg.Req.Fields); err != nil {
			return nil, err
		}
		sub = forwardReadFields
	}
	body := []byte{sub, msg.Req.Hop}
	body = binary.BigEndian.AppendUint64(body, msg.Req.BucketID)
	body = binary.BigEndian.AppendUint64(body, msg.Req.ClientEpoch)
	body = append(body, msg.Req.RequestID[:]...)
	body = binary.BigEndian.AppendUint16(body, uint16(len(msg.Req.ID)))
	body = append(body, msg.Req.ID...)
	body = binary.BigEndian.AppendUint16(body, uint16(len(msg.Req.EncID)))
	body = append(body, msg.Req.EncID...)
	if sub == forwardReadFields {
		body = append(body, byte(len(msg.Req.Fields)))
		for _, id := range msg.Req.Fields {
			body = binary.BigEndian.AppendUint16(body, id)
		}
	}
	if sub == forwardReadField {
		return appendFieldForward(body, *msg.Field)
	}
	return body, nil
}

// checkForwardFields holds a forwarded projection to the one encoding the decoder accepts:
// at most 64 ids, strictly ascending.
func checkForwardFields(ids []uint16) error {
	if len(ids) > ringMaxFields {
		return peerRefusal("a forwarded projection names %d fields and at most %d are taken", len(ids), ringMaxFields)
	}
	for position := 1; position < len(ids); position++ {
		if ids[position] <= ids[position-1] {
			return peerRefusal("a forwarded projection lists field ids %d then %d; they must ascend with none repeated", ids[position-1], ids[position])
		}
	}
	return nil
}

// DecodeForwardReadBody reads a FORWARD_READ body; the caller fills Slot, DBID, FromNode
// and FrameEpoch from the envelope.
func DecodeForwardReadBody(body []byte) (ForwardReadBody, error) {
	r := &peerReader{rest: body, what: "FORWARD_READ"}
	var msg ForwardReadBody
	sub, err := r.u8()
	if err != nil {
		return ForwardReadBody{}, err
	}
	if sub > forwardReadField {
		return ForwardReadBody{}, peerRefusal("FORWARD_READ sub-kind %d is not 0 document, 1 stored, 2 projected document or 3 store read", sub)
	}
	msg.Stored = sub == forwardReadStored
	if msg.Req.Hop, err = r.u8(); err != nil {
		return ForwardReadBody{}, err
	}
	if msg.Req.BucketID, err = r.u64(); err != nil {
		return ForwardReadBody{}, err
	}
	if msg.Req.ClientEpoch, err = r.u64(); err != nil {
		return ForwardReadBody{}, err
	}
	id, err := r.take(16)
	if err != nil {
		return ForwardReadBody{}, err
	}
	copy(msg.Req.RequestID[:], id)
	idLen, err := r.u16()
	if err != nil {
		return ForwardReadBody{}, err
	}
	text, err := r.take(int(idLen))
	if err != nil {
		return ForwardReadBody{}, err
	}
	msg.Req.ID = string(text)
	encLen, err := r.u16()
	if err != nil {
		return ForwardReadBody{}, err
	}
	enc, err := r.take(int(encLen))
	if err != nil {
		return ForwardReadBody{}, err
	}
	msg.Req.EncID = append([]byte(nil), enc...)
	if sub == forwardReadFields {
		count, err := r.u8()
		if err != nil {
			return ForwardReadBody{}, err
		}
		if int(count) > ringMaxFields {
			return ForwardReadBody{}, peerRefusal("a forwarded projection names %d fields and at most %d are taken", count, ringMaxFields)
		}
		msg.Req.Fields = make([]uint16, 0, count)
		for range count {
			id, err := r.u16()
			if err != nil {
				return ForwardReadBody{}, err
			}
			msg.Req.Fields = append(msg.Req.Fields, id)
		}
		if err := checkForwardFields(msg.Req.Fields); err != nil {
			return ForwardReadBody{}, err
		}
	}
	if sub == forwardReadField {
		field, err := r.fieldForward(msg.Req)
		if err != nil {
			return ForwardReadBody{}, err
		}
		msg.Field = &field
	}
	return msg, r.done()
}

// EncodeForwardReadDocument is a FORWARD_READ_REPLY carrying a rendered document:
// [0][served-by len:1][served-by][slot:2][encLen:2][encID][bodyLen:4][body].
func EncodeForwardReadDocument(doc Document) ([]byte, error) {
	if len(doc.ServedBy) > 255 || len(doc.EncID) > 1<<16-1 {
		return nil, peerRefusal("a document served by a %d-byte name with a %d-byte id does not fit the reply", len(doc.ServedBy), len(doc.EncID))
	}
	body := []byte{forwardAnswered, byte(len(doc.ServedBy))}
	body = append(body, doc.ServedBy...)
	body = binary.BigEndian.AppendUint16(body, doc.Slot)
	body = binary.BigEndian.AppendUint16(body, uint16(len(doc.EncID)))
	body = append(body, doc.EncID...)
	body = binary.BigEndian.AppendUint32(body, uint32(len(doc.Body)))
	return append(body, doc.Body...), nil
}

// DecodeForwardReadDocument reads a FORWARD_READ_REPLY for a document read: the document
// (without its Decl, which the caller attaches from its own cache) or the owner's refusal.
func DecodeForwardReadDocument(body []byte) (Document, error) {
	r := &peerReader{rest: body, what: "FORWARD_READ_REPLY"}
	status, err := r.u8()
	if err != nil {
		return Document{}, err
	}
	if status == forwardRefused {
		return Document{}, decodeForwardRefusal(r)
	}
	if status != forwardAnswered {
		return Document{}, peerRefusal("FORWARD_READ_REPLY status %d is not 0 answered or 1 refused", status)
	}
	var doc Document
	nameLen, err := r.u8()
	if err != nil {
		return Document{}, err
	}
	name, err := r.take(int(nameLen))
	if err != nil {
		return Document{}, err
	}
	doc.ServedBy = string(name)
	if doc.Slot, err = r.u16(); err != nil {
		return Document{}, err
	}
	encLen, err := r.u16()
	if err != nil {
		return Document{}, err
	}
	enc, err := r.take(int(encLen))
	if err != nil {
		return Document{}, err
	}
	doc.EncID = append([]byte(nil), enc...)
	length, err := r.u32()
	if err != nil {
		return Document{}, err
	}
	raw, err := r.take(int(min(int64(length), int64(len(r.rest))+1)))
	if err != nil {
		return Document{}, err
	}
	doc.Body = append([]byte(nil), raw...)
	return doc, r.done()
}

// EncodeForwardReadStored is a FORWARD_READ_REPLY carrying stored field values, or
// found 0: [0][found:1][count:2]{[field:2][len:4][value]}. Fields ascend by id.
func EncodeForwardReadStored(stored map[uint16][]byte, found bool) ([]byte, error) {
	if len(stored) > 1<<16-1 {
		return nil, peerRefusal("a document of %d fields does not fit the reply", len(stored))
	}
	body := []byte{forwardAnswered, 0}
	if found {
		body[1] = 1
	}
	ids := make([]uint16, 0, len(stored))
	for id := range stored {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	body = binary.BigEndian.AppendUint16(body, uint16(len(ids)))
	for _, id := range ids {
		body = binary.BigEndian.AppendUint16(body, id)
		body = binary.BigEndian.AppendUint32(body, uint32(len(stored[id])))
		body = append(body, stored[id]...)
	}
	return body, nil
}

// DecodeForwardReadStored reads the reply of a stored read.
func DecodeForwardReadStored(body []byte) (stored map[uint16][]byte, found bool, err error) {
	r := &peerReader{rest: body, what: "FORWARD_READ_REPLY"}
	status, err := r.u8()
	if err != nil {
		return nil, false, err
	}
	if status == forwardRefused {
		return nil, false, decodeForwardRefusal(r)
	}
	if status != forwardAnswered {
		return nil, false, peerRefusal("FORWARD_READ_REPLY status %d is not 0 answered or 1 refused", status)
	}
	flag, err := r.u8()
	if err != nil {
		return nil, false, err
	}
	if flag > 1 {
		return nil, false, peerRefusal("FORWARD_READ_REPLY found flag %d is not 0 or 1", flag)
	}
	count, err := r.u16()
	if err != nil {
		return nil, false, err
	}
	if int(count)*6 > len(r.rest) {
		return nil, false, peerRefusal("FORWARD_READ_REPLY says %d fields and %d bytes remain", count, len(r.rest))
	}
	stored = make(map[uint16][]byte)
	var last int32 = -1
	for range count {
		id, err := r.u16()
		if err != nil {
			return nil, false, err
		}
		if int32(id) <= last {
			return nil, false, peerRefusal("FORWARD_READ_REPLY fields are not ascending at field %d", id)
		}
		last = int32(id)
		length, err := r.u32()
		if err != nil {
			return nil, false, err
		}
		value, err := r.take(int(min(int64(length), int64(len(r.rest))+1)))
		if err != nil {
			return nil, false, err
		}
		stored[id] = append([]byte(nil), value...)
	}
	return stored, flag == 1, r.done()
}
