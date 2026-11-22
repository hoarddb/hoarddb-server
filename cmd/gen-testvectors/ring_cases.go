package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"slices"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"

	hc "github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// ringGroup is the group of the ring opcodes' vectors: RING_LIST to RING_TOPOLOGY, the
// RING_REPLY that answers them, the OpError a refusal travels in, and the CLUSTER_PROBE
// pair of the control channel. No server binary serves the ring yet, so these exchanges
// are not replayed (replay false); every payload is written by the server's own encoder
// in package wire and read back by its own decoder before it is written down, so the
// vectors cannot drift from the codecs.
const ringGroup = "ring"

// ringJSONLimit is the largest payload that also gets an Extended JSON rendering: a topology
// table is 16 to 33 KiB of binary, and its base64 would only repeat the hex.
const ringJSONLimit = 2048

// ringReplyFields are the wire names of RING_REPLY (entry-ring-client-wire.md section 2).
var ringReplyFields = []string{"write_outcome", "copies_confirmed", "served_by", "position", "id", "found", "elements", "keys", "priorities", "scores", "scanned", "skipped", "entries",
	"lease", "count", "more", "cursor", "deduped", "reason", "guard_field", "document", "document_truncated", "epoch", "state_seq", "outcome", "answers", "expanded", "expand_filtered", "served_from", "aggregate"}

var ringFieldTable = map[string][]string{
	"list":  {"database", "bucket", "filter", "fields", "limit", "cursor", "epoch", "local", "expand"},
	"count": {"database", "bucket", "filter"},
	"admin": {"cmd", "database", "name", "bucket", "rf", "fields", "aggregates", "add", "drop", "slot", "anyway", "limit", "cursor", "node_id", "data_addr", "witness", "accept_loss",
		"expect_seq", "expect_drop_seq", "roles", "credential", "enabled", "role", "db"},
	"topology":       {"epoch", "state_seq"},
	"topology reply": {"epoch", "state_seq", "unchanged", "members", "witnesses", "voters", "table", "databases[].name", "databases[].id", "databases[].rf"},
	"admin reply": {"cmd", "database_id", "rf", "bucket_id", "state", "decl_version", "schema_version_seen", "fields[].id", "fields[].name",
		"fields[].type", "aggregates[].store", "aggregates[].id", "aggregates[].kind", "aggregates[].field", "aggregates[].by", "aggregates[].state", "buckets[].name", "buckets[].bucket_id", "buckets[].state", "more", "cursor"},
	"admin seeding reply": {"cmd", "rows[].database", "rows[].slot", "rows[].state", "rows[].source", "rows[].bytes_done", "rows[].bytes_known", "rows[].entries", "rows[].idle_ms",
		"rows[].refusal", "more", "cursor", "database", "slot"},
	"error": {"code", "message", "detail"},
}

// ringDecoder is one opcode's strict decoders: the request's and the reply's.
type ringDecoder struct {
	request func([]byte) error
	reply   func([]byte) error
}

func decoderOf[T any](decode func([]byte) (T, error)) func([]byte) error {
	return func(raw []byte) error {
		_, err := decode(raw)
		return err
	}
}

func controlDecode(raw []byte) error {
	_, _, err := transport.DecodeControl(raw)
	return err
}

// ringDecoders are the server's own decoders, which every ring vector is replayed through.
var ringDecoders = map[transport.Opcode]ringDecoder{
	transport.OpRingList:     {decoderOf(wire.DecodeRingList), decoderOf(wire.DecodeRingReply)},
	transport.OpRingCount:    {decoderOf(wire.DecodeRingCount), decoderOf(wire.DecodeRingReply)},
	transport.OpRingKeyed:    {decoderOf(wire.DecodeRingKeyed), decoderOf(wire.DecodeRingReply)},
	transport.OpRingBatch:    {decoderOf(wire.DecodeRingBatch), decoderOf(wire.DecodeRingReply)},
	transport.OpRingGetMany:  {decoderOf(wire.DecodeRingGetMany), decoderOf(wire.DecodeRingReply)},
	transport.OpRingAdmin:    {decoderOf(wire.DecodeRingAdmin), decoderOf(wire.DecodeRingAdminReply)},
	transport.OpRingTopology: {decoderOf(wire.DecodeRingTopology), decoderOf(wire.DecodeRingTopologyReply)},
	transport.OpClusterProbe: {controlDecode, controlDecode},
}

// ringBuilder collects the ring vectors and the first error.
type ringBuilder struct {
	vectors []vector
	next    int
	err     error
}

func (b *ringBuilder) fail(format string, args ...any) {
	if b.err == nil {
		b.err = fmt.Errorf(format, args...)
	}
}

func (b *ringBuilder) must(raw []byte, err error) []byte {
	if err != nil {
		b.fail("encoding a ring vector: %v", err)
	}
	return raw
}

func (b *ringBuilder) requestID() [transport.RequestIDLen]byte {
	b.next++
	return fixedRequestID(9000 + b.next)
}

func ringDoc(pairs ...any) []byte {
	var doc bson.D
	for i := 0; i < len(pairs); i += 2 {
		doc = append(doc, bson.E{Key: pairs[i].(string), Value: pairs[i+1]})
	}
	raw, err := bson.Marshal(doc)
	if err != nil {
		panic(err) // invariant: the pairs are literals written in this file
	}
	return raw
}

func (b *ringBuilder) request(op transport.Opcode, fields []string, payload []byte) *vectorRequest {
	if err := ringDecoders[op].request(payload); err != nil {
		b.fail("the server's decoder refuses its own %s request: %v", op, err)
	}
	return b.requestOf(op, fields, payload)
}

func (b *ringBuilder) requestOf(op transport.Opcode, fields []string, payload []byte) *vectorRequest {
	id := b.requestID()
	out := &vectorRequest{Opcode: uint8(op), OpcodeName: op.String(), RequestIDHex: hex.EncodeToString(id[:]),
		PayloadHex: hex.EncodeToString(payload), AllowedFields: nonNil(fields)}
	if json, err := extendedJSON(payload); err == nil && isBSONRequest(op) && len(payload) <= ringJSONLimit {
		out.PayloadJSON = json
	}
	return out
}

func isBSONRequest(op transport.Opcode) bool {
	return op != transport.OpRingKeyed && op != transport.OpRingBatch && op != transport.OpRingGetMany && op != transport.OpClusterProbe
}

func (b *ringBuilder) response(request, op transport.Opcode, fields []string, payload []byte) *vectorResponse {
	if op == transport.OpRingReply || op == transport.OpClusterProbeReply {
		if err := ringDecoders[request].reply(payload); err != nil {
			b.fail("the server's decoder refuses its own %s reply: %v", request, err)
		}
	}
	return b.responseOf(op, fields, payload)
}

func (b *ringBuilder) responseOf(op transport.Opcode, fields []string, payload []byte) *vectorResponse {
	id := b.requestID()
	out := &vectorResponse{Opcode: uint8(op), OpcodeName: op.String(), RequestIDHex: hex.EncodeToString(id[:]),
		PayloadHex: hex.EncodeToString(payload), Fields: nonNil(fields), Vary: []string{}, AdditiveFields: []string{}, AbsentFields: []string{}}
	if json, err := extendedJSON(payload); err == nil && op != transport.OpClusterProbeReply && len(payload) <= ringJSONLimit {
		out.PayloadJSON = json
	}
	return out
}

// exchange adds a request answered with a RING_REPLY (or a probe reply).
func (b *ringBuilder) exchange(name, notes string, op transport.Opcode, requestFields []string, request []byte, replyFields []string, reply []byte) {
	replyOp := transport.OpRingReply
	if op == transport.OpClusterProbe {
		replyOp = transport.OpClusterProbeReply
	}
	built := vector{Name: name, Group: ringGroup, Kind: "exchange", Status: "frozen", Notes: notes,
		Topology: "static", Replay: boolPointer(false),
		Request: b.request(op, requestFields, request), Response: b.response(op, replyOp, replyFields, reply)}
	built.Response.RequestIDHex = built.Request.RequestIDHex
	b.vectors = append(b.vectors, built)
}

// refused adds a request answered with the OpError payload failure.
func (b *ringBuilder) refused(name, notes string, op transport.Opcode, requestFields []string, request []byte, failure wire.RingError, contains ...string) {
	payload := b.must(wire.EncodeRingError(failure))
	if _, err := wire.DecodeRingError(payload); err != nil {
		b.fail("the server's decoder refuses its own error payload: %v", err)
	}
	built := vector{Name: name, Group: ringGroup, Kind: "exchange", Status: "frozen", Notes: notes,
		Topology: "static", Replay: boolPointer(false),
		Request:  b.request(op, requestFields, request),
		Response: b.responseOf(transport.OpError, ringFieldTable["error"], payload),
		Error:    &vectorError{Code: int(failure.Code), Name: failure.Code.String(), MessageContains: nonNil(contains)}}
	built.Response.RequestIDHex = built.Request.RequestIDHex
	b.vectors = append(b.vectors, built)
}

// refusedWhole adds a request the server's strict decoder refuses, answered with the OpError that
// refuses the whole frame. It is the one exchange whose request does not decode.
func (b *ringBuilder) refusedWhole(name, notes string, op transport.Opcode, request []byte, failure wire.RingError, contains ...string) {
	if err := ringDecoders[op].request(request); err == nil {
		b.fail("%s: the server's decoder accepts the request", name)
		return
	}
	payload := b.must(wire.EncodeRingError(failure))
	built := vector{Name: name, Group: ringGroup, Kind: "exchange", Status: "frozen", Notes: notes,
		Topology: "static", Replay: boolPointer(false),
		Request:  b.requestOf(op, nil, request),
		Response: b.responseOf(transport.OpError, ringFieldTable["error"], payload),
		Error:    &vectorError{Code: int(failure.Code), Name: failure.Code.String(), MessageContains: nonNil(contains)}}
	built.Response.RequestIDHex = built.Request.RequestIDHex
	b.vectors = append(b.vectors, built)
}

// refusal adds a payload a strict decoder must refuse: a request the server answers with
// INVALID_ARGUMENT, or a reply a driver must not accept. The decoder is run here, so a
// refusal vector the server would accept cannot be written.
func (b *ringBuilder) refusal(name, notes string, op transport.Opcode, direction string, fields []string, payload []byte) {
	decode := ringDecoders[op].request
	if direction == "reply" {
		decode = ringDecoders[op].reply
	}
	err := decode(payload)
	if err == nil {
		b.fail("%s: the server's decoder accepts it", name)
		return
	}
	v := vector{Name: name, Group: ringGroup, Kind: "refuse", Status: "frozen", Notes: notes, Topology: "static", Replay: boolPointer(false),
		Refuse: &vectorRefusal{Direction: direction, Code: int(hc.CodeInvalidArgument), Name: hc.CodeInvalidArgument.String()}}
	if direction == "reply" {
		v.Response = b.responseOf(transport.OpRingReply, fields, payload)
		if op == transport.OpClusterProbe {
			v.Response.Opcode, v.Response.OpcodeName = uint8(transport.OpClusterProbeReply), transport.OpClusterProbeReply.String()
		}
	} else {
		v.Request = b.requestOf(op, fields, payload)
	}
	b.vectors = append(b.vectors, v)
}

// truncated and padded are the two cuts every strict decoder refuses.
func truncated(raw []byte) []byte { return slices.Clone(raw[:len(raw)-3]) }
func padded(raw []byte) []byte    { return append(slices.Clone(raw), 0) }

// ringStringValue is a BSON string as an op carries it: the type byte, then the value.
func ringStringValue(text string) []byte {
	out := binary.LittleEndian.AppendUint32([]byte{byte(bsontype.String)}, uint32(len(text)+1))
	return append(append(out, text...), 0)
}

// ringReplyID is the same string as a reply's id member.
func ringReplyID(text string) bson.RawValue {
	return bson.RawValue{Type: bsontype.String, Value: ringStringValue(text)[1:]}
}

// ringVectors builds every vector of the ring opcodes from the server's own encoders.
func ringVectors() ([]vector, error) {
	b := &ringBuilder{}
	epoch, stateSeq := int64(4), int64(9)
	stamp := func(reply wire.RingReply) wire.RingReply {
		reply.Epoch, reply.StateSeq = &epoch, &stateSeq
		return reply
	}
	encID, err := store.EncodeComponent("ada")
	if err != nil {
		return nil, fmt.Errorf("encoding the document id: %w", err)
	}
	body := ringDoc("_id_", "ada", "Name", "Ada")

	b.listVectors(stamp, encID, body)
	b.countVectors(stamp)
	b.keyedVectors(stamp, encID, body)
	b.adminVectors()
	b.topologyVectors()
	b.probeVectors()
	b.replyVectors(stamp)
	b.projectionVectors(stamp, encID, body)
	// Last, so the request ids of every earlier vector stay what they were.
	b.seedingVectors(ringFieldTable["admin"], ringFieldTable["admin seeding reply"])
	b.membershipVectors()
	// After everything else, so no earlier vector's request id moves.
	b.guardVectors(stamp, encID)
	b.expiredVectors(encID)
	b.expandedListVectors(stamp, encID, body)
	b.aggregateVectors(stamp)
	b.incVectors(stamp)
	b.getManyVectors(stamp)
	// Last, so no earlier vector's request id moves.
	b.userVectors()
	if b.err != nil {
		return nil, b.err
	}
	return b.vectors, nil
}

// expandedListVectors is fetch-through on the wire: a list that expands a field, answered with one
// expanded row and one row whose far document is gone, and a get_fields that asks for a local read.
func (b *ringBuilder) expandedListVectors(stamp func(wire.RingReply) wire.RingReply, encID, body []byte) {
	fields := ringFieldTable["list"]
	request := b.must(wire.EncodeRingList(wire.RingListRequest{Database: "app", Bucket: "Message",
		Options: wire.RingReadOptions{Expand: []wire.RingExpandSpec{{Field: "Author", Fields: []string{"Name"}}}}}))
	entries := hc.AppendEntry(hc.AppendEntry(nil, hc.ListEntry{Slot: 5737, EncID: encID, Body: body}), hc.ListEntry{Slot: 6001, EncID: encID, Body: body})
	found := b.must(bson.Marshal(bson.D{{Key: "Author", Value: bson.D{{Key: "expanded", Value: true}, {Key: "doc", Value: bson.D{{Key: "_id_", Value: "ada"}, {Key: "Name", Value: "Ada"}}}}}}))
	gone := b.must(bson.Marshal(bson.D{{Key: "Author", Value: bson.D{{Key: "expanded", Value: false}, {Key: "reason", Value: "missing"}}}}))
	b.exchange("ring list: expand an author field", "expand names the scalar fields whose values are ids of another bucket; the node reads each distinct id once by key and answers with expanded, one document per row in row order, each member {expanded: true, doc} or {expanded: false, reason}. A document that is gone is expanded false, never an error. served_from says whether a copy on the node answered (local) or the owner (owner).",
		transport.OpRingList, fields, request, ringReplyFields, b.must(wire.EncodeRingReply(stamp(wire.RingReply{Entries: entries, Expanded: [][]byte{found, gone}, Served: "owner"}))))
	encoded := b.must(wire.EncodeRingKeyed(wire.RingKeyedRequest{Prologue: wire.RingPrologue{Epoch: 4, StateSeq: 9}, Read: &wire.RingRead{
		Class: wire.RingClassGetFields, BucketID: 0x1f4a2c9b7de30015, DeclVersion: 1, SchemaVersionSeen: 1, EncID: encID, Fields: []string{"Author"},
		Options: wire.RingReadOptions{Local: true, Expand: []wire.RingExpandSpec{{Field: "Author"}}}}}))
	foundTrue := true
	b.exchange("ring keyed: a get_fields with expand and local", "Read classes 0x81, 0x83 and 0x89 take an optional trailer after their class form: a 2-byte length and a BSON document {local, expand: [{field, database, bucket, fields, where}]}, at most 8 expansions. No other class takes it.",
		transport.OpRingKeyed, nil, encoded, ringReplyFields, b.must(wire.EncodeRingReply(stamp(wire.RingReply{Found: &foundTrue, Elements: [][]byte{body}, Expanded: [][]byte{found}, Served: "local"}))))

	header := wire.RingRead{BucketID: 0x1f4a2c9b7de30015, DeclVersion: 1, SchemaVersionSeen: 1, EncID: encID}
	prologue := wire.RingPrologue{Epoch: 4, StateSeq: 9}
	plainGet := header
	plainGet.Class, plainGet.Options = wire.RingClassGet, wire.RingReadOptions{Local: true}
	b.exchange("ring keyed: a get with local (0x81 trailer)", "Read class 0x81 get takes the same optional trailer as 0x83 and 0x89: the class form, then a 2-byte length and a BSON document. local asks a ready copy on the node to answer; served_from says which side did.",
		transport.OpRingKeyed, nil, b.must(wire.EncodeRingKeyed(wire.RingKeyedRequest{Prologue: prologue, Read: &plainGet})), ringReplyFields,
		b.must(wire.EncodeRingReply(stamp(wire.RingReply{Found: &foundTrue, Elements: [][]byte{body}, Served: "local"}))))
	storeList := header
	storeList.FieldID, storeList.Class, storeList.Limit = 4, wire.RingClassStoreList, 2
	storeList.Options = wire.RingReadOptions{Expand: []wire.RingExpandSpec{{Bucket: "User", Fields: []string{"Name"}}}}
	b.exchange("ring keyed: a hashStore page with expand (0x83 trailer)", "Read class 0x83 store list takes the trailer too: expand names the bucket whose documents the element keys are the ids of (a spec with no field, since the store is the field). The reply carries expanded, one member per element in order.",
		transport.OpRingKeyed, nil, b.must(wire.EncodeRingKeyed(wire.RingKeyedRequest{Prologue: prologue, Read: &storeList})), ringReplyFields,
		b.must(wire.EncodeRingReply(stamp(wire.RingReply{Found: &foundTrue, Elements: [][]byte{{5, 0, 0, 0, 0}}, Keys: []string{"ada"},
			Expanded: [][]byte{b.must(bson.Marshal(bson.D{{Key: "Members", Value: bson.D{{Key: "expanded", Value: true}, {Key: "doc", Value: bson.D{{Key: "_id_", Value: "ada"}, {Key: "Name", Value: "Ada"}}}}}}))}}))))
	plain := b.must(wire.EncodeRingKeyed(wire.RingKeyedRequest{Prologue: prologue, Read: &wire.RingRead{Class: wire.RingClassGet, BucketID: 0x1f4a2c9b7de30015, DeclVersion: 1, SchemaVersionSeen: 1, EncID: encID}}))
	emptyTrailer := append(slices.Clone(plain), 0, 5, 5, 0, 0, 0, 0)
	b.refusal("ring keyed refused: an empty options trailer", "A trailer that names neither local nor expand has no encoding: an empty options set is no trailer at all.", transport.OpRingKeyed, "request", nil, emptyTrailer)
}

func (b *ringBuilder) listVectors(stamp func(wire.RingReply) wire.RingReply, encID, body []byte) {
	fields := ringFieldTable["list"]
	request := b.must(wire.EncodeRingList(wire.RingListRequest{Database: "app", Bucket: "User"}))
	entries := hc.AppendEntry(nil, hc.ListEntry{Slot: 5737, EncID: encID, Body: body})
	b.exchange("ring list: the first page of a bucket", "RING_LIST names the bucket by database and bucket name; limit, filter, cursor and epoch are absent rather than zero. The reply is a RING_REPLY: entries is a concatenation of [slot:2 BE][uvarint idlen][encID][uvarint bodylen][body] records.",
		transport.OpRingList, fields, request, ringReplyFields, b.must(wire.EncodeRingReply(stamp(wire.RingReply{Entries: entries}))))
	b.refused("ring list: a bucket the database does not declare", "An OpError carries {code, message}; a list of an unknown bucket is NOT_FOUND and the text names the fix.",
		transport.OpRingList, fields, request, wire.RingError{Code: hc.CodeNotFound, Message: hc.CodeNotFound.Text(hc.CondBucketNotFound, map[string]string{"db": "app", "bucket": "User"})}, "app", "User")
	b.refusal("ring list refused: members out of order", "bucket before database: a request has one byte layout.", transport.OpRingList, "request", fields,
		ringDoc("bucket", "User", "database", "app"))
	b.refusal("ring list refused: an extra member", "A member queries Formats does not define is refused, not ignored.", transport.OpRingList, "request", fields,
		ringDoc("database", "app", "bucket", "User", "sort", "name"))
	b.refusal("ring list refused: a widened integer", "limit is an int32; an int64 is refused even when the value fits.", transport.OpRingList, "request", fields,
		ringDoc("database", "app", "bucket", "User", "limit", int64(20)))
	b.refusal("ring list refused: a written default", "epoch 0 is absent, not written.", transport.OpRingList, "request", fields,
		ringDoc("database", "app", "bucket", "User", "epoch", int64(0)))
	b.refusal("ring list refused: truncated", "Cut three bytes short.", transport.OpRingList, "request", fields, truncated(request))
	b.refusal("ring list refused: trailing byte", "A byte after the document is not canonical.", transport.OpRingList, "request", fields, padded(request))
}

func (b *ringBuilder) countVectors(stamp func(wire.RingReply) wire.RingReply) {
	fields := ringFieldTable["count"]
	count := int64(3)
	request := b.must(wire.EncodeRingCount(wire.RingCountRequest{Database: "app", Bucket: "User"}))
	b.exchange("ring count: documents in a bucket", "The reply is a RING_REPLY with count as int64; a present zero is written.",
		transport.OpRingCount, fields, request, ringReplyFields, b.must(wire.EncodeRingReply(stamp(wire.RingReply{Count: &count}))))
	b.refused("ring count: a session with no role on the database", "PERMISSION_DENIED is one text for a forbidden bucket and one that does not exist, with no detail.",
		transport.OpRingCount, fields, request, wire.RingError{Code: hc.CodePermissionDenied, Message: "this session cannot use that bucket: the user holds no role on the bucket's database that allows this operation. Nothing was done. Ask an administrator to run 'grant role reader@<database> to <user>' if the user should have access"}, "Nothing was done")
	b.refusal("ring count refused: a limit", "A count takes no limit: a limited count is a wrong number.", transport.OpRingCount, "request", fields,
		ringDoc("database", "app", "bucket", "User", "limit", int32(5)))
	b.refusal("ring count refused: members out of order", "bucket before database.", transport.OpRingCount, "request", fields, ringDoc("bucket", "User", "database", "app"))
	b.refusal("ring count refused: truncated", "Cut three bytes short.", transport.OpRingCount, "request", fields, truncated(request))
}

func (b *ringBuilder) keyedVectors(stamp func(wire.RingReply) wire.RingReply, encID, body []byte) {
	read := wire.RingKeyedRequest{Prologue: wire.RingPrologue{Epoch: 4, StateSeq: 9, DeadlineMs: 5000},
		Read: &wire.RingRead{Class: wire.RingClassGet, BucketID: 0x1f4a2c9b7de30015, DeclVersion: 1, SchemaVersionSeen: 1, EncID: encID}}
	getRequest := b.must(wire.EncodeRingKeyed(read))
	found := true
	b.exchange("ring keyed: get one document", "RING_KEYED is binary: a 20-byte client prologue (epoch, state_seq, deadline), the class byte 0x81 get, the 40-byte header and the encoded document id. The reply is a RING_REPLY with found and the document's BSON in elements.",
		transport.OpRingKeyed, nil, getRequest, ringReplyFields, b.must(wire.EncodeRingReply(stamp(wire.RingReply{Found: &found, Elements: [][]byte{body}}))))
	copies := int32(1)
	insert := wire.RingKeyedRequest{Prologue: wire.RingPrologue{Epoch: 4, StateSeq: 9, DeadlineMs: 5000},
		Op: hc.Op{BucketID: 0x1f4a2c9b7de30015, DeclVersion: 1, SchemaVersionSeen: 1, Class: hc.OpInsert, EncID: encID, IDValue: ringStringValue("ada"),
			Fields: []hc.FieldOp{{FieldID: 2, Value: ringStringValue("Ada")}}}}
	position := hc.Position{Epoch: 4, Seq: 98213}
	b.exchange("ring keyed: insert one document", "A write class (0x01 insert) carries the document id value and the fields by field id. The reply carries write_outcome, copies_confirmed, served_by, position and the id the insert used.",
		transport.OpRingKeyed, nil, b.must(wire.EncodeRingKeyed(insert)), ringReplyFields,
		b.must(wire.EncodeRingReply(stamp(wire.RingReply{WriteOutcome: "applied", CopiesConfirmed: &copies, ServedBy: "node-a", Position: &position, ID: ringReplyID("ada")}))))
	b.keyedStoreVectors(stamp, encID)
	b.refused("ring keyed: the slot belongs to another node", "NOT_OWNER carries a detail naming the owner and its address (the client address when its topology row has one, the data address otherwise, as here); re-send the same request id there once.",
		transport.OpRingKeyed, nil, getRequest, wire.RingError{Code: hc.CodeNotOwner, Message: "slot 5737 belongs to node-b. Retry the same request there",
			Detail: &hc.Detail{Owner: "node-b", OwnerAddr: "192.0.2.2:4433", Epoch: 4, Slot: 5737}}, "node-b")
	b.refused("ring keyed: the client's epoch is behind", "STALE_EPOCH carries the node's epoch and the owner; the driver fetches RING_TOPOLOGY once and re-sends. owner_addr is the address a driver dials: the owner's client address when its topology row has one (here node-a, which published a.example:7434), its data address otherwise.",
		transport.OpRingKeyed, nil, getRequest, wire.RingError{Code: hc.CodeStaleEpoch, Message: "the request carries epoch 3 and this cluster is at epoch 4. Fetch the topology again",
			Detail: &hc.Detail{Epoch: 4, Owner: "node-a", OwnerAddr: "a.example:7434"}}, "epoch")
	b.refused("ring keyed: the client's declaration is stale", "SCHEMA_NOT_CURRENT: an alter published since the client described the bucket. Nothing was written; describe the bucket again and re-send under the same request id.",
		transport.OpRingKeyed, nil, b.must(wire.EncodeRingKeyed(insert)), wire.RingError{Code: hc.CodeSchemaNotCurrent,
			Message: hc.CodeSchemaNotCurrent.Text(hc.CondClientDeclarationStale, map[string]string{"db": "app", "bucket": "User", "d": "1", "s": "2"})}, "Nothing was written")
	unknownClass := slices.Clone(getRequest)
	unknownClass[wire.RingPrologueLen] = 0x7f
	b.refusal("ring keyed refused: an unknown class byte", "0x7f is neither a write class nor a read class.", transport.OpRingKeyed, "request", nil, unknownClass)
	b.refusal("ring keyed refused: truncated", "The header is cut short.", transport.OpRingKeyed, "request", nil, truncated(getRequest))
	b.refusal("ring keyed refused: trailing byte", "A get form has nothing after the encoded id.", transport.OpRingKeyed, "request", nil, padded(getRequest))
	b.refusal("ring keyed refused: shorter than the prologue", "Under 20 bytes.", transport.OpRingKeyed, "request", nil, getRequest[:19])
	b.batchVectors(stamp, insert, encID)
}

// batchVectors are RING_BATCH (0xE0): one prologue, a count byte and that many RING_KEYED write
// bodies; the reply is a RING_REPLY with outcome and one answer per operation, in request order.
func (b *ringBuilder) batchVectors(stamp func(wire.RingReply) wire.RingReply, insert wire.RingKeyedRequest, encID []byte) {
	prologue := wire.RingPrologue{Epoch: 4, StateSeq: 9, DeadlineMs: 5000}
	header := hc.Op{BucketID: 0x1f4a2c9b7de30015, DeclVersion: 1, SchemaVersionSeen: 1, EncID: encID}
	upsert := header
	upsert.Class, upsert.IDValue, upsert.Fields = hc.OpUpsert, ringStringValue("ada"), []hc.FieldOp{{FieldID: 2, Value: ringStringValue("Ada")}}
	remove := header
	remove.Class = hc.OpDelete
	request := b.must(wire.EncodeRingBatch(wire.RingBatchRequest{Prologue: prologue, Ops: []hc.Op{insert.Op, upsert, remove}}))
	copies := int32(1)
	position := hc.Position{Epoch: 4, Seq: 98213}
	later := hc.Position{Epoch: 4, Seq: 98214}
	applied := stamp(wire.RingReply{WriteOutcome: "applied", CopiesConfirmed: &copies, ServedBy: "node-a", Position: &position, ID: ringReplyID("ada")})
	removed := stamp(wire.RingReply{WriteOutcome: "applied", CopiesConfirmed: &copies, ServedBy: "node-a", Position: &later})
	refused := wire.RingError{Code: hc.CodeDocumentExists, Message: "document \"ada\" already exists in app.User. Nothing was written. Send an upsert to replace it"}
	b.exchange("ring batch: three writes, one refused", "RING_BATCH is binary: the 20-byte client prologue once, a count byte (1 to 255), then that many RING_KEYED write bodies back to back (class byte, 40-byte header, class form). Reads are not allowed. The reply is a RING_REPLY with outcome (ok when every operation was answered with a reply, partial when any was refused) and answers, one per operation in request order: each is {reply: the bytes of a lone RING_REPLY} or {error: the bytes of an OpError payload}. A refused operation does not stop the others, and there is no atomicity across them. To retry, send the whole frame again under the same request id: the operations that were applied answer deduped.",
		transport.OpRingBatch, nil, request, ringReplyFields,
		b.must(wire.EncodeRingReply(stamp(wire.RingReply{Outcome: "partial", Answers: []wire.RingAnswer{{Reply: &applied}, {Error: &refused}, {Reply: &removed}}}))))
	b.exchange("ring batch: every operation applied", "One operation, answered as the lone RING_KEYED would be: outcome ok and one answer.",
		transport.OpRingBatch, nil, b.must(wire.EncodeRingBatch(wire.RingBatchRequest{Prologue: prologue, Ops: []hc.Op{upsert}})), ringReplyFields,
		b.must(wire.EncodeRingReply(stamp(wire.RingReply{Outcome: "ok", Answers: []wire.RingAnswer{{Reply: &applied}}}))))
	zero := slices.Clone(request)
	zero[wire.RingPrologueLen] = 0
	b.refusal("ring batch refused: a count of zero", "A batch carries 1 to 255 operations.", transport.OpRingBatch, "request", nil, zero[:wire.RingPrologueLen+1])
	b.refusal("ring batch refused: a trailing byte", "Nothing follows the last operation.", transport.OpRingBatch, "request", nil, padded(request))
	b.refusal("ring batch refused: truncated", "The last operation is cut short.", transport.OpRingBatch, "request", nil, truncated(request))
	read := slices.Clone(request)
	read[wire.RingPrologueLen+1] = wire.RingClassGet
	b.refusal("ring batch refused: a read class", "A batch carries writes only; the error names the operation.", transport.OpRingBatch, "request", nil, read)
	short := slices.Clone(request[:wire.RingPrologueLen])
	_, shortErr := wire.DecodeRingBatch(short)
	b.refusedWhole("ring batch: a frame that does not decode is refused whole", "A payload that is not a prologue and a count is one OpError, INVALID_ARGUMENT; nothing was done. A refusal of one operation is an answer inside a RING_REPLY instead.",
		transport.OpRingBatch, short, wire.RingError{Code: hc.CodeInvalidArgument, Message: shortErr.(*hc.Error).Message}, "prologue")
	countAhead := slices.Clone(request)
	countAhead[wire.RingPrologueLen] = 4
	b.refusal("ring batch refused: a count past the operations", "The count says 4 and 3 follow.", transport.OpRingBatch, "request", nil, countAhead)
	b.batchEdgeVectors(batchEdgeInputs{stamp: stamp, prologue: prologue, insert: insert, header: header, upsert: upsert, applied: applied})
	b.refusal("ring batch reply refused: ok over a refused answer", "outcome is partial when any answer is an error.", transport.OpRingBatch, "reply", ringReplyFields,
		ringDoc("outcome", "ok", "answers", []bson.D{{{Key: "error", Value: b.must(wire.EncodeRingError(refused))}}}))
}

// guardVectors are the guarded update (F7): an update that carries an if and a return in the
// trailer, answered applied with the document, and the same guard refused with not_applied.
func (b *ringBuilder) guardVectors(stamp func(wire.RingReply) wire.RingReply, encID []byte) {
	prologue := wire.RingPrologue{Epoch: 4, StateSeq: 9, DeadlineMs: 5000}
	update := wire.RingKeyedRequest{Prologue: prologue, Op: hc.Op{BucketID: 0x1f4a2c9b7de30015, DeclVersion: 1, SchemaVersionSeen: 1, Class: hc.OpUpdate, EncID: encID,
		Fields: []hc.FieldOp{{FieldID: 2, OpCode: hc.UpdateSet, Value: ringStringValue("shipped")}},
		Guard:  []hc.GuardTerm{{FieldID: 2, Value: ringStringValue("processing")}}, Return: hc.ReturnAfter}}
	request := b.must(wire.EncodeRingKeyed(update))
	copies := int32(1)
	position := hc.Position{Epoch: 4, Seq: 98215}
	b.exchange("ring update guard applied", "An update and a delete end in one trailer byte: bit 0 says a guard section follows ([n:1][(field id:2][value length:4][typed value]) x n), bits 1 and 2 are the return mode (1 after, 2 before), every other bit is refused. The guard rides the client body only. When every term holds the reply is the ordinary applied reply plus document, a BSON document of the scalar fields, _id_ first, as the update left it (return after) or as it was (return before).",
		transport.OpRingKeyed, nil, request, ringReplyFields,
		b.must(wire.EncodeRingReply(stamp(wire.RingReply{WriteOutcome: "applied", CopiesConfirmed: &copies, ServedBy: "node-a", Position: &position,
			Document: ringDoc("_id_", "ada", "Name", "shipped")}))))
	b.exchange("ring update guard refused", "A guard that does not hold is a successful RING_REPLY, not an OpError: write_outcome not_applied, reason guard, guard_field naming the first field that differed and document, the current document. It has no position and no copies_confirmed, because nothing was written and no position was taken. A retry under the same request id is evaluated again.",
		transport.OpRingKeyed, nil, request, ringReplyFields,
		b.must(wire.EncodeRingReply(stamp(wire.RingReply{WriteOutcome: "not_applied", ServedBy: "node-a", Reason: "guard", GuardField: "Name",
			Document: ringDoc("_id_", "ada", "Name", "cancelled")}))))
}

// keyedStoreVectors are the store reads and the multi-element pop added by U85: a keyed read
// of one element, a page and a peek that carry keys, a pop of several, and the peek cap.
func (b *ringBuilder) keyedStoreVectors(stamp func(wire.RingReply) wire.RingReply, encID []byte) {
	header := wire.RingRead{BucketID: 0x1f4a2c9b7de30015, DeclVersion: 1, SchemaVersionSeen: 1, EncID: encID, FieldID: 4}
	prologue := wire.RingPrologue{Epoch: 4, StateSeq: 9, DeadlineMs: 5000}
	element := []byte{5, 0, 0, 0, 0}
	found := true
	storeGet := header
	storeGet.Class, storeGet.Key = wire.RingClassStoreGet, "grace"
	b.exchange("ring keyed: read one element by key (store_get)", "Read class 0x88 store_get reads one element of a hashStore or btreeStore by its key, or of a heapStore by its entry id: the class byte, the 40-byte header, the document id, the 2-byte field id, then a 2-byte key length and the key (1 to 1024 bytes). The reply is found and the element in elements; a key that is not there is found false. It takes no position.",
		transport.OpRingKeyed, nil, b.must(wire.EncodeRingKeyed(wire.RingKeyedRequest{Prologue: prologue, Read: &storeGet})), ringReplyFields,
		b.must(wire.EncodeRingReply(stamp(wire.RingReply{Found: &found, Elements: [][]byte{element}}))))
	list := header
	list.Class, list.Limit = wire.RingClassStoreList, 2
	b.exchange("ring keyed: a hashStore page carries the keys", "A store list of a hashStore or btreeStore replies with keys, one string per element in the same order as elements; a heapStore's list also carries priorities, and a fifoStore's or lifoStore's carries neither. Absent members are omitted.",
		transport.OpRingKeyed, nil, b.must(wire.EncodeRingKeyed(wire.RingKeyedRequest{Prologue: prologue, Read: &list})), ringReplyFields,
		b.must(wire.EncodeRingReply(stamp(wire.RingReply{Found: &found, Elements: [][]byte{element, element}, Keys: []string{"grace", "linus"}, More: true, Cursor: []byte("linus")}))))
	peek := header
	peek.Class, peek.Limit = wire.RingClassPeek, 2
	peekRequest := b.must(wire.EncodeRingKeyed(wire.RingKeyedRequest{Prologue: prologue, Read: &peek}))
	b.exchange("ring keyed: a heapStore peek carries entry ids and priorities", "A peek of a heapStore replies with keys (the entry ids) and priorities (int64), parallel to elements, highest priority first. A peek count is 1 to 100.",
		transport.OpRingKeyed, nil, peekRequest, ringReplyFields,
		b.must(wire.EncodeRingReply(stamp(wire.RingReply{Found: &found, Elements: [][]byte{element, element}, Keys: []string{"high", "mid"}, Priorities: []int64{9, 5}}))))
	tooMany := slices.Clone(peekRequest)
	tooMany[len(tooMany)-1] = 101
	b.refusal("ring keyed refused: a peek of more than 100", "The peek count is 1 to 100, the same cap the server enforces for a pop.", transport.OpRingKeyed, "request", nil, tooMany)
	popArg, err := wire.PopArg(3)
	if err != nil {
		b.fail("encoding the pop arg: %v", err)
		return
	}
	pop := wire.RingKeyedRequest{Prologue: prologue, Op: hc.Op{BucketID: header.BucketID, DeclVersion: 1, SchemaVersionSeen: 1, Class: hc.OpStoreVerb, EncID: encID,
		Fields: []hc.FieldOp{{FieldID: 4, Verb: hc.VerbPop, Value: popArg}}}}
	copies := int32(1)
	position := hc.Position{Epoch: 4, Seq: 98214}
	b.exchange("ring keyed: pop up to three elements", "A pop with no arg takes one element. A pop with the arg {n: int32} (a BSON document, n 1 to 100) takes up to n in one record and replies with them in elements, found true when at least one came out. All the elements must fit the 1024 bytes of a dedupe entry together, or the pop is refused RESULT_TOO_LARGE before it is applied, naming the count that fits.",
		transport.OpRingKeyed, nil, b.must(wire.EncodeRingKeyed(pop)), ringReplyFields,
		b.must(wire.EncodeRingReply(stamp(wire.RingReply{WriteOutcome: "applied", CopiesConfirmed: &copies, ServedBy: "node-a", Position: &position, Found: &found, Elements: [][]byte{element, element, element}}))))
}

func (b *ringBuilder) adminVectors() {
	fields := ringFieldTable["admin"]
	replyFields := ringFieldTable["admin reply"]
	rf := int32(2)
	request := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminCreateDatabase, Name: "app", RF: &rf}))
	b.exchange("ring admin: create a database", "RING_ADMIN is one BSON command document; cmd first. The reply names the cmd and the ids a RING_KEYED header needs.",
		transport.OpRingAdmin, fields, request, replyFields, b.must(wire.EncodeRingAdminReply(wire.RingAdminReply{Cmd: wire.AdminCreateDatabase, DatabaseID: 1, RF: 2, Storage: "disk"})))
	describe := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminDescribeBucket, Database: "app", Bucket: "User"}))
	b.exchange("ring admin: describe a bucket", "bucket_id is 16 lowercase hex digits; fields lists the id of every field, _id_ first.",
		transport.OpRingAdmin, fields, describe, replyFields, b.must(wire.EncodeRingAdminReply(wire.RingAdminReply{Cmd: wire.AdminDescribeBucket, BucketID: 0x1f4a2c9b7de30015, State: "active",
			DeclVersion: 1, SchemaVersionSeen: 1, Fields: []wire.RingAdminReplyField{{ID: 1, Name: "_id_", Type: "any"}, {ID: 2, Name: "Name", Type: "string"}, {ID: 3, Name: "Purchases", Type: "fifoStore"}}})))
	alter := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminAlterBucket, Database: "app", Bucket: "User",
		Add: []wire.RingAdminField{{Name: "Email", Type: "string"}}, Drop: []string{"Age"}}))
	b.exchange("ring admin: alter a bucket", "alter_bucket takes add [{name, type}] and drop [name], both always present and one of them non-empty. The reply is the describe_bucket reply of the table now in force: decl_version is one higher, a dropped field is gone from fields and its id is never reused.",
		transport.OpRingAdmin, fields, alter, replyFields, b.must(wire.EncodeRingAdminReply(wire.RingAdminReply{Cmd: wire.AdminAlterBucket, BucketID: 0x1f4a2c9b7de30015, State: "active",
			DeclVersion: 2, SchemaVersionSeen: 2, Fields: []wire.RingAdminReplyField{{ID: 1, Name: "_id_", Type: "any"}, {ID: 2, Name: "Name", Type: "string"}, {ID: 4, Name: "Email", Type: "string"}}})))
	drop := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminDropBucket, Database: "app", Bucket: "User"}))
	b.exchange("ring admin: drop a bucket", "drop_bucket takes database and bucket and replies with the bucket_id and state \"dropping\" as soon as the bucket is fenced: it stops answering at once, its documents are deleted in the background and the name is free when they are gone.",
		transport.OpRingAdmin, fields, drop, replyFields, b.must(wire.EncodeRingAdminReply(wire.RingAdminReply{Cmd: wire.AdminDropBucket, BucketID: 0x1f4a2c9b7de30015, State: "dropping"})))
	b.refused("ring admin: drop a bucket that is not there", "A bucket that does not exist, and a drop that already finished, are both NOT_FOUND with the commands to list or create it.",
		transport.OpRingAdmin, fields, drop, wire.RingError{Code: hc.CodeNotFound, Message: hc.CodeNotFound.Text(hc.CondBucketNotFound, map[string]string{"bucket": "User", "db": "app"})}, "show buckets in app")
	b.refused("ring admin: a type change", "A declared type never changes: INVALID_ARGUMENT names the field, both types and the steps that work.",
		transport.OpRingAdmin, fields, b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminAlterBucket, Database: "app", Bucket: "User",
			Add: []wire.RingAdminField{{Name: "Age", Type: "string"}}, Drop: []string{}})),
		wire.RingError{Code: hc.CodeInvalidArgument, Message: "field \"Age\" in app.User is declared int and this statement declares it string; a declared type never changes, because stored values were written as int. Add a new field and copy the values: 'alter bucket User in app { add <NewField> <type> }', update into <NewField>, then 'alter bucket User in app { drop Age }'"}, "a declared type never changes")
	b.refusal("ring admin refused: an alter with nothing in it", "Both lists empty names nothing to change.", transport.OpRingAdmin, "request", fields,
		ringDoc("cmd", wire.AdminAlterBucket, "database", "app", "bucket", "User", "add", bson.A{}, "drop", bson.A{}))
	b.refusal("ring admin refused: an alter without its drop list", "add and drop are both required, even when empty.", transport.OpRingAdmin, "request", fields,
		ringDoc("cmd", wire.AdminAlterBucket, "database", "app", "bucket", "User", "add", bson.A{bson.D{{Key: "name", Value: "Email"}, {Key: "type", Value: "string"}}}))
	b.refused("ring admin: no role on the database", "PERMISSION_DENIED for an admin command names neither the database nor whether it exists.",
		transport.OpRingAdmin, fields, request, wire.RingError{Code: hc.CodePermissionDenied, Message: "this session cannot run that command: the database does not exist on this node, or the user holds no role on it that allows the command. Nothing was done"}, "Nothing was done")
	b.refusal("ring admin refused: members out of order", "name before cmd.", transport.OpRingAdmin, "request", fields, ringDoc("name", "app", "cmd", wire.AdminCreateDatabase))
	b.refusal("ring admin refused: an extra member", "A create_database takes no database member.", transport.OpRingAdmin, "request", fields,
		ringDoc("cmd", wire.AdminCreateDatabase, "name", "app", "database", "x"))
	b.refusal("ring admin refused: a widened integer", "rf is an int32.", transport.OpRingAdmin, "request", fields, ringDoc("cmd", wire.AdminCreateDatabase, "name", "app", "rf", int64(2)))
	b.refusal("ring admin refused: an unknown command", "cmd must be one of the admin commands.", transport.OpRingAdmin, "request", fields, ringDoc("cmd", "drop_database", "name", "app"))
	b.refusal("ring admin refused: truncated", "Cut three bytes short.", transport.OpRingAdmin, "request", fields, truncated(request))
}

func (b *ringBuilder) topologyVectors() {
	fields, replyFields := ringFieldTable["topology"], ringFieldTable["topology reply"]
	first := b.must(wire.EncodeRingTopology(wire.RingTopologyRequest{}))
	state := hc.ClusterState{Epoch: 4, StateSeq: 9, Members: []hc.Member{
		{ID: "node-a", DataAddr: "192.0.2.1:4433", ClientAddr: "a.example:7434", State: hc.MemberActive, Suffrage: "voter"},
		{ID: "node-b", DataAddr: "192.0.2.2:4433", State: hc.MemberDown, Suffrage: "voter"},
		{ID: "node-c", DataAddr: "192.0.2.3:4433", State: hc.MemberActive, Suffrage: "nonvoter"},
		{ID: "node-w", DataAddr: "192.0.2.4:4433", ClientAddr: "w.example:7434", State: hc.MemberActive, Suffrage: "voter", Witness: true}}}
	databases := []wire.RingTopologyDatabase{{Name: "app", ID: 1, RF: 2}, {Name: "logs", ID: 2, RF: 1}}
	three := b.must(wire.EncodeRingTopologyReply(wire.RingTopologyReply{Epoch: 4, StateSeq: 9, Table: hc.BuildTable(state), Witnesses: []hc.Member{state.Members[3]}, Voters: 3, Databases: databases}))
	b.exchange("ring topology: the first call after AUTH", "epoch 0 and state_seq 0 ask for everything. members is count, then per member state byte, a flags byte (bit 0: the member votes), id, data address and client address (empty means the data address; a driver connects to the client address when there is one); witnesses, absent when there are none, lists the witnesses (members that hold no documents, voting or not) in the same layout (they are never ranked, so never in members or the table); voters, absent when 0, is how many members vote, counting one that is joining or dead and so is on neither list; table is 8192 slots of count and rank indices into members (the full ranking, a down member keeps its rank, never truncated by RF). The owner of a slot is the first of the first rf names whose state is not down.",
		transport.OpRingTopology, fields, first, replyFields, three)
	single := hc.ClusterState{Epoch: 1, StateSeq: 3, Members: []hc.Member{{ID: "node-a", DataAddr: "127.0.0.1:4433", State: hc.MemberActive, Suffrage: "voter"}}}
	b.exchange("ring topology: a single node", "One member and a table that names it for every slot.",
		transport.OpRingTopology, fields, first, replyFields,
		b.must(wire.EncodeRingTopologyReply(wire.RingTopologyReply{Epoch: 1, StateSeq: 3, Table: hc.BuildTable(single), Databases: databases[:1]})))
	held := b.must(wire.EncodeRingTopology(wire.RingTopologyRequest{Epoch: 4, StateSeq: 9}))
	b.exchange("ring topology: the view held is current", "A non-zero epoch and a state_seq that both match answer unchanged: members, table and databases are absent and the driver keeps what it has.",
		transport.OpRingTopology, fields, held, replyFields, b.must(wire.EncodeRingTopologyReply(wire.RingTopologyReply{Epoch: 4, StateSeq: 9, Unchanged: true})))
	b.refused("ring topology: a session the server refuses", "PERMISSION_DENIED when the connection has no identity; any authenticated session otherwise gets the members and the table.",
		transport.OpRingTopology, fields, first, wire.RingError{Code: hc.CodePermissionDenied, Message: "this session cannot read the topology: the connection is not authenticated as a user. Nothing was done"}, "topology")

	b.refusal("ring topology refused: members out of order", "state_seq before epoch.", transport.OpRingTopology, "request", fields, ringDoc("state_seq", int64(9), "epoch", int64(4)))
	b.refusal("ring topology refused: an extra member", "limit is not a topology member.", transport.OpRingTopology, "request", fields, ringDoc("epoch", int64(4), "state_seq", int64(9), "limit", int32(1)))
	b.refusal("ring topology refused: a widened integer", "epoch is an int64.", transport.OpRingTopology, "request", fields, ringDoc("epoch", int32(4), "state_seq", int64(9)))
	b.refusal("ring topology refused: truncated", "Cut three bytes short.", transport.OpRingTopology, "request", fields, truncated(held))

	// The refusals use two members, so each carries a 24 KiB table rather than 32 KiB.
	two := hc.ClusterState{Epoch: 4, StateSeq: 9, Members: state.Members[:2]}
	two.Members = []hc.Member{{ID: "node-a", DataAddr: "192.0.2.1:4433", State: hc.MemberActive, Suffrage: "voter"}, {ID: "node-b", DataAddr: "192.0.2.2:4433", State: hc.MemberActive, Suffrage: "voter"}}
	table := hc.BuildTable(two)
	twoReply := b.must(wire.EncodeRingTopologyReply(wire.RingTopologyReply{Epoch: 4, StateSeq: 9, Table: table, Databases: databases[:1]}))
	memberBlob := wire.AppendTopologyMembers(nil, table.Members())
	tableBlob := table.Encode()
	reply := func(members, tableBytes []byte, rf any) []byte {
		return ringDoc("epoch", int64(4), "state_seq", int64(9), "members", members, "table", tableBytes,
			"databases", bson.A{bson.D{{Key: "name", Value: "app"}, {Key: "id", Value: int64(1)}, {Key: "rf", Value: rf}}})
	}
	members := table.Members()
	reordered := wire.AppendTopologyMembers(nil, []hc.Member{members[1], members[0]})
	extra := wire.AppendTopologyMembers(nil, append(slices.Clone(members), hc.Member{ID: "node-c", DataAddr: "192.0.2.3:4433", State: hc.MemberActive}))
	nonCanonical := append([]byte{2, byte(hc.MemberActive), 1 /* voter */, 0x86, 0x00}, memberBlob[4:]...)
	reservedFlag := slices.Clone(memberBlob)
	reservedFlag[2] |= 0x02
	rankedWitness := wire.AppendTopologyMembers(nil, members[:1])
	replyWith := func(witnesses []byte) []byte {
		return ringDoc("epoch", int64(4), "state_seq", int64(9), "members", memberBlob, "witnesses", witnesses, "table", tableBlob,
			"databases", bson.A{bson.D{{Key: "name", Value: "app"}, {Key: "id", Value: int64(1)}, {Key: "rf", Value: int32(2)}}})
	}
	b.refusal("ring topology reply refused: members out of order", "Members are sorted by id bytes, because the table's rank indices count in that order.", transport.OpRingTopology, "reply", replyFields, reply(reordered, tableBlob, int32(2)))
	b.refusal("ring topology reply refused: an extra member", "A third member the table does not rank.", transport.OpRingTopology, "reply", replyFields, reply(extra, tableBlob, int32(2)))
	b.refusal("ring topology reply refused: a reserved flag bit", "Only bit 0 of a member's flags byte is defined; the other seven must be zero.", transport.OpRingTopology, "reply", replyFields, reply(reservedFlag, tableBlob, int32(2)))
	b.refusal("ring topology reply refused: a witness that is also ranked", "node-a is in members and in witnesses; a witness is never ranked.", transport.OpRingTopology, "reply", replyFields, replyWith(rankedWitness))
	b.refusal("ring topology reply refused: an empty witnesses blob", "witnesses is absent when there is no witness, never written with a count of zero.", transport.OpRingTopology, "reply", replyFields, replyWith([]byte{0}))
	b.exchange("ring topology: a witness is listed apart from the members", "A witness is in witnesses, not in members or the table, so no slot is ever routed to it; voters counts the votes.",
		transport.OpRingTopology, fields, first, replyFields,
		b.must(wire.EncodeRingTopologyReply(wire.RingTopologyReply{Epoch: 4, StateSeq: 9, Table: table, Witnesses: []hc.Member{{ID: "node-w", DataAddr: "192.0.2.4:4433", State: hc.MemberActive, Suffrage: "voter", Witness: true}}, Voters: 3, Databases: databases[:1]})))
	b.refusal("ring topology reply refused: a widened integer", "rf is an int32.", transport.OpRingTopology, "reply", replyFields, reply(memberBlob, tableBlob, int64(2)))
	b.refusal("ring topology reply refused: a non-canonical varint", "An id length written as the two-byte varint 0x86 0x00.", transport.OpRingTopology, "reply", replyFields, reply(nonCanonical, tableBlob, int32(2)))
	b.refusal("ring topology reply refused: a truncated table", "The table ends one byte early.", transport.OpRingTopology, "reply", replyFields, reply(memberBlob, tableBlob[:len(tableBlob)-1], int32(2)))
	b.refusal("ring topology reply refused: a padded table", "A byte after slot 8191.", transport.OpRingTopology, "reply", replyFields, reply(memberBlob, padded(tableBlob), int32(2)))
	b.refusal("ring topology reply refused: truncated", "Cut three bytes short.", transport.OpRingTopology, "reply", replyFields, truncated(twoReply))
	b.refusal("ring topology reply refused: unchanged with blobs", "unchanged carries nothing else.", transport.OpRingTopology, "reply", replyFields,
		ringDoc("epoch", int64(4), "state_seq", int64(9), "unchanged", true, "members", memberBlob, "table", tableBlob))
}

func (b *ringBuilder) probeVectors() {
	probe := b.must(transport.EncodeControl(transport.Envelope{Version: transport.EnvelopeVersion, Epoch: 4, StateSeq: 9}, transport.Probe{Nonce: 0x0102030405060708}))
	reply := b.must(transport.EncodeControl(transport.Envelope{Version: transport.EnvelopeVersion, Epoch: 4, StateSeq: 9}, transport.ProbeReply{Nonce: 0x0102030405060708, State: byte(hc.MemberActive), AppliedStateSeq: 9}))
	b.exchange("cluster probe: a liveness probe", "Node to node, on the control channel: the 30-byte envelope (version, kind, epoch, state_seq, flags, slot, database id; big-endian) then an 8-byte nonce. The reply echoes the nonce and adds the member state byte and the applied state_seq (17 bytes).",
		transport.OpClusterProbe, nil, probe, nil, reply)
	b.refusal("cluster probe refused: a short body", "The body must be exactly 8 bytes.", transport.OpClusterProbe, "request", nil, truncated(probe))
	b.refusal("cluster probe refused: a long body", "A byte after the nonce.", transport.OpClusterProbe, "request", nil, padded(probe))
	b.refusal("cluster probe reply refused: a short body", "The body must be exactly 17 bytes.", transport.OpClusterProbe, "reply", nil, truncated(reply))
}

func (b *ringBuilder) replyVectors(stamp func(wire.RingReply) wire.RingReply) {
	reply := b.must(wire.EncodeRingReply(stamp(wire.RingReply{ServedBy: "node-a"})))
	b.refusal("ring reply refused: members out of order", "epoch before served_by.", transport.OpRingList, "reply", ringReplyFields, ringDoc("epoch", int64(4), "served_by", "node-a"))
	b.refusal("ring reply refused: an extra member", "A member the reply table does not define.", transport.OpRingList, "reply", ringReplyFields, ringDoc("served_by", "node-a", "colour", "red"))
	b.refusal("ring reply refused: a widened integer", "copies_confirmed is an int32.", transport.OpRingList, "reply", ringReplyFields, ringDoc("copies_confirmed", int64(1)))
	b.refusal("ring reply refused: a written default", "deduped false is absent, not written.", transport.OpRingList, "reply", ringReplyFields, ringDoc("deduped", false))
	b.refusal("ring reply refused: truncated", "Cut three bytes short.", transport.OpRingList, "reply", ringReplyFields, truncated(reply))
	errorPayload := b.must(wire.EncodeRingError(wire.RingError{Code: hc.CodeNotOwner, Message: "slot 5737 belongs to node-b",
		Detail: &hc.Detail{Owner: "node-b", OwnerAddr: "192.0.2.2:4433", Epoch: 4, Slot: 5737}}))
	b.vectors = append(b.vectors, vector{Name: "ring error refused: truncated", Group: ringGroup, Kind: "refuse", Status: "frozen",
		Notes: "An OpError payload cut short is not an error a driver may act on.", Topology: "static", Replay: boolPointer(false),
		Refuse:   &vectorRefusal{Direction: "error", Code: int(hc.CodeInvalidArgument), Name: hc.CodeInvalidArgument.String()},
		Response: b.responseOf(transport.OpError, ringFieldTable["error"], truncated(errorPayload))})
	if _, err := wire.DecodeRingError(truncated(errorPayload)); err == nil {
		b.fail("the server's decoder accepts a truncated error payload")
	}
}

// projectionVectors are the field projection added by U87: the fields member of a list and
// read class 0x89 get_fields. They are generated last so the request ids of the vectors
// before them do not move.
func (b *ringBuilder) projectionVectors(stamp func(wire.RingReply) wire.RingReply, encID, body []byte) {
	fields := ringFieldTable["list"]
	entries := hc.AppendEntry(nil, hc.ListEntry{Slot: 5737, EncID: encID, Body: body})
	found := true
	read := wire.RingKeyedRequest{Prologue: wire.RingPrologue{Epoch: 4, StateSeq: 9, DeadlineMs: 5000}}
	projected := b.must(wire.EncodeRingList(wire.RingListRequest{Database: "app", Bucket: "User", Fields: []string{"Name", "Bio"}}))
	b.exchange("ring list: only the named fields", "fields (1 to 64 names of 1 to 255 bytes, none twice; _id_ allowed and ignored) sits between filter and limit. Each entry's body is _id_ first, then the named scalars in field-id order, whatever order the request names them; a store field is never named. A request without fields is the first-page request and is unchanged.",
		transport.OpRingList, fields, projected, ringReplyFields, b.must(wire.EncodeRingReply(stamp(wire.RingReply{Entries: entries}))))
	b.refused("ring list: a field the bucket does not declare", "FIELD_TYPE names the field and the statement that lists the declared ones. Nothing was read.",
		transport.OpRingList, fields, projected, wire.RingError{Code: hc.CodeFieldType, Message: `field "Bio" is not declared in app.User (declared scalar fields: _id_, Name). Nothing was read. Run 'describe bucket User in app' to see the fields, or correct the name`}, "Bio", "describe bucket")
	b.refusal("ring list refused: empty fields", "An empty fields array is refused: leave the member out to get every field.", transport.OpRingList, "request", fields,
		ringDoc("database", "app", "bucket", "User", "fields", bson.A{}))
	b.refusal("ring list refused: a field named twice", "Each name once.", transport.OpRingList, "request", fields,
		ringDoc("database", "app", "bucket", "User", "fields", bson.A{"Name", "Name"}))
	b.refusal("ring list refused: fields that is not an array of strings", "fields is an array, and every element is a string.", transport.OpRingList, "request", fields,
		ringDoc("database", "app", "bucket", "User", "fields", bson.A{"Name", int32(3)}))
	b.refusal("ring list refused: fields after limit", "fields sits between filter and limit; the layout is canonical.", transport.OpRingList, "request", fields,
		ringDoc("database", "app", "bucket", "User", "limit", int32(5), "fields", bson.A{"Name"}))
	getFields := wire.RingKeyedRequest{Prologue: read.Prologue,
		Read: &wire.RingRead{Class: wire.RingClassGetFields, BucketID: 0x1f4a2c9b7de30015, DeclVersion: 1, SchemaVersionSeen: 1, EncID: encID, Fields: []string{"Name"}}}
	getFieldsRequest := b.must(wire.EncodeRingKeyed(getFields))
	b.exchange("ring keyed: get only the named fields (get_fields)", "Read class 0x89 get_fields is a get that returns _id_ and the named scalar fields only, in field-id order: the class byte, the 40-byte header, the document id, then a 1-byte count (1 to 64) and, per name, a 1-byte length and the name. It carries no field id. The reply is the get reply.",
		transport.OpRingKeyed, nil, getFieldsRequest, ringReplyFields, b.must(wire.EncodeRingReply(stamp(wire.RingReply{Found: &found, Elements: [][]byte{body}}))))
	storeFields := getFields
	storeFields.Read = &wire.RingRead{Class: wire.RingClassGetFields, BucketID: getFields.Read.BucketID, DeclVersion: 1, SchemaVersionSeen: 1, EncID: encID, Fields: []string{"Friends"}}
	b.refused("ring keyed: get_fields names a store field", "FIELD_TYPE names the store field and the statement that reads it; a projection returns scalar fields only. Nothing was read.",
		transport.OpRingKeyed, nil, b.must(wire.EncodeRingKeyed(storeFields)), wire.RingError{Code: hc.CodeFieldType, Message: `field "Friends" in app.User is a hash store field, and fields returns scalar fields only. Nothing was read. Run 'db.User["<id>"].Friends.list()' to read the store, or take "Friends" out of fields`}, "Friends", "list()")
	noNames := slices.Clone(getFieldsRequest)
	noNames = append(noNames[:len(noNames)-len("Name")-2], 0)
	b.refusal("ring keyed refused: get_fields with no names", "A count of 0 is refused: a plain get returns every field.", transport.OpRingKeyed, "request", nil, noNames)
	b.refusal("ring keyed refused: get_fields with a trailing byte", "Nothing follows the last name.", transport.OpRingKeyed, "request", nil, padded(getFieldsRequest))
}

// seedingVectors are the three seeding commands (admin-wire section 8): a status page, the two
// actions, and the refusals a driver must be able to tell apart.
func (b *ringBuilder) seedingVectors(fields, replyFields []string) {
	slot := int32(2638)
	status := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminSeedingStatus, Database: "app", Limit: ptrInt32(2)}))
	b.exchange("ring admin: seeding status", "seeding_status takes database, slot, limit and cursor, all optional, and answers rows in database then slot order. state is placing, seeding, stalled, ready or retired; every row member is always written (an empty text, a 0 counter); more and cursor are written only on a page that is not the last (here limit is 2 and the node holds a third row). A stalled pull is a row, not an error.",
		transport.OpRingAdmin, fields, status, replyFields, b.must(wire.EncodeRingAdminReply(wire.RingAdminReply{Cmd: wire.AdminSeedingStatus, Seeding: &wire.RingAdminSeeding{Rows: []wire.RingAdminSeedRow{
			{Database: "app", Slot: 2638, State: "stalled", Source: "node-b", BytesDone: 10 << 20, BytesKnown: 40 << 20, Entries: 123456, IdleMS: 372000, Refusal: "node-b has not answered since 14:02:11"},
			{Database: "app", Slot: 2639, State: "placing"}}, More: true, Cursor: []byte{0x0a, 0x4f, 'a', 'p', 'p'}}})))
	b.exchange("ring admin: seeding status with a retiring row", "The last page: no more and no cursor. retiring is a slot whose range delete has started and cannot be stopped; a driver shows it as waiting and offers neither retry nor forget.",
		transport.OpRingAdmin, fields, b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminSeedingStatus})), replyFields, b.must(wire.EncodeRingAdminReply(wire.RingAdminReply{Cmd: wire.AdminSeedingStatus,
			Seeding: &wire.RingAdminSeeding{Rows: []wire.RingAdminSeedRow{{Database: "app", Slot: 7, State: "retired"}, {Database: "app", Slot: 9, State: "retiring"}}}})))
	retry := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminSeedingRetry, Database: "app", Slot: &slot}))
	b.exchange("ring admin: seeding retry", "seeding_retry takes database and slot (0 to 8191) and restarts that slot's pull from a fresh cut. The reply echoes database and slot.",
		transport.OpRingAdmin, fields, retry, replyFields, b.must(wire.EncodeRingAdminReply(wire.RingAdminReply{Cmd: wire.AdminSeedingRetry, Seeding: &wire.RingAdminSeeding{Database: "app", Slot: slot}})))
	forget := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminSeedingForget, Database: "app", Slot: &slot}))
	b.exchange("ring admin: seeding forget", "seeding_forget removes the retired record of a slot and keeps its data on disk until another copy accepts the node's next pull of the slot, or until the node leaves the slot's copy set after being named a copy again; either deletes the data. The reply echoes database and slot.",
		transport.OpRingAdmin, fields, forget, replyFields, b.must(wire.EncodeRingAdminReply(wire.RingAdminReply{Cmd: wire.AdminSeedingForget, Seeding: &wire.RingAdminSeeding{Database: "app", Slot: slot}})))
	forgetAnyway := b.must(wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminSeedingForget, Database: "app", Slot: &slot, Anyway: true}))
	b.exchange("ring admin: seeding forget anyway", "seeding_forget takes an optional anyway bool, written only when true. A plain forget of a retired record that no other member has confirmed a copy of is refused SEED_HELD, because this node may hold the only copy; anyway forgets it regardless. The reply is the same as a plain forget.",
		transport.OpRingAdmin, fields, forgetAnyway, replyFields, b.must(wire.EncodeRingAdminReply(wire.RingAdminReply{Cmd: wire.AdminSeedingForget, Seeding: &wire.RingAdminSeeding{Database: "app", Slot: slot}})))
	b.refused("ring admin: seeding retry of a slot with no seed", "SEED_NOT_HELD (0x26, exit 2): the node has no seeding record for the slot. A node that does no seeding answers this to every retry and forget.",
		transport.OpRingAdmin, fields, retry, wire.RingError{Code: hc.CodeSeedNotHeld, Message: hc.CodeSeedNotHeld.Text(hc.CondSeedNotHeld, map[string]string{"slot": "2638", "db": "app", "record": "absent"})}, "status seeding")
	b.refused("ring admin: seeding forget of a slot that is not retired", "SEED_HELD (0x2A, exit 8): only a retired record can be forgotten; nothing was changed.",
		transport.OpRingAdmin, fields, forget, wire.RingError{Code: hc.CodeSeedHeld, Message: hc.CodeSeedHeld.Text(hc.CondSeedHeld, map[string]string{"slot": "2638", "db": "app", "state": "seeding"})}, "retired record")
	b.refused("ring admin: seeding forget of the only copy", "SEED_HELD (0x2A, exit 8): a retired record that no other member has confirmed a copy of may be the only copy of the slot's data; nothing was changed. The text names the anyway form.",
		transport.OpRingAdmin, fields, forget, wire.RingError{Code: hc.CodeSeedHeld, Message: hc.CodeSeedHeld.Text(hc.CondSeedHeldOnlyCopy, map[string]string{"slot": "2638", "db": "app"})}, "seeding forget app 2638 anyway")
	b.refused("ring admin: seeding without the cluster-manager role", "The three seeding commands need the cluster-manager role; PERMISSION_DENIED names no database, slot or user.",
		transport.OpRingAdmin, fields, status, wire.RingError{Code: hc.CodePermissionDenied, Message: "this session cannot run that command: seeding is run by the cluster-manager role granted on every database (cluster-manager@*). Nothing was done. Ask an administrator to run 'grant role cluster-manager@* to <user>' if the user should run it"}, "cluster-manager")
	b.refusal("ring admin refused: a seeding retry without its slot", "seeding_retry needs a slot.", transport.OpRingAdmin, "request", fields, ringDoc("cmd", wire.AdminSeedingRetry, "database", "app"))
	b.refusal("ring admin refused: a seeding slot past the last", "A slot is 0 to 8191.", transport.OpRingAdmin, "request", fields, ringDoc("cmd", wire.AdminSeedingForget, "database", "app", "slot", int32(8192)))
	b.refusal("ring admin refused: a seeding slot as int64", "slot is an int32.", transport.OpRingAdmin, "request", fields, ringDoc("cmd", wire.AdminSeedingRetry, "database", "app", "slot", int64(5)))
	b.refusal("ring admin refused: a seeding status slot without a database", "A slot belongs to a database.", transport.OpRingAdmin, "request", fields, ringDoc("cmd", wire.AdminSeedingStatus, "slot", int32(5)))
	b.refusal("ring admin reply refused: an unknown seeding state", "state is placing, seeding, stalled, ready or retired.", transport.OpRingAdmin, "reply", replyFields,
		ringDoc("cmd", wire.AdminSeedingStatus, "rows", bson.A{bson.D{{Key: "database", Value: "app"}, {Key: "slot", Value: int32(1)}, {Key: "state", Value: "paused"}, {Key: "source", Value: ""},
			{Key: "bytes_done", Value: int64(0)}, {Key: "bytes_known", Value: int64(0)}, {Key: "entries", Value: int64(0)}, {Key: "idle_ms", Value: int64(0)}, {Key: "refusal", Value: ""}}}))
}

func ptrInt32(v int32) *int32 { return &v }

// expiredVectors is last, after the membership vectors, so the request ids of every earlier vector
// stay what they were. A keyed get of a document that exists but is past its expire time is a NOT_FOUND
// whose detail says so, so a driver tells it from a plain miss without reading the message.
func (b *ringBuilder) expiredVectors(encID []byte) {
	read := wire.RingKeyedRequest{Prologue: wire.RingPrologue{Epoch: 4, StateSeq: 9, DeadlineMs: 5000},
		Read: &wire.RingRead{Class: wire.RingClassGet, BucketID: 0x1f4a2c9b7de30015, DeclVersion: 1, SchemaVersionSeen: 1, EncID: encID}}
	getRequest := b.must(wire.EncodeRingKeyed(read))
	b.refused("ring keyed: get of an expired document", "NOT_FOUND carries the detail {expired: true} when the document exists but its expire time has passed: the owner treats it as absent until the sweep removes it. A plain miss has no detail. A driver acts on detail.expired, never on the text, and still treats both as not found.",
		transport.OpRingKeyed, nil, getRequest, wire.RingError{Code: hc.CodeNotFound, Message: "document \"ada\" is not in app.User. The document expired: its 'expire at' time has passed, so it reads as absent until the sweep removes it",
			Detail: &hc.Detail{Expired: true}}, "expired")
}
