package main

import (
	"encoding/binary"
	"slices"

	hc "github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// getManyNote is the sentence every RING_GET_MANY vector's notes carry.
const getManyNote = "Reads only; keys may differ in bucket; no atomicity, no snapshot."

// getManyVectors are the exchanges and refusals of RING_GET_MANY (0xE1, entry-ring-multiget-spec.md):
// one prologue, a count byte and that many keys, each a 2-byte big-endian length and a RING_KEYED
// get or get_fields body. The reply is a RING_REPLY with outcome and one answer per key in request order.
// Added after incVectors, so no earlier request id moves.
func (b *ringBuilder) getManyVectors(stamp func(wire.RingReply) wire.RingReply) {
	encode := func(text string) []byte {
		encoded, err := store.EncodeComponent(text)
		if err != nil {
			b.fail("encoding the document id %q: %v", text, err)
		}
		return encoded
	}
	prologue := wire.RingPrologue{Epoch: 4, StateSeq: 9, DeadlineMs: 5000}
	user := func(id string) wire.RingRead {
		return wire.RingRead{Class: wire.RingClassGet, BucketID: 0x1f4a2c9b7de30015, DeclVersion: 1, SchemaVersionSeen: 1, EncID: encode(id)}
	}
	post := func(id string, fields ...string) wire.RingRead {
		read := wire.RingRead{Class: wire.RingClassGet, BucketID: 0x1f4a2c9b7de30016, DeclVersion: 1, SchemaVersionSeen: 1, EncID: encode(id)}
		if len(fields) > 0 {
			read.Class, read.Fields = wire.RingClassGetFields, fields
		}
		return read
	}
	frame := func(reads ...wire.RingRead) []byte {
		return b.must(wire.EncodeRingGetMany(wire.RingGetManyRequest{Prologue: prologue, Reads: reads}))
	}
	found := true
	missing := false
	hit := func(id string) wire.RingAnswer {
		return wire.RingAnswer{Reply: &wire.RingReply{Found: &found, Elements: [][]byte{ringDoc("_id_", id, "Name", "Ada")}}}
	}
	miss := wire.RingAnswer{Reply: &wire.RingReply{Found: &missing}}
	answered := func(outcome string, answers ...wire.RingAnswer) []byte {
		return b.must(wire.EncodeRingReply(stamp(wire.RingReply{Outcome: outcome, Answers: answers})))
	}
	local := hit("ada")
	local.Reply.Served = "local"

	b.exchange("ring get-many: every key found, in request order", "RING_GET_MANY is binary: the 20-byte client prologue once, a count byte (1 to 255), then each key as a 2-byte big-endian length and the body of a RING_KEYED get (0x81) or get_fields (0x89) read. The reply is a RING_REPLY with outcome (ok when every key was answered with a reply, partial when any was answered with an error) and answers, one per key in request order: each is {reply: the bytes of a lone get reply} or {error: the bytes of an OpError payload}. "+getManyNote,
		transport.OpRingGetMany, nil, frame(user("ada"), user("grace")), ringReplyFields, answered("ok", hit("ada"), hit("grace")))
	b.exchange("ring get-many: two buckets, a miss and a get_fields", "Each body carries its own 40-byte header, so one frame names keys of different buckets (and databases). A document that is gone is found:false, an answer and not an error; the drivers' rule that a miss is a typed not-found error applies to a single-value read only. get_fields returns only the named scalars. A repeated key is answered twice and read once. "+getManyNote,
		transport.OpRingGetMany, nil, frame(user("ada"), post("p1"), user("nobody"), post("p2", "Name"), user("ada")), ringReplyFields,
		answered("ok", hit("ada"), hit("p1"), miss, hit("p2"), hit("ada")))
	b.exchange("ring get-many: a key answered from a ready copy says so", "local: true on a key (the options trailer of its body) lets a ready copy on the receiving node answer it; the answer then carries served_from local. Keys that are not local are read at their owner. "+getManyNote,
		transport.OpRingGetMany, nil, frame(withLocal(user("ada"))), ringReplyFields, answered("ok", local))
	denied := wire.RingError{Code: hc.CodePermissionDenied, Message: "this session cannot use that bucket: the user holds no role on the bucket's database that allows this operation. Nothing was done. Ask an administrator to run 'grant role reader@<database> to <user>' if the user should have access"}
	b.exchange("ring get-many: one key refused, the others answered", "Authorization is per key: a key on a bucket the session may not read answers the uniform PERMISSION_DENIED (it names neither the bucket nor whether it exists) and the outcome is partial. A refused key does not stop the others. "+getManyNote,
		transport.OpRingGetMany, nil, frame(user("ada"), post("p1"), user("grace")), ringReplyFields,
		answered("partial", hit("ada"), wire.RingAnswer{Error: &denied}, hit("grace")))
	tooLarge := wire.RingError{Code: hc.CodeResultTooLarge, Message: `key 1 of 3 (bucket "Post", id "p88") was not returned: its answer does not fit what is left of the 16 MiB reply. Reads change nothing: ask for it again in a smaller frame, or with GetFields naming only the fields you need`}
	b.exchange("ring get-many: a key over the reply budget", "The reply holds at most min(max_result_bytes, 16 MiB), charged as each found document arrives; found:false and error answers are always kept. A document that does not fit what is left answers RESULT_TOO_LARGE naming the key; a key not read because the reply was already full answers RESULT_TOO_LARGE too ('the 16 MiB reply was already full, so it was not read'); a document larger than the whole budget says 'Read this document with Get'. Ask again for such keys in a smaller frame. "+getManyNote,
		transport.OpRingGetMany, nil, frame(user("ada"), post("p88"), user("grace")), ringReplyFields,
		answered("partial", hit("ada"), wire.RingAnswer{Error: &tooLarge}, hit("grace")))
	unread := wire.RingError{Code: hc.CodeUnavailable, Message: "key 2 of 3 was not read: the frame's deadline of 5000 ms was spent. Reads change nothing, so resend it: in a smaller frame or with a longer deadline"}
	b.exchange("ring get-many: the deadline leaves a key not read", "deadline_ms is the frame's budget. A key not read when it is spent answers UNAVAILABLE ('was not read'); one cut off while it was being read answers UNAVAILABLE ('was cut off while it was being read'). Reads change nothing, so resending is safe. Which keys fall in the tail is not deterministic. "+getManyNote,
		transport.OpRingGetMany, nil, frame(user("ada"), user("grace"), user("linus")), ringReplyFields,
		answered("partial", hit("ada"), hit("grace"), wire.RingAnswer{Error: &unread}))

	valid := frame(user("ada"), post("p1"), user("grace"))
	refuseWhole := func(name, notes string, payload []byte) {
		_, err := wire.DecodeRingGetMany(payload)
		coded, ok := err.(*hc.Error)
		if !ok {
			b.fail("%s: the decoder's refusal is %T, want *cluster.Error", name, err)
			return
		}
		b.refusedWhole(name, notes+" A frame that does not decode is one OpError, INVALID_ARGUMENT, naming the key where decoding stopped; nothing was read. "+getManyNote,
			transport.OpRingGetMany, payload, wire.RingError{Code: coded.Code, Message: coded.Message}, "key")
	}
	// The key lengths sit after the prologue, the count byte and the earlier keys.
	keyStart := func(index int) int {
		at := wire.RingPrologueLen + 1
		for range index {
			at += 2 + int(binary.BigEndian.Uint16(valid[at:]))
		}
		return at
	}
	past := slices.Clone(valid)
	binary.BigEndian.PutUint16(past[keyStart(1):], 0x1000)
	refuseWhole("ring get-many: a key length past the bytes left is refused whole", "Each key is its 2-byte length and exactly that many bytes of body; the decoder never guesses where a body ends.", past)
	zeroLength := slices.Clone(valid)
	binary.BigEndian.PutUint16(zeroLength[keyStart(1):], 0)
	refuseWhole("ring get-many: a key length of zero is refused whole", "A body is at least its class byte, so a length of 0 is never a key.", zeroLength)
	zeroCount := slices.Clone(valid[:wire.RingPrologueLen+1])
	zeroCount[wire.RingPrologueLen] = 0
	refuseWhole("ring get-many: a count of zero is refused whole", "A frame carries 1 to 255 keys.", zeroCount)
	write := slices.Clone(valid)
	write[keyStart(2)+2] = hc.OpUpdate
	refuseWhole("ring get-many: a write class names its key", "Only get and get_fields are admitted; the refusal names the class and says to send writes in a RING_BATCH.", write)
	otherRead := slices.Clone(valid)
	otherRead[keyStart(1)+2] = wire.RingClassStoreList
	refuseWhole("ring get-many: a store read names its key", "A get-many reads whole documents; any other read class is refused naming the class.", otherRead)

	// Frames the strict decoder refuses, as refusal vectors.
	countAhead := slices.Clone(valid)
	countAhead[wire.RingPrologueLen] = 4
	b.refusal("ring get-many refused: a count past the keys", "The count says 4 and 3 keys follow.", transport.OpRingGetMany, "request", nil, countAhead)
	b.refusal("ring get-many refused: a trailing byte", "Nothing follows the last key.", transport.OpRingGetMany, "request", nil, padded(valid))
	b.refusal("ring get-many refused: truncated", "The last key is cut short.", transport.OpRingGetMany, "request", nil, truncated(valid))
	b.refusal("ring get-many refused: a key whose length ends inside its own body", "A length shorter than the body cuts the key short; the cut slice is decoded as a whole body and refused.", transport.OpRingGetMany, "request", nil, shortLength(valid, keyStart(0)))
	expand := withExpand(user("ada"))
	b.refusal("ring get-many refused: expand on a key", "A get-many does not expand; use GetExpanded for one document.", transport.OpRingGetMany, "request", nil, expandedFrame(b, prologue, expand))
}

// withLocal returns read with the local option.
func withLocal(read wire.RingRead) wire.RingRead {
	read.Options.Local = true
	return read
}

// withExpand returns read asking for an expansion, which a get-many refuses.
func withExpand(read wire.RingRead) wire.RingRead {
	read.Class = wire.RingClassGetFields
	read.Fields = []string{"Name"}
	read.Options.Expand = []wire.RingExpandSpec{{Field: "Name", Database: "app", Bucket: "Post"}}
	return read
}

// shortLength is frame with the length of the key at start made one byte too short.
func shortLength(frame []byte, start int) []byte {
	out := slices.Clone(frame)
	binary.BigEndian.PutUint16(out[start:], binary.BigEndian.Uint16(out[start:])-1)
	return out
}

// expandedFrame builds the frame by hand: the encoder refuses expand, which is the point.
func expandedFrame(b *ringBuilder, prologue wire.RingPrologue, read wire.RingRead) []byte {
	keyed, err := wire.EncodeRingKeyed(wire.RingKeyedRequest{Prologue: prologue, Read: &read})
	if err != nil {
		b.fail("encoding the expanded read: %v", err)
		return nil
	}
	lone := keyed[wire.RingPrologueLen:]
	out := binary.BigEndian.AppendUint64(nil, prologue.Epoch)
	out = binary.BigEndian.AppendUint64(out, prologue.StateSeq)
	out = binary.BigEndian.AppendUint32(out, prologue.DeadlineMs)
	out = append(out, 1)
	out = binary.BigEndian.AppendUint16(out, uint16(len(lone)))
	return append(out, lone...)
}
