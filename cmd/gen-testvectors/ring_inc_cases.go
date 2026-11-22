package main

import (
	"github.com/hoarddb/hoarddb-bson/bson"

	hc "github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// incVectors are the exchanges of the inc store verb (verb 5): an inc that creates the element, and an
// inc that asks for the element back. Added after aggregateVectors, so no earlier request id moves.
func (b *ringBuilder) incVectors(stamp func(wire.RingReply) wire.RingReply) {
	encID, err := store.EncodeComponent("p1")
	if err != nil {
		b.err = err
		return
	}
	increments := b.must(bson.Marshal(bson.D{{Key: "Amount", Value: int32(1)}, {Key: "Tax", Value: int32(30)}}))
	plain := b.must(wire.IncArg("2026-11-05T14", increments, false))
	after := b.must(wire.IncArg("2026-11-05T14", increments, true))
	send := func(arg []byte) []byte {
		return b.must(wire.EncodeRingKeyed(wire.RingKeyedRequest{Prologue: wire.RingPrologue{Epoch: 4, StateSeq: 9, DeadlineMs: 5000},
			Op: hc.Op{BucketID: 0x1f4a2c9b7de30016, DeclVersion: 1, SchemaVersionSeen: 1, Class: hc.OpStoreVerb, EncID: encID,
				Fields: []hc.FieldOp{{FieldID: 3, Verb: hc.VerbInc, Value: arg}}}}))
	}
	copies := int32(1)
	position := hc.Position{Epoch: 4, Seq: 98215}
	b.exchange("ring store: inc creates an element", "Verb 5 on a hashStore or btreeStore adds to the numbers of the element under k, and creates the element when k is absent. The arg is {k: string, v: a BSON document of dotted paths (three levels at most) to int32, int64 or finite double amounts, r omitted}; an amount that is not a number, a decimal included, is INVALID_ARGUMENT. The reply is the ordinary write acknowledgement.",
		transport.OpRingKeyed, nil, send(plain), ringReplyFields,
		b.must(wire.EncodeRingReply(stamp(wire.RingReply{WriteOutcome: "applied", CopiesConfirmed: &copies, ServedBy: "node-a", Position: &position}))))
	found := true
	element := b.must(bson.Marshal(bson.D{{Key: "Amount", Value: int32(5)}, {Key: "Tax", Value: int32(90)}}))
	position.Seq++
	b.exchange("ring store: inc with return after", "The arg adds r: true (written only when true, so an arg has one byte form). The element as applied comes back in elements with found true; a retry with the same request id is answered the same element and nothing is added twice.",
		transport.OpRingKeyed, nil, send(after), ringReplyFields,
		b.must(wire.EncodeRingReply(stamp(wire.RingReply{WriteOutcome: "applied", CopiesConfirmed: &copies, ServedBy: "node-a", Position: &position, Found: &found, Elements: [][]byte{element}}))))
}
