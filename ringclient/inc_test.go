package ringclient

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// counterClient dials a fake server for a bucket Meter { Name, Hits hashStore } whose keyed requests go to onKeyed.
func counterClient(t *testing.T, onKeyed func(conn *fakeConn, frame *transport.Frame, request wire.RingKeyedRequest)) *Client {
	t.Helper()
	var decl, schema atomic.Int64
	server := startFakeServer(t, func(conn *fakeConn, frame *transport.Frame) {
		if frame.Opcode == transport.OpRingAdmin {
			request, err := wire.DecodeRingAdmin(frame.Payload)
			if err != nil {
				t.Error(err)
				return
			}
			adminReplyFrame(t, conn, frame, wire.RingAdminReply{Cmd: request.Cmd, BucketID: 77, State: "active", DeclVersion: decl.Load(), SchemaVersionSeen: schema.Load(),
				Fields: []wire.RingAdminReplyField{{ID: 1, Name: "_id_", Type: "string"}, {ID: 2, Name: "Name", Type: "string"}, {ID: 3, Name: "Hits", Type: "hashStore"}}})
			return
		}
		request, err := wire.DecodeRingKeyed(frame.Payload)
		if err != nil {
			t.Error(err)
			return
		}
		onKeyed(conn, frame, request)
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// TestIncSendsTheCanonicalArg: [GUARD: sending a nested document instead of dotted keys]. The arg is the
// {k, v} document with the dotted paths as keys in sorted order, whatever order the map iterates in, and
// r only when the element is asked for back.
func TestIncSendsTheCanonicalArg(t *testing.T) {
	var seen atomic.Pointer[wire.RingKeyedRequest]
	client := counterClient(t, func(conn *fakeConn, frame *transport.Frame, request wire.RingKeyedRequest) {
		seen.Store(&request)
		reply, err := wire.EncodeRingReply(wire.RingReply{WriteOutcome: "applied"})
		if err != nil {
			t.Error(err)
			return
		}
		conn.reply(frame, reply)
	})
	meters := client.Bucket("db", "Meter")
	increments := map[string]any{"s": float64(30.5), "d.14.t": 1, "t": int32(1), "n": int64(-2)}
	for range 5 { // a map iterates in random order; the bytes must not
		if _, err := meters.Inc(testContext(t), "m1", "Hits", "2026-11-05T14", increments); err != nil {
			t.Fatal(err)
		}
		sent := seen.Load()
		if sent == nil || sent.Op.Class != cluster.OpStoreVerb || len(sent.Op.Fields) != 1 || sent.Op.Fields[0].Verb != cluster.VerbInc || sent.Op.Fields[0].FieldID != 3 {
			t.Fatalf("the request was %+v, want one inc on field id 3", sent)
		}
		want, err := wire.IncArg("2026-11-05T14", mustMarshal(t, bson.D{{Key: "d.14.t", Value: int64(1)}, {Key: "n", Value: int64(-2)}, {Key: "s", Value: 30.5}, {Key: "t", Value: int32(1)}}), false)
		if err != nil {
			t.Fatal(err)
		}
		if string(sent.Op.Fields[0].Value) != string(want) {
			t.Fatalf("the arg is %v, want %v", bson.Raw(sent.Op.Fields[0].Value), bson.Raw(want))
		}
	}
	for name, bad := range map[string]any{"a string": "1", "a decimal": primitive.NewDecimal128(0, 1), "an empty set": nil} {
		sendsBefore := seen.Load()
		increments := map[string]any{"n": bad}
		if name == "an empty set" {
			increments = map[string]any{}
		}
		_, err := meters.Inc(testContext(t), "m1", "Hits", "k", increments)
		if err == nil {
			t.Fatalf("an inc of %s was sent", name)
		}
		if name != "an empty set" && !strings.Contains(err.Error(), "integer minor units") {
			t.Fatalf("an inc of %s gave %v, want the refusal naming integer minor units", name, err)
		}
		if seen.Load() != sendsBefore {
			t.Fatalf("an inc of %s reached the server", name)
		}
	}
}

func mustMarshal(t *testing.T, doc bson.D) []byte {
	t.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestIncAfterReturnsTheElement: [GUARD: returning Found only]. IncAfter sends r and hands back the element
// the reply carries; Inc sends no r.
func TestIncAfterReturnsTheElement(t *testing.T) {
	element := mustMarshal(t, bson.D{{Key: "t", Value: int32(7)}})
	var sentReturn atomic.Int32
	client := counterClient(t, func(conn *fakeConn, frame *transport.Frame, request wire.RingKeyedRequest) {
		var arg struct {
			K string `bson:"k"`
			V []byte `bson:"v"`
			R bool   `bson:"r"`
		}
		if err := bson.Unmarshal(request.Op.Fields[0].Value, &arg); err != nil {
			t.Error(err)
			return
		}
		if arg.R {
			sentReturn.Add(1)
		}
		found := true
		reply, err := wire.EncodeRingReply(wire.RingReply{WriteOutcome: "applied", Found: &found, Elements: [][]byte{element}})
		if err != nil {
			t.Error(err)
			return
		}
		conn.reply(frame, reply)
	})
	meters := client.Bucket("db", "Meter")
	got, written, err := meters.IncAfter(testContext(t), "m1", "Hits", "k", map[string]any{"t": 1})
	if err != nil || string(got) != string(element) || !written.Applied() {
		t.Fatalf("IncAfter = %v, %+v, %v, want the element %v", got, written, err, bson.Raw(element))
	}
	if sentReturn.Load() != 1 {
		t.Fatal("IncAfter did not ask for the element back (r)")
	}
	if _, err := meters.Inc(testContext(t), "m1", "Hits", "k", map[string]any{"t": 1}); err != nil || sentReturn.Load() != 1 {
		t.Fatalf("Inc asked for the element back or failed: %v", err)
	}
}
