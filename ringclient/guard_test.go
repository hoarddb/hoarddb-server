package ringclient

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// guardClient dials a fake server whose keyed requests are handed to onKeyed.
func guardClient(t *testing.T, onKeyed func(conn *fakeConn, frame *transport.Frame, request wire.RingKeyedRequest)) *Client {
	t.Helper()
	var decl, schema atomic.Int64
	describe := describeHandler(t, &decl, &schema)
	server := startFakeServer(t, func(conn *fakeConn, frame *transport.Frame) {
		if frame.Opcode != transport.OpRingKeyed {
			describe(conn, frame)
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

// A guard names fields, and the request carries their ids; the refusal reaches the caller
// as a NotApplied result with the owner's document, and an undeclared name is refused
// before anything is sent.
func TestUpdateGuardNamesResolveAndRefuse(t *testing.T) {
	var seen atomic.Pointer[wire.RingKeyedRequest]
	current, err := bson.Marshal(bson.D{{Key: "_id_", Value: "ada"}, {Key: "Name", Value: "Bob"}})
	if err != nil {
		t.Fatal(err)
	}
	client := guardClient(t, func(conn *fakeConn, frame *transport.Frame, request wire.RingKeyedRequest) {
		seen.Store(&request)
		reply, err := wire.EncodeRingReply(wire.RingReply{WriteOutcome: "not_applied", Reason: "guard", GuardField: "Name", Document: current})
		if err != nil {
			t.Error(err)
			return
		}
		conn.reply(frame, reply)
	})
	users := client.Bucket("db", "User")
	result, err := users.Update(testContext(t), "ada", Update{Set: bson.D{{Key: "Name", Value: "Eve"}}},
		If(Guard{{Key: "Name", Value: "Ada"}}), Return(ReturnAfter))
	if err != nil {
		t.Fatal(err)
	}
	if result.Applied() || !result.NotApplied || result.Reason != "guard" || result.GuardField != "Name" || string(result.Document) != string(current) {
		t.Fatalf("result = %+v, want a NotApplied guard refusal on Name with the current document", result)
	}
	sent := seen.Load()
	if sent == nil || len(sent.Op.Guard) != 1 || sent.Op.Guard[0].FieldID != 2 || sent.Op.Return != cluster.ReturnAfter {
		t.Fatalf("the request carried guard %+v return %d, want one term on field id 2 and return after", sent, sent.Op.Return)
	}
	_, err = users.Update(testContext(t), "ada", Update{Set: bson.D{{Key: "Name", Value: "Eve"}}}, If(Guard{{Key: "Nope", Value: 1}}))
	if err == nil || !strings.Contains(err.Error(), "Nope") || !strings.Contains(err.Error(), "Name string") {
		t.Fatalf("an undeclared guard field gave %v, want it named with the declared scalars", err)
	}
}

func TestGuardOnAStoreFieldIsRefused(t *testing.T) {
	var sentAny atomic.Bool
	client := guardClient(t, func(conn *fakeConn, frame *transport.Frame, _ wire.RingKeyedRequest) {
		sentAny.Store(true)
		reply, _ := wire.EncodeRingReply(wire.RingReply{WriteOutcome: "applied"})
		conn.reply(frame, reply)
	})
	users := client.Bucket("db", "User")
	_, err := users.Update(testContext(t), "ada", Update{Set: bson.D{{Key: "Name", Value: "Eve"}}}, If(Guard{{Key: "Purchases", Value: 1}}))
	if err == nil || !strings.Contains(err.Error(), "Purchases") || !strings.Contains(err.Error(), "claim or ack") {
		t.Fatalf("a guard on a store gave %v, want it refused with the store named", err)
	}
	var coded *Error
	if !errors.As(err, &coded) || coded.Code != cluster.CodeFieldType {
		t.Errorf("a guard on a store gave %T %v, want an *Error with code FIELD_TYPE", err, err)
	}
	_, err = users.Update(testContext(t), "ada", Update{Set: bson.D{{Key: "Name", Value: "Eve"}}}, If(Guard{{Key: "Nope", Value: 1}}))
	if !errors.As(err, &coded) || coded.Code != cluster.CodeFieldType {
		t.Errorf("a guard on an undeclared field gave %T %v, want an *Error with code FIELD_TYPE", err, err)
	}
	if _, err := users.Delete(testContext(t), "ada", Return(ReturnAfter)); err == nil || !strings.Contains(err.Error(), "ReturnBefore") {
		t.Fatalf("return after on a delete gave %v, want it refused", err)
	}
	if sentAny.Load() {
		t.Fatal("a refused guard still sent a request")
	}
}

// An Upsert or Insert has no document to guard and returns no document, so If and Return
// are refused before anything is sent instead of being dropped.
func TestUpsertAndInsertRefuseIfAndReturn(t *testing.T) {
	var sent atomic.Int32
	client := guardClient(t, func(conn *fakeConn, frame *transport.Frame, request wire.RingKeyedRequest) {
		sent.Add(1)
		reply, err := wire.EncodeRingReply(wire.RingReply{WriteOutcome: "applied"})
		if err != nil {
			t.Error(err)
			return
		}
		conn.reply(frame, reply)
	})
	users := client.Bucket("db", "User")
	ctx := testContext(t)
	for name, call := range map[string]func() error{
		"upsert if": func() error {
			_, err := users.Upsert(ctx, "ada", Doc{"Name": "Eve"}, If(Guard{{Key: "Name", Value: "Ada"}}))
			return err
		},
		"upsert return": func() error {
			_, err := users.Upsert(ctx, "ada", Doc{"Name": "Eve"}, Return(ReturnAfter))
			return err
		},
		"insert if": func() error {
			_, err := users.Insert(ctx, "ada", Doc{"Name": "Eve"}, If(Guard{{Key: "Name", Value: "Ada"}}))
			return err
		},
	} {
		err := call()
		if err == nil || !strings.Contains(err.Error(), "INVALID_ARGUMENT") || !strings.Contains(err.Error(), "Update") {
			t.Errorf("%s: want an INVALID_ARGUMENT error naming Update, got %v", name, err)
		}
	}
	if sent.Load() != 0 {
		t.Errorf("a refused call must send nothing, sent %d requests", sent.Load())
	}
	if _, err := users.Upsert(ctx, "ada", Doc{"Name": "Eve"}); err != nil || sent.Load() != 1 {
		t.Errorf("a plain upsert must still be sent: err=%v sent=%d", err, sent.Load())
	}
}

// A guard past the cap and a return after on a delete are refusals the caller can match: an
// *Error with INVALID_ARGUMENT, naming the cap or the fix, and nothing is sent.
func TestGuardRefusalsAreTypedInvalidArgument(t *testing.T) {
	var sentAny atomic.Bool
	client := guardClient(t, func(conn *fakeConn, frame *transport.Frame, _ wire.RingKeyedRequest) {
		sentAny.Store(true)
		reply, _ := wire.EncodeRingReply(wire.RingReply{WriteOutcome: "applied"})
		conn.reply(frame, reply)
	})
	users := client.Bucket("db", "User")
	var coded *Error
	_, err := users.Delete(testContext(t), "ada", Return(ReturnAfter))
	if !errors.As(err, &coded) || coded.Code != cluster.CodeInvalidArgument || !strings.Contains(coded.Message, "ReturnBefore") {
		t.Errorf("return after on a delete gave %T %v, want an *Error with code INVALID_ARGUMENT naming ReturnBefore", err, err)
	}
	tooMany := make(Guard, 0, cluster.MaxGuardTerms+1)
	for range cluster.MaxGuardTerms + 1 {
		tooMany = append(tooMany, bson.E{Key: "Name", Value: "Ada"})
	}
	_, err = users.Update(testContext(t), "ada", Update{Set: bson.D{{Key: "Name", Value: "Eve"}}}, If(tooMany))
	if !errors.As(err, &coded) || coded.Code != cluster.CodeInvalidArgument || !strings.Contains(coded.Message, "cluster.MaxGuardTerms") {
		t.Errorf("a guard of %d terms gave %T %v, want an *Error with code INVALID_ARGUMENT naming the cap", cluster.MaxGuardTerms+1, err, err)
	}
	if sentAny.Load() {
		t.Fatal("a refused guard or return still sent a request")
	}
}
