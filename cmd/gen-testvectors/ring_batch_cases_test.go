package main

import (
	"encoding/hex"
	"strings"
	"testing"

	hc "github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/transport"
)

// batchVector returns the committed ring group vector with this exact name.
func batchVector(t *testing.T, file vectorFile, name string) vector {
	t.Helper()
	for _, v := range file.Vectors {
		if v.Group == ringGroup && v.Name == name {
			return v
		}
	}
	t.Fatalf("no ring vector named %q: a driver has nothing to replay for that RING_BATCH case", name)
	return vector{}
}

func batchReply(t *testing.T, v vector) wire.RingReply {
	t.Helper()
	reply, err := wire.DecodeRingReply(ringVectorBytes(t, v.Name, v.Response.PayloadHex))
	if err != nil {
		t.Fatalf("vector %q: the reply does not decode: %v", v.Name, err)
	}
	return reply
}

// TestRingBatchVectorsCoverEveryCaseOfSpecSection7: [GUARD the RING_BATCH cases a driver must
// replay: mixed outcomes, whole-frame refusal, the 255 cap, a trimmed reply, a not-attempted
// tail and a batch of one equal to the lone request].
func TestRingBatchVectorsCoverEveryCaseOfSpecSection7(t *testing.T) {
	file, err := decodeVectorFile(committedVectors(t))
	if err != nil {
		t.Fatal(err)
	}

	mixed := batchReply(t, batchVector(t, file, "ring batch: three writes, one refused"))
	if mixed.Outcome != "partial" || len(mixed.Answers) != 3 || mixed.Answers[1].Error == nil {
		t.Errorf("mixed outcomes: outcome %q with %d answers, want partial and the middle one refused", mixed.Outcome, len(mixed.Answers))
	}
	whole := batchVector(t, file, "ring batch: a frame that does not decode is refused whole")
	if whole.Error == nil || whole.Error.Code != int(hc.CodeInvalidArgument) {
		t.Errorf("whole-frame refusal: error %+v, want INVALID_ARGUMENT", whole.Error)
	}

	frameCap := batchVector(t, file, "ring batch: 255 operations, the most a frame holds")
	capRequest, err := wire.DecodeRingBatch(ringVectorBytes(t, frameCap.Name, frameCap.Request.PayloadHex))
	if err != nil || len(capRequest.Ops) != 255 {
		t.Errorf("255 cap: request decodes to %d operations, error %v, want 255", len(capRequest.Ops), err)
	}
	if capReply := batchReply(t, frameCap); capReply.Outcome != "ok" || len(capReply.Answers) != 255 {
		t.Errorf("255 cap: outcome %q with %d answers, want ok and 255", capReply.Outcome, len(capReply.Answers))
	}

	trimmed := batchReply(t, batchVector(t, file, "ring batch: a reply over the budget drops a claim's elements, not the claim"))
	if len(trimmed.Answers) != 1 || trimmed.Answers[0].Reply == nil {
		t.Fatalf("budget trim: answers %+v, want one reply", trimmed.Answers)
	}
	if inner := trimmed.Answers[0].Reply; inner.WriteOutcome != "applied" || len(inner.Lease) != 16 || len(inner.Elements) != 0 {
		t.Errorf("budget trim: inner reply %+v, want applied, a 16-byte lease and no elements", inner)
	}

	tail := batchReply(t, batchVector(t, file, "ring batch: the deadline leaves a not-attempted tail"))
	if tail.Outcome != "partial" || len(tail.Answers) != 3 || tail.Answers[0].Reply == nil {
		t.Fatalf("not-attempted tail: outcome %q, answers %+v, want partial with the first applied", tail.Outcome, tail.Answers)
	}
	for _, answer := range tail.Answers[1:] {
		if answer.Error == nil || answer.Error.Code != hc.CodeUnavailable ||
			!strings.Contains(answer.Error.Message, "not attempted") || !strings.Contains(answer.Error.Message, "same request id") {
			t.Errorf("not-attempted tail: answer %+v, want UNAVAILABLE saying not attempted and to resend under the same request id", answer.Error)
		}
	}

	one := batchVector(t, file, "ring batch: a batch of one answers as the lone request would")
	lone := batchVector(t, file, "ring keyed: insert one document")
	oneReply := batchReply(t, one)
	if len(oneReply.Answers) != 1 || oneReply.Answers[0].Reply == nil {
		t.Fatalf("batch of one: answers %+v, want one reply", oneReply.Answers)
	}
	inner, err := wire.EncodeRingReply(*oneReply.Answers[0].Reply)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := hex.EncodeToString(inner), strings.ToLower(lone.Response.PayloadHex); got != want {
		t.Errorf("batch of one: the inner reply is not byte-equal to the lone insert's reply\n got %s\nwant %s", got, want)
	}
	batchRequest, err := wire.DecodeRingBatch(ringVectorBytes(t, one.Name, one.Request.PayloadHex))
	if err != nil || len(batchRequest.Ops) != 1 || one.Request.Opcode != uint8(transport.OpRingBatch) {
		t.Fatalf("batch of one: request %+v error %v", batchRequest, err)
	}
	loneRequest, err := wire.DecodeRingKeyed(ringVectorBytes(t, lone.Name, lone.Request.PayloadHex))
	if err != nil || loneRequest.Op.Class != batchRequest.Ops[0].Class || string(loneRequest.Op.IDValue) != string(batchRequest.Ops[0].IDValue) {
		t.Errorf("batch of one: the operation is not the lone insert's (error %v)", err)
	}
}
