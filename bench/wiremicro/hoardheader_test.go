package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/store"
)

func fakeAdminReply() wire.RingAdminReply {
	return wire.RingAdminReply{
		Cmd: wire.AdminCreateBucket, BucketID: 77, DeclVersion: 3, SchemaVersionSeen: 5,
		Fields: []wire.RingAdminReplyField{{ID: 1, Name: "_id_", Type: "string"}, {ID: 2, Name: "Payload", Type: "string"}},
	}
}

func TestBucketFromAdminReplyTakesTheHeaderValuesFromTheReply(t *testing.T) {
	bucket, err := bucketFromAdminReply(fakeAdminReply())
	if err != nil {
		t.Fatal(err)
	}
	if bucket.BucketID != 77 || bucket.DeclVersion != 3 || bucket.SchemaVersionSeen != 5 || bucket.PayloadFieldID != 2 {
		t.Fatalf("got %+v", bucket)
	}
}

func TestBucketFromAdminReplyRefusesAReplyWithoutThePayloadField(t *testing.T) {
	reply := fakeAdminReply()
	reply.Fields = reply.Fields[:1]
	if _, err := bucketFromAdminReply(reply); err == nil || !strings.Contains(err.Error(), "Payload") {
		t.Fatalf("err %v", err)
	}
	reply = fakeAdminReply()
	reply.BucketID = 0
	if _, err := bucketFromAdminReply(reply); err == nil {
		t.Fatal("a zero bucket id was accepted")
	}
}

func TestInsertOpCarriesHeaderIDAndPayload(t *testing.T) {
	bucket, _ := bucketFromAdminReply(fakeAdminReply())
	op, err := bucket.insertOp("0190-abc", "xyz")
	if err != nil {
		t.Fatal(err)
	}
	enc, _ := store.EncodeComponent("0190-abc")
	if op.Class != cluster.OpInsert || op.BucketID != 77 || op.DeclVersion != 3 || op.SchemaVersionSeen != 5 || !bytes.Equal(op.EncID, enc) {
		t.Fatalf("header %+v", op)
	}
	if !bytes.Equal(op.IDValue, typedString("0190-abc")) {
		t.Fatalf("id value % x", op.IDValue)
	}
	if len(op.Fields) != 1 || op.Fields[0].FieldID != 2 || !bytes.Equal(op.Fields[0].Value, typedString("xyz")) {
		t.Fatalf("fields %+v", op.Fields)
	}
}

func TestGetReadNamesTheBucketAndTheEncodedID(t *testing.T) {
	bucket, _ := bucketFromAdminReply(fakeAdminReply())
	read, err := bucket.getRead("k")
	if err != nil {
		t.Fatal(err)
	}
	enc, _ := store.EncodeComponent("k")
	if read.Class != wire.RingClassGet || read.BucketID != 77 || read.DeclVersion != 3 || read.SchemaVersionSeen != 5 || !bytes.Equal(read.EncID, enc) {
		t.Fatalf("read %+v", read)
	}
}

func TestTypedStringIsTagLengthTextNul(t *testing.T) {
	want := []byte{0x02, 4, 0, 0, 0, 'a', 'b', 'c', 0}
	if got := typedString("abc"); !bytes.Equal(got, want) {
		t.Fatalf("got % x want % x", got, want)
	}
}

func TestOnlyAnAppliedWriteCountsAsAnInsert(t *testing.T) {
	if err := requireApplied("a", wire.RingReply{WriteOutcome: "applied"}); err != nil {
		t.Fatal(err)
	}
	for _, outcome := range []string{"", "applied_degraded", "unknown"} {
		if err := requireApplied("a", wire.RingReply{WriteOutcome: outcome}); err == nil {
			t.Errorf("write_outcome %q was accepted", outcome)
		}
	}
}
