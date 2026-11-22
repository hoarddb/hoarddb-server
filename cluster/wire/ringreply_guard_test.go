package wire

import (
	"reflect"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
)

func mustRingGuardDocument(t *testing.T) []byte {
	t.Helper()
	doc, err := bson.Marshal(bson.D{
		{Key: "_id_", Value: "s1"},
		{Key: "Status", Value: "cancelled"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func TestRingReplyCarriesNotAppliedWithItsDocument(t *testing.T) {
	document := mustRingGuardDocument(t)
	cases := []struct {
		name  string
		reply RingReply
	}{
		{
			name: "a guard refused the write",
			reply: RingReply{
				WriteOutcome: outcomeNotApplied,
				ServedBy:     "node-a",
				Reason:       "guard",
				GuardField:   "Status",
				Document:     document,
			},
		},
		{
			name: "an applied reply returns the document it applied",
			reply: RingReply{
				WriteOutcome: outcomeApplied,
				ServedBy:     "node-a",
				Document:     document,
			},
		},
		{
			name: "an unknown outcome still carries the document of a durable write",
			reply: RingReply{
				WriteOutcome: outcomeUnknown,
				ServedBy:     "node-a",
				Position:     &cluster.Position{Epoch: 42, Seq: 55401},
				Document:     document,
			},
		},
		{
			name: "a deduped retry repeats no document",
			reply: RingReply{
				WriteOutcome:      outcomeAppliedDegraded,
				CopiesConfirmed:   u65Int32(0),
				Deduped:           true,
				DocumentTruncated: true,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := EncodeRingReply(tc.reply)
			if err != nil {
				t.Fatal(err)
			}
			got, err := DecodeRingReply(raw)
			if err != nil {
				t.Fatalf("decoding the reply it wrote: %v", err)
			}
			if !reflect.DeepEqual(got, tc.reply) {
				t.Fatalf("decoded as %+v, want %+v", got, tc.reply)
			}
		})
	}
}

func TestRingReplyRefusesNotAppliedWithAPosition(t *testing.T) {
	document := mustRingGuardDocument(t)
	cases := []struct {
		name  string
		reply RingReply
		said  []string
	}{
		{
			name: "not_applied with a position",
			reply: RingReply{
				WriteOutcome:    outcomeNotApplied,
				CopiesConfirmed: u65Int32(1),
				Reason:          "guard",
				GuardField:      "Status",
				Position:        &cluster.Position{Epoch: 42, Seq: 55401},
			},
			said: []string{"not_applied", "position", "send"},
		},
		{
			name:  "not_applied without a reason",
			reply: RingReply{WriteOutcome: outcomeNotApplied, GuardField: "Status"},
			said:  []string{"not_applied", "reason", "send"},
		},
		{
			name:  "reason on an applied reply",
			reply: RingReply{WriteOutcome: outcomeApplied, Reason: "guard"},
			said:  []string{"reason", "not_applied", "send"},
		},
		{
			name:  "document_truncated without deduped",
			reply: RingReply{WriteOutcome: outcomeApplied, DocumentTruncated: true},
			said:  []string{"document_truncated", "deduped", "get"},
		},
		{
			name:  "a document that is not BSON",
			reply: RingReply{WriteOutcome: outcomeApplied, Document: []byte{1, 2, 3}},
			said:  []string{"document", "BSON", "send"},
		},
		{
			name:  "guard_field without a reason",
			reply: RingReply{WriteOutcome: outcomeApplied, GuardField: "Status"},
			said:  []string{"guard_field", "reason", "send"},
		},
		{
			name:  "a document on a read reply",
			reply: RingReply{Found: u65Bool(true), Document: document},
			said:  []string{"document", "not_applied", "send"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := EncodeRingReply(tc.reply)
			u65AssertRefused(t, err, tc.said...)
		})
	}
}
