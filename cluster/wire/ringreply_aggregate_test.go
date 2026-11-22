package wire

import (
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// The aggregate member is a whole BSON document like expanded and document, so bytes that are not
// one are refused by the encoder and by the decoder.
func TestAMalformedAggregateMemberIsRefused(t *testing.T) {
	if _, err := EncodeRingReply(RingReply{Aggregate: []byte{1, 2, 3}}); err == nil {
		t.Error("EncodeRingReply accepted an aggregate that is not a BSON document")
	}
	raw, err := bson.Marshal(ringReplyDoc{Aggregate: []byte{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeRingReply(raw); err == nil {
		t.Error("DecodeRingReply accepted an aggregate that is not a BSON document")
	}
	good, err := bson.Marshal(bson.D{{Key: "sum", Value: int64(7)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := EncodeRingReply(RingReply{Aggregate: good}); err != nil {
		t.Errorf("EncodeRingReply refused a valid aggregate document: %v", err)
	}
}
