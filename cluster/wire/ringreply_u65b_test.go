package wire

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

func u65bStoreListReply() RingReply {
	return RingReply{
		Found: u65Bool(true), Elements: [][]byte{{5, 0, 0, 0, 0}}, More: true, Cursor: []byte{1, 2, 3},
		Epoch: u65Int64(42), StateSeq: u65Int64(41),
	}
}

func u65bEveryReply() []RingReply {
	last := u65bStoreListReply()
	last.More, last.Cursor = false, nil
	return []RingReply{
		u65bStoreListReply(),
		last,
		{Count: u65Int64(0)},
		{Count: u65Int64(1 << 40), Epoch: u65Int64(1), StateSeq: u65Int64(2)},
	}
}

func TestU65bReplyCarriesCountMoreCursorInTheNoteOrder(t *testing.T) {
	raw := mustU65Reply(t, RingReply{
		Lease: u65Lease(), Count: u65Int64(7), More: true, Cursor: []byte{9, 9}, Deduped: true, Epoch: u65Int64(42),
	})
	want, err := bson.Marshal(bson.D{
		{Key: "lease", Value: u65Lease()},
		{Key: "count", Value: int64(7)},
		{Key: "more", Value: true},
		{Key: "cursor", Value: []byte{9, 9}},
		{Key: "deduped", Value: true},
		{Key: "epoch", Value: int64(42)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, want) {
		t.Fatalf("reply\n got %x\nwant %x", raw, want)
	}
}

func TestU65bReplyRoundTripsAndKeepsAPresentZeroCount(t *testing.T) {
	for i, reply := range u65bEveryReply() {
		raw := mustU65Reply(t, reply)
		got, err := DecodeRingReply(raw)
		if err != nil {
			t.Fatalf("reply %d: %v", i, err)
		}
		if (got.Count == nil) != (reply.Count == nil) || (got.Count != nil && *got.Count != *reply.Count) ||
			got.More != reply.More || !bytes.Equal(got.Cursor, reply.Cursor) {
			t.Fatalf("reply %d decoded as %+v, want %+v", i, got, reply)
		}
	}
	if got, err := DecodeRingReply(mustU65Reply(t, RingReply{Count: u65Int64(0)})); err != nil || got.Count == nil {
		t.Fatalf("a count of 0 must stay present: %+v, %v", got, err)
	}
}

func TestU65bReplyEncoderRefusals(t *testing.T) {
	for name, tc := range map[string]struct {
		reply RingReply
		said  string
	}{
		"negative count":    {RingReply{Count: u65Int64(-1)}, "count"},
		"cursor past 2 KiB": {RingReply{More: true, Cursor: make([]byte, ringMaxAfter+1)}, "cursor"},
	} {
		_, err := EncodeRingReply(tc.reply)
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		u65AssertRefused(t, err, tc.said)
	}
}

func TestU65bReplyDecoderRefusesWhatIsNotCanonical(t *testing.T) {
	marshal := func(doc bson.D) []byte {
		raw, err := bson.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	for name, tc := range map[string]struct {
		raw  []byte
		said string
	}{
		"count as int32":      {marshal(bson.D{{Key: "count", Value: int32(1)}}), "layout"},
		"negative count":      {marshal(bson.D{{Key: "count", Value: int64(-2)}}), "count"},
		"explicit false more": {marshal(bson.D{{Key: "more", Value: false}}), "layout"},
		"empty cursor":        {marshal(bson.D{{Key: "cursor", Value: []byte{}}}), "layout"},
		"more before count":   {marshal(bson.D{{Key: "more", Value: true}, {Key: "count", Value: int64(1)}}), "layout"},
		"cursor before more":  {marshal(bson.D{{Key: "cursor", Value: []byte{1}}, {Key: "more", Value: true}}), "layout"},
		"cursor past 2 KiB":   {marshal(bson.D{{Key: "more", Value: true}, {Key: "cursor", Value: make([]byte, ringMaxAfter+1)}}), "cursor"},
		"duplicate count":     {marshal(bson.D{{Key: "count", Value: int64(1)}, {Key: "count", Value: int64(1)}}), "layout"},
	} {
		_, err := DecodeRingReply(tc.raw)
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		u65AssertRefused(t, err, tc.said)
	}
}

func TestU65bReplyDecoderSurvivesMutation(t *testing.T) {
	var seeds [][]byte
	for _, reply := range u65bEveryReply() {
		seeds = append(seeds, mustU65Reply(t, reply))
	}
	rng := rand.New(rand.NewPCG(65, 2))
	accepted := 0
	for range 100_000 {
		mutated := u65Mutate(rng, seeds[rng.IntN(len(seeds))])
		reply, err := DecodeRingReply(mutated)
		if err != nil {
			continue
		}
		accepted++
		again, err := EncodeRingReply(reply)
		if err != nil || !bytes.Equal(again, mutated) {
			t.Fatalf("accepted %x re-encodes as %x, %v", mutated, again, err)
		}
	}
	if accepted == 0 {
		t.Fatal("no mutated reply was accepted, so the re-encode check never ran")
	}
}
