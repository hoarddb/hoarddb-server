package wire

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
)

// u65DetailCases is the six shapes of queries Formats, "The error detail", with the
// document each must encode to.
func u65DetailCases() map[cluster.Code]struct {
	detail cluster.Detail
	doc    bson.D
} {
	return map[cluster.Code]struct {
		detail cluster.Detail
		doc    bson.D
	}{
		cluster.CodeNotOwner: {
			cluster.Detail{Owner: "node-b", OwnerAddr: "192.0.2.2:7001", Epoch: 42, Slot: 4312},
			bson.D{{Key: "owner", Value: "node-b"}, {Key: "owner_addr", Value: "192.0.2.2:7001"}, {Key: "epoch", Value: int64(42)}, {Key: "slot", Value: int32(4312)}},
		},
		cluster.CodeStaleEpoch: {
			cluster.Detail{Epoch: 43, Owner: "node-b", OwnerAddr: "192.0.2.2:7001"},
			bson.D{{Key: "epoch", Value: int64(43)}, {Key: "owner", Value: "node-b"}, {Key: "owner_addr", Value: "192.0.2.2:7001"}},
		},
		cluster.CodeOwnerDown: {
			cluster.Detail{Owner: "node-c", MissingSlots: 12},
			bson.D{{Key: "owner", Value: "node-c"}, {Key: "missing_slots", Value: int32(12)}},
		},
		cluster.CodeNoQuorum: {
			cluster.Detail{Voters: 3, Answered: 1},
			bson.D{{Key: "voters", Value: int32(3)}, {Key: "answered", Value: int32(1)}},
		},
		cluster.CodeLeaseLapsed: {
			cluster.Detail{Node: "node-a", LapsedMs: 1800},
			bson.D{{Key: "node", Value: "node-a"}, {Key: "lapsed_ms", Value: int64(1800)}},
		},
		cluster.CodeRFUnsupported: {
			cluster.Detail{Rf: 3, Voters: 2, DataMembers: 2},
			bson.D{{Key: "rf", Value: int32(3)}, {Key: "voters", Value: int32(2)}, {Key: "data_members", Value: int32(2)}},
		},
	}
}

func u65ErrorText(code cluster.Code) string {
	return "Bucket \"users\" is refused: " + code.String() + ". Run 'nodes' to see the cluster"
}

func u65EveryError(t *testing.T) []RingError {
	t.Helper()
	var all []RingError
	for _, code := range cluster.Codes() {
		all = append(all, RingError{Code: code, Message: u65ErrorText(code)})
	}
	for code, tc := range u65DetailCases() {
		detail := tc.detail
		all = append(all, RingError{Code: code, Message: u65ErrorText(code), Detail: &detail})
	}
	return all
}

func TestU65ErrorPayloadIsCodeMessageDetail(t *testing.T) {
	raw, err := EncodeRingError(RingError{Code: cluster.CodeNotFound, Message: "Bucket \"users\" not found"})
	if err != nil {
		t.Fatal(err)
	}
	want, _ := bson.Marshal(bson.D{{Key: "code", Value: int32(0x01)}, {Key: "message", Value: "Bucket \"users\" not found"}})
	if !bytes.Equal(raw, want) {
		t.Fatalf("payload\n got %x\nwant %x", raw, want)
	}
}

func TestU65ErrorDetailShapesFollowQueriesFormats(t *testing.T) {
	for code, tc := range u65DetailCases() {
		detail := tc.detail
		raw, err := EncodeRingError(RingError{Code: code, Message: "m", Detail: &detail})
		if err != nil {
			t.Fatalf("%s: %v", code, err)
		}
		want, err := bson.Marshal(bson.D{
			{Key: "code", Value: int32(code)},
			{Key: "message", Value: "m"},
			{Key: "detail", Value: tc.doc},
		})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(raw, want) {
			t.Fatalf("%s payload\n got %x\nwant %x", code, raw, want)
		}
		got, err := DecodeRingError(raw)
		if err != nil || got.Code != code || got.Message != "m" || got.Detail == nil || *got.Detail != tc.detail {
			t.Fatalf("%s decodes as %+v, %v", code, got, err)
		}
	}
}

func TestU65ErrorEveryCodeRoundTrips(t *testing.T) {
	for _, refusal := range u65EveryError(t) {
		raw, err := EncodeRingError(refusal)
		if err != nil {
			t.Fatalf("%s: %v", refusal.Code, err)
		}
		got, err := DecodeRingError(raw)
		if err != nil {
			t.Fatalf("%s: %v", refusal.Code, err)
		}
		again, err := EncodeRingError(got)
		if err != nil || !bytes.Equal(again, raw) {
			t.Fatalf("%s re-encodes as %x (%v), want %x", refusal.Code, again, err, raw)
		}
	}
}

func TestU65ErrorEncoderRefusals(t *testing.T) {
	owner := cluster.Detail{Owner: "node-b", OwnerAddr: "a:1", Epoch: 1, Slot: 2}
	cases := map[string]struct {
		refusal RingError
		said    string
	}{
		"unregistered code":                {RingError{Code: 0x99, Message: "m"}, "0x99"},
		"zero code":                        {RingError{Code: 0, Message: "m"}, "code"},
		"empty message":                    {RingError{Code: cluster.CodeNotFound}, "message"},
		"message over 64 KiB":              {RingError{Code: cluster.CodeNotFound, Message: strings.Repeat("m", 1<<16+1)}, "65536"},
		"detail on a code with none":       {RingError{Code: cluster.CodeNotFound, Message: "m", Detail: &owner}, "NOT_FOUND"},
		"empty detail on a code with none": {RingError{Code: cluster.CodeInvalidArgument, Message: "m", Detail: &cluster.Detail{}}, "no detail shape"},
		"member of another shape":          {RingError{Code: cluster.CodeNoQuorum, Message: "m", Detail: &cluster.Detail{Voters: 1, Answered: 1, Owner: "x"}}, "owner"},
		"owner empty":                      {RingError{Code: cluster.CodeNotOwner, Message: "m", Detail: &cluster.Detail{Slot: 2}}, "owner"},
		"slot past the last slot":          {RingError{Code: cluster.CodeNotOwner, Message: "m", Detail: &cluster.Detail{Owner: "n", Slot: 8192}}, "slot"},
		"epoch above int64":                {RingError{Code: cluster.CodeStaleEpoch, Message: "m", Detail: &cluster.Detail{Owner: "n", Epoch: math.MaxInt64 + 1}}, "epoch"},
		"negative voters":                  {RingError{Code: cluster.CodeNoQuorum, Message: "m", Detail: &cluster.Detail{Voters: -1}}, "voters"},
		"count above int32":                {RingError{Code: cluster.CodeRFUnsupported, Message: "m", Detail: &cluster.Detail{Rf: math.MaxInt32 + 1}}, "rf"},
		"address over 256 bytes":           {RingError{Code: cluster.CodeNotOwner, Message: "m", Detail: &cluster.Detail{Owner: "n", OwnerAddr: strings.Repeat("a", 257)}}, "owner_addr"},
	}
	for name, tc := range cases {
		_, err := EncodeRingError(tc.refusal)
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		u65AssertRefused(t, err, tc.said)
	}
}

func TestU65ErrorDecoderRefusesWhatIsNotCanonical(t *testing.T) {
	marshal := func(doc bson.D) []byte {
		raw, err := bson.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	code := func(c int32) bson.E { return bson.E{Key: "code", Value: c} }
	message := bson.E{Key: "message", Value: "m"}
	cases := map[string]struct {
		raw  []byte
		said string
	}{
		"code as int64":               {marshal(bson.D{{Key: "code", Value: int64(1)}, message}), "layout"},
		"message first":               {marshal(bson.D{message, code(1)}), "layout"},
		"no message":                  {marshal(bson.D{code(1)}), "message"},
		"extra field":                 {marshal(bson.D{code(1), message, {Key: "x", Value: int32(1)}}), "layout"},
		"code above 16 bits":          {marshal(bson.D{code(0x10001), message}), "outside"},
		"negative code":               {marshal(bson.D{code(-1), message}), "outside"},
		"unregistered code":           {marshal(bson.D{code(0x77), message}), "0x77"},
		"detail on INVALID_ARGUMENT":  {marshal(bson.D{code(int32(cluster.CodeInvalidArgument)), message, {Key: "detail", Value: bson.D{{Key: "voters", Value: int32(1)}}}}), "INVALID_ARGUMENT"},
		"detail of another shape":     {marshal(bson.D{code(int32(cluster.CodeNoQuorum)), message, {Key: "detail", Value: bson.D{{Key: "owner", Value: "n"}}}}), "layout"},
		"detail fields reordered":     {marshal(bson.D{code(int32(cluster.CodeNoQuorum)), message, {Key: "detail", Value: bson.D{{Key: "answered", Value: int32(1)}, {Key: "voters", Value: int32(3)}}}}), "layout"},
		"detail with a widened count": {marshal(bson.D{code(int32(cluster.CodeNoQuorum)), message, {Key: "detail", Value: bson.D{{Key: "voters", Value: int64(3)}, {Key: "answered", Value: int32(1)}}}}), "layout"},
		"empty detail":                {marshal(bson.D{code(int32(cluster.CodeNotOwner)), message, {Key: "detail", Value: bson.D{}}}), "owner"},
		"not a document":              {[]byte{9, 9}, "BSON"},
		"extra field nested too deep": {marshal(bson.D{code(1), message, {Key: "x", Value: bson.Raw(u65Nest(20))}}), "too deep"},
		"valid message past the cap":  {marshal(bson.D{code(1), {Key: "message", Value: strings.Repeat("m", maxRingErrorPayload)}}), "over the limit"},
	}
	for name, tc := range cases {
		_, err := DecodeRingError(tc.raw)
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		u65AssertRefused(t, err, tc.said)
	}
}

func TestU65ErrorDecoderSurvivesMutation(t *testing.T) {
	var seeds [][]byte
	for _, refusal := range u65EveryError(t) {
		raw, err := EncodeRingError(refusal)
		if err != nil {
			t.Fatal(err)
		}
		seeds = append(seeds, raw)
	}
	rng := rand.New(rand.NewPCG(67, 67))
	accepted := 0
	for range 100_000 {
		mutated := u65Mutate(rng, seeds[rng.IntN(len(seeds))])
		refusal, err := DecodeRingError(mutated)
		if err != nil {
			continue
		}
		accepted++
		again, err := EncodeRingError(refusal)
		if err != nil || !bytes.Equal(again, mutated) {
			t.Fatalf("accepted %x re-encodes as %x, %v", mutated, again, err)
		}
	}
	if accepted == 0 {
		t.Fatal("no mutated payload was accepted, so the re-encode check never ran")
	}
}

func TestU65ErrorDecodeAllocationIsBoundedByTheInput(t *testing.T) {
	lie := binary.LittleEndian.AppendUint32(nil, 0x7fffffff)
	lie = append(lie, 0x02, 'm', 'e', 's', 's', 'a', 'g', 'e', 0, 0xff, 0xff, 0xff, 0x7f, 'x', 0)
	detailLie := binary.LittleEndian.AppendUint32(nil, 0x7fffffff)
	detailLie = append(detailLie, 0x03, 'd', 'e', 't', 'a', 'i', 'l', 0, 0xff, 0xff, 0xff, 0x7f, 0)
	for _, input := range [][]byte{lie, detailLie} {
		if _, err := DecodeRingError(input); err == nil {
			t.Fatalf("lying payload %x was accepted", input)
		}
	}
	perOp := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = DecodeRingError(lie)
			_, _ = DecodeRingError(detailLie)
		}
	}).AllocedBytesPerOp()
	if perOp > 1<<14 {
		t.Fatalf("two lying payloads allocated %d bytes", perOp)
	}
	if _, err := DecodeRingError(make([]byte, 1<<17)); err == nil {
		t.Fatal("a payload past the error cap was accepted")
	}
}
