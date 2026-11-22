package wire

import (
	"bytes"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
)

func u67Filter(t *testing.T, doc bson.D) []byte {
	t.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func u67EveryList(t *testing.T) []RingListRequest {
	return []RingListRequest{
		{Database: "app", Bucket: "User"},
		{Database: "app", Bucket: "User", Limit: u65Int32(0)},
		{Database: "app", Bucket: "User", Limit: u65Int32(-3), Epoch: 7},
		{Database: "app", Bucket: "User", Filter: u67Filter(t, bson.D{{Key: "City", Value: "Oslo"}}), Limit: u65Int32(100), Cursor: bytes.Repeat([]byte{1}, 30), Epoch: 1 << 40},
		{Database: "app", Bucket: "User", Fields: []string{"Name", "Bio", "_id_"}},
		{Database: "app", Bucket: "User", Filter: u67Filter(t, bson.D{{Key: "City", Value: "Oslo"}}), Fields: []string{"Name"}, Limit: u65Int32(10), Cursor: []byte{7}, Epoch: 3},
		{Database: "app", Bucket: "User", Options: RingReadOptions{Local: true, Expand: []RingExpandSpec{{Field: "Author", Fields: []string{"Name"}, Where: u67Filter(t, bson.D{{Key: "City", Value: "Oslo"}})}}}},
	}
}

func u67EveryCount(t *testing.T) []RingCountRequest {
	return []RingCountRequest{
		{Database: "app", Bucket: "User"},
		{Database: "app", Bucket: "User", Filter: u67Filter(t, bson.D{{Key: "City", Value: "Oslo"}, {Key: "Age", Value: int32(3)}})},
	}
}

// TestU67ListAndCountRequestsRoundTrip: [GUARD the codec keeps every member].
func TestU67ListAndCountRequestsRoundTrip(t *testing.T) {
	for _, req := range u67EveryList(t) {
		raw, err := EncodeRingList(req)
		if err != nil {
			t.Fatalf("%+v: %v", req, err)
		}
		got, err := DecodeRingList(raw)
		if err != nil || !reflect.DeepEqual(got, req) {
			t.Fatalf("round trip of %+v = %+v, %v", req, got, err)
		}
	}
	for _, req := range u67EveryCount(t) {
		raw, err := EncodeRingCount(req)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeRingCount(raw)
		if err != nil || !reflect.DeepEqual(got, req) {
			t.Fatalf("round trip of %+v = %+v, %v", req, got, err)
		}
	}
}

// TestU67ListRequestRefusals: [GUARD each strict-decoder rule].
func TestU67ListRequestRefusals(t *testing.T) {
	good := u67EveryList(t)[3]
	for name, tc := range map[string]struct {
		doc  bson.D
		said string
	}{
		"no database":        {bson.D{{Key: "bucket", Value: "User"}}, "database"},
		"empty bucket":       {bson.D{{Key: "database", Value: "app"}, {Key: "bucket", Value: ""}}, "bucket"},
		"members reordered":  {bson.D{{Key: "bucket", Value: "User"}, {Key: "database", Value: "app"}}, "layout"},
		"extra member":       {bson.D{{Key: "database", Value: "app"}, {Key: "bucket", Value: "User"}, {Key: "sort", Value: "x"}}, "layout"},
		"limit as int64":     {bson.D{{Key: "database", Value: "app"}, {Key: "bucket", Value: "User"}, {Key: "limit", Value: int64(5)}}, "layout"},
		"epoch zero written": {bson.D{{Key: "database", Value: "app"}, {Key: "bucket", Value: "User"}, {Key: "epoch", Value: int64(0)}}, "layout"},
		"negative epoch":     {bson.D{{Key: "database", Value: "app"}, {Key: "bucket", Value: "User"}, {Key: "epoch", Value: int64(-1)}}, "epoch"},
		"empty cursor":       {bson.D{{Key: "database", Value: "app"}, {Key: "bucket", Value: "User"}, {Key: "cursor", Value: []byte{}}}, "layout"},
	} {
		raw := u67Filter(t, tc.doc)
		if _, err := DecodeRingList(raw); err == nil || !strings.Contains(err.Error(), tc.said) {
			t.Errorf("%s: DecodeRingList error = %v, want it to say %q", name, err, tc.said)
		}
	}
	raw, err := EncodeRingList(good)
	if err != nil {
		t.Fatal(err)
	}
	for name, mangled := range map[string][]byte{"trailing byte": append(bytes.Clone(raw), 0), "cut": raw[:len(raw)-3], "empty": nil} {
		if _, err := DecodeRingList(mangled); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	long := good
	long.Cursor = make([]byte, ringMaxAfter+1)
	if _, err := EncodeRingList(long); err == nil {
		t.Error("a cursor over the cap was encoded")
	}
	long = good
	long.Bucket = strings.Repeat("b", 1022)
	if _, err := EncodeRingList(long); err == nil {
		t.Error("a bucket name over 1021 bytes was encoded")
	}
	long = good
	long.Filter = []byte{1, 2, 3}
	if _, err := EncodeRingList(long); err == nil {
		t.Error("a filter that is not a BSON document was encoded")
	}
	if _, err := DecodeRingCount(u67Filter(t, bson.D{{Key: "database", Value: "app"}, {Key: "bucket", Value: "User"}, {Key: "limit", Value: int32(1)}})); err == nil {
		t.Error("a count with a limit was accepted: a limited count is a wrong number")
	}
}

func TestU67ListAndCountDecodersSurviveMutation(t *testing.T) {
	var lists, counts [][]byte
	for _, req := range u67EveryList(t) {
		raw, err := EncodeRingList(req)
		if err != nil {
			t.Fatal(err)
		}
		lists = append(lists, raw)
	}
	for _, req := range u67EveryCount(t) {
		raw, err := EncodeRingCount(req)
		if err != nil {
			t.Fatal(err)
		}
		counts = append(counts, raw)
	}
	rng := rand.New(rand.NewPCG(67, 67))
	var acceptedList, acceptedCount int
	for range 100_000 {
		mutated := u65Mutate(rng, lists[rng.IntN(len(lists))])
		if req, err := DecodeRingList(mutated); err == nil {
			acceptedList++
			if again, err := EncodeRingList(req); err != nil || !bytes.Equal(again, mutated) {
				t.Fatalf("accepted list %x re-encodes as %x, %v", mutated, again, err)
			}
		}
		mutated = u65Mutate(rng, counts[rng.IntN(len(counts))])
		if req, err := DecodeRingCount(mutated); err == nil {
			acceptedCount++
			if again, err := EncodeRingCount(req); err != nil || !bytes.Equal(again, mutated) {
				t.Fatalf("accepted count %x re-encodes as %x, %v", mutated, again, err)
			}
		}
	}
	if acceptedList == 0 || acceptedCount == 0 {
		t.Fatalf("accepted %d lists and %d counts: the re-encode check never ran", acceptedList, acceptedCount)
	}
}

func TestU67ListDecodeAllocationIsBoundedByTheInput(t *testing.T) {
	lie := []byte{0xff, 0xff, 0xff, 0x7f, 0x02, 'd', 'a', 't', 'a', 'b', 'a', 's', 'e', 0, 0xff, 0xff, 0xff, 0x7f, 0}
	perOp := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = DecodeRingList(lie)
			_, _ = DecodeRingCount(lie)
		}
	}).AllocedBytesPerOp()
	if perOp > 1<<14 {
		t.Fatalf("a lying request of %d bytes allocated %d bytes", len(lie), perOp)
	}
	if _, err := DecodeRingList(make([]byte, maxVerbArgBytes+ringMaxName)); err == nil {
		t.Fatal("an oversize request was accepted")
	}
}

// TestU67ReplyCarriesEntries: [GUARD the entries member is in the reply codec and checked].
func TestU67ReplyCarriesEntries(t *testing.T) {
	var blob []byte
	for slot := uint16(1); slot <= 3; slot++ {
		blob = cluster.AppendEntry(blob, cluster.ListEntry{Slot: slot, EncID: []byte{0x02, 'a', 0, 0}, Body: []byte{5, 0, 0, 0, 0}})
	}
	reply := RingReply{Entries: blob, More: true, Cursor: bytes.Repeat([]byte{9}, 30), Epoch: u65Int64(2), StateSeq: u65Int64(3)}
	raw, err := EncodeRingReply(reply)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeRingReply(raw)
	if err != nil || !bytes.Equal(got.Entries, blob) || !got.More || !bytes.Equal(got.Cursor, reply.Cursor) {
		t.Fatalf("round trip = %+v, %v", got, err)
	}
	for name, bad := range map[string]RingReply{
		"entries that do not parse": {Entries: []byte{0, 1, 2}},
	} {
		if _, err := EncodeRingReply(bad); err == nil {
			t.Errorf("%s: encoded", name)
		}
	}
	rng := rand.New(rand.NewPCG(68, 68))
	accepted := 0
	for range 100_000 {
		mutated := u65Mutate(rng, raw)
		if decoded, err := DecodeRingReply(mutated); err == nil {
			accepted++
			if again, err := EncodeRingReply(decoded); err != nil || !bytes.Equal(again, mutated) {
				t.Fatalf("accepted reply %x re-encodes as %x, %v", mutated, again, err)
			}
		}
	}
	if accepted == 0 {
		t.Fatal("no mutated reply was accepted")
	}
}
