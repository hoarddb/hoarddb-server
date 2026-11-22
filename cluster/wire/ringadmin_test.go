package wire

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
)

func u75Int32(v int32) *int32 { return &v }

func u75Requests() []RingAdminRequest {
	return []RingAdminRequest{
		{Cmd: AdminCreateDatabase, Name: "app"},
		{Cmd: AdminCreateDatabase, Name: "app", RF: u75Int32(1)},
		{Cmd: AdminCreateBucket, Database: "app", Name: "User", Fields: []RingAdminField{{Name: "Name", Type: "string"}, {Name: "Purchases", Type: "fifoStore"}, {Name: "Friends", Type: "hashStore"}}},
		{Cmd: AdminCreateBucket, Database: "app", Name: "Empty", Fields: []RingAdminField{}},
		{Cmd: AdminDescribeBucket, Database: "app", Bucket: "User"},
		{Cmd: AdminShowBuckets, Database: "app"},
		{Cmd: AdminShowBuckets, Database: "app", Limit: u75Int32(5), Cursor: []byte{1, 2, 3}},
	}
}

func u75Replies() []RingAdminReply {
	fields := []RingAdminReplyField{{ID: 1, Name: "_id_", Type: "any"}, {ID: 2, Name: "Name", Type: "string"}, {ID: 3, Name: "Purchases", Type: "fifoStore"}}
	return []RingAdminReply{
		{Cmd: AdminCreateDatabase, DatabaseID: 3, RF: 1, Storage: "disk"},
		{Cmd: AdminCreateDatabase, DatabaseID: 4, RF: 1, Storage: "memory"},
		{Cmd: AdminCreateBucket, BucketID: 0xf1f4a2c9b7de3001, State: "active", DeclVersion: 1, SchemaVersionSeen: 1, Fields: fields},
		{Cmd: AdminDescribeBucket, BucketID: 7, State: "active", DeclVersion: 4, SchemaVersionSeen: 2, Fields: fields[:1]},
		{Cmd: AdminShowBuckets, Buckets: []RingAdminListed{}},
		{Cmd: AdminShowBuckets, Buckets: []RingAdminListed{{"User", 9, "active"}, {"Order", 10, "dropping"}}, More: true, Cursor: []byte{4, 5}},
	}
}

// TestU75RingAdminRequestsRoundTrip: [GUARD every command is encoded and decoded to the same value].
func TestU75RingAdminRequestsRoundTrip(t *testing.T) {
	for _, req := range u75Requests() {
		raw, err := EncodeRingAdmin(req)
		if err != nil {
			t.Fatalf("%s: %v", req.Cmd, err)
		}
		got, err := DecodeRingAdmin(raw)
		if err != nil {
			t.Fatalf("%s: %v", req.Cmd, err)
		}
		again, err := EncodeRingAdmin(got)
		if err != nil || !bytes.Equal(again, raw) {
			t.Fatalf("%s: decoded %+v re-encodes as %x, %v", req.Cmd, got, again, err)
		}
		if got.Cmd != req.Cmd || got.Database != req.Database || got.Name != req.Name || got.Bucket != req.Bucket || len(got.Fields) != len(req.Fields) {
			t.Fatalf("%s: round trip gave %+v, want %+v", req.Cmd, got, req)
		}
	}
}

// TestU75RingAdminRepliesRoundTrip: [GUARD every reply shape is encoded and decoded to the same value].
func TestU75RingAdminRepliesRoundTrip(t *testing.T) {
	for _, reply := range u75Replies() {
		raw, err := EncodeRingAdminReply(reply)
		if err != nil {
			t.Fatalf("%s: %v", reply.Cmd, err)
		}
		got, err := DecodeRingAdminReply(raw)
		if err != nil {
			t.Fatalf("%s: %v", reply.Cmd, err)
		}
		if got.Cmd != reply.Cmd || got.BucketID != reply.BucketID || got.DeclVersion != reply.DeclVersion || got.SchemaVersionSeen != reply.SchemaVersionSeen ||
			len(got.Fields) != len(reply.Fields) || len(got.Buckets) != len(reply.Buckets) || got.More != reply.More || got.DatabaseID != reply.DatabaseID {
			t.Fatalf("%s: round trip gave %+v, want %+v", reply.Cmd, got, reply)
		}
		for i := range reply.Fields {
			if got.Fields[i] != reply.Fields[i] {
				t.Fatalf("field %d: %+v, want %+v", i, got.Fields[i], reply.Fields[i])
			}
		}
		again, err := EncodeRingAdminReply(got)
		if err != nil || !bytes.Equal(again, raw) {
			t.Fatalf("%s: decoded reply re-encodes as %x, %v", reply.Cmd, again, err)
		}
	}
}

// TestU75RingAdminRefusalsNameTheFieldAndTheFix: [GUARD each limit is checked and the text
// names the member and what to send instead].
func TestU75RingAdminRefusalsNameTheFieldAndTheFix(t *testing.T) {
	long := strings.Repeat("x", 1022)
	many := make([]RingAdminField, maxUserFields+1)
	for i := range many {
		many[i] = RingAdminField{Name: fmt.Sprintf("f%d", i), Type: "string"}
	}
	cases := []struct {
		name string
		req  RingAdminRequest
		want []string
	}{
		{"unknown cmd", RingAdminRequest{Cmd: "drop_database", Name: "app"}, []string{"cmd", "create_database", "show_buckets"}},
		{"empty database name", RingAdminRequest{Cmd: AdminCreateDatabase}, []string{"name", "1021"}},
		{"long bucket name", RingAdminRequest{Cmd: AdminCreateBucket, Database: "app", Name: long, Fields: []RingAdminField{}}, []string{"name", "1021"}},
		{"control character", RingAdminRequest{Cmd: AdminCreateDatabase, Name: "a\x00b"}, []string{"name", "control character"}},
		{"rf below one", RingAdminRequest{Cmd: AdminCreateDatabase, Name: "app", RF: u75Int32(0)}, []string{"rf", "1 or more", "create_database"}},
		{"synonym type", RingAdminRequest{Cmd: AdminCreateBucket, Database: "app", Name: "B", Fields: []RingAdminField{{Name: "Age", Type: "integer"}}}, []string{"Age", "integer", "synonym", `"int"`}},
		{"lowercase store word", RingAdminRequest{Cmd: AdminCreateBucket, Database: "app", Name: "B", Fields: []RingAdminField{{Name: "Q", Type: "fifostore"}}}, []string{"fifostore", "fifoStore"}},
		{"parameterized type", RingAdminRequest{Cmd: AdminCreateBucket, Database: "app", Name: "B", Fields: []RingAdminField{{Name: "A", Type: "array<int>"}}}, []string{"array<int>", "array"}},
		{"empty field name", RingAdminRequest{Cmd: AdminCreateBucket, Database: "app", Name: "B", Fields: []RingAdminField{{Name: "", Type: "string"}}}, []string{"field 0", "name"}},
		{"long field name", RingAdminRequest{Cmd: AdminCreateBucket, Database: "app", Name: "B", Fields: []RingAdminField{{Name: strings.Repeat("n", 256), Type: "string"}}}, []string{"255"}},
		{"too many fields", RingAdminRequest{Cmd: AdminCreateBucket, Database: "app", Name: "B", Fields: many}, []string{"999", "fields"}},
		{"limit below zero", RingAdminRequest{Cmd: AdminShowBuckets, Database: "app", Limit: u75Int32(-1)}, []string{"limit"}},
		{"long cursor", RingAdminRequest{Cmd: AdminShowBuckets, Database: "app", Cursor: make([]byte, ringMaxAfter+1)}, []string{"cursor", fmt.Sprint(ringMaxAfter)}},
		{"stray member", RingAdminRequest{Cmd: AdminDescribeBucket, Database: "app", Bucket: "B", Name: "x"}, []string{"describe_bucket", "name"}},
	}
	for _, tc := range cases {
		_, err := EncodeRingAdmin(tc.req)
		var coded *cluster.Error
		if err == nil || !u75Coded(err, &coded) || coded.Code != cluster.CodeInvalidArgument {
			t.Errorf("%s: encoded or wrong error: %v", tc.name, err)
			continue
		}
		for _, word := range tc.want {
			if !strings.Contains(coded.Message, word) {
				t.Errorf("%s: the refusal %q does not mention %q", tc.name, coded.Message, word)
			}
		}
	}
}

func u75Coded(err error, out **cluster.Error) bool {
	coded, ok := err.(*cluster.Error)
	if ok {
		*out = coded
	}
	return ok
}

// TestU75RingAdminDecoderIsStrict: [GUARD canonical layout: order, extras, written defaults,
// wrong types, size cap before decoding].
func TestU75RingAdminDecoderIsStrict(t *testing.T) {
	doc := func(elements ...bson.E) []byte {
		raw, err := bson.Marshal(bson.D(elements))
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	good := bson.E{Key: "cmd", Value: AdminCreateDatabase}
	cases := map[string][]byte{
		"empty":             nil,
		"extra member":      doc(good, bson.E{Key: "name", Value: "app"}, bson.E{Key: "extra", Value: int32(1)}),
		"reordered":         doc(bson.E{Key: "name", Value: "app"}, good),
		"duplicate":         doc(good, bson.E{Key: "name", Value: "app"}, bson.E{Key: "name", Value: "app"}),
		"rf as int64":       doc(good, bson.E{Key: "name", Value: "app"}, bson.E{Key: "rf", Value: int64(1)}),
		"name as int":       doc(good, bson.E{Key: "name", Value: int32(4)}),
		"cmd as int":        doc(bson.E{Key: "cmd", Value: int32(1)}, bson.E{Key: "name", Value: "app"}),
		"no cmd":            doc(bson.E{Key: "name", Value: "app"}),
		"limit as int64":    doc(bson.E{Key: "cmd", Value: AdminShowBuckets}, bson.E{Key: "database", Value: "app"}, bson.E{Key: "limit", Value: int64(5)}),
		"explicit empty":    doc(bson.E{Key: "cmd", Value: AdminShowBuckets}, bson.E{Key: "database", Value: "app"}, bson.E{Key: "cursor", Value: []byte{}}),
		"fields not array":  doc(bson.E{Key: "cmd", Value: AdminCreateBucket}, bson.E{Key: "database", Value: "app"}, bson.E{Key: "name", Value: "B"}, bson.E{Key: "fields", Value: "x"}),
		"oversize document": append(doc(good, bson.E{Key: "name", Value: "app"}), make([]byte, 2<<20)...),
	}
	for name, raw := range cases {
		if _, err := DecodeRingAdmin(raw); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// TestU75RingAdminDecodersSurviveMutation: [GUARD 100000 mutations: no panic, an accepted
// body re-encodes to the same bytes].
func TestU75RingAdminDecodersSurviveMutation(t *testing.T) {
	var requests, replies [][]byte
	for _, req := range u75Requests() {
		raw, err := EncodeRingAdmin(req)
		if err != nil {
			t.Fatal(err)
		}
		requests = append(requests, raw)
	}
	for _, reply := range u75Replies() {
		raw, err := EncodeRingAdminReply(reply)
		if err != nil {
			t.Fatal(err)
		}
		replies = append(replies, raw)
	}
	rng := rand.New(rand.NewPCG(75, 75))
	var acceptedRequests, acceptedReplies int
	for range 100_000 {
		mutated := u65Mutate(rng, requests[rng.IntN(len(requests))])
		if req, err := DecodeRingAdmin(mutated); err == nil {
			acceptedRequests++
			if again, err := EncodeRingAdmin(req); err != nil || !bytes.Equal(again, mutated) {
				t.Fatalf("accepted request %x re-encodes as %x, %v", mutated, again, err)
			}
		}
		mutated = u65Mutate(rng, replies[rng.IntN(len(replies))])
		if reply, err := DecodeRingAdminReply(mutated); err == nil {
			acceptedReplies++
			if again, err := EncodeRingAdminReply(reply); err != nil || !bytes.Equal(again, mutated) {
				t.Fatalf("accepted reply %x re-encodes as %x, %v", mutated, again, err)
			}
		}
	}
	if acceptedRequests == 0 || acceptedReplies == 0 {
		t.Fatalf("accepted %d requests and %d replies: the re-encode check never ran", acceptedRequests, acceptedReplies)
	}
}

// TestU75RingAdminDecodeAllocationIsBoundedByTheInput: [GUARD lengths are checked before
// allocating].
func TestU75RingAdminDecodeAllocationIsBoundedByTheInput(t *testing.T) {
	lie := []byte{0xff, 0xff, 0xff, 0x7f, 0x02, 'c', 'm', 'd', 0, 0xff, 0xff, 0xff, 0x7f, 0}
	perOp := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = DecodeRingAdmin(lie)
			_, _ = DecodeRingAdminReply(lie)
		}
	}).AllocedBytesPerOp()
	if perOp > 1<<14 {
		t.Fatalf("a lying body of %d bytes allocated %d bytes", len(lie), perOp)
	}
}

// ExampleEncodeRingAdmin builds the create_bucket body of the spec's example and reads it back.
func ExampleEncodeRingAdmin() {
	body, err := EncodeRingAdmin(RingAdminRequest{Cmd: AdminCreateBucket, Database: "app", Name: "User",
		Fields: []RingAdminField{{Name: "Name", Type: "string"}, {Name: "Purchases", Type: "fifoStore"}}})
	if err != nil {
		fmt.Println("encode:", err)
		return
	}
	req, err := DecodeRingAdmin(body)
	if err != nil {
		fmt.Println("decode:", err)
		return
	}
	fmt.Println(req.Cmd, req.Database, req.Name, len(req.Fields))
	// Output: create_bucket app User 2
}
