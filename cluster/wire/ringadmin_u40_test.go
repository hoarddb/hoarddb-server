package wire

import (
	"bytes"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
)

func u40Requests() []RingAdminRequest {
	return []RingAdminRequest{
		{Cmd: AdminAlterBucket, Database: "app", Bucket: "User", Add: []RingAdminField{{Name: "Email", Type: "string"}, {Name: "Orders", Type: "fifoStore"}}, Drop: []string{}},
		{Cmd: AdminAlterBucket, Database: "app", Bucket: "User", Add: []RingAdminField{}, Drop: []string{"Age", "City"}},
		{Cmd: AdminAlterBucket, Database: "app", Bucket: "User", Add: []RingAdminField{{Name: "Email", Type: "string"}}, Drop: []string{"Age"}},
	}
}

func u40Reply() RingAdminReply {
	return RingAdminReply{Cmd: AdminAlterBucket, BucketID: 0xf1f4a2c9b7de3001, State: "active", DeclVersion: 2, SchemaVersionSeen: 2,
		Fields: []RingAdminReplyField{{ID: 1, Name: "_id_", Type: "any"}, {ID: 2, Name: "Name", Type: "string"}, {ID: 6, Name: "Email", Type: "string"}}}
}

// TestU40AlterBucketRoundTrips: an alter_bucket request and its reply decode to the same value
// and re-encode to the same bytes.
func TestU40AlterBucketRoundTrips(t *testing.T) {
	for _, req := range u40Requests() {
		raw, err := EncodeRingAdmin(req)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeRingAdmin(raw)
		if err != nil {
			t.Fatal(err)
		}
		if got.Cmd != req.Cmd || got.Database != req.Database || got.Bucket != req.Bucket ||
			len(got.Add) != len(req.Add) || len(got.Drop) != len(req.Drop) || got.Fields != nil {
			t.Fatalf("round trip gave %+v, want %+v", got, req)
		}
		if again, err := EncodeRingAdmin(got); err != nil || !bytes.Equal(again, raw) {
			t.Fatalf("re-encode: %v", err)
		}
	}
	raw, err := EncodeRingAdminReply(u40Reply())
	if err != nil {
		t.Fatal(err)
	}
	reply, err := DecodeRingAdminReply(raw)
	if err != nil || reply.Cmd != AdminAlterBucket || reply.DeclVersion != 2 || len(reply.Fields) != 3 {
		t.Fatalf("reply = %+v, %v", reply, err)
	}
}

// TestU40AlterBucketRefusalsNameTheFix: each malformed request is INVALID_ARGUMENT naming the
// member and what to send.
func TestU40AlterBucketRefusalsNameTheFix(t *testing.T) {
	cases := []struct {
		name string
		req  RingAdminRequest
		want string
	}{
		{"no change", RingAdminRequest{Cmd: AdminAlterBucket, Database: "app", Bucket: "B", Add: []RingAdminField{}, Drop: []string{}}, "there is nothing to change"},
		{"add missing", RingAdminRequest{Cmd: AdminAlterBucket, Database: "app", Bucket: "B", Drop: []string{"x"}}, "add and drop are both required"},
		{"drop missing", RingAdminRequest{Cmd: AdminAlterBucket, Database: "app", Bucket: "B", Add: []RingAdminField{{Name: "x", Type: "int"}}}, "add and drop are both required"},
		{"a synonym", RingAdminRequest{Cmd: AdminAlterBucket, Database: "app", Bucket: "B", Add: []RingAdminField{{Name: "x", Type: "integer"}}, Drop: []string{}}, `alter_bucket: field 0 (x) has type "integer", a synonym`},
		{"an empty drop", RingAdminRequest{Cmd: AdminAlterBucket, Database: "app", Bucket: "B", Add: []RingAdminField{}, Drop: []string{""}}, "drop 0 is a name of 0 bytes"},
		{"a control byte", RingAdminRequest{Cmd: AdminAlterBucket, Database: "app", Bucket: "B", Add: []RingAdminField{}, Drop: []string{"a\x00"}}, "drop 0 is not printable UTF-8"},
		{"a stray name", RingAdminRequest{Cmd: AdminAlterBucket, Database: "app", Bucket: "B", Name: "x", Add: []RingAdminField{{Name: "x", Type: "int"}}, Drop: []string{}}, "takes only database bucket add drop and cmd, and this request also sets name"},
		{"no database", RingAdminRequest{Cmd: AdminAlterBucket, Bucket: "B", Add: []RingAdminField{{Name: "x", Type: "int"}}, Drop: []string{}}, "database is 0 bytes"},
		{"fields on a describe", RingAdminRequest{Cmd: AdminDescribeBucket, Database: "app", Bucket: "B", Add: []RingAdminField{}}, "also sets add"},
	}
	for _, c := range cases {
		_, err := EncodeRingAdmin(c.req)
		var refusal *cluster.Error
		if err == nil || !asRefusal(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want INVALID_ARGUMENT containing %q", c.name, err, c.want)
		}
	}
}

func asRefusal(err error, target **cluster.Error) bool {
	for err != nil {
		if coded, ok := err.(*cluster.Error); ok {
			*target = coded
			return true
		}
		unwrapped, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = unwrapped.Unwrap()
	}
	return false
}

// TestU40AlterBucketDecoderIsStrict: reordered, extra, widened and absent-as-null members are refused.
func TestU40AlterBucketDecoderIsStrict(t *testing.T) {
	doc := func(elements ...bson.E) []byte {
		raw, err := bson.Marshal(bson.D(elements))
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	cmd := bson.E{Key: "cmd", Value: AdminAlterBucket}
	database, bucket := bson.E{Key: "database", Value: "app"}, bson.E{Key: "bucket", Value: "B"}
	add := bson.E{Key: "add", Value: bson.A{bson.D{{Key: "name", Value: "x"}, {Key: "type", Value: "int"}}}}
	drop := bson.E{Key: "drop", Value: bson.A{}}
	if _, err := DecodeRingAdmin(doc(cmd, database, bucket, add, drop)); err != nil {
		t.Fatalf("the canonical document: %v", err)
	}
	cases := map[string][]byte{
		"reordered":        doc(cmd, bucket, database, add, drop),
		"extra member":     doc(cmd, database, bucket, add, drop, bson.E{Key: "name", Value: "x"}),
		"drop before add":  doc(cmd, database, bucket, drop, add),
		"drop as strings":  doc(cmd, database, bucket, add, bson.E{Key: "drop", Value: "Age"}),
		"drop with a null": doc(cmd, database, bucket, add, bson.E{Key: "drop", Value: bson.A{nil}}),
		"add missing":      doc(cmd, database, bucket, drop),
		"add as null":      doc(cmd, database, bucket, bson.E{Key: "add", Value: nil}, drop),
		"type as int":      doc(cmd, database, bucket, bson.E{Key: "add", Value: bson.A{bson.D{{Key: "name", Value: "x"}, {Key: "type", Value: int32(1)}}}}, drop),
		"field order":      doc(cmd, database, bucket, bson.E{Key: "add", Value: bson.A{bson.D{{Key: "type", Value: "int"}, {Key: "name", Value: "x"}}}}, drop),
	}
	for name, raw := range cases {
		if _, err := DecodeRingAdmin(raw); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// TestU40AlterBucketDecodersSurviveMutation: 100000 mutations of requests and replies: no panic,
// and whatever is accepted re-encodes to the same bytes.
func TestU40AlterBucketDecodersSurviveMutation(t *testing.T) {
	var requests [][]byte
	for _, req := range u40Requests() {
		raw, err := EncodeRingAdmin(req)
		if err != nil {
			t.Fatal(err)
		}
		requests = append(requests, raw)
	}
	reply, err := EncodeRingAdminReply(u40Reply())
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(40, 40))
	var acceptedRequests, acceptedReplies int
	for range 100_000 {
		mutated := u65Mutate(rng, requests[rng.IntN(len(requests))])
		if req, err := DecodeRingAdmin(mutated); err == nil {
			acceptedRequests++
			if again, err := EncodeRingAdmin(req); err != nil || !bytes.Equal(again, mutated) {
				t.Fatalf("accepted request %x re-encodes as %x, %v", mutated, again, err)
			}
		}
		mutated = u65Mutate(rng, reply)
		if got, err := DecodeRingAdminReply(mutated); err == nil {
			acceptedReplies++
			if again, err := EncodeRingAdminReply(got); err != nil || !bytes.Equal(again, mutated) {
				t.Fatalf("accepted reply %x re-encodes as %x, %v", mutated, again, err)
			}
		}
	}
	if acceptedRequests == 0 || acceptedReplies == 0 {
		t.Fatalf("accepted %d requests and %d replies: the re-encode check never ran", acceptedRequests, acceptedReplies)
	}
}

// TestU40AlterBucketLyingLengthsAllocateLittle: a body that claims a huge array allocates in
// proportion to the bytes it has.
func TestU40AlterBucketLyingLengthsAllocateLittle(t *testing.T) {
	lie := []byte{0xff, 0xff, 0xff, 0x7f, 0x02, 'c', 'm', 'd', 0, 0xff, 0xff, 0xff, 0x7f, 0}
	perOp := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, _ = DecodeRingAdmin(lie)
		}
	}).AllocedBytesPerOp()
	if perOp > 1<<14 {
		t.Fatalf("a lying body allocated %d bytes", perOp)
	}
}
