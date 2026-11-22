package wire

import (
	"bytes"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
)

func u78Requests() []RingAdminRequest {
	return []RingAdminRequest{
		{Cmd: AdminShowDatabases},
		{Cmd: AdminShowDatabases, Limit: u75Int32(5), Cursor: []byte("app")},
		{Cmd: AdminDescribeDatabase, Database: "app"},
		{Cmd: AdminDescribeDatabase, Database: "app", Limit: u75Int32(2), Cursor: []byte{1, 2, 3}},
	}
}

func u78Replies() []RingAdminReply {
	return []RingAdminReply{
		{Cmd: AdminShowDatabases, Databases: []RingAdminDatabase{}},
		{Cmd: AdminShowDatabases, Databases: []RingAdminDatabase{{"app", 1, 1}, {"vault", 2, 3}}, More: true, Cursor: []byte("vault")},
		{Cmd: AdminDescribeDatabase, Name: "app", DatabaseID: 1, RF: 1, Storage: "disk", Buckets: []RingAdminListed{}},
		{Cmd: AdminDescribeDatabase, Name: "app", DatabaseID: 1, RF: 1, Storage: "disk", Buckets: []RingAdminListed{{"User", 9, "active"}}, More: true, Cursor: []byte{4}},
		{Cmd: AdminDescribeDatabase, Name: "scratch", DatabaseID: 2, RF: 1, Storage: "memory", Buckets: []RingAdminListed{}, MemoryBytes: 4096, MemoryCapBytes: 1 << 30},
	}
}

// TestU78AdminDatabaseCommandsRoundTrip: [GUARD both new commands and their replies are
// encoded and decoded to the same value].
func TestU78AdminDatabaseCommandsRoundTrip(t *testing.T) {
	for _, req := range u78Requests() {
		raw, err := EncodeRingAdmin(req)
		if err != nil {
			t.Fatalf("%s: %v", req.Cmd, err)
		}
		got, err := DecodeRingAdmin(raw)
		if err != nil {
			t.Fatalf("%s: %v", req.Cmd, err)
		}
		if got.Cmd != req.Cmd || got.Database != req.Database || !bytes.Equal(got.Cursor, req.Cursor) || (got.Limit == nil) != (req.Limit == nil) {
			t.Fatalf("%s: round trip gave %+v, want %+v", req.Cmd, got, req)
		}
	}
	for _, reply := range u78Replies() {
		raw, err := EncodeRingAdminReply(reply)
		if err != nil {
			t.Fatalf("%s: %v", reply.Cmd, err)
		}
		got, err := DecodeRingAdminReply(raw)
		if err != nil {
			t.Fatalf("%s: %v", reply.Cmd, err)
		}
		if got.Cmd != reply.Cmd || got.Name != reply.Name || got.DatabaseID != reply.DatabaseID || got.RF != reply.RF ||
			len(got.Databases) != len(reply.Databases) || len(got.Buckets) != len(reply.Buckets) || got.More != reply.More || !bytes.Equal(got.Cursor, reply.Cursor) {
			t.Fatalf("%s: round trip gave %+v, want %+v", reply.Cmd, got, reply)
		}
		for i := range reply.Databases {
			if got.Databases[i] != reply.Databases[i] {
				t.Fatalf("database %d: %+v, want %+v", i, got.Databases[i], reply.Databases[i])
			}
		}
	}
}

// TestU78AdminDatabaseRefusalsNameTheFix: [GUARD each limit is checked and the text names the
// member and what to send instead].
func TestU78AdminDatabaseRefusalsNameTheFix(t *testing.T) {
	cases := []struct {
		name string
		req  RingAdminRequest
		want []string
	}{
		{"show_databases names a database", RingAdminRequest{Cmd: AdminShowDatabases, Database: "app"}, []string{"show_databases", "database"}},
		{"show_databases negative limit", RingAdminRequest{Cmd: AdminShowDatabases, Limit: u75Int32(-1)}, []string{"limit", "show_databases"}},
		{"show_databases long cursor", RingAdminRequest{Cmd: AdminShowDatabases, Cursor: make([]byte, ringMaxName+1)}, []string{"cursor", "1021"}},
		{"describe_database without a name", RingAdminRequest{Cmd: AdminDescribeDatabase}, []string{"database", "1021"}},
		{"describe_database stray bucket", RingAdminRequest{Cmd: AdminDescribeDatabase, Database: "app", Bucket: "User"}, []string{"describe_database", "bucket"}},
		{"describe_database negative limit", RingAdminRequest{Cmd: AdminDescribeDatabase, Database: "app", Limit: u75Int32(-1)}, []string{"limit", "describe_database"}},
		{"unknown cmd names the new ones", RingAdminRequest{Cmd: "show_nodes"}, []string{"show_databases", "describe_database"}},
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
	badReplies := map[string]RingAdminReply{
		"show_databases without a list": {Cmd: AdminShowDatabases},
		"show_databases with a name":    {Cmd: AdminShowDatabases, Databases: []RingAdminDatabase{}, Name: "app"},
		"show_databases zero id":        {Cmd: AdminShowDatabases, Databases: []RingAdminDatabase{{"app", 0, 1}}},
		"show_databases empty name":     {Cmd: AdminShowDatabases, Databases: []RingAdminDatabase{{"", 1, 1}}},
		"describe_database no buckets":  {Cmd: AdminDescribeDatabase, Name: "app", DatabaseID: 1, RF: 1},
		"describe_database no name":     {Cmd: AdminDescribeDatabase, DatabaseID: 1, RF: 1, Buckets: []RingAdminListed{}},
		"describe_database with a list": {Cmd: AdminDescribeDatabase, Name: "app", DatabaseID: 1, RF: 1, Buckets: []RingAdminListed{}, Databases: []RingAdminDatabase{}},
		"show_buckets with databases":   {Cmd: AdminShowBuckets, Buckets: []RingAdminListed{}, Databases: []RingAdminDatabase{}},
		"create_database with a name":   {Cmd: AdminCreateDatabase, DatabaseID: 1, RF: 1, Name: "app"},
	}
	for name, reply := range badReplies {
		if _, err := EncodeRingAdminReply(reply); err == nil {
			t.Errorf("%s: encoded", name)
		}
	}
}

// TestU78AdminDatabaseDecoderIsStrict: [GUARD canonical layout: order, extras, written
// defaults, wrong types].
func TestU78AdminDatabaseDecoderIsStrict(t *testing.T) {
	doc := func(elements ...bson.E) []byte {
		raw, err := bson.Marshal(bson.D(elements))
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	show := bson.E{Key: "cmd", Value: AdminShowDatabases}
	describe := bson.E{Key: "cmd", Value: AdminDescribeDatabase}
	cases := map[string][]byte{
		"show with extra member":       doc(show, bson.E{Key: "extra", Value: int32(1)}),
		"show with database":           doc(show, bson.E{Key: "database", Value: "app"}),
		"show limit as int64":          doc(show, bson.E{Key: "limit", Value: int64(5)}),
		"show empty cursor written":    doc(show, bson.E{Key: "cursor", Value: []byte{}}),
		"show reordered":               doc(bson.E{Key: "limit", Value: int32(5)}, show),
		"describe without database":    doc(describe),
		"describe database as int":     doc(describe, bson.E{Key: "database", Value: int32(1)}),
		"describe cursor before limit": doc(describe, bson.E{Key: "database", Value: "app"}, bson.E{Key: "cursor", Value: []byte{1}}, bson.E{Key: "limit", Value: int32(5)}),
	}
	for name, raw := range cases {
		if _, err := DecodeRingAdmin(raw); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// TestU78AdminDatabaseDecodersSurviveMutation: [GUARD 100000 mutations: no panic, an accepted
// body re-encodes to the same bytes].
func TestU78AdminDatabaseDecodersSurviveMutation(t *testing.T) {
	var requests, replies [][]byte
	for _, req := range u78Requests() {
		raw, err := EncodeRingAdmin(req)
		if err != nil {
			t.Fatal(err)
		}
		requests = append(requests, raw)
	}
	for _, reply := range u78Replies() {
		raw, err := EncodeRingAdminReply(reply)
		if err != nil {
			t.Fatal(err)
		}
		replies = append(replies, raw)
	}
	rng := rand.New(rand.NewPCG(78, 78))
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

// TestU78AdminDatabaseDecodeAllocationIsBounded: [GUARD lengths are checked before allocating,
// also when the lying body names a new command].
func TestU78AdminDatabaseDecodeAllocationIsBounded(t *testing.T) {
	for _, cmd := range []string{AdminShowDatabases, AdminDescribeDatabase} {
		lie := []byte{0xff, 0xff, 0xff, 0x7f, 0x02, 'c', 'm', 'd', 0}
		lie = append(lie, byte(len(cmd)+1), 0, 0, 0)
		lie = append(lie, cmd...)
		lie = append(lie, 0, 0x04, 'x', 0, 0xff, 0xff, 0xff, 0x7f, 0)
		perOp := testing.Benchmark(func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_, _ = DecodeRingAdmin(lie)
				_, _ = DecodeRingAdminReply(lie)
			}
		}).AllocedBytesPerOp()
		if perOp > 1<<14 {
			t.Fatalf("a lying %s body of %d bytes allocated %d bytes", cmd, len(lie), perOp)
		}
	}
}
