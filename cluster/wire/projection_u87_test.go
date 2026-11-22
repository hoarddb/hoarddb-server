package wire

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// U87 projection: list and get return only the named scalar fields and _id_, and read no
// store state they do not return. The unit's tests live here; the codec seeds of the
// existing mutation tests carry a projected request too (ringlist_test.go,
// ringkeyed_test.go, peercodec_test.go).

// u87Counts counts what a read asks of the KV: store-state Gets and iterator Values.
type u87Counts struct {
	gets   atomic.Int64
	values atomic.Int64
}

type u87KV struct {
	storage.KV
	counts *u87Counts
}

func (k u87KV) NewSnapshot() storage.KVSnapshot {
	return u87Snapshot{KVSnapshot: k.KV.NewSnapshot(), counts: k.counts}
}

type u87Snapshot struct {
	storage.KVSnapshot
	counts *u87Counts
}

func (s u87Snapshot) Get(key []byte) ([]byte, error) {
	s.counts.gets.Add(1)
	return s.KVSnapshot.Get(key)
}

func (s u87Snapshot) NewIterator(opts storage.IterOptions) storage.KVIterator {
	return u87Iterator{KVIterator: s.KVSnapshot.NewIterator(opts), counts: s.counts}
}

type u87Iterator struct {
	storage.KVIterator
	counts *u87Counts
}

func (i u87Iterator) Value() ([]byte, error) {
	i.counts.values.Add(1)
	return i.KVIterator.Value()
}

const u87Docs = 6

// u87Start serves a node with bucket Wide (three scalars, a hashStore and a fifoStore)
// and u87Docs documents, each with one element in the fifoStore.
func u87Start(t *testing.T) (*u66Env, *u66Conn, *u87Counts) {
	t.Helper()
	env := u66Start(t)
	ctx := context.Background()
	if _, err := env.node.CreateBucket(ctx, u30iDB, "Wide", Field{Name: "Name", Type: "string"}, Field{Name: "Bio", Type: "string"},
		Field{Name: "Age", Type: "int"}, Field{Name: "Friends", Type: "hashStore"}, Field{Name: "Queue", Type: "fifoStore"}); err != nil {
		t.Fatal(err)
	}
	ref, err := env.node.bucket(ctx, u30iDB, "Wide")
	if err != nil {
		t.Fatal(err)
	}
	env.ref = ref
	c := env.login(t)
	for i := 1; i <= u87Docs; i++ {
		doc := bson.D{{Key: "_id_", Value: fmt.Sprintf("w%d", i)}, {Key: "Name", Value: fmt.Sprintf("name%d", i)},
			{Key: "Bio", Value: strings.Repeat("b", 1000)}, {Key: "Age", Value: int32(30 + i%2)}}
		if i == 3 {
			doc = bson.D{{Key: "_id_", Value: "w3"}, {Key: "Name", Value: "name3"}} // Bio and Age unset
		}
		if _, err := env.node.Insert(ctx, u30iDB, "Wide", doc); err != nil {
			t.Fatal(err)
		}
		arg := u66Arg(t, func() ([]byte, error) { return encodeElementArg(u66Item(t, "q")) })
		u66Reply(t, c.keyed(env.verb(t, fmt.Sprintf("w%d", i), "Queue", cluster.VerbPush, arg)))
	}
	counts := &u87Counts{}
	env.node.reader.cfg.KV = u87KV{KV: env.node.reader.cfg.KV, counts: counts}
	env.node.scanner.cfg.KV = u87KV{KV: env.node.scanner.cfg.KV, counts: counts}
	return env, c, counts
}

func u87GetFields(t *testing.T, env *u66Env, id string, fields ...string) RingKeyedRequest {
	t.Helper()
	req := env.read(t, RingClassGetFields, id, "", 0)
	req.Read.Fields = fields
	return req
}

func u87Body(t *testing.T, reply RingReply) bson.Raw {
	t.Helper()
	if reply.Found == nil || !*reply.Found || len(reply.Elements) != 1 {
		t.Fatalf("reply %+v, want one document", reply)
	}
	return bson.Raw(reply.Elements[0])
}

func u87Marshal(t *testing.T, doc bson.D) []byte {
	t.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func u87Lines(t *testing.T, blob []byte) []cluster.ListEntry {
	t.Helper()
	entries, err := cluster.DecodeEntries(blob)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

// TestU87GetFieldsReturnsIDAndTheNamedScalarsInFieldIDOrder: [GUARD the projected body
// keeps _id_ first, the requested scalars in field-id order and drops the rest].
func TestU87GetFieldsReturnsIDAndTheNamedScalarsInFieldIDOrder(t *testing.T) {
	env, c, _ := u87Start(t)
	cases := []struct {
		name   string
		id     string
		fields []string
		want   bson.D
	}{
		{"one field", "w1", []string{"Name"}, bson.D{{Key: "_id_", Value: "w1"}, {Key: "Name", Value: "name1"}}},
		{"request order is not body order", "w1", []string{"Age", "Name"}, bson.D{{Key: "_id_", Value: "w1"}, {Key: "Name", Value: "name1"}, {Key: "Age", Value: int32(31)}}},
		{"_id_ is allowed and ignored", "w1", []string{"_id_", "Name"}, bson.D{{Key: "_id_", Value: "w1"}, {Key: "Name", Value: "name1"}}},
		{"_id_ alone", "w1", []string{"_id_"}, bson.D{{Key: "_id_", Value: "w1"}}},
		{"an unset field is absent", "w3", []string{"Bio", "Name"}, bson.D{{Key: "_id_", Value: "w3"}, {Key: "Name", Value: "name3"}}},
	}
	for _, tc := range cases {
		got := u87Body(t, u66Reply(t, c.keyed(u87GetFields(t, env, tc.id, tc.fields...))))
		if want := u87Marshal(t, tc.want); !bytes.Equal(got, want) {
			t.Errorf("%s: body %v, want %v", tc.name, got, bson.Raw(want))
		}
	}
	missing := u66Reply(t, c.keyed(u87GetFields(t, env, "nobody", "Name")))
	if missing.Found == nil || *missing.Found || len(missing.Elements) != 0 {
		t.Fatalf("a projected get of an absent document is found=false, got %+v", missing)
	}
}

// TestU87UnprojectedBodiesAreByteForByteWhatTheyWere: [GUARD an absent fields member
// changes nothing: the get and list bodies are the bytes pinned from before the unit].
func TestU87UnprojectedBodiesAreByteForByteWhatTheyWere(t *testing.T) {
	env, c, _ := u87Start(t)
	got := u87Body(t, u66Reply(t, c.keyed(env.read(t, RingClassGet, "w3", "", 0))))
	if hex.EncodeToString(got) != u87PinnedGetBody {
		t.Fatalf("get body\n got %x\nwant %s", []byte(got), u87PinnedGetBody)
	}
	limit := int32(2)
	filter := u67Filter(t, bson.D{{Key: "Name", Value: "name3"}})
	reply := u66Reply(t, c.list(t, RingListRequest{Database: u30iDB, Bucket: "Wide", Filter: filter, Limit: &limit}))
	if hex.EncodeToString(reply.Entries) != u87PinnedListEntries {
		t.Fatalf("list entries\n got %x\nwant %s", reply.Entries, u87PinnedListEntries)
	}
	raw, err := EncodeRingList(RingListRequest{Database: "app", Bucket: "Wide"})
	if err != nil || hex.EncodeToString(raw) != u87PinnedListRequest {
		t.Fatalf("list request %x, %v, want %s", raw, err, u87PinnedListRequest)
	}
}

// TestU87GetFieldsRefusalsNameTheFieldAndTheFix: [GUARD an undeclared name is FIELD_TYPE
// with the describe fix, a store name names the verb that reads it, and both come before
// the document is read].
func TestU87GetFieldsRefusalsNameTheFieldAndTheFix(t *testing.T) {
	env, c, counts := u87Start(t)
	undeclared := u66Refusal(t, c.keyed(u87GetFields(t, env, "w1", "Name", "Nmae")), cluster.CodeFieldType)
	for _, word := range []string{`"Nmae"`, "app.Wide", "describe bucket Wide", "Name, Bio, Age"} {
		if !strings.Contains(undeclared.Message, word) {
			t.Errorf("undeclared refusal %q lacks %q", undeclared.Message, word)
		}
	}
	// The refusal does not depend on the document being there.
	absent := u66Refusal(t, c.keyed(u87GetFields(t, env, "nobody", "Nmae")), cluster.CodeFieldType)
	if !strings.Contains(absent.Message, `"Nmae"`) {
		t.Errorf("a projected get of an absent document with a bad name: %q", absent.Message)
	}
	for _, store := range []string{"Queue", "Friends"} {
		refusal := u66Refusal(t, c.keyed(u87GetFields(t, env, "w1", "Name", store)), cluster.CodeFieldType)
		for _, word := range []string{`"` + store + `"`, "store field", fmt.Sprintf(`db.Wide["<id>"].%s.list()`, store)} {
			if !strings.Contains(refusal.Message, word) {
				t.Errorf("store refusal %q lacks %q", refusal.Message, word)
			}
		}
	}
	if reads := counts.gets.Load() + counts.values.Load(); reads != 0 {
		t.Fatalf("%d KV reads happened before the refusals", reads)
	}
}

func u87ListEntries(t *testing.T, c *u66Conn, req RingListRequest) ([]cluster.ListEntry, RingReply) {
	t.Helper()
	req.Database, req.Bucket = u30iDB, "Wide"
	reply := u66Reply(t, c.list(t, req))
	return u87Lines(t, reply.Entries), reply
}

// TestU87ListProjectsEveryPageAndKeepsOrderAndCursor: [GUARD the list projects each body
// and keeps the (slot, id) order and the cursor of the unprojected list].
func TestU87ListProjectsEveryPageAndKeepsOrderAndCursor(t *testing.T) {
	env, c, _ := u87Start(t)
	_ = env
	all, _ := u87ListEntries(t, c, RingListRequest{})
	if len(all) != u87Docs {
		t.Fatalf("%d documents", len(all))
	}
	var seen []cluster.ListEntry
	var cursor []byte
	limit := int32(2)
	for pages := 0; ; pages++ {
		if pages > 10 {
			t.Fatal("the cursor never ended")
		}
		entries, reply := u87ListEntries(t, c, RingListRequest{Fields: []string{"Age", "Name"}, Limit: &limit, Cursor: cursor})
		seen = append(seen, entries...)
		if !reply.More {
			break
		}
		cursor = reply.Cursor
	}
	if len(seen) != len(all) {
		t.Fatalf("projected pages hold %d documents, the whole list %d", len(seen), len(all))
	}
	for i, entry := range seen {
		if entry.Slot != all[i].Slot || !bytes.Equal(entry.EncID, all[i].EncID) {
			t.Fatalf("entry %d is %x in slot %d, the unprojected list has %x in slot %d", i, entry.EncID, entry.Slot, all[i].EncID, all[i].Slot)
		}
		full := bson.Raw(all[i].Body)
		want := bson.D{{Key: "_id_", Value: full.Lookup("_id_").StringValue()}, {Key: "Name", Value: full.Lookup("Name").StringValue()}}
		if age, err := full.LookupErr("Age"); err == nil {
			want = append(want, bson.E{Key: "Age", Value: age.Int32()})
		}
		if !bytes.Equal(entry.Body, u87Marshal(t, want)) {
			t.Errorf("entry %d body %v, want %v", i, bson.Raw(entry.Body), want)
		}
	}
}

// TestU87FilterStillSeesEveryDeclaredScalar: [GUARD a filter on a field the projection
// leaves out still matches].
func TestU87FilterStillSeesEveryDeclaredScalar(t *testing.T) {
	_, c, _ := u87Start(t)
	filter := u67Filter(t, bson.D{{Key: "Age", Value: int32(31)}})
	plain, _ := u87ListEntries(t, c, RingListRequest{Filter: filter})
	projected, _ := u87ListEntries(t, c, RingListRequest{Filter: filter, Fields: []string{"Name"}})
	if len(plain) == 0 || len(projected) != len(plain) {
		t.Fatalf("filter on Age matched %d documents unprojected and %d with fields [Name]", len(plain), len(projected))
	}
	for _, entry := range projected {
		if _, err := bson.Raw(entry.Body).LookupErr("Age"); err == nil {
			t.Errorf("a body returned Age, which the projection left out: %v", bson.Raw(entry.Body))
		}
	}
}

// TestU87ListRefusalsNameTheFieldAndTheFix: [GUARD the list resolves names against the
// declaration and refuses before reading anything].
func TestU87ListRefusalsNameTheFieldAndTheFix(t *testing.T) {
	_, c, counts := u87Start(t)
	req := RingListRequest{Database: u30iDB, Bucket: "Wide", Fields: []string{"Nmae"}}
	refusal := u66Refusal(t, c.list(t, req), cluster.CodeFieldType)
	for _, word := range []string{`"Nmae"`, "describe bucket Wide"} {
		if !strings.Contains(refusal.Message, word) {
			t.Errorf("undeclared refusal %q lacks %q", refusal.Message, word)
		}
	}
	req.Fields = []string{"Friends"}
	refusal = u66Refusal(t, c.list(t, req), cluster.CodeFieldType)
	if !strings.Contains(refusal.Message, `db.Wide["<id>"].Friends.list()`) {
		t.Errorf("store refusal %q does not name the verb that reads the store", refusal.Message)
	}
	if reads := counts.gets.Load() + counts.values.Load(); reads != 0 {
		t.Fatalf("%d KV reads happened before the refusals", reads)
	}
}

// TestU87AProjectionReadsFewerKeys: [GUARD a projection with no store field reads no
// store state, and skips the stored value of every scalar it neither returns nor
// filters on]. Rule 11: a projection reads fewer keys than the whole body does.
func TestU87AProjectionReadsFewerKeys(t *testing.T) {
	env, c, counts := u87Start(t)
	measure := func(run func()) (gets, values int64) {
		counts.gets.Store(0)
		counts.values.Store(0)
		run()
		return counts.gets.Load(), counts.values.Load()
	}
	listAll := func(fields []string) func() {
		return func() {
			entries, _ := u87ListEntries(t, c, RingListRequest{Fields: fields})
			if len(entries) != u87Docs {
				t.Fatalf("%d documents", len(entries))
			}
		}
	}
	gets, values := measure(listAll(nil))
	// Two store fields are declared, so every document costs two state reads; its header,
	// Name, Bio and Age cost four value reads (w3 has only the header and Name).
	if gets != 2*u87Docs || values != 4*(u87Docs-1)+2 {
		t.Fatalf("unprojected list: %d store-state reads and %d values, want %d and %d", gets, values, 2*u87Docs, 4*(u87Docs-1)+2)
	}
	gets, values = measure(listAll([]string{"Name"}))
	if gets != 0 || values != 2*u87Docs {
		t.Errorf("list with fields [Name]: %d store-state reads and %d values, want 0 and %d (header and Name)", gets, values, 2*u87Docs)
	}
	filter := u67Filter(t, bson.D{{Key: "Name", Value: "name1"}})
	gets, values = measure(func() { u87ListEntries(t, c, RingListRequest{Fields: []string{"Name"}, Filter: filter}) })
	if gets != 0 || values != 2*u87Docs {
		t.Errorf("a filtered projection of Name read %d store-state keys and %d values, want 0 and %d", gets, values, 2*u87Docs)
	}
	gets, _ = measure(func() { u66Reply(t, c.keyed(env.read(t, RingClassGet, "w1", "", 0))) })
	if gets != 2 {
		t.Fatalf("unprojected get: %d store-state reads, want 2", gets)
	}
	gets, values = measure(func() { u66Reply(t, c.keyed(u87GetFields(t, env, "w1", "Name"))) })
	if gets != 0 || values != 2 {
		t.Errorf("get with fields [Name]: %d store-state reads and %d values, want 0 and 2", gets, values)
	}
}

// TestU87TheByteCeilingCountsTheProjectedSize: [GUARD max_result_bytes accounting uses the
// size of the entry that is returned, not the size of the whole document].
func TestU87TheByteCeilingCountsTheProjectedSize(t *testing.T) {
	env, _, _ := u87Start(t)
	ref, err := env.node.bucket(context.Background(), u30iDB, "Wide")
	if err != nil {
		t.Fatal(err)
	}
	const ceiling = 1500 // one 1000-byte Bio fits, two do not; the projected entries are about 40 bytes
	plain, err := env.node.listPage(context.Background(), pageRequest{ref: ref, limit: 10, ceiling: ceiling})
	if err != nil {
		t.Fatal(err)
	}
	projected, err := env.node.listPage(context.Background(), pageRequest{ref: ref, limit: 10, ceiling: ceiling, fields: []string{"Name"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(plain.Entries) >= u87Docs || !plain.More || len(projected.Entries) != u87Docs || projected.More {
		t.Fatalf("under a %d byte ceiling the whole list took %d entries (more %v) and the projection %d (more %v), want fewer than 6 then all 6",
			ceiling, len(plain.Entries), plain.More, len(projected.Entries), projected.More)
	}
}

// TestU87NodeGetFieldsAndListPageProject: [GUARD the in-process API projects like the wire].
func TestU87NodeGetFieldsAndListPageProject(t *testing.T) {
	env, _, _ := u87Start(t)
	ctx := context.Background()
	doc, err := env.node.GetFields(ctx, u30iDB, "Wide", "w1", []string{"Name"})
	if err != nil {
		t.Fatal(err)
	}
	if want := u87Marshal(t, bson.D{{Key: "_id_", Value: "w1"}, {Key: "Name", Value: "name1"}}); !bytes.Equal(doc.Body, want) {
		t.Fatalf("GetFields body %v, want %v", bson.Raw(doc.Body), bson.Raw(want))
	}
	page, err := env.node.ListPage(ctx, ListPageRequest{Database: u30iDB, Bucket: "Wide", Limit: 2, Fields: []string{"Name"}})
	if err != nil || len(page.Entries) != 2 {
		t.Fatalf("ListPage %+v, %v", page, err)
	}
	for _, entry := range page.Entries {
		id := bson.Raw(entry.Body).Lookup("_id_").StringValue()
		want := u87Marshal(t, bson.D{{Key: "_id_", Value: id}, {Key: "Name", Value: "name" + id[1:]}})
		if !bytes.Equal(entry.Body, want) {
			t.Fatalf("ListPage body %v, want %v", bson.Raw(entry.Body), bson.Raw(want))
		}
	}
	if _, err := env.node.GetFields(ctx, u30iDB, "Wide", "w1", []string{"Queue"}); err == nil {
		t.Fatal("GetFields of a store field was accepted")
	}
}

// TestU87ListFieldsCodecRefusals: [GUARD every malformed fields member is refused naming
// the member and the fix].
func TestU87ListFieldsCodecRefusals(t *testing.T) {
	base := func(fields any) []byte {
		return u87Marshal(t, bson.D{{Key: "database", Value: "app"}, {Key: "bucket", Value: "User"}, {Key: "fields", Value: fields}})
	}
	names := func(n int) bson.A {
		out := make(bson.A, n)
		for i, name := range u87Names(n) {
			out[i] = name
		}
		return out
	}
	cases := map[string]struct {
		raw  []byte
		said []string
	}{
		"empty array":                {base(bson.A{}), []string{"fields", "at least one"}},
		"65 names":                   {base(names(65)), []string{"fields", "64"}},
		"empty name":                 {base(bson.A{"Name", ""}), []string{"fields", "1 to 255"}},
		"256 byte name":              {base(bson.A{strings.Repeat("n", 256)}), []string{"fields", "255"}},
		"duplicate":                  {base(bson.A{"Name", "Age", "Name"}), []string{"fields", `"Name"`, "twice"}},
		"duplicate _id_":             {base(bson.A{"_id_", "_id_"}), []string{"fields", `"_id_"`, "twice"}},
		"an integer element":         {base(bson.A{"Name", int32(3)}), []string{"fields", "string"}},
		"fields as a string":         {base("Name"), []string{"fields", "array"}},
		"an array past the byte cap": {base(names(5000)), []string{"fields", "bytes"}},
		"fields as a doc":            {base(bson.D{{Key: "0", Value: "Name"}}), []string{"fields", "array"}},
		"a null element":             {base(bson.A{nil}), []string{"fields", "string"}},
		"a nested array":             {base(bson.A{bson.A{"Name"}}), []string{"fields", "string"}},
		"after the cursor":           {u87Marshal(t, bson.D{{Key: "database", Value: "app"}, {Key: "bucket", Value: "User"}, {Key: "cursor", Value: []byte{1}}, {Key: "fields", Value: bson.A{"Name"}}}), []string{"layout"}},
		"before the filter":          {u87Marshal(t, bson.D{{Key: "database", Value: "app"}, {Key: "bucket", Value: "User"}, {Key: "fields", Value: bson.A{"Name"}}, {Key: "filter", Value: bson.D{{Key: "A", Value: "b"}}}}), []string{"layout"}},
		"array keys not 0..n":        {u87Marshal(t, bson.D{{Key: "database", Value: "app"}, {Key: "bucket", Value: "User"}, {Key: "fields", Value: bson.D{{Key: "1", Value: "Name"}}}}), []string{"fields"}},
	}
	for name, tc := range cases {
		_, err := DecodeRingList(tc.raw)
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		u65AssertRefused(t, err, tc.said...)
	}
	for name, fields := range map[string][]string{"empty": {}, "too many": make([]string, 65), "duplicate": {"A", "A"}, "an empty name": {""}} {
		if _, err := EncodeRingList(RingListRequest{Database: "app", Bucket: "User", Fields: fields}); err == nil {
			t.Errorf("encoder accepted %s fields", name)
		}
	}
}

// TestU87ListFieldsMemberSitsAfterTheFilter: [GUARD the layout of the canonical request].
func TestU87ListFieldsMemberSitsAfterTheFilter(t *testing.T) {
	filter := u67Filter(t, bson.D{{Key: "City", Value: "Oslo"}})
	limit := int32(5)
	raw, err := EncodeRingList(RingListRequest{Database: "app", Bucket: "User", Filter: filter, Fields: []string{"Name", "Bio"}, Limit: &limit, Epoch: 3})
	if err != nil {
		t.Fatal(err)
	}
	want := u87Marshal(t, bson.D{{Key: "database", Value: "app"}, {Key: "bucket", Value: "User"}, {Key: "filter", Value: bson.D{{Key: "City", Value: "Oslo"}}},
		{Key: "fields", Value: bson.A{"Name", "Bio"}}, {Key: "limit", Value: limit}, {Key: "epoch", Value: int64(3)}})
	if !bytes.Equal(raw, want) {
		t.Fatalf("request\n got %v\nwant %v", bson.Raw(raw), bson.Raw(want))
	}
}

// TestU87ListFieldsAtTheCapStillDecodes: [GUARD the size cap leaves room for 64 names of
// 255 bytes, and nothing past it is decoded].
func TestU87ListFieldsAtTheCapStillDecodes(t *testing.T) {
	fields := make([]string, ringMaxFields)
	for i := range fields {
		fields[i] = fmt.Sprintf("%0255d", i)
	}
	raw, err := EncodeRingList(RingListRequest{Database: "app", Bucket: "User", Fields: fields})
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeRingList(raw)
	if err != nil || len(got.Fields) != ringMaxFields {
		t.Fatalf("a request with 64 names of 255 bytes: %d names, %v", len(got.Fields), err)
	}
}

// u87ForwardSeeds are the projected reads the forward codec must carry.
func TestU87ForwardReadCarriesTheProjection(t *testing.T) {
	for _, fields := range [][]uint16{{}, {2}, {2, 4, 9}} {
		req := ReadRequest{BucketID: 4, EncID: []byte("e"), ID: "id", Hop: 1, RequestID: [16]byte{5}, Fields: fields}
		raw, err := EncodeForwardReadBody(ForwardReadBody{Req: req})
		if err != nil {
			t.Fatalf("%v: %v", fields, err)
		}
		got, err := DecodeForwardReadBody(raw)
		if err != nil || got.Stored || got.Req.Fields == nil || fmt.Sprint(got.Req.Fields) != fmt.Sprint(fields) {
			t.Fatalf("round trip of fields %v: %+v, %v", fields, got, err)
		}
	}
	plain, err := EncodeForwardReadBody(ForwardReadBody{Req: ReadRequest{BucketID: 4, EncID: []byte("e")}})
	if err != nil {
		t.Fatal(err)
	}
	if plain[0] != 0 {
		t.Fatalf("an unprojected forwarded read has sub-kind %d, want 0 as before", plain[0])
	}
	got, err := DecodeForwardReadBody(plain)
	if err != nil || got.Req.Fields != nil {
		t.Fatalf("an unprojected forward decoded with fields %v, %v", got.Req.Fields, err)
	}
	for name, fields := range map[string][]uint16{"descending": {4, 2}, "duplicate": {2, 2}, "65 ids": make([]uint16, 65)} {
		if _, err := EncodeForwardReadBody(ForwardReadBody{Req: ReadRequest{BucketID: 4, EncID: []byte("e"), Fields: fields}}); err == nil {
			t.Errorf("%s fields were encoded", name)
		}
	}
	if _, err := EncodeForwardReadBody(ForwardReadBody{Stored: true, Req: ReadRequest{BucketID: 4, EncID: []byte("e"), Fields: []uint16{2}}}); err == nil {
		t.Error("a stored read with a projection was encoded")
	}
}

// TestU87GetFieldsRoundTripsAndPlacesTheNamesAfterTheDocumentID: [GUARD the wire layout of
// read class 0x89].
func TestU87GetFieldsRoundTripsAndPlacesTheNamesAfterTheDocumentID(t *testing.T) {
	read := RingRead{Class: RingClassGetFields, BucketID: 41, DeclVersion: 7, SchemaVersionSeen: 7, EncID: u65EncID, Fields: []string{"Name", "Bio"}}
	raw, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Read: &read})
	if err != nil {
		t.Fatal(err)
	}
	const want = "89" + "0000000000000029" + "0000000000000007" + "0000000000000007" + "00000000000000000000000000000000" + "5070310000" +
		"02" + "04" + "4e616d65" + "03" + "42696f"
	if got := hex.EncodeToString(raw[RingPrologueLen:]); got != want {
		t.Fatalf("form\n got %s\nwant %s", got, want)
	}
	back, err := DecodeRingKeyed(raw)
	if err != nil || back.Read == nil || fmt.Sprint(back.Read.Fields) != "[Name Bio]" {
		t.Fatalf("decoded %+v, %v", back, err)
	}
}

// TestU87GetFieldsRefusals: [GUARD a get_fields without names, with too many or with a
// bad name is refused naming the member; no other class takes names].
func TestU87GetFieldsRefusals(t *testing.T) {
	base := RingRead{Class: RingClassGetFields, BucketID: 41, DeclVersion: 7, SchemaVersionSeen: 7, EncID: u65EncID, Fields: []string{"Name"}}
	with := func(mutate func(*RingRead)) RingRead { read := base; mutate(&read); return read }
	cases := map[string]RingRead{
		"no names":              with(func(r *RingRead) { r.Fields = nil }),
		"65 names":              with(func(r *RingRead) { r.Fields = u87Names(65) }),
		"an empty name":         with(func(r *RingRead) { r.Fields = []string{"Name", ""} }),
		"a 256 byte name":       with(func(r *RingRead) { r.Fields = []string{strings.Repeat("n", 256)} }),
		"a duplicate":           with(func(r *RingRead) { r.Fields = []string{"Name", "Name"} }),
		"invalid UTF-8":         with(func(r *RingRead) { r.Fields = []string{"\xff"} }),
		"a field id":            with(func(r *RingRead) { r.FieldID = 3 }),
		"a limit":               with(func(r *RingRead) { r.Limit = 3 }),
		"names on a plain get":  with(func(r *RingRead) { r.Class = RingClassGet }),
		"names on a store list": with(func(r *RingRead) { r.Class, r.FieldID, r.Limit = RingClassStoreList, 4, 10 }),
	}
	for name, read := range cases {
		if _, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Read: &read}); err == nil {
			t.Errorf("%s: the encoder accepted it", name)
		}
	}
	good, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Read: &base})
	if err != nil {
		t.Fatal(err)
	}
	body := good[RingPrologueLen:]
	idEnd := 1 + ringHeaderLen + len(u65EncID)
	forge := func(tail ...byte) []byte {
		return append(append(bytes.Clone(good[:RingPrologueLen]), body[:idEnd]...), tail...)
	}
	for name, tail := range map[string][]byte{
		"a count of 0":               {0},
		"a count of 65":              u87Count65(),
		"a name length of 0":         {1, 0},
		"a count past the names":     {2, 1, 'a'},
		"a name length past the end": {1, 9, 'a'},
		"bytes after the names":      {1, 1, 'a', 0},
		"no count":                   {},
	} {
		if _, err := DecodeRingKeyed(forge(tail...)); err == nil {
			t.Errorf("%s: the decoder accepted it", name)
		} else {
			u65AssertRefused(t, err, "fields")
		}
	}
}

var _ = transport.OpRingList

const (
	u87PinnedGetBody     = "43000000025f69645f0003000000773300024e616d6500060000006e616d65330002517565756500160000006669666f53746f726520283120656c656d656e74290000"
	u87PinnedListEntries = "0e9e0550773300004343000000025f69645f0003000000773300024e616d6500060000006e616d65330002517565756500160000006669666f53746f726520283120656c656d656e74290000"
	u87PinnedListRequest = "28000000026461746162617365000400000061707000026275636b65740005000000576964650000"
)

// u87Names returns n distinct short field names.
func u87Names(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("f%d", i)
	}
	return out
}

// u87Count65 is the tail of a get_fields that counts 65 distinct names.
func u87Count65() []byte {
	tail := []byte{65}
	for _, name := range u87Names(65) {
		tail = append(append(tail, byte(len(name))), name...)
	}
	return tail
}

// TestU87AProjectionRefusalComesAfterAuthorization: [GUARD a session with no role on the
// database learns no field name from a projection refusal: the grant is asked first, then
// the names are judged].
func TestU87AProjectionRefusalComesAfterAuthorization(t *testing.T) {
	app, _, _ := u70Start(t)
	nobody := u70Login(t, app, "nobody", "")
	refusal := u66Refusal(t, nobody.keyed(u87GetFields(t, app, "b1", "Nmae")), cluster.CodePermissionDenied)
	if strings.Contains(refusal.Message, "Name") || strings.Contains(refusal.Message, "declared") {
		t.Errorf("a denied get_fields leaks the declaration: %q", refusal.Message)
	}
	refusal = u66Refusal(t, nobody.list(t, RingListRequest{Database: u30iDB, Bucket: "Box", Fields: []string{"Nmae"}}), cluster.CodePermissionDenied)
	if strings.Contains(refusal.Message, "Name") || strings.Contains(refusal.Message, "declared") {
		t.Errorf("a denied list leaks the declaration: %q", refusal.Message)
	}
	reader := u70Login(t, app, "reader", "read@app")
	u66Refusal(t, reader.keyed(u87GetFields(t, app, "b1", "Nmae")), cluster.CodeFieldType)
	got := u66Reply(t, reader.list(t, RingListRequest{Database: u30iDB, Bucket: "Box", Fields: []string{"Name"}}))
	if entries := u87Lines(t, got.Entries); len(entries) != 1 || bson.Raw(entries[0].Body).Lookup("Name").StringValue() != "one" {
		t.Fatalf("a reader's projected list: %+v", got)
	}
}
