package server

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Tests for step 1 of the spread MVP (document placement spread MVP note, rev
// 2.1, section 5 row 1): the part name and its one display function, the
// catalog fields parts, part and part_of, the boot refusal of a data directory
// from before spread collections (B6), the 0x1F name rule (B7) and parts kept
// out of listings and counts (N9). There is no router yet, so a part is made
// by hand, with the create the router will send.

const unitSeparator = "\x1f"

// createPartByHand makes the part p of collection with the create the router
// will send: whole, part and part_of set.
func createPartByHand(t *testing.T, s *Server, collection string, p int, partOf uint64) string {
	t.Helper()
	name := partName(collection, p)
	resp, err := s.CreateBucket(CreateBucketRequest{
		Database: "app", Name: name, Type: BucketTypeHash,
		Options: map[string]interface{}{"placement": "whole"},
		Part:    int32(p), PartOf: partOf,
	})
	if err != nil || !resp.Success {
		t.Fatalf("creating part %d of %s by hand = %+v, %v", p, collection, resp, err)
	}
	return name
}

// writeRawEntry puts a catalog entry in s's store exactly as given, the way a
// build before this one would have written it.
func writeRawEntry(t *testing.T, s *Server, database, bucket string, entry catalogEntry) {
	t.Helper()
	entry.Slot = int32(storage.SlotOf(storage.PlacementKey(database, bucket, nil)))
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now().UTC()
	}
	encoded, err := bson.Marshal(entry)
	if err != nil {
		t.Fatalf("encoding the raw entry: %v", err)
	}
	batch := s.kv.NewBatch()
	if err := batch.Set(catalogKey(database, bucket), encoded); err != nil {
		t.Fatalf("setting the raw entry: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing the raw entry: %v", err)
	}
}

// TestPartNameIsTheCollectionThenUnitSeparatorThenHex4 pins the part name: the
// collection, 0x1F, and the part number as four lower-case hex digits, five
// bytes of suffix, and the strict reading back.
func TestPartNameIsTheCollectionThenUnitSeparatorThenHex4(t *testing.T) {
	for _, row := range []struct {
		p    int
		want string
	}{{0, "User\x1f0000"}, {3, "User\x1f0003"}, {255, "User\x1f00ff"}} {
		if got := partName("User", row.p); got != row.want {
			t.Errorf("partName(User, %d) = %q, want %q", row.p, got, row.want)
		}
		collection, p, ok := partNameParts(row.want)
		if !ok || collection != "User" || p != row.p {
			t.Errorf("partNameParts(%q) = %q, %d, %v; want User, %d, true", row.want, collection, p, ok, row.p)
		}
	}
	if got := len(partName(strings.Repeat("a", maxSpreadNameBytes), 1)); got != maxNameBytes {
		t.Errorf("the longest part name is %d bytes, want %d (the name limit)", got, maxNameBytes)
	}
	for _, notAPart := range []string{"User", "User\x1f", "User\x1f003", "User\x1f00FF", "User\x1f000g", "a\x1fb\x1f0003", "\x1f0003", "User\x1f00031"} {
		if _, _, ok := partNameParts(notAPart); ok {
			t.Errorf("partNameParts(%q) accepted a name that is not part-shaped", notAPart)
		}
	}
}

// TestDisplayBucketIsTheOneNameAClientSees holds that a part displays as its
// collection and every other name as itself.
func TestDisplayBucketIsTheOneNameAClientSees(t *testing.T) {
	for name, want := range map[string]string{
		"User":                  "User",
		"User\x1f0003":          "User",
		"a\x1fb":                "a",
		"a\x1fb\x1fc":           "a",
		"":                      "",
		partName("Orders", 255): "Orders",
	} {
		if got := displayBucket(name); got != want {
			t.Errorf("displayBucket(%q) = %q, want %q", name, got, want)
		}
	}
	if err := bucketNotFoundErr("app", partName("User", 3)); strings.Contains(err.Error(), unitSeparator) || !strings.Contains(err.Error(), `"User"`) {
		t.Errorf("the not-found error for a part is %q; want the displayed name and no 0x1F", err)
	}
	if text := notHolderErrText("app", partName("User", 3)); strings.Contains(text, unitSeparator) {
		t.Errorf("the not-holder text for a part is %q; want no 0x1F", text)
	}
}

// notHolderErrText is notHolderErr for a name, with a placeholder owner.
func notHolderErrText(database, bucket string) string {
	return notHolderErr(database, bucket, ring.Node{ID: "n1", Addr: "n1.example:4433"})
}

// TestSpreadTemplateRecordsPartsAtCreate is the catalog half of the template:
// a spread hash bucket records parts 256 at create, a whole bucket and every
// other type record none, a request cannot set it, and it survives a restart.
func TestSpreadTemplateRecordsPartsAtCreate(t *testing.T) {
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	s := first.core
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	if resp := createPlacementBucket(t, s, "Spread", BucketTypeHash, "spread"); !resp.Success {
		t.Fatalf("create spread: %+v", resp)
	}
	if resp := createPlacementBucket(t, s, "Whole", BucketTypeHash, "whole"); !resp.Success {
		t.Fatalf("create whole: %+v", resp)
	}
	if resp := createPlacementBucket(t, s, "Queue", BucketTypeFIFO, ""); !resp.Success {
		t.Fatalf("create fifo: %+v", resp)
	}
	forced, err := s.CreateBucket(CreateBucketRequest{Database: "app", Name: "Forced", Type: BucketTypeHash,
		Options: map[string]interface{}{"placement": "spread", "parts": 8, "part": 3}})
	if err != nil || !forced.Success {
		t.Fatalf("create with a parts option = %+v, %v", forced, err)
	}
	check := func(s *Server, when string) {
		t.Helper()
		for name, want := range map[string]int32{"Spread": 256, "Forced": 256, "Whole": 0, "Queue": 0} {
			entry, found, err := s.readCatalogEntry("app", name)
			if err != nil || !found {
				t.Fatalf("%s: reading the entry of %s = %v, %v", when, name, found, err)
			}
			if entry.Parts != want || entry.Part != 0 || entry.PartOf != 0 {
				t.Errorf("%s: the entry of %s has parts %d, part %d, part_of %d; want parts %d, no part, no part_of",
					when, name, entry.Parts, entry.Part, entry.PartOf, want)
			}
		}
	}
	check(s, "before a restart")
	second := restartCatalog(t, first, dataDir)
	check(second.core, "after a restart")
}

// TestPartCreateRecordsPartAndPartOfAndBootsAgain is the catalog half of a
// part: whole, with part and part_of, no parts; it is attached again by the
// boot refusal's check, and it is not a template.
func TestPartCreateRecordsPartAndPartOfAndBootsAgain(t *testing.T) {
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	s := first.core
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	if resp := createPlacementBucket(t, s, "User", BucketTypeHash, ""); !resp.Success {
		t.Fatalf("create the template: %+v", resp)
	}
	zero := createPartByHand(t, s, "User", 0, 4242)
	three := createPartByHand(t, s, "User", 3, 4242)
	check := func(s *Server, when string) {
		t.Helper()
		for name, p := range map[string]int32{zero: 0, three: 3} {
			entry, found, err := s.readCatalogEntry("app", name)
			if err != nil || !found {
				t.Fatalf("%s: reading the entry of part %q = %v, %v", when, name, found, err)
			}
			if entry.Placement != "whole" || entry.Parts != 0 || entry.Part != p || entry.PartOf != 4242 {
				t.Errorf("%s: the entry of part %d = placement %q, parts %d, part %d, part_of %d; want whole, no parts, part %d, part_of 4242",
					when, p, entry.Placement, entry.Parts, entry.Part, entry.PartOf, p)
			}
		}
		if s.bucketStoreOpen("app", three) == false {
			t.Errorf("%s: part 3 is not served", when)
		}
	}
	check(s, "before a restart")
	check(restartCatalog(t, first, dataDir).core, "after a restart")
}

// TestPartCreateRefusals holds the rules a part's create keeps: part_of only on
// a part-shaped name, a part's number is the one in its name, a part is whole,
// and a name with 0x1F but no part_of is a control character as before.
func TestPartCreateRefusals(t *testing.T) {
	s := newTestServer(t)
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	name := partName("User", 3)
	for label, req := range map[string]CreateBucketRequest{
		"part_of on a plain name":     {Database: "app", Name: "User", Type: BucketTypeHash, PartOf: 7},
		"part on a plain name":        {Database: "app", Name: "User", Type: BucketTypeHash, Part: 3},
		"a part name without part_of": {Database: "app", Name: name, Type: BucketTypeHash},
	} {
		if err := createNameRefusal(req); err == nil {
			t.Errorf("%s: createNameRefusal accepted it", label)
		}
		if err := s.createShapeRefusal(req); err == nil {
			t.Errorf("%s: createShapeRefusal accepted it", label)
		}
	}
	ok := CreateBucketRequest{Database: "app", Name: name, Type: BucketTypeHash, Part: 3, PartOf: 7}
	if err := createNameRefusal(ok); err != nil {
		t.Errorf("a part-shaped name with part_of set was refused: %v", err)
	}
	if err := s.createShapeRefusal(ok); err != nil {
		t.Errorf("createShapeRefusal refused a part's create: %v", err)
	}
	// The exemption is for the bucket's name only: a declare of store fields
	// naming a part is refused, and so is a control character in a field.
	declare := mustMarshalBSON(t, DeclareStoreFieldsRequest{Database: "app", Name: name})
	if err := clientNameRefusal(transport.OpDeclareStoreFields, declare); err == nil {
		t.Error("a declare of store fields naming a part was accepted")
	}
	withBadField := ok
	withBadField.Options = map[string]interface{}{"stores": []interface{}{map[string]interface{}{"name": badName, "engine": "fifo"}}}
	if err := createNameRefusal(withBadField); err == nil {
		t.Error("the exemption let a control character through in a field name")
	}
	// The create itself refuses what a replica or a replay could be sent.
	for label, req := range map[string]CreateBucketRequest{
		"a number that is not the name's": {Database: "app", Name: name, Type: BucketTypeHash, Part: 4, PartOf: 7,
			Options: map[string]interface{}{"placement": "whole"}},
		"spread":     {Database: "app", Name: name, Type: BucketTypeHash, Part: 3, PartOf: 7, Options: map[string]interface{}{"placement": "spread"}},
		"no part_of": {Database: "app", Name: name, Type: BucketTypeHash, Part: 3, Options: map[string]interface{}{"placement": "whole"}},
		"a fifo":     {Database: "app", Name: name, Type: BucketTypeFIFO, Part: 3, PartOf: 7},
	} {
		resp, err := s.CreateBucket(req)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		if resp.Success {
			t.Errorf("%s: the create of a malformed part succeeded", label)
		}
		if strings.Contains(resp.Message, unitSeparator) {
			t.Errorf("%s: the refusal %q holds 0x1F", label, resp.Message)
		}
	}
}

// TestBootRefusesOnlyPreSpreadEntries is B6: a hash entry whose name has no
// 0x1F and that has no placement, or is spread with no parts, stops the load
// with the one-line message. A whole hash entry, a spread entry with parts, a
// part, and an entry of another type never match. This replaces 12a's reading
// of a hash entry with no placement as whole.
func TestBootRefusesOnlyPreSpreadEntries(t *testing.T) {
	const message = "this data directory holds buckets from before spread collections; start with a new data directory"
	// The refusal names the setting that moves the node to an empty directory
	// and the directory it is using (rule 3: the error says how to fix it).
	wantDirectoryAdvice := ": set storage.data_dir (HOARDDB_DATA_DIR) to an empty directory; this node is using "
	boot := func(t *testing.T, entries map[string]catalogEntry) error {
		t.Helper()
		dataDir := t.TempDir()
		first := startCatalogServerAt(t, dataDir)
		if resp, err := first.core.CreateDatabase("app"); err != nil || !resp.Success {
			t.Fatalf("CreateDatabase = %+v, %v", resp, err)
		}
		next := int64(900_000)
		for name, entry := range entries {
			next++
			entry.ID = next
			writeRawEntry(t, first.core, "app", name, entry)
		}
		first.stop(t)
		second := startCatalogServerAt(t, dataDir)
		return second.qs.LoadCatalog()
	}
	for label, entry := range map[string]catalogEntry{
		"a hash entry with no placement (a build before 12a)": {Type: BucketTypeHash},
		"a spread entry with no parts (a 12a-only build)":     {Type: BucketTypeHash, Placement: "spread"},
	} {
		err := boot(t, map[string]catalogEntry{"Old": entry})
		if err == nil || !strings.HasPrefix(err.Error(), message+wantDirectoryAdvice) {
			t.Errorf("%s: LoadCatalog = %v, want it to start with %q", label, err, message+wantDirectoryAdvice)
		}
		if !errors.Is(err, errPreSpreadDataDirectory) {
			t.Errorf("%s: the refusal does not match errPreSpreadDataDirectory", label)
		}
	}
	err := boot(t, map[string]catalogEntry{
		"Whole":               {Type: BucketTypeHash, Placement: "whole"},
		"Spread":              {Type: BucketTypeHash, Placement: "spread", Parts: 256},
		partName("Spread", 3): {Type: BucketTypeHash, Placement: "whole", Part: 3, PartOf: 9},
		partName("Bare", 1):   {Type: BucketTypeHash},
		"Queue":               {Type: BucketTypeFIFO},
		"Tree":                {Type: BucketTypeBTree},
	})
	if err != nil {
		t.Errorf("a data directory of whole, spread-with-parts, part and non-hash entries was refused: %v", err)
	}
}

// TestNewHandlerRefusesAnyClientFrameNamingAPartName is B7's door: a client
// frame whose bucket holds 0x1F is NOT FOUND of the displayed name, whatever
// it carries, and a create that claims to be a part is refused before anything
// is made.
func TestNewHandlerRefusesAnyClientFrameNamingAPartName(t *testing.T) {
	s := newTestServer(t)
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	if resp := createPlacementBucket(t, s, "User", BucketTypeHash, ""); !resp.Success {
		t.Fatalf("create the template: %+v", resp)
	}
	name := createPartByHand(t, s, "User", 3, 55)
	handler := NewHandler(s)
	send := func(op transport.Opcode, request any) *transport.Frame {
		t.Helper()
		frame, err := handler(t.Context(), transport.NewFrame(op, mustMarshalBSON(t, request)))
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return frame
	}
	requests := map[string]struct {
		op      transport.Opcode
		request any
	}{
		"put":     {transport.OpPut, InsertRequest{Database: "app", Bucket: name, Key: "ada", Data: map[string]interface{}{"Name": "Ada"}}},
		"get":     {transport.OpGet, FindRequest{Database: "app", Bucket: name, Key: "ada"}},
		"count":   {transport.OpCount, CountRequest{Database: "app", Bucket: name}},
		"stats":   {transport.OpBucketStats, BucketStatsRequest{Database: "app", Bucket: name}},
		"info":    {transport.OpBucketInfo, BucketInfoRequest{Database: "app", Bucket: name}},
		"delete":  {transport.OpDeleteBucket, DeleteBucketRequest{Database: "app", Name: name}},
		"declare": {transport.OpDeclareStoreFields, DeclareStoreFieldsRequest{Database: "app", Name: name}},
		"create":  {transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: partName("Other", 1), Type: BucketTypeHash, Part: 1, PartOf: 5}},
	}
	for label, row := range requests {
		reply := send(row.op, row.request)
		text := string(reply.Payload)
		if reply.Opcode != transport.OpError && refusalText(t, reply) == "" {
			t.Errorf("%s: a client frame naming a part was answered %s", label, reply.Opcode)
		}
		if strings.Contains(text, unitSeparator) || strings.Contains(refusalTextOrEmpty(t, reply), unitSeparator) {
			t.Errorf("%s: the refusal holds 0x1F: %q", label, text)
		}
	}
	reply := send(transport.OpGet, FindRequest{Database: "app", Bucket: name, Key: "ada"})
	if reply.Opcode != transport.OpError || !strings.Contains(string(reply.Payload), `"User"`) || !strings.Contains(string(reply.Payload), "not found") {
		t.Errorf("a get naming a part answered %s %q, want NOT FOUND of the displayed name", reply.Opcode, reply.Payload)
	}
	if _, found, _ := s.readCatalogEntry("app", partName("Other", 1)); found {
		t.Error("a client create claiming to be a part made a bucket")
	}
	if entry, found, err := s.readCatalogEntry("app", name); err != nil || !found || entry.PartOf != 55 {
		t.Errorf("the part was changed by a refused client frame: %+v, %v, %v", entry, found, err)
	}
	// A frame that carries 0x1F in a value is not a frame that names a part.
	ok := send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: "ada", Data: map[string]interface{}{"Name": "A\x1fda"}})
	if ok.Opcode == transport.OpError {
		t.Errorf("a put with 0x1F in a value was refused: %s", ok.Payload)
	}
}

func refusalTextOrEmpty(t *testing.T, frame *transport.Frame) string {
	t.Helper()
	if frame.Opcode == transport.OpError {
		return refusalText(t, frame)
	}
	return string(frame.Payload)
}

// TestPartsStayOutOfListingsAndCounts is N9: LIST_BUCKETS, the LIST_DATABASES
// count and hoarddb_node_buckets count a collection once, whatever parts it
// has, at create, after a restart and after a drop. The template, which is
// the user's bucket, is listed.
func TestPartsStayOutOfListingsAndCounts(t *testing.T) {
	dataDir := t.TempDir()
	first := startCatalogServerAt(t, dataDir)
	s := first.core
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	if resp := createPlacementBucket(t, s, "User", BucketTypeHash, ""); !resp.Success {
		t.Fatalf("create the template: %+v", resp)
	}
	if resp := createPlacementBucket(t, s, "Orders", BucketTypeHash, "whole"); !resp.Success {
		t.Fatalf("create orders: %+v", resp)
	}
	for p := 0; p < 5; p++ {
		createPartByHand(t, s, "User", p, 99)
	}
	check := func(s *Server, when string, wantBuckets int64) {
		t.Helper()
		listing, err := s.ListBuckets(ListBucketsRequest{Database: "app"})
		if err != nil {
			t.Fatalf("%s: ListBuckets: %v", when, err)
		}
		var names []string
		for _, bucket := range listing.Buckets {
			names = append(names, bucket.Name)
			if strings.Contains(bucket.Name, unitSeparator) {
				t.Errorf("%s: LIST_BUCKETS names %q", when, bucket.Name)
			}
		}
		if len(names) != int(wantBuckets) {
			t.Errorf("%s: LIST_BUCKETS = %v, want %d buckets", when, names, wantBuckets)
		}
		databases, err := s.listDatabases(false)
		if err != nil {
			t.Fatalf("%s: listDatabases: %v", when, err)
		}
		if len(databases.Databases) != 1 || int64(databases.Databases[0].BucketCount) != wantBuckets {
			t.Errorf("%s: LIST_DATABASES = %+v, want a count of %d", when, databases.Databases, wantBuckets)
		}
		if got := s.bucketsAttached.Load(); got != wantBuckets {
			t.Errorf("%s: hoarddb_node_buckets counts %d, want %d", when, got, wantBuckets)
		}
	}
	check(s, "after creating a collection with 5 parts", 2)
	check(restartCatalog(t, first, dataDir).core, "after a restart", 2)
}

// TestPartsAreDroppedWithoutMovingTheCounts holds the other end: dropping a
// part by hand leaves the counts alone, and dropping the collection's
// template changes them by one.
func TestPartsAreDroppedWithoutMovingTheCounts(t *testing.T) {
	s := newTestServer(t)
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	if resp := createPlacementBucket(t, s, "User", BucketTypeHash, ""); !resp.Success {
		t.Fatalf("create the template: %+v", resp)
	}
	name := createPartByHand(t, s, "User", 1, 3)
	if got := s.bucketsAttached.Load(); got != 1 {
		t.Fatalf("hoarddb_node_buckets = %d with a template and a part, want 1", got)
	}
	if resp, err := s.DeleteBucket(DeleteBucketRequest{Database: "app", Name: name}); err != nil || !resp.Success {
		t.Fatalf("dropping the part = %+v, %v", resp, err)
	}
	if got := s.bucketsAttached.Load(); got != 1 {
		t.Errorf("hoarddb_node_buckets = %d after dropping a part, want 1 (it was never counted)", got)
	}
	databases, err := s.listDatabases(false)
	if err != nil || databases.Databases[0].BucketCount != 1 {
		t.Errorf("LIST_DATABASES = %+v, %v after dropping a part, want a count of 1", databases, err)
	}
}

// TestSpreadFieldsAddNoResidentBytes holds rule 11 for parts, part and
// part_of: they live in the catalog entry on disk and in the one flag word,
// never in the record. The record stays 88 bytes.
func TestSpreadFieldsAddNoResidentBytes(t *testing.T) {
	if got := unsafe.Sizeof(bucketRec{}); got != 88 {
		t.Errorf("unsafe.Sizeof(bucketRec{}) = %d, want 88 (parts, part and part_of are catalog fields; the part flag is a bit)", got)
	}
}

// TestPartFieldsAreReadForAMovingPart holds the source side of a migration: a
// part's number and template incarnation are read from its entry so the copy
// stays a part, and a plain bucket costs no read and has none.
func TestPartFieldsAreReadForAMovingPart(t *testing.T) {
	s := newTestServer(t)
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	if resp := createPlacementBucket(t, s, "User", BucketTypeHash, ""); !resp.Success {
		t.Fatalf("create the template: %+v", resp)
	}
	name := createPartByHand(t, s, "User", 3, 55)
	if part, partOf, err := s.partFieldsOf("app", name); err != nil || part != 3 || partOf != 55 {
		t.Errorf("partFieldsOf(part) = %d, %d, %v; want 3, 55", part, partOf, err)
	}
	if part, partOf, err := s.partFieldsOf("app", "User"); err != nil || part != 0 || partOf != 0 {
		t.Errorf("partFieldsOf(template) = %d, %d, %v; want none", part, partOf, err)
	}
}

// TestEveryCatalogEntryWriterCopiesThePartFields is T-63 for the fields of
// this step: the two literals that write a hash entry from a request or a
// pulled copy set Parts, Part and PartOf. A dropped field fails the next start
// (a template without parts is a pre-spread entry) or turns a part into a
// plain bucket that nothing lists.
func TestEveryCatalogEntryWriterCopiesThePartFields(t *testing.T) {
	writers := map[string]bool{"service.go:createBucketLocked": false, "migrate.go:materializeRange": false}
	fset := token.NewFileSet()
	for key := range writers {
		file, function, _ := strings.Cut(key, ":")
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", file, err)
		}
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != function {
				continue
			}
			ast.Inspect(fn, func(node ast.Node) bool {
				lit, ok := node.(*ast.CompositeLit)
				if !ok {
					return true
				}
				if ident, ok := lit.Type.(*ast.Ident); !ok || ident.Name != "catalogEntry" {
					return true
				}
				set := map[string]bool{}
				for _, element := range lit.Elts {
					if pair, ok := element.(*ast.KeyValueExpr); ok {
						if name, ok := pair.Key.(*ast.Ident); ok {
							set[name.Name] = true
						}
					}
				}
				if !set["Placement"] {
					return true // a carrier that is never written (T-63 lists it)
				}
				writers[key] = true
				for _, field := range []string{"Parts", "Part", "PartOf"} {
					if !set[field] {
						t.Errorf("%s: the catalogEntry literal does not set %s", key, field)
					}
				}
				return true
			})
		}
	}
	for key, found := range writers {
		if !found {
			t.Errorf("%s: no catalogEntry literal found; the scan is broken", key)
		}
	}
}

// TestBucketStatsOfAPartShowsTheCollection holds the describe boundary of
// displayBucket directly: the statistics of a part, asked for in-process (no
// client can reach it through the door), name the collection and no 0x1F.
func TestBucketStatsOfAPartShowsTheCollection(t *testing.T) {
	s := newTestServer(t)
	if resp, err := s.CreateDatabase("app"); err != nil || !resp.Success {
		t.Fatalf("CreateDatabase = %+v, %v", resp, err)
	}
	if resp := createPlacementBucket(t, s, "User", BucketTypeHash, ""); !resp.Success {
		t.Fatalf("create the template: %+v", resp)
	}
	name := createPartByHand(t, s, "User", 3, 55)
	stats, err := s.BucketStats(BucketStatsRequest{Database: "app", Bucket: name})
	if err != nil || !stats.Found {
		t.Fatalf("BucketStats(part) = %+v, %v", stats, err)
	}
	if stats.Bucket != "User" || strings.Contains(stats.Bucket, unitSeparator) {
		t.Errorf("BucketStats of a part names %q, want User", stats.Bucket)
	}
	info, err := s.BucketInfo(BucketInfoRequest{Database: "app", Bucket: name})
	if err != nil {
		t.Fatalf("BucketInfo(part): %v", err)
	}
	if info.Bucket.Name != "User" {
		t.Errorf("BucketInfo of a part names %q, want User", info.Bucket.Name)
	}
}
