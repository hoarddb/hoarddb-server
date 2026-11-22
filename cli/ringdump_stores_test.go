package cli

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// dumpElemsOf returns the elems records of one field of one entity, in file order.
func dumpElemsOf(records []dumpRecord, database, bucket, id, field string) []*dumpElems {
	var found []*dumpElems
	for _, record := range records {
		elems, isElems := record.(*dumpElems)
		if isElems && elems.DB == database && elems.B == bucket && elems.ID == id && elems.Field == field {
			found = append(found, elems)
		}
	}
	return found
}

// dumpElemsFields is every elems field of one entity in file order, so a test can pin that
// the store fields follow the declaration and each other in it.
func dumpElemsFields(records []dumpRecord, database, bucket, id string) []string {
	var fields []string
	for _, record := range records {
		elems, isElems := record.(*dumpElems)
		if isElems && elems.DB == database && elems.B == bucket && elems.ID == id {
			fields = append(fields, elems.Field)
		}
	}
	return fields
}

// dumpChunkStrings reads one string member of every element of a field, across its chunks
// in file order: the order StoreList answered in, which is the order the dump records.
func dumpChunkStrings(t *testing.T, chunks []*dumpElems, member string) []string {
	t.Helper()
	var values []string
	for _, chunk := range chunks {
		for _, element := range chunk.Els {
			value, ok := element.Lookup(member).StringValueOK()
			if !ok {
				t.Fatalf("an element of field %q has no string member %q: %v", chunk.Field, member, bson.Raw(element))
			}
			values = append(values, value)
		}
	}
	return values
}

// dumpChunkInt32 reads one int32 member of every element of a field, across its chunks in
// file order.
func dumpChunkInt32(t *testing.T, chunks []*dumpElems, member string) []int32 {
	t.Helper()
	var values []int32
	for _, chunk := range chunks {
		for _, element := range chunk.Els {
			value, ok := element.Lookup(member).Int32OK()
			if !ok {
				t.Fatalf("an element of field %q has no int32 member %q: %v", chunk.Field, member, bson.Raw(element))
			}
			values = append(values, value)
		}
	}
	return values
}

// TestDumpCarriesEveryStoreTypeInStoreOrder: every non-blob store field of an entity is in
// the file with its own kind, in the declaration's order and in the store's own StoreList
// order (section 3.1, "Order of elements"). A fifoStore and a lifoStore list in arrival
// order, oldest first, whatever a pop takes; a hashStore and a btreeStore list in element
// key order. The heap is on the stub, because a heap push client call does not exist yet.
func TestDumpCarriesEveryStoreTypeInStoreOrder(t *testing.T) {
	t.Run("fifo, lifo, hash and btree on the ring", func(t *testing.T) {
		addr, client := newRingTestHost(t)
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		dumpTestDatabase(t, client, "app")
		if _, err := client.CreateBucket(ctx, "app", "User", []ringclient.FieldDecl{
			{Name: "Name", Type: "string"},
			{Name: "Purchases", Type: "fifoStore"},
			{Name: "Photos", Type: "lifoStore"},
			{Name: "Friends", Type: "hashStore"},
			{Name: "Recipes", Type: "btreeStore"},
		}); err != nil {
			t.Fatalf("creating app.User on the test host: %v", err)
		}
		user := client.Bucket("app", "User")
		if _, err := user.Insert(ctx, "ada", ringclient.Doc{"Name": "Ada"}); err != nil {
			t.Fatalf("inserting ada into app.User: %v", err)
		}
		// The order each store is written in; what StoreList must answer is pinned below.
		for _, product := range []string{"tea", "cake", "jam"} {
			if _, err := user.Push(ctx, "ada", "Purchases", ringclient.Doc{"Product": product}); err != nil {
				t.Fatalf("pushing %q into ada's Purchases: %v", product, err)
			}
		}
		for _, photo := range []string{"beach", "hike", "sunset"} {
			if _, err := user.Push(ctx, "ada", "Photos", ringclient.Doc{"Title": photo}); err != nil {
				t.Fatalf("pushing %q into ada's Photos: %v", photo, err)
			}
		}
		for _, friend := range []string{"zoe", "amy", "mia"} {
			if _, err := user.Put(ctx, "ada", "Friends", friend, ringclient.Doc{"Name": friend}); err != nil {
				t.Fatalf("putting %q into ada's Friends: %v", friend, err)
			}
		}
		for _, recipe := range []string{"pear", "apple", "fig"} {
			if _, err := user.Put(ctx, "ada", "Recipes", recipe, ringclient.Doc{"Title": recipe}); err != nil {
				t.Fatalf("putting %q into ada's Recipes: %v", recipe, err)
			}
		}
		outPath := filepath.Join(t.TempDir(), "stores.hoarddump")

		code, stdout, stderr := dumpOnHost(t, addr, "--db", "app", "--bucket", "User", "--out", outPath)
		if code != ExitOK {
			t.Fatalf("dumping a bucket of four store types: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
		}
		records := readDumpRecords(t, outPath)

		if want := []string{"Purchases", "Photos", "Friends", "Recipes"}; !reflect.DeepEqual(dumpElemsFields(records, "app", "User", "ada"), want) {
			t.Errorf("the elems records of ada are %v, want the declared order %v",
				dumpElemsFields(records, "app", "User", "ada"), want)
		}

		// A fifoStore lists in arrival order, oldest first: the same order a restore must
		// push in for a pop to take the original order.
		purchases := dumpElemsOf(records, "app", "User", "ada", "Purchases")
		if len(purchases) != 1 {
			t.Fatalf("the dump holds %d elems records for Purchases, want 1", len(purchases))
		}
		if purchases[0].Kind != "fifoStore" || purchases[0].Seq != 0 || purchases[0].Keys != nil || purchases[0].Prios != nil {
			t.Errorf("the Purchases record is kind %q seq %d keys %v prios %v, want fifoStore, 0 and no keys or priorities",
				purchases[0].Kind, purchases[0].Seq, purchases[0].Keys, purchases[0].Prios)
		}
		if want := []string{"tea", "cake", "jam"}; !reflect.DeepEqual(dumpChunkStrings(t, purchases, "Product"), want) {
			t.Errorf("Purchases holds %v, want arrival order (oldest first) %v", dumpChunkStrings(t, purchases, "Product"), want)
		}

		// A lifoStore lists in arrival order too (a pop takes the newest), so the record is
		// the push order and a restore that pushes in the record's order keeps the pop order.
		photos := dumpElemsOf(records, "app", "User", "ada", "Photos")
		if len(photos) != 1 {
			t.Fatalf("the dump holds %d elems records for Photos, want 1", len(photos))
		}
		if photos[0].Kind != "lifoStore" || photos[0].Seq != 0 || photos[0].Keys != nil || photos[0].Prios != nil {
			t.Errorf("the Photos record is kind %q seq %d keys %v prios %v, want lifoStore, 0 and no keys or priorities",
				photos[0].Kind, photos[0].Seq, photos[0].Keys, photos[0].Prios)
		}
		if want := []string{"beach", "hike", "sunset"}; !reflect.DeepEqual(dumpChunkStrings(t, photos, "Title"), want) {
			t.Errorf("Photos holds %v, want arrival order (oldest first) %v", dumpChunkStrings(t, photos, "Title"), want)
		}

		for _, keyed := range []struct {
			field  string
			member string
			want   []string
		}{
			{field: "Friends", member: "Name", want: []string{"amy", "mia", "zoe"}},
			{field: "Recipes", member: "Title", want: []string{"apple", "fig", "pear"}},
		} {
			chunks := dumpElemsOf(records, "app", "User", "ada", keyed.field)
			if len(chunks) != 1 {
				t.Fatalf("the dump holds %d elems records for %s, want 1", len(chunks), keyed.field)
			}
			if chunks[0].Kind != "hashStore" && chunks[0].Kind != "btreeStore" {
				t.Errorf("the %s record is kind %q, want the declared store word", keyed.field, chunks[0].Kind)
			}
			if chunks[0].Seq != 0 || chunks[0].Prios != nil {
				t.Errorf("the %s record is seq %d prios %v, want seq 0 and no priorities", keyed.field, chunks[0].Seq, chunks[0].Prios)
			}
			if !reflect.DeepEqual(chunks[0].Keys, keyed.want) {
				t.Errorf("%s carries the keys %v, want element key order %v", keyed.field, chunks[0].Keys, keyed.want)
			}
			if got := dumpChunkStrings(t, chunks, keyed.member); !reflect.DeepEqual(got, keyed.want) {
				t.Errorf("%s holds the elements %v, want %v in key order", keyed.field, got, keyed.want)
			}
		}

		verification, err := verifyDump(outPath)
		if err != nil {
			t.Fatalf("the dump %s does not pass the check restore --verify runs: %v", outPath, err)
		}
		if verification.Entities != 1 || verification.Elements != 12 {
			t.Errorf("the verification reads %d entities and %d elements, want 1 and 12 (three per store, four stores)",
				verification.Entities, verification.Elements)
		}
	})

	t.Run("heap on the stub", func(t *testing.T) {
		// A heapStore lists highest priority first (cluster/wire/fieldread.go:ListField), and
		// its entry ids and priorities travel with the elements. The stub answers in that
		// order, so a dump that reorders, drops or misaligns them fails here.
		entryIDs := []string{"entry-9", "entry-4", "entry-1"}
		priorities := []int64{90, 40, 10}
		titles := []string{"nine", "four", "one"}
		elements := make([]bson.Raw, len(entryIDs))
		for index := range elements {
			body, err := bson.Marshal(bson.D{{Key: "Title", Value: titles[index]}})
			if err != nil {
				t.Fatalf("building heap element %d: %v", index, err)
			}
			elements[index] = bson.Raw(body)
		}
		documentBody, err := bson.Marshal(bson.D{
			{Key: dumpBucketIDField, Value: "b1"},
			{Key: "Name", Value: "B"},
		})
		if err != nil {
			t.Fatalf("building the body of b1: %v", err)
		}
		stub := &dumpStub{
			databasePages: dumpStubDatabasePages([]ringclient.DatabaseSummary{{Name: "app", RF: 1}}, 500),
			bucketPages: map[string][]ringclient.BucketPage{
				"app": dumpStubBucketPages([]ringclient.BucketSummary{{Name: "Book", State: "active"}}, 500),
			},
			descriptions: map[string]ringclient.DatabaseDescription{"app": {Name: "app", RF: 1, Storage: "disk"}},
			declarations: map[string]ringclient.BucketInfo{
				"app/Book": {Database: "app", Name: "Book", State: "active", Fields: []ringclient.FieldInfo{
					{Name: dumpBucketIDField, Type: "string"},
					{Name: "Name", Type: "string"},
					{Name: "Books", Type: "heapStore"},
				}},
			},
			documentPages: map[string][]ringclient.EntryPage{
				"app/Book": dumpStubDocumentPages([]ringclient.Entry{{ID: "b1", Body: bson.Raw(documentBody)}}, ringDumpListPageSize),
			},
			elementPages: map[string][]ringclient.ElementPage{
				"app/Book/b1/Books": {{
					Elements: elements, Keys: entryIDs, Priorities: priorities,
				}},
			},
		}
		outPath := filepath.Join(t.TempDir(), "heap.hoarddump")

		code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--bucket", "Book", "--out", outPath)
		if code != ExitOK {
			t.Fatalf("dumping a heapStore field: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
		}
		chunks := dumpElemsOf(readDumpRecords(t, outPath), "app", "Book", "b1", "Books")
		if len(chunks) != 1 {
			t.Fatalf("the dump holds %d elems records for Books, want 1", len(chunks))
		}
		if chunks[0].Kind != "heapStore" || chunks[0].Seq != 0 {
			t.Errorf("the Books record is kind %q seq %d, want heapStore and 0", chunks[0].Kind, chunks[0].Seq)
		}
		if !reflect.DeepEqual(chunks[0].Keys, entryIDs) {
			t.Errorf("Books carries the entry ids %v, want %v in the store's order", chunks[0].Keys, entryIDs)
		}
		if !reflect.DeepEqual(chunks[0].Prios, priorities) {
			t.Errorf("Books carries the priorities %v, want %v in the store's order", chunks[0].Prios, priorities)
		}
		if got := dumpChunkStrings(t, chunks, "Title"); !reflect.DeepEqual(got, titles) {
			t.Errorf("Books holds the elements %v, want %v in the store's order", got, titles)
		}
		if want := []string{"app/Book/b1/Books"}; !reflect.DeepEqual(stub.calls.storeListFields, want) {
			t.Errorf("StoreList was asked for %v, want %v", stub.calls.storeListFields, want)
		}
		if len(stub.calls.storeListLimits) != 1 || stub.calls.storeListLimits[0] != 1000 {
			t.Errorf("StoreList limits %v, want one request for 1000 elements", stub.calls.storeListLimits)
		}
		if len(stub.calls.storeListAfters) != 1 || stub.calls.storeListAfters[0] != nil {
			t.Errorf("StoreList cursors %v, want one first request with no cursor", stub.calls.storeListAfters)
		}
	})
}

// TestDumpChunksALargeStore: one store field of 2,500 small elements is three elems
// records, seq 0, 1 and 2, holding 1000, 1000 and 500 elements, and the summary counts
// every element once (section 3.1, the 1000-element cap).
func TestDumpChunksALargeStore(t *testing.T) {
	const elementCount = 2500
	elements := make([]bson.Raw, elementCount)
	for index := range elements {
		body, err := bson.Marshal(bson.D{{Key: "N", Value: int32(index)}})
		if err != nil {
			t.Fatalf("building element %d: %v", index, err)
		}
		elements[index] = bson.Raw(body)
	}
	documentBody, err := bson.Marshal(bson.D{{Key: dumpBucketIDField, Value: "u1"}})
	if err != nil {
		t.Fatalf("building the body of u1: %v", err)
	}
	stub := &dumpStub{
		databasePages: dumpStubDatabasePages([]ringclient.DatabaseSummary{{Name: "app", RF: 1}}, 500),
		bucketPages: map[string][]ringclient.BucketPage{
			"app": dumpStubBucketPages([]ringclient.BucketSummary{{Name: "User", State: "active"}}, 500),
		},
		descriptions: map[string]ringclient.DatabaseDescription{"app": {Name: "app", RF: 1, Storage: "disk"}},
		declarations: map[string]ringclient.BucketInfo{
			"app/User": {Database: "app", Name: "User", State: "active", Fields: []ringclient.FieldInfo{
				{Name: dumpBucketIDField, Type: "string"},
				{Name: "Feed", Type: "fifoStore"},
			}},
		},
		documentPages: map[string][]ringclient.EntryPage{
			"app/User": dumpStubDocumentPages([]ringclient.Entry{{ID: "u1", Body: bson.Raw(documentBody)}}, ringDumpListPageSize),
		},
		elementPages: map[string][]ringclient.ElementPage{
			"app/User/u1/Feed": dumpStubElementPages(ringclient.ElementPage{Elements: elements}, 1000),
		},
	}
	outPath := filepath.Join(t.TempDir(), "large.hoarddump")

	code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--out", outPath)
	if code != ExitOK {
		t.Fatalf("dumping 2,500 elements: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
	chunks := dumpElemsOf(readDumpRecords(t, outPath), "app", "User", "u1", "Feed")
	if len(chunks) != 3 {
		t.Fatalf("the dump holds %d elems records for 2,500 elements, want 3", len(chunks))
	}
	wantLengths := []int{1000, 1000, 500}
	for index, chunk := range chunks {
		if chunk.Seq != int32(index) {
			t.Errorf("elems record %d carries seq %d, want %d", index+1, chunk.Seq, index)
		}
		if len(chunk.Els) != wantLengths[index] {
			t.Errorf("elems record %d holds %d elements, want %d", index+1, len(chunk.Els), wantLengths[index])
		}
	}
	order := dumpChunkInt32(t, chunks, "N")
	if len(order) != elementCount {
		t.Fatalf("the chunks hold %d elements together, want %d", len(order), elementCount)
	}
	for index, number := range order {
		if number != int32(index) {
			t.Fatalf("element %d carries N %d: the chunks must follow the pages in order", index, number)
		}
	}
	if want := elementCount / 1000; stub.calls.StoreList != want+1 {
		t.Errorf("StoreList was called %d times for 2,500 elements served 1,000 per page, want %d (one page at a time, following the cursor)",
			stub.calls.StoreList, want+1)
	}
	verification, err := verifyDump(outPath)
	if err != nil {
		t.Fatalf("the dump %s does not pass the check restore --verify runs: %v", outPath, err)
	}
	if verification.Elements != elementCount {
		t.Errorf("the verification reads %d elements, want %d", verification.Elements, elementCount)
	}
}

// TestDumpChunksOneElementLargerThanTheByteCap: the first element of a chunk always goes in,
// however large, so a 20 MiB element (under the node's 24 MiB document default, over the
// 12 MiB elems byte cap) is a chunk by itself and the elements after it fill the next one
// (section 3.1).
func TestDumpChunksOneElementLargerThanTheByteCap(t *testing.T) {
	large, err := bson.Marshal(bson.D{{Key: "Blob", Value: strings.Repeat("x", 20<<20)}})
	if err != nil {
		t.Fatalf("building the 20 MiB element: %v", err)
	}
	small := make([]bson.Raw, 2)
	for index := range small {
		body, err := bson.Marshal(bson.D{{Key: "Blob", Value: "s"}})
		if err != nil {
			t.Fatalf("building small element %d: %v", index, err)
		}
		small[index] = bson.Raw(body)
	}
	documentBody, err := bson.Marshal(bson.D{{Key: dumpBucketIDField, Value: "u1"}})
	if err != nil {
		t.Fatalf("building the body of u1: %v", err)
	}
	stub := &dumpStub{
		databasePages: dumpStubDatabasePages([]ringclient.DatabaseSummary{{Name: "app", RF: 1}}, 500),
		bucketPages: map[string][]ringclient.BucketPage{
			"app": dumpStubBucketPages([]ringclient.BucketSummary{{Name: "User", State: "active"}}, 500),
		},
		descriptions: map[string]ringclient.DatabaseDescription{"app": {Name: "app", RF: 1, Storage: "disk"}},
		declarations: map[string]ringclient.BucketInfo{
			"app/User": {Database: "app", Name: "User", State: "active", Fields: []ringclient.FieldInfo{
				{Name: dumpBucketIDField, Type: "string"},
				{Name: "Blobs", Type: "fifoStore"},
			}},
		},
		documentPages: map[string][]ringclient.EntryPage{
			"app/User": dumpStubDocumentPages([]ringclient.Entry{{ID: "u1", Body: bson.Raw(documentBody)}}, ringDumpListPageSize),
		},
		elementPages: map[string][]ringclient.ElementPage{
			"app/User/u1/Blobs": dumpStubElementPages(ringclient.ElementPage{Elements: []bson.Raw{bson.Raw(large), small[0], small[1]}}, 1000),
		},
	}
	outPath := filepath.Join(t.TempDir(), "big.hoarddump")

	code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--out", outPath)
	if code != ExitOK {
		t.Fatalf("dumping an element over the byte cap: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
	chunks := dumpElemsOf(readDumpRecords(t, outPath), "app", "User", "u1", "Blobs")
	if len(chunks) != 2 {
		t.Fatalf("the dump holds %d elems records for [large, small, small], want 2", len(chunks))
	}
	if len(chunks[0].Els) != 1 {
		t.Fatalf("the first chunk holds %d elements, want the 20 MiB element alone", len(chunks[0].Els))
	}
	if len(chunks[0].Els[0]) != len(large) {
		t.Fatalf("the first chunk's element is %d bytes, want the %d-byte element", len(chunks[0].Els[0]), len(large))
	}
	if chunks[0].Seq != 0 || chunks[1].Seq != 1 {
		t.Errorf("the chunks carry seq %d and %d, want 0 and 1", chunks[0].Seq, chunks[1].Seq)
	}
	if len(chunks[1].Els) != 2 || len(chunks[1].Els[0]) != len(small[0]) {
		t.Errorf("the second chunk holds %d elements, want the two small ones", len(chunks[1].Els))
	}
	if len(chunks[0].Els[0]) < 12<<20 {
		t.Fatalf("the large element is %d bytes and the test needs one over the 12 MiB cap", len(chunks[0].Els[0]))
	}
	verification, err := verifyDump(outPath)
	if err != nil {
		t.Fatalf("the dump %s does not pass the check restore --verify runs: %v", outPath, err)
	}
	if verification.Elements != 3 {
		t.Errorf("the verification reads %d elements, want 3", verification.Elements)
	}
	t.Logf("the 20 MiB element is %d bytes and the two small ones are %d and %d bytes", len(large), len(small[0]), len(small[1]))
}

// TestDumpWarnsForEachBlobField: a blobStore field is skipped without a StoreList call, and
// the bucket says once per blob field that its payload is not in the file, however many
// entities the bucket holds (section 3.1, the bucket record's unsupported list).
func TestDumpWarnsForEachBlobField(t *testing.T) {
	elements := make([]bson.Raw, 1)
	body, err := bson.Marshal(bson.D{{Key: "Product", Value: "tea"}})
	if err != nil {
		t.Fatalf("building the fifo element: %v", err)
	}
	elements[0] = bson.Raw(body)
	entries := make([]ringclient.Entry, 2)
	ids := []string{"f1", "f2"}
	for index := range entries {
		documentBody, err := bson.Marshal(bson.D{{Key: dumpBucketIDField, Value: ids[index]}})
		if err != nil {
			t.Fatalf("building the body of %q: %v", ids[index], err)
		}
		entries[index] = ringclient.Entry{ID: ids[index], Body: bson.Raw(documentBody)}
	}
	stub := &dumpStub{
		databasePages: dumpStubDatabasePages([]ringclient.DatabaseSummary{{Name: "app", RF: 1}}, 500),
		bucketPages: map[string][]ringclient.BucketPage{
			"app": dumpStubBucketPages([]ringclient.BucketSummary{{Name: "Files", State: "active"}}, 500),
		},
		descriptions: map[string]ringclient.DatabaseDescription{"app": {Name: "app", RF: 1, Storage: "disk"}},
		declarations: map[string]ringclient.BucketInfo{
			"app/Files": {Database: "app", Name: "Files", State: "active", Fields: []ringclient.FieldInfo{
				{Name: dumpBucketIDField, Type: "string"},
				{Name: "Data", Type: "blobStore"},
				{Name: "Thumb", Type: "blobStore"},
				{Name: "Purchases", Type: "fifoStore"},
			}},
		},
		documentPages: map[string][]ringclient.EntryPage{
			"app/Files": dumpStubDocumentPages(entries, ringDumpListPageSize),
		},
		elementPages: map[string][]ringclient.ElementPage{
			"app/Files/f1/Purchases": dumpStubElementPages(ringclient.ElementPage{Elements: elements}, 1000),
			"app/Files/f2/Purchases": dumpStubElementPages(ringclient.ElementPage{Elements: elements}, 1000),
		},
	}
	outPath := filepath.Join(t.TempDir(), "files.hoarddump")

	code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--out", outPath)
	if code != ExitOK {
		t.Fatalf("dumping a bucket with two blob fields: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
	for _, field := range []string{"Data", "Thumb"} {
		want := "Bucket app.Files field " + field + " (blobStore): blob payloads are not dumped, so they are not in this file and restore cannot bring them back."
		if got := strings.Count(stderr, want); got != 1 {
			t.Errorf("the run says the blob warning for %q %d times, want exactly 1 (one per bucket and field, not per entity):\nstderr: %s", field, got, stderr)
		}
	}
	if got := strings.Count(stderr, "(blobStore)"); got != 2 {
		t.Errorf("the run prints %d blob warnings, want 2 (one per blobStore field):\nstderr: %s", got, stderr)
	}
	for _, asked := range stub.calls.storeListFields {
		if strings.HasSuffix(asked, "/Data") || strings.HasSuffix(asked, "/Thumb") {
			t.Errorf("StoreList was asked for the blob field %s; a blob payload is never listed and reading one is wasted work", asked)
		}
	}
	if got := stub.calls.storeListCallsFor("app/Files/f1/Purchases"); got != 1 {
		t.Errorf("StoreList was asked for the first entity's Purchases %d times, want 1", got)
	}
	if got := stub.calls.storeListCallsFor("app/Files/f2/Purchases"); got != 1 {
		t.Errorf("StoreList was asked for the second entity's Purchases %d times, want 1", got)
	}
}

// TestDumpOfAQueueWithAClaimedMessageLeavesItOutAndSaysSo (plan rev 7, section 3.2 item 5):
// a queue is read through StoreList like every store and nothing else is called for it, so
// the messages a consumer holds under a lease are in no list reply and are absent from the
// file. The dump says so. This pins today's limitation: bringing a queue-state read back
// has to change this test.
func TestDumpOfAQueueWithAClaimedMessageLeavesItOutAndSaysSo(t *testing.T) {
	addr, client := newRingTestHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := client.CreateDatabase(ctx, "app", 1); err != nil {
		t.Fatalf("creating database app on the test host: %v", err)
	}
	if _, err := client.CreateBucket(ctx, "app", "Jobs", []ringclient.FieldDecl{
		{Name: "Name", Type: "string"},
		{Name: "Queue", Type: "fifoStore"},
	}); err != nil {
		t.Fatalf("creating app.Jobs on the test host: %v", err)
	}
	jobs := client.Bucket("app", "Jobs")
	if _, err := jobs.Insert(ctx, "job-7", ringclient.Doc{"Name": "seven"}); err != nil {
		t.Fatalf("inserting job-7 into app.Jobs: %v", err)
	}
	for _, message := range []string{"m1", "m2", "m3", "m4", "m5"} {
		if _, err := jobs.Push(ctx, "job-7", "Queue", ringclient.Doc{"Message": message}); err != nil {
			t.Fatalf("pushing %q into job-7's Queue: %v", message, err)
		}
	}
	// Three messages are under a lease a consumer still holds: they are in the lease's own
	// records, and StoreList walks element keys only.
	lease, err := jobs.Claim(ctx, "job-7", "Queue", 3)
	if err != nil {
		t.Fatalf("claiming three messages of job-7's Queue: %v", err)
	}
	if len(lease.Elements) != 3 {
		t.Fatalf("the claim returned %d messages, want 3", len(lease.Elements))
	}
	outPath := filepath.Join(t.TempDir(), "jobs.hoarddump")

	code, stdout, stderr := dumpOnHost(t, addr, "--db", "app", "--bucket", "Jobs", "--out", outPath)
	if code != ExitOK {
		t.Fatalf("dumping a queue with a claimed message: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
	chunks := dumpElemsOf(readDumpRecords(t, outPath), "app", "Jobs", "job-7", "Queue")
	if len(chunks) != 1 {
		t.Fatalf("the dump holds %d elems records for Queue, want 1", len(chunks))
	}
	if want := []string{"m4", "m5"}; !reflect.DeepEqual(dumpChunkStrings(t, chunks, "Message"), want) {
		t.Errorf("Queue holds %v, want %v: the three claimed messages are in no list reply", dumpChunkStrings(t, chunks, "Message"), want)
	}
	if !strings.Contains(stderr, dumpQueueLeftOutLine) {
		t.Errorf("the run does not carry the queue sentence %q:\nstderr: %s", dumpQueueLeftOutLine, stderr)
	}
	lines := strings.Split(strings.TrimRight(stderr, "\n"), "\n")
	doneAt := -1
	for index, line := range lines {
		if strings.HasPrefix(line, "Done: ") {
			doneAt = index
			break
		}
	}
	if doneAt < 0 || doneAt+1 >= len(lines) || lines[doneAt+1] != dumpQueueLeftOutLine {
		t.Errorf("the queue sentence does not follow the end line (Done line %d of %d):\nstderr: %s", doneAt, len(lines), stderr)
	}
	verification, err := verifyDump(outPath)
	if err != nil {
		t.Fatalf("the dump %s does not pass the check restore --verify runs: %v", outPath, err)
	}
	if verification.Elements != 2 {
		t.Errorf("the verification reads %d elements, want 2 (the five pushed less the three claimed)", verification.Elements)
	}
}

// TestDumpOfADocumentDeletedAfterItsListEntryKeepsItAndGoesOn: a dump runs while the
// database serves, so a document can be deleted between the page that listed it and the
// read of its stores. The ring answers NOT_FOUND for the store read; the dump keeps the doc
// record it already wrote (a document deleted after the dump passed is still in, section
// 3.6), writes no elements for it and carries on, instead of failing the whole run on one
// delete. Any other refusal still stops the run (TestDumpFailureLeavesNoFinalFile).
func TestDumpOfADocumentDeletedAfterItsListEntryKeepsItAndGoesOn(t *testing.T) {
	feed, err := bson.Marshal(bson.D{{Key: "N", Value: int32(1)}})
	if err != nil {
		t.Fatalf("building an element: %v", err)
	}
	entries := make([]ringclient.Entry, 0, 2)
	for _, id := range []string{"u1", "u2"} {
		body, err := bson.Marshal(bson.D{{Key: dumpBucketIDField, Value: id}})
		if err != nil {
			t.Fatalf("building the body of %s: %v", id, err)
		}
		entries = append(entries, ringclient.Entry{ID: id, Body: bson.Raw(body)})
	}
	stub := &dumpStub{
		databasePages: dumpStubDatabasePages([]ringclient.DatabaseSummary{{Name: "app", RF: 1}}, 500),
		bucketPages: map[string][]ringclient.BucketPage{
			"app": dumpStubBucketPages([]ringclient.BucketSummary{{Name: "User", State: "active"}}, 500),
		},
		descriptions: map[string]ringclient.DatabaseDescription{"app": {Name: "app", RF: 1, Storage: "disk"}},
		declarations: map[string]ringclient.BucketInfo{
			"app/User": {Database: "app", Name: "User", State: "active", Fields: []ringclient.FieldInfo{
				{Name: dumpBucketIDField, Type: "string"},
				{Name: "Feed", Type: "fifoStore"},
			}},
		},
		documentPages: map[string][]ringclient.EntryPage{
			"app/User": dumpStubDocumentPages(entries, ringDumpListPageSize),
		},
		elementPages: map[string][]ringclient.ElementPage{
			"app/User/u1/Feed": {{Elements: []bson.Raw{bson.Raw(feed)}}},
		},
		failures: map[string]error{
			"StoreList app/User/u2/Feed": &ringclient.Error{Code: cluster.CodeNotFound,
				Message: `document "u2" is not in app.User. Run 'db.User.list({limit: 10})' to see what exists`},
		},
	}
	outPath := filepath.Join(t.TempDir(), "deleted.hoarddump")

	code, stdout, stderr := runStubDump(t, stub, "--db", "app", "--out", outPath)
	if code != ExitOK {
		t.Fatalf("dumping with u2 deleted after its list entry: exit %d, want %d\nstdout: %s\nstderr: %s", code, ExitOK, stdout, stderr)
	}
	if strings.Contains(stderr, "u2") {
		t.Errorf("the dump warned about the deleted entity, and it does not warn per entity:\n%s", stderr)
	}
	records := readDumpRecords(t, outPath)
	docs := 0
	for _, record := range records {
		if doc, isDoc := record.(*dumpDoc); isDoc && (doc.ID == "u1" || doc.ID == "u2") {
			docs++
		}
	}
	if docs != 2 {
		t.Errorf("the dump holds %d of the two doc records, want both: a document deleted after it was listed is still in", docs)
	}
	if got := len(dumpElemsOf(records, "app", "User", "u1", "Feed")); got != 1 {
		t.Errorf("u1 has %d elems records, want 1", got)
	}
	if got := len(dumpElemsOf(records, "app", "User", "u2", "Feed")); got != 0 {
		t.Errorf("u2 has %d elems records, want none: its store was gone", got)
	}
	if _, err := verifyDump(outPath); err != nil {
		t.Errorf("the dump does not verify: %v", err)
	}
}
