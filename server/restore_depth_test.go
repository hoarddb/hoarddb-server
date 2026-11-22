package server

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// BUG-0663: a dump record wraps a client's document, and restore scanned every
// record at one flat limit. A document at the bound, which a client may store,
// came back out of a dump one level deeper in some records, and the restore
// refused the whole bucket. These tests store the deepest document a client may
// send, through the handler a client reaches, dump it, and restore it into a
// fresh server.

// deepClientDocument is a document nested to exactly maxDocumentDepth levels,
// the deepest a client may send, as the map a request carries.
func deepClientDocument(levels int) map[string]interface{} {
	if levels <= 1 {
		return map[string]interface{}{"n": int32(1)}
	}
	return map[string]interface{}{"n": int32(1), "a": deepClientDocument(levels - 1)}
}

func TestDeepClientDocumentIsExactlyTheBound(t *testing.T) {
	encoded := mustMarshalBSON(t, deepClientDocument(maxDocumentDepth))
	if _, err := validateAt(encoded, maxDocumentDepth); err != nil {
		t.Fatalf("the test document is deeper than %d levels: %v", maxDocumentDepth, err)
	}
	if _, err := validateAt(encoded, maxDocumentDepth-1); !errors.Is(err, ErrDocumentTooDeep) {
		t.Fatalf("the test document is shallower than %d levels: it would not exercise the bound", maxDocumentDepth)
	}
}

// restoreEverySection restores d into dest as the CLI would, every section the
// dump holds, and returns the server's closing answer. A chunk the server
// refuses fails the test with its words.
func restoreEverySection(t *testing.T, dest *Server, d dumpedBucket) *RestoreDoneResponse {
	t.Helper()
	started, err := dest.startRestore(rootContext(), RestoreStartRequest{
		Database: "app", Bucket: d.manifest.Bucket, FormatVersion: d.manifest.FormatVersion, StoreType: d.manifest.StoreType,
		Stores: d.manifest.Stores, Bytes: 1, Dead: d.done.Dead,
	})
	if err != nil {
		t.Fatalf("starting the restore of %s: %v", d.manifest.Bucket, err)
	}
	for _, section := range []string{"", dumpSectionStores, dumpSectionDead} {
		if len(d.sections[section]) == 0 {
			continue
		}
		if _, err := dest.restoreChunk(rootContext(), RestoreChunkRequest{
			Database: "app", Bucket: d.manifest.Bucket, SessionID: started.SessionID, Data: d.sections[section], Section: section,
		}); err != nil {
			t.Fatalf("restoring the %q section of %s: %v", section, d.manifest.Bucket, err)
		}
	}
	done, err := dest.restoreDone(rootContext(), RestoreDoneRequest{
		Database: "app", Bucket: d.manifest.Bucket, SessionID: started.SessionID,
		Documents: d.done.Documents, Elements: d.done.Elements, Dead: d.done.Dead,
	})
	if err != nil || !done.Success {
		t.Fatalf("finishing the restore of %s: %+v, %v", d.manifest.Bucket, done, err)
	}
	return done
}

// clientSend sends one request the way a client's connection does, through the
// handler, so a document the server's request scan would refuse fails the setup
// rather than hiding in a test that skipped it.
func clientSend(t *testing.T, s *Server, op transport.Opcode, request any) {
	t.Helper()
	handlerSend(t, NewHandler(s), op, request)
}

var queuePushOpcodes = map[BucketType]transport.Opcode{
	BucketTypeFIFO: transport.OpFIFOPush, BucketTypeLIFO: transport.OpLIFOPush, BucketTypeHeap: transport.OpHeapPush,
}

// pushDeepEntry stores the deepest entry a client may push into bucket.
func pushDeepEntry(t *testing.T, s *Server, bucket string, bucketType BucketType) {
	t.Helper()
	clientSend(t, s, queuePushOpcodes[bucketType], PushRequest{
		Database: "app", Bucket: bucket, Priority: 10, Data: deepClientDocument(maxDocumentDepth),
	})
}

// deadLetterHeadEntry claims the head entry until its deliveries run out and the
// next claim dead-letters it, as a consumer that never acks does.
func deadLetterHeadEntry(t *testing.T, s *Server, clock *leaseClockAt, bucket string) {
	t.Helper()
	for claims := 0; claims <= store.MaxDeliveries; claims++ {
		claimOf(t, s, bucket, 1, 1_000)
		clock.advance(2 * time.Second)
	}
	if dead := bucketStateOf(t, s, "app", bucket).Dead; dead != 1 {
		t.Fatalf("setup: %s holds %d dead entries after %d unacked claims, want 1", bucket, dead, store.MaxDeliveries+1)
	}
}

// TestADumpOfTheDeepestQueueEntriesRestores walks every way a queue or heap
// entry reaches a dump: live, claimed, and dead-lettered. The dead record of a
// heap holds its entry two levels down, so a 100-level entry is 102 deep in it.
func TestADumpOfTheDeepestQueueEntriesRestores(t *testing.T) {
	for _, bucketType := range leaseBucketTypes {
		for _, state := range []string{"live", "claimed", "dead"} {
			t.Run(string(bucketType)+"/"+state, func(t *testing.T) {
				source := newTestServer(t)
				clock := withLeaseClock(source)
				bucket := string(bucketType)
				createTestBucket(t, source, "app", bucket, bucketType)
				pushDeepEntry(t, source, bucket, bucketType)
				switch state {
				case "claimed":
					claimOf(t, source, bucket, 1, 60_000)
				case "dead":
					deadLetterHeadEntry(t, source, clock, bucket)
				}

				dumped := dumpSections(t, source, bucket, DumpStartRequest{AcceptDead: true})
				dest := newTestServer(t)
				withLeaseClock(dest)
				answer := restoreEverySection(t, dest, dumped)

				wantLive, wantDead := int64(1), int64(0)
				if state == "dead" {
					wantLive, wantDead = 0, 1
				}
				if answer.Applied != wantLive || answer.DeadApplied != wantDead {
					t.Fatalf("the restore applied %d entries and %d dead entries, want %d and %d", answer.Applied, answer.DeadApplied, wantLive, wantDead)
				}
				// Compared by count and entry number: a restored entry is pushed again, and
				// is not promised the byte-identical record it was dumped as.
				again := dumpSections(t, dest, bucket, DumpStartRequest{AcceptDead: true})
				if again.done.Documents != dumped.done.Documents || !slices.Equal(again.documentNumbers(t), dumped.documentNumbers(t)) {
					t.Errorf("the restored bucket dumps %d live entries %v, want the source's %d, %v",
						again.done.Documents, again.documentNumbers(t), dumped.done.Documents, dumped.documentNumbers(t))
				}
				want, got := deadListOf(t, source, bucket), deadListOf(t, dest, bucket)
				if len(got) != len(want) {
					t.Fatalf("the restored bucket lists %d dead entries, want %d", len(got), len(want))
				}
				for position := range want {
					if !slices.Equal(want[position].Data, got[position].Data) {
						t.Errorf("dead entry %d came back changed", position)
					}
				}
			})
		}
	}
}

// TestADumpOfTheDeepestDocumentsRestores: a hash or btree document is its own
// record, so it is the bound and no deeper.
func TestADumpOfTheDeepestDocumentsRestores(t *testing.T) {
	for _, bucketType := range []BucketType{BucketTypeHash, BucketTypeBTree} {
		t.Run(string(bucketType), func(t *testing.T) {
			source := newTestServer(t)
			createTestBucket(t, source, "app", "docs", bucketType)
			clientSend(t, source, transport.OpPut, InsertRequest{Database: "app", Bucket: "docs", Key: "k", Data: deepClientDocument(maxDocumentDepth)})
			dumped := dumpSections(t, source, "docs", DumpStartRequest{})
			dest := newTestServer(t)
			if answer := restoreEverySection(t, dest, dumped); answer.Applied != 1 {
				t.Fatalf("the restore applied %d documents, want 1", answer.Applied)
			}
		})
	}
}

// TestADumpOfTheDeepestStoreFieldElementsRestores covers the "stores" section:
// an element of every engine, a heap under a fifo and a hash, and a blob's
// metadata. The record wraps the element one level down, and a blob's metadata
// two, under the dump's own "extra" key.
func TestADumpOfTheDeepestStoreFieldElementsRestores(t *testing.T) {
	rows := []struct {
		name string
		path []interface{}
		decl StoreDeclaration
	}{
		{name: "fifo", decl: StoreDeclaration{Name: "F", Engine: "fifoStore"}},
		{name: "lifo", decl: StoreDeclaration{Name: "F", Engine: "lifoStore"}},
		{name: "heap", decl: StoreDeclaration{Name: "F", Engine: "heapStore"}},
		{name: "hash", decl: StoreDeclaration{Name: "F", Engine: "hashStore"}},
		{name: "btree", decl: StoreDeclaration{Name: "F", Engine: "btreeStore"}},
		{name: "blob", decl: StoreDeclaration{Name: "F", Engine: "blobStore"}},
		{name: "heap under fifo", path: []interface{}{"P", int64(0)},
			decl: StoreDeclaration{Name: "P", Engine: "fifoStore", Fields: []StoreDeclaration{{Name: "F", Engine: "heapStore"}}}},
		{name: "heap under hash", path: []interface{}{"P", "k"},
			decl: StoreDeclaration{Name: "P", Engine: "hashStore", Fields: []StoreDeclaration{{Name: "F", Engine: "heapStore"}}}},
		{name: "blob under fifo", path: []interface{}{"P", int64(0)},
			decl: StoreDeclaration{Name: "P", Engine: "fifoStore", Fields: []StoreDeclaration{{Name: "F", Engine: "blobStore"}}}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			source := newTestServer(t)
			if resp := createWithStores(t, source, "app", "Post", BucketTypeHash, row.decl); !resp.Success {
				t.Fatalf("creating Post: %s", resp.Message)
			}
			clientSend(t, source, transport.OpPut, InsertRequest{Database: "app", Bucket: "Post", Key: "p1", Data: map[string]interface{}{"title": "p1"}})
			deep := deepClientDocument(maxDocumentDepth)
			if row.path != nil {
				// The L2 element the path names is a shallow one.
				parentOp := transport.OpFieldPush
				if _, keyed := row.path[1].(string); keyed {
					parentOp = transport.OpFieldPut
				}
				parent := FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: row.path[0].(string), Data: map[string]interface{}{"n": int32(0)}}
				if parentOp == transport.OpFieldPut {
					parent.Element = row.path[1].(string)
				}
				clientSend(t, source, parentOp, parent)
			}
			request := FieldRequest{Database: "app", Bucket: "Post", Key: "p1", Field: "F", Path: row.path, Data: deep, Priority: 10}
			leaf := row.decl
			if len(leaf.Fields) > 0 {
				leaf = leaf.Fields[0]
			}
			op := transport.OpFieldPush
			switch leaf.Engine {
			case "hashStore", "btreeStore":
				op, request.Element = transport.OpFieldPut, "e"
			case "blobStore":
				op, request.Element, request.Content = transport.OpFieldPut, "e", []byte("content")
			}
			clientSend(t, source, op, request)

			dumped := dumpSections(t, source, "Post", DumpStartRequest{AcceptStores: true})
			if len(dumped.sections[dumpSectionStores]) == 0 {
				t.Fatalf("setup: the dump has no stores section")
			}
			dest := newTestServer(t)
			restoreEverySection(t, dest, dumped)
		})
	}
}

// TestADumpOfABlobBucketWithDeepMetadataRestores: a blob's caller-supplied
// metadata sits one level down in the request and in the dumped document.
func TestADumpOfABlobBucketWithDeepMetadataRestores(t *testing.T) {
	source := newTestServer(t)
	createTestBucket(t, source, "app", "files", BucketTypeBlob)
	clientSend(t, source, transport.OpBlobWrite, BlobWriteRequest{
		Database: "app", Bucket: "files", Key: "f", Data: []byte("content"), Metadata: deepClientDocument(maxDocumentDepth),
	})
	dumped := dumpSections(t, source, "files", DumpStartRequest{})
	dest := newTestServer(t)
	restoreEverySection(t, dest, dumped)
}

// TestRestoreRefusesARecordPastItsKindsLimit keeps the limit tight: each record
// kind is refused one level past a legal document and its own wrapper, and
// nothing it carries is written. The limit follows the kind, so a flat looser
// limit would let the deeper kinds through.
func TestRestoreRefusesARecordPastItsKindsLimit(t *testing.T) {
	tooDeep := mustMarshalBSON(t, deepClientDocument(maxDocumentDepth+1))
	heapEntry := func(value map[string]interface{}) []byte {
		return mustMarshalBSON(t, map[string]interface{}{"_id_": "h1", "priority": int64(10), "value": value})
	}
	tooDeepValue := deepClientDocument(maxDocumentDepth + 1)
	rows := []struct {
		name       string
		bucketType BucketType
		section    string
		stores     []DumpStore
		record     []byte
	}{
		{name: "hash document", bucketType: BucketTypeHash, record: tooDeep},
		{name: "fifo document", bucketType: BucketTypeFIFO, record: tooDeep},
		{name: "heap entry", bucketType: BucketTypeHeap, record: heapEntry(tooDeepValue)},
		{name: "fifo dead entry", bucketType: BucketTypeFIFO, section: dumpSectionDead,
			record: mustMarshalBSON(t, dumpDeadRecord{Data: tooDeep, Deliveries: 5, DeadAt: time.UnixMilli(1_800_000_000_000), Reason: store.DeadReasonMaxDeliveries})},
		{name: "heap dead entry", bucketType: BucketTypeHeap, section: dumpSectionDead,
			record: mustMarshalBSON(t, dumpDeadRecord{Data: heapEntry(tooDeepValue), Deliveries: 5, DeadAt: time.UnixMilli(1_800_000_000_000), Reason: store.DeadReasonMaxDeliveries})},
		{name: "hash field element", bucketType: BucketTypeHash, section: dumpSectionStores, stores: []DumpStore{{Name: "F", Engine: "hash"}},
			record: mustMarshalBSON(t, dumpElementRecord{Field: "F", Owner: "p1", Key: "e", Value: tooDeep})},
		{name: "blob field element", bucketType: BucketTypeHash, section: dumpSectionStores, stores: []DumpStore{{Name: "F", Engine: "blob"}},
			record: mustMarshalBSON(t, dumpElementRecord{Field: "F", Owner: "p1", Key: "e", Value: mustMarshalBSON(t, map[string]interface{}{"extra": tooDeepValue})})},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			dest := newTestServer(t)
			started, err := dest.startRestore(rootContext(), RestoreStartRequest{
				Database: "app", Bucket: "b", FormatVersion: 3, StoreType: row.bucketType, Stores: row.stores, Bytes: 1,
			})
			if err != nil {
				t.Fatalf("starting the restore: %v", err)
			}
			_, err = dest.restoreChunk(rootContext(), RestoreChunkRequest{
				Database: "app", Bucket: "b", SessionID: started.SessionID, Data: row.record, Section: row.section,
			})
			if !errors.Is(err, ErrDocumentTooDeep) {
				t.Fatalf("a %s one level past its limit = %v, want ErrDocumentTooDeep", row.name, err)
			}
		})
	}
}

// TestRecordDepthLimitTable pins the table the pull and the restore share: a
// row that moves must move here on purpose.
func TestRecordDepthLimitTable(t *testing.T) {
	rows := []struct {
		kind       depthRecordKind
		bucketType BucketType
		want       int
	}{
		{recordClaimed, BucketTypeFIFO, 100}, {recordClaimed, BucketTypeHeap, 100},
		{recordDead, BucketTypeFIFO, 101}, {recordDead, BucketTypeLIFO, 101}, {recordDead, BucketTypeHeap, 102},
		{recordStored, BucketTypeHash, 100}, {recordStored, BucketTypeHeap, 100},
		{recordDumped, BucketTypeHash, 100}, {recordDumped, BucketTypeBTree, 100}, {recordDumped, BucketTypeFIFO, 100},
		{recordDumped, BucketTypeLIFO, 100}, {recordDumped, BucketTypeHeap, 101}, {recordDumped, BucketTypeBlob, 101},
	}
	for _, row := range rows {
		if got := recordDepthLimit(row.kind, row.bucketType); got != row.want {
			t.Errorf("recordDepthLimit(%d, %s) = %d, want %d", row.kind, row.bucketType, got, row.want)
		}
	}
}
