package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
	"github.com/hoarddb/hoarddb-server/transport"
)

// A copy of a bucket a node pulls from another member is written as the
// source stored it and decoded later, by the reads and checks that read the
// bucket. Every value of the copy a later read decodes as a document is held
// to the bound a client's document gets before anything of the copy is
// written: a copy carrying one nested past it is refused whole, and a copy
// whose values sit at the bound lands unchanged.

// pulledFamily is one kind of value a pulled copy carries: how a source comes
// to hold one nested depth levels, and the bucket that holds it.
type pulledFamily struct {
	name      string
	storeType BucketType
	stores    []StoreDeclaration
	// seed writes one value of the family, nested depth levels, into the
	// source bucket through the server's own verbs, which leave the depth to
	// the request path.
	seed func(t *testing.T, s *Server, bucket string, depth int)
	// extra is a record appended to the source's stream, for a value no verb
	// writes in one call: the source's delivery records.
	extra func(t *testing.T, depth int) migrateStreamRecord
}

// nestedMap is a document nested depth levels, the root included, as a
// client's request decodes into.
func nestedMap(depth int) map[string]interface{} {
	document := map[string]interface{}{}
	for level := depth; level > 1; level-- {
		document = map[string]interface{}{"a": document}
	}
	return document
}

func insertNested(t *testing.T, s *Server, bucket string, depth int) {
	t.Helper()
	if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: bucket, Key: "deep", Data: nestedMap(depth)}); err != nil || !resp.Success {
		t.Fatalf("inserting a document nested %d levels into %s: %+v, %v", depth, bucket, resp, err)
	}
}

func pushNested(priority int64) func(t *testing.T, s *Server, bucket string, depth int) {
	return func(t *testing.T, s *Server, bucket string, depth int) {
		t.Helper()
		if resp, err := s.Push(PushRequest{Database: "app", Bucket: bucket, Data: nestedMap(depth), Priority: priority}); err != nil || !resp.Success {
			t.Fatalf("pushing an entry nested %d levels into %s: %+v, %v", depth, bucket, resp, err)
		}
	}
}

// insertOwner inserts the document the field-store families' elements belong to.
func insertOwner(t *testing.T, s *Server, bucket string) {
	t.Helper()
	if resp, err := s.Insert(InsertRequest{Database: "app", Bucket: bucket, Key: "p1", Data: map[string]interface{}{"title": "p1"}}); err != nil || !resp.Success {
		t.Fatalf("inserting p1 into %s: %+v, %v", bucket, resp, err)
	}
}

// deliveryRecord is a record under the bucket's delivery namespace: kind,
// then the key's rest.
func deliveryRecord(kind byte, rest []byte, value []byte) migrateStreamRecord {
	key := append([]byte{storage.NSDelivery, kind}, rest...)
	return migrateStreamRecord{Key: key, Value: value}
}

// claimedEntryRecord is a claimed entry: its lease (20 bytes) and origin (a
// queue position), and as its value the return count, then the entry.
func claimedEntryRecord(t *testing.T, depth int) migrateStreamRecord {
	t.Helper()
	rest := append(bytes.Repeat([]byte{0x01}, 20), binary.BigEndian.AppendUint64(nil, 1)...)
	return deliveryRecord('e', rest, append(binary.AppendUvarint(nil, 2), mustMarshal(t, nestedMap(depth))...))
}

// deadRecordOf is a dead entry whose data is data.
func deadRecordOf(t *testing.T, rest []byte, data []byte) migrateStreamRecord {
	t.Helper()
	value := wireRequest(wireDocument("data", data), bsoncore.AppendInt32Element(nil, "deliveries", 5),
		bsoncore.AppendDateTimeElement(nil, "dead_at", 1), wireString("reason", "max_deliveries"))
	return deliveryRecord('x', rest, value)
}

// pulledFamilies is every kind of value a copy carries that a read decodes as
// a document, with the depth a client may store it at.
func pulledFamilies() []pulledFamily {
	return []pulledFamily{
		{name: "a hash document", storeType: BucketTypeHash, seed: insertNested},
		{name: "a btree document", storeType: BucketTypeBTree, seed: insertNested},
		{name: "a fifo entry", storeType: BucketTypeFIFO, seed: pushNested(0)},
		{name: "a lifo entry", storeType: BucketTypeLIFO, seed: pushNested(0)},
		{name: "a heap entry", storeType: BucketTypeHeap, seed: pushNested(7)},
		{name: "a fifoStore element", storeType: BucketTypeHash, stores: []StoreDeclaration{{Name: "Comments", Engine: "fifoStore"}},
			seed: func(t *testing.T, s *Server, bucket string, depth int) {
				t.Helper()
				insertOwner(t, s, bucket)
				fieldOK(t)(s.FieldPushCtx(context.Background(), FieldRequest{Database: "app", Bucket: bucket, Key: "p1", Field: "Comments", Data: nestedMap(depth)}))
			}},
		{name: "a hashStore element", storeType: BucketTypeBTree, stores: []StoreDeclaration{{Name: "Notes", Engine: "hashStore"}},
			seed: func(t *testing.T, s *Server, bucket string, depth int) {
				t.Helper()
				insertOwner(t, s, bucket)
				fieldOK(t)(s.FieldPutCtx(context.Background(), FieldRequest{Database: "app", Bucket: bucket, Key: "p1", Field: "Notes", Element: "n1", Data: nestedMap(depth)}))
			}},
		{name: "a heapStore element under a fifoStore element", storeType: BucketTypeHash,
			stores: []StoreDeclaration{{Name: "Comments", Engine: "fifoStore", Fields: []StoreDeclaration{{Name: "Ranked", Engine: "heapStore"}}}},
			seed: func(t *testing.T, s *Server, bucket string, depth int) {
				t.Helper()
				insertOwner(t, s, bucket)
				pushed := fieldOK(t)(s.FieldPushCtx(context.Background(), FieldRequest{Database: "app", Bucket: bucket, Key: "p1", Field: "Comments", Data: map[string]interface{}{"n": 1}}))
				fieldOK(t)(s.FieldPushCtx(context.Background(), FieldRequest{Database: "app", Bucket: bucket, Key: "p1", Field: "Ranked",
					Path: []interface{}{"Comments", *pushed.Position}, Priority: 3, Data: nestedMap(depth)}))
			}},
		{name: "a claimed fifo entry", storeType: BucketTypeFIFO, extra: claimedEntryRecord},
		{name: "a dead fifo entry", storeType: BucketTypeFIFO, extra: func(t *testing.T, depth int) migrateStreamRecord {
			t.Helper()
			return deadRecordOf(t, binary.BigEndian.AppendUint64(nil, 1), mustMarshal(t, nestedMap(depth)))
		}},
		{name: "a dead heap entry", storeType: BucketTypeHeap, extra: func(t *testing.T, depth int) migrateStreamRecord {
			t.Helper()
			heapEntry := wireRequest(wireString("_id_", "e1"), bsoncore.AppendInt64Element(nil, "priority", 7), wireDocument("value", mustMarshal(t, nestedMap(depth))))
			rest := append(binary.BigEndian.AppendUint64(nil, 1), "e1"...)
			return deadRecordOf(t, binary.BigEndian.AppendUint64(rest, 1), heapEntry)
		}},
	}
}

// pulledCopy is a source bucket of one family, the stream a pull of it
// carries, and the bucket the pull lands as.
type pulledCopy struct {
	stream []byte
	pulled migratedBucket
}

// copyOf builds family's source bucket on s, with its value nested depth
// levels, and the pull of it as the bucket "pulled".
func copyOf(t *testing.T, s *Server, family pulledFamily, depth int) pulledCopy {
	t.Helper()
	if resp := createWithStores(t, s, "app", "source", family.storeType, family.stores...); !resp.Success {
		t.Fatalf("creating the source bucket: %s", resp.Message)
	}
	if family.seed != nil {
		family.seed(t, s, "source", depth)
	}
	source := lookupBucket(s, "app", "source")
	var stream bytes.Buffer
	if err := s.streamBucketRange(&stream, s.kv, source.slot, source.databaseID, source.id.Load()); err != nil {
		t.Fatalf("streaming the source bucket: %v", err)
	}
	if family.extra != nil {
		stream.Write(mustMarshal(t, family.extra(t, depth)))
	}
	live, err := s.liveGenerations("app")
	if err != nil || len(live) != 1 {
		t.Fatalf("the live generations of app: %v, %v", live, err)
	}
	declared := source.storeFields()
	return pulledCopy{stream: stream.Bytes(), pulled: migratedBucket{database: "app", name: "pulled", storeType: family.storeType,
		stores: declared.stores, nextStoreID: declared.next, generation: live[0]}}
}

// bucketKeyCount is how many keys every bucket of the node holds together.
func bucketKeyCount(t *testing.T, kv storage.KV) int {
	t.Helper()
	snap := kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: storage.PairPrefix(0, 0), Upper: storage.PairPrefix(storage.SlotCount, 0)})
	defer it.Close()
	count := 0
	for it.Seek(nil); it.Valid(); it.Next() {
		count++
	}
	return count
}

// TestPulledValuesPastTheBoundAreRefusedBeforeTheyAreWritten: a copy
// carrying a value one level deeper than a client may store it is refused by
// the depth scan, and nothing of it is written: no key, no catalog entry, no
// served bucket.
func TestPulledValuesPastTheBoundAreRefusedBeforeTheyAreWritten(t *testing.T) {
	for _, family := range pulledFamilies() {
		t.Run(family.name, func(t *testing.T) {
			s := newKVFaultServer(t.TempDir(), kvfault.New())
			deep := copyOf(t, s, family, maxDocumentDepth+1)
			keysBefore := bucketKeyCount(t, s.kv)

			err := s.materializeRange(deep.pulled, deep.stream, 0)
			if !errors.Is(err, ErrDocumentTooDeep) {
				t.Fatalf("a copy carrying %s nested past the bound was not refused by the depth scan: %v", family.name, err)
			}
			if keysAfter := bucketKeyCount(t, s.kv); keysAfter != keysBefore {
				t.Errorf("the refused copy wrote %d keys", keysAfter-keysBefore)
			}
			if _, catalogued, err := s.readCatalogEntry("app", "pulled"); err != nil || catalogued {
				t.Errorf("the refused copy left a catalog entry (catalogued=%v, err=%v)", catalogued, err)
			}
			if bucket := lookupBucket(s, "app", "pulled"); bucket != nil && bucket.id.Load() != 0 {
				t.Errorf("the refused copy is served as bucket id %d", bucket.id.Load())
			}
		})
	}
}

// streamRecords splits a migration stream into its records, unchecked: the
// test's own reading of what it built.
func streamRecords(t *testing.T, stream []byte) []migrateStreamRecord {
	t.Helper()
	var records []migrateStreamRecord
	for len(stream) > 0 {
		raw, rest, ok := bsoncore.ReadDocument(stream)
		if !ok {
			t.Fatalf("the stream is damaged with %d bytes left", len(stream))
		}
		var record migrateStreamRecord
		if err := bson.Unmarshal(raw, &record); err != nil {
			t.Fatalf("decoding a stream record: %v", err)
		}
		records = append(records, record)
		stream = rest
	}
	return records
}

// TestPulledValuesAtTheBoundAreCopied is the other side: a copy whose values
// sit at the bound a client stores them at lands, and the landed bucket holds
// every value the source held, unchanged. A blob bucket and a blobStore field,
// whose values are not documents, copy too.
func TestPulledValuesAtTheBoundAreCopied(t *testing.T) {
	families := append(pulledFamilies(),
		pulledFamily{name: "a blob", storeType: BucketTypeBlob, seed: func(t *testing.T, s *Server, bucket string, _ int) {
			t.Helper()
			if resp, err := s.BlobWrite(BlobWriteRequest{Database: "app", Bucket: bucket, Key: "b", Data: []byte("inline")}); err != nil || !resp.Success {
				t.Fatalf("blob write: %+v, %v", resp, err)
			}
		}},
		pulledFamily{name: "a blobStore element", storeType: BucketTypeHash, stores: []StoreDeclaration{{Name: "Photos", Engine: "blobStore"}},
			seed: func(t *testing.T, s *Server, bucket string, _ int) {
				t.Helper()
				insertOwner(t, s, bucket)
				fieldOK(t)(s.FieldPutCtx(context.Background(), FieldRequest{Database: "app", Bucket: bucket, Key: "p1", Field: "Photos", Element: "ph1", Content: []byte("small")}))
			}},
	)
	for _, family := range families {
		t.Run(family.name, func(t *testing.T) {
			s := newKVFaultServer(t.TempDir(), kvfault.New())
			atTheBound := copyOf(t, s, family, maxDocumentDepth)

			if err := s.materializeRange(atTheBound.pulled, atTheBound.stream, 0); err != nil {
				t.Fatalf("a copy carrying %s at the bound was refused: %v", family.name, err)
			}
			landed := lookupBucket(s, "app", "pulled")
			if landed == nil || landed.id.Load() == 0 {
				t.Fatal("the copy did not land as a served bucket")
			}
			snap := s.kv.NewSnapshot()
			defer snap.Discard()
			for _, record := range streamRecords(t, atTheBound.stream) {
				// The state document is rewritten for the copy, and a blob's
				// metadata names the copy's own payload file.
				if record.BlobID != "" || record.Key[0] == storage.NSState || record.Key[0] == storage.NSBlobMeta {
					continue
				}
				stored, err := snap.Get(append(storage.Scope(landed.slot, landed.databaseID, landed.id.Load()), record.Key...))
				if err != nil || !bytes.Equal(stored, record.Value) {
					t.Errorf("the copy does not hold the source's value under %x unchanged (err %v)", record.Key, err)
				}
			}
		})
	}
}

// compactReadAnswer is a GET_FAST answer as a member relays it: the status
// byte, then the document.
func compactReadAnswer(status uint8, document []byte) ReplResponse {
	return ReplResponse{Opcode: int32(transport.OpGetFastResponse), Payload: transport.MarshalGetFastResponse(status, document)}
}

// TestARelayedCompactReadAnswerPastTheBoundIsRefused: a compact read's answer
// a member relays carries a stored document after its status byte, and that
// document is scanned before the answer goes on to the driver.
func TestARelayedCompactReadAnswerPastTheBoundIsRefused(t *testing.T) {
	deep := compactReadAnswer(transport.GetFastFound, nestedDocumentBytes(maxDocumentDepth+1, false))
	if err := refuseDeepRelayedAnswers([]ReplResponse{deep}); !errors.Is(err, ErrDocumentTooDeep) {
		t.Fatalf("a relayed compact read answer nested past the bound was not refused by the depth scan: %v", err)
	}
	malformed := compactReadAnswer(transport.GetFastFound, []byte{0x05, 0x00, 0x00, 0x00, 0x01})
	if err := refuseDeepRelayedAnswers([]ReplResponse{malformed}); !errors.Is(err, errMalformedBSON) {
		t.Fatalf("a relayed compact read answer that is not BSON after its status byte was not refused: %v", err)
	}
}

// TestARelayedCompactReadAnswerAtTheBoundIsRelayed is the other side: a found
// document at the bound and a miss, which carries no document, pass.
func TestARelayedCompactReadAnswerAtTheBoundIsRelayed(t *testing.T) {
	relayed := []ReplResponse{
		compactReadAnswer(transport.GetFastFound, nestedDocumentBytes(maxDocumentDepth, false)),
		compactReadAnswer(transport.GetFastNotFound, nil),
	}
	if err := refuseDeepRelayedAnswers(relayed); err != nil {
		t.Fatalf("a relayed compact read answer at the bound, or a miss, was refused: %v", err)
	}
}
