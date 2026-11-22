package server

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/bsontype"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Build step 7b: every boundary where bytes from outside this node's own
// store enter makes them a bson.Document through Validate, at that site's own
// depth limit. One test per boundary holds each (site, limit) row: the limit
// is written out here as a number, so a row whose number changes in the
// server turns its test red, in either direction. Each test also sends bytes
// the depth scan accepts and Validate refuses (a boolean byte that is neither
// 0 nor 1), which only a boundary that validates refuses before anything
// decodes them.

// badBooleanDocument is {flag: <boolean byte 2>}: framed correctly at every
// level, so the depth scan, which skips a boolean by its size, accepts it,
// and Validate refuses it for its value.
func badBooleanDocument() []byte {
	return bsoncore.BuildDocument(nil, append([]byte{byte(bsontype.Boolean)}, "flag\x00\x02"...))
}

// malformedWording is the part of the refusal of bytes that are not BSON.
const malformedWording = "is not well-formed BSON"

// frameRow is one opcode of the frame boundary and the depth its payload may
// reach.
type frameRow struct {
	op    transport.Opcode
	limit int
}

// TestFrameBoundaryLimits drives NewHandler with payloads nested exactly to
// each opcode's limit, which the boundary must pass, and one level past it,
// which it must refuse with the depth bound's refusal. The limit is the
// request's: 101 (a document's 100 levels inside the request envelope), and
// 102 for the two opcodes that carry their documents in a list.
func TestFrameBoundaryLimits(t *testing.T) {
	s := newTestServer(t)
	handler := NewHandler(s)
	handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "docs", Type: BucketTypeHash})
	for _, row := range []frameRow{
		{transport.OpPut, 101},
		{transport.OpUpdate, 101},
		{transport.OpScan, 101},
		{transport.OpCreateBucket, 101},
		{transport.OpGossip, 101},
		{transport.OpBatchPut, 102},
		{transport.OpDeclareStoreFields, 102},
	} {
		t.Run(row.op.String(), func(t *testing.T) {
			for _, arrays := range []bool{false, true} {
				atLimit := readScanAnswer(t, sendOrderFrame(t, rootSession(), handler, row.op, nestedDocumentBytes(row.limit, arrays)))
				if atLimit.isScanRefusal() {
					t.Errorf("arrays=%t: a payload %d levels deep was refused for its depth: %s", arrays, row.limit, atLimit.message)
				}
				pastLimit := readScanAnswer(t, sendOrderFrame(t, rootSession(), handler, row.op, nestedDocumentBytes(row.limit+1, arrays)))
				if !pastLimit.isScanRefusal() {
					t.Errorf("arrays=%t: a payload %d levels deep was answered %s %q, want the depth bound's refusal",
						arrays, row.limit+1, pastLimit.opcode, pastLimit.message)
				}
			}
		})
	}

	t.Run("a value only Validate refuses", func(t *testing.T) {
		var answer scanAnswer
		reads := readsDuring(t, func() []byte {
			payload := wireRequest(wireString("database", "app"), wireString("bucket", "docs"), wireString("key", "k"),
				wireDocument("data", badBooleanDocument()))
			answer = readScanAnswer(t, sendOrderFrame(t, rootSession(), handler, transport.OpPut, payload))
			return payload
		})
		if !answer.refused || answer.code != int(transport.ErrInvalidArgument) || !strings.Contains(answer.message, malformedWording) {
			t.Errorf("a PUT holding a boolean byte of 2 was answered code %d %q, want INVALID_ARGUMENT %q",
				answer.code, answer.message, malformedWording)
		}
		if len(reads) > 0 {
			t.Errorf("bytes the boundary refuses were read by %v", reads)
		}
	})
}

// TestGetFastStaysExempt: GET_FAST's payload is a compact layout of its own,
// not BSON (server/authz.go:hasWireLayoutPayload), so the frame boundary does
// not validate it; read as BSON it would be refused as malformed.
func TestGetFastStaysExempt(t *testing.T) {
	s := newTestServer(t)
	handler := NewHandler(s)
	handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "docs", Type: BucketTypeHash})
	answer := readScanAnswer(t, sendOrderFrame(t, rootSession(), handler, transport.OpGetFast, mustGetFastPayload(t, "app", "docs", "k")))
	if answer.refused && strings.Contains(answer.message, malformedWording) {
		t.Errorf("GET_FAST was refused as BSON: code %d %q", answer.code, answer.message)
	}
	if _, err := validateRequest(transport.OpGetFast, mustGetFastPayload(t, "app", "docs", "k")); err != nil {
		t.Errorf("validateRequest(GET_FAST) = %v, want the exemption", err)
	}
}

// pushEntryAnswer sends one replication push carrying entry, as a peer would,
// to a node whose cluster knows only its token, and returns the REPL_ACK's
// error text, "" for a push it accepted. Nothing is listened on or dialled.
func pushEntryAnswer(t *testing.T, entry ReplEntry) string {
	t.Helper()
	token, err := GenerateClusterToken()
	if err != nil {
		t.Fatalf("generating a cluster token: %v", err)
	}
	node := &Server{}
	node.cluster = &Cluster{s: node, token: ClusterToken{Token: token}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	push := mustMarshalBSON(t, ReplPushRequest{Token: token, FromNode: "peer", Entries: []ReplEntry{entry}})
	frame, err := handleReplPush(context.Background(), transport.NewFrame(transport.OpReplPush, push), node)
	if err != nil {
		t.Fatalf("REPL_PUSH: %v", err)
	}
	var answer ReplPushResponse
	if err := bson.Unmarshal(frame.Payload, &answer); err != nil {
		t.Fatalf("decoding the REPL_ACK: %v", err)
	}
	if answer.Success {
		return ""
	}
	return answer.Error
}

// TestPeerPushBoundaryLimits: every entry of a peer's push is validated at its
// own opcode's request limit (101, 102 for BATCH_PUT and DECLARE_STORE_FIELDS)
// by handleReplPush, since a push does not pass NewHandler for its entries.
// The node refuses every push for a reason of its own (it has no stream), so
// an entry within the limit is checked only for not being refused for depth.
func TestPeerPushBoundaryLimits(t *testing.T) {
	for _, row := range []frameRow{
		{transport.OpPut, 101},
		{transport.OpUpdate, 101},
		{transport.OpBatchPut, 102},
		{transport.OpDeclareStoreFields, 102},
	} {
		t.Run(row.op.String(), func(t *testing.T) {
			for _, arrays := range []bool{false, true} {
				entry := ReplEntry{Seq: 1, Op: int32(row.op), Bucket: "docs", Payload: nestedDocumentBytes(row.limit, arrays)}
				if refusal := pushEntryAnswer(t, entry); strings.Contains(refusal, nestingRefusalPhrase) {
					t.Errorf("arrays=%t: an entry %d levels deep was refused for its depth: %s", arrays, row.limit, refusal)
				}
				entry.Payload = nestedDocumentBytes(row.limit+1, arrays)
				if refusal := pushEntryAnswer(t, entry); !strings.Contains(refusal, nestingRefusalPhrase) {
					t.Errorf("arrays=%t: an entry %d levels deep was answered %q, want the depth bound's refusal", arrays, row.limit+1, refusal)
				}
			}
		})
	}

	t.Run("a value only Validate refuses", func(t *testing.T) {
		payload := wireRequest(wireString("database", "app"), wireString("bucket", "docs"), wireString("key", "k"),
			wireDocument("data", badBooleanDocument()))
		var refusal string
		reads := readsDuring(t, func() []byte {
			refusal = pushEntryAnswer(t, ReplEntry{Seq: 1, Op: int32(transport.OpPut), Bucket: "docs", Payload: payload})
			return payload
		})
		if !strings.Contains(refusal, malformedWording) {
			t.Errorf("an entry holding a boolean byte of 2 was answered %q, want %q", refusal, malformedWording)
		}
		if len(reads) > 0 {
			t.Errorf("an entry the boundary refuses was read by %v", reads)
		}
	})
}

// TestWALReplayBoundaryLimits: replay validates every logged entry at its own
// opcode's request limit before anything decodes it, since replay does not
// pass NewHandler. A put whose request is exactly at the limit (101) is
// applied, one level past it is passed over; a batch's limit is 102.
func TestWALReplayBoundaryLimits(t *testing.T) {
	s := newTestServer(t)
	w, err := storage.OpenWAL(filepath.Join(t.TempDir(), "wal"), nil)
	if err != nil {
		t.Fatalf("opening the WAL: %v", err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Errorf("closing the WAL: %v", err)
		}
	}()
	attachWALToStartedServer(s, w)
	logEntry := func(op transport.Opcode, key string, payload []byte) {
		t.Helper()
		if _, err := w.Append("docs", byte(op), []byte(key), payload); err != nil {
			t.Fatalf("logging %s: %v", op, err)
		}
	}
	logEntry(transport.OpCreateDB, "", mustMarshalBSON(t, CreateDatabaseRequest{Name: "app"}))
	logEntry(transport.OpCreateBucket, "", mustMarshalBSON(t, CreateBucketRequest{Database: "app", Name: "docs", Type: BucketTypeHash}))
	// A put's request holds its document one level down, a batch's two.
	putAt := func(key string, requestDepth int) []byte {
		return wireRequest(wireString("database", "app"), wireString("bucket", "docs"), wireString("key", key),
			wireDocument("data", nestedDocumentBytes(requestDepth-1, false)))
	}
	batchAt := func(key string, requestDepth int) []byte {
		return wireRequest(wireString("database", "app"), wireString("bucket", "docs"),
			wireDocumentList("data", withRootKey(t, nestedDocumentBytes(requestDepth-2, false), key)))
	}
	want := map[string]bool{}
	for _, row := range []struct {
		op      transport.Opcode
		limit   int
		payload func(key string, requestDepth int) []byte
	}{
		{transport.OpPut, 101, putAt},
		{transport.OpBatchPut, 102, batchAt},
	} {
		atLimit, pastLimit := fmt.Sprintf("%s-at-%d", row.op, row.limit), fmt.Sprintf("%s-at-%d", row.op, row.limit+1)
		logEntry(row.op, atLimit, row.payload(atLimit, row.limit))
		logEntry(row.op, pastLimit, row.payload(pastLimit, row.limit+1))
		want[atLimit], want[pastLimit] = true, false
	}
	badBoolean := wireRequest(wireString("database", "app"), wireString("bucket", "docs"), wireString("key", "bad-boolean"),
		wireDocument("data", badBooleanDocument()))
	logEntry(transport.OpPut, "bad-boolean", badBoolean)
	want["bad-boolean"] = false

	reads := readsDuring(t, func() []byte {
		if err := s.replayWAL(context.Background()); err != nil {
			t.Fatalf("replaying the log: %v", err)
		}
		return badBoolean
	})
	for key, stored := range want {
		found, err := s.Find(FindRequest{Database: "app", Bucket: "docs", Key: key})
		if err != nil {
			t.Fatalf("reading %s: %v", key, err)
		}
		if found.Found != stored {
			t.Errorf("after replay, %s is stored: %t, want %t", key, found.Found, stored)
		}
	}
	if len(reads) > 0 {
		t.Errorf("an entry holding a boolean byte of 2 was read by %v before replay passed it over", reads)
	}
}

// restoreChunkAnswer starts a restore of bucket b of bucketType with stores
// declared and sends record as one chunk of section, returning the chunk's
// error.
func restoreChunkAnswer(t *testing.T, bucketType BucketType, stores []DumpStore, section string, record []byte) error {
	t.Helper()
	dest := newTestServer(t)
	started, err := dest.startRestore(rootContext(), RestoreStartRequest{
		Database: "app", Bucket: "b", FormatVersion: 3, StoreType: bucketType, Stores: stores, Bytes: 1,
	})
	if err != nil {
		t.Fatalf("starting the restore: %v", err)
	}
	_, err = dest.restoreChunk(rootContext(), RestoreChunkRequest{
		Database: "app", Bucket: "b", SessionID: started.SessionID, Data: record, Section: section,
	})
	return err
}

// TestRestoreBoundaryLimits holds the restore rows: a chunk's records at the
// limit of their kind (recordDepthLimit's recordDumped and recordDead rows,
// storeElementRecordDepthLimit's wider blob row for the stores section), an
// element record at its own engine's limit once the engine is known (101 for
// hash, 102 for blob), and a dumped document's own 100. A record is nested
// from bytes, so at the limit it may still be refused for its shape, but
// never for its depth.
func TestRestoreBoundaryLimits(t *testing.T) {
	elementFields := func(field string) [][]byte {
		return [][]byte{wireString("field", field), wireString("owner", "p1"), wireString("key", "e"),
			wireDocument("value", wireRequest())}
	}
	for _, row := range []struct {
		name       string
		bucketType BucketType
		section    string
		stores     []DumpStore
		limit      int
		// rootElements go into the record's root ahead of the nesting.
		rootElements [][]byte
	}{
		{name: "hash document", bucketType: BucketTypeHash, limit: 100},
		{name: "fifo document", bucketType: BucketTypeFIFO, limit: 100},
		{name: "heap document", bucketType: BucketTypeHeap, limit: 101},
		{name: "blob document", bucketType: BucketTypeBlob, limit: 101},
		{name: "fifo dead entry", bucketType: BucketTypeFIFO, section: dumpSectionDead, limit: 101},
		{name: "heap dead entry", bucketType: BucketTypeHeap, section: dumpSectionDead, limit: 102},
		{name: "undeclared store element", bucketType: BucketTypeHash, section: dumpSectionStores, limit: 102},
		{name: "hash store element", bucketType: BucketTypeHash, section: dumpSectionStores,
			stores: []DumpStore{{Name: "F", Engine: "hash"}}, limit: 101, rootElements: elementFields("F")},
		{name: "blob store element", bucketType: BucketTypeHash, section: dumpSectionStores,
			stores: []DumpStore{{Name: "F", Engine: "blob"}}, limit: 102, rootElements: elementFields("F")},
	} {
		t.Run(row.name, func(t *testing.T) {
			for _, arrays := range []bool{false, true} {
				if err := restoreChunkAnswer(t, row.bucketType, row.stores, row.section,
					nestedDocumentBytes(row.limit, arrays, row.rootElements...)); errors.Is(err, ErrDocumentTooDeep) {
					t.Errorf("arrays=%t: a record %d levels deep was refused for its depth: %v", arrays, row.limit, err)
				}
				if err := restoreChunkAnswer(t, row.bucketType, row.stores, row.section,
					nestedDocumentBytes(row.limit+1, arrays, row.rootElements...)); !errors.Is(err, ErrDocumentTooDeep) {
					t.Errorf("arrays=%t: a record %d levels deep = %v, want ErrDocumentTooDeep", arrays, row.limit+1, err)
				}
			}
		})
	}

	t.Run("dumped document", func(t *testing.T) {
		for _, arrays := range []bool{false, true} {
			if _, err := decodeRestoreDocument(nestedDocumentBytes(100, arrays)); err != nil {
				t.Errorf("arrays=%t: a dumped document 100 levels deep was refused: %v", arrays, err)
			}
			if _, err := decodeRestoreDocument(nestedDocumentBytes(101, arrays)); !errors.Is(err, ErrDocumentTooDeep) {
				t.Errorf("arrays=%t: a dumped document 101 levels deep = %v, want ErrDocumentTooDeep", arrays, err)
			}
		}
	})

	t.Run("a value only Validate refuses", func(t *testing.T) {
		record := wireRequest(wireString("_id_", "k"), wireDocument("data", badBooleanDocument()))
		if err := restoreChunkAnswer(t, BucketTypeHash, nil, "", record); !errors.Is(err, ErrMalformedPayload) {
			t.Errorf("a dumped record holding a boolean byte of 2 = %v, want ErrMalformedPayload", err)
		}
		if _, err := decodeRestoreDocument(badBooleanDocument()); !errors.Is(err, ErrMalformedPayload) {
			t.Errorf("a dumped document holding a boolean byte of 2 = %v, want ErrMalformedPayload", err)
		}
	})
}

// TestUpdateResultBoundaryLimit: the result of an update that can deepen a
// document (updateMayDeepen) is measured over its encoding at the stored
// document's own bound, 100, by updateGuard.check. A result past the
// library's encoding ceiling, which a rename into a long path can build, is
// still the depth bound's refusal and not a fault of the server.
func TestUpdateResultBoundaryLimit(t *testing.T) {
	guard := newUpdateGuard(UpdateRequest{Key: "k", Rename: map[string]string{"a": "b"}})
	if !guard.deepens {
		t.Fatal("a rename does not mark the update as one that can deepen its document")
	}
	stored := mustMarshalBSON(t, map[string]interface{}{"n": int32(1)})
	if err := guard.check("k", stored, deepClientDocument(100)); err != nil {
		t.Errorf("a result 100 levels deep was refused: %v", err)
	}
	for _, levels := range []int{101, bson.MaxNestingCeiling + 50} {
		err := guard.check("k", stored, deepClientDocument(levels))
		if !errors.Is(err, ErrDocumentTooDeep) || codeFor(err) != transport.ErrInvalidArgument {
			t.Errorf("a result %d levels deep = %v (code %d), want ErrDocumentTooDeep as INVALID_ARGUMENT", levels, err, codeFor(err))
		}
	}
}

// TestMemberAnswerBoundaryLimit: every answer a node reads back from a member
// it asked passes decodePeerAnswer, which validates it at 103 before decoding
// it: a document's 100 levels, three down inside the answer's lists. Bytes
// under a key the answer's type does not declare are validated too, though
// the decode would skip them unread.
func TestMemberAnswerBoundaryLimit(t *testing.T) {
	var answer struct {
		Success bool `bson:"success"`
	}
	for _, arrays := range []bool{false, true} {
		if err := decodePeerAnswer(nestedDocumentBytes(103, arrays), &answer); err != nil {
			t.Errorf("arrays=%t: an answer 103 levels deep was refused: %v", arrays, err)
		}
		if err := decodePeerAnswer(nestedDocumentBytes(104, arrays), &answer); !errors.Is(err, ErrDocumentTooDeep) {
			t.Errorf("arrays=%t: an answer 104 levels deep = %v, want ErrDocumentTooDeep", arrays, err)
		}
	}
	skipped := wireRequest(bsoncore.AppendInt32Element(nil, "n", 1), wireDocument("undeclared", badBooleanDocument()))
	if err := decodePeerAnswer(skipped, &answer); !errors.Is(err, errMalformedBSON) {
		t.Errorf("an answer holding a boolean byte of 2 under a key its type skips = %v, want errMalformedBSON", err)
	}
}

// TestRelayedAnswerBoundaryLimits: the answers a member relays inside a
// replication push's answer are carried as binary values, so peer.call
// validates each one through refuseDeepRelayedAnswers: a relayed answer at a
// member answer's 103, and GET_FAST's compact answer, whose document is a
// stored document, at 100.
func TestRelayedAnswerBoundaryLimits(t *testing.T) {
	relayed := func(payload []byte) []ReplResponse {
		return []ReplResponse{{Opcode: int32(transport.OpBTreeResponse), Payload: payload}}
	}
	for _, arrays := range []bool{false, true} {
		if err := refuseDeepRelayedAnswers(relayed(nestedDocumentBytes(103, arrays))); err != nil {
			t.Errorf("arrays=%t: a relayed answer 103 levels deep was refused: %v", arrays, err)
		}
		if err := refuseDeepRelayedAnswers(relayed(nestedDocumentBytes(104, arrays))); !errors.Is(err, ErrDocumentTooDeep) {
			t.Errorf("arrays=%t: a relayed answer 104 levels deep = %v, want ErrDocumentTooDeep", arrays, err)
		}
		found := func(document []byte) []ReplResponse {
			return []ReplResponse{compactReadAnswer(transport.GetFastFound, document)}
		}
		if err := refuseDeepRelayedAnswers(found(nestedDocumentBytes(100, arrays))); err != nil {
			t.Errorf("arrays=%t: a relayed compact read of a document 100 levels deep was refused: %v", arrays, err)
		}
		if err := refuseDeepRelayedAnswers(found(nestedDocumentBytes(101, arrays))); !errors.Is(err, ErrDocumentTooDeep) {
			t.Errorf("arrays=%t: a relayed compact read of a document 101 levels deep = %v, want ErrDocumentTooDeep", arrays, err)
		}
	}
	if err := refuseDeepRelayedAnswers(relayed(badBooleanDocument())); !errors.Is(err, errMalformedBSON) {
		t.Errorf("a relayed answer holding a boolean byte of 2 = %v, want errMalformedBSON", err)
	}
	if err := refuseDeepRelayedAnswers([]ReplResponse{compactReadAnswer(transport.GetFastFound, badBooleanDocument())}); !errors.Is(err, errMalformedBSON) {
		t.Errorf("a relayed compact read holding a boolean byte of 2 = %v, want errMalformedBSON", err)
	}
}

// TestVersionedGetBoundaryLimits: readVersionedAnswer validates a versioned
// get's answer at a member answer's 103 and the document it carries, which a
// repair stores, at a stored document's 100. The member's answer is
// substituted on the asking node's connection.
func TestVersionedGetBoundaryLimits(t *testing.T) {
	tc := newTestCluster(t, 2, 1)
	defer tc.stop()
	asker := clusterOf(tc, 0)
	member := mustNode(t, asker, tc.addrs[1])
	substitute := installSubstituteConn(t, tc, 0, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ask := func(t *testing.T, answer []byte) error {
		t.Helper()
		substitute.answerWith(t, opcodeIs(transport.OpVersionedGet), &transport.Frame{Opcode: transport.OpVersionedGet, Payload: answer})
		_, err := asker.readVersionedAnswer(ctx, member, ConsistentGetRequest{Database: testDatabase, Bucket: testBucket, Key: "k"})
		return err
	}
	t.Run("answer", func(t *testing.T) {
		if err := ask(t, nestedDocumentBytes(103, false)); err != nil {
			t.Errorf("an answer 103 levels deep was refused: %v", err)
		}
		if err := ask(t, nestedDocumentBytes(104, false)); !errors.Is(err, ErrDocumentTooDeep) {
			t.Errorf("an answer 104 levels deep = %v, want ErrDocumentTooDeep", err)
		}
	})
	t.Run("document", func(t *testing.T) {
		for _, arrays := range []bool{false, true} {
			atLimit := mustMarshal(t, VersionedGetResponse{Found: true, Version: 1, Data: bson.Raw(nestedDocumentBytes(100, arrays))})
			if err := ask(t, atLimit); err != nil {
				t.Errorf("arrays=%t: a document 100 levels deep was refused: %v", arrays, err)
			}
			pastLimit := mustMarshal(t, VersionedGetResponse{Found: true, Version: 1, Data: bson.Raw(nestedDocumentBytes(101, arrays))})
			if err := ask(t, pastLimit); !errors.Is(err, ErrDocumentTooDeep) {
				t.Errorf("arrays=%t: a document 101 levels deep = %v, want ErrDocumentTooDeep", arrays, err)
			}
		}
	})
}

// migrationStream is records encoded one after another, as a source streams
// them.
func migrationStream(t *testing.T, records ...migrateStreamRecord) []byte {
	t.Helper()
	var stream []byte
	for _, record := range records {
		stream = append(stream, mustMarshalBSON(t, record)...)
	}
	return stream
}

// TestMigrationStreamBoundaryLimits: readMigrateStream validates every record
// of a pulled stream at a member answer's 103, and every value of a record
// that a later read decodes at the limit storedDocumentOf gives its kind,
// before any of the copy is written.
func TestMigrationStreamBoundaryLimits(t *testing.T) {
	hashLayout := migratedLayout{storeType: BucketTypeHash}
	for _, arrays := range []bool{false, true} {
		if _, err := readMigrateStream(nestedDocumentBytes(103, arrays), hashLayout); err != nil {
			t.Errorf("arrays=%t: a record 103 levels deep was refused: %v", arrays, err)
		}
		if _, err := readMigrateStream(nestedDocumentBytes(104, arrays), hashLayout); !errors.Is(err, ErrDocumentTooDeep) {
			t.Errorf("arrays=%t: a record 104 levels deep = %v, want ErrDocumentTooDeep", arrays, err)
		}
	}

	fifoDead := append([]byte{storage.NSDelivery, 'x'}, binary.BigEndian.AppendUint64(nil, 1)...)
	for _, row := range []struct {
		name   string
		key    []byte
		layout migratedLayout
		limit  int
	}{
		{name: "state document", key: []byte{storage.NSState}, layout: hashLayout, limit: 103},
		{name: "blob metadata", key: []byte{storage.NSBlobMeta, 'b'}, layout: migratedLayout{storeType: BucketTypeBlob}, limit: 101},
		{name: "hash document", key: []byte{storage.NSDocuments, 'k'}, layout: hashLayout, limit: 100},
		{name: "fifo entry", key: append([]byte{storage.NSQueue}, binary.BigEndian.AppendUint64(nil, 1)...),
			layout: migratedLayout{storeType: BucketTypeFIFO}, limit: 100},
		{name: "dead fifo entry", key: fifoDead, layout: migratedLayout{storeType: BucketTypeFIFO}, limit: 101},
		{name: "dead heap entry", key: fifoDead, layout: migratedLayout{storeType: BucketTypeHeap}, limit: 102},
	} {
		t.Run(row.name, func(t *testing.T) {
			for _, arrays := range []bool{false, true} {
				atLimit := migrationStream(t, migrateStreamRecord{Key: row.key, Value: nestedDocumentBytes(row.limit, arrays)})
				if _, err := readMigrateStream(atLimit, row.layout); errors.Is(err, ErrDocumentTooDeep) {
					t.Errorf("arrays=%t: a value %d levels deep was refused for its depth: %v", arrays, row.limit, err)
				}
				pastLimit := migrationStream(t, migrateStreamRecord{Key: row.key, Value: nestedDocumentBytes(row.limit+1, arrays)})
				if _, err := readMigrateStream(pastLimit, row.layout); !errors.Is(err, ErrDocumentTooDeep) {
					t.Errorf("arrays=%t: a value %d levels deep = %v, want ErrDocumentTooDeep", arrays, row.limit+1, err)
				}
			}
		})
	}

	t.Run("a value only Validate refuses", func(t *testing.T) {
		stream := migrationStream(t, migrateStreamRecord{Key: []byte{storage.NSDocuments, 'k'}, Value: badBooleanDocument()})
		if _, err := readMigrateStream(stream, hashLayout); !errors.Is(err, errMalformedBSON) {
			t.Errorf("a pulled document holding a boolean byte of 2 = %v, want errMalformedBSON", err)
		}
	})
}
