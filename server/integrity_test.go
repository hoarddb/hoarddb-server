package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// Data integrity round-trip: N known documents are written through the same
// service layer the QUIC handlers call, read back, and compared byte-for-byte
// against the originals, with a running SHA-256 digest per bucket. This is a
// correctness test, not a benchmark: it answers "is anything lost or
// corrupted", not "how fast".
//
// Documents use strings, bools, nested documents and string arrays only.
// BSON integer width (int vs int32 vs int64) is a property of the driver's
// encoding, not of storage fidelity, so numeric fields are deliberately absent:
// their presence would test the driver, not the database.
const integrityDocCount = 2000

// integrityDoc builds the i-th known document. Deterministic: the same index
// always yields the same document, so the test needs no fixtures on disk.
func integrityDoc(i int) map[string]interface{} {
	rng := rand.New(rand.NewSource(int64(i)))
	return map[string]interface{}{
		"_id_":     fmt.Sprintf("doc_%06d", i),
		"username": fmt.Sprintf("user_%06d", i),
		"email":    fmt.Sprintf("u%d@test.com", i),
		"active":   i%2 == 0,
		"address": map[string]interface{}{
			"city": fmt.Sprintf("city-%d", i%50),
			"zip":  fmt.Sprintf("%05d", 10000+i%89999),
		},
		"tags": []interface{}{
			fmt.Sprintf("tag-%d", rng.Intn(100)),
			fmt.Sprintf("tag-%d", rng.Intn(100)),
		},
	}
}

// normValue converts driver-specific decoded types to plain Go values so
// content can be compared with reflect.DeepEqual. The driver decodes arrays
// as bson.A and nested documents as bson.M: distinct named types from
// []interface{} and map[string]interface{} even when the content is
// identical, so DeepEqual on the raw decoded form fails on type identity,
// not on data.
func normValue(v interface{}) interface{} {
	switch typed := v.(type) {
	case bson.M:
		out := make(map[string]interface{}, len(typed))
		for key, elem := range typed {
			out[key] = normValue(elem)
		}
		return out
	case map[string]interface{}:
		out := make(map[string]interface{}, len(typed))
		for key, elem := range typed {
			out[key] = normValue(elem)
		}
		return out
	case bson.A:
		out := make([]interface{}, len(typed))
		for i, elem := range typed {
			out[i] = normValue(elem)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(typed))
		for i, elem := range typed {
			out[i] = normValue(elem)
		}
		return out
	default:
		return v
	}
}

// canonBytes encodes a document with keys sorted at every level, so the bytes
// are deterministic regardless of Go map iteration order. BSON preserves key
// order on the wire and Go randomizes map iteration per marshal call, which
// means two marshals of equal maps can differ byte-for-byte; comparing those
// bytes tests the runtime, not the database.
func canonBytes(doc map[string]interface{}) []byte {
	keys := make([]string, 0, len(doc))
	for key := range doc {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	ordered := make(bson.D, 0, len(doc))
	for _, key := range keys {
		ordered = append(ordered, bson.E{Key: key, Value: canonValue(doc[key])})
	}
	raw, err := bson.Marshal(ordered)
	if err != nil {
		panic(fmt.Sprintf("canonBytes: marshal ordered document: %v", err))
	}
	return raw
}

// canonValue converts nested values into order-stable forms for canonBytes.
func canonValue(v interface{}) interface{} {
	switch typed := normValue(v).(type) {
	case map[string]interface{}:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		ordered := make(bson.D, 0, len(typed))
		for _, key := range keys {
			ordered = append(ordered, bson.E{Key: key, Value: canonValue(typed[key])})
		}
		return ordered
	case []interface{}:
		out := make(bson.A, len(typed))
		for i, elem := range typed {
			out[i] = canonValue(elem)
		}
		return out
	default:
		return typed
	}
}

// checkDocAsRead decodes one stored document and compares its content against
// the expected document. It returns the canonical bytes of the read document
// for the running digest.
func checkDocAsRead(t *testing.T, raw bson.Raw, want map[string]interface{}) []byte {
	t.Helper()
	var got map[string]interface{}
	if err := bson.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("unmarshal stored document: %v", err)
	}
	normalized, ok := normValue(got).(map[string]interface{})
	if !ok {
		t.Fatalf("decoded document is %T, want object", normValue(got))
	}
	if !reflect.DeepEqual(normalized, want) {
		t.Fatalf("document mismatch:\n got: %v\nwant: %v", normalized, want)
	}
	return canonBytes(normalized)
}

// digestHex returns the running SHA-256 hex over a sequence of byte slices.
func digestHex(chunks ...[]byte) string {
	sum := sha256.New()
	for _, chunk := range chunks {
		sum.Write(chunk)
	}
	return hex.EncodeToString(sum.Sum(nil))
}

// TestIntegrityKeyedStores writes N known documents to the hash, btree and
// blob buckets and reads every one back by key.
func TestIntegrityKeyedStores(t *testing.T) {
	for _, storeType := range []BucketType{BucketTypeHash, BucketTypeBTree, BucketTypeBlob} {
		t.Run(string(storeType), func(t *testing.T) {
			s := newTestServer(t)
			createTypedTestBucket(t, s, "app", "b", storeType)

			want := make([]map[string]interface{}, integrityDocCount)
			for i := range want {
				want[i] = integrityDoc(i)
				key := want[i]["_id_"].(string)
				resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "b", Key: key, Data: want[i]})
				if err != nil || !resp.Success {
					t.Fatalf("insert %d: %+v, %v", i, resp, err)
				}
			}

			countResp, err := s.Count(CountRequest{Database: "app", Bucket: "b"})
			if err != nil {
				t.Fatalf("count: %v", err)
			}
			if countResp.Count != integrityDocCount {
				t.Fatalf("count = %d, want %d", countResp.Count, integrityDocCount)
			}

			var readChunks, wantChunks [][]byte
			for i := range want {
				key := want[i]["_id_"].(string)
				found, err := s.Find(FindRequest{Database: "app", Bucket: "b", Key: key})
				if err != nil {
					t.Fatalf("find %q: %v", key, err)
				}
				if !found.Found {
					t.Fatalf("find %q: document missing", key)
				}
				readChunks = append(readChunks, checkDocAsRead(t, found.Data, want[i]))
				wantChunks = append(wantChunks, canonBytes(want[i]))
			}

			if got, expected := digestHex(readChunks...), digestHex(wantChunks...); got != expected {
				t.Fatalf("digest mismatch: got %s want %s", got, expected)
			}
		})
	}
}

// TestIntegrityQueues writes N known documents to FIFO and LIFO buckets and
// drains them, asserting exact pop order and exact content.
func TestIntegrityQueues(t *testing.T) {
	for _, tc := range []struct {
		storeType BucketType
		// order maps pop position to push index: FIFO preserves, LIFO reverses.
		order func(push, pop int) int
	}{
		{BucketTypeFIFO, func(push, pop int) int { return pop }},
		{BucketTypeLIFO, func(push, pop int) int { return push - 1 - pop }},
	} {
		t.Run(string(tc.storeType), func(t *testing.T) {
			s := newTestServer(t)
			createTypedTestBucket(t, s, "app", "b", tc.storeType)

			pushed := make([]map[string]interface{}, integrityDocCount)
			for i := range pushed {
				doc := integrityDoc(i)
				delete(doc, "_id_") // Push assigns the ID; the echo is the contract.
				resp, err := s.Push(PushRequest{Database: "app", Bucket: "b", Data: doc})
				if err != nil || !resp.Success {
					t.Fatalf("push %d: %+v, %v", i, resp, err)
				}
				doc["_id_"] = resp.ID
				pushed[i] = doc
			}

			peeked, err := s.Peek(PeekRequest{Database: "app", Bucket: "b"})
			if err != nil || !peeked.Found {
				t.Fatalf("peek: %+v, %v", peeked, err)
			}
			first := pushed[tc.order(integrityDocCount, 0)]
			checkDocAsRead(t, peeked.Items[0], first)

			var readChunks, wantChunks [][]byte
			for pop := 0; pop < integrityDocCount; pop++ {
				want := pushed[tc.order(integrityDocCount, pop)]
				popped, err := s.Pop(PopRequest{Database: "app", Bucket: "b"})
				if err != nil {
					t.Fatalf("pop %d: %v", pop, err)
				}
				if !popped.Found {
					t.Fatalf("pop %d: bucket drained early", pop)
				}
				readChunks = append(readChunks, checkDocAsRead(t, popped.Items[0], want))
				wantChunks = append(wantChunks, canonBytes(want))
			}

			drained, err := s.Pop(PopRequest{Database: "app", Bucket: "b"})
			if err != nil {
				t.Fatalf("drain check pop: %v", err)
			}
			if drained.Found {
				t.Fatal("pop after full drain returned a document")
			}

			if got, expected := digestHex(readChunks...), digestHex(wantChunks...); got != expected {
				t.Fatalf("digest mismatch: got %s want %s", got, expected)
			}
		})
	}
}

// TestIntegrityHeap pushes N documents with distinct priorities and asserts
// they pop highest-first with exact content.
func TestIntegrityHeap(t *testing.T) {
	s := newTestServer(t)
	createTypedTestBucket(t, s, "app", "b", BucketTypeHeap)

	byPriority := make(map[int64]map[string]interface{}, integrityDocCount)
	for i := 0; i < integrityDocCount; i++ {
		doc := integrityDoc(i)
		delete(doc, "_id_")
		priority := int64(i + 1)
		resp, err := s.Push(PushRequest{Database: "app", Bucket: "b", Data: doc, Priority: priority})
		if err != nil || !resp.Success {
			t.Fatalf("push %d: %+v, %v", i, resp, err)
		}
		doc["_id_"] = resp.ID
		byPriority[priority] = doc
	}

	var readChunks, wantChunks [][]byte
	for pop := 0; pop < integrityDocCount; pop++ {
		want := byPriority[int64(integrityDocCount-pop)]
		popped, err := s.Pop(PopRequest{Database: "app", Bucket: "b"})
		if err != nil {
			t.Fatalf("pop %d: %v", pop, err)
		}
		if !popped.Found {
			t.Fatalf("pop %d: heap drained early", pop)
		}
		readChunks = append(readChunks, checkDocAsRead(t, popped.Items[0], want))
		wantChunks = append(wantChunks, canonBytes(want))
	}

	if got, expected := digestHex(readChunks...), digestHex(wantChunks...); got != expected {
		t.Fatalf("digest mismatch: got %s want %s", got, expected)
	}
}

// TestIntegrityGetFastEquivalence asserts the compact GET_FAST wire path
// returns byte-identical documents to the standard GET path, and that a miss
// surfaces as GetFastNotFound rather than an error.
func TestIntegrityGetFastEquivalence(t *testing.T) {
	s := newTestServer(t)
	createTypedTestBucket(t, s, "app", "b", BucketTypeHash)

	const docs = 200
	for i := 0; i < docs; i++ {
		doc := integrityDoc(i)
		key := doc["_id_"].(string)
		resp, err := s.Insert(InsertRequest{Database: "app", Bucket: "b", Key: key, Data: doc})
		if err != nil || !resp.Success {
			t.Fatalf("insert %d: %+v, %v", i, resp, err)
		}
	}

	for i := 0; i < docs; i++ {
		key := fmt.Sprintf("doc_%06d", i)

		standard, err := s.Find(FindRequest{Database: "app", Bucket: "b", Key: key})
		if err != nil || !standard.Found {
			t.Fatalf("standard find %q: %+v, %v", key, standard, err)
		}

		requestPayload, err := transport.MarshalGetFast(transport.GetFastRequest{Database: "app", Bucket: "b", Key: key})
		if err != nil {
			t.Fatalf("marshal get-fast %q: %v", key, err)
		}
		frame := transport.NewFrame(transport.OpGetFast, requestPayload)
		answered, err := handleGetFast(t.Context(), frame, s)
		if err != nil {
			t.Fatalf("get-fast handler %q: %v", key, err)
		}
		status, documentBytes, err := transport.UnmarshalGetFastResponse(answered.Payload)
		if err != nil {
			t.Fatalf("decode get-fast response %q: %v", key, err)
		}
		if status != transport.GetFastFound {
			t.Fatalf("get-fast %q: status %#x, want found", key, status)
		}
		if !bytes.Equal(documentBytes, []byte(standard.Data)) {
			t.Fatalf("get-fast %q: document bytes differ from standard GET", key)
		}
	}

	missingPayload, err := transport.MarshalGetFast(transport.GetFastRequest{Database: "app", Bucket: "b", Key: "doc_999999"})
	if err != nil {
		t.Fatalf("marshal get-fast miss: %v", err)
	}
	missed, err := handleGetFast(t.Context(), transport.NewFrame(transport.OpGetFast, missingPayload), s)
	if err != nil {
		t.Fatalf("get-fast handler miss: %v", err)
	}
	status, documentBytes, err := transport.UnmarshalGetFastResponse(missed.Payload)
	if err != nil {
		t.Fatalf("decode get-fast miss response: %v", err)
	}
	if status != transport.GetFastNotFound {
		t.Fatalf("get-fast miss: status %#x, want not-found", status)
	}
	if len(documentBytes) != 0 {
		t.Fatalf("get-fast miss: payload is %d bytes, want empty", len(documentBytes))
	}
}
