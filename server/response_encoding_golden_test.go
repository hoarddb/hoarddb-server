package server

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// goldenDocument is the stored document every "found" case carries.
func goldenDocument(t testing.TB) bson.Raw {
	t.Helper()
	encoded, err := bson.Marshal(bson.D{{Key: "name", Value: "ada"}, {Key: "n", Value: int32(7)}})
	if err != nil {
		t.Fatalf("encoding the golden document: %v", err)
	}
	return bson.Raw(encoded)
}

// responseGoldenCases returns one or more representative values of every
// response type the server puts on the wire, keyed by a stable name. Only
// values that marshal successfully belong here: a value that does not is the
// missing-bson-tag defect, and is pinned by its own tests.
func responseGoldenCases(t testing.TB) map[string]any {
	t.Helper()
	doc := goldenDocument(t)
	at := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	user := UserInfo{Username: "ada", Roles: []UserRole{{Role: "readWrite", DB: "app"}}, Enabled: true, Epoch: 3, CreatedAt: at, PasswordChangedAt: at}
	return map[string]any{
		"InsertResponse/ok":           InsertResponse{ID: "k1", Success: true, Seq: 9},
		"InsertResponse/fail":         InsertResponse{Message: "disk full"},
		"FindResponse/found":          FindResponse{Found: true, Data: doc},
		"FindResponse/found-message":  FindResponse{Found: true, Data: doc, Message: "m"},
		"PushResponse/ok":             PushResponse{ID: "p1", Success: true, Seq: 4},
		"PopResponse/found":           PopResponse{Found: true, Items: []bson.Raw{doc}},
		"PeekResponse/found":          PeekResponse{Found: true, Items: []bson.Raw{doc}},
		"AuthResponse/ok":             AuthResponse{Success: true, Token: "tok", TokenExpiresAt: 100, TokenDeadline: 200, ProtocolVersion: 1, ServerVersion: "v", Capabilities: []string{"pipelining"}},
		"AuthFailResponse/reason":     AuthFailResponse{Reason: 2, Message: "no"},
		"HealthResponse/ok":           HealthResponse{NodeID: "n1", Uptime: "1s", Version: "v", BucketCount: 2, Healthy: true},
		"HealthResponse/capacity":     HealthResponse{NodeID: "n1", Healthy: true, Capacity: &HealthCapacity{SampledAt: 5, Rows: []CapacityRow{{Resource: "fds", Threshold: 0.8, State: "ok"}}}},
		"NodeListResponse/two":        NodeListResponse{Nodes: []*NodeInfo{{NodeID: "a", Addr: "h:1", Fingerprint: "f", JoinedAt: at, Status: "active"}}},
		"DeleteResponse/ok":           DeleteResponse{Success: true},
		"DeleteResponse/spared":       DeleteResponse{Success: true, Spared: true, Skipped: true, Message: "m"},
		"CreateDatabaseResponse/ok":   CreateDatabaseResponse{Success: true, Message: "created"},
		"CreateBucketResponse/ok":     CreateBucketResponse{Success: true, Message: "created"},
		"SearchResponse/hit":          SearchResponse{Results: []bson.Raw{doc}, Total: 1, IndexUsed: "by_name"},
		"ScanResponse/page":           ScanResponse{Found: true, Database: "d", Bucket: "b", Documents: []bson.Raw{doc}, Total: 1, HasMore: true, IndexUsed: "none"},
		"ScanResponse/missing-bucket": ScanResponse{Message: "bucket not found"},
		"BatchInsertResponse/ok":      BatchInsertResponse{IDs: []string{"a", "b"}, Count: 2, Success: true},
		"ListDatabasesResponse/one":   ListDatabasesResponse{Databases: []DatabaseInfo{{Name: "app", BucketCount: 1}}},
		"ListBucketsResponse/one":     ListBucketsResponse{Found: true, Database: "app", Buckets: []BucketInfo{{Name: "u", Type: BucketTypeHash, Entries: 3, Stores: []string{"s"}}}},
		"ListBucketsResponse/missing": ListBucketsResponse{Message: "database not found"},
		"DeleteDatabaseResponse/ok":   DeleteDatabaseResponse{Success: true},
		"DeleteBucketResponse/ok":     DeleteBucketResponse{Success: true},
		"CountResponse/n":             CountResponse{Count: 12},
		"DistinctResponse/values":     DistinctResponse{Values: []interface{}{"a", int32(1)}},
		"UpdateResponse/ok":           UpdateResponse{Success: true, Doc: map[string]interface{}{"a": int32(1)}},
		"BTreeRangeResponse/empty":    BTreeRangeResponse{},
		"BTreeRangeResponse/entry":    BTreeRangeResponse{Entries: []BTreeEntry{{Key: "k", Value: doc}}},
		"BlobWriteResponse/ok":        BlobWriteResponse{Success: true, Key: "k", BlobID: "b", Size: 3},
		"BlobReadResponse/found":      BlobReadResponse{Found: true, BlobID: "b", Data: []byte("abc"), Size: 3, Metadata: map[string]interface{}{"m": "v"}},
		"BlobReadResponse/miss":       BlobReadResponse{Message: "blob not found"},
		"BucketStatsResponse/found":   BucketStatsResponse{Found: true, Database: "d", Bucket: "b", Type: BucketTypeHash, Entries: 1, LogicalBytes: 2, Indexes: []string{"i"}, Stores: []StoreStats{{Name: "s", Engine: "hash", Elements: 4}}},
		"BucketStatsResponse/miss":    BucketStatsResponse{Message: "bucket not found"},
		"PingResponse/ok":             PingResponse{OK: true},
		"UserMutationResponse/ok":     UserMutationResponse{Success: true, Message: "m", Version: 2, Changed: []string{"roles"}},
		"UserListResponse/one":        UserListResponse{Users: []UserInfo{user}, Version: 2, Master: "n1", MasterAddr: "h:1"},
		"UserGetResponse/found":       UserGetResponse{Found: true, User: user, Version: 2},
		"RestoreStartResponse/ok":     RestoreStartResponse{Success: true, SessionID: "s", Created: true, MissingIndexes: []string{"i"}},
		"RestoreChunkResponse/ok":     RestoreChunkResponse{Success: true, Documents: 3},
		"RestoreDoneResponse/ok":      RestoreDoneResponse{Success: true, Applied: 3, Skipped: 1, ElementsApplied: 2},
		"DumpStartResponse/ok":        DumpStartResponse{Success: true, Message: "m"},
		"DumpChunkResponse/ok":        DumpChunkResponse{Success: true, Data: []byte("xyz"), EOF: true, Documents: 1, Section: "stores"},
		"DumpDoneResponse/ok":         DumpDoneResponse{Success: true, Bytes: 9, Documents: 2, Digest: "sha256:ab", Elements: 1, StoresDigest: "sha256:cd"},
		"AuthConfigPushResponse/ack":  AuthConfigPushResponse{Ack: true, Term: 1, Version: 2},
		"ErrorResponse/code":          ErrorResponse{Code: 5, Message: "m", MaxResultBytes: 10, RetainedBytes: 11},
		"exists/true":                 map[string]bool{"exists": true},
		"exists/false":                map[string]bool{"exists": false},
	}
}

// goldenPath holds the pre-fix encodings as hex, one per case. It was produced
// from commit 4decdb6 (the parent of the bson-tag fix), before any bson tag was
// added to a response type, by marshalling responseGoldenCases with bson.Marshal
// and writing the result. Do not regenerate it from a tree with the fix in it:
// that would make this test compare the new encoding with itself.
//
// One deliberate wire change has been made by hand since: catalog-gc build
// step CG-1 removed HealthResponse.EntryCount, so the two HealthResponse
// entries lost the 20-byte entrycount element and 20 from their length prefix.
// Another, peek({limit: N}): PeekResponse/found answers an items array where
// it answered a data document (the peek-n-spec record §3). A third,
// BUG-0493: AuthResponse/ok gained the trailing int32 element
// password_min_length (25 bytes, here zero), so its length prefix rose by 25.
// A fourth, BUG-0517: SearchResponse/hit gained the bool has_more (11 bytes,
// here false) between message and index_used, so its length prefix rose by 11.
// A fifth, pop({limit: N}): PopResponse/found answers an items array where it
// answered a data document, the same bytes as PeekResponse/found
// (the pop-n-spec record §4.1).
const goldenPath = "testdata/response_encoding_golden.json"

// TestResponseEncodingUnchangedFromBUG0120Base pins the wire bytes of every
// response that marshals today, so the fix for empty not-found frames cannot
// rename a field or change an encoding a driver already decodes.
func TestResponseEncodingUnchangedFromBUG0120Base(t *testing.T) {
	raw, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("reading %s: %v", goldenPath, err)
	}
	var golden map[string]string
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("decoding %s: %v", goldenPath, err)
	}
	cases := responseGoldenCases(t)
	names := make([]string, 0, len(cases))
	for name := range cases {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			want, ok := golden[name]
			if !ok {
				t.Fatalf("no golden entry for %q in %s", name, goldenPath)
			}
			encoded, err := bson.Marshal(cases[name])
			if err != nil {
				t.Fatalf("marshal %s: %v", name, err)
			}
			if got := hex.EncodeToString(encoded); got != want {
				t.Errorf("encoding of %s changed\n got: %s\nwant: %s", name, got, want)
			}
		})
	}
	if len(golden) != len(cases) {
		t.Errorf("golden file has %d entries, cases table has %d", len(golden), len(cases))
	}
}
