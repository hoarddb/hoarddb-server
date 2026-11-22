package server

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// TestDumpCarriesTheKeyInTheValue is shared-storage spec T-20: a dumped
// document carries its key inside its value, so a restore re-derives the
// identical key. The shared store changes how every engine derives a key, and
// the upgrade path is dump and restore, so a break here loses every key
// silently. Each engine is dumped through runDumpWalk's real enumerator
// (bucketEnumerator), restored into a fresh server, and compared key by key.
//
// Blob is excluded: its payloads are never dumped (GAP-28;
// applyRestoreDocument skips blob), so there is no restored key to compare.
func TestDumpCarriesTheKeyInTheValue(t *testing.T) {
	const database, elements = "app", 4
	ctx := rootContext()

	cases := []struct {
		btype BucketType
		// write puts element i into the source bucket without naming its key
		// inside the document, so only the server can put the key there.
		write func(t *testing.T, s *Server, bucket string, i int)
		// keysOf returns every key the bucket holds, in the engine's order.
		keysOf func(t *testing.T, s *Server, bucket string, want []string) []string
	}{
		{btype: BucketTypeHash, write: insertKeyed, keysOf: findKeys},
		{btype: BucketTypeBTree, write: insertKeyed, keysOf: findKeys},
		{btype: BucketTypeFIFO, write: pushUnkeyed, keysOf: popKeys},
		{btype: BucketTypeLIFO, write: pushUnkeyed, keysOf: popKeys},
		{btype: BucketTypeHeap, write: pushUnkeyed, keysOf: popKeys},
	}

	source := newTestServer(t)
	target := newTestServer(t)
	for _, tc := range cases {
		t.Run(string(tc.btype), func(t *testing.T) {
			bucket := "dump_" + string(tc.btype)
			createTestBucket(t, source, database, bucket, tc.btype)
			for i := 0; i < elements; i++ {
				tc.write(t, source, bucket, i)
			}
			wantKeys := []string{"k0", "k1", "k2", "k3"}

			manifest, raw := dumpBucketRaw(t, source, ctx, database, bucket, elements)
			req := restoreRequestFromManifest(manifest, int64(len(raw)), false)
			if _, _, msg := restoreBucketRaw(t, target, ctx, req, raw, elements); msg != "" {
				t.Fatalf("restore refused: %s", msg)
			}

			originalKeys := tc.keysOf(t, source, bucket, wantKeys)
			restoredKeys := tc.keysOf(t, target, bucket, wantKeys)
			if len(originalKeys) != elements {
				t.Fatalf("source holds keys %v, want %d", originalKeys, elements)
			}
			if !reflect.DeepEqual(restoredKeys, originalKeys) {
				t.Fatalf("restored keys %v, want the original keys %v: the dumped value did not carry its key", restoredKeys, originalKeys)
			}
		})
	}
	t.Log("blob is excluded: payloads are never dumped (GAP-28), so a restore has no key to re-derive")
}

func insertKeyed(t *testing.T, s *Server, bucket string, i int) {
	t.Helper()
	resp, err := s.Insert(InsertRequest{Database: "app", Bucket: bucket, Key: fmt.Sprintf("k%d", i), Data: map[string]interface{}{"n": i}})
	if err != nil || !resp.Success {
		t.Fatalf("insert k%d: %+v, %v", i, resp, err)
	}
}

func pushUnkeyed(t *testing.T, s *Server, bucket string, i int) {
	t.Helper()
	resp, err := s.Push(PushRequest{Database: "app", Bucket: bucket, Priority: int64(i), Data: map[string]interface{}{"n": i}})
	if err != nil || !resp.Success {
		t.Fatalf("push %d: %+v, %v", i, resp, err)
	}
}

// findKeys looks every wanted key up and returns it when the bucket serves it,
// or "<missing:k>" when it does not: a restore that re-derived a different key
// leaves the original one unanswerable.
func findKeys(t *testing.T, s *Server, bucket string, want []string) []string {
	t.Helper()
	var found []string
	for _, key := range want {
		resp, err := s.Find(FindRequest{Database: "app", Bucket: bucket, Key: key})
		if err != nil {
			t.Fatalf("find %q: %v", key, err)
		}
		if !resp.Found {
			found = append(found, "<missing:"+key+">")
			continue
		}
		found = append(found, key)
	}
	return found
}

// popKeys drains a queue or heap and returns each element's _id_ in pop
// order, which for a heap also carries the priority the restore re-attached.
func popKeys(t *testing.T, s *Server, bucket string, _ []string) []string {
	t.Helper()
	var popped []string
	for {
		resp, err := s.Pop(PopRequest{Database: "app", Bucket: bucket})
		if err != nil {
			t.Fatalf("pop: %v", err)
		}
		if !resp.Found {
			return popped
		}
		var doc bson.M
		if err := bson.Unmarshal(resp.Items[0], &doc); err != nil {
			t.Fatalf("decode popped element: %v", err)
		}
		popped = append(popped, fmt.Sprint(doc["_id_"]))
	}
}
