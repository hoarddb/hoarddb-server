package store

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// BenchmarkHashIndexedOverwrite is the index half of an overwrite in a hash
// bucket with two indexes, on a ten-field document: the map path the store
// used (oracleIndexDocument) and the field read it uses now (bson-codec-spec
// 9.2: informational, not a gate).
func BenchmarkHashIndexedOverwrite(b *testing.B) {
	document := func(version int) []byte {
		encoded, err := bson.Marshal(map[string]interface{}{
			"email": fmt.Sprintf("user%d@example.com", version), "age": int32(40 + version), "name": "Dalek",
			"payload": strings.Repeat("abcdefghij", 10), "score": 2.5, "active": true, "city": "Oslo",
			"address": map[string]interface{}{"street": "Main", "zip": "0150"},
			"tags":    []interface{}{"a", "b", "c"}, "seq": int64(version),
		})
		if err != nil {
			b.Fatalf("encoding: %v", err)
		}
		return encoded
	}
	previous, next := document(1), document(2)
	specs := []IndexSpec{
		{ID: 0, Name: "email_1", Fields: []IndexKeyPart{{Field: "email"}}, Unique: true, Version: IndexSpecVersion},
		{ID: 1, Name: "age_1", Fields: []IndexKeyPart{{Field: "age"}}, Version: IndexSpecVersion},
	}
	for _, path := range []struct {
		name  string
		index func(h *HashStore, snap storage.KVSnapshot, batch storage.KVBatch) error
	}{
		{"map", func(h *HashStore, snap storage.KVSnapshot, batch storage.KVBatch) error {
			return oracleIndexDocument(h, snap, nil, batch, "k", previous, true, next)
		}},
		{"fields", func(h *HashStore, snap storage.KVSnapshot, batch storage.KVBatch) error {
			return h.indexDocument(snap, nil, batch, "k", previous, true, next)
		}},
	} {
		b.Run(path.name, func(b *testing.B) {
			kv := kvfault.New()
			im, err := NewIndexManager(kv, 1, 1, 1, specs)
			if err != nil {
				b.Fatalf("NewIndexManager: %v", err)
			}
			h := NewHashStore(kv, 1, 1, 1, new(OwedApplies))
			h.SetIndexManager(im)
			snap := kv.NewSnapshot()
			defer snap.Discard()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				batch := kv.NewBatch()
				if err := path.index(h, snap, batch); err != nil {
					b.Fatalf("indexing: %v", err)
				}
				batch.Discard()
			}
		})
	}
}
