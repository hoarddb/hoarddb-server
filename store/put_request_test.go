package store

import (
	"errors"
	"reflect"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// insertEngine is a hash or btree bucket seen through the calls the insert
// intent changes.
type insertEngine struct {
	name  string
	put   func(info ApplyInfo, req PutRequest) error
	batch func(info ApplyInfo, req PutBatchRequest) error
	get   func(key string) ([]byte, error)
	count func() (int64, error)
}

func insertEngines() []func(storage.KV) insertEngine {
	return []func(storage.KV) insertEngine{
		func(kv storage.KV) insertEngine {
			h := NewHashStore(kv, applyTestSlot, 1, applyTestID, new(OwedApplies))
			return insertEngine{name: "hash",
				put: func(info ApplyInfo, req PutRequest) error {
					_, err := h.PutBelow(info, req)
					return err
				},
				batch: func(info ApplyInfo, req PutBatchRequest) error {
					_, err := h.PutBatch(info, req)
					return err
				},
				get:   h.Get,
				count: h.Len,
			}
		},
		func(kv storage.KV) insertEngine {
			b := NewBTreeStore(kv, applyTestSlot, 1, applyTestID, new(OwedApplies))
			return insertEngine{name: "btree",
				put: func(info ApplyInfo, req PutRequest) error {
					_, err := b.PutBelow(info, req)
					return err
				},
				batch: func(info ApplyInfo, req PutBatchRequest) error {
					_, err := b.PutBatch(info, req)
					return err
				},
				get:   func(key string) ([]byte, error) { return b.Get(key) },
				count: b.Len,
			}
		},
	}
}

// storedN is the "n" field of the document stored under key, or -1 when the
// key is absent.
func storedN(t *testing.T, engine insertEngine, key string) int32 {
	t.Helper()
	raw, err := engine.get(key)
	if errors.Is(err, storage.ErrKeyNotFound) || (err == nil && raw == nil) {
		return -1
	}
	if err != nil {
		t.Fatalf("%s: reading %q: %v", engine.name, key, err)
	}
	var document struct {
		N int32 `bson:"n"`
	}
	if err := bson.Unmarshal(raw, &document); err != nil {
		t.Fatalf("%s: decoding %q: %v", engine.name, key, err)
	}
	return document.N
}

func requireCount(t *testing.T, engine insertEngine, want int64) {
	t.Helper()
	got, err := engine.count()
	if err != nil {
		t.Fatalf("%s: counting: %v", engine.name, err)
	}
	if got != want {
		t.Fatalf("%s: the bucket counts %d documents, want %d", engine.name, got, want)
	}
}

// TestPutRequestRefuseExistingRefusesInsideTheApply is the store half of the
// insert that refuses an existing _id_ (document-placement build doc step 4,
// BD-2, BD-3, BD-4): told RefuseExisting, a hash or btree bucket refuses a
// held key with *DocumentExistsError from inside the apply, so nothing is
// written and the count does not move; a live refusal is consumed (the mark
// moves to its sequence) and a replayed one is left above the mark; a batch
// reports every held position and writes none of its documents; and the
// owner's verdict is followed instead of the copy's own check.
func TestPutRequestRefuseExistingRefusesInsideTheApply(t *testing.T) {
	for _, build := range insertEngines() {
		kv := kvfault.New()
		engine := build(kv)
		t.Run(engine.name, func(t *testing.T) {
			doc := func(id string, n int32) map[string]interface{} {
				return map[string]interface{}{"_id_": id, "n": n}
			}
			if err := engine.batch(ApplyInfo{Seq: 1}, PutBatchRequest{Items: []map[string]interface{}{doc("a", 1), doc("c", 1)}}); err != nil {
				t.Fatalf("seeding: %v", err)
			}

			// A live insert of a held key: refused, nothing written, consumed.
			err := engine.put(ApplyInfo{Seq: 2}, PutRequest{Key: "a", Value: doc("a", 2), RefuseExisting: true})
			var exists *DocumentExistsError
			if !errors.As(err, &exists) || exists.Key != "a" {
				t.Fatalf("a live insert of the held key answered %v; want *DocumentExistsError for \"a\"", err)
			}
			if got := storedN(t, engine, "a"); got != 1 {
				t.Fatalf("after the refused insert \"a\" holds n=%d; want the first document's 1", got)
			}
			requireCount(t, engine, 2)
			if st := stateOf(t, kv); st.AppliedSeq != 2 {
				t.Fatalf("after the live refusal at sequence 2 the mark is %d; want 2 (consumed)", st.AppliedSeq)
			}

			// The same refusal met at replay is left above the mark.
			err = engine.put(ApplyInfo{Seq: 3, Replay: true}, PutRequest{Key: "a", Value: doc("a", 3), RefuseExisting: true})
			if !errors.As(err, &exists) {
				t.Fatalf("a replayed insert of the held key answered %v; want *DocumentExistsError", err)
			}
			if st := stateOf(t, kv); st.AppliedSeq != 2 {
				t.Fatalf("after the replayed refusal at sequence 3 the mark is %d; want 2 (left above the mark)", st.AppliedSeq)
			}

			// Two held positions out of four: both named, nothing written.
			err = engine.batch(ApplyInfo{Seq: 4}, PutBatchRequest{RefuseExisting: true,
				Items: []map[string]interface{}{doc("a", 4), doc("b", 4), doc("c", 4), doc("d", 4)}})
			if !errors.As(err, &exists) || !reflect.DeepEqual(exists.Positions, []int{1, 3}) {
				t.Fatalf("a batch with \"a\" and \"c\" held answered %v; want *DocumentExistsError at positions [1 3]", err)
			}
			for key, want := range map[string]int32{"a": 1, "b": -1, "c": 1, "d": -1} {
				if got := storedN(t, engine, key); got != want {
					t.Errorf("after the refused batch %q holds n=%d; want %d (-1 is absent)", key, got, want)
				}
			}
			requireCount(t, engine, 2)
			if st := stateOf(t, kv); st.AppliedSeq != 4 {
				t.Fatalf("after the refused batch at sequence 4 the mark is %d; want 4 (consumed)", st.AppliedSeq)
			}

			// The owner refused it: refused without reading, even for a key this
			// copy does not hold.
			err = engine.put(ApplyInfo{Seq: 5, OwnerDecided: OwnerRefused}, PutRequest{Key: "z", Value: doc("z", 5), RefuseExisting: true})
			if !errors.As(err, &exists) {
				t.Fatalf("an insert the owner refused answered %v; want *DocumentExistsError", err)
			}
			if got := storedN(t, engine, "z"); got != -1 {
				t.Fatalf("an insert the owner refused stored \"z\" (n=%d)", got)
			}

			// The owner applied it: put over the held key, and the held key noted.
			noted := 0
			applied := ApplyInfo{Seq: 6, OwnerDecided: OwnerApplied, NoteHeldID: func() { noted++ }}
			if err := engine.put(applied, PutRequest{Key: "a", Value: doc("a", 6), RefuseExisting: true}); err != nil {
				t.Fatalf("an insert the owner applied over a held key answered %v; want it put", err)
			}
			if got := storedN(t, engine, "a"); got != 6 || noted != 1 {
				t.Fatalf("after the owner-applied insert \"a\" holds n=%d and the held key was noted %d times; want 6 and 1", got, noted)
			}
			applied.Seq = 7
			if err := engine.put(applied, PutRequest{Key: "y", Value: doc("y", 7), RefuseExisting: true}); err != nil {
				t.Fatalf("an insert the owner applied of a new key answered %v", err)
			}
			if noted != 1 {
				t.Fatalf("an owner-applied insert of a key this copy did not hold was noted as held")
			}

			// Without the intent a held key is replaced, as it always was.
			if err := engine.put(ApplyInfo{Seq: 8}, PutRequest{Key: "c", Value: doc("c", 8)}); err != nil {
				t.Fatalf("an upsert of a held key answered %v", err)
			}
			if got := storedN(t, engine, "c"); got != 8 {
				t.Fatalf("after the upsert \"c\" holds n=%d; want 8", got)
			}
			requireCount(t, engine, 3)
		})
	}
}
