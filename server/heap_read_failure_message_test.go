package server

import (
	"bytes"
	"encoding/hex"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage"
)

// heapElementKey is the storage key of the one entry a test pushed into the
// heap bucket "held": the key the heap's wrappers used to print as hex.
func heapElementKey(t *testing.T, s *Server) []byte {
	t.Helper()
	b := lookupBucket(s, "app", "held")
	prefix := storage.Key(b.slot, b.databaseID, b.id.Load(), storage.NSQueue)
	snap := s.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{Lower: prefix, Upper: append(bytes.Clone(prefix), 0xff)})
	defer it.Close()
	it.Seek(nil)
	if !it.Valid() {
		t.Fatal("the heap bucket holds no entry")
	}
	return bytes.Clone(it.Key())
}

// requireNoKeyAndNextStep asserts BUG-0591's contract on a client message.
func requireNoKeyAndNextStep(t *testing.T, message string, key []byte) {
	t.Helper()
	if strings.Contains(message, hex.EncodeToString(key)) {
		t.Errorf("the message carries the entry's storage key as hex: %s", message)
	}
	for _, want := range []string{`"held"`, `"app"`, "retry", "node log"} {
		if !strings.Contains(message, want) {
			t.Errorf("the message lacks %q: %s", want, message)
		}
	}
}

func pushOneHeapEntry(t *testing.T, s *Server) {
	t.Helper()
	resp, err := s.Push(PushRequest{Database: "app", Bucket: "held", Data: map[string]interface{}{"n": 1}, Priority: 5})
	if err != nil || !resp.Success {
		t.Fatalf("pushing the heap entry: %+v, %v", resp, err)
	}
}

// TestHeapPeekValueReadFailureOmitsStorageKey: a peek whose value read fails
// for a reason other than a damaged file answers with the bucket and the next
// step, not the entry's storage key (BUG-0591); the key goes to the log.
func TestHeapPeekValueReadFailureOmitsStorageKey(t *testing.T) {
	s, kv, _, _ := newFaultNodeWithBucket(t, BucketTypeHeap)
	pushOneHeapEntry(t, s)
	var logged bytes.Buffer
	s.logger = slog.New(slog.NewTextHandler(&logged, nil))
	key := heapElementKey(t, s)

	kv.FailNextReadOf(key)
	resp, err := s.Peek(PeekRequest{Database: "app", Bucket: "held", Limit: 1})
	if err != nil {
		t.Fatalf("peek: %v", err)
	}
	if resp.Found {
		t.Fatalf("the peek found an entry through an injected read failure: %+v", resp)
	}
	requireNoKeyAndNextStep(t, resp.Message, key)
	if !strings.Contains(logged.String(), hex.EncodeToString(key)) {
		t.Errorf("the node log does not carry the key; log: %q", logged.String())
	}
}

// TestHeapPopReadFailureOmitsStorageKey is the pop's value read.
func TestHeapPopReadFailureOmitsStorageKey(t *testing.T) {
	s, kv, _, _ := newFaultNodeWithBucket(t, BucketTypeHeap)
	pushOneHeapEntry(t, s)
	key := heapElementKey(t, s)

	kv.FailNextReadOf(key)
	resp, err := s.Pop(PopRequest{Database: "app", Bucket: "held"})
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if resp.Found {
		t.Fatalf("the pop removed an entry through an injected read failure: %+v", resp)
	}
	requireNoKeyAndNextStep(t, resp.Message, key)
}

// deleteFailingKV makes every batch delete fail as the Badger store's does when
// it refuses a key, which the fault store cannot be armed to do.
type deleteFailingKV struct{ storage.KV }

func (d deleteFailingKV) NewBatch() storage.KVBatch {
	return deleteFailingBatch{d.KV.NewBatch()}
}

type deleteFailingBatch struct{ storage.KVBatch }

func (deleteFailingBatch) Delete(key []byte) error {
	return &storage.KeyError{Operation: "adding a key to a batch", Key: bytes.Clone(key), Cause: errors.New("injected delete failure")}
}

// TestHeapPopDeleteFailureOmitsStorageKey: a pop whose delete fails.
func TestHeapPopDeleteFailureOmitsStorageKey(t *testing.T) {
	s, _, _, _ := newFaultNodeWithBucket(t, BucketTypeHeap)
	pushOneHeapEntry(t, s)
	key := heapElementKey(t, s)
	s.kv = deleteFailingKV{s.kv}

	resp, err := s.Pop(PopRequest{Database: "app", Bucket: "held"})
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if resp.Found {
		t.Fatalf("the pop removed an entry through an injected delete failure: %+v", resp)
	}
	requireNoKeyAndNextStep(t, resp.Message, key)
}
