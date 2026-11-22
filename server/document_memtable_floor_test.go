package server

import (
	"testing"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The per-document limit (at most 30 MiB) and the memtable setting (at least
// 32 MiB) are tuned separately, and the atomic batch bound is a fraction of
// the memtable. A document of the largest size must still be stored on a node
// at the smallest memtable: a value of 1 MiB or more goes to the value log and
// counts as almost nothing towards the bound.
func TestLargestDocumentIsStoredAtTheSmallestMemtable(t *testing.T) {
	s := newTestServerFloorMemtable(t)
	s.config.Storage.MaxDocumentBytes = config.MaxDocumentBytesCeiling
	handler := NewHandler(s)
	handlerSend(t, handler, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	handlerSend(t, handler, transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "docs", Type: BucketTypeHash})

	if frame := putLimitDocument(t, handler, "thirty", docOfEncodedSize(config.MaxDocumentBytesCeiling)); frame.Opcode == transport.OpError {
		t.Fatalf("a %d-byte document was refused at a %d MiB memtable: %.300s", config.MaxDocumentBytesCeiling, config.MinMemtableMB, frame.Payload)
	}
	found, err := s.Find(FindRequest{Database: "app", Bucket: "docs", Key: "thirty"})
	if err != nil || !found.Found {
		t.Fatalf("Find: %+v, %v", found, err)
	}
}
