package server

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// damagedStallKV refuses every commit once armed, with the error the node
// store returns for a write behind a level 0 that a damaged table stalls
// (BUG-0381). The real stall needs a store tuned to a handful of level-0
// tables; the storage and Badger tests drive that, and this one pins what the
// server does with the error.
type damagedStallKV struct {
	*kvfault.Store
	refuse atomic.Bool
	dir    string
}

type damagedStallBatch struct {
	storage.KVBatch
	kv *damagedStallKV
}

func (kv *damagedStallKV) NewBatch() storage.KVBatch {
	return damagedStallBatch{KVBatch: kv.Store.NewBatch(), kv: kv}
}

func (batch damagedStallBatch) Commit(sync bool) error {
	if batch.kv.refuse.Load() {
		batch.Discard()
		cause := errors.Join(badger.ErrWritesRefused, badger.ErrL0StalledByDamagedTable,
			errors.New("a table block could not be read: table "+batch.kv.dir+"/000001.sst: checksum mismatch"))
		return &storage.StoreRefusesWritesError{Dir: batch.kv.dir, Cause: cause}
	}
	return batch.KVBatch.Commit(sync)
}

// TestAWriteBehindADamagedTableIsRefusedToTheClientWithTheRepair: an insert
// whose commit the store refuses because a damaged table stalls it comes back
// as an ordinary unsuccessful response (no new error code) whose message names
// the damaged file and says to restore the node.
func TestAWriteBehindADamagedTableIsRefusedToTheClientWithTheRepair(t *testing.T) {
	dataDir := t.TempDir()
	kv := &damagedStallKV{Store: kvfault.New(), dir: dataDir}
	node := newKVFaultServer(dataDir, kv)
	if resp, err := node.CreateBucket(CreateBucketRequest{Database: "app", Name: "events", Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("create: %+v, %v", resp, err)
	}
	kv.refuse.Store(true)

	refusal := ""
	resp, err := node.Insert(InsertRequest{Database: "app", Bucket: "events", Key: "k", Data: map[string]interface{}{"n": 1}})
	switch {
	case err != nil:
		refusal = err.Error()
	case !resp.Success:
		refusal = resp.Message
	default:
		t.Fatalf("an insert the store refused was acknowledged")
	}
	for _, want := range []string{"000001.sst", "damaged", "restore"} {
		if !strings.Contains(refusal, want) {
			t.Errorf("the refusal does not contain %q: %s", want, refusal)
		}
	}
	if strings.Contains(refusal, "free space") {
		t.Errorf("the refusal blames the disk: %s", refusal)
	}
}
