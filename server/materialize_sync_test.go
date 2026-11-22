package server

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// TestMaterializedBucketSurvivesPowerLoss is the acceptance test for materialise durability. A
// joiner answers the hand-off once materializeRange returns, and the holder
// then releases its copy, so everything the materialise wrote must be durable
// by then: a power loss right after it returns must not take the bucket's
// catalog entry or its keys with it. The fault store discards every batch
// committed without a sync, as the disk would.
func TestMaterializedBucketSurvivesPowerLoss(t *testing.T) {
	kv := kvfault.New()
	s := newKVFaultServer(t.TempDir(), kv)
	source := newMigrationSource(t, s, "source")
	// The source's own writes are not under test; make them durable so only
	// the materialise's writes are at risk.
	if err := kv.Sync(); err != nil {
		t.Fatalf("syncing the source: %v", err)
	}

	if err := s.materializeRange(migratedBucket{database: "app", name: "pulled", storeType: BucketTypeBlob}, source.stream, 0); err != nil {
		t.Fatalf("materialising %q: %v", "pulled", err)
	}
	pulled := lookupBucket(s, "app", "pulled")
	slot, databaseID, id := pulled.slot, pulled.databaseID, pulled.id.Load()
	keysBefore := liveKeysIn(t, kv, slot, databaseID, id)
	if keysBefore == 0 {
		t.Fatalf("precondition: the materialise wrote no keys under {slot %d, id %d}", slot, id)
	}

	kv.PowerLoss()

	if _, catalogued, err := s.readCatalogEntry("app", "pulled"); err != nil || !catalogued {
		t.Errorf("after a power loss following the materialise, the pulled bucket's catalog entry is gone (catalogued=%v, err=%v) although the joiner had already answered the hand-off", catalogued, err)
	}
	if keysAfter := liveKeysIn(t, kv, slot, databaseID, id); keysAfter != keysBefore {
		t.Errorf("a power loss after the materialise left %d of its %d keys under {slot %d, id %d}", keysAfter, keysBefore, slot, id)
	}
}

// materializeSyncKV is the fault store with a Sync that can be made to fail, the
// disk error BUG-0456 needs after the materialise's catalog commit.
type materializeSyncKV struct {
	*kvfault.Store
	failSync atomic.Bool
}

func (kv *materializeSyncKV) Sync() error {
	if kv.failSync.Load() {
		return errors.New("injected disk error")
	}
	return kv.Store.Sync()
}

// TestMaterializeSyncFailureLeavesNoUnattachedCatalogEntry is BUG-0456: a
// materialise whose Sync fails after the catalog commit used to leave the
// bucket catalogued but unattached, and the retry was refused as already
// catalogued until a restart.
func TestMaterializeSyncFailureLeavesNoUnattachedCatalogEntry(t *testing.T) {
	kv := &materializeSyncKV{Store: kvfault.New()}
	s := newKVFaultServer(t.TempDir(), kv)
	source := newMigrationSource(t, s, "source")
	pulled := migratedBucket{database: "app", name: "pulled", storeType: BucketTypeBlob}

	kv.failSync.Store(true)
	if err := s.materializeRange(pulled, source.stream, 0); err == nil {
		t.Fatal("materialising with a failing Sync returned no error")
	}
	if _, catalogued, err := s.readCatalogEntry("app", "pulled"); err != nil || catalogued {
		t.Errorf("after the failed materialise the node holds a catalog entry for a bucket it does not serve (catalogued=%v, err=%v)", catalogued, err)
	}

	kv.failSync.Store(false)
	if err := s.materializeRange(pulled, source.stream, 0); err != nil {
		t.Fatalf("retrying the materialise once Sync works: %v", err)
	}
	if served := lookupBucket(s, "app", "pulled"); served == nil || served.id.Load() == 0 {
		t.Errorf("the retried materialise did not attach the bucket")
	}
}

// TestMaterializeIndexAttachFailureCanBeRetried is BUG-0456's second entry: an
// index record NewIndexManager rejects failed the attach after the commit.
func TestMaterializeIndexAttachFailureCanBeRetried(t *testing.T) {
	s := newKVFaultServer(t.TempDir(), kvfault.New())
	source := newMigrationSource(t, s, "source")
	damaged := []catalogIndex{
		{ID: 1, Name: "by_a", Fields: []string{"a"}},
		{ID: 1, Name: "by_b", Fields: []string{"b"}},
	}
	pulled := migratedBucket{database: "app", name: "pulled", storeType: BucketTypeBlob, indices: damaged}

	if err := s.materializeRange(pulled, source.stream, 0); err == nil {
		t.Fatal("materialising a range whose index records share an id returned no error")
	}
	if _, catalogued, err := s.readCatalogEntry("app", "pulled"); err != nil || catalogued {
		t.Errorf("after the failed materialise the node holds a catalog entry for a bucket it does not serve (catalogued=%v, err=%v)", catalogued, err)
	}

	pulled.indices = damaged[:1]
	if err := s.materializeRange(pulled, source.stream, 0); err != nil {
		t.Fatalf("retrying the materialise with a sound index record: %v", err)
	}
	if served := lookupBucket(s, "app", "pulled"); served == nil || served.id.Load() == 0 {
		t.Errorf("the retried materialise did not attach the bucket")
	}
}
