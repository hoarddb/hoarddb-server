package server

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// TestAPulledBucketSurvivesAPowerLossOnceServed: a pulled copy is
// served — bucketStoreOpen, handleMigrate's success and answerCopy's Holds all
// read the id attached after the catalog commit — only once it is durable,
// because a holder releases its own copy on that answer. The joiner here is a
// fault store that loses every batch committed without a sync at a power loss.
//
// The pull is driven through Server.materializeRange with a real source's
// range stream: a server cannot run its cluster pull on a kvfault store,
// because NewService opens Badger (stop-and-ask S-4, taken as recommended).
// kvfault models a sync as covering every earlier commit (Store.Sync), so this
// test cannot see a Badger whose Sync skips immutable memtables; the patch's
// own test, storage/kv_badger_sync_linux_test.go, guards that half.
//
// Guard: delete the s.kv.Sync() that Server.materializeRange takes after
// Server.commitMigratedCatalog and before Server.attachSharedEngineLocked. The
// fix is that one store-wide Sync, not a synced catalog batch: it is taken
// outside the allocator's mutex, and it makes the range's earlier key batches
// durable with the catalog entry.
func TestAPulledBucketSurvivesAPowerLossOnceServed(t *testing.T) {
	const database, bucket, documents = "app", "pulled", 10
	source := newTestServer(t)
	if resp, err := source.CreateBucket(CreateBucketRequest{Database: database, Name: bucket, Type: BucketTypeHash}); err != nil || !resp.Success {
		t.Fatalf("creating the source bucket: %+v, %v", resp, err)
	}
	for i := 0; i < documents; i++ {
		key := fmt.Sprintf("doc%02d", i)
		if resp, err := source.Insert(InsertRequest{Database: database, Bucket: bucket, Key: key, Data: map[string]interface{}{"v": i}}); err != nil || !resp.Success {
			t.Fatalf("inserting %s: %+v, %v", key, resp, err)
		}
	}
	held := lookupBucket(source, database, bucket)
	var stream bytes.Buffer
	if err := source.streamBucketRange(&stream, source.kv, held.slot, held.databaseID, held.id.Load()); err != nil {
		t.Fatalf("streaming the source's range: %v", err)
	}

	dataDir := t.TempDir()
	kv := kvfault.New()
	joiner := newKVFaultServer(dataDir, kv)
	if err := joiner.materializeRange(migratedBucket{database: database, name: bucket, storeType: BucketTypeHash, placement: placementWhole}, stream.Bytes(), 0); err != nil {
		t.Fatalf("materializing the pulled range: %v", err)
	}
	if !joiner.bucketStoreOpen(database, bucket) {
		t.Fatal("setup: the pulled bucket is not served after its materialise")
	}

	// The joiner now answers that it holds the copy; a holder may release its
	// own on that answer. The power goes.
	kv.PowerLoss()
	restarted := newKVFaultServer(dataDir, kv)
	if err := restarted.LoadCatalog(); err != nil {
		t.Fatalf("LoadCatalog after the power loss: %v", err)
	}
	if !restarted.bucketStoreOpen(database, bucket) {
		t.Fatalf("after a power loss the pulled bucket %q is gone from the joiner's catalog, though the joiner had answered that it holds it: "+
			"the materialise's catalog batch was not synced", bucket)
	}
	for i := 0; i < documents; i++ {
		key := fmt.Sprintf("doc%02d", i)
		resp, err := restarted.Find(FindRequest{Database: database, Bucket: bucket, Key: key})
		if err != nil || resp == nil || !resp.Found {
			t.Fatalf("after a power loss %s of the pulled bucket is missing (%v): the range's key batches were not made durable with the catalog", key, err)
		}
	}
}
