package server

import (
	"context"
	"testing"
	"time"
)

// A client drop of a database with no live generation, landing while
// the first bucket of that database is being created, must not detach the
// create's struct, which the drop never saw: the create is acknowledged and
// served, here and after a restart.
func TestAClientDropOfANewDatabaseMidFirstCreateLeavesTheCreateServed(t *testing.T) {
	dataDir := t.TempDir()
	server := startCatalogServerAt(t, dataDir)
	node := server.core
	const database, bucket = "freshdb", "first"

	creatorParked := make(chan struct{})
	releaseCreator := make(chan struct{})
	hook := func(hookDatabase, hookBucket string) (uint16, bool) {
		if hookDatabase == database && hookBucket == bucket {
			close(creatorParked)
			<-releaseCreator
		}
		return 0, false
	}
	slotOverrideHook.Store(&hook)
	t.Cleanup(func() { slotOverrideHook.Store(nil) })

	type createAnswer struct {
		resp *CreateBucketResponse
		err  error
	}
	createDone := make(chan createAnswer, 1)
	go func() {
		resp, err := node.createBucketLocked(context.Background(), CreateBucketRequest{Database: database, Name: bucket, Type: BucketTypeHash})
		createDone <- createAnswer{resp, err}
	}()
	select {
	case <-creatorParked:
	case <-time.After(10 * time.Second):
		t.Fatal("the create never reached its slot choice")
	}

	type dropAnswer struct {
		resp *DeleteDatabaseResponse
		err  error
	}
	dropDone := make(chan dropAnswer, 1)
	go func() {
		resp, err := node.DeleteDatabase(DeleteDatabaseRequest{Name: database})
		dropDone <- dropAnswer{resp, err}
	}()
	// Either the drop answers (the fix) or it has already taken the database
	// out of the registry under the parked create (the bug); the create is
	// released after whichever happens.
	var dropResult *dropAnswer
	deadline := time.Now().Add(10 * time.Second)
	for dropResult == nil && node.serverDataAccessor().getDB(database) != nil && time.Now().Before(deadline) {
		select {
		case answer := <-dropDone:
			dropResult = &answer
		case <-time.After(time.Millisecond):
		}
	}
	close(releaseCreator)

	created := <-createDone
	if dropResult == nil {
		answer := <-dropDone
		dropResult = &answer
	}
	if dropResult.err != nil {
		t.Errorf("the drop: %+v, %v", dropResult.resp, dropResult.err)
	}
	if created.err != nil || created.resp == nil || !created.resp.Success {
		t.Fatalf("the create: %+v, %v", created.resp, created.err)
	}
	if got := viewOf(t, node, database, bucket); !got.catalogued || !got.open || !got.listed {
		t.Fatalf("an acknowledged create is not served: %+v", got)
	}
	assertSameAfterRestart(t, server, dataDir, database, bucket)
}

// A first create that fails before it commits leaves a bare struct
// that keeps its database listed. A later drop of the database must remove it,
// so the "creating" mark that the first-create guard puts on a struct in flight must be
// cleared when the create returns without committing.
func TestADropRemovesTheGhostOfAFailedFirstCreate(t *testing.T) {
	server := startCatalogServerAt(t, t.TempDir())
	node := server.core
	const database = "ghost"

	failed, err := node.CreateBucket(CreateBucketRequest{Database: database, Name: "x", Type: BucketTypeBTree,
		Options: map[string]interface{}{"indices": []interface{}{"name"}}})
	if err != nil || failed.Success {
		t.Fatalf("precondition: the create was meant to fail: %+v, %v", failed, err)
	}
	if resp, err := node.DeleteDatabase(DeleteDatabaseRequest{Name: database}); err != nil || !resp.Success {
		t.Fatalf("the drop: %+v, %v", resp, err)
	}
	listing, err := node.ListDatabases(ListDatabasesRequest{})
	if err != nil {
		t.Fatalf("listing databases: %v", err)
	}
	for _, info := range listing.Databases {
		if info.Name == database {
			t.Fatalf("database %q is still listed after its drop: %+v", database, listing.Databases)
		}
	}
}
