package harness_test

import (
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// TestACopyNodeBehindOnTheSchemaRefusesAndThePublicationStillRuns: at the node, not at a
// stub. A record written under a declaration version the copy has not published is refused
// retryably after the bounded wait (nothing is appended), a record at the version it holds
// passes at once, and a schema bump still commits and publishes while such a record waits.
func TestACopyNodeBehindOnTheSchemaRefusesAndThePublicationStillRuns(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{NoDetection: true})
	leaderID := c.AwaitLeader(10 * time.Second)
	leader := c.Node(leaderID)
	dbID, err := leader.CreateDatabaseRF(proofCtx(t), proofDB, 2)
	if err != nil {
		t.Fatal(err)
	}
	c.AwaitConverged(10 * time.Second)
	bucketID, err := leader.CreateBucket(proofCtx(t), proofDB, proofBucket, wire.Field{Name: "name", Type: "string"})
	if err != nil {
		t.Fatal(err)
	}
	var copyID string
	for _, id := range c.IDs() {
		if id != leaderID {
			copyID = id
		}
	}
	copyNode := c.Node(copyID)
	// Let the copy cache the declaration, as a replicated write would have.
	c.Await("the copy to resolve the bucket", 20*time.Second, func() bool {
		_, _, err := copyNode.LocalDocument(proofCtx(t), proofDB, proofBucket, "probe")
		return err == nil
	})
	if err := copyNode.SchemaReady(dbID, cluster.Op{BucketID: bucketID, DeclVersion: 1}); err != nil {
		t.Fatalf("a record at a version the copy holds was refused: %v", err)
	}
	waiting := make(chan error, 1)
	began := time.Now()
	go func() { waiting <- copyNode.SchemaReady(dbID, cluster.Op{BucketID: bucketID, DeclVersion: 1 << 40}) }()
	if err := leader.BumpSchema(proofCtx(t), dbID, 1, 2, cluster.BumpNote{}); err != nil {
		t.Fatalf("a schema bump while a record waits in SchemaReady: %v", err)
	}
	select {
	case err := <-waiting:
		if err == nil || !strings.Contains(err.Error(), "Nothing was applied") {
			t.Fatalf("a record ahead of the copy's schema: %v, want a retryable refusal", err)
		}
		if took := time.Since(began); took < 500*time.Millisecond || took > 1500*time.Millisecond {
			t.Fatalf("refused after %s: the copy must wait for its publication first, and answer inside the owner's 2 s call", took)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("SchemaReady never returned")
	}
}

// TestAStalledDeclarationOwnerCannotHoldACopyForTheWholeCallTimeout: a copy that has not
// cached a bucket's declaration fetches it from the declaration's owner. When that owner
// has stopped answering, the fetch must give up well inside the owner's 2 s call timeout,
// or the replicated write that asked fails with the call and is retried into the same
// wait until the write concern runs out (found by the stalled-copy test). The other two
// nodes are stalled for each node in turn, so whichever node does not hold the
// declaration, or own it, must read it remotely. [GUARD: raise the fetch budget to 2 s]
func TestAStalledDeclarationOwnerCannotHoldACopyForTheWholeCallTimeout(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{NoDetection: true})
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	dbID, err := leader.CreateDatabaseRF(proofCtx(t), proofDB, 2)
	if err != nil {
		t.Fatal(err)
	}
	c.AwaitConverged(10 * time.Second)
	bucketID, err := leader.CreateBucket(proofCtx(t), proofDB, proofBucket, wire.Field{Name: "name", Type: "string"})
	if err != nil {
		t.Fatal(err)
	}
	var slowest time.Duration
	refusals := 0
	for _, asking := range c.IDs() {
		for _, other := range c.IDs() {
			if other != asking {
				c.Stall(other, true)
			}
		}
		began := time.Now()
		err := c.Node(asking).SchemaReady(dbID, cluster.Op{BucketID: bucketID, DeclVersion: 1})
		took := time.Since(began)
		for _, other := range c.IDs() {
			if other != asking {
				c.Stall(other, false)
			}
		}
		if err != nil {
			// pi F7: no declaration and no way to fetch it is a retryable refusal, never
			// "ready": the copy cannot resolve the record's store fields without it.
			if !strings.Contains(err.Error(), "Nothing was applied") || !strings.Contains(err.Error(), "could not fetch it") {
				t.Fatalf("%s: SchemaReady with the declaration's owner stalled: %v, want the retryable refusal", asking, err)
			}
			refusals++
		}
		slowest = max(slowest, took)
	}
	t.Logf("slowest SchemaReady with the other nodes stalled: %s, %d refusals", slowest, refusals)
	if refusals == 0 {
		t.Fatal("no node needed the declaration from a stalled owner, so the fetch-failure path was not exercised")
	}
	if slowest > 1200*time.Millisecond {
		t.Fatalf("a copy waited %s for a stalled declaration owner, want well under the 2 s call timeout", slowest)
	}
}
