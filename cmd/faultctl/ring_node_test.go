package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// These tests run the real ring write path and digest the directory a real ring node leaves,
// <dir>/badger. Every other test in this package builds its keys by hand, which is how a tool
// pointed at the wrong store (the legacy <data_dir>/store) once passed all of them.

const ringTestDB = "app"

func ringTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// writeAccountWrites creates the bucket and applies inserts, a store-field push and a delete
// through the node's own write path, so every key under test is one the product wrote.
func writeAccountWrites(t *testing.T, ctx context.Context, writer func(id string) *wire.Node, creator *wire.Node) []string {
	t.Helper()
	if _, err := creator.CreateBucket(ctx, ringTestDB, "Account",
		wire.Field{Name: "Name", Type: "string"}, wire.Field{Name: "Events", Type: "fifoStore"}); err != nil {
		t.Fatalf("creating the bucket: %v", err)
	}
	ids := []string{"a1", "a2", "a3", "a4"}
	for _, id := range ids {
		node := writer(id)
		if _, err := node.Insert(ctx, ringTestDB, "Account", bson.D{{Key: "_id_", Value: id}, {Key: "Name", Value: "name " + id}}); err != nil {
			t.Fatalf("inserting %s: %v", id, err)
		}
	}
	push := wire.PushRequest{
		StoreTarget: wire.StoreTarget{Database: ringTestDB, Bucket: "Account", ID: "a1", Field: "Events"},
		Element:     bson.D{{Key: "kind", Value: "login"}},
	}
	if err := writer("a1").Push(ctx, push); err != nil {
		t.Fatalf("pushing to a store field: %v", err)
	}
	if err := writer("a2").Delete(ctx, ringTestDB, "Account", "a2"); err != nil {
		t.Fatalf("deleting a2: %v", err)
	}
	return ids
}

func TestDigestOfARealRingNodeHoldsItsPairs(t *testing.T) {
	dir := t.TempDir()
	ctx := ringTestContext(t)
	node, err := wire.OpenNode(ctx, wire.NodeConfig{Dir: dir, MemTableMB: 32})
	if err != nil {
		t.Fatalf("opening a ring node: %v", err)
	}
	if _, err := node.CreateDatabase(ctx, ringTestDB); err != nil {
		t.Fatalf("creating the database: %v", err)
	}
	writeAccountWrites(t, ctx, func(string) *wire.Node { return node }, node)
	if err := node.Close(); err != nil {
		t.Fatalf("stopping the node: %v", err)
	}

	digest, err := digestStore(filepath.Join(dir, "badger"), "solo")
	if err != nil {
		t.Fatalf("digesting a real ring node: %v", err)
	}
	if len(digest.Pairs) == 0 {
		t.Fatal("the digest of a ring node that took writes holds no pair: the tool is reading the wrong directory")
	}
	var liveKeys uint64
	for _, pair := range digest.Pairs {
		if pair.State == "no-record" {
			t.Errorf("pair (%d, %d) of a real node has no pair record", pair.Slot, pair.Database)
		}
		liveKeys += pair.LiveKeys
	}
	// Three live documents, one store-field element and its state keys at the least.
	if liveKeys < 4 {
		t.Errorf("the digest counts %d live keys, want at least 4", liveKeys)
	}
}

func TestDigestRefusesALegacyNodeStore(t *testing.T) {
	// The legacy store holds slot keys and no pair or seed record, and has no FORMAT file beside it.
	legacyDir := buildStore(t, []kvEntry{documentEntry(5, 7, 1, "d1", "alpha"), documentEntry(9, 7, 2, "e1", "delta")})
	_, err := digestStore(legacyDir, "legacy")
	if err == nil {
		t.Fatal("digest of a store with slot keys and no pair record succeeded")
	}
	for _, want := range []string{"not an entry-ring node store", "legacy", "ring/badger"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not say %q", err, want)
		}
	}
}

// TestHealthyRF2CopiesDigestEqual is the check this tool exists for: both copies of a pair a
// real cluster replicated, after a clean stop, digest equal at the same position.
func TestHealthyRF2CopiesDigestEqual(t *testing.T) {
	cl := harness.NewCluster(t, 3, harness.Options{})
	ctx := ringTestContext(t)
	leader := cl.Node(cl.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(ctx, ringTestDB, 2); err != nil {
		t.Fatalf("creating an RF 2 database: %v", err)
	}
	cl.AwaitConverged(10 * time.Second)
	owners := map[string]string{}
	ownerOf := func(id string) *wire.Node {
		if name, found := owners[id]; found {
			return cl.Node(name)
		}
		name, _, err := leader.Owner(ctx, ringTestDB, "Account", id)
		if err != nil {
			t.Fatalf("finding the owner of %s: %v", id, err)
		}
		owners[id] = name
		return cl.Node(name)
	}
	// The bucket must exist before Owner can place a document in it.
	if _, err := leader.CreateBucket(ctx, ringTestDB, "Account",
		wire.Field{Name: "Name", Type: "string"}, wire.Field{Name: "Events", Type: "fifoStore"}); err != nil {
		t.Fatalf("creating the bucket: %v", err)
	}
	cl.AwaitLeases(15 * time.Second)
	for _, id := range []string{"a1", "a2", "a3", "a4"} {
		retrySettling(t, "inserting "+id, func() error {
			_, err := ownerOf(id).Insert(ctx, ringTestDB, "Account", bson.D{{Key: "_id_", Value: id}, {Key: "Name", Value: "name " + id}})
			return err
		})
	}
	push := wire.PushRequest{
		StoreTarget: wire.StoreTarget{Database: ringTestDB, Bucket: "Account", ID: "a1", Field: "Events"},
		Element:     bson.D{{Key: "kind", Value: "login"}},
	}
	retrySettling(t, "pushing to a store field", func() error { return ownerOf("a1").Push(ctx, push) })
	retrySettling(t, "deleting a2", func() error { return ownerOf("a2").Delete(ctx, ringTestDB, "Account", "a2") })
	awaitCopiesAtOwnerPosition(t, cl, ownerOf, []string{"a1", "a2", "a3", "a4"})

	var files []digestFile
	for _, id := range cl.IDs() {
		cl.Kill(id)
		files = append(files, digestOf(t, filepath.Join(cl.DataDir(id), "badger"), id))
	}
	verdicts := compareDigests(files)
	if len(verdicts) == 0 {
		t.Fatal("no pair is held by two nodes of an RF 2 cluster")
	}
	for _, verdict := range verdicts {
		// A pair with a delete whose sweep had not finished on one node is not comparable yet;
		// a healthy pair is never different.
		if verdict.Kind == verdictDifferent {
			t.Errorf("pair (%d, %d) of a healthy RF 2 cluster reads different: %s", verdict.Slot, verdict.Database, verdict.Detail)
		}
	}
	equal := 0
	for _, verdict := range verdicts {
		if verdict.Kind == verdictEqual {
			equal++
		}
	}
	if equal == 0 {
		t.Errorf("no pair was judged equal: %+v", verdicts)
	}
}

// retrySettling runs write again while the cluster refuses it as still settling (no quorum or
// lease yet, which refuses before anything is applied), for ten seconds at most.
func retrySettling(t *testing.T, what string, write func() error) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := write()
		if err == nil {
			return
		}
		var coded *cluster.Error
		settling := errors.As(err, &coded) && (coded.Code == cluster.CodeUnavailable || coded.Code == cluster.CodeNoQuorum || coded.Code == cluster.CodeLeaseLapsed)
		if !settling || time.Now().After(deadline) {
			t.Fatalf("%s: %v", what, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// awaitCopiesAtOwnerPosition waits until every node that holds the pair of each id reports the
// owner's applied position, which is when a stop leaves the copies comparable.
func awaitCopiesAtOwnerPosition(t *testing.T, cl *harness.Cluster, ownerOf func(string) *wire.Node, ids []string) {
	t.Helper()
	for _, id := range ids {
		owner, copies, err := cl.Node(cl.IDs()[0]).Owner(ringTestContext(t), ringTestDB, "Account", id)
		if err != nil {
			t.Fatalf("finding the copies of %s: %v", id, err)
		}
		want, err := ownerOf(id).PairPosition(ringTestContext(t), ringTestDB, "Account", id)
		if err != nil {
			t.Fatalf("reading the owner's position for %s: %v", id, err)
		}
		cl.Await(fmt.Sprintf("copies of %s reach %v", id, want), 20*time.Second, func() bool {
			for _, name := range append([]string{owner}, copies...) {
				got, err := cl.Node(name).PairPosition(ringTestContext(t), ringTestDB, "Account", id)
				if err != nil || got != (cluster.Position{Epoch: want.Epoch, Seq: want.Seq}) {
					return false
				}
			}
			return true
		})
	}
}
