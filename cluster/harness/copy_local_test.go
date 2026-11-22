package harness_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// TestARealCopyAnswersALocalGetAndSaysSo: [GUARD replicate-for-locality works: on a three-node
// cluster at RF 2 the node that holds a copy of a document answers get {local: true} itself and
// says served local, the owner says owner, and the node outside the copy set asks the owner].
// A refusal is retried only when it is the cluster still settling (UNAVAILABLE, a quorum or
// lease refusal); a copy that never answers local fails the test.
func TestARealCopyAnswersALocalGetAndSaysSo(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{})
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(proofCtx(t), proofDB, 2); err != nil {
		t.Fatal(err)
	}
	c.AwaitConverged(10 * time.Second)
	if _, err := leader.CreateBucket(proofCtx(t), proofDB, "User", wire.Field{Name: "Name", Type: "string"}); err != nil {
		t.Fatal(err)
	}
	c.AwaitLeases(15 * time.Second)
	served := map[string]int{}
	for i := 0; i < 6; i++ {
		id := fmt.Sprintf("u%d", i)
		owner, _, err := c.Node(c.IDs()[0]).Owner(proofCtx(t), proofDB, "User", id)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Node(owner).Insert(proofCtx(t), proofDB, "User", bson.D{{Key: "_id_", Value: id}, {Key: "Name", Value: "Name" + id}}); err != nil {
			t.Fatalf("inserting %s through its owner %s: %v", id, owner, err)
		}
		for _, node := range c.IDs() {
			var saw wire.Document
			deadline := time.Now().Add(3 * time.Second)
			for {
				doc, err := c.Node(node).GetLocal(proofCtx(t), proofDB, "User", id)
				var coded *cluster.Error
				settling := errors.As(err, &coded) && (coded.Code == cluster.CodeUnavailable || coded.Code == cluster.CodeNoQuorum || coded.Code == cluster.CodeLeaseLapsed)
				if err != nil && !settling {
					t.Fatalf("local get of %s on %s: %v", id, node, err)
				}
				saw = doc
				// The owner answers as owner at once; a copy may lag a moment and is asked again, for 3 seconds at most.
				if err == nil && (node == owner || doc.Served == "local") {
					break
				}
				if time.Now().After(deadline) {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			switch {
			case node == owner && saw.Served != "owner":
				t.Errorf("%s: the owner %s says %q, want owner", id, node, saw.Served)
			case node != owner:
				served[saw.Served]++
			}
		}
	}
	// Of the 12 reads on non-owners, the 6 on copies must have been served local; the other 6 are
	// on the node outside the copy set and are answered by the owner.
	if served["local"] != 6 || served["owner"] != 6 {
		t.Fatalf("non-owner local gets were served %v, want 6 local (the copy) and 6 owner (outside the copy set)", served)
	}
}
