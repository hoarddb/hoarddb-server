package harness_test

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// bug0766Cluster forms three voters with an RF 2 database and a bucket with a fifoStore and
// a hashStore field.
func bug0766Cluster(t *testing.T, opts harness.Options) *harness.Cluster {
	t.Helper()
	c := harness.NewCluster(t, 3, opts)
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(proofCtx(t), proofDB, 2); err != nil {
		t.Fatalf("creating the RF 2 database: %v", err)
	}
	c.AwaitConverged(10 * time.Second)
	if _, err := leader.CreateBucket(proofCtx(t), proofDB, "logs",
		wire.Field{Name: "name", Type: "string"}, wire.Field{Name: "Queue", Type: "fifoStore"}, wire.Field{Name: "Table", Type: "hashStore"}); err != nil {
		t.Fatalf("creating the bucket: %v", err)
	}
	return c
}

func bug0766Target(id, field string) wire.StoreTarget {
	return wire.StoreTarget{Database: proofDB, Bucket: "logs", ID: id, Field: field}
}

// bug0766Fill creates document id through its owner and puts three queue elements and two
// table entries in it, and leases two queue messages.
func bug0766Fill(t *testing.T, c *harness.Cluster, id string) (owner string, lease wire.Lease) {
	t.Helper()
	owner, _, err := c.Node(c.IDs()[0]).Owner(proofCtx(t), proofDB, "logs", id)
	if err != nil {
		t.Fatalf("placing %s: %v", id, err)
	}
	node := c.Node(owner)
	if _, err := node.Insert(proofCtx(t), proofDB, "logs", bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "n-" + id}}); err != nil {
		t.Fatalf("inserting %s through its owner %s: %v", id, owner, err)
	}
	for _, item := range []string{"a", "b", "c"} {
		if err := node.Push(proofCtx(t), wire.PushRequest{StoreTarget: bug0766Target(id, "Queue"), Element: bson.D{{Key: "item", Value: item}}}); err != nil {
			t.Fatalf("pushing %s to %s: %v", item, id, err)
		}
	}
	for _, key := range []string{"k1", "k2"} {
		if err := node.Put(proofCtx(t), wire.PutRequest{StoreTarget: bug0766Target(id, "Table"), Key: key, Element: bson.D{{Key: "item", Value: "v-" + key}}}); err != nil {
			t.Fatalf("putting %s in %s: %v", key, id, err)
		}
	}
	lease, err = node.Claim(proofCtx(t), wire.ClaimRequest{StoreTarget: bug0766Target(id, "Queue"), Count: 2})
	if err != nil {
		t.Fatalf("claiming from %s: %v", id, err)
	}
	return owner, lease
}

// TestAStoreReadThroughANonOwnerIsForwarded (BUG-0766): count, list, peek, get and lease_get
// of a store field answer the owner's answer through every node, as a document get does.
// Nine documents, so every node owns some and is a non-owner of others.
func TestAStoreReadThroughANonOwnerIsForwarded(t *testing.T) {
	c := bug0766Cluster(t, harness.Options{})
	for i := 0; i < 9; i++ {
		id := fmt.Sprintf("log-%d", i)
		owner, lease := bug0766Fill(t, c, id)
		ownerNode := c.Node(owner)
		wantCount, err := ownerNode.CountField(proofCtx(t), bug0766Target(id, "Queue"))
		if err != nil || wantCount != 1 {
			t.Fatalf("%s at its owner %s: count %d err %v, want 1 (three pushed, two leased)", id, owner, wantCount, err)
		}
		wantList, err := ownerNode.ListField(proofCtx(t), wire.FieldListRequest{StoreTarget: bug0766Target(id, "Table")})
		if err != nil || len(wantList.Elements) != 2 {
			t.Fatalf("%s at its owner: list %d elements err %v, want 2", id, len(wantList.Elements), err)
		}
		wantPeek, err := ownerNode.Peek(proofCtx(t), wire.PeekRequest{StoreTarget: bug0766Target(id, "Queue"), Count: 5})
		if err != nil {
			t.Fatalf("%s at its owner: peek: %v", id, err)
		}
		wantGot, err := ownerNode.GetField(proofCtx(t), wire.StoreGetRequest{StoreTarget: bug0766Target(id, "Table"), Key: "k2"})
		if err != nil || !wantGot.Found {
			t.Fatalf("%s at its owner: get found %v err %v", id, wantGot.Found, err)
		}
		wantLease, err := ownerNode.LeaseElements(proofCtx(t), wire.LeaseElementsRequest{StoreTarget: bug0766Target(id, "Queue"), Lease: lease.ID})
		if err != nil || !wantLease.Found || len(wantLease.Elements) != 2 {
			t.Fatalf("%s at its owner: lease found %v (%d messages) err %v", id, wantLease.Found, len(wantLease.Elements), err)
		}
		for _, through := range c.IDs() {
			if through == owner {
				continue
			}
			node := c.Node(through)
			where := fmt.Sprintf("%s (owner %s) read through %s", id, owner, through)
			if count, err := node.CountField(proofCtx(t), bug0766Target(id, "Queue")); err != nil || count != wantCount {
				t.Errorf("%s: count %d err %v, want %d", where, count, err, wantCount)
			}
			if page, err := node.ListField(proofCtx(t), wire.FieldListRequest{StoreTarget: bug0766Target(id, "Table")}); err != nil ||
				!bytes.Equal(bytes.Join(page.Elements, nil), bytes.Join(wantList.Elements, nil)) || strings.Join(page.Keys, ",") != strings.Join(wantList.Keys, ",") {
				t.Errorf("%s: list %v err %v, want %v", where, page, err, wantList)
			}
			if peek, err := node.Peek(proofCtx(t), wire.PeekRequest{StoreTarget: bug0766Target(id, "Queue"), Count: 5}); err != nil ||
				!bytes.Equal(bytes.Join(peek, nil), bytes.Join(wantPeek, nil)) || len(peek) != 1 {
				t.Errorf("%s: peek %d elements err %v, want %d", where, len(peek), err, len(wantPeek))
			}
			if got, err := node.GetField(proofCtx(t), wire.StoreGetRequest{StoreTarget: bug0766Target(id, "Table"), Key: "k2"}); err != nil || !got.Found || !bytes.Equal(got.Element, wantGot.Element) {
				t.Errorf("%s: get found %v err %v, want the owner's element", where, got.Found, err)
			}
			if got, err := node.GetField(proofCtx(t), wire.StoreGetRequest{StoreTarget: bug0766Target(id, "Table"), Key: "absent"}); err != nil || got.Found {
				t.Errorf("%s: get of an absent key found %v err %v, want an answer of not found", where, got.Found, err)
			}
			if got, err := node.LeaseElements(proofCtx(t), wire.LeaseElementsRequest{StoreTarget: bug0766Target(id, "Queue"), Lease: lease.ID}); err != nil ||
				!got.Found || got.Until != wantLease.Until || !bytes.Equal(bytes.Join(got.Elements, nil), bytes.Join(wantLease.Elements, nil)) {
				t.Errorf("%s: lease_get %+v err %v, want %+v", where, got, err, wantLease)
			}
		}
	}
}

// TestAStoreReadOfAMissingDocumentThroughANonOwnerIsNotFound (BUG-0766): the owner's refusal
// reaches the client with its own code through the node it asked.
func TestAStoreReadOfAMissingDocumentThroughANonOwnerIsNotFound(t *testing.T) {
	c := bug0766Cluster(t, harness.Options{})
	owner, _, err := c.Node(c.IDs()[0]).Owner(proofCtx(t), proofDB, "logs", "ghost")
	if err != nil {
		t.Fatal(err)
	}
	for _, through := range c.IDs() {
		if through == owner {
			continue
		}
		_, err := c.Node(through).CountField(proofCtx(t), bug0766Target("ghost", "Queue"))
		var coded *cluster.Error
		if !errors.As(err, &coded) || coded.Code != cluster.CodeNotFound {
			t.Errorf("count of a missing document through %s: %v, want NOT_FOUND", through, err)
		}
	}
}

// TestAStoreReadThroughASurvivorOfADeadOwnerSaysSoInTrueWords (BUG-0759): the read is forwarded,
// the owner is gone, and the refusal is the coded UNAVAILABLE that names the owner and says
// nothing was changed; it is not the paused-writes sentence with its placeholders unfilled.
func TestAStoreReadThroughASurvivorOfADeadOwnerSaysSoInTrueWords(t *testing.T) {
	c := bug0766Cluster(t, harness.Options{NoDetection: true, WCTimeout: 5 * time.Second})
	owner, _ := bug0766Fill(t, c, "dead-1")
	survivor := ""
	for _, id := range c.IDs() {
		if id != owner {
			survivor = id
			break
		}
	}
	c.Kill(owner)
	for attempt := 0; attempt < 3; attempt++ {
		_, err := c.Node(survivor).CountField(proofCtx(t), bug0766Target("dead-1", "Queue"))
		var coded *cluster.Error
		if !errors.As(err, &coded) || coded.Code != cluster.CodeUnavailable {
			t.Fatalf("attempt %d: %v, want a coded UNAVAILABLE", attempt, err)
		}
		for _, want := range []string{owner, "'nodes'", "Nothing was changed"} {
			if !strings.Contains(coded.Message, want) {
				t.Errorf("attempt %d: %q does not contain %q", attempt, coded.Message, want)
			}
		}
		for _, bad := range []string{"<seq>", "<cause>", "paused", "retained log"} {
			if strings.Contains(coded.Message, bad) {
				t.Errorf("attempt %d: %q contains %q", attempt, coded.Message, bad)
			}
		}
	}
}
