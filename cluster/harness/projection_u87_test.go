package harness_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// TestU87AProjectedReadForwardedThroughANonOwnerReturnsTheProjectedBody: [GUARD the
// projection rides FORWARD_READ: the owner builds the projected body, and a node that is
// not the owner relays it. Without the forwarded member the outsider would answer with the
// whole document].
func TestU87AProjectedReadForwardedThroughANonOwnerReturnsTheProjectedBody(t *testing.T) {
	c := proofSetup(t, harness.Options{})
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateBucket(proofCtx(t), proofDB, "wide", wire.Field{Name: "name", Type: "string"},
		wire.Field{Name: "bio", Type: "string"}, wire.Field{Name: "queue", Type: "fifoStore"}); err != nil {
		t.Fatalf("creating the bucket: %v", err)
	}
	id, owner, copyNode, outsider := proofID(t, c, "u87")
	if _, err := c.Node(owner).Insert(proofCtx(t), proofDB, "wide", bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada"}, {Key: "bio", Value: "wrote the first program"}}); err != nil {
		t.Fatalf("insert through the owner: %v (background: %v)", err, c.BackgroundErrors())
	}
	want, err := bson.Marshal(bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada"}, {Key: "bio", Value: "wrote the first program"}})
	if err != nil {
		t.Fatal(err)
	}
	nameOnly, err := bson.Marshal(bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: "Ada"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, via := range []string{outsider, copyNode, owner} {
		// Asked in descending field-id order, answered in field-id order.
		both, err := c.Node(via).GetFields(proofCtx(t), proofDB, "wide", id, []string{"bio", "name"})
		if err != nil || !bytes.Equal(both.Body, want) {
			t.Fatalf("a projected get of bio and name through %s: %v, %v, want %v", via, bson.Raw(both.Body), err, bson.Raw(want))
		}
		doc, err := c.Node(via).GetFields(proofCtx(t), proofDB, "wide", id, []string{"name"})
		if err != nil {
			t.Fatalf("a projected get through %s: %v", via, err)
		}
		if !bytes.Equal(doc.Body, nameOnly) {
			t.Fatalf("through %s (answered by %s) the body is %v, want %v", via, doc.ServedBy, bson.Raw(doc.Body), bson.Raw(nameOnly))
		}
		full, err := c.Node(via).Get(proofCtx(t), proofDB, "wide", id)
		if err != nil || bson.Raw(full.Body).Lookup("bio").StringValue() == "" {
			t.Fatalf("an unprojected get through %s lost the bio: %v, %v", via, bson.Raw(full.Body), err)
		}
	}
	// A bad name is refused where the request enters, whatever node that is.
	if _, err := c.Node(outsider).GetFields(proofCtx(t), proofDB, "wide", id, []string{"nmae"}); err == nil {
		t.Fatalf("a projected get of an undeclared field through %s was answered", outsider)
	} else if msg := fmt.Sprint(err); !bytes.Contains([]byte(msg), []byte(`"nmae"`)) {
		t.Fatalf("the refusal %q does not name the field", msg)
	}
}
