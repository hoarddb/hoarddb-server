package harness_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// TestAForwardToAnUnreachableOwnerIsACodedUnavailable (BUG-0753): a write or read a survivor
// forwards to an owner that is gone failed with the peer layer's text ("dial tcp ...: connection
// refused", "peer is in backoff ...") and no code. The error must be a coded UNAVAILABLE that
// names the owner and quotes 'nodes'; a write must say its fate is unknown and that a retry
// with the same request id is safe, a read must say nothing was changed. The three shapes are
// the first failure (the connection the kill tore down or the refused dial) and the calls the
// peer layer then refuses while the owner is in backoff.
func TestAForwardToAnUnreachableOwnerIsACodedUnavailable(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{NoDetection: true, WCTimeout: 5 * time.Second})
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(proofCtx(t), "rf3", 3); err != nil {
		t.Fatalf("creating the RF 3 database: %v", err)
	}
	c.AwaitConverged(10 * time.Second)
	if _, err := leader.CreateBucket(proofCtx(t), "rf3", proofBucket, wire.Field{Name: "name", Type: "string"}); err != nil {
		t.Fatalf("creating the bucket: %v", err)
	}
	owner, _, err := c.Node(c.IDs()[0]).Owner(proofCtx(t), "rf3", proofBucket, "fwd-1")
	if err != nil {
		t.Fatalf("placing fwd-1: %v", err)
	}
	survivor := ""
	for _, id := range c.IDs() {
		if id != owner {
			survivor = id
			break
		}
	}
	if _, err := c.Node(survivor).Insert(proofCtx(t), "rf3", proofBucket, bson.D{{Key: "_id_", Value: "fwd-1"}, {Key: "name", Value: "Ada"}}); err != nil {
		t.Fatalf("warming insert through %s: %v", survivor, err)
	}
	c.Kill(owner)

	requireUnreachable := func(shape string, err error, wantFate bool) {
		t.Helper()
		var coded *cluster.Error
		if !errors.As(err, &coded) {
			t.Fatalf("%s: error %v is not a *cluster.Error", shape, err)
		}
		if coded.Code != cluster.CodeUnavailable {
			t.Fatalf("%s: code %v, want UNAVAILABLE: %v", shape, coded.Code, err)
		}
		for _, want := range []string{owner, "'nodes'"} {
			if !strings.Contains(coded.Message, want) {
				t.Errorf("%s: message %q does not contain %q", shape, coded.Message, want)
			}
		}
		fate := strings.Contains(coded.Message, "UNKNOWN") && strings.Contains(coded.Message, "same request id")
		if fate != wantFate {
			t.Errorf("%s: message %q states an unknown fate and the retry rule = %v, want %v", shape, coded.Message, fate, wantFate)
		}
		if !wantFate && !strings.Contains(coded.Message, "Nothing was changed") {
			t.Errorf("%s: a read must say nothing was changed: %q", shape, coded.Message)
		}
	}
	for _, shape := range []string{"first failure", "second call, owner in backoff", "third call"} {
		_, err := c.Node(survivor).Insert(proofCtx(t), "rf3", proofBucket, bson.D{{Key: "_id_", Value: "fwd-1"}, {Key: "name", Value: "Bea"}})
		requireUnreachable("write, "+shape, err, true)
		_, err = c.Node(survivor).Get(proofCtx(t), "rf3", proofBucket, "fwd-1")
		requireUnreachable("read, "+shape, err, false)
	}
}
