package harness_test

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

func nearestVector(values ...float32) []byte {
	out := make([]byte, 4*len(values))
	for i, value := range values {
		binary.LittleEndian.PutUint32(out[4*i:], math.Float32bits(value))
	}
	return out
}

// TestANearestThroughANonOwnerAnswersTheOwnersAnswer: nearest of a store field is forwarded to
// the owner like count, list and peek, so every node answers the same elements, keys, scores
// and counts. Nine documents, so every node owns some and is a non-owner of others.
func TestANearestThroughANonOwnerAnswersTheOwnersAnswer(t *testing.T) {
	c := bug0766Cluster(t, harness.Options{})
	for i := 0; i < 9; i++ {
		id := fmt.Sprintf("vec-%d", i)
		owner, _ := bug0766Fill(t, c, id)
		node := c.Node(owner)
		for key, values := range map[string][]float32{"a": {1, 0}, "b": {0, 1}, "c": {-1, 0}, "d": {0.5, 0.5}} {
			element := bson.D{{Key: "item", Value: key}, {Key: "Vector", Value: primitive.Binary{Subtype: 0x00, Data: nearestVector(values...)}}}
			if err := node.Put(proofCtx(t), wire.PutRequest{StoreTarget: bug0766Target(id, "Table"), Key: key, Element: element}); err != nil {
				t.Fatalf("putting %s in %s: %v", key, id, err)
			}
		}
		request := wire.NearestRequest{StoreTarget: bug0766Target(id, "Table"), Metric: wire.RingMetricCosine, K: 2, VectorField: "Vector", Vector: nearestVector(1, 0)}
		want, err := node.Nearest(proofCtx(t), request)
		// The two entries bug0766Fill put hold no Vector, so they are skipped, not scored.
		if err != nil || len(want.Keys) != 2 || want.Keys[0] != "a" || want.Keys[1] != "d" || want.Scanned != 4 || want.Skipped != 2 {
			t.Fatalf("%s at its owner %s: %+v, %v, want a then d, 4 scored, 2 skipped", id, owner, want, err)
		}
		for _, through := range c.IDs() {
			if through == owner {
				continue
			}
			got, err := c.Node(through).Nearest(proofCtx(t), request)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Errorf("%s (owner %s) nearest through %s: %+v, %v, want the owner's %+v", id, owner, through, got, err, want)
			}
		}
	}
}

// TestANearestThroughASurvivorOfADeadOwnerIsTheCodedUnavailableListGives: the nearest is
// forwarded, the owner is gone, and the refusal is the coded UNAVAILABLE that names the owner
// and says nothing was changed, as for count; never an internal error.
func TestANearestThroughASurvivorOfADeadOwnerIsTheCodedUnavailableListGives(t *testing.T) {
	c := bug0766Cluster(t, harness.Options{NoDetection: true, WCTimeout: 5 * time.Second})
	owner, _ := bug0766Fill(t, c, "dead-vec")
	survivor := ""
	for _, id := range c.IDs() {
		if id != owner {
			survivor = id
			break
		}
	}
	c.Kill(owner)
	request := wire.NearestRequest{StoreTarget: bug0766Target("dead-vec", "Table"), Metric: wire.RingMetricCosine, K: 2, VectorField: "Vector", Vector: nearestVector(1, 0)}
	for attempt := 0; attempt < 3; attempt++ {
		_, nearestErr := c.Node(survivor).Nearest(proofCtx(t), request)
		_, listErr := c.Node(survivor).ListField(proofCtx(t), wire.FieldListRequest{StoreTarget: bug0766Target("dead-vec", "Table")})
		var nearest, list *cluster.Error
		if !errors.As(listErr, &list) || list.Code != cluster.CodeUnavailable {
			t.Fatalf("attempt %d: the list twin gave %v, want a coded UNAVAILABLE", attempt, listErr)
		}
		if !errors.As(nearestErr, &nearest) || nearest.Code != cluster.CodeUnavailable {
			t.Fatalf("attempt %d: nearest gave %v, want the coded UNAVAILABLE list gives", attempt, nearestErr)
		}
		for _, want := range []string{owner, "'nodes'", "Nothing was changed"} {
			if !strings.Contains(nearest.Message, want) {
				t.Errorf("attempt %d: %q does not contain %q", attempt, nearest.Message, want)
			}
		}
	}
}
