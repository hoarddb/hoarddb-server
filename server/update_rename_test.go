package server

import (
	"fmt"
	"maps"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// updateWithConcernAndRename is an update as a client sends it, with the write
// concern UpdateRequest does not carry.
type updateWithConcernAndRename struct {
	Database     string            `bson:"database"`
	Bucket       string            `bson:"bucket"`
	Key          string            `bson:"key"`
	Rename       map[string]string `bson:"rename"`
	WriteConcern string            `bson:"writeconcern"`
}

// renamedFieldsOn is the part of one node's own copy of key that the rename
// shapes touch, printed with its keys sorted, or why it could not be read.
func renamedFieldsOn(tc *testCluster, index int, key string) string {
	found, err := tc.nodes[index].server.core.Find(FindRequest{Database: testDatabase, Bucket: testBucket, Key: key})
	if err != nil {
		return fmt.Sprintf("read failed: %v", err)
	}
	if !found.Found {
		return "missing"
	}
	var stored map[string]interface{}
	if err := bson.Unmarshal(found.Data, &stored); err != nil {
		return fmt.Sprintf("undecodable: %v", err)
	}
	renamedFields := map[string]interface{}{}
	for _, field := range []string{"a", "b", "c", "t", "x"} {
		if fieldValue, present := stored[field]; present {
			renamedFields[field] = fieldValue
		}
	}
	return fmt.Sprint(renamedFields)
}

// TestOverlappingRenameLeavesEveryCopyIdentical checks, on a real cluster, that
// the owner and both replicas re-run one $rename on their own copy, and one
// replica then restarts and replays its log. Every copy must hold the same
// document. Before part 1 one key per shape was a coin flip (two outcomes,
// split roughly 85/15, per copy), so the test writes ten keys of each shape:
// before the fix all thirty agreeing on all three copies is below one in a
// million. Since part 2 each of these updates is refused, before any operator
// runs, on the owner and on every copy that applies the logged entry, so every
// copy holds the stored document unchanged.
func TestOverlappingRenameLeavesEveryCopyIdentical(t *testing.T) {
	const keysPerShape = 10
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	createTestHashBucket(t, tc)

	wantByKey := map[string]string{}
	for _, shape := range renameShapes[:3] {
		for n := range keysPerShape {
			key := fmt.Sprintf("%s-%d", strings.Fields(shape.name)[0], n)
			wantByKey[key] = fmt.Sprint(shape.want)
			tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: testBucket, Key: key,
				Data: maps.Clone(shape.stored), WriteConcern: "all"})
			refused := tc.frame(0, transport.OpUpdate, updateWithConcernAndRename{Database: testDatabase, Bucket: testBucket,
				Key: key, Rename: shape.req.Rename, WriteConcern: "all"})
			if refused.Opcode != transport.OpError || !strings.Contains(string(refused.Payload), "both touch") {
				t.Fatalf("the overlapping rename of %s was answered %s: %s; want E-16", key, refused.Opcode, refused.Payload)
			}
		}
	}

	replica := (ownerIndexOf(t, tc, testBucket) + 1) % len(tc.nodes)
	tc.restartNode(replica)

	divergence := func() []string {
		var wrongCopies []string
		for key, want := range wantByKey {
			for index := range tc.nodes {
				if got := renamedFieldsOn(tc, index, key); got != want {
					wrongCopies = append(wrongCopies, fmt.Sprintf("%s on node %d: %s, want %s", key, index+1, got, want))
				}
			}
		}
		return wrongCopies
	}
	if !eventually(func() bool { return len(divergence()) == 0 }) {
		wrongCopies := divergence()
		t.Fatalf("after restarting replica node %d, %d copies differ from the stored document:\n%s",
			replica+1, len(wrongCopies), strings.Join(wrongCopies, "\n"))
	}
}
