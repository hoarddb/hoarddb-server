package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestSpreadOnOneNodeThenJoinCountsEveryDocument is test 13 (B2): 1,000
// inserts into a spread collection on a cluster of one node, whose router ran
// at the one-member door, so every document went to a part; a second node
// joins, the parts and perhaps the template move, and count() and a full
// find() through each node give every document once, never fewer.
func TestSpreadOnOneNodeThenJoinCountsEveryDocument(t *testing.T) {
	tc := newTestCluster(t, 1, 1)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: "app"})
	sc := &spreadCluster{testCluster: tc, collection: "User"}
	sc.createCollection(0, "User", nil)
	keys := sc.insertMany(0, "doc", 1000)
	if count, refused := sc.count(0); refused != "" || count != int64(len(keys)) {
		t.Fatalf("count() on one node is %d (refused %q), want %d", count, refused, len(keys))
	}
	joiner := joinFifth(t, tc, reserveNodePorts(t))
	through := map[string]func(transport.Opcode, any) *transport.Frame{
		"the first node": func(op transport.Opcode, request any) *transport.Frame { return tc.frame(0, op, request) },
		"the joiner":     func(op transport.Opcode, request any) *transport.Frame { return tc.frameOn(joiner, op, request) },
	}
	for door, send := range through {
		spreadEventually(t, 90*time.Second, fmt.Sprintf("count() through %s is %d", door, len(keys)), func() bool {
			var counted CountResponse
			reply := send(transport.OpCount, CountRequest{Database: "app", Bucket: "User"})
			if reply.Opcode == transport.OpError {
				return false
			}
			if err := bson.Unmarshal(reply.Payload, &counted); err != nil {
				t.Fatal(err)
			}
			if counted.Count > int64(len(keys)) {
				t.Fatalf("count() through %s is %d, more than the %d inserted", door, counted.Count, len(keys))
			}
			return counted.Count == int64(len(keys))
		})
		// The count above can reach its total through parts that have already finished moving
		// while another part of the same collection is still being copied; a find() sent at that
		// moment is refused as retryable ("is still copying it"), which is the product's answer
		// and not a short result, so it is retried like the count is. What must never happen is
		// an answer with fewer documents, and that is asserted below.
		var page ScanResponse
		spreadEventually(t, 90*time.Second, fmt.Sprintf("find() through %s is answered", door), func() bool {
			reply := send(transport.OpScan, ScanRequest{Database: "app", Bucket: "User", Limit: 5000})
			if reply.Opcode == transport.OpError {
				return false
			}
			page = ScanResponse{}
			if err := bson.Unmarshal(reply.Payload, &page); err != nil {
				t.Fatalf("find() through %s: %v", door, err)
			}
			return true
		})
		seen := map[string]bool{}
		for _, document := range page.Documents {
			id, _ := document.Lookup("_id_").StringValueOK()
			seen[id] = true
		}
		if len(seen) != len(keys) || page.Total != int64(len(keys)) {
			t.Errorf("find() through %s returned %d distinct documents of total %d, want %d", door, len(seen), page.Total, len(keys))
		}
	}
}

// movingPartDoor is the one-member door with one part answering as a part
// does while it moves mid fan-out: its read was sent on once already and
// found the part moved again (readMovedOnRetry).
type movingPartDoor struct {
	localSpreadDoor
	moving string
}

func (d movingPartDoor) readParts(ctx context.Context, database string, reads []partRead, held *resultBudget) []partAnswer {
	answers := d.localSpreadDoor.readParts(ctx, database, reads, held)
	for i, read := range reads {
		if read.part == d.moving {
			fireAnswerHooks(answers[i : i+1])
			answers[i] = partAnswer{err: readMovedOnRetry(database, read.part)}
		}
	}
	return answers
}

// TestFanOutRefusesWhenAPartMovesMidRead: a part whose read meets its take-over
// mid fan-out (moved on retry, or the hand-off) refuses the whole read,
// retryable and under the collection's name, with no document of the other
// parts returned: never a short count.
func TestFanOutRefusesWhenAPartMovesMidRead(t *testing.T) {
	n := newSpreadNode(t, nil)
	for i := 0; i < 50; i++ {
		if reply := n.send(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: fmt.Sprintf("m-%d", i), Data: map[string]interface{}{"N": int32(i)}}); reply.Opcode == transport.OpError {
			t.Fatal(refusalText(t, reply))
		}
	}
	template, err := n.s.loadSpreadTemplate("app", "User")
	if err != nil {
		t.Fatal(err)
	}
	router := spreadRouter{s: n.s, door: movingPartDoor{localSpreadDoor: localSpreadDoor{s: n.s}, moving: partName("User", spreadPartOf(t, "m-7"))}, t: template}
	for label, read := range fanOutRequests() {
		if read.op == transport.OpDumpStart {
			continue
		}
		reply := router.routeFanOut(context.Background(), transport.NewFrame(read.op, spreadMarshal(t, read.request)))
		if hook := reply.TakeWrittenHook(); hook != nil {
			hook()
		}
		text := refusalTextOrEmpty(t, reply)
		if reply.Opcode != transport.OpError || errorCodeOf(t, reply) != transport.ErrUnavailable || !strings.Contains(text, "moved to another node") ||
			!strings.Contains(text, `"User"`) || bsonNamesAPart(reply.Payload) {
			t.Errorf("%s with a part moving mid read answered %s %q, want the retry under the collection's name", label, reply.Opcode, reply.Payload)
		}
	}
	if held := n.s.queryInFlight(); held != 0 {
		t.Errorf("%d execute slots held", held)
	}
}
