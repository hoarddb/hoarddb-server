package harness_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// getManyClient sends RING_GET_MANY frames to the ring handler of one node the way a connection
// would, without TLS. It keeps the bucket headers it described, as a driver keeps a bucket.
type getManyClient struct {
	t       *testing.T
	node    string
	handler transport.RequestHandler
	headers map[string]wire.RingAdminReply
}

func newGetManyClient(t *testing.T, c *harness.Cluster, node string, options ...wire.RingOption) *getManyClient {
	t.Helper()
	handler, err := wire.NewRingHandler(c.Node(node), batchNext, batchAllowAll{}, options...)
	if err != nil {
		t.Fatal(err)
	}
	return &getManyClient{t: t, node: node, handler: handler, headers: map[string]wire.RingAdminReply{}}
}

// header describes database.bucket once and keeps the answer.
func (g *getManyClient) header(database, bucket string) wire.RingAdminReply {
	g.t.Helper()
	key := database + "." + bucket
	if header, ok := g.headers[key]; ok {
		return header
	}
	payload, err := wire.EncodeRingAdmin(wire.RingAdminRequest{Cmd: wire.AdminDescribeBucket, Database: database, Bucket: bucket})
	if err != nil {
		g.t.Fatal(err)
	}
	reply, err := g.handler(proofCtx(g.t), transport.NewFrameWithID(transport.OpRingAdmin, [16]byte{0x61}, payload))
	if err != nil || reply.Opcode != transport.OpRingReply {
		g.t.Fatalf("describe_bucket %s on %s: %v %v", key, g.node, reply, err)
	}
	header, err := wire.DecodeRingAdminReply(reply.Payload)
	if err != nil {
		g.t.Fatal(err)
	}
	g.headers[key] = header
	return header
}

// read is the get of id in database.bucket; local asks a ready copy on this node to answer.
func (g *getManyClient) read(database, bucket, id string, local bool) wire.RingRead {
	g.t.Helper()
	header := g.header(database, bucket)
	encoded, err := store.EncodeComponent(id)
	if err != nil {
		g.t.Fatal(err)
	}
	return wire.RingRead{
		Class: wire.RingClassGet, BucketID: header.BucketID, DeclVersion: header.DeclVersion,
		SchemaVersionSeen: header.SchemaVersionSeen, EncID: encoded, Options: wire.RingReadOptions{Local: local},
	}
}

// reads is the get of every id of proofDB.proofBucket.
func (g *getManyClient) reads(ids ...string) []wire.RingRead {
	g.t.Helper()
	out := make([]wire.RingRead, len(ids))
	for i, id := range ids {
		out[i] = g.read(proofDB, proofBucket, id, false)
	}
	return out
}

// try sends one frame and returns its RING_REPLY, or the error the frame was refused with. It
// does not fail the test, so a goroutine may call it.
func (g *getManyClient) try(ctx context.Context, prologue wire.RingPrologue, reads []wire.RingRead) (wire.RingReply, error) {
	payload, err := wire.EncodeRingGetMany(wire.RingGetManyRequest{Prologue: prologue, Reads: reads})
	if err != nil {
		return wire.RingReply{}, fmt.Errorf("encoding the frame: %w", err)
	}
	frame, err := g.handler(ctx, transport.NewFrameWithID(transport.OpRingGetMany, [16]byte{0x6D}, payload))
	if err != nil {
		return wire.RingReply{}, fmt.Errorf("the handler: %w", err)
	}
	if frame.Opcode != transport.OpRingReply {
		refusal, err := wire.DecodeRingError(frame.Payload)
		if err != nil {
			return wire.RingReply{}, fmt.Errorf("opcode %v with an undecodable payload: %w", frame.Opcode, err)
		}
		return wire.RingReply{}, fmt.Errorf("the frame was refused %s: %s", refusal.Code, refusal.Message)
	}
	reply, err := wire.DecodeRingReply(frame.Payload)
	if err != nil {
		return wire.RingReply{}, fmt.Errorf("decoding the reply: %w", err)
	}
	if len(reply.Answers) != len(reads) {
		return wire.RingReply{}, fmt.Errorf("%d answers for %d keys", len(reply.Answers), len(reads))
	}
	return reply, nil
}

// send is try that fails the test on a frame-level refusal.
func (g *getManyClient) send(prologue wire.RingPrologue, reads []wire.RingRead) wire.RingReply {
	g.t.Helper()
	reply, err := g.try(proofCtx(g.t), prologue, reads)
	if err != nil {
		g.t.Fatalf("get-many through %s: %v", g.node, err)
	}
	return reply
}

// docID is the _id_ of a found answer, "" for found:false, and "!" plus the code for an error.
func docID(answer wire.RingAnswer) string {
	switch {
	case answer.Error != nil:
		return "!" + answer.Error.Code.String()
	case answer.Reply == nil || answer.Reply.Found == nil:
		return "?"
	case !*answer.Reply.Found:
		return ""
	case len(answer.Reply.Elements) != 1:
		return "?"
	}
	return bson.Raw(answer.Reply.Elements[0]).Lookup("_id_").StringValue()
}

// docName is the name field of a found answer.
func docName(answer wire.RingAnswer) string {
	if answer.Reply == nil || len(answer.Reply.Elements) != 1 {
		return ""
	}
	value, err := bson.Raw(answer.Reply.Elements[0]).LookupErr("name")
	if err != nil {
		return ""
	}
	return value.StringValue()
}

// ownersOf places every id at the committed table and returns, per id, its owner and its
// copy (the second node of its set, "" below RF 2).
func ownersOf(t *testing.T, c *harness.Cluster, database string, ids []string) (owners, copies map[string]string) {
	t.Helper()
	owners, copies = map[string]string{}, map[string]string{}
	for _, id := range ids {
		owner, set, err := c.Node(c.IDs()[0]).Owner(proofCtx(t), database, proofBucket, id)
		if err != nil {
			t.Fatalf("placing %s: %v", id, err)
		}
		owners[id] = owner
		if len(set) > 1 {
			copies[id] = set[1]
		}
	}
	return owners, copies
}

// idsByOwner groups ids by owner, keeping their order.
func idsByOwner(ids []string, owners map[string]string) map[string][]string {
	grouped := map[string][]string{}
	for _, id := range ids {
		grouped[owners[id]] = append(grouped[owners[id]], id)
	}
	return grouped
}

// getManyInsert writes id with the name "<database>/<id>" (so the same id in two databases is told apart) into database.bucket through via and retries
// while the cluster settles (a lapsed lease, a majority not yet seen: BUG-0808). An insert that
// was refused was not applied, and the id is fixed, so the retry cannot duplicate it.
func getManyInsert(t *testing.T, c *harness.Cluster, via, database, id string) {
	t.Helper()
	var last error
	for range 100 {
		_, last = c.Node(via).Insert(proofCtx(t), database, proofBucket, bson.D{{Key: "_id_", Value: id}, {Key: "name", Value: database + "/" + id}})
		if last == nil || strings.Contains(last.Error(), "already exists") {
			return
		}
		c.AwaitLeases(30 * time.Second)
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("insert of %s into %s through %s: %v (background: %v)", id, database, via, last, c.BackgroundErrors())
}

// awaitOnCopies waits until every id is held by its owner and its copy, so a read that asks a
// copy to answer, or an owner that is killed next, loses nothing.
func awaitOnCopies(t *testing.T, c *harness.Cluster, database string, ids []string) {
	t.Helper()
	owners, copies := ownersOf(t, c, database, ids)
	c.Await("every document on its owner and its copy", 30*time.Second, func() bool {
		for _, id := range ids {
			for _, node := range []string{owners[id], copies[id]} {
				if node == "" {
					continue
				}
				if _, found, err := c.Node(node).LocalDocument(proofCtx(t), database, proofBucket, id); err != nil || !found {
					return false
				}
			}
		}
		return true
	})
}

// ringCode is the code of an error answer, or "" for a reply.
func ringCode(answer wire.RingAnswer) cluster.Code {
	if answer.Error == nil {
		return 0
	}
	return answer.Error.Code
}

// asCoded reports whether err carries a *cluster.Error and sets it.
func asCoded(err error, target **cluster.Error) bool { return errors.As(err, target) }

// sendSettled is send for a frame that should answer every key: a lease or a majority the
// cluster has not confirmed yet (BUG-0808) answers a key LEASE_LAPSED or NO_QUORUM, as it does a
// lone read, so those two codes, and only those, are retried for up to 30 seconds.
func (g *getManyClient) sendSettled(prologue wire.RingPrologue, reads []wire.RingRead) wire.RingReply {
	g.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		reply := g.send(prologue, reads)
		transient := false
		for _, answer := range reply.Answers {
			if code := ringCode(answer); code == cluster.CodeLeaseLapsed || code == cluster.CodeNoQuorum {
				transient = true
			}
		}
		if !transient || time.Now().After(deadline) {
			return reply
		}
		time.Sleep(200 * time.Millisecond)
	}
}
