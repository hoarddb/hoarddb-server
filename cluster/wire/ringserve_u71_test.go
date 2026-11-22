package wire

import (
	"bytes"
	"context"
	"encoding/binary"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// u71KV counts the snapshots a reader opens, so a refusal can be shown to come before any read.
type u71KV struct {
	storage.KV
	snapshots atomic.Int64
}

func (k *u71KV) NewSnapshot() storage.KVSnapshot {
	k.snapshots.Add(1)
	return k.KV.NewSnapshot()
}

// leaseGet is a lease_get of the document id on the named field.
func (e *u66Env) leaseGet(t *testing.T, id, field string, lease []byte) RingKeyedRequest {
	t.Helper()
	req := e.read(t, RingClassLeaseGet, id, field, 0)
	copy(req.Read.Lease[:], lease)
	return req
}

func u71Push(t *testing.T, env *u66Env, c *u66Conn, id, field string, labels ...string) {
	t.Helper()
	for _, label := range labels {
		arg := u66Arg(t, func() ([]byte, error) { return encodeElementArg(u66Item(t, label)) })
		u66Reply(t, c.keyed(env.verb(t, id, field, cluster.VerbPush, arg)))
	}
}

// u71Claim claims count messages under the request id and returns the first reply and the
// payload to send again.
func u71Claim(t *testing.T, env *u66Env, c *u66Conn, id, field string, count uint16) (RingReply, [16]byte, []byte) {
	t.Helper()
	claim := env.header()
	claim.Class, claim.EncID, claim.Count = cluster.OpClaim, u66EncID(t, id), count
	claim.Fields = []cluster.FieldOp{{FieldID: env.fieldID(t, field)}}
	payload, err := EncodeRingKeyed(RingKeyedRequest{Op: claim})
	if err != nil {
		t.Fatal(err)
	}
	requestID := u66ID()
	return u66Reply(t, c.send(transport.OpRingKeyed, requestID, payload)), requestID, payload
}

func TestU71LeaseGetReturnsWhatALostClaimReplyLeased(t *testing.T) {
	for _, tc := range []struct {
		field string
		want  []string
	}{{"Queue", []string{"a", "b"}}, {"Stack", []string{"d", "c"}}} {
		env := u66Start(t)
		c := env.login(t)
		u71Push(t, env, c, "b1", tc.field, "a", "b", "c", "d")
		first, requestID, payload := u71Claim(t, env, c, "b1", tc.field, 2)
		if len(first.Elements) != 2 || len(first.Lease) != 16 {
			t.Fatalf("%s: claim reply %+v", tc.field, first)
		}
		// The reply is lost: the client sends the same request id again.
		retry := u66Reply(t, c.send(transport.OpRingKeyed, requestID, payload))
		if !retry.Deduped || len(retry.Elements) != 0 || !bytes.Equal(retry.Lease, first.Lease) {
			t.Fatalf("%s: the retry %+v, want the same lease, deduped and no elements", tc.field, retry)
		}
		got := u66Reply(t, c.keyed(env.leaseGet(t, "b1", tc.field, retry.Lease)))
		if got.Found == nil || !*got.Found || !bytes.Equal(got.Lease, first.Lease) || len(got.Elements) != 2 ||
			!bytes.Equal(got.Elements[0], first.Elements[0]) || !bytes.Equal(got.Elements[1], first.Elements[1]) {
			t.Fatalf("%s: lease_get %+v, want the claimed elements %x", tc.field, got, first.Elements)
		}
		if got.WriteOutcome != "" || got.Position != nil {
			t.Errorf("%s: a read carries a write outcome or position: %+v", tc.field, got)
		}
	}
}

func TestU71LeaseGetTakesNoPositionAndWritesNoRecord(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	u71Push(t, env, c, "b1", "Queue", "a")
	first, _, _ := u71Claim(t, env, c, "b1", "Queue", 1)
	before := u69Pair(env, "b1", t)
	for range 3 {
		u66Reply(t, c.keyed(env.leaseGet(t, "b1", "Queue", first.Lease)))
	}
	if after := u69Pair(env, "b1", t); u69Moved(before, after) {
		t.Fatalf("a lease_get moved the pair: %+v then %+v", before, after)
	}
}

func TestU71LeaseGetIsNotFoundOnceTheLeaseIsAckedReleasedOrExpired(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	u71Push(t, env, c, "b1", "Queue", "a", "b", "c")
	acked, _, _ := u71Claim(t, env, c, "b1", "Queue", 1)
	released, _, _ := u71Claim(t, env, c, "b1", "Queue", 1)
	lapsing, _, _ := u71Claim(t, env, c, "b1", "Queue", 1)
	lease := func(class byte, id []byte) {
		op := env.header()
		op.Class, op.EncID = class, u66EncID(t, "b1")
		op.Fields = []cluster.FieldOp{{FieldID: env.fieldID(t, "Queue")}}
		copy(op.LeaseID[:], id)
		u66Reply(t, c.keyed(RingKeyedRequest{Op: op}))
	}
	notFound := func(name string, id []byte) {
		t.Helper()
		got := u66Reply(t, c.keyed(env.leaseGet(t, "b1", "Queue", id)))
		if got.Found == nil || *got.Found || len(got.Elements) != 0 {
			t.Errorf("%s: lease_get %+v, want found=false and no elements", name, got)
		}
	}
	for _, held := range [][]byte{acked.Lease, released.Lease, lapsing.Lease} {
		if got := u66Reply(t, c.keyed(env.leaseGet(t, "b1", "Queue", held))); got.Found == nil || !*got.Found || len(got.Elements) != 1 {
			t.Fatalf("a live lease is not found: %+v", got)
		}
	}
	lease(cluster.OpAck, acked.Lease)
	lease(cluster.OpRelease, released.Lease)
	notFound("acked", acked.Lease)
	notFound("released", released.Lease)
	notFound("unknown", bytes.Repeat([]byte{7}, 16))
	// Two hours on, the lease that nobody touched is past its deadline though no write has
	// returned its messages yet.
	env.node.reader.cfg.WallMillis = func() int64 { return time.Now().Add(2 * time.Hour).UnixMilli() }
	notFound("expired", lapsing.Lease)
}

func TestU71LeaseGetRefusesAWrongFieldOrStoreTypeBeforeReadingAnything(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	u71Push(t, env, c, "b1", "Queue", "a")
	first, _, _ := u71Claim(t, env, c, "b1", "Queue", 1)
	counted := &u71KV{KV: env.node.reader.cfg.KV}
	env.node.reader.cfg.KV = counted
	for name, tc := range map[string]struct {
		field string
		code  cluster.Code
		said  string
	}{
		"a scalar field":   {"Name", cluster.CodeFieldType, "Name"},
		"a hash store":     {"Table", cluster.CodeInvalidArgument, "fifoStore or lifoStore"},
		"a heap store":     {"Ranked", cluster.CodeInvalidArgument, "fifoStore or lifoStore"},
		"a btree store":    {"Tree", cluster.CodeInvalidArgument, "fifoStore or lifoStore"},
		"a blob store":     {"Files", cluster.CodeInvalidArgument, "fifoStore or lifoStore"},
		"an unknown field": {"", cluster.CodeFieldType, "#999"},
	} {
		req := env.leaseGet(t, "b1", tc.field, first.Lease)
		if tc.field == "" {
			req.Read.FieldID = 999
		}
		refusal := u66Refusal(t, c.keyed(req), tc.code)
		if !strings.Contains(refusal.Message, tc.said) {
			t.Errorf("%s: refusal %q does not say %q", name, refusal.Message, tc.said)
		}
		if reads := counted.snapshots.Load(); reads != 0 {
			t.Fatalf("%s: %d snapshots were opened before the refusal", name, reads)
		}
	}
}

func TestU71LeaseGetOfAnotherBucketsOrDocumentsLeaseLeaksNothing(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	ctx := context.Background()
	if _, err := env.node.CreateBucket(ctx, u30iDB, "Box2", Field{Name: "Name", Type: "string"}, Field{Name: "Queue", Type: "fifoStore"}); err != nil {
		t.Fatal(err)
	}
	if _, err := env.node.Insert(ctx, u30iDB, "Box2", u71Doc("b1")); err != nil {
		t.Fatal(err)
	}
	if _, err := env.node.Insert(ctx, u30iDB, "Box", u71Doc("b2")); err != nil {
		t.Fatal(err)
	}
	other, err := env.node.bucket(ctx, u30iDB, "Box2")
	if err != nil {
		t.Fatal(err)
	}
	u71Push(t, env, c, "b1", "Queue", "secret")
	first, _, _ := u71Claim(t, env, c, "b1", "Queue", 1)

	// The same lease id, document id and field id, asked of the other bucket.
	entry, ok := entryNamed(other.table, "Queue")
	if !ok {
		t.Fatal("no Queue in Box2")
	}
	read := RingRead{Class: RingClassLeaseGet, BucketID: other.entry.Decl.BucketID, DeclVersion: other.version, SchemaVersionSeen: other.entry.Seen,
		EncID: u66EncID(t, "b1"), FieldID: entry.ID}
	copy(read.Lease[:], first.Lease)
	got := u66Reply(t, c.keyed(RingKeyedRequest{Read: &read}))
	if got.Found == nil || *got.Found || len(got.Elements) != 0 || len(got.Lease) != 0 {
		t.Fatalf("another bucket's lease: %+v", got)
	}
	// Another document of the same bucket.
	got = u66Reply(t, c.keyed(env.leaseGet(t, "b2", "Queue", first.Lease)))
	if got.Found == nil || *got.Found || len(got.Elements) != 0 || len(got.Lease) != 0 {
		t.Fatalf("another document's lease: %+v", got)
	}
	// A document that does not exist is an answer too.
	got = u66Reply(t, c.keyed(env.leaseGet(t, "nobody", "Queue", first.Lease)))
	if got.Found == nil || *got.Found {
		t.Fatalf("a missing document: %+v", got)
	}
}

func u71Doc(id string) bson.D { return bson.D{{Key: "_id_", Value: id}, {Key: "Name", Value: id}} }

func TestU71AMalformedLeaseIDIsRefusedWithTheFix(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	u71Push(t, env, c, "b1", "Queue", "a")
	first, _, _ := u71Claim(t, env, c, "b1", "Queue", 1)
	valid, err := EncodeRingKeyed(env.leaseGet(t, "b1", "Queue", first.Lease))
	if err != nil {
		t.Fatal(err)
	}
	for name, payload := range map[string][]byte{
		"a lease id of 15 bytes": valid[:len(valid)-1],
		"a lease id of 4 bytes":  valid[:len(valid)-12],
		"no lease id":            valid[:len(valid)-16],
		"a lease id of 17 bytes": append(bytes.Clone(valid), 1),
	} {
		refusal := u66Refusal(t, c.send(transport.OpRingKeyed, u66ID(), payload), cluster.CodeInvalidArgument)
		if !strings.Contains(refusal.Message, "16-byte lease id") || !strings.Contains(refusal.Message, "claim") {
			t.Errorf("%s: refusal %q does not name the 16-byte lease id and where to get it", name, refusal.Message)
		}
	}
	zero := env.leaseGet(t, "b1", "Queue", nil)
	if _, err := EncodeRingKeyed(zero); err == nil || !strings.Contains(err.Error(), "lease") {
		t.Errorf("a zero lease id encoded: %v", err)
	}
	zeroed := bytes.Clone(valid)
	clear(zeroed[len(zeroed)-16:])
	refusal := u66Refusal(t, c.send(transport.OpRingKeyed, u66ID(), zeroed), cluster.CodeInvalidArgument)
	if !strings.Contains(refusal.Message, "nothing to lease") {
		t.Errorf("a zero lease id: %q", refusal.Message)
	}
	// The connection is fine after the refusals.
	if got := u66Reply(t, c.keyed(env.leaseGet(t, "b1", "Queue", first.Lease))); got.Found == nil || !*got.Found {
		t.Fatalf("after the refusals: %+v", got)
	}
}

func TestU71OnlyALeaseGetCarriesALeaseID(t *testing.T) {
	env := u66Start(t)
	for _, class := range []byte{RingClassGet, RingClassPeek, RingClassStoreList, RingClassStoreCount} {
		req := env.read(t, class, "b1", "Queue", 5)
		if class == RingClassGet {
			req = env.read(t, class, "b1", "", 0)
		}
		if class == RingClassStoreCount {
			req = env.read(t, class, "b1", "Queue", 0)
		}
		req.Read.Lease[3] = 1
		if _, err := EncodeRingKeyed(req); err == nil || !strings.Contains(err.Error(), "lease") {
			t.Errorf("class 0x%02x with a lease id: %v", class, err)
		}
	}
	// A lease_get takes no count and no cursor.
	req := env.leaseGet(t, "b1", "Queue", bytes.Repeat([]byte{1}, 16))
	req.Read.Limit = 5
	if _, err := EncodeRingKeyed(req); err == nil {
		t.Error("a lease_get with a count encoded")
	}
	req = env.leaseGet(t, "b1", "Queue", bytes.Repeat([]byte{1}, 16))
	req.Read.After = []byte{1}
	if _, err := EncodeRingKeyed(req); err == nil {
		t.Error("a lease_get with a cursor encoded")
	}
}

func TestU71ALeaseGetRoundTripsAndPlacesTheLeaseAfterTheFieldID(t *testing.T) {
	lease := bytes.Repeat([]byte{0xAB}, 16)
	read := RingRead{Class: RingClassLeaseGet, BucketID: 41, DeclVersion: 7, SchemaVersionSeen: 7, EncID: u65EncID, FieldID: 4}
	copy(read.Lease[:], lease)
	raw, err := EncodeRingKeyed(RingKeyedRequest{Prologue: u65Prologue(), Read: &read})
	if err != nil {
		t.Fatal(err)
	}
	// ... field id (2 bytes BE) then the 16 lease bytes, and nothing after.
	tail := binary.BigEndian.AppendUint16(nil, 4)
	tail = append(tail, lease...)
	if !bytes.HasSuffix(raw, tail) || raw[RingPrologueLen] != 0x87 {
		t.Fatalf("lease_get encodes as %x", raw)
	}
	again, err := DecodeRingKeyed(raw)
	if err != nil || again.Read == nil || !bytes.Equal(again.Read.EncID, read.EncID) || again.Read.Lease != read.Lease || again.Read.FieldID != 4 {
		t.Fatalf("decoded %+v, %v", again.Read, err)
	}
}

func TestU71TheLeaseReplyIsBoundedLikeAPeek(t *testing.T) {
	if leaseReplyBytes != peekReplyBytes || leaseReplyBytes >= maxSubScanBody {
		t.Fatalf("a lease reply may carry %d bytes, a peek %d, a frame %d", leaseReplyBytes, peekReplyBytes, maxSubScanBody)
	}
}

func TestU71NodeLeaseElementsRecoversADedupedClaim(t *testing.T) {
	node, _ := u63Fresh(t)
	ctx := context.Background()
	u62Push(t, node, "Queue", "a", "b", "c")
	var requestID [16]byte
	requestID[0] = 71
	first, err := node.Claim(ctx, ClaimRequest{StoreTarget: u62Target("Queue"), Count: 2}, WithRequestID(requestID))
	if err != nil {
		t.Fatal(err)
	}
	retry, err := node.Claim(ctx, ClaimRequest{StoreTarget: u62Target("Queue"), Count: 2}, WithRequestID(requestID))
	if err != nil || !retry.Deduped || retry.ID != first.ID || len(retry.Elements) != 0 {
		t.Fatalf("retry %+v, %v: want the same lease id, deduped, without elements", retry, err)
	}
	got, err := node.LeaseElements(ctx, LeaseElementsRequest{StoreTarget: u62Target("Queue"), Lease: retry.ID})
	if err != nil || !got.Found || got.Until != first.Until || len(got.Elements) != 2 ||
		!bytes.Equal(got.Elements[0], first.Elements[0]) || !bytes.Equal(got.Elements[1], first.Elements[1]) {
		t.Fatalf("LeaseElements %+v, %v; the claim leased %x until %d", got, err, first.Elements, first.Until)
	}
	// A hash store has no leases: refused by name, before anything is read.
	if _, err := node.LeaseElements(ctx, LeaseElementsRequest{StoreTarget: u62Target("Table"), Lease: first.ID}); err == nil ||
		!strings.Contains(err.Error(), "fifoStore or lifoStore") {
		t.Fatalf("a hash store: %v", err)
	}
}
