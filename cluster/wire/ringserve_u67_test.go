package wire

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// u67Lease builds a release or an extend of the lease on b1's Queue; length is the extend's
// lease length (the Until member of the request, build SPEC-GAP U67-1).
func (e *u66Env) lease(t *testing.T, class byte, field string, lease []byte, length int64) RingKeyedRequest {
	op := e.header()
	op.Class, op.EncID = class, u66EncID(t, "b1")
	if class == cluster.OpExtend {
		op.Until = length
	}
	op.Fields = []cluster.FieldOp{{FieldID: e.fieldID(t, field)}}
	copy(op.LeaseID[:], lease)
	return RingKeyedRequest{Op: op}
}

func (e *u66Env) pushQueue(t *testing.T, c *u66Conn, labels ...string) {
	t.Helper()
	for _, label := range labels {
		arg := u66Arg(t, func() ([]byte, error) { return encodeElementArg(u66Item(t, label)) })
		u66Reply(t, c.keyed(e.verb(t, "b1", "Queue", cluster.VerbPush, arg)))
	}
}

func (e *u66Env) claimQueue(t *testing.T, c *u66Conn, count uint16) RingReply {
	t.Helper()
	claim := e.header()
	claim.Class, claim.EncID, claim.Count = cluster.OpClaim, u66EncID(t, "b1"), count
	claim.Fields = []cluster.FieldOp{{FieldID: e.fieldID(t, "Queue")}}
	return u66Reply(t, c.keyed(RingKeyedRequest{Op: claim}))
}

func (e *u66Env) queueLen(t *testing.T, c *u66Conn) int {
	t.Helper()
	return len(u66Reply(t, c.keyed(e.read(t, RingClassPeek, "b1", "Queue", 10))).Elements)
}

// TestU67ReleaseAndExtendOverTLS: [GUARD the ring handler's release and extend cases].
// found says whether the lease was live: the reply has no member for the state.
func TestU67ReleaseAndExtendOverTLS(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	env.pushQueue(t, c, "a", "b")
	claimed := env.claimQueue(t, c, 1)
	extended := u66Reply(t, c.keyed(env.lease(t, cluster.OpExtend, "Queue", claimed.Lease, 60_000)))
	if extended.WriteOutcome != "applied" || extended.Position == nil || extended.Found == nil || !*extended.Found {
		t.Fatalf("extend reply %+v, want applied and found", extended)
	}
	released := u66Reply(t, c.keyed(env.lease(t, cluster.OpRelease, "Queue", claimed.Lease, 0)))
	if released.WriteOutcome != "applied" || released.Found == nil || !*released.Found {
		t.Fatalf("release reply %+v, want applied and found", released)
	}
	if got := env.queueLen(t, c); got != 2 {
		t.Fatalf("after the release the queue holds %d, want both back", got)
	}
	gone := u66Reply(t, c.keyed(env.lease(t, cluster.OpRelease, "Queue", claimed.Lease, 0)))
	if gone.WriteOutcome != "applied" || gone.Found == nil || *gone.Found {
		t.Fatalf("release of a gone lease %+v, want applied and found=false", gone)
	}
	late := u66Reply(t, c.keyed(env.lease(t, cluster.OpExtend, "Queue", claimed.Lease, 60_000)))
	if late.Found == nil || *late.Found {
		t.Fatalf("extend of a gone lease %+v, want found=false", late)
	}
}

// TestU67ARetriedReleaseAndExtendAreDeduped: [GUARD the request id reaches the writer].
func TestU67ARetriedReleaseAndExtendAreDeduped(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	env.pushQueue(t, c, "a")
	claimed := env.claimQueue(t, c, 1)
	// In this order: a release first would leave the extend nothing live to extend.
	for _, tc := range []struct {
		name string
		req  RingKeyedRequest
	}{
		{"extend", env.lease(t, cluster.OpExtend, "Queue", claimed.Lease, 60_000)},
		{"release", env.lease(t, cluster.OpRelease, "Queue", claimed.Lease, 0)},
	} {
		name := tc.name
		payload, err := EncodeRingKeyed(tc.req)
		if err != nil {
			t.Fatal(err)
		}
		id := u66ID()
		first := u66Reply(t, c.send(transport.OpRingKeyed, id, payload))
		second := u66Reply(t, c.send(transport.OpRingKeyed, id, payload))
		if first.Deduped || !second.Deduped || *first.Position != *second.Position || second.Found == nil || !*second.Found {
			t.Errorf("%s: first %+v, retry %+v; want the retry deduped at the same position with the same found", name, first, second)
		}
	}
	if got := env.queueLen(t, c); got != 1 {
		t.Fatalf("the queue holds %d after the lease was released once, want 1", got)
	}
}

// TestU67RefusedReleaseAndExtendDoNotMoveThePair: [GUARD preconditions before a position].
func TestU67RefusedReleaseAndExtendDoNotMoveThePair(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	env.pushQueue(t, c, "a")
	claimed := env.claimQueue(t, c, 1)
	before := u63Position(env.node)
	for name, length := range map[string]int64{"zero": 0, "too short": store.MinLeaseMs - 1, "too long": store.MaxLeaseMs + 1, "absolute": 1 << 40} {
		refusal := u66Refusal(t, c.keyed(env.lease(t, cluster.OpExtend, "Queue", claimed.Lease, length)), cluster.CodeInvalidArgument)
		if !strings.Contains(refusal.Message, "Nothing was changed") {
			t.Errorf("%s: %q", name, refusal.Message)
		}
	}
	for _, class := range []byte{cluster.OpRelease, cluster.OpExtend} {
		refusal := u66Refusal(t, c.keyed(env.lease(t, class, "Table", claimed.Lease, 5000)), cluster.CodeInvalidArgument)
		if !strings.Contains(refusal.Message, "hashStore") || !strings.Contains(refusal.Message, "fifoStore or lifoStore") {
			t.Errorf("class 0x%02x on a hashStore: %q", class, refusal.Message)
		}
	}
	if after := u63Position(env.node); after != before {
		t.Fatalf("the pair moved from %+v to %+v on refused requests", before, after)
	}
	env.pushQueue(t, c, "after")
	if next := env.claimQueue(t, c, 1); len(next.Lease) != 16 {
		t.Fatalf("the pair is not writable after the refusals: %+v", next)
	}
}

func (c *u66Conn) list(t *testing.T, req RingListRequest) *transport.Frame {
	t.Helper()
	payload, err := EncodeRingList(req)
	if err != nil {
		t.Fatal(err)
	}
	return c.send(transport.OpRingList, u66ID(), payload)
}

func (c *u66Conn) count(t *testing.T, req RingCountRequest) *transport.Frame {
	t.Helper()
	payload, err := EncodeRingCount(req)
	if err != nil {
		t.Fatal(err)
	}
	return c.send(transport.OpRingCount, u66ID(), payload)
}

// u67BoxDocs adds b2..b7 to Box, named after their parity, so b1..b7 hold 4 odd and 3 even.
func u67BoxDocs(t *testing.T, env *u66Env) {
	t.Helper()
	for i := 2; i <= 7; i++ {
		name := "odd"
		if i%2 == 0 {
			name = "even"
		}
		doc := bson.D{{Key: "_id_", Value: fmt.Sprintf("b%d", i)}, {Key: "Name", Value: name}}
		if _, err := env.node.Insert(context.Background(), u30iDB, "Box", doc); err != nil {
			t.Fatal(err)
		}
	}
}

func u67EntryIds(t *testing.T, blob []byte) []string {
	t.Helper()
	entries, err := cluster.DecodeEntries(blob)
	if err != nil {
		t.Fatal(err)
	}
	return u67Ids(t, entries)
}

// TestU67ListOverTLSPagesWithACursor: [GUARD the list case of NewRingHandler and the
// entries, more and cursor members].
func TestU67ListOverTLSPagesWithACursor(t *testing.T) {
	env := u66Start(t)
	u67BoxDocs(t, env)
	c := env.login(t)
	var seen []string
	var cursor []byte
	for pages := 0; ; pages++ {
		if pages > 10 {
			t.Fatal("the cursor never ended")
		}
		limit := int32(3)
		reply := u66Reply(t, c.list(t, RingListRequest{Database: u30iDB, Bucket: "Box", Limit: &limit, Cursor: cursor}))
		if reply.WriteOutcome != "" || reply.Found != nil || reply.Epoch == nil || reply.StateSeq == nil || reply.More != (len(reply.Cursor) > 0) {
			t.Fatalf("list reply %+v", reply)
		}
		ids := u67EntryIds(t, reply.Entries)
		if len(ids) > 3 {
			t.Fatalf("a page of %d with limit 3", len(ids))
		}
		seen = append(seen, ids...)
		if !reply.More {
			break
		}
		cursor = reply.Cursor
	}
	whole, err := env.node.List(context.Background(), u30iDB, "Box", nil, 0)
	if err != nil || len(whole) != 7 || len(seen) != 7 {
		t.Fatalf("List %d, %v; pages saw %v", len(whole), err, seen)
	}
	for i, doc := range whole {
		if bson.Raw(doc.Body).Lookup("_id_").StringValue() != seen[i] {
			t.Fatalf("pages saw %v, List has another order at %d", seen, i)
		}
	}
	filter := u67Filter(t, bson.D{{Key: "Name", Value: "even"}})
	even := u66Reply(t, c.list(t, RingListRequest{Database: u30iDB, Bucket: "Box", Filter: filter}))
	if ids := u67EntryIds(t, even.Entries); len(ids) != 3 || even.More {
		t.Fatalf("filtered list = %v more %v, want the 3 even documents", ids, even.More)
	}
	none := u66Reply(t, c.list(t, RingListRequest{Database: u30iDB, Bucket: "Box", Filter: u67Filter(t, bson.D{{Key: "Name", Value: "zzz"}})}))
	if len(none.Entries) != 0 || none.More || none.Epoch == nil {
		t.Fatalf("a list that matches nothing = %+v", none)
	}
}

// TestU67CountOverTLS: [GUARD the count case and a present zero].
func TestU67CountOverTLS(t *testing.T) {
	env := u66Start(t)
	u67BoxDocs(t, env)
	c := env.login(t)
	all := u66Reply(t, c.count(t, RingCountRequest{Database: u30iDB, Bucket: "Box"}))
	if all.Count == nil || *all.Count != 7 || all.Epoch == nil || all.StateSeq == nil || len(all.Entries) != 0 {
		t.Fatalf("count reply %+v", all)
	}
	odd := u66Reply(t, c.count(t, RingCountRequest{Database: u30iDB, Bucket: "Box", Filter: u67Filter(t, bson.D{{Key: "Name", Value: "odd"}})}))
	if odd.Count == nil || *odd.Count != 3 {
		t.Fatalf("filtered count %+v, want 3", odd)
	}
	zero := u66Reply(t, c.count(t, RingCountRequest{Database: u30iDB, Bucket: "Box", Filter: u67Filter(t, bson.D{{Key: "Name", Value: "zzz"}})}))
	if zero.Count == nil || *zero.Count != 0 {
		t.Fatalf("a count of nothing must be a present zero: %+v", zero)
	}
}

// TestU67ListAndCountRefusalsOverTLS: [GUARD the refusal path of the two handlers].
func TestU67ListAndCountRefusalsOverTLS(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	negative, huge := int32(-1), int32(10001)
	bad := u67Filter(t, bson.D{{Key: "Shoe", Value: int32(1)}})
	for name, tc := range map[string]struct {
		frame *transport.Frame
		code  cluster.Code
		said  string
	}{
		"limit below one":   {c.list(t, RingListRequest{Database: u30iDB, Bucket: "Box", Limit: &negative}), cluster.CodeInvalidArgument, "not a page size"},
		"limit above max":   {c.list(t, RingListRequest{Database: u30iDB, Bucket: "Box", Limit: &huge}), cluster.CodeInvalidArgument, "10000"},
		"bad cursor":        {c.list(t, RingListRequest{Database: u30iDB, Bucket: "Box", Cursor: []byte("not a cursor, not at all, no")}), cluster.CodeInvalidArgument, "cursor"},
		"unknown bucket":    {c.list(t, RingListRequest{Database: u30iDB, Bucket: "Nope"}), cluster.CodeNotFound, "Nope"},
		"unknown database":  {c.list(t, RingListRequest{Database: "nodb", Bucket: "Box"}), cluster.CodeNotFound, "nodb"},
		"undeclared filter": {c.list(t, RingListRequest{Database: u30iDB, Bucket: "Box", Filter: bad}), cluster.CodeFieldType, "Shoe"},
		"count unknown":     {c.count(t, RingCountRequest{Database: u30iDB, Bucket: "Nope"}), cluster.CodeNotFound, "Nope"},
		"count undeclared":  {c.count(t, RingCountRequest{Database: u30iDB, Bucket: "Box", Filter: bad}), cluster.CodeFieldType, "Shoe"},
		"stale epoch":       {c.list(t, RingListRequest{Database: u30iDB, Bucket: "Box", Epoch: int64(env.node.topology.Committed()) + 5}), cluster.CodeStaleEpoch, ""},
		"not a request":     {c.send(transport.OpRingList, u66ID(), []byte("nonsense")), cluster.CodeInvalidArgument, "BSON"},
		"count with limit":  {c.send(transport.OpRingCount, u66ID(), u66BSON(t, bson.D{{Key: "database", Value: u30iDB}, {Key: "bucket", Value: "Box"}, {Key: "limit", Value: int32(1)}})), cluster.CodeInvalidArgument, "limit"},
	} {
		refusal := u66Refusal(t, tc.frame, tc.code)
		if !strings.Contains(refusal.Message, tc.said) {
			t.Errorf("%s: %q does not say %q", name, refusal.Message, tc.said)
		}
		if tc.code == cluster.CodeStaleEpoch && (refusal.Detail == nil || refusal.Detail.Owner != "n1") {
			t.Errorf("%s: detail %+v, want the owner", name, refusal.Detail)
		}
	}
	// The connection is still good afterwards.
	u66Reply(t, c.list(t, RingListRequest{Database: u30iDB, Bucket: "Box"}))
}

func (e *u66Env) storeList(t *testing.T, field string, limit uint16, after []byte) RingKeyedRequest {
	t.Helper()
	req := e.read(t, RingClassStoreList, "b1", field, limit)
	req.Read.After = after
	return req
}

// TestU67StoreListAndCountOverTLS: [GUARD the store list and store count cases].
func TestU67StoreListAndCountOverTLS(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	if empty := u66Reply(t, c.keyed(env.read(t, RingClassStoreCount, "b1", "Queue", 0))); empty.Count == nil || *empty.Count != 0 {
		t.Fatalf("count of an empty field must be a present zero: %+v", empty)
	}
	env.pushQueue(t, c, "a", "b", "c", "d", "e")
	count := u66Reply(t, c.keyed(env.read(t, RingClassStoreCount, "b1", "Queue", 0)))
	if count.Count == nil || *count.Count != 5 || count.WriteOutcome != "" || count.Epoch == nil || count.Position != nil {
		t.Fatalf("store count reply %+v", count)
	}
	var seen []string
	var after []byte
	for pages := 0; ; pages++ {
		if pages > 10 {
			t.Fatal("the cursor never ended")
		}
		reply := u66Reply(t, c.keyed(env.storeList(t, "Queue", 2, after)))
		if reply.WriteOutcome != "" || reply.Position != nil || reply.Found == nil || !*reply.Found || len(reply.Elements) > 2 || reply.More != (len(reply.Cursor) > 0) {
			t.Fatalf("store list reply %+v", reply)
		}
		for _, element := range reply.Elements {
			seen = append(seen, u62Item(t, element))
		}
		if !reply.More {
			break
		}
		after = reply.Cursor
	}
	if strings.Join(seen, ",") != "a,b,c,d,e" {
		t.Fatalf("pages listed %v", seen)
	}
	put := u66Arg(t, func() ([]byte, error) { return encodePutArg("k1", u66Item(t, "v1")) })
	u66Reply(t, c.keyed(env.verb(t, "b1", "Table", cluster.VerbPut, put)))
	table := u66Reply(t, c.keyed(env.storeList(t, "Table", 10, nil)))
	if len(table.Elements) != 1 || table.More || len(table.Cursor) != 0 {
		t.Fatalf("hash list reply %+v", table)
	}
	nothing := u66Reply(t, c.keyed(env.storeList(t, "Stack", 10, nil)))
	if nothing.Found == nil || *nothing.Found || len(nothing.Elements) != 0 {
		t.Fatalf("a list of an empty field is found=false: %+v", nothing)
	}
	if files := u66Reply(t, c.keyed(env.read(t, RingClassStoreCount, "b1", "Files", 0))); files.Count == nil || *files.Count != 0 {
		t.Fatalf("a blobStore counts: %+v", files)
	}
}

// TestU67StoreListAndCountRefusalsOverTLS: [GUARD the refusal path and that a read moves nothing].
func TestU67StoreListAndCountRefusalsOverTLS(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	before := u63Position(env.node)
	blob := u66Refusal(t, c.keyed(env.storeList(t, "Files", 10, nil)), cluster.CodeInvalidArgument)
	if !strings.Contains(blob.Message, "blobStore") || !strings.Contains(blob.Message, "count") {
		t.Errorf("list of a blobStore: %q", blob.Message)
	}
	scalar := u66Refusal(t, c.keyed(env.storeList(t, "Name", 10, nil)), cluster.CodeFieldType)
	if !strings.Contains(scalar.Message, "Name") && !strings.Contains(scalar.Message, "#") {
		t.Errorf("list of a scalar: %q", scalar.Message)
	}
	u66Refusal(t, c.keyed(env.read(t, RingClassStoreCount, "b1", "Name", 0)), cluster.CodeFieldType)
	gone := env.read(t, RingClassStoreCount, "ghost", "Queue", 0)
	u66Refusal(t, c.keyed(gone), cluster.CodeNotFound)
	cursor := u66Refusal(t, c.keyed(env.storeList(t, "Queue", 10, []byte{1, 2, 3})), cluster.CodeInvalidArgument)
	if !strings.Contains(cursor.Message, "cursor") {
		t.Errorf("a cursor of the wrong width: %q", cursor.Message)
	}
	if after := u63Position(env.node); after != before {
		t.Fatalf("reads moved the pair from %+v to %+v", before, after)
	}
	u66Reply(t, c.keyed(env.read(t, RingClassStoreCount, "b1", "Queue", 0)))
}

func intValue(n int32) []byte {
	return append([]byte{0x10}, byte(n), byte(n>>8), byte(n>>16), byte(n>>24))
}

func (e *u66Env) update(t *testing.T, id string, fields ...cluster.FieldOp) RingKeyedRequest {
	op := e.header()
	op.Class, op.EncID, op.Fields = cluster.OpUpdate, u66EncID(t, id), fields
	return RingKeyedRequest{Op: op}
}

func (e *u66Env) name(t *testing.T, c *u66Conn) bson.RawValue {
	t.Helper()
	got := u66Reply(t, c.keyed(e.read(t, RingClassGet, "b1", "", 0)))
	value, err := bson.Raw(got.Elements[0]).LookupErr("Name")
	if err != nil {
		return bson.RawValue{}
	}
	return value
}

// TestU67UpdateOverTLS: [GUARD the update case of the ring handler].
func TestU67UpdateOverTLS(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	nameID := env.fieldID(t, "Name")
	set := u66Reply(t, c.keyed(env.update(t, "b1", cluster.FieldOp{FieldID: nameID, OpCode: cluster.UpdateSet, Value: intValue(40)})))
	if set.WriteOutcome != "applied" || set.Position == nil || set.Found != nil || set.ServedBy != "n1" {
		t.Fatalf("update reply %+v", set)
	}
	u66Reply(t, c.keyed(env.update(t, "b1", cluster.FieldOp{FieldID: nameID, OpCode: cluster.UpdateInc, Value: intValue(2)})))
	if got := env.name(t, c); got.Type != bson.TypeInt32 || got.Int32() != 42 {
		t.Fatalf("after set 40 and inc 2 the field is %v", got)
	}
	u66Reply(t, c.keyed(env.update(t, "b1", cluster.FieldOp{FieldID: nameID, OpCode: cluster.UpdateUnset})))
	if got := env.name(t, c); got.Type != 0 {
		t.Fatalf("after the unset the field is %v", got)
	}
}

// TestU67ARetriedUpdateIsDeduped: [GUARD the request id reaches the writer]. An inc applied
// twice is the failure the dedupe window exists for.
func TestU67ARetriedUpdateIsDeduped(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	nameID := env.fieldID(t, "Name")
	u66Reply(t, c.keyed(env.update(t, "b1", cluster.FieldOp{FieldID: nameID, OpCode: cluster.UpdateSet, Value: intValue(1)})))
	payload, err := EncodeRingKeyed(env.update(t, "b1", cluster.FieldOp{FieldID: nameID, OpCode: cluster.UpdateInc, Value: intValue(5)}))
	if err != nil {
		t.Fatal(err)
	}
	id := u66ID()
	first := u66Reply(t, c.send(transport.OpRingKeyed, id, payload))
	second := u66Reply(t, c.send(transport.OpRingKeyed, id, payload))
	if first.Deduped || !second.Deduped || *first.Position != *second.Position {
		t.Fatalf("first %+v, retry %+v", first, second)
	}
	if got := env.name(t, c); got.Int32() != 6 {
		t.Fatalf("a retried inc applied twice: %v", got)
	}
}

// TestU67RefusedUpdatesOverTLSDoNotMoveThePair: [GUARD the refusals of update by name].
func TestU67RefusedUpdatesOverTLSDoNotMoveThePair(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	nameID, queueID := env.fieldID(t, "Name"), env.fieldID(t, "Queue")
	before := u63Position(env.node)
	for name, tc := range map[string]struct {
		req  RingKeyedRequest
		code cluster.Code
		said string
	}{
		"mul":           {env.update(t, "b1", cluster.FieldOp{FieldID: nameID, OpCode: cluster.UpdateMul, Value: intValue(2)}), cluster.CodeInvalidArgument, "mul of field"},
		"rename":        {env.update(t, "b1", cluster.FieldOp{FieldID: nameID, OpCode: cluster.UpdateRenameTo, Value: []byte{0, 9}}), cluster.CodeInvalidArgument, "does not declare"},
		"a store field": {env.update(t, "b1", cluster.FieldOp{FieldID: queueID, OpCode: cluster.UpdateUnset}), cluster.CodeFieldType, "Queue"},
		"unknown field": {env.update(t, "b1", cluster.FieldOp{FieldID: 999, OpCode: cluster.UpdateUnset}), cluster.CodeFieldType, "#999"},
		"absent":        {env.update(t, "ghost", cluster.FieldOp{FieldID: nameID, OpCode: cluster.UpdateSet, Value: intValue(1)}), cluster.CodeNotFound, "does not exist"},
		"inc on a text": {env.update(t, "b1", cluster.FieldOp{FieldID: nameID, OpCode: cluster.UpdateInc, Value: intValue(1)}), cluster.CodeInvalidArgument, "not a number"},
	} {
		refusal := u66Refusal(t, c.keyed(tc.req), tc.code)
		if !strings.Contains(refusal.Message, tc.said) || (tc.code == cluster.CodeInvalidArgument && !strings.Contains(refusal.Message, "Nothing was changed")) {
			t.Errorf("%s: %q does not say %q", name, refusal.Message, tc.said)
		}
	}
	if after := u63Position(env.node); after != before {
		t.Fatalf("refused updates moved the pair from %+v to %+v", before, after)
	}
	u66Reply(t, c.keyed(env.update(t, "b1", cluster.FieldOp{FieldID: nameID, OpCode: cluster.UpdateSet, Value: intValue(7)})))
}
