package wire

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

func u75Send(t *testing.T, c *u66Conn, req RingAdminRequest) *transport.Frame {
	t.Helper()
	body, err := EncodeRingAdmin(req)
	if err != nil {
		t.Fatal(err)
	}
	return c.send(transport.OpRingAdmin, u66ID(), body)
}

func u75Reply(t *testing.T, frame *transport.Frame) RingAdminReply {
	t.Helper()
	if frame.Opcode != transport.OpRingReply {
		t.Fatalf("answered %s %x, want RING_REPLY", frame.Opcode, frame.Payload)
	}
	reply, err := DecodeRingAdminReply(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	return reply
}

func u75Fields() []RingAdminField {
	return []RingAdminField{{Name: "Name", Type: "string"}, {Name: "Purchases", Type: "fifoStore"}, {Name: "Friends", Type: "hashStore"}}
}

// TestU75TheWireCreatesADatabaseAndABucketAndTellsTheIds: [GUARD the reply carries the
// values a RING_KEYED header needs, equal to what the node resolves, and they work].
func TestU75TheWireCreatesADatabaseAndABucketAndTellsTheIds(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	created := u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateDatabase, Name: "shop"}))
	if created.Cmd != AdminCreateDatabase || created.DatabaseID == 0 || created.RF != 1 {
		t.Fatalf("create_database reply %+v", created)
	}
	bucket := u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateBucket, Database: "shop", Name: "User", Fields: u75Fields()}))
	known, err := env.node.ClientBucket(t.Context(), "shop", "User")
	if err != nil {
		t.Fatal(err)
	}
	if bucket.BucketID == 0 || bucket.BucketID != known.BucketID || bucket.DeclVersion != known.DeclVersion || bucket.SchemaVersionSeen != known.SchemaVersionSeen || bucket.State != "active" {
		t.Fatalf("create_bucket reply %+v, node says %+v", bucket, known)
	}
	if len(bucket.Fields) != 4 || bucket.Fields[0] != (RingAdminReplyField{ID: 1, Name: "_id_", Type: "any"}) {
		t.Fatalf("field table %+v", bucket.Fields)
	}
	for _, field := range bucket.Fields {
		if known.FieldIDs[field.Name] != field.ID {
			t.Fatalf("field %+v, node says id %d", field, known.FieldIDs[field.Name])
		}
	}
	if described := u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminDescribeBucket, Database: "shop", Bucket: "User"})); fmt.Sprint(described.Fields) != fmt.Sprint(bucket.Fields) ||
		described.BucketID != bucket.BucketID || described.DeclVersion != bucket.DeclVersion || described.SchemaVersionSeen != bucket.SchemaVersionSeen {
		t.Fatalf("describe_bucket %+v differs from create_bucket %+v", described, bucket)
	}
	// The ids work in a RING_KEYED header.
	insert := cluster.Op{BucketID: bucket.BucketID, DeclVersion: bucket.DeclVersion, SchemaVersionSeen: bucket.SchemaVersionSeen,
		Class: cluster.OpInsert, EncID: u66EncID(t, "ada"), IDValue: stringValue("ada"),
		Fields: []cluster.FieldOp{{FieldID: bucket.Fields[1].ID, Value: stringValue("Ada")}}}
	if reply := u66Reply(t, c.keyed(RingKeyedRequest{Op: insert})); reply.WriteOutcome != outcomeApplied {
		t.Fatalf("insert with the described ids: %+v", reply)
	}
}

// TestU75ARetriedCreateBucketCreatesNothingTwice: [GUARD an identical repeat answers with the
// same bucket and a different table is BUCKET_EXISTS that names describe_bucket's fix].
func TestU75ARetriedCreateBucketCreatesNothingTwice(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateDatabase, Name: "shop"}))
	req := RingAdminRequest{Cmd: AdminCreateBucket, Database: "shop", Name: "User", Fields: u75Fields()}
	body, err := EncodeRingAdmin(req)
	if err != nil {
		t.Fatal(err)
	}
	id := u66ID()
	first := u75Reply(t, c.send(transport.OpRingAdmin, id, body))
	again := u75Reply(t, c.send(transport.OpRingAdmin, id, body))
	if first.BucketID != again.BucketID || fmt.Sprint(first.Fields) != fmt.Sprint(again.Fields) {
		t.Fatalf("a retry with the same request id gave %+v then %+v", first, again)
	}
	req.Fields = u75Fields()[:2]
	refusal := u66Refusal(t, u75Send(t, c, req), cluster.CodeBucketExists)
	if !strings.Contains(refusal.Message, "describe bucket User in shop") {
		t.Errorf("the BUCKET_EXISTS text does not name the fix: %s", refusal.Message)
	}
	if page := u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminShowBuckets, Database: "shop"})); len(page.Buckets) != 1 {
		t.Fatalf("a retry created a second bucket: %+v", page.Buckets)
	}
}

// TestU75ARepeatedCreateDatabaseIsConflictAndNothingWedges: [GUARD the repeat is CONFLICT
// naming the fix, and the next create still works].
func TestU75ARepeatedCreateDatabaseIsConflictAndNothingWedges(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateDatabase, Name: "shop"}))
	refusal := u66Refusal(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateDatabase, Name: "shop"}), cluster.CodeConflict)
	if !strings.Contains(refusal.Message, "show databases") {
		t.Errorf("the CONFLICT text does not name the fix: %s", refusal.Message)
	}
	if next := u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateDatabase, Name: "shop2"})); next.DatabaseID == 0 {
		t.Fatalf("a create after a conflict: %+v", next)
	}
}

// TestU75RefusalsNameThingAndFix: [GUARD each refusal carries its ring code and the command
// that fixes it].
func TestU75RefusalsNameThingAndFix(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateDatabase, Name: "shop"}))
	refusal := u66Refusal(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateBucket, Database: "shop", Name: "_hidden", Fields: []RingAdminField{}}), cluster.CodeReservedName)
	if !strings.Contains(refusal.Message, "_hidden") || !strings.Contains(refusal.Message, "show buckets in shop") {
		t.Errorf("RESERVED_NAME text: %s", refusal.Message)
	}
	two := int32(2)
	refusal = u66Refusal(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateDatabase, Name: "ha", RF: &two}), cluster.CodeRFUnsupported)
	if refusal.Detail == nil || refusal.Detail.Rf != 2 || refusal.Detail.Voters != 1 || !strings.Contains(refusal.Message, "join") {
		t.Errorf("RF_UNSUPPORTED %+v %s", refusal.Detail, refusal.Message)
	}
	if _, ok := env.node.topology.DatabaseID("ha"); ok {
		t.Error("a refused create_database committed the database")
	}
	refusal = u66Refusal(t, u75Send(t, c, RingAdminRequest{Cmd: AdminDescribeBucket, Database: "shop", Bucket: "Nope"}), cluster.CodeNotFound)
	if !strings.Contains(refusal.Message, "Nope") || !strings.Contains(refusal.Message, "show buckets in shop") {
		t.Errorf("NOT_FOUND text: %s", refusal.Message)
	}
	refusal = u66Refusal(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateBucket, Database: "nowhere", Name: "B", Fields: []RingAdminField{}}), cluster.CodeNotFound)
	if !strings.Contains(refusal.Message, "create database nowhere") {
		t.Errorf("a missing database does not name its fix: %s", refusal.Message)
	}
	u66Refusal(t, c.send(transport.OpRingAdmin, u66ID(), []byte{1, 2, 3}), cluster.CodeInvalidArgument)
	if described := u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminDescribeBucket, Database: "shop", Bucket: "_buckets"})); described.DeclVersion != 0 || len(described.Fields) != 8 {
		t.Errorf("describe _buckets: %+v", described)
	}
}

// TestU75ShowBucketsPagesAndHidesTheSystemDocuments: [GUARD reverse-index documents and
// _buckets are never listed, a cursor continues without a repeat or a gap].
func TestU75ShowBucketsPagesAndHidesTheSystemDocuments(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateDatabase, Name: "shop"}))
	if page := u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminShowBuckets, Database: "shop"})); len(page.Buckets) != 0 || page.More {
		t.Fatalf("an empty database lists %+v", page)
	}
	want := map[string]uint64{}
	for i := range 7 {
		name := fmt.Sprintf("B%d", i)
		created := u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateBucket, Database: "shop", Name: name, Fields: []RingAdminField{}}))
		want[name] = created.BucketID
	}
	limit := int32(3)
	got := map[string]uint64{}
	var cursor []byte
	pages := 0
	for {
		page := u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminShowBuckets, Database: "shop", Limit: &limit, Cursor: cursor}))
		pages++
		if len(page.Buckets) > int(limit) {
			t.Fatalf("a page of %d with limit %d", len(page.Buckets), limit)
		}
		for _, listed := range page.Buckets {
			if _, dup := got[listed.Name]; dup || listed.State != "active" {
				t.Fatalf("bucket %+v repeated or not active", listed)
			}
			got[listed.Name] = listed.BucketID
		}
		if !page.More {
			break
		}
		if len(page.Cursor) == 0 || pages > 10 {
			t.Fatalf("more without a usable cursor after %d pages: %+v", pages, page)
		}
		cursor = page.Cursor
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("paged %v, created %v", got, want)
	}
	if pages < 3 {
		t.Fatalf("7 buckets at limit 3 took %d pages", pages)
	}
	// A cursor belongs to its database.
	other := u75Reply(t, u75Send(t, c, RingAdminRequest{Cmd: AdminCreateDatabase, Name: "other"}))
	_ = other
	u66Refusal(t, u75Send(t, c, RingAdminRequest{Cmd: AdminShowBuckets, Database: "other", Limit: &limit, Cursor: cursor}), cluster.CodeInvalidArgument)
}

// TestU75ConcurrentCreatesDoNotRace: [GUARD creates are serialized: no lost database id,
// and equal bucket creates converge on one id].
func TestU75ConcurrentCreatesDoNotRace(t *testing.T) {
	env := u66Start(t)
	const clients = 8
	conns := make([]*u66Conn, clients)
	for i := range conns {
		conns[i] = env.login(t)
	}
	frames := make([]*transport.Frame, clients)
	var wg sync.WaitGroup
	for i := range conns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body, err := EncodeRingAdmin(RingAdminRequest{Cmd: AdminCreateDatabase, Name: fmt.Sprintf("db%d", i)})
			if err != nil {
				return
			}
			conns[i].write(transport.OpRingAdmin, [16]byte{0xD0, byte(i)}, body)
			frames[i] = conns[i].read()
		}()
	}
	wg.Wait()
	ids := map[int64]bool{}
	for i, frame := range frames {
		if frame == nil {
			t.Fatalf("client %d got no frame", i)
		}
		reply := u75Reply(t, frame)
		ids[reply.DatabaseID] = true
	}
	if len(ids) != clients {
		t.Fatalf("%d concurrent create_database calls gave %d distinct ids", clients, len(ids))
	}
	buckets := make(chan uint64, clients)
	for i := range conns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body, err := EncodeRingAdmin(RingAdminRequest{Cmd: AdminCreateBucket, Database: "db0", Name: "Same", Fields: u75Fields()})
			if err != nil {
				return
			}
			conns[i].write(transport.OpRingAdmin, [16]byte{0xD1, byte(i)}, body)
			reply, err := DecodeRingAdminReply(conns[i].read().Payload)
			if err == nil {
				buckets <- reply.BucketID
			}
		}()
	}
	wg.Wait()
	close(buckets)
	distinct := map[uint64]bool{}
	for id := range buckets {
		distinct[id] = true
	}
	if len(distinct) != 1 {
		t.Fatalf("%d concurrent identical create_bucket calls gave %d bucket ids", clients, len(distinct))
	}
}

// TestU75AdminCommandsAskTheAuthorizerTwiceAndDenyUniformly: [GUARD class-only ask first,
// then the database as sent; a missing and a forbidden database get one text naming neither].
func TestU75AdminCommandsAskTheAuthorizerTwiceAndDenyUniformly(t *testing.T) {
	app, _, gate := u70Start(t)
	nobody := u70Login(t, app, "nobody", "")
	commands := []RingAdminRequest{
		{Cmd: AdminCreateDatabase, Name: "fresh"},
		{Cmd: AdminCreateBucket, Database: "app", Name: "X", Fields: []RingAdminField{}},
		{Cmd: AdminDescribeBucket, Database: "app", Bucket: "Box"},
		{Cmd: AdminShowBuckets, Database: "app"},
	}
	for _, req := range commands {
		refusal := u66Refusal(t, u75Send(t, nobody, req), cluster.CodePermissionDenied)
		if refusal.Detail != nil {
			t.Errorf("%s: the denial carries a detail", req.Cmd)
		}
		for _, name := range []string{"app", "vault", "Box", "fresh", "n1"} {
			if strings.Contains(refusal.Message, name) {
				t.Errorf("%s: the denial names %q: %s", req.Cmd, name, refusal.Message)
			}
		}
	}
	if _, ok := app.node.topology.DatabaseID("fresh"); ok {
		t.Fatal("a denied create_database committed")
	}
	if page := u75Reply(t, u75Send(t, u70Login(t, app, "reader", "read@app"), RingAdminRequest{Cmd: AdminShowBuckets, Database: "app"})); len(page.Buckets) != 1 {
		t.Fatalf("a reader's show_buckets: %+v", page)
	}
	// A reader holds no create: the class-only ask passes nowhere.
	reader := u70Login(t, app, "reader2", "read@app")
	u66Refusal(t, u75Send(t, reader, commands[1]), cluster.CodePermissionDenied)
	// A writer on app only: forbidden vault and missing nowhere read the same.
	writer := u70Login(t, app, "writer", "write@app")
	forbidden := u66Refusal(t, u75Send(t, writer, RingAdminRequest{Cmd: AdminDescribeBucket, Database: "vault", Bucket: "Box"}), cluster.CodePermissionDenied)
	missing := u66Refusal(t, u75Send(t, writer, RingAdminRequest{Cmd: AdminDescribeBucket, Database: "nowhere", Bucket: "Box"}), cluster.CodePermissionDenied)
	if forbidden.Message != missing.Message {
		t.Errorf("a forbidden and a missing database differ:\n%s\n%s", forbidden.Message, missing.Message)
	}
	u66Refusal(t, u75Send(t, writer, commands[0]), cluster.CodePermissionDenied)
	asks := gate.asks()
	var sawClassOnly, sawNamed bool
	for _, ask := range asks {
		if ask.Class == RingClassCreateDatabase && ask.Database == "" {
			sawClassOnly = true
		}
		if ask.Class == RingClassCreateDatabase && ask.Database == "fresh" {
			sawNamed = true
		}
	}
	if !sawClassOnly || !sawNamed {
		t.Errorf("create_database asked class-only %v and named %v: %+v", sawClassOnly, sawNamed, asks)
	}
}
