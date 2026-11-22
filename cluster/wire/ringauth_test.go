package wire

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

// u70AllowAll admits every request. It is for tests of what the handler does once a
// request is admitted; production code has no such authorizer, on purpose.
type u70AllowAll struct{}

func (u70AllowAll) Authorize(context.Context, RingAccess) error { return nil }

var errU70NoGrant = errors.New("u70: no grant")

// u70Gate is a grant table for the handler tests: the session's grants are "read@db" or
// "write@db" (or "write@*"), set by the AUTH payload "user|grant,grant". It records every
// ask so a test can see the handler asked before it acted. The class-to-level rule here is
// the spec's (reads read, everything else writes); the server's real table is checked
// against the legacy opRules in package server.
type u70Gate struct {
	mu    sync.Mutex
	asked []RingAccess
	// denyTopology makes the class-only topology ask fail, as a session without an identity would.
	denyTopology bool
}

// u70KeyedClasses is the classes a RING_KEYED frame carries: all but list and count, which
// have frames of their own.
func u70KeyedClasses() []RingClass {
	var keyed []RingClass
	for _, class := range RingClasses() {
		if class != RingClassList && class != RingClassCount && class < RingClassCreateDatabase {
			keyed = append(keyed, class)
		}
	}
	return keyed
}

func u70Level(class RingClass) string {
	if class >= RingClassGet && class <= RingClassCount || class == RingClassLeaseGet || class == RingClassStoreGet || class == RingClassGetFields || class == RingClassNearest || class == RingClassAggregate || class == RingClassDescribeBucket || class == RingClassShowBuckets {
		return "read"
	}
	return "write"
}

func (g *u70Gate) Authorize(ctx context.Context, access RingAccess) error {
	g.mu.Lock()
	g.asked = append(g.asked, access)
	g.mu.Unlock()
	sess, ok := transport.SessionFromContext(ctx)
	if !ok {
		return errU70NoGrant
	}
	identity, ok := sess.Identity()
	if !ok {
		return errU70NoGrant
	}
	if access.Class == RingClassTopology {
		if g.denyTopology {
			return errU70NoGrant
		}
		if access.Database == "" {
			return nil
		}
		for _, grant := range identity.Grants {
			if grant.DB == "*" || grant.DB == access.Database {
				return nil
			}
		}
		return errors.New("u70: no role on the database")
	}
	need := u70Level(access.Class)
	for _, grant := range identity.Grants {
		covers := access.Database == "" || grant.DB == "*" || grant.DB == access.Database
		if covers && (grant.Role == "write" || grant.Role == need) {
			return nil
		}
	}
	return errU70NoGrant
}

func (g *u70Gate) asks() []RingAccess {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]RingAccess(nil), g.asked...)
}

func (g *u70Gate) askedCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.asked)
}

// u70Next authenticates any AUTH and sets the identity the payload describes.
func u70Next(ctx context.Context, frame *transport.Frame) (*transport.Frame, error) {
	if frame.Opcode != transport.OpAuth {
		return transport.NewFrameWithID(transport.OpError, frame.RequestID, []byte{5, 0, 0, 0, 0}), nil
	}
	user, grants, _ := strings.Cut(string(frame.Payload), "|")
	identity := transport.Identity{Username: user}
	for _, grant := range strings.Split(grants, ",") {
		if role, db, ok := strings.Cut(grant, "@"); ok {
			identity.Grants = append(identity.Grants, transport.ScopedGrant{Role: role, DB: db})
		}
	}
	if sess, ok := transport.SessionFromContext(ctx); ok {
		sess.SetIdentity(identity)
	}
	return transport.NewFrameWithID(transport.OpAuthOK, frame.RequestID, nil), nil
}

// u70Start serves a node with bucket Box in database app and the same bucket Box, under
// its own id, in database vault, behind the ring handler and a grant table.
func u70Start(t *testing.T) (*u66Env, *u66Env, *u70Gate) {
	t.Helper()
	node, _ := u63Fresh(t)
	ctx := context.Background()
	if _, err := node.CreateDatabase(ctx, "vault"); err != nil {
		t.Fatal(err)
	}
	if _, err := node.CreateBucket(ctx, "vault", "Box", Field{Name: "Name", Type: "string"},
		Field{Name: "Queue", Type: "fifoStore"}, Field{Name: "Stack", Type: "lifoStore"},
		Field{Name: "Ranked", Type: "heapStore"}, Field{Name: "Table", Type: "hashStore"},
		Field{Name: "Tree", Type: "btreeStore"}, Field{Name: "Files", Type: "blobStore"}); err != nil {
		t.Fatal(err)
	}
	node.topology.Refresh()
	gate := &u70Gate{}
	handler, err := NewRingHandler(node, u70Next, gate)
	if err != nil {
		t.Fatal(err)
	}
	tlsCfg, _, err := transport.MakeServerTLSConfig(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	listener, err := transport.ListenTCP("127.0.0.1:0", tlsCfg, transport.NewConnLimiter(64, 64), transport.TCPConfig{},
		handler, &transport.SessionDeps{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	appRef, err := node.bucket(ctx, u30iDB, "Box")
	if err != nil {
		t.Fatal(err)
	}
	vaultRef, err := node.bucket(ctx, "vault", "Box")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	return &u66Env{node: node, addr: addr, ref: appRef}, &u66Env{node: node, addr: addr, ref: vaultRef}, gate
}

// u70Login authenticates as user holding the grants ("read@app,write@vault").
func u70Login(t *testing.T, env *u66Env, user, grants string) *u66Conn {
	t.Helper()
	c := env.dial(t)
	if reply := c.send(transport.OpAuth, u66ID(), []byte(user+"|"+grants)); reply.Opcode != transport.OpAuthOK {
		t.Fatalf("AUTH answered %s", reply.Opcode)
	}
	return c
}

// u70Request builds a request of class against the bucket of env, for the document b1.
func u70Request(t *testing.T, env *u66Env, class RingClass) RingKeyedRequest {
	t.Helper()
	switch class {
	case cluster.OpInsert, cluster.OpUpsert:
		return env.doc(t, class, "u70-new", "x")
	case cluster.OpUpdate:
		req := env.doc(t, class, "b1", "x")
		req.Op.IDValue = nil // an update names its document by the key alone
		req.Op.Fields[0].OpCode = cluster.UpdateSet
		return req
	case cluster.OpDelete:
		op := env.header()
		op.Class, op.EncID = cluster.OpDelete, u66EncID(t, "b1")
		return RingKeyedRequest{Op: op}
	case cluster.OpStoreVerb:
		return env.verb(t, "b1", "Queue", cluster.VerbPush, u66Arg(t, func() ([]byte, error) { return encodeElementArg(u66Item(t, "u70")) }))
	case cluster.OpClaim:
		op := env.header()
		op.Class, op.EncID, op.Count = cluster.OpClaim, u66EncID(t, "b1"), 1
		op.Fields = []cluster.FieldOp{{FieldID: env.fieldID(t, "Queue")}}
		return RingKeyedRequest{Op: op}
	case cluster.OpAck, cluster.OpRelease, cluster.OpExtend:
		op := env.header()
		op.Class, op.EncID, op.LeaseID = class, u66EncID(t, "b1"), [16]byte{9}
		op.Fields = []cluster.FieldOp{{FieldID: env.fieldID(t, "Queue")}}
		if class == cluster.OpExtend {
			op.Until = 1 << 40
		}
		return RingKeyedRequest{Op: op}
	}
	if class == RingClassGet {
		return env.read(t, class, "b1", "", 0)
	}
	if class == RingClassGetFields {
		return u87GetFields(t, env, "b1", "Name")
	}
	if class == RingClassLeaseGet {
		return env.leaseGet(t, "b1", "Queue", bytes.Repeat([]byte{1}, 16))
	}
	if class == RingClassNearest {
		return u70Nearest(t, env, "b1", "Table")
	}
	if class == RingClassAggregate {
		return u70Aggregate(t, env, "b1", "Table")
	}
	if class == RingClassStoreCount {
		return env.read(t, class, "b1", "Queue", 0)
	}
	if class == RingClassStoreGet {
		return u85StoreGet(env, t, "b1", "Table", "k1")
	}
	return env.read(t, class, "b1", "Queue", 5)
}

// u70Aggregate builds a valid aggregate read against field of the document id: a describe, which needs no
// declared aggregate.
func u70Aggregate(t *testing.T, env *u66Env, id, field string) RingKeyedRequest {
	t.Helper()
	req := env.read(t, RingClassAggregate, id, field, 0)
	req.Read.Aggregate = &RingAggregateRead{Kind: AggregateReadDescribe}
	return req
}

// u70Nearest builds a valid nearest request against field of the document id: cosine, k 3,
// the vector field "Vector" and a 2-dimensional query.
func u70Nearest(t *testing.T, env *u66Env, id, field string) RingKeyedRequest {
	t.Helper()
	req := env.read(t, RingClassNearest, id, field, 0)
	req.Read.Nearest = &RingNearest{Metric: RingMetricCosine, K: 3, Field: "Vector", Vector: []byte{0, 0, 0x80, 0x3f, 0, 0, 0, 0}}
	return req
}

func TestNearestNeedsReader(t *testing.T) {
	app, _, gate := u70Start(t)
	reader := u70Login(t, app, "reader", "read@app")
	// An allowed ask reaches the handler; whatever it answers is not PERMISSION_DENIED (a
	// refusal of the class, not of the session).
	frame := reader.keyed(u70Nearest(t, app, "b1", "Table"))
	if frame.Opcode == transport.OpError {
		refusal, err := DecodeRingError(frame.Payload)
		if err != nil {
			t.Fatal(err)
		}
		if refusal.Code == cluster.CodePermissionDenied {
			t.Fatalf("a read grant was denied a nearest: %s", refusal.Message)
		}
	}
	asked := false
	for _, access := range gate.asks() {
		if access.Class == RingClassNearest && access.Database == "app" {
			asked = true
		}
	}
	if !asked {
		t.Fatalf("the handler never asked the authorizer for a nearest on app: %+v", gate.asks())
	}
	if level := u70Level(RingClassNearest); level != "read" {
		t.Fatalf("a nearest is %s, want read", level)
	}
	nobody := u70Login(t, app, "nobody", "")
	refusal := u66Refusal(t, nobody.keyed(u70Nearest(t, app, "b1", "Table")), cluster.CodePermissionDenied)
	if refusal.Detail != nil {
		t.Errorf("the denial carries a detail %+v", refusal.Detail)
	}
	other := u70Login(t, app, "other", "read@vault")
	u66Refusal(t, other.keyed(u70Nearest(t, app, "b1", "Table")), cluster.CodePermissionDenied)
}

func TestU70NewRingHandlerRefusesToBuildWithoutAnAuthorizer(t *testing.T) {
	node, _ := u63Fresh(t)
	handler, err := NewRingHandler(node, u66AuthNext, nil)
	if err == nil || handler != nil {
		t.Fatalf("a nil authorizer built a handler (%v, %v)", handler != nil, err)
	}
	if !strings.Contains(err.Error(), "authorizer") {
		t.Errorf("the refusal does not name the authorizer: %v", err)
	}
}

func TestU70ASessionWithNoGrantsIsDeniedEveryClass(t *testing.T) {
	app, _, gate := u70Start(t)
	c := u70Login(t, app, "nobody", "")
	before := u69Pair(app, "b1", t)
	if len(u70KeyedClasses()) != 18 {
		t.Fatalf("%d classes are keyed, the wire has 18", len(u70KeyedClasses()))
	}
	for _, class := range u70KeyedClasses() {
		refusal := u66Refusal(t, c.keyed(u70Request(t, app, class)), cluster.CodePermissionDenied)
		if refusal.Detail != nil {
			t.Errorf("class 0x%02x: the denial carries a detail %+v", class, refusal.Detail)
		}
		for _, name := range []string{"app", "vault", "Box", "n1"} {
			if strings.Contains(refusal.Message, name) {
				t.Errorf("class 0x%02x: the denial names %q: %s", class, name, refusal.Message)
			}
		}
	}
	if after := u69Pair(app, "b1", t); u69Moved(before, after) {
		t.Fatalf("the pair moved under denied requests: %+v then %+v", before, after)
	}
	if gate.askedCount() == 0 {
		t.Fatal("the handler never asked the authorizer")
	}
	// Nothing was written: an authorized session sees an untouched queue and no new document.
	reader := u70Login(t, app, "reader", "read@app")
	if peeked := u66Reply(t, reader.keyed(app.read(t, RingClassPeek, "b1", "Queue", 10))); len(peeked.Elements) != 0 {
		t.Fatalf("a denied push left %d elements", len(peeked.Elements))
	}
	if got := u66Reply(t, reader.keyed(app.read(t, RingClassGet, "u70-new", "", 0))); got.Found == nil || *got.Found {
		t.Fatalf("a denied insert stored a document: %+v", got)
	}
	if got := u66Reply(t, reader.keyed(app.read(t, RingClassGet, "b1", "", 0))); got.Found == nil || !*got.Found {
		t.Fatalf("a denied delete removed the document: %+v", got)
	}
}

func TestU70AReadOnlyGrantReadsButDoesNotWrite(t *testing.T) {
	app, _, _ := u70Start(t)
	c := u70Login(t, app, "reader", "read@app")
	if got := u66Reply(t, c.keyed(app.read(t, RingClassGet, "b1", "", 0))); got.Found == nil || !*got.Found {
		t.Fatalf("a reader cannot get: %+v", got)
	}
	u66Reply(t, c.keyed(app.read(t, RingClassPeek, "b1", "Queue", 5)))
	// Naming fewer fields is no way around the grant: a reader may project, a session with
	// no role on the database may not (TestU70ASessionWithNoGrantsIsDeniedEveryClass).
	if got := u66Reply(t, c.keyed(u87GetFields(t, app, "b1", "Name"))); got.Found == nil || !*got.Found {
		t.Fatalf("a reader cannot project a get: %+v", got)
	}
	for _, class := range u70KeyedClasses() {
		if u70Level(class) != "write" {
			continue
		}
		u66Refusal(t, c.keyed(u70Request(t, app, class)), cluster.CodePermissionDenied)
	}
}

func TestU70AWriteGrantOnOneDatabaseCannotTouchAnother(t *testing.T) {
	app, vault, _ := u70Start(t)
	c := u70Login(t, app, "writer", "write@app")
	u66Reply(t, c.keyed(app.doc(t, cluster.OpInsert, "w1", "ok")))
	before := u69Pair(vault, "b1", t)
	for _, class := range u70KeyedClasses() {
		u66Refusal(t, c.keyed(u70Request(t, vault, class)), cluster.CodePermissionDenied)
	}
	if after := u69Pair(vault, "b1", t); u69Moved(before, after) {
		t.Fatalf("the other database's pair moved: %+v then %+v", before, after)
	}
	// The grant on vault is what opens vault, and only for what it says.
	both := u70Login(t, app, "both", "read@vault,write@app")
	u66Reply(t, both.keyed(vault.read(t, RingClassGet, "b1", "", 0)))
	u66Refusal(t, both.keyed(u70Request(t, vault, cluster.OpInsert)), cluster.CodePermissionDenied)
}

func TestU70ARefusalIsTheSameForAMissingAndAForbiddenBucket(t *testing.T) {
	app, vault, _ := u70Start(t)
	missing := vault.read(t, RingClassGet, "b1", "", 0)
	missing.Read.BucketID ^= 0x5A5A5A5A
	for _, grants := range []string{"", "write@app"} {
		c := u70Login(t, app, "probe", grants)
		forbidden := u66Refusal(t, c.keyed(vault.read(t, RingClassGet, "b1", "", 0)), cluster.CodePermissionDenied)
		absent := u66Refusal(t, c.keyed(missing), cluster.CodePermissionDenied)
		if forbidden.Message != absent.Message || forbidden.Detail != nil || absent.Detail != nil {
			t.Errorf("grants %q: a forbidden bucket and a missing one are told apart:\n forbidden: %+v\n absent:    %+v", grants, forbidden, absent)
		}
	}
}

func TestU70StaleEpochAndFieldTextsAreOnlyForAnAuthorizedSession(t *testing.T) {
	app, vault, _ := u70Start(t)
	c := u70Login(t, app, "writer", "write@app")
	stale := vault.read(t, RingClassGet, "b1", "", 0)
	stale.Prologue.Epoch = 99
	refusal := u66Refusal(t, c.keyed(stale), cluster.CodePermissionDenied)
	if refusal.Detail != nil || strings.Contains(refusal.Message, "slot") {
		t.Errorf("a forbidden bucket answered with the stale-epoch detail: %+v", refusal)
	}
	badField := vault.read(t, RingClassPeek, "b1", "Queue", 5)
	badField.Read.FieldID = 4000
	if refusal := u66Refusal(t, c.keyed(badField), cluster.CodePermissionDenied); strings.Contains(refusal.Message, "declared") {
		t.Errorf("a forbidden bucket listed its fields: %q", refusal.Message)
	}
	// The authorized side still gets both texts.
	own := app.read(t, RingClassGet, "b1", "", 0)
	own.Prologue.Epoch = 99
	if got := u66Refusal(t, c.keyed(own), cluster.CodeStaleEpoch); got.Detail == nil {
		t.Errorf("an authorized stale request lost its detail")
	}
	ownField := app.read(t, RingClassPeek, "b1", "Queue", 5)
	ownField.Read.FieldID = 4000
	u66Refusal(t, c.keyed(ownField), cluster.CodeFieldType)
}

func TestU70ACachedBucketIDDoesNotSkipTheAuthorizer(t *testing.T) {
	app, vault, _ := u70Start(t)
	owner := u70Login(t, app, "both", "read@vault")
	u66Reply(t, owner.keyed(vault.read(t, RingClassGet, "b1", "", 0))) // warms the id cache
	other := u70Login(t, app, "app-only", "read@app")
	u66Refusal(t, other.keyed(vault.read(t, RingClassGet, "b1", "", 0)), cluster.CodePermissionDenied)
}

// A database that cannot answer must not tell a session with no grants anything: it is
// refused before the lookup that would hit the broken database.
func TestU70ASessionWithNoGrantsLearnsNothingFromAnUnreachableDatabase(t *testing.T) {
	app, _, _ := u70Start(t)
	other, err := app.node.CreateDatabase(context.Background(), "zzbroken")
	if err != nil {
		t.Fatal(err)
	}
	app.node.topology.Refresh()
	u69WatchNames(app).broken[other] = true
	unknown := app.read(t, RingClassGet, "b1", "", 0)
	unknown.Read.BucketID ^= 0x5A5A5A5A
	none := u70Login(t, app, "nobody", "")
	u66Refusal(t, none.keyed(unknown), cluster.CodePermissionDenied)
	reader := u70Login(t, app, "reader", "read@app")
	if refusal := u66Refusal(t, reader.keyed(unknown), cluster.CodeUnavailable); strings.Contains(refusal.Message, "zzbroken") {
		t.Errorf("the unavailable text names a database: %q", refusal.Message)
	}
}

// u70ListCount sends a RING_LIST and a RING_COUNT for the bucket of env and returns both answers.
func u70ListCount(t *testing.T, c *u66Conn, env *u66Env, bucket string) (list, count *transport.Frame) {
	t.Helper()
	return c.list(t, RingListRequest{Database: env.ref.db, Bucket: bucket}),
		c.count(t, RingCountRequest{Database: env.ref.db, Bucket: bucket})
}

func TestU70ASessionWithNoGrantsIsDeniedListAndCount(t *testing.T) {
	app, _, gate := u70Start(t)
	c := u70Login(t, app, "nobody", "")
	for _, bucket := range []string{"Box", "Ghost"} {
		list, count := u70ListCount(t, c, app, bucket)
		for name, frame := range map[string]*transport.Frame{"list": list, "count": count} {
			refusal := u66Refusal(t, frame, cluster.CodePermissionDenied)
			if refusal.Detail != nil {
				t.Errorf("%s %s: the denial carries a detail %+v", name, bucket, refusal.Detail)
			}
			for _, word := range []string{"app", "vault", "Box", "Ghost", "n1"} {
				if strings.Contains(refusal.Message, word) {
					t.Errorf("%s %s: the denial names %q: %s", name, bucket, word, refusal.Message)
				}
			}
		}
	}
	for _, ask := range gate.asks() {
		if ask.Database != "" {
			t.Errorf("a session with no grant reached the per-database ask %+v", ask)
		}
	}
}

func TestU70AReadGrantPassesListAndCountAndOnlyOnItsDatabase(t *testing.T) {
	app, vault, _ := u70Start(t)
	c := u70Login(t, app, "reader", "read@app")
	list, count := u70ListCount(t, c, app, "Box")
	if got := u66Reply(t, list); got.Entries == nil {
		t.Errorf("a reader got no entries from list: %+v", got)
	}
	if got := u66Reply(t, count); got.Count == nil || *got.Count != 1 {
		t.Errorf("a reader's count is %+v, want 1", got.Count)
	}
	list, count = u70ListCount(t, c, vault, "Box")
	u66Refusal(t, list, cluster.CodePermissionDenied)
	u66Refusal(t, count, cluster.CodePermissionDenied)
}

func TestU70ListAndCountRefuseAForbiddenAndAMissingBucketAlike(t *testing.T) {
	app, vault, _ := u70Start(t)
	c := u70Login(t, app, "reader", "read@app")
	forbiddenList, forbiddenCount := u70ListCount(t, c, vault, "Box")
	absentList, absentCount := u70ListCount(t, c, vault, "Ghost")
	a := u66Refusal(t, forbiddenList, cluster.CodePermissionDenied)
	b := u66Refusal(t, absentList, cluster.CodePermissionDenied)
	d := u66Refusal(t, forbiddenCount, cluster.CodePermissionDenied)
	e := u66Refusal(t, absentCount, cluster.CodePermissionDenied)
	if a.Message != b.Message || d.Message != e.Message || a.Message != d.Message {
		t.Errorf("a forbidden and a missing bucket are told apart:\n%q\n%q\n%q\n%q", a.Message, b.Message, d.Message, e.Message)
	}
	// A session that may read the database is told the bucket is missing.
	list, count := u70ListCount(t, c, app, "Ghost")
	u66Refusal(t, list, cluster.CodeNotFound)
	u66Refusal(t, count, cluster.CodeNotFound)
}
