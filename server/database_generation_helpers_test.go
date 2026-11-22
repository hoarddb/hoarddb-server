package server

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/ring"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/transport"
)

// pushHold holds every replication push that carries entries a peer sends
// until it is released, and records the entries of each push it held. An empty
// push — the stream's probe — passes, so the sender goes on to replay its log
// and the entries it means to deliver are what is held. A test reads what was held to
// know that the entry it means to delay was delayed, rather than assuming it.
type pushHold struct {
	mu       sync.Mutex
	held     []ReplEntry
	released chan struct{}
	once     sync.Once
}

// holdPushes holds p's replication pushes that carry entries — stream frames
// and forwarded requests alike — from now until Release.
func holdPushes(p *peer) *pushHold {
	hold := &pushHold{released: make(chan struct{})}
	wrapPeerConn(p, func(inner peerConn) peerConn { return &recordingHeldConn{peerConn: inner, hold: hold} })
	return hold
}

// Release lets every held push through, and every later one. Safe to call
// more than once.
func (h *pushHold) Release() {
	h.once.Do(func() { close(h.released) })
}

// heldEntry returns the first held entry with op whose payload names
// database, if one was held.
func (h *pushHold) heldEntry(op transport.Opcode, database string) (ReplEntry, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, entry := range h.held {
		if transport.Opcode(entry.Op) != op {
			continue
		}
		if _, named := decodeFrameMeta(entry.Payload).scope(op); named == database {
			return entry, true
		}
	}
	return ReplEntry{}, false
}

func (h *pushHold) record(entries []ReplEntry) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.held = append(h.held, entries...)
}

// recordingHeldConn is heldPushConn that also records what it held.
type recordingHeldConn struct {
	peerConn
	hold *pushHold
}

func (c *recordingHeldConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	if op == transport.OpReplPush {
		select {
		case <-c.hold.released:
		default:
			var push ReplPushRequest
			if err := bson.Unmarshal(payload, &push); err != nil || len(push.Entries) == 0 {
				return c.peerConn.SendRequest(ctx, lane, op, payload)
			}
			c.hold.record(push.Entries)
			select {
			case <-c.hold.released:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return c.peerConn.SendRequest(ctx, lane, op, payload)
}

// waitAcked waits until p has acknowledged its source's stream through seq:
// the deterministic end of a test that must know an entry was delivered and
// applied, where waiting for the stream to look idle is not enough — a stream
// that has queued nothing yet looks caught up (review F-8).
func waitAcked(t *testing.T, p *peer, seq uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := waitForStream(ctx, p, func() bool { return p.ackedThrough(p.c.self.ID) >= seq }); err != nil {
		t.Fatalf("%s did not acknowledge %s's stream through sequence %d (acknowledged %d): %v",
			p.node.ID, p.c.self.ID, seq, p.ackedThrough(p.c.self.ID), err)
	}
}

// waitAckedEverywhere waits until every other running member has
// acknowledged everything node index's log addresses to it now.
func waitAckedEverywhere(t *testing.T, tc *testCluster, index int) {
	t.Helper()
	c := clusterOf(tc, index)
	for _, member := range c.otherMembers() {
		if !nodeRunning(tc, member.ID) {
			continue
		}
		waitDelivered(t, tc, index, c.peerFor(member))
	}
}

// waitDelivered waits until p has acknowledged the last entry node index's log
// addresses to it now. A peer acknowledges only what it was sent, so the log's
// last sequence is the wrong mark when its tail is entries p is not owed.
func waitDelivered(t *testing.T, tc *testCluster, index int, p *peer) {
	t.Helper()
	core := tc.nodes[index].server.core
	entries, err := core.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading node %d's log: %v", index+1, err)
	}
	addressed, _, err := core.cluster.replayableEntries(entries, p)
	if err != nil {
		t.Fatalf("addressing node %d's log to %s: %v", index+1, p.node.ID, err)
	}
	if len(addressed) == 0 {
		return
	}
	waitAcked(t, p, addressed[len(addressed)-1].Seq)
}

// nodeRunning reports whether the member with id is one of tc's running nodes.
func nodeRunning(tc *testCluster, id string) bool {
	for i, addr := range tc.addrs {
		if addr == id {
			return tc.nodes[i].server != nil
		}
	}
	return true
}

// clusterOf is node index's cluster.
func clusterOf(tc *testCluster, index int) *Cluster {
	return tc.nodes[index].server.core.cluster
}

// peerOf is node from's peer for node to.
func peerOf(t *testing.T, tc *testCluster, from, to int) *peer {
	t.Helper()
	c := clusterOf(tc, from)
	return c.peerFor(mustNode(t, c, tc.addrs[to]))
}

// waitListedBy waits until every node in indexes lists the database.
func waitListedBy(t *testing.T, tc *testCluster, database string, indexes ...int) {
	t.Helper()
	for _, index := range indexes {
		node := tc.nodes[index]
		pollUntil(t, fmt.Sprintf("node %d lists database %q", index+1, database), func() bool { return listsDatabase(t, node, database) })
	}
}

// holdsKeyIn reports whether a node's own store holds key in database/bucket.
func holdsKeyIn(node *testNode, database, bucket, key string) bool {
	resp, err := node.server.core.Find(FindRequest{Database: database, Bucket: bucket, Key: key})
	return err == nil && resp != nil && resp.Found
}

// bucketOwnedIn returns a bucket name, prefix000 to prefix999, that node
// index owns in database.
func bucketOwnedIn(t *testing.T, tc *testCluster, index int, database, prefix string) string {
	t.Helper()
	c := clusterOf(tc, index)
	for attempt := 0; attempt < 1000; attempt++ {
		name := fmt.Sprintf("%s%03d", prefix, attempt)
		if c.ownsBucket(placementOf(database, name)) {
			return name
		}
	}
	t.Fatalf("setup: no bucket name %s000..%s999 in %q is owned by node %d", prefix, prefix, database, index+1)
	return ""
}

// bucketKeptByItsOwnerAndPlacedOn returns a bucket of testDatabase whose
// owner keeps it after newcomer joins and on which newcomer is placed, so the
// newcomer pulls it from its owner, with the owner's index.
func bucketKeptByItsOwnerAndPlacedOn(t *testing.T, tc *testCluster, newcomer ring.Node) (string, int) {
	t.Helper()
	members := clusterOf(tc, 0).Members()
	after := ringOf(t, append(append([]ring.Node{}, members...), newcomer), tc.rf)
	for attempt := 0; attempt < 1000; attempt++ {
		name := fmt.Sprintf("snap%03d", attempt)
		key := placementOf(testDatabase, name)
		was, err := clusterOf(tc, 0).Owner(key)
		if err != nil {
			t.Fatalf("owner of %s: %v", name, err)
		}
		set, err := after.Replicas(key.encoded, tc.rf)
		if err != nil {
			t.Fatalf("replicas of %s after the join: %v", name, err)
		}
		if set[0].ID == was.ID && containsID(nodeIDs(set), newcomer.ID) {
			return name, nodeIndexByAddr(t, tc.addrs, was.ID)
		}
	}
	t.Fatal("setup: no bucket name in snap000..snap999 keeps its owner and is placed on the newcomer")
	return "", 0
}

// restartableJoiner is a joiner whose process a test can replace. Its one
// cleanup stops whichever process is current, so no Service is stopped twice
// (a second Stop waits on a closed store for ever).
type restartableJoiner struct {
	node  *testNode
	ports *testNodePorts
}

// joinNewcomer adds the node reserved in ports through node via, starts it,
// and brings its membership up to via's.
func joinNewcomer(t *testing.T, tc *testCluster, via int, ports *testNodePorts) *restartableJoiner {
	t.Helper()
	viaCluster := clusterOf(tc, via)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := viaCluster.proposeJoin(ctx, ports.ringNode()); err != nil {
		t.Fatalf("proposeJoin: %v", err)
	}
	dir := t.TempDir()
	if err := SaveClusterToken(dir, ClusterToken{Token: tc.token, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("save joiner token: %v", err)
	}
	newcomer := ports.ringNode()
	trustJoinerFingerprint(t, tc, newcomer.Addr, dir)
	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dir
	cfg.Server.Listen = newcomer.Addr
	cfg.Server.NodeID = newcomer.ID
	cfg.Server.ReplicationFactor = tc.rf
	cfg.HashRing.SeedNodes = append(append([]string{}, tc.addrs...), newcomer.Addr)
	cfg.HashRing.SeedFingerprints = append([]string(nil), tc.fingerprints...)
	cfg.Metrics.Enabled = false
	cfg.Audit.Enabled = false
	cfg.Logging.Output = "stderr"
	cfg.TCP.Listen = ports.driver.Addr().String()
	// The test pulls what it needs (pulledFromOwner). A sweep would pull a
	// copy a late drop deleted straight back from its owner, which hides the
	// loss behind a fresh copy.
	cfg.Server.AutoRebalance = false
	joiner := &restartableJoiner{ports: ports}
	joiner.start(t, cfg)
	// Every member adopts the join now: a member left on the earlier epoch
	// refuses the newcomer's gossip and addresses it nothing until a later
	// round of its own gossip, which no fixed wait bounds.
	members := []*Cluster{joiner.cluster()}
	for index, node := range tc.nodes {
		if index != via && node.server != nil {
			members = append(members, clusterOf(tc, index))
		}
	}
	for _, member := range members {
		if err := member.reconcileMembership(viaCluster.self.ID, viaCluster.memberNodes(), viaCluster.memberIDs(), viaCluster.Epoch()); err != nil {
			t.Fatalf("bring %s's membership to %s's: %v", member.self.ID, viaCluster.self.ID, err)
		}
	}
	t.Cleanup(joiner.stop)
	return joiner
}

func (j *restartableJoiner) start(t *testing.T, cfg *config.Config) {
	t.Helper()
	srv, err := NewService(cfg, quietLogger())
	if err != nil {
		t.Fatalf("new joining node: %v", err)
	}
	srv.driverListener = j.ports.driver
	srv.internodeListener = j.ports.internode
	// Healthy by default, as in testCluster.startNode (BUG-0553).
	setCapacityProbes(t, srv.core, harnessProbes)
	if err := srv.LoadCatalog(); err != nil {
		t.Fatalf("load the joiner's catalog: %v", err)
	}
	if err := startWithRetry(srv, 3*time.Second); err != nil {
		t.Fatalf("start the joiner: %v", err)
	}
	// A restart finds the user already there, which is not a failure.
	if err := srv.core.SeedRootUser(testRootUser, testRootPassword); err != nil {
		t.Logf("seed the joiner's root user: %v", err)
	}
	j.node = &testNode{cfg: cfg, addr: j.ports.ringAddr, server: srv, client: dialAddr(t, j.ports.driver.Addr().String())}
}

func (j *restartableJoiner) stop() {
	if j.node.client != nil {
		_ = j.node.client.Close()
		j.node.client = nil
	}
	if j.node.server != nil {
		_ = j.node.server.Stop()
		j.node.server = nil
	}
}

// restart stops the joiner's process and starts another on its data
// directory and addresses.
func (j *restartableJoiner) restart(t *testing.T) {
	t.Helper()
	cfg := j.node.cfg
	j.stop()
	j.ports = j.ports.rebind(t)
	j.start(t, cfg)
}

func (j *restartableJoiner) cluster() *Cluster { return j.node.server.core.cluster }

// lastLoggedSeq is the sequence of the last entry with op on bucket in node
// index's own log, or fails the test.
func lastLoggedSeq(t *testing.T, tc *testCluster, index int, op transport.Opcode, bucket string) uint64 {
	t.Helper()
	entries, err := tc.nodes[index].server.core.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading node %d's log: %v", index+1, err)
	}
	var seq uint64
	for _, entry := range entries {
		if transport.Opcode(entry.Op) == op && entry.Bucket == bucket {
			seq = entry.Seq
		}
	}
	if seq == 0 {
		t.Fatalf("setup: node %d's log holds no %v of bucket %q", index+1, op, bucket)
	}
	return seq
}

// createBucketAtOne creates a hash bucket through node index at write concern
// one: CreateBucketRequest has no write-concern field, and a create waits for
// the bucket's default concern, which a held replica never meets.
func createBucketAtOne(tc *testCluster, index int, database, bucket string) {
	tc.ok(index, transport.OpCreateBucket, bson.M{
		"database": database, "name": bucket, "type": string(BucketTypeHash), "write_concern": "one",
	})
}

// putIn writes one document to database/bucket through node index.
func putIn(tc *testCluster, index int, database, bucket, key, concern string) {
	tc.ok(index, transport.OpPut, InsertRequest{
		Database: database, Bucket: bucket, Key: key, Data: map[string]interface{}{"n": 1}, WriteConcern: concern,
	})
}

// createRefusalOf returns the message of a create bucket's answer that is a
// refusal — an error frame, or a response that says it did not succeed — and
// fails the test on a success.
func createRefusalOf(t *testing.T, frame *transport.Frame) string {
	t.Helper()
	if frame.Opcode == transport.OpError {
		var refused ErrorResponse
		if err := bson.Unmarshal(frame.Payload, &refused); err != nil {
			t.Fatalf("decode the error frame: %v", err)
		}
		return refused.Message
	}
	var created CreateBucketResponse
	if err := bson.Unmarshal(frame.Payload, &created); err != nil {
		t.Fatalf("decode the create's answer: %v", err)
	}
	if created.Success {
		t.Fatalf("the create was answered as a success: %q", created.Message)
	}
	return created.Message
}

// loggedDatabaseDDL is the sequence of the last op on database that node
// index logged as its own — the entries it ships — or fails the test.
func loggedDatabaseDDL(t *testing.T, tc *testCluster, index int, op transport.Opcode, database string) uint64 {
	t.Helper()
	entries, err := tc.nodes[index].server.core.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading node %d's log: %v", index+1, err)
	}
	var seq uint64
	for _, entry := range entries {
		if entry.Origin.Kind == storage.WALOriginReplica {
			continue
		}
		if _, named := decodeFrameMeta(entry.Data).scope(op); transport.Opcode(entry.Op) == op && named == database {
			seq = entry.Seq
		}
	}
	if seq == 0 {
		t.Fatalf("setup: node %d's log holds no %v of database %q of its own", index+1, op, database)
	}
	return seq
}

// requireUndelivered fails the test unless p has not acknowledged seq: the
// proof that a hold delayed the entry the test means to deliver late.
func requireUndelivered(t *testing.T, p *peer, seq uint64, what string) {
	t.Helper()
	if acked := p.ackedThrough(p.c.self.ID); acked >= seq {
		t.Fatalf("setup: %s (sequence %d) was delivered to %s before the hold was released (acknowledged through %d)", what, seq, p.node.ID, acked)
	}
}

// splitHoldConn delivers a peer's pushes up to and including sequence upTo and
// holds what follows until released. A frame that runs past upTo is cut
// there: the peer then reports it applied through upTo only, the sender
// retries from the entry after it, and that push is held.
type splitHoldConn struct {
	peerConn
	upTo     uint64
	released <-chan struct{}
}

func (c *splitHoldConn) SendRequest(ctx context.Context, lane transport.Lane, op transport.Opcode, payload []byte) (*transport.Frame, error) {
	if op != transport.OpReplPush {
		return c.peerConn.SendRequest(ctx, lane, op, payload)
	}
	select {
	case <-c.released:
		return c.peerConn.SendRequest(ctx, lane, op, payload)
	default:
	}
	var push ReplPushRequest
	if err := bson.Unmarshal(payload, &push); err != nil || len(push.Entries) == 0 {
		return c.peerConn.SendRequest(ctx, lane, op, payload)
	}
	cut := 0
	for cut < len(push.Entries) && push.Entries[cut].Seq <= c.upTo {
		cut++
	}
	if cut == 0 {
		select {
		case <-c.released:
			return c.peerConn.SendRequest(ctx, lane, op, payload)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	push.Entries = push.Entries[:cut]
	shortened, err := bson.Marshal(push)
	if err != nil {
		return nil, fmt.Errorf("re-encoding a cut push: %w", err)
	}
	return c.peerConn.SendRequest(ctx, lane, op, shortened)
}

// pulledFromOwner makes the joiner pull database/bucket from its owner now,
// rather than waiting for its sweep, which a membership change it has not yet
// adopted can defer past any fixed wait. A non-owner replica pulls from the
// owner only (Cluster.ensureMigrated), and a pull that lands has recorded its
// snapshot position from it before the copy opened (Cluster.pullBucketPlaced);
// the position itself is forgotten again once the owner's stream passes it, so
// it is not what is waited for.
func pulledFromOwner(t *testing.T, joiner *restartableJoiner, owner *Cluster, database, bucket string) {
	t.Helper()
	c := joiner.cluster()
	if replicas, err := c.Replicas(placementOf(database, bucket)); err != nil || replicas[0].ID != owner.self.ID {
		t.Fatalf("setup: the joiner does not place %s/%s on %s first: %v %v", database, bucket, owner.self.ID, replicas, err)
	}
	pullNow(t, c, database, bucket)
}

// pullNow makes c pull database/bucket from its owner and waits until the copy
// is open. A pull the rebalance started at the same time can land first, and
// this one then fails as already catalogued: that is the copy wanted, so the
// error is not the test's failure when the copy is open.
func pullNow(t *testing.T, c *Cluster, database, bucket string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := c.ensureMigrated(ctx, database, bucket)
	if !eventually(func() bool { return c.s.bucketStoreOpen(database, bucket) }) {
		t.Fatalf("setup: the pull of %s/%s did not land (%v)", database, bucket, err)
	}
}

// heldBucketID is the id of the copy of database/bucket a node serves, or 0.
// A copy deleted and pulled again has a new id, so a test that must know a
// copy survived compares ids, not contents a re-pull would restore.
func heldBucketID(node *testNode, database, bucket string) uint64 {
	db := node.server.core.serverDataAccessor().getDB(database)
	if db == nil {
		return 0
	}
	held := db.getBucket(bucket)
	if held == nil {
		return 0
	}
	return held.id.Load()
}
