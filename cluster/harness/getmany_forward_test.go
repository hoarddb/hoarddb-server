package harness_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// getManyCluster forms three voters with an RF 2 database holding count documents
// (doc-000 ...), written through n1 and held by their owner and their copy.
func getManyCluster(t *testing.T, count int) (*harness.Cluster, []string) {
	t.Helper()
	return getManyClusterWith(t, count, harness.Options{})
}

// getManyClusterWith is getManyCluster with more options (the Raft timeout, detection and the late
// identity are set here). It does what t3Cluster does, but its inserts retry every refusal while
// the cluster settles, so a majority that is not yet confirmed on a slow VM (BUG-0808) does not
// fail a test that is about reads.
func getManyClusterWith(t *testing.T, count int, opts harness.Options) (*harness.Cluster, []string) {
	t.Helper()
	opts.Late, opts.NoDetection, opts.RaftTimeout = 1, true, 400*time.Millisecond
	c := harness.NewCluster(t, 3, opts)
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(proofCtx(t), proofDB, 2); err != nil {
		t.Fatalf("creating the RF 2 database: %v", err)
	}
	c.AwaitConverged(10 * time.Second)
	var bucketErr error
	for range 20 {
		c.AwaitLeases(30 * time.Second)
		if _, bucketErr = leader.CreateBucket(proofCtx(t), proofDB, proofBucket, wire.Field{Name: "name", Type: "string"}); !harness.IsTransientRefusal(bucketErr) {
			break
		}
	}
	if bucketErr != nil {
		t.Fatalf("creating the bucket: %v", bucketErr)
	}
	c.AwaitLeases(30 * time.Second)
	ids := make([]string, count)
	for i := range ids {
		ids[i] = fmt.Sprintf("doc-%03d", i)
		getManyInsert(t, c, "n1", proofDB, ids[i])
	}
	awaitOnCopies(t, c, proofDB, ids)
	return c, ids
}

// TestGetManyForwardsEveryKeyThroughItsRealOwner: a frame sent to one node names documents owned
// by all three. Every key answers its own document, in request order, a missing id is found:false,
// and served_from is absent unless the frame asked for it (local: true), when it says "owner" or,
// for a key a ready copy on the receiving node answered, "local".
func TestGetManyForwardsEveryKeyThroughItsRealOwner(t *testing.T) {
	c, ids := getManyCluster(t, 60)
	owners, copies := ownersOf(t, c, proofDB, ids)
	grouped := idsByOwner(ids, owners)
	if len(grouped) != 3 {
		t.Fatalf("the %d documents have owners %v; the test needs all three nodes", len(ids), grouped)
	}
	for _, via := range c.IDs() {
		client := newGetManyClient(t, c, via)
		// Request order is not owner order: take the ids in reverse, with a miss in the middle.
		var asked []string
		for i := len(ids) - 1; i >= 0; i-- {
			asked = append(asked, ids[i])
			if i == len(ids)/2 {
				asked = append(asked, "doc-absent")
			}
		}
		before := c.Node(via).GetManyStats().Keys.Load()
		reply := client.sendSettled(wire.RingPrologue{}, client.reads(asked...))
		if reply.Outcome != "ok" {
			t.Fatalf("via %s: outcome %q, want ok (background: %v)", via, reply.Outcome, c.BackgroundErrors())
		}
		for i, id := range asked {
			got := docID(reply.Answers[i])
			if id == "doc-absent" {
				if got != "" {
					t.Errorf("via %s: key %d (%s) answered %q, want found:false", via, i, id, got)
				}
				continue
			}
			if got != id || docName(reply.Answers[i]) != proofDB+"/"+id {
				t.Errorf("via %s: key %d wants %s (owner %s) and was answered %q name %q", via, i, id, owners[id], got, docName(reply.Answers[i]))
				continue
			}
			if served := reply.Answers[i].Reply.Served; served != "" {
				t.Errorf("via %s: key %s says served_from %q though the frame did not ask for it (an owner's answer says nothing; a copy's would say local)", via, id, served)
			}
		}
		// sendSettled resends the whole frame while a lease or majority is unconfirmed, and the
		// counter counts every resend, so the count is a whole number of frames, at least one.
		if got := c.Node(via).GetManyStats().Keys.Load() - before; got < int64(len(asked)) || got%int64(len(asked)) != 0 {
			t.Errorf("via %s: the node counted %d keys for frames of %d", via, got, len(asked))
		}

		// local: true on every key. The node's own keys and the keys of a pair it is only a
		// copy of say where they came from; a key it neither owns nor copies goes to the owner.
		localReads := make([]wire.RingRead, len(ids))
		for i, id := range ids {
			localReads[i] = client.read(proofDB, proofBucket, id, true)
		}
		reply = client.sendSettled(wire.RingPrologue{}, localReads)
		for i, id := range ids {
			if docID(reply.Answers[i]) != id {
				t.Fatalf("via %s: local key %s answered %+v", via, id, reply.Answers[i])
			}
			want := "owner"
			if copies[id] == via {
				want = "local"
			}
			if got := reply.Answers[i].Reply.Served; got != want {
				t.Errorf("via %s: local key %s (owner %s, copy %s) says served_from %q, want %q", via, id, owners[id], copies[id], got, want)
			}
		}
	}
}

// TestGetManyDeadOwnerAnswersOnlyItsKeys: with an owner down and detection off (so it is still the
// owner), the keys it owns answer the lone read's own refusal, naming it, and the keys of the live
// owners answer their documents. The frame is a reply, never a refusal, and its outcome is partial.
func TestGetManyDeadOwnerAnswersOnlyItsKeys(t *testing.T) {
	c, ids := getManyCluster(t, 60)
	owners, _ := ownersOf(t, c, proofDB, ids)
	client := newGetManyClient(t, c, "n1")
	// One frame while every node is up: n1 learns which database holds the bucket id, which it
	// cannot ask a dead member for afterwards (the lone read has the same dependence).
	if warm := client.sendSettled(wire.RingPrologue{}, client.reads(ids...)); warm.Outcome != "ok" {
		t.Fatalf("the warm-up frame answered %q", warm.Outcome)
	}
	c.Kill("n3")
	for _, shape := range []string{"first failure", "second frame, owner in backoff"} {
		reply := client.send(wire.RingPrologue{}, client.reads(ids...))
		dead, live := 0, 0
		for i, id := range ids {
			answer := reply.Answers[i]
			if owners[id] != "n3" {
				live++
				if docID(answer) != id {
					t.Errorf("%s: %s (owner %s, alive) answered %+v", shape, id, owners[id], answer)
				}
				continue
			}
			dead++
			if answer.Error == nil {
				t.Errorf("%s: %s is owned by the dead n3 and was answered a reply", shape, id)
				continue
			}
			// The same refusal as the lone read gives (BUG-0753): coded UNAVAILABLE, naming the
			// owner and quoting 'nodes', saying nothing was changed.
			_, lone := c.Node("n1").Get(proofCtx(t), proofDB, proofBucket, id)
			var coded *cluster.Error
			if !asCoded(lone, &coded) || coded.Code != answer.Error.Code {
				t.Errorf("%s: %s: the frame answered %s, the lone read %v", shape, id, answer.Error.Code, lone)
			}
			if answer.Error.Code != cluster.CodeUnavailable {
				t.Errorf("%s: %s answered %s (%s), want UNAVAILABLE", shape, id, answer.Error.Code, answer.Error.Message)
			}
			for _, want := range []string{"n3", "'nodes'", "Nothing was changed"} {
				if !strings.Contains(answer.Error.Message, want) {
					t.Errorf("%s: %s: message %q lacks %q", shape, id, answer.Error.Message, want)
				}
			}
		}
		if dead == 0 || live == 0 {
			t.Fatalf("%s: the sample had %d dead-owner and %d live-owner keys; it needs both", shape, dead, live)
		}
		if reply.Outcome != "partial" {
			t.Errorf("%s: outcome %q, want partial", shape, reply.Outcome)
		}
	}
	if n := c.Node("n1").GetManyStats().Errors.Load(); n == 0 {
		t.Errorf("the node counted no per-key error for a frame with dead-owner keys")
	}
}

// TestGetManyAcrossDatabasesWithDifferentRF: one frame names documents of an RF 1 database and of
// an RF 2 database, in an interleaved order. Each key is answered from its own owner set.
func TestGetManyAcrossDatabasesWithDifferentRF(t *testing.T) {
	c := proofSetup(t, harness.Options{RaftTimeout: 400 * time.Millisecond})
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(proofCtx(t), "solo", 1); err != nil {
		t.Fatalf("creating the RF 1 database: %v", err)
	}
	c.AwaitConverged(10 * time.Second)
	var bucketErr error
	for range 20 {
		c.AwaitLeases(30 * time.Second)
		if _, bucketErr = leader.CreateBucket(proofCtx(t), "solo", proofBucket, wire.Field{Name: "name", Type: "string"}); !harness.IsTransientRefusal(bucketErr) {
			break
		}
	}
	if bucketErr != nil {
		t.Fatalf("creating the bucket: %v", bucketErr)
	}
	c.AwaitLeases(30 * time.Second)
	var ids []string
	for i := range 24 {
		id := fmt.Sprintf("two-%02d", i)
		ids = append(ids, id)
		getManyInsert(t, c, "n1", proofDB, id)
		getManyInsert(t, c, "n1", "solo", id)
	}
	awaitOnCopies(t, c, proofDB, ids)
	ownersTwo, _ := ownersOf(t, c, proofDB, ids)
	ownersOne, copiesOne := ownersOf(t, c, "solo", ids)
	for _, id := range ids {
		if copiesOne[id] != "" {
			t.Fatalf("the RF 1 database gave %s a copy on %s", id, copiesOne[id])
		}
		if _, found, err := c.Node(ownersOne[id]).LocalDocument(proofCtx(t), "solo", proofBucket, id); err != nil || !found {
			t.Fatalf("the RF 1 owner %s does not hold %s: %v", ownersOne[id], id, err)
		}
	}
	for _, via := range c.IDs() {
		client := newGetManyClient(t, c, via)
		var reads []wire.RingRead
		var want, wantName []string
		for _, id := range ids {
			reads = append(reads, client.read(proofDB, proofBucket, id, false), client.read("solo", proofBucket, id, false))
			want, wantName = append(want, id, id), append(wantName, proofDB+"/"+id, "solo/"+id)
		}
		reads = append(reads, client.read("solo", proofBucket, "two-absent", false))
		want, wantName = append(want, ""), append(wantName, "")
		reply := client.sendSettled(wire.RingPrologue{}, reads)
		if reply.Outcome != "ok" {
			for i, answer := range reply.Answers {
				if answer.Error != nil {
					t.Errorf("via %s: key %d answered %s: %s", via, i, answer.Error.Code, answer.Error.Message)
				}
			}
			t.Fatalf("via %s: outcome %q (%v)", via, reply.Outcome, c.BackgroundErrors())
		}
		for i := range reads {
			if got := docName(reply.Answers[i]); got != wantName[i] {
				t.Errorf("via %s: key %d wants the document named %q and was answered %q: the key was read from the wrong database", via, i, wantName[i], got)
			}
			if got := docID(reply.Answers[i]); got != want[i] {
				t.Errorf("via %s: key %d wants %q and was answered %q (RF 2 owner %s, RF 1 owner %s)", via, i, want[i], got, ownersTwo[ids[min(i/2, len(ids)-1)]], ownersOne[ids[min(i/2, len(ids)-1)]])
			}
		}
	}
}

// TestGetManyReadsYourAckedWrite: on an RF 1 database (the pipelined write path), a write answered
// applied is visible to the next get-many through every node, and so is the update that follows.
// The loop is the "20 runs" of the spec: each round is a fresh write and a fresh read.
func TestGetManyReadsYourAckedWrite(t *testing.T) {
	c := proofSetup(t, harness.Options{RaftTimeout: 400 * time.Millisecond})
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(proofCtx(t), "solo", 1); err != nil {
		t.Fatalf("creating the RF 1 database: %v", err)
	}
	c.AwaitConverged(10 * time.Second)
	var bucketErr error
	for range 20 {
		c.AwaitLeases(30 * time.Second)
		if _, bucketErr = leader.CreateBucket(proofCtx(t), "solo", proofBucket, wire.Field{Name: "name", Type: "string"}); !harness.IsTransientRefusal(bucketErr) {
			break
		}
	}
	if bucketErr != nil {
		t.Fatalf("creating the bucket: %v", bucketErr)
	}
	c.AwaitLeases(30 * time.Second)
	clients := map[string]*getManyClient{}
	for _, node := range c.IDs() {
		clients[node] = newGetManyClient(t, c, node)
	}
	var written []string
	for round := range 20 {
		id := fmt.Sprintf("ack-%02d", round)
		writer := c.IDs()[round%3]
		getManyInsert(t, c, writer, "solo", id)
		written = append(written, id)
		for _, node := range c.IDs() {
			reads := make([]wire.RingRead, len(written))
			for i, earlier := range written {
				reads[i] = clients[node].read("solo", proofBucket, earlier, false)
			}
			reply := clients[node].sendSettled(wire.RingPrologue{}, reads)
			for i, earlier := range written {
				if got := docID(reply.Answers[i]); got != earlier {
					t.Fatalf("round %d: a write of %s through %s was answered applied, and get-many through %s then answered %q for %s", round, id, writer, node, got, earlier)
				}
			}
		}
		if _, err := c.Node(writer).Upsert(proofCtx(t), "solo", proofBucket, docOf(id, "round "+id)); err != nil {
			t.Fatalf("round %d: upserting %s: %v", round, id, err)
		}
		for _, node := range c.IDs() {
			reply := clients[node].sendSettled(wire.RingPrologue{}, []wire.RingRead{clients[node].read("solo", proofBucket, id, false)})
			if got := docName(reply.Answers[0]); got != "round "+id {
				t.Fatalf("round %d: after an acknowledged update of %s, get-many through %s read the name %q", round, id, node, got)
			}
		}
	}
}
