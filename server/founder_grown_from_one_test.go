package server

import (
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// founderGrowBuckets is how many buckets the founder holds before the join:
// enough that the joiner owns several of them on the joined ring.
const founderGrowBuckets = 20

// founderGrowSettleBound is how long the 1 to 2 join is given to settle. With
// the take-over worker running on every member it settles in about two
// seconds on loopback; a 3 to 4 join on the same harness takes as long.
const founderGrowSettleBound = 30 * time.Second

// TestAClusterGrownFromOneNodeSettlesItsFirstJoin is BUG-0498. A node started
// as a one-member cluster returns from Cluster.Start before it starts its
// take-over worker (runTakeOver), and nothing starts the worker when a join
// makes the cluster two members. The founder's copies of the buckets the join
// moved to the joiner keep their moved mark and never take the joiner's term,
// so the founder's own hand-off waits on itself ("n1 cannot finish handing off
// until n1 answers"), the pause never lifts, and the joiner's stream to the
// founder stalls on the new term: a write the joiner acknowledges at majority
// is held on the joiner alone. Guard: a founder that never took a moved
// bucket's term passes the first check only if the pause is judged wrongly,
// and the second only if the write reaches it.
func TestAClusterGrownFromOneNodeSettlesItsFirstJoin(t *testing.T) {
	tc := newTestCluster(t, 1, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	s := &settlingCluster{tc: tc, ports: reserveNodePorts(t)}
	joined := s.joinedRing(t)
	movedToJoiner := bucketNamed(t, "moved", func(name string) bool {
		return ownerOn(joined, name) == s.ports.ringAddr
	})
	s.buckets = append(s.buckets, movedToJoiner)
	for index := 0; index < founderGrowBuckets; index++ {
		s.buckets = append(s.buckets, fmt.Sprintf("grown%03d", index))
	}
	for _, name := range s.buckets {
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: name, Key: "doc", Data: map[string]interface{}{"n": 1}})
	}
	s.join(t, 0)

	founder := clusterOf(tc, 0)
	if !joinSettles(s, founderGrowSettleBound) {
		report := founder.joinFreeze.Load()
		t.Errorf("the 1 to 2 join did not settle within %v: the founder's report stalls %v waiting on %v, %d buckets of the change copied",
			founderGrowSettleBound, report.Stalled, report.Awaited, report.Copied)
	}

	// A write the joiner acknowledges at majority, with both members up, is on
	// both copies: the founder's copy is in the bucket's set (RF=3, two members).
	put := tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: movedToJoiner, Key: "after-join",
		Data: map[string]interface{}{"n": 2}, WriteConcern: "majority"})
	var answer InsertResponse
	if err := bson.Unmarshal(put.Payload, &answer); err != nil || !answer.Success {
		t.Fatalf("the put at majority through the founder: %v, %+v", err, answer)
	}
	if answer.CopiesConfirmed < 2 {
		t.Errorf("a put at majority to bucket %q, owned by the joiner, was acknowledged on %d of the 2 members' copies",
			movedToJoiner, answer.CopiesConfirmed)
	}
	var onFounder bool
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline) && !onFounder; time.Sleep(100 * time.Millisecond) {
		found, err := tc.nodes[0].server.core.Find(FindRequest{Database: testDatabase, Bucket: movedToJoiner, Key: "after-join"})
		if err != nil {
			t.Fatalf("reading the founder's copy of %q: %v", movedToJoiner, err)
		}
		onFounder = found.Found
	}
	if !onFounder {
		t.Errorf("the acknowledged put to %q never reached the founder's copy: the joiner holds it alone", movedToJoiner)
	}
}

// joinSettles wakes every member until none reports the membership change
// paused, and reports whether that happened within bound.
func joinSettles(s *settlingCluster, bound time.Duration) bool {
	for deadline := time.Now().Add(bound); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		wakeAll(s.members())
		paused := false
		for _, member := range s.members() {
			paused = paused || member.joinFreeze.Load().Paused
		}
		if !paused {
			return true
		}
	}
	return false
}

// startLoneFounder starts one node at factor 3, alone, holding
// founderGrowBuckets buckets of one document each, with a joiner's ports
// reserved. configure and tune are newTestClusterTuned's hooks and may be nil.
func startLoneFounder(t *testing.T, configure func(index int, cfg *config.Config), tune func(index int, c *Cluster)) *settlingCluster {
	t.Helper()
	tc := newTestClusterTuned(t, 1, 3, configure, tune)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	s := &settlingCluster{tc: tc, ports: reserveNodePorts(t)}
	for index := 0; index < founderGrowBuckets; index++ {
		name := fmt.Sprintf("grown%03d", index)
		s.buckets = append(s.buckets, name)
		tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: name, Type: BucketTypeHash})
		tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: name, Key: "doc", Data: map[string]interface{}{"n": index}})
	}
	return s
}

// joinAndSettle joins the reserved node through the founder and fails the
// test unless the change settles within founderGrowSettleBound.
func (s *settlingCluster) joinAndSettle(t *testing.T, what string) {
	t.Helper()
	s.join(t, 0)
	if !joinSettles(s, founderGrowSettleBound) {
		report := clusterOf(s.tc, 0).joinFreeze.Load()
		t.Fatalf("%s did not settle within %v: the founder's report stalls %v waiting on %v, %d buckets of the change copied",
			what, founderGrowSettleBound, report.Stalled, report.Awaited, report.Copied)
	}
}

// admitJoiner makes the joiner one of tc's nodes, pinned by the next joiner and
// pinning it, and reserves the next joiner's ports: the next join goes through
// the founder like the first, with no node restarted in between.
func (s *settlingCluster) admitJoiner(t *testing.T) {
	t.Helper()
	s.tc.nodes = append(s.tc.nodes, s.joiner)
	s.tc.addrs = append(s.tc.addrs, s.ports.ringAddr)
	s.tc.fingerprints = append(s.tc.fingerprints, preGeneratedNodeFingerprints(t, []string{s.joiner.cfg.Storage.DataDir})...)
	s.joiner = nil
	s.ports = reserveNodePorts(t)
}

// TestAFounderGrownFromOneNodeSettlesASecondJoinWithoutARestart is BUG-0498's
// second acceptance test: the founder of a cluster grown from one node to two
// takes a third node, with no member restarted, and the 2 to 3 join settles
// too; a write at all to a bucket the third node owns is then on every copy,
// the founder's included. Guard: a founder whose take-over worker started only
// at Start, or only once for the first join's buckets, holds this join's moved
// copies back as it held the first's.
func TestAFounderGrownFromOneNodeSettlesASecondJoinWithoutARestart(t *testing.T) {
	s := startLoneFounder(t, nil, nil)
	s.joinAndSettle(t, "the 1 to 2 join")
	s.admitJoiner(t)

	joined := s.joinedRing(t)
	movedToThird := bucketNamed(t, "third", func(name string) bool {
		return ownerOn(joined, name) == s.ports.ringAddr
	})
	s.tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: movedToThird, Type: BucketTypeHash})
	s.tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: movedToThird, Key: "doc", Data: map[string]interface{}{"n": 1}})
	waitForServing(t, s.tc, testDatabase, movedToThird, 2)
	s.buckets = append(s.buckets, movedToThird)
	s.joinAndSettle(t, "the 2 to 3 join, with the founder not restarted")

	put := s.tc.frame(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: movedToThird, Key: "after-third",
		Data: map[string]interface{}{"n": 3}, WriteConcern: "all"})
	var answer InsertResponse
	if err := bson.Unmarshal(put.Payload, &answer); err != nil || !answer.Success {
		t.Fatalf("the put at all through the founder: %v, %+v", err, answer)
	}
	if answer.CopiesConfirmed < 3 {
		t.Errorf("a put at all to bucket %q, owned by the third node, was acknowledged on %d of the 3 members' copies",
			movedToThird, answer.CopiesConfirmed)
	}
	var onFounder bool
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline) && !onFounder; time.Sleep(100 * time.Millisecond) {
		found, err := s.tc.nodes[0].server.core.Find(FindRequest{Database: testDatabase, Bucket: movedToThird, Key: "after-third"})
		if err != nil {
			t.Fatalf("reading the founder's copy of %q: %v", movedToThird, err)
		}
		onFounder = found.Found
	}
	if !onFounder {
		t.Errorf("the acknowledged put to %q never reached the founder's copy", movedToThird)
	}
}

// loneFounderWithAuthMaster starts a lone founder (with fastAuthLoop) and
// reserves joiner ports such that the joined ring makes the founder the auth
// master (founderMaster) or the joiner. Which of the two owns the reserved
// auth key depends only on their addresses, and some founder addresses own it
// against nearly every joiner, so a founder is replaced when 32 joiners do not
// give the wanted master.
func loneFounderWithAuthMaster(t *testing.T, founderMaster bool) *settlingCluster {
	t.Helper()
	for founders := 0; founders < 8; founders++ {
		s := startLoneFounder(t, fastAuthLoop, nil)
		founderID := clusterOf(s.tc, 0).self.ID
		for joiners := 0; joiners < 32; joiners++ {
			master, err := s.joinedRing(t).Owner(reservedPlacement(authReservedKey).encoded)
			if err != nil {
				t.Fatalf("placing the auth key on the joined ring: %v", err)
			}
			if (master.ID == founderID) == founderMaster {
				return s
			}
			s.ports = reserveNodePorts(t)
		}
		s.tc.stop()
	}
	t.Fatalf("setup: no founder and joiner addresses in 8 by 32 made the founder the auth master = %v", founderMaster)
	return nil
}

// fastAuthLoop shortens the auth-config loop's retry so a mastership change is
// picked up within a second.
func fastAuthLoop(_ int, cfg *config.Config) {
	cfg.Auth.AuthMasterRetrySeconds = 1
}

// TestAFounderGrownFromOneNodePushesItsAuthChangesToTheJoiner is BUG-0498's
// auth-config loop: a founder that stays the auth master after its first join
// pushes a user created after the join to the joiner. Guard: with no
// auth-config loop on the founder, nothing ever pushes, and the joiner never
// learns the user.
func TestAFounderGrownFromOneNodePushesItsAuthChangesToTheJoiner(t *testing.T) {
	s := loneFounderWithAuthMaster(t, true)
	// Not waited to settle: the take-over worker's test is the one above, and
	// this loop runs whether or not the join has settled.
	s.join(t, 0)

	founder := s.tc.nodes[0].server.core
	if err := founder.auth.CreateUser("grownuser", CredentialMaterial{
		Algo: "argon2id", Salt: testSalt("grownuser"), Hash: testHash("grownuser"), Params: defaultArgon2Params(),
	}, []Grant{{Role: "reader", DB: testDatabase}}, true); err != nil {
		t.Fatalf("creating a user on the founder: %v", err)
	}
	founder.scheduleAuthConfigPush()

	joiner := s.joiner.server.core
	pushed := false
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline) && !pushed; time.Sleep(100 * time.Millisecond) {
		_, pushed = joiner.auth.UserInfo("grownuser")
	}
	if !pushed {
		term, version := joiner.auth.ConfigTermVersion()
		t.Errorf("a user created on the founder, the auth master, after the join never reached the joiner within 10s (joiner at term %d version %d, founder at version %d)",
			term, version, founder.auth.Version())
	}
}

// TestAFounderGrownFromOneNodeFollowsTheJoinerAsAuthMaster is BUG-0498's
// auth-config loop the other way: when the join makes the joiner the auth
// master, the founder becomes its follower, subject to the staleness bound.
// Guard: with no auth-config loop on the founder, it keeps the master's role
// it took alone at start and is never held to the bound.
func TestAFounderGrownFromOneNodeFollowsTheJoinerAsAuthMaster(t *testing.T) {
	s := loneFounderWithAuthMaster(t, false)
	founder := s.tc.nodes[0].server.core
	if founder.auth.IsFollower() {
		t.Fatal("setup: the founder alone is already its auth master's follower")
	}
	// Not waited to settle: the take-over worker's test is the one above, and
	// this loop runs whether or not the join has settled.
	s.join(t, 0)

	following := false
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline) && !following; time.Sleep(100 * time.Millisecond) {
		following = founder.auth.IsFollower()
	}
	if !following {
		t.Errorf("the join made the joiner the auth master, and the founder did not become its follower within 10s")
	}
}

// TestAFounderGrownFromOneNodeRunsWALRetention is BUG-0498's retention loop: a
// founder grown from one node to two runs its timed retention pass. Guard: with
// no retention loop on the founder its log is never pruned for as long as it
// runs, and the pass's line never appears.
func TestAFounderGrownFromOneNodeRunsWALRetention(t *testing.T) {
	founderLog := &syncBuffer{}
	s := startLoneFounder(t,
		func(_ int, cfg *config.Config) { cfg.Server.WALRetentionSeconds = 1 },
		func(_ int, c *Cluster) {
			c.logger = slog.New(slog.NewTextHandler(founderLog, &slog.HandlerOptions{Level: slog.LevelInfo}))
		})
	// Not waited to settle: the take-over worker's test is the one above, and
	// this loop runs whether or not the join has settled.
	founderID := clusterOf(s.tc, 0).self.ID
	joined := s.joinedRing(t)
	keptByFounder := bucketNamed(t, "kept", func(name string) bool { return ownerOn(joined, name) == founderID })
	s.tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: keptByFounder, Type: BucketTypeHash})
	s.join(t, 0)
	// A node alone logs nothing (it writes straight to its store), so this put
	// is what the founder's log holds for a pass to prune.
	s.tc.ok(0, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: keptByFounder, Key: "after-join",
		Data: map[string]interface{}{"n": 2}, WriteConcern: "all"})

	const prunedLine = "WAL retention pruned acknowledged segments"
	pruned := false
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline) && !pruned; time.Sleep(200 * time.Millisecond) {
		pruned = strings.Contains(founderLog.String(), prunedLine)
	}
	if !pruned {
		t.Errorf("the founder ran no WAL retention pass within 15s of its first join (no %q line)", prunedLine)
	}
}
