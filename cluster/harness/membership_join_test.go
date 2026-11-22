package harness_test

import (
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
)

// TestJoinOnAnEmptyClusterIsActiveInOneCall: the transcript's first line, byte for byte, and the
// joiner is active, ranked and a voter when the call returns.
func TestJoinOnAnEmptyClusterIsActiveInOneCall(t *testing.T) {
	c := joinCluster(t, 1, 1, nil)
	joiner := c.OpenJoiner()
	got := mustJoin(t, c, "n1", joiner)
	want := fmt.Sprintf("Added %s@%s as a member: joining, then active when it holds the slots it will own (epoch 2)", joiner, c.Addr(joiner))
	if got.Result != want {
		t.Fatalf("the line is\n%s\nwant\n%s", got.Result, want)
	}
	state := c.AwaitConverged(15 * time.Second)
	member, ok := memberOf(state, joiner)
	if !ok || member.State != cluster.MemberActive || member.Suffrage != "voter" {
		t.Fatalf("after the call %s is %+v, want an active voter", joiner, member)
	}
	if got.Epoch != 2 || got.StateSeq != member.JoinedStateSeq || state.Epoch != 2 {
		t.Fatalf("outcome %+v, joined at %d, state epoch %d; want epoch 2 and the registration's position", got, member.JoinedStateSeq, state.Epoch)
	}
}

// TestTheJoinTranscriptPrintsThreeLines is the acceptance of queries section 8: three joins on a fresh
// cluster print the three lines, a data member that votes, a witness that votes, and a data member
// that does not because the cluster already has its three voters.
// [BREAK: JoinAutoPromotes never checks the cap in PromoteWithin].
func TestTheJoinTranscriptPrintsThreeLines(t *testing.T) {
	c := joinCluster(t, 1, 3, func(o *harness.Options) { o.Witnesses = []string{"n3"} })
	b, w, d := c.OpenJoiner(), c.OpenJoiner(), c.OpenJoiner() // n2, n3 (the witness), n4
	lines := []string{
		mustJoin(t, c, "n1", b).Result,
		mustJoin(t, c, "n1", w).Result,
		mustJoin(t, c, "n1", d).Result,
	}
	want := []string{
		fmt.Sprintf("Added %s@%s as a member: joining, then active when it holds the slots it will own (epoch 2)", b, c.Addr(b)),
		fmt.Sprintf("Added %s@%s as a witness: it votes and holds no documents (epoch 2)", w, c.Addr(w)),
		fmt.Sprintf("Added %s@%s as a member: joining (not a voter: the cluster already has its 3 voters) (epoch 3)", d, c.Addr(d)),
	}
	for i := range want {
		if lines[i] != want[i] {
			t.Errorf("line %d\n got %s\nwant %s", i+1, lines[i], want[i])
		}
	}
	state := c.AwaitConverged(15 * time.Second)
	if !slices.Equal(state.Voters, []string{"n1", "n2", "n3"}) {
		t.Fatalf("voters are %v, want n1 n2 n3", state.Voters)
	}
	for id, wantState := range map[string]cluster.MemberState{b: cluster.MemberActive, w: cluster.MemberActive, d: cluster.MemberActive} {
		if m, _ := memberOf(state, id); m.State != wantState {
			t.Errorf("%s is %s, want %s", id, m.State, wantState)
		}
	}
	if m, _ := memberOf(state, w); !m.Witness {
		t.Errorf("%s is not recorded as a witness: %+v", w, m)
	}
}

// TestAWitnessJoinIsActiveAtOnceAndVotes: a witness is active and votes without ranking, so the
// epoch does not move. The join never asks the witness to seed.
func TestAWitnessJoinIsActiveAtOnceAndVotes(t *testing.T) {
	c := joinCluster(t, 1, 1, func(o *harness.Options) { o.Witnesses = []string{"n2"} })
	joiner := c.OpenJoiner()
	before := c.Node("n1").State().Epoch
	got := mustJoin(t, c, "n1", joiner)
	state := c.AwaitConverged(15 * time.Second)
	member, _ := memberOf(state, joiner)
	if !member.Witness || member.State != cluster.MemberActive || !slices.Contains(state.Voters, joiner) {
		t.Fatalf("witness row %+v, voters %v", member, state.Voters)
	}
	if state.Epoch != before || got.Epoch != before {
		t.Fatalf("a witness moved the epoch from %d to %d (reply %d)", before, state.Epoch, got.Epoch)
	}
}

// TestJoinIsRefusedWhileADatabaseExists (J1). [BREAK: skip the database check in joinOnLeader].
func TestJoinIsRefusedWhileADatabaseExists(t *testing.T) {
	c := joinCluster(t, 1, 1, nil)
	if _, err := c.Node("n1").CreateDatabaseRF(proofCtx(t), "app", 1); err != nil {
		t.Fatal(err)
	}
	joiner := c.OpenJoiner()
	_, err := joinAt(t, c, "n1", joiner, false)
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument {
		t.Fatalf("a join with data present: %v", err)
	}
	for _, text := range []string{`database "app"`, "does not seed a joining node", "'create database app'"} {
		if !strings.Contains(refusal.Message, text) {
			t.Errorf("J1 text lacks %q:\n%s", text, refusal.Message)
		}
	}
	if _, ok := memberOf(c.Node("n1").State(), joiner); ok {
		t.Fatal("the refused join registered the node")
	}
}

// TestJoinRefusesAWitnessFlagTheNodeDoesNotHave (J2).
func TestJoinRefusesAWitnessFlagTheNodeDoesNotHave(t *testing.T) {
	c := joinCluster(t, 1, 1, nil)
	joiner := c.OpenJoiner()
	_, err := joinAt(t, c, "n1", joiner, true)
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || !strings.Contains(refusal.Message, "as a witness") || !strings.Contains(refusal.Message, "is configured as a member") {
		t.Fatalf("a witness flag on a data node: %v", err)
	}
	if _, ok := memberOf(c.Node("n1").State(), joiner); ok {
		t.Fatal("the refused join registered the node")
	}
}

// TestAJoinOfAKnownIdAtAnotherAddressIsConflict (J3).
func TestAJoinOfAKnownIdAtAnotherAddressIsConflict(t *testing.T) {
	c := joinCluster(t, 1, 1, nil)
	joiner := c.OpenJoiner()
	mustJoin(t, c, "n1", joiner)
	_, err := c.Membership("n1").Join(askCtx(t), joiner, "127.0.0.1:1", false)
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeConflict || !strings.Contains(refusal.Message, "replace-node "+joiner) {
		t.Fatalf("a join of a known id at another address: %v", err)
	}
}

// TestAJoinRetriedAfterALostReplyReturnsTheSameOutcome: the same join, run again at the leader a
// leader change later, answers the position the registration committed at, not the current one.
// [BREAK: answer the current state_seq instead of the member row's joined_state_seq].
func TestAJoinRetriedAfterALostReplyReturnsTheSameOutcome(t *testing.T) {
	c := joinCluster(t, 3, 2, nil)
	joiner := c.OpenJoiner()
	first := mustJoin(t, c, c.AwaitLeader(10*time.Second), joiner)
	// Another join moves the state past the first one's position before the retry, so an answer
	// built from the current state_seq differs from the first call's and the equality below is what
	// fails, not the guard at the end.
	mustJoin(t, c, c.AwaitLeader(10*time.Second), c.OpenJoiner())
	old := c.AwaitLeader(10 * time.Second)
	c.Kill(old)
	var leader string
	c.Await("a new leader", 20*time.Second, func() bool {
		for _, id := range c.Running() {
			if c.Node(id).IsLeader() {
				leader = id
				return true
			}
		}
		return false
	})
	again, err := joinAt(t, c, leader, joiner, false)
	if err != nil {
		t.Fatalf("the retried join at the new leader: %v", err)
	}
	// The line quotes the current epoch, which the second join legitimately moved; the position and
	// the rest of the line are the registration's.
	againLine, _, _ := strings.Cut(again.Result, " (epoch ")
	firstLine, _, _ := strings.Cut(first.Result, " (epoch ")
	if again.StateSeq != first.StateSeq || againLine != firstLine {
		t.Fatalf("the retry answered state_seq %d %q, the first call %d %q", again.StateSeq, again.Result, first.StateSeq, first.Result)
	}
	if c.Node(leader).State().StateSeq == first.StateSeq {
		t.Fatal("the state never moved past the registration, so the test cannot tell the positions apart")
	}
}

// TestPromoteOfAVoterReturnsTheCommittedFacts: promoting a node that votes commits nothing and
// answers the same position twice.
func TestPromoteOfAVoterReturnsTheCommittedFacts(t *testing.T) {
	c := joinCluster(t, 1, 1, nil)
	joiner := c.OpenJoiner()
	mustJoin(t, c, "n1", joiner)
	first, err := c.Membership("n1").Promote(askCtx(t), joiner)
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.Membership("n1").Promote(askCtx(t), joiner)
	if err != nil {
		t.Fatal(err)
	}
	if first.StateSeq != second.StateSeq || first.Epoch != second.Epoch || !strings.Contains(second.Result, "already votes") {
		t.Fatalf("promote twice: %+v then %+v", first, second)
	}
}

// TestPromoteAtTheCapIsRefusedNamingLeave (P1).
func TestPromoteAtTheCapIsRefusedNamingLeave(t *testing.T) {
	c := joinCluster(t, 1, 3, nil)
	for range 3 {
		mustJoin(t, c, "n1", c.OpenJoiner())
	}
	_, err := c.Membership("n1").Promote(askCtx(t), "n4")
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeInvalidArgument ||
		!strings.Contains(refusal.Message, "promote n4 would make 4 voters and cluster.voters is 3") || !strings.Contains(refusal.Message, "'leave <voter>'") {
		t.Fatalf("a promotion past the cap: %v", err)
	}
}

// TestTheJoinerAsksForActivationWhenItsPairsAreReady: a node registered but not activated asks the
// leader, on its own, once nothing is left to pull, and the leader's gate lets it in. This pins the
// MEMBERSHIP_RELAY{activate} send.
func TestTheJoinerAsksForActivationWhenItsPairsAreReady(t *testing.T) {
	c := joinCluster(t, 1, 1, func(o *harness.Options) { o.JoinerActivation = true })
	joiner := c.OpenJoiner()
	ctx := askCtx(t)
	if err := c.Node("n1").JoinRegisterOnly(ctx, joiner, c.Addr(joiner)); err != nil {
		t.Fatal(err)
	}
	c.Await(joiner+" to be activated at its own ask", 20*time.Second, func() bool {
		m, ok := memberOf(c.Node("n1").State(), joiner)
		return ok && m.State == cluster.MemberActive
	})
	if asks := c.Node(joiner).ActivationAsks(); asks < 1 {
		t.Fatalf("%s became active but never asked", joiner)
	}
}

// TestAJoinOfACompletedJoinIsAnsweredEvenWhenADatabaseExists (pi S9): J1 holds back a join that
// would make a node an owner of documents it does not hold; a repeat of a join that already
// completed makes nothing, so it is answered from the row, with the registration's position.
// [BREAK: check J1 before the known row in joinOnLeader]
func TestAJoinOfACompletedJoinIsAnsweredEvenWhenADatabaseExists(t *testing.T) {
	c := joinCluster(t, 1, 1, nil)
	joiner := c.OpenJoiner()
	first := mustJoin(t, c, "n1", joiner)
	if _, err := c.Node("n1").CreateDatabaseRF(proofCtx(t), "app", 1); err != nil {
		t.Fatal(err)
	}
	again, err := joinAt(t, c, "n1", joiner, false)
	if err != nil {
		t.Fatalf("a repeat of a completed join after a database exists: %v", err)
	}
	if again.StateSeq != first.StateSeq {
		t.Fatalf("the repeat answered state_seq %d, the join %d", again.StateSeq, first.StateSeq)
	}
}

// TestAJoinAsAWitnessOfADataMemberIsConflictNamingTheRole (pi N1): the flag disagrees with the row,
// which is not a node taken at another address, so the text says so.
// [BREAK: return idTaken for the witness mismatch]
func TestAJoinAsAWitnessOfADataMemberIsConflictNamingTheRole(t *testing.T) {
	c := joinCluster(t, 1, 1, nil)
	joiner := c.OpenJoiner()
	mustJoin(t, c, "n1", joiner)
	_, err := joinAt(t, c, "n1", joiner, true)
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeConflict {
		t.Fatalf("a witness join of a data member: %v", err)
	}
	if !strings.Contains(refusal.Message, "not a witness") || strings.Contains(refusal.Message, "already a member at") {
		t.Fatalf("the text is about an address, not the role:\n%s", refusal.Message)
	}
}

// TestRegisteringAJoinerRunsTheGatesOfJoin (pi S3): the only exported way to register a node besides
// the statement is JoinRegisterOnly, and it asks the node who it is and applies J3 like join. An
// address nobody answers at registers nothing. [BREAK: JoinRegisterOnly calls raft.Join directly]
func TestRegisteringAJoinerRunsTheGatesOfJoin(t *testing.T) {
	c := joinCluster(t, 1, 2, nil)
	ghost := c.OpenJoiner()
	if err := c.Node("n1").JoinRegisterOnly(askCtx(t), ghost, "127.0.0.1:1"); err == nil {
		t.Fatal("a registration at an address nobody answers at was accepted")
	}
	if _, ok := memberOf(c.Node("n1").State(), ghost); ok {
		t.Fatalf("the refused registration left a row for %s", ghost)
	}
	if err := c.Node("n1").JoinRegisterOnly(askCtx(t), ghost, c.Addr(ghost)); err != nil {
		t.Fatalf("the registration at the node's own address: %v", err)
	}
	err := c.Node("n1").JoinRegisterOnly(askCtx(t), ghost, "127.0.0.1:1")
	var refusal *cluster.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeConflict {
		t.Fatalf("a registration of a known id at another address: %v", err)
	}
}

// TestAJoinedMemberPublishesItsOwnClientAddr: the joiner states its client listener address in its
// identity reply and the leader records it in REGISTER_MEMBER, so the member's DriverAddr is the
// driver port and never its peer port; a joiner that states none has no driver address, and the
// leader does not make one up from its peer address.
// [BREAK: joinOnLeader builds the member without ClientAddr]
func TestAJoinedMemberPublishesItsOwnClientAddr(t *testing.T) {
	c := joinCluster(t, 1, 2, func(o *harness.Options) {
		o.ClientAddr = func(id, dataAddr string) string {
			if id == "n3" {
				return ""
			}
			host, _, err := net.SplitHostPort(dataAddr)
			if err != nil {
				t.Fatal(err)
			}
			return net.JoinHostPort(host, "7434")
		}
	})
	withClient, without := c.OpenJoiner(), c.OpenJoiner()
	want := c.Node(withClient).Identity().ClientAddr
	if want == "" || want == c.Addr(withClient) {
		t.Fatalf("the test needs a client address apart from the peer address, got %q (peer %q)", want, c.Addr(withClient))
	}
	mustJoin(t, c, "n1", withClient)
	mustJoin(t, c, "n1", without)
	state := c.Node("n1").State()
	row, ok := memberOf(state, withClient)
	if !ok || row.ClientAddr != want || row.DriverAddr() != want {
		t.Fatalf("the joined row is %+v, want client address %q", row, want)
	}
	row, ok = memberOf(state, without)
	if !ok || row.ClientAddr != "" || row.DriverAddr() != "" {
		t.Fatalf("a joiner that states no client address has the row %+v", row)
	}
}
