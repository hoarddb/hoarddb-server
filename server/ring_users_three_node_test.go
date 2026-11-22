package server

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// The cluster's users across three real members (ring user store step M2b.3, spec 11 M2b list):
// a user created through one member logs in on every other once that member has installed the
// change (its watermark, never a sleep), through a leader change, a restart of every node, a member
// that was down, a joiner that receives the table in a snapshot, and two creates of one name at
// once; revocation reaches every node; a login needs no quorum, and a change without one answers
// the server's own text through a default client within the test budget.

var threeNodeIDs = []string{"node-a", "node-b", "node-c"}

// threeMembers starts node-a with the root password, joins node-b and node-c as voting data
// members and returns the host and root's session on node-a.
func threeMembers(t *testing.T, budget time.Duration) (*trio, *ringclient.Client) {
	t.Helper()
	tr := newTrio(t, threeNodeIDs, nil)
	tr.budget = budget
	tr.start(rootTestPassword, "node-a")
	tr.start("", "node-b", "node-c")
	root := tr.login("node-a", "root", rootTestPassword, 30*time.Second)
	tr.join(root, "node-b", "node-c")
	return tr, root
}

// createUser creates name through client with the drop fence it reads first, and returns the
// state_seq every member must install before name logs in there.
func createUser(t *testing.T, client *ringclient.Client, name, password string, roles []ringclient.UserGrant) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	list, err := client.ShowUsers(ctx)
	if err != nil {
		t.Fatalf("show users before creating %s: %v", name, err)
	}
	result, err := client.CreateUser(ctx, name, password, roles, list.DropSeq)
	if err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	return result.Revision
}

// installedEverywhere waits until every running member listed has installed seq.
func (tr *trio) installedEverywhere(seq int64, ids ...string) {
	tr.t.Helper()
	for _, id := range ids {
		tr.awaitInstalled(id, seq)
	}
}

// awaitVoters waits until the committed state lists every id as a voter, so the members left
// after one stops can elect a leader.
func (tr *trio) awaitVoters(timeout time.Duration, ids ...string) {
	tr.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		voters := tr.qs(ids[0]).ringNode.State().Voters
		missing := 0
		for _, id := range ids {
			if !slices.Contains(voters, id) {
				missing++
			}
		}
		if missing == 0 {
			return
		}
		if time.Now().After(deadline) {
			tr.t.Fatalf("after %v the voters are %v, want %v", timeout, voters, ids)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

var everyDatabaseRoot = []ringclient.UserGrant{{Role: "root", DB: "*"}}

// TestAWitnessRefusesAuthWithTheWitnessText (B6): a witness answers every password login with its
// own text, before checking anything, so no session (and no statement) ever runs through it.
// Break: let a witness fall through to the fetching text; red.
func TestAWitnessRefusesAuthWithTheWitnessText(t *testing.T) {
	tr := newTrio(t, []string{"node-a", "node-b", "node-w"}, nil)
	tr.nodes["node-w"].cfg.Cluster.Witness = true
	tr.start(rootTestPassword, "node-a")
	tr.start("", "node-b", "node-w")
	root := tr.login("node-a", "root", rootTestPassword, 30*time.Second)
	tr.join(root, "node-b", "node-w")
	tr.awaitInstalled("node-w", tr.qs("node-a").ringNode.Users().StateSeq)
	for _, password := range []string{rootTestPassword, "a-wrong-password"} {
		_, err := tr.dial("node-w", "root", password)
		if err == nil || !strings.Contains(err.Error(), "this node is a witness: it holds no password key and serves no logins or statements") {
			t.Fatalf("a login on the witness with password %q answered %v, want the witness text", password, err)
		}
	}
}

// TestAUserCreatedOnNodeALogsInOnNodeB: the table is the cluster's; node-b serves the login once it
// has installed the create.
func TestAUserCreatedOnNodeALogsInOnNodeB(t *testing.T) {
	tr, root := threeMembers(t, 0)
	seq := createUser(t, root, "ada", "ada-password-12", nil)
	tr.awaitInstalled("node-b", seq)
	if _, err := tr.dial("node-b", "ada", "ada-password-12"); err != nil {
		t.Fatalf("ada does not log in on node-b once it installed the create: %v", err)
	}
}

// TestAWrongPasswordIsRefusedOnEveryNode: every member checks the same peppered hash.
func TestAWrongPasswordIsRefusedOnEveryNode(t *testing.T) {
	tr, root := threeMembers(t, 0)
	tr.installedEverywhere(createUser(t, root, "ada", "ada-password-12", nil), threeNodeIDs...)
	for _, id := range threeNodeIDs {
		if _, err := tr.dial(id, "ada", "ada-password-13"); err == nil {
			t.Fatalf("a wrong password logs in on %s", id)
		}
		if _, err := tr.dial(id, "ada", "ada-password-12"); err != nil {
			t.Fatalf("the right password does not log in on %s: %v", id, err)
		}
	}
}

// TestAUserCreatedBeforeALeaderChangeLogsInAfterIt: the leader stops; the two members left elect
// another, keep the users, and take a new one.
func TestAUserCreatedBeforeALeaderChangeLogsInAfterIt(t *testing.T) {
	tr, root := threeMembers(t, 0)
	tr.installedEverywhere(createUser(t, root, "ada", "ada-password-12", everyDatabaseRoot), threeNodeIDs...)
	tr.awaitVoters(30*time.Second, threeNodeIDs...)
	old := tr.leader(10 * time.Second)
	tr.stop(old)
	var left []string
	for _, id := range threeNodeIDs {
		if id != old {
			left = append(left, id)
		}
	}
	deadline := time.Now().Add(30 * time.Second)
	for leader := tr.leader(30 * time.Second); leader == old; leader = tr.leader(30 * time.Second) {
		if time.Now().After(deadline) {
			t.Fatal("no other member took over")
		}
	}
	for _, id := range left {
		tr.login(id, "ada", "ada-password-12", 10*time.Second)
	}
	ada := tr.login(left[0], "ada", "ada-password-12", 10*time.Second)
	seq := createUser(t, ada, "bob", "bob-password-12", nil)
	tr.installedEverywhere(seq, left...)
	tr.login(left[1], "bob", "bob-password-12", 10*time.Second)
}

// TestUsersSurviveARestartOfEveryNode: every member stops; started again, each serves the users
// from its own Raft state.
func TestUsersSurviveARestartOfEveryNode(t *testing.T) {
	tr, root := threeMembers(t, 0)
	tr.installedEverywhere(createUser(t, root, "ada", "ada-password-12", nil), threeNodeIDs...)
	tr.stop(threeNodeIDs...)
	tr.start("", threeNodeIDs...)
	for _, id := range threeNodeIDs {
		tr.login(id, "ada", "ada-password-12", 30*time.Second)
		tr.login(id, "root", rootTestPassword, 30*time.Second)
	}
}

// TestANodeDownDuringACreateLogsTheUserInAfterItCatchesUp: node-c is down while ada is created;
// started again, it installs the create and serves ada's login.
func TestANodeDownDuringACreateLogsTheUserInAfterItCatchesUp(t *testing.T) {
	tr, root := threeMembers(t, 0)
	tr.stop("node-c")
	seq := createUser(t, root, "ada", "ada-password-12", nil)
	tr.start("", "node-c")
	tr.awaitInstalled("node-c", seq)
	if _, err := tr.dial("node-c", "ada", "ada-password-12"); err != nil {
		t.Fatalf("ada does not log in on node-c after it caught up: %v", err)
	}
}

// TestAJoinerReceivesTheUsersFromASnapshot: node-a's log is compacted past its start (more than
// the 64 trailing entries a snapshot keeps), so node-b joins from the snapshot, which carries the
// table, and serves every user.
func TestAJoinerReceivesTheUsersFromASnapshot(t *testing.T) {
	tr := newTrio(t, []string{"node-a", "node-b"}, nil)
	tr.start(rootTestPassword, "node-a")
	tr.start("", "node-b")
	root := tr.login("node-a", "root", rootTestPassword, 30*time.Second)
	createUser(t, root, "ada", "ada-password-12", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	grant := ringclient.UserGrant{Role: "reader", DB: "app"}
	for range 40 {
		if _, err := root.GrantRole(ctx, "ada", grant, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := root.RevokeRole(ctx, "ada", grant, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := tr.qs("node-a").ringNode.TakeRaftSnapshot(); err != nil {
		t.Fatal(err)
	}
	snapshots := filepath.Join(tr.nodes["node-b"].cfg.Storage.DataDir, ringDirName, "raft", "snapshots")
	tr.join(root, "node-b")
	entries, err := os.ReadDir(snapshots)
	if err != nil || len(entries) == 0 {
		t.Fatalf("node-b holds no Raft snapshot after joining a compacted log (%v): it did not join from one", err)
	}
	tr.awaitInstalled("node-b", tr.qs("node-a").ringNode.Users().StateSeq)
	tr.login("node-b", "ada", "ada-password-12", 10*time.Second)
	tr.login("node-b", "root", rootTestPassword, 10*time.Second)
}

// TestConcurrentCreatesOfOneNameOnTwoNodesCommitOne (spec 7.2): ada is created through node-a and
// node-b at once, with different passwords; one create commits, the other is refused, and every
// node holds the one row. The loser's refusal is USER_EXISTS, not CONFLICT (spec 4.2): CONFLICT
// tells a wrapper to retry, and a rerun of this create can never succeed.
func TestConcurrentCreatesOfOneNameOnTwoNodesCommitOne(t *testing.T) {
	tr, rootA := threeMembers(t, 0)
	rootB := tr.login("node-b", "root", rootTestPassword, 30*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	list, err := rootA.ShowUsers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, client := range []*ringclient.Client{rootA, rootB} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = client.CreateUser(ctx, "ada", []string{"ada-password-aa", "ada-password-bb"}[i], nil, list.DropSeq)
		}()
	}
	wg.Wait()
	if (errs[0] == nil) == (errs[1] == nil) {
		t.Fatalf("two creates of one name answered %v and %v, want exactly one success", errs[0], errs[1])
	}
	winner := 0
	if errs[0] != nil {
		winner = 1
	}
	var refused *ringclient.Error
	if !errors.As(errs[1-winner], &refused) || refused.Code != cluster.CodeUserExists {
		t.Fatalf("the losing create answered %v, want a USER_EXISTS", errs[1-winner])
	}
	tr.installedEverywhere(tr.qs("node-a").ringNode.Users().StateSeq, threeNodeIDs...)
	for _, id := range threeNodeIDs {
		if n := len(tr.qs(id).ringNode.Users().Users); n != 2 {
			t.Fatalf("%s holds %d users, want root and one ada", id, n)
		}
		tr.login(id, "ada", []string{"ada-password-aa", "ada-password-bb"}[winner], 10*time.Second)
	}
}

// sessionEnds waits until client's session is refused or closed.
func sessionEnds(t *testing.T, client *ringclient.Client, where string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := client.DescribeNode(ctx)
		cancel()
		if err != nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the session on %s still answers 10 s after the change", where)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestDroppingAUserEndsItsSessionsOnEveryNode: ada is logged in on every member; dropping her
// through node-a ends each session when that member applies the drop.
func TestDroppingAUserEndsItsSessionsOnEveryNode(t *testing.T) {
	tr, root := threeMembers(t, 0)
	tr.installedEverywhere(createUser(t, root, "ada", "ada-password-12", everyDatabaseRoot), threeNodeIDs...)
	sessions := map[string]*ringclient.Client{}
	for _, id := range threeNodeIDs {
		sessions[id] = tr.login(id, "ada", "ada-password-12", 10*time.Second)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := root.DropUser(ctx, "ada", nil); err != nil {
		t.Fatal(err)
	}
	for id, session := range sessions {
		sessionEnds(t, session, id)
	}
}

// TestANewPasswordEndsTheOldSessionsOnEveryNode: a password change through node-b ends ada's
// sessions on every member; the new password logs in everywhere and the old one nowhere.
func TestANewPasswordEndsTheOldSessionsOnEveryNode(t *testing.T) {
	tr, root := threeMembers(t, 0)
	tr.installedEverywhere(createUser(t, root, "ada", "ada-password-12", everyDatabaseRoot), threeNodeIDs...)
	sessions := map[string]*ringclient.Client{}
	for _, id := range threeNodeIDs {
		sessions[id] = tr.login(id, "ada", "ada-password-12", 10*time.Second)
	}
	rootB := tr.login("node-b", "root", rootTestPassword, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	password := "ada-password-34"
	changed, err := rootB.AlterUser(ctx, "ada", ringclient.UserChange{Password: &password})
	if err != nil {
		t.Fatal(err)
	}
	for id, session := range sessions {
		sessionEnds(t, session, id)
	}
	tr.installedEverywhere(changed.Revision, threeNodeIDs...)
	for _, id := range threeNodeIDs {
		if _, err := tr.dial(id, "ada", "ada-password-12"); err == nil {
			t.Fatalf("the old password still logs in on %s", id)
		}
		tr.login(id, "ada", password, 10*time.Second)
	}
}

// TestALoginWithoutAQuorumIsServedFromTheLocalTable (spec 6.4): with the two other members down,
// node-a has no quorum and still checks logins against the table it applied.
func TestALoginWithoutAQuorumIsServedFromTheLocalTable(t *testing.T) {
	tr, root := threeMembers(t, 0)
	tr.installedEverywhere(createUser(t, root, "ada", "ada-password-12", nil), threeNodeIDs...)
	tr.stop("node-b", "node-c")
	if _, err := tr.dial("node-a", "ada", "ada-password-12"); err != nil {
		t.Fatalf("a login on a member without a quorum was refused: %v", err)
	}
	if _, err := tr.dial("node-a", "ada", "ada-password-13"); err == nil {
		t.Fatal("a wrong password logs in on a member without a quorum")
	}
}

// TestAUserChangeWithoutAQuorumAnswersTheServersTextThroughADefaultClient (B4, M2b.2): with the
// other members down, a create through a default client on node-a answers the server's no-quorum
// text, naming the test's budget, well before the client's own 30 s deadline; never the client's
// deadline text. Break: leave the budget unplumbed; red (the text names 20 seconds and the call
// takes 20 s).
func TestAUserChangeWithoutAQuorumAnswersTheServersTextThroughADefaultClient(t *testing.T) {
	tr, root := threeMembers(t, 2*time.Second)
	tr.stop("node-b", "node-c")
	ctx, cancel := context.WithTimeout(context.Background(), ringclient.DefaultRequestTimeout+10*time.Second)
	defer cancel()
	began := time.Now()
	_, err := root.CreateUser(ctx, "ada", "ada-password-12", nil, tr.qs("node-a").ringNode.Users().DropSeq)
	took := time.Since(began)
	if err == nil {
		t.Fatal("a create without a quorum succeeded")
	}
	text := err.Error()
	decided := strings.Contains(text, `create user "ada" was not applied: no leader of the cluster answered within 2 seconds, so nothing changed.`)
	undecided := strings.Contains(text, `create user "ada" may or may not have been applied: this node sent it, but no answer came back within 2 seconds.`)
	if !decided && !undecided {
		t.Fatalf("a create without a quorum answered %q, want the server's no-quorum text naming the 2 s budget", text)
	}
	if took > 15*time.Second {
		t.Fatalf("the answer took %v; the 2 s budget should end it in seconds", took)
	}
}
