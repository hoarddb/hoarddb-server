package server

import (
	"bytes"
	"context"
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/argon2"

	"github.com/hoarddb/hoarddb-server/authrules"
	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/clusterpepper"
	"github.com/hoarddb/hoarddb-server/config"
)

// The root task (ring user store spec 6.5, step M2b.1): a cluster member started with a root
// password creates the cluster's first administrator through the cluster, once, whatever the order
// of starts; a node of a cluster that has users ignores the password with a warning; a witness
// never runs the task.

const (
	rootTestPassword = "root-password-1"
	notUsedWarning   = "HOARDDB_ROOT_PASSWORD was not used: this cluster already has users. Log in as an existing administrator, or unset it."
	witnessWarning   = "HOARDDB_ROOT_PASSWORD was not used: this node is a witness, which holds no password key and serves no logins. Set it on a data node instead."
	pepperCreatedLog = "created the cluster's password key"
)

// TestTheRootPasswordBootstrapsOnlyAnEmptyCluster: a cluster started without a root password has
// no users and says how to get one; started again with HOARDDB_ROOT_PASSWORD on its data node, it
// creates exactly one administrator, root on every database, who logs in. Break: let the task
// propose on a table that is not empty; red (a second start would conflict-loop or add a row).
func TestTheRootPasswordBootstrapsOnlyAnEmptyCluster(t *testing.T) {
	tr := newTrio(t, []string{"node-a"}, nil)
	tr.start("", "node-a")
	if _, err := tr.dial("node-a", "root", rootTestPassword); err == nil || !strings.Contains(err.Error(), "this cluster has no users yet") {
		t.Fatalf("a login on a cluster started without a root password answered %v, want the no-users text", err)
	}
	if _, err := os.Stat(filepath.Join(tr.nodes["node-a"].cfg.Storage.DataDir, clusterPepperCredential.relPath)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a node with no root password made a password key: %v", err)
	}
	tr.stop("node-a")

	tr.start(rootTestPassword, "node-a")
	tr.login("node-a", "root", rootTestPassword, 30*time.Second)
	view := tr.qs("node-a").ringNode.Users()
	if len(view.Users) != 1 || view.Users[0].Name != "root" || len(view.Users[0].Roles) != 1 ||
		view.Users[0].Roles[0] != (authrules.Grant{Role: authrules.RoleRoot, DB: authrules.AllDatabases}) {
		t.Fatalf("the cluster's users after the root start: %+v", view.Users)
	}
	if !strings.Contains(tr.nodes["node-a"].logs.String(), "created the cluster's first administrator") {
		t.Fatalf("the node did not log the administrator it created:\n%s", tr.nodes["node-a"].logs)
	}
}

// TestARootPasswordOnANodeOfAClusterWithUsersIsIgnoredWithAWarning (spec 6.2): restarted with
// another root password, the node warns in its log and to its operator and changes nothing: the
// old password still logs in, the new one does not. Break: let the task propose whenever its own
// request is not remembered; red.
func TestARootPasswordOnANodeOfAClusterWithUsersIsIgnoredWithAWarning(t *testing.T) {
	tr := newTrio(t, []string{"node-a"}, nil)
	tr.start(rootTestPassword, "node-a")
	tr.login("node-a", "root", rootTestPassword, 30*time.Second)
	before := tr.qs("node-a").ringNode.Users().Users[0]
	tr.stop("node-a")

	tr.start("another-root-password-2", "node-a")
	awaitText(t, tr.nodes["node-a"].logs, notUsedWarning, 30*time.Second)
	awaitText(t, tr.nodes["node-a"].operator, notUsedWarning, time.Second)
	tr.login("node-a", "root", rootTestPassword, 10*time.Second)
	if _, err := tr.dial("node-a", "root", "another-root-password-2"); err == nil {
		t.Fatal("the ignored root password logs in")
	}
	after := tr.qs("node-a").ringNode.Users().Users
	if len(after) != 1 || after[0].ChangedSeq != before.ChangedSeq {
		t.Fatalf("the ignored root password changed the users: %+v, was %+v", after, before)
	}
}

// TestTwoNodesWithARootPasswordCreateOneAdministrator (spec 5.2, 6.5): both data nodes are given a
// root password; the first creates the administrator, the second, once joined, sees the cluster's
// users and warns, never makes a password key of its own, and fetches the cluster's. Break: let
// the task generate a pepper before this node is a committed member; red (node-b logs a key it
// created).
func TestTwoNodesWithARootPasswordCreateOneAdministrator(t *testing.T) {
	tr := newTrio(t, []string{"node-a", "node-b"}, nil)
	tr.start(rootTestPassword, "node-a")
	tr.start("node-b-root-password", "node-b")
	root := tr.login("node-a", "root", rootTestPassword, 30*time.Second)
	tr.join(root, "node-b")
	awaitText(t, tr.nodes["node-b"].logs, notUsedWarning, 30*time.Second)
	if strings.Contains(tr.nodes["node-b"].logs.String(), pepperCreatedLog) {
		t.Fatalf("node-b made a password key of its own:\n%s", tr.nodes["node-b"].logs)
	}
	if users := tr.qs("node-b").ringNode.Users().Users; len(users) != 1 || users[0].Name != "root" {
		t.Fatalf("the cluster holds %+v, want the one administrator", users)
	}
	tr.login("node-b", "root", rootTestPassword, 30*time.Second)
	if _, err := tr.dial("node-b", "root", "node-b-root-password"); err == nil {
		t.Fatal("node-b's ignored root password logs in")
	}
}

// TestTwoRootTasksRacingCreateOneAdministrator (spec 6.5 race): two root tasks proposing at once
// on one cluster end with one administrator; the loser warns and stops. Break: retry a refused
// bootstrap with a fresh proposal instead of reading the table; red (the loser never stops).
func TestTwoRootTasksRacingCreateOneAdministrator(t *testing.T) {
	tr := newTrio(t, []string{"node-a"}, nil)
	tr.start("", "node-a")
	qs := tr.qs("node-a")
	var tasks []*rootTask
	for _, name := range []string{"root", "admin"} {
		root, err := newClusterRoot(name, rootTestPassword, "HOARDDB_ROOT_PASSWORD", qs.core.auth.argon2)
		if err != nil {
			t.Fatal(err)
		}
		tasks = append(tasks, newRootTask(root, qs.ringNode, qs.users, qs.userDeadline(), qs.logger, tr.nodes["node-a"].operator))
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var wg sync.WaitGroup
	for _, task := range tasks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			task.run(ctx)
		}()
	}
	wg.Wait()
	if ctx.Err() != nil {
		t.Fatal("a racing root task did not stop within a minute")
	}
	if users := qs.ringNode.Users().Users; len(users) != 1 {
		t.Fatalf("two racing root tasks left %d users, want one: %+v", len(users), users)
	}
	awaitText(t, tr.nodes["node-a"].operator, notUsedWarning, time.Second)
}

// TestAWitnessRunsNoRootTaskAndWarns (F16, spec 6.2): a witness given a root password warns at
// start, runs no root task and makes no password key. Break: arm the task on a witness; red.
func TestAWitnessRunsNoRootTaskAndWarns(t *testing.T) {
	tr := newTrio(t, []string{"node-a", "node-w"}, func(id string, cfg *config.Config) { cfg.Cluster.Witness = id == "node-w" })
	tr.start(rootTestPassword, "node-a")
	tr.start(rootTestPassword, "node-w")
	if !strings.Contains(tr.nodes["node-w"].operator.String(), witnessWarning) || !strings.Contains(tr.nodes["node-w"].logs.String(), "this node is a witness, which holds no password key") {
		t.Fatalf("the witness did not warn about its root password:\n%s", tr.nodes["node-w"].logs)
	}
	if tr.qs("node-w").clusterRoot != nil {
		t.Fatal("the witness armed a root task")
	}
	root := tr.login("node-a", "root", rootTestPassword, 30*time.Second)
	tr.join(root, "node-w")
	if _, err := os.Stat(filepath.Join(tr.nodes["node-w"].cfg.Storage.DataDir, clusterPepperCredential.relPath)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the witness holds a password key file: %v", err)
	}
}

// fakeRootNode is the cluster as a root task sees it, scripted.
type fakeRootNode struct {
	mu        sync.Mutex
	view      cluster.UserView
	ready     bool
	peppers   int
	proposals int
	propose   func(f *fakeRootNode, body []byte) (cluster.Result, error)
	// root is the task's own first administrator, so a scripted commit can record its request.
	root *clusterRoot
}

func (f *fakeRootNode) Users() cluster.UserView {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.view
}

func (f *fakeRootNode) FirstAdministratorReady() ([16]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return [16]byte{1}, f.ready
}

func (f *fakeRootNode) BootstrapPepper() (clusterpepper.Pepper, error) {
	f.mu.Lock()
	f.peppers++
	f.mu.Unlock()
	return clusterpepper.FromBytes(bytes.Repeat([]byte{7}, clusterpepper.Len))
}

func (f *fakeRootNode) ProposeUserCommand(_ context.Context, kind byte, body []byte) (cluster.Result, error) {
	f.mu.Lock()
	f.proposals++
	f.mu.Unlock()
	if kind != cluster.CommandUserBootstrap {
		return cluster.Result{}, errors.New("the root task proposed another kind")
	}
	return f.propose(f, body)
}

// applied records the task's bootstrap as committed at seq in the view.
func (f *fakeRootNode) applied(_ []byte, seq int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.view.Users = []cluster.UserRow{{Name: f.root.username, CreatedSeq: seq, ChangedSeq: seq}}
	f.view.Requests = []cluster.UserRequest{{ID: f.root.request, StateSeq: seq}}
	f.view.PepperKID, f.view.StateSeq = "07070707", seq
}

type nowInstalled struct{}

func (nowInstalled) awaitInstalled(context.Context, int64) error { return nil }

// runFakeRootTask runs one task against node until it ends or timeout, and returns its log and
// what it told the operator.
func runFakeRootTask(t *testing.T, node *fakeRootNode, timeout time.Duration) (logs, operator string, ended bool) {
	t.Helper()
	root, err := newClusterRoot("root", rootTestPassword, "HOARDDB_ROOT_PASSWORD", Argon2Params{})
	if err != nil {
		t.Fatal(err)
	}
	node.mu.Lock()
	node.root = root
	node.mu.Unlock()
	var logBuf, opBuf ringClusterBuffer
	task := newRootTask(root, node, nowInstalled{}, time.Second, slog.New(slog.NewTextHandler(&logBuf, nil)), &opBuf)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		task.run(ctx)
	}()
	select {
	case <-done:
		ended = ctx.Err() == nil
	case <-time.After(timeout + 5*time.Second):
		t.Fatal("the root task did not return after its context ended")
	}
	return logBuf.String(), opBuf.String(), ended
}

// TestALostBootstrapReplyIsReportedAsSuccess (B14): the bootstrap commits but its reply is lost;
// the task finds its request among the cluster's applied ones and reports the administrator it
// created, never the "not used" warning. Break: treat a non-empty table as someone else's; red.
func TestALostBootstrapReplyIsReportedAsSuccess(t *testing.T) {
	node := &fakeRootNode{ready: true, propose: func(f *fakeRootNode, body []byte) (cluster.Result, error) {
		f.applied(body, 7)
		return cluster.Result{}, &wire.UndecidedError{Err: errors.New("the relay was cut after the send")}
	}}
	logs, operator, ended := runFakeRootTask(t, node, 10*time.Second)
	if !ended {
		t.Fatalf("the task did not end after its bootstrap committed:\n%s", logs)
	}
	if strings.Contains(logs+operator, "was not used") {
		t.Fatalf("the node that created the administrator warned that its password was not used:\n%s%s", logs, operator)
	}
	if !strings.Contains(logs, "created the cluster's first administrator") {
		t.Fatalf("the task did not report its success:\n%s", logs)
	}
	if node.proposals != 1 {
		t.Fatalf("the task proposed %d times, want once", node.proposals)
	}
}

// TestTheRootTaskNeverProposesOnAClusterWithUsersOrAKey (spec 5.2, 6.5): a table with a row, or a
// state that names a key, ends the task with the warning before any pepper or proposal. Break:
// skip the table check; red.
func TestTheRootTaskNeverProposesOnAClusterWithUsersOrAKey(t *testing.T) {
	for name, view := range map[string]cluster.UserView{
		"a user": {Users: []cluster.UserRow{{Name: "ops", CreatedSeq: 3, ChangedSeq: 3}}, PepperKID: "0a1b2c3d", StateSeq: 4},
		"a key":  {PepperKID: "0a1b2c3d", StateSeq: 4},
		// Not a state a cluster reaches (USER_BOOTSTRAP sets both); it pins that each check holds alone.
		"a user without a key": {Users: []cluster.UserRow{{Name: "ops", CreatedSeq: 3, ChangedSeq: 3}}, StateSeq: 4},
		"neither":              {StateSeq: 4},
	} {
		t.Run(name, func(t *testing.T) {
			node := &fakeRootNode{view: view, ready: true, propose: func(f *fakeRootNode, body []byte) (cluster.Result, error) {
				f.applied(body, 5)
				return cluster.Result{StateSeq: 5}, nil
			}}
			logs, operator, ended := runFakeRootTask(t, node, 10*time.Second)
			if !ended {
				t.Fatalf("the task did not end:\n%s", logs)
			}
			if name == "neither" {
				if node.peppers != 1 || node.proposals != 1 {
					t.Fatalf("on an empty cluster the task made %d keys and %d proposals, want one each", node.peppers, node.proposals)
				}
				return
			}
			if node.peppers != 0 || node.proposals != 0 {
				t.Fatalf("with %s the task made %d keys and %d proposals, want none", name, node.peppers, node.proposals)
			}
			if !strings.Contains(operator, notUsedWarning) {
				t.Fatalf("with %s the operator was told %q, want the warning", name, operator)
			}
		})
	}
}

// TestTheRootTaskWaitsForALeaderAndItsOwnMembership (spec 6.5): until the node sees a leader and
// its own committed row it neither makes a pepper nor proposes; once it does, it creates the
// administrator. Break: skip the readiness wait; red.
func TestTheRootTaskWaitsForALeaderAndItsOwnMembership(t *testing.T) {
	node := &fakeRootNode{propose: func(f *fakeRootNode, body []byte) (cluster.Result, error) {
		f.applied(body, 9)
		return cluster.Result{StateSeq: 9}, nil
	}}
	var early int
	time.AfterFunc(1500*time.Millisecond, func() {
		node.mu.Lock()
		defer node.mu.Unlock()
		early = node.peppers + node.proposals
		node.ready = true
	})
	logs, _, ended := runFakeRootTask(t, node, 20*time.Second)
	node.mu.Lock()
	defer node.mu.Unlock()
	if !ended {
		t.Fatalf("the task never finished once ready:\n%s", logs)
	}
	if early != 0 || !node.ready {
		t.Fatalf("before the node was ready the task made %d keys and proposals, want none (ready at the end: %v)", early, node.ready)
	}
	if node.peppers != 1 || node.proposals != 1 {
		t.Fatalf("the task made %d keys and %d proposals, want one each", node.peppers, node.proposals)
	}
}

// TestTheRootTaskEndsWhenTheNodeCloses: a task that never finds a leader returns when its context
// ends, with nothing left running. Break: loop without watching the context; red (it never returns).
func TestTheRootTaskEndsWhenTheNodeCloses(t *testing.T) {
	node := &fakeRootNode{ready: true, propose: func(*fakeRootNode, []byte) (cluster.Result, error) {
		return cluster.Result{}, &cluster.Error{Code: cluster.CodeUnavailable, Message: "no leader is elected yet"}
	}}
	if _, _, ended := runFakeRootTask(t, node, 2*time.Second); ended {
		t.Fatal("the task ended without a leader")
	}
	if node.proposals == 0 {
		t.Fatal("the task never tried")
	}
}

// TestARealBootstrapLogsNoPasswordHashOrPepper (spec 9, the secrecy rule): after a real bootstrap
// on three members (one creates, two join with the password set and are told it was not used), no
// node's log, operator output or health report (which carries password_key), and no file under any node's ring directory, holds the root
// password, its client hash, the pepper or the derived key in any spelling. Break: log the
// credential in the success line; red.
func TestARealBootstrapLogsNoPasswordHashOrPepper(t *testing.T) {
	ids := []string{"node-a", "node-b", "node-c"}
	tr := newTrio(t, ids, nil)
	tr.start(rootTestPassword, "node-a")
	tr.start(rootTestPassword, "node-b", "node-c")
	root := tr.login("node-a", "root", rootTestPassword, 30*time.Second)
	tr.join(root, "node-b", "node-c")
	for _, id := range ids[1:] {
		awaitText(t, tr.nodes[id].logs, notUsedWarning, 30*time.Second)
	}
	qs := tr.qs("node-a")
	row := qs.ringNode.Users().Users[0]
	pepper, err := newClusterPepperFile(qs.core.credentials, tr.nodes["node-a"].cfg.Storage.DataDir).Load()
	if err != nil {
		t.Fatal(err)
	}
	clusterID, _ := qs.ringNode.FirstAdministratorReady()
	key, err := hkdf.Key(sha256.New, pepper.Bytes(), []byte("hoarddb-cluster-pepper-v1"), "cluster-pepper-v1:"+hex.EncodeToString(clusterID[:]), clusterpepper.Len)
	if err != nil {
		t.Fatal(err)
	}
	clientHash := argon2.IDKey([]byte(rootTestPassword), row.Salt, uint32(row.Iterations), uint32(row.Memory), uint8(row.Parallelism), uint32(len(row.Hash)))
	if !pepper.Key(clusterID).Verify(clientHash, row.Hash) {
		t.Fatal("the recomputed client hash does not verify: the scan would look for the wrong bytes")
	}
	secrets := map[string][]byte{"root password": []byte(rootTestPassword), "client hash": clientHash, "pepper": pepper.Bytes(), "pepper key": key}
	health := map[string]string{}
	for _, id := range ids {
		report, err := tr.qs(id).core.HealthCheck()
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(report)
		if err != nil {
			t.Fatal(err)
		}
		health[id] = string(encoded)
	}
	tr.stop(ids...)
	for _, id := range ids {
		for source, text := range map[string]string{"log": tr.nodes[id].logs.String(), "operator output": tr.nodes[id].operator.String(), "health report": health[id]} {
			for name, secret := range secrets {
				for _, form := range clusterpepper.SecretForms(secret) {
					if strings.Contains(text, form) {
						t.Fatalf("%s's %s holds the %s (%q)", id, source, name, form)
					}
				}
			}
		}
		ringDir := filepath.Join(tr.nodes[id].cfg.Storage.DataDir, ringDirName)
		err := filepath.WalkDir(ringDir, func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return err
			}
			contents, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for name, secret := range map[string][]byte{"root password": secrets["root password"], "client hash": clientHash, "pepper": secrets["pepper"], "pepper key": key} {
				if bytes.Contains(contents, secret) {
					t.Errorf("%s holds the %s", path, name)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("scanning %s: %v", ringDir, err)
		}
	}
}

// awaitText waits until buf holds want.
func awaitText(t *testing.T, buf *ringClusterBuffer, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !strings.Contains(buf.String(), want) {
		if time.Now().After(deadline) {
			t.Fatalf("after %v the output lacks %q:\n%s", timeout, want, buf)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// auditOrder records whether the audit line was written before the wait for the install began.
type auditOrder struct {
	mu            sync.Mutex
	audited       bool
	auditedAtWait bool
}

func (a *auditOrder) awaitInstalled(ctx context.Context, _ int64) error {
	a.mu.Lock()
	a.auditedAtWait = a.audited
	a.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
}

// TestTheRootTaskAuditsTheBootstrapWhenItsCommitIsKnown (M2a review F7): the administrator's
// creation is audited as soon as the commit is known, before (and whether or not) this node has
// installed it. Break: audit after the install wait; red.
func TestTheRootTaskAuditsTheBootstrapWhenItsCommitIsKnown(t *testing.T) {
	node := &fakeRootNode{ready: true, propose: func(f *fakeRootNode, body []byte) (cluster.Result, error) {
		f.applied(body, 4)
		return cluster.Result{StateSeq: 4}, nil
	}}
	root, err := newClusterRoot("root", rootTestPassword, "HOARDDB_ROOT_PASSWORD", Argon2Params{})
	if err != nil {
		t.Fatal(err)
	}
	node.root = root
	order := &auditOrder{}
	var entries []AuditEntry
	task := newRootTask(root, node, order, 200*time.Millisecond, discardLogger(), nil)
	task.audit = func(entry AuditEntry) {
		order.mu.Lock()
		defer order.mu.Unlock()
		order.audited = true
		entries = append(entries, entry)
	}
	task.run(context.Background())
	if !order.auditedAtWait || len(entries) != 1 || entries[0].Event != EventUserCreate || entries[0].Target != "user:root" {
		t.Fatalf("audited before the install wait: %v; entries %+v", order.auditedAtWait, entries)
	}
}
