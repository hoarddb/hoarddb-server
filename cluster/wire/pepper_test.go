package wire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/authrules"
	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/clusterpepper"
	"github.com/hoarddb/hoarddb-server/transport"
)

// pepperRaftTest is a one-node Raft leader (n1) whose state holds a joining data member data2 and a
// witness wit (rows only), with a node whose pepper keeper keeps its file in a temporary directory.
type pepperRaftTest struct {
	t    *testing.T
	raft *cluster.Raft
	node *Node
	file clusterpepper.File
	next byte
}

func newPepperRaftTest(t *testing.T) *pepperRaftTest {
	t.Helper()
	raft := openDivergedRaft(t, t.TempDir())
	pt := &pepperRaftTest{t: t, raft: raft, node: bareNode(raft), file: clusterpepper.File{Path: filepath.Join(t.TempDir(), clusterpepper.FileName)}}
	for _, m := range []cluster.Member{{ID: "data2", Incarnation: 1, DataAddr: "data2:1"}, {ID: "wit", Incarnation: 1, DataAddr: "wit:1", Witness: true}} {
		pt.propose(cluster.CommandRegisterMember, cluster.RegisterMemberBody(m, make([]byte, 1024), cluster.BuildStateRange()))
	}
	pt.reopenKeeper()
	return pt
}

// reopenKeeper builds the node's pepper keeper from its file, as a node open does.
func (pt *pepperRaftTest) reopenKeeper() {
	pt.t.Helper()
	keeper, err := newPepperKeeper(pt.file)
	if err != nil {
		pt.t.Fatalf("opening the pepper keeper: %v", err)
	}
	pt.node.cl.pepper = keeper
}

func (pt *pepperRaftTest) propose(kind byte, body []byte) {
	pt.t.Helper()
	if _, err := pt.raft.Propose(cluster.Command{Version: cluster.CommandVersion, Kind: kind, BaseSeq: uint64(pt.raft.StateSeq()), Body: body}); err != nil {
		pt.t.Fatalf("kind 0x%02x: %v", kind, err)
	}
}

// bootstrapUsers commits USER_BOOTSTRAP with a root credential peppered under p, as the root task
// of M2b will, and so fixes the state's pepper kid to p's.
func (pt *pepperRaftTest) bootstrapUsers(p clusterpepper.Pepper) {
	pt.t.Helper()
	pt.next++
	var request [cluster.UserRequestIDLen]byte
	request[0], request[1] = 0xBE, pt.next
	salt := bytes.Repeat([]byte{0x31}, authrules.MinSaltLen)
	hash := p.Key(pt.raft.State().ClusterID).Hash(bytes.Repeat([]byte{0x42}, authrules.Argon2HashLen))
	cred := cluster.UserCredential{Salt: salt, Hash: hash, Memory: authrules.MinArgon2MemoryKiB, Iterations: authrules.MinArgon2Iterations, Parallelism: 4, PepperKID: p.KID()}
	pt.propose(cluster.CommandUserBootstrap, cluster.UserBootstrapBody{Request: request, Name: "root", Credential: cred, CreatedMs: 1}.Encode())
}

func (pt *pepperRaftTest) setState(id, state string) {
	pt.t.Helper()
	pt.propose(cluster.CommandSetMemberState, bsoncore.BuildDocument(nil, append(bsoncore.AppendStringElement(nil, "id", id), bsoncore.AppendStringElement(nil, "state", state)...)))
}

func (pt *pepperRaftTest) ask(from string) transport.PepperReply {
	pt.t.Helper()
	nonce := [transport.PepperNonceLen]byte{0x77, pt.next}
	reply := pt.node.servePepper(context.Background(), transport.Handshake{NodeID: from}, transport.PepperRequest{Nonce: nonce})
	if reply.Nonce != nonce {
		pt.t.Fatalf("the reply to %s echoes nonce %x, want %x", from, reply.Nonce, nonce)
	}
	return reply
}

func mustGenerate(t *testing.T) clusterpepper.Pepper {
	t.Helper()
	p, err := clusterpepper.Generate()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// TestTheProposerOfUserBootstrapGeneratesThePepper (spec 5.2, F1): a data member that sees no kid
// in the state and holds no pepper generates one, writes it durably before it is used, and hands
// the same pepper back when asked again (a crash before the commit keeps it); once its
// USER_BOOTSTRAP commits, the node holds the cluster's key.
func TestTheProposerOfUserBootstrapGeneratesThePepper(t *testing.T) {
	pt := newPepperRaftTest(t)
	if got := pt.node.PasswordKeyStatus(); got != PasswordKeyNoneYet {
		t.Fatalf("status before any user: %q, want %q", got, PasswordKeyNoneYet)
	}
	p, err := pt.node.BootstrapPepper()
	if err != nil {
		t.Fatalf("BootstrapPepper: %v", err)
	}
	onDisk, err := pt.file.Load()
	if err != nil || !onDisk.Equal(p) {
		t.Fatalf("the generated pepper is not on disk before it is used: %v, %v", onDisk, err)
	}
	again, err := pt.node.BootstrapPepper()
	if err != nil || !again.Equal(p) {
		t.Fatalf("a second ask generated another pepper: %v, %v", again, err)
	}
	pt.reopenKeeper()
	afterRestart, err := pt.node.BootstrapPepper()
	if err != nil || !afterRestart.Equal(p) {
		t.Fatalf("a restart before the commit generated another pepper: %v, %v", afterRestart, err)
	}
	pt.bootstrapUsers(p)
	if got := pt.node.PasswordKeyStatus(); got != PasswordKeyHeld {
		t.Fatalf("status after the bootstrap: %q, want %q", got, PasswordKeyHeld)
	}
	key, kid, err := pt.node.PasswordKey()
	if err != nil || kid != p.KID() {
		t.Fatalf("PasswordKey after the bootstrap: kid %q, %v", kid, err)
	}
	clientHash := bytes.Repeat([]byte{0x42}, authrules.Argon2HashLen)
	root, _ := pt.raft.Users().User("root")
	if !key.Verify(clientHash, root.Hash) {
		t.Fatal("the node's key does not verify the hash its bootstrap committed")
	}
}

// TestANodeThatIsNotBootstrappingNeverGeneratesAPepper (spec 5.2): once the state names a kid no
// node generates (it fetches), a witness never generates, and a node that keeps no pepper file
// generates nothing. Break: let BootstrapPepper generate whenever the node holds none; red.
func TestANodeThatIsNotBootstrappingNeverGeneratesAPepper(t *testing.T) {
	pt := newPepperRaftTest(t)
	pt.bootstrapUsers(mustGenerate(t))
	if _, err := pt.node.BootstrapPepper(); err == nil || !strings.Contains(err.Error(), "already has a password key") {
		t.Fatalf("BootstrapPepper with a kid in the state: %v, want a refusal", err)
	}
	if _, err := pt.file.Load(); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a node that is not bootstrapping wrote a pepper file: %v", err)
	}
	witness := newPepperRaftTest(t)
	witness.node.cfg.Witness = true
	if _, err := witness.node.BootstrapPepper(); err == nil || !strings.Contains(err.Error(), "witness") {
		t.Fatalf("BootstrapPepper on a witness: %v, want the witness refusal", err)
	}
	if _, err := witness.file.Load(); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a witness wrote a pepper file: %v", err)
	}
	bare := newPepperRaftTest(t)
	bare.node.cl.pepper = nil
	if _, err := bare.node.BootstrapPepper(); err == nil {
		t.Fatal("a node with no pepper file generated a pepper")
	}
}

// TestAWitnessIsRefusedThePepper (owner ruling, spec 5.3): a holder answers ok to a joining data
// member and refuses a witness with the spec's text, whatever channel admitted it. Break: drop the
// witness check from servePepper; red.
func TestAWitnessIsRefusedThePepper(t *testing.T) {
	pt := newPepperRaftTest(t)
	p, err := pt.node.BootstrapPepper()
	if err != nil {
		t.Fatal(err)
	}
	pt.bootstrapUsers(p)
	reply := pt.ask("wit")
	if reply.Status != transport.PepperRefused || reply.Message != "wit is a witness: it serves no client logins and is not given the cluster's password key." {
		t.Fatalf("the witness was answered %d %q", reply.Status, reply.Message)
	}
	if reply.Pepper.Held() || reply.KID != "" {
		t.Fatal("the refusal to a witness carries key material")
	}
	ok := pt.ask("data2")
	if ok.Status != transport.PepperOK || ok.KID != p.KID() || !bytes.Equal(ok.Pepper.Bytes(), p.Bytes()) {
		t.Fatalf("the joining data member was answered %d kid %q", ok.Status, ok.KID)
	}
}

// TestAPepperIsRefusedToAnAskerWhoseCommittedRowMayNotHoldIt (spec 5.3, F12): the holder re-reads
// its committed state for every request: no row, leaving and dead are refused, down is served (a
// member restarting after a failure recovers its key). Break: drop the per-request row check;
// red.
func TestAPepperIsRefusedToAnAskerWhoseCommittedRowMayNotHoldIt(t *testing.T) {
	pt := newPepperRaftTest(t)
	p, err := pt.node.BootstrapPepper()
	if err != nil {
		t.Fatal(err)
	}
	pt.bootstrapUsers(p)
	if reply := pt.ask("stranger"); reply.Status != transport.PepperRefused || !strings.Contains(reply.Message, "stranger is not a member") {
		t.Fatalf("an id with no row was answered %d %q", reply.Status, reply.Message)
	}
	pt.setState("data2", "active")
	pt.setState("data2", "down")
	if reply := pt.ask("data2"); reply.Status != transport.PepperOK {
		t.Fatalf("a down member was answered %d %q, want its key", reply.Status, reply.Message)
	}
	pt.setState("data2", "dead")
	if reply := pt.ask("data2"); reply.Status != transport.PepperRefused || !strings.Contains(reply.Message, "data2 is dead") {
		t.Fatalf("a dead member was answered %d %q", reply.Status, reply.Message)
	}
}

// TestAHolderAnswersNotHeldUnlessItHoldsTheStatesKey (spec 5.3): a node with no pepper, and a
// bootstrap candidate whose pepper no committed row uses, answer not held, so no asker can take a
// pepper that may lose the bootstrap race.
func TestAHolderAnswersNotHeldUnlessItHoldsTheStatesKey(t *testing.T) {
	pt := newPepperRaftTest(t)
	if reply := pt.ask("data2"); reply.Status != transport.PepperNotHeld {
		t.Fatalf("a node holding nothing answered %d", reply.Status)
	}
	if _, err := pt.node.BootstrapPepper(); err != nil {
		t.Fatal(err)
	}
	if reply := pt.ask("data2"); reply.Status != transport.PepperNotHeld {
		t.Fatalf("a bootstrap candidate before its commit answered %d, want not held", reply.Status)
	}
	pt.bootstrapUsers(mustGenerate(t))
	if reply := pt.ask("data2"); reply.Status != transport.PepperNotHeld {
		t.Fatalf("a holder whose key lost the race answered %d, want not held", reply.Status)
	}
}

// TestTheNoKeyTextIsChosenByCase (spec 6.2, B7, B6): before any user, while fetching and on a
// witness, PasswordKey refuses with its own text, and never with a key. Break: return the
// fetching text in every case; red.
func TestTheNoKeyTextIsChosenByCase(t *testing.T) {
	pt := newPepperRaftTest(t)
	assertNoKey := func(want string) {
		t.Helper()
		_, _, err := pt.node.PasswordKey()
		var noKey *PasswordKeyError
		if !errors.As(err, &noKey) || !errors.Is(err, ErrNoPasswordKey) || !strings.HasPrefix(noKey.Error(), want) {
			t.Fatalf("PasswordKey: %v, want a refusal starting %q", err, want)
		}
	}
	assertNoKey("this cluster has no users yet, so no login can succeed.")
	pt.bootstrapUsers(mustGenerate(t))
	assertNoKey("password logins are not available on this node yet: it has not received the cluster's password key (auth.cluster.pepper) from another member.")
	if got := pt.node.PasswordKeyStatus(); got != PasswordKeyFetching {
		t.Fatalf("status of a data member without the key: %q, want %q", got, PasswordKeyFetching)
	}
	pt.node.cfg.Witness = true
	assertNoKey("this node is a witness: it holds no password key and serves no logins or statements.")
	if got := pt.node.PasswordKeyStatus(); got != PasswordKeyWitness {
		t.Fatalf("status of a witness: %q, want %q", got, PasswordKeyWitness)
	}
}

// TestANodeWithTheWrongPepperMovesItAsideAndRefusesUntilItFetches (spec 5.4, 6.2, 7.1): a node
// whose file names another kid than the state's stops using it at once, keeps it as
// auth.cluster.pepper.wrong-<kid>, logs the wrong-key text, and refuses with the fetching text;
// the fetch then stores the cluster's key and logins resume.
func TestANodeWithTheWrongPepperMovesItAsideAndRefusesUntilItFetches(t *testing.T) {
	pt := newPepperRaftTest(t)
	wrong := mustGenerate(t)
	if err := pt.file.Store(wrong); err != nil {
		t.Fatal(err)
	}
	pt.reopenKeeper()
	clusterKey := mustGenerate(t)
	pt.bootstrapUsers(clusterKey)
	if _, _, err := pt.node.PasswordKey(); !errors.Is(err, ErrNoPasswordKey) {
		t.Fatalf("a node holding the wrong key gave a key: %v", err)
	}
	var logged []string
	pt.node.cl.pepper.log = func(line string) { logged = append(logged, line) }
	blocked := errors.New("fetch blocked by the test")
	pt.node.cl.pepper.ask = func(context.Context, string, transport.PepperRequest) (transport.PepperReply, error) {
		return transport.PepperReply{}, blocked
	}
	if done := pt.node.pepperStep(context.Background()); done {
		t.Fatal("the step reports done while the fetch is blocked")
	}
	aside := pt.file.Path + ".wrong-" + wrong.KID()
	kept, err := (clusterpepper.File{Path: aside}).Load()
	if err != nil || !kept.Equal(wrong) {
		t.Fatalf("the wrong file was not kept at %s: %v", aside, err)
	}
	wantLine := fmt.Sprintf("this node's auth.cluster.pepper (key %s) is not the cluster's (key %s); it was moved to %s and the cluster's key is being fetched from another member. Logins on this node resume when it arrives.", wrong.KID(), clusterKey.KID(), filepath.Base(aside))
	if !containsLine(logged, wantLine) {
		t.Fatalf("the wrong-key line was not logged; logged:\n%s", strings.Join(logged, "\n"))
	}
	pt.node.cl.pepper.ask = func(_ context.Context, _ string, req transport.PepperRequest) (transport.PepperReply, error) {
		reply := transport.PepperReply{Nonce: req.Nonce, Status: transport.PepperOK, KID: clusterKey.KID()}
		reply.Pepper = keyBytes(clusterKey)
		return reply, nil
	}
	if done := pt.node.pepperStep(context.Background()); !done {
		t.Fatal("the step did not store the cluster's key")
	}
	if _, kid, err := pt.node.PasswordKey(); err != nil || kid != clusterKey.KID() {
		t.Fatalf("after the fetch: kid %q, %v", kid, err)
	}
	if onDisk, err := pt.file.Load(); err != nil || !onDisk.Equal(clusterKey) {
		t.Fatalf("the fetched key is not on disk: %v", err)
	}
}

// TestAFetchedKeyIsCheckedBeforeItIsKept (spec 5.3): a reply whose kid is not the state's, whose
// bytes do not give its kid, or that echoes another nonce is discarded and logged, and the node
// keeps asking.
func TestAFetchedKeyIsCheckedBeforeItIsKept(t *testing.T) {
	pt := newPepperRaftTest(t)
	clusterKey := mustGenerate(t)
	pt.bootstrapUsers(clusterKey)
	other := mustGenerate(t)
	var logged []string
	pt.node.cl.pepper.log = func(line string) { logged = append(logged, line) }
	for name, forge := range map[string]func(req transport.PepperRequest) transport.PepperReply{
		"another kid": func(req transport.PepperRequest) transport.PepperReply {
			reply := transport.PepperReply{Nonce: req.Nonce, Status: transport.PepperOK, KID: other.KID()}
			reply.Pepper = keyBytes(other)
			return reply
		},
		"bytes that do not give the kid": func(req transport.PepperRequest) transport.PepperReply {
			reply := transport.PepperReply{Nonce: req.Nonce, Status: transport.PepperOK, KID: clusterKey.KID()}
			reply.Pepper = keyBytes(other)
			return reply
		},
		"another nonce": func(req transport.PepperRequest) transport.PepperReply {
			reply := transport.PepperReply{Status: transport.PepperOK, KID: clusterKey.KID()}
			reply.Nonce[0] = req.Nonce[0] ^ 0xFF
			reply.Pepper = keyBytes(clusterKey)
			return reply
		},
	} {
		logged = nil
		pt.node.cl.pepper.ask = func(_ context.Context, _ string, req transport.PepperRequest) (transport.PepperReply, error) {
			return forge(req), nil
		}
		if pt.node.pepperStep(context.Background()) {
			t.Fatalf("%s: the node kept the key", name)
		}
		if _, err := pt.file.Load(); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("%s: a file was written: %v", name, err)
		}
		if len(logged) == 0 {
			t.Fatalf("%s: the discarded reply was not logged", name)
		}
		for _, line := range logged {
			for _, form := range clusterpepper.SecretForms(other.Bytes()) {
				if strings.Contains(line, form) {
					t.Fatalf("%s: a log line holds key bytes: %s", name, line)
				}
			}
		}
	}
}

// TestAWitnessRunsNoPepperFetcher (spec 5.3, F16): a node configured as a witness never asks for
// the key, so it is never refused and never logs on both sides every 30 seconds. Break: start the
// fetcher on a witness; red.
func TestAWitnessRunsNoPepperFetcher(t *testing.T) {
	pt := newPepperRaftTest(t)
	pt.node.cfg.Witness = true
	pt.bootstrapUsers(mustGenerate(t))
	var asked sync.Once
	called := false
	pt.node.cl.pepper.ask = func(context.Context, string, transport.PepperRequest) (transport.PepperReply, error) {
		asked.Do(func() { called = true })
		return transport.PepperReply{}, errors.New("no")
	}
	if pt.node.startsPepperFetcher() {
		t.Fatal("a witness starts the pepper fetcher")
	}
	if done := pt.node.pepperStep(context.Background()); !done || called {
		t.Fatalf("a witness's pepper step asked a member (done %v, asked %v)", done, called)
	}
}

func containsLine(lines []string, want string) bool {
	for _, line := range lines {
		if line == want {
			return true
		}
	}
	return false
}

// TestAMissingPepperFileIsFetchedAgain (spec 5.4): a data member whose file was moved away opens
// without a key and without an error, and its first step after the state names a kid fetches it.
func TestAMissingPepperFileIsFetchedAgain(t *testing.T) {
	pt := newPepperRaftTest(t)
	p, err := pt.node.BootstrapPepper()
	if err != nil {
		t.Fatal(err)
	}
	pt.bootstrapUsers(p)
	if err := os.Rename(pt.file.Path, pt.file.Path+".elsewhere"); err != nil {
		t.Fatal(err)
	}
	pt.reopenKeeper()
	if got := pt.node.PasswordKeyStatus(); got != PasswordKeyFetching {
		t.Fatalf("status without the file: %q, want %q", got, PasswordKeyFetching)
	}
	pt.node.cl.pepper.ask = func(_ context.Context, _ string, req transport.PepperRequest) (transport.PepperReply, error) {
		reply := transport.PepperReply{Nonce: req.Nonce, Status: transport.PepperOK, KID: p.KID()}
		reply.Pepper = keyBytes(p)
		return reply, nil
	}
	if !pt.node.pepperStep(context.Background()) || pt.node.PasswordKeyStatus() != PasswordKeyHeld {
		t.Fatalf("the missing file was not fetched again: %q", pt.node.PasswordKeyStatus())
	}
}

// TestDescribeNodeCarriesThePasswordKeyStatus (spec 5.2, B2, B22): describe_node reports the
// node's password_key after cluster_version and before host, leaves it out on a node that is not
// a cluster member, refuses a value outside the four, and a node reports its own status.
func TestDescribeNodeCarriesThePasswordKeyStatus(t *testing.T) {
	node := &RingAdminNode{NodeID: "node-a", Version: "v", MemberState: "active", Members: 3, Voters: 3, DefaultRF: 1, Serving: ServingOK,
		ClusterVersion: 2, PasswordKey: PasswordKeyHeld, Host: &RingAdminHost{CPUs: 1}}
	raw, err := EncodeRingAdminReply(RingAdminReply{Cmd: AdminDescribeNode, Node: node})
	if err != nil {
		t.Fatal(err)
	}
	version, key, host := bytes.Index(raw, []byte("cluster_version\x00")), bytes.Index(raw, []byte("password_key\x00")), bytes.Index(raw, []byte("host\x00"))
	if key < 0 || version >= key || key >= host {
		t.Fatalf("password_key is at %d, cluster_version at %d and host at %d: want it between them", key, version, host)
	}
	got, err := DecodeRingAdminReply(raw)
	if err != nil || got.Node.PasswordKey != PasswordKeyHeld {
		t.Fatalf("decoded password_key %q, %v", got.Node.PasswordKey, err)
	}
	node.PasswordKey = ""
	if raw, err = EncodeRingAdminReply(RingAdminReply{Cmd: AdminDescribeNode, Node: node}); err != nil || bytes.Contains(raw, []byte("password_key")) {
		t.Fatalf("a node that is not a cluster member wrote password_key (%v)", err)
	}
	node.PasswordKey = "lost"
	if _, err := EncodeRingAdminReply(RingAdminReply{Cmd: AdminDescribeNode, Node: node}); err == nil || !strings.Contains(err.Error(), `password_key "lost"`) {
		t.Fatalf("password_key lost gave %v", err)
	}
	pt := newPepperRaftTest(t)
	pt.node.live = u83Liveness{"n1", true}
	if got := pt.node.describeNode(RingNodeFacts{Version: "v", StartedAt: time.Now()}).PasswordKey; got != PasswordKeyNoneYet {
		t.Fatalf("describe_node of a cluster with no users reports %q", got)
	}
}

func keyBytes(p clusterpepper.Pepper) transport.PepperKey {
	return transport.NewPepperKey(p.Bytes())
}

// TestTheNoKeyTextsNameTheBackupFix (review F2, rule 3): when no member can hand over the key, the
// login refusal and the fetcher's log line name the one fix: restore auth.cluster.pepper from a
// backup of this node's own data directory (a sealed copy opens only on the node that sealed it).
// Break: drop the sentence from fetchingText; red.
func TestTheNoKeyTextsNameTheBackupFix(t *testing.T) {
	pt := newPepperRaftTest(t)
	pt.bootstrapUsers(mustGenerate(t))
	_, _, err := pt.node.PasswordKey()
	var logged []string
	pt.node.cl.pepper.log = func(line string) { logged = append(logged, line) }
	pt.node.cl.pepper.ask = func(_ context.Context, _ string, req transport.PepperRequest) (transport.PepperReply, error) {
		return transport.PepperReply{Nonce: req.Nonce, Status: transport.PepperNotHeld}, nil
	}
	pt.node.pepperStep(context.Background())
	for what, text := range map[string]string{"the login refusal": fmt.Sprint(err), "the fetcher's log": strings.Join(logged, "\n")} {
		for _, want := range []string{"restore auth.cluster.pepper", "backup of this node's own data directory", "opens only on the node that sealed it"} {
			if !strings.Contains(text, want) {
				t.Errorf("%s does not say %q:\n%s", what, want, text)
			}
		}
	}
}

// failingMoveFile is a pepper file whose MoveAside fails while fail is set.
type failingMoveFile struct {
	clusterpepper.File
	fail bool
}

func (f *failingMoveFile) MoveAside(kid string) (string, error) {
	if f.fail {
		return "", errors.New("the test refuses this rename")
	}
	return f.File.MoveAside(kid)
}

// TestAWrongPepperFileIsNeverOverwrittenWhenItCannotBeMovedAside (review F6): a wrong file the node
// failed to move aside is not replaced by a fetched key: the node keeps refusing, retries the move
// on its next round, and only then stores the cluster's key, so the wrong file is never deleted.
// Break: let fetchPepper store while a wrong file is still in place; red.
func TestAWrongPepperFileIsNeverOverwrittenWhenItCannotBeMovedAside(t *testing.T) {
	pt := newPepperRaftTest(t)
	wrong := mustGenerate(t)
	if err := pt.file.Store(wrong); err != nil {
		t.Fatal(err)
	}
	file := &failingMoveFile{File: pt.file, fail: true}
	keeper, err := newPepperKeeper(file)
	if err != nil {
		t.Fatal(err)
	}
	pt.node.cl.pepper = keeper
	clusterKey := mustGenerate(t)
	pt.bootstrapUsers(clusterKey)
	keeper.ask = func(_ context.Context, _ string, req transport.PepperRequest) (transport.PepperReply, error) {
		return transport.PepperReply{Nonce: req.Nonce, Status: transport.PepperOK, KID: clusterKey.KID(), Pepper: keyBytes(clusterKey)}, nil
	}
	for round := range 2 {
		if pt.node.pepperStep(context.Background()) {
			t.Fatalf("round %d: the node stored a key while the wrong file could not be moved", round)
		}
		if onDisk, err := pt.file.Load(); err != nil || !onDisk.Equal(wrong) {
			t.Fatalf("round %d: the wrong file was replaced (%v)", round, err)
		}
		if _, _, err := pt.node.PasswordKey(); !errors.Is(err, ErrNoPasswordKey) {
			t.Fatalf("round %d: the node gave a key: %v", round, err)
		}
	}
	// The write itself refuses too, whatever path reaches it while the wrong file is in place.
	if pt.node.fetchPepper(context.Background(), pt.raft.PepperView()) {
		t.Fatal("a direct fetch stored a key over the wrong file")
	}
	if onDisk, err := pt.file.Load(); err != nil || !onDisk.Equal(wrong) {
		t.Fatalf("a direct fetch replaced the wrong file (%v)", err)
	}
	file.fail = false
	if !pt.node.pepperStep(context.Background()) {
		t.Fatal("once the move works the node does not fetch")
	}
	if kept, err := (clusterpepper.File{Path: pt.file.Path + ".wrong-" + wrong.KID()}).Load(); err != nil || !kept.Equal(wrong) {
		t.Fatalf("the wrong file was not kept: %v", err)
	}
	if onDisk, err := pt.file.Load(); err != nil || !onDisk.Equal(clusterKey) {
		t.Fatalf("the cluster's key is not on disk: %v", err)
	}
}

// TestAKeyWhoseKidSharesAPrefixIsDiscarded (review F5): a reply carrying a real key whose kid shares
// the state's first 4 hex characters is discarded, so the kid compare is the full 8 characters.
// Break: compare reply.KID[:4] with the state's; red.
func TestAKeyWhoseKidSharesAPrefixIsDiscarded(t *testing.T) {
	pt := newPepperRaftTest(t)
	clusterKey := mustGenerate(t)
	pt.bootstrapUsers(clusterKey)
	var twin clusterpepper.Pepper
	for tries := 0; ; tries++ {
		if tries > 2_000_000 {
			t.Fatal("no pepper with a kid sharing 16 bits found in 2,000,000 tries")
		}
		candidate := mustGenerate(t)
		if kid := candidate.KID(); kid[:4] == clusterKey.KID()[:4] && kid != clusterKey.KID() {
			twin = candidate
			break
		}
	}
	pt.node.cl.pepper.ask = func(_ context.Context, _ string, req transport.PepperRequest) (transport.PepperReply, error) {
		return transport.PepperReply{Nonce: req.Nonce, Status: transport.PepperOK, KID: twin.KID(), Pepper: keyBytes(twin)}, nil
	}
	if pt.node.pepperStep(context.Background()) {
		t.Fatalf("the node kept key %s for the state's %s", twin.KID(), clusterKey.KID())
	}
	if _, err := pt.file.Load(); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a file was written: %v", err)
	}
}

// movingFile is a pepper file whose MoveAside fails while fail is set and, while renameThenSync is
// set, renames the file and then reports the directory sync failing, as clusterpepper.MoveAside
// does when its syncDir fails.
type movingFile struct {
	clusterpepper.File
	fail, renameThenSync bool
}

func (f *movingFile) MoveAside(kid string) (string, error) {
	switch {
	case f.fail:
		return "", errors.New("the test refuses this rename")
	case f.renameThenSync:
		target := f.Path + ".wrong-" + kid
		if err := os.Rename(f.Path, target); err != nil {
			return "", err
		}
		return target, fmt.Errorf("syncing the directory: %w", clusterpepper.ErrMovedNotSynced)
	}
	return f.File.MoveAside(kid)
}

func stuckNode(t *testing.T) (*pepperRaftTest, *movingFile, clusterpepper.Pepper, clusterpepper.Pepper) {
	t.Helper()
	pt := newPepperRaftTest(t)
	wrong := mustGenerate(t)
	if err := pt.file.Store(wrong); err != nil {
		t.Fatal(err)
	}
	file := &movingFile{File: pt.file, fail: true}
	keeper, err := newPepperKeeper(file)
	if err != nil {
		t.Fatal(err)
	}
	pt.node.cl.pepper = keeper
	clusterKey := mustGenerate(t)
	pt.bootstrapUsers(clusterKey)
	keeper.ask = func(_ context.Context, _ string, req transport.PepperRequest) (transport.PepperReply, error) {
		return transport.PepperReply{Nonce: req.Nonce, Status: transport.PepperOK, KID: clusterKey.KID(), Pepper: keyBytes(clusterKey)}, nil
	}
	if pt.node.pepperStep(context.Background()) {
		t.Fatal("the node fetched while the wrong file could not be moved")
	}
	return pt, file, wrong, clusterKey
}

// TestProbeOperatorMovesByHand (review G1): the node could not move a wrong file aside and the
// operator does it by hand, to the very name the node would pick; within one more round the node
// sees the file gone, fetches the cluster's key and reports held, and the operator's copy is kept.
// Break: treat ErrSourceGone as a failed move; red.
func TestProbeOperatorMovesByHand(t *testing.T) {
	pt, file, wrong, clusterKey := stuckNode(t)
	byHand := pt.file.Path + ".wrong-" + wrong.KID()
	if err := os.Rename(pt.file.Path, byHand); err != nil {
		t.Fatal(err)
	}
	file.fail = false
	if !pt.node.pepperStep(context.Background()) || pt.node.PasswordKeyStatus() != PasswordKeyHeld {
		t.Fatalf("after the operator's move the node reports %q", pt.node.PasswordKeyStatus())
	}
	if kept, err := (clusterpepper.File{Path: byHand}).Load(); err != nil || !kept.Equal(wrong) {
		t.Fatalf("the operator's copy is gone: %v", err)
	}
	if onDisk, err := pt.file.Load(); err != nil || !onDisk.Equal(clusterKey) {
		t.Fatalf("the cluster's key is not on disk: %v", err)
	}
}

// TestARenameWhoseDirectorySyncFailedCountsAsMoved (review G1): a move whose rename succeeded and
// whose directory sync failed is a move: the node does not pick another name or retry the rename,
// and it fetches in the same round (the key's write syncs the directory again). Break: treat
// ErrMovedNotSynced as a failed move; red.
func TestARenameWhoseDirectorySyncFailedCountsAsMoved(t *testing.T) {
	pt, file, wrong, clusterKey := stuckNode(t)
	file.fail, file.renameThenSync = false, true
	if !pt.node.pepperStep(context.Background()) || pt.node.PasswordKeyStatus() != PasswordKeyHeld {
		t.Fatalf("after a rename whose sync failed the node reports %q", pt.node.PasswordKeyStatus())
	}
	if kept, err := (clusterpepper.File{Path: pt.file.Path + ".wrong-" + wrong.KID()}).Load(); err != nil || !kept.Equal(wrong) {
		t.Fatalf("the wrong file is not kept: %v", err)
	}
	if _, err := os.Stat(pt.file.Path + ".wrong-" + wrong.KID() + ".1"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a second name was picked: %v", err)
	}
	if onDisk, err := pt.file.Load(); err != nil || !onDisk.Equal(clusterKey) {
		t.Fatalf("the cluster's key is not on disk: %v", err)
	}
}

// TestFirstAdministratorReadyNeedsItsOwnCommittedDataRow (spec 6.5; M2b review a, d): with a
// leader known, a node may propose the cluster's first administrator only when its own committed
// row exists, is not a witness and is joining, active or down: a node whose row is missing (a
// joiner that has not applied its admission), leaving or dead is not ready. Break: drop the
// membership half, or the state check; red.
func TestFirstAdministratorReadyNeedsItsOwnCommittedDataRow(t *testing.T) {
	pt := newPepperRaftTest(t)
	if pt.node.LeaderID() == "" {
		t.Fatal("the test node knows no leader; the case needs one")
	}
	ready := func(id string) bool {
		pt.node.cfg.NodeID = id
		_, ok := pt.node.FirstAdministratorReady()
		return ok
	}
	if !ready("data2") {
		t.Fatal("a joining data member that knows a leader is not ready")
	}
	if ready("stranger") {
		t.Fatal("a node whose own row is missing is ready while a leader is known")
	}
	if ready("wit") {
		t.Fatal("a witness row is ready")
	}
	pt.setState("data2", "active")
	pt.setState("data2", "down")
	if !ready("data2") {
		t.Fatal("a down data member restarting is not ready")
	}
	pt.setState("data2", "dead")
	if ready("data2") {
		t.Fatal("a dead member is ready")
	}
	leaving := newPepperRaftTest(t)
	leaving.setState("data2", "active")
	leaving.setState("data2", "leaving")
	leaving.node.cfg.NodeID = "data2"
	if _, ok := leaving.node.FirstAdministratorReady(); ok {
		t.Fatal("a leaving member is ready")
	}
}
