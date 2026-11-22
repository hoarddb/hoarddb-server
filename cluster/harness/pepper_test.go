package harness_test

import (
	"bytes"
	"context"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/crypto/argon2"

	"github.com/hoarddb/hoarddb-server/authrules"
	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/clusterpepper"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The cluster pepper across real nodes (ring user store spec 5, step P): pinned TLS, the control
// channel, Raft, the file. The root bootstrap task and the user binding are later merges (M2b,
// M2a), so these tests do what those will: take the bootstrap pepper from a node, pepper an
// Argon2id client hash with it, and propose the user command through ProposeUserCommand.

const pepperTestPassword = "correct horse battery staple, 2026"

// userCredential derives a client hash from password and peppers it under node's key, as the
// binding of M2a will; it returns the credential and the unpeppered client hash.
type userCredential struct {
	cred       cluster.UserCredential
	clientHash []byte
}

func clientHashOf(password string, salt []byte) []byte {
	return argon2.IDKey([]byte(password), salt, authrules.MinArgon2Iterations, uint32(authrules.MinArgon2MemoryKiB), 4, authrules.Argon2HashLen)
}

func credentialUnder(key clusterpepper.Key, kid, password string, saltFill byte) userCredential {
	salt := bytes.Repeat([]byte{saltFill}, authrules.MinSaltLen)
	clientHash := clientHashOf(password, salt)
	return userCredential{
		cred: cluster.UserCredential{Salt: salt, Hash: key.Hash(clientHash), Memory: authrules.MinArgon2MemoryKiB,
			Iterations: authrules.MinArgon2Iterations, Parallelism: 4, PepperKID: kid},
		clientHash: clientHash,
	}
}

var pepperRequestSeq atomic.Uint32

func userRequestID() [cluster.UserRequestIDLen]byte {
	var id [cluster.UserRequestIDLen]byte
	id[0] = 0xF1
	n := pepperRequestSeq.Add(1)
	id[1], id[2] = byte(n>>8), byte(n)
	return id
}

// bootstrapUsersOn makes node id the USER_BOOTSTRAP proposer: it takes the node's bootstrap pepper
// (generated there), peppers root's hash and commits the bootstrap.
func bootstrapUsersOn(t *testing.T, c *harness.Cluster, id string) (clusterpepper.Pepper, userCredential) {
	t.Helper()
	node := c.Node(id)
	pepper, err := node.BootstrapPepper()
	if err != nil {
		t.Fatalf("BootstrapPepper on %s: %v", id, err)
	}
	root := credentialUnder(pepper.Key(node.State().ClusterID), pepper.KID(), pepperTestPassword, 0x51)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body := cluster.UserBootstrapBody{Request: userRequestID(), Name: "root", Credential: root.cred, CreatedMs: time.Now().UnixMilli()}.Encode()
	if _, err := node.ProposeUserCommand(ctx, cluster.CommandUserBootstrap, body); err != nil {
		t.Fatalf("USER_BOOTSTRAP from %s: %v", id, err)
	}
	return pepper, root
}

func awaitHeld(t *testing.T, c *harness.Cluster, id string, timeout time.Duration) {
	t.Helper()
	c.Await(id+" to hold the cluster's password key", timeout, func() bool {
		return c.Node(id).PasswordKeyStatus() == wire.PasswordKeyHeld
	})
}

// derivedKeyBytes recomputes the pepper key (spec 5.1) outside clusterpepper, so the scans look for
// the key's bytes without the package having to hand them out.
func derivedKeyBytes(t *testing.T, pepper clusterpepper.Pepper, clusterID [16]byte) []byte {
	t.Helper()
	key, err := hkdf.Key(sha256.New, pepper.Bytes(), []byte("hoarddb-cluster-pepper-v1"), "cluster-pepper-v1:"+hex.EncodeToString(clusterID[:]), clusterpepper.Len)
	if err != nil {
		t.Fatal(err)
	}
	clientHash := bytes.Repeat([]byte{9}, 32)
	mac := hmac.New(sha256.New, key)
	mac.Write(clientHash)
	if !bytes.Equal(pepper.Key(clusterID).Hash(clientHash), mac.Sum(nil)) {
		t.Fatal("the recomputed pepper key is not the one the node uses")
	}
	return key
}

// scanForSecrets fails when any form of any secret is in text.
func scanForSecrets(t *testing.T, where, text string, secrets map[string][]byte) {
	t.Helper()
	for name, secret := range secrets {
		for _, form := range clusterpepper.SecretForms(secret) {
			if strings.Contains(text, form) {
				t.Errorf("%s holds the %s (as %q)", where, name, form)
			}
		}
	}
}

// scanDirForSecrets reads every file under dir and fails when a secret is in one.
func scanDirForSecrets(t *testing.T, dir string, secrets map[string][]byte) int {
	t.Helper()
	files := 0
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files++
		scanForSecrets(t, path, string(contents), secrets)
		return nil
	})
	if err != nil {
		t.Fatalf("scanning %s: %v", dir, err)
	}
	return files
}

// TestAJoinerHoldsTheClustersPepperAndAWitnessHoldsNone (step P harness test): the bootstrap
// proposer's key reaches the other data member, a node joined later fetches it once its row is
// committed and verifies the root hash with it, and the witness neither asks for nor holds it. The
// whole run's log, every background error and every refusal text hold no form of the key.
func TestAJoinerHoldsTheClustersPepperAndAWitnessHoldsNone(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{ClusterPepper: true, NoDetection: true, Witnesses: []string{"n3"}, Late: 1, CaptureLog: true})
	for id, want := range map[string]string{"n1": wire.PasswordKeyNoneYet, "n2": wire.PasswordKeyNoneYet, "n3": wire.PasswordKeyWitness} {
		if got := c.Node(id).PasswordKeyStatus(); got != want {
			t.Fatalf("%s before any user: %q, want %q", id, got, want)
		}
	}
	pepper, root := bootstrapUsersOn(t, c, "n1")
	awaitHeld(t, c, "n2", 10*time.Second)
	joiner := c.AddNode()
	awaitHeld(t, c, joiner, 30*time.Second)
	state := c.Node(joiner).State()
	for _, id := range []string{"n1", "n2", joiner} {
		onDisk, err := c.PepperFile(id).Load()
		if err != nil || onDisk.KID() != state.PepperKID || onDisk.KID() != pepper.KID() {
			t.Fatalf("%s's file holds key %v (%v); the state names %s", id, onDisk, err, state.PepperKID)
		}
	}
	key, kid, err := c.Node(joiner).PasswordKey()
	if err != nil || kid != pepper.KID() {
		t.Fatalf("the joiner's PasswordKey: kid %q, %v", kid, err)
	}
	committed, ok := c.Node(joiner).State().Users[0], len(state.Users) == 1
	if !ok || !key.Verify(root.clientHash, committed.Hash) {
		t.Fatal("the joiner's key does not verify the root hash the bootstrap proposer made")
	}
	if got := c.Node("n3").PasswordKeyStatus(); got != wire.PasswordKeyWitness {
		t.Fatalf("the witness reports %q", got)
	}
	if _, err := c.PepperFile("n3").Load(); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the witness holds a pepper file: %v", err)
	}
	_, _, witnessErr := c.Node("n3").PasswordKey()
	if !errors.Is(witnessErr, wire.ErrNoPasswordKey) || !strings.HasPrefix(witnessErr.Error(), "this node is a witness") {
		t.Fatalf("the witness's PasswordKey: %v", witnessErr)
	}

	// Every text the run produced: log lines (Info and above), background errors, refusals.
	refusals := []string{witnessErr.Error()}
	if _, err := c.Node("n2").BootstrapPepper(); err != nil {
		refusals = append(refusals, err.Error())
	}
	if _, err := c.Node("n3").BootstrapPepper(); err != nil {
		refusals = append(refusals, err.Error())
	}
	ask, err := transport.NewControlMessage(transport.Envelope{}, transport.PepperRequest{Nonce: [transport.PepperNonceLen]byte{1}})
	if err != nil {
		t.Fatal(err)
	}
	if raw, err := c.CallAs("n3", "n1", transport.ChannelControl, ask); err == nil {
		decoded, decodeErr := raw.Control()
		reply, isReply := decoded.(transport.PepperReply)
		if decodeErr != nil || !isReply || reply.Status != transport.PepperRefused {
			t.Fatalf("a witness's PEPPER_REQUEST was answered with a %T, status %d (%v)", decoded, reply.Status, decodeErr)
		}
		refusals = append(refusals, reply.Message)
	} else {
		refusals = append(refusals, err.Error())
	}
	lines := c.LogLines()
	if !containsSubstring(lines, "handed the cluster's password key (key "+pepper.KID()+") to") {
		t.Fatalf("no hand-out was logged; the scan below would read nothing of the key's life:\n%s", strings.Join(lines, "\n"))
	}
	if !containsSubstring(lines, "n3 is a witness: it serves no client logins and is not given the cluster's password key.") {
		t.Fatalf("the refusal to the witness was not logged")
	}
	secrets := map[string][]byte{"pepper": pepper.Bytes(), "pepper key": derivedKeyBytes(t, pepper, state.ClusterID)}
	all := strings.Join(append(append(lines, c.BackgroundErrors()...), refusals...), "\n")
	scanForSecrets(t, "the cluster's log, errors and refusals", all, secrets)
}

func containsSubstring(lines []string, want string) bool {
	for _, line := range lines {
		if strings.Contains(line, want) {
			return true
		}
	}
	return false
}

// TestEveryDataMemberHoldsThePepperWithinOneRoundOfUserBootstrap (spec 5.3, B2): the fetchers of
// the two other data members wait for the state's kid; the USER_BOOTSTRAP commit wakes them, and
// both hold the key within 2 s. Break: remove the kid-change wake from the apply hook; red.
func TestEveryDataMemberHoldsThePepperWithinOneRoundOfUserBootstrap(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{ClusterPepper: true, NoDetection: true})
	time.Sleep(1500 * time.Millisecond) // past the fetchers' first round, so only the wake can bring them
	bootstrapUsersOn(t, c, "n1")
	committed := time.Now()
	for _, id := range []string{"n2", "n3"} {
		awaitHeld(t, c, id, 2*time.Second-time.Since(committed))
	}
}

// TestAJoinerWithoutThePepperCannotLogAnyoneIn (spec 5.5, 6.6): while a joiner's fetch is held it
// reports fetching and its PasswordKey refuses with the fetching text, before any derivation can
// run; once the fetch goes through, the key it holds verifies the root hash. Deviation: the AUTH
// path that calls PasswordKey is M2a's (spec 12, M2a.1); this test stops at the key. Break: let
// PasswordKey answer while the node holds no key; red.
func TestAJoinerWithoutThePepperCannotLogAnyoneIn(t *testing.T) {
	var held atomic.Bool
	held.Store(true)
	c := harness.NewCluster(t, 2, harness.Options{ClusterPepper: true, NoDetection: true, Late: 1,
		BeforePepperFetch: func(id, _ string) error {
			if id == "n3" && held.Load() {
				return errors.New("the test holds this fetch")
			}
			return nil
		}})
	_, root := bootstrapUsersOn(t, c, "n1")
	joiner := c.AddNode()
	c.Await("the joiner to apply the bootstrap", 30*time.Second, func() bool { return c.Node(joiner).State().PepperKID != "" })
	time.Sleep(time.Second)
	if got := c.Node(joiner).PasswordKeyStatus(); got != wire.PasswordKeyFetching {
		t.Fatalf("the held joiner reports %q, want %q", got, wire.PasswordKeyFetching)
	}
	if _, _, err := c.Node(joiner).PasswordKey(); !errors.Is(err, wire.ErrNoPasswordKey) || !strings.HasPrefix(err.Error(), "password logins are not available on this node yet") {
		t.Fatalf("the held joiner's PasswordKey: %v, want the fetching text", err)
	}
	held.Store(false)
	awaitHeld(t, c, joiner, 40*time.Second)
	key, _, err := c.Node(joiner).PasswordKey()
	if err != nil || !key.Verify(root.clientHash, c.Node(joiner).State().Users[0].Hash) {
		t.Fatalf("after the fetch the joiner cannot verify root: %v", err)
	}
}

// TestAPepperIsRefusedToAMemberRemovedWhileItsConnectionStaysOpen (spec 5.3, F12): one control
// connection, pinned and admitted while n3 was an active member, asks once (ok), stays open while
// n3 is committed dead, and asks again: the holder reads n3's committed row per request and
// refuses. Deviation: remove-node and leave are not built at this base (cluster/wire/membership.go
// notBuiltYet), so the membership change is the failure detector's dead commit, which the same
// check refuses. Break: drop the per-request row check; red.
func TestAPepperIsRefusedToAMemberRemovedWhileItsConnectionStaysOpen(t *testing.T) {
	profile := maps.Clone(harness.P0)
	profile["dead_ms"] = 3000
	c := harness.NewCluster(t, 3, harness.Options{ClusterPepper: true, Timings: profile})
	bootstrapUsersOn(t, c, "n1")
	holder := c.AwaitLeader(10 * time.Second)
	if holder == "n3" {
		t.Skip("n3 leads; the test needs n3 to ask a holder other than itself")
	}
	awaitHeld(t, c, holder, 10*time.Second)
	client := c.ClientAs("n3", holder, transport.ChannelControl)
	defer func() { _ = client.Close() }()
	ask := func(nonce byte) transport.PepperReply {
		t.Helper()
		msg, err := transport.NewControlMessage(transport.Envelope{}, transport.PepperRequest{Nonce: [transport.PepperNonceLen]byte{nonce}})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		raw, err := client.Call(ctx, msg)
		if err != nil {
			t.Fatalf("PEPPER_REQUEST %d on the held connection: %v", nonce, err)
		}
		decoded, err := raw.Control()
		reply, ok := decoded.(transport.PepperReply)
		if err != nil || !ok {
			t.Fatalf("PEPPER_REQUEST %d answered %T (%v)", nonce, decoded, err)
		}
		return reply
	}
	if first := ask(1); first.Status != transport.PepperOK {
		t.Fatalf("n3 as a member was answered %d %q", first.Status, first.Message)
	}
	c.Kill("n3")
	c.Await("the holder to apply n3 dead", 40*time.Second, func() bool {
		m, ok := memberOf(c.Node(holder).State(), "n3")
		return ok && m.State == cluster.MemberDead
	})
	second := ask(2)
	if second.Status != transport.PepperRefused || !strings.Contains(second.Message, "n3 is dead") {
		t.Fatalf("the removed member on its old connection was answered %d %q, want refused", second.Status, second.Message)
	}
	if second.Pepper.Held() {
		t.Fatal("the refusal carries key bytes")
	}
}

// TestAPepperIsHandedOnlyToACommittedMember (regression: admission is cluster.Raft.AdmitPeer):
// a node that is not a member is refused at the control channel's handshake, and the identity
// channel, which admits it, refuses the kind. Break: serve PEPPER_REQUEST on the identity channel;
// red.
func TestAPepperIsHandedOnlyToACommittedMember(t *testing.T) {
	c := harness.NewCluster(t, 2, harness.Options{ClusterPepper: true, NoDetection: true, Late: 1})
	bootstrapUsersOn(t, c, "n1")
	stranger := c.OpenJoiner()
	msg, err := transport.NewControlMessage(transport.Envelope{}, transport.PepperRequest{Nonce: [transport.PepperNonceLen]byte{3}})
	if err != nil {
		t.Fatal(err)
	}
	for _, ch := range []transport.Channel{transport.ChannelControl, transport.ChannelIdentity} {
		raw, err := c.CallAs(stranger, "n1", ch, msg)
		if err == nil {
			decoded, decodeErr := raw.Control()
			if reply, ok := decoded.(transport.PepperReply); ok && decodeErr == nil && reply.Status == transport.PepperOK {
				t.Fatalf("a node that is not a member got the key on the %v channel", ch)
			}
			t.Fatalf("a node that is not a member was answered on the %v channel with a %T (%v)", ch, decoded, decodeErr)
		}
	}
}

// TestTwoBootstrappersEndWithOnePepperOnEveryNode (spec 5.2): two data members generate two keys
// and propose USER_BOOTSTRAP at once; one commits, the other is refused, and the loser moves its
// file aside and fetches the cluster's, so every data member ends holding the committed kid.
func TestTwoBootstrappersEndWithOnePepperOnEveryNode(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{ClusterPepper: true, NoDetection: true})
	peppers := map[string]clusterpepper.Pepper{}
	for _, id := range []string{"n1", "n2"} {
		p, err := c.Node(id).BootstrapPepper()
		if err != nil {
			t.Fatal(err)
		}
		peppers[id] = p
	}
	if peppers["n1"].Equal(peppers["n2"]) {
		t.Fatal("two bootstrappers drew one key")
	}
	var wg sync.WaitGroup
	results := map[string]error{}
	var mu sync.Mutex
	for id, p := range peppers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			node := c.Node(id)
			root := credentialUnder(p.Key(node.State().ClusterID), p.KID(), pepperTestPassword, 0x60+byte(len(id)))
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, err := node.ProposeUserCommand(ctx, cluster.CommandUserBootstrap,
				cluster.UserBootstrapBody{Request: userRequestID(), Name: "root-" + id, Credential: root.cred, CreatedMs: 1}.Encode())
			mu.Lock()
			results[id] = err
			mu.Unlock()
		}()
	}
	wg.Wait()
	state := c.AwaitConverged(10 * time.Second)
	winner := ""
	for id, p := range peppers {
		if p.KID() == state.PepperKID {
			winner = id
		}
	}
	if winner == "" || results[winner] != nil {
		t.Fatalf("no bootstrapper's key is the state's %s (results %v)", state.PepperKID, results)
	}
	loser := map[string]string{"n1": "n2", "n2": "n1"}[winner]
	var refusal *cluster.Error
	if !errors.As(results[loser], &refusal) || refusal.Code != cluster.CodeConflict {
		t.Fatalf("the losing bootstrap answered %v, want CONFLICT", results[loser])
	}
	for _, id := range []string{"n1", "n2", "n3"} {
		awaitHeld(t, c, id, 40*time.Second)
	}
	aside := c.PepperFile(loser).Path + ".wrong-" + peppers[loser].KID()
	if kept, err := (clusterpepper.File{Path: aside}).Load(); err != nil || !kept.Equal(peppers[loser]) {
		t.Fatalf("the loser's key was not kept at %s: %v", aside, err)
	}
}

// TestAClusterWhoseFirstNodeIsLostBeforeUserBootstrapCanStillBootstrap (spec 5.2, F1): the node
// that committed BOOTSTRAP is gone before any user exists; another data member generates the key,
// its USER_BOOTSTRAP commits and the third member fetches the key. Break: restrict generation to
// the node that committed BOOTSTRAP; red.
func TestAClusterWhoseFirstNodeIsLostBeforeUserBootstrapCanStillBootstrap(t *testing.T) {
	c := harness.NewCluster(t, 3, harness.Options{ClusterPepper: true, NoDetection: true})
	c.Kill("n1")
	c.AwaitLeader(20 * time.Second)
	bootstrapUsersOn(t, c, "n2")
	awaitHeld(t, c, "n3", 10*time.Second)
	if kid := c.Node("n3").State().PepperKID; kid == "" {
		t.Fatal("the bootstrap did not set the state's kid")
	}
}

// scenarioWithUsers runs bootstrap, two creates, a password change and enough settings commands to
// force a Raft snapshot, then stops every node so each file is whole, and returns what must never
// be on disk.
func scenarioWithUsers(t *testing.T) (*harness.Cluster, map[string][]byte, map[string][]byte) {
	t.Helper()
	c := harness.NewCluster(t, 3, harness.Options{ClusterPepper: true, NoDetection: true, HoldSeedReports: true})
	pepper, root := bootstrapUsersOn(t, c, "n1")
	awaitHeld(t, c, "n2", 10*time.Second)
	n2 := c.Node("n2")
	state := n2.State()
	key, kid, err := n2.PasswordKey()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rootRow := state.Users[0]
	clientHashes := map[string][]byte{"root's client hash": root.clientHash}
	passwords := map[string][]byte{"root's password": []byte(pepperTestPassword)}
	for i, name := range []string{"ada", "bob"} {
		password := fmt.Sprintf("password of %s %d", name, i)
		cred := credentialUnder(key, kid, password, byte(0x70+i))
		body := cluster.UserCreateBody{Request: userRequestID(), By: "root", BySeq: rootRow.CreatedSeq, Name: name, Credential: cred.cred, Enabled: true, CreatedMs: 2, ExpectDropSeq: n2.State().UserDropSeq}.Encode()
		if _, err := n2.ProposeUserCommand(ctx, cluster.CommandUserCreate, body); err != nil {
			t.Fatalf("creating %s: %v", name, err)
		}
		clientHashes[name+"'s client hash"] = cred.clientHash
		passwords[name+"'s password"] = []byte(password)
	}
	ada, ok := n2.State().Users[0], n2.State().Users[0].Name == "ada"
	if !ok {
		t.Fatalf("the first row is %s, want ada", ada.Name)
	}
	changed := credentialUnder(key, kid, "ada's new password", 0x7F)
	alter := cluster.UserAlterBody{Request: userRequestID(), By: "root", BySeq: rootRow.CreatedSeq, Name: "ada", ExpectSeq: ada.ChangedSeq, Credential: &changed.cred, PasswordChangedMs: 3}.Encode()
	if _, err := n2.ProposeUserCommand(ctx, cluster.CommandUserAlter, alter); err != nil {
		t.Fatalf("changing ada's password: %v", err)
	}
	clientHashes["ada's new client hash"] = changed.clientHash
	passwords["ada's new password"] = []byte("ada's new password")
	c.AwaitConverged(20 * time.Second)
	for _, id := range c.IDs() {
		if err := c.Node(id).TakeRaftSnapshot(); err != nil {
			t.Fatalf("snapshot on %s: %v", id, err)
		}
	}
	keys := map[string][]byte{"pepper": pepper.Bytes(), "pepper key": derivedKeyBytes(t, pepper, state.ClusterID)}
	for _, id := range c.IDs() {
		c.Kill(id)
	}
	for name, secret := range clientHashes {
		passwords[name] = secret
	}
	return c, keys, passwords
}

// TestThePepperNeverAppearsInTheRaftDirectory (spec 9, P): after bootstrap, creates, a password
// change and a snapshot, no file in any node's data directory (the Raft log, its snapshots, the
// store) holds the pepper or the derived key, raw, hex or base64. Break: put the pepper in the
// USER_BOOTSTRAP body; red.
func TestThePepperNeverAppearsInTheRaftDirectory(t *testing.T) {
	c, keys, _ := scenarioWithUsers(t)
	for _, id := range c.IDs() {
		dir := c.DataDir(id)
		if snapshots := filepath.Join(dir, "raft", "snapshots"); !hasFiles(t, snapshots) {
			t.Fatalf("%s took no Raft snapshot; the scan would not cover one (%s)", id, snapshots)
		}
		if files := scanDirForSecrets(t, dir, keys); files == 0 {
			t.Fatalf("%s's data directory holds no file", id)
		}
	}
}

// TestTheRaftDirectoryHoldsNoPasswordAndNoUnpepperedHash (spec 3 requirement 2, P): the same run
// leaves no password and no unpeppered client hash in any node's data directory; only peppered
// hashes are replicated. Break: propose the unpeppered hash; red.
func TestTheRaftDirectoryHoldsNoPasswordAndNoUnpepperedHash(t *testing.T) {
	c, _, passwords := scenarioWithUsers(t)
	for _, id := range c.IDs() {
		scanDirForSecrets(t, c.DataDir(id), passwords)
	}
}

func hasFiles(t *testing.T, dir string) bool {
	t.Helper()
	found := false
	err := filepath.WalkDir(dir, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		found = found || !entry.IsDir()
		return nil
	})
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("listing %s: %v", dir, err)
	}
	return found
}
