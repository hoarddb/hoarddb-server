package wire

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

func relayApplied(seq uint64) transport.RelayReply {
	return transport.RelayReply{Status: transport.RelayApplied, Epoch: 1, StateSeq: seq}
}

// TestRelayLedgerAnswersARepeatFromItsMemory: after an applied proposal the same (sender, id)
// gets the stored answer and is not handed out to be committed again; another sender, or
// another id, is a new proposal.
func TestRelayLedgerAnswersARepeatFromItsMemory(t *testing.T) {
	var ledger relayLedger
	key := relayKey{from: "n2", id: [transport.RelayRequestIDLen]byte{1}}
	_, call, owner := ledger.begin(key)
	if !owner || call == nil {
		t.Fatal("the first copy of a proposal is not handed out to be committed")
	}
	ledger.finish(key, call, relayApplied(7))
	stored, again, owner := ledger.begin(key)
	if again != nil || owner || stored != relayApplied(7) {
		t.Fatalf("a repeat got stored %+v call %v owner %v, want the stored answer and nothing to commit", stored, again, owner)
	}
	for _, other := range []relayKey{{from: "n3", id: key.id}, {from: "n2", id: [transport.RelayRequestIDLen]byte{2}}} {
		if _, call, owner := ledger.begin(other); !owner || call == nil {
			t.Fatalf("%+v was taken for a repeat of %+v", other, key)
		}
	}
}

// TestRelayLedgerDoesNotRememberARefusal: a refusal or a not-leader answer changed nothing, so
// a repeat is evaluated again.
func TestRelayLedgerDoesNotRememberARefusal(t *testing.T) {
	var ledger relayLedger
	key := relayKey{from: "n2"}
	_, call, _ := ledger.begin(key)
	ledger.finish(key, call, transport.RelayReply{Status: transport.RelayNotLeader, Leader: "n1"})
	if _, call, owner := ledger.begin(key); !owner || call == nil {
		t.Fatal("a NOT_LEADER answer was remembered as if the command had applied")
	}
}

// TestRelayLedgerHoldsARepeatThatArrivesWhileTheFirstCommits: the second copy waits for the
// first and gets its answer; only one is handed out.
func TestRelayLedgerHoldsARepeatThatArrivesWhileTheFirstCommits(t *testing.T) {
	var ledger relayLedger
	key := relayKey{from: "n2", id: [transport.RelayRequestIDLen]byte{5}}
	_, first, owner := ledger.begin(key)
	if !owner {
		t.Fatal("the first copy was not the owner")
	}
	var waiters sync.WaitGroup
	for range 8 {
		_, second, owner := ledger.begin(key)
		if owner || second != first {
			t.Fatalf("a copy during the commit got owner %v call %p, want to wait on %p", owner, second, first)
		}
		waiters.Add(1)
		go func() {
			defer waiters.Done()
			<-second.done
			if second.reply != relayApplied(9) {
				t.Errorf("the waiting copy got %+v", second.reply)
			}
		}()
	}
	ledger.finish(key, first, relayApplied(9))
	waiters.Wait()
}

// TestRelayLedgerIsBounded: the ledger forgets the oldest proposal past relayLedgerCap, so its
// memory is fixed.
func TestRelayLedgerIsBounded(t *testing.T) {
	var ledger relayLedger
	for i := 0; i <= relayLedgerCap; i++ {
		key := relayKey{from: "n2", id: [transport.RelayRequestIDLen]byte{byte(i), byte(i >> 8)}}
		_, call, _ := ledger.begin(key)
		ledger.finish(key, call, relayApplied(uint64(i)))
	}
	if len(ledger.applied) != relayLedgerCap || len(ledger.order) != relayLedgerCap {
		t.Fatalf("ledger holds %d answers and %d order entries, want %d", len(ledger.applied), len(ledger.order), relayLedgerCap)
	}
	oldest := relayKey{from: "n2"}
	if _, call, owner := ledger.begin(oldest); !owner || call == nil {
		t.Fatal("the oldest proposal is still remembered past the cap")
	}
}

func TestOnlyTheKindsWithACallerAreRelayable(t *testing.T) {
	for _, kind := range []byte{cluster.CommandSetMemberState, cluster.CommandCreateDatabase, cluster.CommandSetSetting, cluster.CommandBumpSchema,
		cluster.CommandSeeded, cluster.CommandRetire} {
		if !relayable(kind) {
			t.Errorf("kind 0x%02x is not relayable", kind)
		}
	}
	for _, kind := range []byte{cluster.CommandBootstrap, cluster.CommandRegisterMember, cluster.CommandRemoveMember, cluster.CommandAlterDatabase,
		cluster.CommandDropDatabase, cluster.CommandSetVoters, cluster.CommandSetClusterVersion, 0, 0x7F} {
		if relayable(kind) {
			t.Errorf("kind 0x%02x is relayable; the leader owns it", kind)
		}
	}
}

// TestCommittedEffectReadsWhatACommandNamed: a proposer recognises an attempt it lost track of
// by the state it leaves, for the two commands a second application would refuse.
func TestCommittedEffectReadsWhatACommandNamed(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	id, err := node.CreateDatabase(ctx, "relayfx")
	if err != nil {
		t.Fatal(err)
	}
	createBody := func(name string, id uint64) []byte { return createDatabaseBody(name, int64(id), 1, "") }
	if _, done := node.committedEffect(cluster.CommandCreateDatabase, createBody("relayfx", id)); !done {
		t.Fatal("a database that exists under that name and id shows no effect")
	}
	for name, body := range map[string][]byte{"another id": createBody("relayfx", id+1), "another name": createBody("other", id)} {
		if _, done := node.committedEffect(cluster.CommandCreateDatabase, body); done {
			t.Fatalf("a CREATE_DATABASE with %s shows an effect it did not have", name)
		}
	}
	if err := node.BumpSchema(ctx, id, 1, 2, cluster.BumpNote{}); err != nil {
		t.Fatal(err)
	}
	if _, done := node.committedEffect(cluster.CommandBumpSchema, cluster.BumpSchemaBody(int64(id), 1, 2)); !done {
		t.Fatal("a bump to a version the database reached shows no effect")
	}
	if _, done := node.committedEffect(cluster.CommandBumpSchema, cluster.BumpSchemaBody(int64(id), 2, 3)); done {
		t.Fatal("a bump to a version the database has not reached shows an effect")
	}
	if _, done := node.committedEffect(cluster.CommandSetSetting, nil); done {
		t.Fatal("a command with no readable effect claimed one")
	}
}

// u30iSeedBits is a 1,024-byte seed bitmap with the named slots set.
func u30iSeedBits(slots ...int) []byte {
	bits := make([]byte, 1024)
	for _, slot := range slots {
		bits[slot/8] |= 1 << (slot % 8)
	}
	return bits
}

// TestCommittedEffectReadsTheBitRow: SEEDED shows its effect only once every bit it names
// is set in the committed row, RETIRE only once every bit it names is clear, and an absent
// row is all clear, so a RETIRE of it is already done and a SEEDED of it is not.
// [BREAK: done = true in committedEffect.]
func TestCommittedEffectReadsTheBitRow(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	self := node.Identity().ID
	var dbID int64
	for _, db := range node.State().Databases {
		if db.Name == u30iDB {
			dbID = db.ID
		}
	}
	if dbID == 0 {
		t.Fatalf("database %q is absent from the committed state", u30iDB)
	}
	effect := func(kind byte, slots ...int) bool {
		_, done := node.committedEffect(kind, cluster.SeedBitmapBody(self, dbID, u30iSeedBits(slots...)))
		return done
	}
	if effect(cluster.CommandSeeded, 3) {
		t.Fatal("an absent row shows a SEEDED effect")
	}
	if !effect(cluster.CommandRetire, 3) {
		t.Fatal("an absent row shows no RETIRE effect, though every bit a RETIRE clears is already clear")
	}
	if _, err := node.propose(ctx, cluster.CommandSeeded, cluster.SeedBitmapBody(self, dbID, u30iSeedBits(3, 5))); err != nil {
		t.Fatalf("SEEDED of slots 3 and 5: %v", err)
	}
	if !effect(cluster.CommandSeeded, 3, 5) {
		t.Fatal("a row holding both named bits shows no SEEDED effect")
	}
	if effect(cluster.CommandSeeded, 3, 4) {
		t.Fatal("a row missing one named bit shows a SEEDED effect")
	}
	if effect(cluster.CommandRetire, 3) {
		t.Fatal("a row holding the named bit shows a RETIRE effect")
	}
	if !effect(cluster.CommandRetire, 4) {
		t.Fatal("a row whose named bit is clear shows no RETIRE effect")
	}
	if _, err := node.propose(ctx, cluster.CommandRetire, cluster.SeedBitmapBody(self, dbID, u30iSeedBits(3))); err != nil {
		t.Fatalf("RETIRE of slot 3: %v", err)
	}
	if !effect(cluster.CommandRetire, 3) {
		t.Fatal("a cleared row shows no RETIRE effect")
	}
	if effect(cluster.CommandSeeded, 3) {
		t.Fatal("a cleared row shows a SEEDED effect")
	}
	if _, done := node.committedEffect(cluster.CommandSeeded, []byte{}); done {
		t.Fatal("a body with no readable node, db_id or bits claimed an effect")
	}
}

// TestCommittedEffectReadsSettingsAndMemberStates: the two value-setting commands name what
// they set, so a repeat is recognised as done.
func TestCommittedEffectReadsSettingsAndMemberStates(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := node.SetSetting(ctx, "dead_ms", 424242); err != nil {
		t.Fatal(err)
	}
	set := func(value int64) []byte {
		return bsoncore.BuildDocument(nil, append(bsoncore.AppendStringElement(nil, "key", "dead_ms"), bsoncore.AppendInt64Element(nil, "value", value)...))
	}
	if _, done := node.committedEffect(cluster.CommandSetSetting, set(424242)); !done {
		t.Fatal("a setting that holds the value shows no effect")
	}
	if _, done := node.committedEffect(cluster.CommandSetSetting, set(1)); done {
		t.Fatal("a setting that holds another value shows an effect")
	}
	self := node.Identity().ID
	member := func(state string) []byte {
		return bsoncore.BuildDocument(nil, append(bsoncore.AppendStringElement(nil, "id", self), bsoncore.AppendStringElement(nil, "state", state)...))
	}
	if _, done := node.committedEffect(cluster.CommandSetMemberState, member("active")); !done {
		t.Fatal("a member in the named state shows no effect")
	}
	if _, done := node.committedEffect(cluster.CommandSetMemberState, member("down")); done {
		t.Fatal("a member in another state shows an effect")
	}
}

// TestAnUnknownRaftErrorIsAnUndecidedOutcome: only the FSM's own refusals prove a command is
// not in the log. Any other error from the Raft library leaves the outcome unknown, and the
// leader says so (NO_QUORUM, undecided) instead of calling it a refusal.
func TestAnUnknownRaftErrorIsAnUndecidedOutcome(t *testing.T) {
	node := &Node{}
	if reply := node.relayReply(cluster.Result{}, errors.New("commit through raft: log store is wedged")); reply.Status != transport.RelayNoQuorum {
		t.Fatalf("an unknown error answered %+v, want NO_QUORUM", reply)
	}
	refusal := &cluster.Error{Code: cluster.CodeConflict, Message: "refused"}
	if reply := node.relayReply(cluster.Result{}, refusal); reply.Status != transport.RelayRefused || cluster.Code(reply.Code) != cluster.CodeConflict {
		t.Fatalf("an FSM refusal answered %+v, want REFUSED", reply)
	}
	if reply := node.relayReply(cluster.Result{}, &cluster.NotLeaderError{Refusal: refusal, Leader: "n1", Undecided: true}); reply.Status != transport.RelayNoQuorum {
		t.Fatalf("lost leadership answered %+v, want NO_QUORUM", reply)
	}
	if decided(errors.New("commit through raft: wedged")) || !decided(refusal) {
		t.Fatal("decided must hold for an FSM refusal and not for an unknown error")
	}
}

func TestOnlyStateMovedIsRetryable(t *testing.T) {
	if !retryable(&cluster.Error{Code: cluster.CodeStateMoved}) {
		t.Fatal("STATE_MOVED changed nothing and must be retried")
	}
	for _, code := range []cluster.Code{cluster.CodeConflict, cluster.CodeInvalidArgument, cluster.CodeSchemaNotCurrent, cluster.CodeUnavailable} {
		if retryable(&cluster.Error{Code: code}) {
			t.Fatalf("%s is final", code)
		}
	}
}

// TestARetriedCreateThatLostTheRaceDoesNotReportItsOwnOptions: a timed-out create is recognised by
// the committed row, and a row of the same name and id with other storage or copies is another
// create's effect, not this one's.
// [BREAK: drop the rf and storage comparison in committedEffect]
func TestARetriedCreateThatLostTheRaceDoesNotReportItsOwnOptions(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	id, err := node.CreateDatabaseWithStorage(ctx, "raced", 1, "memory")
	if err != nil {
		t.Fatal(err)
	}
	if _, done := node.committedEffect(cluster.CommandCreateDatabase, createDatabaseBody("raced", int64(id), 1, "memory")); !done {
		t.Fatal("the create that committed shows no effect")
	}
	for name, body := range map[string][]byte{
		"disk storage":       createDatabaseBody("raced", int64(id), 1, ""),
		"named disk storage": createDatabaseBody("raced", int64(id), 1, "disk"),
		"another copy count": createDatabaseBody("raced", int64(id), 2, "memory"),
		"no readable rf":     bsoncore.BuildDocument(nil, append(bsoncore.AppendStringElement(nil, "name", "raced"), bsoncore.AppendInt64Element(nil, "id", int64(id))...)),
	} {
		if _, done := node.committedEffect(cluster.CommandCreateDatabase, body); done {
			t.Errorf("a create with %s was read as the effect of a memory create", name)
		}
	}
}
