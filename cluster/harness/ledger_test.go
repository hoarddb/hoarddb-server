package harness_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// ledgerBucket is the bucket proofIDsOwnedBy places ids in.
const ledgerBucket = proofBucket

// ledgerSetup forms three voters with a database of replication factor rf and a bucket
// that declares the name and seq fields the ledger writes.
func ledgerSetup(t *testing.T, rf int, opts harness.Options) *harness.Cluster {
	t.Helper()
	c := harness.NewCluster(t, 3, opts)
	leader := c.Node(c.AwaitLeader(10 * time.Second))
	if _, err := leader.CreateDatabaseRF(proofCtx(t), proofDB, rf); err != nil {
		t.Fatalf("creating the RF=%d database: %v", rf, err)
	}
	c.AwaitConverged(10 * time.Second)
	if _, err := leader.CreateBucket(proofCtx(t), proofDB, ledgerBucket, wire.Field{Name: "name", Type: "string"}, wire.Field{Name: "seq", Type: "int"}); err != nil {
		t.Fatalf("creating the bucket: %v", err)
	}
	return c
}

// recordingTB is a test whose failures and log lines are kept instead of failing the run,
// so a test of the judge can assert what the judge said.
type recordingTB struct {
	testing.TB
	errors, logs []string
}

func (r *recordingTB) Errorf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}
func (r *recordingTB) Logf(format string, args ...any) {
	r.logs = append(r.logs, fmt.Sprintf(format, args...))
}

// TestAnInsertNamesTheOwnerThatOrderedIt: a write through a node that does not own the
// pair is forwarded, and the answer names the owner that ordered it, not the entry node.
func TestAnInsertNamesTheOwnerThatOrderedIt(t *testing.T) {
	c := proofSetup(t, harness.Options{})
	_, owner, copyNode, outsider := proofID(t, c, "served")
	ids := proofIDsOwnedBy(t, c, owner, copyNode, "served", 3)
	for i, via := range []string{outsider, copyNode, owner} {
		got := proofInsert(t, c, via, ids[i], "Ada")
		if got.ServedBy != owner {
			t.Fatalf("an insert through %s was ordered by %q, want the owner %s", via, got.ServedBy, owner)
		}
	}
}

// TestTheLedgerRecordsAcksObservationsAndEvents: what a client wrote and read is kept in
// order, and a read returns the counter in the body, not a log position.
func TestTheLedgerRecordsAcksObservationsAndEvents(t *testing.T) {
	c := ledgerSetup(t, 2, harness.Options{})
	ids := proofIDsOwnedBy(t, c, "n1", "n2", "rec", 3)
	if o, _, err := c.Node("n1").Owner(proofCtx(t), proofDB, ledgerBucket, ids[0]); err != nil || o != "n1" {
		t.Fatalf("the ids are placed for owner n1, got %q (err %v)", o, err)
	}
	l := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket, ProbeID: ids[0]})
	ctx := proofCtx(t)
	// Burn two seqs on other ids so the counter and the log position cannot be equal by chance.
	for _, id := range ids[:2] {
		if _, err := l.Write(ctx, c, "n3", "ada", id); err != nil {
			t.Fatalf("write %s: %v", id, err)
		}
	}
	l.Fault("kill n3-not-really")
	l.Note(c, "after the writes")
	if _, err := l.Write(ctx, c, "n1", "bea", ids[2]); err != nil {
		t.Fatalf("write: %v", err)
	}
	seq, err := l.Read(ctx, c, "n2", "ada", ids[1])
	if err != nil || seq != 2 {
		t.Fatalf("read of ada's second write: seq %d err %v, want 2", seq, err)
	}
	if seq, err = l.Read(ctx, c, "n1", "bea", ids[2]); err != nil || seq != 1 {
		t.Fatalf("read of bea's first write: seq %d err %v, want 1", seq, err)
	}
	if seq, err = l.Read(ctx, c, "n1", "bea", "absent"); err == nil || seq != -1 {
		t.Fatalf("read of an absent id: seq %d err %v, want -1 and NOT_FOUND", seq, err)
	}
	acks, obs, events := l.Acks(), l.Obs(), l.Events()
	if len(acks) != 3 || len(obs) != 3 || len(events) != 2 {
		t.Fatalf("recorded %d acks, %d observations, %d events, want 3, 3, 2", len(acks), len(obs), len(events))
	}
	if acks[0].Client != "ada" || acks[0].Seq != 1 || acks[1].Seq != 2 || acks[2].Client != "bea" || acks[2].Seq != 1 {
		t.Fatalf("acks out of order or misnumbered: %+v", acks)
	}
	if acks[0].Via != "n3" || acks[0].ServedBy != "n1" || acks[0].RF != 2 || acks[0].Outcome != machine.OutcomeApplied || acks[0].Copies != 2 || len(acks[0].Running) != 3 {
		t.Fatalf("ack 0 is %+v, want entry n3, owner n1, RF 2, applied/2, three running", acks[0])
	}
	if obs[2].Code != cluster.CodeNotFound || obs[2].Seq != -1 {
		t.Fatalf("the absent read was recorded as %+v, want NOT_FOUND and seq -1", obs[2])
	}
	if obs[0].ServedBy != "n1" || !obs[0].ViaRunning {
		t.Fatalf("the first read was recorded as %+v, want served by the owner n1 from a running node", obs[0])
	}
	if events[0].What != "kill n3-not-really" || events[0].Sampled || !events[1].Sampled || events[1].Epoch == 0 || len(events[1].Positions) != 3 {
		t.Fatalf("events are %+v, want the fault then a sample with the epoch and three positions", events)
	}
	if events[1].Positions["n1"].Seq < 1 {
		t.Fatalf("the sample saw the probe pair of the owner at %+v after a write to the pair", events[1].Positions["n1"])
	}
}

// ledgerIDOwnedBy returns an id whose slot the committed table gives to owner.
func ledgerIDOwnedBy(t *testing.T, c *harness.Cluster, owner, seed string) string {
	t.Helper()
	for i := 0; i < 5000; i++ {
		id := fmt.Sprintf("%s-%d", seed, i)
		if got, _, err := c.Node(c.IDs()[0]).Owner(proofCtx(t), proofDB, ledgerBucket, id); err == nil && got == owner {
			return id
		}
	}
	t.Fatalf("no id owned by %s", owner)
	return ""
}

func verdictNamed(t *testing.T, verdicts []harness.Verdict, name string) harness.Verdict {
	t.Helper()
	for _, v := range verdicts {
		if v.Name == name {
			return v
		}
	}
	t.Fatalf("no verdict named %q in %+v", name, verdicts)
	return harness.Verdict{}
}

// TestNoAckedWriteLostFailsOnAMissingIdAndExcusesTheDeclaredOne: with one copy per slot,
// killing the owner after its ack loses the write. Undeclared, that is a violation that
// names the id and the kill; declared, the verdict holds and the loss is printed.
func TestNoAckedWriteLostFailsOnAMissingIdAndExcusesTheDeclaredOne(t *testing.T) {
	c := ledgerSetup(t, 1, harness.Options{FastDetection: true})
	victim := "n2"
	id := ledgerIDOwnedBy(t, c, victim, "lost")
	rec := &recordingTB{TB: t}
	l := harness.NewLedger(rec, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	if _, err := l.Write(proofCtx(t), c, "n1", "ada", id); err != nil {
		t.Fatalf("write: %v", err)
	}
	l.Fault("kill " + victim)
	c.Kill(victim)

	verdict := verdictNamed(t, l.Check(proofCtx(t), c, harness.CheckOptions{Settle: time.Second}), harness.InvNoAckedWriteLost)
	if verdict.OK || !strings.Contains(verdict.Detail, id) || verdict.After == nil || verdict.After.What != "kill "+victim {
		t.Fatalf("an acked write on a killed single copy was judged %+v, want a violation naming %s and the kill", verdict, id)
	}

	rec.logs = nil
	verdicts := l.Check(proofCtx(t), c, harness.CheckOptions{Settle: time.Second, Lost: map[string]string{id: "RF 1 owner killed"}})
	verdict = verdictNamed(t, verdicts, harness.InvNoAckedWriteLost)
	if !verdict.OK || len(verdict.Excused) != 1 || !strings.Contains(verdict.Excused[0], id+": acked applied/1 via n1") || !strings.HasSuffix(verdict.Excused[0], "excused: RF 1 owner killed") {
		t.Fatalf("the declared loss was judged %+v, want OK with the id printed in Excused", verdict)
	}
	if joined := strings.Join(rec.logs, "\n"); !strings.Contains(joined, id) || !strings.Contains(joined, "RF 1 owner killed") {
		t.Fatalf("the declared loss set was not logged: %q", joined)
	}
}

// judge runs Check with a short settle on a ledger whose acks were fed by hand and so are
// not on the cluster, and returns the verdict of one invariant.
func judge(t *testing.T, l *harness.Ledger, c *harness.Cluster, name string) harness.Verdict {
	t.Helper()
	return verdictNamed(t, l.Check(proofCtx(t), c, harness.CheckOptions{Settle: 50 * time.Millisecond}), name)
}

// TestAStaleReadFailsAtTheMomentItIsSeen: seq 5 then seq 3 for one client and id fails the
// test when the second is recorded, naming both nodes and the fault before it; an equal
// re-read is not a regression.
func TestAStaleReadFailsAtTheMomentItIsSeen(t *testing.T) {
	c := ledgerSetup(t, 2, harness.Options{})
	rec := &recordingTB{TB: t}
	l := harness.NewLedger(rec, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	l.FeedObs(harness.Obs{Client: "ada", ID: "x", Seq: 5, Via: "n1", ServedBy: "n1", ViaRunning: true})
	if len(rec.errors) != 0 {
		t.Fatalf("a first read failed the test: %v", rec.errors)
	}
	l.Fault("partition n1|n2,n3")
	l.FeedObs(harness.Obs{Client: "ada", ID: "x", Seq: 3, Via: "n2", ServedBy: "n3", ViaRunning: true})
	if len(rec.errors) != 1 {
		t.Fatalf("a read of seq 3 after seq 5 raised %d failures at once, want 1: %v", len(rec.errors), rec.errors)
	}
	l.FeedObs(harness.Obs{Client: "ada", ID: "x", Seq: 3, Via: "n2", ServedBy: "n3", ViaRunning: true})
	if len(rec.errors) != 1 {
		t.Fatalf("an equal re-read failed the test: %v", rec.errors)
	}
	verdict := judge(t, l, c, harness.InvReadsMonotone)
	for _, want := range []string{`"ada"`, " x ", "seq 3", "seq 5", "served by n1", "served by n3"} {
		if !strings.Contains(verdict.Detail, want) {
			t.Fatalf("the verdict %q does not contain %q", verdict.Detail, want)
		}
	}
	if verdict.OK || verdict.After == nil || verdict.After.What != "partition n1|n2,n3" {
		t.Fatalf("the verdict is %+v, want a violation after the partition", verdict)
	}
	if !strings.Contains(rec.errors[0], "partition n1|n2,n3") {
		t.Fatalf("the online failure %q does not name the fault before it", rec.errors[0])
	}
}

// TestTwoOwnersAckingOneIdAtOneEpochIsAViolation: two tails at one epoch; one owner at two
// epochs is a handoff and fine.
func TestTwoOwnersAckingOneIdAtOneEpochIsAViolation(t *testing.T) {
	c := ledgerSetup(t, 2, harness.Options{})
	l := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	l.FeedAck(harness.Ack{Client: "ada", ID: "handoff", Seq: 1, Pos: cluster.Position{Epoch: 2, Seq: 1}, ServedBy: "n1", Outcome: machine.OutcomeApplied, Copies: 2, RF: 2})
	l.FeedAck(harness.Ack{Client: "ada", ID: "handoff", Seq: 2, Pos: cluster.Position{Epoch: 3, Seq: 1}, ServedBy: "n2", Outcome: machine.OutcomeApplied, Copies: 2, RF: 2})
	if verdict := judge(t, l, c, harness.InvOneTailPerPair); !verdict.OK {
		t.Fatalf("a handoff across epochs was judged %+v", verdict)
	}
	// One owner acking twice at one epoch is an ordinary run of writes, not two tails.
	l.FeedAck(harness.Ack{Client: "ada", ID: "steady", Seq: 3, Pos: cluster.Position{Epoch: 3, Seq: 1}, ServedBy: "n1", Outcome: machine.OutcomeApplied, Copies: 2, RF: 2})
	l.FeedAck(harness.Ack{Client: "ada", ID: "steady", Seq: 4, Pos: cluster.Position{Epoch: 3, Seq: 2}, ServedBy: "n1", Outcome: machine.OutcomeApplied, Copies: 2, RF: 2})
	if verdict := judge(t, l, c, harness.InvOneTailPerPair); !verdict.OK {
		t.Fatalf("one owner acknowledging an id twice at one epoch was judged %+v", verdict)
	}
	l.Fault("partition n1|n2,n3")
	l.FeedAck(harness.Ack{Client: "bea", ID: "split", Seq: 1, Pos: cluster.Position{Epoch: 3, Seq: 4}, ServedBy: "n1", Outcome: machine.OutcomeApplied, Copies: 2, RF: 2})
	l.FeedAck(harness.Ack{Client: "bea", ID: "split", Seq: 2, Pos: cluster.Position{Epoch: 3, Seq: 4}, ServedBy: "n2", Outcome: machine.OutcomeApplied, Copies: 2, RF: 2})
	verdict := judge(t, l, c, harness.InvOneTailPerPair)
	if verdict.OK || !strings.Contains(verdict.Detail, "split") || !strings.Contains(verdict.Detail, "epoch 3") || !strings.Contains(verdict.Detail, "n1 and n2") || verdict.After == nil || verdict.After.What != "partition n1|n2,n3" {
		t.Fatalf("two owners at epoch 3 were judged %+v", verdict)
	}
}

// TestAppliedWithAMinorityOfCopiesIsAViolation: the outcome word must agree with the copies.
func TestAppliedWithAMinorityOfCopiesIsAViolation(t *testing.T) {
	c := ledgerSetup(t, 2, harness.Options{})
	l := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	good := []harness.Ack{
		{ID: "a", Seq: 1, Outcome: machine.OutcomeApplied, Copies: 2, RF: 3},
		{ID: "b", Seq: 2, Outcome: machine.OutcomeAppliedDegraded, Copies: 1, RF: 3},
		{ID: "c", Seq: 3, Outcome: machine.OutcomeUnknown, Copies: 1, RF: 3},
	}
	for _, ack := range good {
		ack.Client = "ada"
		l.FeedAck(ack)
	}
	if verdict := judge(t, l, c, harness.InvAckVocabularySound); !verdict.OK {
		t.Fatalf("sound answers were judged %+v", verdict)
	}
	for _, bad := range []harness.Ack{
		{ID: "d", Seq: 4, Outcome: machine.OutcomeApplied, Copies: 1, RF: 3},
		{ID: "e", Seq: 5, Outcome: machine.OutcomeAppliedDegraded, Copies: 2, RF: 3},
		{ID: "f", Seq: 6, Outcome: machine.OutcomeAppliedDegraded, Copies: 0, RF: 3},
	} {
		fresh := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
		bad.Client = "ada"
		fresh.FeedAck(bad)
		if verdict := judge(t, fresh, c, harness.InvAckVocabularySound); verdict.OK || !strings.Contains(verdict.Detail, bad.ID) {
			t.Fatalf("%s %v/%d at RF 3 was judged %+v", bad.ID, bad.Outcome, bad.Copies, verdict)
		}
	}
	both := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	both.FeedAck(harness.Ack{Client: "ada", ID: "g", Seq: 7, Outcome: machine.OutcomeUnknown, Copies: 1, RF: 3})
	both.FeedAck(harness.Ack{Client: "ada", ID: "g", Seq: 7, Outcome: machine.OutcomeApplied, Copies: 2, RF: 3})
	if verdict := judge(t, both, c, harness.InvAckVocabularySound); verdict.OK || !strings.Contains(verdict.Detail, "both unknown and applied") {
		t.Fatalf("an unknown beside an applied of one request was judged %+v", verdict)
	}
}

// TestADeadlineFromARunningNodeIsAViolationAndFromAKilledNodeIsNot: I5 separates a node that
// is down, whose clients time out by contract, from one that is up and owes a named code.
func TestADeadlineFromARunningNodeIsAViolationAndFromAKilledNodeIsNot(t *testing.T) {
	c := ledgerSetup(t, 2, harness.Options{})
	const deadline = "context deadline exceeded"
	killed := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	killed.FeedObs(harness.Obs{Client: "ada", ID: "x", Seq: -1, Via: "n1", Text: deadline, ViaRunning: false})
	killed.FeedObs(harness.Obs{Client: "ada", ID: "x", Seq: -1, Via: "n2", Code: cluster.CodeNoQuorum, ViaRunning: true})
	killed.FeedObs(harness.Obs{Client: "ada", ID: "y", Seq: -1, Via: "n2", Code: cluster.CodeNotFound, ViaRunning: true})
	if verdict := judge(t, killed, c, harness.InvRefusalsAreNamed); !verdict.OK {
		t.Fatalf("a dead node's deadline and a live node's NO_QUORUM were judged %+v", verdict)
	}
	running := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	running.Fault("partition n1|n2,n3")
	running.FeedObs(harness.Obs{Client: "ada", ID: "x", Seq: -1, Via: "n1", Text: deadline, ViaRunning: true})
	verdict := judge(t, running, c, harness.InvRefusalsAreNamed)
	if verdict.OK || !strings.Contains(verdict.Detail, "n1") || !strings.Contains(verdict.Detail, deadline) || verdict.After == nil || verdict.After.What != "partition n1|n2,n3" {
		t.Fatalf("a live node's bare deadline was judged %+v", verdict)
	}
	unnamed := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	unnamed.FeedObs(harness.Obs{Client: "ada", ID: "x", Seq: -1, Via: "n1", Code: cluster.CodeSeqGap, ViaRunning: true})
	if verdict := judge(t, unnamed, c, harness.InvRefusalsAreNamed); verdict.OK {
		t.Fatalf("a live node's SEQ_GAP was judged %+v, want a violation: it is not a refusal a fault may cause", verdict)
	}
	endless := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	for range 4 {
		endless.FeedObs(harness.Obs{Client: "ada", ID: "x", Seq: -1, Via: "n1", Code: cluster.CodeLeaseLapsed, ViaRunning: true})
	}
	if verdict := verdictNamed(t, endless.Check(proofCtx(t), c, harness.CheckOptions{Settle: 50 * time.Millisecond, MaxRefused: 3}), harness.InvRefusalsAreNamed); verdict.OK {
		t.Fatalf("four refusals in a row with a bound of three were judged %+v", verdict)
	}
}

// TestTheLedgerJudgesAnOwnerKillAtRF2AsClean re-states
// TestTheOwnerKilledAtRF2LosesNoWriteBothCopiesAcknowledged through the ledger: every
// invariant holds across an owner kill, a failover and writes after it.
func TestTheLedgerJudgesAnOwnerKillAtRF2AsClean(t *testing.T) {
	c := ledgerSetup(t, 2, harness.Options{FastDetection: true})
	_, owner, copyNode, outsider := proofID(t, c, "o")
	ids := proofIDsOwnedBy(t, c, owner, copyNode, "ledger", 6)
	l := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket, ProbeID: ids[0]})
	ctx := proofCtx(t)
	for _, id := range ids {
		got, err := l.Write(ctx, c, owner, "ada", id)
		if err != nil || got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed != 2 {
			t.Fatalf("before the kill %s answered %v/%d err %v, want applied/2", id, got.Outcome, got.CopiesConfirmed, err)
		}
	}
	l.Note(c, "before the kill")
	epochBefore := c.Node(copyNode).State().Epoch
	l.Fault("kill " + owner)
	c.Kill(owner)
	c.Await("the survivors to commit the owner down (an epoch bump)", 30*time.Second, func() bool {
		for _, id := range []string{copyNode, outsider} {
			if c.Node(id).State().Epoch <= epochBefore {
				return false
			}
		}
		return true
	})
	c.AwaitLeader(10 * time.Second)
	c.AwaitLeases(20 * time.Second)
	l.Note(c, "after the failover")
	for _, via := range []string{copyNode, outsider} {
		for _, id := range ids {
			deadline := time.Now().Add(30 * time.Second)
			for {
				if _, err := l.Read(ctx, c, via, "bea", id); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("acknowledged write %s is not readable through %s after the failover", id, via)
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
	fresh := 0
	for i := 0; fresh < 4; i++ {
		id := fmt.Sprintf("post-%d", i)
		if o, _, err := c.Node(copyNode).Owner(ctx, proofDB, ledgerBucket, id); err != nil || o != copyNode {
			continue
		}
		via := []string{copyNode, outsider}[fresh%2]
		deadline := time.Now().Add(30 * time.Second)
		for {
			got, err := l.Write(ctx, c, via, "ada", id)
			if err == nil {
				if got.Outcome != machine.OutcomeAppliedDegraded || got.CopiesConfirmed != 1 {
					t.Fatalf("write %s through %s answered %v with %d copies, want applied_degraded and 1", id, via, got.Outcome, got.CopiesConfirmed)
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("a write of %s through %s was never acknowledged after the failover: %v", id, via, err)
			}
			time.Sleep(100 * time.Millisecond)
		}
		fresh++
	}
	l.Note(c, "after the new writes")
	for _, v := range l.Check(ctx, c, harness.CheckOptions{Settle: 20 * time.Second}) {
		if !v.OK {
			t.Errorf("%s was violated: %s (after %+v)", v.Name, v.Detail, v.After)
		}
	}
}

// TestAnAckedWriteThatNeverLandedIsLostBehindAnotherClientsOlderDocument: ada's acked seq 9
// was acknowledged after bea's seq 1, yet the document is bea's seq 1, so ada's write is lost.
// The mirror order, bea acked after ada, is the spec's "another client wrote since" and is fine.
func TestAnAckedWriteThatNeverLandedIsLostBehindAnotherClientsOlderDocument(t *testing.T) {
	c := ledgerSetup(t, 2, harness.Options{})
	ctx := proofCtx(t)
	lost := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	if _, err := lost.Write(ctx, c, "n1", "bea", "x-lost"); err != nil {
		t.Fatalf("write: %v", err)
	}
	lost.FeedAck(harness.Ack{Client: "ada", ID: "x-lost", Seq: 9, Outcome: machine.OutcomeApplied, Copies: 2, RF: 2, ServedBy: "n1"})
	verdict := judge(t, lost, c, harness.InvNoAckedWriteLost)
	if verdict.OK || !strings.Contains(verdict.Detail, `"ada" seq 9`) {
		t.Fatalf("ada's later acked seq 9 that never landed was judged %+v, want a violation naming it", verdict)
	}

	later := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	later.FeedAck(harness.Ack{Client: "ada", ID: "x-later", Seq: 9, Outcome: machine.OutcomeApplied, Copies: 2, RF: 2, ServedBy: "n1"})
	if _, err := later.Write(ctx, c, "n1", "bea", "x-later"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if verdict := judge(t, later, c, harness.InvNoAckedWriteLost); !verdict.OK {
		t.Fatalf("a document another client acked after ada's was judged %+v, want OK", verdict)
	}
}

// TestTwoWritersOnOneIdAreNotARegression: seq is each writer's own counter. ada reading bea's
// seq 5 and then cea's seq 1 is a new writer's upsert, not a stale read; the same writer going
// from seq 5 to seq 3 still is.
func TestTwoWritersOnOneIdAreNotARegression(t *testing.T) {
	c := ledgerSetup(t, 2, harness.Options{})
	rec := &recordingTB{TB: t}
	l := harness.NewLedger(rec, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	l.FeedObs(harness.Obs{Client: "ada", ID: "x", Writer: "bea", Seq: 5, Via: "n1", ServedBy: "n1", ViaRunning: true})
	l.FeedObs(harness.Obs{Client: "ada", ID: "x", Writer: "cea", Seq: 1, Via: "n1", ServedBy: "n1", ViaRunning: true})
	if len(rec.errors) != 0 || !judge(t, l, c, harness.InvReadsMonotone).OK {
		t.Fatalf("cea's seq 1 after bea's seq 5 was judged a regression: %v", rec.errors)
	}
	l.FeedObs(harness.Obs{Client: "ada", ID: "x", Writer: "bea", Seq: 3, Via: "n2", ServedBy: "n2", ViaRunning: true})
	if len(rec.errors) != 1 || judge(t, l, c, harness.InvReadsMonotone).OK {
		t.Fatalf("bea's seq 3 after bea's seq 5 was not judged a regression: %v", rec.errors)
	}
}

// TestAnAnswerOlderThanWhatTheClientAlreadyHadIsARegression: the spec's handoff promise is "no
// stale read" (write-path-and-replication, HANDOFF_READY: "B is current (no missed write) and A
// has stopped ordering and answering (no stale read)"; conventions rule 35). So after a served
// read, NOT_FOUND is a regression; and a client that holds an applied (majority) ack for its own
// seq must not read an older seq of its own writes. An applied_degraded ack promises less.
func TestAnAnswerOlderThanWhatTheClientAlreadyHadIsARegression(t *testing.T) {
	c := ledgerSetup(t, 2, harness.Options{})
	missing := &recordingTB{TB: t}
	l := harness.NewLedger(missing, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	l.FeedObs(harness.Obs{Client: "ada", ID: "gone", Code: cluster.CodeNotFound, Seq: -1, Via: "n1", ViaRunning: true})
	l.FeedObs(harness.Obs{Client: "ada", ID: "gone", Writer: "bea", Seq: 5, Via: "n1", ServedBy: "n1", ViaRunning: true})
	if len(missing.errors) != 0 {
		t.Fatalf("NOT_FOUND before any served read failed the test: %v", missing.errors)
	}
	l.Fault("kill n1")
	l.FeedObs(harness.Obs{Client: "ada", ID: "gone", Code: cluster.CodeNotFound, Seq: -1, Via: "n2", ViaRunning: true})
	verdict := judge(t, l, c, harness.InvReadsMonotone)
	if len(missing.errors) != 1 || verdict.OK || !strings.Contains(verdict.Detail, "NOT_FOUND") || verdict.After == nil || verdict.After.What != "kill n1" {
		t.Fatalf("NOT_FOUND after a served read: online %v, verdict %+v, want one failure naming NOT_FOUND after the kill", missing.errors, verdict)
	}

	own := &recordingTB{TB: t}
	l = harness.NewLedger(own, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	l.FeedAck(harness.Ack{Client: "ada", ID: "mine", Seq: 5, Outcome: machine.OutcomeApplied, Copies: 2, RF: 2})
	l.FeedObs(harness.Obs{Client: "bea", ID: "mine", Writer: "ada", Seq: 3, Via: "n1", ViaRunning: true})
	if len(own.errors) != 0 {
		t.Fatalf("another client reading seq 3 failed the test: %v", own.errors)
	}
	l.FeedObs(harness.Obs{Client: "ada", ID: "mine", Writer: "ada", Seq: 3, Via: "n1", ServedBy: "n1", ViaRunning: true})
	verdict = judge(t, l, c, harness.InvReadsMonotone)
	if len(own.errors) != 1 || verdict.OK || !strings.Contains(verdict.Detail, "seq 3") || !strings.Contains(verdict.Detail, "seq 5") {
		t.Fatalf("a read of own seq 3 after an applied ack of seq 5: online %v, verdict %+v", own.errors, verdict)
	}

	degraded := &recordingTB{TB: t}
	l = harness.NewLedger(degraded, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	l.FeedAck(harness.Ack{Client: "ada", ID: "thin", Seq: 5, Outcome: machine.OutcomeAppliedDegraded, Copies: 1, RF: 2})
	l.FeedObs(harness.Obs{Client: "ada", ID: "thin", Writer: "ada", Seq: 3, Via: "n1", ServedBy: "n1", ViaRunning: true})
	if len(degraded.errors) != 0 {
		t.Fatalf("a read below an applied_degraded ack failed the test: %v", degraded.errors)
	}
}

// TestAnAckIsJudgedAgainstTheConcernItAskedFor: I4 reads Ack.Concern. A concern-one write
// answered applied with one copy at RF 2 is sound; the same ack at the default concern, or a
// concern-all ack short of every copy, is not.
// [BREAK: copiesNeeded returns majority(rf) for ConcernOne].
func TestAnAckIsJudgedAgainstTheConcernItAskedFor(t *testing.T) {
	c := ledgerSetup(t, 2, harness.Options{})
	sound := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	sound.FeedAck(harness.Ack{Client: "ada", ID: "a", Seq: 1, Outcome: machine.OutcomeApplied, Copies: 1, RF: 2, Concern: machine.ConcernOne})
	sound.FeedAck(harness.Ack{Client: "ada", ID: "b", Seq: 2, Outcome: machine.OutcomeApplied, Copies: 3, RF: 3, Concern: machine.ConcernAll})
	if verdict := judge(t, sound, c, harness.InvAckVocabularySound); !verdict.OK {
		t.Fatalf("a concern-one ack with one copy at RF 2 was judged %+v", verdict)
	}
	for _, bad := range []harness.Ack{
		{ID: "c", Seq: 3, Outcome: machine.OutcomeApplied, Copies: 1, RF: 2},
		{ID: "d", Seq: 4, Outcome: machine.OutcomeApplied, Copies: 2, RF: 3, Concern: machine.ConcernAll},
		{ID: "e", Seq: 5, Outcome: machine.OutcomeAppliedDegraded, Copies: 1, RF: 2, Concern: machine.ConcernOne},
	} {
		fresh := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
		bad.Client = "ada"
		fresh.FeedAck(bad)
		if verdict := judge(t, fresh, c, harness.InvAckVocabularySound); verdict.OK || !strings.Contains(verdict.Detail, bad.ID) {
			t.Fatalf("%s %v/%d concern %v at RF %d was judged %+v", bad.ID, bad.Outcome, bad.Copies, bad.Concern, bad.RF, verdict)
		}
	}
}

// TestTheLedgerRecordsTheConcernOfAWrite: Write notes the level its options asked for, so a
// concern-one write answered with the copy down is applied/1 and judged sound by I4.
// [BREAK: Ledger.write leaves Ack.Concern unset].
func TestTheLedgerRecordsTheConcernOfAWrite(t *testing.T) {
	c := ledgerSetup(t, 2, harness.Options{NoDetection: true, NoProbes: true})
	_, owner, copyNode, _ := proofID(t, c, "rc")
	ids := proofIDsOwnedBy(t, c, owner, copyNode, "rc", 2)
	l := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket, ProbeID: ids[0]})
	ctx := proofCtx(t)
	if _, err := l.Write(ctx, c, owner, "ada", "rc-warm"); err != nil {
		t.Fatal(err)
	}
	c.Kill(copyNode)
	if _, err := l.Write(ctx, c, owner, "ada", ids[0], wire.WithConcern(machine.ConcernOne)); err != nil {
		t.Fatal(err)
	}
	acks := l.Acks()
	last := acks[len(acks)-1]
	if last.Concern != machine.ConcernOne || last.Outcome != machine.OutcomeApplied || last.Copies != 1 || last.RF != 2 {
		t.Fatalf("the concern-one write was recorded as %+v, want concern one applied/1 at RF 2", last)
	}
	if verdict := judge(t, l, c, harness.InvAckVocabularySound); !verdict.OK {
		t.Fatalf("a concern-one ack with the copy down was judged %+v", verdict)
	}
}

// TestAConcernOneAckIsNotAPromiseTheClientsOwnReadCanBreak: an applied ack with one copy at RF 2
// may be discarded by a failover, so reading older of one's own id afterwards is not a
// regression; an applied ack that reached the majority still is one.
// [BREAK: monotoneState.ack counts every applied ack, whatever its copies].
func TestAConcernOneAckIsNotAPromiseTheClientsOwnReadCanBreak(t *testing.T) {
	c := ledgerSetup(t, 2, harness.Options{})
	thin := &recordingTB{TB: t}
	l := harness.NewLedger(thin, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	l.FeedAck(harness.Ack{Client: "ada", ID: "one", Seq: 5, Outcome: machine.OutcomeApplied, Copies: 1, RF: 2, Concern: machine.ConcernOne})
	l.FeedObs(harness.Obs{Client: "ada", ID: "one", Writer: "ada", Seq: 3, Via: "n1", ServedBy: "n1", ViaRunning: true})
	if verdict := judge(t, l, c, harness.InvReadsMonotone); len(thin.errors) != 0 || !verdict.OK {
		t.Fatalf("a read below a concern-one ack: online %v, verdict %+v, want no regression", thin.errors, verdict)
	}
	firm := &recordingTB{TB: t}
	l = harness.NewLedger(firm, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	l.FeedAck(harness.Ack{Client: "ada", ID: "two", Seq: 5, Outcome: machine.OutcomeApplied, Copies: 2, RF: 2})
	l.FeedObs(harness.Obs{Client: "ada", ID: "two", Writer: "ada", Seq: 3, Via: "n1", ServedBy: "n1", ViaRunning: true})
	if verdict := judge(t, l, c, harness.InvReadsMonotone); len(firm.errors) != 1 || verdict.OK {
		t.Fatalf("a read below a majority ack: online %v, verdict %+v, want a regression", firm.errors, verdict)
	}
}

// TestTheOfflineReplayJudgesAReadBeforeTheAckThatCameAfterIt: ack seq 3, read seq 3, ack seq 5,
// read seq 5 in that record order is clean. The read of seq 3 came before the ack of seq 5, so
// the offline replay must not apply the later ack to it.
// [BREAK: checkMonotone's sort comparator returns 0 for every pair].
func TestTheOfflineReplayJudgesAReadBeforeTheAckThatCameAfterIt(t *testing.T) {
	c := ledgerSetup(t, 2, harness.Options{})
	l := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	l.FeedAck(harness.Ack{Client: "ada", ID: "x", Seq: 3, Outcome: machine.OutcomeApplied, Copies: 2, RF: 2})
	l.FeedObs(harness.Obs{Client: "ada", ID: "x", Writer: "ada", Seq: 3, Via: "n1", ServedBy: "n1", ViaRunning: true})
	l.FeedAck(harness.Ack{Client: "ada", ID: "x", Seq: 5, Outcome: machine.OutcomeApplied, Copies: 2, RF: 2})
	l.FeedObs(harness.Obs{Client: "ada", ID: "x", Writer: "ada", Seq: 5, Via: "n1", ServedBy: "n1", ViaRunning: true})
	if verdict := judge(t, l, c, harness.InvReadsMonotone); !verdict.OK {
		t.Fatalf("a read recorded before a later ack was judged %+v", verdict)
	}
}

// TestADocumentNoAckNamesCountsAsLaterOnlyAboveItsWritersAcks: I1 accepts a document whose
// writer's seq is above every ack of the id (a write that landed unacknowledged after them) and
// rejects one below the highest ack (an older write beating an acknowledged one).
// [BREAK: reflectsOrFollows returns true for a document no ack names].
func TestADocumentNoAckNamesCountsAsLaterOnlyAboveItsWritersAcks(t *testing.T) {
	c := ledgerSetup(t, 2, harness.Options{})
	ctx := proofCtx(t)
	for _, doc := range []struct {
		id  string
		seq int64
	}{{"newer", 7}, {"older", 1}} {
		if _, err := c.Node("n1").Upsert(ctx, proofDB, ledgerBucket, bson.D{{Key: "_id_", Value: doc.id}, {Key: "name", Value: "ada"}, {Key: "seq", Value: doc.seq}}); err != nil {
			t.Fatalf("placing %s: %v", doc.id, err)
		}
	}
	newer := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	newer.FeedAck(harness.Ack{Client: "ada", ID: "newer", Seq: 2, Outcome: machine.OutcomeApplied, Copies: 2, RF: 2})
	newer.FeedAck(harness.Ack{Client: "ada", ID: "newer", Seq: 3, Outcome: machine.OutcomeApplied, Copies: 2, RF: 2})
	if verdict := judge(t, newer, c, harness.InvNoAckedWriteLost); !verdict.OK {
		t.Fatalf("a document above every ack of its writer was judged %+v", verdict)
	}
	older := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	older.FeedAck(harness.Ack{Client: "ada", ID: "older", Seq: 2, Outcome: machine.OutcomeApplied, Copies: 2, RF: 2})
	older.FeedAck(harness.Ack{Client: "ada", ID: "older", Seq: 3, Outcome: machine.OutcomeApplied, Copies: 2, RF: 2})
	if verdict := judge(t, older, c, harness.InvNoAckedWriteLost); verdict.OK || !strings.Contains(verdict.Detail, "older") {
		t.Fatalf("an unacknowledged document below its writer's acks was judged %+v, want a violation", verdict)
	}
}

// TestADocumentWithNoNameFieldIsReportedNotAPanic: the ledger writes name and seq, but a
// document a test placed by hand may lack name. A read records it with an empty Writer, and I1
// says the writer cannot be judged instead of panicking.
// [BREAK: readable reads the name with StringValue, which panics on an absent field].
func TestADocumentWithNoNameFieldIsReportedNotAPanic(t *testing.T) {
	c := ledgerSetup(t, 2, harness.Options{})
	ctx := proofCtx(t)
	if _, err := c.Node("n1").Upsert(ctx, proofDB, ledgerBucket, bson.D{{Key: "_id_", Value: "anon"}, {Key: "seq", Value: int64(4)}}); err != nil {
		t.Fatalf("placing the unnamed document: %v", err)
	}
	l := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	if seq, err := l.Read(ctx, c, "n1", "ada", "anon"); err != nil || seq != 4 {
		t.Fatalf("reading the unnamed document gave seq %d, err %v", seq, err)
	}
	if got := l.Obs(); len(got) != 1 || got[0].Writer != "" {
		t.Fatalf("the unnamed document was recorded as %+v, want an empty Writer", got)
	}
	l.FeedAck(harness.Ack{Client: "ada", ID: "anon", Seq: 4, Outcome: machine.OutcomeApplied, Copies: 2, RF: 2})
	if verdict := judge(t, l, c, harness.InvNoAckedWriteLost); verdict.OK || !strings.Contains(verdict.Detail, "no name field") {
		t.Fatalf("an unnamed document was judged %+v, want a violation saying it has no name field", verdict)
	}
}

// TestStaleEpochIsANamedRefusalButNotForever: STALE_EPOCH is the client face of the fence
// (entry-ring-conventions section 7), so a running node answering it while a membership change
// commits is judged clean; a run of it longer than the bound is still "refusing forever".
// [BREAK: namedRefusals lacks cluster.CodeStaleEpoch].
func TestStaleEpochIsANamedRefusalButNotForever(t *testing.T) {
	c := ledgerSetup(t, 2, harness.Options{})
	brief := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	for range 2 {
		brief.FeedObs(harness.Obs{Client: "ada", ID: "x", Seq: -1, Via: "n3", Code: cluster.CodeStaleEpoch, ViaRunning: true})
	}
	brief.FeedObs(harness.Obs{Client: "ada", ID: "x", Writer: "ada", Seq: 1, Via: "n3", ServedBy: "n3", ViaRunning: true})
	if verdict := judge(t, brief, c, harness.InvRefusalsAreNamed); !verdict.OK {
		t.Fatalf("a short run of STALE_EPOCH was judged %+v, want OK", verdict)
	}
	endless := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	for range 4 {
		endless.FeedObs(harness.Obs{Client: "ada", ID: "x", Seq: -1, Via: "n3", Code: cluster.CodeStaleEpoch, ViaRunning: true})
	}
	if verdict := verdictNamed(t, endless.Check(proofCtx(t), c, harness.CheckOptions{Settle: 50 * time.Millisecond, MaxRefused: 3}), harness.InvRefusalsAreNamed); verdict.OK {
		t.Fatalf("four STALE_EPOCH answers in a row with a bound of three were judged %+v", verdict)
	}
}

// TestADeclaredLostIdIsNotARegression: I2 honours the same declared loss set I1 does. An id the
// row declared lost (its latest ack was discarded by a failover) may read older or NOT_FOUND
// afterwards; the same reads of an id not declared are still a regression.
// [BREAK: checkMonotone ignores its lost set].
func TestADeclaredLostIdIsNotARegression(t *testing.T) {
	c := ledgerSetup(t, 2, harness.Options{})
	feed := func(l *harness.Ledger) {
		l.FeedAck(harness.Ack{Client: "ada", ID: "gone", Seq: 5, Outcome: machine.OutcomeApplied, Copies: 2, RF: 2})
		l.FeedObs(harness.Obs{Client: "ada", ID: "gone", Writer: "ada", Seq: 5, Via: "n1", ServedBy: "n1", ViaRunning: true})
		l.FeedObs(harness.Obs{Client: "ada", ID: "gone", Writer: "ada", Seq: 3, Via: "n2", ServedBy: "n2", ViaRunning: true})
		l.FeedObs(harness.Obs{Client: "ada", ID: "gone", Code: cluster.CodeNotFound, Seq: -1, Via: "n2", ViaRunning: true})
	}
	opts := harness.CheckOptions{Settle: 50 * time.Millisecond, Lost: map[string]string{"gone": "owner killed with one copy"}}
	declared := harness.NewLedger(&recordingTB{TB: t}, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	feed(declared)
	if verdict := verdictNamed(t, declared.Check(proofCtx(t), c, opts), harness.InvReadsMonotone); !verdict.OK {
		t.Fatalf("reads of a declared-lost id were judged %+v, want OK", verdict)
	}
	undeclared := harness.NewLedger(&recordingTB{TB: t}, harness.LedgerOptions{DB: proofDB, Bucket: ledgerBucket})
	feed(undeclared)
	opts.Lost = map[string]string{"other": "unrelated"}
	if verdict := verdictNamed(t, undeclared.Check(proofCtx(t), c, opts), harness.InvReadsMonotone); verdict.OK {
		t.Fatalf("the same reads of an undeclared id were judged %+v, want a regression", verdict)
	}
}
