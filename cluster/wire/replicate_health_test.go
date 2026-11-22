package wire

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/peer"
)

// waitedFor stands for the wc_timeout a reported wait ran.
const waitedFor = 5 * time.Second

// healthRig is a Replicator with a settable clock and copy set, and no link: the breaker's
// record is driven by the calls a writer and the lanes make, never by a send.
type healthRig struct {
	rep    *Replicator
	now    atomic.Int64
	mu     sync.Mutex
	copies []string
}

func newHealthRig(t *testing.T, copies ...string) *healthRig {
	t.Helper()
	rig := &healthRig{copies: copies}
	rig.now.Store(time.Unix(1_700_000_000, 0).UnixNano())
	rig.rep = mustReplicator(t, ReplicatorConfig{
		Self: "owner", Pairs: &cluster.PairSet{}, Advancer: t3Advancer{},
		Copies: func(uint16, uint64) []string {
			rig.mu.Lock()
			defer rig.mu.Unlock()
			return append([]string(nil), rig.copies...)
		},
		Now: func() time.Time { return time.Unix(0, rig.now.Load()) },
	})
	return rig
}

func (rig *healthRig) advance(d time.Duration) { rig.now.Add(int64(d)) }

func (rig *healthRig) setCopies(copies ...string) {
	rig.mu.Lock()
	defer rig.mu.Unlock()
	rig.copies = copies
}

func (rig *healthRig) suspect(peer string) bool {
	rig.rep.mu.Lock()
	defer rig.rep.mu.Unlock()
	return rig.rep.suspectLocked(peer, rig.rep.cfg.Now())
}

func (rig *healthRig) ack(peer string, slot uint16, seq uint64) {
	rig.rep.OnWriteAck(WriteAck{From: peer, Slot: slot, DB: 1, Pos: u32Pos(1, seq), Outcome: AckApplied})
}

// awaitReturnsAtOnce runs Await with a real budget and reports whether it returned before the budget ended.
func (rig *healthRig) awaitReturnsAtOnce(slot uint16, seq uint64, copies int) (count int, atOnce bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	count = rig.rep.Await(ctx, slot, 1, u32Pos(1, seq), copies)
	return count, time.Since(start) < 100*time.Millisecond
}

// TestAHealthyPeerIsNeverMarked: waits that are met, and a timeout report for a position every
// copy holds, leave no record.
// [BREAK: NoteWaitTimeout marks every current copy].
func TestAHealthyPeerIsNeverMarked(t *testing.T) {
	rig := newHealthRig(t, "copy")
	rig.ack("copy", 1, 3)
	if count, atOnce := rig.awaitReturnsAtOnce(1, 3, 1); count != 1 || !atOnce {
		t.Fatalf("a met wait returned %d, at once %v", count, atOnce)
	}
	rig.rep.NoteWaitTimeout(1, 1, u32Pos(1, 3), waitedFor)
	if rig.suspect("copy") {
		t.Fatal("a copy that holds the position was marked")
	}
}

// TestTwoPairsSharingTheDeadPeerPayOnce: one timeout on one pair makes the other pair's wait free.
// [BREAK: key the mark by pair].
func TestTwoPairsSharingTheDeadPeerPayOnce(t *testing.T) {
	rig := newHealthRig(t, "dead")
	rig.rep.NoteWaitTimeout(1, 1, u32Pos(1, 1), waitedFor)
	if _, atOnce := rig.awaitReturnsAtOnce(2, 1, 1); !atOnce {
		t.Fatal("the second pair still waited for a peer the first pair's timeout marked")
	}
}

// TestRF3AllStillWaitsForTheHealthyCopy: with one suspect and one healthy copy missing, the wait
// goes on for the healthy one and ends when it acks.
// [BREAK: Await returns when any missing copy is suspect].
func TestRF3AllStillWaitsForTheHealthyCopy(t *testing.T) {
	rig := newHealthRig(t, "dead")
	rig.rep.NoteWaitTimeout(1, 1, u32Pos(1, 1), waitedFor)
	rig.setCopies("dead", "live")
	if !rig.suspect("dead") || rig.suspect("live") {
		t.Fatalf("setup: dead suspect %v, live suspect %v", rig.suspect("dead"), rig.suspect("live"))
	}
	if _, atOnce := rig.awaitReturnsAtOnce(1, 2, 2); atOnce {
		t.Fatal("the wait skipped a healthy copy it should wait for")
	}
	done := make(chan int, 1)
	go func() { done <- rig.rep.Await(context.Background(), 1, 1, u32Pos(1, 2), 2) }()
	time.Sleep(20 * time.Millisecond)
	rig.ack("live", 1, 2)
	select {
	case count := <-done:
		if count != 1 {
			t.Fatalf("Await returned %d acks, want the one real ack", count)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Await did not end when the only waitable copy acked")
	}
}

// TestCommittedDownCopyEndsTheWaitAtOnce: a member the committed state calls down is no longer a
// copy, so nothing is waitable and the wait ends with no mark made (finding 1 of the note).
// [BREAK: Await ignores an empty waitable set].
func TestCommittedDownCopyEndsTheWaitAtOnce(t *testing.T) {
	rig := newHealthRig(t)
	if count, atOnce := rig.awaitReturnsAtOnce(1, 1, 1); count != 0 || !atOnce {
		t.Fatalf("a wait with no current copy returned %d, at once %v, want 0 at once", count, atOnce)
	}
}

// TestAWaitingWriterWakesWhenItsPeerTurnsSuspect: a writer already waiting is released by the mark
// another writer's timeout makes, without waiting for its own budget.
// [BREAK: markSuspectLocked does not wake suspectChanged].
func TestAWaitingWriterWakesWhenItsPeerTurnsSuspect(t *testing.T) {
	rig := newHealthRig(t, "dead")
	done := make(chan int, 1)
	go func() { done <- rig.rep.Await(context.Background(), 1, 1, u32Pos(1, 1), 1) }()
	time.Sleep(30 * time.Millisecond)
	rig.rep.NoteWaitTimeout(2, 1, u32Pos(1, 1), waitedFor) // another pair, same peer
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a waiting Await was not woken when its peer turned suspect")
	}
}

// TestSuspectHoldGrowsAndIsForgotten: each episode holds longer up to suspectMax, a hold ends by
// itself, and the record goes after suspectForget of quiet.
// [BREAK: until is not recomputed from the strikes].
func TestSuspectHoldGrowsAndIsForgotten(t *testing.T) {
	rig := newHealthRig(t, "copy")
	hold := func() time.Duration {
		rig.rep.mu.Lock()
		defer rig.rep.mu.Unlock()
		return rig.rep.health["copy"].until.Sub(rig.rep.cfg.Now())
	}
	rig.rep.NoteWaitTimeout(1, 1, u32Pos(1, 1), waitedFor)
	if hold() != suspectBase {
		t.Fatalf("first hold %v, want %v", hold(), suspectBase)
	}
	want := suspectBase
	for i := 0; i < 8; i++ {
		rig.advance(hold() + time.Millisecond) // the hold ends by itself
		if rig.suspect("copy") {
			t.Fatal("a hold did not end at until")
		}
		rig.rep.NoteWaitTimeout(1, 1, u32Pos(1, 1), waitedFor)
		want = min(want*2, suspectMax)
		if hold() != want {
			t.Fatalf("hold of episode %d is %v, want %v", i+2, hold(), want)
		}
	}
	rig.advance(suspectMax + time.Second)
	rig.rep.Retry()
	rig.advance(suspectForget)
	rig.rep.Retry()
	rig.rep.mu.Lock()
	left := len(rig.rep.health)
	rig.rep.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d peer records remain after suspectForget of quiet", left)
	}
}

// TestSuspectStateIsRaceFreeUnderManyWriters: sixteen writers waiting, marking and clearing at
// once: no data race and no stuck Await.
// [BREAK: suspectChanged is replaced outside the replicator's lock].
func TestSuspectStateIsRaceFreeUnderManyWriters(t *testing.T) {
	rig := newHealthRig(t, "a", "b")
	var wg sync.WaitGroup
	for writer := 0; writer < 16; writer++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				slot, seq := uint16(writer%4), uint64(i+1)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
				rig.rep.Await(ctx, slot, 1, u32Pos(1, seq), 2)
				cancel()
				switch i % 3 {
				case 0:
					rig.rep.NoteWaitTimeout(slot, 1, u32Pos(1, seq), waitedFor)
				case 1:
					rig.advance(suspectBase)
					rig.ack("a", slot, seq)
				default:
					rig.rep.Retry()
				}
			}
		}()
	}
	wg.Wait()
}

// holdLongerThanBase leaves copy suspect for four seconds, the second strike's hold, with the
// episode begun now.
func (rig *healthRig) holdLongerThanBase() {
	rig.rep.NoteWaitTimeout(1, 1, u32Pos(1, 1), waitedFor)
	rig.advance(suspectBase + time.Millisecond)
	rig.rep.NoteWaitTimeout(1, 1, u32Pos(1, 1), waitedFor)
}

// TestSuspectPeerIsClearedByItsNextAck: a peer marked by a timed-out wait stops being skipped
// once it acks, at least suspectBase after the mark, and the next wait on it is a normal one.
// [BREAK: OnWriteAck does not clear the mark].
func TestSuspectPeerIsClearedByItsNextAck(t *testing.T) {
	rig := newHealthRig(t, "copy")
	rig.holdLongerThanBase()
	if !rig.suspect("copy") {
		t.Fatal("a timed-out wait did not mark the copy")
	}
	if _, atOnce := rig.awaitReturnsAtOnce(1, 2, 1); !atOnce {
		t.Fatal("a wait for a suspect copy blocked")
	}
	rig.advance(suspectBase)
	rig.ack("copy", 2, 5) // any pair: the peer is replicating again
	if rig.suspect("copy") {
		t.Fatal("an applied ack from the copy, suspectBase after the mark, left it suspect")
	}
	if _, atOnce := rig.awaitReturnsAtOnce(1, 9, 1); atOnce {
		t.Fatal("the wait after the clear returned at once: it must wait for the copy as before")
	}
}

// TestAnAckSoonAfterTheMarkDoesNotClearIt: the ack of the write that timed out can trail the mark
// by a moment; it is not the peer replicating again.
// [BREAK: the suspectBase condition is dropped from the ack clear].
func TestAnAckSoonAfterTheMarkDoesNotClearIt(t *testing.T) {
	rig := newHealthRig(t, "copy")
	rig.holdLongerThanBase()
	rig.advance(suspectBase / 2)
	rig.ack("copy", 1, 1)
	if !rig.suspect("copy") {
		t.Fatal("an ack half of suspectBase after the mark cleared it")
	}
}

// TestANotAppliedAckDoesNotClearTheMark: a copy that answers not applied (seeding, or behind) is
// answering but is not holding the write, so it stays skipped.
// [BREAK: any ack clears the mark].
func TestANotAppliedAckDoesNotClearTheMark(t *testing.T) {
	rig := newHealthRig(t, "copy")
	rig.holdLongerThanBase()
	rig.advance(suspectBase)
	rig.rep.OnWriteAck(WriteAck{From: "copy", Slot: 1, DB: 1, Outcome: AckNotApplied})
	if !rig.suspect("copy") {
		t.Fatal("a not-applied ack cleared the mark")
	}
}

// TestResyncClearsTheMark: the supervisor's probe seeing the peer return is observed health.
// [BREAK: Resync leaves the mark].
func TestResyncClearsTheMark(t *testing.T) {
	rig := newHealthRig(t, "copy")
	rig.holdLongerThanBase()
	rig.rep.Resync("copy")
	if rig.suspect("copy") {
		t.Fatal("Resync left the peer suspect")
	}
}

// TestSuspectDoesNotOutliveTheCommittedReturn: a member that goes from down to active in the
// committed state is cleared at once, before its hold would end.
// [BREAK: the wentActive hook is skipped].
func TestSuspectDoesNotOutliveTheCommittedReturn(t *testing.T) {
	rig := newHealthRig(t, "copy")
	rig.holdLongerThanBase()
	state := cluster.ClusterState{Members: []cluster.Member{{ID: "copy", State: cluster.MemberActive}, {ID: "other", State: cluster.MemberActive}}}
	clearReturnedSuspects(rig.rep, map[string]bool{"copy": true}, state)
	if rig.suspect("copy") {
		t.Fatal("a member the committed state moved from down to active is still suspect")
	}
	rig.holdLongerThanBase()
	clearReturnedSuspects(rig.rep, map[string]bool{}, state) // not down before: no return
	if !rig.suspect("copy") {
		t.Fatal("a member that was not down was cleared as a return")
	}
}

// sendFailureOwner is an owner with one copy and a link that fails every Write while failing is set.
func sendFailureOwner(t *testing.T) (*u32Node, func(error)) {
	t.Helper()
	owner := newU32Node(t, "owner", nil)
	owner.copies = []string{"copy"}
	owner.ownRun(u32Epoch, 1, 3)
	owner.rep.Push(u32Slot, u32DB, u32Pos(u32Epoch, 3))
	return owner, func(err error) {
		owner.link.mu.Lock()
		defer owner.link.mu.Unlock()
		owner.link.writeErr = err
	}
}

func (n *u32Node) suspects(peerID string) bool {
	n.rep.mu.Lock()
	defer n.rep.mu.Unlock()
	return n.rep.suspectLocked(peerID, n.rep.cfg.Now())
}

// flushFailing runs one flush that must fail and report an error.
func flushFailing(t *testing.T, owner *u32Node) {
	t.Helper()
	if err := owner.flush(context.Background()); err == nil {
		t.Fatal("a failed send was not reported")
	}
}

// TestThreeFailedSendsMarkThePeer: one reset connection must not mark a peer; a run of
// suspectSendFailures real attempts that failed does. Transport backoff refusals between the
// attempts are not attempts: they neither count nor end the run.
// [BREAK: the run threshold is 1, or a failed send is not counted, or a refusal ends the run].
func TestThreeFailedSendsMarkThePeer(t *testing.T) {
	owner, failWith := sendFailureOwner(t)
	refused := fmt.Errorf("calling copy on the data channel: %w for another 1s after 2 failures", peer.ErrBackoff)
	for failures := 1; failures <= suspectSendFailures; failures++ {
		failWith(errors.New("dial copy: connection refused"))
		flushFailing(t, owner)
		if got := owner.suspects("copy"); got != (failures == suspectSendFailures) {
			t.Fatalf("after %d failed attempts the peer suspect is %v", failures, got)
		}
		failWith(refused)
		for range 5 {
			flushFailing(t, owner)
		}
	}
}

// TestBackoffRefusalsAfterOneFailedDialDoNotMark: the transport refuses every call for 50 ms after
// one failed dial, so a burst of refusals reaches any threshold in milliseconds. It is one failure
// of a peer that is probably alive (BUG-0745's lost batch is exactly this), never a mark.
// [BREAK: noteSendFailure counts peer.ErrBackoff].
func TestBackoffRefusalsAfterOneFailedDialDoNotMark(t *testing.T) {
	owner, failWith := sendFailureOwner(t)
	failWith(errors.New("dial copy: connection refused"))
	flushFailing(t, owner)
	failWith(fmt.Errorf("calling copy on the data channel: %w for another 50ms after 1 failures", peer.ErrBackoff))
	for range 20 {
		flushFailing(t, owner)
	}
	if owner.suspects("copy") {
		t.Fatal("one failed dial followed by a burst of backoff refusals marked the peer")
	}
}

// TestASuccessfulSendEndsTheRunOfFailures: failures must be consecutive, so a send that goes
// through resets the count.
// [BREAK: a successful send is counted as a failure].
func TestASuccessfulSendEndsTheRunOfFailures(t *testing.T) {
	owner, failWith := sendFailureOwner(t)
	failWith(errors.New("connection reset"))
	for range suspectSendFailures - 1 {
		_ = owner.flush(context.Background())
	}
	failWith(nil)
	if err := owner.flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	owner.ownRun(u32Epoch, 4, 4)
	owner.rep.Push(u32Slot, u32DB, u32Pos(u32Epoch, 4))
	failWith(errors.New("connection reset"))
	for range suspectSendFailures - 1 {
		_ = owner.flush(context.Background())
	}
	if owner.suspects("copy") {
		t.Fatal("failures separated by a successful send marked the peer")
	}
}

// TestFailedBatchedSendsMarkThePeerToo: the batching lane reports its failures the same way.
// [BREAK: sendShipments does not count a failed WriteBatch].
func TestFailedBatchedSendsMarkThePeerToo(t *testing.T) {
	owner, link := batchOwner(t, batchSlots(2), 2, nil)
	for failures := 1; failures <= suspectSendFailures; failures++ {
		link.batchMu.Lock()
		link.failCall = len(link.calls) + 1
		link.batchMu.Unlock()
		if !owner.shipPass("copy") {
			t.Fatal("the failed pass left nothing queued to ship again")
		}
		if got := owner.suspects("copy"); got != (failures >= suspectSendFailures) {
			t.Fatalf("after %d failed batched sends the peer suspect is %v", failures, got)
		}
	}
}

// TestStrikesAreForgottenOnTheSendFailurePathToo: a peer quiet for suspectForget starts again at
// the first hold, whichever trigger marks it next. Failed sends below the threshold must not
// refresh the clock the forgetting reads.
// [BREAK: noteSendFailure sets lastEvent before the forget check].
func TestStrikesAreForgottenOnTheSendFailurePathToo(t *testing.T) {
	rig := newHealthRig(t, "copy")
	for range 6 {
		rig.rep.NoteWaitTimeout(1, 1, u32Pos(1, 1), waitedFor)
		rig.advance(suspectMax + time.Second)
	}
	rig.advance(suspectForget + time.Second) // no Retry in between: no sweep
	for range suspectSendFailures {
		rig.rep.noteSendFailure(context.Background(), "copy", errors.New("dial copy: connection refused"))
	}
	rig.rep.mu.Lock()
	hold := rig.rep.health["copy"].until.Sub(rig.rep.cfg.Now())
	rig.rep.mu.Unlock()
	if hold != suspectBase {
		t.Fatalf("the hold after %v of quiet is %v, want the first hold %v", suspectForget, hold, suspectBase)
	}
}

// TestSporadicSendFailuresDoNotKeepStrikesAlive: single failed sends far apart are not marks, so
// they must not keep a peer's old strikes from being forgotten.
// [BREAK: any failed send refreshes the clock strikes are forgotten by].
func TestSporadicSendFailuresDoNotKeepStrikesAlive(t *testing.T) {
	rig := newHealthRig(t, "copy")
	for range 6 {
		rig.rep.NoteWaitTimeout(1, 1, u32Pos(1, 1), waitedFor)
		rig.advance(suspectMax + time.Second)
	}
	rig.advance(suspectForget / 2)
	rig.rep.noteSendFailure(context.Background(), "copy", errors.New("connection reset"))
	rig.advance(suspectForget/2 + time.Second)
	rig.rep.NoteWaitTimeout(1, 1, u32Pos(1, 1), waitedFor)
	rig.rep.mu.Lock()
	hold := rig.rep.health["copy"].until.Sub(rig.rep.cfg.Now())
	rig.rep.mu.Unlock()
	if hold != suspectBase {
		t.Fatalf("a mark after a long quiet and one stray failed send holds %v, want %v", hold, suspectBase)
	}
}

// TestAPeerThatAckedOnAnyPairDuringTheWaitIsNotMarked: a copy that refuses one pair (seeding, a
// deferred fence, a schema it cannot resolve yet) is answering on its others. The wait that timed
// out on the stuck pair must not make healthy pairs skip it.
// [BREAK: NoteWaitTimeout marks a copy whatever it acked during the wait].
func TestAPeerThatAckedOnAnyPairDuringTheWaitIsNotMarked(t *testing.T) {
	rig := newHealthRig(t, "copy")
	rig.advance(waitedFor / 2)
	rig.ack("copy", 2, 7) // pair B's copy acks...
	rig.advance(waitedFor / 2)
	rig.rep.NoteWaitTimeout(1, 1, u32Pos(1, 1), waitedFor) // ...pair A, which it refuses, timed out
	if rig.suspect("copy") {
		t.Fatal("a peer that gave an applied ack during the wait was marked")
	}
}

// TestAPeerSilentOnEveryPairIsMarked: the mirror: an ack from before the wait began says nothing
// about now, and with no ack at all the peer is marked.
// [BREAK: any earlier ack protects the peer for ever].
func TestAPeerSilentOnEveryPairIsMarked(t *testing.T) {
	rig := newHealthRig(t, "copy", "silent")
	rig.ack("copy", 2, 7)
	rig.advance(waitedFor + time.Second) // the ack is older than the wait that timed out
	rig.rep.NoteWaitTimeout(1, 1, u32Pos(1, 1), waitedFor)
	if !rig.suspect("copy") || !rig.suspect("silent") {
		t.Fatalf("copy suspect %v, silent suspect %v, want both marked", rig.suspect("copy"), rig.suspect("silent"))
	}
}

// logCapture is a slog handler that keeps every record, installed as the default for one test.
type logCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (c *logCapture) Enabled(context.Context, slog.Level) bool { return true }
func (c *logCapture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, r)
	return nil
}
func (c *logCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *logCapture) WithGroup(string) slog.Handler      { return c }

func captureLogs(t *testing.T) *logCapture {
	t.Helper()
	capture := &logCapture{}
	previous := slog.Default()
	slog.SetDefault(slog.New(capture))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return capture
}

// lines returns "level message key=value ..." for each record kept.
func (c *logCapture) lines() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, r := range c.records {
		line := r.Level.String() + " " + r.Message
		r.Attrs(func(a slog.Attr) bool {
			line += fmt.Sprintf(" %s=%v", a.Key, a.Value)
			return true
		})
		out = append(out, line)
	}
	return out
}

func requireOneLine(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != 1 {
		t.Fatalf("got %d log lines %q, want exactly one", len(got), got)
	}
	for _, part := range want {
		if !strings.Contains(got[0], part) {
			t.Fatalf("log line %q lacks %q", got[0], part)
		}
	}
}

// TestSuspectTransitionsAreLoggedOnceAtInfo: a mark, an extension to a longer hold and a clear each
// log one INFO line naming the peer, the reason and the hold; a repeated report on a peer that is
// already suspect, and a failed send below the threshold, log nothing, so there is no per-write line.
// [BREAK: markSuspectLocked and clearSuspectLocked log nothing, or log on every call].
func TestSuspectTransitionsAreLoggedOnceAtInfo(t *testing.T) {
	logs := captureLogs(t)
	rig := newHealthRig(t, "copy")
	rig.rep.noteSendFailure(context.Background(), "copy", errors.New("dial copy: connection refused"))
	if got := logs.lines(); len(got) != 0 {
		t.Fatalf("a failed send below the threshold logged %q", got)
	}
	rig.rep.NoteWaitTimeout(1, 1, u32Pos(1, 1), waitedFor)
	requireOneLine(t, logs.lines(), "INFO", "peer=copy", "reason=a concern wait ran the whole wc_timeout", "hold=2s")
	rig.rep.NoteWaitTimeout(2, 1, u32Pos(1, 1), waitedFor) // already suspect: no new line
	for range 5 {
		rig.rep.noteSendFailure(context.Background(), "copy", errors.New("dial copy: connection refused"))
	}
	if got := logs.lines(); len(got) != 1 { // the failed sends are evidence on a suspect peer, but no strike rose yet
		t.Fatalf("repeated evidence inside one hold logged %q, want only the mark and nothing more", got)
	}
	rig.advance(suspectBase + time.Millisecond)
	rig.rep.noteSendFailure(context.Background(), "copy", errors.New("dial copy: connection refused"))
	lines := logs.lines()
	if last := lines[len(lines)-1]; !strings.Contains(last, "peer=copy") || !strings.Contains(last, "hold=4s") {
		t.Fatalf("the line after the hold ended is %q, want the peer and hold=4s", last)
	}
	before := len(logs.lines())
	rig.advance(suspectBase)
	rig.ack("copy", 3, 3)
	after := logs.lines()
	if len(after) != before+1 || !strings.Contains(after[before], "peer=copy") || !strings.Contains(after[before], "reason=an applied ack") {
		t.Fatalf("the clear logged %q, want one line naming the peer and an applied ack", after[before:])
	}
}

// TestAPeerThatKeepsFailingLogsEachLongerHoldOnce: sends that keep failing inside a hold extend it
// without a line, and log one line each time the hold grows.
// [BREAK: a strike that extends a suspect hold is not logged, or every failure is].
func TestAPeerThatKeepsFailingLogsEachLongerHoldOnce(t *testing.T) {
	logs := captureLogs(t)
	rig := newHealthRig(t, "copy")
	rig.rep.NoteWaitTimeout(1, 1, u32Pos(1, 1), waitedFor)
	for range 6 { // a failed send every second keeps the hold alive: strikes at 2 s, 4 s and 6 s
		rig.advance(time.Second)
		rig.rep.noteSendFailure(context.Background(), "copy", errors.New("dial copy: connection refused"))
		rig.rep.noteSendFailure(context.Background(), "copy", errors.New("dial copy: connection refused"))
		rig.rep.noteSendFailure(context.Background(), "copy", errors.New("dial copy: connection refused"))
	}
	got := logs.lines()
	if len(got) != 4 || !strings.Contains(got[1], "still suspect") || !strings.Contains(got[1], "hold=4s") || !strings.Contains(got[2], "hold=8s") || !strings.Contains(got[3], "hold=16s") {
		t.Fatalf("log lines %q, want the mark then one line each for the holds of 4 s, 8 s and 16 s", got)
	}
}

// onStateRig is a supervisor over a node that has only a replicator: enough for onState, which
// reads the committed state and tells the replicator what it means.
func onStateRig(t *testing.T, rig *healthRig) *supervisor {
	t.Helper()
	return newSupervisor(&Node{cfg: NodeConfig{NodeID: "owner"}, cl: &clusterParts{repl: rig.rep}})
}

func stateWith(copyState cluster.MemberState) cluster.ClusterState {
	return cluster.ClusterState{Members: []cluster.Member{
		{ID: "owner", State: cluster.MemberActive}, {ID: "copy", State: copyState},
	}}
}

// TestOnStateClearsASuspectThatTheCommittedStateReturned goes through the supervisor, not the helper: a
// member committed down and then active again is cleared when onState sees the second state.
// [BREAK: supervisor.onState does not call clearReturnedSuspects].
func TestOnStateClearsASuspectThatTheCommittedStateReturned(t *testing.T) {
	rig := newHealthRig(t, "copy")
	s := onStateRig(t, rig)
	s.onState(stateWith(cluster.MemberDown))
	rig.holdLongerThanBase()
	s.onState(stateWith(cluster.MemberDown))
	if !rig.suspect("copy") {
		t.Fatal("a state that keeps the member down cleared the mark")
	}
	s.onState(stateWith(cluster.MemberActive))
	if rig.suspect("copy") {
		t.Fatal("onState left a member suspect after the committed state moved it from down to active")
	}
}

// TestAWriteAlreadyWaitingWhenACopyIsCommittedDownIsWoken: the committed state removes the copy
// from the pair while a write waits for it; the write ends at once, not at wc_timeout.
// [BREAK: supervisor.onState does not call copySetChanged].
func TestAWriteAlreadyWaitingWhenACopyIsCommittedDownIsWoken(t *testing.T) {
	rig := newHealthRig(t, "copy")
	s := onStateRig(t, rig)
	done := make(chan int, 1)
	go func() { done <- rig.rep.Await(context.Background(), 1, 1, u32Pos(1, 1), 1) }()
	time.Sleep(30 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("Await returned before the copy set changed")
	default:
	}
	rig.setCopies() // topology.Refresh has run: the member is down, so it is no longer a copy
	s.onState(stateWith(cluster.MemberDown))
	select {
	case count := <-done:
		if count != 0 {
			t.Fatalf("Await returned %d acks, want 0", count)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a waiting write was not woken when the committed state removed its copy")
	}
}
