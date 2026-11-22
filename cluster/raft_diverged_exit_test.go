package cluster

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
)

// closedWithin reports whether ch is closed within d.
func closedWithin(ch <-chan struct{}, d time.Duration) bool {
	select {
	case <-ch:
		return true
	case <-time.After(d):
		return false
	}
}

// TestDivergedClosesOnTheFirstDivergenceOnly (F2, mixed-build spec 6.1 item 4): Raft.Diverged is
// open until a divergence is recorded, closed once the first one is, and a second refused entry
// neither panics (a double close) nor reopens it. Break: never close it (the first wait times
// out), or close it in noteDivergence outside the compare-and-swap (the second entry panics).
func TestDivergedClosesOnTheFirstDivergenceOnly(t *testing.T) {
	r := openOrFail(t, quickRaft(t, t.TempDir(), "n1"))
	createDatabase(t, r, "app", 1)
	if closedWithin(r.Diverged(), 20*time.Millisecond) {
		t.Fatal("Diverged is closed on a healthy node")
	}
	for i := range 2 {
		bad := divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(r.State().StateSeq), Body: unreadFieldBody("x", int64(2+i))})
		if err := r.AppendWithoutDryRun(bad); err != nil {
			t.Fatal(err)
		}
		if !closedWithin(r.Diverged(), 2*time.Second) {
			t.Fatalf("after refused entry %d Diverged is still open", i+1)
		}
	}
}

// TestDivergedIsClosedAtOnceForADivergenceRecordedDuringReplay: a log that holds a refused entry
// closes the channel while OpenRaft is still replaying, and a caller that asks after OpenRaft
// returned finds it closed (review m6: the server checks once right after Start). Break: create
// the channel after NewRaft, so a Restore at start closes a channel nobody holds.
func TestDivergedIsClosedAtOnceForADivergenceRecordedDuringReplay(t *testing.T) {
	dir := t.TempDir()
	first := openOrFail(t, quickRaft(t, dir, "n1"))
	createDatabase(t, first, "app", 1)
	if err := first.AppendWithoutDryRun(divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(first.State().StateSeq), Body: unreadFieldBody("x", 2)})); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := openOrFail(t, quickRaft(t, dir, "n1"))
	if !closedWithin(second.Diverged(), 5*time.Second) {
		t.Fatal("a node that replayed a refused entry never closed Diverged")
	}
}

// fatalLineWants are the phrases of text 1 that an operator acts on.
var fatalLineWants = []string{
	"this node's cluster state and the node that proposed committed entry",
	"disagree: ",
	"The proposer's build is the one that wrote the entry and the only one that can apply the log",
	"or its raft log is damaged",
	"Keep its data directory untouched",
	"'replace-node <node> --with <node>@<addr>'",
	"Exit code 70.",
}

// TestTheFatalLineIsTextOneAndNamesTheLeaderOnlyWhenKnown (F2, text 1 and check 24; also test 10's
// start-fresh sentence): the line the node logs at the first divergence is text 1 with the entry,
// the term and the reason; the leader of that term is named when the node knows it (a real Raft
// whose leader is this node) and the clause is absent when it does not (an FSM with no raft
// behind it). Break: drop the replace-node sentence, or name an empty leader (", )").
func TestTheFatalLineIsTextOneAndNamesTheLeaderOnlyWhenKnown(t *testing.T) {
	var sink lockedBuffer
	cfg := quickRaft(t, t.TempDir(), "n1")
	cfg.Logger = hclog.New(&hclog.LoggerOptions{Name: "raft", Level: hclog.Warn, Output: &sink})
	r := openOrFail(t, cfg)
	createDatabase(t, r, "app", 1)
	if err := r.AppendWithoutDryRun(divergedRaw(t, Command{Version: CommandVersion, Kind: CommandCreateDatabase, BaseSeq: uint64(r.State().StateSeq), Body: unreadFieldBody("x", 2)})); err != nil {
		t.Fatal(err)
	}
	record := waitDiverged(t, r)
	line := sink.String()
	for _, want := range fatalLineWants {
		if !strings.Contains(line, want) {
			t.Errorf("the fatal line does not say %q:\n%s", want, line)
		}
	}
	if want := "(term " + strconv.FormatUint(record.Term, 10) + ", the leader of that term, n1)"; !strings.Contains(line, want) {
		t.Errorf("the fatal line does not name the leader this node knows, want %q:\n%s", want, line)
	}
	requireNoOtherBuild(t, line)
	if strings.Contains(line, "start it fresh") {
		t.Errorf("the fatal line sends a cluster member to an empty directory:\n%s", line)
	}
	if strings.Contains(line, "remove it") {
		t.Errorf("the fatal line tells the operator to remove the field:\n%s", line)
	}

	var bare lockedBuffer
	f := divergedFSM(t)
	f.logger = hclog.New(&hclog.LoggerOptions{Name: "raft", Level: hclog.Warn, Output: &bare})
	f.Apply(divergedEntry(7, []byte{255, 1}))
	text := bare.String()
	if !strings.Contains(text, "committed entry 7 (term ") || !strings.Contains(text, "the leader of that term) disagree") {
		t.Errorf("a node that knows no leader must name none:\n%s", text)
	}
}

// TestTextFourSaysTheNodeIsStopping (F2): with the exit in place the refusal of a proposal says
// what is true of the node, as the spec's text 4: it is stopping, the reason is in the earlier
// error, and the command goes to another member. Break: restore G1's "restart this node".
func TestTextFourSaysTheNodeIsStopping(t *testing.T) {
	text := divergedProposalRefusal().Error()
	for _, want := range []string{"is stopping", "see the earlier error", "Send the command to another member"} {
		if !strings.Contains(text, want) {
			t.Errorf("text 4 does not say %q: %s", want, text)
		}
	}
}

// TestTheDivergenceTextsAreWholeAndNameNoOtherRelease: [GUARD text 1 and the refusal of a later
// entry send a diverged node to the build the proposing member runs, a build that is running now,
// and never to an earlier one or to an empty directory; red when either text names a build that
// was never released or tells a member to start fresh]. Both are compared whole.
func TestTheDivergenceTextsAreWholeAndNameNoOtherRelease(t *testing.T) {
	fatal := FatalText(Divergence{Index: 7, Term: 3, Reason: "the cause"}, "n1")
	wantFatal := "this node's cluster state and the node that proposed committed entry 7 (term 3, the leader of that term, n1) disagree: the cause. " +
		"Stopping so this node cannot serve a stale view or drop what the cluster accepted. " +
		"The proposer's build is the one that wrote the entry and the only one that can apply the log: start the nodes that stop with this message on the build the proposer runs ('nodes' lists the members). " +
		"Starting a node on a different build can be blocked by local formats it has already written, in which case keep its data directory untouched. " +
		"If the node stops again with this message after that, its stored state is stale or its raft log is damaged (the log store has no page checksums, and a hard kill cannot truncate its tail, so damage means disk trouble). " +
		"Keep its data directory untouched (do not delete it: with replication factor 1 it holds the only copy) and replace the node: move its ring directory aside and keep it, " +
		"start it again with the same node id, keys and address, wait until 'nodes' shows it down, then run 'replace-node <node> --with <node>@<addr>' on any member. " +
		"Exit code 70."
	if fatal != wantFatal {
		t.Errorf("text 1 is not the whole text:\n got: %s\nwant: %s", fatal, wantFatal)
	}
	later := divergedApplyRefusal().Error()
	if want := "this node's cluster state disagrees with the cluster's (see the earlier error), so it applies no more entries. Start it on the build the proposing member runs and keep its data directory"; !strings.Contains(later, want) {
		t.Errorf("the refusal of a later entry does not say %q: %s", want, later)
	}
	for _, text := range []string{fatal, later} {
		requireNoOtherBuild(t, text)
		for _, banned := range []string{"older build", "start it fresh", "development directory"} {
			if strings.Contains(text, banned) {
				t.Errorf("the text says %q: %s", banned, text)
			}
		}
	}
}
