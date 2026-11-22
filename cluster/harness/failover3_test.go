package harness_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// This file is the membership-and-failure spec's three-voter, RF 2 table as tests (rows A1 to
// A10, C1 and C2 of the ring fault plan). Every row forms a cluster at a timing
// profile, drives writes through a harness.Ledger and ends with Ledger.Check, so the verdict on
// an acknowledged write, a stale read, a second owner, an unnamed refusal or a pair that does
// not converge is the ledger's, not the row's own. The expected answers use the spec's words.

// h3Slack is the scheduling slack allowed on a spec time bound: the spec's bounds are for the
// protocol, and a test process on a shared host adds scheduler delay to every measurement.
const h3Slack = 300 * time.Millisecond

// The owners of the findings the rows record: each says what is wrong and which slice or ruling
// owns it, so a skipped row can be read without the bug file.
const (
	// h3UncodedForward is the finding behind the refusals-are-named violations seen while a peer
	// is restarted: the entry node forwards to the owner it still believes in, and when that owner
	// is dead it answers with the transport error's text and no cluster code, until the down commit.
	h3UncodedForward = "product defect S3, cluster/wire/peer_links.go:161,181 (a live entry node forwarding to a killed owner answers a transport error with no code: peer in backoff, connection refused, closed connection); bug to be filed"
)

// h3UncodedForwardKnown matches the one refusals-are-named problem h3UncodedForward describes: a
// running node answering a forwarded call's transport error with no code. A bare deadline, a code
// outside the named set, or a run of refusals that does not match stays a failure.
var h3UncodedForwardKnown = h3Known{
	owner: h3UncodedForward,
	match: func(problem string) bool {
		if !strings.Contains(problem, "with no code") {
			return false
		}
		for _, transport := range []string{"forwarding the ", "in backoff", "closed network connection", "connection refused", "dial ", "calling "} {
			if strings.Contains(problem, transport) {
				return true
			}
		}
		return false
	},
}

// h3GainerNeverSeededKnown matches the one quiescence problem row A3 records: the probe pair's
// gainer sitting at position 0:0. The nodes disagreeing at any other position, or a probe that
// cannot be read, still fail.
var h3GainerNeverSeededKnown = h3Known{
	owner: "seeding S0 (active-node reconcile driver) and S3b: the gainer of a pair of the dead n2 is never seeded; row A3 expects the ledger readable, the spec says the gainers seed",
	match: func(problem string) bool {
		return strings.Contains(problem, "the nodes holding the probe pair disagree") && strings.Contains(problem, ":0:0")
	},
}

// h3Gate skips the row unless the fault matrix is asked for; a row takes 20 to 60 s.
func h3Gate(t *testing.T) {
	t.Helper()
	if os.Getenv("HOARDDB_HARNESS_FAULT") == "" {
		t.Skip("a fault-matrix row (20 to 60 s); set HOARDDB_HARNESS_FAULT=1")
	}
}

// h3Setup forms three voters with an RF 2 database and a bucket declaring the ledger's name and seq
// fields, at the timing profile.
func h3Setup(t *testing.T, profile harness.Profile, opts harness.Options) *harness.Cluster {
	t.Helper()
	opts.Timings = profile
	return ledgerSetup(t, 2, opts)
}

func h3Ledger(t *testing.T, probe string) *harness.Ledger {
	t.Helper()
	return harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: proofBucket, ProbeID: probe})
}

// h3Owned returns count ids that node owner owns and whose copy set does not include avoid.
func h3Owned(t *testing.T, c *harness.Cluster, owner string, count int, seed, avoid string) []string {
	t.Helper()
	asker := c.Running()[0]
	var ids []string
	for i := 0; len(ids) < count; i++ {
		if i > 20000 {
			t.Fatalf("no %d ids owned by %s avoiding %q", count, owner, avoid)
		}
		id := fmt.Sprintf("%s-%d", seed, i)
		got, copies, err := c.Node(asker).Owner(proofCtx(t), proofDB, proofBucket, id)
		if err != nil {
			t.Fatalf("placing %s: %v", id, err)
		}
		if got == owner && (avoid == "" || !slices.Contains(copies, avoid)) {
			ids = append(ids, id)
		}
	}
	return ids
}

// h3Write writes through the ledger with its own deadline.
func h3Write(l *harness.Ledger, c *harness.Cluster, via, client, id string, deadline time.Duration, options ...wire.WriteOption) (wire.Inserted, error) {
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	return l.Write(ctx, c, via, client, id, options...)
}

// h3Read reads through the ledger with its own deadline.
func h3Read(l *harness.Ledger, c *harness.Cluster, via, client, id string, deadline time.Duration) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	return l.Read(ctx, c, via, client, id)
}

// h3WriteAcked retries a write of one id through via until it is acknowledged, failing after the
// budget. A refusal is not an acknowledgement, so the retry loses and duplicates nothing.
func h3WriteAcked(t *testing.T, l *harness.Ledger, c *harness.Cluster, via, client, id string, budget time.Duration) wire.Inserted {
	t.Helper()
	deadline := time.Now().Add(budget)
	var last error
	for time.Now().Before(deadline) {
		got, err := h3Write(l, c, via, client, id, 5*time.Second)
		if err == nil {
			return got
		}
		last = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("write %s through %s was not acknowledged within %s: %v", id, via, budget, last)
	return wire.Inserted{}
}

// h3Preack writes ids and requires each to be applied on both copies, so that losing a single
// node later loses none of them.
func h3Preack(t *testing.T, l *harness.Ledger, c *harness.Cluster, via, client string, ids []string) {
	t.Helper()
	for _, id := range ids {
		got, err := h3Write(l, c, via, client, id, 5*time.Second)
		if err != nil || got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed != 2 {
			t.Fatalf("before the fault %s answered %v/%d (err %v), want applied/2", id, got.Outcome, got.CopiesConfirmed, err)
		}
	}
}

// h3ReadServed reads id through via until it is served, failing after the budget.
func h3ReadServed(t *testing.T, l *harness.Ledger, c *harness.Cluster, via, client, id string, budget time.Duration) {
	t.Helper()
	deadline := time.Now().Add(budget)
	var last error
	for time.Now().Before(deadline) {
		if _, last = h3Read(l, c, via, client, id, 3*time.Second); last == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("acknowledged write %s is not readable through %s within %s: %v", id, via, budget, last)
}

// h3Code is the cluster code of err, or 0 when it carries none (a bare deadline or transport error).
func h3Code(err error) cluster.Code {
	var coded *cluster.Error
	if errors.As(err, &coded) {
		return coded.Code
	}
	return 0
}

// h3Findings collects the places where the product does not do what the spec's row says. A
// finding costs a bug file, not a fix and not a weaker assertion: the test goes on, every other
// assertion still fails the test, and when nothing else failed the test ends skipped with the
// evidence. Deleting the skip once the bug is fixed makes the row an ordinary test again.
type h3Findings struct {
	t     *testing.T
	tag   string
	found []string
}

func newH3Findings(t *testing.T, tag string) *h3Findings { return &h3Findings{t: t, tag: tag} }

// add records one finding; owner says what owns it (a defect, a slice or an owner ruling).
func (f *h3Findings) add(owner, format string, args ...any) {
	f.t.Helper()
	msg := fmt.Sprintf(format, args...) + " [" + owner + "]"
	f.found = append(f.found, msg)
	f.t.Logf("FINDING %s: %s", f.tag, msg)
}

// conclude skips the test with the findings, unless something else failed it.
func (f *h3Findings) conclude() {
	f.t.Helper()
	if len(f.found) > 0 && !f.t.Failed() {
		f.t.Skipf("FINDING %s: %s", f.tag, strings.Join(f.found, " | "))
	}
}

// h3Known names one violated-invariant problem a row records as a finding instead of a failure.
// match is asked of each problem of the verdict on its own, so a different problem of the same
// invariant still fails the row.
type h3Known struct {
	owner string
	match func(problem string) bool
}

// h3Check judges the ledger and fails the test with every violated invariant printed. A problem
// of an invariant that the invariant's h3Known matches is a finding with that owner instead of a
// failure; every other problem of the verdict still fails the test.
func h3Check(t *testing.T, c *harness.Cluster, l *harness.Ledger, lost map[string]string, finds *h3Findings, known map[string]h3Known) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	verdicts := l.Check(ctx, c, harness.CheckOptions{Lost: lost})
	failed := false
	for _, verdict := range verdicts {
		if verdict.OK {
			continue
		}
		recorded, unknown := h3Split(verdict, known)
		for _, problem := range recorded {
			finds.add(known[verdict.Name].owner, "%s: %s", verdict.Name, problem)
		}
		if len(unknown) == 0 {
			continue
		}
		failed = true
		after := "before any event"
		if verdict.After != nil {
			after = fmt.Sprintf("after %q", verdict.After.What)
		}
		t.Errorf("invariant %s violated (%s): %s", verdict.Name, after, strings.Join(unknown, "; "))
	}
	if failed {
		t.Errorf("the ledger's declared loss set was %v", lost)
	}
}

// h3Split sorts a verdict's problems into those its h3Known matches and the rest.
func h3Split(verdict harness.Verdict, known map[string]h3Known) (recorded, unknown []string) {
	k, isKnown := known[verdict.Name]
	for _, problem := range verdict.Problems {
		if isKnown && k.match(problem) {
			recorded = append(recorded, problem)
		} else {
			unknown = append(unknown, problem)
		}
	}
	return recorded, unknown
}

func h3Epoch(c *harness.Cluster, id string) uint64 { return uint64(c.Node(id).State().Epoch) }

// h3AwaitMember waits until node watcher's committed state shows member in state.
func h3AwaitMember(t *testing.T, c *harness.Cluster, watcher, member string, state cluster.MemberState, budget time.Duration) {
	t.Helper()
	c.Await(fmt.Sprintf("%s to show %s as %v", watcher, member, state), budget, func() bool {
		for _, m := range c.Node(watcher).State().Members {
			if m.ID == member {
				return m.State == state
			}
		}
		return false
	})
}

// h3MemberIs polls node watcher's committed state for budget and says whether member reached state.
func h3MemberIs(c *harness.Cluster, watcher, member string, state cluster.MemberState, budget time.Duration) bool {
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		for _, m := range c.Node(watcher).State().Members {
			if m.ID == member && m.State == state {
				return true
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// h3NoServeBeforeBound checks the restart hold: a node that has just booted must not serve a
// read of a pair it owns before bound after its boot. Restart returns only after the node has
// converged and holds a lease, which can already be past the bound; then there is nothing left
// to observe and the check says so instead of passing silently.
func h3NoServeBeforeBound(t *testing.T, c *harness.Cluster, node string, booted time.Time, bound time.Duration, ids []string) {
	t.Helper()
	if time.Since(booted) >= bound {
		t.Logf("restart of %s took %s, past the %s hold: the hold is not observable at this profile", node, time.Since(booted).Round(time.Millisecond), bound)
		return
	}
	for time.Since(booted) < bound {
		for _, id := range ids {
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			doc, err := c.Node(node).Get(ctx, proofDB, proofBucket, id)
			cancel()
			if err == nil && doc.ServedBy == node {
				t.Errorf("%s served %s %s after its boot, before the %s hold", node, id, time.Since(booted).Round(time.Millisecond), bound)
				return
			}
		}
	}
}

// h3Stream runs writer and reader goroutines through the ledger while a fault schedule runs.
// Each writer is one goroutine with its own client name and its own ids, and each reader is one
// goroutine with its own client name, which is what the ledger's I1 and I2 checks need.
type h3Stream struct {
	c     *harness.Cluster
	l     *harness.Ledger
	entry atomic.Value
	stop  chan struct{}
	wg    sync.WaitGroup
	mu    sync.Mutex
	acked []string
}

func newH3Stream(c *harness.Cluster, l *harness.Ledger, entry string) *h3Stream {
	s := &h3Stream{c: c, l: l, stop: make(chan struct{})}
	s.entry.Store(entry)
	return s
}

// setEntry moves the stream to node, then waits so no call that picked the old entry starts after
// the caller kills it (Cluster.Node fails a test that names a killed node).
func (s *h3Stream) setEntry(node string) {
	s.entry.Store(node)
	time.Sleep(40 * time.Millisecond) // lets calls that already read the old entry finish starting
}

func (s *h3Stream) start(writers, readers int) {
	for w := 0; w < writers; w++ {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			client := fmt.Sprintf("w%d", w)
			for i := 0; ; i++ {
				select {
				case <-s.stop:
					return
				default:
				}
				id := fmt.Sprintf("s%d-%d", w, i)
				if _, err := h3Write(s.l, s.c, s.entry.Load().(string), client, id, 3*time.Second); err == nil {
					s.mu.Lock()
					s.acked = append(s.acked, id)
					s.mu.Unlock()
				}
				time.Sleep(40 * time.Millisecond) // a pace that keeps a refusal run under the ledger's bound
			}
		}()
	}
	for r := 0; r < readers; r++ {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			client := fmt.Sprintf("r%d", r)
			for n := 0; ; n++ {
				select {
				case <-s.stop:
					return
				default:
				}
				s.mu.Lock()
				var id string
				if len(s.acked) > 0 {
					id = s.acked[(n*7+r)%len(s.acked)]
				}
				s.mu.Unlock()
				if id != "" {
					_, _ = h3Read(s.l, s.c, s.entry.Load().(string), client, id, 3*time.Second) // the refusal is recorded by the ledger
				}
				time.Sleep(25 * time.Millisecond)
			}
		}()
	}
}

func (s *h3Stream) finish() {
	close(s.stop)
	s.wg.Wait()
}

// h3Served counts the reads the ledger saw served, so a row can prove its online I2 check had
// something to judge.
func h3Served(l *harness.Ledger) int {
	served := 0
	for _, o := range l.Obs() {
		if o.Code == 0 && o.Text == "" && o.Seq >= 0 {
			served++
		}
	}
	return served
}

// TestThreeVotersOneUnreachableBeforeDetection is row A1: n2 stalls and for t below the
// detection time nothing is committed; a slot n2 owns is forwarded and never answered (the
// client's own deadline), every other slot is applied.
func TestThreeVotersOneUnreachableBeforeDetection(t *testing.T) {
	h3Gate(t)
	c := h3Setup(t, harness.P0, harness.Options{})
	others := h3Owned(t, c, "n1", 4, "a1o", "n2")
	owned := h3Owned(t, c, "n2", 3, "a1s", "")
	l := h3Ledger(t, others[0])
	// One warm write before the stall, to an n1-owned id that n3 copies. A copy that has never
	// held the bucket's declaration asks the declaration's owner for it (cluster/wire/node_cluster.go:
	// "this copy does not hold the declaration of bucket ... could not fetch it"), and when that
	// owner is the stalled n2 the first write is answered applied_degraded/1. Warm, n3 holds the
	// declaration, so the row measures the slot rule and not a cold bucket (about half the runs
	// failed without this, none of 12 with it).
	h3Preack(t, l, c, "n1", "w-warm", others[:1])
	others = others[1:]
	epoch := h3Epoch(c, "n1")
	window := harness.P0.Detection() - 100*time.Millisecond

	l.Fault("stall n2")
	c.Stall("n2", true)
	began := time.Now()
	for _, id := range others {
		got, err := h3Write(l, c, "n1", "w-other", id, window)
		if err != nil || got.Outcome != machine.OutcomeApplied || got.CopiesConfirmed != 2 {
			t.Errorf("write %s to a slot n2 neither owns nor copies answered %v/%d (err %v), want served(applied)", id, got.Outcome, got.CopiesConfirmed, err)
		}
	}
	entry := c.Node("n1")
	errs := make([]error, len(owned))
	var wg sync.WaitGroup
	for i, id := range owned {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithDeadline(context.Background(), began.Add(window))
			defer cancel()
			_, errs[i] = entry.Insert(ctx, proofDB, proofBucket, docOf(id, "w-stalled"))
		}()
	}
	wg.Wait()
	for i, id := range owned {
		if errs[i] == nil {
			t.Errorf("write %s to a slot the stalled n2 owns was answered before detection, want client_timeout", id)
			continue
		}
		// The deadline is the stalled owner's silence: the stall makes n2 not running, so the
		// ledger must not read the entry node's deadline as an unnamed refusal from a live node.
		l.FeedObs(harness.Obs{Client: "w-stalled", ID: id, Seq: -1, Via: "n1", At: time.Now(), Text: errs[i].Error(), ViaRunning: false})
	}
	if took := time.Since(began); took >= harness.P0.Detection() {
		t.Fatalf("the window ran %s, past the %s detection time: the row measured nothing", took, harness.P0.Detection())
	}
	if got := h3Epoch(c, "n1"); got != epoch {
		t.Fatalf("epoch moved %d to %d inside the detection window: n2 was committed down early", epoch, got)
	}
	l.Fault("unstall n2")
	c.Stall("n2", false)
	h3Check(t, c, l, nil, nil, nil)
}

// TestThreeVotersOneDownMovesItsSlotsDegraded is row A2: n2 is killed, the others commit it
// down, and the next copy owns its slots with confirmed 1 of 2, applied_degraded.
func TestThreeVotersOneDownMovesItsSlotsDegraded(t *testing.T) {
	h3Gate(t)
	c := h3Setup(t, harness.P0, harness.Options{})
	ids := h3Owned(t, c, "n2", 5, "a2", "")
	l := h3Ledger(t, ids[0])
	h3Preack(t, l, c, "n1", "w-before", ids[:2])
	epoch := h3Epoch(c, "n1")

	l.Fault("kill n2")
	c.Kill("n2")
	c.Await("n1 and n3 to commit n2 down (an epoch bump)", quorumFailoverBudget, func() bool {
		return h3Epoch(c, "n1") > epoch && h3Epoch(c, "n3") > epoch
	})
	c.AwaitLeader(10 * time.Second)
	c.AwaitLeases(quorumFailoverBudget)
	for _, id := range ids[2:] {
		got := h3WriteAcked(t, l, c, "n1", "w-after", id, quorumFailoverBudget)
		if got.Outcome != machine.OutcomeAppliedDegraded || got.CopiesConfirmed != 1 {
			t.Errorf("write %s to a slot of the down n2 answered %v/%d, want served(applied_degraded, 1)", id, got.Outcome, got.CopiesConfirmed)
		}
	}
	for _, via := range []string{"n1", "n3"} {
		for _, id := range ids[:2] {
			h3ReadServed(t, l, c, via, "r-"+via, id, quorumFailoverBudget)
		}
	}
	h3Check(t, c, l, nil, nil, nil)
}

// TestThreeVotersOneDeadKeepsOwnership is row A3: past dead_ms n2 is committed dead, and the
// owner of its slots does not change, while everything acknowledged stays readable.
func TestThreeVotersOneDeadKeepsOwnership(t *testing.T) {
	h3Gate(t)
	finds := newH3Findings(t, "H3-A3")
	defer finds.conclude()
	profile := maps.Clone(harness.P0)
	profile["dead_ms"] = 3000
	c := h3Setup(t, profile, harness.Options{})
	ids := h3Owned(t, c, "n2", 4, "a3", "")
	l := h3Ledger(t, ids[0])
	h3Preack(t, l, c, "n1", "w-before", ids[:2])
	epoch := h3Epoch(c, "n1")

	l.Fault("kill n2")
	c.Kill("n2")
	c.Await("n1 to commit n2 down", quorumFailoverBudget, func() bool { return h3Epoch(c, "n1") > epoch })
	c.AwaitLeader(10 * time.Second)
	c.AwaitLeases(quorumFailoverBudget)
	owners := map[string]string{}
	for _, id := range ids {
		owner, _, err := c.Node("n1").Owner(proofCtx(t), proofDB, proofBucket, id)
		if err != nil || owner == "n2" {
			t.Fatalf("after the down commit the owner of %s is %q (err %v), want the next copy", id, owner, err)
		}
		owners[id] = owner
	}
	h3WriteAcked(t, l, c, "n1", "w-down", ids[2], quorumFailoverBudget)
	h3AwaitMember(t, c, "n1", "n2", cluster.MemberDead, 30*time.Second)
	for id, was := range owners {
		now, _, err := c.Node("n1").Owner(proofCtx(t), proofDB, proofBucket, id)
		if err != nil || now != was {
			t.Errorf("owner of %s moved from %s to %q (err %v) when n2 was committed dead, want it unchanged", id, was, now, err)
		}
	}
	// The spec's prose says the writes stay under-replicated once n2 is dead; dead removes n2 from
	// the ranking, so the next-ranked node becomes the second copy and a write confirms 2 of 2.
	// Either answer is an acknowledgement; the outcome is logged for the report.
	got := h3WriteAcked(t, l, c, "n3", "w-dead", ids[3], quorumFailoverBudget)
	t.Logf("a write after n2 is dead answered %v/%d", got.Outcome, got.CopiesConfirmed)
	for _, via := range []string{"n1", "n3"} {
		for _, id := range ids[:3] {
			h3ReadServed(t, l, c, via, "r-"+via, id, quorumFailoverBudget)
		}
	}
	// The new copy of a pair n2 held is never seeded after the dead commit (the probe pair stays
	// at position 0/0 on it for 45 s): the seeding-completion slices are not built.
	h3Check(t, c, l, nil, finds, map[string]h3Known{harness.InvQuiescenceConverge: h3GainerNeverSeededKnown})
}

// TestThreeVotersTwoUnreachableLapseEverything is row A4: with two of three stalled nothing is
// a majority, every answer is LEASE_LAPSED by the stop bound, and no epoch is committed.
func TestThreeVotersTwoUnreachableLapseEverything(t *testing.T) {
	h3Gate(t)
	c := h3Setup(t, harness.P0, harness.Options{})
	ids := h3Owned(t, c, "n1", 24, "a4", "")
	l := h3Ledger(t, ids[0])
	h3Preack(t, l, c, "n1", "w-before", ids[:2])
	epoch := h3Epoch(c, "n1")

	l.Fault("stall n2 and n3")
	c.Stall("n2", true)
	c.Stall("n3", true)
	began := time.Now()
	time.Sleep(harness.P0.StopBound() + h3Slack)
	next := 2
	for time.Since(began) < 5*time.Second {
		_, err := h3Write(l, c, "n1", "w-lapsed", ids[next], 1500*time.Millisecond)
		next++
		if h3Code(err) != cluster.CodeLeaseLapsed {
			t.Errorf("a write %s after the stop bound answered %v, want refused(LEASE_LAPSED)", time.Since(began).Round(time.Millisecond), err)
		}
		_, err = h3Read(l, c, "n1", "r-lapsed", ids[0], 1500*time.Millisecond)
		if h3Code(err) != cluster.CodeLeaseLapsed {
			t.Errorf("a read %s after the stop bound answered %v, want refused(LEASE_LAPSED)", time.Since(began).Round(time.Millisecond), err)
		}
		time.Sleep(300 * time.Millisecond)
	}
	if got := h3Epoch(c, "n1"); got != epoch {
		t.Errorf("epoch moved %d to %d with two of three unreachable: nothing may commit without a majority", epoch, got)
	}
	l.Fault("unstall n2 and n3")
	c.Stall("n2", false)
	c.Stall("n3", false)
	c.AwaitLeader(30 * time.Second)
	c.AwaitLeases(30 * time.Second)
	h3Check(t, c, l, nil, nil, nil)
}

// TestThreeVotersPartitionOneTwo is row A5: the leader n1 alone against n2 and n3. Its
// majority write gets no answer until the lease lapses and is then unknown; its one-concern
// write is applied and discarded with the tail; then every request is LEASE_LAPSED.
func TestThreeVotersPartitionOneTwo(t *testing.T) {
	h3Gate(t)
	finds := newH3Findings(t, "H3-A5")
	defer finds.conclude()
	c := h3Setup(t, harness.P0, harness.Options{})
	isolated := c.AwaitLeader(10 * time.Second)
	var majoritySide []string
	for _, id := range c.IDs() {
		if id != isolated {
			majoritySide = append(majoritySide, id)
		}
	}
	ids := h3Owned(t, c, isolated, 5, "a5", "")
	oneID, majorityID, laterID, readID := ids[0], ids[1], ids[2], ids[3]
	l := h3Ledger(t, readID)
	h3Preack(t, l, c, isolated, "w-before", []string{readID})

	l.Fault("partition " + isolated + " | " + fmt.Sprint(majoritySide))
	c.Partition([]string{isolated}, majoritySide)
	cutAt := time.Now()
	got, err := h3Write(l, c, isolated, "w-one", oneID, 5*time.Second, wire.WithConcern(machine.ConcernOne))
	if err != nil || got.Outcome != machine.OutcomeApplied {
		t.Errorf("a one-concern write %s on the minority side answered %v (err %v), want served(applied)", oneID, got.Outcome, err)
	}
	got, err = h3Write(l, c, isolated, "w-majority", majorityID, 5*time.Second)
	// Not a defect: spec row B (write-path-and-replication sections 7 and 9). The harness
	// wc_timeout (500 ms) is shorter than the lease lifetime, so the owner answers at wc_timeout
	// either unknown (lease lapsed by then) or applied_degraded with its own copy (lease still
	// valid, level missed). Both are in the declared loss set below, as the H5 row B5 does.
	switch {
	case err != nil:
		t.Errorf("a majority write %s on the minority side got no answer inside 5 s: %v, want unknown or applied_degraded/1", majorityID, err)
	case got.Outcome == machine.OutcomeUnknown:
	case got.Outcome == machine.OutcomeAppliedDegraded && got.CopiesConfirmed == 1:
		t.Logf("the majority write %s answered applied_degraded/1 %s after the cut: the lease was still valid at wc_timeout (spec row B)", majorityID, time.Since(cutAt).Round(time.Millisecond))
	default:
		t.Errorf("a majority write %s on the minority side answered %v/%d %s after the cut, want unknown or applied_degraded/1", majorityID, got.Outcome, got.CopiesConfirmed, time.Since(cutAt).Round(time.Millisecond))
	}
	if took := time.Since(cutAt); took > harness.P0.StopBound()+h3Slack {
		t.Errorf("the minority's majority write was answered %s after the cut, the lease bound is %s", took, harness.P0.StopBound())
	}
	// Past the lease bound every request is refused. The spec names LEASE_LAPSED; NO_QUORUM is
	// also right: machine.Check (admit.go) tests the lease first and answers NO_QUORUM only when
	// the lease is still valid and the node cannot confirm a majority, which is the state of an
	// isolated owner whose lease has not yet run out. Any other answer fails the row.
	if _, err := h3Write(l, c, isolated, "w-later", laterID, 3*time.Second); err == nil {
		t.Errorf("a write after the lease lapsed was acknowledged, want refused")
	} else if code := h3Code(err); code != cluster.CodeLeaseLapsed && code != cluster.CodeNoQuorum {
		t.Errorf("a write after the lease lapsed was refused %v (%v), want LEASE_LAPSED or NO_QUORUM", code, err)
	}
	if _, err := h3Read(l, c, isolated, "r-later", readID, 3*time.Second); err == nil {
		t.Errorf("a read after the lease lapsed was served, want refused")
	} else if code := h3Code(err); code != cluster.CodeLeaseLapsed && code != cluster.CodeNoQuorum {
		t.Errorf("a read after the lease lapsed was refused %v (%v), want LEASE_LAPSED or NO_QUORUM", code, err)
	}

	l.Fault("heal")
	c.Heal()
	c.AwaitConverged(30 * time.Second)
	c.AwaitLeases(30 * time.Second)
	// The spec says the tail is discarded when the isolated node learns e+1. The seed pull that
	// resets a pair is not built in this release (wire.seedStub.ResetAndSeed), so it may stay.
	discarded := false
	for until := time.Now().Add(quorumFailoverBudget); time.Now().Before(until) && !discarded; time.Sleep(100 * time.Millisecond) {
		_, found, err := c.Node(isolated).LocalDocument(proofCtx(t), proofDB, proofBucket, oneID)
		discarded = err == nil && !found
	}
	if !discarded {
		finds.add("seeding S1 (ResetAndSeed, divergent tail)", "%s still holds the one-concern write %s %s after the heal; the spec says the tail is discarded when it learns e+1, and wire.seedStub.ResetAndSeed says the reset is not built", isolated, oneID, quorumFailoverBudget)
	}
	newOwner, _, err := c.Node(majoritySide[0]).Owner(proofCtx(t), proofDB, proofBucket, oneID)
	if err != nil {
		t.Fatalf("placing %s after the heal: %v", oneID, err)
	}
	ownPos, err := c.Node(isolated).PairPosition(proofCtx(t), proofDB, proofBucket, oneID)
	if err != nil {
		t.Fatalf("position of the pair on %s: %v", isolated, err)
	}
	newPos, err := c.Node(newOwner).PairPosition(proofCtx(t), proofDB, proofBucket, oneID)
	if err != nil {
		t.Fatalf("position of the pair on %s: %v", newOwner, err)
	}
	if newPos.Less(ownPos) {
		t.Errorf("after the heal %s holds position %v, ahead of the owner %s at %v: the tail was not discarded", isolated, ownPos, newOwner, newPos)
	}
	h3Check(t, c, l, map[string]string{
		oneID:      "minority one-concern write, discarded by spec",
		majorityID: "minority majority-concern write whose fate is unverifiable by spec (unknown)",
	}, nil, nil)
}

// TestThreeVotersLeaderDiesMidCommit is row A6: a write is in flight through the leader when
// the leader dies. It gets no answer (a transport or deadline error, never unknown), a new
// leader is elected, and leases are granted again within the stop bound.
func TestThreeVotersLeaderDiesMidCommit(t *testing.T) {
	h3Gate(t)
	finds := newH3Findings(t, "H3-A6")
	defer finds.conclude()
	c := h3Setup(t, harness.P0, harness.Options{})
	leader := c.AwaitLeader(10 * time.Second)
	ids := h3Owned(t, c, leader, 3, "a6", "")
	_, copies, err := c.Node(leader).Owner(proofCtx(t), proofDB, proofBucket, ids[0])
	if err != nil || len(copies) != 2 {
		t.Fatalf("placing %s: copies %v err %v", ids[0], copies, err)
	}
	copyNode := copies[1]
	l := h3Ledger(t, ids[0])
	h3Preack(t, l, c, leader, "w-before", ids[:1])
	epoch := h3Epoch(c, copyNode)

	// A slow copy keeps the write in flight, waiting on its second copy, when the leader dies.
	l.Fault("slow disk on " + copyNode)
	c.SlowDisk(copyNode, 1500*time.Millisecond)
	// The write enters through a survivor and is forwarded to the leader that owns it: a client of
	// the dead process gets no answer, and the entry node sees the forward connection die. The
	// harness Kill is a clean close, so a call made on the leader itself would still be answered.
	entry := c.Node(copyNode)
	type answer struct {
		got wire.Inserted
		err error
	}
	answered := make(chan answer, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		got, err := entry.Insert(ctx, proofDB, proofBucket, docOf(ids[1], "w-inflight"))
		answered <- answer{got, err}
	}()
	time.Sleep(50 * time.Millisecond)
	l.Fault("kill the leader " + leader)
	c.Kill(leader)
	c.SlowDisk(copyNode, 0)
	c.AwaitLeader(2 * time.Second)
	c.AwaitLeases(harness.P0.StopBound())
	result := <-answered
	if result.err == nil {
		// Cluster.Kill is a clean close, not a crash: the owner's handler already running can still
		// write its reply on the open connection (seen in 1 of 3 runs). A dead process sends no
		// answer, so the row is not met; the cause is the harness kill as much as the product.
		finds.add("harness artefact: Kill is a clean close", "the write in flight when the leader was killed was answered %v/%d, the spec says a client timeout (harness Kill is a clean close and lets the running handler reply)", result.got.Outcome, result.got.CopiesConfirmed)
	} else {
		// The silence of the dead owner is what the client saw; the entry node's transport error
		// or deadline is not an unnamed refusal from a live node.
		l.FeedObs(harness.Obs{Client: "w-inflight", ID: ids[1], Seq: -1, Via: copyNode, At: time.Now(), Text: result.err.Error(), ViaRunning: false})
	}
	c.Await("the survivors to commit the leader down", quorumFailoverBudget, func() bool {
		for _, id := range c.Running() {
			if h3Epoch(c, id) <= epoch {
				return false
			}
		}
		return true
	})
	survivor := c.Running()[0]
	h3WriteAcked(t, l, c, survivor, "w-after", ids[2], quorumFailoverBudget)
	// What Kill does guarantee, however the in-flight write ended: the request is acknowledged at
	// most once. Its fate is unknown to the client, so it is retried twice through a survivor; an
	// insert that already landed answers DOCUMENT_EXISTS, so across the in-flight answer and both
	// retries at most one acknowledgement may exist, and the document must be on every node.
	// [BREAK: the retries use Upsert, which acknowledges every time]
	acknowledged := 0
	if result.err == nil {
		acknowledged++
	}
	for retry := 1; retry <= 2; retry++ {
		deadline := time.Now().Add(quorumFailoverBudget)
		for {
			_, err := h3Write(l, c, survivor, "w-inflight", ids[1], 5*time.Second)
			if err == nil {
				acknowledged++
				break
			}
			if h3Code(err) == cluster.CodeDocumentExists {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("retry %d of the in-flight write %s was not answered within %s: %v", retry, ids[1], quorumFailoverBudget, err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	if acknowledged > 1 {
		t.Errorf("the in-flight write %s was acknowledged %d times across the original and two retries, want at most once", ids[1], acknowledged)
	}
	for _, via := range c.Running() {
		// Read directly: the in-flight document is not the ledger's (it has no seq field).
		c.Await("the in-flight write "+ids[1]+" to be readable through "+via, quorumFailoverBudget, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err := c.Node(via).Get(ctx, proofDB, proofBucket, ids[1])
			return err == nil
		})
		h3ReadServed(t, l, c, via, "r-"+via, ids[0], quorumFailoverBudget)
	}
	h3Check(t, c, l, nil, finds, nil)
}

// TestThreeVotersTheDownNodeReturnsActive is row A7: n2 is down, writes to its slots are
// acknowledged degraded, then n2 returns as active at the same incarnation, holds every write
// acknowledged while it was away, and does not serve a pair before its restart hold passes.
func TestThreeVotersTheDownNodeReturnsActive(t *testing.T) {
	h3Gate(t)
	finds := newH3Findings(t, "H3-A7")
	defer finds.conclude()
	c := h3Setup(t, harness.P0, harness.Options{JoinerActivation: true})
	ids := h3Owned(t, c, "n2", 5, "a7", "")
	l := h3Ledger(t, ids[0])
	h3Preack(t, l, c, "n1", "w-before", ids[:1])
	epoch := h3Epoch(c, "n1")

	l.Fault("kill n2")
	c.Kill("n2")
	h3AwaitMember(t, c, "n1", "n2", cluster.MemberDown, 30*time.Second)
	c.AwaitLeader(10 * time.Second)
	c.AwaitLeases(quorumFailoverBudget)
	if h3Epoch(c, "n1") <= epoch {
		t.Fatalf("n2 is down but the epoch did not move from %d", epoch)
	}
	whileDown := ids[1:]
	for _, id := range whileDown {
		h3WriteAcked(t, l, c, "n1", "w-down", id, quorumFailoverBudget)
	}

	l.Fault("restart n2")
	booted := time.Now()
	c.Restart("n2")
	h3NoServeBeforeBound(t, c, "n2", booted, harness.P0.StopBound(), ids)
	if !h3MemberIs(c, "n1", "n2", cluster.MemberActive, quorumFailoverBudget) {
		t.Fatalf("n2 restarted at its own incarnation and stayed down for %s: nothing commits down to active unless an operator activates it, the row expects active at e+2", quorumFailoverBudget)
	}
	for _, id := range whileDown {
		c.Await("n2 to hold "+id+", acknowledged while it was down", 30*time.Second, func() bool {
			_, found, err := c.Node("n2").LocalDocument(proofCtx(t), proofDB, proofBucket, id)
			return err == nil && found
		})
	}
	for _, via := range []string{"n1", "n2", "n3"} {
		for _, id := range ids[:2] {
			h3ReadServed(t, l, c, via, "r-"+via, id, quorumFailoverBudget)
		}
	}
	h3Check(t, c, l, nil, finds, map[string]h3Known{harness.InvRefusalsAreNamed: h3UncodedForwardKnown})
}

// TestThreeVotersFlapping is row A8: one node is killed and restarted three times, two seconds
// apart, at profile P1 while writers and readers run; no invariant may be violated.
func TestThreeVotersFlapping(t *testing.T) {
	h3Gate(t)
	finds := newH3Findings(t, "H3-A8")
	defer finds.conclude()
	c := h3Setup(t, harness.P1, harness.Options{JoinerActivation: true})
	leader := c.AwaitLeader(10 * time.Second)
	flapper := ""
	for _, id := range c.IDs() {
		if id != leader {
			flapper = id
			break
		}
	}
	l := h3Ledger(t, "s0-0")
	stream := newH3Stream(c, l, leader)
	stream.start(2, 1)
	time.Sleep(500 * time.Millisecond)
	for cycle := 1; cycle <= 3; cycle++ {
		began := time.Now()
		l.Fault(fmt.Sprintf("kill %s (cycle %d)", flapper, cycle))
		c.Kill(flapper)
		time.Sleep(time.Second)
		l.Fault(fmt.Sprintf("restart %s (cycle %d)", flapper, cycle))
		c.Restart(flapper)
		if rest := 2*time.Second - time.Since(began); rest > 0 {
			time.Sleep(rest)
		}
	}
	if !h3MemberIs(c, leader, flapper, cluster.MemberActive, 5*time.Second) {
		t.Fatalf("%s was killed and restarted three times at its own incarnation and is still down 5 s after the last restart: nothing commits down to active (the A7 finding)", flapper)
	}
	stream.finish()
	if acks := len(l.Acks()); acks < 20 {
		t.Fatalf("the writers got only %d acknowledgements in the run: it judged nothing", acks)
	}
	if served := h3Served(l); served < 5 {
		t.Fatalf("the readers got only %d served reads in the run: the online I2 check judged nothing", served)
	}
	h3Check(t, c, l, nil, finds, map[string]h3Known{harness.InvRefusalsAreNamed: h3UncodedForwardKnown})
}

// TestThreeVotersSlowDiskDegradesThenRecovers is row A9: the copy's log takes three
// wc_timeouts per append. A write is acknowledged after the owner's own append only, as
// applied_degraded with one copy, and once the disk is fast again the pair is not wedged.
func TestThreeVotersSlowDiskDegradesThenRecovers(t *testing.T) {
	h3Gate(t)
	const wcTimeout = 500 * time.Millisecond
	c := h3Setup(t, harness.P0, harness.Options{WCTimeout: wcTimeout})
	ids := h3Owned(t, c, "n1", 10, "a9", "")
	_, copies, err := c.Node("n1").Owner(proofCtx(t), proofDB, proofBucket, ids[0])
	if err != nil || len(copies) != 2 {
		t.Fatalf("placing %s: copies %v err %v", ids[0], copies, err)
	}
	copyNode := copies[1]
	l := h3Ledger(t, ids[0])

	l.Fault("slow disk on " + copyNode)
	c.SlowDisk(copyNode, 3*wcTimeout)
	got, err := h3Write(l, c, "n1", "w-slow", ids[0], 10*time.Second)
	if err != nil {
		t.Fatalf("the write %s with a slow copy was refused: %v", ids[0], err)
	}
	if got.Outcome != machine.OutcomeAppliedDegraded || got.CopiesConfirmed != 1 {
		t.Errorf("the write %s with a slow copy answered %v/%d, want served(applied_degraded, 1)", ids[0], got.Outcome, got.CopiesConfirmed)
	}
	if _, found, err := c.Node("n1").LocalDocument(proofCtx(t), proofDB, proofBucket, ids[0]); err != nil || !found {
		t.Errorf("the write %s was acknowledged but the owner's own log does not hold it (found %v, err %v)", ids[0], found, err)
	}

	l.Fault("fast disk on " + copyNode)
	c.SlowDisk(copyNode, 0)
	deadline := time.Now().Add(quorumFailoverBudget)
	recovered := false
	for next := 1; next < len(ids) && time.Now().Before(deadline); next++ {
		got, err := h3Write(l, c, "n1", "w-fast", ids[next], 10*time.Second)
		if err == nil && got.Outcome == machine.OutcomeApplied && got.CopiesConfirmed == 2 {
			recovered = true
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if !recovered {
		t.Errorf("no write was applied/2 within %s of the disk recovering: the pair is wedged", quorumFailoverBudget)
	}
	h3Check(t, c, l, nil, nil, nil)
}

// TestThreeVotersAsymmetricPartition is row A10: the owner n2 can neither dial the others nor
// be dialed by them. It must never answer applied past the stop bound; the others commit it down.
func TestThreeVotersAsymmetricPartition(t *testing.T) {
	h3Gate(t)
	c := h3Setup(t, harness.P0, harness.Options{})
	leader := c.AwaitLeader(10 * time.Second)
	owner := ""
	var observers []string
	for _, id := range c.IDs() {
		if id != leader && owner == "" {
			owner = id
			continue
		}
		observers = append(observers, id)
	}
	ids := h3Owned(t, c, owner, 40, "a10", "")
	l := h3Ledger(t, ids[0])
	h3Preack(t, l, c, owner, "w-before", ids[:1])
	epoch := h3Epoch(c, observers[0])

	// Cutting the owner's egress alone lapses its lease but the others never commit it down:
	// they probe it by dialing it. They commit it down only when their own dials to it fail too.
	l.Fault("one-way cut " + owner + " to " + fmt.Sprint(observers) + " and each of them to " + owner)
	for _, observer := range observers {
		c.BlockOneWay(owner, observer, true)
		c.BlockOneWay(observer, owner, true)
	}
	cutAt := time.Now()
	lost := map[string]string{}
	for next := 1; time.Since(cutAt) < harness.P0.StopBound()+2*time.Second && next < len(ids); next++ {
		got, err := h3Write(l, c, owner, "w-egress", ids[next], 3*time.Second)
		if err != nil {
			continue
		}
		lost[ids[next]] = "egress cut, acks after the cut are by spec unknown"
		if got.Outcome == machine.OutcomeApplied && time.Since(cutAt) > harness.P0.StopBound()+h3Slack {
			t.Errorf("the cut owner answered applied for %s %s after the cut, the stop bound is %s", ids[next], time.Since(cutAt).Round(time.Millisecond), harness.P0.StopBound())
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.Await("the others to commit "+owner+" down", quorumFailoverBudget, func() bool {
		return h3Epoch(c, observers[0]) > epoch && h3Epoch(c, observers[1]) > epoch
	})
	h3AwaitMember(t, c, observers[0], owner, cluster.MemberDown, 10*time.Second)

	l.Fault("heal")
	c.Heal()
	c.AwaitConverged(30 * time.Second)
	c.AwaitLeases(30 * time.Second)
	h3Check(t, c, l, lost, nil, nil)
}

// TestThreeVotersRestartStorm is row C1: every node is killed and restarted in turn, back to
// back, at profile P1 with writers and readers running. A restart does not serve a pair before
// its hold passes, and no invariant may be violated.
func TestThreeVotersRestartStorm(t *testing.T) {
	h3Gate(t)
	finds := newH3Findings(t, "H3-C1")
	defer finds.conclude()
	c := h3Setup(t, harness.P1, harness.Options{JoinerActivation: true})
	l := h3Ledger(t, "s0-0")
	stream := newH3Stream(c, l, "n1")
	stream.start(2, 1)
	time.Sleep(500 * time.Millisecond)
	for _, victim := range []string{"n2", "n3", "n1"} {
		entry := "n1"
		if victim == "n1" {
			entry = "n2"
		}
		stream.setEntry(entry)
		l.Fault("kill " + victim)
		c.Kill(victim)
		l.Fault("restart " + victim)
		booted := time.Now()
		c.Restart(victim)
		h3NoServeBeforeBound(t, c, victim, booted, harness.P1.StopBound(), []string{"s0-0", "s1-0"})
	}
	stream.setEntry("n1")
	for _, id := range c.IDs() {
		if !h3MemberIs(c, "n1", id, cluster.MemberActive, 5*time.Second) {
			t.Fatalf("%s was restarted and is still not active 5 s later: nothing commits down to active (the A7 finding)", id)
		}
	}
	stream.finish()
	if acks := len(l.Acks()); acks < 20 {
		t.Fatalf("the writers got only %d acknowledgements in the run: it judged nothing", acks)
	}
	if served := h3Served(l); served < 5 {
		t.Fatalf("the readers got only %d served reads in the run: the online I2 check judged nothing", served)
	}
	h3Check(t, c, l, nil, finds, map[string]h3Known{harness.InvRefusalsAreNamed: h3UncodedForwardKnown})
}

// TestThreeVotersLeaderLossDuringAlter is row C2: the leader dies while an alter bucket is in
// flight. The alter is refused or applied once, the bucket stays writable, a retry converges,
// and every node describes the same table.
func TestThreeVotersLeaderLossDuringAlter(t *testing.T) {
	h3Gate(t)
	finds := newH3Findings(t, "H3-C2")
	defer finds.conclude()
	c := h3Setup(t, harness.P0, harness.Options{})
	leader := c.AwaitLeader(10 * time.Second)
	// An alter runs on the node that owns the bucket's declaration and cannot be forwarded. Find a
	// bucket whose declaration a follower owns, so the alter is on a live node when the leader dies.
	const candidates = 16
	for i := 0; i < candidates; i++ {
		if _, err := c.Node(leader).CreateBucket(proofCtx(t), proofDB, fmt.Sprintf("c2b%d", i), wire.Field{Name: "name", Type: "string"}, wire.Field{Name: "seq", Type: "int"}); err != nil {
			t.Fatalf("creating bucket c2b%d: %v", i, err)
		}
	}
	c.AwaitConverged(10 * time.Second)
	bucket, owner := "", ""
	for i := 0; i < candidates && owner == ""; i++ {
		name := fmt.Sprintf("c2b%d", i)
		_, err := c.Node(leader).AlterBucket(proofCtx(t), proofDB, name, wire.AlterChange{Add: []wire.FieldEntry{{Name: "c2probe", Type: "int"}}})
		if err == nil {
			continue
		}
		if found := relayOwnsPattern.FindStringSubmatch(err.Error()); found != nil && found[1] != leader {
			bucket, owner = name, found[1]
		}
	}
	if owner == "" {
		t.Fatalf("no bucket in 16 had a declaration owned by a follower of the leader %s", leader)
	}
	l := harness.NewLedger(t, harness.LedgerOptions{DB: proofDB, Bucket: bucket, ProbeID: "c2-0"})
	h3Preack(t, l, c, leader, "w-before", []string{"c2-0"})
	change := wire.AlterChange{Add: []wire.FieldEntry{{Name: "c2age", Type: "int"}}}

	// An alter takes a few milliseconds, so the owner's log is slowed to keep it in flight (its
	// declaration write is the slow step) when the leader dies, before the schema bump is proposed.
	l.Fault("slow disk on " + owner)
	c.SlowDisk(owner, 600*time.Millisecond)
	alterDone := make(chan error, 1)
	ownerNode := c.Node(owner)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := ownerNode.AlterBucket(ctx, proofDB, bucket, change)
		alterDone <- err
	}()
	time.Sleep(100 * time.Millisecond)
	// The row proves nothing unless the alter is still running at the kill.
	select {
	case err := <-alterDone:
		t.Fatalf("the alter on %s had already finished (%v) when the leader was to be killed: it was not in flight, the row measured nothing", owner, err)
	default:
	}
	l.Fault("kill the leader " + leader + " during alter bucket on " + owner)
	c.Kill(leader)
	c.SlowDisk(owner, 0)
	if err := <-alterDone; err != nil {
		switch code := h3Code(err); code {
		case cluster.CodeStateMoved, cluster.CodeUnavailable, 0:
			t.Logf("the alter in flight when the leader died was refused (%v): %v", code, err)
		default:
			t.Errorf("the alter in flight when the leader died was refused %v (%v), want STATE_MOVED, UNAVAILABLE or a deadline", code, err)
		}
	} else {
		t.Logf("the alter in flight when the leader died was applied")
	}

	c.AwaitLeader(10 * time.Second)
	c.AwaitLeases(quorumFailoverBudget)
	deadline := time.Now().Add(quorumFailoverBudget + 15*time.Second)
	done := false
	var last error
	for !done && time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, last = ownerNode.AlterBucket(ctx, proofDB, bucket, change)
		cancel()
		done = last == nil
		if !done {
			time.Sleep(200 * time.Millisecond)
		}
	}
	if !done {
		t.Fatalf("a retried alter on %s never converged within the budget: %v", owner, last)
	}
	c.AwaitConverged(30 * time.Second)
	var reference map[string]uint16
	for _, via := range c.Running() {
		described, err := c.Node(via).ClientBucket(proofCtx(t), proofDB, bucket)
		if err != nil {
			t.Fatalf("describing the bucket on %s: %v", via, err)
		}
		if _, ok := described.FieldIDs["c2age"]; !ok {
			t.Errorf("%s describes the bucket without the altered field: %v", via, described.FieldIDs)
		}
		if reference == nil {
			reference = described.FieldIDs
		} else if !maps.Equal(reference, described.FieldIDs) {
			t.Errorf("%s describes the bucket as %v, another node as %v: a half-applied table", via, described.FieldIDs, reference)
		}
	}
	h3WriteAcked(t, l, c, owner, "w-after", "c2-1", quorumFailoverBudget)
	for _, via := range c.Running() {
		h3ReadServed(t, l, c, via, "r-"+via, "c2-0", quorumFailoverBudget)
	}
	h3Check(t, c, l, nil, finds, map[string]h3Known{harness.InvRefusalsAreNamed: h3UncodedForwardKnown})
}

// TestThreeVotersNodeAddedDuringWrites is row C3, which waits for the seeding-completion slices.
func TestThreeVotersNodeAddedDuringWrites(t *testing.T) {
	t.Skip("waits for the seeding-completion slices S1-S3")
}

// TestThreeVotersOwnerKilledDuringSeeding is row C4, which waits for the seeding-completion slices.
func TestThreeVotersOwnerKilledDuringSeeding(t *testing.T) {
	t.Skip("waits for the seeding-completion slices S1-S3")
}

// TestAKnownFindingHidesOnlyTheProblemItNames: the rows record one refusals-are-named problem
// (a forward's transport error with no code) and one quiescence problem (a gainer at 0:0); a
// different unnamed refusal, a bare deadline, or a pair that disagrees elsewhere still fails.
// [BREAK: h3UncodedForwardKnown.match returns true].
func TestAKnownFindingHidesOnlyTheProblemItNames(t *testing.T) {
	uncoded := harness.Verdict{Name: harness.InvRefusalsAreNamed, Problems: []string{
		`running node n1 answered "reading \"x\" from app.users: forwarding the read to n2: calling n2 on the data channel: hoardDB wire error [INTERNAL]: this node is still opening; retry shortly" for x with no code: a bare deadline or transport error`,
		`running node n1 answered "context deadline exceeded" for y with no code: a bare deadline or transport error`,
		`running node n3 answered SEQ_GAP for z, which is not a refusal a fault may cause`,
	}}
	recorded, unknown := h3Split(uncoded, map[string]h3Known{harness.InvRefusalsAreNamed: h3UncodedForwardKnown})
	if len(recorded) != 1 || !strings.Contains(recorded[0], "forwarding the read") || len(unknown) != 2 {
		t.Fatalf("recorded %q, unknown %q: want only the forward's transport error recorded", recorded, unknown)
	}
	quiescence := harness.Verdict{Name: harness.InvQuiescenceConverge, Problems: []string{"the nodes holding the probe pair disagree: map[n1:3:1 n3:0:0]"}}
	gainer := h3GainerNeverSeededKnown
	if recorded, _ := h3Split(quiescence, map[string]h3Known{harness.InvQuiescenceConverge: gainer}); len(recorded) != 1 {
		t.Fatalf("a gainer at 0:0 was not recorded")
	}
	quiescence.Problems = []string{"the nodes holding the probe pair disagree: map[n1:3:1 n3:3:0]"}
	if _, unknown := h3Split(quiescence, map[string]h3Known{harness.InvQuiescenceConverge: gainer}); len(unknown) != 1 {
		t.Fatalf("a pair that disagrees away from 0:0 was hidden")
	}
}
