package main

import (
	"context"
	"errors"
	"github.com/hoarddb/hoarddb-bson/bson"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/bench/social/ringload"
	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// lockedLedger is a deliveryLedger safe for the concurrent pushes of a parallel fan-out.
type lockedLedger struct {
	mutex      sync.Mutex
	sent, done []int
}

func (l *lockedLedger) sending(follower int) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.sent = append(l.sent, follower)
}

func (l *lockedLedger) delivered(follower int) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.done = append(l.done, follower)
}

// fanoutModel is a model holding users first..first+count-1 with empty Feeds.
func fanoutModel(t *testing.T, first, count int) (*ringModel, []int) {
	t.Helper()
	model := newRingModel()
	conn := model.connect()
	followers := make([]int, count)
	for index := range count {
		followers[index] = first + index
		if err := conn.insert(t.Context(), ringload.UserBucket, strconv.Itoa(first+index), ringclient.Doc{"Name": "u"}); err != nil {
			t.Fatal(err)
		}
	}
	return model, followers
}

func feedLength(t *testing.T, model *ringModel, user int) int {
	t.Helper()
	count, err := model.connect().count(t.Context(), userStore(user, ringload.UserFeed))
	if err != nil {
		t.Fatal(err)
	}
	return int(count)
}

// gaugedPush counts the pushes in flight and holds each until release is closed or hold passes.
type gaugedPush struct {
	ringAPI
	inFlight, peak atomic.Int64
	reached        chan struct{} // closed when the peak first reaches target
	target         int64
	release        chan struct{}
	once           sync.Once
}

func (g *gaugedPush) pushWithID(ctx context.Context, store storeRef, element any, id [16]byte) error {
	now := g.inFlight.Add(1)
	defer g.inFlight.Add(-1)
	for {
		peak := g.peak.Load()
		if now <= peak || g.peak.CompareAndSwap(peak, now) {
			break
		}
	}
	if now >= g.target {
		g.once.Do(func() { close(g.reached) })
	}
	select {
	case <-g.release:
	case <-time.After(2 * time.Second):
	}
	return g.ringAPI.pushWithID(ctx, store, element, id)
}

// TestRingFanoutIsBoundedAndOverlaps: with width 4 and 20 followers exactly 4 pushes are in
// flight together (they overlap on one connection) and never more. [Drop the bound: the peak
// is 20; send serially: the peak is 1 and the wait for 4 in flight times out.]
func TestRingFanoutIsBoundedAndOverlaps(t *testing.T) {
	model, followers := fanoutModel(t, 100, 20)
	gauge := &gaugedPush{ringAPI: model.connect(), target: 4, reached: make(chan struct{}), release: make(chan struct{})}
	ops := &ringOps{api: gauge, fanoutWidth: 4}
	ledger := &lockedLedger{}
	done := make(chan error, 1)
	go func() { done <- ops.pushFeeds(t.Context(), followers, bson.D{{Key: "k", Value: "post"}}, ledger) }()
	select {
	case <-gauge.reached:
	case <-time.After(5 * time.Second):
		t.Fatalf("only %d pushes were ever in flight together, want 4", gauge.peak.Load())
	}
	time.Sleep(50 * time.Millisecond) // room for a fifth push to start if the bound were missing
	close(gauge.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if peak := gauge.peak.Load(); peak != 4 {
		t.Errorf("peak pushes in flight %d, want exactly the width 4", peak)
	}
	for _, follower := range followers {
		if got := feedLength(t, model, follower); got != 1 {
			t.Errorf("user %d feed holds %d copies, want 1", follower, got)
		}
	}
	slices.Sort(ledger.sent)
	slices.Sort(ledger.done)
	if !slices.Equal(ledger.sent, followers) || !slices.Equal(ledger.done, followers) {
		t.Errorf("ledger sent %v delivered %v, want every follower once", ledger.sent, ledger.done)
	}
}

// TestRingFanoutKeepsPushesToOneEntityInOrder: a follower named twice receives its copies in
// list order, one after the other, even while others run in parallel.
func TestRingFanoutKeepsPushesToOneEntityInOrder(t *testing.T) {
	model, followers := fanoutModel(t, 100, 8)
	ops := &ringOps{api: model.connect(), fanoutWidth: 8}
	list := append(slices.Clone(followers), 103, 103, 100)
	if err := ops.pushFeeds(t.Context(), list, bson.D{{Key: "k", Value: "x"}}, &lockedLedger{}); err != nil {
		t.Fatal(err)
	}
	if got := feedLength(t, model, 103); got != 3 {
		t.Fatalf("user 103 feed holds %d copies, want 3", got)
	}
	// The single worker for 103 pushes its three copies back to back: no other push to 103 can
	// interleave, and each is sent after the previous one was acknowledged.
	var seen int
	for _, line := range model.requestLog() {
		if line == "push User/103/Feed" {
			seen++
		}
	}
	if seen != 3 {
		t.Errorf("%d pushes to user 103, want 3", seen)
	}
}

// flakyPush fails the first attempt of the listed followers with an unknown outcome (the push
// IS applied before the failure is reported, as for a lost reply), and refuses some outright.
type flakyPush struct {
	ringAPI
	mutex    sync.Mutex
	flaky    map[int]bool
	refused  map[int]bool
	attempts map[int][][16]byte
	// afterRefusal, when set, holds every push that is not refused until a refused push has
	// answered, so a scheduler that runs the other worker first cannot start every copy before
	// the refusal exists to be seen.
	afterRefusal chan struct{}
	refusalOnce  sync.Once
}

func (f *flakyPush) pushWithID(ctx context.Context, store storeRef, element any, id [16]byte) error {
	user, _ := strconv.Atoi(store.id)
	f.mutex.Lock()
	f.attempts[user] = append(f.attempts[user], id)
	first := len(f.attempts[user]) == 1
	f.mutex.Unlock()
	if f.refused[user] {
		if f.afterRefusal != nil {
			defer f.refusalOnce.Do(func() { close(f.afterRefusal) })
		}
		return &ringclient.Error{Code: cluster.CodeNotOwner, Message: "not the owner"}
	}
	if f.afterRefusal != nil {
		select {
		case <-f.afterRefusal:
		case <-ctx.Done():
			return ctx.Err()
		}
		// The refusing worker marks the run stopped a few instructions after its push answers;
		// give it that time before this push counts as one started after the refusal.
		time.Sleep(20 * time.Millisecond)
	}
	err := f.ringAPI.pushWithID(ctx, store, element, id)
	if err == nil && f.flaky[user] && first {
		return &ringclient.Error{Code: cluster.CodeUnavailable, Message: "reply lost"}
	}
	return err
}

// TestRingFanoutRetriesOnlyTheFailedPushUnderItsOwnID: a push whose reply was lost is sent
// again once under the same id and the feed holds one copy; the others are sent once.
// [Retry under a fresh id: the feed holds two copies. Retry everything: attempts are 2 each.]
func TestRingFanoutRetriesOnlyTheFailedPushUnderItsOwnID(t *testing.T) {
	model, followers := fanoutModel(t, 100, 10)
	flaky := &flakyPush{ringAPI: model.connect(), flaky: map[int]bool{102: true, 107: true}, attempts: map[int][][16]byte{}}
	ops := &ringOps{api: flaky, fanoutWidth: 3}
	ledger := &lockedLedger{}
	if err := ops.pushFeeds(t.Context(), followers, bson.D{{Key: "k", Value: "x"}}, ledger); err != nil {
		t.Fatal(err)
	}
	for _, follower := range followers {
		want := 1
		if got := len(flaky.attempts[follower]); (flaky.flaky[follower] && got != 2) || (!flaky.flaky[follower] && got != 1) {
			t.Errorf("user %d was pushed %d times", follower, got)
		}
		if flaky.flaky[follower] && flaky.attempts[follower][0] != flaky.attempts[follower][1] {
			t.Errorf("user %d retry used a different request id", follower)
		}
		if got := feedLength(t, model, follower); got != want {
			t.Errorf("user %d feed holds %d copies, want %d", follower, got, want)
		}
	}
	if len(ledger.sent) != 10 || len(ledger.done) != 10 {
		t.Errorf("ledger sent %d delivered %d, want one of each per follower, retries included once", len(ledger.sent), len(ledger.done))
	}
}

// TestRingFanoutDoesNotRetryARefusalAndStops: a refusal is final, is reported with the
// earliest failed follower, leaves that copy issued and not delivered, and nothing new starts.
func TestRingFanoutDoesNotRetryARefusalAndStops(t *testing.T) {
	model, followers := fanoutModel(t, 100, 30)
	flaky := &flakyPush{ringAPI: model.connect(), refused: map[int]bool{100: true}, attempts: map[int][][16]byte{}, afterRefusal: make(chan struct{})}
	ops := &ringOps{api: flaky, fanoutWidth: 2}
	ledger := &lockedLedger{}
	err := ops.pushFeeds(t.Context(), followers, bson.D{{Key: "k", Value: "x"}}, ledger)
	var refusal *ringclient.Error
	if !errors.As(err, &refusal) || refusal.Code != cluster.CodeNotOwner {
		t.Fatalf("error %v, want the refusal", err)
	}
	if got := len(flaky.attempts[100]); got != 1 {
		t.Errorf("refused push attempted %d times, want 1", got)
	}
	if slices.Contains(ledger.done, 100) || !slices.Contains(ledger.sent, 100) {
		t.Errorf("ledger sent %v delivered %v: the refused copy must be issued and not delivered", ledger.sent, ledger.done)
	}
	if len(ledger.sent) >= len(followers) {
		t.Errorf("%d copies were started after a refusal at the first follower", len(ledger.sent))
	}
}

func TestRingFanoutFlagsAndSettings(t *testing.T) {
	cfg, err := parseFlags([]string{"-engine", "ring", "-mix", "wh"})
	if err != nil || cfg.RingFanoutParallel || cfg.RingFanoutWidth != 8 {
		t.Fatalf("default: parallel %v width %d err %v; want off and 8", cfg.RingFanoutParallel, cfg.RingFanoutWidth, err)
	}
	cfg, err = parseFlags([]string{"-engine", "ring", "-mix", "wh", "-ring-fanout-parallel", "-ring-fanout-width", "4"})
	if err != nil || !cfg.RingFanoutParallel || cfg.RingFanoutWidth != 4 {
		t.Fatalf("on: %+v err %v", cfg, err)
	}
	cfg, err = parseFlags([]string{"-engine", "mongo", "-mix", "wh", "-ring-fanout-parallel"})
	if err != nil || cfg.RingFanoutParallel {
		t.Errorf("a non-ring engine must drop the flag: %v %v", cfg.RingFanoutParallel, err)
	}
}
