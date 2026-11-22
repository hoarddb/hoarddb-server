package harness_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
)

// The two-slot cycle of the review of the deferred-pair gate (finding F1, 2026-11-16), built
// from the reviewer's probe. Section 5 of the deferred pair gate spec says waits
// through another pair end; the review constructed one that, through a membership change,
// closes into a cycle the local declaration read does not break:
//
//   - bucket One declares in slot T, which moves to the joiner; the joiner pulled T before One
//     existed. A document of One lives in slot S, which does not move;
//   - the stores are copied; One's document then gets pushes (in S), bucket Two is created with
//     its declaration in S (above S's copy), and a document of Two in T gets pushes;
//   - the stores go back to their copies with the logs kept, the old nodes restart with the
//     replays held, the joiner is activated (T moves to it), the replays are let go.
//
// By reading: S's copies need One's declaration from T's new owner, the joiner, which lacks it;
// the joiner's handoff of T needs T's old copies, which the gate makes answer an error; T's old
// copies need Two's declaration from S's owner, whose store is below it and whose replay is
// stuck before it. Neither declaration is in the pair that needs it, so the local read does not
// apply. The run was first seen to stand still at the review's tip, before BUG-0834's promotion
// rule (a new owner also asks the holders that left the copy set) was merged.

// cycleRun is one run of the construction, with what was acknowledged on each side.
type cycleRun struct {
	t      *testing.T
	c      *harness.Cluster
	db     uint64
	old    []string
	joiner string

	one, two         string // the bucket names
	docS, docT       string // One's document (in S) and Two's document (in T)
	slotS, slotT     uint16
	copiesS, copiesT []string // the old copy sets, before the activation
	tailS, tailT     cluster.Position
	lowTargetT       *cluster.Position
}

func (r *cycleRun) create(name string) uint64 {
	r.t.Helper()
	var id uint64
	h1Retry(r.t, r.c, "creating "+name, func(ctx context.Context) error {
		var err error
		id, err = r.c.Node(r.c.AwaitLeader(30*time.Second)).CreateBucket(ctx, h1DB, name, wire.Field{Name: "Name", Type: "string"}, wire.Field{Name: "Queue", Type: "fifoStore"})
		return err
	})
	return id
}

func (r *cycleRun) place(bucket string, want func(uint16) bool) (string, uint16) {
	r.t.Helper()
	for i := 0; i < 4_000_000; i++ {
		id := fmt.Sprintf("%s-d%d", bucket, i)
		slot, err := wire.PlacementOf(h1DB, bucket, id)
		if err != nil {
			r.t.Fatal(err)
		}
		if want(slot) {
			return id, slot
		}
	}
	r.t.Fatalf("no document id of %s has the slot the cycle needs", bucket)
	return "", 0
}

func (r *cycleRun) insert(bucket, id string) {
	r.t.Helper()
	h1Retry(r.t, r.c, "inserting "+id, func(ctx context.Context) error {
		_, err := r.c.Node(r.old[0]).Insert(ctx, h1DB, bucket, bson.D{{Key: "_id_", Value: id}, {Key: "Name", Value: id}})
		return err
	})
}

func (r *cycleRun) push(bucket, id, item string) {
	r.t.Helper()
	h1Retry(r.t, r.c, "pushing "+item, func(ctx context.Context) error {
		return r.c.Node(r.old[0]).Push(ctx, wire.PushRequest{StoreTarget: wire.StoreTarget{Database: h1DB, Bucket: bucket, ID: id, Field: "Queue"}, Element: bson.D{{Key: "item", Value: item}}})
	})
}

// served reads a document's queue through an old node and returns which acknowledged items it holds.
func (r *cycleRun) served(bucket, id, prefix string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	elements, err := r.c.Node(r.old[0]).Peek(ctx, wire.PeekRequest{StoreTarget: wire.StoreTarget{Database: h1DB, Bucket: bucket, ID: id, Field: "Queue"}, Count: 100})
	if err != nil {
		return nil, err
	}
	var items []string
	for _, element := range elements {
		for i := 1; i <= h1PerPhase; i++ {
			item := fmt.Sprintf("%s-%d", prefix, i)
			if bytes.Contains(element, []byte(item+"\x00")) {
				items = append(items, item)
			}
		}
	}
	return items, nil
}

// cycleScenario builds the construction up to the activation, with the replays still held, and
// returns what lets them go.
func cycleScenario(t *testing.T, gated bool) (r *cycleRun, release func()) {
	t.Helper()
	c := harness.NewCluster(t, 3, harness.Options{NoDetection: true, WCTimeout: 5 * time.Second, Late: 1, NoDeferredGate: !gated})
	r = &cycleRun{t: t, c: c, old: c.IDs()}
	h1Retry(t, c, "creating the database", func(ctx context.Context) error {
		var err error
		r.db, err = c.Node(c.AwaitLeader(30*time.Second)).CreateDatabaseRF(ctx, h1DB, h1RF)
		return err
	})
	c.AwaitConverged(30 * time.Second)
	r.joiner = c.AddNode()
	c.Await("the joiner to have pulled every pair it will hold", 5*time.Minute, func() bool {
		ctx, cancel := h1Ctx()
		defer cancel()
		return c.Node(c.AwaitLeader(30*time.Second)).CheckSeeded(ctx, r.joiner) == nil
	})
	before := c.AwaitConverged(30 * time.Second)
	after := before
	after.Members = slices.Clone(before.Members)
	for i := range after.Members {
		if after.Members[i].ID == r.joiner {
			after.Members[i].State = cluster.MemberActive
		}
	}
	tableBefore, tableAfter := cluster.BuildTable(before), cluster.BuildTable(after)
	ownerOf := func(table *cluster.SlotTable, slot uint16) string {
		owner, _ := table.Owner(slot, h1RF)
		return owner.ID
	}
	copiesOf := func(table *cluster.SlotTable, slot uint16) []string {
		var ids []string
		for _, member := range table.Copies(slot, h1RF) {
			ids = append(ids, member.ID)
		}
		return ids
	}
	moves := func(slot uint16) bool { return ownerOf(tableAfter, slot) == r.joiner }
	stable := func(slot uint16) bool {
		return ownerOf(tableAfter, slot) == ownerOf(tableBefore, slot) && !slices.Contains(copiesOf(tableAfter, slot), r.joiner)
	}

	// One: its declaration in a slot T that moves, its id-to-name document in a stable slot.
	for i := 0; i < 2000 && r.one == ""; i++ {
		name := fmt.Sprintf("One%d", i)
		decl := h1DeclSlot(t, name)
		if !moves(decl) {
			continue
		}
		if reverse := h1ReverseSlot(t, r.create(name)); stable(reverse) && reverse != decl {
			r.one, r.slotT = name, decl
		}
	}
	if r.one == "" {
		t.Fatal("no bucket name One<n> declares in a slot that moves with its id-to-name document in one that stays")
	}
	r.docS, r.slotS = r.place(r.one, func(slot uint16) bool { return slot != r.slotT && stable(slot) })
	r.copiesS, r.copiesT = copiesOf(tableBefore, r.slotS), copiesOf(tableBefore, r.slotT)
	t.Logf("One=%s declares in slot T=%d (owner %s -> %s, old copies %v); its document %s is in slot S=%d (owner %s, copies %v)",
		r.one, r.slotT, ownerOf(tableBefore, r.slotT), ownerOf(tableAfter, r.slotT), r.copiesT, r.docS, r.slotS, ownerOf(tableBefore, r.slotS), r.copiesS)
	r.insert(r.one, r.docS)

	// The store copies, then everything the cycle needs above them.
	for _, id := range r.old {
		c.Kill(id)
	}
	for _, id := range r.old {
		c.CopyStore(id)
	}
	for _, id := range r.old {
		c.RestartUnsynced(id)
	}
	c.AwaitLeader(60 * time.Second)
	c.AwaitConverged(60 * time.Second)
	c.AwaitLeases(60 * time.Second)
	for i := 1; i <= h1PerPhase; i++ {
		r.push(r.one, r.docS, fmt.Sprintf("one-%d", i))
	}
	// Two: its declaration in S (above S's pushes), its id-to-name document in a stable third slot.
	for i := 0; i < 1_000_000 && r.two == ""; i++ {
		name := fmt.Sprintf("Two%d", i)
		if h1DeclSlot(t, name) != r.slotS {
			continue
		}
		if reverse := h1ReverseSlot(t, r.create(name)); stable(reverse) && reverse != r.slotS && reverse != r.slotT {
			r.two = name
		}
	}
	if r.two == "" {
		t.Fatalf("no bucket name Two<n> declares in slot S=%d with its id-to-name document in a third stable slot", r.slotS)
	}
	r.docT, _ = r.place(r.two, func(slot uint16) bool { return slot == r.slotT })
	r.insert(r.two, r.docT)
	for i := 1; i <= h1PerPhase; i++ {
		r.push(r.two, r.docT, fmt.Sprintf("two-%d", i))
	}
	t.Logf("Two=%s declares in slot S=%d; its document %s is in slot T=%d", r.two, r.slotS, r.docT, r.slotT)
	// Read at a copy of each pair: a node outside the copy set answers 0:0.
	h1Retry(t, c, "reading S's position", func(ctx context.Context) error {
		var err error
		r.tailS, err = c.Node(r.copiesS[0]).PairPosition(ctx, h1DB, r.one, r.docS)
		return err
	})
	h1Retry(t, c, "reading T's position", func(ctx context.Context) error {
		var err error
		r.tailT, err = c.Node(r.copiesT[0]).PairPosition(ctx, h1DB, r.two, r.docT)
		return err
	})

	// The cut's effect by hand, then the restart cut off from each other, as H1 does.
	for _, id := range r.old {
		c.Kill(id)
	}
	for _, id := range r.old {
		c.RestoreStore(id)
	}
	release = c.HoldBootReplays()
	var each [][]string
	for _, id := range c.IDs() {
		each = append(each, []string{id})
	}
	c.Partition(each...)
	for _, id := range r.old {
		c.RestartUnsynced(id)
	}
	c.Heal()
	c.AwaitLeader(60 * time.Second)
	c.AwaitConverged(60 * time.Second)
	c.AwaitLeases(60 * time.Second)
	for _, side := range []struct {
		name   string
		slot   uint16
		copies []string
		tail   cluster.Position
	}{{"S", r.slotS, r.copiesS, r.tailS}, {"T", r.slotT, r.copiesT, r.tailT}} {
		for _, id := range side.copies {
			if !c.Node(id).BootReplayPending(side.slot, r.db) {
				t.Fatalf("SCENARIO NOT REACHED: slot %s=%d is not deferred on %s after the restart", side.name, side.slot, id)
			}
			position, _, err := c.Node(id).PairDump(side.slot, r.db)
			if err != nil || !position.Less(side.tail) {
				t.Fatalf("SCENARIO NOT REACHED: slot %s=%d on %s is at %s (%v), want below the acknowledged %s", side.name, side.slot, id, position, err, side.tail)
			}
			t.Logf("slot %s=%d on %s: deferred at %s, its log holds acknowledged pushes up to %s", side.name, side.slot, id, position, side.tail)
		}
	}
	c.ActivateWhileSeedingMoves(r.joiner, 90*time.Second, 240*time.Second)
	c.AwaitConverged(60 * time.Second)
	for _, id := range c.Running() {
		if owner, _ := c.Node(id).OwnerOfSlot(r.slotT, r.db); owner != r.joiner {
			t.Fatalf("SCENARIO NOT REACHED: %s says slot T=%d is owned by %q after the activation, want %s", id, r.slotT, owner, r.joiner)
		}
	}
	t.Logf("scenario reached: S and T are deferred on every old copy and %s now owns T", r.joiner)
	return r, release
}

// watch lets the replays go and samples once a second until both sides serve every acknowledged
// push or observe has passed. It reports whether both served.
func (r *cycleRun) watch(release func(), observe time.Duration) bool {
	began := time.Now()
	release()
	seen := map[string]string{}
	note := func(key, fact string) {
		if seen[key] != fact {
			seen[key] = fact
			r.t.Logf("[+%5.1fs] %s: %s", time.Since(began).Seconds(), key, fact)
		}
	}
	wantS := []string{"one-1", "one-2", "one-3"}
	wantT := []string{"two-1", "two-2", "two-3"}
	for {
		if target, known := r.c.Node(r.joiner).HandoffTarget(r.slotT, r.db); known {
			note("T's target on "+r.joiner, fmt.Sprintf("%s (acknowledged up to %s)", target, r.tailT))
			if target.Less(r.tailT) && r.lowTargetT == nil {
				r.lowTargetT = &target
			}
		} else {
			note("T's target on "+r.joiner, "none published")
		}
		for _, side := range []struct {
			name   string
			slot   uint16
			copies []string
		}{{"S", r.slotS, r.copiesS}, {"T", r.slotT, r.copiesT}} {
			var pending []string
			for _, id := range side.copies {
				if r.c.Node(id).BootReplayPending(side.slot, r.db) {
					pending = append(pending, id)
				}
			}
			note(side.name+" replay pending on", fmt.Sprint(pending))
		}
		itemsS, errS := r.served(r.one, r.docS, "one")
		itemsT, errT := r.served(r.two, r.docT, "two")
		note("S serves", fmt.Sprintf("%v (%v)", itemsS, errS))
		note("T serves", fmt.Sprintf("%v (%v)", itemsT, errT))
		both := errS == nil && errT == nil && slices.Equal(itemsS, wantS) && slices.Equal(itemsT, wantT)
		if both || time.Since(began) > observe {
			r.report()
			return both
		}
		time.Sleep(time.Second)
	}
}

// report logs where each node stands and the background reports, the gate's refusals counted
// apart, so the log says who waited for whom.
func (r *cycleRun) report() {
	for _, id := range r.c.Running() {
		positionS, _, _ := r.c.Node(id).PairDump(r.slotS, r.db)
		positionT, _, _ := r.c.Node(id).PairDump(r.slotT, r.db)
		r.t.Logf("%s: S deferred=%v at %s (acknowledged %s); T deferred=%v at %s (acknowledged %s)", id,
			r.c.Node(id).BootReplayPending(r.slotS, r.db), positionS, r.tailS, r.c.Node(id).BootReplayPending(r.slotT, r.db), positionT, r.tailT)
	}
	reports, refusals := map[string]int{}, 0
	for _, text := range r.c.BackgroundErrors() {
		if strings.Contains(text, "is replaying its log after a restart") {
			refusals++
		}
		if len(text) > 400 {
			text = text[:400] + "..."
		}
		reports[text]++
	}
	r.t.Logf("background reports that quote the gate's refusal (is replaying its log after a restart): %d", refusals)
	for text, count := range reports {
		r.t.Logf("background report, %d times: %s", count, text)
	}
}

// TestATwoSlotCycleThroughAMembershipChangeEnds is the cycle with the gate: it passes when S and
// T are released and serve every acknowledged push within the window (the spec's H1 window,
// 120 s by default), and fails with CYCLE STANDSTILL otherwise. A failure is the review's F1
// standing on this build, a finding for the owner, not a flake.
func TestATwoSlotCycleThroughAMembershipChangeEnds(t *testing.T) {
	if os.Getenv("HOARDDB_HARNESS_H1") == "" {
		t.Skip("the deferred-pair gate's two-slot cycle (3 to 8 minutes); set HOARDDB_HARNESS_H1=1")
	}
	observe := h1Observe(t)
	r, release := cycleScenario(t, true)
	if !r.watch(release, observe) {
		t.Errorf("CYCLE STANDSTILL: %s after the release, S=%d or T=%d does not serve every acknowledged push", observe, r.slotS, r.slotT)
	}
	if r.lowTargetT != nil {
		t.Errorf("the joiner's handoff published the target %s for T, below the acknowledged %s", *r.lowTargetT, r.tailT)
	}
}

// TestWithoutTheGateTheTwoSlotCycle is the same construction with the gate switched off, a
// contrast and not a pass criterion: it logs whether both sides serve and what target the
// joiner published for T, and fails only when the scenario is not reached.
func TestWithoutTheGateTheTwoSlotCycle(t *testing.T) {
	if os.Getenv("HOARDDB_HARNESS_H1") == "" {
		t.Skip("the deferred-pair gate's two-slot cycle (3 to 8 minutes); set HOARDDB_HARNESS_H1=1")
	}
	r, release := cycleScenario(t, false)
	both := r.watch(release, h1Observe(t))
	t.Logf("WITHOUT THE GATE: both sides serve every acknowledged push: %v; T's first target below the acknowledged %s: %v", both, r.tailT, r.lowTargetT)
}
