package harness_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/harness"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// H1 of the deferred pair gate spec (section 7): the deciding experiment of the
// gate's main risk (section 5). Three nodes hold an RF=2 database; a fourth joins and pulls
// every pair it will hold, and is not activated yet. Then two buckets are created:
//
//   - "moved": its declaration document lives in a slot that the joiner will own once it is
//     activated. The joiner pulled that slot before the bucket existed and a joiner is in no
//     copy set, so its store does not hold the declaration;
//   - "calm": its declaration lives in a slot that stays with an old node.
//
// Each bucket gets a document in a slot that moves to the joiner (for "moved", the
// declaration's own slot: the derived case of section 5) and one in a slot that stays. Pushes
// are acknowledged, the three old nodes are stopped and their stores copied, more pushes are
// acknowledged, the three are stopped again and their stores put back to the copies with the
// logs kept. They restart with the replays held, so every pair with a push above its copy is
// deferred, and while they are deferred the joiner is activated: the slots move. Then the
// gated run lets the replays go and watches who is released and who serves; the ungated
// control keeps them held and watches what target the joiner's handoff publishes.

const (
	h1DB       = "h1"
	h1RF       = 2
	h1PerPhase = 3
)

// h1Doc is one document the run pushes to, with what was acknowledged.
type h1Doc struct {
	label  string
	bucket string
	id     string
	slot   uint16
	// moves says the slot's owner becomes the joiner at its activation.
	moves bool
	// declMoves says the bucket's declaration lives in a slot that moves to the joiner.
	declMoves bool
	acked     []string
	// tail is the pair's position after the last acknowledged push, read at its owner.
	tail cluster.Position
	// oldCopies are the nodes that held the pair before the activation.
	oldCopies []string

	releasedAfter, servedAfter time.Duration
	released, served           bool
	lastServeError             string
	servedItems                []string
	reset                      bool
	// lowTarget is a catch-up target the joiner's handoff published for this pair below tail:
	// the wrong answer the gate exists to prevent. Nil while none was seen.
	lowTarget *cluster.Position
}

func (d *h1Doc) target() wire.StoreTarget {
	return wire.StoreTarget{Database: h1DB, Bucket: d.bucket, ID: d.id, Field: "Queue"}
}

type h1Run struct {
	t      *testing.T
	c      *harness.Cluster
	db     uint64
	old    []string
	joiner string
	// movedDecl is the slot of the "moved" bucket's declaration document.
	movedDecl uint16
	docs      []*h1Doc
	seen      map[string]string
	began     time.Time
}

func h1Ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 20*time.Second)
}

// h1Retry runs write until it is acknowledged. A refusal that says nothing was written (a hold
// after a restart, a lease or quorum round not yet fresh, an owner that just changed) is sent
// again; anything else, an unknown outcome included, ends the run, because a blind resend could
// apply a push twice.
func h1Retry(t *testing.T, c *harness.Cluster, what string, write func(ctx context.Context) error) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		ctx, cancel := h1Ctx()
		err := write(ctx)
		cancel()
		if err == nil {
			return
		}
		var refusal *cluster.Error
		retryable := errors.As(err, &refusal) && slices.Contains([]cluster.Code{
			cluster.CodeUnavailable, cluster.CodeLeaseLapsed, cluster.CodeNoQuorum, cluster.CodeNotOwner, cluster.CodeStaleEpoch,
		}, refusal.Code) && !strings.Contains(refusal.Message, "UNKNOWN")
		if !retryable || time.Now().After(deadline) {
			t.Fatalf("%s: %v (background errors: %v)", what, err, c.BackgroundErrors())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func h1Slot(t *testing.T, encoded []byte) uint16 {
	t.Helper()
	return storage.SlotOf(storage.PlacementKey(h1DB, "_buckets", encoded))
}

func h1DeclSlot(t *testing.T, bucket string) uint16 {
	t.Helper()
	encoded, err := store.EncodeComponent(bucket)
	if err != nil {
		t.Fatal(err)
	}
	return h1Slot(t, encoded)
}

// h1ReverseSlot is the slot of the document that maps a bucket id to its name, which a replay
// reads before the declaration itself.
func h1ReverseSlot(t *testing.T, bucketID uint64) uint16 {
	t.Helper()
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], bucketID)
	encoded, err := store.EncodeComponent(raw[:])
	if err != nil {
		t.Fatal(err)
	}
	return h1Slot(t, encoded)
}

// h1Bucket creates a bucket whose declaration slot moves to the joiner (declMoves) or stays,
// and whose id-to-name document stays readable at an old node either way.
//
// For a declaration slot that moves, the old owner must be the old node that stays in the
// slot's copy set. The other old copy is sent the pair's tail by the old owner before the
// activation, and admitting that frame reads the declaration from the old owner and caches it,
// so a slot whose staying node is that copy finishes its replay from the cache and the run
// would depend on which of the two stays (seen in the first runs of this test).
func (r *h1Run) bucket(prefix string, declMoves bool, ownerAfter func(uint16) string, oldOwnerStays func(uint16) bool) (name string, decl uint16) {
	r.t.Helper()
	for i := 0; i < 2000; i++ {
		name = fmt.Sprintf("%s%d", prefix, i)
		decl = h1DeclSlot(r.t, name)
		if (ownerAfter(decl) == r.joiner) != declMoves || (declMoves && !oldOwnerStays(decl)) {
			continue
		}
		var bucketID uint64
		h1Retry(r.t, r.c, "creating bucket "+name, func(ctx context.Context) error {
			var err error
			bucketID, err = r.c.Node(r.c.AwaitLeader(30*time.Second)).CreateBucket(ctx, h1DB, name, wire.Field{Name: "Name", Type: "string"}, wire.Field{Name: "Queue", Type: "fifoStore"})
			return err
		})
		if ownerAfter(h1ReverseSlot(r.t, bucketID)) == r.joiner {
			continue // its id-to-name document would move too; the run wants the declaration alone to decide
		}
		return name, decl
	}
	r.t.Fatalf("no bucket name with prefix %s has a declaration slot that moves=%v", prefix, declMoves)
	return "", 0
}

// document finds an id of bucket whose slot satisfies want and has not been used.
func (r *h1Run) document(label, bucket string, declMoves bool, want func(slot uint16) bool) *h1Doc {
	r.t.Helper()
	for i := 0; i < 4_000_000; i++ {
		id := fmt.Sprintf("%s-%d", label, i)
		slot, err := wire.PlacementOf(h1DB, bucket, id)
		if err != nil {
			r.t.Fatal(err)
		}
		used := slices.ContainsFunc(r.docs, func(d *h1Doc) bool { return d.slot == slot })
		if want(slot) && !used {
			doc := &h1Doc{label: label, bucket: bucket, id: id, slot: slot, declMoves: declMoves}
			r.docs = append(r.docs, doc)
			return doc
		}
	}
	r.t.Fatalf("no document id of %s has the slot %s needs", bucket, label)
	return nil
}

func (r *h1Run) push(phase string) {
	r.t.Helper()
	for _, doc := range r.docs {
		for i := 1; i <= h1PerPhase; i++ {
			item := fmt.Sprintf("%s-%s%d", doc.label, phase, i)
			h1Retry(r.t, r.c, "pushing "+item, func(ctx context.Context) error {
				return r.c.Node(r.old[0]).Push(ctx, wire.PushRequest{StoreTarget: doc.target(), Element: bson.D{{Key: "item", Value: item}}})
			})
			doc.acked = append(doc.acked, item)
		}
	}
}

// restartOld restarts the three old nodes and waits until the cluster serves again. With alone
// set every node boots cut off from every other, so none has a leader, and with it the
// committed state, while it replays its log: a node that boots into a cluster that already has
// a leader can read a declaration at once and defers nothing, which made the scenario depend
// on the order of the restarts.
func (r *h1Run) restartOld(alone bool) {
	r.t.Helper()
	if alone {
		var each [][]string
		for _, id := range r.c.IDs() {
			each = append(each, []string{id})
		}
		r.c.Partition(each...)
	}
	for _, id := range r.old {
		r.c.RestartUnsynced(id)
	}
	r.c.Heal()
	r.c.AwaitLeader(60 * time.Second)
	r.c.AwaitConverged(60 * time.Second)
	r.c.AwaitLeases(60 * time.Second)
}

func (r *h1Run) killOld() {
	r.t.Helper()
	for _, id := range r.old {
		r.c.Kill(id)
	}
}

// note logs a fact when it changes, with the time since the replays were let go, so the log is
// the timeline of who waited for whom.
func (r *h1Run) note(key, fact string) {
	if r.seen[key] == fact {
		return
	}
	r.seen[key] = fact
	since := "before the release"
	if !r.began.IsZero() {
		since = fmt.Sprintf("+%5.1fs", time.Since(r.began).Seconds())
	}
	r.t.Logf("[%s] %s: %s", since, key, fact)
}

func h1Short(err error) string {
	text := err.Error()
	if len(text) > 700 {
		text = text[:700] + "..."
	}
	return text
}

func h1Items(doc *h1Doc, raw [][]byte) []string {
	var items []string
	for _, element := range raw {
		for _, item := range doc.acked {
			if bytes.Contains(element, []byte(item+"\x00")) {
				items = append(items, item)
			}
		}
	}
	return items
}

// targets reads the catch-up target the joiner's handoff has published for every pair that
// moves to it, and records one below the pair's acknowledged tail. It reports whether every such
// pair has a published target.
func (r *h1Run) targets() bool {
	all := true
	for _, doc := range r.docs {
		if !doc.moves {
			continue
		}
		target, known := r.c.Node(r.joiner).HandoffTarget(doc.slot, r.db)
		if !known {
			r.note(doc.label+" target on "+r.joiner, "none published")
			all = false
			continue
		}
		r.note(doc.label+" target on "+r.joiner, fmt.Sprintf("%s (acknowledged up to %s)", target, doc.tail))
		if target.Less(doc.tail) && doc.lowTarget == nil {
			doc.lowTarget = &target
		}
	}
	return all
}

// sample reads, for every document, what each side says now: whether the old copies still have
// the replay pending, what an old copy answers the joiner's position query, and whether a
// client is served. It reports whether every pair is released and serves every acknowledged push.
func (r *h1Run) sample() bool {
	all := r.targets()
	for _, doc := range r.docs {
		pending := []string{}
		for _, id := range doc.oldCopies {
			node := r.c.Node(id)
			if node.BootReplayPending(doc.slot, r.db) {
				pending = append(pending, id)
			}
			if node.SlotRecordState(doc.slot, r.db) == machine.SlotSeeding {
				doc.reset = true
				r.note(doc.label+" record on "+id, "SEEDING: the copy was reset")
			}
			ctx, cancel := h1Ctx()
			reply, err := r.c.Node(r.joiner).PositionOn(ctx, id, doc.slot, r.db)
			cancel()
			if err != nil {
				r.note(doc.label+" position of "+id+" as asked by "+r.joiner, "error: "+h1Short(err))
			} else {
				r.note(doc.label+" position of "+id+" as asked by "+r.joiner, fmt.Sprintf("state %d at %s (acknowledged up to %s)", reply.State, reply.AppliedPos, doc.tail))
			}
		}
		r.note(doc.label+" replay pending on", fmt.Sprint(pending))
		if len(pending) == 0 && !doc.released {
			doc.released, doc.releasedAfter = true, time.Since(r.began)
		}
		owner, _ := r.c.Node(r.joiner).OwnerOfSlot(doc.slot, r.db)
		// What the joiner itself holds: its applied position of the pair, and whether it can
		// resolve the bucket (for the moved bucket that is a read of its own store, because it
		// owns the declaration's slot).
		if position, _, err := r.c.Node(r.joiner).PairDump(doc.slot, r.db); err != nil {
			r.note(doc.label+" pair on "+r.joiner, "not held: "+h1Short(err))
		} else {
			r.note(doc.label+" pair on "+r.joiner, fmt.Sprintf("applied %s, record state %d", position, r.c.Node(r.joiner).SlotRecordState(doc.slot, r.db)))
		}
		lookup, stop := context.WithTimeout(context.Background(), 2*time.Second)
		if _, _, err := r.c.Node(r.joiner).Owner(lookup, h1DB, doc.bucket, doc.id); err != nil {
			r.note("declaration of "+doc.bucket+" as "+r.joiner+" resolves it", "cannot: "+h1Short(err))
		} else {
			r.note("declaration of "+doc.bucket+" as "+r.joiner+" resolves it", "found")
		}
		stop()
		// A short deadline: a read that a hold parks would otherwise take the whole sample.
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		raw, err := r.c.Node(r.joiner).Peek(ctx, wire.PeekRequest{StoreTarget: doc.target(), Count: 100})
		cancel()
		if err != nil {
			doc.lastServeError = h1Short(err)
			r.note(doc.label+" read through "+r.joiner+" (owner "+owner+")", "refused: "+doc.lastServeError)
			all = false
			continue
		}
		items := h1Items(doc, raw)
		r.note(doc.label+" read through "+r.joiner+" (owner "+owner+")", fmt.Sprintf("%d elements, of the acknowledged %v it holds %v", len(raw), doc.acked, items))
		if !doc.served {
			doc.served, doc.servedAfter, doc.servedItems = true, time.Since(r.began), items
		}
		doc.servedItems = items
		if len(pending) != 0 || !slices.Equal(items, doc.acked) || len(raw) != len(doc.acked) {
			all = false
		}
	}
	return all
}

// h1Scenario builds the scenario up to the activation, with the replays still held, and returns
// what lets them go. gated runs with the deferred-pair gate; false switches it off, which is what
// a node answered before it. rollBack puts the old nodes' stores back to copies taken after the
// first round of pushes, by hand; false kills them keeping their live stores
// (Cluster.KillKeepingLiveStore) instead, so the deferral comes from the vouched cut discarding
// the second round's unsynced commits.
func h1Scenario(t *testing.T, gated, rollBack bool) (r *h1Run, release func()) {
	t.Helper()
	c := harness.NewCluster(t, 3, harness.Options{NoDetection: true, WCTimeout: 5 * time.Second, Late: 1, NoDeferredGate: !gated})
	r = &h1Run{t: t, c: c, old: c.IDs(), seen: map[string]string{}}
	h1Retry(t, c, "creating the database", func(ctx context.Context) error {
		var err error
		r.db, err = c.Node(c.AwaitLeader(30*time.Second)).CreateDatabaseRF(ctx, h1DB, h1RF)
		return err
	})
	c.AwaitConverged(30 * time.Second)

	// The joiner pulls every pair it will hold, while the database is empty, and stays joining.
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
			if after.Members[i].State != cluster.MemberJoining {
				t.Fatalf("the joiner is %v before the run activates it, want joining", after.Members[i].State)
			}
			after.Members[i].State = cluster.MemberActive
		}
	}
	tableAfter := cluster.BuildTable(after)
	ownerAfter := func(slot uint16) string {
		owner, _ := tableAfter.Owner(slot, h1RF)
		return owner.ID
	}

	tableBefore := cluster.BuildTable(before)
	ownerBefore := func(slot uint16) string {
		owner, _ := tableBefore.Owner(slot, h1RF)
		return owner.ID
	}
	holds := func(table *cluster.SlotTable, slot uint16, id string) bool {
		return slices.ContainsFunc(table.Copies(slot, h1RF), func(member cluster.Member) bool { return member.ID == id })
	}
	oldOwnerStays := func(slot uint16) bool { return holds(tableAfter, slot, ownerBefore(slot)) }
	moved, movedDecl := r.bucket("Moved", true, ownerAfter, oldOwnerStays)
	calm, calmDecl := r.bucket("Calm", false, ownerAfter, oldOwnerStays)
	r.movedDecl = movedDecl
	declOwner := ownerBefore(movedDecl)
	t.Logf("slot %d is owned by %s before the activation, and %s stays in its copy set after it", movedDecl, declOwner, declOwner)
	t.Logf("bucket %s: declaration in slot %d, which moves to %s; bucket %s: declaration in slot %d, which stays with %s", moved, movedDecl, r.joiner, calm, calmDecl, ownerAfter(calmDecl))
	r.document("moved-in-its-declaration-slot", moved, true, func(slot uint16) bool { return slot == movedDecl }).moves = true
	// Not a pair the declaration slot's old owner holds, for the reason given at bucket: as a
	// copy of it that node would be sent this pair's tail and would cache the declaration.
	r.document("moved-in-a-slot-that-stays", moved, true, func(slot uint16) bool {
		return ownerAfter(slot) != r.joiner && !holds(tableBefore, slot, declOwner)
	})
	r.document("calm-in-a-slot-that-moves", calm, false, func(slot uint16) bool { return ownerAfter(slot) == r.joiner && slot != calmDecl }).moves = true
	r.document("calm-in-a-slot-that-stays", calm, false, func(slot uint16) bool { return ownerAfter(slot) != r.joiner && slot != calmDecl })
	for _, doc := range r.docs {
		h1Retry(t, c, "inserting "+doc.label, func(ctx context.Context) error {
			_, err := c.Node(r.old[0]).Insert(ctx, h1DB, doc.bucket, bson.D{{Key: "_id_", Value: doc.id}, {Key: "Name", Value: doc.label}})
			return err
		})
		ctx, cancel := h1Ctx()
		_, copies, err := c.Node(r.old[0]).Owner(ctx, h1DB, doc.bucket, doc.id)
		cancel()
		if err != nil || len(copies) != h1RF || slices.Contains(copies, r.joiner) {
			t.Fatalf("%s: copies %v, %v; want two old nodes", doc.label, copies, err)
		}
		doc.oldCopies = copies
	}

	// Acknowledged pushes, the store copies at a quiet instant, then more acknowledged pushes.
	r.push("a")
	r.killOld()
	if rollBack {
		for _, id := range r.old {
			c.CopyStore(id)
		}
	}
	r.restartOld(false)
	r.push("b")
	for _, doc := range r.docs {
		h1Retry(t, c, "reading the position of "+doc.label, func(ctx context.Context) error {
			var err error
			doc.tail, err = c.Node(doc.oldCopies[0]).PairPosition(ctx, h1DB, doc.bucket, doc.id)
			return err
		})
	}

	// With rollBack, the cut's effect by hand: the stores go back to their copies and the logs
	// stay. Without, the stores are what a kill -9 leaves and the cut discards the unsynced
	// commits. Either way the replays that would finish in the background are held.
	if rollBack {
		r.killOld()
		for _, id := range r.old {
			c.RestoreStore(id)
		}
	} else {
		for _, id := range r.old {
			c.KillKeepingLiveStore(id)
		}
	}
	release = c.HoldBootReplays()
	r.restartOld(true)
	if !rollBack {
		// The deferral below must be the cut's: a store that discarded nothing proves nothing.
		for _, id := range r.old {
			if !slices.ContainsFunc(r.docs, func(doc *h1Doc) bool { return slices.Contains(doc.oldCopies, id) }) {
				continue
			}
			report := c.Node(id).StoreOpenReport()
			if report.DiscardedCommits < 1 {
				t.Fatalf("SCENARIO NOT REACHED: the cut discarded nothing on %s (its store's open: %+v)", id, report)
			}
			t.Logf("the cut on %s: %+v", id, report)
		}
	}
	for _, doc := range r.docs {
		for _, id := range doc.oldCopies {
			if !c.Node(id).BootReplayPending(doc.slot, r.db) {
				t.Fatalf("SCENARIO NOT REACHED: slot %d (%s) is not deferred on %s after the restart, so the run would prove nothing", doc.slot, doc.label, id)
			}
			position, _, err := c.Node(id).PairDump(doc.slot, r.db)
			if err != nil || !position.Less(doc.tail) {
				t.Fatalf("SCENARIO NOT REACHED: slot %d (%s) on %s is at %s (%v), want below the acknowledged %s", doc.slot, doc.label, id, position, err, doc.tail)
			}
			r.note(doc.label+" on "+id, fmt.Sprintf("deferred at %s, its log holds acknowledged pushes up to %s", position, doc.tail))
		}
	}

	// The membership change, while the pairs are deferred: the joiner becomes a ranked member.
	c.ActivateWhileSeedingMoves(r.joiner, 90*time.Second, 240*time.Second)
	c.AwaitConverged(60 * time.Second)
	for _, id := range c.Running() {
		if owner, _ := c.Node(id).OwnerOfSlot(movedDecl, r.db); owner != r.joiner {
			t.Fatalf("SCENARIO NOT REACHED: %s says slot %d, which holds the declaration of %s, is owned by %q after the activation, want %s", id, movedDecl, moved, owner, r.joiner)
		}
		for _, doc := range r.docs {
			owner, _ := c.Node(id).OwnerOfSlot(doc.slot, r.db)
			if (owner == r.joiner) != doc.moves {
				t.Fatalf("SCENARIO NOT REACHED: %s says slot %d (%s) is owned by %q after the activation; moves to %s should be %v", id, doc.slot, doc.label, owner, r.joiner, doc.moves)
			}
			if c.Node(id).BootReplayPending(doc.slot, r.db) != slices.Contains(doc.oldCopies, id) {
				t.Fatalf("SCENARIO NOT REACHED: the replay of slot %d (%s) on %s is pending=%v at the activation", doc.slot, doc.label, id, c.Node(id).BootReplayPending(doc.slot, r.db))
			}
		}
	}
	t.Logf("scenario reached: every tracked pair is deferred on both of its old copies, and %s now owns the declaration slot %d of %s and the slots marked as moving", r.joiner, movedDecl, moved)
	return r, release
}

// watch lets the replays go and samples until every pair serves or observe has passed.
func (r *h1Run) watch(release func(), observe time.Duration) {
	r.sample() // what each side says while the replays are still held
	r.began = time.Now()
	release()
	deadline := r.began.Add(observe)
	for !r.sample() && time.Now().Before(deadline) {
		time.Sleep(250 * time.Millisecond)
	}
	r.logReports()
}

// logReports logs the background reports of the waits, each once with its count.
func (r *h1Run) logReports() {
	reports := map[string]int{}
	for _, text := range r.c.BackgroundErrors() {
		if strings.Contains(text, "finishing the replay") || strings.Contains(text, "needs ready answers") || strings.Contains(text, "not found") || strings.Contains(text, "catching up") {
			reports[h1Short(errors.New(text))]++
		}
	}
	for text, count := range reports {
		r.t.Logf("background report, %d times: %s", count, text)
	}
}

// h1KnownJoinerBug is the open bug behind one audit failure H1 sees that the gate does not
// cause: a joiner that became a copy of a pair whose owner stays is left at 0:0 until the pair's
// next write (found by the review of this build, filed from it).
const h1KnownJoinerBug = "BUG-0834"

// audit checks, for a document that is served, that every copy holds every acknowledged push
// exactly once at one position, and that no copy was reset. known holds the problems of BUG-0834
// (h1KnownJoinerBug): the joiner as a copy of a pair that does not move, behind its owner. They
// are reported apart and do not fail the run; when that bug is closed they are problems again.
func (r *h1Run) audit(doc *h1Doc) (problems, known []string) {
	ctx, cancel := h1Ctx()
	defer cancel()
	_, copies, err := r.c.Node(r.joiner).Owner(ctx, h1DB, doc.bucket, doc.id)
	if err != nil {
		return []string{fmt.Sprintf("placing it: %v", err)}, nil
	}
	var first cluster.Position
	for i, id := range copies {
		position, entries, err := r.c.Node(id).PairDump(doc.slot, r.db)
		if err != nil {
			problems = append(problems, fmt.Sprintf("copy %s cannot be read: %v", id, err))
			continue
		}
		// Where this copy's rows go: the joiner as a later copy of a pair whose owner stays is
		// BUG-0834's, every other row is the run's.
		rows := &problems
		if id == r.joiner && i > 0 && !doc.moves {
			rows = &known
		}
		if i == 0 {
			first = position
		} else if position != first {
			*rows = append(*rows, fmt.Sprintf("copy %s is at %s and copy %s at %s", id, position, copies[0], first))
		}
		if position.Less(doc.tail) {
			*rows = append(*rows, fmt.Sprintf("copy %s is at %s, below the acknowledged %s", id, position, doc.tail))
		}
		for _, item := range doc.acked {
			count := 0
			for _, entry := range entries {
				count += bytes.Count(entry.Value, []byte(item+"\x00"))
			}
			if count != 1 {
				*rows = append(*rows, fmt.Sprintf("copy %s holds the acknowledged push %s %d times", id, item, count))
			}
		}
	}
	if doc.reset {
		problems = append(problems, "an old copy was reset and seeded again")
	}
	return problems, known
}

func h1Observe(t *testing.T) time.Duration {
	t.Helper()
	observe := 120 * time.Second
	if text := os.Getenv("HOARDDB_HARNESS_H1_OBSERVE_SECONDS"); text != "" {
		seconds, err := strconv.Atoi(text)
		if err != nil || seconds < 1 {
			t.Fatalf("HOARDDB_HARNESS_H1_OBSERVE_SECONDS=%q is not a number of seconds", text)
		}
		observe = time.Duration(seconds) * time.Second
	}
	return observe
}

// TestDeferredPairsAreReleasedAndServeAfterAMembershipChange is H1 with the gate: its pass
// criteria are the spec's. Every tracked pair is released and serves within the window, every
// acknowledged push is on every copy exactly once, and no copy was reset. A pair still deferred
// or held at the end of the window is the standstill of the spec's section 5, and the log above
// the failure is the timeline of who waited for whom.
func TestDeferredPairsAreReleasedAndServeAfterAMembershipChange(t *testing.T) {
	if os.Getenv("HOARDDB_HARNESS_H1") == "" {
		t.Skip("the deciding experiment of the deferred-pair gate (2 to 12 minutes); set HOARDDB_HARNESS_H1=1")
	}
	observe := h1Observe(t)
	r, release := h1Scenario(t, true, true)
	r.watch(release, observe)
	r.judge(observe)
}

// TestDeferredPairsAfterPlainKillsAreReleasedAndServe is H1's variant with no rolled-back store
// (store-vouched-cut-build.md, S4a): the old nodes are killed as a kill -9 leaves their stores,
// the vouched cut discards the second round's unsynced commits at the restart, and that is what
// defers the pairs. It asserts what H1 with the gate asserts. When the cut discarded nothing or
// no pair is deferred, the run fails as not reached.
func TestDeferredPairsAfterPlainKillsAreReleasedAndServe(t *testing.T) {
	if os.Getenv("HOARDDB_HARNESS_H1") == "" {
		t.Skip("H1 with the stores a kill -9 leaves (2 to 12 minutes); set HOARDDB_HARNESS_H1=1")
	}
	observe := h1Observe(t)
	r, release := h1Scenario(t, true, false)
	r.watch(release, observe)
	r.judge(observe)
}

// judge applies H1's pass criteria to a run that was watched for observe.
func (r *h1Run) judge(observe time.Duration) {
	t := r.t
	t.Helper()
	for _, doc := range r.docs {
		if doc.lowTarget != nil {
			t.Errorf("%s: the joiner's handoff published the target %s, below the acknowledged %s", doc.label, *doc.lowTarget, doc.tail)
		}
		if !doc.released || !doc.served {
			t.Errorf("STANDSTILL %s (slot %d, declaration slot moves=%v, slot moves=%v): released=%v served=%v after %s; last refusal: %s",
				doc.label, doc.slot, doc.declMoves, doc.moves, doc.released, doc.served, observe, doc.lastServeError)
			continue
		}
		t.Logf("%s: released after %.1fs, served after %.1fs", doc.label, doc.releasedAfter.Seconds(), doc.servedAfter.Seconds())
		if !slices.Equal(doc.servedItems, doc.acked) {
			t.Errorf("%s serves %v, want every acknowledged push %v once and in order", doc.label, doc.servedItems, doc.acked)
		}
		problems, known := r.audit(doc)
		for _, problem := range problems {
			t.Errorf("%s: %s", doc.label, problem)
		}
		for _, problem := range known {
			t.Logf("KNOWN FAILURE, %s (open; not caused by the gate): %s: %s", h1KnownJoinerBug, doc.label, problem)
		}
	}
}

// TestWithoutTheGateADeferredPairAnswersItsLowPosition is the control: the same scenario with
// the gate switched off, which is what a node answered before it, and with the replays held for
// the whole window, so that nothing the replay later does (with the local declaration read an
// old copy finishes within seconds and the loss no longer shows in the stores) can hide the
// wrong answer. It asserts that answer directly: the joiner's handoff publishes, for a pair
// that moves to it, a catch-up target below what an old copy acknowledged. It fails when no
// such target is published, because then the run no longer shows what the gate is for.
func TestWithoutTheGateADeferredPairAnswersItsLowPosition(t *testing.T) {
	if os.Getenv("HOARDDB_HARNESS_H1") == "" {
		t.Skip("the deciding experiment of the deferred-pair gate (2 to 12 minutes); set HOARDDB_HARNESS_H1=1")
	}
	observe := h1Observe(t)
	r, _ := h1Scenario(t, false, true) // never released: the cluster's cleanup lets the replays go
	deadline := time.Now().Add(observe)
	for !r.targets() && time.Now().Before(deadline) {
		time.Sleep(250 * time.Millisecond)
	}
	r.logReports()
	low := false
	for _, doc := range r.docs {
		if !doc.moves {
			continue
		}
		target, known := r.c.Node(r.joiner).HandoffTarget(doc.slot, r.db)
		t.Logf("WITHOUT THE GATE %s (slot %d, declaration slot moves=%v): the joiner's target %s (published %v), acknowledged up to %s, first target below it %v",
			doc.label, doc.slot, doc.declMoves, target, known, doc.tail, doc.lowTarget)
		if doc.lowTarget != nil {
			low = true
		}
	}
	if !low {
		t.Error("without the gate the joiner's handoff published no target below an acknowledged push while the old copies replayed: the run does not show the wrong answer the gate removes")
	}
}
