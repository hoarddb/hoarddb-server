package wire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// The steps a drop reports to its crash seam, in order (conv 21).
const (
	u42Marker      = "marker"
	u42Reverse     = "reverse"
	u42Bump        = "bump"
	u42Sweep       = "sweep"
	u42Swept       = "swept"
	u42Declaration = "declaration"
	u42Done        = "done"
)

var errU42Crash = errors.New("the process died here")

// u42Bumper counts the bumps a drop commits and can fail them.
type u42Bumper struct {
	node  *Node
	calls atomic.Int64
	fail  func(call int64) error
}

func (b *u42Bumper) BumpSchema(ctx context.Context, dbID uint64, from, to int64, note cluster.BumpNote) error {
	call := b.calls.Add(1)
	if b.fail != nil {
		if err := b.fail(call); err != nil {
			return err
		}
	}
	return b.node.BumpSchema(ctx, dbID, from, to, note)
}

// u42Rig is a node with database app, bucket User (a dozen documents spread over many slots and
// three pushes) and bucket Order (five documents), and a DropBucket over watched collaborators.
type u42Rig struct {
	t        *testing.T
	node     *Node
	dir      string
	dbID     uint64
	bucketID uint64
	sender   *u40Sender
	bumper   *u42Bumper
	drop     *DropBucket
	userDocs []string
	// resume lets a paused background loop run again.
	resume func()
}

func newU42Rig(t *testing.T) *u42Rig {
	t.Helper()
	node, dir := u30iFresh(t)
	ctx := context.Background()
	rig := &u42Rig{t: t, node: node, dir: dir}
	rig.dbID, _ = node.database(u30iDB)
	for i := range 12 {
		id := fmt.Sprintf("user%02d", i)
		rig.userDocs = append(rig.userDocs, id)
		if _, err := node.Insert(ctx, u30iDB, "User", u30iUser(id, "Ada"+id, int32(30+i))); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range rig.userDocs[:3] {
		if err := node.Push(ctx, PushRequest{StoreTarget: StoreTarget{Database: u30iDB, Bucket: "User", ID: id, Field: "Purchases"}, Element: u43Element()}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := node.CreateBucket(ctx, u30iDB, "Order", Field{Name: "Total", Type: "int"}); err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		if _, err := node.Insert(ctx, u30iDB, "Order", bson.D{{Key: "_id_", Value: fmt.Sprintf("o%d", i)}, {Key: "Total", Value: int32(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	ref, err := node.bucket(ctx, u30iDB, "User")
	if err != nil {
		t.Fatal(err)
	}
	rig.bucketID = ref.entry.Decl.BucketID
	rig.install()
	return rig
}

// pause holds the node's background sweep off until the test ends, so a half-finished drop can
// be looked at; Drain still works.
func (r *u42Rig) pause() {
	r.t.Helper()
	loop := r.node.retire
	loop.loopMu.Lock()
	var once sync.Once
	r.resume = func() { once.Do(loop.loopMu.Unlock) }
	r.t.Cleanup(r.resume)
}

// install builds a fresh DropBucket over a watched sender and a counting bumper.
func (r *u42Rig) install() {
	// These tests run the drop to its end themselves; the loop must not free names behind them.
	r.node.retire.setOnSwept(nil)
	r.sender = &u40Sender{inner: nodeSender{node: r.node}}
	r.bumper = &u42Bumper{node: r.node}
	config := r.node.dropper.cfg
	config.Sender, config.Bumper = r.sender, r.bumper
	r.drop = NewDropBucket(config)
}

// crashAt ends the run after the n-th report of step (1 is the first).
func (r *u42Rig) crashAt(step string, n int) {
	seen := 0
	r.drop.crashAfter = func(got string) error {
		if got == step {
			if seen++; seen == n {
				return errU42Crash
			}
		}
		return nil
	}
}

func (r *u42Rig) run() error { return r.drop.Run(context.Background(), r.dbID, "User") }

func (r *u42Rig) mustRun() {
	r.t.Helper()
	if err := r.run(); err != nil {
		r.t.Fatalf("drop: %v", err)
	}
}

// restart closes the node, opens it again on the same directory and rebuilds the rig over it.
func (r *u42Rig) restart() {
	r.t.Helper()
	if err := r.node.Close(); err != nil {
		r.t.Fatal(err)
	}
	r.node = u30iOpen(r.t, r.dir)
	r.install()
}

// keys counts the keys under a bucket's scope in every slot.
func (r *u42Rig) keys(bucketID uint64) int {
	r.t.Helper()
	snap := r.node.kv.NewSnapshot()
	defer snap.Discard()
	it := snap.NewIterator(storage.IterOptions{})
	defer it.Close()
	count := 0
	for it.Seek(nil); it.Valid(); it.Next() {
		key := it.Key()
		if len(key) >= 18 && key[0] != 0xFF && binary.BigEndian.Uint64(key[2:10]) == r.dbID && binary.BigEndian.Uint64(key[10:18]) == bucketID {
			count++
		}
	}
	return count
}

func (r *u42Rig) orderBucket() uint64 {
	r.t.Helper()
	ref, err := r.node.bucket(context.Background(), u30iDB, "Order")
	if err != nil {
		r.t.Fatal(err)
	}
	return ref.entry.Decl.BucketID
}

func (r *u42Rig) declaration() (machine.Declaration, bool) {
	r.t.Helper()
	decl, _, found, err := r.node.fetcher.Read(context.Background(), r.dbID, "User")
	if err != nil {
		r.t.Fatal(err)
	}
	return decl, found
}

func (r *u42Rig) reverseFound() bool {
	r.t.Helper()
	_, found, err := r.node.fetcher.readName(context.Background(), r.dbID, r.bucketID)
	if err != nil {
		r.t.Fatal(err)
	}
	return found
}

// wantGone checks the end state of a finished drop: no declaration, no reverse document, not a
// key under the bucket, the other bucket whole, and the name usable again.
func (r *u42Rig) wantGone(orderKeys int) {
	r.t.Helper()
	if _, found := r.declaration(); found {
		r.t.Fatal("the declaration survived a finished drop")
	}
	if r.reverseFound() {
		r.t.Fatal("the reverse-index document survived a finished drop")
	}
	if n := r.keys(r.bucketID); n != 0 {
		r.t.Fatalf("%d keys of the dropped bucket survive", n)
	}
	if n := r.keys(r.orderBucket()); n != orderKeys {
		r.t.Fatalf("bucket Order holds %d keys, want %d: the drop touched another bucket", n, orderKeys)
	}
	for i := range 5 {
		if _, err := r.node.Get(context.Background(), u30iDB, "Order", fmt.Sprintf("o%d", i)); err != nil {
			r.t.Fatalf("Order document o%d after the drop: %v", i, err)
		}
	}
}

// TestDropCommitsOneBump: a whole drop moves the database's schema_version by exactly one.
func TestDropCommitsOneBump(t *testing.T) {
	rig := newU42Rig(t)
	orderKeys := rig.keys(rig.orderBucket())
	before := rig.node.topology.Published(rig.dbID)
	rig.mustRun()
	if got := rig.bumper.calls.Load(); got != 1 {
		t.Fatalf("a drop committed %d bumps, want 1", got)
	}
	if got := rig.node.topology.Published(rig.dbID); got != before+1 {
		t.Fatalf("schema_version %d to %d, want one step", before, got)
	}
	rig.wantGone(orderKeys)
}

// TestDropRefusesAtItsBumpAndResumes: a drop whose bump fails stops before it deletes anything
// (the declaration is still there, dropping, and the documents are whole); running it again
// commits the bump and finishes.
func TestDropRefusesAtItsBumpAndResumes(t *testing.T) {
	rig := newU42Rig(t)
	orderKeys := rig.keys(rig.orderBucket())
	total := rig.keys(rig.bucketID)
	rig.bumper.fail = func(call int64) error {
		if call == 1 {
			return &cluster.Error{Code: cluster.CodeUnavailable, Message: "the cluster has no leader"}
		}
		return nil
	}
	if err := rig.run(); err == nil {
		t.Fatal("a drop whose bump failed reported success")
	}
	decl, found := rig.declaration()
	if !found || decl.State != machine.StateDropping {
		t.Fatalf("after the refused bump the declaration is %+v (found %v), want dropping", decl, found)
	}
	if got := rig.keys(rig.bucketID); got != total {
		t.Fatalf("a refused bump left %d of %d keys: the drop deleted before it was visible", got, total)
	}
	if !rig.reverseFound() {
		t.Fatal("the reverse-index document went before the bump")
	}
	rig.mustRun()
	rig.wantGone(orderKeys)
}

// TestDeclarationDeletedBeforeReverseDocument: the declaration goes first and the reverse
// document last, and a crash between them leaves the inert reverse document and no declaration.
func TestDeclarationDeletedBeforeReverseDocument(t *testing.T) {
	rig := newU42Rig(t)
	rig.mustRun()
	nameEnc, _ := store.EncodeComponent("User")
	idEnc, _ := store.EncodeComponent(idBytes(rig.bucketID))
	declAt, reverseAt := -1, -1
	for i, req := range rig.sender.sent {
		if req.Op.Class != cluster.OpDelete {
			continue
		}
		switch {
		case bytes.Equal(req.Op.EncID, nameEnc):
			declAt = i
		case bytes.Equal(req.Op.EncID, idEnc):
			reverseAt = i
		}
	}
	if declAt < 0 || reverseAt < 0 || declAt >= reverseAt {
		t.Fatalf("declaration delete at write %d and reverse delete at write %d, want declaration first", declAt, reverseAt)
	}

	crashed := newU42Rig(t)
	crashed.crashAt(u42Declaration, 1)
	if err := crashed.run(); !errors.Is(err, errU42Crash) {
		t.Fatalf("run = %v, want the crash", err)
	}
	if _, found := crashed.declaration(); found {
		t.Fatal("the declaration survived its delete step")
	}
	if !crashed.reverseFound() {
		t.Fatal("the reverse-index document is gone although the run died before deleting it")
	}
	// The retry cannot tell its own orphan from another bucket's reservation, so it answers
	// NOT_FOUND and touches no reverse document (conv 21 step 5).
	err := crashed.run()
	u30iCode(t, err, cluster.CodeNotFound)
	if !crashed.reverseFound() {
		t.Fatal("the retry deleted a reverse document it cannot prove is its own")
	}
	if n := crashed.keys(crashed.bucketID); n != 0 {
		t.Fatalf("%d keys of the bucket survive", n)
	}
}

// TestDropSweepIsOneRangePerHeldSlot: every slot this node holds for the database is swept
// exactly once, as one 'b' record each, and the bitmap the declaration carries names them.
func TestDropSweepIsOneRangePerHeldSlot(t *testing.T) {
	rig := newU42Rig(t)
	held := rig.node.pairs.HeldSlots(rig.dbID)
	if len(held) < 3 {
		t.Fatalf("setup: %d held slots", len(held))
	}
	var swept []uint16
	var mu sync.Mutex
	rig.drop.cfg.Sweeper = &u42CountingSweeper{inner: rig.node.retire, onSweep: func(slot uint16) {
		mu.Lock()
		swept = append(swept, slot)
		mu.Unlock()
	}}
	rig.crashAt(u42Swept, 1)
	if err := rig.run(); !errors.Is(err, errU42Crash) {
		t.Fatalf("run = %v, want the crash after the bitmap was written", err)
	}
	if len(swept) != len(held) {
		t.Fatalf("%d sweeps for %d held slots: %v", len(swept), len(held), swept)
	}
	seen := map[uint16]bool{}
	for _, slot := range swept {
		if seen[slot] {
			t.Fatalf("slot %d swept twice", slot)
		}
		seen[slot] = true
	}
	_, bitmap, found, err := rig.node.fetcher.Read(context.Background(), rig.dbID, "User")
	if err != nil || !found || len(bitmap) != store.SweptBytes {
		t.Fatalf("the dropping declaration carries a %d-byte bitmap (found %v, %v), want %d", len(bitmap), found, err, store.SweptBytes)
	}
	for _, slot := range held {
		if bitmap[slot/8]&(1<<(slot%8)) == 0 {
			t.Fatalf("slot %d is held and swept and its bit is clear", slot)
		}
	}
	for slot := range uint16(storage.SlotCount) {
		if bitmap[slot/8]&(1<<(slot%8)) != 0 && !seen[slot] {
			t.Fatalf("bit %d is set for a slot nobody swept", slot)
		}
	}
}

type u42CountingSweeper struct {
	inner   BucketSweeper
	onSweep func(slot uint16)
}

func (s *u42CountingSweeper) HeldSlots(db uint64) []uint16 { return s.inner.HeldSlots(db) }

func (s *u42CountingSweeper) RecordSweeps(db, bucket uint64, slots []uint16) error {
	return s.inner.RecordSweeps(db, bucket, slots)
}

func (s *u42CountingSweeper) Enqueue(db, bucket uint64) error { return s.inner.Enqueue(db, bucket) }

func (s *u42CountingSweeper) SweepBucket(ctx context.Context, slot uint16, db, bucket uint64) error {
	s.onSweep(slot)
	return s.inner.SweepBucket(ctx, slot, db, bucket)
}

// TestARetryOfADropSkipsItsBump: a drop that died after its bump commits no second bump when it
// is run again, however many times.
func TestARetryOfADropSkipsItsBump(t *testing.T) {
	rig := newU42Rig(t)
	rig.crashAt(u42Bump, 1)
	if err := rig.run(); !errors.Is(err, errU42Crash) {
		t.Fatalf("run = %v, want the crash", err)
	}
	published := rig.node.topology.Published(rig.dbID)
	rig.install()
	rig.crashAt(u42Marker, 1) // the marker step is skipped on a retry; this seam must not fire
	rig.mustRun()
	if got := rig.bumper.calls.Load(); got != 0 {
		t.Fatalf("the retry committed %d bumps, want none", got)
	}
	if got := rig.node.topology.Published(rig.dbID); got != published {
		t.Fatalf("schema_version %d, want %d: the retry fenced the database again", got, published)
	}
}

// TestADropIsOneStepPerCrashPoint: the drop dies after each of its steps in turn; the data is
// never lost early, running it again finishes it, and so does a restart of the node.
func TestADropIsOneStepPerCrashPoint(t *testing.T) {
	type point struct {
		step string
		nth  int
		// whole is true while no key of the bucket has been deleted yet.
		whole bool
	}
	points := []point{
		{u42Marker, 1, true}, {u42Reverse, 1, true}, {u42Bump, 1, false},
		{u42Sweep, 1, false}, {u42Sweep, 3, false}, {u42Swept, 1, false}, {u42Declaration, 1, false},
	}
	for _, p := range points {
		for _, restart := range []bool{false, true} {
			name := fmt.Sprintf("%s#%d restart=%v", p.step, p.nth, restart)
			t.Run(name, func(t *testing.T) {
				rig := newU42Rig(t)
				orderKeys := rig.keys(rig.orderBucket())
				total := rig.keys(rig.bucketID)
				rig.crashAt(p.step, p.nth)
				if err := rig.run(); !errors.Is(err, errU42Crash) {
					t.Fatalf("run = %v, want the crash", err)
				}
				if p.whole {
					if got := rig.keys(rig.bucketID); got != total {
						t.Fatalf("%d of %d keys after the %s step: data went before the drop was visible", got, total, p.step)
					}
				}
				if got := rig.keys(rig.orderBucket()); got != orderKeys {
					t.Fatalf("bucket Order holds %d keys, want %d", got, orderKeys)
				}
				if restart {
					rig.restart()
				} else {
					rig.install()
				}
				err := rig.run()
				if p.step == u42Declaration {
					// The declaration was already deleted: the retry is NOT_FOUND by design and the drop
					// is done but for the sweep a restart resumes.
					u30iCode(t, err, cluster.CodeNotFound)
					if err := rig.node.retire.Drain(context.Background()); err != nil {
						t.Fatal(err)
					}
					if n := rig.keys(rig.bucketID); n != 0 {
						t.Fatalf("%d keys survive", n)
					}
					return
				}
				if err != nil {
					t.Fatalf("the retry: %v", err)
				}
				rig.wantGone(orderKeys)
				u30iCode(t, rig.run(), cluster.CodeNotFound)
			})
		}
	}
}

// TestARestartResumesTheSweepWithoutTheDrop: a drop that died after its bump leaves 'b' records,
// and the node finishes the sweep by itself after a restart, deleting nothing live.
func TestARestartResumesTheSweepWithoutTheDrop(t *testing.T) {
	rig := newU42Rig(t)
	orderKeys := rig.keys(rig.orderBucket())
	rig.crashAt(u42Bump, 1)
	if err := rig.run(); !errors.Is(err, errU42Crash) {
		t.Fatalf("run = %v", err)
	}
	rig.restart()
	if err := rig.node.retire.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := rig.keys(rig.bucketID); n != 0 {
		t.Fatalf("%d keys survive the resumed sweep", n)
	}
	if n := rig.keys(rig.orderBucket()); n != orderKeys {
		t.Fatalf("bucket Order holds %d keys, want %d", n, orderKeys)
	}
	if decl, found := rig.declaration(); !found || decl.State != machine.StateDropping {
		t.Fatalf("the sweep alone must leave the declaration dropping for the drop to finish: %+v %v", decl, found)
	}
	rig.mustRun()
	rig.wantGone(orderKeys)
}

// TestAKilledBucketSweepDeletesNothingLiveAndResumes: the commit of the second sweep batch
// fails; no key outside the dropped bucket is gone, the 'b' record is still there, and the
// next pass finishes.
func TestAKilledBucketSweepDeletesNothingLiveAndResumes(t *testing.T) {
	rig := newU42Rig(t)
	orderKeys := rig.keys(rig.orderBucket())
	rig.pause()
	rig.crashAt(u42Bump, 1)
	_ = rig.run()
	counting := &u43CountingKV{KV: rig.node.kv}
	counting.failAt = 2
	rig.node.retire.deps.KV = counting
	rig.node.retire.deps.StepKeys = 3
	err := rig.node.retire.Drain(context.Background())
	if !errors.Is(err, errU43Killed) {
		t.Fatalf("Drain = %v, want the kill", err)
	}
	if n := rig.keys(rig.orderBucket()); n != orderKeys {
		t.Fatalf("bucket Order holds %d keys, want %d", n, orderKeys)
	}
	if left := u43RecordCountKind(t, rig.node, reclaimBucketKind); left != 1 {
		t.Fatalf("%d 'b' records survive the killed step, want exactly the one whose commit died", left)
	}
	rig.node.retire.deps.KV = rig.node.kv
	if err := rig.node.retire.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := rig.keys(rig.bucketID); n != 0 {
		t.Fatalf("%d keys survive the resumed sweep", n)
	}
	if n := rig.keys(rig.orderBucket()); n != orderKeys {
		t.Fatalf("bucket Order holds %d keys, want %d", n, orderKeys)
	}
}

// TestADroppingBucketRefusesReadsAndWrites: between the bump and the end of the sweep every
// read and write of the bucket answers BUCKET_DROPPING, the listing shows it as dropping, and
// the name cannot be created again; after the drop the name is free and a new bucket under it
// has a new id and none of the old documents.
func TestADroppingBucketRefusesReadsAndWrites(t *testing.T) {
	rig := newU42Rig(t)
	ctx := context.Background()
	rig.pause()
	rig.crashAt(u42Bump, 1)
	_ = rig.run()
	if _, err := rig.node.Insert(ctx, u30iDB, "User", u30iUser("late", "Late", 1)); err == nil {
		t.Fatal("an insert into a dropping bucket was accepted")
	} else {
		u30iCode(t, err, cluster.CodeBucketDropping)
	}
	_, err := rig.node.Get(ctx, u30iDB, "User", rig.userDocs[0])
	u30iCode(t, err, cluster.CodeBucketDropping)
	_, err = rig.node.Count(ctx, CountRequest{Database: u30iDB, Bucket: "User"})
	u30iCode(t, err, cluster.CodeBucketDropping)
	_, err = rig.node.AlterBucket(ctx, u30iDB, "User", AlterChange{Drop: []string{"City"}})
	u30iCode(t, err, cluster.CodeBucketDropping)
	_, err = rig.node.CreateBucket(ctx, u30iDB, "User", Field{Name: "Name", Type: "string"})
	u30iCode(t, err, cluster.CodeBucketExists)
	if err == nil || !strings.Contains(err.Error(), "dropping") {
		t.Fatalf("the create refusal does not name the state: %v", err)
	}
	if decl, found := rig.declaration(); !found || decl.State != machine.StateDropping {
		t.Fatalf("declaration %+v (found %v), want dropping", decl, found)
	}
	listed, err := rig.node.List(ctx, u30iDB, "Order", nil, 0)
	if err != nil || len(listed) != 5 {
		t.Fatalf("listing the other bucket while one drops: %d documents, %v", len(listed), err)
	}

	rig.install()
	rig.mustRun()
	newID, err := rig.node.CreateBucket(ctx, u30iDB, "User", Field{Name: "Name", Type: "string"})
	if err != nil {
		t.Fatalf("the name is not free after the drop: %v", err)
	}
	if newID == rig.bucketID {
		t.Fatal("the re-created bucket reused the dropped id")
	}
	_, err = rig.node.Get(ctx, u30iDB, "User", rig.userDocs[0])
	u30iCode(t, err, cluster.CodeNotFound)
	if n := rig.keys(rig.bucketID); n != 0 {
		t.Fatalf("%d keys of the dead id survive", n)
	}
}

// TestDroppingAMissingBucketIsNotFoundWithTheFix: the refusal names the bucket and the commands.
func TestDroppingAMissingBucketIsNotFoundWithTheFix(t *testing.T) {
	node, _ := u30iFresh(t)
	err := node.DropBucket(context.Background(), u30iDB, "Nope")
	u30iCode(t, err, cluster.CodeNotFound)
	for _, want := range []string{`"Nope"`, "show buckets in app", "create bucket Nope"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal %q lacks %q", err, want)
		}
	}
	err = node.DropBucket(context.Background(), u30iDB, "_buckets")
	u30iCode(t, err, cluster.CodeReservedName)
}

// TestADropWhileAStoreWriteIsInFlightWedgesNoPairOrReplay: a push has passed its checks and
// holds its pair when a drop writes its marker; the push applies against a node whose cache has
// lost the declaration, which now reads dropping. The apply must be a no-op, not a failure
// that fences the pair; and the replay of the log after the drop has finished (the declaration
// gone) must open the node.
func TestADropWhileAStoreWriteIsInFlightWedgesNoPairOrReplay(t *testing.T) {
	rig := newU42Rig(t)
	node := rig.node
	ctx := context.Background()
	id := u40LaterDocument(t, node)
	if _, err := node.Insert(ctx, u30iDB, "User", u30iUser(id, "Ada", 36)); err != nil {
		t.Fatal(err)
	}
	gate := &u40GateLog{class: cluster.OpStoreVerb, inner: node.writer.cfg.Log, reached: make(chan struct{}), release: make(chan struct{})}
	node.writer.cfg.Log = gate
	pushed := make(chan error, 1)
	go func() {
		pushed <- node.Push(ctx, PushRequest{StoreTarget: StoreTarget{Database: u30iDB, Bucket: "User", ID: id, Field: "Purchases"}, Element: u43Element()})
	}()
	select {
	case <-gate.reached:
	case <-time.After(10 * time.Second):
		t.Fatal("the push never reached its append")
	}
	dropped := make(chan error, 1)
	go func() { dropped <- node.DropBucket(ctx, u30iDB, "User") }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if decl, found := rig.declaration(); found && decl.State == machine.StateDropping {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the drop never wrote its marker")
		}
		time.Sleep(time.Millisecond)
	}
	// The node forgets the declaration, as a restart or an eviction would.
	node.fetcher.Forget(rig.dbID)
	close(gate.release)
	if err := <-pushed; err != nil {
		t.Fatalf("the push admitted before the drop failed its apply: %v", err)
	}
	if err := <-dropped; err != nil {
		t.Fatalf("the drop: %v", err)
	}
	slot := u40DocSlot(t, node, "Order", "o0")
	if flags := node.pairs.Snapshot(slot, rig.dbID).Flags; flags&flagReplayNeeded != 0 {
		t.Fatalf("the pair of the order documents is fenced for replay (flags %b)", flags)
	}
	for _, held := range node.pairs.HeldSlots(rig.dbID) {
		if flags := node.pairs.Snapshot(held, rig.dbID).Flags; flags&flagReplayNeeded != 0 {
			t.Fatalf("pair %d is fenced for replay (flags %b)", held, flags)
		}
	}
	if _, err := node.Upsert(ctx, u30iDB, "Order", bson.D{{Key: "_id_", Value: "o0"}, {Key: "Total", Value: int32(9)}}); err != nil {
		t.Fatalf("a later write to a pair of the database: %v", err)
	}
	rig.restart()
	if _, err := rig.node.Get(ctx, u30iDB, "Order", "o1"); err != nil {
		t.Fatalf("after the restart: %v", err)
	}
}

// TestFieldEngineOfADroppedBucketIsRetired: a record naming a bucket whose drop this node holds
// the 'b' record for resolves to the retired-field outcome, which the apply turns into an empty
// write. Once the sweep has freed the name and deleted the record nothing proves the drop (a
// not-found is also what a lagging declaration owner answers, BUG-0771), so it is "declaration
// unavailable" and the record waits (BUG-0772 owns the release).
func TestFieldEngineOfADroppedBucketIsRetired(t *testing.T) {
	rig := newU42Rig(t)
	// The node's background retire sweep works any 'b' record it finds and deletes it, the one
	// written by hand below included; held off, the record is still there when FieldEngine looks
	// for it (the sweep's last laps can trail the inline drop's return, mostly on a fast tmpfs).
	rig.pause()
	rig.mustRun()
	rig.node.fetcher.Forget(rig.dbID)
	engines := declaredEngines{node: rig.node}
	proven, err := rig.node.hasBucketRecord(rig.dbID, rig.bucketID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = engines.FieldEngine(rig.dbID, rig.bucketID, 5)
	if proven && !errors.Is(err, errRetiredField) {
		t.Fatalf("FieldEngine of a dropped bucket with its 'b' record = %v, want errRetiredField", err)
	}
	if !proven && (!errors.Is(err, ErrDeclUnavailable) || errors.Is(err, errRetiredField)) {
		t.Fatalf("FieldEngine of a swept bucket with no record = %v, want ErrDeclUnavailable", err)
	}
	batch := rig.node.kv.NewBatch()
	if err := batch.Set(BucketRecord{Slot: 1, DBID: rig.dbID, BucketID: rig.bucketID}.Key(), nil); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}
	if _, err := engines.FieldEngine(rig.dbID, rig.bucketID, 5); !errors.Is(err, errRetiredField) {
		t.Fatalf("FieldEngine of a dropped bucket with a 'b' record = %v, want errRetiredField", err)
	}
}

// TestADropAddsNoPerBucketGoroutine: the sweep is the node's one goroutine. Creating and
// dropping fifty buckets leaves the goroutine count where it was.
func TestADropAddsNoPerBucketGoroutine(t *testing.T) {
	node, _ := u30iFresh(t)
	ctx := context.Background()
	runtime.GC()
	before := runtime.NumGoroutine()
	for i := range 50 {
		name := fmt.Sprintf("B%02d", i)
		if _, err := node.CreateBucket(ctx, u30iDB, name, Field{Name: "N", Type: "int"}); err != nil {
			t.Fatal(err)
		}
		if _, err := node.Insert(ctx, u30iDB, name, bson.D{{Key: "_id_", Value: "x"}, {Key: "N", Value: int32(1)}}); err != nil {
			t.Fatal(err)
		}
		if err := node.DropBucket(ctx, u30iDB, name); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(50 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before+2 {
		t.Fatalf("%d goroutines after 50 drops, %d before", after, before)
	}
}

// TestABucketRecordForALiveBucketDeletesNothing: a 'b' record names a bucket whose declaration
// is active under that very id (a damaged record: only a dropping bucket gets one). The sweep
// refuses and keeps its hands off the data.
func TestABucketRecordForALiveBucketDeletesNothing(t *testing.T) {
	rig := newU42Rig(t)
	rig.pause()
	total := rig.keys(rig.bucketID)
	slot := rig.node.pairs.HeldSlots(rig.dbID)[0]
	for _, held := range rig.node.pairs.HeldSlots(rig.dbID) {
		rec := BucketRecord{Slot: held, DBID: rig.dbID, BucketID: rig.bucketID}
		if err := rig.node.retire.writeRecords([][]byte{rec.Key()}); err != nil {
			t.Fatal(err)
		}
	}
	_ = slot
	err := rig.node.retire.Drain(context.Background())
	if !errors.Is(err, errLiveBucket) {
		t.Fatalf("Drain = %v, want the refusal to sweep a live bucket", err)
	}
	if got := rig.keys(rig.bucketID); got != total {
		t.Fatalf("a live bucket lost %d keys to a damaged record", total-got)
	}
}

// TestASlowDropNeverDeletesTheRecreatedBucket: a run that swept and was then overtaken (another
// run finished the drop and the name was created again under a new id) must find, when it
// reaches its deletes, that the declaration is not the one it was dropping, and delete nothing.
func TestASlowDropNeverDeletesTheRecreatedBucket(t *testing.T) {
	rig := newU42Rig(t)
	ctx := context.Background()
	slow := rig.drop
	rig.crashAt(u42Swept, 1)
	if err := rig.run(); !errors.Is(err, errU42Crash) {
		t.Fatalf("run = %v", err)
	}
	rig.install()
	rig.mustRun()
	newID, err := rig.node.CreateBucket(ctx, u30iDB, "User", Field{Name: "Name", Type: "string"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rig.node.Insert(ctx, u30iDB, "User", bson.D{{Key: "_id_", Value: "fresh"}, {Key: "Name", Value: "Fresh"}}); err != nil {
		t.Fatal(err)
	}
	slow.crashAfter = nil
	stale := &dropRun{DropBucket: slow, ctx: ctx, dbID: rig.dbID, dbName: u30iDB, name: "User"}
	if _, err := stale.finish(rig.bucketID); err != nil {
		t.Fatalf("the overtaken run: %v", err)
	}
	decl, found := rig.declaration()
	if !found || decl.BucketID != newID || decl.State != machine.StateActive {
		t.Fatalf("the re-created declaration is %+v (found %v), want active with id %016x", decl, found, newID)
	}
	if name, found, err := rig.node.fetcher.readName(ctx, rig.dbID, newID); err != nil || !found || name != "User" {
		t.Fatalf("the re-created bucket's reverse document: %q %v %v", name, found, err)
	}
	if _, err := rig.node.Get(ctx, u30iDB, "User", "fresh"); err != nil {
		t.Fatalf("the re-created bucket lost its document: %v", err)
	}
}

// TestTheDropSweepRerunsAfterASeedCompletes: keys of a dropping bucket that a seed delivers after
// the bump's own pass finished are removed by the re-run on ready, and the other bucket's keys
// in the same pair are not.
func TestTheDropSweepRerunsAfterASeedCompletes(t *testing.T) {
	rig := newU42Rig(t)
	rig.pause()
	orderKeys := rig.keys(rig.orderBucket())
	rig.crashAt(u42Bump, 1)
	_ = rig.run()
	if err := rig.node.retire.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := rig.keys(rig.bucketID); n != 0 {
		t.Fatalf("setup: %d keys survive the sweep", n)
	}
	// A seed brings a copy of a pair taken before the bump.
	slot := rig.node.pairs.HeldSlots(rig.dbID)[0]
	enc, _ := store.EncodeComponent("late")
	stray := store.DocumentPrefix(slot, rig.dbID, rig.bucketID, enc)
	batch := rig.node.kv.NewBatch()
	if err := batch.Set(stray, []byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := batch.Set(store.FieldKey(stray, 2), []byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := batch.Commit(false); err != nil {
		t.Fatal(err)
	}
	if err := rig.node.retire.RerunOnReady(slot, rig.dbID); err != nil {
		t.Fatal(err)
	}
	if err := rig.node.retire.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := rig.keys(rig.bucketID); n != 0 {
		t.Fatalf("%d keys of the dropped bucket survive the re-run", n)
	}
	if n := rig.keys(rig.orderBucket()); n != orderKeys {
		t.Fatalf("bucket Order holds %d keys, want %d", n, orderKeys)
	}
}
