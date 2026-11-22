package wire

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/machine"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
)

// The tests in this file cover the features that run in the first section of an RF 1 owner write (the guard,
// the expiry rules and the aggregate dry run) on the pipelined path, where earlier writes of the pair can be
// appended and not yet applied while a later write plans. Each feature reads the document it writes, so the
// pending-key rule (pendingKeys, Pipeline.Conflicts) must make a write wait for every pending write to the
// same document before its guard, its expiry judgement or its aggregate check reads.

// writeOpAs is writeOp with a whole request id, for tests that send more writes than one byte can name.
func (fx *fixture) writeOpAs(op cluster.Op, requestID [16]byte) (WriteAnswer, error) {
	return fx.w.Write(context.Background(), WriteRequest{Slot: testSlot, DBID: testDB, Op: op, RequestID: requestID, Deadline: 10 * time.Second})
}

// startOp runs one write in the background and returns a channel for its outcome.
func (fx *fixture) startOp(op cluster.Op, requestID [16]byte) <-chan pairLockResult {
	done := make(chan pairLockResult, 1)
	go func() {
		answer, err := fx.writeOpAs(op, requestID)
		done <- pairLockResult{answer: answer, err: err}
	}()
	return done
}

func pipelineRequestID(group byte, n uint64) [16]byte {
	id := [16]byte{7, 7, group}
	binary.BigEndian.PutUint64(id[8:], n)
	return id
}

// requirePipelined fails the test unless the fixture's writes take the RF 1 pipelined path, so a test in this
// file cannot pass on the locked path by accident.
func (fx *fixture) requirePipelined(t *testing.T) {
	t.Helper()
	dbRow, ok := fx.topo.Database(testDB)
	if !ok {
		t.Fatal("the fixture has no database row")
	}
	if _, pipelined := fx.w.pipelinedAdvancer(dbRow); !pipelined {
		t.Fatalf("database RF %d with advancer %T does not take the pipelined path", dbRow.RF, fx.w.cfg.Advancer)
	}
}

func scalarInt32(t *testing.T, raw []byte) int32 {
	t.Helper()
	if len(raw) != 5 || raw[0] != 0x10 {
		t.Fatalf("scalar %x is not an int32", raw)
	}
	return int32(binary.LittleEndian.Uint32(raw[1:]))
}

// TestAPipelinedGuardSeesTheEarlierPendingWriteToItsDocument: [GUARD: a conflict rule without the document key,
// or a guard judged in the first section without waiting for the pending list]. Write A (if Name is "Ada" set
// "Bob") is appended and its flush is held. Write B (if Name is "Bob" set "Cy") on the same document must not
// plan while A is pending: it waits, then sees A's result and applies. Write C (if Name is "Ada") is then
// NOT_APPLIED with the current document, Name "Cy", and takes no position.
func TestAPipelinedGuardSeesTheEarlierPendingWriteToItsDocument(t *testing.T) {
	fx := newGuardFixture(t)
	fx.requirePipelined(t)
	fx.mustWrite(testSlot, "s1") // Name = "Ada"
	base := fx.wal.LastSequence()
	release := fx.pairLockHold(t)

	first := fx.startOp(guardedUpdate(t, "s1", []cluster.GuardTerm{{FieldID: 2, Value: guardString("Ada")}}, setField(2, guardString("Bob"))), pipelineRequestID(1, 1))
	fx.waitForLog(t, base, 1)
	second := fx.startOp(guardedUpdate(t, "s1", []cluster.GuardTerm{{FieldID: 2, Value: guardString("Bob")}}, setField(2, guardString("Cy"))), pipelineRequestID(1, 2))
	time.Sleep(150 * time.Millisecond)
	if got := fx.wal.LastSequence() - base; got != 1 {
		release()
		t.Fatalf("the log holds %d new records while the first guarded write is pending, want 1: the second write to the same document planned without waiting for it", got)
	}
	select {
	case got := <-second:
		release()
		t.Fatalf("the second guarded write was answered (%+v, %v) while the first was still pending: its guard read a state missing the first", got.answer, got.err)
	default:
	}
	release()

	for name, done := range map[string]<-chan pairLockResult{"first": first, "second": second} {
		got := fx.mustAnswer(t, done)
		if got.err != nil || got.answer.NotApplied || got.answer.Outcome != machine.OutcomeApplied {
			t.Fatalf("%s guarded write = %+v, %v, want applied: the second guard must see the first write's Name", name, got.answer, got.err)
		}
	}
	if got := fx.storedScalar("s1", 2); string(got) != string(guardString("Cy")) {
		t.Fatalf("Name = %x after both writes, want Cy", got)
	}

	logged := fx.wal.LastSequence()
	stale, err := fx.writeOpAs(guardedUpdate(t, "s1", []cluster.GuardTerm{{FieldID: 2, Value: guardString("Ada")}}, setField(2, guardString("Zed"))), pipelineRequestID(1, 3))
	if err != nil || !stale.NotApplied || stale.GuardField != 2 {
		t.Fatalf("stale guarded write = %+v, %v, want NOT_APPLIED on field 2", stale, err)
	}
	scalars, err := DecodeScalarSet(stale.Result)
	if err != nil || string(scalars[2]) != string(guardString("Cy")) {
		t.Fatalf("NOT_APPLIED carried Name %x (%v), want the current document's Cy", scalars[2], err)
	}
	if fx.wal.LastSequence() != logged {
		t.Fatalf("a NOT_APPLIED write appended %d records, want none", fx.wal.LastSequence()-logged)
	}
}

// TestPipelinedGuardedIncrementsLoseNoUpdate: [GUARD: as above]. Six clients run a compare-and-set loop on one
// counter (if Count is n set n+1, and on NOT_APPLIED learn n from the returned document) while four others insert
// documents into the same pair, so the pending list is never long empty. Every successful increment must be
// counted once: a guard judged against a state missing a pending increment would let two clients both apply n+1.
func TestPipelinedGuardedIncrementsLoseNoUpdate(t *testing.T) {
	fx := newGuardFixture(t)
	fx.requirePipelined(t)
	fx.wal.SetFsyncHook(func() { time.Sleep(2 * time.Millisecond) })
	fx.mustWrite(testSlot, "ctr")
	if answer, err := fx.writeOpAs(guardedUpdate(t, "ctr", nil, setField(5, guardInt32(0))), pipelineRequestID(2, 0)); err != nil || answer.Outcome != machine.OutcomeApplied {
		t.Fatalf("seeding Count: %+v, %v", answer, err)
	}

	const counters, increments, inserters, inserts = 6, 5, 4, 8
	var wait sync.WaitGroup
	var sent atomic.Uint64
	for client := range counters {
		wait.Add(1)
		go func() {
			defer wait.Done()
			current := int32(0)
			for done := 0; done < increments; {
				op := guardedUpdate(t, "ctr", []cluster.GuardTerm{{FieldID: 5, Value: guardInt32(current)}}, setField(5, guardInt32(current+1)))
				answer, err := fx.writeOpAs(op, pipelineRequestID(byte(10+client), sent.Add(1)))
				if err != nil {
					t.Errorf("client %d: %v", client, err)
					return
				}
				if !answer.NotApplied {
					current++
					done++
					continue
				}
				scalars, err := DecodeScalarSet(answer.Result)
				if err != nil {
					t.Errorf("client %d: decoding the NOT_APPLIED document: %v", client, err)
					return
				}
				current = scalarInt32(t, scalars[5])
			}
		}()
	}
	for inserter := range inserters {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for n := range inserts {
				id := fmt.Sprintf("o%d-%d", inserter, n)
				answer, err := fx.w.Write(context.Background(), fx.pairLockRequest(testSlot, id, uint64(1000+inserter*100+n)))
				if err != nil || answer.Outcome != machine.OutcomeApplied {
					t.Errorf("insert %s = %+v, %v", id, answer, err)
					return
				}
			}
		}()
	}
	wait.Wait()
	if t.Failed() {
		return
	}
	if got := scalarInt32(t, fx.storedScalar("ctr", 5)); got != counters*increments {
		t.Fatalf("Count = %d after %d applied increments: a guarded write planned against a state missing a pending one", got, counters*increments)
	}
}

// newExpiringGuardFixture is newGuardFixture whose bucket expires a document at field 7 (Until, a datetime),
// judged at the clock it returns.
func newExpiringGuardFixture(t *testing.T) (*fixture, *atomic.Int64) {
	t.Helper()
	fx := newGuardFixture(t)
	table := &fx.schema.decl.Fields
	table.Entries = append(table.Entries, machine.FieldEntry{ID: 7, Type: "datetime", Name: "Until"})
	table.NextFieldID = 8
	fx.schema.decl.Expire = machine.ExpireTable{BucketField: 7}
	clock := &atomic.Int64{}
	clock.Store(500)
	fx.w = NewWriter(WriterConfig{
		Topology: fx.topo, Pairs: fx.pairs, Log: fx.wal, KV: fx.kv,
		Applier: NewDocumentApplier(fx.kv, nil).WithFieldEngines(scalarOnlyEngines{}).WithResultCap(1024),
		Clock:   fixedClock{}, Lease: fx.lease, Dedupe: NewKVDedupeReader(fx.kv), Schema: fx.schema, Replication: fx.repl,
		WCTimeout: time.Second, DedupeWindow: machine.WindowConfig{Window: 8192, MaxResult: 1024},
		WallMillis: clock.Load,
	})
	return fx, clock
}

// seedExpiring inserts document id (Name "Ada") and sets it to expire at until.
func (fx *fixture) seedExpiring(t *testing.T, id string, until int64, requestID [16]byte) {
	t.Helper()
	fx.mustWrite(testSlot, id)
	answer, err := fx.writeOpAs(guardedUpdate(t, id, nil, setField(7, datetimeScalar(until))), requestID)
	if err != nil || answer.Outcome != machine.OutcomeApplied {
		t.Fatalf("setting Until of %s: %+v, %v", id, answer, err)
	}
}

// TestAPipelinedGuardFindsAnExpiredDocumentAbsent: [GUARD: the guard judged without the attempt's clock, or with
// no wait for a pending write to its document]. While another document's write is pending, a guard on a document
// past its time is NOT_FOUND at once, takes no position and does not wait for the flush (it overlaps). When the
// pending write is to the same document and moves its expire time later, the guard waits for it and then finds the
// document live and applies.
func TestAPipelinedGuardFindsAnExpiredDocumentAbsent(t *testing.T) {
	t.Run("expired, another document pending", func(t *testing.T) {
		fx, clock := newExpiringGuardFixture(t)
		fx.requirePipelined(t)
		fx.seedExpiring(t, "e1", 1000, pipelineRequestID(3, 1))
		base := fx.wal.LastSequence()
		release := fx.pairLockHold(t)
		other := fx.writeOne(testSlot, "x1", 50)
		fx.waitForLog(t, base, 1)

		clock.Store(2000)
		guarded := fx.startOp(guardedUpdate(t, "e1", []cluster.GuardTerm{{FieldID: 2, Value: guardString("Ada")}}, setField(2, guardString("Bob"))), pipelineRequestID(3, 2))
		var got pairLockResult
		select {
		case got = <-guarded:
		case <-time.After(5 * time.Second):
			release()
			t.Fatal("a guard on another document than the pending write waited for that write's flush")
		}
		if codeOf(t, got.err) != cluster.CodeNotFound || got.answer.NotApplied {
			release()
			t.Fatalf("guard on an expired document = %+v, %v, want NOT_FOUND", got.answer, got.err)
		}
		if appended := fx.wal.LastSequence() - base; appended != 1 {
			release()
			t.Fatalf("the log holds %d new records, want only the pending write's: a refused guard takes no position", appended)
		}
		release()
		if done := fx.mustAnswer(t, other); done.err != nil || done.answer.Outcome != machine.OutcomeApplied {
			t.Fatalf("the pending write = %+v, %v, want applied", done.answer, done.err)
		}
		if name := fx.storedScalar("e1", 2); string(name) != string(guardString("Ada")) {
			t.Fatalf("Name = %x, want the expired document untouched", name)
		}
	})
	t.Run("a pending write to the document extends it", func(t *testing.T) {
		fx, clock := newExpiringGuardFixture(t)
		fx.requirePipelined(t)
		fx.seedExpiring(t, "e2", 1000, pipelineRequestID(4, 1))
		base := fx.wal.LastSequence()
		release := fx.pairLockHold(t)
		extend := fx.startOp(guardedUpdate(t, "e2", nil, setField(7, datetimeScalar(5000))), pipelineRequestID(4, 2))
		fx.waitForLog(t, base, 1)

		clock.Store(2000)
		guarded := fx.startOp(guardedUpdate(t, "e2", []cluster.GuardTerm{{FieldID: 2, Value: guardString("Ada")}}, setField(2, guardString("Bob"))), pipelineRequestID(4, 3))
		time.Sleep(150 * time.Millisecond)
		select {
		case got := <-guarded:
			release()
			t.Fatalf("the guard was answered (%+v, %v) while the write extending its document was pending: it judged expiry on a state missing that write", got.answer, got.err)
		default:
		}
		release()
		if done := fx.mustAnswer(t, extend); done.err != nil || done.answer.Outcome != machine.OutcomeApplied {
			t.Fatalf("the extending write = %+v, %v, want applied", done.answer, done.err)
		}
		got := fx.mustAnswer(t, guarded)
		if got.err != nil || got.answer.NotApplied || got.answer.Outcome != machine.OutcomeApplied {
			t.Fatalf("guard after the extension = %+v, %v, want applied: the document lives until 5000 and the clock reads 2000", got.answer, got.err)
		}
		if name := fx.storedScalar("e2", 2); string(name) != string(guardString("Bob")) {
			t.Fatalf("Name = %x, want Bob", name)
		}
	})
}

// TestAGuardedWriteWithAnUnknownFlushIsDedupedAfterTheReplay: [GUARD: the guard evaluated before the dedupe
// lookup, or a guarded write's failed flush answered as a refusal]. A guarded update (if Name is "Ada" set "Bob",
// answer with the document after) whose flush fails is answered with an unknown outcome and no refusal code. The
// replay, standing in for the restart, applies it from the record, which carries no guard. Another write then sets
// Name to "Zed", so the guard no longer holds; the retry with the same request id is still deduped at the original
// position with the document the original applied, never NOT_APPLIED, and nothing is appended for it.
func TestAGuardedWriteWithAnUnknownFlushIsDedupedAfterTheReplay(t *testing.T) {
	fx := newGuardFixture(t)
	fx.requirePipelined(t)
	fx.mustWrite(testSlot, "s1") // Name = "Ada", position 1
	op := guardedUpdate(t, "s1", []cluster.GuardTerm{{FieldID: 2, Value: guardString("Ada")}}, setField(2, guardString("Bob")))
	op.Return = cluster.ReturnAfter
	requestID := pipelineRequestID(5, 1)

	override := func(seq uint64) (bool, error) {
		return true, fmt.Errorf("the flush through sequence %d failed: injected EIO", seq)
	}
	fx.wal.syncOverride.Store(&override)
	_, err := fx.writeOpAs(op, requestID)
	requireUnknownOutcome(t, err)
	if name := fx.storedScalar("s1", 2); string(name) != string(guardString("Ada")) {
		t.Fatalf("Name = %x after a failed flush, want Ada: a record whose flush failed must not be applied", name)
	}
	fx.wal.syncOverride.Store(nil)
	if err := fx.wal.WAL.SyncThrough(fx.wal.LastSequence()); err != nil {
		t.Fatal(err)
	}
	replayer := cluster.NewReplayer(fx.wal.WAL, func(slot uint16, db uint64) cluster.Position {
		return fx.pairs.Snapshot(slot, db).AppliedPos
	})
	if err := replayer.Replay(0, cluster.Position{}, fx.w.Replay); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	fx.markReady(testSlot)
	if name := fx.storedScalar("s1", 2); string(name) != string(guardString("Bob")) {
		t.Fatalf("Name = %x after the replay, want Bob: the replay applies the record", name)
	}
	original := fx.pairs.Snapshot(testSlot, testDB).AppliedPos

	if answer, err := fx.writeOpAs(guardedUpdate(t, "s1", nil, setField(2, guardString("Zed"))), pipelineRequestID(5, 2)); err != nil || answer.Outcome != machine.OutcomeApplied {
		t.Fatalf("setting Name to Zed: %+v, %v", answer, err)
	}
	logged := fx.wal.LastSequence()
	retry, err := fx.writeOpAs(op, requestID)
	if err != nil || !retry.Deduped || retry.NotApplied || retry.Pos != original {
		t.Fatalf("retry = %+v, %v, want deduped at the original position %s and never NOT_APPLIED", retry, err, original)
	}
	returned, truncated := splitGuardResult(retry.Result)
	if truncated {
		t.Fatal("the recorded document was marked truncated")
	}
	scalars, err := DecodeScalarSet(returned)
	if err != nil || string(scalars[2]) != string(guardString("Bob")) {
		t.Fatalf("the retry returned Name %x (%v), want the document the original applied, Bob", scalars[2], err)
	}
	if fx.wal.LastSequence() != logged {
		t.Fatalf("the retry appended %d records, want none", fx.wal.LastSequence()-logged)
	}
}

// aggPrefixOf is aggPrefix for any document of Ledger.
func aggPrefixOf(t *testing.T, node *Node, id, field string) []byte {
	t.Helper()
	ref, err := node.bucket(context.Background(), u30iDB, "Ledger")
	if err != nil {
		t.Fatal(err)
	}
	fieldID, err := ref.storeField(field)
	if err != nil {
		t.Fatal(err)
	}
	encID, err := store.EncodeComponent(id)
	if err != nil {
		t.Fatal(err)
	}
	return store.StorePrefix(ref.slot(encID), ref.dbID, ref.entry.Decl.BucketID, encID, fieldID)
}

// samePairIDs returns n document ids other than l1 whose documents sit in l1's slot, so their writes share l1's
// pair and its pending list.
func samePairIDs(t *testing.T, node *Node, n int) []string {
	t.Helper()
	ref, err := node.bucket(context.Background(), u30iDB, "Ledger")
	if err != nil {
		t.Fatal(err)
	}
	slotOf := func(id string) uint16 {
		encID, err := store.EncodeComponent(id)
		if err != nil {
			t.Fatal(err)
		}
		return ref.slot(encID)
	}
	want := slotOf("l1")
	var ids []string
	for i := 0; len(ids) < n; i++ {
		if i > 1<<22 {
			t.Fatalf("found only %d ids in slot %d", len(ids), want)
		}
		if id := fmt.Sprintf("p%d", i); slotOf(id) == want {
			ids = append(ids, id)
		}
	}
	return ids
}

// aggExpected is what the pushes to one document's store add up to.
type aggExpected struct {
	sum     int64
	bySum   map[string]int64
	byCount map[string]uint64
}

// checkAggregatesOf compares every aggregate of field of document id with want.
func checkAggregatesOf(t *testing.T, node *Node, id, field string, want aggExpected) {
	t.Helper()
	prefix := aggPrefixOf(t, node, id, field)
	snapshot := node.kv.NewSnapshot()
	defer snapshot.Discard()
	read := func(aggID uint8, by string) []byte {
		var byKey []byte
		if by != "" {
			var err error
			if byKey, err = store.EncodeComponent(by); err != nil {
				t.Fatal(err)
			}
		}
		raw, err := snapshot.Get(store.AggregateKey(prefix, aggID, byKey))
		if errors.Is(err, storage.ErrKeyNotFound) {
			t.Fatalf("%s.%s has no aggregate %d key for %q", id, field, aggID, by)
		}
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	for _, entry := range aggSetOf(t, node, field).Entries {
		switch entry.Kind {
		case machine.AggSum:
			sum, _, err := store.DecodeSumValue(nil, read(entry.ID, ""))
			if err != nil || sum.Whole != want.sum {
				t.Fatalf("%s.%s sum = %d (%v), the pushes add up to %d", id, field, sum.Whole, err, want.sum)
			}
		case machine.AggSumBy:
			for currency, total := range want.bySum {
				sum, _, n, err := store.DecodeSumByValue(nil, read(entry.ID, currency))
				if err != nil || sum.Whole != total || n != want.byCount[currency] {
					t.Fatalf("%s.%s sum by %s = %d over %d (%v), want %d over %d", id, field, currency, sum.Whole, n, err, total, want.byCount[currency])
				}
			}
		default:
			for currency, count := range want.byCount {
				if n, err := store.DecodeCountByValue(nil, read(entry.ID, currency)); err != nil || n != count {
					t.Fatalf("%s.%s count by %s = %d (%v), want %d", id, field, currency, n, err, count)
				}
			}
		}
	}
}

// TestPipelinedPushesKeepTheAggregatesExact: [GUARD: an aggregate dry run or apply that reads a sum missing a
// pending push to its document]. Concurrent pushes onto the aggregated fifoStore of four documents in one pair,
// four clients per document, with a slow flush so the pair pipelines: every document's plain sum, sum by and
// count by equal what its pushes add up to. Fewer flushes than pushes shows the pushes shared group commits, which
// a pair holding its lock across the flush cannot do.
func TestPipelinedPushesKeepTheAggregatesExact(t *testing.T) {
	node := aggRig(t)
	ctx := context.Background()
	ref, err := node.bucket(ctx, u30iDB, "Ledger")
	if err != nil {
		t.Fatal(err)
	}
	dbRow, ok := node.writer.cfg.Topology.Database(ref.dbID)
	if !ok {
		t.Fatal("the node has no row for the database")
	}
	if _, pipelined := node.writer.pipelinedAdvancer(dbRow); !pipelined {
		t.Fatalf("database RF %d does not take the pipelined path", dbRow.RF)
	}
	ids := append([]string{"l1"}, samePairIDs(t, node, 3)...)
	for _, id := range ids[1:] {
		if _, err := node.Insert(ctx, u30iDB, "Ledger", bson.D{{Key: "_id_", Value: id}, {Key: "Name", Value: id}}); err != nil {
			t.Fatal(err)
		}
	}
	var flushes atomic.Int64
	node.wal.SetFsyncHook(func() {
		flushes.Add(1)
		time.Sleep(5 * time.Millisecond)
	})

	const clientsPerDoc, pushesPerClient = 4, 6
	currencies := []string{"USD", "EUR", "GBP"}
	var mu sync.Mutex
	want := map[string]*aggExpected{}
	for _, id := range ids {
		want[id] = &aggExpected{bySum: map[string]int64{}, byCount: map[string]uint64{}}
	}
	var wait sync.WaitGroup
	for docIndex, id := range ids {
		for client := range clientsPerDoc {
			wait.Add(1)
			go func() {
				defer wait.Done()
				for n := range pushesPerClient {
					amount := int64(docIndex*1000 + client*100 + n + 1)
					currency := currencies[(client+n)%len(currencies)]
					element := bson.D{{Key: "Amount", Value: amount}, {Key: "Currency", Value: currency}}
					target := StoreTarget{Database: u30iDB, Bucket: "Ledger", ID: id, Field: "Fifo"}
					if err := node.Push(ctx, PushRequest{StoreTarget: target, Element: element}); err != nil {
						t.Errorf("push to %s: %v", id, err)
						return
					}
					mu.Lock()
					expected := want[id]
					expected.sum += amount
					expected.bySum[currency] += amount
					expected.byCount[currency]++
					mu.Unlock()
				}
			}()
		}
	}
	wait.Wait()
	node.wal.SetFsyncHook(nil)
	if t.Failed() {
		return
	}
	for _, id := range ids {
		checkAggregatesOf(t, node, id, "Fifo", *want[id])
	}
	pushes := int64(len(ids) * clientsPerDoc * pushesPerClient)
	if got := flushes.Load(); got >= pushes {
		t.Fatalf("%d pushes to one pair took %d flushes, want fewer: the pair did not pipeline", pushes, got)
	}
}
