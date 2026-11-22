package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

// unique on a cluster (GAP-14 build doc Step 7): refused on the bucket's owner
// before its WAL append, deterministic after it, classified on replay, never
// replayed out of order, and loud when a replica disagrees with its owner.

// recordedLog is a slog handler that keeps each record's level, message and
// attributes, for tests that read what a node logged.
type recordedLog struct {
	mu      sync.Mutex
	records []recordedEntry
}

type recordedEntry struct {
	level slog.Level
	msg   string
	attrs map[string]string
}

func (r *recordedLog) Enabled(context.Context, slog.Level) bool { return true }

func (r *recordedLog) Handle(_ context.Context, record slog.Record) error {
	entry := recordedEntry{level: record.Level, msg: record.Message, attrs: make(map[string]string)}
	record.Attrs(func(attr slog.Attr) bool {
		entry.attrs[attr.Key] = attr.Value.String()
		return true
	})
	r.mu.Lock()
	r.records = append(r.records, entry)
	r.mu.Unlock()
	return nil
}

func (r *recordedLog) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *recordedLog) WithGroup(string) slog.Handler      { return r }

// matching returns the records at level whose message contains substring.
func (r *recordedLog) matching(level slog.Level, substring string) []recordedEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	var matched []recordedEntry
	for _, entry := range r.records {
		if entry.level == level && strings.Contains(entry.msg, substring) {
			matched = append(matched, entry)
		}
	}
	return matched
}

// recordClusterLogs makes every node started afterwards log into the returned
// recorder, until the test ends.
func recordClusterLogs(t *testing.T) *recordedLog {
	t.Helper()
	recorder := &recordedLog{}
	clusterTestLogger.Store(slog.New(recorder))
	t.Cleanup(func() { clusterTestLogger.Store(nil) })
	return recorder
}

// setBeforeLocalApplyHook installs beforeLocalApplyHook for the test.
func setBeforeLocalApplyHook(t *testing.T, hook func(nodeID string, repl ReplEntry)) {
	t.Helper()
	beforeLocalApplyHook.Store(&hook)
	t.Cleanup(func() { beforeLocalApplyHook.Store(nil) })
}

// uniqueCluster starts count nodes at rf with testDatabase/Users declaring a
// unique index on Email, and returns the cluster and the bucket owner's index.
func uniqueCluster(t *testing.T, count, rf int) (*testCluster, int) {
	t.Helper()
	tc := newTestCluster(t, count, rf)
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	created := tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "Users", Type: BucketTypeHash,
		Options: map[string]interface{}{"placement": "whole", "indices": []interface{}{uniqueOn("Email")}}})
	var resp CreateBucketResponse
	if err := bson.Unmarshal(created.Payload, &resp); err != nil || !resp.Success {
		t.Fatalf("creating the unique bucket = %+v, %v", resp, err)
	}
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, "Users"))
	if err != nil {
		t.Fatalf("resolving the owner: %v", err)
	}
	return tc, nodeIndexByAddr(t, tc.addrs, owner.ID)
}

func putEmail(t *testing.T, tc *testCluster, index int, key, email string) *transport.Frame {
	t.Helper()
	return tc.frame(index, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: "Users", Key: key,
		Data: map[string]interface{}{"Email": email}})
}

// refusalOf decodes an error frame, failing unless it is one.
func refusalOf(t *testing.T, frame *transport.Frame) ErrorResponse {
	t.Helper()
	if frame.Opcode != transport.OpError {
		t.Fatalf("answered opcode %s (%s), want an error frame", frame.Opcode, string(frame.Payload))
	}
	var answer ErrorResponse
	if err := bson.Unmarshal(frame.Payload, &answer); err != nil {
		t.Fatalf("decoding the error frame: %v", err)
	}
	return answer
}

// holdsDocument reports whether node index's own copy of Users holds key. A
// node that is a holder but whose catalog has not learned Users yet does not
// hold the document: a bucket's create replicates asynchronously, so a poll
// that uses this helper must read that window as "not yet", not as a fatal
// error (BUG-0238).
func holdsDocument(t *testing.T, tc *testCluster, index int, key string) bool {
	t.Helper()
	found, err := tc.nodes[index].server.core.Find(FindRequest{Database: testDatabase, Bucket: "Users", Key: key})
	if errors.Is(err, ErrBucketNotFound) {
		return false
	}
	if err != nil {
		t.Fatalf("Find(%s) on node %d: %v", key, index+1, err)
	}
	return found.Found
}

// TestHoldsDocumentAbsorbsACatalogThatLags (BUG-0238): the create of Users is
// held at one replica's admission, so that node is guaranteed to be a holder
// whose catalog has not learned the bucket. A poll on holdsDocument must wait
// for the create rather than fail on the replica's "bucket not found"; the
// pre-fix fatal is exactly what reddened a full-suite run.
func TestHoldsDocumentAbsorbsACatalogThatLags(t *testing.T) {
	tc := newTestCluster(t, 3, 3)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	owner, err := tc.nodes[0].server.core.cluster.Owner(placementOf(testDatabase, "Users"))
	if err != nil {
		t.Fatalf("resolving Users' owner: %v", err)
	}
	ownerIndex := nodeIndexByAddr(t, tc.addrs, owner.ID)
	lagging := ownerIndex
	for index := range tc.nodes {
		if index != ownerIndex {
			lagging = index
			break
		}
	}
	if lagging == ownerIndex {
		t.Fatalf("setup: no replica of Users to hold behind its create")
	}

	reached := make(chan struct{})
	release := make(chan struct{})
	var reachedOnce, releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAll()
	laggingID := tc.addrs[lagging]
	admitted := func(c *Cluster, entry ReplEntry) {
		if c.self.ID != laggingID || transport.Opcode(entry.Op) != transport.OpCreateBucket || entry.Bucket != "Users" {
			return
		}
		reachedOnce.Do(func() { close(reached) })
		<-release
	}
	afterReplicaAdmittedHook.Store(&admitted)
	defer afterReplicaAdmittedHook.Store(nil)

	tc.ok(0, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: "Users", Type: BucketTypeHash})
	select {
	case <-reached:
	case <-time.After(15 * time.Second):
		t.Fatalf("setup: node %d's create of Users was never held", lagging+1)
	}

	answer := tc.frame(ownerIndex, transport.OpBatchPut, BatchInsertRequest{
		Database:     testDatabase,
		Bucket:       "Users",
		Data:         []map[string]interface{}{{"_id_": "lag1", "Email": "lag@example.com"}},
		WriteConcern: "one",
	})
	if answer.Opcode == transport.OpError {
		t.Fatalf("the batch at the owner: %s", string(answer.Payload))
	}
	// The create is still held on the replica, so its catalog has not learned
	// Users: this read must answer "not held yet", not fail.
	if holdsDocument(t, tc, lagging, "lag1") {
		t.Fatalf("setup: node %d holds the document although its create is still held", lagging+1)
	}
	releaseAll()
	pollUntil(t, "node holds lag1", func() bool { return holdsDocument(t, tc, lagging, "lag1") })
}

// waitForAllWALs waits until every node's WAL has passed its recorded
// sequence by at least grew entries.
func waitForAllWALs(t *testing.T, tc *testCluster, base []uint64, grew uint64) {
	t.Helper()
	for i, node := range tc.nodes {
		waitForWALSequence(t, node, base[i]+grew)
	}
}

func walTails(tc *testCluster) []uint64 {
	tails := make([]uint64, len(tc.nodes))
	for i, node := range tc.nodes {
		tails[i] = node.server.core.wal.LastSequence()
	}
	return tails
}

// walGrowthReport is one line per node: its WAL tail before and after the
// refused duplicate and the keys of the entries it appended in between. A
// failed assertion prints it so the extra entry itself says whether the log
// gained the refused duplicate (a real ordering defect) or a valid write that
// arrived late (the load shape the baseline wait removes).
func walGrowthReport(t *testing.T, tc *testCluster, base, tails []uint64) string {
	t.Helper()
	var report strings.Builder
	for i, node := range tc.nodes {
		entries, err := node.server.core.wal.ReadFrom(base[i] + 1)
		if err != nil {
			t.Fatalf("reading node %d's WAL after sequence %d: %v", i+1, base[i], err)
		}
		keys := make([]string, 0, len(entries))
		for _, entry := range entries {
			label, ok := bson.RawValue{Type: bson.TypeString, Value: entry.Key}.StringValueOK()
			if !ok {
				label = fmt.Sprintf("%q", entry.Key)
			}
			keys = append(keys, fmt.Sprintf("%s(seq %d)", label, entry.Seq))
		}
		fmt.Fprintf(&report, "\n  node %d: before=%d after=%d grew=%d new=%v",
			i+1, base[i], tails[i], tails[i]-base[i], keys)
	}
	return report.String()
}

// counterValue reads a counter from the default registry by name, 0 when it
// is not registered.
func gatheredCounter(t *testing.T, name string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() == name {
			total := 0.0
			for _, metric := range family.GetMetric() {
				total += metric.GetCounter().GetValue()
			}
			return total
		}
	}
	return 0
}

// TestUniqueIndexRefusesSecondDocumentThroughANonOwner (spec §10's cluster
// variant): a duplicate sent to a node that does not own the bucket is refused
// with the duplicate's own code.
func TestUniqueIndexRefusesSecondDocumentThroughANonOwner(t *testing.T) {
	tc, owner := uniqueCluster(t, 3, 3)
	defer tc.stop()
	other := (owner + 1) % len(tc.nodes)
	if frame := putEmail(t, tc, other, "doc1", "dalek@example.com"); frame.Opcode == transport.OpError {
		t.Fatalf("the first write through a non-owner: %s", string(frame.Payload))
	}
	answer := refusalOf(t, putEmail(t, tc, other, "doc2", "dalek@example.com"))
	if answer.Code != wantDuplicateKeyCode || !strings.Contains(answer.Message, `"doc1"`) {
		t.Fatalf("a duplicate through a non-owner answered code %d, %q; want code %d naming doc1", answer.Code, answer.Message, wantDuplicateKeyCode)
	}
}

// walTailsWithOwnersLast is every node's WAL tail once the owner's last entry
// is in it: what the owner acknowledged at majority one replica may apply
// after the answer.
func walTailsWithOwnersLast(t *testing.T, tc *testCluster, owner int) []uint64 {
	t.Helper()
	ownersLast := tc.nodes[owner].server.core.wal.LastSequence()
	for index, node := range tc.nodes {
		if index == owner {
			continue
		}
		pollUntil(t, fmt.Sprintf("node %d's WAL holds the owner's entry %d", index+1, ownersLast), func() bool {
			return holdsOwnerEntry(t, node, tc.addrs[owner], ownersLast, node.server.core.wal.LastSequence())
		})
	}
	return walTails(tc)
}

// holdsOwnerEntry reports whether node's WAL, up to sequence upTo, holds the
// entry it applied from source's entry seq.
func holdsOwnerEntry(t *testing.T, node *testNode, source string, seq, upTo uint64) bool {
	t.Helper()
	entries, err := node.server.core.wal.ReadFrom(1)
	if err != nil {
		t.Fatalf("reading %s's WAL: %v", node.addr, err)
	}
	return slices.ContainsFunc(entries, func(entry storage.WALEntry) bool {
		return entry.Seq <= upTo && entry.Origin.Source == source && entry.Origin.Seq == seq
	})
}

// TestTheWALBaseHoldsTheOwnersLastWriteOnEveryNode (BUG-0487): the base of
// TestPreCheckKeepsTheWALClean is read after a write acknowledged at majority,
// so a replica that applies it after the answer grows by it and by the next
// write ("grew by 2 entries"). The base is read only once every replica holds
// the owner's last entry. Here the owner's stream to one replica is held past
// the answer.
func TestTheWALBaseHoldsTheOwnersLastWriteOnEveryNode(t *testing.T) {
	tc, owner := uniqueCluster(t, 3, 3)
	defer tc.stop()
	lagging := (owner + 1) % len(tc.nodes)
	release := holdStreamPushesOnly(peerOf(t, tc, owner, lagging))
	defer release()
	if frame := putEmail(t, tc, owner, "doc1", "dalek@example.com"); frame.Opcode == transport.OpError {
		t.Fatalf("write doc1: %s", string(frame.Payload))
	}
	ownersLast := tc.nodes[owner].server.core.wal.LastSequence()
	time.AfterFunc(time.Second, release)
	base := walTailsWithOwnersLast(t, tc, owner)
	if !holdsOwnerEntry(t, tc.nodes[lagging], tc.addrs[owner], ownersLast, base[lagging]) {
		t.Errorf("node %d's WAL base %d does not hold the owner's entry %d", lagging+1, base[lagging], ownersLast)
	}
}

// TestPreCheckKeepsTheWALClean: on an idle bucket a refused duplicate is
// refused on the owner before its append, so no node's WAL gains an entry
// for it — a duplicate costs one seek, not an append, an apply and a ship to
// every replica (build doc §6.2 "What a violating insert costs").
func TestPreCheckKeepsTheWALClean(t *testing.T) {
	tc, owner := uniqueCluster(t, 3, 3)
	defer tc.stop()
	if frame := putEmail(t, tc, owner, "doc1", "dalek@example.com"); frame.Opcode == transport.OpError {
		t.Fatalf("write doc1: %s", string(frame.Payload))
	}
	// The baseline is read only once every node holds doc1, so what follows is
	// the refused duplicate's cost alone (BUG-0149).
	base := walTailsWithOwnersLast(t, tc, owner)
	refusalOf(t, putEmail(t, tc, owner, "doc2", "dalek@example.com"))
	// A valid write after the refusal, so every node has caught up with the
	// owner's log before the tails are compared.
	if frame := putEmail(t, tc, owner, "doc3", "other@example.com"); frame.Opcode == transport.OpError {
		t.Fatalf("write doc3: %s", string(frame.Payload))
	}
	waitForAllWALs(t, tc, base, 1)
	time.Sleep(200 * time.Millisecond)
	tails := walTails(tc)
	for i, tail := range tails {
		if grew := tail - base[i]; grew != 1 {
			t.Errorf("node %d's WAL grew by %d entries for one refused duplicate and one write, want 1%s",
				i+1, grew, walGrowthReport(t, tc, base, tails))
		}
	}
}

// TestPreCheckAndApplyCheckGiveOneError (§4 F-2(b)): the refusal a client
// gets is the same whether the owner's pre-check caught the duplicate or the
// apply did — the same violation must not look like two errors depending on
// timing.
func TestPreCheckAndApplyCheckGiveOneError(t *testing.T) {
	tc, owner := uniqueCluster(t, 2, 1)
	defer tc.stop()
	ownerCore := tc.nodes[owner].server.core
	var planted sync.Once
	setBeforeLocalApplyHook(t, func(_ string, entry ReplEntry) {
		var req InsertRequest
		if bson.Unmarshal(entry.Payload, &req) != nil || req.Key != "late" {
			return
		}
		planted.Do(func() {
			if refusal := insertOutcome(t, ownerCore, testDatabase, "Users", "holder", map[string]interface{}{"Email": "dalek@example.com"}); refusal != "" {
				t.Errorf("planting the holder: %s", refusal)
			}
		})
	})
	inApply := refusalOf(t, putEmail(t, tc, owner, "late", "dalek@example.com"))
	preChecked := refusalOf(t, putEmail(t, tc, owner, "late", "dalek@example.com"))
	if inApply.Code != preChecked.Code || inApply.Message != preChecked.Message {
		t.Fatalf("one violation gave two errors:\n  in the apply: %d %q\n  pre-checked:  %d %q",
			inApply.Code, inApply.Message, preChecked.Code, preChecked.Message)
	}
	if inApply.Code != wantDuplicateKeyCode {
		t.Fatalf("the refusal's code is %d, want %d", inApply.Code, wantDuplicateKeyCode)
	}
}

// holdFirstApplyOf holds the first local apply of the put of key — the
// owner's — until release is closed, and closes held when it arrives.
func holdFirstApplyOf(t *testing.T, key string) (held, release chan struct{}) {
	t.Helper()
	held, release = make(chan struct{}), make(chan struct{})
	var once sync.Once
	setBeforeLocalApplyHook(t, func(_ string, entry ReplEntry) {
		var req InsertRequest
		if bson.Unmarshal(entry.Payload, &req) != nil || req.Key != key {
			return
		}
		once.Do(func() {
			close(held)
			<-release
		})
	})
	return held, release
}

// raceTwoWritesPastThePreCheck sends first and then second, both carrying one
// Email, so that second passes the owner's pre-check — first is appended but
// held before its apply — and is appended behind it. It returns both answers.
func raceTwoWritesPastThePreCheck(t *testing.T, tc *testCluster, owner int, first, second string) (*transport.Frame, *transport.Frame) {
	t.Helper()
	held, release := holdFirstApplyOf(t, first)
	base := tc.nodes[owner].server.core.wal.LastSequence()
	answers := make(chan *transport.Frame, 1)
	go func() {
		answers <- sendPutOn(t, tc, owner, first, "dalek@example.com")
	}()
	select {
	case <-held:
	case <-time.After(10 * time.Second):
		t.Fatal("the first write never reached its apply")
	}
	secondAnswer := make(chan *transport.Frame, 1)
	go func() {
		secondAnswer <- sendPutOn(t, tc, owner, second, "dalek@example.com")
	}()
	waitForWALSequence(t, tc.nodes[owner], base+2)
	close(release)
	return <-answers, <-secondAnswer
}

// sendPutOn writes key on its own connection to node index, so two writes
// can be in flight at once.
func sendPutOn(t *testing.T, tc *testCluster, index int, key, email string) *transport.Frame {
	t.Helper()
	client := tc.dial(index)
	defer func() { _ = client.Close() }()
	payload, err := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: "Users", Key: key, Data: map[string]interface{}{"Email": email}})
	if err != nil {
		t.Errorf("marshal: %v", err)
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	frame, err := client.SendRequest(ctx, transport.OpPut, payload)
	if err != nil {
		t.Errorf("put %s: %v", key, err)
		return nil
	}
	return frame
}

// TestPostAppendRejectionIsDeterministicOnEveryReplica (spec §4.4.3): two
// writes of one value both pass the pre-check and are appended; the apply
// ticket orders them by sequence on the owner and on every replica, so the
// same entry meets the same state everywhere — every node holds the first and
// not the second.
func TestPostAppendRejectionIsDeterministicOnEveryReplica(t *testing.T) {
	tc, owner := uniqueCluster(t, 3, 3)
	defer tc.stop()
	base := walTails(tc)
	firstAnswer, secondAnswer := raceTwoWritesPastThePreCheck(t, tc, owner, "first", "second")
	if firstAnswer == nil || firstAnswer.Opcode == transport.OpError {
		t.Fatalf("the first write was refused: %v", firstAnswer)
	}
	refusalOf(t, secondAnswer)
	waitForAllWALs(t, tc, base, 2)
	// A failure prints every node's WAL before and after and which of the two
	// documents it holds. A node that reached base+2 and holds the second, or
	// neither, is a divergent replica (a real ordering defect); one that has
	// not reached base+2 yet is the load shape this file's criteria describe.
	holds := func(i int) (bool, bool) {
		return holdsDocument(t, tc, i, "first"), holdsDocument(t, tc, i, "second")
	}
	report := func() string {
		var b strings.Builder
		b.WriteString(walGrowthReport(t, tc, base, walTails(tc)))
		for i := range tc.nodes {
			first, second := holds(i)
			fmt.Fprintf(&b, "\n  node %d holds first=%v second=%v", i+1, first, second)
		}
		return b.String()
	}
	deadline := time.Now().Add(10 * time.Second)
	for i := range tc.nodes {
		for !holdsDocument(t, tc, i, "first") && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if first, second := holds(i); !first || second {
			t.Errorf("node %d holds first=%v second=%v, want the first and not the second%s",
				i+1, first, second, report())
		}
	}
}

// TestDuplicateRejectionIsNotAReplayFailure (spec §10): a WAL holding a
// refused duplicate never replays as a failure — the operator-facing
// "skipped" count is for entries that should have applied. The
// live refusal is consumed, its sequence below the mark, so replay skips the
// entry outright rather than meet it and reject it again: neither count moves,
// and the refused document stays absent.
func TestDuplicateRejectionIsNotAReplayFailure(t *testing.T) {
	recorder := recordClusterLogs(t)
	tc, owner := uniqueCluster(t, 3, 3)
	defer tc.stop()
	firstAnswer, secondAnswer := raceTwoWritesPastThePreCheck(t, tc, owner, "first", "second")
	if firstAnswer == nil || firstAnswer.Opcode == transport.OpError {
		t.Fatalf("the first write was refused: %v", firstAnswer)
	}
	refusalOf(t, secondAnswer)
	tc.restartNode(owner)
	var replayed []recordedEntry
	for _, entry := range recorder.matching(slog.LevelInfo, "replayed write-ahead log") {
		if entry.attrs["rejected_by_constraint"] != "" || entry.attrs["entries"] != "" {
			replayed = append(replayed, entry)
		}
	}
	if len(replayed) == 0 {
		t.Fatal("the restarted owner logged no replay line")
	}
	last := replayed[len(replayed)-1]
	if last.attrs["rejected_by_constraint"] != "0" || last.attrs["skipped"] != "0" {
		t.Fatalf("replay reported %v, want rejected_by_constraint=0 and skipped=0: the live refusal was consumed", last.attrs)
	}
	if holdsDocument(t, tc, owner, "second") {
		t.Fatal("after the owner restarted it holds the duplicate it refused")
	}
}

// TestReplayCountsARefusalItMeetsAndLeavesItAboveTheMark: an entry logged and
// not applied before a crash is above the bucket's mark when replay meets it,
// so a unique-index duplicate among those entries is refused at replay and
// counted rejected_by_constraint, not skipped. The entry is placed in the log
// by hand because BUG-0246 consumes a live unique-index refusal, so the path
// TestDuplicateRejectionIsNotAReplayFailure used to take no longer reaches
// replay; the type and path refusal tests cover the branch for their classes,
// but the unique-index class BUG-0246 changed had no test.
func TestReplayCountsARefusalItMeetsAndLeavesItAboveTheMark(t *testing.T) {
	recorder := recordClusterLogs(t)
	tc, owner := uniqueCluster(t, 3, 3)
	defer tc.stop()
	putEmail(t, tc, owner, "first", "dalek@example.com")
	if !holdsDocument(t, tc, owner, "first") {
		t.Fatal("setup: the first document is not on the owner")
	}

	duplicate, err := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: "Users", Key: "second",
		Data: map[string]interface{}{"Email": "dalek@example.com"}})
	if err != nil {
		t.Fatalf("encoding the duplicate: %v", err)
	}
	restartNodeAfterEditingItsWAL(t, tc, owner, func(wal *storage.WAL) {
		if _, err := wal.AppendFrom("Users", byte(transport.OpPut), []byte("second"), duplicate,
			storage.WALOrigin{Kind: storage.WALOriginOwn}); err != nil {
			t.Fatalf("placing the duplicate in the owner's log: %v", err)
		}
	})

	replayed := recorder.matching(slog.LevelInfo, "replayed write-ahead log")
	if len(replayed) == 0 {
		t.Fatal("the restarted owner logged no replay line")
	}
	last := replayed[len(replayed)-1]
	if last.attrs["rejected_by_constraint"] != "1" || last.attrs["skipped"] != "0" {
		t.Fatalf("replay reported %v, want rejected_by_constraint=1 and skipped=0: the refusal met at replay was not counted", last.attrs)
	}
	if holdsDocument(t, tc, owner, "second") {
		t.Fatal("the replay stored the duplicate it refused")
	}
}

// TestRecordedFailureInAUniqueBucketNeverCommutes (§4 F-3, S-6): doc1's apply
// fails on the owner and is recorded; its replicas apply it; doc2, with the
// same Email and another key, is then accepted on the owner. Key-only
// commutation lets replay apply doc1 beneath the mark, where its outcome is
// the opposite of the replicas' — silently. In a bucket declaring a unique
// index a recorded failure never commutes: replay takes the loud path.
func TestRecordedFailureInAUniqueBucketNeverCommutes(t *testing.T) {
	tc, owner := uniqueCluster(t, 3, 3)
	defer tc.stop()
	failed := failNextApplyOn(t, tc.nodes[owner].server.core.kv, anySeq)
	putEmail(t, tc, owner, "doc1", "dalek@example.com")
	if failed.Load() == 0 {
		t.Fatal("doc1's apply never reached the owner's store; the test failed nothing")
	}
	if holdsDocument(t, tc, owner, "doc1") {
		t.Fatal("precondition: doc1 is on the owner although its apply failed")
	}
	if frame := putEmail(t, tc, owner, "doc2", "dalek@example.com"); frame.Opcode == transport.OpError {
		t.Fatalf("doc2 was refused on the owner, which does not hold doc1: %s", string(frame.Payload))
	}
	before := notReplayedCount(t)
	tc.restartNode(owner)
	if !holdsDocument(t, tc, owner, "doc2") || holdsDocument(t, tc, owner, "doc1") {
		t.Fatalf("after replay the owner holds doc1=%v doc2=%v, want doc2 only", holdsDocument(t, tc, owner, "doc1"), holdsDocument(t, tc, owner, "doc2"))
	}
	if moved := notReplayedCount(t) - before; moved != 1 {
		t.Fatalf("hoarddb_apply_failures_not_replayed_total moved by %v, want 1: the recorded failure was consumed without the loud path", moved)
	}
}

// TestReplicaThatDisagreesWithTheOwnerIsLoud (§4 F-1, S-5): a replica whose
// state differs from its owner's at a position refuses a write its owner
// accepted, acknowledges the push, and the client is told the write
// succeeded. The owner's outcome travels on the entry, and the replica that
// disagrees logs one ERROR and counts it. An entry whose owner did not say —
// an owner from before the field — reports nothing.
func TestReplicaThatDisagreesWithTheOwnerIsLoud(t *testing.T) {
	recorder := recordClusterLogs(t)
	tc, owner := uniqueCluster(t, 3, 3)
	defer tc.stop()
	replica := (owner + 1) % len(tc.nodes)
	replicaCore := tc.nodes[replica].server.core
	// The create reaches a replica by replication; plant only once it has.
	deadline := time.Now().Add(10 * time.Second)
	for !replicaCore.bucketStoreOpen(testDatabase, "Users") {
		if time.Now().After(deadline) {
			t.Fatal("the bucket never reached the replica")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if refusal := insertOutcome(t, replicaCore, testDatabase, "Users", "planted", map[string]interface{}{"Email": "dalek@example.com"}); refusal != "" {
		t.Fatalf("planting doc1 on one replica: %s", refusal)
	}
	const counter = "hoarddb_replica_constraint_disagreements_total"
	before := gatheredCounter(t, counter)
	base := walTails(tc)
	if frame := putEmail(t, tc, owner, "doc2", "dalek@example.com"); frame.Opcode == transport.OpError {
		t.Fatalf("the owner refused doc2, which it has no holder for: %s", string(frame.Payload))
	}
	waitForAllWALs(t, tc, base, 1)
	deadline = time.Now().Add(5 * time.Second)
	for gatheredCounter(t, counter) == before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if moved := gatheredCounter(t, counter) - before; moved != 1 {
		t.Errorf("%s moved by %v, want 1", counter, moved)
	}
	if loud := recorder.matching(slog.LevelError, "Users"); len(loud) != 1 {
		t.Errorf("%d ERROR lines name the bucket, want 1: %+v", len(loud), loud)
	}

	payload, err := bson.Marshal(InsertRequest{Database: testDatabase, Bucket: "Users", Key: "doc3", Data: map[string]interface{}{"Email": "dalek@example.com"}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	quietBefore := gatheredCounter(t, counter)
	if _, err := replicaCore.cluster.applyAsReplica(context.Background(), ReplEntry{Seq: 1 << 40, Op: int32(transport.OpPut), Bucket: "Users", Payload: payload,
		Term: bucketTermOn(t, replicaCore, testDatabase, "Users")}, true, "test-owner", false); err != nil {
		t.Fatalf("applying an entry with no owner outcome: %v", err)
	}
	if gatheredCounter(t, counter) != quietBefore {
		t.Error("an entry whose owner gave no outcome was reported as a disagreement")
	}
}

// TestMigratedUniqueBucketIsVerifiedOnArrival (§4 F-1's repair path, C-46): a
// migrated range lands raw, index entries included, and nothing checked them.
// A target that receives a source's duplicate marks the unique index
// conflicted instead of serving it ready.
func TestMigratedUniqueBucketIsVerifiedOnArrival(t *testing.T) {
	tc := newTestCluster(t, 3, 1)
	defer tc.stop()
	tc.ok(0, transport.OpCreateDB, CreateDatabaseRequest{Name: testDatabase})
	const users = "unique_users"
	pair := migrationPairFor(t, tc, users)
	tc.ok(pair.source, transport.OpCreateBucket, CreateBucketRequest{Database: testDatabase, Name: users, Type: BucketTypeHash,
		Options: map[string]interface{}{"placement": "whole", "indices": []interface{}{uniqueOn("Email")}}})
	tc.ok(pair.source, transport.OpPut, InsertRequest{Database: testDatabase, Bucket: users, Key: "holder",
		Data: map[string]interface{}{"Email": "same@example.com"}})
	source, target := tc.nodes[pair.source].server.core, tc.nodes[pair.target].server.core

	// Plant a second holder on the source past the check: its document and its
	// index entry, raw.
	planted := servedBucket(t, source, testDatabase, users)
	document, err := bson.Marshal(map[string]interface{}{"_id_": "planted", "Email": "same@example.com"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	encoded, err := store.EncodeComponent("same@example.com")
	if err != nil {
		t.Fatalf("EncodeComponent: %v", err)
	}
	batch := source.kv.NewBatch()
	for key, value := range map[string][]byte{
		string(storage.Key(planted.slot, planted.databaseID, planted.id.Load(), storage.NSDocuments, []byte("planted"))): document,
		string(append(append(indexEntryPrefix(planted, 0), encoded...), "planted"...)):                                   {},
	} {
		if err := batch.Set([]byte(key), value); err != nil {
			t.Fatalf("planting: %v", err)
		}
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing the plant: %v", err)
	}
	batch.Discard()

	pair.pull(t, tc, users)
	stored := storedCatalogIndices(t, target, testDatabase, users)
	if len(stored) != 1 || stored[0]["state"] != "conflicted" {
		t.Fatalf("the target records %v after receiving a duplicate, want the unique index conflicted", stored)
	}
}
