package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// ledgerEntryOf is the entry for author in a decoded ledger.
func ledgerEntryOf(t *testing.T, ledger ackLedgerFile, author int) ackEntry {
	t.Helper()
	for _, entry := range ledger.Authors {
		if entry.User == author {
			return entry
		}
	}
	t.Fatalf("the ledger has no entry for author %d: %+v", author, ledger.Authors)
	return ackEntry{}
}

// TestLedgerRecordsHighestAcknowledgedPostPerAuthor: three authors, posts
// acknowledged in turn and one left in flight; the ledger keeps each author's
// highest acknowledged sequence, and the in-flight one apart.
func TestLedgerRecordsHighestAcknowledgedPostPerAuthor(t *testing.T) {
	graph := newSocialGraph(tinySpec, datasetSeed)
	book := newSocialBook(graph, false)
	ctx := context.Background()
	base := int64(graph.postsPerUser)
	for round := 0; round < 3; round++ {
		for _, author := range []int{1, 2, 3} {
			seq, finish, err := book.beginPost(ctx, nil, author)
			if err != nil {
				t.Fatalf("beginPost(%d): %v", author, err)
			}
			if seq != base+int64(round) {
				t.Fatalf("author %d round %d got seq %d, want %d", author, round, seq, base+int64(round))
			}
			finish(true)
		}
	}
	// Author 3 sends one more post and the reset lands before the reply.
	if _, _, err := book.beginPost(ctx, nil, 3); err != nil {
		t.Fatalf("beginPost(3): %v", err)
	}
	ledger := book.acks.snapshot(placementOfShards(1), nil)
	for _, author := range []int{1, 2} {
		entry := ledgerEntryOf(t, ledger, author)
		if entry.Acked == nil || *entry.Acked != base+2 || entry.InFlight != nil {
			t.Errorf("author %d: acked %v in-flight %v; want acked %d and none in flight", author, entry.Acked, entry.InFlight, base+2)
		}
	}
	entry := ledgerEntryOf(t, ledger, 3)
	if entry.Acked == nil || *entry.Acked != base+2 || entry.InFlight == nil || *entry.InFlight != base+3 {
		t.Errorf("author 3: acked %v in-flight %v; want acked %d and in flight %d", entry.Acked, entry.InFlight, base+2, base+3)
	}
	if len(ledger.Authors) != 3 {
		t.Errorf("the ledger lists %d authors, want only the 3 that posted", len(ledger.Authors))
	}
}

// TestLedgerKeepsAFailedPostInFlight: a post whose outcome is unknown is never
// promoted to acknowledged, and a later acknowledged post does not clear it
// when it is the newest unacknowledged one.
func TestLedgerKeepsAFailedPostInFlight(t *testing.T) {
	graph := newSocialGraph(tinySpec, datasetSeed)
	book := newSocialBook(graph, false)
	seq, finish, err := book.beginPost(context.Background(), nil, 5)
	if err != nil {
		t.Fatal(err)
	}
	finish(false)
	entry := ledgerEntryOf(t, book.acks.snapshot(placementOfShards(1), nil), 5)
	if entry.Acked != nil || entry.InFlight == nil || *entry.InFlight != seq {
		t.Errorf("a failed post left acked %v in-flight %v; want no acknowledged post and %d in flight", entry.Acked, entry.InFlight, seq)
	}
}

// writeTestPosts publishes posts base..base+count-1 of author through ops.
func writeTestPosts(t *testing.T, world *socialWorld, author int, first, count int64) {
	t.Helper()
	ops, err := world.engine.newSocialOps()
	if err != nil {
		t.Fatal(err)
	}
	defer ops.close()
	for seq := first; seq < first+count; seq++ {
		post := postRecord{Author: userID(author), Seq: seq, Text: "ledger test", Created: time.Now().UTC().Truncate(time.Millisecond), Tags: []string{"t"}}
		if _, err := ops.publishPost(context.Background(), post, noLedger{}); err != nil {
			t.Fatalf("publishing %d/%d: %v", author, seq, err)
		}
	}
}

func int64Ptr(value int64) *int64 { return &value }

// TestLedgerCheckFailsOnAMissingAcknowledgedPost: the ledger says author 4
// acknowledged a post the timeline does not hold; the check names the author
// and the sequence and fails.
func TestLedgerCheckFailsOnAMissingAcknowledgedPost(t *testing.T) {
	world := newFakeWorld(t)
	base := int64(world.graph.postsPerUser)
	writeTestPosts(t, world, 4, base, 2)
	ledger := ackLedgerFile{Schema: ackLedgerSchema, Authors: []ackEntry{
		{User: 4, Author: userID(4), Acked: int64Ptr(base + 3)}, // base+2 and base+3 were never written
		{User: 4 + 1, Author: userID(5), Acked: int64Ptr(base - 1)},
	}}
	var out bytes.Buffer
	err := runLedgerCheckOn(context.Background(), &config{Engine: engineHoardDB}, world.social, ledger, &out)
	if err == nil || !strings.Contains(err.Error(), userID(4)) || !strings.Contains(err.Error(), "seq "+itoa(base+3)) {
		t.Fatalf("error = %v; want one naming author %s and seq %d", err, userID(4), base+3)
	}
	var result ledgerCheckResult
	if jsonErr := json.Unmarshal(out.Bytes(), &result); jsonErr != nil || result.Pass || len(result.Missing) != 1 || result.Missing[0].Seq != base+3 {
		t.Errorf("printed %q (%v); want one failing result listing the one missing post", out.String(), jsonErr)
	}
}

func itoa(value int64) string { return strconv.FormatInt(value, 10) }

// TestLedgerCheckPassesWhenEveryAcknowledgedPostIsThere: the passing case, so
// the failing one above is not a check that always fails.
func TestLedgerCheckPassesWhenEveryAcknowledgedPostIsThere(t *testing.T) {
	world := newFakeWorld(t)
	base := int64(world.graph.postsPerUser)
	writeTestPosts(t, world, 4, base, 3)
	ledger := ackLedgerFile{Schema: ackLedgerSchema, Authors: []ackEntry{{User: 4, Author: userID(4), Acked: int64Ptr(base + 2)}}}
	var out bytes.Buffer
	if err := runLedgerCheckOn(context.Background(), &config{Engine: engineHoardDB}, world.social, ledger, &out); err != nil {
		t.Fatalf("a complete timeline failed the check: %v", err)
	}
}

// TestLedgerCheckReportsTheInFlightPostEitherWay: present or absent, the
// in-flight post is reported and never fails the check.
func TestLedgerCheckReportsTheInFlightPostEitherWay(t *testing.T) {
	world := newFakeWorld(t)
	base := int64(world.graph.postsPerUser)
	writeTestPosts(t, world, 4, base, 2) // base+1 is the in-flight post and it landed
	ledger := ackLedgerFile{Schema: ackLedgerSchema, Authors: []ackEntry{
		{User: 4, Author: userID(4), Acked: int64Ptr(base), InFlight: int64Ptr(base + 1)},
		{User: 6, Author: userID(6), Acked: int64Ptr(base - 1), InFlight: int64Ptr(base)}, // never written
	}}
	var out bytes.Buffer
	if err := runLedgerCheckOn(context.Background(), &config{Engine: engineHoardDB}, world.social, ledger, &out); err != nil {
		t.Fatalf("an in-flight post failed the check: %v", err)
	}
	var result ledgerCheckResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.InFlightPresent != 1 || result.InFlightAbsent != 1 || !result.Pass {
		t.Errorf("in-flight present %d absent %d pass %v; want 1, 1, true", result.InFlightPresent, result.InFlightAbsent, result.Pass)
	}
}

// TestLedgerCheckCanBeScopedToOneOwner: with the reads of a lone node covering
// only the buckets it owns (NR-2), -ledger-owner limits the check to them.
func TestLedgerCheckCanBeScopedToOneOwner(t *testing.T) {
	world := newFakeWorld(t)
	base := int64(world.graph.postsPerUser)
	ledger := ackLedgerFile{Schema: ackLedgerSchema, Authors: []ackEntry{
		{User: 4, Author: userID(4), Owner: "node-a", Acked: int64Ptr(base + 9)}, // missing, but owned by the other node
		{User: 5, Author: userID(5), Owner: "node-b", Acked: int64Ptr(base - 1)},
	}}
	var out bytes.Buffer
	if err := runLedgerCheckOn(context.Background(), &config{Engine: engineHoardDB, LedgerOwner: "node-b"}, world.social, ledger, &out); err != nil {
		t.Fatalf("an author owned by the other node failed a scoped check: %v", err)
	}
	var result ledgerCheckResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil || result.Checked != 1 || result.Skipped != 1 {
		t.Errorf("checked %d skipped %d (%v); want 1 and 1", result.Checked, result.Skipped, err)
	}
}

// failAfterCaller lets budget calls through and then fails every call, as a
// connection to a node that has been reset does.
type failAfterCaller struct {
	wireCaller
	budget *int64
}

func (c failAfterCaller) call(op transport.Opcode, request, response any) error {
	if *c.budget <= 0 {
		return errors.New("connection reset by peer")
	}
	*c.budget--
	return c.wireCaller.call(op, request, response)
}

// TestLedgerSurvivesAClientSideErrorStream: the connection drops mid-run, as
// it does when a node is reset; the ledger is still written at the end, with
// the posts acknowledged before the drop.
func TestLedgerSurvivesAClientSideErrorStream(t *testing.T) {
	cfg := mixTestConfig("wh", "social_ledger")
	cfg.Conc, cfg.Duration = 1, 300*time.Millisecond
	cfg.LedgerOut = filepath.Join(t.TempDir(), "ledger.json")
	fake := newFakeServer()
	budget := new(int64)
	engine := &hoardEngine{cfg: cfg, dial: func(string) (wireCaller, func(), error) {
		return failAfterCaller{wireCaller: fake.dial(nil, nil), budget: budget}, func() {}, nil
	}}
	*budget = 1 << 40 // seed and warm up freely
	graph := newSocialGraph(tinySpec, datasetSeed)
	if _, err := engine.setupSocial(context.Background(), tinySpec); err != nil {
		t.Fatal(err)
	}
	if err := engine.seedSocial(context.Background(), graph, 1); err != nil {
		t.Fatal(err)
	}
	cfg.Attach = true
	*budget = 400 // enough for some publishes, then the stream of errors
	record, err := runMixLeg(context.Background(), cfg, tinySpec, engine)
	if err != nil {
		t.Fatalf("the leg failed: %v", err)
	}
	if len(record.ErrorsByType) == 0 {
		t.Fatalf("the leg saw no client-side errors, so the test did not exercise the drop")
	}
	encoded, err := os.ReadFile(cfg.LedgerOut)
	if err != nil {
		t.Fatalf("no ledger was written after the error stream: %v", err)
	}
	var ledger ackLedgerFile
	if err := json.Unmarshal(encoded, &ledger); err != nil {
		t.Fatalf("the ledger is not JSON: %v", err)
	}
	acked := 0
	for _, entry := range ledger.Authors {
		if entry.Acked != nil {
			acked++
		}
	}
	if acked == 0 {
		t.Errorf("the ledger holds no acknowledged post although publishes were acknowledged before the drop: %s", encoded)
	}
}

// TestLedgerFlagsParse: -ledger-out, -ledger and -ledger-owner are read, and
// ledgercheck refuses to run without -ledger.
func TestLedgerFlagsParse(t *testing.T) {
	cfg, err := parseFlags([]string{"-mix", "wh", "-ledger-out", "l.json"})
	if err != nil || cfg.LedgerOut != "l.json" {
		t.Fatalf("-ledger-out: %+v, %v", cfg, err)
	}
	cfg, err = parseFlags([]string{"-mode", "ledgercheck", "-ledger", "l.json", "-ledger-owner", "n1"})
	if err != nil || cfg.LedgerFile != "l.json" || cfg.LedgerOwner != "n1" {
		t.Fatalf("-mode ledgercheck: %+v, %v", cfg, err)
	}
	if _, err := parseFlags([]string{"-mode", "ledgercheck"}); err == nil || !strings.Contains(err.Error(), "-ledger") {
		t.Errorf("-mode ledgercheck without -ledger: %v; want a refusal naming -ledger", err)
	}
}

// blockedOwnerEngine is an engine whose owner lookup never answers, as a dial
// to a node that drops packets does until its timeout, and ignores the context
// it is given, as a blocking dial does.
type blockedOwnerEngine struct {
	engine
	release chan struct{}
	asked   atomic.Int64
}

func (b *blockedOwnerEngine) bucketOwner(context.Context, string) (string, error) {
	b.asked.Add(1)
	<-b.release
	return "", errors.New("dial timed out")
}

// TestLedgerWriteDoesNotWaitForOwnersOfADeadNode: after a hard reset of a node
// that drops packets, every owner lookup waits out its dial timeout. The ledger
// must still be written within the deadline the caller sets, with the owners
// recorded as unknown, because scope "all" needs none.
func TestLedgerWriteDoesNotWaitForOwnersOfADeadNode(t *testing.T) {
	cfg := mixTestConfig("wh", "social_ledger_owner")
	cfg.LedgerOut = filepath.Join(t.TempDir(), "ledger.json")
	book := newSocialBook(newSocialGraph(tinySpec, datasetSeed), false)
	for author := range 5 {
		_, finish, err := book.beginPost(context.Background(), nil, author)
		if err != nil {
			t.Fatal(err)
		}
		finish(true)
	}
	blocked := &blockedOwnerEngine{release: make(chan struct{})}
	defer close(blocked.release)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- writeAckLedger(ctx, cfg, blocked, book) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("writing the ledger: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("writeAckLedger was still waiting for bucket owners after 5 s (deadline was 300 ms, %d lookups started)", blocked.asked.Load())
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Errorf("writeAckLedger took %v with a 300 ms deadline", took)
	}
	encoded, err := os.ReadFile(cfg.LedgerOut)
	if err != nil {
		t.Fatalf("no ledger was written: %v", err)
	}
	var ledger ackLedgerFile
	if err := json.Unmarshal(encoded, &ledger); err != nil {
		t.Fatalf("the ledger is not JSON: %v", err)
	}
	if len(ledger.Authors) != 5 || ledger.OwnerErrors == 0 {
		t.Errorf("ledger has %d authors and owner_errors %d; want 5 authors with the unknown owners counted", len(ledger.Authors), ledger.OwnerErrors)
	}
	for _, entry := range ledger.Authors {
		if entry.Owner != "" || entry.Acked == nil {
			t.Errorf("author %d: owner %q acked %v; want no owner and an acknowledged post", entry.User, entry.Owner, entry.Acked)
		}
	}
}

// TestLedgerIsWrittenWhenTheWorkersReturnAnError: a leg whose workers end with
// an error (here a wrong answer, which stops them) still writes the ledger,
// because the leg that lost its node is exactly the one whose ledger is wanted.
// runMixLeg must write the ledger before it looks at what the workers returned.
func TestLedgerIsWrittenWhenTheWorkersReturnAnError(t *testing.T) {
	fake := newFakeServer()
	cfg := mixTestConfig("wh", "social_ledger_workers_fail")
	cfg.LedgerOut = filepath.Join(t.TempDir(), "ledger.json")
	engine := fakeHoardEngineOn(cfg, fake)
	if _, err := engine.setupSocial(context.Background(), tinySpec); err != nil {
		t.Fatal(err)
	}
	if err := engine.seedSocial(context.Background(), newSocialGraph(tinySpec, datasetSeed), 1); err != nil {
		t.Fatal(err)
	}
	cfg.Attach = true
	cfg.Duration = 2 * time.Second
	// The server turns wrong once the leg has posted for a while, so the workers
	// end with an error after some posts were acknowledged.
	fake.mutex.Lock()
	wrongAfter := len(fake.touched) + 200
	fake.mutex.Unlock()
	stop := make(chan struct{})
	flipped := make(chan struct{})
	go func() {
		defer close(flipped)
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
			}
			fake.mutex.Lock()
			if len(fake.touched) >= wrongAfter {
				fake.reverseFeedPeeks = true
				fake.mutex.Unlock()
				return
			}
			fake.mutex.Unlock()
		}
	}()
	defer func() { close(stop); <-flipped }()
	_, err := runMixLeg(context.Background(), cfg, tinySpec, engine)
	if err == nil {
		t.Fatal("the leg against a server that turns wrong succeeded, so the workers returned no error and this test pins nothing")
	}
	encoded, readErr := os.ReadFile(cfg.LedgerOut)
	if readErr != nil {
		t.Fatalf("the leg ended with %q and wrote no ledger: %v", err, readErr)
	}
	var ledger ackLedgerFile
	if err := json.Unmarshal(encoded, &ledger); err != nil {
		t.Fatalf("the ledger is not JSON: %v", err)
	}
	if len(ledger.Authors) == 0 {
		t.Errorf("the ledger holds no author although the leg posted before it failed: %s", encoded)
	}
}
