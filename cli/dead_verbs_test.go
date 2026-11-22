package cli

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/store"
	"github.com/hoarddb/hoarddb-server/transport"
)

const deadIDText = "0000000000000007"

// deadItem is one dead entry as DEAD_LIST answers it.
func deadItem(t *testing.T, deadID, task string) server.DeadItem {
	t.Helper()
	data, err := bson.Marshal(bson.D{{Key: "task", Value: task}})
	if err != nil {
		t.Fatal(err)
	}
	return server.DeadItem{DeadID: deadID, Data: data, Deliveries: 5, DeadAt: time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC), Reason: "max_deliveries"}
}

// TestDeadOptionReaderRefusesBothOrNeither: requeue and discard name one dead
// entry or all of them, and a statement that names both, neither or something
// else is a usage error that shows both forms, with no frame sent. The dead
// list reads its two keys as strictly.
func TestDeadOptionReaderRefusesBothOrNeither(t *testing.T) {
	bothOrNeither := func(verb string) string {
		return verb + ` names one dead entry or all of them, not both and not neither: db.Jobs.` + verb + `({dead_id: "<dead_id>"}) or db.Jobs.` + verb + `({all: true})`
	}
	cases := []struct {
		statement, says string
	}{
		{`db.Jobs.requeue()`, bothOrNeither("requeue")},
		{`db.Jobs.requeue({})`, bothOrNeither("requeue")},
		{`db.Jobs.discard({dead_id: "` + deadIDText + `", all: true})`, bothOrNeither("discard")},
		{`db.Jobs.requeue({all: false})`, `requeue's all is true, as in db.Jobs.requeue({all: true}); got false`},
		{`db.Jobs.discard({dead_id: 7})`, `discard's dead_id is the text dead() returned, as in db.Jobs.discard({dead_id: "<dead_id>"}); got 7`},
		{`db.Jobs.requeue({limit: 3})`, `requeue has the options dead_id and all, and "limit" is not one of them`},
		{`db.Jobs.requeue(3)`, `requeue(3) is not supported: the options go in an option document`},
		{`db.Jobs.dead({limit: 0})`, `dead's limit is a whole number of 1 or more`},
		{`db.Jobs.dead({after: 5})`, `dead's after is the dead_id of the last entry listed, as in db.Jobs.dead({after: "<dead_id>"}); got 5`},
		{`db.Jobs.dead({bogus: 1})`, `dead has the options limit and after, and "bogus" is not one of them`},
		{`db.Jobs.dead(10)`, `dead(10) is not supported: the options go in an option document`},
	}
	for _, tc := range cases {
		t.Run(tc.statement, func(t *testing.T) {
			stub, c, _ := newLeaseStub(t, map[transport.Opcode]interface{}{})
			err := c.handle(tc.statement)
			var usage *UsageError
			if !errors.As(err, &usage) || !strings.Contains(err.Error(), tc.says) {
				t.Fatalf("%s = %v, want a usage error containing\n%s", tc.statement, err, tc.says)
			}
			if sent := stub.sequence(); len(sent) != 0 {
				t.Errorf("the refused statement sent %v", sent)
			}
		})
	}
}

// TestDeadListPrintsAPageAndItsNext: one JSON line per dead entry with its id,
// deliveries, reason, time and data, then the statement that reads the next
// page, carrying the limit the user gave.
func TestDeadListPrintsAPageAndItsNext(t *testing.T) {
	reply := server.DeadListResponse{
		Found: true, Items: []server.DeadItem{deadItem(t, "0000000000000001", "a"), deadItem(t, "0000000000000002", "b")}, Next: "0000000000000002",
		Message: "db.Jobs has 5 dead entries (claimed 5 times without an acknowledgement).",
	}
	stub, c, out := newLeaseStub(t, map[transport.Opcode]interface{}{transport.OpDeadList: reply})
	if err := c.handle(`db.Jobs.dead({limit: 2})`); err != nil {
		t.Fatal(err)
	}
	want := `{"dead_id": "0000000000000001", "deliveries": 5, "reason": "max_deliveries", "dead_at": "2026-10-18T12:00:00Z", "data": {"task": "a"}}` + "\n" +
		`{"dead_id": "0000000000000002", "deliveries": 5, "reason": "max_deliveries", "dead_at": "2026-10-18T12:00:00Z", "data": {"task": "b"}}` + "\n" +
		`more: db.Jobs.dead({limit: 2, after: "0000000000000002"})` + "\n"
	if out.String() != want {
		t.Errorf("dead({limit: 2}) printed\n%s\nwant\n%s", out.String(), want)
	}
	request := bson.Raw(stub.payloadsOf(transport.OpDeadList)[0])
	if limit, _ := request.Lookup("limit").AsInt64OK(); limit != 2 {
		t.Errorf("the request carried limit %d, want 2: %s", limit, request)
	}
	if _, err := request.LookupErr("after"); err == nil {
		t.Errorf("a first page sent an after: %s", request)
	}

	out.Reset()
	reply.Next = ""
	stub, c, out = newLeaseStub(t, map[transport.Opcode]interface{}{transport.OpDeadList: reply})
	if err := c.handle(`db.Jobs.dead({after: "0000000000000000"})`); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "more:") {
		t.Errorf("the last page printed a more line: %s", out.String())
	}
	request = bson.Raw(stub.payloadsOf(transport.OpDeadList)[0])
	if after, _ := request.Lookup("after").StringValueOK(); after != "0000000000000000" {
		t.Errorf("the request carried after %q: %s", after, request)
	}
	if strings.Contains(out.String(), "has 5 dead entries") {
		t.Errorf("dead() printed the server's banner, which belongs to describe bucket and show buckets only:\n%s", out.String())
	}
}

// TestDeadListWithNothingToListIsSaidAsSuch: no dead entries is a line that says
// so, a refusal from the server is an error, and an after past the last entry is
// not mistaken for a bucket with none.
func TestDeadListWithNothingToListIsSaidAsSuch(t *testing.T) {
	cases := []struct {
		name, statement string
		reply           server.DeadListResponse
		wantOut         string
		wantErr         string
	}{
		{"none", `db.Jobs.dead()`, server.DeadListResponse{}, "(no dead entries)\n", ""},
		{"none after an entry", `db.Jobs.dead({after: "0000000000000009"})`, server.DeadListResponse{}, `(no dead entries after "0000000000000009")` + "\n", ""},
		{"a type without dead entries", `db.Users.dead()`, server.DeadListResponse{Message: "dead is not defined for hash buckets: only fifo, lifo and heap buckets have dead entries."}, "", "dead is not defined for hash buckets: only fifo, lifo and heap buckets have dead entries."},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, c, out := newLeaseStub(t, map[transport.Opcode]interface{}{transport.OpDeadList: tc.reply})
			err := c.handle(tc.statement)
			if tc.wantErr != "" {
				var refused *RefusedError
				if !errors.As(err, &refused) || err.Error() != tc.wantErr || strings.Contains(out.String(), "no dead entries") {
					t.Errorf("%s = %v with output %q, want the refusal %q and no \"no dead entries\"", tc.statement, err, out.String(), tc.wantErr)
				}
				return
			}
			if err != nil || out.String() != tc.wantOut {
				t.Errorf("%s = %v, printed %q, want %q", tc.statement, err, out.String(), tc.wantOut)
			}
		})
	}
}

// TestDeadListStoppedShortIsARetryNeverAnEmptyList: a list the server cut short
// is shown as a retry. A page with a cursor and no entries is not "no dead
// entries", and a first record the result ceiling refused is the server's
// error, with its fix, and nothing is printed as a list (the D3 required note).
func TestDeadListStoppedShortIsARetryNeverAnEmptyList(t *testing.T) {
	t.Run("a cursor without entries", func(t *testing.T) {
		_, c, out := newLeaseStub(t, map[transport.Opcode]interface{}{transport.OpDeadList: server.DeadListResponse{Next: "00000000000000aa"}})
		if err := c.handle(`db.Jobs.dead()`); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(out.String(), "no dead entries") || !strings.Contains(out.String(), `more: db.Jobs.dead({after: "00000000000000aa"})`) {
			t.Errorf("a cut-short page with a cursor printed %q, want the retry line and no \"no dead entries\"", out.String())
		}
	})
	t.Run("the first record over the result ceiling", func(t *testing.T) {
		message := "dead({limit: 10}) on app.Jobs would return more than 4 MiB (stopped at 0.0 MiB after 0 entries), so the list is not complete. Ask for fewer entries — dead({limit: 1}) fits — or raise HOARDDB_QUERY_MAX_RESULT_BYTES (4 MiB)"
		payload, err := bson.Marshal(bson.M{"code": uint8(transport.ErrResultTooLarge), "message": message})
		if err != nil {
			t.Fatal(err)
		}
		_, c, out := newLeaseStub(t, map[transport.Opcode]interface{}{transport.OpDeadList: stubRawReply{Opcode: transport.OpError, Payload: payload}})
		err = c.handle(`db.Jobs.dead()`)
		if err == nil || !strings.Contains(err.Error(), "so the list is not complete") || classify(err).Exit != ExitRefused {
			t.Errorf("dead() = %v, want the server's refusal with its fix and a non-zero exit", err)
		}
		if strings.Contains(out.String(), "no dead entries") {
			t.Errorf("a refused list printed %q", out.String())
		}
	})
}

// movingStub scripts a requeue or a discard: each request is answered with the
// next reply, and the 20th request and later are answered with an error so a
// CLI that loops on the wrong member ends the test instead of hanging it.
func movingStub(t *testing.T, opcode transport.Opcode, replies ...server.DeadMoveResponse) (*CLI, *bytes.Buffer, func() int) {
	t.Helper()
	stub, c, out := newLeaseStub(t, map[transport.Opcode]interface{}{opcode: server.DeadMoveResponse{}})
	var mu sync.Mutex
	requests := 0
	stub.answerWith(func(op transport.Opcode) (interface{}, bool) {
		if op != opcode {
			return nil, false
		}
		mu.Lock()
		defer mu.Unlock()
		requests++
		if requests >= 20 {
			payload, _ := bson.Marshal(bson.M{"code": uint8(transport.ErrInternal), "message": "the CLI kept asking"})
			return stubRawReply{Opcode: transport.OpError, Payload: payload}, true
		}
		return replies[min(requests, len(replies))-1], true
	})
	return c, out, func() int {
		mu.Lock()
		defer mu.Unlock()
		return requests
	}
}

// TestRequeueAllLoopsOnDoneNeverOnLeft: one apply moves at most 100, so the CLI
// repeats the request while the last one moved something, and totals what was
// moved. It does not loop while left is above zero: left counts the entries a
// bucket refuses by name, and those never move (D3 required note, repeated at
// D4).
func TestRequeueAllLoopsOnDoneNeverOnLeft(t *testing.T) {
	refused := server.DeadRefusal{DeadID: "00000000000000f1", Message: `requeue of "00000000000000f1" refused: the heap app.Tasks already holds an element with id "job-7" at priority 3.` + "\n" + `  Discard the dead copy with db.Tasks.discard({dead_id: "00000000000000f1"}), or remove the live one first.`}
	for _, verb := range []struct {
		word   string
		opcode transport.Opcode
		past   string
	}{{"requeue", transport.OpDeadRequeue, "requeued"}, {"discard", transport.OpDeadDiscard, "discarded"}} {
		t.Run(verb.word+" moves 100, 50 and then finds only the refused one", func(t *testing.T) {
			c, out, requests := movingStub(t, verb.opcode,
				server.DeadMoveResponse{Success: true, Done: 100, Left: 51},
				server.DeadMoveResponse{Success: false, Done: 50, Left: 1, Refused: []server.DeadRefusal{refused}},
				server.DeadMoveResponse{Success: false, Done: 0, Left: 1, Refused: []server.DeadRefusal{refused}},
			)
			err := c.handle(`db.Tasks.` + verb.word + `({all: true})`)
			if got := requests(); got != 3 {
				t.Errorf("sent %d %s requests, want 3: two that moved entries, and one that moved none", got, verb.word)
			}
			if want := verb.past + " 150, left 1\n"; out.String() != want {
				t.Errorf("printed %q, want %q", out.String(), want)
			}
			var refusedErr *RefusedError
			if !errors.As(err, &refusedErr) || !strings.Contains(err.Error(), `refused: the heap app.Tasks already holds an element with id "job-7"`) {
				t.Errorf("the error is %v, want the refused entry's message", err)
			}
			if classify(err).Exit != ExitRefused {
				t.Errorf("exit %d, want %d for a refused entry", classify(err).Exit, ExitRefused)
			}
			if strings.Count(err.Error(), "f1") < 1 || strings.Count(err.Error(), "refused: the heap") != 1 {
				t.Errorf("the one entry was reported %d times: %v", strings.Count(err.Error(), "refused: the heap"), err)
			}
		})
	}

	t.Run("it stops when nothing is left", func(t *testing.T) {
		c, out, requests := movingStub(t, transport.OpDeadRequeue,
			server.DeadMoveResponse{Success: true, Done: 100, Left: 20},
			server.DeadMoveResponse{Success: true, Done: 20},
		)
		if err := c.handle(`db.Jobs.requeue({all: true})`); err != nil {
			t.Fatal(err)
		}
		if got := requests(); got != 2 || out.String() != "requeued 120\n" {
			t.Errorf("sent %d requests and printed %q, want 2 and \"requeued 120\"", got, out.String())
		}
	})
}

// TestRequeueAllStepsPastAHeadOfRefusedEntries (BUG-0637): a request considers
// 100 entries, refused ones included, so 100 refusals at the head answer done 0
// with the movable entries behind them still left. The loop sends the last
// refused dead_id as after and reaches them; it ends when a request considers
// nothing new and still prints what is left.
func TestRequeueAllStepsPastAHeadOfRefusedEntries(t *testing.T) {
	refusals := make([]server.DeadRefusal, store.MaxDeadMoveLimit)
	for index := range refusals {
		deadID := fmt.Sprintf("%016x", index)
		refusals[index] = server.DeadRefusal{DeadID: deadID, Message: fmt.Sprintf("requeue of %q refused: the heap app.Tasks already holds an element with id \"job-%d\" at priority 3.", deadID, index)}
	}
	lastRefused := refusals[len(refusals)-1].DeadID
	replies := []server.DeadMoveResponse{
		{Success: false, Done: 0, Left: 110, Refused: refusals},
		{Success: true, Done: 10, Left: 100},
		{Success: true, Done: 0, Left: 100},
	}
	stub, c, out := newLeaseStub(t, map[transport.Opcode]interface{}{transport.OpDeadRequeue: server.DeadMoveResponse{}})
	var mu sync.Mutex
	requests := 0
	stub.answerWith(func(op transport.Opcode) (interface{}, bool) {
		if op != transport.OpDeadRequeue {
			return nil, false
		}
		mu.Lock()
		defer mu.Unlock()
		requests++
		if requests > len(replies) {
			payload, _ := bson.Marshal(bson.M{"code": uint8(transport.ErrInternal), "message": "the CLI kept asking"})
			return stubRawReply{Opcode: transport.OpError, Payload: payload}, true
		}
		return replies[requests-1], true
	})

	err := c.handle(`db.Tasks.requeue({all: true})`)

	if want := "requeued 10, left 100\n"; out.String() != want {
		t.Errorf("printed %q, want %q", out.String(), want)
	}
	var refusedErr *RefusedError
	if !errors.As(err, &refusedErr) || strings.Contains(err.Error(), "kept asking") {
		t.Errorf("the error is %v, want the refused entries' messages", err)
	}
	afters := []string{}
	for _, payload := range stub.payloadsOf(transport.OpDeadRequeue) {
		var sent server.DeadMoveRequest
		if err := bson.Unmarshal(payload, &sent); err != nil {
			t.Fatal(err)
		}
		afters = append(afters, sent.After)
	}
	if want := []string{"", lastRefused, lastRefused}; !slices.Equal(afters, want) {
		t.Errorf("the requests carried after %q, want %q: the first from the head, then past the last refused entry", afters, want)
	}
}

// TestRequeueAllDoesNotSpinOnAPermanentlyRefusedEntry: a heap entry whose origin
// key is held by a live element is refused by name on every request and never
// moves. Done is 0 and left is 1 for ever, so a CLI that looped while left was
// above 0 would ask until it was stopped. It asks once.
func TestRequeueAllDoesNotSpinOnAPermanentlyRefusedEntry(t *testing.T) {
	refused := server.DeadRefusal{DeadID: "00000000000000f1", Message: "requeue of \"00000000000000f1\" refused: the heap app.Tasks already holds an element with id \"job-7\" at priority 3."}
	stuck := server.DeadMoveResponse{Success: false, Done: 0, Left: 1, Refused: []server.DeadRefusal{refused}, Message: "1 dead entries are left in app.Tasks, 1 of them refused by name above and staying where they are."}
	for _, verb := range []struct {
		word   string
		opcode transport.Opcode
	}{{"requeue", transport.OpDeadRequeue}, {"discard", transport.OpDeadDiscard}} {
		t.Run(verb.word, func(t *testing.T) {
			c, _, requests := movingStub(t, verb.opcode, stuck, stuck, stuck, stuck)
			err := c.handle(`db.Tasks.` + verb.word + `({all: true})`)
			if got := requests(); got != 1 {
				t.Errorf("%s all asked %d times of a bucket that moved nothing, want once", verb.word, got)
			}
			if err == nil || !strings.Contains(err.Error(), "refused") || strings.Contains(err.Error(), "kept asking") {
				t.Errorf("%s all = %v, want the refused entry's message", verb.word, err)
			}
		})
	}
}

// TestRequeueAndDiscardOneEntryAreOneRequest: a dead_id is one request, whatever
// the answer says is left, and says what happened to that entry.
func TestRequeueAndDiscardOneEntryAreOneRequest(t *testing.T) {
	cases := []struct {
		name, statement string
		opcode          transport.Opcode
		reply           server.DeadMoveResponse
		wantOut         string
		wantErr         string
	}{
		{"requeued", `db.Jobs.requeue({dead_id: "` + deadIDText + `"})`, transport.OpDeadRequeue, server.DeadMoveResponse{Success: true, Done: 1, Left: 4}, "requeued 1\n", ""},
		{"discarded", `db.Jobs.discard({dead_id: "` + deadIDText + `"})`, transport.OpDeadDiscard, server.DeadMoveResponse{Success: true, Done: 1, Left: 4}, "discarded 1\n", ""},
		{"no such entry", `db.Jobs.requeue({dead_id: "` + deadIDText + `"})`, transport.OpDeadRequeue,
			server.DeadMoveResponse{Done: 0, Left: 2, Refused: []server.DeadRefusal{{DeadID: deadIDText, Message: `no dead entry "` + deadIDText + `" in app.Jobs: it may have been requeued or discarded already.` + "\n  List what is there: db.Jobs.dead()"}}},
			"", `no dead entry "` + deadIDText + `" in app.Jobs: it may have been requeued or discarded already.` + "\n  List what is there: db.Jobs.dead()"},
		{"a type without dead entries", `db.Users.discard({all: true})`, transport.OpDeadDiscard,
			server.DeadMoveResponse{Message: "discard is not defined for hash buckets: only fifo, lifo and heap buckets have dead entries."}, "", "discard is not defined for hash buckets: only fifo, lifo and heap buckets have dead entries."},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub, c, out := newLeaseStub(t, map[transport.Opcode]interface{}{tc.opcode: tc.reply})
			err := c.handle(tc.statement)
			if got := stub.sent(tc.opcode); got != 1 {
				t.Errorf("sent %d requests, want 1", got)
			}
			if tc.wantErr == "" {
				if err != nil || out.String() != tc.wantOut {
					t.Errorf("%s = %v, printed %q, want %q", tc.statement, err, out.String(), tc.wantOut)
				}
				return
			}
			var refused *RefusedError
			if !errors.As(err, &refused) || err.Error() != tc.wantErr {
				t.Errorf("%s = %v, want the refusal\n%s", tc.statement, err, tc.wantErr)
			}
			if out.String() != "" {
				t.Errorf("a refused move printed %q beside its error", out.String())
			}
		})
	}
}

// TestDeadVerbsOnAStoreFieldAreRefusedByName: a field has no dead entries; the
// refusal points to pop and nothing is sent.
func TestDeadVerbsOnAStoreFieldAreRefusedByName(t *testing.T) {
	stub, c, _ := newLeaseStub(t, map[transport.Opcode]interface{}{})
	for _, verb := range []string{"dead", "requeue", "discard"} {
		err := c.handle(`db.Post["p1"].Comments.` + verb + `()`)
		var usage *UsageError
		if !errors.As(err, &usage) || !strings.HasPrefix(err.Error(), verb+" is not available on a store field") ||
			!strings.Contains(err.Error(), "Claim, ack, release, extend and the dead verbs work on a fifo, lifo or heap bucket") {
			t.Errorf("%s on a store field = %v, want the refusal by name", verb, err)
		}
	}
	if sent := stub.sequence(); len(sent) != 0 {
		t.Errorf("a refused field verb sent %v", sent)
	}
}

// TestDeadBannerTextMatchesTheServers: the banner describe bucket and show
// buckets print is the server's, word for word, so a bucket's dead entries read
// the same wherever they are met. The server's own copy is the message of a
// DEAD_LIST answer.
func TestDeadBannerTextMatchesTheServers(t *testing.T) {
	c := newDumpTestServer(t)
	for _, line := range []string{`create database app`, `create bucket Jobs { type: fifo }`, `db.Jobs.push({"n": 1})`, `db.Jobs.push({"n": 2})`} {
		if _, err := runStatement(t, c, line); err != nil {
			t.Fatalf("%s: %v", line, err)
		}
	}
	killEveryEntry(t, c, "Jobs")
	var answer server.DeadListResponse
	if err := c.handler.send(transport.OpDeadList, &server.DeadListRequest{Database: "app", Bucket: "Jobs"}, &answer); err != nil {
		t.Fatal(err)
	}
	if want := deadBanner("Jobs", 2); answer.Message != want {
		t.Errorf("the server's banner is\n%s\nthe CLI's is\n%s", answer.Message, want)
	}
	if one := deadBanner("Jobs", 1); !strings.HasPrefix(one, "db.Jobs has 1 dead entry (claimed 5 times") {
		t.Errorf("one dead entry reads %q", one)
	}
}

// killEveryEntry claims a bucket and releases the claim until nothing is left
// to claim. Each release is a return, and the fifth return of an entry
// dead-letters it, so this ends with every entry in the dead list.
func killEveryEntry(t *testing.T, c *CLI, bucket string) {
	t.Helper()
	leaseLine := regexp.MustCompile(`(?m)^lease_id ([0-9a-f]{40}) for`)
	for round := 0; round < 20; round++ {
		printed, err := runStatement(t, c, `db.`+bucket+`.claim({limit: 100})`)
		if err != nil {
			t.Fatalf("claim %d of %s: %v", round, bucket, err)
		}
		parts := leaseLine.FindStringSubmatch(printed)
		if parts == nil {
			return
		}
		if _, err := runStatement(t, c, `db.`+bucket+`.release({lease_id: "`+parts[1]+`"})`); err != nil {
			t.Fatalf("release %d of %s: %v", round, bucket, err)
		}
	}
	t.Fatalf("20 claim-and-release rounds did not dead-letter every entry of %s", bucket)
}

// TestDeadLifecycleAgainstARealServer: entries claimed and released five times
// are listed by dead(), counted beside len, put back by requeue with a fresh
// count and dropped by discard, on a fifo and across several applies (one apply
// moves at most 100).
func TestDeadLifecycleAgainstARealServer(t *testing.T) {
	c := newDumpTestServer(t)
	for _, line := range []string{`create database app`, `create bucket Jobs { type: fifo }`, `create bucket Bin { type: lifo }`} {
		if _, err := runStatement(t, c, line); err != nil {
			t.Fatalf("%s: %v", line, err)
		}
	}
	for index := 0; index < 250; index++ {
		if _, err := runStatement(t, c, fmt.Sprintf(`db.Jobs.push({"n": %d})`, index)); err != nil {
			t.Fatal(err)
		}
	}
	for index := 0; index < 3; index++ {
		if _, err := runStatement(t, c, fmt.Sprintf(`db.Bin.push({"n": %d})`, index)); err != nil {
			t.Fatal(err)
		}
	}
	killEveryEntry(t, c, "Jobs")
	killEveryEntry(t, c, "Bin")

	printed, err := runStatement(t, c, `db.Jobs.dead()`)
	if err != nil || strings.Count(printed, "\n") != 11 || !strings.Contains(printed, `"deliveries": 5, "reason": "max_deliveries"`) ||
		!strings.Contains(printed, `more: db.Jobs.dead({after: "`) {
		t.Fatalf("dead() = %q, %v, want ten dead entries and the line for the next page", printed, err)
	}
	next := regexp.MustCompile(`more: db.Jobs.dead\(\{after: "([0-9a-f]+)"\}\)`).FindStringSubmatch(printed)[1]
	if printed, err = runStatement(t, c, `db.Jobs.dead({limit: 2, after: "`+next+`"})`); err != nil || strings.Count(printed, "\n") != 3 {
		t.Errorf("the second page = %q, %v, want two entries and a more line", printed, err)
	}
	if printed, err = runStatement(t, c, `db.Jobs.len()`); err != nil || !strings.HasPrefix(printed, "0  (") ||
		!strings.Contains(printed, "entries are dead-lettered; see db.Jobs.dead())") {
		t.Errorf("len() of a bucket whose entries are all dead = %q, %v, want 0 and the dead hint", printed, err)
	}

	t.Run("requeue one", func(t *testing.T) {
		binPrinted, err := runStatement(t, c, `db.Bin.dead({limit: 1})`)
		deadID := regexp.MustCompile(`"dead_id": "([0-9a-f]+)"`).FindStringSubmatch(binPrinted)
		if err != nil || deadID == nil {
			t.Fatalf("dead() = %q, %v", binPrinted, err)
		}
		if printed, err := runStatement(t, c, `db.Bin.requeue({dead_id: "`+deadID[1]+`"})`); err != nil || printed != "requeued 1\n" {
			t.Fatalf("requeue = %q, %v", printed, err)
		}
		if printed, err := runStatement(t, c, `db.Bin.requeue({dead_id: "`+deadID[1]+`"})`); err == nil || !strings.Contains(err.Error(), "no dead entry") || printed != "" {
			t.Errorf("requeue of the same entry again = %q, %v, want the no-dead-entry refusal with the fix", printed, err)
		}
		claimed, err := runStatement(t, c, `db.Bin.claim()`)
		if err != nil || !strings.Contains(claimed, " for 30s; item_ids [") || strings.Contains(claimed, "deliveries") {
			t.Errorf("a requeued entry is claimed with its count reset: %q, %v", claimed, err)
		}
	})

	t.Run("requeue all across applies", func(t *testing.T) {
		if printed, err := runStatement(t, c, `db.Jobs.requeue({all: true})`); err != nil || printed != "requeued 250\n" {
			t.Fatalf("requeue all = %q, %v, want requeued 250", printed, err)
		}
		if printed, err := runStatement(t, c, `db.Jobs.len()`); err != nil || printed != "250\n" {
			t.Errorf("len() after requeue all = %q, %v", printed, err)
		}
		if printed, err := runStatement(t, c, `db.Jobs.dead()`); err != nil || printed != "(no dead entries)\n" {
			t.Errorf("dead() after requeue all = %q, %v", printed, err)
		}
	})

	t.Run("discard all across applies", func(t *testing.T) {
		killEveryEntry(t, c, "Jobs")
		if printed, err := runStatement(t, c, `db.Jobs.discard({all: true})`); err != nil || printed != "discarded 250\n" {
			t.Fatalf("discard all = %q, %v, want discarded 250", printed, err)
		}
		if printed, err := runStatement(t, c, `db.Jobs.dead()`); err != nil || printed != "(no dead entries)\n" {
			t.Errorf("dead() after discard all = %q, %v", printed, err)
		}
	})
}

// TestRequeueOfAHeapElementWhoseIdIsTakenIsRefusedAndStops: a dead heap element
// whose id and priority a live element now holds cannot go back. requeue all
// reports it by name, with the fix, moves the others, and returns instead of
// asking again (the real server's answer for the stuck case above).
func TestRequeueOfAHeapElementWhoseIdIsTakenIsRefusedAndStops(t *testing.T) {
	c := newDumpTestServer(t)
	for _, line := range []string{
		`create database app`, `create bucket Tasks { type: heap }`,
		`db.Tasks.push({"_id_": "job-7", "priority": 3, "n": 1})`, `db.Tasks.push({"_id_": "job-8", "priority": 2, "n": 2})`,
	} {
		if _, err := runStatement(t, c, line); err != nil {
			t.Fatalf("%s: %v", line, err)
		}
	}
	killEveryEntry(t, c, "Tasks")
	if _, err := runStatement(t, c, `db.Tasks.push({"_id_": "job-7", "priority": 3, "n": 9})`); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	var printed string
	var err error
	go func() {
		defer close(done)
		printed, err = runStatement(t, c, `db.Tasks.requeue({all: true})`)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("requeue all did not return: it is asking again of an entry that is refused by name")
	}
	var refused *RefusedError
	if !errors.As(err, &refused) || !strings.Contains(err.Error(), `already holds an element with id "job-7" at priority 3`) ||
		!strings.Contains(err.Error(), "Discard the dead copy with db.Tasks.discard({dead_id:") {
		t.Fatalf("requeue all = %v, want the collision named with the fix", err)
	}
	if printed != "requeued 1, left 1\n" {
		t.Errorf("requeue all printed %q, want the one that moved and the one that stayed", printed)
	}
}

// TestDescribeBucketShowsTheDeadBanner: describe bucket and show buckets print
// the three-line banner for a bucket that holds dead entries, once each, and
// nothing for a bucket that holds none.
func TestDescribeBucketShowsTheDeadBanner(t *testing.T) {
	c := newDumpTestServer(t)
	for _, line := range []string{
		`create database app`, `create bucket Jobs { type: fifo }`, `create bucket Quiet { type: fifo }`, `create bucket Users { type: hash }`,
		`db.Jobs.push({"n": 1})`, `db.Jobs.push({"n": 2})`, `db.Quiet.push({"n": 1})`, `db.Users.insert({"_id_": "u1"})`,
	} {
		if _, err := runStatement(t, c, line); err != nil {
			t.Fatalf("%s: %v", line, err)
		}
	}
	banner := deadBanner("Jobs", 2)
	killEveryEntry(t, c, "Jobs")

	for _, statement := range []string{`describe bucket Jobs`, `show buckets`} {
		printed, err := runStatement(t, c, statement)
		if err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
		if strings.Count(printed, banner) != 1 {
			t.Errorf("%s printed the banner %d times, want once:\n%s", statement, strings.Count(printed, banner), printed)
		}
		if strings.Contains(printed, "db.Quiet has") || strings.Contains(printed, "db.Users has") {
			t.Errorf("%s printed a banner for a bucket with no dead entries:\n%s", statement, printed)
		}
	}
	for _, statement := range []string{`describe bucket Quiet`, `describe bucket Users`} {
		printed, err := runStatement(t, c, statement)
		if err != nil || strings.Contains(printed, "dead entr") {
			t.Errorf("%s = %q, %v, want no banner", statement, printed, err)
		}
	}
	for _, statement := range []string{`db.Jobs.len()`, `db.Jobs.peek()`, `db.Jobs.pop()`, `db.Jobs.claim()`} {
		printed, _ := runStatement(t, c, statement)
		if strings.Contains(printed, "Read them") {
			t.Errorf("%s printed the banner, which only describe bucket and show buckets print:\n%s", statement, printed)
		}
	}
}

// dbStatement matches a statement a message tells the reader to type:
// db.Jobs.dead(), db.Slots.discard({dead_id: "ab12"}), db.Post["p1"].Comments.pop().
var dbStatement = regexp.MustCompile(`db\.[A-Za-z]+(?:\["[^"]*"\]\.[A-Za-z]+)?\.[a-z]+\((?:\{[^}]*\})?\)`)

// TestDeadMessagesNameTheFix: every refusal and banner of the dead verbs names
// the statement that fixes it, and the CLI parses that statement. Read from a
// real server for the cases only it can produce: a requeue of an entry that is
// gone, the heap requeue collision and the dead-count banner.
func TestDeadMessagesNameTheFix(t *testing.T) {
	c := newDumpTestServer(t)
	for _, line := range []string{
		`create database app`, `create bucket Work { type: fifo }`, `create bucket Slots { type: heap }`,
		`db.Work.push({"n": 1})`, `db.Slots.push({"_id_": "job-7", "priority": 3, "n": 1})`,
	} {
		if _, err := runStatement(t, c, line); err != nil {
			t.Fatalf("%s: %v", line, err)
		}
	}
	killEveryEntry(t, c, "Work")
	killEveryEntry(t, c, "Slots")
	if _, err := runStatement(t, c, `db.Slots.push({"_id_": "job-7", "priority": 3, "n": 2})`); err != nil {
		t.Fatal(err)
	}
	firstDeadID := func(bucket string) string {
		printed, err := runStatement(t, c, `db.`+bucket+`.dead({limit: 1})`)
		match := deadIDPattern.FindStringSubmatch(printed)
		if err != nil || match == nil {
			t.Fatalf("dead() of %s = %q, %v", bucket, printed, err)
		}
		return match[1]
	}
	workID := firstDeadID("Work")
	if _, err := runStatement(t, c, `db.Work.discard({dead_id: "`+workID+`"})`); err != nil {
		t.Fatal(err)
	}
	_, gone := runStatement(t, c, `db.Work.requeue({dead_id: "`+workID+`"})`)
	_, collision := runStatement(t, c, `db.Slots.requeue({dead_id: "`+firstDeadID("Slots")+`"})`)
	_, both := runStatement(t, c, `db.Work.requeue({all: true, dead_id: "ab"})`)
	_, onField := runStatement(t, c, `db.Post["p1"].Comments.dead()`)

	cases := []struct {
		name    string
		message string
		fix     string
	}{
		{"no dead entry by that id", errText(gone), "List what is there: db.Work.dead()"},
		{"the heap requeue collision", errText(collision), `Discard the dead copy with db.Slots.discard({dead_id: "`},
		{"both or neither", errText(both), `db.Work.requeue({dead_id: "<dead_id>"}) or db.Work.requeue({all: true})`},
		{"the dead-count banner", deadBanner("Work", 3), "Read them:     db.Work.dead()"},
		{"a dead verb on a store field", errText(onField), ".pop(), which removes at most once"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(tc.message, tc.fix) {
				t.Fatalf("the message does not name the fix %q:\n%s", tc.fix, tc.message)
			}
			statements := dbStatement.FindAllString(tc.message, -1)
			if len(statements) == 0 {
				t.Fatalf("the message names no statement to type:\n%s", tc.message)
			}
			for _, statement := range statements {
				if _, err := ParseCommand(statement); err != nil {
					t.Errorf("the message tells the reader to type %q, which the CLI rejects: %v", statement, err)
				}
			}
		})
	}
}

// errText is an error's text, empty for nil.
func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
