package cli

import (
	"bytes"
	"errors"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// leaseOpcodes are the four lease verbs' opcodes, by the word the CLI takes.
var leaseOpcodes = map[string]transport.Opcode{
	"claim":   transport.OpQueueClaim,
	"ack":     transport.OpQueueAck,
	"release": transport.OpQueueRelease,
	"extend":  transport.OpQueueExtend,
}

// newLeaseStub starts the stub with the replies given and a CLI on database
// "app", its output captured. A reply for an opcode the test does not script
// makes the stub answer an error, so a verb that sends the wrong frame is seen.
func newLeaseStub(t *testing.T, script map[transport.Opcode]interface{}) (*stubServer, *CLI, *bytes.Buffer) {
	t.Helper()
	var out bytes.Buffer
	stub, c := newStubServer(t, script, &out)
	c.currentDB = "app"
	out.Reset() // the connection banner is not what these tests read
	return stub, c, &out
}

const leaseIDText = "0b1c2d3e4f5061728394a5b6c7d8e9f001122334"

// TestClaimOptionReaderRefusesUnknownKeysNamingClaim: a positional argument,
// an unknown key and a wrong type are usage errors that name claim and its two
// keys, never the one-key wording of peek and pop, and no frame is sent
// (F-16: the shared reader hard-coded "has one option, limit").
func TestClaimOptionReaderRefusesUnknownKeysNamingClaim(t *testing.T) {
	cases := []struct {
		arguments string
		says      []string
	}{
		{`5`, []string{"claim(5) is not supported", "option document", `db.Jobs.claim({limit: 10, lease: "30s"})`}},
		{`{bogus: true}`, []string{`claim has the options limit and lease, and "bogus" is not one of them`, `db.Jobs.claim({limit: 10, lease: "30s"})`}},
		{`{lmit: 3}`, []string{`"lmit" is not one of them`}},
		{`{limit: "3"}`, []string{"claim's limit is a whole number of 1 or more"}},
		{`{limit: 0}`, []string{"claim's limit is a whole number of 1 or more"}},
		{`{lease: true}`, []string{"claim's lease is a duration with a unit"}},
		{`{limit: 3}, {lease: "5s"}`, []string{"claim takes at most one argument"}},
	}
	for _, tc := range cases {
		t.Run(tc.arguments, func(t *testing.T) {
			stub, c, _ := newLeaseStub(t, map[transport.Opcode]interface{}{transport.OpQueueClaim: server.ClaimResponse{Message: "queue is empty"}})
			err := c.handle(`db.Jobs.claim(` + tc.arguments + `)`)
			var usage *UsageError
			if !errors.As(err, &usage) {
				t.Fatalf("db.Jobs.claim(%s) = %v, want a usage error", tc.arguments, err)
			}
			for _, want := range tc.says {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal lacks %q: %v", want, err)
				}
			}
			if strings.Contains(err.Error(), "has one option") {
				t.Errorf("claim was told it has one option: %v", err)
			}
			if sent := stub.sequence(); len(sent) != 0 {
				t.Errorf("the refused statement sent %v", sent)
			}
		})
	}
}

// TestEveryLeaseVerbReadsItsOptionsStrictly: each verb refuses the key of
// another verb, naming its own keys, and a missing key it cannot do without.
func TestEveryLeaseVerbReadsItsOptionsStrictly(t *testing.T) {
	cases := []struct {
		statement string
		says      string
	}{
		{`db.Jobs.ack({limit: 3})`, `ack has the options lease_id and item_ids, and "limit" is not one of them`},
		{`db.Jobs.release({item_ids: []})`, `release has the option lease_id, and "item_ids" is not it`},
		{`db.Jobs.extend({limit: 1})`, `extend has the options lease_id and lease, and "limit" is not one of them`},
		{`db.Jobs.ack()`, `ack needs the lease_id claim returned, as in db.Jobs.ack({lease_id: "<lease_id>"})`},
		{`db.Jobs.release({})`, `release needs the lease_id claim returned, as in db.Jobs.release({lease_id: "<lease_id>"})`},
		{`db.Jobs.extend({lease_id: "` + leaseIDText + `"})`, `extend needs a lease, as in db.Jobs.extend({lease_id: "<lease_id>", lease: "60s"})`},
		{`db.Jobs.extend({lease: "60s"})`, `extend needs the lease_id claim returned`},
		{`db.Jobs.ack({lease_id: 7})`, `ack's lease_id is the text claim returned, as in db.Jobs.ack({lease_id: "<lease_id>"}); got 7`},
		{`db.Jobs.ack({lease_id: "` + leaseIDText + `", item_ids: "a"})`, `ack's item_ids is a list of the item_ids claim returned`},
		{`db.Jobs.ack({lease_id: "` + leaseIDText + `", item_ids: [1]})`, `ack's item_ids is a list of the item_ids claim returned`},
		{`db.Jobs.ack({lease_id: "` + leaseIDText + `", item_ids: []})`, `ack's item_ids is empty`},
	}
	for _, tc := range cases {
		t.Run(tc.statement, func(t *testing.T) {
			stub, c, _ := newLeaseStub(t, map[transport.Opcode]interface{}{})
			err := c.handle(tc.statement)
			var usage *UsageError
			if !errors.As(err, &usage) || !strings.Contains(err.Error(), tc.says) {
				t.Fatalf("%s = %v, want a usage error containing %q", tc.statement, err, tc.says)
			}
			if sent := stub.sequence(); len(sent) != 0 {
				t.Errorf("the refused statement sent %v", sent)
			}
		})
	}
}

// TestLeaseNeedsAUnit: a bare number is refused with the sentence the spec
// gives, and the four spellings of a duration are sent as milliseconds.
func TestLeaseNeedsAUnit(t *testing.T) {
	stub, c, _ := newLeaseStub(t, map[transport.Opcode]interface{}{transport.OpQueueClaim: server.ClaimResponse{Message: "queue is empty"}})
	err := c.handle(`db.Jobs.claim({lease: 30})`)
	var usage *UsageError
	want := `claim's lease needs a unit, as in db.Jobs.claim({lease: "30s"}); got 30`
	if !errors.As(err, &usage) || err.Error() != want {
		t.Fatalf("claim({lease: 30}) = %v, want the usage error %q", err, want)
	}
	if sent := stub.sequence(); len(sent) != 0 {
		t.Fatalf("a lease with no unit sent %v", sent)
	}

	for _, spelling := range []struct {
		text string
		ms   int64
	}{{`"30s"`, 30_000}, {`"1500ms"`, 1500}, {`"5m"`, 300_000}, {`"1h"`, 3_600_000}, {`"1s"`, 1000}} {
		if err := c.handle(`db.Jobs.claim({lease: ` + spelling.text + `})`); err != nil {
			t.Fatalf("claim({lease: %s}): %v", spelling.text, err)
		}
		payloads := stub.payloadsOf(transport.OpQueueClaim)
		last := bson.Raw(payloads[len(payloads)-1])
		if got, _ := last.Lookup("lease_ms").AsInt64OK(); got != spelling.ms {
			t.Errorf("claim({lease: %s}) sent lease_ms %d, want %d", spelling.text, got, spelling.ms)
		}
	}
	for _, malformed := range []string{`"30"`, `"s"`, `"1.5s"`, `"30 s"`, `"1d"`, `"-5s"`, `"0s"`} {
		err := c.handle(`db.Jobs.claim({lease: ` + malformed + `})`)
		if !errors.As(err, &usage) || !strings.Contains(err.Error(), "claim's lease") {
			t.Errorf("claim({lease: %s}) = %v, want a usage error about claim's lease", malformed, err)
		}
	}
}

// TestLeaseOutOfBoundsRefusedInTheUsersSpelling: a lease over an hour or under
// a second is refused by the CLI, in the words the user typed, and no frame is
// sent. The server's millisecond text stays for drivers.
func TestLeaseOutOfBoundsRefusedInTheUsersSpelling(t *testing.T) {
	cases := []struct {
		statement, want string
	}{
		{`db.Jobs.claim({lease: "2h"})`, `claim's lease "2h" is above the maximum of "1h": claim with lease "1h" and call db.Jobs.extend({lease_id: "<lease_id>", lease: "1h"}) while the work runs. Nothing was sent`},
		{`db.Jobs.claim({lease: "7200000ms"})`, `claim's lease "7200000ms" is above the maximum of "1h": claim with lease "1h" and call db.Jobs.extend({lease_id: "<lease_id>", lease: "1h"}) while the work runs. Nothing was sent`},
		{`db.Jobs.claim({lease: "500ms"})`, `claim's lease "500ms" is below the minimum of "1s": use "1s" or more. Nothing was sent`},
		{`db.Jobs.extend({lease_id: "` + leaseIDText + `", lease: "2h"})`, `extend's lease "2h" is above the maximum of "1h": extend with lease "1h" and extend again while the work runs. Nothing was sent`},
	}
	for _, tc := range cases {
		t.Run(tc.statement, func(t *testing.T) {
			stub, c, _ := newLeaseStub(t, map[transport.Opcode]interface{}{})
			err := c.handle(tc.statement)
			var usage *UsageError
			if !errors.As(err, &usage) || err.Error() != tc.want {
				t.Fatalf("%s =\n%v\nwant the usage error\n%s", tc.statement, err, tc.want)
			}
			if strings.Contains(err.Error(), "7200000 ms") {
				t.Errorf("the refusal speaks the server's milliseconds: %v", err)
			}
			if sent := stub.sequence(); len(sent) != 0 {
				t.Errorf("an out-of-bounds lease sent %v", sent)
			}
		})
	}
}

// TestClaimOnAStoreFieldIsRefusedByName: a field store has no claim (D-15); the
// CLI says so by name, points to pop, and sends nothing. The same holds for the
// other lease verbs, which a field does not have either.
func TestClaimOnAStoreFieldIsRefusedByName(t *testing.T) {
	stub, c, _ := newLeaseStub(t, map[transport.Opcode]interface{}{})
	err := c.handle(`db.Post["p1"].Comments.claim()`)
	var refused *UsageError
	want := `claim is not available on a store field: db.Post["p1"].Comments is consumed with db.Post["p1"].Comments.pop(), which removes at most once. Claim, ack, release, extend and the dead verbs work on a fifo, lifo or heap bucket`
	if !errors.As(err, &refused) || err.Error() != want {
		t.Fatalf("claim on a store field = %v, want the refusal\n%s", err, want)
	}
	for _, verb := range []string{"ack", "release", "extend"} {
		err := c.handle(`db.Post["p1"].Comments.` + verb + `()`)
		if !errors.As(err, &refused) || !strings.HasPrefix(err.Error(), verb+" is not available on a store field") || !strings.Contains(err.Error(), ".pop()") {
			t.Errorf("%s on a store field = %v, want the refusal by name pointing to pop", verb, err)
		}
	}
	if sent := stub.sequence(); len(sent) != 0 {
		t.Errorf("a refused field verb sent %v", sent)
	}
}

// TestCLIClaimSendsOneFrame: one claim statement is one OpQueueClaim frame, with
// the members the user named and none they did not.
func TestCLIClaimSendsOneFrame(t *testing.T) {
	stub, c, _ := newLeaseStub(t, map[transport.Opcode]interface{}{transport.OpQueueClaim: server.ClaimResponse{Message: "queue is empty"}})
	if err := c.handle(`db.Jobs.claim({limit: 3, lease: "45s"})`); err != nil {
		t.Fatal(err)
	}
	if err := c.handle(`db.Jobs.claim()`); err != nil {
		t.Fatal(err)
	}
	if sent := stub.sequence(); len(sent) != 2 || sent[0] != transport.OpQueueClaim || sent[1] != transport.OpQueueClaim {
		t.Fatalf("two claim statements sent %v, want two claim frames and nothing else", sent)
	}
	payloads := stub.payloadsOf(transport.OpQueueClaim)
	first, second := bson.Raw(payloads[0]), bson.Raw(payloads[1])
	if limit, _ := first.Lookup("limit").AsInt64OK(); limit != 3 {
		t.Errorf("limit %d, want 3: %s", limit, first)
	}
	if leaseMs, _ := first.Lookup("lease_ms").AsInt64OK(); leaseMs != 45_000 {
		t.Errorf("lease_ms %d, want 45000: %s", leaseMs, first)
	}
	if bucket, _ := first.Lookup("bucket").StringValueOK(); bucket != "Jobs" {
		t.Errorf("bucket %q, want Jobs", bucket)
	}
	for _, member := range []string{"limit", "lease_ms", "lease_now", "lease_nonce"} {
		if _, err := second.LookupErr(member); err == nil {
			t.Errorf("claim() sent %s, which the user did not name: %s", member, second)
		}
	}
}

// TestLeaseVerbsSendTheirOwnFrames: ack, release and extend each send one frame
// of their own opcode with the lease and the ids the user gave.
func TestLeaseVerbsSendTheirOwnFrames(t *testing.T) {
	script := map[transport.Opcode]interface{}{
		transport.OpQueueAck:     server.LeaseResponse{Success: true, LeaseState: "acked", Acked: 1},
		transport.OpQueueRelease: server.LeaseResponse{Success: true, LeaseState: "released", Released: 1},
		transport.OpQueueExtend:  server.LeaseResponse{Success: true, LeaseState: "extended", LeaseMs: 60_000, LeaseDeadline: time.Date(2026, 10, 18, 12, 1, 0, 0, time.UTC)},
	}
	stub, c, _ := newLeaseStub(t, script)
	statements := []string{
		`db.Jobs.ack({lease_id: "` + leaseIDText + `", item_ids: ["00000000000000aa", "00000000000000bb"]})`,
		`db.Jobs.release({lease_id: "` + leaseIDText + `"})`,
		`db.Jobs.extend({lease_id: "` + leaseIDText + `", lease: "60s"})`,
	}
	for _, statement := range statements {
		if err := c.handle(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	want := []transport.Opcode{transport.OpQueueAck, transport.OpQueueRelease, transport.OpQueueExtend}
	got := stub.sequence()
	if len(got) != len(want) {
		t.Fatalf("sent %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Errorf("frame %d is %v, want %v", index, got[index], want[index])
		}
	}
	ack := bson.Raw(stub.payloadsOf(transport.OpQueueAck)[0])
	if leaseID, _ := ack.Lookup("lease_id").StringValueOK(); leaseID != leaseIDText {
		t.Errorf("ack carried lease_id %q", leaseID)
	}
	ids, err := ack.Lookup("item_ids").Array().Values()
	if err != nil || len(ids) != 2 {
		t.Errorf("ack carried item_ids %v (%v), want two", ack.Lookup("item_ids"), err)
	}
	extend := bson.Raw(stub.payloadsOf(transport.OpQueueExtend)[0])
	if leaseMs, _ := extend.Lookup("lease_ms").AsInt64OK(); leaseMs != 60_000 {
		t.Errorf("extend carried lease_ms %d, want 60000", leaseMs)
	}
}

// TestClaimPrintsEntriesThenTheLeaseLine: each entry is a JSON line, then the
// lease line that carries what ack needs; deliveries appear only when one is
// above 1.
func TestClaimPrintsEntriesThenTheLeaseLine(t *testing.T) {
	entryOne, _ := bson.Marshal(bson.D{{Key: "task", Value: "a"}})
	entryTwo, _ := bson.Marshal(bson.D{{Key: "task", Value: "b"}})
	reply := server.ClaimResponse{
		Found: true, Items: []bson.Raw{entryOne, entryTwo}, ItemIDs: []string{"00000000000000aa", "00000000000000bb"},
		LeaseID: leaseIDText, LeaseMs: 30_000,
	}
	_, c, out := newLeaseStub(t, map[transport.Opcode]interface{}{transport.OpQueueClaim: reply})
	if err := c.handle(`db.Jobs.claim({limit: 2})`); err != nil {
		t.Fatal(err)
	}
	want := "{\"task\": \"a\"}\n{\"task\": \"b\"}\nlease_id " + leaseIDText + " for 30s; item_ids [\"00000000000000aa\", \"00000000000000bb\"]\n"
	if out.String() != want {
		t.Errorf("claim printed\n%q\nwant\n%q", out.String(), want)
	}

	reply.Deliveries = []int32{1, 3}
	reply.LeaseMs = 90_000
	_, c, out = newLeaseStub(t, map[transport.Opcode]interface{}{transport.OpQueueClaim: reply})
	if err := c.handle(`db.Jobs.claim({limit: 2})`); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(out.String(), "lease_id "+leaseIDText+" for 90s; item_ids [\"00000000000000aa\", \"00000000000000bb\"]; deliveries [1, 3]\n") {
		t.Errorf("a redelivered claim printed\n%q", out.String())
	}
}

// TestAckAndReleaseAndExtendPrintTheirOutcomes: the outcome in words, and an
// unknown or expired lease is an Error line with a non-zero exit, never a
// success.
func TestAckAndReleaseAndExtendPrintTheirOutcomes(t *testing.T) {
	expiredText := "lease 0b1c…34 on app.Jobs expired at 2026-10-15T12:00:31Z (bucket clock); its entries go back to the queue on the next claim or pop. Extend a lease before it ends with db.Jobs.extend({lease_id: \"…\", lease: \"60s\"})"
	cases := []struct {
		name, statement string
		reply           server.LeaseResponse
		wantOut         string
		wantErr         string
	}{
		{"ack", `db.Jobs.ack({lease_id: "` + leaseIDText + `"})`, server.LeaseResponse{Success: true, LeaseState: "acked", Acked: 2}, "acked 2\n", ""},
		{"ack with missing and remaining", `db.Jobs.ack({lease_id: "` + leaseIDText + `", item_ids: ["00000000000000aa"]})`,
			server.LeaseResponse{Success: true, LeaseState: "acked", Acked: 1, Missing: 1, Remaining: 3}, "acked 1, missing 1, remaining 3\n", ""},
		{"release", `db.Jobs.release({lease_id: "` + leaseIDText + `"})`, server.LeaseResponse{Success: true, LeaseState: "released", Released: 3}, "released 3\n", ""},
		{"extend", `db.Jobs.extend({lease_id: "` + leaseIDText + `", lease: "60s"})`,
			server.LeaseResponse{Success: true, LeaseState: "extended", LeaseMs: 60_000, LeaseDeadline: time.Date(2026, 10, 18, 12, 1, 0, 0, time.UTC)},
			"extended for 1m (until 2026-10-18T12:01:00Z, bucket clock)\n", ""},
		{"ack of an unknown lease", `db.Jobs.ack({lease_id: "` + leaseIDText + `"})`,
			server.LeaseResponse{LeaseState: "unknown", Message: "lease 0b1c…34 is unknown on app.Jobs. Claim again with db.Jobs.claim()"}, "", "lease 0b1c…34 is unknown on app.Jobs. Claim again with db.Jobs.claim()"},
		{"ack of an expired lease", `db.Jobs.ack({lease_id: "` + leaseIDText + `"})`,
			server.LeaseResponse{LeaseState: "expired", Message: expiredText}, "", expiredText},
		{"release of an expired lease", `db.Jobs.release({lease_id: "` + leaseIDText + `"})`,
			server.LeaseResponse{LeaseState: "expired", Message: expiredText}, "", expiredText},
		{"extend of an unknown lease", `db.Jobs.extend({lease_id: "` + leaseIDText + `", lease: "60s"})`,
			server.LeaseResponse{LeaseState: "unknown", Message: "lease is unknown"}, "", "lease is unknown"},
		{"a verb the bucket's type does not have", `db.Users.ack({lease_id: "` + leaseIDText + `"})`,
			server.LeaseResponse{Message: "ack is not defined for hash buckets: only fifo, lifo and heap buckets issue leases."}, "", "ack is not defined for hash buckets: only fifo, lifo and heap buckets issue leases."},
		{"a refusal with no words", `db.Jobs.ack({lease_id: "` + leaseIDText + `"})`,
			server.LeaseResponse{LeaseState: "expired"}, "", "the server refused the ack of the lease without saying why; its state is expired. Claim again with db.Jobs.claim()"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opcode := leaseOpcodes[strings.TrimPrefix(strings.SplitN(tc.statement, "(", 2)[0], "db.Jobs.")]
			if opcode == 0 {
				opcode = leaseOpcodes[strings.TrimPrefix(strings.SplitN(tc.statement, "(", 2)[0], "db.Users.")]
			}
			_, c, out := newLeaseStub(t, map[transport.Opcode]interface{}{opcode: tc.reply})
			err := c.handle(tc.statement)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("%s: %v", tc.statement, err)
				}
				if out.String() != tc.wantOut {
					t.Errorf("printed %q, want %q", out.String(), tc.wantOut)
				}
				return
			}
			var refused *RefusedError
			if !errors.As(err, &refused) || err.Error() != tc.wantErr {
				t.Fatalf("%s = %v, want the refusal %q", tc.statement, err, tc.wantErr)
			}
			if info := classify(err); info.Exit != ExitRefused {
				t.Errorf("exit %d (%s), want %d: an unknown or expired lease is a non-zero exit", info.Exit, info.Code, ExitRefused)
			}
			if strings.Contains(out.String(), "acked") || strings.Contains(out.String(), "released") {
				t.Errorf("a refused verb printed a success line: %q", out.String())
			}
		})
	}
}

// TestEmptyClaimAndEmptyPopSayWhatIsClaimed: an empty answer from a bucket that
// holds claimed entries says how many, and an empty claim that returned expired
// entries says to claim again. The first words stay the bare absence text.
func TestEmptyClaimAndEmptyPopSayWhatIsClaimed(t *testing.T) {
	cases := []struct {
		name, statement string
		opcode          transport.Opcode
		reply           interface{}
		want            string
	}{
		{"claim, nothing claimed", `db.Jobs.claim()`, transport.OpQueueClaim, server.ClaimResponse{Message: "queue is empty"}, "EMPTY  queue is empty\n"},
		{"claim, three claimed", `db.Jobs.claim()`, transport.OpQueueClaim, server.ClaimResponse{Message: "queue is empty", Pending: 3},
			"EMPTY  queue is empty (3 entries are claimed; db.Jobs.len() does not count them)\n"},
		{"claim, one claimed", `db.Jobs.claim()`, transport.OpQueueClaim, server.ClaimResponse{Message: "queue is empty", Pending: 1},
			"EMPTY  queue is empty (1 entry is claimed; db.Jobs.len() does not count it)\n"},
		{"claim, returned entries", `db.Jobs.claim()`, transport.OpQueueClaim, server.ClaimResponse{Message: "queue is empty", Returned: 40},
			"EMPTY  queue is empty (returned 40 expired entries to the queue; claim again)\n"},
		{"claim, three claimed and two dead", `db.Jobs.claim()`, transport.OpQueueClaim, server.ClaimResponse{Message: "queue is empty", Pending: 3, Dead: 2},
			"EMPTY  queue is empty (3 entries are claimed; db.Jobs.len() does not count them) (2 entries are dead-lettered; see db.Jobs.dead())\n"},
		{"pop, three dead", `db.Jobs.pop()`, transport.OpFIFOPop, server.PopResponse{Message: "queue is empty", Dead: 3},
			"EMPTY  queue is empty (3 entries are dead-lettered; see db.Jobs.dead())\n"},
		{"claim on a heap", `db.Tasks.claim()`, transport.OpQueueClaim, server.ClaimResponse{Message: "heap is empty", Pending: 2},
			"EMPTY  heap is empty (2 entries are claimed; db.Tasks.len() does not count them)\n"},
		{"pop, three claimed", `db.Jobs.pop()`, transport.OpFIFOPop, server.PopResponse{Message: "queue is empty", Pending: 3},
			"EMPTY  queue is empty (3 entries are claimed; db.Jobs.len() does not count them)\n"},
		{"pop, nothing claimed", `db.Jobs.pop()`, transport.OpFIFOPop, server.PopResponse{Message: "queue is empty"}, "EMPTY  queue is empty\n"},
		{"peek, three claimed", `db.Jobs.peek()`, transport.OpFIFOPeek, server.PeekResponse{Message: "queue is empty", Pending: 3},
			"EMPTY  queue is empty (3 entries are claimed; db.Jobs.len() does not count them)\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, c, out := newLeaseStub(t, map[transport.Opcode]interface{}{tc.opcode: tc.reply})
			if err := c.handle(tc.statement); err != nil {
				t.Fatalf("%s: %v", tc.statement, err)
			}
			if out.String() != tc.want {
				t.Errorf("printed %q, want %q", out.String(), tc.want)
			}
		})
	}
}

// TestLenAndPeekSayWhatIsClaimed: len() shows the claimed entries beside the
// number, and says when some have expired, which pop and claim return first.
func TestLenAndPeekSayWhatIsClaimed(t *testing.T) {
	entry, _ := bson.Marshal(bson.D{{Key: "task", Value: "a"}})
	cases := []struct {
		name, statement string
		opcode          transport.Opcode
		reply           interface{}
		want            string
	}{
		{"len, none claimed", `db.Jobs.len()`, transport.OpCount, server.CountResponse{Count: 12}, "12\n"},
		{"len, three claimed", `db.Jobs.len()`, transport.OpCount, server.CountResponse{Count: 12, Pending: 3}, "12  (plus 3 claimed)\n"},
		{"len, three claimed and expired", `db.Jobs.len()`, transport.OpCount, server.CountResponse{Count: 12, Pending: 3, Expired: true},
			"12  (plus 3 claimed, some of them expired; claim or pop returns them first)\n"},
		{"count spelling is the same verb", `db.Jobs.count()`, transport.OpCount, server.CountResponse{Count: 0, Pending: 2, Expired: true},
			"0  (plus 2 claimed, some of them expired; claim or pop returns them first)\n"},
		{"peek, expired", `db.Jobs.peek()`, transport.OpFIFOPeek, server.PeekResponse{Found: true, Items: []bson.Raw{entry}, Pending: 3, Expired: true},
			"{\"task\": \"a\"}\n(plus 3 claimed, some of them expired; claim or pop returns them first)\n"},
		{"peek, claimed but none expired", `db.Jobs.peek()`, transport.OpFIFOPeek, server.PeekResponse{Found: true, Items: []bson.Raw{entry}, Pending: 3},
			"{\"task\": \"a\"}\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, c, out := newLeaseStub(t, map[transport.Opcode]interface{}{tc.opcode: tc.reply})
			if err := c.handle(tc.statement); err != nil {
				t.Fatalf("%s: %v", tc.statement, err)
			}
			if out.String() != tc.want {
				t.Errorf("printed %q, want %q", out.String(), tc.want)
			}
		})
	}
}

// TestCompletionOffersTheLeaseVerbs: Tab completes the four verbs after db.X.
// is typed, and `help cla` completes the topic.
func TestCompletionOffersTheLeaseVerbs(t *testing.T) {
	c := NewCLI()
	for prefix, want := range map[string]string{"cla": "claim", "ack": "ack", "rel": "release", "ext": "extend", "dea": "dead", "req": "requeue", "dis": "discard"} {
		_, got := c.completer()(prefix, len(prefix))
		if !slices.Contains(got, want) {
			t.Errorf("completing %q offered %v, want it to include %q", prefix, got, want)
		}
	}
	if _, got := c.completer()("help cla", 8); len(got) != 1 || got[0] != "claim" {
		t.Errorf("completing `help cla` offered %v, want [claim]", got)
	}
}

// TestHelpTeachesTheLeaseVerbs: the queue and heap blocks of the quick
// reference list the four verbs, pop's line says it removes at most once, and
// help claim shows the loop.
func TestHelpTeachesTheLeaseVerbs(t *testing.T) {
	var quick bytes.Buffer
	NewCommandHandler(&quick, false).handleHelp()
	text := quick.String()
	queueBlock := text[strings.Index(text, "Queue Operations"):strings.Index(text, "B+Tree Operations")]
	heapBlock := text[strings.Index(text, "Heap Operations"):strings.Index(text, "Store Fields")]
	for name, block := range map[string]string{"queue": queueBlock, "heap": heapBlock} {
		for _, verb := range []string{".claim(", ".ack(", ".release(", ".extend(", ".dead(", ".requeue(", ".discard("} {
			if !strings.Contains(block, verb) {
				t.Errorf("the %s block of help lacks %s", name, verb)
			}
		}
		if !strings.Contains(block, "removes at most once; to process without losing work, use claim and ack") {
			t.Errorf("the %s block does not say pop removes at most once and point to claim", name)
		}
	}

	var topic bytes.Buffer
	if err := NewCommandHandler(&topic, false).handleHelpTopic("claim"); err != nil {
		t.Fatalf("help claim: %v", err)
	}
	for _, want := range []string{`db.Jobs.claim({limit: 10, lease: "30s"})`, "db.Jobs.ack(", "db.Jobs.extend(", "db.Jobs.release(", "acked 1", "lease_id 9f3a",
		"db.Jobs.dead()", "Entries claimed 5 times without an acknowledgement", `db.Jobs.requeue({all: true})`, `db.Jobs.discard({dead_id: "<dead_id>"})`} {
		if !strings.Contains(topic.String(), want) {
			t.Errorf("help claim lacks %q:\n%s", want, topic.String())
		}
	}
}

// TestLeaseVerbsDoNotForgetDeclarations: a claim is a data verb, so it leaves
// the cached declarations alone. Left off forgetsDeclarations' list, every
// claim would drop them and cost the next field verb a round trip.
func TestLeaseVerbsDoNotForgetDeclarations(t *testing.T) {
	for _, verb := range []string{"claim", "ack", "release", "extend", "dead", "requeue", "discard"} {
		cmd, err := ParseCommand(`db.Jobs.` + verb + `()`)
		if err != nil {
			t.Fatalf("parsing %s: %v", verb, err)
		}
		if forgetsDeclarations(cmd) {
			t.Errorf("%s forgets the cached declarations", verb)
		}
	}
}

// TestClaimAckLoopAgainstARealServer runs help claim's loop on a real single
// node, on a fifo, a lifo and a heap: claim prints a lease line a person can
// copy, ack takes its item ids, len and an empty pop say what is claimed, and a
// lease that was never issued is a refusal with the server's fix.
func TestClaimAckLoopAgainstARealServer(t *testing.T) {
	c := newDumpTestServer(t)
	for _, line := range []string{
		`create database app`,
		`create bucket Jobs { type: fifo }`, `create bucket Stack { type: lifo }`, `create bucket Tasks { type: heap }`,
		`create bucket Users { type: hash }`,
	} {
		if _, err := runStatement(t, c, line); err != nil {
			t.Fatalf("%s: %v", line, err)
		}
	}
	pushes := map[string][]string{
		"Jobs":  {`{"n": 1}`, `{"n": 2}`, `{"n": 3}`},
		"Stack": {`{"n": 1}`, `{"n": 2}`, `{"n": 3}`},
		"Tasks": {`{"priority": 3, "n": 1}`, `{"priority": 1, "n": 2}`, `{"priority": 2, "n": 3}`},
	}
	for bucket, documents := range pushes {
		for _, document := range documents {
			if _, err := runStatement(t, c, `db.`+bucket+`.push(`+document+`)`); err != nil {
				t.Fatalf("push into %s: %v", bucket, err)
			}
		}
	}

	leaseLine := regexp.MustCompile(`(?m)^lease_id ([0-9a-f]{40}) for 30s; item_ids \[("[0-9a-f]+"(?:, "[0-9a-f]+")*)\]$`)
	for _, bucket := range []string{"Jobs", "Stack", "Tasks"} {
		t.Run(bucket, func(t *testing.T) {
			printed, err := runStatement(t, c, `db.`+bucket+`.claim({limit: 2, lease: "30s"})`)
			if err != nil {
				t.Fatalf("claim: %v", err)
			}
			parts := leaseLine.FindStringSubmatch(printed)
			if parts == nil {
				t.Fatalf("claim printed no lease line a person can copy:\n%s", printed)
			}
			leaseID, itemIDs := parts[1], strings.Split(parts[2], ", ")
			if len(itemIDs) != 2 || strings.Count(printed, "\n") != 3 {
				t.Fatalf("claim of 2 printed %d item ids and %d lines:\n%s", len(itemIDs), strings.Count(printed, "\n"), printed)
			}

			printed, err = runStatement(t, c, `db.`+bucket+`.len()`)
			if err != nil || printed != "1  (plus 2 claimed)\n" {
				t.Errorf("len() = %q, %v, want the one unclaimed entry and the two claimed", printed, err)
			}
			if printed, err = runStatement(t, c, `db.`+bucket+`.ack({lease_id: "`+leaseID+`", item_ids: [`+itemIDs[0]+`]})`); err != nil || printed != "acked 1, remaining 1\n" {
				t.Errorf("ack of one = %q, %v", printed, err)
			}
			if printed, err = runStatement(t, c, `db.`+bucket+`.release({lease_id: "`+leaseID+`"})`); err != nil || printed != "released 1\n" {
				t.Errorf("release = %q, %v", printed, err)
			}
			if _, err = runStatement(t, c, `db.`+bucket+`.release({lease_id: "`+leaseID+`"})`); err == nil || !strings.Contains(err.Error(), "unknown") {
				t.Errorf("a lease released twice = %v, want the unknown-lease refusal", err)
			}
			if _, err = runStatement(t, c, `db.`+bucket+`.ack({lease_id: "`+leaseID+`"})`); err == nil || classify(err).Exit != ExitRefused {
				t.Errorf("ack of a finished lease = %v, want a refusal with a non-zero exit", err)
			}
		})
	}

	t.Run("an empty queue with claims", func(t *testing.T) {
		if _, err := runStatement(t, c, `db.Jobs.claim({limit: 100})`); err != nil {
			t.Fatal(err)
		}
		printed, err := runStatement(t, c, `db.Jobs.claim()`)
		if err != nil || !strings.HasPrefix(printed, "EMPTY  queue is empty (") || !strings.Contains(printed, "entries are claimed; db.Jobs.len() does not count them)") {
			t.Errorf("claim of a drained queue = %q, %v", printed, err)
		}
		printed, err = runStatement(t, c, `db.Jobs.pop()`)
		if err != nil || !strings.Contains(printed, "entries are claimed; db.Jobs.len() does not count them)") {
			t.Errorf("pop of a drained queue = %q, %v", printed, err)
		}
	})

	t.Run("a bucket with no order to take from", func(t *testing.T) {
		_, err := runStatement(t, c, `db.Users.claim()`)
		requireWrongTypeRefused(t, err, "claim is not defined for hash buckets")
	})

	t.Run("extend moves the lease", func(t *testing.T) {
		printed, err := runStatement(t, c, `db.Tasks.claim({limit: 1, lease: "10s"})`)
		if err != nil {
			t.Fatal(err)
		}
		parts := regexp.MustCompile(`lease_id ([0-9a-f]{40}) for 10s`).FindStringSubmatch(printed)
		if parts == nil {
			t.Fatalf("claim printed %q", printed)
		}
		printed, err = runStatement(t, c, `db.Tasks.extend({lease_id: "`+parts[1]+`", lease: "5m"})`)
		if err != nil || !strings.HasPrefix(printed, "extended for 5m (until 20") {
			t.Errorf("extend = %q, %v", printed, err)
		}
	})
}
