package wire

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

func TestARingBatchOpRefusedDoesNotStopTheOthers(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	good1 := env.doc(t, cluster.OpUpsert, "ok-1", "one")
	bad := env.doc(t, cluster.OpUpsert, "bad", "x")
	bad.Op.Fields[0].FieldID = env.fieldID(t, "Queue") // a store field is not a scalar: FIELD_TYPE
	good2 := env.doc(t, cluster.OpUpsert, "ok-2", "two")
	reply := ringBatchReply(t, c.batch(RingBatchRequest{Ops: ringBatchOpsOf(good1, bad, good2)}), 3)
	if reply.Outcome != "partial" {
		t.Fatalf("outcome %q, want partial", reply.Outcome)
	}
	if reply.Answers[0].Reply == nil || reply.Answers[2].Reply == nil {
		t.Fatalf("a refused operation stopped the others: %+v", reply.Answers)
	}
	if refusal := reply.Answers[1].Error; refusal == nil || refusal.Code != cluster.CodeFieldType {
		t.Fatalf("operation 1 = %+v, want FIELD_TYPE", reply.Answers[1])
	}
	for _, id := range []string{"ok-1", "ok-2"} {
		if got := u66Reply(t, c.keyed(env.read(t, RingClassGet, id, "", 0))); got.Found == nil || !*got.Found {
			t.Fatalf("document %s was not written", id)
		}
	}
	if got := u66Reply(t, c.keyed(env.read(t, RingClassGet, "bad", "", 0))); got.Found == nil || *got.Found {
		t.Fatal("the refused document was written")
	}
}

// ringBatchSmallBudget serves a node whose batch reply budget is 1800 bytes and whose largest
// element is taken to be 1100 bytes, so one 1000-byte element fills most of the budget.
func ringBatchSmallBudget(t *testing.T) *u66Env {
	t.Helper()
	node, _ := u63Fresh(t)
	return u66ServeWith(t, node, WithRingResultBudget(1800), func(s *ringServer) { s.elementCeiling = 1100 })
}

func ringBatchPushBig(t *testing.T, env *u66Env, c *u66Conn, n int) {
	t.Helper()
	big := bytes.Repeat([]byte("x"), 1000)
	var pushes []RingKeyedRequest
	for i := range n {
		item := u66Item(t, string(big)+string(rune('a'+i)))
		arg := u66Arg(t, func() ([]byte, error) { return encodeElementArg(item) })
		pushes = append(pushes, env.verb(t, "b1", "Queue", cluster.VerbPush, arg))
	}
	ringBatchReply(t, c.batch(RingBatchRequest{Ops: ringBatchOpsOf(pushes...)}), n)
}

// TestARingBatchNeverAppliesAPopItCannotAnswer: [GUARD a pop removes its element, so an answer that
// dropped it would destroy data]. Three pops whose answers do not all fit the budget: the ones that
// run keep their element whole, and the one that cannot be answered is not run at all.
func TestARingBatchNeverAppliesAPopItCannotAnswer(t *testing.T) {
	env := ringBatchSmallBudget(t)
	c := env.login(t)
	ringBatchPushBig(t, env, c, 3)
	var pops []RingKeyedRequest
	for range 3 {
		pops = append(pops, env.verb(t, "b1", "Queue", cluster.VerbPop, nil))
	}
	reply := ringBatchReply(t, c.batch(RingBatchRequest{Ops: ringBatchOpsOf(pops...)}), 3)
	applied := 0
	for i, answer := range reply.Answers {
		switch {
		case answer.Reply != nil:
			applied++
			if len(answer.Reply.Elements) != 1 {
				t.Fatalf("pop %d was applied and its answer has %d elements: the element is lost", i, len(answer.Reply.Elements))
			}
		case answer.Error == nil || answer.Error.Code != cluster.CodeUnavailable || !strings.Contains(answer.Error.Message, "not applied"):
			t.Fatalf("pop %d = %+v, want a reply with its element or a not-applied refusal", i, answer)
		}
	}
	if applied == 0 || applied == 3 {
		t.Fatalf("%d pops were applied; the budget fits some and not all", applied)
	}
	count := u66Reply(t, c.keyed(env.read(t, RingClassStoreCount, "b1", "Queue", 0)))
	if count.Count == nil || *count.Count != int64(3-applied) {
		t.Fatalf("the queue holds %v elements after %d pops were applied, want %d", count.Count, applied, 3-applied)
	}
}

// TestARingBatchPopsRunTogetherUnderTheDefaultCeiling: [GUARD the reservation is the real bound, 1 KiB
// plus the envelope, not the 1 MiB a stored element can be]. With the smallest legal budget (1 MiB)
// and the production ceiling, a frame of 100 pops of 1 KiB-class elements is not refused.
func TestARingBatchPopsRunTogetherUnderTheDefaultCeiling(t *testing.T) {
	node, _ := u63Fresh(t)
	env := u66ServeWith(t, node, WithRingResultBudget(1<<20))
	c := env.login(t)
	const pops = 100
	ringBatchPushBig(t, env, c, 20)
	var requests []RingKeyedRequest
	for range pops {
		requests = append(requests, env.verb(t, "b1", "Queue", cluster.VerbPop, nil))
	}
	reply := ringBatchReply(t, c.batch(RingBatchRequest{Ops: ringBatchOpsOf(requests...)}), pops)
	for i := range 20 {
		if answer := reply.Answers[i]; answer.Reply == nil || len(answer.Reply.Elements) != 1 {
			t.Fatalf("pop %d = %+v, want its element: the reservation refused a pop that fits", i, answer)
		}
	}
}

// TestARingBatchChargesAPopBeforeTheRepliesItCanDrop: the budget goes to the answers that cannot be
// recomputed first, whatever their place in the frame. Twelve upserts then a pop: the upserts' replies
// are trimmed to their outcome if they do not fit, the pop keeps its element.
func TestARingBatchChargesAPopBeforeTheRepliesItCanDrop(t *testing.T) {
	env := ringBatchSmallBudget(t)
	c := env.login(t)
	ringBatchPushBig(t, env, c, 1)
	var requests []RingKeyedRequest
	for i := range 12 {
		requests = append(requests, env.doc(t, cluster.OpUpsert, "charge-"+string(rune('a'+i)), "x"))
	}
	requests = append(requests, env.verb(t, "b1", "Queue", cluster.VerbPop, nil))
	reply := ringBatchReply(t, c.batch(RingBatchRequest{Ops: ringBatchOpsOf(requests...)}), 13)
	popped := reply.Answers[12].Reply
	if popped == nil || len(popped.Elements) != 1 {
		t.Fatalf("the pop at the end of the frame lost its element: %+v", reply.Answers[12])
	}
	for i := range 12 {
		if answer := reply.Answers[i]; answer.Reply == nil || answer.Reply.WriteOutcome != "applied" {
			t.Fatalf("upsert %d = %+v, want its outcome kept", i, answer)
		}
	}
}

// TestARingBatchOfOneAnswersAsTheLoneRequest: [GUARD rule 7, predictable APIs]. A batch of one
// under a request id a lone request already used is the same dedupe entry, and its inner reply
// is the lone request's reply, byte for byte.
func TestARingBatchOfOneAnswersAsTheLoneRequest(t *testing.T) {
	env := u66Start(t)
	c := env.login(t)
	request := env.doc(t, cluster.OpUpsert, "solo", "Solo")
	payload, err := EncodeRingKeyed(request)
	if err != nil {
		t.Fatal(err)
	}
	id := [16]byte{0xBB, 1}
	first := u66Reply(t, c.send(transport.OpRingKeyed, id, payload))
	if first.Deduped {
		t.Fatal("the first lone request answered deduped")
	}
	lone := c.send(transport.OpRingKeyed, id, payload)
	batched := ringBatchReply(t, c.batchWithID(id, RingBatchRequest{Ops: ringBatchOpsOf(request)}), 1)
	inner := batched.Answers[0].Reply
	if inner == nil || !inner.Deduped {
		t.Fatalf("a batch of one under the lone request's id must hit its dedupe entry: %+v", batched.Answers[0])
	}
	want, err := DecodeRingReply(lone.Payload)
	if err != nil {
		t.Fatal(err)
	}
	wantRaw, err := EncodeRingReply(want)
	if err != nil {
		t.Fatal(err)
	}
	gotRaw, err := EncodeRingReply(*inner)
	if err != nil || !bytes.Equal(gotRaw, wantRaw) {
		t.Fatalf("inner reply %x differs from the lone reply %x (%v)", gotRaw, wantRaw, err)
	}
}

func ringBatchReplySeeds(t *testing.T) []RingReply {
	t.Helper()
	epoch, state, copies := int64(4), int64(9), int32(1)
	applied := RingReply{WriteOutcome: "applied", CopiesConfirmed: &copies, ServedBy: "n1", Epoch: &epoch, StateSeq: &state}
	refused := RingError{Code: cluster.CodeFieldType, Message: "field Queue is a store"}
	return []RingReply{
		{Outcome: "ok", Answers: []RingAnswer{{Reply: &applied}}, Epoch: &epoch, StateSeq: &state},
		{Outcome: "partial", Answers: []RingAnswer{{Reply: &applied}, {Error: &refused}, {Reply: &applied}}, Epoch: &epoch, StateSeq: &state},
	}
}

func TestRingBatchReplyRoundTripsAndRefusesWhatItShouldNot(t *testing.T) {
	seeds := ringBatchReplySeeds(t)
	for _, seed := range seeds {
		raw, err := EncodeRingReply(seed)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeRingReply(raw)
		if err != nil {
			t.Fatal(err)
		}
		if again, err := EncodeRingReply(got); err != nil || !bytes.Equal(again, raw) {
			t.Fatalf("re-encode differs: %v", err)
		}
	}
	wrong := seeds[1]
	wrong.Outcome = "ok"
	if _, err := EncodeRingReply(wrong); err == nil || !strings.Contains(err.Error(), "outcome") {
		t.Fatalf("an ok outcome over an error answer was accepted: %v", err)
	}
	both := seeds[1]
	both.Answers = []RingAnswer{{Reply: both.Answers[0].Reply, Error: both.Answers[1].Error}}
	if _, err := EncodeRingReply(both); err == nil {
		t.Fatal("an answer that is a reply and an error was accepted")
	}
	nested := seeds[0]
	nested.Answers = []RingAnswer{{Reply: &seeds[0]}}
	if _, err := EncodeRingReply(nested); err == nil || !strings.Contains(err.Error(), "nest") {
		t.Fatalf("a batch reply inside a batch reply was accepted: %v", err)
	}
	extra := seeds[0]
	extra.WriteOutcome = "applied"
	if _, err := EncodeRingReply(extra); err == nil {
		t.Fatal("a batch reply with a lone reply's member was accepted")
	}
}

func TestRingBatchReplyDecoderSurvivesMutation(t *testing.T) {
	var seeds [][]byte
	for _, seed := range ringBatchReplySeeds(t) {
		raw, err := EncodeRingReply(seed)
		if err != nil {
			t.Fatal(err)
		}
		seeds = append(seeds, raw)
	}
	rng := rand.New(rand.NewPCG(0xe1, 0xe1))
	accepted := 0
	for range 100_000 {
		mutated := u65Mutate(rng, seeds[rng.IntN(len(seeds))])
		reply, err := DecodeRingReply(mutated)
		if err != nil {
			continue
		}
		accepted++
		if again, err := EncodeRingReply(reply); err != nil || !bytes.Equal(again, mutated) {
			t.Fatalf("accepted reply %x re-encodes as %x, %v", mutated, again, err)
		}
	}
	if accepted == 0 {
		t.Fatal("no mutant was accepted")
	}
}

// TestARingBatchNeverDropsTheElementOfAnIncReturnAfter: [GUARD an inc with return:"after" is not
// recomputable: its dedupe entry holds the after-element only if it fits, and a replay of the same
// request id cannot rebuild it from the store once later incs moved the counters]. Three incs with
// a small reply budget: each answer either keeps its element whole or says the inc was not applied;
// none says found with the element trimmed away. Whether all, some or none fit is not pinned.
func TestARingBatchNeverDropsTheElementOfAnIncReturnAfter(t *testing.T) {
	env := ringBatchSmallBudget(t)
	c := env.login(t)
	increments := bson.D{}
	for i := range 20 {
		increments = append(increments, bson.E{Key: fmt.Sprintf("counter-%036d", i), Value: int32(1)})
	}
	raw := u63Doc(t, increments)
	var incs []RingKeyedRequest
	for i := range 3 {
		arg := u66Arg(t, func() ([]byte, error) { return IncArg(fmt.Sprintf("k%d", i), raw, true) })
		incs = append(incs, env.verb(t, "b1", "Table", cluster.VerbInc, arg))
	}
	reply := ringBatchReply(t, c.batch(RingBatchRequest{Ops: ringBatchOpsOf(incs...)}), 3)
	applied := 0
	for i, answer := range reply.Answers {
		switch {
		case answer.Reply != nil:
			applied++
			if len(answer.Reply.Elements) != 1 {
				t.Fatalf("inc %d was applied and its answer has %d elements: the after-element is lost", i, len(answer.Reply.Elements))
			}
		case answer.Error == nil || answer.Error.Code != cluster.CodeUnavailable || !strings.Contains(answer.Error.Message, "not applied"):
			t.Fatalf("inc %d = %+v, want a reply with its element or a not-applied refusal", i, answer)
		}
	}
	// The property is the loop above; at least one inc must have run, or nothing was exercised.
	if applied == 0 {
		t.Fatal("no inc was applied: the test never saw an after-element")
	}
}
