package server

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// matrixVerbWords is the driver verbs behind each matrixRows label, in the
// words a refusal's message must use to say what a type takes (BUG-0469).
var matrixVerbWords = map[string][]string{
	"put (keyed insert, refuses a key the bucket holds)": {"put"},
	"insert (keyless write, the server assigns the key)": {"insert"},
	"insert many":                  {"insert many"},
	"get":                          {"get"},
	"exists":                       {"exists"},
	"delete":                       {"delete"},
	"update":                       {"update"},
	"push":                         {"push"},
	"pop":                          {"pop"},
	"peek":                         {"peek"},
	"claim":                        {"claim"},
	"ack, release, extend":         {"ack", "release", "extend"},
	"dead, requeue, discard":       {"dead", "requeue", "discard"},
	"range, first, last":           {"range", "first", "last"},
	"put blob, get blob":           {"put blob", "get blob"},
	"find, distinct":               {"find", "distinct"},
	"count":                        {"count"},
	"search (secondary index)":     {"search"},
	"stats":                        {"stats"},
	"consistent get (quorum read)": {"consistent get"},
}

// verbsTheMatrixGives lists, in matrixRows order, the verbs the matrix says a
// type answers.
func verbsTheMatrixGives(typeIndex int) []string {
	var verbs []string
	for _, row := range matrixRows {
		if row.answered[typeIndex] {
			verbs = append(verbs, matrixVerbWords[row.label]...)
		}
	}
	return verbs
}

// TestEveryVerbOnTheWrongStoreTypeIsRefusedTheSameWay is BUG-0469 item 1:
// every verb of operations.md section 1 sent to every store type that does not
// answer it takes ONE path out: an ERROR frame, INVALID_ARGUMENT (rule 7:
// predictable APIs; BUG-0515 did it for the verbs that were already errors),
// whose message names the bucket's type and every verb that type takes, and
// nothing is applied. Before the fix POP, PEEK, FIFO_PUSH, CONSISTENT_GET and
// SCAN answered in band (found:false or success:false).
func TestEveryVerbOnTheWrongStoreTypeIsRefusedTheSameWay(t *testing.T) {
	s := newTestServer(t)
	labelWords := strings.NewReplacer(" ", "_", ",", "", "(", "", ")", "")
	pairs := 0
	for _, row := range matrixRows {
		for typeIndex, bucketType := range matrixTypes {
			if row.answered[typeIndex] {
				continue
			}
			for probeIndex, probe := range row.probes {
				pairs++
				bucket := "w_" + labelWords.Replace(row.label) + "_" + string(bucketType) + "_" + string(rune('a'+probeIndex))
				createTestBucket(t, s, "app", bucket, bucketType)
				seedMatrixBucket(t, s, bucket, bucketType)
				before := wrongTypeEntryCount(t, s, bucket)

				reply, err := s.dispatch(context.Background(), requestFrame(t, probe.opcode, probe.request(bucket)))
				if err != nil {
					t.Fatalf("dispatching %s: %v", probe.opcode, err)
				}
				fireWrittenHook(reply)
				what := row.label + " (" + probe.opcode.String() + ") on a " + string(bucketType) + " bucket"
				if reply.Opcode != transport.OpError {
					var answer bson.M
					_ = bson.Unmarshal(reply.Payload, &answer)
					t.Errorf("%s: answered in band as %s %s, want an ERROR frame", what, reply.Opcode, matrixStringify(answer))
					continue
				}
				refusal := decodeErrorFrame(t, reply, what)
				if refusal.Code != 4 {
					t.Errorf("%s: ERROR code %d (%q), want 4 INVALID_ARGUMENT", what, refusal.Code, refusal.Message)
				}
				if !strings.Contains(refusal.Message, string(bucketType)) {
					t.Errorf("%s: message %q does not name the bucket's type", what, refusal.Message)
				}
				takes := "A " + string(bucketType) + " bucket takes: " + strings.Join(verbsTheMatrixGives(typeIndex), ", ") + "."
				if !strings.HasSuffix(refusal.Message, takes) {
					t.Errorf("%s: message %q does not end with the verbs the type takes (%q)", what, refusal.Message, takes)
				}
				if after := wrongTypeEntryCount(t, s, bucket); after != before {
					t.Errorf("%s: the refusal changed the bucket from %d to %d entries", what, before, after)
				}
			}
		}
	}
	if pairs < 60 {
		t.Fatalf("only %d wrong-type pairs were exercised: the matrix lost rows", pairs)
	}
}

// wrongTypeEntryCount counts a bucket through the unfiltered COUNT every type
// answers.
func wrongTypeEntryCount(t *testing.T, s *Server, bucket string) int {
	t.Helper()
	reply, err := s.dispatch(context.Background(), requestFrame(t, transport.OpCount, CountRequest{Database: "app", Bucket: bucket}))
	if err != nil {
		t.Fatalf("counting %s: %v", bucket, err)
	}
	fireWrittenHook(reply)
	var counted bson.M
	if err := bson.Unmarshal(reply.Payload, &counted); err != nil {
		t.Fatalf("decoding the count of %s: %v", bucket, err)
	}
	switch n := counted["count"].(type) {
	case int32:
		return int(n)
	case int64:
		return int(n)
	}
	t.Fatalf("count of %s: %s", bucket, matrixStringify(counted))
	return 0
}

// TestStoreTypeVerbsMatchTheMatrix keeps the verbs a refusal lists (the
// server's storeTypeVerbs) equal to the matrix this suite holds the handbook to,
// so the advice in an error cannot drift from what the type answers.
func TestStoreTypeVerbsMatchTheMatrix(t *testing.T) {
	for typeIndex, bucketType := range matrixTypes {
		want := strings.Join(verbsTheMatrixGives(typeIndex), ", ")
		if got := strings.Join(storeTypeVerbs[bucketType], ", "); got != want {
			t.Errorf("storeTypeVerbs[%s] = %q, the matrix gives %q", bucketType, got, want)
		}
	}
}

// requireWrongTypeRefusal fails unless err is a refusal under
// ErrWrongBucketType (INVALID_ARGUMENT on the wire) whose message holds every
// fragment; the tests that once read an in-band found:false use it.
func requireWrongTypeRefusal(t *testing.T, err error, fragments ...string) {
	t.Helper()
	if !errors.Is(err, ErrWrongBucketType) {
		t.Fatalf("got %v, want a refusal under ErrWrongBucketType", err)
	}
	for _, fragment := range fragments {
		if !strings.Contains(err.Error(), fragment) {
			t.Errorf("the refusal %q lacks %q", err.Error(), fragment)
		}
	}
}
