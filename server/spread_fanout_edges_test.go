package server

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/transport"
)

// compareThese is compare over a list of queries of the test's own.
func (tw *fanOutTwins) compareThese(phase string, queries []fanOutQuery) {
	tw.t.Helper()
	for _, query := range queries {
		wholeReply, spreadReply := tw.whole.send(query.op, query.request), tw.spread.send(query.op, query.request)
		if spreadReply.Opcode != wholeReply.Opcode {
			tw.t.Errorf("%s, %s: whole answered %s %q, spread %s %q", phase, query.label, wholeReply.Opcode, wholeReply.Payload, spreadReply.Opcode, spreadReply.Payload)
			continue
		}
		if bsonNamesAPart(spreadReply.Payload) {
			tw.t.Errorf("%s, %s: the spread answer names a part: %q", phase, query.label, spreadReply.Payload)
		}
		want, got := normalisedAnswer(tw.t, wholeReply.Payload, query), normalisedAnswer(tw.t, spreadReply.Payload, query)
		if !bytes.Equal(want, got) {
			tw.t.Errorf("%s, %s: the spread answer differs from the whole bucket's:\nwhole  %s\nspread %s", phase, query.label, bson.Raw(want), bson.Raw(got))
		}
	}
}

// edgeScan is a find of app.User compared byte for byte but for bucket_id.
func edgeScan(label string, request ScanRequest) fanOutQuery {
	request.Database, request.Bucket = "app", "User"
	return fanOutQuery{label: label, op: transport.OpScan, request: request, ignore: []string{"bucket_id"}}
}

// TestSpreadFanOutMatchesAWholeBucketAtTheEdges is the step 3 verifier's
// differential edge test: mixed-type and missing sort keys, null against
// missing, odd ids, skips past the end, limit 0 and negative, windows that
// cross many parts, filters that match one part or none, refused requests.
// A search with a limit is left out: its set is the parts' first hits, not a
// whole bucket's (a known difference, recorded for step 7).
func TestSpreadFanOutMatchesAWholeBucketAtTheEdges(t *testing.T) {
	tw := newFanOutTwins(t)
	now := time.Now()
	values := []interface{}{nil, "missing", int32(5), int64(5), 5.0, "five", true, false,
		map[string]interface{}{"a": int32(1)}, []interface{}{int32(1)}, primitive.NewDateTimeFromTime(now), -1.5, "", int32(-7)}
	ids := []string{"A", "a", "z", "~", "é", "Ω", "0", "00", "a b", "a.b", "a-1", "a-10", "a-2"}
	for i := 0; i < 900; i++ {
		id := fmt.Sprintf("k%04d", i)
		if i < len(ids) {
			id = ids[i]
		}
		document := map[string]interface{}{"Name": fmt.Sprintf("n%03d", i%37), "Age": int32(i % 4), "CreatedAt": primitive.NewDateTimeFromTime(now)}
		switch v := values[i%len(values)]; v {
		case "missing":
		default:
			document["Score"] = v
		}
		tw.both(transport.OpPut, InsertRequest{Database: "app", Bucket: "User", Key: id, Data: document})
	}
	one := map[string]interface{}{"Name": "n005"}
	queries := []fanOutQuery{
		edgeScan("sort mixed asc", ScanRequest{Sort: map[string]int{"Score": 1}, Limit: 400}),
		edgeScan("sort mixed desc", ScanRequest{Sort: map[string]int{"Score": -1}, Skip: 33, Limit: 400}),
		edgeScan("sort missing field", ScanRequest{Sort: map[string]int{"Nope": 1}, Skip: 10, Limit: 120}),
		edgeScan("sort missing field desc", ScanRequest{Sort: map[string]int{"Nope": -1}, Skip: 10, Limit: 120}),
		edgeScan("sort tied desc wide", ScanRequest{Sort: map[string]int{"Age": -1}, Skip: 200, Limit: 300}),
		edgeScan("skip past the end", ScanRequest{Skip: 10000, Limit: 10}),
		edgeScan("skip past the end sorted", ScanRequest{Skip: 10000, Limit: 10, Sort: map[string]int{"Age": 1}}),
		edgeScan("skip past the end filtered", ScanRequest{Skip: 10000, Limit: 10, Filter: map[string]interface{}{"Age": int32(1)}}),
		edgeScan("limit 0", ScanRequest{Limit: 0}),
		edgeScan("limit negative", ScanRequest{Limit: -5, Skip: 3}),
		edgeScan("skip negative", ScanRequest{Limit: 7, Skip: -3}),
		edgeScan("window across parts", ScanRequest{Skip: 450, Limit: 90}),
		edgeScan("filter matching one part", ScanRequest{Filter: map[string]interface{}{"_id_": "k0500"}}),
		edgeScan("filter one name", ScanRequest{Filter: one, Limit: 3}),
		edgeScan("filter nothing", ScanRequest{Filter: map[string]interface{}{"Name": "zz"}}),
		edgeScan("filter nothing sorted", ScanRequest{Filter: map[string]interface{}{"Name": "zz"}, Sort: map[string]int{"Age": 1}}),
		edgeScan("filter null", ScanRequest{Filter: map[string]interface{}{"Score": nil}, Limit: 500}),
		edgeScan("project exclude id sorted", ScanRequest{Projection: map[string]int{"_id_": 0}, Sort: map[string]int{"Score": 1}, Limit: 50}),
		edgeScan("project missing field", ScanRequest{Projection: map[string]int{"Nope": 1}, Limit: 50}),
		{label: "count one", op: transport.OpCount, request: CountRequest{Database: "app", Bucket: "User", Filter: one}},
		{label: "distinct Score", op: transport.OpDistinct, request: DistinctRequest{Database: "app", Bucket: "User", Field: "Score"}, asSet: "values"},
		{label: "distinct missing", op: transport.OpDistinct, request: DistinctRequest{Database: "app", Bucket: "User", Field: "Nope"}, asSet: "values"},
		{label: "search index unlimited", op: transport.OpSearch, request: SearchRequest{Database: "app", Bucket: "User", Filter: map[string]interface{}{"Age": int32(2)}}, asSet: "results"},
		{label: "search no filter", op: transport.OpSearch, request: SearchRequest{Database: "app", Bucket: "User"}, asSet: "results"},
		{label: "find bad sort", op: transport.OpScan, request: ScanRequest{Database: "app", Bucket: "User", Sort: map[string]int{"A": 1, "B": 1}}},
		{label: "find bad filter", op: transport.OpScan, request: ScanRequest{Database: "app", Bucket: "User", Filter: map[string]interface{}{"$bogus": 1}}},
	}
	tw.compareThese("edges", queries)
}

// TestDeepPageRefusalSaysWhatWasAsked is the step 3 verifier's deep page: a
// whole bucket answers find({}, {skip: 2900, limit: 10}) under a small result
// ceiling, while the spread collection asks every part for its first 2,910
// and refuses. The refusal must describe the request (its skip and limit), not
// "no limit". Answering it as a whole bucket does is a step 7 gate.
func TestDeepPageRefusalSaysWhatWasAsked(t *testing.T) {
	small := func(cfg *config.Config) { cfg.Query.MaxResultBytes = 256 << 10 }
	s := newTestServerWith(t, small)
	n := &spreadNode{t: t, s: s, handler: NewHandler(s)}
	if reply := n.send(transport.OpCreateBucket, CreateBucketRequest{Database: "app", Name: "User", Type: BucketTypeHash, Options: map[string]interface{}{"placement": "spread"}}); reply.Opcode == transport.OpError {
		t.Fatal(refusalText(t, reply))
	}
	batch := make([]map[string]interface{}, 0, 3000)
	for i := 0; i < 3000; i++ {
		batch = append(batch, map[string]interface{}{"_id_": fmt.Sprintf("k%05d", i), "Pad": string(bytes.Repeat([]byte("x"), 200))})
	}
	for start := 0; start < len(batch); start += 500 {
		if reply := n.send(transport.OpBatchPut, BatchInsertRequest{Database: "app", Bucket: "User", Data: batch[start : start+500]}); reply.Opcode == transport.OpError {
			t.Fatal(refusalText(t, reply))
		}
	}
	reply := n.send(transport.OpScan, ScanRequest{Database: "app", Bucket: "User", Skip: 2900, Limit: 10})
	if reply.Opcode != transport.OpError {
		t.Skip("the deep page was answered: the step 7 gate is met; this test then only guards the text")
	}
	text := refusalTextOrEmpty(t, reply)
	if strings.Contains(text, "no limit") || !strings.Contains(text, "10") {
		t.Errorf("the deep page's refusal does not describe the request (skip 2900, limit 10): %q", text)
	}
}
