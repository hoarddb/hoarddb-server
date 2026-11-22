package server

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestResultTooLargeErrorCarriesItsFields is W-2 and W-3 end to end through
// errorFrame: a result-too-large error wrapped twice, the way a walk's error
// reaches a handler through BTreeStore.IterateDocuments' "iterating key %q:"
// and an executor's own context, decodes to code 14 with both optional fields,
// and its message is the typed error's own text with no wrap prefix.
func TestResultTooLargeErrorCarriesItsFields(t *testing.T) {
	refused := &resultTooLargeError{
		query:     "find() on app.events",
		shape:     resultShapeFind,
		ceiling:   16 << 20,
		retained:  16<<20 + 85,
		documents: 41213,
		limit:     100000,
	}
	wrapped := fmt.Errorf("scanning app.events: %w", fmt.Errorf("iterating key %q: %w", "k0041213", refused))
	if !errors.Is(wrapped, ErrResultTooLarge) {
		t.Fatal("the wrapped error does not match ErrResultTooLarge with errors.Is")
	}

	frame := errorFrame(transport.NewFrame(transport.OpScan, nil), wrapped)
	if frame.Opcode != transport.OpError {
		t.Fatalf("errorFrame opcode = %s, want OpError", frame.Opcode)
	}
	var decoded struct {
		Code           int    `bson:"code"`
		Message        string `bson:"message"`
		MaxResultBytes *int64 `bson:"max_result_bytes"`
		RetainedBytes  *int64 `bson:"retained_bytes"`
	}
	if err := bson.Unmarshal(frame.Payload, &decoded); err != nil {
		t.Fatalf("decoding the error frame: %v", err)
	}
	if decoded.Code != 14 {
		t.Errorf("code = %d, want 14 (RESULT_TOO_LARGE)", decoded.Code)
	}
	if decoded.MaxResultBytes == nil || *decoded.MaxResultBytes != refused.ceiling {
		t.Errorf("max_result_bytes = %v, want %d", decoded.MaxResultBytes, refused.ceiling)
	}
	if decoded.RetainedBytes == nil || *decoded.RetainedBytes != refused.retained {
		t.Errorf("retained_bytes = %v, want %d", decoded.RetainedBytes, refused.retained)
	}
	if decoded.Message != refused.Error() {
		t.Errorf("message = %q\nwant the typed error's own text %q", decoded.Message, refused.Error())
	}
	for _, prefix := range []string{"scanning app.events", "iterating key"} {
		if strings.Contains(decoded.Message, prefix) {
			t.Errorf("message carries the wrap prefix %q: %q", prefix, decoded.Message)
		}
	}
}

// TestErrorFrameOmitsResultFieldsForOtherErrors: W-3's fields appear only
// with RESULT_TOO_LARGE, so every other error frame keeps today's two-field
// shape.
func TestErrorFrameOmitsResultFieldsForOtherErrors(t *testing.T) {
	frame := errorFrame(transport.NewFrame(transport.OpScan, nil), bucketNotFoundErr("app", "events"))
	var fields bson.M
	if err := bson.Unmarshal(frame.Payload, &fields); err != nil {
		t.Fatalf("decoding the error frame: %v", err)
	}
	for _, name := range []string{"max_result_bytes", "retained_bytes"} {
		if _, present := fields[name]; present {
			t.Errorf("a NOT_FOUND error frame carries %s: %v", name, fields)
		}
	}
}

// TestResultTooLargeMessages holds W-2's three shapes: each names the query,
// the ceiling and the fix (AGENTS.md rule 3).
func TestResultTooLargeMessages(t *testing.T) {
	cases := []struct {
		name    string
		refused *resultTooLargeError
		want    string
	}{
		{"find", &resultTooLargeError{
			query: "find() on app.events", shape: resultShapeFind,
			ceiling: 16 << 20, retained: 16 << 20, documents: 41213, limit: 100000,
		}, "find() on app.events would return more than 16 MiB (stopped at 16.0 MiB after 41,213 documents, " +
			"limit 100000). Ask for fewer documents per request — {limit: 1000} and page with skip — or add a " +
			"projection to return fewer fields. The ceiling is HOARDDB_QUERY_MAX_RESULT_BYTES (16 MiB)."},
		{"ranking", &resultTooLargeError{
			query: "find() on app.events", shape: resultShapeRanking,
			ceiling: 16 << 20, retained: 16 << 20, skip: 900000, limit: 100, sortField: "created_at",
		}, "find() on app.events sorts for skip 900000 + limit 100, which needs more than 16 MiB to rank. " +
			"Page by the sort field instead of skip — {created_at: {$gt: <last value you saw>}}, sort " +
			"{created_at: 1}, limit 100 — or add a filter that narrows the match set."},
		{"distinct", &resultTooLargeError{
			query: `distinct("session_id") on app.events`, shape: resultShapeDistinct,
			ceiling: 16 << 20, retained: 16 << 20,
		}, `distinct("session_id") on app.events found more than 16 MiB of distinct values. ` +
			"Add a filter to narrow it, or count instead."},
		{"a ceiling that is not whole MiB", &resultTooLargeError{
			query: "find() on app.events", shape: resultShapeFind,
			ceiling: 3<<20 + 1<<19, retained: 3<<20 + 1<<19, documents: 12, limit: 0,
		}, "find() on app.events would return more than 3.5 MiB (stopped at 3.5 MiB after 12 documents, " +
			"no limit). Ask for fewer documents per request — {limit: 1000} and page with skip — or add a " +
			"projection to return fewer fields. The ceiling is HOARDDB_QUERY_MAX_RESULT_BYTES (3.5 MiB)."},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.refused.Error(); got != tc.want {
				t.Errorf("message:\n got %q\nwant %q", got, tc.want)
			}
		})
	}
}

// TestResponseFrameTooLargeMetricReadsTheGuard: the frame-length guard's
// count lives in transport, and /metrics exports it as
// hoarddb_response_frame_too_large_total.
func TestResponseFrameTooLargeMetricReadsTheGuard(t *testing.T) {
	if got, want := counterValue(t, "hoarddb_response_frame_too_large_total"), float64(transport.OversizedResponses()); got != want {
		t.Fatalf("hoarddb_response_frame_too_large_total = %v, want transport.OversizedResponses() = %v", got, want)
	}
}

// TestResultShapePeekMessageNamesLimitCeilingAndCount: a refused peek says
// which count fits and what to raise, and none of the advice that only a find
// has (skip, a projection).
func TestResultShapePeekMessageNamesLimitCeilingAndCount(t *testing.T) {
	cases := []struct {
		name      string
		query     string
		documents int
		wantFits  string
	}{
		{"bucket", "peek({limit: 1000}) on app.Jobs", 412, "peek({limit: 412}) fits"},
		{"field", `peek({limit: 1000}) on app.Post["p1"].Comments`, 3, "peek({limit: 3}) fits"},
		{"no entry held clamps to one", "peek({limit: 5}) on app.Jobs", 0, "peek({limit: 1}) fits"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			refused := &resultTooLargeError{query: tc.query, shape: resultShapePeek, ceiling: 16 << 20, retained: 16<<20 - 200<<10, documents: tc.documents}
			message := refused.Error()
			for _, want := range []string{tc.query, "more than 16 MiB", "15.8 MiB", tc.wantFits, "HOARDDB_QUERY_MAX_RESULT_BYTES"} {
				if !strings.Contains(message, want) {
					t.Errorf("message lacks %q: %s", want, message)
				}
			}
			for _, banned := range []string{"skip", "projection", "{limit: 1000} and"} {
				if strings.Contains(message, banned) {
					t.Errorf("message mentions %q, which a peek has no way to use: %s", banned, message)
				}
			}
		})
	}
}
