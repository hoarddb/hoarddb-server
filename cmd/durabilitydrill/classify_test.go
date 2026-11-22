package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/ringclient"
)

// Only an answer of "applied" with no error is an acknowledgement. Everything else, a lost
// connection, a passed deadline, a refusal, an "unknown" or "applied_degraded" answer, is
// recorded as unknown: the server promised nothing the drill can hold it to.
func TestClassifyWriteAcknowledgesOnlyApplied(t *testing.T) {
	cases := []struct {
		name    string
		result  ringclient.WriteResult
		err     error
		outcome string
		detail  string
	}{
		{"applied", ringclient.WriteResult{Outcome: "applied"}, nil, outcomeAcked, ""},
		{"unknown outcome", ringclient.WriteResult{Outcome: "unknown"}, nil, outcomeUnknown, "outcome=unknown"},
		{"degraded", ringclient.WriteResult{Outcome: "applied_degraded"}, nil, outcomeUnknown, "outcome=applied_degraded"},
		{"guard refused", ringclient.WriteResult{Outcome: "not_applied", NotApplied: true}, nil, outcomeUnknown, "outcome=not_applied"},
		{"empty outcome", ringclient.WriteResult{}, nil, outcomeUnknown, "outcome="},
		{"lost connection", ringclient.WriteResult{}, io.ErrUnexpectedEOF, outcomeUnknown, "CONN"},
		{"deadline", ringclient.WriteResult{}, fmt.Errorf("insert: %w", context.DeadlineExceeded), outcomeUnknown, "TIMEOUT"},
		{"refusal", ringclient.WriteResult{}, &ringclient.Error{Code: cluster.CodeUnavailable, Message: "starting"}, outcomeUnknown, "UNAVAILABLE"},
		{"applied with an error", ringclient.WriteResult{Outcome: "applied"}, errors.New("reply lost"), outcomeUnknown, "CONN"},
	}
	for _, tc := range cases {
		outcome, detail := classifyWrite(tc.result, tc.err)
		if outcome != tc.outcome || detail != tc.detail {
			t.Errorf("%s: got (%s, %s), want (%s, %s)", tc.name, outcome, detail, tc.outcome, tc.detail)
		}
	}
}

func TestClassifyReadSeparatesAbsentFromUnreadable(t *testing.T) {
	notFound := &ringclient.Error{Code: cluster.CodeNotFound, Message: "no such document"}
	if state, retry := classifyReadError(notFound); state != stateAbsent || retry {
		t.Errorf("NOT_FOUND: got (%s, %v), want absent and no retry", state, retry)
	}
	for _, err := range []error{io.ErrUnexpectedEOF, context.DeadlineExceeded, &ringclient.Error{Code: cluster.CodeUnavailable}} {
		if state, retry := classifyReadError(err); state != stateUnreadable || !retry {
			t.Errorf("%v: got (%s, %v), want unreadable and a retry", err, state, retry)
		}
	}
}
