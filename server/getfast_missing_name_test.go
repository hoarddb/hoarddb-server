package server

import (
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestGetFastOfAMissingNameAnswersNotFoundAndKeepsTheConnection:
// handleGetFast returned its Find error as a Go error, which the transport
// treats as fatal: INTERNAL, then the connection closed, taking every request
// pipelined behind it. A typo in a database or bucket name must be NOT_FOUND
// on a connection that still works.
func TestGetFastOfAMissingNameAnswersNotFoundAndKeepsTheConnection(t *testing.T) {
	addr := newGetFastAuthzServer(t)

	cases := []struct{ name, database, bucket string }{
		{"missing database", "nodb", "events"},
		{"missing bucket", "app", "nobucket"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := mustDialQueryConn(t, addr, "admin", "test123")

			answer, err := sendGetFast(conn, mustGetFastPayload(t, tc.database, tc.bucket, "k01"))
			if err != nil {
				t.Fatalf("GET_FAST %s.%s: %v", tc.database, tc.bucket, err)
			}
			if answer.Opcode != transport.OpError {
				t.Fatalf("GET_FAST %s.%s answered %s; want an ERROR frame", tc.database, tc.bucket, answerText(answer))
			}
			var refusal ErrorResponse
			if err := bson.Unmarshal(answer.Payload, &refusal); err != nil {
				t.Fatalf("decoding the error frame: %v", err)
			}
			if refusal.Code != int(transport.ErrNotFound) {
				t.Errorf("GET_FAST %s.%s answered %s; want code %d (NOT_FOUND)", tc.database, tc.bucket, answerText(answer), transport.ErrNotFound)
			}

			next, err := sendGetFast(conn, mustGetFastPayload(t, "app", "events", "k01"))
			if err != nil {
				t.Fatalf("the next request on the same connection failed: %v", err)
			}
			if next.Opcode != transport.OpGetFastResponse {
				t.Errorf("the next request answered %s; want %s", answerText(next), transport.OpGetFastResponse)
			}
		})
	}
}
