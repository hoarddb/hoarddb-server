package cli

import (
	"bytes"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// TestTheCLISaysWhenAWriteIsOnFewerCopiesThanItsFactor is T-109 (spec §19.7).
// A server whose replicas are stopped answers a write with copies_confirmed
// below a majority of replication_factor (T-104 is that half); the CLI then
// says so in one line on its commentary stream, which a one-shot run sends to
// stderr, and the results stream is what it would have been. A write a
// majority holds (BUG-0691: the default concern's normal answer), a write on
// every copy, and a write on a single node (no count at all), say nothing. The stub stands in
// for the cluster: this package cannot start one.
func TestTheCLISaysWhenAWriteIsOnFewerCopiesThanItsFactor(t *testing.T) {
	cases := []struct {
		name    string
		answer  server.InsertResponse
		chatter string
	}{
		{"two replicas are stopped", server.InsertResponse{ID: "k1", Success: true, CopiesConfirmed: 1, ReplicationFactor: 3}, "stored on 1 of 3 copies; the other copies are behind or refused it.\nRun 'status' and read the node logs - a replica still catching up just after 'create bucket' is normal; a node down or over its disk high-water mark is not.\n"},
		{"a majority holds it, the third follows", server.InsertResponse{ID: "k1", Success: true, CopiesConfirmed: 2, ReplicationFactor: 3}, ""},
		{"every copy holds it", server.InsertResponse{ID: "k1", Success: true, CopiesConfirmed: 3, ReplicationFactor: 3}, ""},
		{"a single node", server.InsertResponse{ID: "k1", Success: true}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var results, chatter bytes.Buffer
			_, c := newStubServer(t, map[transport.Opcode]interface{}{transport.OpPut: tc.answer}, &results)
			c.handler.SetChatter(&chatter)
			c.currentDB = "app"
			results.Reset()
			if err := c.handle(`db.Users.insert({"_id_": "k1"})`); err != nil {
				t.Fatalf("insert: %v", err)
			}
			if got := results.String(); got != "OK  id=k1\n" {
				t.Errorf("the results stream carries %q, want only the insert's line", got)
			}
			if got := chatter.String(); got != tc.chatter {
				t.Errorf("the commentary stream carries %q, want %q", got, tc.chatter)
			}
		})
	}
}
