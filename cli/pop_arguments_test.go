package cli

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// popLevels are the two places pop lives: a bucket and one document's store
// field, in the shape peekLevels uses for peek.
var popLevels = []struct {
	name      string
	statement func(arguments string) string
	opcode    transport.Opcode
	reply     interface{}
}{
	{"bucket", func(arguments string) string { return `db.Jobs.pop(` + arguments + `)` },
		transport.OpFIFOPop, server.PopResponse{Found: false, Message: "queue is empty"}},
	{"field", func(arguments string) string { return `db.Post["p1"].Comments.pop(` + arguments + `)` },
		transport.OpFieldPop, server.FieldResponse{Success: true}},
}

// TestPopRefusesWhatItDoesNotUnderstand is BUG-0546's acceptance test: pop
// ignored its arguments and removed one entry whatever it was given, so
// pop(3) and pop({bogus: true}) each lost an entry without a word. Each is now
// a usage error naming pop({limit: N}), and nothing is removed: the stub
// server sees no pop request, and against the real handlers the queue holds
// the same entries before and after.
func TestPopRefusesWhatItDoesNotUnderstand(t *testing.T) {
	for _, level := range popLevels {
		for _, arguments := range []string{`{bogus: true}`, `3`} {
			t.Run("stub/"+level.name+"/"+arguments, func(t *testing.T) {
				var out bytes.Buffer
				stub, c := newStubServer(t, map[transport.Opcode]interface{}{level.opcode: level.reply}, &out)
				c.currentDB = "app"
				err := c.handle(level.statement(arguments))
				var usage *UsageError
				if !errors.As(err, &usage) || !strings.Contains(err.Error(), "pop({limit:") {
					t.Fatalf("%s = %v, want a usage error naming pop({limit:", level.statement(arguments), err)
				}
				stub.mu.Lock()
				defer stub.mu.Unlock()
				if len(stub.requests) != 0 {
					t.Errorf("the refused statement sent %v", stub.requests)
				}
			})
		}
	}

	c := newDumpTestServer(t)
	for _, line := range []string{
		`create database app`,
		`create bucket Jobs { type: fifo }`,
		`create bucket Post { type: hash, Comments fifoStore }`,
		`db.Post.insert({"_id_": "p1"})`,
		`db.Jobs.push({"n": 1})`, `db.Jobs.push({"n": 2})`,
		`db.Post["p1"].Comments.push({"n": 1})`, `db.Post["p1"].Comments.push({"n": 2})`,
	} {
		if _, err := runStatement(t, c, line); err != nil {
			t.Fatalf("%s: %v", line, err)
		}
	}
	lengths := map[string]string{"bucket": `db.Jobs.length()`, "field": `db.Post["p1"].Comments.len()`}
	for _, level := range popLevels {
		for _, arguments := range []string{`{bogus: true}`, `3`} {
			t.Run("server/"+level.name+"/"+arguments, func(t *testing.T) {
				before, err := runStatement(t, c, lengths[level.name])
				if err != nil {
					t.Fatalf("%s: %v", lengths[level.name], err)
				}
				var usage *UsageError
				if _, err := runStatement(t, c, level.statement(arguments)); !errors.As(err, &usage) {
					t.Fatalf("%s = %v, want a usage error", level.statement(arguments), err)
				}
				after, err := runStatement(t, c, lengths[level.name])
				if err != nil {
					t.Fatalf("%s: %v", lengths[level.name], err)
				}
				if before != after {
					t.Errorf("%s changed the length from %q to %q: it removed an entry it was not asked to", level.statement(arguments), before, after)
				}
			})
		}
	}
}
