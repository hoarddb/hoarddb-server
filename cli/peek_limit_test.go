package cli

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// peekLevels are the two places peek lives: a bucket and one document's store
// field. statement builds the statement at a level.
var peekLevels = []struct {
	name      string
	statement func(arguments string) string
}{
	{"bucket", func(arguments string) string { return `db.Jobs.peek(` + arguments + `)` }},
	{"field", func(arguments string) string { return `db.Post["p1"].Photos.peek(` + arguments + `)` }},
}

// TestPeekPositionalCountIsRefusedWithTheFix: peek(3) is a usage error naming
// peek({limit: 3}) at both levels, and no request reaches the server. It
// replaces TestFieldPeekWithACountIsNeverSilentlyDropped, whose assertion that
// the refusal names list is the advice this build removes; the property it
// guarded, that an argument is never silently dropped, stays.
func TestPeekPositionalCountIsRefusedWithTheFix(t *testing.T) {
	for _, level := range peekLevels {
		t.Run(level.name, func(t *testing.T) {
			var out bytes.Buffer
			stub, c := newStubServer(t, map[transport.Opcode]interface{}{}, &out)
			c.currentDB = "app"
			err := c.handle(level.statement("3"))
			var usage *UsageError
			if !errors.As(err, &usage) {
				t.Fatalf("%s = %v, want a usage error", level.statement("3"), err)
			}
			for _, want := range []string{"peek(3) is not supported", "option document", "peek({limit: 3})", "without removing"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal lacks %q: %v", want, err)
				}
			}
			if strings.Contains(err.Error(), "list(") {
				t.Errorf("the refusal still points at list, which reads the other end of a lifo: %v", err)
			}
			stub.mu.Lock()
			defer stub.mu.Unlock()
			if len(stub.requests) != 0 {
				t.Errorf("the refused statement sent %v", stub.requests)
			}
		})
	}
}

// TestPeekLimitOptionValidation: the strict reader, shared by both levels,
// refuses anything but a whole limit of 1 or more and any other key, naming
// the accepted form, before a request is sent. A limit above the server's cap
// is not the CLI's to refuse.
func TestPeekLimitOptionValidation(t *testing.T) {
	bad := []struct{ arguments, says string }{
		{`{limit: -1}`, "whole number of 1 or more"},
		{`{limit: 0}`, "whole number of 1 or more"},
		{`{limit: 1.5}`, "whole number of 1 or more"},
		{`{limit: "3"}`, "whole number of 1 or more"},
		{`{limit: true}`, "whole number of 1 or more"},
		{`{lmit: 3}`, "limit"},
		{`{limit: 3, extra: 1}`, "limit"},
		{`{limit: 3}, 1`, "at most one argument"},
		{`abc`, "option document"},
	}
	for _, level := range peekLevels {
		for _, tc := range bad {
			t.Run(level.name+"/"+tc.arguments, func(t *testing.T) {
				var out bytes.Buffer
				stub, c := newStubServer(t, map[transport.Opcode]interface{}{}, &out)
				c.currentDB = "app"
				err := c.handle(level.statement(tc.arguments))
				var usage *UsageError
				if !errors.As(err, &usage) || !strings.Contains(err.Error(), tc.says) || !strings.Contains(err.Error(), "limit") {
					t.Fatalf("%s = %v, want a usage error naming %q and the limit form", level.statement(tc.arguments), err, tc.says)
				}
				stub.mu.Lock()
				defer stub.mu.Unlock()
				if len(stub.requests) != 0 {
					t.Errorf("the refused statement sent %v", stub.requests)
				}
			})
		}
	}

	for _, level := range peekLevels {
		t.Run(level.name+"/an option the server owns is sent", func(t *testing.T) {
			var out bytes.Buffer
			opcode, reply := transport.OpFIFOPeek, interface{}(server.PeekResponse{Found: false, Message: "queue is empty"})
			if level.name == "field" {
				opcode, reply = transport.OpFieldPeek, server.FieldResponse{Success: true}
			}
			stub, c := newStubServer(t, map[transport.Opcode]interface{}{opcode: reply}, &out)
			c.currentDB = "app"
			for _, arguments := range []string{`{limit: 5000}`, `{}`, `{limit: 3.0}`} {
				var usage *UsageError
				if err := c.handle(level.statement(arguments)); errors.As(err, &usage) {
					t.Fatalf("%s was refused by the CLI: %v", level.statement(arguments), err)
				}
			}
			stub.mu.Lock()
			defer stub.mu.Unlock()
			if len(stub.requests) != 3 {
				t.Errorf("sent %v, want three requests: the server owns the cap of 1000", stub.requests)
			}
		})
	}
}

var peekedNumber = regexp.MustCompile(`"n": (\d+)`)

// peekedNumbersIn is the n of every line the handler printed.
func peekedNumbersIn(t *testing.T, printed string) []int {
	t.Helper()
	var numbers []int
	for _, line := range strings.Split(strings.TrimSpace(printed), "\n") {
		match := peekedNumber.FindStringSubmatch(line)
		if match == nil {
			t.Fatalf("a printed line carries no n: %q in\n%s", line, printed)
		}
		n, _ := strconv.Atoi(match[1])
		numbers = append(numbers, n)
	}
	return numbers
}

// TestBucketPeekLimitPrintsPopOrder: peek({limit: 3}) prints what three pops
// would, in pop order, one line each, at the bucket level and on a nested
// field, for fifo, lifo and heap.
func TestBucketPeekLimitPrintsPopOrder(t *testing.T) {
	c := newDumpTestServer(t)
	for _, line := range []string{
		`create database app`,
		`create bucket Q { type: fifo }`, `create bucket S { type: lifo }`, `create bucket H { type: heap }`,
		`create bucket Post { type: hash, Comments fifoStore, Photos lifoStore, Ranked heapStore }`,
		`db.Post.insert({"_id_": "p1"})`,
	} {
		if _, err := runStatement(t, c, line); err != nil {
			t.Fatalf("%s: %v", line, err)
		}
	}
	for n := 1; n <= 5; n++ {
		for _, line := range []string{
			fmt.Sprintf(`db.Q.push({"n": %d})`, n), fmt.Sprintf(`db.S.push({"n": %d})`, n), fmt.Sprintf(`db.H.push({"priority": %d, "n": %d})`, n, n),
			fmt.Sprintf(`db.Post["p1"].Comments.push({"n": %d})`, n), fmt.Sprintf(`db.Post["p1"].Photos.push({"n": %d})`, n),
			fmt.Sprintf(`db.Post["p1"].Ranked.push({"priority": %d, "n": %d})`, n, n),
		} {
			if _, err := runStatement(t, c, line); err != nil {
				t.Fatalf("%s: %v", line, err)
			}
		}
	}
	cases := []struct {
		where string
		want  []int
	}{
		{"db.Q", []int{1, 2, 3}}, {"db.S", []int{5, 4, 3}}, {"db.H", []int{5, 4, 3}},
		{`db.Post["p1"].Comments`, []int{1, 2, 3}}, {`db.Post["p1"].Photos`, []int{5, 4, 3}}, {`db.Post["p1"].Ranked`, []int{5, 4, 3}},
	}
	for _, tc := range cases {
		t.Run(tc.where, func(t *testing.T) {
			printed, err := runStatement(t, c, tc.where+`.peek({limit: 3})`)
			if err != nil {
				t.Fatalf("peek({limit: 3}): %v", err)
			}
			if got := peekedNumbersIn(t, printed); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("peek({limit: 3}) printed %v, want %v:\n%s", got, tc.want, printed)
			}
			one, err := runStatement(t, c, tc.where+`.peek()`)
			if err != nil || !reflect.DeepEqual(peekedNumbersIn(t, one), tc.want[:1]) {
				t.Fatalf("peek() printed %q (%v), want the first of %v", one, err, tc.want)
			}
		})
	}
}

// TestPeekOnHashBTreeBlobNamesTheVerbs: a bucket type with no pop order is
// refused with why and the verbs that read it, with and without a limit.
func TestPeekOnHashBTreeBlobNamesTheVerbs(t *testing.T) {
	session := newFixtureSession(t)
	cases := []struct {
		bucket string
		says   []string
	}{
		{"Users", []string{"peek is not defined for hash buckets", `get("<key>")`, "find()"}},
		{"Events", []string{"peek is not defined for btree buckets", `get("<key>")`, "find()"}},
		{"Files", []string{"peek is not defined for blob buckets", `get("<key>")`, `info("<key>")`}},
	}
	for _, tc := range cases {
		for _, arguments := range []string{``, `{limit: 2}`} {
			t.Run(tc.bucket+"/"+arguments, func(t *testing.T) {
				session.out.Reset()
				err := session.cli.handle(`db.` + tc.bucket + `.peek(` + arguments + `)`)
				if err == nil {
					t.Fatalf("peek on %s printed %q and succeeded", tc.bucket, session.out.String())
				}
				for _, want := range tc.says {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("the refusal lacks %q: %v", want, err)
					}
				}
				if session.out.Len() != 0 {
					t.Errorf("printed %q before failing", session.out.String())
				}
			})
		}
	}
}
