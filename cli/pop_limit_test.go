package cli

import (
	"bytes"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// TestPopPositionalCountIsRefusedWithTheFix: pop(3) is a usage error naming
// pop({limit: 3}) at both levels, and no request reaches the server. On the
// base tree it removed one entry and said nothing (BUG-0546).
func TestPopPositionalCountIsRefusedWithTheFix(t *testing.T) {
	for _, level := range popLevels {
		t.Run(level.name, func(t *testing.T) {
			var out bytes.Buffer
			stub, c := newStubServer(t, map[transport.Opcode]interface{}{level.opcode: level.reply}, &out)
			c.currentDB = "app"
			err := c.handle(level.statement("3"))
			var usage *UsageError
			if !errors.As(err, &usage) {
				t.Fatalf("%s = %v, want a usage error", level.statement("3"), err)
			}
			for _, want := range []string{"pop(3) is not supported", "option document", "pop({limit: 3})", "remove and return the next 3"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal lacks %q: %v", want, err)
				}
			}
			if sent := stub.sequence(); len(sent) != 0 {
				t.Errorf("the refused statement sent %v", sent)
			}
		})
	}
}

// TestPopLimitOptionValidation: the strict reader, shared by both levels,
// refuses anything but a whole limit of 1 or more and any other key, naming
// the accepted form, before a request is sent. A limit above the server's cap
// is not the CLI's to refuse.
func TestPopLimitOptionValidation(t *testing.T) {
	bad := []struct{ arguments, says string }{
		{`{limit: -1}`, "whole number of 1 or more"},
		{`{limit: 0}`, "whole number of 1 or more"},
		{`{limit: 1.5}`, "whole number of 1 or more"},
		{`{limit: "3"}`, "whole number of 1 or more"},
		{`{lmit: 3}`, `"lmit" is not it`},
		{`{limit: 3, extra: 1}`, `"extra" is not it`},
		{`{limit: 3}, 1`, "at most one argument"},
		{`abc`, "option document"},
	}
	for _, level := range popLevels {
		for _, tc := range bad {
			t.Run(level.name+"/"+tc.arguments, func(t *testing.T) {
				var out bytes.Buffer
				stub, c := newStubServer(t, map[transport.Opcode]interface{}{level.opcode: level.reply}, &out)
				c.currentDB = "app"
				err := c.handle(level.statement(tc.arguments))
				var usage *UsageError
				if !errors.As(err, &usage) || !strings.Contains(err.Error(), tc.says) || !strings.Contains(err.Error(), "pop({limit:") {
					t.Fatalf("%s = %v, want a usage error naming %q and pop({limit:", level.statement(tc.arguments), err, tc.says)
				}
				if sent := stub.sequence(); len(sent) != 0 {
					t.Errorf("the refused statement sent %v", sent)
				}
			})
		}
	}

	for _, level := range popLevels {
		t.Run(level.name+"/an option the server owns is sent", func(t *testing.T) {
			var out bytes.Buffer
			stub, c := newStubServer(t, map[transport.Opcode]interface{}{level.opcode: level.reply}, &out)
			c.currentDB = "app"
			for _, arguments := range []string{`{limit: 5000}`, `{}`, `{limit: 3.0}`} {
				var usage *UsageError
				if err := c.handle(level.statement(arguments)); errors.As(err, &usage) {
					t.Fatalf("%s was refused by the CLI: %v", level.statement(arguments), err)
				}
			}
			if sent := stub.sent(level.opcode); sent != 3 {
				t.Errorf("sent %d pops, want three: the server owns the cap of 100", sent)
			}
		})
	}
}

// TestCLIPopLimitSendsOneFrame: pop({limit: 3}) is one request carrying
// limit 3 at both levels, never three single pops, which would be three
// separate writes and three chances to lose an answer.
func TestCLIPopLimitSendsOneFrame(t *testing.T) {
	for _, level := range popLevels {
		t.Run(level.name, func(t *testing.T) {
			var out bytes.Buffer
			stub, c := newStubServer(t, map[transport.Opcode]interface{}{level.opcode: level.reply}, &out)
			c.currentDB = "app"
			if err := c.handle(level.statement(`{limit: 3}`)); err != nil {
				t.Fatalf("%s: %v", level.statement(`{limit: 3}`), err)
			}
			payloads := stub.payloadsOf(level.opcode)
			if len(payloads) != 1 {
				t.Fatalf("pop({limit: 3}) sent %d pop frames, want exactly one", len(payloads))
			}
			if limit, ok := bson.Raw(payloads[0]).Lookup("limit").AsInt64OK(); !ok || limit != 3 {
				t.Errorf("the pop frame carries limit %v (%v), want 3: %s", limit, ok, bson.Raw(payloads[0]))
			}
			if len(stub.sequence()) != 1 {
				t.Errorf("the statement sent more than the one pop frame")
			}
		})
	}
}

// TestPopLimitPrintsPopOrder: pop({limit: 3}) removes and prints what three
// pops would, in pop order, one line each, at the bucket level and on a nested
// field, for fifo, lifo and heap; two entries are left.
func TestPopLimitPrintsPopOrder(t *testing.T) {
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
	pushes := func(n int) []string {
		return []string{
			`db.Q.push({"n": ` + strconv.Itoa(n) + `})`, `db.S.push({"n": ` + strconv.Itoa(n) + `})`, `db.H.push({"priority": ` + strconv.Itoa(n) + `, "n": ` + strconv.Itoa(n) + `})`,
			`db.Post["p1"].Comments.push({"n": ` + strconv.Itoa(n) + `})`, `db.Post["p1"].Photos.push({"n": ` + strconv.Itoa(n) + `})`,
			`db.Post["p1"].Ranked.push({"priority": ` + strconv.Itoa(n) + `, "n": ` + strconv.Itoa(n) + `})`,
		}
	}
	for n := 1; n <= 5; n++ {
		for _, line := range pushes(n) {
			if _, err := runStatement(t, c, line); err != nil {
				t.Fatalf("%s: %v", line, err)
			}
		}
	}
	cases := []struct {
		where string
		want  []int
		left  []int
	}{
		{"db.Q", []int{1, 2, 3}, []int{4, 5}}, {"db.S", []int{5, 4, 3}, []int{2, 1}}, {"db.H", []int{5, 4, 3}, []int{2, 1}},
		{`db.Post["p1"].Comments`, []int{1, 2, 3}, []int{4, 5}}, {`db.Post["p1"].Photos`, []int{5, 4, 3}, []int{2, 1}},
		{`db.Post["p1"].Ranked`, []int{5, 4, 3}, []int{2, 1}},
	}
	for _, tc := range cases {
		t.Run(tc.where, func(t *testing.T) {
			printed, err := runStatement(t, c, tc.where+`.pop({limit: 3})`)
			if err != nil {
				t.Fatalf("pop({limit: 3}): %v", err)
			}
			if got := peekedNumbersIn(t, printed); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("pop({limit: 3}) printed %v, want %v:\n%s", got, tc.want, printed)
			}
			left, err := runStatement(t, c, tc.where+`.peek({limit: 9})`)
			if err != nil || !reflect.DeepEqual(peekedNumbersIn(t, left), tc.left) {
				t.Fatalf("after the pop, peek({limit: 9}) printed %q (%v), want %v", left, err, tc.left)
			}
		})
	}
}

// TestPopOnHashBTreeBlobNamesTheVerbs: a bucket type with no pop order is
// refused with why and the verbs that work, with and without a limit.
func TestPopOnHashBTreeBlobNamesTheVerbs(t *testing.T) {
	session := newFixtureSession(t)
	cases := []struct {
		bucket string
		says   []string
	}{
		{"Users", []string{"pop is not defined for hash buckets", `delete("<key>")`, `get("<key>")`, "find()"}},
		{"Events", []string{"pop is not defined for btree buckets", `delete("<key>")`, `get("<key>")`, "find()"}},
		{"Files", []string{"pop is not defined for blob buckets", `delete("<key>")`, `info("<key>")`}},
	}
	for _, tc := range cases {
		for _, arguments := range []string{``, `{limit: 2}`} {
			t.Run(tc.bucket+"/"+arguments, func(t *testing.T) {
				session.out.Reset()
				err := session.cli.handle(`db.` + tc.bucket + `.pop(` + arguments + `)`)
				if err == nil {
					t.Fatalf("pop on %s printed %q and succeeded", tc.bucket, session.out.String())
				}
				for _, want := range tc.says {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("the refusal lacks %q: %v", want, err)
					}
				}
				if strings.Contains(err.Error(), "pop not supported") {
					t.Errorf("the refusal is the old text: %v", err)
				}
			})
		}
	}
}

// TestPeekMessagesUnchangedByTheSharedReader pins every peek usage message
// byte for byte at both levels, so that sharing the reader with pop did not
// move a word of peek's.
func TestPeekMessagesUnchangedByTheSharedReader(t *testing.T) {
	for _, level := range peekLevels {
		where := "db.Jobs"
		if level.name == "field" {
			where = `db.Post["p1"].Photos`
		}
		cases := []struct{ arguments, want string }{
			{`{limit: 1}, 2`, "peek takes at most one argument, an option document: " + where + ".peek({limit: N}) reads the next N without removing them"},
			{`3`, "peek(3) is not supported: the count goes in an option document.\n       Use " + where + ".peek({limit: 3}) to read the next 3 without removing them."},
			{`a`, "peek(a) is not supported: the count goes in an option document.\n       Use " + where + ".peek({limit: N}) to read the next N without removing them."},
			{`{lmit: 3}`, `peek has one option, limit, and "lmit" is not it: ` + where + ".peek({limit: 3}) reads the next 3 without removing them"},
			{`{limit: 0}`, "peek's limit is a whole number of 1 or more, as in " + where + ".peek({limit: 3}); got 0"},
			{`{limit: "3"}`, `peek's limit is a whole number of 1 or more, as in ` + where + `.peek({limit: 3}); got "3"`},
		}
		for _, tc := range cases {
			t.Run(level.name+"/"+tc.arguments, func(t *testing.T) {
				var out bytes.Buffer
				_, c := newStubServer(t, map[transport.Opcode]interface{}{}, &out)
				c.currentDB = "app"
				err := c.handle(level.statement(tc.arguments))
				var usage *UsageError
				if !errors.As(err, &usage) || err.Error() != tc.want {
					t.Fatalf("%s = %q, want the usage error %q", level.statement(tc.arguments), err, tc.want)
				}
			})
		}
	}
}
